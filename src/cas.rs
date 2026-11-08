//! Compacted blobs: a gzip blob becomes a preflate recipe plus content-defined chunks stored once
//! in shared packs, and is served by rebuilding restart-point groups as a stream.
//!
//! Layout in the backing bucket:
//!   .rift/packs/NNNNNNNN.zst   unique chunks, zstd with long-distance matching
//!   .rift/packs/NNNNNNNN.idx   (blake3, offset, length) of every chunk in the pack
//!   .rift/m/<key>              manifest of a compacted blob; the original key keeps an empty stub

use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::{
        Arc, Condvar, Mutex, OnceLock, RwLock,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    thread,
};

use anyhow::{Context, Result, anyhow, ensure};
use fastcdc::v2020::FastCDC;
use preflate_rs::{IndependentRecipe, PreflateConfig, RebuiltGroup, Splicer, preflate_independent};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use zstd::stream::raw::CParameter;

pub use crate::index::{Entry, Ref};
use crate::{
    index::Index,
    s3::Backend,
    sigv4::{hex, sha256_hex, unix_now},
    store::gzip_payload_offset,
};

/// plain bytes per restart-point group
pub const GROUP_PLAIN: usize = 8 << 20;
const PACK_BYTES: usize = 16 << 20;
const PACKS: &str = ".rift/packs/";
const MANIFESTS: &str = ".rift/m/";
const MAGIC: &[u8; 8] = b"riftman1";


pub struct Manifest {
    pub key: String,
    pub size: u64,
    pub sha256: String,
    /// last-modified of the original object, unix seconds
    pub mtime: u64,
    /// gzip header
    pub head: Vec<u8>,
    /// the deflate stream's final partial byte, then the gzip trailer
    pub end: Vec<u8>,
    /// where each group's bytes start in the blob, and the bits the group before left for its first byte
    pub groups: Vec<(u64, Option<u8>)>,
    pub refs: Vec<Ref>,
    /// preflate_independent data, zstd-compressed
    pub recipe: Vec<u8>,
    ref_start: Vec<u64>,
    parsed: OnceLock<IndependentRecipe>,
}

impl Manifest {
    #[allow(clippy::too_many_arguments)]
    fn new(
        key: String,
        size: u64,
        sha256: String,
        mtime: u64,
        head: Vec<u8>,
        end: Vec<u8>,
        groups: Vec<(u64, Option<u8>)>,
        refs: Vec<Ref>,
        recipe: Vec<u8>,
    ) -> Self {
        let mut m = Self { key, size, sha256, mtime, head, end, groups, refs: Vec::new(), recipe, ref_start: Vec::new(), parsed: OnceLock::new() };
        m.set_refs(refs);
        m
    }

    fn set_refs(&mut self, refs: Vec<Ref>) {
        self.ref_start = refs
            .iter()
            .scan(0u64, |acc, r| {
                let s = *acc;
                *acc += u64::from(r[2]);
                Some(s)
            })
            .collect();
        self.refs = refs;
    }

    /// The same blob with its chunks somewhere else.
    fn with_refs(&self, refs: Vec<Ref>) -> Self {
        Self::new(
            self.key.clone(),
            self.size,
            self.sha256.clone(),
            self.mtime,
            self.head.clone(),
            self.end.clone(),
            self.groups.clone(),
            refs,
            self.recipe.clone(),
        )
    }

    pub fn etag(&self) -> String {
        // multipart-style, so clients don't take it for an MD5 of the body
        format!("\"{}-1\"", &self.sha256[..32])
    }

    fn recipe(&self) -> Result<&IndependentRecipe> {
        if let Some(r) = self.parsed.get() {
            return Ok(r);
        }
        let raw = zstd::bulk::decompress(&self.recipe, 1 << 30)?;
        let r = IndependentRecipe::parse(&raw).map_err(|e| anyhow!("recipe of {}: {e:?}", self.key))?;
        let _ = self.parsed.set(r);
        Ok(self.parsed.get().expect("just set"))
    }

    pub fn encode(&self) -> Vec<u8> {
        let header = json!({
            "key": self.key, "size": self.size, "sha256": self.sha256, "mtime": self.mtime,
            "head": hex(&self.head), "end": hex(&self.end),
            "groups": self.groups.iter().map(|&(at, carry)| json!([at, carry])).collect::<Vec<_>>(),
            "refs": self.refs.len(),
        });
        let header = serde_json::to_vec(&header).expect("plain JSON");
        let mut out = MAGIC.to_vec();
        out.extend((header.len() as u32).to_le_bytes());
        out.extend(header);
        for r in &self.refs {
            for v in r {
                out.extend(v.to_le_bytes());
            }
        }
        out.extend(&self.recipe);
        out
    }

    /// The header and where the chunk refs end, from a prefix of the encoded manifest.
    fn header(b: &[u8]) -> Result<(Value, usize, usize)> {
        ensure!(b.len() > 12 && &b[..8] == MAGIC, "not a rift manifest");
        let hlen = u32::from_le_bytes(b[8..12].try_into()?) as usize;
        let h: Value = serde_json::from_slice(b.get(12..12 + hlen).context("short manifest")?)?;
        let nrefs = h["refs"].as_u64().context("refs")? as usize;
        Ok((h, 12 + hlen, 12 + hlen + nrefs * 12))
    }

    fn refs_from(b: &[u8], from: usize, to: usize) -> Result<Vec<Ref>> {
        Ok(b.get(from..to)
            .context("short manifest refs")?
            .chunks_exact(12)
            .map(|c| {
                let v = |i: usize| u32::from_le_bytes(c[i..i + 4].try_into().expect("4 bytes"));
                [v(0), v(4), v(8)]
            })
            .collect())
    }

    /// Roughly what it takes in memory.
    fn bytes(&self) -> usize {
        self.recipe.len() + self.refs.len() * 20 + self.groups.len() * 16 + self.head.len() + self.end.len() + 256
    }

    pub fn decode(b: &[u8]) -> Result<Self> {
        let (h, refs_at, refs_end) = Self::header(b)?;
        let unhex = |v: &Value| -> Result<Vec<u8>> {
            let s = v.as_str().context("hex field")?;
            (0..s.len()).step_by(2).map(|i| Ok(u8::from_str_radix(&s[i..i + 2], 16)?)).collect()
        };
        let groups = h["groups"]
            .as_array()
            .context("groups")?
            .iter()
            .map(|g| Ok((g[0].as_u64().context("group offset")?, g[1].as_u64().map(|c| c as u8))))
            .collect::<Result<Vec<_>>>()?;
        let refs = Self::refs_from(b, refs_at, refs_end)?;
        Ok(Self::new(
            h["key"].as_str().context("key")?.to_string(),
            h["size"].as_u64().context("size")?,
            h["sha256"].as_str().context("sha256")?.to_string(),
            h["mtime"].as_u64().unwrap_or(0),
            unhex(&h["head"])?,
            unhex(&h["end"])?,
            groups,
            refs,
            b[refs_end..].to_vec(),
        ))
    }
}

fn parallel<R: Send>(n: usize, threads: usize, f: impl Fn(usize) -> R + Sync) -> Vec<R> {
    let next = AtomicUsize::new(0);
    let out: Vec<Mutex<Option<R>>> = (0..n).map(|_| Mutex::new(None)).collect();
    thread::scope(|s| {
        for _ in 0..threads.clamp(1, n.max(1)) {
            s.spawn(|| {
                loop {
                    let i = next.fetch_add(1, Ordering::Relaxed);
                    if i >= n {
                        break;
                    }
                    *out[i].lock().unwrap() = Some(f(i));
                }
            });
        }
    });
    out.into_iter().map(|m| m.into_inner().unwrap().expect("every index ran")).collect()
}

/// Unpacks a single-member gzip blob. Returns the manifest without chunk refs or group positions
/// (`end` holds just the gzip trailer until `complete`) and the plain text, or None when the blob
/// isn't gzip preflate can take.
pub fn analyze(key: &str, digest: &str, mtime: u64, data: &[u8], threads: usize) -> Result<Option<(Manifest, Vec<u8>)>> {
    let Some(start) = gzip_payload_offset(data) else { return Ok(None) };
    let config = PreflateConfig { max_chain_length: 4096, plain_text_limit: 4 << 30, verify_compression: false };
    let Ok((raw, deflate_len, plain)) = preflate_independent(&data[start..], &config, GROUP_PLAIN, threads) else {
        return Ok(None);
    };
    if start + deflate_len + 8 != data.len() {
        return Ok(None);
    }
    let trailer = data[start + deflate_len..].to_vec();
    let recipe = zstd::bulk::compress(&raw, 19)?;
    let m = Manifest::new(key.into(), data.len() as u64, digest.into(), mtime, data[..start].to_vec(), trailer, Vec::new(), Vec::new(), recipe);
    Ok(Some((m, plain)))
}

/// Rebuilds the blob from its chunks, records where each group's bytes land and checks the result
/// against the digest. False when it doesn't match; the manifest is then incomplete.
pub fn complete(m: &mut Manifest, workers: usize, pack: &PackFn) -> Result<bool> {
    let (groups, end, digest) = {
        let recipe = m.recipe()?;
        let mut sha = Sha256::new();
        sha.update(&m.head);
        let (mut splicer, mut at, mut groups) = (Splicer::default(), m.head.len() as u64, Vec::new());
        rebuild_groups(m, recipe, 0, recipe.groups(), workers, pack, &mut |_, part| {
            groups.push((at, splicer.carry()));
            let bytes = splicer.push(part);
            sha.update(&bytes);
            at += bytes.len() as u64;
            true
        })?;
        let mut end: Vec<u8> = splicer.finish().into_iter().collect();
        end.extend_from_slice(&m.end);
        sha.update(&end);
        if at + end.len() as u64 != m.size {
            return Ok(false);
        }
        (groups, end, hex(&sha.finalize()))
    };
    if digest != m.sha256 {
        return Ok(false);
    }
    m.groups = groups;
    m.end = end;
    Ok(true)
}


/// Puts unique chunks into packs. Full packs queue up and are compressed in parallel, then handed to
/// `store(id, zstd bytes, index bytes)`; a chunk is only safe to rely on after `flush` succeeds.
pub struct Packer {
    index: Arc<Index>,
    /// chunks in packs not stored yet
    pending: HashMap<[u8; 32], Ref>,
    /// packs dedup mustn't point into (GC is emptying them)
    exclude: HashSet<u32>,
    /// stored size and chunk bytes of every pack
    packs: HashMap<u32, (u64, u64)>,
    id: u32,
    data: Vec<u8>,
    entries: Vec<Entry>,
    /// full packs not stored yet
    queued: Vec<(u32, Vec<u8>, Vec<Entry>)>,
    level: i32,
    threads: usize,
}

pub type PackStore<'a> = dyn Fn(u32, Vec<u8>, Vec<u8>) -> Result<()> + Sync + 'a;

impl Packer {
    pub fn new(index: Arc<Index>, packs: HashMap<u32, (u64, u64)>, next_id: u32, level: i32, threads: usize) -> Self {
        Self {
            index,
            pending: HashMap::new(),
            exclude: HashSet::new(),
            packs,
            id: next_id,
            data: Vec::with_capacity(PACK_BYTES),
            entries: Vec::new(),
            queued: Vec::new(),
            level,
            threads: threads.max(1),
        }
    }

    /// Cuts `plain` into chunks and returns their refs; new chunks go into the open pack.
    pub fn add(&mut self, plain: &[u8], store: &PackStore) -> Result<Vec<Ref>> {
        FastCDC::new(plain, 16 << 10, 64 << 10, 256 << 10)
            .map(|c| {
                let bytes = &plain[c.offset..c.offset + c.length];
                self.add_chunk(*blake3::hash(bytes).as_bytes(), bytes, store)
            })
            .collect()
    }

    /// Where an identical chunk is already stored, or where this one goes in the open pack.
    pub fn add_chunk(&mut self, hash: [u8; 32], bytes: &[u8], store: &PackStore) -> Result<Ref> {
        if let Some(r) = self.pending.get(&hash) {
            return Ok(*r);
        }
        if let Some(r) = self.index.lookup(&hash)?
            && !self.exclude.contains(&r[0])
        {
            return Ok(r);
        }
        if self.data.len() + bytes.len() > PACK_BYTES {
            self.close();
            if self.queued.len() >= self.threads {
                self.flush(store)?;
            }
        }
        let r = [self.id, self.data.len() as u32, bytes.len() as u32];
        self.data.extend_from_slice(bytes);
        self.entries.push((hash, r[1], r[2]));
        self.pending.insert(hash, r);
        Ok(r)
    }

    /// Queues the open pack.
    fn close(&mut self) {
        if !self.data.is_empty() {
            let data = std::mem::replace(&mut self.data, Vec::with_capacity(PACK_BYTES));
            self.queued.push((self.id, data, std::mem::take(&mut self.entries)));
            self.id += 1;
        }
    }

    /// Compresses and stores every queued pack and the open one, in parallel, then indexes them.
    /// Chunks of a pack that fails to store are forgotten, so nothing later refers to them.
    pub fn flush(&mut self, store: &PackStore) -> Result<()> {
        self.close();
        let queued = std::mem::take(&mut self.queued);
        let level = self.level;
        let results = parallel(queued.len(), self.threads, |i| -> Result<u64> {
            let (id, data, entries) = &queued[i];
            let mut z = zstd::bulk::Compressor::new(level)?;
            z.set_parameter(CParameter::EnableLongDistanceMatching(true))?;
            z.set_parameter(CParameter::WindowLog(24))?;
            let mut idx = Vec::with_capacity(entries.len() * 40);
            for (h, at, len) in entries {
                idx.extend(h);
                idx.extend(at.to_le_bytes());
                idx.extend(len.to_le_bytes());
            }
            let z = z.compress(data)?;
            let size = z.len() as u64;
            store(*id, z, idx).map(|_| size)
        });
        let (mut stored, mut failed) = (Vec::new(), None);
        for ((id, _, entries), result) in queued.into_iter().zip(results) {
            for (h, ..) in &entries {
                self.pending.remove(h);
            }
            match result {
                Ok(size) => stored.push((id, size, entries)),
                Err(e) => {
                    failed.get_or_insert(e.context(format!("storing pack {id}")));
                }
            }
        }
        // a stored pack the index doesn't know is caught at the next start, which rebuilds the index
        self.index.add_packs(&stored).context("indexing stored packs")?;
        for (id, size, entries) in &stored {
            self.packs.insert(*id, (*size, entries.iter().map(|e| u64::from(e.2)).sum()));
        }
        failed.map_or(Ok(()), Err)
    }

    /// Drops packs that are about to be deleted, so nothing new refers to their chunks. Returns
    /// their stored sizes.
    fn drop_packs(&mut self, ids: &HashSet<u32>, chunks_of: &dyn Fn(u32) -> Result<Vec<Entry>>) -> Result<Vec<(u32, u64)>> {
        let mut gone = Vec::new();
        for &id in ids {
            gone.push((id, chunks_of(id)?));
        }
        self.index.remove_packs(&gone)?;
        Ok(gone.into_iter().filter_map(|(id, _)| self.packs.remove(&id).map(|(size, _)| (id, size))).collect())
    }
}

/// Manifests loaded from the bucket, least recently used dropped past a byte budget.
pub struct ManifestCache {
    inner: Mutex<(HashMap<String, (Arc<Manifest>, u64)>, usize, u64)>,
    cap: usize,
}

impl ManifestCache {
    pub fn new(cap: usize) -> Self {
        Self { inner: Mutex::new((HashMap::new(), 0, 0)), cap }
    }

    pub fn get(&self, key: &str) -> Option<Arc<Manifest>> {
        let mut g = self.inner.lock().unwrap();
        g.2 += 1;
        let tick = g.2;
        g.0.get_mut(key).map(|(m, used)| {
            *used = tick;
            m.clone()
        })
    }

    pub fn insert(&self, m: Arc<Manifest>) {
        let mut g = self.inner.lock().unwrap();
        let (map, total, tick) = &mut *g;
        *tick += 1;
        *total += m.bytes();
        if let Some((old, _)) = map.insert(m.key.clone(), (m, *tick)) {
            *total -= old.bytes();
        }
        while *total > self.cap && map.len() > 1 {
            let oldest = map.iter().min_by_key(|(_, (_, used))| *used).map(|(k, _)| k.clone()).expect("not empty");
            if let Some((gone, _)) = map.remove(&oldest) {
                *total -= gone.bytes();
            }
        }
    }

    pub fn remove(&self, key: &str) {
        let mut g = self.inner.lock().unwrap();
        if let Some((gone, _)) = g.0.remove(key) {
            g.1 -= gone.bytes();
        }
    }
}

/// Decompressed packs, most recently used kept.
pub struct PackCache {
    cells: Mutex<(HashMap<u32, Arc<OnceLock<Option<Arc<Vec<u8>>>>>>, VecDeque<u32>)>,
    cap: usize,
}

impl PackCache {
    pub fn new(cap: usize) -> Self {
        Self { cells: Mutex::new((HashMap::new(), VecDeque::new())), cap: cap.max(1) }
    }

    pub fn get(&self, id: u32, fetch: &dyn Fn(u32) -> Result<Vec<u8>>) -> Result<Arc<Vec<u8>>> {
        let cell = {
            let mut g = self.cells.lock().unwrap();
            let (map, order) = &mut *g;
            if let Some(i) = order.iter().position(|&x| x == id) {
                order.remove(i);
            }
            order.push_back(id);
            let cell = map.entry(id).or_default().clone();
            while order.len() > self.cap {
                if let Some(old) = order.pop_front() {
                    map.remove(&old);
                }
            }
            cell
        };
        let mut failure = None;
        let v = cell.get_or_init(|| fetch(id).map_err(|e| failure = Some(e)).ok().map(Arc::new));
        match v {
            Some(p) => Ok(p.clone()),
            None => {
                self.cells.lock().unwrap().0.remove(&id);
                Err(failure.unwrap_or_else(|| anyhow!("pack {id} could not be loaded")))
            }
        }
    }
}

/// Fetches a decompressed pack.
pub type PackFn<'a> = dyn Fn(u32) -> Result<Arc<Vec<u8>>> + Sync + 'a;

/// Rebuilds groups `g0..g1` on `workers` threads ahead of the consumer and hands them to `take` in
/// order; stops early when `take` returns false.
fn rebuild_groups(
    m: &Manifest,
    recipe: &IndependentRecipe,
    g0: usize,
    g1: usize,
    workers: usize,
    pack: &PackFn,
    take: &mut dyn FnMut(usize, RebuiltGroup) -> bool,
) -> Result<()> {
    let n = g1.saturating_sub(g0);
    let lookahead = workers.max(1) * 2;
    let slots: Vec<Mutex<Option<Result<RebuiltGroup>>>> = (0..n).map(|_| Mutex::new(None)).collect();
    let (next, emitted, stop, progress) = (AtomicUsize::new(0), Mutex::new(0usize), AtomicBool::new(false), Condvar::new());
    let rebuild = |g: usize| -> Result<RebuiltGroup> {
        let (base, _, gend) = recipe.plain_range(g);
        let (base, gend) = (base as u64, gend as u64);
        let first = m.ref_start.partition_point(|&s| s <= base).saturating_sub(1);
        let last = m.ref_start.partition_point(|&s| s < gend);
        // the group's packs load in parallel
        let mut ids: Vec<u32> = m.refs[first..last].iter().map(|r| r[0]).collect();
        ids.sort_unstable();
        ids.dedup();
        let loaded: Vec<Result<Arc<Vec<u8>>>> = thread::scope(|s| {
            let handles: Vec<_> = ids.iter().map(|&id| s.spawn(move || pack(id))).collect();
            handles.into_iter().map(|h| h.join().unwrap_or_else(|_| Err(anyhow!("pack loader panicked")))).collect()
        });
        let mut packs = HashMap::new();
        for (id, p) in ids.iter().zip(loaded) {
            packs.insert(*id, p?);
        }
        let mut window = Vec::with_capacity((gend - base) as usize);
        for i in first..last {
            let [p, at, len] = m.refs[i];
            let chunk = packs[&p].get(at as usize..(at + len) as usize).context("chunk outside its pack")?;
            let cs = m.ref_start[i];
            let (lo, hi) = (cs.max(base), (cs + u64::from(len)).min(gend));
            window.extend_from_slice(&chunk[(lo - cs) as usize..(hi - cs) as usize]);
        }
        recipe.recreate_group(g, &window).map_err(|e| anyhow!("{}: group {g}: {e:?}", m.key))
    };
    thread::scope(|s| {
        for _ in 0..workers.clamp(1, n.max(1)) {
            s.spawn(|| {
                loop {
                    let i = next.fetch_add(1, Ordering::Relaxed);
                    if i >= n || stop.load(Ordering::Relaxed) {
                        break;
                    }
                    {
                        let mut e = emitted.lock().unwrap();
                        while i >= *e + lookahead && !stop.load(Ordering::Relaxed) {
                            e = progress.wait(e).unwrap();
                        }
                    }
                    if stop.load(Ordering::Relaxed) {
                        break;
                    }
                    let part = rebuild(g0 + i);
                    *slots[i].lock().unwrap() = Some(part);
                    let _e = emitted.lock().unwrap();
                    progress.notify_all();
                }
            });
        }
        let mut outcome = Ok(());
        for i in 0..n {
            let part = {
                let mut e = emitted.lock().unwrap();
                loop {
                    if let Some(p) = slots[i].lock().unwrap().take() {
                        break p;
                    }
                    e = progress.wait(e).unwrap();
                }
            };
            match part {
                Ok(p) => {
                    if !take(g0 + i, p) {
                        break;
                    }
                }
                Err(e) => {
                    outcome = Err(e);
                    break;
                }
            }
            *emitted.lock().unwrap() = i + 1;
            progress.notify_all();
        }
        stop.store(true, Ordering::Relaxed);
        let _e = emitted.lock().unwrap();
        progress.notify_all();
        outcome
    })
}

/// Streams bytes `start..end` of a completed blob into `sink`, rebuilding groups on `workers`
/// threads ahead of the writer. Stops early when `sink` returns false.
pub fn stream(m: &Manifest, start: u64, end: u64, workers: usize, pack: &PackFn, sink: &mut dyn FnMut(&[u8]) -> bool) -> Result<()> {
    let head_len = m.head.len() as u64;
    let end_at = m.size - m.end.len() as u64;
    let clip = |at: u64, len: usize| {
        let (lo, hi) = (start.max(at), end.min(at + len as u64));
        (lo < hi).then(|| (lo - at) as usize..(hi - at) as usize)
    };
    if let Some(r) = clip(0, m.head.len())
        && !sink(&m.head[r])
    {
        return Ok(());
    }
    if start < end_at && end > head_len && !m.groups.is_empty() {
        let recipe = m.recipe()?;
        ensure!(recipe.groups() == m.groups.len(), "{}: recipe and manifest disagree", m.key);
        let g0 = m.groups.partition_point(|&(at, _)| at <= start.max(head_len)).saturating_sub(1);
        let g1 = m.groups.partition_point(|&(at, _)| at < end.min(end_at));
        let mut splicer = Splicer::resume(m.groups[g0].1);
        let mut stopped = false;
        rebuild_groups(m, recipe, g0, g1, workers, pack, &mut |g, part| {
            let bytes = splicer.push(part);
            if let Some(r) = clip(m.groups[g].0, bytes.len())
                && !sink(&bytes[r])
            {
                stopped = true;
                return false;
            }
            true
        })?;
        if stopped {
            return Ok(());
        }
    }
    if let Some(r) = clip(end_at, m.end.len()) {
        sink(&m.end[r]);
    }
    Ok(())
}

/// What's kept in memory per compacted blob; its manifest is loaded when needed.
#[derive(Clone, Copy)]
pub struct BlobInfo {
    pub size: u64,
    pub mtime: u64,
}

/// The compacted blobs of one bucket.
pub struct Store {
    pub backend: Arc<Backend>,
    cache: PackCache,
    /// every compacted blob; listing the manifests' names rebuilds this
    compacted: RwLock<HashMap<String, BlobInfo>>,
    manifests: ManifestCache,
    packer: Mutex<Packer>,
    /// held while a manifest is added, replaced or removed
    mutations: Mutex<()>,
    /// packs nothing refers to any more: (id, when, stored size); deleted after a grace period
    retired: Mutex<Vec<(u32, u64, u64)>>,
    /// blobs forgotten since the last GC
    forgotten: AtomicUsize,
    /// compacted blobs whose original bytes are back in the bucket after a read, since when
    hot: Mutex<HashMap<String, u64>>,
    /// blobs being copied back right now
    copying: Mutex<HashSet<String>>,
}

#[derive(Default)]
pub struct GcReport {
    pub deleted: usize,
    pub deleted_bytes: u64,
    pub retired: usize,
    pub rewritten: usize,
    pub moved: usize,
}

fn pack_key(id: u32) -> String {
    format!("{PACKS}{id:08}.zst")
}

fn idx_key(id: u32) -> String {
    format!("{PACKS}{id:08}.idx")
}

/// A manifest's name carries its blob's size and date, so a listing is enough to start.
fn manifest_key(key: &str, info: BlobInfo) -> String {
    format!("{MANIFESTS}{key}@{}@{}", info.size, info.mtime)
}

fn parse_manifest_key(name: &str) -> Option<(String, BlobInfo)> {
    let (rest, mtime) = name.strip_prefix(MANIFESTS)?.rsplit_once('@')?;
    let (key, size) = rest.rsplit_once('@')?;
    Some((key.to_string(), BlobInfo { size: size.parse().ok()?, mtime: mtime.parse().ok()? }))
}

impl Store {
    /// Opens the chunk index in `state_dir` (rebuilt from the bucket's .idx files when it doesn't
    /// match the packs there) and loads the catalog of compacted blobs.
    pub fn open(
        backend: Arc<Backend>,
        state_dir: &std::path::Path,
        level: i32,
        cache_packs: usize,
        threads: usize,
        manifest_cache: usize,
    ) -> Result<Self> {
        let listing = backend.list(PACKS)?;
        let name = |key: &str| -> Option<(u32, String)> {
            let (id, ext) = key.strip_prefix(PACKS)?.split_once('.')?;
            Some((id.parse().ok()?, ext.to_string()))
        };
        let (mut zst, mut idx, mut next) = (HashMap::new(), HashSet::new(), 0u32);
        for o in &listing {
            if let Some((id, ext)) = name(&o.key) {
                next = next.max(id + 1);
                match ext.as_str() {
                    "zst" => {
                        zst.insert(id, o.size);
                    }
                    "idx" => {
                        idx.insert(id);
                    }
                    _ => {}
                }
            }
        }
        // half a pack is one whose store failed: nothing can refer to it
        for id in zst.keys().filter(|id| !idx.contains(id)).copied().collect::<Vec<_>>() {
            backend.delete(&pack_key(id))?;
            zst.remove(&id);
        }
        for id in idx.iter().filter(|id| !zst.contains_key(id)) {
            backend.delete(&idx_key(*id))?;
        }
        let index = Arc::new(Index::open(&state_dir.join("index.redb"))?);
        let mut packs = index.packs()?;
        let in_step = packs.len() == zst.len() && zst.iter().all(|(id, size)| packs.get(id).is_some_and(|p| p.0 == *size));
        if !in_step {
            eprintln!("rift: rebuilding the chunk index from {} packs", zst.len());
            let mut all = Vec::with_capacity(zst.len());
            for (&id, &size) in &zst {
                all.push((id, size, Self::read_idx(&backend, id)?));
            }
            packs = all.iter().map(|(id, size, e)| (*id, (*size, e.iter().map(|x| u64::from(x.2)).sum()))).collect();
            index.rebuild(all)?;
        }
        let mut compacted = HashMap::new();
        for o in backend.list(MANIFESTS)? {
            if let Some((key, info)) = parse_manifest_key(&o.key) {
                compacted.insert(key, info);
                continue;
            }
            // named before sizes went into the name: read it once and rename it
            let m = Manifest::decode(&backend.get(&o.key)?.0)?;
            let info = BlobInfo { size: m.size, mtime: m.mtime };
            backend.put(&manifest_key(&m.key, info), m.encode(), &[])?;
            backend.delete(&o.key)?;
            compacted.insert(m.key, info);
        }
        Ok(Self {
            backend,
            cache: PackCache::new(cache_packs),
            compacted: RwLock::new(compacted),
            manifests: ManifestCache::new(manifest_cache),
            packer: Mutex::new(Packer::new(index, packs, next, level, threads)),
            mutations: Mutex::new(()),
            retired: Mutex::new(Vec::new()),
            forgotten: AtomicUsize::new(0),
            hot: Mutex::new(HashMap::new()),
            copying: Mutex::new(HashSet::new()),
        })
    }

    fn read_idx(backend: &Backend, id: u32) -> Result<Vec<Entry>> {
        Ok(backend
            .get(&idx_key(id))?
            .0
            .chunks_exact(40)
            .map(|e| {
                let v = |i: usize| u32::from_le_bytes(e[i..i + 4].try_into().expect("4 bytes"));
                (e[..32].try_into().expect("32 bytes"), v(32), v(36))
            })
            .collect())
    }

    pub fn len(&self) -> usize {
        self.compacted.read().unwrap().len()
    }

    pub fn info(&self, key: &str) -> Option<BlobInfo> {
        self.compacted.read().unwrap().get(key).copied()
    }

    /// The manifest of a compacted blob, from the cache or the bucket.
    pub fn manifest(&self, key: &str) -> Result<Option<Arc<Manifest>>> {
        let Some(info) = self.info(key) else { return Ok(None) };
        if let Some(m) = self.manifests.get(key) {
            return Ok(Some(m));
        }
        let m = Arc::new(Manifest::decode(&self.backend.get(&manifest_key(key, info))?.0)?);
        self.manifests.insert(m.clone());
        Ok(Some(m))
    }

    /// A compacted blob's chunk refs, reading only that part of its manifest.
    fn refs(&self, key: &str, info: BlobInfo) -> Result<Vec<Ref>> {
        if let Some(m) = self.manifests.get(key) {
            return Ok(m.refs.clone());
        }
        let name = manifest_key(key, info);
        let mut b = self.backend.get_range(&name, 0, 64 << 10)?;
        let (_, from, to) = Manifest::header(&b)?;
        if b.len() < to {
            b = self.backend.get_range(&name, 0, to as u64)?;
        }
        Manifest::refs_from(&b, from, to)
    }

    /// The key was deleted or overwritten: its manifest goes; GC reclaims its chunks.
    pub fn forget(&self, key: &str) {
        let _m = self.mutations.lock().unwrap();
        self.hot.lock().unwrap().remove(key);
        self.manifests.remove(key);
        if let Some(info) = self.compacted.write().unwrap().remove(key) {
            self.forgotten.fetch_add(1, Ordering::Relaxed);
            if let Err(e) = self.backend.delete(&manifest_key(key, info)) {
                eprintln!("rift: could not delete the manifest of {key}: {e:#}");
            }
        }
    }

    /// Points a still-compacted blob at moved chunks.
    fn replace_manifest(&self, m: Manifest) -> Result<()> {
        let _m = self.mutations.lock().unwrap();
        let Some(info) = self.info(&m.key) else { return Ok(()) }; // deleted meanwhile
        self.backend.put(&manifest_key(&m.key, info), m.encode(), &[])?;
        self.manifests.insert(Arc::new(m));
        Ok(())
    }

    /// The original bytes of this compacted blob are in the bucket; serve them as they are.
    pub fn is_hot(&self, key: &str) -> bool {
        self.hot.lock().unwrap().contains_key(key)
    }

    /// A listing showed the original bytes at `key`.
    pub fn note_hot(&self, key: &str, since: u64) {
        self.hot.lock().unwrap().entry(key.to_string()).or_insert(since);
    }

    /// Claims the copy-back of a blob that was just read cold; false if it's hot or being copied.
    pub fn begin_hot_copy(&self, key: &str) -> bool {
        !self.is_hot(key) && self.copying.lock().unwrap().insert(key.to_string())
    }

    /// Puts the rebuilt original, already checked against its digest, back over the stub. Only
    /// over the stub: if the blob was deleted or replaced meanwhile, nothing is written.
    pub fn finish_hot_copy(&self, key: &str, file: &std::path::Path, size: u64) -> Result<()> {
        let done = (|| -> Result<()> {
            match self.backend.head(key)? {
                Some((0, etag)) if self.info(key).is_some() => {
                    self.backend.put_file(key, std::fs::File::open(file)?, size, &[("if-match".into(), etag)])?;
                    self.hot.lock().unwrap().insert(key.to_string(), unix_now());
                    crate::metrics::add(&crate::metrics::METRICS.hot_copies_made, 1);
                }
                Some((n, _)) if n == size => self.note_hot(key, unix_now()),
                _ => {}
            }
            Ok(())
        })();
        self.copying.lock().unwrap().remove(key);
        done
    }

    pub fn abandon_hot_copy(&self, key: &str) {
        self.copying.lock().unwrap().remove(key);
    }

    /// Swaps the stub back in over a hot copy nobody has read lately.
    pub fn cool(&self, key: &str, etag: &str) -> Result<()> {
        let Some(info) = self.info(key) else { return Ok(()) };
        self.backend.put(key, Vec::new(), &[("x-amz-meta-rift-size".into(), info.size.to_string()), ("if-match".into(), etag.to_string())])?;
        self.hot.lock().unwrap().remove(key);
        Ok(())
    }

    /// For leaving rift: puts every compacted blob's original bytes back (rebuilt and checked against
    /// its digest first) and drops its manifest; deletes the packs only if every blob made it.
    /// Returns (restored, failed, bytes restored). Run it with no gateway serving the bucket.
    pub fn eject(&self, threads: usize) -> Result<(usize, usize, u64)> {
        let mut blobs: Vec<(String, BlobInfo)> = self.compacted.read().unwrap().iter().map(|(k, i)| (k.clone(), *i)).collect();
        blobs.sort_by(|a, b| a.0.cmp(&b.0));
        let (mut restored, mut failed, mut bytes) = (0, 0, 0u64);
        for (key, info) in blobs {
            let tmp = std::env::temp_dir().join(format!("rift-eject-{}", std::process::id()));
            let attempt = (|| -> Result<()> {
                let m = Manifest::decode(&self.backend.get(&manifest_key(&key, info))?.0)?;
                let mut file = std::io::BufWriter::new(std::fs::File::create(&tmp)?);
                let (mut sha, mut n, mut io) = (Sha256::new(), 0u64, Ok(()));
                self.stream(&m, 0, m.size, threads, &mut |b| {
                    sha.update(b);
                    n += b.len() as u64;
                    io = std::io::Write::write_all(&mut file, b);
                    io.is_ok()
                })?;
                io?;
                std::io::Write::flush(&mut file)?;
                drop(file);
                ensure!(n == m.size && hex(&sha.finalize()) == m.sha256, "rebuild didn't match its digest");
                match self.backend.head(&key)? {
                    Some((0, etag)) => {
                        self.backend.put_file(&key, std::fs::File::open(&tmp)?, m.size, &[("if-match".into(), etag)])?;
                    }
                    Some((size, _)) if size == m.size => {} // the original is already there
                    _ => {}                                 // deleted: nothing to put back
                }
                self.backend.delete(&manifest_key(&key, info))
            })();
            let _ = std::fs::remove_file(&tmp);
            match attempt {
                Ok(()) => {
                    restored += 1;
                    bytes += info.size;
                    eprintln!("rift: put back {key} ({:.1} MB)", info.size as f64 / 1e6);
                }
                Err(e) => {
                    failed += 1;
                    eprintln!("rift: {key}: {e:#}");
                }
            }
        }
        if failed == 0 {
            for o in self.backend.list(PACKS)? {
                self.backend.delete(&o.key)?;
            }
        }
        Ok((restored, failed, bytes))
    }

    /// Publishes counts and sizes for /-/metrics. Call it between compactions (it takes the packer).
    pub fn publish_totals(&self) {
        use crate::metrics::{METRICS, set};
        let (blobs, bytes) = {
            let c = self.compacted.read().unwrap();
            (c.len() as u64, c.values().map(|i| i.size).sum::<u64>())
        };
        set(&METRICS.compacted_blobs, blobs);
        set(&METRICS.compacted_logical_bytes, bytes);
        set(&METRICS.hot_copies, self.hot.lock().unwrap().len() as u64);
        let packer = self.packer.lock().unwrap();
        set(&METRICS.packs, packer.packs.len() as u64);
        set(&METRICS.pack_bytes, packer.packs.values().map(|p| p.0).sum());
        set(&METRICS.retired_pack_bytes, self.retired.lock().unwrap().iter().map(|r| r.2).sum());
    }

    pub fn gc_due(&self) -> bool {
        self.forgotten.load(Ordering::Relaxed) > 0 || !self.retired.lock().unwrap().is_empty()
    }

    /// Reclaims chunks no manifest uses. A pack with nothing live is retired; a pack with less than
    /// `rewrite_below` of its bytes live has those chunks copied to new packs and its blobs pointed
    /// there, then is retired. Retired packs are deleted `grace` seconds later, so reads that
    /// started before still find them. Run it between compactions, never during one.
    pub fn gc(&self, rewrite_below: f64, grace: u64) -> Result<GcReport> {
        let mut report = GcReport::default();
        let now = unix_now();
        let mut packer = self.packer.lock().unwrap();
        {
            let mut retired = self.retired.lock().unwrap();
            let (due, waiting): (Vec<_>, Vec<_>) = retired.drain(..).partition(|r| r.1 + grace <= now);
            *retired = waiting;
            for (id, when, size) in due {
                match self.backend.delete(&pack_key(id)).and_then(|_| self.backend.delete(&idx_key(id))) {
                    Ok(()) => {
                        report.deleted += 1;
                        report.deleted_bytes += size;
                    }
                    Err(e) => {
                        eprintln!("rift: gc: deleting pack {id}: {e:#}");
                        retired.push((id, when, size));
                    }
                }
            }
        }
        self.forgotten.store(0, Ordering::Relaxed);
        let store = |i: u32, z: Vec<u8>, x: Vec<u8>| self.store_pack(i, z, x);
        let blobs: Vec<(String, BlobInfo)> = self.compacted.read().unwrap().iter().map(|(k, i)| (k.clone(), *i)).collect();
        // pass 1: bytes each pack serves (a chunk several blobs use counts several times, which only
        // ever overstates what's live) and the packs each blob uses
        let mut served: HashMap<u32, u64> = HashMap::new();
        let mut uses: Vec<(String, Vec<u32>)> = Vec::with_capacity(blobs.len());
        for (key, info) in &blobs {
            // a chunk list we can't read could hide live chunks: skip this round rather than guess
            let refs = self.refs(key, *info).with_context(|| format!("reading the chunk list of {key}"))?;
            for r in &refs {
                *served.entry(r[0]).or_default() += u64::from(r[2]);
            }
            let mut packs: Vec<u32> = refs.iter().map(|r| r[0]).collect();
            packs.sort_unstable();
            packs.dedup();
            uses.push((key.clone(), packs));
        }
        let (mut dead, mut sparse) = (HashSet::new(), HashSet::new());
        for (&id, &(_, total)) in &packer.packs {
            match served.get(&id) {
                None => {
                    dead.insert(id);
                }
                Some(&bytes) if (bytes as f64) < rewrite_below * total as f64 => {
                    sparse.insert(id);
                }
                Some(_) => {}
            }
        }
        // pass 2, only for blobs using a sparse pack: exactly which of its chunks are live
        let mut live: HashMap<u32, HashSet<u32>> = HashMap::new();
        for (key, packs) in &uses {
            if !packs.iter().any(|p| sparse.contains(p)) {
                continue;
            }
            let Some(info) = self.info(key) else { continue }; // forgotten meanwhile
            let refs = self.refs(key, info).with_context(|| format!("reading the chunk list of {key}"))?;
            for r in refs.iter().filter(|r| sparse.contains(&r[0])) {
                live.entry(r[0]).or_default().insert(r[1]);
            }
        }
        // copy the live chunks of sparse packs; dedup mustn't point into them meanwhile
        let mut moved: HashMap<(u32, u32), Ref> = HashMap::new();
        packer.exclude = sparse.clone();
        let copied = (|| -> Result<()> {
            for &id in &sparse {
                let entries = Self::read_idx(&self.backend, id)?;
                let data = self.fetch_pack(id)?;
                let used = live.get(&id);
                for &(h, at, len) in entries.iter().filter(|e| used.is_some_and(|u| u.contains(&e.1))) {
                    let bytes = data.get(at as usize..(at + len) as usize).context("chunk outside its pack")?;
                    moved.insert((id, at), packer.add_chunk(h, bytes, &store)?);
                }
            }
            packer.flush(&store)
        })();
        packer.exclude.clear();
        // on failure the old packs are still what every manifest uses; copies made so far just serve dedup
        copied.context("copying live chunks out of sparse packs")?;
        let mut stuck: HashSet<u32> = HashSet::new();
        for (key, packs) in &uses {
            if !packs.iter().any(|p| sparse.contains(p)) {
                continue;
            }
            let repointed = (|| -> Result<()> {
                let Some(m) = self.manifest(key)? else { return Ok(()) }; // deleted meanwhile
                let refs = m.refs.iter().map(|r| moved.get(&(r[0], r[1])).copied().unwrap_or(*r)).collect();
                self.replace_manifest(m.with_refs(refs))
            })();
            if let Err(e) = repointed {
                eprintln!("rift: gc: pointing {key} at its moved chunks: {e:#}");
                stuck.extend(packs.iter().filter(|p| sparse.contains(p)));
            }
        }
        let retire: HashSet<u32> = dead.iter().chain(sparse.difference(&stuck)).copied().collect();
        let sizes = packer.drop_packs(&retire, &|id| Self::read_idx(&self.backend, id))?;
        self.retired.lock().unwrap().extend(sizes.into_iter().map(|(id, size)| (id, now, size)));
        report.retired = dead.len();
        report.rewritten = sparse.len() - stuck.len();
        report.moved = moved.len();
        Ok(report)
    }

    fn fetch_pack(&self, id: u32) -> Result<Vec<u8>> {
        let (z, _) = self.backend.get(&pack_key(id))?;
        Ok(zstd::bulk::decompress(&z, PACK_BYTES * 2)?)
    }

    pub fn stream(&self, m: &Manifest, start: u64, end: u64, workers: usize, sink: &mut dyn FnMut(&[u8]) -> bool) -> Result<()> {
        stream(m, start, end, workers, &|id| self.cache.get(id, &|id| self.fetch_pack(id)), sink)
    }

    fn store_pack(&self, id: u32, pack: Vec<u8>, idx: Vec<u8>) -> Result<()> {
        self.backend.put(&pack_key(id), pack, &[])?;
        self.backend.put(&idx_key(id), idx, &[])?;
        Ok(())
    }

    /// Compacts the given blobs (key, sha256, last-modified); returns how many were compacted and
    /// the bytes they took before and after (packs counted as they grow).
    pub fn compact(&self, blobs: &[(String, String, u64)], threads: usize) -> (usize, u64) {
        let mut ready = Vec::new();
        for (key, digest, mtime) in blobs {
            let attempt = (|| -> Result<Option<(Manifest, String)>> {
                let (data, etag) = self.backend.get(key)?;
                ensure!(sha256_hex(&data) == *digest, "content doesn't match its digest");
                let Some((mut m, plain)) = analyze(key, digest, *mtime, &data, threads)? else { return Ok(None) };
                let refs = self.packer.lock().unwrap().add(&plain, &|id, z, idx| self.store_pack(id, z, idx))?;
                m.set_refs(refs);
                Ok(Some((m, etag)))
            })();
            match attempt {
                Ok(Some(r)) => ready.push(r),
                Ok(None) => eprintln!("rift: {key}: not rebuildable byte-exact, kept as is"),
                Err(e) => eprintln!("rift: {key}: {e:#}"),
            }
        }
        if let Err(e) = self.packer.lock().unwrap().flush(&|id, z, idx| self.store_pack(id, z, idx)) {
            eprintln!("rift: storing a pack failed, its blobs stay as they were: {e:#}");
        }
        let (mut done, mut bytes) = (0, 0);
        for (m, etag) in ready {
            let (key, size) = (m.key.clone(), m.size);
            match self.commit(m, &etag, threads) {
                Ok(true) => {
                    done += 1;
                    bytes += size;
                }
                Ok(false) => eprintln!("rift: {key}: stored form didn't rebuild exactly, kept as is"),
                Err(e) => eprintln!("rift: {key}: {e:#}"),
            }
        }
        (done, bytes)
    }

    /// Rebuilds the blob from what's stored (cold cache) and checks it against its digest, writes the
    /// manifest, and only then swaps the original for a stub.
    fn commit(&self, mut m: Manifest, etag: &str, threads: usize) -> Result<bool> {
        let key = m.key.clone();
        let fresh = PackCache::new(32);
        if !complete(&mut m, threads, &|id| fresh.get(id, &|id| self.fetch_pack(id))).context("rebuild from the store")? {
            return Ok(false);
        }
        let info = BlobInfo { size: m.size, mtime: m.mtime };
        self.backend.put(&manifest_key(&key, info), m.encode(), &[])?;
        let size = m.size;
        {
            let _m = self.mutations.lock().unwrap();
            self.compacted.write().unwrap().insert(key.clone(), info);
            self.manifests.insert(Arc::new(m));
        }
        // the stub keeps the key in listings; If-Match so a blob deleted meanwhile isn't brought back
        let stub = self.backend.put(
            &key,
            Vec::new(),
            &[("x-amz-meta-rift-size".into(), size.to_string()), ("if-match".into(), etag.to_string())],
        );
        if let Err(e) = stub {
            self.forget(&key);
            return Err(e.context("replacing the original with a stub"));
        }
        Ok(true)
    }
}

/// The sha256 of a registry blob from its data key (".../blobs/sha256/ab/abcd…/data").
pub fn blob_digest(key: &str) -> Option<String> {
    let rest = key.strip_suffix("/data")?;
    let (dir, digest) = rest.rsplit_once('/')?;
    let (dir, two) = dir.rsplit_once('/')?;
    (dir.ends_with("blobs/sha256")
        && digest.len() == 64
        && digest.starts_with(two)
        && two.len() == 2
        && digest.bytes().all(|b| b.is_ascii_hexdigit()))
    .then(|| digest.to_string())
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use super::*;

    fn gzip(data: &[u8]) -> Vec<u8> {
        let mut e = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::new(6));
        e.write_all(data).unwrap();
        e.finish().unwrap()
    }

    // Compaction into packs, then every range served by the streaming rebuild matches the original.
    #[test]
    fn compact_and_stream_ranges() {
        let mut text = Vec::new();
        for i in 0..400_000u32 {
            text.extend(format!("line {i} value {} {}\n", i.wrapping_mul(2_654_435_761) % 1000, i % 7).as_bytes());
        }
        let blob = gzip(&text);
        let digest = sha256_hex(&blob);
        let (mut m, plain) = analyze("k", &digest, 0, &blob, 4).unwrap().expect("preflate takes it");
        let packs: Mutex<HashMap<u32, Arc<Vec<u8>>>> = Mutex::new(HashMap::new());
        let dir = std::env::temp_dir().join(format!("rift-cas-test-{}", std::process::id()));
        let index = Arc::new(Index::open(&dir.join("index.redb")).unwrap());
        let mut packer = Packer::new(index, HashMap::new(), 0, 3, 2);
        let store = |id: u32, z: Vec<u8>, _idx: Vec<u8>| {
            packs.lock().unwrap().insert(id, Arc::new(zstd::bulk::decompress(&z, 64 << 20)?));
            Ok(())
        };
        let refs = packer.add(&plain, &store).unwrap();
        packer.flush(&store).unwrap();
        m.set_refs(refs);
        let pack = |id: u32| packs.lock().unwrap().get(&id).cloned().context("missing pack");
        assert!(complete(&mut m, 3, &pack).unwrap(), "rebuilds byte-exact");
        assert!(m.groups.len() > 1, "test needs several groups");
        let m = Manifest::decode(&m.encode()).unwrap();
        let size = blob.len() as u64;
        let cuts = [0, 1, 9, 10, 4096, size / 3, size / 2, m.groups[1].0, m.groups[1].0 + 1, size - 9, size - 1];
        for &a in &cuts {
            for &b in &[a + 1, a + 100_000, size] {
                let b = b.min(size);
                if a >= b {
                    continue;
                }
                let mut out = Vec::new();
                stream(&m, a, b, 3, &pack, &mut |x| {
                    out.extend_from_slice(x);
                    true
                })
                .unwrap();
                assert!(out[..] == blob[a as usize..b as usize], "range {a}..{b}");
            }
        }
    }

    #[test]
    fn manifest_names() {
        let info = BlobInfo { size: 1234, mtime: 99 };
        let name = manifest_key("docker/registry/v2/blobs/sha256/ab/abc/data", info);
        let (key, back) = parse_manifest_key(&name).unwrap();
        assert_eq!((key.as_str(), back.size, back.mtime), ("docker/registry/v2/blobs/sha256/ab/abc/data", 1234, 99));
        assert!(parse_manifest_key(".rift/m/docker/registry/v2/blobs/sha256/ab/abc/data").is_none());
    }

    #[test]
    fn manifest_cache_keeps_recent() {
        let m = |k: &str| {
            Arc::new(Manifest::new(k.into(), 0, "0".repeat(64), 0, vec![], vec![], vec![], vec![], vec![0; 1000]))
        };
        let cache = ManifestCache::new(3000);
        cache.insert(m("a"));
        cache.insert(m("b"));
        assert!(cache.get("a").is_some()); // a is now the most recent
        cache.insert(m("c")); // over budget: b goes
        assert!(cache.get("a").is_some() && cache.get("c").is_some() && cache.get("b").is_none());
    }

    #[test]
    fn digests_from_keys() {
        let d = "ab".to_string() + &"0".repeat(62);
        assert_eq!(blob_digest(&format!("docker/registry/v2/blobs/sha256/ab/{d}/data")), Some(d.clone()));
        assert_eq!(blob_digest(&format!("docker/registry/v2/blobs/sha256/cd/{d}/data")), None);
        assert_eq!(blob_digest("docker/registry/v2/repositories/x/_layers/sha256/ab/link"), None);
    }
}
