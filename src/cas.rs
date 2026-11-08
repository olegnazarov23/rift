//! Compacted blobs: a gzip blob becomes a preflate recipe plus content-defined chunks stored once
//! in shared packs, and is served by rebuilding restart-point groups as a stream.
//!
//! Layout in the backing bucket:
//!   .rift/packs/NNNNNNNN.zst   unique chunks, zstd with long-distance matching
//!   .rift/packs/NNNNNNNN.idx   (blake3, offset, length) of every chunk in the pack
//!   .rift/m/<key>              manifest of a compacted blob; the original key keeps an empty stub

use std::{
    collections::{HashMap, VecDeque},
    sync::{
        Arc, Condvar, Mutex, OnceLock, RwLock,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    thread,
};

use anyhow::{Context, Result, anyhow, bail, ensure};
use fastcdc::v2020::FastCDC;
use preflate_rs::{IndependentRecipe, PreflateConfig, RebuiltGroup, Splicer, preflate_independent};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use zstd::stream::raw::CParameter;

use crate::{
    s3::Backend,
    sigv4::{hex, sha256_hex},
    store::gzip_payload_offset,
};

/// plain bytes per restart-point group
pub const GROUP_PLAIN: usize = 8 << 20;
const PACK_BYTES: usize = 16 << 20;
const PACKS: &str = ".rift/packs/";
const MANIFESTS: &str = ".rift/m/";
const MAGIC: &[u8; 8] = b"riftman1";

/// pack, offset, length
pub type Ref = [u32; 3];

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

    pub fn decode(b: &[u8]) -> Result<Self> {
        ensure!(b.len() > 12 && &b[..8] == MAGIC, "not a rift manifest");
        let hlen = u32::from_le_bytes(b[8..12].try_into()?) as usize;
        let h: Value = serde_json::from_slice(b.get(12..12 + hlen).context("short manifest")?)?;
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
        let nrefs = h["refs"].as_u64().context("refs")? as usize;
        let refs_at = 12 + hlen;
        let refs_bytes = b.get(refs_at..refs_at + nrefs * 12).context("short manifest refs")?;
        let refs = refs_bytes
            .chunks_exact(12)
            .map(|c| {
                let v = |i: usize| u32::from_le_bytes(c[i..i + 4].try_into().expect("4 bytes"));
                [v(0), v(4), v(8)]
            })
            .collect();
        Ok(Self::new(
            h["key"].as_str().context("key")?.to_string(),
            h["size"].as_u64().context("size")?,
            h["sha256"].as_str().context("sha256")?.to_string(),
            h["mtime"].as_u64().unwrap_or(0),
            unhex(&h["head"])?,
            unhex(&h["end"])?,
            groups,
            refs,
            b[refs_at + nrefs * 12..].to_vec(),
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

/// Unpacks a single-member gzip blob and rebuilds it from the recipe, checking the result hashes to
/// `digest`. Returns the manifest (no chunk refs yet) and the plain text, or None when the blob
/// isn't gzip or can't be rebuilt exactly.
pub fn analyze(key: &str, digest: &str, mtime: u64, data: &[u8], threads: usize) -> Result<Option<(Manifest, Vec<u8>)>> {
    let Some(start) = gzip_payload_offset(data) else { return Ok(None) };
    let config = PreflateConfig { max_chain_length: 4096, plain_text_limit: 4 << 30, verify_compression: false };
    let Ok((raw, deflate_len, plain)) = preflate_independent(&data[start..], &config, GROUP_PLAIN, threads) else {
        return Ok(None);
    };
    if start + deflate_len + 8 != data.len() {
        return Ok(None);
    }
    let recipe = IndependentRecipe::parse(&raw).map_err(|e| anyhow!("{e:?}"))?;
    let parts = parallel(recipe.groups(), threads, |g| {
        let (base, _, end) = recipe.plain_range(g);
        recipe.recreate_group(g, &plain[base..end])
    });
    let mut sha = Sha256::new();
    sha.update(&data[..start]);
    let (mut splicer, mut at, mut groups) = (Splicer::default(), start as u64, Vec::new());
    for part in parts {
        let Ok(part) = part else { return Ok(None) };
        groups.push((at, splicer.carry()));
        let bytes = splicer.push(part);
        sha.update(&bytes);
        at += bytes.len() as u64;
    }
    let mut end: Vec<u8> = splicer.finish().into_iter().collect();
    end.extend_from_slice(&data[start + deflate_len..]);
    sha.update(&end);
    if at + end.len() as u64 != data.len() as u64 || hex(&sha.finalize()) != digest {
        return Ok(None);
    }
    let recipe = zstd::bulk::compress(&raw, 19)?;
    let m = Manifest::new(key.into(), data.len() as u64, digest.into(), mtime, data[..start].to_vec(), end, groups, Vec::new(), recipe);
    Ok(Some((m, plain)))
}

/// Puts unique chunks into packs. A full pack is handed to `store(id, zstd bytes, index bytes)`.
pub struct Packer {
    index: HashMap<[u8; 32], Ref>,
    id: u32,
    data: Vec<u8>,
    entries: Vec<([u8; 32], u32, u32)>,
    level: i32,
}

pub type PackSink<'a> = dyn FnMut(u32, Vec<u8>, Vec<u8>) -> Result<()> + 'a;

impl Packer {
    pub fn new(index: HashMap<[u8; 32], Ref>, next_id: u32, level: i32) -> Self {
        Self { index, id: next_id, data: Vec::with_capacity(PACK_BYTES), entries: Vec::new(), level }
    }

    /// Cuts `plain` into chunks and returns their refs; new chunks go into the open pack.
    pub fn add(&mut self, plain: &[u8], store: &mut PackSink) -> Result<Vec<Ref>> {
        let mut refs = Vec::new();
        for c in FastCDC::new(plain, 16 << 10, 64 << 10, 256 << 10) {
            let bytes = &plain[c.offset..c.offset + c.length];
            let hash = *blake3::hash(bytes).as_bytes();
            if let Some(r) = self.index.get(&hash) {
                refs.push(*r);
                continue;
            }
            if self.data.len() + bytes.len() > PACK_BYTES {
                self.seal(store)?;
            }
            let r = [self.id, self.data.len() as u32, bytes.len() as u32];
            self.data.extend_from_slice(bytes);
            self.entries.push((hash, r[1], r[2]));
            self.index.insert(hash, r);
            refs.push(r);
        }
        Ok(refs)
    }

    /// Stores the open pack, if it holds anything. On failure its chunks are forgotten, so nothing
    /// later refers to them.
    pub fn seal(&mut self, store: &mut PackSink) -> Result<()> {
        if self.data.is_empty() {
            return Ok(());
        }
        let stored = (|| {
            let mut z = zstd::bulk::Compressor::new(self.level)?;
            z.set_parameter(CParameter::EnableLongDistanceMatching(true))?;
            z.set_parameter(CParameter::WindowLog(24))?;
            let mut idx = Vec::with_capacity(self.entries.len() * 40);
            for (h, at, len) in &self.entries {
                idx.extend(h);
                idx.extend(at.to_le_bytes());
                idx.extend(len.to_le_bytes());
            }
            store(self.id, z.compress(&self.data)?, idx)
        })();
        if stored.is_err() {
            for (h, ..) in &self.entries {
                self.index.remove(h);
            }
        } else {
            self.id += 1;
        }
        self.data.clear();
        self.entries.clear();
        stored
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

/// Streams bytes `start..end` of the blob into `sink`, rebuilding groups on `workers` threads ahead
/// of the writer. Stops early when `sink` returns false.
pub fn stream(
    m: &Manifest,
    start: u64,
    end: u64,
    workers: usize,
    pack: &(dyn Fn(u32) -> Result<Arc<Vec<u8>>> + Sync),
    sink: &mut dyn FnMut(&[u8]) -> bool,
) -> Result<()> {
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
        let result = thread::scope(|s| {
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
            let mut splicer = Splicer::resume(m.groups[g0].1);
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
                let bytes = match part {
                    Ok(p) => splicer.push(p),
                    Err(e) => {
                        outcome = Err(e);
                        break;
                    }
                };
                if let Some(r) = clip(m.groups[g0 + i].0, bytes.len())
                    && !sink(&bytes[r])
                {
                    outcome = Err(anyhow!("stopped"));
                    break;
                }
                *emitted.lock().unwrap() = i + 1;
                progress.notify_all();
            }
            stop.store(true, Ordering::Relaxed);
            let _e = emitted.lock().unwrap();
            progress.notify_all();
            outcome
        });
        match result {
            Err(e) if e.to_string() == "stopped" => return Ok(()),
            other => other?,
        }
    }
    if let Some(r) = clip(end_at, m.end.len()) {
        sink(&m.end[r]);
    }
    Ok(())
}

/// The compacted blobs of one bucket.
pub struct Store {
    pub backend: Arc<Backend>,
    cache: PackCache,
    compacted: RwLock<HashMap<String, Arc<Manifest>>>,
    packer: Mutex<Packer>,
}

fn pack_key(id: u32) -> String {
    format!("{PACKS}{id:08}.zst")
}

fn manifest_key(key: &str) -> String {
    format!("{MANIFESTS}{key}")
}

impl Store {
    /// Loads the chunk index and the manifests already in the bucket.
    pub fn open(backend: Arc<Backend>, level: i32, cache_packs: usize) -> Result<Self> {
        let (mut index, mut next) = (HashMap::new(), 0u32);
        for o in backend.list(PACKS)? {
            let Some((id, ext)) = o.key.strip_prefix(PACKS).and_then(|n| n.split_once('.')) else { continue };
            let Ok(id) = id.parse::<u32>() else { continue };
            next = next.max(id + 1);
            if ext == "idx" {
                for e in backend.get(&o.key)?.0.chunks_exact(40) {
                    let v = |i: usize| u32::from_le_bytes(e[i..i + 4].try_into().expect("4 bytes"));
                    index.insert(e[..32].try_into().expect("32 bytes"), [id, v(32), v(36)]);
                }
            }
        }
        let mut compacted = HashMap::new();
        for o in backend.list(MANIFESTS)? {
            let m = Manifest::decode(&backend.get(&o.key)?.0)?;
            compacted.insert(m.key.clone(), Arc::new(m));
        }
        Ok(Self {
            backend,
            cache: PackCache::new(cache_packs),
            compacted: RwLock::new(compacted),
            packer: Mutex::new(Packer::new(index, next, level)),
        })
    }

    pub fn len(&self) -> usize {
        self.compacted.read().unwrap().len()
    }

    pub fn compacted(&self, key: &str) -> Option<Arc<Manifest>> {
        self.compacted.read().unwrap().get(key).cloned()
    }

    /// The key was deleted or overwritten: its manifest goes (its chunks stay until a chunk GC).
    pub fn forget(&self, key: &str) {
        if self.compacted.write().unwrap().remove(key).is_some()
            && let Err(e) = self.backend.delete(&manifest_key(key))
        {
            eprintln!("rift: could not delete the manifest of {key}: {e:#}");
        }
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
        self.backend.put(&format!("{PACKS}{id:08}.idx"), idx, &[])?;
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
                let refs = self.packer.lock().unwrap().add(&plain, &mut |id, z, idx| self.store_pack(id, z, idx))?;
                m.set_refs(refs);
                Ok(Some((m, etag)))
            })();
            match attempt {
                Ok(Some(r)) => ready.push(r),
                Ok(None) => eprintln!("rift: {key}: not rebuildable byte-exact, kept as is"),
                Err(e) => eprintln!("rift: {key}: {e:#}"),
            }
        }
        if let Err(e) = self.packer.lock().unwrap().seal(&mut |id, z, idx| self.store_pack(id, z, idx)) {
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

    /// Writes the manifest, rebuilds the blob from what's stored (cold cache) and only then swaps the
    /// original for a stub.
    fn commit(&self, m: Manifest, etag: &str, threads: usize) -> Result<bool> {
        let key = m.key.clone();
        self.backend.put(&manifest_key(&key), m.encode(), &[])?;
        let fresh = PackCache::new(8);
        let (mut sha, mut n) = (Sha256::new(), 0u64);
        let rebuilt = stream(&m, 0, m.size, threads, &|id| fresh.get(id, &|id| self.fetch_pack(id)), &mut |b| {
            sha.update(b);
            n += b.len() as u64;
            true
        });
        if rebuilt.is_err() || n != m.size || hex(&sha.finalize()) != m.sha256 {
            let _ = self.backend.delete(&manifest_key(&key));
            if let Err(e) = rebuilt {
                bail!("rebuild from the store failed: {e:#}");
            }
            return Ok(false);
        }
        let size = m.size;
        self.compacted.write().unwrap().insert(key.clone(), Arc::new(m));
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
        let (mut m, plain) = analyze("k", &digest, 0, &blob, 4).unwrap().expect("rebuildable");
        assert!(m.groups.len() > 1, "test needs several groups");
        let packs: Mutex<HashMap<u32, Arc<Vec<u8>>>> = Mutex::new(HashMap::new());
        let mut packer = Packer::new(HashMap::new(), 0, 3);
        let mut sink = |id: u32, z: Vec<u8>, _idx: Vec<u8>| {
            packs.lock().unwrap().insert(id, Arc::new(zstd::bulk::decompress(&z, 64 << 20)?));
            Ok(())
        };
        let refs = packer.add(&plain, &mut sink).unwrap();
        packer.seal(&mut sink).unwrap();
        m.set_refs(refs);
        let m = Manifest::decode(&m.encode()).unwrap();
        let pack = |id: u32| packs.lock().unwrap().get(&id).cloned().context("missing pack");
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
    fn digests_from_keys() {
        let d = "ab".to_string() + &"0".repeat(62);
        assert_eq!(blob_digest(&format!("docker/registry/v2/blobs/sha256/ab/{d}/data")), Some(d.clone()));
        assert_eq!(blob_digest(&format!("docker/registry/v2/blobs/sha256/cd/{d}/data")), None);
        assert_eq!(blob_digest("docker/registry/v2/repositories/x/_layers/sha256/ab/link"), None);
    }
}
