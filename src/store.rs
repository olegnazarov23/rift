//! What a blob would cost in the store: byte-exact recompression plus content dedup.
//! Nothing is written anywhere; this only measures.

use std::{
    collections::HashSet,
    io::Cursor,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
        mpsc,
    },
    thread,
};

use fastcdc::v2020::FastCDC;
use lepton_jpeg::{DEFAULT_THREAD_POOL, EnabledFeatures, decode_lepton, encode_lepton};
use preflate_rs::{PreflateConfig, preflate_independent, recreate_independent};
use zstd::stream::raw::CParameter;

/// plain bytes per restart-point group (groups analyze and rebuild in parallel)
const GROUP_PLAIN: usize = 8 << 20;
/// unique chunks are packed into blocks this size before compression
const PACK_BYTES: usize = 16 << 20;
/// per chunk reference in a blob's manifest
pub const REF_BYTES: u64 = 12;
/// per unique chunk in the index (hash + location)
pub const INDEX_ENTRY_BYTES: u64 = 44;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Kind {
    /// unpacked byte-exactly, content deduped
    Gzip,
    /// Lepton
    Jpeg,
    /// deduped as is
    Other,
}

pub struct BlobCost {
    pub kind: Kind,
    pub bytes: u64,
    /// stored outside the shared packs: recipe + framing + refs (gzip), Lepton (jpeg), refs (other)
    pub side_bytes: u64,
    /// content handed to dedup
    pub plain_bytes: u64,
    /// part of it not seen before (lands in packs)
    pub new_bytes: u64,
    /// why a gzip or jpeg blob was kept as is
    pub fallback: Option<&'static str>,
}

/// Offset of the deflate payload in a gzip member, or None if this isn't gzip.
pub fn gzip_payload_offset(b: &[u8]) -> Option<usize> {
    if b.len() < 18 || b[0] != 0x1f || b[1] != 0x8b || b[2] != 8 {
        return None;
    }
    let flags = b[3];
    let mut i = 10;
    if flags & 4 != 0 {
        i += 2 + u16::from_le_bytes([*b.get(i)?, *b.get(i + 1)?]) as usize; // FEXTRA
    }
    for bit in [8, 16] {
        if flags & bit != 0 {
            i += b.get(i..)?.iter().position(|&c| c == 0)? + 1; // FNAME, FCOMMENT
        }
    }
    if flags & 2 != 0 {
        i += 2; // FHCRC
    }
    (i < b.len()).then_some(i)
}

/// Content-defined chunk dedup; unique chunks are packed and compressed on background threads.
pub struct Dedup {
    index: HashSet<[u8; 32]>,
    pack: Vec<u8>,
    tx: Option<mpsc::SyncSender<Vec<u8>>>,
    workers: Vec<thread::JoinHandle<()>>,
    stored: Arc<AtomicU64>,
    pub plain_bytes: u64,
    pub unique_bytes: u64,
}

impl Dedup {
    /// Packs are compressed like the store does (long-distance matching, 16 MB window), at `level`.
    pub fn new(level: i32, threads: usize) -> Self {
        let (tx, rx) = mpsc::sync_channel::<Vec<u8>>(threads);
        let rx = Arc::new(Mutex::new(rx));
        let stored = Arc::new(AtomicU64::new(0));
        let workers = (0..threads.max(1))
            .map(|_| {
                let (rx, stored) = (rx.clone(), stored.clone());
                thread::spawn(move || {
                    let mut z = zstd::bulk::Compressor::new(level).expect("zstd");
                    z.set_parameter(CParameter::EnableLongDistanceMatching(true)).expect("zstd");
                    z.set_parameter(CParameter::WindowLog(24)).expect("zstd");
                    loop {
                        let Ok(pack) = rx.lock().unwrap().recv() else { break };
                        stored.fetch_add(z.compress(&pack).expect("zstd").len() as u64, Ordering::Relaxed);
                    }
                })
            })
            .collect();
        Self {
            index: HashSet::new(),
            pack: Vec::with_capacity(PACK_BYTES),
            tx: Some(tx),
            workers,
            stored,
            plain_bytes: 0,
            unique_bytes: 0,
        }
    }

    /// Adds content; returns how many chunks it was cut into.
    pub fn add(&mut self, plain: &[u8]) -> u64 {
        let mut n = 0;
        for c in FastCDC::new(plain, 16 << 10, 64 << 10, 256 << 10) {
            let bytes = &plain[c.offset..c.offset + c.length];
            n += 1;
            if self.index.insert(*blake3::hash(bytes).as_bytes()) {
                self.unique_bytes += bytes.len() as u64;
                self.pack.extend_from_slice(bytes);
                if self.pack.len() >= PACK_BYTES {
                    self.seal();
                }
            }
        }
        self.plain_bytes += plain.len() as u64;
        n
    }

    fn seal(&mut self) {
        let pack = std::mem::replace(&mut self.pack, Vec::with_capacity(PACK_BYTES));
        self.tx.as_ref().unwrap().send(pack).unwrap();
    }

    /// Waits for compression; returns (compressed pack bytes, unique chunks).
    pub fn finish(mut self) -> (u64, u64) {
        if !self.pack.is_empty() {
            self.seal();
        }
        drop(self.tx.take());
        for w in self.workers.drain(..) {
            w.join().unwrap();
        }
        (self.stored.load(Ordering::Relaxed), self.index.len() as u64)
    }

    /// Adds a blob's content and returns what the blob costs.
    pub fn commit(&mut self, u: Unpacked) -> BlobCost {
        let before = self.unique_bytes;
        let chunks = if u.content.is_empty() { 0 } else { self.add(&u.content) };
        BlobCost {
            kind: u.kind,
            bytes: u.bytes,
            side_bytes: u.side_bytes + REF_BYTES * chunks,
            plain_bytes: u.content.len() as u64,
            new_bytes: self.unique_bytes - before,
            fallback: u.fallback,
        }
    }
}

/// A blob after the expensive part (unpacking, recompression, the rebuild check), before dedup.
pub struct Unpacked {
    pub kind: Kind,
    pub bytes: u64,
    /// recipe + framing (gzip) or Lepton (jpeg); chunk refs are added at commit
    pub side_bytes: u64,
    /// content for the chunk store: the unpacked gzip, or the blob itself
    pub content: Vec<u8>,
    /// why a gzip or jpeg blob was kept as is
    pub fallback: Option<&'static str>,
}

/// Recompresses one blob byte-exactly; safe to run on many blobs at once.
pub fn unpack(data: Vec<u8>, threads: usize, verify: bool) -> Unpacked {
    let bytes = data.len() as u64;
    let mut fallback = None;

    if let Some(start) = gzip_payload_offset(&data) {
        let config = PreflateConfig { max_chain_length: 4096, plain_text_limit: 4 << 30, verify_compression: false };
        match preflate_independent(&data[start..], &config, GROUP_PLAIN, threads) {
            // single gzip member: deflate stream plus 8-byte trailer cover the blob
            Ok((recipe, deflate_len, plain)) if start + deflate_len + 8 == data.len() => {
                if !verify
                    || recreate_independent(&plain, &recipe, threads).is_ok_and(|r| r[..] == data[start..start + deflate_len])
                {
                    let recipe_z = zstd::bulk::compress(&recipe, 19).map_or(recipe.len(), |z| z.len()) as u64;
                    let side_bytes = recipe_z + start as u64 + 8;
                    return Unpacked { kind: Kind::Gzip, bytes, side_bytes, content: plain, fallback: None };
                }
                fallback = Some("gzip rebuild did not match");
            }
            Ok(_) => fallback = Some("multi-member gzip"),
            Err(_) => fallback = Some("gzip from an unsupported encoder"),
        }
    } else if data.starts_with(&[0xFF, 0xD8]) {
        let mut lep = Cursor::new(Vec::new());
        let write = EnabledFeatures::compat_lepton_vector_write();
        if encode_lepton(&mut Cursor::new(&data), &mut lep, &write, &DEFAULT_THREAD_POOL).is_ok() {
            let lep = lep.into_inner();
            let ok = !verify || {
                let mut out = Vec::new();
                let read = EnabledFeatures::compat_lepton_vector_read();
                decode_lepton(&mut Cursor::new(&lep), &mut out, &read, &DEFAULT_THREAD_POOL).is_ok() && out == data
            };
            if ok && (lep.len() as u64) < bytes {
                let side_bytes = lep.len() as u64;
                return Unpacked { kind: Kind::Jpeg, bytes, side_bytes, content: Vec::new(), fallback: None };
            }
            fallback = Some(if ok { "jpeg Lepton can't shrink" } else { "jpeg rebuild did not match" });
        } else {
            fallback = Some("jpeg Lepton doesn't support");
        }
    }

    // anything else is deduped and zstd-packed as is (lossless by construction)
    Unpacked { kind: Kind::Other, bytes, side_bytes: 0, content: data, fallback }
}
