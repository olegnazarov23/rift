//! Prometheus metrics, served at /-/metrics.

use std::{
    fmt::Write,
    sync::atomic::{AtomicU64, Ordering::Relaxed},
};

pub struct Metrics {
    // from the compactor's listing of the bucket
    pub bucket_logical_bytes: AtomicU64,
    pub bucket_stored_bytes: AtomicU64,
    pub bucket_listed_at: AtomicU64,
    // set by the compactor after each round
    pub compacted_blobs: AtomicU64,
    pub compacted_logical_bytes: AtomicU64,
    pub hot_copies: AtomicU64,
    pub packs: AtomicU64,
    pub pack_bytes: AtomicU64,
    pub retired_pack_bytes: AtomicU64,
    // counters
    pub compactions: AtomicU64,
    pub compacted_bytes: AtomicU64,
    pub compaction_ms: AtomicU64,
    pub compaction_failures: AtomicU64,
    pub cold_reads: AtomicU64,
    pub cold_read_bytes: AtomicU64,
    pub cold_read_ms: AtomicU64,
    pub cold_read_errors: AtomicU64,
    pub hot_copies_made: AtomicU64,
    pub hot_copies_cooled: AtomicU64,
    pub redirects: AtomicU64,
    pub passthrough: AtomicU64,
    pub backend_errors: AtomicU64,
    pub gc_runs: AtomicU64,
    pub gc_deleted_packs: AtomicU64,
    pub gc_deleted_bytes: AtomicU64,
    pub gc_moved_chunks: AtomicU64,
}

macro_rules! zero {
    ($($f:ident),*) => { Metrics { $($f: AtomicU64::new(0)),* } };
}

pub static METRICS: Metrics = zero!(
    bucket_logical_bytes, bucket_stored_bytes, bucket_listed_at, compacted_blobs, compacted_logical_bytes,
    hot_copies, packs, pack_bytes, retired_pack_bytes, compactions, compacted_bytes, compaction_ms,
    compaction_failures, cold_reads, cold_read_bytes, cold_read_ms, cold_read_errors, hot_copies_made,
    hot_copies_cooled, redirects, passthrough, backend_errors, gc_runs, gc_deleted_packs, gc_deleted_bytes,
    gc_moved_chunks
);

pub fn add(counter: &AtomicU64, n: u64) {
    counter.fetch_add(n, Relaxed);
}

pub fn set(gauge: &AtomicU64, v: u64) {
    gauge.store(v, Relaxed);
}

pub fn render() -> String {
    let m = &METRICS;
    let mut out = String::new();
    let mut put = |name: &str, kind: &str, help: &str, value: f64| {
        let _ = writeln!(out, "# HELP rift_{name} {help}\n# TYPE rift_{name} {kind}\nrift_{name} {value}");
    };
    let g = |a: &AtomicU64| a.load(Relaxed) as f64;
    let (logical, stored) = (g(&m.bucket_logical_bytes), g(&m.bucket_stored_bytes));
    put("bucket_logical_bytes", "gauge", "Bytes the registry sees in the bucket, compacted blobs at their original size.", logical);
    put("bucket_stored_bytes", "gauge", "Bytes actually stored in the bucket, rift's packs and manifests included.", stored);
    put(
        "bucket_saved_ratio",
        "gauge",
        "1 - stored / logical, as of the last listing.",
        if logical > 0.0 { 1.0 - stored / logical } else { 0.0 },
    );
    put("bucket_listed_timestamp_seconds", "gauge", "When the compactor last listed the bucket.", g(&m.bucket_listed_at));
    put("compacted_blobs", "gauge", "Blobs stored compacted.", g(&m.compacted_blobs));
    put("compacted_logical_bytes", "gauge", "Original size of the compacted blobs.", g(&m.compacted_logical_bytes));
    put("hot_copies", "gauge", "Compacted blobs whose original is back in the bucket after a read.", g(&m.hot_copies));
    put("packs", "gauge", "Packs in use.", g(&m.packs));
    put("pack_bytes", "gauge", "Stored size of the packs in use.", g(&m.pack_bytes));
    put("retired_pack_bytes", "gauge", "Stored size of packs waiting out the GC grace period.", g(&m.retired_pack_bytes));
    put("compactions_total", "counter", "Blobs compacted.", g(&m.compactions));
    put("compacted_bytes_total", "counter", "Original bytes of the blobs compacted.", g(&m.compacted_bytes));
    put("compaction_seconds_total", "counter", "Time spent compacting.", g(&m.compaction_ms) / 1e3);
    put("compaction_failures_total", "counter", "Blobs that were left as they were (not rebuildable byte-exact, or errors).", g(&m.compaction_failures));
    put("cold_reads_total", "counter", "Reads served by rebuilding a compacted blob.", g(&m.cold_reads));
    put("cold_read_bytes_total", "counter", "Bytes served by rebuilding.", g(&m.cold_read_bytes));
    put("cold_read_seconds_total", "counter", "Time spent streaming rebuilt blobs.", g(&m.cold_read_ms) / 1e3);
    put("cold_read_errors_total", "counter", "Rebuilds that failed part way.", g(&m.cold_read_errors));
    put("hot_copies_made_total", "counter", "Originals put back after a cold read.", g(&m.hot_copies_made));
    put("hot_copies_cooled_total", "counter", "Hot copies replaced by the stub again.", g(&m.hot_copies_cooled));
    put("redirects_total", "counter", "Presigned GETs redirected to the backend.", g(&m.redirects));
    put("passthrough_requests_total", "counter", "Requests passed through to the backend.", g(&m.passthrough));
    put("backend_errors_total", "counter", "Backend requests that failed or answered 5xx.", g(&m.backend_errors));
    put("gc_runs_total", "counter", "Chunk GC rounds.", g(&m.gc_runs));
    put("gc_deleted_packs_total", "counter", "Packs deleted by GC.", g(&m.gc_deleted_packs));
    put("gc_deleted_bytes_total", "counter", "Stored bytes of the packs GC deleted.", g(&m.gc_deleted_bytes));
    put("gc_moved_chunks_total", "counter", "Live chunks GC copied out of sparse packs.", g(&m.gc_moved_chunks));
    out
}
