//! rift: byte-exact recompression for container registries and object storage.

mod cas;
mod gateway;
mod index;
mod metrics;
mod registry;
mod s3;
mod sigv4;
mod store;

use std::{
    collections::{BTreeMap, HashSet},
    fs,
    path::{Path, PathBuf},
    sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicBool, Ordering},
        mpsc,
    },
    thread,
    time::Instant,
};

use anyhow::{Context, Result, bail};
use clap::{Args, Parser, Subcommand};
use serde_json::{Value, json};

use registry::Registry;
use store::{BlobCost, Dedup, INDEX_ENTRY_BYTES, Kind, Unpacked};

/// compressed input being unpacked at once; peak memory is about 10x this, plus the zstd contexts
const IN_FLIGHT_BYTES: u64 = 256 << 20;

#[derive(Parser)]
#[command(name = "rift", version, about = "Byte-exact recompression for container registries and object storage")]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Measure how much smaller your data would be, byte-exact, without changing anything
    Scan(ScanArgs),
    /// Serve S3 in front of a bucket, compacting blobs that go cold
    Gateway(GatewayArgs),
    /// Put every compacted blob back as it was and remove rift's data (stop the gateway first)
    Eject(EjectArgs),
}

#[derive(Args)]
struct EjectArgs {
    /// The backing S3 endpoint
    #[arg(long)]
    backend: String,
    #[arg(long)]
    bucket: String,
    #[arg(long, default_value = "us-east-1")]
    region: String,
    #[arg(long)]
    threads: Option<usize>,
    /// The gateway's --state-dir, if it set one
    #[arg(long)]
    state_dir: Option<std::path::PathBuf>,
    /// Do it; without this, only say what would happen
    #[arg(long)]
    yes: bool,
}

fn eject(args: &EjectArgs) -> Result<()> {
    let env = |k: &str| std::env::var(k).with_context(|| format!("set {k}"));
    let backend = s3::Backend::new(&args.backend, &args.bucket, &args.region, env("RIFT_BACKEND_ACCESS_KEY")?, env("RIFT_BACKEND_SECRET_KEY")?)?;
    let threads = args.threads.unwrap_or_else(|| thread::available_parallelism().map_or(4, |n| n.get()));
    let store = cas::Store::open(Arc::new(backend), &state_dir(&args.state_dir, &args.bucket), 19, 64, threads, 64 << 20)?;
    if !args.yes {
        println!("{} compacted blobs in {}/{} would be put back as they were, then rift's packs deleted.", store.len(), args.backend, args.bucket);
        println!("Stop the gateway first, then run again with --yes.");
        return Ok(());
    }
    let (restored, failed, bytes) = store.eject(threads)?;
    println!("put back {restored} blobs ({:.2} GB)", bytes as f64 / 1e9);
    if failed > 0 {
        bail!("{failed} blobs couldn't be put back; rift's packs were kept, so nothing is lost. Fix and run again.");
    }
    println!("rift's packs and manifests are gone; point the registry back at the bucket.");
    Ok(())
}

#[derive(Args)]
struct GatewayArgs {
    /// Address to listen on
    #[arg(long, default_value = "127.0.0.1:9100")]
    listen: String,
    /// The backing S3 endpoint, e.g. https://s3.us-east-1.amazonaws.com
    #[arg(long)]
    backend: String,
    /// Bucket name clients use; also the backing bucket unless --backend-bucket
    #[arg(long)]
    bucket: String,
    #[arg(long)]
    backend_bucket: Option<String>,
    #[arg(long, default_value = "us-east-1")]
    region: String,
    /// Compact blobs nobody has written or read for this long (30s, 10m, 12h, 7d)
    #[arg(long, default_value = "7d")]
    cold_after: String,
    /// How often to look for cold blobs
    #[arg(long, default_value = "1m")]
    interval: String,
    /// Smallest blob worth compacting, in MB
    #[arg(long, default_value_t = 1.0)]
    min_size_mb: f64,
    /// zstd level for packs
    #[arg(long, default_value_t = 19)]
    pack_level: i32,
    /// Threads for compaction and for each rebuild (default: all cores)
    #[arg(long)]
    threads: Option<usize>,
    /// How long a pack no blob uses stays readable before it's deleted
    #[arg(long, default_value = "10m")]
    gc_grace: String,
    /// Rewrite packs with less than this fraction of their bytes still in use
    #[arg(long, default_value_t = 0.5)]
    gc_rewrite_below: f64,
    /// The backend as clients reach it; presigned GETs of blobs stored as they are redirect there
    #[arg(long)]
    redirect_endpoint: Option<String>,
    /// Don't put a blob's original bytes back after a cold read
    #[arg(long)]
    no_hot_copy: bool,
    /// Memory for manifests loaded from the bucket, in MB
    #[arg(long, default_value_t = 512)]
    manifest_cache_mb: usize,
    /// Where the chunk index lives (rebuilt from the bucket if lost); default: a temp directory
    #[arg(long)]
    state_dir: Option<std::path::PathBuf>,
}

fn state_dir(given: &Option<std::path::PathBuf>, bucket: &str) -> std::path::PathBuf {
    given.clone().unwrap_or_else(|| std::env::temp_dir().join("rift").join(bucket))
}

/// "30s", "10m", "12h", "7d" or plain seconds.
fn seconds(s: &str) -> Result<u64> {
    let (n, unit) = s.split_at(s.find(|c: char| !c.is_ascii_digit()).unwrap_or(s.len()));
    let n: u64 = n.parse().with_context(|| format!("bad duration {s}"))?;
    Ok(n * match unit {
        "" | "s" => 1,
        "m" => 60,
        "h" => 3600,
        "d" => 86_400,
        _ => bail!("bad duration unit in {s}"),
    })
}

fn gateway(args: &GatewayArgs) -> Result<()> {
    let env = |k: &str| std::env::var(k).with_context(|| format!("set {k}"));
    let bucket = args.backend_bucket.as_deref().unwrap_or(&args.bucket);
    let backend = s3::Backend::new(&args.backend, bucket, &args.region, env("RIFT_BACKEND_ACCESS_KEY")?, env("RIFT_BACKEND_SECRET_KEY")?)?;
    let client_keys = match (std::env::var("RIFT_CLIENT_ACCESS_KEY"), std::env::var("RIFT_CLIENT_SECRET_KEY")) {
        (Ok(a), Ok(s)) => Some((a, s)),
        _ => None,
    };
    let loopback = args.listen.starts_with("127.") || args.listen.starts_with("localhost:") || args.listen.starts_with("[::1]");
    if client_keys.is_none() && !loopback {
        bail!("set RIFT_CLIENT_ACCESS_KEY and RIFT_CLIENT_SECRET_KEY to listen beyond localhost");
    }
    let threads = args.threads.unwrap_or_else(|| thread::available_parallelism().map_or(4, |n| n.get()));
    let dir = state_dir(&args.state_dir, bucket);
    let store = Arc::new(cas::Store::open(Arc::new(backend), &dir, args.pack_level, 64, threads, args.manifest_cache_mb << 20)?);
    eprintln!(
        "rift gateway on {} for {}/{bucket}: {} compacted blobs, compacting after {} cold, client signatures {}",
        args.listen,
        args.backend,
        store.len(),
        args.cold_after,
        if client_keys.is_some() { "checked" } else { "not checked" }
    );
    store.publish_totals();
    let reads = Arc::new(Mutex::new(std::collections::HashMap::new()));
    let schedule = gateway::Schedule {
        cold_after: seconds(&args.cold_after)?,
        min_size: (args.min_size_mb * 1e6) as u64,
        interval: std::time::Duration::from_secs(seconds(&args.interval)?),
        threads,
        rewrite_below: args.gc_rewrite_below,
        grace: seconds(&args.gc_grace)?,
    };
    {
        let (store, reads) = (store.clone(), reads.clone());
        thread::spawn(move || gateway::compactor(store, reads, schedule));
    }
    Arc::new(gateway::Gateway {
        store,
        bucket: args.bucket.clone(),
        client_keys,
        reads,
        workers: threads,
        redirect: args.redirect_endpoint.clone(),
        hot_copy: !args.no_hot_copy,
    })
    .serve(&args.listen)
}

#[derive(Args)]
struct ScanArgs {
    /// A directory, or registry://HOST/REPO (e.g. registry://docker.io/library/python)
    source: String,
    /// Registry tags to scan, comma-separated (default: the most recent --latest tags)
    #[arg(long, value_delimiter = ',')]
    tags: Vec<String>,
    /// How many recent tags to scan when --tags isn't given
    #[arg(long, default_value_t = 10)]
    latest: usize,
    /// Platform to pick from multi-arch images
    #[arg(long, default_value = "linux/amd64")]
    platform: String,
    /// Stop after this many GB of input (a sample)
    #[arg(long)]
    max_gb: Option<f64>,
    /// Threads to use (default: all cores)
    #[arg(long)]
    threads: Option<usize>,
    /// Storage price in $ per GB-month, for the cost line
    #[arg(long, default_value_t = 0.023)]
    price: f64,
    /// zstd level for the shared packs, as the store uses (lower is faster and a little pessimistic)
    #[arg(long, default_value_t = 19)]
    level: i32,
    /// Skip rebuilding each blob to prove it comes back byte-exact
    #[arg(long)]
    no_verify: bool,
    /// Print machine-readable JSON
    #[arg(long)]
    json: bool,
}

struct Scan<'a> {
    args: &'a ScanArgs,
    dedup: Dedup,
    costs: Vec<BlobCost>,
    limit: Option<u64>,
    sampled: bool,
}

impl Scan<'_> {
    /// Adds one unpacked blob; false once the sample limit is reached.
    fn commit(&mut self, name: &str, u: Unpacked) -> bool {
        let c = self.dedup.commit(u);
        let what = match (c.kind, c.fallback) {
            (_, Some(why)) => format!("kept as is: {why}"),
            (Kind::Gzip, _) => format!(
                "unpacked {}, new {}, recipe+refs {}",
                mb(c.plain_bytes),
                mb(c.new_bytes),
                mb(c.side_bytes)
            ),
            (Kind::Jpeg, _) => format!("lepton {}", mb(c.side_bytes)),
            (Kind::Other, _) => format!("new {}", mb(c.new_bytes)),
        };
        eprintln!("[{:4}] {:<56} {:>9}  {:<5}  {what}", self.costs.len() + 1, short(name, 56), mb(c.bytes), kind(c.kind));
        self.costs.push(c);
        let seen: u64 = self.costs.iter().map(|c| c.bytes).sum();
        if self.limit.is_some_and(|l| seen >= l) {
            self.sampled = true;
            return false;
        }
        true
    }

    /// Unpacks blobs from `input` on worker threads and commits them in input order.
    fn run(&mut self, input: mpsc::Receiver<(String, Vec<u8>)>, threads: usize) {
        let verify = !self.args.no_verify;
        let stop = AtomicBool::new(false);
        let in_flight = (Mutex::new(0u64), Condvar::new());
        let (job_tx, job_rx) = mpsc::sync_channel::<(usize, String, Vec<u8>)>(0);
        let job_rx = Mutex::new(job_rx);
        let (done_tx, done_rx) = mpsc::channel::<(usize, String, Unpacked)>();
        thread::scope(|sc| {
            let (stop, in_flight, job_rx) = (&stop, &in_flight, &job_rx);
            sc.spawn(move || {
                for (i, (name, data)) in input.into_iter().enumerate() {
                    let n = data.len() as u64;
                    let mut used = in_flight.0.lock().unwrap();
                    while *used > 0 && *used + n > IN_FLIGHT_BYTES && !stop.load(Ordering::Relaxed) {
                        used = in_flight.1.wait(used).unwrap();
                    }
                    if stop.load(Ordering::Relaxed) {
                        break;
                    }
                    *used += n;
                    drop(used);
                    if job_tx.send((i, name, data)).is_err() {
                        break;
                    }
                }
            });
            for _ in 0..threads {
                let done_tx = done_tx.clone();
                sc.spawn(move || {
                    loop {
                        let Ok((i, name, data)) = job_rx.lock().unwrap().recv() else { break };
                        if stop.load(Ordering::Relaxed) {
                            continue;
                        }
                        if done_tx.send((i, name, store::unpack(data, threads, verify))).is_err() {
                            break;
                        }
                    }
                });
            }
            drop(done_tx);
            let (mut pending, mut next) = (BTreeMap::new(), 0);
            'done: for (i, name, u) in &done_rx {
                pending.insert(i, (name, u));
                while let Some((name, u)) = pending.remove(&next) {
                    next += 1;
                    let n = u.bytes;
                    let more = self.commit(&name, u);
                    *in_flight.0.lock().unwrap() -= n;
                    if !more {
                        stop.store(true, Ordering::Relaxed);
                    }
                    in_flight.1.notify_all();
                    if !more {
                        break 'done;
                    }
                }
            }
            drop(done_rx);
        });
    }
}

fn kind(k: Kind) -> &'static str {
    match k {
        Kind::Gzip => "gzip",
        Kind::Jpeg => "jpeg",
        Kind::Other => "other",
    }
}

fn mb(b: u64) -> String {
    let b = b as f64;
    match b {
        _ if b >= 1e9 => format!("{:.2} GB", b / 1e9),
        _ if b >= 1e6 => format!("{:.1} MB", b / 1e6),
        _ => format!("{:.1} KB", b / 1e3),
    }
}

fn short(s: &str, n: usize) -> String {
    let len = s.chars().count();
    if len <= n { s.to_string() } else { format!("…{}", s.chars().skip(len + 1 - n).collect::<String>()) }
}

/// Regular files under `dir`, in path order; symlinks are skipped so nothing is counted twice.
fn files(dir: &Path, out: &mut Vec<PathBuf>) -> Result<()> {
    let mut entries = fs::read_dir(dir)
        .with_context(|| format!("reading {}", dir.display()))?
        .collect::<std::io::Result<Vec<_>>>()?;
    entries.sort_by_key(|e| e.path());
    for e in entries {
        let t = e.file_type()?;
        if t.is_dir() {
            files(&e.path(), out)?;
        } else if t.is_file() {
            out.push(e.path());
        }
    }
    Ok(())
}

fn scan(args: &ScanArgs) -> Result<()> {
    let threads = args.threads.unwrap_or_else(|| thread::available_parallelism().map_or(4, |n| n.get()));
    let started = Instant::now();
    let mut s = Scan {
        args,
        dedup: Dedup::new(args.level, threads),
        costs: Vec::new(),
        limit: args.max_gb.map(|g| (g * 1e9) as u64),
        sampled: false,
    };
    let (mut identical, mut identical_bytes) = (0u64, 0u64);

    let source = if let Some(rest) = args.source.strip_prefix("registry://") {
        let (host, repo) = rest.split_once('/').context("expected registry://HOST/REPO")?;
        let mut reg = Registry::new(host, repo)?;
        let tags = if args.tags.is_empty() { reg.tags(args.latest)? } else { args.tags.clone() };
        let name = reg.name();
        eprintln!("rift scan: {name} ({} tags, {}), {threads} threads", tags.len(), args.platform);
        let (mut seen, mut listed, mut scanned, mut todo) = (HashSet::new(), 0, Vec::new(), Vec::new());
        for tag in &tags {
            match reg.layers(tag, &args.platform) {
                Ok(layers) => {
                    scanned.push(tag.clone());
                    listed += layers.len();
                    // registries already store identical blobs once
                    todo.extend(layers.into_iter().filter(|l| seen.insert(l.digest.clone())).map(|l| (tag.clone(), l)));
                }
                Err(e) => eprintln!("skipping {tag}: {e:#}"),
            }
        }
        // blobs download one ahead of the scan
        let (tx, rx) = mpsc::sync_channel(1);
        let fetcher = thread::spawn(move || -> Result<()> {
            for (tag, layer) in todo {
                let data = reg.blob(&layer.digest)?;
                anyhow::ensure!(data.len() as u64 == layer.size, "{}: size mismatch", layer.digest);
                let hex = layer.digest.split_once(':').map_or(&layer.digest[..], |(_, h)| h);
                if tx.send((format!("{tag} {}", &hex[..hex.len().min(12)]), data)).is_err() {
                    break; // the scan stopped early
                }
            }
            Ok(())
        });
        s.run(rx, threads);
        if !s.sampled {
            fetcher.join().expect("blob fetcher panicked")?;
        }
        json!({"kind": "registry", "name": name, "tags": scanned, "platform": args.platform,
               "layer_refs": listed, "unique_blobs": seen.len()})
    } else {
        let root = PathBuf::from(&args.source);
        let mut list = Vec::new();
        if root.is_file() {
            list.push(root.clone());
        } else {
            files(&root, &mut list)?;
        }
        anyhow::ensure!(!list.is_empty(), "no files under {}", root.display());
        let files = list.len();
        eprintln!("rift scan: {} ({files} files), {threads} threads", root.display());
        let name = root.display().to_string();
        let (tx, rx) = mpsc::sync_channel(1);
        let reader = thread::spawn(move || -> Result<(u64, u64)> {
            let (mut seen, mut identical, mut identical_bytes) = (HashSet::new(), 0, 0);
            for path in list {
                let data = fs::read(&path).with_context(|| format!("reading {}", path.display()))?;
                if !seen.insert(*blake3::hash(&data).as_bytes()) {
                    identical += 1;
                    identical_bytes += data.len() as u64;
                    continue;
                }
                let rel = path.strip_prefix(&root).ok().filter(|r| !r.as_os_str().is_empty());
                let name = rel.unwrap_or(&path).display().to_string();
                if tx.send((name, data)).is_err() {
                    break; // the scan stopped early
                }
            }
            Ok((identical, identical_bytes))
        });
        s.run(rx, threads);
        (identical, identical_bytes) = reader.join().expect("file reader panicked")?;
        json!({"kind": "directory", "name": name, "files": files})
    };
    anyhow::ensure!(!s.costs.is_empty(), "nothing to scan");

    let (plain, unique) = (s.dedup.plain_bytes, s.dedup.unique_bytes);
    let (packs, unique_chunks) = s.dedup.finish();
    let costs = &s.costs;
    let shared = packs + unique_chunks * INDEX_ENTRY_BYTES;
    // pack and index bytes are charged to blobs by the new content each one brought
    let per_new = shared as f64 / unique.max(1) as f64;
    let after = |c: &BlobCost| c.side_bytes as f64 + c.new_bytes as f64 * per_new;
    let input: u64 = costs.iter().map(|c| c.bytes).sum();
    let stored = costs.iter().map(|c| c.side_bytes).sum::<u64>() + shared;
    let pct = |before: f64, after: f64| 100.0 * (1.0 - after / before.max(1.0));
    let saved_pct = pct(input as f64, stored as f64);

    let mut kinds = Vec::new();
    for k in [Kind::Gzip, Kind::Jpeg, Kind::Other] {
        let v: Vec<&BlobCost> = costs.iter().filter(|c| c.kind == k).collect();
        if !v.is_empty() {
            let (b, a) = (v.iter().map(|c| c.bytes).sum::<u64>(), v.iter().map(|c| after(c)).sum::<f64>());
            kinds.push((k, v.len(), b, a));
        }
    }
    let mut kept: BTreeMap<&str, (usize, u64)> = BTreeMap::new();
    for c in costs {
        if let Some(why) = c.fallback {
            let e = kept.entry(why).or_default();
            e.0 += 1;
            e.1 += c.bytes;
        }
    }
    let mismatches: usize = kept.iter().filter(|(why, _)| why.contains("did not match")).map(|(_, v)| v.0).sum();
    let per_100tb = saved_pct / 100.0 * 100e3 * args.price * 12.0;
    let saved_year = input.saturating_sub(stored) as f64 / 1e9 * args.price * 12.0;
    let secs = started.elapsed().as_secs_f64();

    if args.json {
        let breakdown: serde_json::Map<String, Value> = kinds
            .iter()
            .map(|&(k, n, b, a)| {
                (kind(k).into(), json!({"blobs": n, "bytes": b, "projected_bytes": a.round() as u64, "saved_pct": pct(b as f64, a)}))
            })
            .collect();
        let kept: serde_json::Map<String, Value> =
            kept.iter().map(|(why, (n, b))| (why.to_string(), json!({"blobs": n, "bytes": b}))).collect();
        println!(
            "{}",
            serde_json::to_string_pretty(&json!({
                "source": source, "sampled": s.sampled, "threads": threads, "zstd_level": args.level,
                "verified": !args.no_verify, "rebuild_mismatches": mismatches,
                "blobs": costs.len(), "input_bytes": input, "projected_bytes": stored, "saved_pct": saved_pct,
                "unpacked_bytes": plain, "unique_unpacked_bytes": unique, "unique_chunks": unique_chunks,
                "breakdown": breakdown, "kept_as_is": kept,
                "identical_files": identical, "identical_bytes": identical_bytes,
                "price_per_gb_month": args.price, "saved_per_year": saved_year, "saved_per_year_per_100tb": per_100tb,
                "seconds": secs,
            }))?
        );
        return Ok(());
    }

    println!();
    println!("rift scan  {}", source["name"].as_str().unwrap_or_default());
    println!();
    println!("  Stored today   {:>10}   {} blobs{}", mb(input), costs.len(), if s.sampled { " (sample)" } else { "" });
    println!("  With rift      {:>10}   {saved_pct:.1}% less", mb(stored));
    println!();
    println!("  {:<6} {:>6} {:>12} {:>12} {:>8}", "", "blobs", "today", "with rift", "saved");
    for &(k, n, b, a) in &kinds {
        println!("  {:<6} {n:>6} {:>12} {:>12} {:>7.1}%", kind(k), mb(b), mb(a.round() as u64), pct(b as f64, a));
    }
    println!();
    if let (Some(refs), Some(tags)) = (source["layer_refs"].as_u64(), source["tags"].as_array()) {
        let range = match (tags.first(), tags.last()) {
            (Some(a), Some(b)) if tags.len() > 1 => format!(" ({} … {})", a.as_str().unwrap_or_default(), b.as_str().unwrap_or_default()),
            (Some(a), _) => format!(" ({})", a.as_str().unwrap_or_default()),
            _ => String::new(),
        };
        println!(
            "  Registry       {refs} layer refs across {} tags{range}, {} unique blobs (already stored once each)",
            tags.len(),
            source["unique_blobs"]
        );
    }
    if identical > 0 {
        println!("  Identical      {identical} files ({}) copy other files byte for byte; left out above", mb(identical_bytes));
    }
    if plain > 0 {
        println!("  Content        {} once unpacked; {:.0}% repeats across blobs", mb(plain), pct(plain as f64, unique as f64));
    }
    for (why, (n, b)) in &kept {
        println!("  Kept as is     {n} × {why} ({})", mb(*b));
    }
    if args.no_verify {
        println!("  Byte-exact     not checked (--no-verify)");
    } else if mismatches == 0 {
        println!("  Byte-exact     every recompressed blob was rebuilt and compared");
    } else {
        println!("  Byte-exact     {mismatches} blob(s) did not rebuild exactly; costed as stored today");
    }
    if input >= 1_000_000_000_000 {
        println!("  Cost           ${saved_year:.0}/year saved on this data (at ${}/GB-month)", args.price);
    }
    println!("  Cost           ${per_100tb:.0}/year saved per 100 TB like this (at ${}/GB-month)", args.price);
    println!("  Took           {secs:.0} s ({:.1} MB/s, {threads} threads)", input as f64 / 1e6 / secs);
    if args.level < 19 {
        println!("  Note           packs measured at zstd -{}; the store uses -19, about 4 points better", args.level);
    }
    if s.sampled {
        println!("  Note           sampled; dedup grows with history, so the full number is usually higher");
    }
    println!();
    Ok(())
}

fn main() -> Result<()> {
    match Cli::parse().cmd {
        Cmd::Scan(args) => scan(&args),
        Cmd::Gateway(args) => gateway(&args),
        Cmd::Eject(args) => eject(&args),
    }
}
