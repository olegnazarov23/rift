//! `rift gateway`: the S3 endpoint a registry (or anything else that stores in S3) talks to.
//! Requests pass through to the backing bucket, re-signed. Blobs the compactor has compacted are
//! served by rebuilding them, byte-exact; listings show their original sizes.

use std::{
    collections::HashMap,
    fs::File,
    io::{self, Read, Write},
    path::PathBuf,
    sync::{Arc, Mutex, mpsc},
    thread,
    time::{Duration, UNIX_EPOCH},
};

use anyhow::{Result, anyhow};
use sha2::{Digest, Sha256};
use tiny_http::{Header, Request, Response, Server, StatusCode};

use crate::{
    cas::{Manifest, Store, blob_digest},
    metrics::{METRICS, add, set},
    s3::{self, Backend, decode_aws_chunked, xml_all, xml_escape, xml_unescape},
    sigv4::{self, hex, parse_query, unix_now, uri_decode},
};

pub struct Gateway {
    pub store: Arc<Store>,
    /// bucket name clients use
    pub bucket: String,
    pub client_keys: Option<(String, String)>,
    /// last time each blob was read, unix seconds; the compactor leaves recently read blobs alone
    pub reads: Arc<Mutex<HashMap<String, u64>>>,
    pub workers: usize,
    /// the backend as clients reach it: presigned GETs of blobs stored as they are redirect there
    pub redirect: Option<String>,
    /// put a blob's original bytes back after a cold read, until it goes cold again
    pub hot_copy: bool,
    /// largest request body accepted (bodies are held in memory while they're checked)
    pub max_body: usize,
    /// requests in flight before new ones get 503 SlowDown
    pub max_requests: usize,
    /// cold rebuilds at once (each uses `workers` threads)
    pub cold_reads: Arc<Slots>,
    /// bytes of temp disk for rebuilt blobs waiting on their clients
    pub spill: Arc<Slots>,
    /// write a JSON line per request to stdout
    pub access_log: bool,
}

/// A counting semaphore over units (rebuilds, bytes): waits up to a limit for them to be free.
/// Asking for more than the whole limit waits for all of it.
pub struct Slots {
    used: Mutex<u64>,
    freed: std::sync::Condvar,
    max: u64,
}

impl Slots {
    pub fn new(max: u64) -> Self {
        Self { used: Mutex::new(0), freed: std::sync::Condvar::new(), max: max.max(1) }
    }

    /// Takes `n` units, or gives up after `wait`.
    fn take(self: &Arc<Self>, n: u64, wait: Duration) -> Option<Taken> {
        let n = n.clamp(1, self.max);
        let used = self.used.lock().unwrap();
        let (mut used, _) = self.freed.wait_timeout_while(used, wait, |u| *u + n > self.max).unwrap();
        if *used + n > self.max {
            return None;
        }
        *used += n;
        Some(Taken(self.clone(), n))
    }
}

/// Units taken from `Slots`, given back when dropped (a panicking holder included).
struct Taken(Arc<Slots>, u64);

impl Drop for Taken {
    fn drop(&mut self) {
        *self.0.used.lock().unwrap_or_else(|e| e.into_inner()) -= self.1;
        self.0.freed.notify_all();
    }
}

/// Counts a request in flight until dropped.
struct InFlight(Arc<std::sync::atomic::AtomicUsize>);

impl Drop for InFlight {
    fn drop(&mut self) {
        self.0.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
    }
}

/// Hides request signatures (headers and presigned URLs) from logs.
fn redact(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut rest = s;
    while let Some(i) = rest.find("Signature=") {
        out.push_str(&rest[..i + "Signature=".len()]);
        rest = &rest[i + "Signature=".len()..];
        let end = rest.find(|c: char| !c.is_ascii_hexdigit()).unwrap_or(rest.len());
        out.push('…');
        rest = &rest[end..];
    }
    out.push_str(rest);
    out
}

/// Query parameters that only carry a presigned URL's signature.
const PRESIGN_PARAMS: &[&str] = &[
    "X-Amz-Algorithm", "X-Amz-Credential", "X-Amz-Date", "X-Amz-Expires", "X-Amz-SignedHeaders",
    "X-Amz-Signature", "X-Amz-Security-Token",
];

/// Names the operation for newer AWS SDKs (GitLab's s3_v2 driver sends it); S3 ignores it.
const SDK_HINT: &str = "x-id";

static SPILL_ID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Whether rift passes on a key: none that start with '/', hold an empty segment, or a '.' or '..'
/// one. Backends normalize such keys differently (MinIO drops a leading '/', so "//.rift/x" reached
/// rift's own data), and registries never use them.
fn valid_key(key: &str) -> bool {
    !key.starts_with('/') && !key.contains("//") && !key.split('/').any(|s| s == "." || s == "..")
}

/// rift's own data, out of clients' reach.
fn internal(key: &str) -> bool {
    key == ".rift" || key.starts_with(".rift/")
}

/// The requests registries make, and the only ones rift passes on: bucket configuration,
/// tagging, ACLs, versions and the like are refused, with rift's credentials or not.
#[derive(Clone, Copy, Debug, PartialEq)]
enum Op {
    /// ListObjects V1 or V2
    List,
    /// ListMultipartUploads
    Uploads,
    Location,
    HeadBucket,
    /// DeleteObjects
    DeleteMany,
    /// GetObject or HeadObject, with response-* overrides
    Get,
    /// ListParts
    Parts,
    Put,
    /// CopyObject
    Copy,
    /// UploadPart or UploadPartCopy
    Part,
    /// CreateMultipartUpload
    Create,
    /// CompleteMultipartUpload
    Complete,
    Delete,
    /// AbortMultipartUpload
    Abort,
}

fn operation(method: &str, key: &str, query: &[(String, String)], copy: bool) -> Option<Op> {
    let names: Vec<&str> = query.iter().map(|(k, _)| k.as_str()).collect();
    let only = |allowed: &[&str]| names.iter().all(|n| allowed.contains(n));
    let has = |n: &str| names.contains(&n);
    const LIST: &[&str] =
        &["list-type", "prefix", "delimiter", "max-keys", "continuation-token", "start-after", "encoding-type", "fetch-owner", "marker"];
    const UPLOADS: &[&str] = &["uploads", "prefix", "delimiter", "key-marker", "upload-id-marker", "max-uploads", "encoding-type"];
    const PARTS: &[&str] = &["uploadId", "part-number-marker", "max-parts", "encoding-type"];
    if key.is_empty() {
        return match method {
            "GET" if has("uploads") && only(UPLOADS) => Some(Op::Uploads),
            "GET" if names == ["location"] => Some(Op::Location),
            "GET" if only(LIST) => Some(Op::List),
            "HEAD" if names.is_empty() => Some(Op::HeadBucket),
            "POST" if names == ["delete"] => Some(Op::DeleteMany),
            _ => None,
        };
    }
    let upload = has("uploadId");
    match method {
        "GET" if upload && only(PARTS) => Some(Op::Parts),
        "GET" | "HEAD" if names.iter().all(|n| n.starts_with("response-")) => Some(Op::Get),
        "PUT" if upload && has("partNumber") && names.len() == 2 => Some(Op::Part),
        "PUT" if names.is_empty() && copy => Some(Op::Copy),
        "PUT" if names.is_empty() => Some(Op::Put),
        "POST" if names == ["uploads"] => Some(Op::Create),
        "POST" if names == ["uploadId"] => Some(Op::Complete),
        "DELETE" if names.is_empty() => Some(Op::Delete),
        "DELETE" if names == ["uploadId"] => Some(Op::Abort),
        _ => None,
    }
}

enum After {
    Nothing,
    /// DeleteObjects: forget the keys the bucket reports deleted
    ForgetDeleted,
    RewriteList,
    /// these keys were deleted or overwritten if the request succeeds
    Forget(Vec<String>),
}

fn h(name: &str, value: &str) -> Header {
    Header::from_bytes(name.as_bytes(), value.as_bytes()).expect("ASCII header")
}

/// A response with a known length always says so. tiny_http otherwise switches to chunked encoding
/// from 32 KB up, and a chunked HEAD carries no Content-Length, which Docker refuses.
fn sized<R: Read>(status: StatusCode, headers: Vec<Header>, body: R, len: Option<usize>) -> Response<R> {
    let r = Response::new(status, headers, body, len, None);
    if len.is_some() { r.with_chunked_threshold(usize::MAX) } else { r }
}

thread_local! {
    /// the response this thread last sent: status, length, how it was served (for the access log)
    static SENT: std::cell::Cell<Option<(u16, Option<usize>, &'static str)>> = const { std::cell::Cell::new(None) };
}

/// Sends a response and notes it for the access log. `served`: "pass" (from the bucket),
/// "compacted" (rebuilt from packs), "redirect", "local" (health and metrics) or "rift" (rift's own
/// answer: refusals and errors).
fn send<R: Read>(req: Request, resp: Response<R>, served: &'static str) -> Result<()> {
    SENT.with(|s| s.set(Some((resp.status_code().0, resp.data_length(), served))));
    req.respond(resp)?;
    Ok(())
}

/// The access key a request was signed with, from its Authorization header or presigned query.
fn access_key_id(authorization: Option<&str>, query: &str) -> Option<String> {
    let id = |credential: &str| credential.split('/').next().filter(|k| !k.is_empty()).map(str::to_string);
    authorization
        .and_then(|a| a.split("Credential=").nth(1))
        .and_then(id)
        .or_else(|| parse_query(query).into_iter().find(|(k, _)| k == "X-Amz-Credential").and_then(|(_, v)| id(&v)))
}

fn error(req: Request, status: u16, code: &str, message: &str) -> Result<()> {
    let body = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Error><Code>{code}</Code><Message>{}</Message></Error>",
        xml_escape(message)
    );
    send(req, Response::from_data(body).with_status_code(status).with_header(h("content-type", "application/xml")), "rift")
}

/// Request headers worth passing to the backing store.
fn pass_headers(headers: &[(String, String)], chunked: bool) -> Vec<(String, String)> {
    headers
        .iter()
        .filter(|(k, _)| match k.as_str() {
            "content-type" | "content-md5" | "cache-control" | "content-disposition" | "content-language" | "expires"
            | "range" | "if-match" | "if-none-match" | "if-modified-since" | "if-unmodified-since" => true,
            "content-encoding" => !chunked,
            "x-amz-content-sha256" | "x-amz-date" | "x-amz-security-token" | "x-amz-decoded-content-length"
            | "x-amz-trailer" => false,
            k if chunked && (k.starts_with("x-amz-checksum-") || k == "x-amz-sdk-checksum-algorithm") => false,
            k => k.starts_with("x-amz-"),
        })
        .cloned()
        .collect()
}

/// Response headers worth passing back.
fn back_headers(resp: &reqwest::blocking::Response) -> Vec<Header> {
    resp.headers()
        .iter()
        .filter(|(k, _)| {
            matches!(
                k.as_str(),
                "content-type" | "etag" | "last-modified" | "content-range" | "accept-ranges" | "cache-control"
                    | "content-disposition" | "content-encoding" | "content-language" | "expires"
            ) || k.as_str().starts_with("x-amz-")
        })
        .filter_map(|(k, v)| Header::from_bytes(k.as_str().as_bytes(), v.as_bytes()).ok())
        .collect()
}

/// Puts compacted blobs' sizes back into a ListObjectsV2 result and hides rift's own objects.
fn rewrite_list(xml: &str, compacted: &dyn Fn(&str) -> Option<(u64, u64)>) -> String {
    let set = |block: &str, tag: &str, value: &str| {
        let (open, close) = (format!("<{tag}>"), format!("</{tag}>"));
        match (block.find(&open), block.find(&close)) {
            (Some(a), Some(b)) if a < b => format!("{}{open}{value}{}", &block[..a], &block[b..]),
            _ => block.to_string(),
        }
    };
    let mut out = String::with_capacity(xml.len());
    let mut rest = xml;
    let mut hidden = 0usize;
    while let Some(i) = rest.find("<Contents>") {
        out.push_str(&rest[..i]);
        let Some(len) = rest[i..].find("</Contents>") else {
            rest = &rest[i..];
            break;
        };
        let end = i + len + "</Contents>".len();
        let block = &rest[i..end];
        let key = s3::xml_first(block, "Key").map(|k| uri_decode(&xml_unescape(k))).unwrap_or_default();
        if internal(&key) || !valid_key(&key) {
            hidden += 1;
        } else if let Some((size, mtime)) = compacted(&key) {
            out.push_str(&set(&set(block, "Size", &size.to_string()), "LastModified", &s3::iso(mtime)));
        } else {
            out.push_str(block);
        }
        rest = &rest[end..];
    }
    out.push_str(rest);
    // and every common prefix inside rift's data (a listing with prefix=.rift/ and a delimiter)
    let mut kept = String::with_capacity(out.len());
    let mut rest = out.as_str();
    while let Some(i) = rest.find("<CommonPrefixes>") {
        kept.push_str(&rest[..i]);
        let Some(len) = rest[i..].find("</CommonPrefixes>") else {
            rest = &rest[i..];
            break;
        };
        let end = i + len + "</CommonPrefixes>".len();
        let prefix = s3::xml_first(&rest[i..end], "Prefix").map(|p| uri_decode(&xml_unescape(p))).unwrap_or_default();
        if internal(prefix.trim_end_matches('/')) || prefix.starts_with(".rift/") || prefix.starts_with('/') {
            hidden += 1;
        } else {
            kept.push_str(&rest[i..end]);
        }
        rest = &rest[end..];
    }
    kept.push_str(rest);
    out = kept;
    if hidden > 0
        && let Some(n) = s3::xml_first(&out, "KeyCount").and_then(|n| n.parse::<usize>().ok())
    {
        out = out.replacen(&format!("<KeyCount>{n}</KeyCount>"), &format!("<KeyCount>{}</KeyCount>", n.saturating_sub(hidden)), 1);
    }
    out
}

/// "bytes=a-b", "bytes=a-" or "bytes=-n" to a half-open range.
fn parse_range(r: &str, size: u64) -> Option<(u64, u64)> {
    let spec = r.trim().strip_prefix("bytes=")?;
    if spec.contains(',') {
        return None;
    }
    let (a, b) = spec.split_once('-')?;
    let (start, end) = if a.is_empty() {
        (size.saturating_sub(b.parse().ok()?), size)
    } else {
        let s: u64 = a.parse().ok()?;
        (s, if b.is_empty() { size } else { (b.parse::<u64>().ok()? + 1).min(size) })
    };
    (start < end).then_some((start, end))
}

struct ChannelReader {
    rx: mpsc::Receiver<Vec<u8>>,
    buf: Vec<u8>,
    at: usize,
}

impl Read for ChannelReader {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        while self.at == self.buf.len() {
            match self.rx.recv() {
                Ok(b) => (self.buf, self.at) = (b, 0),
                Err(_) => return Ok(0),
            }
        }
        let n = out.len().min(self.buf.len() - self.at);
        out[..n].copy_from_slice(&self.buf[self.at..self.at + n]);
        self.at += n;
        Ok(n)
    }
}

/// A cold read's bytes on their way to the client. The rebuild writes them to a temp file as fast
/// as it can and the response reads them back as fast as the client takes them, so a slow client
/// holds temp disk (out of the spill budget), not a rebuild slot or memory. The file is deleted,
/// and its bytes given back to the budget, once both sides are done with it.
struct Spill {
    path: PathBuf,
    state: Mutex<SpillState>,
    more: std::sync::Condvar,
    /// the file's share of the budget, given back after the file is deleted
    _reserved: Taken,
}

#[derive(Default)]
struct SpillState {
    written: u64,
    /// set when the rebuild ends: whether every byte was written
    done: Option<bool>,
    reader_gone: bool,
}

impl Spill {
    /// The file to write to and a reader that follows it.
    fn create(path: PathBuf, reserved: Taken) -> io::Result<(Arc<Spill>, File, SpillReader)> {
        let spill = Arc::new(Spill { path, state: Mutex::default(), more: Default::default(), _reserved: reserved });
        let writer = crate::cas::create_private(&spill.path)?;
        let file = File::open(&spill.path)?;
        Ok((spill.clone(), writer, SpillReader { spill, file, at: 0 }))
    }

    /// Records bytes written; false once the client has gone.
    fn wrote(&self, n: usize) -> bool {
        let mut s = self.state.lock().unwrap();
        s.written += n as u64;
        self.more.notify_all();
        !s.reader_gone
    }

    fn finish(&self, whole: bool) {
        self.state.lock().unwrap().done = Some(whole);
        self.more.notify_all();
    }
}

impl Drop for Spill {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

struct SpillReader {
    spill: Arc<Spill>,
    file: File,
    at: u64,
}

impl Read for SpillReader {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        if out.is_empty() {
            return Ok(0);
        }
        let at = self.at;
        let ready = {
            let s = self.spill.state.lock().unwrap();
            let s = self.spill.more.wait_while(s, |s| s.written == at && s.done.is_none()).unwrap();
            if s.written == at {
                // a short rebuild must not look like the end of the blob
                return if s.done == Some(true) { Ok(0) } else { Err(io::Error::other("the rebuild failed part way")) };
            }
            s.written - at
        };
        let want = out.len().min(usize::try_from(ready).unwrap_or(usize::MAX));
        let n = self.file.read(&mut out[..want])?;
        if n == 0 {
            return Err(io::Error::other("temp file shorter than what was written"));
        }
        self.at += n as u64;
        Ok(n)
    }
}

impl Drop for SpillReader {
    fn drop(&mut self) {
        self.spill.state.lock().unwrap().reader_gone = true;
    }
}

/// Where a rebuild's bytes go: a spill file, or (when the temp dir can't be written) a channel
/// the client drains, which holds the rebuild to the client's pace.
enum Sink {
    File(Arc<Spill>, File),
    Channel(mpsc::SyncSender<Vec<u8>>),
}

impl Sink {
    /// Passes bytes on; false when there's no point going on: the client has gone (unless `keep`),
    /// or the temp file can't be written.
    fn put(&mut self, b: &[u8], keep: bool) -> bool {
        match self {
            Sink::File(spill, file) => file.write_all(b).is_ok() && (spill.wrote(b.len()) || keep),
            Sink::Channel(tx) => tx.send(b.to_vec()).is_ok(),
        }
    }
}

impl Gateway {
    /// Serves HTTP, or HTTPS with `tls` = (certificate PEM, private key PEM).
    pub fn serve(self: Arc<Self>, listen: &str, tls: Option<(Vec<u8>, Vec<u8>)>, limits: tiny_http::Limits) -> Result<()> {
        let listener = std::net::TcpListener::bind(listen).map_err(|e| anyhow!("listening on {listen}: {e}"))?;
        let ssl = tls.map(|(certificate, private_key)| tiny_http::SslConfig { certificate, private_key });
        let server = Server::from_listener_with_limits(listener, ssl, limits).map_err(|e| anyhow!("listening on {listen}: {e}"))?;
        let active = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        for req in server.incoming_requests() {
            if active.load(std::sync::atomic::Ordering::Relaxed) >= self.max_requests {
                // answered off this thread: a client that doesn't read its response would stall
                // every request behind it (a connection carries one request at a time, so this is
                // bounded by --max-connections)
                thread::spawn(move || {
                    let _ = error(req, 503, "SlowDown", "too many requests in flight, retry");
                });
                continue;
            }
            active.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let (gw, in_flight) = (self.clone(), InFlight(active.clone()));
            thread::spawn(move || {
                let _in_flight = in_flight;
                gw.handle(req);
            });
        }
        Ok(())
    }

    fn handle(&self, req: Request) {
        let started = std::time::Instant::now();
        let method = req.method().as_str().to_string();
        let url = req.url().to_string();
        let header = |name: &'static str| req.headers().iter().find(|x| x.field.equiv(name)).map(|x| x.value.as_str().to_string());
        let (remote, forwarded_for) = (req.remote_addr().map(|a| a.ip().to_string()), header("x-forwarded-for"));
        let key_id = access_key_id(header("authorization").as_deref(), url.split_once('?').map_or("", |(_, q)| q));
        let bytes_in = req.body_length();
        if std::env::var_os("RIFT_DEBUG").is_some() {
            let hs: Vec<String> = req.headers().iter().map(|x| format!("{}: {}", x.field, redact(x.value.as_str()))).collect();
            eprintln!("rift: debug: {method} {} [{}]", redact(&url), hs.join("; "));
        }
        SENT.with(|s| s.set(None));
        if let Err(e) = self.route(req, &method, &url) {
            eprintln!("rift: {method} {}: {e:#}", redact(&url));
        }
        if self.access_log {
            // no response: the request failed, or the client went away, before one was sent
            let (status, bytes_out, served) = SENT.with(|s| s.take()).unwrap_or((0, None, "none"));
            let line = serde_json::json!({
                "time": s3::iso(unix_now()),
                "remote": remote,
                "forwarded_for": forwarded_for,
                "key_id": key_id,
                "method": method,
                "path": redact(&url),
                "status": status,
                "bytes_in": bytes_in,
                "bytes_out": bytes_out,
                "ms": started.elapsed().as_millis() as u64,
                "served": served,
            });
            println!("{line}");
        }
    }

    fn route(&self, req: Request, method: &str, url: &str) -> Result<()> {
        let (raw_path, raw_query) = url.split_once('?').unwrap_or((url, ""));
        // no bucket name can start with '-'
        if raw_path == "/-/metrics" {
            let out = Response::from_data(crate::metrics::render());
            return send(req, out.with_header(h("content-type", "text/plain; version=0.0.4")), "local");
        }
        if raw_path == "/-/healthz" {
            let body = format!("{{\"status\":\"ok\",\"compacted\":{}}}\n", self.store.len());
            return send(req, Response::from_data(body).with_header(h("content-type", "application/json")), "local");
        }
        let headers: Vec<(String, String)> = req
            .headers()
            .iter()
            .map(|x| (x.field.as_str().as_str().to_ascii_lowercase(), x.value.as_str().to_string()))
            .collect();
        let header = |name: &str| headers.iter().find(|(k, _)| k == name).map(|(_, v)| v.clone());
        let mut query = parse_query(raw_query);
        let presigned = query.iter().any(|(k, _)| k == "X-Amz-Signature");
        let mut seed = None;
        if let Some((access, secret)) = &self.client_keys {
            let signed = if presigned {
                matches!(method, "GET" | "HEAD") && sigv4::verify_presigned(access, secret, method, raw_path, raw_query, &header)
            } else {
                match header("authorization").map(|a| sigv4::verify(access, secret, method, raw_path, raw_query, &a, &header)) {
                    Some(Ok(s)) => seed = Some(s),
                    Some(Err(sigv4::Denied::Skewed)) => {
                        let msg = format!(
                            "signed at {}, over {} minutes from this server's clock ({})",
                            header("x-amz-date").unwrap_or_default(),
                            sigv4::MAX_SKEW / 60,
                            sigv4::amz_date(sigv4::unix_now())
                        );
                        return error(req, 403, "RequestTimeTooSkewed", &msg);
                    }
                    _ => {}
                }
                seed.is_some()
            };
            if !signed {
                return error(req, 403, "SignatureDoesNotMatch", "signature check failed");
            }
        }
        let seed = seed.as_ref();
        if self.client_keys.is_some() {
            // one way to authenticate: a presigned signature moved into a header would skip its expiry
            if !presigned && query.iter().any(|(k, _)| PRESIGN_PARAMS.contains(&k.as_str())) {
                return error(req, 400, "InvalidRequest", "only one auth mechanism allowed: the Authorization header or a presigned URL");
            }
            // as S3 requires: headers that change what's stored or how a body is read must be signed
            let signed: Vec<String> = if presigned {
                query.iter().find(|(k, _)| k == "X-Amz-SignedHeaders").map_or(vec![], |(_, v)| v.split(';').map(str::to_string).collect())
            } else {
                header("authorization")
                    .and_then(|a| a.split(',').find_map(|f| f.trim().strip_prefix("SignedHeaders=").map(str::to_string)))
                    .map_or(vec![], |v| v.split(';').map(str::to_string).collect())
            };
            let unsigned = headers
                .iter()
                .find(|(k, _)| (k.starts_with("x-amz-") || k == "content-md5" || k == "content-encoding") && !signed.contains(k));
            if let Some((k, _)) = unsigned {
                return error(req, 403, "AccessDenied", &format!("there were headers present in the request which were not signed: {k}"));
            }
        }
        query.retain(|(k, _)| !PRESIGN_PARAMS.contains(&k.as_str()) && k != SDK_HINT);
        let path = raw_path.trim_start_matches('/');
        let (bucket, raw_key) = path.split_once('/').unwrap_or((path, ""));
        if bucket != self.bucket {
            return error(req, 404, "NoSuchBucket", bucket);
        }
        let key = uri_decode(raw_key);
        if !valid_key(&key) {
            return error(req, 400, "InvalidRequest", "keys may not start with '/' or hold empty, '.' or '..' segments");
        }
        if internal(&key) {
            return error(req, 403, "AccessDenied", "reserved for rift");
        }
        let copy_source = header("x-amz-copy-source");
        let Some(op) = operation(method, &key, &query, copy_source.is_some()) else {
            return error(req, 501, "NotImplemented", "rift passes on only the requests registries make");
        };
        // a body only where one is meant: a GET or a presigned URL can't make rift buffer one
        let body = req.body_length().is_some_and(|n| n > 0) || header("transfer-encoding").is_some();
        if body && (presigned || matches!(op, Op::Get | Op::List | Op::Uploads | Op::Location | Op::HeadBucket | Op::Parts | Op::Delete | Op::Abort)) {
            return error(req, 400, "InvalidRequest", "this request can't carry a body");
        }
        match op {
            Op::Get => {
                if blob_digest(&key).is_some() {
                    self.reads.lock().unwrap().insert(key.clone(), unix_now());
                }
                // response-content-type and friends, as S3 applies them
                let overrides: Vec<(String, String)> =
                    query.iter().filter_map(|(k, v)| Some((k.strip_prefix("response-")?.to_string(), v.clone()))).collect();
                match self.store.info(&key) {
                    Some(_) if !self.store.is_hot(&key) => match self.store.manifest(&key) {
                        Ok(Some(m)) => self.serve_compacted(req, method, &m, header("range"), &overrides),
                        Ok(None) => self.forward(req, method, &key, &query, &headers, After::Nothing, seed),
                        Err(e) => error(req, 500, "InternalError", &format!("loading the manifest: {e:#}")),
                    },
                    // stored as it is: a client following a presigned URL fetches it from the backend directly
                    _ => match &self.redirect {
                        Some(endpoint) if presigned && method == "GET" && overrides.is_empty() => {
                            self.redirect_to_backend(req, &key, endpoint)
                        }
                        _ => self.forward(req, method, &key, &query, &headers, After::Nothing, seed),
                    },
                }
            }
            // old registries stat files with V1 listings
            Op::List => self.forward(req, method, &key, &query, &headers, After::RewriteList, seed),
            Op::Copy => self.copy(req, &key, &query, &headers, seed),
            Op::Part if copy_source.is_some() => self.copy(req, &key, &query, &headers, seed),
            // these replace or delete the object, and with it its compacted form
            Op::Put | Op::Complete | Op::Delete => self.forward(req, method, &key, &query, &headers, After::Forget(vec![key.clone()]), seed),
            Op::DeleteMany => self.forward(req, method, &key, &query, &headers, After::ForgetDeleted, seed),
            Op::Uploads | Op::Location | Op::HeadBucket | Op::Parts | Op::Part | Op::Create | Op::Abort => {
                self.forward(req, method, &key, &query, &headers, After::Nothing, seed)
            }
        }
    }

    fn forward(
        &self,
        mut req: Request,
        method: &str,
        key: &str,
        query: &[(String, String)],
        headers: &[(String, String)],
        after: After,
        seed: Option<&sigv4::Seed>,
    ) -> Result<()> {
        if req.body_length().is_some_and(|n| n > self.max_body) {
            return error(req, 413, "EntityTooLarge", "request body over --max-body-mb");
        }
        let mut body = Vec::new();
        req.as_reader().take(self.max_body as u64 + 1).read_to_end(&mut body)?;
        if body.len() > self.max_body {
            return error(req, 413, "EntityTooLarge", "request body over --max-body-mb");
        }
        let header = |name: &str| headers.iter().find(|(k, _)| k == name).map(|(_, v)| v.as_str());
        let payload = header("x-amz-content-sha256").unwrap_or("");
        // only the signed payload hash says the body is aws-chunked: an unsigned Content-Encoding
        // could otherwise turn the hash check off
        let chunked = payload.starts_with("STREAMING-");
        let mut trailers = Vec::new();
        if chunked {
            // signed streaming uploads chain each chunk's signature from the request's
            let signed = seed.filter(|_| payload.starts_with("STREAMING-AWS4-HMAC-SHA256-PAYLOAD"));
            match decode_aws_chunked(&body, signed) {
                Ok((data, t)) => {
                    body = data;
                    // trailing checksums go on as headers, so the backend still checks them
                    trailers = t.into_iter().filter(|(k, _)| k.starts_with("x-amz-checksum-")).collect();
                }
                Err(e) => return error(req, 400, "IncompleteBody", &format!("{e:#}")),
            }
            if header("x-amz-decoded-content-length").is_some_and(|n| n.parse() != Ok(body.len())) {
                return error(req, 400, "IncompleteBody", "decoded length doesn't match x-amz-decoded-content-length");
            }
        } else if payload.len() == 64 && payload.bytes().all(|b| b.is_ascii_hexdigit()) && sigv4::sha256_hex(&body) != payload.to_ascii_lowercase() {
            return error(req, 400, "XAmzContentSHA256Mismatch", "the body doesn't match its signed hash");
        }
        if matches!(after, After::ForgetDeleted) {
            let keys: Vec<String> = xml_all(&String::from_utf8_lossy(&body), "Key").into_iter().map(xml_unescape).collect();
            if keys.iter().any(|k| !valid_key(k) || internal(k)) {
                return error(req, 403, "AccessDenied", "a key to delete is rift's or isn't one rift passes on");
            }
        }
        let rest = if key.is_empty() { String::new() } else { Backend::key_path(key) };
        add(&METRICS.passthrough, 1);
        let mut pass = pass_headers(headers, chunked);
        pass.extend(trailers);
        let resp = match self.store.backend.send(method, &rest, query, &pass, body) {
            Ok(r) => r,
            Err(e) => {
                add(&METRICS.backend_errors, 1);
                return error(req, 502, "BadGateway", &format!("{e:#}"));
            }
        };
        let status = resp.status().as_u16();
        if status >= 500 {
            add(&METRICS.backend_errors, 1);
        }
        let back = back_headers(&resp);
        let ok = (200..300).contains(&status);
        match after {
            After::RewriteList if status == 200 => {
                let xml = resp.text()?;
                let body = rewrite_list(&xml, &|k| self.store.info(k).map(|i| (i.size, i.mtime)));
                let mut out = Response::from_data(body).with_status_code(200);
                for x in back {
                    out.add_header(x);
                }
                send(req, out, "pass")?;
                return Ok(());
            }
            After::Forget(keys) if ok => keys.iter().for_each(|k| self.store.forget(k)),
            After::ForgetDeleted if status == 200 => {
                let xml = resp.text()?;
                for deleted in xml_all(&xml, "Deleted") {
                    if let Some(k) = s3::xml_first(deleted, "Key") {
                        self.store.forget(&xml_unescape(k));
                    }
                }
                let mut out = Response::from_data(xml).with_status_code(200);
                for x in back {
                    out.add_header(x);
                }
                return send(req, out, "pass");
            }
            _ => {}
        }
        let len = resp.headers().get("content-length").and_then(|v| v.to_str().ok()?.parse::<usize>().ok());
        if std::env::var_os("RIFT_DEBUG").is_some() {
            eprintln!("rift: debug: -> {status} content-length {len:?}");
        }
        send(req, sized(StatusCode(status), back, resp, len), "pass")?;
        Ok(())
    }

    fn serve_compacted(&self, req: Request, method: &str, m: &Arc<Manifest>, range: Option<String>, overrides: &[(String, String)]) -> Result<()> {
        let (start, end, partial) = match range.as_deref().map(|r| parse_range(r, m.size)) {
            None => (0, m.size, false),
            Some(Some((s, e))) => (s, e, true),
            Some(None) => return error(req, 416, "InvalidRange", "the range is not satisfiable"),
        };
        let mut headers = vec![
            h("content-type", "application/octet-stream"),
            h("etag", &m.etag()),
            h("last-modified", &httpdate::fmt_http_date(UNIX_EPOCH + Duration::from_secs(m.mtime))),
            h("accept-ranges", "bytes"),
        ];
        if partial {
            headers.push(h("content-range", &format!("bytes {start}-{}/{}", end - 1, m.size)));
        }
        for (name, value) in overrides {
            if matches!(name.as_str(), "content-type" | "content-language" | "expires" | "cache-control" | "content-disposition" | "content-encoding") {
                headers.retain(|x| !x.field.as_str().as_str().eq_ignore_ascii_case(name));
                headers.push(h(name, value));
            }
        }
        let status = StatusCode(if partial { 206 } else { 200 });
        let len = Some((end - start) as usize);
        if method == "HEAD" {
            send(req, sized(status, headers, io::empty(), len), "compacted")?;
            return Ok(());
        }
        let Some(reserved) = self.spill.take(end - start, Duration::from_secs(60)) else {
            return error(req, 503, "SlowDown", "too many cold reads waiting on slow clients, retry");
        };
        let Some(slot) = self.cold_reads.take(1, Duration::from_secs(60)) else {
            return error(req, 503, "SlowDown", "too many cold reads in progress, retry");
        };
        let n = SPILL_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        // not guessable, so another local user can't plant something at the path first
        let nonce = std::time::SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |d| d.subsec_nanos());
        let path = std::env::temp_dir().join(format!("rift-read-{}-{n}-{nonce:08x}", std::process::id()));
        let (sink, reader): (Sink, Box<dyn Read + Send>) = match Spill::create(path, reserved) {
            Ok((spill, file, reader)) => (Sink::File(spill, file), Box::new(reader)),
            Err(e) => {
                eprintln!("rift: no temp file for a cold read ({e}); streaming it from memory");
                let (tx, rx) = mpsc::sync_channel::<Vec<u8>>(8);
                (Sink::Channel(tx), Box::new(ChannelReader { rx, buf: Vec::new(), at: 0 }))
            }
        };
        // a full read also puts the original back, from the temp file, while the blob is in use
        let copy = matches!(sink, Sink::File(..))
            && self.hot_copy
            && start == 0
            && end == m.size
            && self.store.begin_hot_copy(&m.key);
        let (store, m, workers) = (self.store.clone(), m.clone(), self.workers);
        add(&METRICS.cold_reads, 1);
        thread::spawn(move || {
            let _slot = slot;
            let (mut sink, started) = (sink, std::time::Instant::now());
            let (mut sha, mut sent) = (Sha256::new(), 0u64);
            // the hot copy wants every byte, so the rebuild goes on if the client leaves
            let result = store.stream(&m, start, end, workers, &mut |b| {
                let ok = sink.put(b, copy);
                if ok {
                    if copy {
                        sha.update(b);
                    }
                    sent += b.len() as u64;
                }
                ok
            });
            add(&METRICS.cold_read_bytes, sent);
            add(&METRICS.cold_read_ms, started.elapsed().as_millis() as u64);
            if let Err(e) = &result {
                add(&METRICS.cold_read_errors, 1);
                eprintln!("rift: rebuilding {}: {e:#}", m.key);
            }
            let whole = result.is_ok() && sent == end - start;
            if let Sink::File(spill, file) = sink {
                drop(file);
                spill.finish(whole);
                if copy && whole && hex(&sha.finalize()) == m.sha256 {
                    if let Err(e) = store.finish_hot_copy(&m.key, &spill.path, m.size) {
                        eprintln!("rift: putting {} back after a cold read: {e:#}", m.key);
                    }
                } else if copy {
                    store.abandon_hot_copy(&m.key);
                }
            }
        });
        send(req, sized(status, headers, reader, len), "compacted")?;
        Ok(())
    }

    fn redirect_to_backend(&self, req: Request, key: &str, endpoint: &str) -> Result<()> {
        match self.store.backend.presign_get(key, endpoint, 900) {
            Ok(url) => {
                add(&METRICS.redirects, 1);
                send(req, Response::empty(307).with_header(h("location", &url)), "redirect")?;
                Ok(())
            }
            Err(e) => error(req, 500, "InternalError", &format!("{e:#}")),
        }
    }

    fn copy(&self, req: Request, key: &str, query: &[(String, String)], headers: &[(String, String)], seed: Option<&sigv4::Seed>) -> Result<()> {
        let source = headers.iter().find(|(k, _)| k == "x-amz-copy-source").map(|(_, v)| v.clone()).unwrap_or_default();
        let (source, version) = source.split_once('?').map_or((source.as_str(), None), |(s, v)| (s, Some(v)));
        let source = uri_decode(source);
        let (src_bucket, src_key) = source.trim_start_matches('/').split_once('/').unwrap_or(("", ""));
        if src_bucket != self.bucket {
            return error(req, 400, "InvalidRequest", "copies only within the bucket");
        }
        if !valid_key(src_key) || internal(src_key) {
            return error(req, 403, "AccessDenied", "can't copy from there");
        }
        let part = query.iter().any(|(k, _)| k == "partNumber");
        let source_manifest = match self.store.manifest(src_key) {
            Ok(m) => m,
            Err(e) => return error(req, 500, "InternalError", &format!("loading the manifest: {e:#}")),
        };
        if let Some(m) = source_manifest {
            if part {
                return error(req, 501, "NotImplemented", "part copy from a compacted blob");
            }
            let Some(slot) = self.cold_reads.take(1, Duration::from_secs(60)) else {
                return error(req, 503, "SlowDown", "too many cold reads in progress, retry");
            };
            let mut bytes = Vec::with_capacity(m.size as usize);
            let rebuilt = self.store.stream(&m, 0, m.size, self.workers, &mut |b| {
                bytes.extend_from_slice(b);
                true
            });
            drop(slot);
            rebuilt?;
            let pass: Vec<(String, String)> = pass_headers(headers, false)
                .into_iter()
                .filter(|(k, _)| !k.starts_with("x-amz-copy-source") && k != "x-amz-metadata-directive")
                .collect();
            let etag = self.store.backend.put(key, bytes, &pass)?;
            self.store.forget(key);
            let xml = format!(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<CopyObjectResult><LastModified>{}</LastModified><ETag>{}</ETag></CopyObjectResult>",
                s3::iso(unix_now()),
                xml_escape(&etag)
            );
            send(req, Response::from_data(xml).with_header(h("content-type", "application/xml")), "compacted")?;
            return Ok(());
        }
        // the source names the backing bucket
        let mut hs = headers.to_vec();
        for (k, v) in &mut hs {
            if k == "x-amz-copy-source" {
                *v = format!("/{}{}", self.store.backend.bucket, Backend::key_path(src_key));
                if let Some(version) = version {
                    v.push('?');
                    v.push_str(version);
                }
            }
        }
        let after = if part { After::Nothing } else { After::Forget(vec![key.to_string()]) };
        self.forward(req, "PUT", key, query, &hs, after, seed)
    }
}

pub struct Schedule {
    /// compact blobs nobody has written or read for this many seconds
    pub cold_after: u64,
    pub min_size: u64,
    pub interval: Duration,
    pub threads: usize,
    /// rewrite packs with less than this fraction of their bytes live
    pub rewrite_below: f64,
    /// seconds a retired pack stays readable before it's deleted
    pub grace: u64,
}

/// Deletes temp files of cold reads that a killed rift left behind: those nobody has written to for
/// an hour. A client still reading one has it open, which keeps its bytes.
fn sweep_spills(dir: &std::path::Path) {
    let Ok(entries) = std::fs::read_dir(dir) else { return };
    for e in entries.flatten() {
        let idle = e.metadata().and_then(|m| m.modified()).ok().and_then(|t| t.elapsed().ok());
        if e.file_name().to_string_lossy().starts_with("rift-read-") && idle.is_some_and(|d| d > Duration::from_secs(3600)) {
            let _ = std::fs::remove_file(e.path());
        }
    }
}

/// Every `interval` (the first round within a minute of starting): lists the bucket, stubs idle hot
/// copies again, compacts cold blobs, then reclaims chunks of deleted ones (always on the first
/// round, to clear packs an interrupted run left behind).
pub fn compactor(store: Arc<Store>, reads: Arc<Mutex<HashMap<String, u64>>>, s: Schedule) {
    let (mut first, mut wait) = (true, s.interval.min(Duration::from_secs(60)));
    loop {
        thread::sleep(wait);
        wait = s.interval;
        sweep_spills(&std::env::temp_dir());
        match store.backend.list("") {
            Ok(objects) => {
                let now = unix_now();
                let (mut logical, mut stored) = (0u64, 0u64);
                for o in &objects {
                    stored += o.size;
                    if !o.key.starts_with(".rift/") {
                        logical += store.info(&o.key).map_or(o.size, |i| i.size);
                    }
                }
                set(&METRICS.bucket_logical_bytes, logical);
                set(&METRICS.bucket_stored_bytes, stored);
                set(&METRICS.bucket_listed_at, now);
                let (mut cold, mut cooling) = (Vec::new(), Vec::new());
                for o in objects.into_iter().filter(|o| !o.key.starts_with(".rift/")) {
                    let Some(digest) = blob_digest(&o.key) else { continue };
                    let read = reads.lock().unwrap().get(&o.key).copied().unwrap_or(0);
                    let idle = now.saturating_sub(o.modified.max(read)) >= s.cold_after;
                    match store.info(&o.key) {
                        // the original is back after a read: stub it again once nobody reads it
                        Some(i) if o.size == i.size && o.size > 0 => {
                            store.note_hot(&o.key, o.modified);
                            if idle {
                                cooling.push((o.key, o.etag));
                            }
                        }
                        Some(_) => {}
                        None if o.size >= s.min_size && idle && !store.is_kept(&o.key, &o.etag) => {
                            if o.size > store.max_unpacked {
                                store.keep(&o.key, &o.etag, &format!("{} MB, over --max-unpacked-mb", o.size >> 20));
                                add(&METRICS.compaction_failures, 1);
                            } else {
                                cold.push((o.key, digest, o.modified));
                            }
                        }
                        None => {}
                    }
                }
                let cooled = cooling.iter().filter(|(k, etag)| store.cool(k, etag).map_err(|e| eprintln!("rift: cooling {k}: {e:#}")).is_ok()).count();
                if cooled > 0 {
                    add(&METRICS.hot_copies_cooled, cooled as u64);
                    eprintln!("rift: stubbed {cooled} hot copies again");
                }
                if !cold.is_empty() {
                    eprintln!("rift: compacting {} cold blobs", cold.len());
                    let started = std::time::Instant::now();
                    let (done, bytes) = store.compact(&cold, s.threads);
                    add(&METRICS.compactions, done as u64);
                    add(&METRICS.compacted_bytes, bytes);
                    add(&METRICS.compaction_ms, started.elapsed().as_millis() as u64);
                    add(&METRICS.compaction_failures, (cold.len() - done) as u64);
                    eprintln!(
                        "rift: compacted {done} of {} blobs ({:.1} MB) in {:.1}s",
                        cold.len(),
                        bytes as f64 / 1e6,
                        started.elapsed().as_secs_f64()
                    );
                }
            }
            Err(e) => eprintln!("rift: compactor can't list the bucket: {e:#}"),
        }
        if first || store.gc_due() {
            first = false;
            let report = store.gc(s.rewrite_below, s.grace);
            if let Ok(r) = &report {
                add(&METRICS.gc_runs, 1);
                add(&METRICS.gc_deleted_packs, r.deleted as u64);
                add(&METRICS.gc_deleted_bytes, r.deleted_bytes);
                add(&METRICS.gc_moved_chunks, r.moved as u64);
            }
            match report {
                Ok(r) if r.deleted + r.retired + r.rewritten > 0 => eprintln!(
                    "rift: gc: deleted {} packs ({:.1} MB); retired {} unused and rewrote {} sparse ({} chunks moved)",
                    r.deleted,
                    r.deleted_bytes as f64 / 1e6,
                    r.retired,
                    r.rewritten,
                    r.moved
                ),
                Ok(_) => {}
                Err(e) => eprintln!("rift: gc: {e:#}"),
            }
        }
        store.publish_totals();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Small deterministic generator for robustness tests.
    struct Rng(u64);
    impl Rng {
        fn next(&mut self) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0
        }
        fn bytes(&mut self, max: usize) -> Vec<u8> {
            const ALPHABET: &[u8] = b"aZ09-_.~/%&=;:<>\r\n\"'@ +xX;chunk-signature=Size>Key>Contents>bytes=,-AWS4-HMAC-SHA256 Credential=Signature=";
            let n = (self.next() % max as u64) as usize;
            (0..n)
                .map(|_| {
                    let r = self.next();
                    if r % 4 == 0 { r as u8 } else { ALPHABET[(r >> 8) as usize % ALPHABET.len()] }
                })
                .collect()
        }
    }

    // Parsers that see client or backend input must never panic, whatever they're fed.
    #[test]
    fn parsers_survive_garbage() {
        let mut rng = Rng(0x9e37_79b9_7f4a_7c15);
        let seed = sigv4::Seed { signature: "0".repeat(64), amz_date: "20260101T000000Z".into(), scope: "x".into(), key: vec![1; 32] };
        for _ in 0..20_000 {
            let b = rng.bytes(300);
            let t = String::from_utf8_lossy(&b).into_owned();
            let _ = sigv4::parse_query(&t);
            let _ = sigv4::uri_decode(&t);
            let _ = sigv4::parse_amz_date(&t);
            let _ = s3::xml_unescape(&t);
            let _ = s3::xml_all(&t, "Key");
            let _ = s3::parse_iso(&t);
            let _ = s3::decode_aws_chunked(&b, None);
            let _ = s3::decode_aws_chunked(&b, Some(&seed));
            let _ = rewrite_list(&t, &|k| (k.len() % 2 == 0).then_some((1, 2)));
            let _ = parse_range(&t, rng.next() % 1000);
            let _ = redact(&t);
            let header = |h: &str| (h == "x-amz-date").then(|| t.clone());
            let _ = sigv4::verify("AK", "SK", "GET", &t, &t, &t, &header);
            let _ = sigv4::verify_presigned("AK", "SK", "GET", &t, &t, &header);
        }
    }

    #[test]
    fn slots_weigh_requests() {
        let s = Arc::new(Slots::new(16));
        let a = s.take(10, Duration::ZERO).expect("fits");
        assert!(s.take(10, Duration::ZERO).is_none(), "over the limit");
        let b = s.take(6, Duration::ZERO).expect("fits exactly");
        drop((a, b));
        let all = s.take(100, Duration::ZERO).expect("more than the limit waits for all of it");
        assert_eq!(all.1, 16);
        let freer = thread::spawn(move || {
            thread::sleep(Duration::from_millis(50));
            drop(all);
        });
        let one = s.take(1, Duration::from_secs(5));
        assert!(one.is_some(), "a waiter gets units given back");
        freer.join().unwrap();
        drop(one);
        let held = s.take(16, Duration::ZERO).unwrap();
        let _ = thread::spawn(move || {
            let _held = held;
            panic!("a handler bug");
        })
        .join();
        assert!(s.take(16, Duration::ZERO).is_some(), "a panicking holder gives its units back");
    }

    #[test]
    fn spill_feeds_a_slow_reader() {
        let budget = Arc::new(Slots::new(1 << 20));
        let reserved = budget.take(1000, Duration::ZERO).unwrap();
        let path = std::env::temp_dir().join(format!("rift-spill-test-{}", std::process::id()));
        let (spill, mut file, mut reader) = Spill::create(path.clone(), reserved).unwrap();
        let writer = thread::spawn(move || {
            for i in 0..10u8 {
                file.write_all(&[i; 100]).unwrap();
                assert!(spill.wrote(100), "the reader is still there");
                thread::sleep(Duration::from_millis(3));
            }
            spill.finish(true);
        });
        let (mut got, mut buf) = (Vec::new(), [0u8; 37]);
        loop {
            let n = reader.read(&mut buf).unwrap();
            if n == 0 {
                break;
            }
            got.extend_from_slice(&buf[..n]);
        }
        writer.join().unwrap();
        assert_eq!(got, (0..10u8).flat_map(|i| [i; 100]).collect::<Vec<_>>());
        drop(reader);
        assert!(!path.exists(), "the temp file goes once both sides are done");
        assert!(budget.take(1 << 20, Duration::ZERO).is_some(), "and its bytes go back to the budget");
    }

    #[test]
    fn private_files_refuse_planted_paths() {
        let dir = std::env::temp_dir().join(format!("rift-private-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let (target, planted) = (dir.join("victim"), dir.join("planted"));
        std::fs::write(&target, b"keep me").unwrap();
        #[cfg(unix)]
        std::os::unix::fs::symlink(&target, &planted).unwrap();
        assert!(crate::cas::create_private(&planted).is_err(), "a symlink at the path isn't followed");
        assert_eq!(std::fs::read(&target).unwrap(), b"keep me");
        let fresh = dir.join("fresh");
        crate::cas::create_private(&fresh).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(std::fs::metadata(&fresh).unwrap().permissions().mode() & 0o777, 0o600);
        }
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn spill_reports_a_failed_rebuild() {
        let budget = Arc::new(Slots::new(1 << 20));
        let reserved = budget.take(3, Duration::ZERO).unwrap();
        let path = std::env::temp_dir().join(format!("rift-spill-fail-{}", std::process::id()));
        let (spill, mut file, mut reader) = Spill::create(path, reserved).unwrap();
        file.write_all(b"abc").unwrap();
        spill.wrote(3);
        spill.finish(false);
        let mut buf = [0u8; 8];
        assert_eq!(reader.read(&mut buf).unwrap(), 3);
        assert!(reader.read(&mut buf).is_err(), "a short rebuild must not look like the end of the blob");
        drop(reader);
        assert!(!spill.wrote(0), "the writer sees the client has gone");
    }

    #[test]
    fn sweeps_only_stale_spills() {
        let dir = std::env::temp_dir().join(format!("rift-sweep-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let (old, new, other) = (dir.join("rift-read-1-1"), dir.join("rift-read-1-2"), dir.join("unrelated"));
        for p in [&old, &new, &other] {
            std::fs::write(p, b"x").unwrap();
        }
        let hours_ago = std::time::SystemTime::now() - Duration::from_secs(2 * 3600);
        for p in [&old, &other] {
            File::options().write(true).open(p).unwrap().set_modified(hours_ago).unwrap();
        }
        sweep_spills(&dir);
        assert!(!old.exists(), "left by a killed rift");
        assert!(new.exists(), "may still be written");
        assert!(other.exists(), "not ours");
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn drops_stalled_and_oversized_clients() {
        use std::{
            io::{Read, Write},
            net::TcpStream,
            time::Instant,
        };
        let serve = |limits: tiny_http::Limits| {
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            let addr = listener.local_addr().unwrap();
            (Server::from_listener_with_limits(listener, None, limits).unwrap(), addr)
        };
        // how long until the server closes the connection
        let closes_after = |s: &TcpStream| {
            let (t, mut s) = (Instant::now(), s.try_clone().unwrap());
            s.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
            let mut b = [0u8; 512];
            while let Ok(n) = s.read(&mut b) {
                if n == 0 {
                    break;
                }
            }
            t.elapsed()
        };
        let limits = tiny_http::Limits {
            idle_timeout: Some(Duration::from_millis(500)),
            header_timeout: Some(Duration::from_millis(800)),
            max_header_bytes: Some(4096),
            max_headers: Some(20),
            max_connections: Some(3),
        };
        let (server, addr) = serve(limits);
        // a client that connects and sends nothing
        assert!(closes_after(&TcpStream::connect(addr).unwrap()) < Duration::from_secs(3));
        // a client that drips its headers a byte at a time, each well within the idle timeout
        let drip = TcpStream::connect(addr).unwrap();
        let mut writer = drip.try_clone().unwrap();
        thread::spawn(move || {
            for b in b"GET / HTTP/1.1\r\nHost: x\r\nX-Slow: abcdefghijklmnopqrstuvwxyz\r\n\r\n" {
                if writer.write_all(&[*b]).is_err() {
                    return;
                }
                thread::sleep(Duration::from_millis(100));
            }
        });
        assert!(closes_after(&drip) < Duration::from_secs(3), "the header deadline cuts it off before it's done");
        // headers bigger than allowed
        let mut big = TcpStream::connect(addr).unwrap();
        let _ = big.write_all(format!("GET / HTTP/1.1\r\nX-Big: {}\r\n\r\n", "a".repeat(8000)).as_bytes());
        assert!(closes_after(&big) < Duration::from_secs(3));
        assert!(server.try_recv().unwrap().is_none(), "none of those became a request");
        // and an ordinary request still gets through
        let mut ok = TcpStream::connect(addr).unwrap();
        ok.write_all(b"GET /x HTTP/1.1\r\nHost: x\r\n\r\n").unwrap();
        let rq = server.recv_timeout(Duration::from_secs(3)).unwrap().expect("a request");
        assert_eq!(rq.url(), "/x");
        rq.respond(Response::from_string("ok")).unwrap();

        // connections beyond the cap are closed as they arrive; the others stay open
        let (_server, addr) = serve(tiny_http::Limits { idle_timeout: Some(Duration::from_secs(5)), max_connections: Some(2), ..Default::default() });
        let held: Vec<TcpStream> = (0..2).map(|_| TcpStream::connect(addr).unwrap()).collect();
        thread::sleep(Duration::from_millis(200));
        assert!(closes_after(&TcpStream::connect(addr).unwrap()) < Duration::from_secs(1), "over the cap");
        let mut probe = held[0].try_clone().unwrap();
        probe.set_read_timeout(Some(Duration::from_millis(300))).unwrap();
        let mut b = [0u8; 8];
        assert!(probe.read(&mut b).is_err(), "still open: the read times out rather than seeing the close");
    }

    #[test]
    fn serves_https() {
        let rcgen::CertifiedKey { cert, key_pair } = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
        let ssl = |key: Vec<u8>| tiny_http::SslConfig { certificate: cert.pem().into_bytes(), private_key: key };
        assert!(Server::https("127.0.0.1:0", ssl(b"not a key".to_vec())).is_err(), "a bad key is an error, not a panic");
        let server = Server::https("127.0.0.1:0", ssl(key_pair.serialize_pem().into_bytes())).unwrap();
        let addr = server.server_addr().to_ip().unwrap();
        let answer = thread::spawn(move || server.recv().unwrap().respond(Response::from_string("ok")).unwrap());
        let client = reqwest::blocking::Client::builder()
            .add_root_certificate(reqwest::Certificate::from_pem(cert.pem().as_bytes()).unwrap())
            .resolve("localhost", addr)
            .build()
            .unwrap();
        let body = client.get(format!("https://localhost:{}/", addr.port())).send().unwrap().text().unwrap();
        assert_eq!(body, "ok");
        answer.join().unwrap();
    }

    #[test]
    fn passes_on_only_what_registries_ask() {
        let q = |s: &str| parse_query(s);
        let op = |m: &str, k: &str, query: &str| operation(m, k, &q(query), false);
        assert_eq!(op("GET", "", "list-type=2&prefix=docker/&delimiter=/&max-keys=1000"), Some(Op::List));
        assert_eq!(op("GET", "", "prefix=a&marker=b&max-keys=1"), Some(Op::List), "V1, as distribution 2.8 stats");
        assert_eq!(op("GET", "", "uploads&prefix=docker/"), Some(Op::Uploads));
        assert_eq!(op("POST", "", "delete"), Some(Op::DeleteMany));
        assert_eq!(op("GET", "k", ""), Some(Op::Get));
        assert_eq!(op("GET", "k", "response-content-type=text/plain"), Some(Op::Get));
        assert_eq!(op("GET", "k", "uploadId=u&max-parts=100"), Some(Op::Parts));
        assert_eq!(op("PUT", "k", ""), Some(Op::Put));
        assert_eq!(operation("PUT", "k", &[], true), Some(Op::Copy));
        assert_eq!(op("PUT", "k", "partNumber=1&uploadId=u"), Some(Op::Part));
        assert_eq!(op("POST", "k", "uploads"), Some(Op::Create));
        assert_eq!(op("POST", "k", "uploadId=u"), Some(Op::Complete));
        assert_eq!(op("DELETE", "k", ""), Some(Op::Delete));
        assert_eq!(op("DELETE", "k", "uploadId=u"), Some(Op::Abort));
        // what rift refuses: metadata that would drop a compacted blob, bucket configuration, versions
        for (m, k, query) in [
            ("PUT", "k", "tagging"),
            ("DELETE", "k", "tagging"),
            ("PUT", "k", "acl"),
            ("PUT", "k", "retention"),
            ("PUT", "k", "legal-hold"),
            ("DELETE", "k", "versionId=v"),
            ("GET", "k", "versionId=v"),
            ("GET", "k", "partNumber=1"),
            ("GET", "", "versions"),
            ("PUT", "", "policy"),
            ("PUT", "", "lifecycle"),
            ("GET", "", "acl"),
            ("POST", "k", "select&select-type=2"),
            ("POST", "k", "restore"),
        ] {
            assert_eq!(op(m, k, query), None, "{m} {k}?{query}");
        }
    }

    #[test]
    fn keys_rift_refuses() {
        assert!(valid_key("docker/registry/v2/blobs/sha256/ab/abcd/data"));
        assert!(valid_key("a/b/"), "a trailing slash is a key like any other");
        for k in ["/.rift/packs/0", "a//b", "./a", "a/../b", "a/.", ".."] {
            assert!(!valid_key(k), "{k}");
        }
        assert!(internal(".rift/m/x") && internal(".rift") && !internal(".riftx/a") && !internal("a/.rift/b"));
    }

    #[test]
    fn listings_hide_rift_data() {
        let xml = "<ListBucketResult><KeyCount>4</KeyCount>\
            <Contents><Key>docker/a</Key><Size>1</Size></Contents>\
            <Contents><Key>.rift/packs/00000000.zst</Key><Size>9</Size></Contents>\
            <Contents><Key>%2Erift%2Fm%2Fx</Key><Size>9</Size></Contents>\
            <CommonPrefixes><Prefix>.rift/m/</Prefix></CommonPrefixes>\
            <CommonPrefixes><Prefix>docker/</Prefix></CommonPrefixes></ListBucketResult>";
        let out = rewrite_list(xml, &|_| None);
        assert!(!out.contains(".rift") && !out.contains("%2Erift"), "{out}");
        assert!(out.contains("<Key>docker/a</Key>") && out.contains("<Prefix>docker/</Prefix>"));
        assert!(out.contains("<KeyCount>1</KeyCount>"), "{out}");
    }

    #[test]
    fn access_key_ids() {
        let auth = "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20261109/us-east-1/s3/aws4_request, SignedHeaders=host, Signature=abc";
        assert_eq!(access_key_id(Some(auth), "").as_deref(), Some("AKIDEXAMPLE"));
        let presigned = "X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=riftclient%2F20261109%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Signature=abc";
        assert_eq!(access_key_id(None, presigned).as_deref(), Some("riftclient"));
        assert_eq!(access_key_id(None, "list-type=2"), None);
        assert_eq!(access_key_id(Some("Bearer x"), ""), None);
    }

    #[test]
    fn redacts_signatures() {
        assert_eq!(redact("Credential=AK/x, SignedHeaders=host, Signature=abc123"), "Credential=AK/x, SignedHeaders=host, Signature=…");
        assert_eq!(redact("/b/k?X-Amz-Date=1&X-Amz-Signature=deadbeef&x=1"), "/b/k?X-Amz-Date=1&X-Amz-Signature=…&x=1");
    }

    #[test]
    fn ranges() {
        assert_eq!(parse_range("bytes=0-", 10), Some((0, 10)));
        assert_eq!(parse_range("bytes=2-4", 10), Some((2, 5)));
        assert_eq!(parse_range("bytes=-3", 10), Some((7, 10)));
        assert_eq!(parse_range("bytes=5-100", 10), Some((5, 10)));
        assert_eq!(parse_range("bytes=10-", 10), None);
    }

    #[test]
    fn list_rewrite() {
        let xml = "<ListBucketResult><KeyCount>3</KeyCount><Contents><Key>a/data</Key><LastModified>2026-01-01T00:00:00.000Z</LastModified><Size>0</Size></Contents><Contents><Key>.rift/m/x</Key><Size>5</Size></Contents><Contents><Key>b</Key><Size>7</Size></Contents></ListBucketResult>";
        let out = rewrite_list(xml, &|k| (k == "a/data").then_some((1234, 1_369_353_600)));
        assert!(out.contains("<Key>a/data</Key><LastModified>2013-05-24T00:00:00.000Z</LastModified><Size>1234</Size>"));
        assert!(!out.contains(".rift/"));
        assert!(out.contains("<Key>b</Key><Size>7</Size>"));
        assert!(out.contains("<KeyCount>2</KeyCount>"));
    }
}
