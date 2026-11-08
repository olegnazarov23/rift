//! `rift gateway`: the S3 endpoint a registry (or anything else that stores in S3) talks to.
//! Requests pass through to the backing bucket, re-signed. Blobs the compactor has compacted are
//! served by rebuilding them, byte-exact; listings show their original sizes.

use std::{
    collections::HashMap,
    io::{self, Read, Write},
    sync::{Arc, Mutex, mpsc},
    thread,
    time::{Duration, UNIX_EPOCH},
};

use anyhow::{Result, anyhow};
use sha2::{Digest, Sha256};
use tiny_http::{Header, Request, Response, Server, StatusCode};

use crate::{
    cas::{Manifest, Store, blob_digest},
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
}

/// Query parameters that only carry a presigned URL's signature.
const PRESIGN_PARAMS: &[&str] = &[
    "X-Amz-Algorithm", "X-Amz-Credential", "X-Amz-Date", "X-Amz-Expires", "X-Amz-SignedHeaders",
    "X-Amz-Signature", "X-Amz-Security-Token", "x-id",
];

static COPY_ID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Bucket-level GETs that aren't object listings.
const BUCKET_SUBRESOURCES: &[&str] = &[
    "uploads", "versions", "location", "versioning", "acl", "policy", "policyStatus", "lifecycle", "cors",
    "tagging", "website", "notification", "logging", "replication", "encryption", "object-lock", "accelerate",
    "requestPayment", "analytics", "inventory", "metrics", "ownershipControls", "publicAccessBlock",
    "intelligent-tiering",
];

enum After {
    Nothing,
    RewriteList,
    /// these keys were deleted or overwritten if the request succeeds
    Forget(Vec<String>),
}

fn h(name: &str, value: &str) -> Header {
    Header::from_bytes(name.as_bytes(), value.as_bytes()).expect("ASCII header")
}

fn error(req: Request, status: u16, code: &str, message: &str) -> Result<()> {
    let body = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Error><Code>{code}</Code><Message>{}</Message></Error>",
        xml_escape(message)
    );
    req.respond(Response::from_data(body).with_status_code(status).with_header(h("content-type", "application/xml")))?;
    Ok(())
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
        let key = s3::xml_first(block, "Key").map(xml_unescape).unwrap_or_default();
        if key.starts_with(".rift/") {
            hidden += 1;
        } else if let Some((size, mtime)) = compacted(&key) {
            out.push_str(&set(&set(block, "Size", &size.to_string()), "LastModified", &s3::iso(mtime)));
        } else {
            out.push_str(block);
        }
        rest = &rest[end..];
    }
    out.push_str(rest);
    let ours = "<CommonPrefixes><Prefix>.rift/</Prefix></CommonPrefixes>";
    if out.contains(ours) {
        out = out.replacen(ours, "", 1);
        hidden += 1;
    }
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

impl Gateway {
    pub fn serve(self: Arc<Self>, listen: &str) -> Result<()> {
        let server = Server::http(listen).map_err(|e| anyhow!("listening on {listen}: {e}"))?;
        for req in server.incoming_requests() {
            let gw = self.clone();
            thread::spawn(move || gw.handle(req));
        }
        Ok(())
    }

    fn handle(&self, req: Request) {
        let method = req.method().as_str().to_string();
        let url = req.url().to_string();
        if let Err(e) = self.route(req, &method, &url) {
            eprintln!("rift: {method} {url}: {e:#}");
        }
    }

    fn route(&self, req: Request, method: &str, url: &str) -> Result<()> {
        let (raw_path, raw_query) = url.split_once('?').unwrap_or((url, ""));
        let headers: Vec<(String, String)> = req
            .headers()
            .iter()
            .map(|x| (x.field.as_str().as_str().to_ascii_lowercase(), x.value.as_str().to_string()))
            .collect();
        let header = |name: &str| headers.iter().find(|(k, _)| k == name).map(|(_, v)| v.clone());
        let mut query = parse_query(raw_query);
        let presigned = query.iter().any(|(k, _)| k == "X-Amz-Signature");
        if let Some((access, secret)) = &self.client_keys {
            let signed = if presigned {
                matches!(method, "GET" | "HEAD") && sigv4::verify_presigned(access, secret, method, raw_path, raw_query, &header)
            } else {
                header("authorization").is_some_and(|a| sigv4::verify(access, secret, method, raw_path, raw_query, &a, &header))
            };
            if !signed {
                return error(req, 403, "SignatureDoesNotMatch", "signature check failed");
            }
        }
        query.retain(|(k, _)| !PRESIGN_PARAMS.contains(&k.as_str()));
        let path = raw_path.trim_start_matches('/');
        let (bucket, raw_key) = path.split_once('/').unwrap_or((path, ""));
        if bucket != self.bucket {
            return error(req, 404, "NoSuchBucket", bucket);
        }
        let key = uri_decode(raw_key);
        if key.starts_with(".rift/") {
            return error(req, 403, "AccessDenied", "reserved for rift");
        }
        let has = |name: &str| query.iter().any(|(k, _)| k == name);
        match method {
            "GET" | "HEAD" if !key.is_empty() && query.is_empty() => {
                if blob_digest(&key).is_some() {
                    self.reads.lock().unwrap().insert(key.clone(), unix_now());
                }
                match self.store.compacted(&key) {
                    Some(m) if !self.store.is_hot(&key) => self.serve_compacted(req, method, &m, header("range")),
                    // stored as it is: a client following a presigned URL fetches it from the backend directly
                    _ => match &self.redirect {
                        Some(endpoint) if presigned && method == "GET" => self.redirect_to_backend(req, &key, endpoint),
                        _ => self.forward(req, method, &key, &query, &headers, After::Nothing),
                    },
                }
            }
            // ListObjects V1 or V2 (old registries stat files with V1 listings)
            "GET" if key.is_empty() && !query.iter().any(|(k, _)| BUCKET_SUBRESOURCES.contains(&k.as_str())) => {
                self.forward(req, method, &key, &query, &headers, After::RewriteList)
            }
            "PUT" if !key.is_empty() && header("x-amz-copy-source").is_some() => self.copy(req, &key, &query, &headers),
            _ => {
                // requests that create, replace or delete an object end its compacted form
                let changes = !key.is_empty()
                    && match method {
                        "PUT" | "DELETE" => !has("uploadId"),
                        "POST" => has("uploadId"),
                        _ => false,
                    };
                let after = if changes { After::Forget(vec![key.clone()]) } else { After::Nothing };
                self.forward(req, method, &key, &query, &headers, after)
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
        mut after: After,
    ) -> Result<()> {
        let mut body = Vec::new();
        req.as_reader().read_to_end(&mut body)?;
        let chunked = headers.iter().any(|(k, v)| {
            (k == "x-amz-content-sha256" && v.starts_with("STREAMING-")) || (k == "content-encoding" && v.contains("aws-chunked"))
        });
        if chunked {
            body = match decode_aws_chunked(&body) {
                Ok(b) => b,
                Err(e) => return error(req, 400, "IncompleteBody", &format!("{e:#}")),
            };
        }
        if key.is_empty() && query.iter().any(|(k, _)| k == "delete") {
            after = After::Forget(xml_all(&String::from_utf8_lossy(&body), "Key").into_iter().map(xml_unescape).collect());
        }
        let rest = if key.is_empty() { String::new() } else { Backend::key_path(key) };
        let resp = match self.store.backend.send(method, &rest, query, &pass_headers(headers, chunked), body) {
            Ok(r) => r,
            Err(e) => return error(req, 502, "BadGateway", &format!("{e:#}")),
        };
        let status = resp.status().as_u16();
        let back = back_headers(&resp);
        let ok = (200..300).contains(&status);
        match after {
            After::RewriteList if status == 200 => {
                let xml = resp.text()?;
                let body = rewrite_list(&xml, &|k| self.store.compacted(k).map(|m| (m.size, m.mtime)));
                let mut out = Response::from_data(body).with_status_code(200);
                for x in back {
                    out.add_header(x);
                }
                req.respond(out)?;
                return Ok(());
            }
            After::Forget(keys) if ok => keys.iter().for_each(|k| self.store.forget(k)),
            _ => {}
        }
        let len = resp.headers().get("content-length").and_then(|v| v.to_str().ok()?.parse::<usize>().ok());
        req.respond(Response::new(StatusCode(status), back, resp, len, None))?;
        Ok(())
    }

    fn serve_compacted(&self, req: Request, method: &str, m: &Arc<Manifest>, range: Option<String>) -> Result<()> {
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
        let status = StatusCode(if partial { 206 } else { 200 });
        let len = Some((end - start) as usize);
        if method == "HEAD" {
            req.respond(Response::new(status, headers, io::empty(), len, None))?;
            return Ok(());
        }
        let (tx, rx) = mpsc::sync_channel::<Vec<u8>>(8);
        let (store, m, workers) = (self.store.clone(), m.clone(), self.workers);
        // a full read also writes the bytes aside, to put the original back while the blob is in use
        let copy = self.hot_copy && start == 0 && end == m.size && self.store.begin_hot_copy(&m.key);
        thread::spawn(move || {
            let tmp = copy.then(|| {
                let n = COPY_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                std::env::temp_dir().join(format!("rift-copy-{}-{n}", std::process::id()))
            });
            let mut file = tmp.as_ref().and_then(|p| std::fs::File::create(p).ok().map(io::BufWriter::new));
            let (mut sha, mut written) = (Sha256::new(), 0u64);
            let result = store.stream(&m, start, end, workers, &mut |b| {
                if let Some(f) = file.as_mut() {
                    if f.write_all(b).is_ok() {
                        sha.update(b);
                        written += b.len() as u64;
                    } else {
                        file = None;
                    }
                }
                tx.send(b.to_vec()).is_ok()
            });
            if let Err(e) = &result {
                eprintln!("rift: rebuilding {}: {e:#}", m.key);
            }
            if copy {
                let flushed = file.take().is_some_and(|mut f| f.flush().is_ok());
                let whole = result.is_ok() && flushed && written == m.size && hex(&sha.finalize()) == m.sha256;
                match (&tmp, whole) {
                    (Some(p), true) => {
                        if let Err(e) = store.finish_hot_copy(&m.key, p, m.size) {
                            eprintln!("rift: putting {} back after a cold read: {e:#}", m.key);
                        }
                    }
                    _ => store.abandon_hot_copy(&m.key),
                }
                if let Some(p) = &tmp {
                    let _ = std::fs::remove_file(p);
                }
            }
        });
        req.respond(Response::new(status, headers, ChannelReader { rx, buf: Vec::new(), at: 0 }, len, None))?;
        Ok(())
    }

    fn redirect_to_backend(&self, req: Request, key: &str, endpoint: &str) -> Result<()> {
        match self.store.backend.presign_get(key, endpoint, 900) {
            Ok(url) => {
                req.respond(Response::empty(307).with_header(h("location", &url)))?;
                Ok(())
            }
            Err(e) => error(req, 500, "InternalError", &format!("{e:#}")),
        }
    }

    fn copy(&self, req: Request, key: &str, query: &[(String, String)], headers: &[(String, String)]) -> Result<()> {
        let source = headers.iter().find(|(k, _)| k == "x-amz-copy-source").map(|(_, v)| v.clone()).unwrap_or_default();
        let (source, version) = source.split_once('?').map_or((source.as_str(), None), |(s, v)| (s, Some(v)));
        let source = uri_decode(source);
        let (src_bucket, src_key) = source.trim_start_matches('/').split_once('/').unwrap_or(("", ""));
        if src_bucket != self.bucket {
            return error(req, 400, "InvalidRequest", "copies only within the bucket");
        }
        let part = query.iter().any(|(k, _)| k == "partNumber");
        if let Some(m) = self.store.compacted(src_key) {
            if part {
                return error(req, 501, "NotImplemented", "part copy from a compacted blob");
            }
            let mut bytes = Vec::with_capacity(m.size as usize);
            self.store.stream(&m, 0, m.size, self.workers, &mut |b| {
                bytes.extend_from_slice(b);
                true
            })?;
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
            req.respond(Response::from_data(xml).with_header(h("content-type", "application/xml")))?;
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
        self.forward(req, "PUT", key, query, &hs, after)
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

/// Every `interval`: compacts cold blobs, then reclaims chunks of deleted ones (always on the first
/// round, to clear packs an interrupted run left behind).
pub fn compactor(store: Arc<Store>, reads: Arc<Mutex<HashMap<String, u64>>>, s: Schedule) {
    let mut first = true;
    loop {
        thread::sleep(s.interval);
        match store.backend.list("") {
            Ok(objects) => {
                let now = unix_now();
                let (mut cold, mut cooling) = (Vec::new(), Vec::new());
                for o in objects.into_iter().filter(|o| !o.key.starts_with(".rift/")) {
                    let Some(digest) = blob_digest(&o.key) else { continue };
                    let read = reads.lock().unwrap().get(&o.key).copied().unwrap_or(0);
                    let idle = now.saturating_sub(o.modified.max(read)) >= s.cold_after;
                    match store.compacted(&o.key) {
                        // the original is back after a read: stub it again once nobody reads it
                        Some(m) if o.size == m.size && o.size > 0 => {
                            store.note_hot(&o.key, o.modified);
                            if idle {
                                cooling.push((o.key, o.etag));
                            }
                        }
                        Some(_) => {}
                        None if o.size >= s.min_size && idle => cold.push((o.key, digest, o.modified)),
                        None => {}
                    }
                }
                let cooled = cooling.iter().filter(|(k, etag)| store.cool(k, etag).map_err(|e| eprintln!("rift: cooling {k}: {e:#}")).is_ok()).count();
                if cooled > 0 {
                    eprintln!("rift: stubbed {cooled} hot copies again");
                }
                if !cold.is_empty() {
                    eprintln!("rift: compacting {} cold blobs", cold.len());
                    let started = std::time::Instant::now();
                    let (done, bytes) = store.compact(&cold, s.threads);
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
            match store.gc(s.rewrite_below, s.grace) {
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
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
