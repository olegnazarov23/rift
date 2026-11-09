//! The backing S3 bucket, plus the pieces of S3's wire format the gateway reads or rewrites.

use anyhow::{Context, Result, bail, ensure};
use reqwest::blocking::{Client, Response};

use crate::{
    creds::Credentials,
    sigv4::{Seed, amz_date, canonical_query, chunk_signature, civil, days_from_civil, sign, unix_now, uri_encode},
};

pub struct Backend {
    client: Client,
    endpoint: String,
    host: String,
    pub bucket: String,
    region: String,
    creds: Credentials,
}

pub struct Object {
    pub key: String,
    pub size: u64,
    pub modified: u64,
    pub etag: String,
}

/// What a HEAD says about an object, rift's stub metadata included.
pub struct Head {
    pub size: u64,
    pub etag: String,
    /// on a stub: the compacted blob's size and date
    pub rift_size: Option<u64>,
    pub rift_mtime: Option<u64>,
}

impl Head {
    fn from(resp: &Response) -> Self {
        let num = |name: &str| resp.headers().get(name).and_then(|v| v.to_str().ok()?.parse().ok());
        Self {
            size: num("content-length").unwrap_or(0),
            etag: header(resp, "etag"),
            rift_size: num("x-amz-meta-rift-size"),
            rift_mtime: num("x-amz-meta-rift-mtime"),
        }
    }

    /// The empty object rift leaves in a compacted blob's place.
    pub fn is_stub(&self) -> bool {
        self.size == 0 && self.rift_size.is_some_and(|n| n > 0)
    }
}

/// Whether a response is a stub, read from its headers.
pub fn stub_of(resp: &Response) -> Option<Head> {
    let head = Head::from(resp);
    (resp.status().is_success() && head.is_stub()).then_some(head)
}

impl Backend {
    pub fn new(endpoint: &str, bucket: &str, region: &str, creds: Credentials) -> Result<Self> {
        let url = reqwest::Url::parse(endpoint).context("backend endpoint")?;
        let host = url.host_str().context("backend endpoint has no host")?;
        let host = url.port().map_or_else(|| host.to_string(), |p| format!("{host}:{p}"));
        Ok(Self {
            // S3 closes connections idle for about 20 seconds; reusing one it just closed fails
            client: Client::builder().timeout(None).pool_idle_timeout(std::time::Duration::from_secs(15)).build()?,
            endpoint: endpoint.trim_end_matches('/').to_string(),
            host,
            bucket: bucket.to_string(),
            region: region.to_string(),
            creds,
        })
    }

    /// Where the bucket keys come from, for the log.
    pub fn keys_from(&self) -> &str {
        self.creds.describe()
    }

    /// Sends a signed request. `rest` is the encoded path after the bucket ("" or "/key");
    /// `headers` are lowercase, and all of them are signed: S3 refuses unsigned `x-amz-*` and
    /// `content-md5` headers (MinIO doesn't check).
    pub fn send(
        &self,
        method: &str,
        rest: &str,
        query: &[(String, String)],
        headers: &[(String, String)],
        body: impl Into<reqwest::blocking::Body>,
    ) -> Result<Response> {
        let body = body.into();
        // a GET or HEAD carries nothing that could be lost: if the connection dies under it (the
        // server closed it as idle just then), it's sent once more
        if matches!(method, "GET" | "HEAD") && body.as_bytes().is_some_and(|b| b.is_empty()) {
            match self.send_once(method, rest, query, headers, Vec::new().into()) {
                Err(e) if e.downcast_ref::<reqwest::Error>().is_some_and(|e| e.is_request() || e.is_connect()) => {}
                other => return other,
            }
        }
        self.send_once(method, rest, query, headers, body)
    }

    fn send_once(
        &self,
        method: &str,
        rest: &str,
        query: &[(String, String)],
        headers: &[(String, String)],
        body: reqwest::blocking::Body,
    ) -> Result<Response> {
        let path = format!("/{}{rest}", self.bucket);
        let q = canonical_query(query);
        let date = amz_date(unix_now());
        let keys = self.creds.keys()?;
        let mut signed: Vec<(&str, &str)> =
            vec![("host", &self.host), ("x-amz-content-sha256", "UNSIGNED-PAYLOAD"), ("x-amz-date", &date)];
        if let Some(token) = &keys.token {
            signed.push(("x-amz-security-token", token));
        }
        signed.extend(
            headers
                .iter()
                .filter(|(k, _)| !matches!(k.as_str(), "host" | "x-amz-content-sha256" | "x-amz-date" | "x-amz-security-token"))
                .map(|(k, v)| (k.as_str(), v.as_str())),
        );
        let (sig, signed_list) = sign(&keys.secret, &self.region, &date, method, &path, &q, &signed, "UNSIGNED-PAYLOAD");
        let auth = format!(
            "AWS4-HMAC-SHA256 Credential={}/{}/{}/s3/aws4_request, SignedHeaders={signed_list}, Signature={sig}",
            keys.access,
            &date[..8],
            self.region
        );
        let url = if q.is_empty() { format!("{}{path}", self.endpoint) } else { format!("{}{path}?{q}", self.endpoint) };
        let mut req = self
            .client
            .request(reqwest::Method::from_bytes(method.as_bytes())?, url)
            .header("authorization", auth)
            .header("x-amz-date", &date)
            .header("x-amz-content-sha256", "UNSIGNED-PAYLOAD");
        if let Some(token) = &keys.token {
            req = req.header("x-amz-security-token", token);
        }
        for (k, v) in headers {
            req = req.header(k.as_str(), v.as_str());
        }
        req.body(body).send().with_context(|| format!("{method} {path}"))
    }

    pub fn key_path(key: &str) -> String {
        format!("/{}", uri_encode(key, true))
    }

    fn ok(resp: Response, what: &str) -> Result<Response> {
        if !resp.status().is_success() {
            let status = resp.status();
            bail!("{what}: {status} {}", resp.text().unwrap_or_default());
        }
        Ok(resp)
    }

    /// Body and ETag.
    pub fn get(&self, key: &str) -> Result<(Vec<u8>, String)> {
        let resp = Self::ok(self.send("GET", &Self::key_path(key), &[], &[], Vec::new())?, key)?;
        let etag = header(&resp, "etag");
        Ok((resp.bytes()?.to_vec(), etag))
    }

    /// Bytes `start..end` (fewer if the object is shorter).
    pub fn get_range(&self, key: &str, start: u64, end: u64) -> Result<Vec<u8>> {
        let range = format!("bytes={start}-{}", end.saturating_sub(1).max(start));
        let resp = Self::ok(self.send("GET", &Self::key_path(key), &[], &[("range".into(), range)], Vec::new())?, key)?;
        Ok(resp.bytes()?.to_vec())
    }

    /// Returns the new ETag.
    pub fn put(&self, key: &str, body: Vec<u8>, headers: &[(String, String)]) -> Result<String> {
        let resp = Self::ok(self.send("PUT", &Self::key_path(key), &[], headers, body)?, key)?;
        Ok(header(&resp, "etag"))
    }

    /// Streams a file into `key`; returns the new ETag.
    pub fn put_file(&self, key: &str, file: std::fs::File, len: u64, headers: &[(String, String)]) -> Result<String> {
        let body = reqwest::blocking::Body::sized(file, len);
        let resp = Self::ok(self.send("PUT", &Self::key_path(key), &[], headers, body)?, key)?;
        Ok(header(&resp, "etag"))
    }

    /// Size and ETag, or None if there's no such object.
    pub fn head(&self, key: &str) -> Result<Option<(u64, String)>> {
        Ok(self.head_object(key)?.map(|h| (h.size, h.etag)))
    }

    /// The object's size, ETag and stub metadata, or None if there's no such object.
    pub fn head_object(&self, key: &str) -> Result<Option<Head>> {
        let resp = self.send("HEAD", &Self::key_path(key), &[], &[], Vec::new())?;
        if resp.status().as_u16() == 404 {
            return Ok(None);
        }
        let resp = Self::ok(resp, key)?;
        Ok(Some(Head::from(&resp)))
    }

    /// Body and ETag, or None if there's no such object.
    pub fn get_opt(&self, key: &str) -> Result<Option<(Vec<u8>, String)>> {
        let resp = self.send("GET", &Self::key_path(key), &[], &[], Vec::new())?;
        if resp.status().as_u16() == 404 {
            return Ok(None);
        }
        let resp = Self::ok(resp, key)?;
        let etag = header(&resp, "etag");
        Ok(Some((resp.bytes()?.to_vec(), etag)))
    }

    /// A conditional PUT (`If-None-Match: *` or `If-Match: <etag>` among `headers`): the new ETag,
    /// or None when the condition failed (the object exists, changed, or is gone).
    pub fn put_if(&self, key: &str, body: Vec<u8>, headers: &[(String, String)]) -> Result<Option<String>> {
        Self::condition(self.send("PUT", &Self::key_path(key), &[], headers, body)?, key, headers)
    }

    /// `put_if` for a file.
    pub fn put_file_if(&self, key: &str, file: std::fs::File, len: u64, headers: &[(String, String)]) -> Result<Option<String>> {
        let body = reqwest::blocking::Body::sized(file, len);
        Self::condition(self.send("PUT", &Self::key_path(key), &[], headers, body)?, key, headers)
    }

    fn condition(resp: Response, key: &str, headers: &[(String, String)]) -> Result<Option<String>> {
        // 412: the condition failed; 409: another conditional write raced this one; 404: If-Match
        // on an object that isn't there
        let if_match = headers.iter().any(|(k, _)| k == "if-match");
        match resp.status().as_u16() {
            412 | 409 => Ok(None),
            404 if if_match => Ok(None),
            _ => Ok(Some(header(&Self::ok(resp, key)?, "etag"))),
        }
    }

    /// A presigned GET of `key`, addressed to `endpoint` (the backend as clients reach it).
    pub fn presign_get(&self, key: &str, endpoint: &str, expires: u64) -> Result<String> {
        let url = reqwest::Url::parse(endpoint).context("redirect endpoint")?;
        let host = url.host_str().context("redirect endpoint has no host")?;
        let host = url.port().map_or_else(|| host.to_string(), |p| format!("{host}:{p}"));
        let path = format!("/{}{}", self.bucket, Self::key_path(key));
        let date = amz_date(unix_now());
        let keys = self.creds.keys()?;
        let mut params = vec![
            ("X-Amz-Algorithm".to_string(), "AWS4-HMAC-SHA256".to_string()),
            ("X-Amz-Credential".to_string(), format!("{}/{}/{}/s3/aws4_request", keys.access, &date[..8], self.region)),
            ("X-Amz-Date".to_string(), date.clone()),
            ("X-Amz-Expires".to_string(), expires.to_string()),
            ("X-Amz-SignedHeaders".to_string(), "host".to_string()),
        ];
        if let Some(token) = &keys.token {
            params.push(("X-Amz-Security-Token".to_string(), token.clone()));
        }
        let q = canonical_query(&params);
        let (sig, _) = sign(&keys.secret, &self.region, &date, "GET", &path, &q, &[("host", &host)], "UNSIGNED-PAYLOAD");
        Ok(format!("{}{path}?{q}&X-Amz-Signature={sig}", endpoint.trim_end_matches('/')))
    }

    pub fn delete(&self, key: &str) -> Result<()> {
        Self::ok(self.send("DELETE", &Self::key_path(key), &[], &[], Vec::new())?, key)?;
        Ok(())
    }

    /// Bucket settings that cost rift its savings or put its data at risk, as log warnings.
    pub fn settings_warnings(&self) -> Vec<String> {
        let read = |what: &str| -> std::result::Result<Option<String>, String> {
            match self.send("GET", "", &[(what.to_string(), String::new())], &[], Vec::new()) {
                Ok(r) if r.status().is_success() => Ok(Some(r.text().unwrap_or_default())),
                Ok(r) if r.status().as_u16() == 404 => Ok(None),
                Ok(r) => Err(r.status().as_u16().to_string()),
                Err(e) => Err(format!("{e:#}")),
            }
        };
        let (mut out, mut unread) = (Vec::new(), Vec::new());
        let lifecycle = read("lifecycle").unwrap_or_else(|e| {
            unread.push(("lifecycle rules", "s3:GetLifecycleConfiguration", e));
            None
        });
        if let Some(l) = &lifecycle {
            out.extend(lifecycle_warnings(l));
        }
        match read("versioning") {
            Ok(Some(v)) if xml_first(&v, "Status") == Some("Enabled") && !lifecycle.as_deref().is_some_and(expires_noncurrent) => out.push(
                "versioning is on, so the originals rift replaces with stubs and the packs it deletes stay as noncurrent versions, and nothing is saved; add a lifecycle rule that expires noncurrent versions".into(),
            ),
            Ok(_) => {}
            Err(e) => unread.push(("versioning", "s3:GetBucketVersioning", e)),
        }
        match read("object-lock") {
            Ok(Some(o)) if xml_first(&o, "ObjectLockEnabled") == Some("Enabled") => out.push(
                "Object Lock is on: originals rift replaces and packs it deletes are kept until their retention ends, and only then is anything saved".into(),
            ),
            Ok(_) => {}
            Err(e) => unread.push(("Object Lock setting", "s3:GetBucketObjectLockConfiguration", e)),
        }
        for (what, permission, e) in unread {
            out.push(format!("couldn't read the bucket's {what} ({e}); give rift {permission} to have it checked"));
        }
        out
    }

    /// Every object under `prefix`.
    pub fn list(&self, prefix: &str) -> Result<Vec<Object>> {
        let mut out = Vec::new();
        let mut token: Option<String> = None;
        loop {
            let mut q = vec![("list-type".to_string(), "2".to_string()), ("prefix".to_string(), prefix.to_string())];
            if let Some(t) = &token {
                q.push(("continuation-token".into(), t.clone()));
            }
            let xml = Self::ok(self.send("GET", "", &q, &[], Vec::new())?, "list")?.text()?;
            for c in xml_all(&xml, "Contents") {
                out.push(Object {
                    key: xml_unescape(xml_first(c, "Key").unwrap_or_default()),
                    size: xml_first(c, "Size").and_then(|s| s.parse().ok()).unwrap_or(0),
                    modified: xml_first(c, "LastModified").and_then(parse_iso).unwrap_or(0),
                    etag: xml_unescape(xml_first(c, "ETag").unwrap_or_default()),
                });
            }
            token = (xml_first(&xml, "IsTruncated") == Some("true"))
                .then(|| xml_first(&xml, "NextContinuationToken").map(xml_unescape))
                .flatten();
            if token.is_none() {
                return Ok(out);
            }
        }
    }
}

fn header(resp: &Response, name: &str) -> String {
    resp.headers().get(name).and_then(|v| v.to_str().ok()).unwrap_or_default().to_string()
}

/// Inner text of each `<tag>…</tag>`, in order (no nesting of the same tag).
/// Enabled lifecycle rules that would delete or archive rift's data under `.rift/`. Rules with a
/// tag filter are skipped: rift's objects carry no tags.
pub fn lifecycle_warnings(xml: &str) -> Vec<String> {
    let mut out = Vec::new();
    for rule in xml_all(xml, "Rule") {
        if xml_first(rule, "Status") != Some("Enabled") || rule.contains("<Tag>") {
            continue;
        }
        let prefix = xml_first(rule, "Prefix").map(xml_unescape).unwrap_or_default();
        if !(".rift/".starts_with(&prefix) || prefix.starts_with(".rift/")) {
            continue;
        }
        let id = xml_first(rule, "ID").map_or_else(|| "without an ID".to_string(), |i| format!("\"{}\"", xml_unescape(i)));
        if xml_first(rule, "Expiration").is_some_and(|e| e.contains("<Days>") || e.contains("<Date>")) {
            out.push(format!(
                "lifecycle rule {id} deletes objects under .rift/, rift's packs and manifests, and the compacted blobs with them; limit it to your registry's prefix, such as docker/"
            ));
        }
        for t in xml_all(rule, "Transition") {
            if let Some(class @ ("GLACIER" | "DEEP_ARCHIVE")) = xml_first(t, "StorageClass") {
                out.push(format!(
                    "lifecycle rule {id} moves objects under .rift/ to {class}, where compacted blobs can't be read until restored; limit it to your registry's prefix, such as docker/"
                ));
            }
        }
    }
    out
}

/// Whether an enabled lifecycle rule expires noncurrent versions.
pub fn expires_noncurrent(xml: &str) -> bool {
    xml_all(xml, "Rule")
        .into_iter()
        .any(|r| xml_first(r, "Status") == Some("Enabled") && !r.contains("<Tag>") && r.contains("<NoncurrentVersionExpiration>"))
}

pub fn xml_all<'a>(xml: &'a str, tag: &str) -> Vec<&'a str> {
    let (open, close) = (format!("<{tag}>"), format!("</{tag}>"));
    let mut out = Vec::new();
    let mut rest = xml;
    while let Some(i) = rest.find(&open) {
        let body = &rest[i + open.len()..];
        let Some(j) = body.find(&close) else { break };
        out.push(&body[..j]);
        rest = &body[j + close.len()..];
    }
    out
}

pub fn xml_first<'a>(xml: &'a str, tag: &str) -> Option<&'a str> {
    xml_all(xml, tag).into_iter().next()
}

/// Undoes XML escaping: the five named entities and numeric ones (MinIO writes quotes as `&#34;`).
pub fn xml_unescape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut rest = s;
    while let Some(i) = rest.find('&') {
        out.push_str(&rest[..i]);
        let tail = &rest[i..];
        let Some(end) = tail.find(';') else {
            out.push_str(tail);
            return out;
        };
        let entity = &tail[1..end];
        let decoded = match entity {
            "lt" => Some('<'),
            "gt" => Some('>'),
            "quot" => Some('"'),
            "apos" => Some('\''),
            "amp" => Some('&'),
            _ => entity
                .strip_prefix("#x")
                .and_then(|h| u32::from_str_radix(h, 16).ok())
                .or_else(|| entity.strip_prefix('#').and_then(|d| d.parse().ok()))
                .and_then(char::from_u32),
        };
        match decoded {
            Some(c) => {
                out.push(c);
                rest = &tail[end + 1..];
            }
            None => {
                out.push('&');
                rest = &tail[1..];
            }
        }
    }
    out.push_str(rest);
    out
}

pub fn xml_escape(s: &str) -> String {
    s.replace('&', "&amp;").replace('<', "&lt;").replace('>', "&gt;").replace('"', "&quot;").replace('\'', "&apos;")
}

/// "2026-11-07T12:34:56.000Z" to unix seconds.
pub fn parse_iso(s: &str) -> Option<u64> {
    let n = |a: usize, b: usize| s.get(a..b)?.parse::<u32>().ok();
    let days = days_from_civil(n(0, 4)? as i64, n(5, 7)?, n(8, 10)?);
    Some(days as u64 * 86_400 + u64::from(n(11, 13)? * 3600 + n(14, 16)? * 60 + n(17, 19)?))
}

pub fn iso(unix: u64) -> String {
    let (y, m, d) = civil((unix / 86_400) as i64);
    let s = unix % 86_400;
    format!("{y:04}-{m:02}-{d:02}T{:02}:{:02}:{:02}.000Z", s / 3600, s / 60 % 60, s % 60)
}

/// Strips aws-chunked framing ("<hex size>[;chunk-signature=…]\r\n<data>\r\n", ending with a
/// zero-size chunk and optional trailers). With `seed` (a signed streaming upload) every chunk's
/// signature must chain from it. Returns the data and the trailing headers, e.g. checksums;
/// a trailer's own signature isn't checked, but checksums go on to the backend, which checks them.
pub fn decode_aws_chunked(body: &[u8], seed: Option<&Seed>) -> Result<(Vec<u8>, Vec<(String, String)>)> {
    let line = |at: usize| -> Result<(&str, usize)> {
        let end = body[at..].windows(2).position(|w| w == b"\r\n").context("aws-chunked: no line end")? + at;
        Ok((std::str::from_utf8(&body[at..end])?, end + 2))
    };
    let (mut out, mut i) = (Vec::with_capacity(body.len()), 0);
    let mut previous = seed.map(|s| s.signature.clone());
    loop {
        let (header, next) = line(i)?;
        let (size, signature) = header.split_once(";chunk-signature=").map_or((header, None), |(n, s)| (n, Some(s)));
        let size = usize::from_str_radix(size.trim(), 16).context("aws-chunked: bad chunk size")?;
        let data = body.get(next..next + size).context("aws-chunked: chunk runs past the body")?;
        if let (Some(seed), Some(prev)) = (seed, previous.as_mut()) {
            let expected = chunk_signature(seed, prev, data);
            ensure!(signature == Some(expected.as_str()), "aws-chunked: chunk signature doesn't match");
            *prev = expected;
        }
        out.extend_from_slice(data);
        if size == 0 {
            i = next;
            break;
        }
        i = next + size + 2;
    }
    let mut trailers = Vec::new();
    while i < body.len() {
        let (l, next) = line(i)?;
        i = next;
        if l.is_empty() {
            break;
        }
        if let Some((k, v)) = l.split_once(':') {
            let k = k.trim().to_ascii_lowercase();
            if k != "x-amz-trailer-signature" {
                trailers.push((k, v.trim().to_string()));
            }
        }
    }
    Ok((out, trailers))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stubs_by_their_metadata() {
        let h = |size, rift_size| Head { size, etag: String::new(), rift_size, rift_mtime: None };
        assert!(h(0, Some(1234)).is_stub());
        assert!(!h(0, None).is_stub(), "an empty object of the registry's own");
        assert!(!h(1234, Some(1234)).is_stub(), "the original, back as a hot copy");
        assert!(!h(0, Some(0)).is_stub());
    }

    #[test]
    fn lifecycle_rules_that_touch_rift() {
        let rule = |id: &str, filter: &str, action: &str, status: &str| {
            format!("<Rule><ID>{id}</ID><Filter>{filter}</Filter><Status>{status}</Status>{action}</Rule>")
        };
        let expire = "<Expiration><Days>30</Days></Expiration>";
        let glacier = "<Transition><Days>30</Days><StorageClass>GLACIER</StorageClass></Transition>";
        let xml = [
            rule("all", "", expire, "Enabled"),
            rule("registry", "<Prefix>docker/</Prefix>", expire, "Enabled"),
            rule("packs", "<Prefix>.rift/packs/</Prefix>", glacier, "Enabled"),
            rule("ir", "", "<Transition><Days>1</Days><StorageClass>GLACIER_IR</StorageClass></Transition>", "Enabled"),
            rule("tagged", "<Tag><Key>k</Key><Value>v</Value></Tag>", expire, "Enabled"),
            rule("off", "", expire, "Disabled"),
            rule("markers", "", "<Expiration><ExpiredObjectDeleteMarker>true</ExpiredObjectDeleteMarker></Expiration>", "Enabled"),
            rule("old", "", "<NoncurrentVersionExpiration><NoncurrentDays>7</NoncurrentDays></NoncurrentVersionExpiration>", "Enabled"),
        ]
        .concat();
        let w = lifecycle_warnings(&format!("<LifecycleConfiguration>{xml}</LifecycleConfiguration>"));
        assert_eq!(w.len(), 2, "{w:#?}");
        assert!(w[0].contains("\"all\" deletes") && w[1].contains("\"packs\" moves") && w[1].contains("GLACIER"));
        assert!(expires_noncurrent(&xml));
        assert!(!expires_noncurrent(&rule("a", "", expire, "Enabled")));
        // the old form, a prefix outside any filter
        assert_eq!(lifecycle_warnings("<Rule><Prefix></Prefix><Status>Enabled</Status><Expiration><Days>1</Days></Expiration></Rule>").len(), 1);
    }

    #[test]
    fn signs_every_header_it_sends() {
        // S3 refuses a request that carries an unsigned content-md5; MinIO doesn't care
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let server = std::thread::spawn(move || {
            let (mut s, _) = listener.accept().unwrap();
            let mut buf = vec![0u8; 8192];
            let n = std::io::Read::read(&mut s, &mut buf).unwrap();
            std::io::Write::write_all(&mut s, b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").unwrap();
            String::from_utf8_lossy(&buf[..n]).to_ascii_lowercase()
        });
        let backend = Backend::new(&endpoint, "b", "us-east-1", Credentials::fixed("AK", "SK")).unwrap();
        let headers = [("content-md5", "XUFAKrxLKna5cZ2REBfFkg=="), ("content-type", "application/octet-stream"), ("x-amz-acl", "private")]
            .map(|(k, v)| (k.to_string(), v.to_string()));
        backend.send("PUT", "/k", &[], &headers, b"hello".to_vec()).unwrap();
        let request = server.join().unwrap();
        let signed = request.split("signedheaders=").nth(1).unwrap().split(',').next().unwrap();
        assert_eq!(signed, "content-md5;content-type;host;x-amz-acl;x-amz-content-sha256;x-amz-date");
    }

    #[test]
    fn chunked() {
        let body = b"5;chunk-signature=ab\r\nhello\r\n6;chunk-signature=cd\r\n world\r\n0;chunk-signature=ef\r\n\r\n";
        assert_eq!(decode_aws_chunked(body, None).unwrap().0, b"hello world");
        let (data, trailers) = decode_aws_chunked(b"3\r\nabc\r\n0\r\nx-amz-checksum-crc32:AAAA\r\n\r\n", None).unwrap();
        assert_eq!((data.as_slice(), trailers), (&b"abc"[..], vec![("x-amz-checksum-crc32".to_string(), "AAAA".to_string())]));
    }

    // the S3 documentation's chunked-upload example, with signatures checked
    #[test]
    fn chunked_signatures() {
        let seed = Seed {
            signature: "4f232c4386841ef735655705268965c44a0e4690baa4adea153f7db9fa80a0a9".into(),
            amz_date: "20130524T000000Z".into(),
            scope: "20130524/us-east-1/s3/aws4_request".into(),
            key: crate::sigv4::signing_key("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY", "20130524", "us-east-1"),
        };
        let mut body = b"10000;chunk-signature=ad80c730a21e5b8d04586a2213dd63b9a0e99e0e2307b0ade35a65485a288648\r\n".to_vec();
        body.extend([b'a'; 65536]);
        body.extend(b"\r\n400;chunk-signature=0055627c9e194cb4542bae2aa5492e3c1575bbb81b612b7d234b86a503ef5497\r\n");
        body.extend([b'a'; 1024]);
        body.extend(b"\r\n0;chunk-signature=b6c6ea8a5354eaf15b3cb7646744f4275b71ea724fed81ceb9323e279d449df9\r\n\r\n");
        assert_eq!(body.len(), 66824);
        assert_eq!(decode_aws_chunked(&body, Some(&seed)).unwrap().0, vec![b'a'; 66560]);
        let mut tampered = body.clone();
        tampered[200] = b'b';
        assert!(decode_aws_chunked(&tampered, Some(&seed)).is_err(), "a changed byte breaks the chain");
    }

    #[test]
    fn times() {
        assert_eq!(parse_iso("2013-05-24T00:00:00.000Z"), Some(1_369_353_600));
        assert_eq!(iso(1_369_353_600), "2013-05-24T00:00:00.000Z");
    }

    #[test]
    fn xml() {
        let x = "<a><Key>x&amp;y</Key></a><a><Key>z</Key></a>";
        assert_eq!(xml_all(x, "Key"), ["x&amp;y", "z"]);
        assert_eq!(xml_unescape(xml_first(x, "Key").unwrap()), "x&y");
        assert_eq!(xml_unescape("&#34;ce1a&#34; &#x27;a&#x27; &amp;lt; & x"), "\"ce1a\" 'a' &lt; & x");
    }
}
