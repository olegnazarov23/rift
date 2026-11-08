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

impl Backend {
    pub fn new(endpoint: &str, bucket: &str, region: &str, creds: Credentials) -> Result<Self> {
        let url = reqwest::Url::parse(endpoint).context("backend endpoint")?;
        let host = url.host_str().context("backend endpoint has no host")?;
        let host = url.port().map_or_else(|| host.to_string(), |p| format!("{host}:{p}"));
        Ok(Self {
            client: Client::builder().timeout(None).build()?,
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
    /// `headers` are lowercase and any `x-amz-*` among them is signed.
    pub fn send(
        &self,
        method: &str,
        rest: &str,
        query: &[(String, String)],
        headers: &[(String, String)],
        body: impl Into<reqwest::blocking::Body>,
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
                .filter(|(k, _)| k.starts_with("x-amz-") && k != "x-amz-content-sha256" && k != "x-amz-date")
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
        let resp = self.send("HEAD", &Self::key_path(key), &[], &[], Vec::new())?;
        if resp.status().as_u16() == 404 {
            return Ok(None);
        }
        let resp = Self::ok(resp, key)?;
        let size = header(&resp, "content-length").parse().unwrap_or(0);
        Ok(Some((size, header(&resp, "etag"))))
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
