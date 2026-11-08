//! The backing S3 bucket, plus the pieces of S3's wire format the gateway reads or rewrites.

use anyhow::{Context, Result, bail};
use reqwest::blocking::{Client, Response};

use crate::sigv4::{amz_date, canonical_query, civil, days_from_civil, sign, unix_now, uri_encode};

pub struct Backend {
    client: Client,
    endpoint: String,
    host: String,
    pub bucket: String,
    region: String,
    access_key: String,
    secret_key: String,
}

pub struct Object {
    pub key: String,
    pub size: u64,
    pub modified: u64,
}

impl Backend {
    pub fn new(endpoint: &str, bucket: &str, region: &str, access_key: String, secret_key: String) -> Result<Self> {
        let url = reqwest::Url::parse(endpoint).context("backend endpoint")?;
        let host = url.host_str().context("backend endpoint has no host")?;
        let host = url.port().map_or_else(|| host.to_string(), |p| format!("{host}:{p}"));
        Ok(Self {
            client: Client::builder().timeout(None).build()?,
            endpoint: endpoint.trim_end_matches('/').to_string(),
            host,
            bucket: bucket.to_string(),
            region: region.to_string(),
            access_key,
            secret_key,
        })
    }

    /// Sends a signed request. `rest` is the encoded path after the bucket ("" or "/key");
    /// `headers` are lowercase and any `x-amz-*` among them is signed.
    pub fn send(
        &self,
        method: &str,
        rest: &str,
        query: &[(String, String)],
        headers: &[(String, String)],
        body: Vec<u8>,
    ) -> Result<Response> {
        let path = format!("/{}{rest}", self.bucket);
        let q = canonical_query(query);
        let date = amz_date(unix_now());
        let mut signed: Vec<(&str, &str)> =
            vec![("host", &self.host), ("x-amz-content-sha256", "UNSIGNED-PAYLOAD"), ("x-amz-date", &date)];
        signed.extend(
            headers
                .iter()
                .filter(|(k, _)| k.starts_with("x-amz-") && k != "x-amz-content-sha256" && k != "x-amz-date")
                .map(|(k, v)| (k.as_str(), v.as_str())),
        );
        let (sig, signed_list) =
            sign(&self.secret_key, &self.region, &date, method, &path, &q, &signed, "UNSIGNED-PAYLOAD");
        let auth = format!(
            "AWS4-HMAC-SHA256 Credential={}/{}/{}/s3/aws4_request, SignedHeaders={signed_list}, Signature={sig}",
            self.access_key,
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

    /// Returns the new ETag.
    pub fn put(&self, key: &str, body: Vec<u8>, headers: &[(String, String)]) -> Result<String> {
        let resp = Self::ok(self.send("PUT", &Self::key_path(key), &[], headers, body)?, key)?;
        Ok(header(&resp, "etag"))
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

pub fn xml_unescape(s: &str) -> String {
    s.replace("&lt;", "<").replace("&gt;", ">").replace("&quot;", "\"").replace("&apos;", "'").replace("&amp;", "&")
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
/// zero-size chunk and optional trailers). Chunk signatures aren't checked.
pub fn decode_aws_chunked(body: &[u8]) -> Result<Vec<u8>> {
    let mut out = Vec::with_capacity(body.len());
    let mut i = 0;
    loop {
        let line_end = body[i..].windows(2).position(|w| w == b"\r\n").context("aws-chunked: no chunk header")? + i;
        let header = std::str::from_utf8(&body[i..line_end])?;
        let size = usize::from_str_radix(header.split(';').next().unwrap_or("").trim(), 16)
            .context("aws-chunked: bad chunk size")?;
        i = line_end + 2;
        if size == 0 {
            return Ok(out);
        }
        let data = body.get(i..i + size).context("aws-chunked: chunk runs past the body")?;
        out.extend_from_slice(data);
        i += size + 2;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chunked() {
        let body = b"5;chunk-signature=ab\r\nhello\r\n6;chunk-signature=cd\r\n world\r\n0;chunk-signature=ef\r\n\r\n";
        assert_eq!(decode_aws_chunked(body).unwrap(), b"hello world");
        assert_eq!(decode_aws_chunked(b"3\r\nabc\r\n0\r\nx-amz-checksum-crc32:AAAA\r\n\r\n").unwrap(), b"abc");
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
    }
}
