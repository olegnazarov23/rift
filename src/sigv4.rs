//! AWS Signature Version 4 as S3 uses it: signing requests to the backing bucket and checking the
//! ones clients send to the gateway.

use std::time::{SystemTime, UNIX_EPOCH};

use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};

pub fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

pub fn sha256_hex(data: &[u8]) -> String {
    hex(&Sha256::digest(data))
}

fn hmac(key: &[u8], data: &[u8]) -> Vec<u8> {
    let mut m = Hmac::<Sha256>::new_from_slice(key).expect("HMAC takes any key length");
    m.update(data);
    m.finalize().into_bytes().to_vec()
}

/// Percent-encoding as SigV4 wants it; `/` is kept when `keep_slash`.
pub fn uri_encode(s: &str, keep_slash: bool) -> String {
    let mut out = String::with_capacity(s.len());
    for &b in s.as_bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => out.push(b as char),
            b'/' if keep_slash => out.push('/'),
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

pub fn uri_decode(s: &str) -> String {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        let byte = (b[i] == b'%' && i + 2 < b.len())
            .then(|| std::str::from_utf8(&b[i + 1..i + 3]).ok().and_then(|h| u8::from_str_radix(h, 16).ok()))
            .flatten();
        match byte {
            Some(v) => {
                out.push(v);
                i += 3;
            }
            None => {
                out.push(b[i]);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// Query parameters from a raw query string, decoded; a bare name has an empty value.
pub fn parse_query(q: &str) -> Vec<(String, String)> {
    q.split('&')
        .filter(|p| !p.is_empty())
        .map(|p| match p.split_once('=') {
            Some((k, v)) => (uri_decode(k), uri_decode(v)),
            None => (uri_decode(p), String::new()),
        })
        .collect()
}

pub fn canonical_query(params: &[(String, String)]) -> String {
    let mut enc: Vec<(String, String)> =
        params.iter().map(|(k, v)| (uri_encode(k, false), uri_encode(v, false))).collect();
    enc.sort();
    enc.iter().map(|(k, v)| format!("{k}={v}")).collect::<Vec<_>>().join("&")
}

/// Days since 1970-01-01 to (year, month, day), proleptic Gregorian.
pub fn civil(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (yoe + era * 400 + i64::from(m <= 2), m, d)
}

/// (year, month, day) to days since 1970-01-01.
pub fn days_from_civil(y: i64, m: u32, d: u32) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y.rem_euclid(400);
    let mp = if m > 2 { m - 3 } else { m + 9 } as i64;
    let doy = (153 * mp + 2) / 5 + d as i64 - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

pub fn unix_now() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |d| d.as_secs())
}

/// "20261107T120000Z"
pub fn amz_date(unix: u64) -> String {
    let (y, m, d) = civil((unix / 86_400) as i64);
    let s = unix % 86_400;
    format!("{y:04}{m:02}{d:02}T{:02}{:02}{:02}Z", s / 3600, s / 60 % 60, s % 60)
}

/// The signature for a request and its signed-header list. `path` is the URI path exactly as sent
/// and `query` is canonical; every header passed in is signed.
#[allow(clippy::too_many_arguments)]
pub fn sign(
    secret: &str,
    region: &str,
    amz_date: &str,
    method: &str,
    path: &str,
    query: &str,
    headers: &[(&str, &str)],
    payload_hash: &str,
) -> (String, String) {
    let mut hs: Vec<(String, String)> = headers
        .iter()
        .map(|(k, v)| (k.to_ascii_lowercase(), v.split_whitespace().collect::<Vec<_>>().join(" ")))
        .collect();
    hs.sort();
    let canonical_headers: String = hs.iter().map(|(k, v)| format!("{k}:{v}\n")).collect();
    let signed = hs.iter().map(|(k, _)| k.as_str()).collect::<Vec<_>>().join(";");
    let request = format!("{method}\n{path}\n{query}\n{canonical_headers}\n{signed}\n{payload_hash}");
    let date = &amz_date[..8];
    let scope = format!("{date}/{region}/s3/aws4_request");
    let to_sign = format!("AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{}", sha256_hex(request.as_bytes()));
    (hex(&hmac(&signing_key(secret, date, region), to_sign.as_bytes())), signed)
}

/// The key a day's requests in a region are signed with.
pub fn signing_key(secret: &str, date: &str, region: &str) -> Vec<u8> {
    [date, region, "s3", "aws4_request"].iter().fold(format!("AWS4{secret}").into_bytes(), |k, part| hmac(&k, part.as_bytes()))
}

/// What a verified request's streamed chunks are signed against.
pub struct Seed {
    pub signature: String,
    pub amz_date: String,
    pub scope: String,
    pub key: Vec<u8>,
}

/// The signature of one aws-chunked chunk, chained from the previous one (the seed's first).
pub fn chunk_signature(seed: &Seed, previous: &str, chunk: &[u8]) -> String {
    const EMPTY: &str = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
    let to_sign = format!(
        "AWS4-HMAC-SHA256-PAYLOAD\n{}\n{}\n{previous}\n{EMPTY}\n{}",
        seed.amz_date,
        seed.scope,
        sha256_hex(chunk)
    );
    hex(&hmac(&seed.key, to_sign.as_bytes()))
}

/// How far a signed request's clock may be from ours, as S3 allows.
pub const MAX_SKEW: u64 = 15 * 60;

/// Why a signed request was refused.
#[derive(Debug, PartialEq)]
pub enum Denied {
    /// other credentials, a malformed header, or a request changed after signing
    Signature,
    /// signed over 15 minutes from our clock: a replay, or the client's clock is off (S3 says
    /// RequestTimeTooSkewed, which SDKs answer by correcting their clock and retrying)
    Skewed,
}

/// Checks a client's `Authorization` header against our credentials, and that it was signed within
/// the last 15 minutes. `header` looks up request headers by lowercase name; `path` and `raw_query`
/// are as the client sent them. Returns what streamed chunks are then signed against.
pub fn verify(
    access_key: &str,
    secret: &str,
    method: &str,
    path: &str,
    raw_query: &str,
    authorization: &str,
    header: &dyn Fn(&str) -> Option<String>,
) -> Result<Seed, Denied> {
    let fields = authorization.strip_prefix("AWS4-HMAC-SHA256").ok_or(Denied::Signature)?;
    let (mut credential, mut signed, mut signature) = ("", "", "");
    for f in fields.split(',') {
        let f = f.trim();
        if let Some(v) = f.strip_prefix("Credential=") {
            credential = v;
        } else if let Some(v) = f.strip_prefix("SignedHeaders=") {
            signed = v;
        } else if let Some(v) = f.strip_prefix("Signature=") {
            signature = v;
        }
    }
    let scope: Vec<&str> = credential.split('/').collect();
    let date = header("x-amz-date").ok_or(Denied::Signature)?;
    if scope.len() != 5 || scope[0] != access_key || !date.starts_with(scope[1]) {
        return Err(Denied::Signature);
    }
    if parse_amz_date(&date).ok_or(Denied::Signature)?.abs_diff(unix_now()) > MAX_SKEW {
        return Err(Denied::Skewed);
    }
    let values: Vec<(String, String)> =
        signed.split(';').map(|h| (h.to_string(), header(h).unwrap_or_default())).collect();
    let headers: Vec<(&str, &str)> = values.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
    let payload = header("x-amz-content-sha256").unwrap_or_else(|| "UNSIGNED-PAYLOAD".into());
    let query = canonical_query(&parse_query(raw_query));
    let (expected, _) = sign(secret, scope[2], &date, method, path, &query, &headers, &payload);
    let equal = expected.len() == signature.len()
        && expected.bytes().zip(signature.bytes()).fold(0u8, |acc, (a, b)| acc | (a ^ b)) == 0;
    if !equal {
        return Err(Denied::Signature);
    }
    Ok(Seed { signature: expected, scope: scope[1..].join("/"), key: signing_key(secret, scope[1], scope[2]), amz_date: date })
}

/// "20261107T120000Z" to unix seconds.
pub fn parse_amz_date(s: &str) -> Option<u64> {
    let n = |a: usize, b: usize| s.get(a..b)?.parse::<u32>().ok();
    let days = days_from_civil(n(0, 4)? as i64, n(4, 6)?, n(6, 8)?);
    Some(days as u64 * 86_400 + u64::from(n(9, 11)? * 3600 + n(11, 13)? * 60 + n(13, 15)?))
}

/// Checks a presigned URL (signature in the query string) against our credentials, and that it
/// hasn't expired.
pub fn verify_presigned(
    access_key: &str,
    secret: &str,
    method: &str,
    path: &str,
    raw_query: &str,
    header: &dyn Fn(&str) -> Option<String>,
) -> bool {
    let params = parse_query(raw_query);
    let get = |k: &str| params.iter().find(|(n, _)| n == k).map(|(_, v)| v.as_str());
    let (Some("AWS4-HMAC-SHA256"), Some(credential), Some(date), Some(expires), Some(signed), Some(signature)) = (
        get("X-Amz-Algorithm"),
        get("X-Amz-Credential"),
        get("X-Amz-Date"),
        get("X-Amz-Expires"),
        get("X-Amz-SignedHeaders"),
        get("X-Amz-Signature"),
    ) else {
        return false;
    };
    let scope: Vec<&str> = credential.split('/').collect();
    if scope.len() != 5 || scope[0] != access_key || !date.starts_with(scope[1]) {
        return false;
    }
    match (parse_amz_date(date), expires.parse::<u64>()) {
        (Some(t), Ok(e)) if unix_now() <= t + e => {}
        _ => return false,
    }
    let unsigned: Vec<(String, String)> = params.iter().filter(|(k, _)| k != "X-Amz-Signature").cloned().collect();
    let values: Vec<(String, String)> =
        signed.split(';').map(|h| (h.to_string(), header(h).unwrap_or_default())).collect();
    let headers: Vec<(&str, &str)> = values.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
    let (expected, _) =
        sign(secret, scope[2], date, method, path, &canonical_query(&unsigned), &headers, "UNSIGNED-PAYLOAD");
    expected.len() == signature.len()
        && expected.bytes().zip(signature.bytes()).fold(0u8, |acc, (a, b)| acc | (a ^ b)) == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    // "GET Object" example from the S3 SigV4 documentation
    #[test]
    fn aws_example_signature() {
        let empty = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
        let (sig, signed) = sign(
            "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            "us-east-1",
            "20130524T000000Z",
            "GET",
            "/test.txt",
            "",
            &[
                ("Host", "examplebucket.s3.amazonaws.com"),
                ("Range", "bytes=0-9"),
                ("x-amz-content-sha256", empty),
                ("x-amz-date", "20130524T000000Z"),
            ],
            empty,
        );
        assert_eq!(signed, "host;range;x-amz-content-sha256;x-amz-date");
        assert_eq!(sig, "f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41");
    }

    // "Example: PUT Object" from the S3 chunked-upload (SigV4 streaming) documentation
    #[test]
    fn aws_streaming_example() {
        let secret = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
        let (seed, _) = sign(
            secret,
            "us-east-1",
            "20130524T000000Z",
            "PUT",
            "/examplebucket/chunkObject.txt",
            "",
            &[
                ("content-encoding", "aws-chunked"),
                ("content-length", "66824"),
                ("host", "s3.amazonaws.com"),
                ("x-amz-content-sha256", "STREAMING-AWS4-HMAC-SHA256-PAYLOAD"),
                ("x-amz-date", "20130524T000000Z"),
                ("x-amz-decoded-content-length", "66560"),
                ("x-amz-storage-class", "REDUCED_REDUNDANCY"),
            ],
            "STREAMING-AWS4-HMAC-SHA256-PAYLOAD",
        );
        assert_eq!(seed, "4f232c4386841ef735655705268965c44a0e4690baa4adea153f7db9fa80a0a9");
        let s = Seed {
            signature: seed.clone(),
            amz_date: "20130524T000000Z".into(),
            scope: "20130524/us-east-1/s3/aws4_request".into(),
            key: signing_key(secret, "20130524", "us-east-1"),
        };
        let c1 = chunk_signature(&s, &seed, &[b'a'; 65536]);
        assert_eq!(c1, "ad80c730a21e5b8d04586a2213dd63b9a0e99e0e2307b0ade35a65485a288648");
        let c2 = chunk_signature(&s, &c1, &[b'a'; 1024]);
        assert_eq!(c2, "0055627c9e194cb4542bae2aa5492e3c1575bbb81b612b7d234b86a503ef5497");
        assert_eq!(chunk_signature(&s, &c2, b""), "b6c6ea8a5354eaf15b3cb7646744f4275b71ea724fed81ceb9323e279d449df9");
    }

    #[test]
    fn verify_checks_signature_and_clock() {
        let signed_at = |unix: u64| {
            let date = amz_date(unix);
            let headers: Vec<(String, String)> = [("host", "h:1"), ("x-amz-content-sha256", "UNSIGNED-PAYLOAD"), ("x-amz-date", date.as_str())]
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect();
            let refs: Vec<(&str, &str)> = headers.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
            let (sig, list) = sign("SK", "us-east-1", &date, "GET", "/b/k", "", &refs, "UNSIGNED-PAYLOAD");
            let auth = format!("AWS4-HMAC-SHA256 Credential=AK/{}/us-east-1/s3/aws4_request, SignedHeaders={list}, Signature={sig}", &date[..8]);
            let header = move |h: &str| headers.iter().find(|(k, _)| k == h).map(|(_, v)| v.clone());
            (auth, header)
        };
        let (auth, header) = signed_at(unix_now());
        assert!(verify("AK", "SK", "GET", "/b/k", "", &auth, &header).is_ok());
        assert_eq!(verify("AK", "SK", "PUT", "/b/k", "", &auth, &header).err(), Some(Denied::Signature));
        assert_eq!(verify("AK", "other", "GET", "/b/k", "", &auth, &header).err(), Some(Denied::Signature));
        let (stale, header) = signed_at(unix_now() - 3600);
        assert_eq!(verify("AK", "SK", "GET", "/b/k", "", &stale, &header).err(), Some(Denied::Skewed), "an hour-old signature is a replay");
        let (ahead, header) = signed_at(unix_now() + 3600);
        assert_eq!(verify("AK", "SK", "GET", "/b/k", "", &ahead, &header).err(), Some(Denied::Skewed));
        let (recent, header) = signed_at(unix_now() - 60);
        assert!(verify("AK", "SK", "GET", "/b/k", "", &recent, &header).is_ok(), "a minute of skew is fine");
    }

    #[test]
    fn presigned_roundtrip() {
        // sign a URL the way S3 clients do, then check it
        let date = amz_date(unix_now());
        let mut params = vec![
            ("X-Amz-Algorithm".to_string(), "AWS4-HMAC-SHA256".to_string()),
            ("X-Amz-Credential".to_string(), format!("AK/{}/us-east-1/s3/aws4_request", &date[..8])),
            ("X-Amz-Date".to_string(), date.clone()),
            ("X-Amz-Expires".to_string(), "600".to_string()),
            ("X-Amz-SignedHeaders".to_string(), "host".to_string()),
        ];
        let q = canonical_query(&params);
        let (sig, _) = sign("SK", "us-east-1", &date, "GET", "/b/k", &q, &[("host", "h:1")], "UNSIGNED-PAYLOAD");
        params.push(("X-Amz-Signature".to_string(), sig));
        let raw = canonical_query(&params);
        let host = |h: &str| (h == "host").then(|| "h:1".to_string());
        assert!(verify_presigned("AK", "SK", "GET", "/b/k", &raw, &host));
        assert!(!verify_presigned("AK", "other", "GET", "/b/k", &raw, &host));
        assert!(!verify_presigned("AK", "SK", "GET", "/b/other", &raw, &host));
    }

    #[test]
    fn dates() {
        assert_eq!(amz_date(1_369_353_600), "20130524T000000Z");
        assert_eq!(parse_amz_date("20130524T010203Z"), Some(1_369_353_600 + 3723));
        assert_eq!(days_from_civil(2013, 5, 24), 1_369_353_600 / 86_400);
        assert_eq!(civil(days_from_civil(2024, 2, 29)), (2024, 2, 29));
    }

    #[test]
    fn encoding() {
        assert_eq!(uri_encode("a b/c~d", true), "a%20b/c~d");
        assert_eq!(uri_decode("docker%2Fregistry%2"), "docker/registry%2");
        assert_eq!(canonical_query(&parse_query("prefix=a%2Fb&list-type=2&delimiter=%2F")), "delimiter=%2F&list-type=2&prefix=a%2Fb");
    }
}
