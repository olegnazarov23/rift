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
    let key = [date, region, "s3", "aws4_request"]
        .iter()
        .fold(format!("AWS4{secret}").into_bytes(), |k, part| hmac(&k, part.as_bytes()));
    (hex(&hmac(&key, to_sign.as_bytes())), signed)
}

/// Checks a client's `Authorization` header against our credentials. `header` looks up request
/// headers by lowercase name; `path` and `raw_query` are as the client sent them.
pub fn verify(
    access_key: &str,
    secret: &str,
    method: &str,
    path: &str,
    raw_query: &str,
    authorization: &str,
    header: &dyn Fn(&str) -> Option<String>,
) -> bool {
    let Some(fields) = authorization.strip_prefix("AWS4-HMAC-SHA256") else { return false };
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
    let Some(date) = header("x-amz-date") else { return false };
    if scope.len() != 5 || scope[0] != access_key || !date.starts_with(scope[1]) {
        return false;
    }
    let values: Vec<(String, String)> =
        signed.split(';').map(|h| (h.to_string(), header(h).unwrap_or_default())).collect();
    let headers: Vec<(&str, &str)> = values.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
    let payload = header("x-amz-content-sha256").unwrap_or_else(|| "UNSIGNED-PAYLOAD".into());
    let query = canonical_query(&parse_query(raw_query));
    let (expected, _) = sign(secret, scope[2], &date, method, path, &query, &headers, &payload);
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

    #[test]
    fn dates() {
        assert_eq!(amz_date(1_369_353_600), "20130524T000000Z");
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
