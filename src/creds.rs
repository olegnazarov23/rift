//! Keys for the backing bucket: static ones, or AWS's temporary ones (IRSA, EKS Pod Identity, ECS
//! task roles, EC2 instance roles), fetched again before they expire.

use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use anyhow::{Context, Result, anyhow, bail, ensure};
use reqwest::blocking::Client;

use crate::{
    s3::{parse_iso, xml_first, xml_unescape},
    sigv4::{unix_now, uri_encode},
};

/// One set of keys; `token` comes with temporary ones.
#[derive(Debug, PartialEq)]
pub struct Keys {
    pub access: String,
    pub secret: String,
    pub token: Option<String>,
}

/// Temporary keys are fetched again this long before they expire, so the 15-minute presigned
/// URLs made with them never outlive them.
const REFRESH_BEFORE: u64 = 15 * 60;
/// After a failed refresh, wait this long before trying again.
const RETRY_AFTER: u64 = 30;

const NO_KEYS: &str = "no keys for the bucket: set RIFT_BACKEND_ACCESS_KEY and RIFT_BACKEND_SECRET_KEY, or run with an AWS role (IRSA, EKS Pod Identity, ECS task role or EC2 instance profile)";

enum Fetch {
    /// IRSA: trade the projected service account token for keys at STS
    WebIdentity { role: String, token_file: String, session: String, sts: String },
    /// EKS Pod Identity and ECS: a local endpoint hands them out
    Container { url: String, token: Option<String>, token_file: Option<String> },
    /// an EC2 instance role, from the instance metadata service (IMDSv2)
    Imds { endpoint: String },
}

struct Current {
    keys: Arc<Keys>,
    /// unix seconds; u64::MAX for static keys
    expires: u64,
    next_try: u64,
}

pub struct Credentials {
    what: String,
    fetch: Option<Fetch>,
    client: Client,
    current: Mutex<Current>,
    refreshing: AtomicBool,
}

impl Credentials {
    /// The first source that's set, in the order AWS's SDKs look: RIFT_BACKEND_ACCESS_KEY and
    /// RIFT_BACKEND_SECRET_KEY (with RIFT_BACKEND_SESSION_TOKEN), AWS_ACCESS_KEY_ID and
    /// AWS_SECRET_ACCESS_KEY (with AWS_SESSION_TOKEN), a web identity token (AWS_ROLE_ARN and
    /// AWS_WEB_IDENTITY_TOKEN_FILE, as IRSA sets them), container credentials
    /// (AWS_CONTAINER_CREDENTIALS_FULL_URI or _RELATIVE_URI), then the EC2 instance role.
    pub fn from_env(region: &str) -> Result<Self> {
        Self::from_vars(region, &|k| std::env::var(k).ok().filter(|v| !v.is_empty()))
    }

    fn from_vars(region: &str, var: &dyn Fn(&str) -> Option<String>) -> Result<Self> {
        let client = Client::builder().connect_timeout(Duration::from_secs(2)).timeout(Duration::from_secs(10)).build()?;
        let fixed = |what: &str, keys: Keys| Credentials {
            what: format!("{what}{}", if keys.token.is_some() { " with a session token" } else { "" }),
            fetch: None,
            client: client.clone(),
            current: Mutex::new(Current { keys: Arc::new(keys), expires: u64::MAX, next_try: 0 }),
            refreshing: AtomicBool::new(false),
        };
        for (access, secret, token) in [
            ("RIFT_BACKEND_ACCESS_KEY", "RIFT_BACKEND_SECRET_KEY", "RIFT_BACKEND_SESSION_TOKEN"),
            ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN"),
        ] {
            if let (Some(a), Some(s)) = (var(access), var(secret)) {
                return Ok(fixed(access, Keys { access: a, secret: s, token: var(token) }));
            }
        }
        let (fetch, what) = if let (Some(role), Some(token_file)) = (var("AWS_ROLE_ARN"), var("AWS_WEB_IDENTITY_TOKEN_FILE")) {
            let region = var("AWS_REGION").or_else(|| var("AWS_DEFAULT_REGION")).unwrap_or_else(|| region.to_string());
            let sts = var("AWS_ENDPOINT_URL_STS").unwrap_or_else(|| sts_endpoint(&region));
            let session = var("AWS_ROLE_SESSION_NAME").unwrap_or_else(|| format!("rift-{}", unix_now()));
            let what = format!("web identity for {role}");
            (Fetch::WebIdentity { role, token_file, session, sts }, what)
        } else if let Some(url) = var("AWS_CONTAINER_CREDENTIALS_FULL_URI")
            .or_else(|| var("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI").map(|p| format!("http://169.254.170.2{p}")))
        {
            let what = format!("container credentials from {url}");
            let (token, token_file) = (var("AWS_CONTAINER_AUTHORIZATION_TOKEN"), var("AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE"));
            (Fetch::Container { url, token, token_file }, what)
        } else if var("AWS_EC2_METADATA_DISABLED").is_some_and(|v| v.eq_ignore_ascii_case("true")) {
            bail!(NO_KEYS);
        } else {
            let endpoint = var("AWS_EC2_METADATA_SERVICE_ENDPOINT").unwrap_or_else(|| "http://169.254.169.254".into());
            (Fetch::Imds { endpoint: endpoint.trim_end_matches('/').to_string() }, "the EC2 instance role".to_string())
        };
        let (keys, expires) = match fetch_keys(&client, &fetch) {
            Ok(k) => k,
            // nothing was configured, and this isn't an EC2 instance with a role either
            Err(e) if matches!(fetch, Fetch::Imds { .. }) => return Err(anyhow!("{NO_KEYS} ({e:#})")),
            Err(e) => return Err(e.context(format!("getting keys: {what}"))),
        };
        Ok(Self {
            what,
            fetch: Some(fetch),
            client,
            current: Mutex::new(Current { keys: Arc::new(keys), expires, next_try: 0 }),
            refreshing: AtomicBool::new(false),
        })
    }

    /// Fixed keys, for tests.
    #[cfg(test)]
    pub fn fixed(access: &str, secret: &str) -> Self {
        let (a, s) = (access.to_string(), secret.to_string());
        Self::from_vars("us-east-1", &move |k| match k {
            "RIFT_BACKEND_ACCESS_KEY" => Some(a.clone()),
            "RIFT_BACKEND_SECRET_KEY" => Some(s.clone()),
            _ => None,
        })
        .unwrap()
    }

    /// Where the keys come from, for the log.
    pub fn describe(&self) -> &str {
        &self.what
    }

    /// Keys to sign with now. One caller refreshes temporary keys when they near their expiry;
    /// the others go on with the current ones meanwhile.
    pub fn keys(&self) -> Result<Arc<Keys>> {
        let now = unix_now();
        let (keys, expires, due) = {
            let c = self.current.lock().unwrap();
            (c.keys.clone(), c.expires, self.fetch.is_some() && now + REFRESH_BEFORE >= c.expires && now >= c.next_try)
        };
        if !due || self.refreshing.swap(true, Ordering::SeqCst) {
            ensure!(now < expires, "the keys from {} expired and couldn't be refreshed", self.what);
            return Ok(keys);
        }
        let fetched = fetch_keys(&self.client, self.fetch.as_ref().unwrap());
        let mut c = self.current.lock().unwrap();
        self.refreshing.store(false, Ordering::SeqCst);
        match fetched {
            Ok((keys, expires)) => {
                (c.keys, c.expires) = (Arc::new(keys), expires);
                Ok(c.keys.clone())
            }
            Err(e) => {
                c.next_try = now + RETRY_AFTER;
                ensure!(now < c.expires, "refreshing keys from {}: {e:#}", self.what);
                eprintln!("rift: refreshing keys from {}: {e:#}; the current ones last {}s more", self.what, c.expires - now);
                Ok(c.keys.clone())
            }
        }
    }
}

fn sts_endpoint(region: &str) -> String {
    let domain = if region.starts_with("cn-") { "amazonaws.com.cn" } else { "amazonaws.com" };
    format!("https://sts.{region}.{domain}/")
}

fn fetch_keys(client: &Client, fetch: &Fetch) -> Result<(Keys, u64)> {
    let text = |resp: reqwest::blocking::Response, what: &str| -> Result<String> {
        let status = resp.status();
        let body = resp.text().unwrap_or_default();
        ensure!(status.is_success(), "{what}: {status} {}", xml_first(&body, "Message").unwrap_or(&body));
        Ok(body)
    };
    match fetch {
        Fetch::WebIdentity { role, token_file, session, sts } => {
            let token = std::fs::read_to_string(token_file).with_context(|| format!("reading {token_file}"))?;
            let form = [
                ("Action", "AssumeRoleWithWebIdentity"),
                ("Version", "2011-06-15"),
                ("RoleArn", role.as_str()),
                ("RoleSessionName", session.as_str()),
                ("WebIdentityToken", token.trim()),
            ];
            let body: Vec<String> = form.iter().map(|(k, v)| format!("{k}={}", uri_encode(v, false))).collect();
            let resp = client
                .post(sts)
                .header("content-type", "application/x-www-form-urlencoded")
                .body(body.join("&"))
                .send()
                .with_context(|| format!("POST {sts}"))?;
            parse_sts(&text(resp, "STS")?)
        }
        Fetch::Container { url, token, token_file } => {
            let auth = match token_file {
                Some(f) => Some(std::fs::read_to_string(f).with_context(|| format!("reading {f}"))?.trim().to_string()),
                None => token.clone(),
            };
            let mut req = client.get(url);
            if let Some(a) = auth {
                req = req.header("authorization", a);
            }
            parse_json(&text(req.send().with_context(|| format!("GET {url}"))?, url)?)
        }
        Fetch::Imds { endpoint } => {
            let resp = client
                .put(format!("{endpoint}/latest/api/token"))
                .header("x-aws-ec2-metadata-token-ttl-seconds", "21600")
                .send()
                .context("instance metadata")?;
            let token = text(resp, "instance metadata token")?;
            let base = format!("{endpoint}/latest/meta-data/iam/security-credentials/");
            let get = |url: &str| client.get(url).header("x-aws-ec2-metadata-token", &token).send().context("instance metadata");
            let roles = text(get(&base)?, "instance roles")?;
            let role = roles.lines().next().unwrap_or("").trim();
            ensure!(!role.is_empty(), "the instance has no role");
            parse_json(&text(get(&format!("{base}{role}"))?, "instance role keys")?)
        }
    }
}

/// STS's answer: `<Credentials>` with AccessKeyId, SecretAccessKey, SessionToken, Expiration.
fn parse_sts(xml: &str) -> Result<(Keys, u64)> {
    let get = |tag: &str| xml_first(xml, tag).map(xml_unescape).with_context(|| format!("STS answered without {tag}"));
    let keys = Keys { access: get("AccessKeyId")?, secret: get("SecretAccessKey")?, token: Some(get("SessionToken")?) };
    Ok((keys, expiry(&get("Expiration")?)?))
}

/// The JSON that container endpoints and the instance metadata service hand out.
fn parse_json(text: &str) -> Result<(Keys, u64)> {
    let v: serde_json::Value = serde_json::from_str(text).context("credentials JSON")?;
    let get = |k: &str| v.get(k).and_then(|x| x.as_str()).map(str::to_string).with_context(|| format!("credentials without {k}"));
    let keys = Keys { access: get("AccessKeyId")?, secret: get("SecretAccessKey")?, token: get("Token").ok() };
    Ok((keys, expiry(&get("Expiration")?)?))
}

fn expiry(s: &str) -> Result<u64> {
    parse_iso(s).with_context(|| format!("expiration {s:?}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sigv4::amz_date;
    use std::{
        collections::HashMap,
        io::{Read, Write},
        net::TcpListener,
        sync::atomic::AtomicUsize,
    };

    fn iso(unix: u64) -> String {
        let d = amz_date(unix);
        format!("{}-{}-{}T{}:{}:{}Z", &d[0..4], &d[4..6], &d[6..8], &d[9..11], &d[11..13], &d[13..15])
    }

    fn vars(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
        let m: HashMap<String, String> = pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect();
        move |k| m.get(k).cloned()
    }

    #[test]
    fn static_keys_in_order() {
        let both = vars(&[
            ("RIFT_BACKEND_ACCESS_KEY", "R"),
            ("RIFT_BACKEND_SECRET_KEY", "RS"),
            ("AWS_ACCESS_KEY_ID", "A"),
            ("AWS_SECRET_ACCESS_KEY", "AS"),
        ]);
        let c = Credentials::from_vars("us-east-1", &both).unwrap();
        assert_eq!(*c.keys().unwrap(), Keys { access: "R".into(), secret: "RS".into(), token: None });
        let aws = vars(&[("AWS_ACCESS_KEY_ID", "A"), ("AWS_SECRET_ACCESS_KEY", "AS"), ("AWS_SESSION_TOKEN", "T")]);
        let c = Credentials::from_vars("us-east-1", &aws).unwrap();
        assert_eq!(c.keys().unwrap().token.as_deref(), Some("T"));
        assert_eq!(c.describe(), "AWS_ACCESS_KEY_ID with a session token");
        let none = vars(&[("AWS_EC2_METADATA_DISABLED", "true")]);
        assert!(Credentials::from_vars("us-east-1", &none).err().unwrap().to_string().starts_with("no keys"));
    }

    #[test]
    fn parses_answers() {
        // shaped like AWS's documented AssumeRoleWithWebIdentity response
        let sts = "<AssumeRoleWithWebIdentityResponse xmlns=\"https://sts.amazonaws.com/doc/2011-06-15/\">\
            <AssumeRoleWithWebIdentityResult><Credentials>\
            <SessionToken>AQoDYXdzEE0a8ANXXXXXXXXNO1ewxE5TijQyp+IEXAMPLE</SessionToken>\
            <SecretAccessKey>wJalrXUtnFEMI/K7MDENG/bPxRfiCYzEXAMPLEKEY</SecretAccessKey>\
            <Expiration>2014-10-24T23:00:23Z</Expiration><AccessKeyId>ASgeIAIOSFODNN7EXAMPLE</AccessKeyId>\
            </Credentials></AssumeRoleWithWebIdentityResult></AssumeRoleWithWebIdentityResponse>";
        let (keys, expires) = parse_sts(sts).unwrap();
        assert_eq!(keys.access, "ASgeIAIOSFODNN7EXAMPLE");
        assert_eq!(keys.token.as_deref(), Some("AQoDYXdzEE0a8ANXXXXXXXXNO1ewxE5TijQyp+IEXAMPLE"));
        assert_eq!(iso(expires), "2014-10-24T23:00:23Z");
        let imds = r#"{"Code":"Success","LastUpdated":"2026-11-08T12:00:00Z","Type":"AWS-HMAC","AccessKeyId":"AKIDEXAMPLE","SecretAccessKey":"S","Token":"T","Expiration":"2026-11-08T18:00:00Z"}"#;
        let (keys, expires) = parse_json(imds).unwrap();
        assert_eq!((keys.access.as_str(), keys.token.as_deref(), iso(expires).as_str()), ("AKIDEXAMPLE", Some("T"), "2026-11-08T18:00:00Z"));
        assert!(parse_json(r#"{"AccessKeyId":"A"}"#).is_err());
        assert!(parse_sts("<Error><Message>nope</Message></Error>").is_err());
    }

    /// A container credentials endpoint that answers each request with the next of `expiries`.
    fn endpoint(expiries: Vec<u64>) -> (String, Arc<AtomicUsize>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("http://{}/creds", listener.local_addr().unwrap());
        let served = Arc::new(AtomicUsize::new(0));
        let count = served.clone();
        std::thread::spawn(move || {
            for (n, stream) in listener.incoming().enumerate() {
                let mut s = stream.unwrap();
                let mut buf = [0u8; 4096];
                let _ = s.read(&mut buf);
                let Some(&exp) = expiries.get(n) else { return };
                // counted before answering: the client may finish reading before this thread runs on
                count.fetch_add(1, Ordering::SeqCst);
                let body = format!(r#"{{"AccessKeyId":"K{n}","SecretAccessKey":"S","Token":"T","Expiration":"{}"}}"#, iso(exp));
                let _ = write!(s, "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len());
            }
        });
        (url, served)
    }

    #[test]
    fn refreshes_before_expiry() {
        let now = unix_now();
        // the first keys are inside the refresh window, the second aren't
        let (url, served) = endpoint(vec![now + 600, now + 3600]);
        let c = Credentials::from_vars("us-east-1", &vars(&[("AWS_CONTAINER_CREDENTIALS_FULL_URI", &url)])).unwrap();
        assert_eq!(c.keys().unwrap().access, "K1", "refetched: the first ones expire within 15 minutes");
        assert_eq!(c.keys().unwrap().access, "K1", "kept: these last an hour");
        assert_eq!(served.load(Ordering::SeqCst), 2);
        // the endpoint is gone now: the current keys still last, so they're used
        c.current.lock().unwrap().expires = now + 60;
        assert_eq!(c.keys().unwrap().access, "K1");
        // and once they've expired, signing fails rather than using dead keys
        let mut cur = c.current.lock().unwrap();
        (cur.expires, cur.next_try) = (now - 1, 0);
        drop(cur);
        assert!(c.keys().is_err());
    }
}
