//! Minimal OCI/Docker registry client: token auth (anonymous, or RIFT_REGISTRY_USER and
//! RIFT_REGISTRY_PASSWORD), manifests resolved to one platform, blob downloads.

use anyhow::{Context, Result, anyhow, bail};
use reqwest::blocking::{Client, Response};
use reqwest::header::{ACCEPT, AUTHORIZATION, LINK, WWW_AUTHENTICATE};
use serde_json::Value;

const MANIFEST_TYPES: &str = "application/vnd.oci.image.index.v1+json, \
    application/vnd.docker.distribution.manifest.list.v2+json, \
    application/vnd.oci.image.manifest.v1+json, \
    application/vnd.docker.distribution.manifest.v2+json";

pub struct Layer {
    pub digest: String,
    pub size: u64,
}

pub struct Registry {
    client: Client,
    host: String,
    repo: String,
    token: Option<String>,
}

impl Registry {
    pub fn new(host: &str, repo: &str) -> Result<Self> {
        let host = match host {
            "docker.io" | "index.docker.io" => "registry-1.docker.io",
            h => h,
        };
        let repo = if host == "registry-1.docker.io" && !repo.contains('/') {
            format!("library/{repo}")
        } else {
            repo.to_string()
        };
        let client = Client::builder().user_agent("rift-scan/0.1").timeout(None).build()?;
        Ok(Self { client, host: host.to_string(), repo, token: None })
    }

    pub fn name(&self) -> String {
        format!("{}/{}", self.host, self.repo)
    }

    fn get(&mut self, path: &str, accept: Option<&str>) -> Result<Response> {
        let url = format!("https://{}/v2/{}/{}", self.host, self.repo, path);
        self.get_url(&url, accept)
    }

    fn get_url(&mut self, url: &str, accept: Option<&str>) -> Result<Response> {
        for attempt in 0..2 {
            let mut req = self.client.get(url);
            if let Some(a) = accept {
                req = req.header(ACCEPT, a);
            }
            if let Some(t) = &self.token {
                req = req.header(AUTHORIZATION, format!("Bearer {t}"));
            }
            let resp = req.send().with_context(|| format!("GET {url}"))?;
            if resp.status().as_u16() == 401 && attempt == 0 {
                let challenge = resp
                    .headers()
                    .get(WWW_AUTHENTICATE)
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or_default()
                    .to_string();
                self.token = Some(self.fetch_token(&challenge)?);
                continue;
            }
            if !resp.status().is_success() {
                bail!("GET {url}: {}", resp.status());
            }
            return Ok(resp);
        }
        unreachable!()
    }

    /// Bearer realm="...",service="...",scope="..." -> token
    fn fetch_token(&self, challenge: &str) -> Result<String> {
        let field = |k: &str| {
            let key = format!("{k}=\"");
            challenge.split(key.as_str()).nth(1).and_then(|s| s.split('"').next()).map(str::to_string)
        };
        let realm = field("realm").ok_or_else(|| anyhow!("registry wants auth but gave no token realm: {challenge}"))?;
        let mut query = Vec::new();
        if let Some(service) = field("service") {
            query.push(("service", service));
        }
        query.push(("scope", field("scope").unwrap_or(format!("repository:{}:pull", self.repo))));
        let mut req = self.client.get(&realm).query(&query);
        if let (Ok(user), Ok(pass)) = (std::env::var("RIFT_REGISTRY_USER"), std::env::var("RIFT_REGISTRY_PASSWORD")) {
            req = req.basic_auth(user, Some(pass));
        }
        let body: Value = serde_json::from_str(&req.send()?.error_for_status()?.text()?)?;
        body["token"]
            .as_str()
            .or(body["access_token"].as_str())
            .map(str::to_string)
            .ok_or_else(|| anyhow!("no token in the registry's auth response"))
    }

    /// The `latest` most recent tags, oldest first.
    pub fn tags(&mut self, latest: usize) -> Result<Vec<String>> {
        if self.host == "registry-1.docker.io" {
            // the registry API lists tags alphabetically; Docker Hub's API knows push order
            let url = format!(
                "https://hub.docker.com/v2/repositories/{}/tags?page_size={}&ordering=last_updated",
                self.repo,
                latest.clamp(1, 100)
            );
            let body: Value = serde_json::from_str(&self.client.get(&url).send()?.error_for_status()?.text()?)?;
            let mut tags: Vec<String> = body["results"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(|t| t["name"].as_str().map(str::to_string))
                .take(latest)
                .collect();
            tags.reverse();
            return Ok(tags);
        }
        // the distribution API has no push dates; natural order is the best guess at build order
        let mut url = format!("https://{}/v2/{}/tags/list?n=1000", self.host, self.repo);
        let mut tags = Vec::new();
        loop {
            let resp = self.get_url(&url, None)?;
            let next = resp
                .headers()
                .get(LINK)
                .and_then(|v| v.to_str().ok())
                .filter(|l| l.contains("rel=\"next\""))
                .and_then(|l| l.split(';').next())
                .map(|u| u.trim().trim_matches(['<', '>']).to_string());
            let body: Value = serde_json::from_str(&resp.text()?)?;
            tags.extend(body["tags"].as_array().into_iter().flatten().filter_map(|t| t.as_str().map(str::to_string)));
            match next {
                Some(n) if n.starts_with('/') => url = format!("https://{}{n}", self.host),
                Some(n) if n.starts_with("https://") => url = n,
                _ => break,
            }
        }
        tags.sort_by_cached_key(|t| natural(t));
        Ok(tags[tags.len().saturating_sub(latest)..].to_vec())
    }

    pub fn layers(&mut self, tag: &str, platform: &str) -> Result<Vec<Layer>> {
        let mut m: Value = serde_json::from_str(&self.get(&format!("manifests/{tag}"), Some(MANIFEST_TYPES))?.text()?)?;
        if let Some(list) = m["manifests"].as_array() {
            let (os, arch) = platform.split_once('/').unwrap_or(("linux", platform));
            let digest = list
                .iter()
                .find(|e| e["platform"]["os"] == os && e["platform"]["architecture"] == arch)
                .and_then(|e| e["digest"].as_str())
                .ok_or_else(|| anyhow!("{tag}: no {platform} image"))?
                .to_string();
            m = serde_json::from_str(&self.get(&format!("manifests/{digest}"), Some(MANIFEST_TYPES))?.text()?)?;
        }
        Ok(m["layers"]
            .as_array()
            .into_iter()
            .flatten()
            .map(|l| Layer {
                digest: l["digest"].as_str().unwrap_or_default().to_string(),
                size: l["size"].as_u64().unwrap_or(0),
            })
            .collect())
    }

    pub fn blob(&mut self, digest: &str) -> Result<Vec<u8>> {
        // the registry redirects to a CDN; reqwest drops the Authorization header across hosts
        let mut wait = 2;
        loop {
            match self.get(&format!("blobs/{digest}"), None).and_then(|r| Ok(r.bytes()?)) {
                Ok(b) => return Ok(b.to_vec()),
                Err(e) if wait <= 16 => {
                    eprintln!("retrying {digest} in {wait}s: {e:#}");
                    std::thread::sleep(std::time::Duration::from_secs(wait));
                    wait *= 2;
                }
                Err(e) => return Err(e.context(format!("downloading {digest}"))),
            }
        }
    }
}

#[derive(PartialEq, Eq, PartialOrd, Ord)]
enum Part {
    Num(usize, String),
    Text(String),
}

/// Sort key that puts "v1.10" after "v1.9" and "build-1000" after "build-999".
fn natural(s: &str) -> Vec<Part> {
    let mut parts = Vec::new();
    let mut rest = s;
    while let Some(c) = rest.chars().next() {
        let digits = c.is_ascii_digit();
        let end = rest.find(|ch: char| ch.is_ascii_digit() != digits).unwrap_or(rest.len());
        let (run, tail) = rest.split_at(end);
        parts.push(if digits {
            let n = run.trim_start_matches('0');
            Part::Num(n.len(), n.to_string())
        } else {
            Part::Text(run.to_string())
        });
        rest = tail;
    }
    parts
}

#[cfg(test)]
mod tests {
    use super::natural;

    #[test]
    fn natural_order() {
        let mut tags = vec!["build-1000", "v1.10", "build-999", "v1.9", "latest", "v1.9.1"];
        tags.sort_by_cached_key(|t| natural(t));
        assert_eq!(tags, ["build-999", "build-1000", "latest", "v1.9", "v1.9.1", "v1.10"]);
    }
}
