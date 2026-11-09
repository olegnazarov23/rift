//! At most one compactor per bucket. A gateway that may compact holds `.rift/compactor` while it
//! does: it rewrites the object every RENEW, and a gateway that sees it unchanged for STALE takes
//! it over. The writes are conditional (If-None-Match, If-Match), so two can't take it at once.
//! Compaction, cooling, GC and cleanup run only while `holding()`, which lapses (VALID) well
//! before anyone else may take over.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::s3::Backend;
use crate::sigv4::unix_now;

pub const KEY: &str = ".rift/compactor";
const RENEW: Duration = Duration::from_secs(20);
/// unchanged this long: whoever held it has stopped
const STALE: Duration = Duration::from_secs(120);
/// a holder that couldn't renew for this long stops compacting, before anyone may take over
const VALID: Duration = Duration::from_secs(60);

pub struct Lock {
    backend: Arc<Backend>,
    /// this gateway's id, kept in its state dir, so a restart takes the lock back at once
    id: String,
    /// false when the bucket can't do conditional writes: then this gateway always compacts
    enforced: bool,
    state: Mutex<State>,
}

#[derive(Default)]
struct State {
    /// our version of the object, while we hold it
    held: Option<String>,
    renewed: Option<Instant>,
    /// someone else's version, and since when it hasn't changed
    seen: Option<(String, Instant)>,
    /// counts the times this gateway took the lock, so the compactor knows to start afresh
    taken: u64,
    said_waiting: bool,
}

impl Lock {
    pub fn new(backend: Arc<Backend>, id: String, enforced: bool) -> Arc<Self> {
        Arc::new(Self { backend, id, enforced, state: Mutex::default() })
    }

    /// Whether this gateway is the compactor right now.
    pub fn holding(&self) -> bool {
        if !self.enforced {
            return true;
        }
        let s = self.state.lock().unwrap();
        s.held.is_some() && s.renewed.is_some_and(|t| t.elapsed() < VALID)
    }

    /// How many times this gateway has taken the lock; changes when it takes it again.
    pub fn taken(&self) -> u64 {
        if !self.enforced {
            return 1;
        }
        self.state.lock().unwrap().taken
    }

    fn body(&self) -> Vec<u8> {
        serde_json::json!({"id": self.id, "host": host(), "pid": std::process::id(), "renewed": unix_now()}).to_string().into_bytes()
    }

    /// Whose the object is, from its body: whether it's this gateway's own from before a restart
    /// (same id, same host: a copied state dir elsewhere doesn't count), and who for the log.
    fn holder(&self) -> Option<(bool, String)> {
        let (body, _) = self.backend.get_opt(KEY).ok()??;
        let v: serde_json::Value = serde_json::from_slice(&body).ok()?;
        let theirs = v["host"].as_str().unwrap_or("");
        let ours = v["id"].as_str() == Some(self.id.as_str()) && theirs == host();
        Some((ours, if theirs.is_empty() { format!("pid {}", v["pid"]) } else { theirs.to_string() }))
    }

    /// One step: renew the lock if this gateway holds it, else take it if it's free or stale.
    pub fn tick(&self) {
        let held = self.state.lock().unwrap().held.clone();
        if let Some(etag) = held {
            match self.backend.put_if(KEY, self.body(), &[("if-match".into(), etag)]) {
                Ok(Some(new)) => {
                    let mut s = self.state.lock().unwrap();
                    s.held = Some(new);
                    s.renewed = Some(Instant::now());
                }
                Ok(None) => {
                    eprintln!("rift: another gateway took over {KEY}; this one stops compacting");
                    let mut s = self.state.lock().unwrap();
                    (s.held, s.renewed, s.seen) = (None, None, None);
                }
                // not renewed: holding() lapses after VALID, and the next tick tries again
                Err(e) => eprintln!("rift: renewing {KEY}: {e:#}"),
            }
            return;
        }
        let current = match self.backend.head(KEY) {
            Ok(c) => c.map(|(_, etag)| etag),
            Err(e) => {
                eprintln!("rift: reading {KEY}: {e:#}");
                return;
            }
        };
        let take = match &current {
            None => Some(("*".to_string(), "if-none-match")),
            Some(etag) => {
                let mut s = self.state.lock().unwrap();
                match &s.seen {
                    Some((seen, since)) if seen == etag && since.elapsed() >= STALE => Some((etag.clone(), "if-match")),
                    Some((seen, _)) if seen == etag => None,
                    // renewed by a holder already known to be another gateway
                    _ if s.said_waiting => {
                        s.seen = Some((etag.clone(), Instant::now()));
                        None
                    }
                    _ => {
                        s.seen = Some((etag.clone(), Instant::now()));
                        drop(s);
                        // our own, from before a restart: take it back now
                        match self.holder() {
                            Some((true, _)) => Some((etag.clone(), "if-match")),
                            Some((false, host)) => {
                                let mut s = self.state.lock().unwrap();
                                if !s.said_waiting {
                                    s.said_waiting = true;
                                    eprintln!("rift: another gateway ({host}) is compacting this bucket; this one stands by");
                                }
                                None
                            }
                            None => None,
                        }
                    }
                }
            }
        };
        let Some((value, condition)) = take else { return };
        match self.backend.put_if(KEY, self.body(), &[(condition.into(), value)]) {
            Ok(Some(etag)) => {
                let mut s = self.state.lock().unwrap();
                s.held = Some(etag);
                s.renewed = Some(Instant::now());
                s.seen = None;
                s.taken += 1;
                s.said_waiting = false;
                eprintln!("rift: compacting this bucket (holding {KEY})");
            }
            Ok(None) => self.state.lock().unwrap().seen = None, // someone else was quicker
            Err(e) => eprintln!("rift: taking {KEY}: {e:#}"),
        }
    }

    /// Ticks forever.
    pub fn run(self: Arc<Self>) {
        if !self.enforced {
            return;
        }
        loop {
            self.tick();
            std::thread::sleep(RENEW);
        }
    }

    /// For `rift eject`: whether a compactor renewed the lock in the last few minutes.
    pub fn active(backend: &Backend) -> Option<u64> {
        let (body, _) = backend.get_opt(KEY).ok()??;
        let v: serde_json::Value = serde_json::from_slice(&body).ok()?;
        let renewed = v["renewed"].as_u64()?;
        (unix_now().saturating_sub(renewed) < 2 * STALE.as_secs()).then_some(renewed)
    }
}

/// The machine's name (a pod's, in Kubernetes).
fn host() -> String {
    std::env::var("HOSTNAME").unwrap_or_default()
}

/// Whether the bucket refuses a PUT whose `If-None-Match: *` fails, tried on a throwaway object.
/// Some S3-compatible stores ignore the header, or reject it.
pub fn conditional_writes(backend: &Backend, id: &str) -> bool {
    let key = format!(".rift/probe-{id}");
    let only_new = [("if-none-match".to_string(), "*".to_string())];
    let first = backend.put_if(&key, b"probe".to_vec(), &only_new);
    let second = backend.put_if(&key, b"probe".to_vec(), &only_new);
    let _ = backend.delete(&key);
    matches!((first, second), (Ok(Some(_)), Ok(None)))
}
