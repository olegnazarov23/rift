# rift threat model

For security reviewers and for the security teams of organizations running rift. It covers what
rift protects, the trust boundaries it sits on, the threats considered, and the control and
evidence for each. Known gaps are at the end. Report vulnerabilities as [SECURITY.md](../SECURITY.md)
says.

## What rift is

rift is an S3 endpoint placed in front of a container registry's existing bucket. The registry
signs its S3 requests with client keys that only rift accepts. rift re-signs them with its own
credentials (an IAM role or a backend key) and passes them to the bucket. Blobs nobody has touched
for a while are compacted: gzip layers are unpacked with preflate, which records how to recreate the
exact compressed bytes, and the content is deduplicated into zstd packs under `.rift/` in the same
bucket. A compacted blob is rebuilt when read.

```
docker / CI ──► registry ──(S3 API, client keys)──► rift ──(S3 API, rift's role or key)──► bucket
     │                                              ▲
     └──── presigned GET (if the registry redirects) ┘
```

## Assets

| Asset | What matters |
|---|---|
| Blob bytes (image layers, configs, manifests) | integrity: every digest must still match; confidentiality of private images; availability |
| rift's data under `.rift/` (packs, manifests, indexes) | losing it loses the compacted blobs |
| rift's bucket credentials | full read and write on the bucket |
| Client keys | whoever holds them can do anything through rift that the registry can |
| The gateway's availability | pushes and pulls of the whole registry depend on it |

## Trust boundaries

1. **Network to rift.** Untrusted. Legitimate callers are the registry (client keys) and, when the
   registry redirects downloads, any client it handed a presigned URL.
2. **rift to the bucket.** The bucket is trusted storage; rift authenticates to it with its own
   credentials.
3. **The host.** rift writes temp files (cold reads, hot copies) and a local chunk index. In a
   container these are private; on a shared host other local users are a consideration.
4. **Supply chain.** Source, dependencies, build and the published image.

## Threats and controls

| Threat | Control | Evidence |
|---|---|---|
| Unauthenticated requests | SigV4 required on every request, in the header or a presigned URL, not both; signatures compared in constant time; rift won't start without client keys (both set, the secret at least 16 characters) unless `--no-client-auth` is given, which works only on a loopback IP address | unit tests on AWS's published signing vectors; live: unsigned, wrong-key and empty-secret requests get 403; a presigned signature moved into a header gets 400 |
| Replaying a captured request | header signatures valid within 15 minutes of rift's clock (`RequestTimeTooSkewed` otherwise); presigned URLs at most 7 days, not dated in the future | unit tests; live: 14 minutes accepted, 16 refused, both directions |
| Altered requests | signed payload hashes checked; every chunk of a signed `aws-chunked` upload checked against the chain of chunk signatures; trailing checksums passed to the bucket, which checks them; `x-amz-*` headers, `Content-MD5` and `Content-Encoding` must be signed, as S3 requires | minio-go uploads accepted; the same upload with one byte flipped in transit refused, nothing written; a replayed request with an added unsigned header gets 403 |
| Presigned URL misuse | GET and HEAD only, for the signed path and query; rift's presign parameters are stripped before forwarding | unit tests on verification; live: a presigned GET URL used for PUT gets 403 |
| Requests a registry never makes | only the operations registries use are passed on (object reads, writes, copies, deletes and multipart uploads; listings); tagging, ACLs, versions, bucket configuration and the rest get 501, whatever the keys | unit test of the allowlist; live: PutObjectTagging and PutBucketPolicy get 501 and the blob stays byte-exact |
| Reaching rift's internal data or another bucket | requests for `.rift/` are refused after decoding the path, and so are keys starting with `/` or holding empty, `.` or `..` segments (backends normalize those differently); DeleteObjects and copies can't name `.rift/`; listings hide it however it's encoded; one bucket per gateway, others get `NoSuchBucket` | live: signed GET and DELETE of a pack, `%2Erift/…` and `//.rift/…` get 403 or 400 and the pack stays; DeleteObjects and CopyObject naming `.rift/` get 403; another bucket gets 404 |
| Exhausting the gateway | request body cap (413); caps on requests and connections; one request at a time per connection; client idle timeout, and a header deadline enforced by closing the socket (slowloris, slow TLS handshakes); header size and count limits; an unread body drained only up to 64 KiB, then the connection closed; GET and presigned requests can't carry a body; cold rebuilds and temp disk bounded | live: a `Content-Length` of 2^62 refused and the process fine; 40,000 pipelined requests while other clients were served; 70 MB body refused at 64 MB; slowloris and drip-fed handshakes closed within seconds; three presigned GETs with 300 MB bodies left the gateway at 2 MB RSS |
| Deflate bombs and oversized blobs | compaction stops unpacking at `--max-unpacked-mb` or 100 times the blob's size; a blob that crashed rift twice mid-compaction is left as it is; blobs left as they are aren't retried every round | live: a 1 GB bomb refused after 100 MB; two kills mid-compaction, then the blob is kept, not crash-looped |
| Losing or corrupting data | a blob is replaced only after rift rebuilds it from what it stored and checks its sha256; conditional writes (`If-Match`) so a blob deleted meanwhile isn't brought back; deleted packs kept for a grace period, and deleted only after a final check that no manifest uses them; one compactor at a time (a lock object with conditional writes), packs and manifests written only where nothing is; serving gateways go by what the bucket holds, never by what they remember; `rift eject` puts every original back | soak test on MinIO: 99 pushes, 37 `kill -9` (15 mid-compaction), 474 blobs verified, no mismatches, bucket audit clean; on AWS S3: every blob byte-exact, GC and eject checked, audit clean; with two compactors and two serving gateways on one bucket, 32 serving-gateway kills and 9 compactor kills (4 takeovers) over 25 minutes, 807 blobs verified, no mismatches, audit clean; a later run found a GC bug that lost two blobs after a takeover, fixed in 0.1.6, and two runs since (13 compactor kills, 8 takeovers, 912 blobs) were clean |
| Bucket settings that destroy rift's data | at start, rift warns about lifecycle rules that expire or archive `.rift/`, about versioning without expiry of noncurrent versions, and about Object Lock | live on MinIO and AWS S3 |
| Secrets in logs, forged log lines | signatures and tokens masked in debug logs, error lines and the access log, matched by their decoded names; control characters escaped; errors on rift's side give clients a generic message, and the details (which can name the backend) go to the log | tests; live: no signature in any log after signed and presigned traffic, encoded names included; a header carrying escape sequences logged as text |
| Eavesdropping | optional TLS (rustls 0.23: TLS 1.2 and 1.3); without it, rift belongs on a private network | test makes a real HTTPS request; live registry push over TLS 1.3 |
| Local users on a shared host | temp files created with `O_EXCL` and mode 0600 under unguessable names; the default state dir is under the user's home (`~/.cache/rift`), not the shared temp dir, and created 0700; the image runs as non-root on distroless (no shell) with a read-only root filesystem in the chart | test: a planted symlink is refused and its target untouched |
| Stolen bucket credentials | IAM roles supported (IRSA, Pod Identity, ECS, EC2) with temporary keys refreshed before expiry; least-privilege policy documented | live against every credential source |
| Compromised dependencies or build | dependencies checked against RustSec on every push and weekly; actions pinned to commits; base images pinned by digest; release images scanned (Trivy, fails on fixable critical issues), signed with cosign (keyless), with provenance and an SBOM (which lists the Rust crates from 0.1.4); Dependabot alerts and updates | 0.1.3 verified from outside with `cosign verify` and `gh attestation verify` |

## What rift sees

rift has to read image content to compact it, so whoever runs rift can see every blob passing
through, exactly as with the bucket itself. Cold reads and hot copies pass through temp files on
the gateway host: put that volume on encrypted disk. rift's own writes take the bucket's default
encryption.

## Known gaps

- **A paused compactor.** One compactor at a time is enforced by a lock object in the bucket with
  conditional writes; a compactor frozen past the lock's two minutes (a stopped VM) could finish a
  step it had started once it wakes, if a second compactor had taken over meanwhile. The chart
  runs one compactor. Serving gateways don't take part ([HA.md](HA.md)).
- **One shared client identity.** The client keys are a single shared secret between the registry
  and rift, with no per-user identity or authorization in rift. User-level access control remains
  the registry's. Rotating the keys means restarting rift with the new ones.
- **Metrics and health are unauthenticated.** They show counts and sizes, not names or keys; keep
  the port private.
- **Parsing untrusted compressed data.** The deflate parser (preflate, in Rust) handles data pushed
  by registry users. Unpacking is bounded, and manifests read from the bucket are fuzzed in tests,
  but the deflate parser isn't fuzzed by this project yet.
- **Vendored HTTP server.** tiny_http is vendored with patches (rustls 0.23, timeouts and limits)
  because upstream hasn't released since 2022, so its fixes come from this repository.
- **Ranged reads of compacted blobs.** A ranged GET of a compacted blob rebuilds the groups it
  touches (8 MB of content each, so `bytes=1-` rebuilds nearly all of it) and doesn't leave a hot
  copy, so repeated ranges cost a rebuild each. Rebuilds are capped
  (`--max-cold-reads`), so this can delay other cold reads but not exhaust the host. Registries
  rarely send ranges; docker resumes interrupted pulls with them.
- **No outside review yet.** An adversarial review of 0.1.3 in November 2026 found an
  unauthenticated crash (a huge `Content-Length`) and a way for a client-key holder to lose a
  compacted blob (PutObjectTagging), among others; all are fixed in 0.1.4. This document is meant
  to make an outside review quicker.
