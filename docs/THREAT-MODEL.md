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
| Unauthenticated requests | SigV4 required on every request, in the header or a presigned URL; signatures compared in constant time | unit tests on AWS's published signing vectors; live: unsigned and wrong-key requests get 403 |
| Replaying a captured request | header signatures valid within 15 minutes of rift's clock (`RequestTimeTooSkewed` otherwise); presigned URLs at most 7 days, not dated in the future | unit tests; live: 14 minutes accepted, 16 refused, both directions |
| Altered request bodies | signed payload hashes checked; every chunk of a signed `aws-chunked` upload checked against the chain of chunk signatures; trailing checksums passed to the bucket, which checks them | minio-go uploads accepted; the same upload with one byte flipped in transit refused, nothing written |
| Presigned URL misuse | GET and HEAD only, for the signed path and query; rift's presign parameters are stripped before forwarding | unit tests on verification; live: a presigned GET URL used for PUT gets 403 |
| Reaching rift's internal data or another bucket | requests for `.rift/` are refused (after decoding the path); one bucket per gateway, others get `NoSuchBucket` | live: signed GET and DELETE of a pack, and `%2Erift/…`, get 403 and the pack stays; another bucket gets 404 |
| Exhausting the gateway | request body cap (413); caps on requests and connections; client idle timeout and header deadline (slowloris); header size and count limits; cold rebuilds and temp disk bounded | live: 70 MB body refused at 64 MB; 150 slowloris connections against a cap of 100 dropped within 7 s and service resumed; slow downloads hold temp disk, not rebuild slots |
| Deflate bombs and oversized blobs | compaction stops unpacking at `--max-unpacked-mb` or 100 times the blob's size; a blob that crashed rift twice mid-compaction is left as it is; blobs left as they are aren't retried every round | live: a 1 GB bomb refused after 100 MB; two kills mid-compaction, then the blob is kept, not crash-looped |
| Losing or corrupting data | a blob is replaced only after rift rebuilds it from what it stored and checks its sha256; conditional writes (`If-Match`) so a blob deleted meanwhile isn't brought back; deleted packs kept for a grace period; `rift eject` puts every original back | soak test on MinIO: 99 pushes, 37 `kill -9` (15 mid-compaction), 474 blobs verified, no mismatches, bucket audit clean; on AWS S3: every blob byte-exact, GC and eject checked, audit clean |
| Bucket settings that destroy rift's data | at start, rift warns about lifecycle rules that expire or archive `.rift/`, about versioning without expiry of noncurrent versions, and about Object Lock | live on MinIO and AWS S3 |
| Secrets in logs | signatures and keys redacted in debug logs, error lines and the access log | tests; live: no signature or secret in any log after signed and presigned traffic |
| Eavesdropping | optional TLS (rustls 0.23: TLS 1.2 and 1.3); without it, rift belongs on a private network | test makes a real HTTPS request; live registry push over TLS 1.3 |
| Local users on a shared host | temp files created with `O_EXCL` and mode 0600 under unguessable names; default state dir 0700; the image runs as non-root on distroless (no shell) with a read-only root filesystem in the chart | test: a planted symlink is refused and its target untouched |
| Stolen bucket credentials | IAM roles supported (IRSA, Pod Identity, ECS, EC2) with temporary keys refreshed before expiry; least-privilege policy documented | live against every credential source |
| Compromised dependencies or build | dependencies checked against RustSec on every push and weekly; actions pinned to commits; base images pinned by digest; release images scanned (Trivy, fails on fixable critical issues), signed with cosign (keyless), with provenance and an SBOM (which lists the Rust crates from 0.1.4); Dependabot alerts and updates | 0.1.3 verified from outside with `cosign verify` and `gh attestation verify` |

## What rift sees

rift has to read image content to compact it, so whoever runs rift can see every blob passing
through, exactly as with the bucket itself. Cold reads and hot copies pass through temp files on
the gateway host: put that volume on encrypted disk. rift's own writes take the bucket's default
encryption.

## Known gaps

- **No high availability.** One gateway per bucket; while it's down, pulls of compacted blobs fail
  (recent blobs are still in the bucket as they were).
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
- **No outside review yet.** This document is meant to make one quicker.
