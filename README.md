# rift

Byte-exact recompression for container registries and object storage.

- `rift gateway` is an S3 endpoint in front of the bucket your registry already uses
  (distribution, Harbor, GitLab). Blobs nobody has touched for a while are stored smaller, and
  every byte comes back exactly as it was pushed, so digests never change.
- `rift scan` tells you how much smaller your data would be. It only reads. Every blob it
  recompresses is rebuilt in memory and compared with the original before it counts.

**Status: early.** Tested end to end on MinIO under distribution 2.8, Harbor 2.14 and GitLab's
registry 4.41: 39–59% less storage on our test images. The same chunk store saved 79% on 30
consecutive CI builds of one project. Not yet run on AWS S3 at scale. To try it on your registry, see
[docs/PILOT.md](docs/PILOT.md).

## Scan a registry

    rift scan registry://docker.io/library/redis --latest 10
    rift scan registry://ghcr.io/OWNER/IMAGE --tags v1.2.0,v1.3.0
    RIFT_REGISTRY_USER=me RIFT_REGISTRY_PASSWORD=token rift scan registry://registry.example.com/team/app

Each digest is counted once, the way the registry stores it, so the savings are on top of
the layer sharing you already have.

## Scan a directory

    rift scan /mnt/bucket-copy --max-gb 50

Files that are byte-for-byte copies of other files are counted once and listed separately,
so they don't inflate the number.

## What happens to each blob

- **gzip** (image layers, `.gz` logs and exports): the deflate stream is unpacked, with a small
  recipe that recreates it bit for bit ([preflate](https://github.com/microsoft/preflate-rs)).
  The unpacked content goes into a content-defined chunk store (FastCDC, 64 KB average):
  repeats across blobs are kept once, and new chunks are packed and compressed with zstd.
- **JPEG**: [Lepton](https://github.com/microsoft/lepton_jpeg_rust), about 22% smaller.
- **Anything else** goes through the same chunk store as it is.

A gzip stream that rift can't rebuild exactly (multi-member files, some encoders) is kept as
is, and the report says how many there were and why.

## Options

    --tags a,b,c     registry tags to scan (default: the --latest N most recent, oldest first)
    --platform       which image of a multi-arch tag (default linux/amd64)
    --max-gb N       stop after N GB of input
    --threads N      default: all cores
    --price P        $/GB-month for the cost line (default 0.023, S3 Standard)
    --level L        zstd level for the packs (default 19, as the store; 9 is faster, ~4 points low)
    --no-verify      skip the rebuild check (faster)
    --json           machine-readable report

## How close is the estimate

On 136 unique blobs from 21 Docker Hub images (1.29 GB), the scan predicted 466.3 MB.
Building the real chunk store from the same blobs took 468.7 MB. With `--level 9` the scan
predicts 522.7 MB: faster, and pessimistic by about 4 points.

## Gateway

`rift gateway` is the S3 endpoint your registry talks to, in front of its existing bucket:

    RIFT_BACKEND_ACCESS_KEY=… RIFT_BACKEND_SECRET_KEY=… \
    RIFT_CLIENT_ACCESS_KEY=… RIFT_CLIENT_SECRET_KEY=… \
    rift gateway --backend https://s3.us-east-1.amazonaws.com --bucket my-registry --cold-after 7d

Point the registry's S3 storage at it (distribution, Harbor and GitLab all take a `regionendpoint`),
sign with the client keys, and set `redirect: disable: true`. Instead of backend keys, rift can use
an AWS role: IRSA, EKS Pod Identity, ECS task roles or EC2 instance profiles.

- Requests pass through to the bucket, re-signed with the backend keys.
- Blobs nobody has written or read for `--cold-after` are compacted. preflate unpacks them, the
  content goes into deduplicated packs under `.rift/` in the same bucket, and each blob is rebuilt
  from what was stored and checked against its digest. Only then is the original replaced by an
  empty stub.
- A read of a compacted blob streams the rebuild, byte ranges included. Listings show the original
  size and date. Deleting a blob drops its manifest.
- Chunk GC runs after deletes. Packs nothing uses are retired; packs under half used
  (`--gc-rewrite-below`) have their live chunks copied out first. Retired packs stay readable for
  `--gc-grace` (default 10m), so reads already streaming finish, then they're deleted.
- After a full cold read, the original is put back in the bucket (a hot copy) until nobody has read
  it for `--cold-after`; then the stub goes back (`--no-hot-copy` turns this off).
- With `--redirect-endpoint`, a registry that redirects blob downloads to storage keeps doing so:
  blobs stored as they are get a presigned redirect to the backend, and only cold ones stream
  through rift.
- `GET /-/metrics` (Prometheus: stored vs logical bytes, compaction, cold reads, GC) and
  `GET /-/healthz`; `rift eject` puts every original back and removes rift's data.
- Every request must be signed with the client keys within 15 minutes of rift's clock; bodies are
  checked against their signed hash, chunk signatures and checksums. `--tls-cert`/`--tls-key` serve
  HTTPS. `--max-body-mb`, `--max-requests`, `--max-cold-reads` and `--max-spill-gb` bound what one
  client can take (details in [docs/PILOT.md](docs/PILOT.md#5-security)).

Running it for a pilot (Docker, Helm, registry settings, leaving): [docs/PILOT.md](docs/PILOT.md).
Not done yet: more than one gateway per bucket.

Tested locally against distribution 2.8.3 and MinIO with 3 redis and 3 postgres images (69 blobs,
577.7 MB). The bucket held 238.6 MB, 58.7% less. Every blob read back through the registry was
byte-exact, `docker pull` from scratch worked and the image ran, and registry garbage collection
deleted compacted blobs cleanly. The same under Harbor v2.14.6 (its `storage_service.s3` pointed at
rift): 3 images, 249.1 MB stored in 138.2 MB, every blob byte-exact, `docker pull` from scratch fine.
Deleting 3 of the 6 images and running both garbage collectors took the first bucket from 237.7 MB
to 146.9 MB for the remaining 249.1 MB, close to the 138.2 MB of a fresh store of the same images;
every remaining blob stayed byte-exact.

## Build

    cargo build --release          # or: docker build -t rift:0.1.0 .

Or use the published image, `ghcr.io/olegnazarov23/rift:0.1.0` (amd64 and arm64). Pushing a `v*` tag
publishes a new one.

preflate-rs is vendored in `vendor/preflate-rs` (our branch of microsoft/preflate-rs with restart
points; refresh it with `scripts/vendor-preflate.sh`). Lepton comes from crates.io. A Helm chart is
in `deploy/helm/rift`.

## License

Apache-2.0, see [LICENSE](LICENSE). `vendor/preflate-rs` is Microsoft's preflate-rs, also
Apache-2.0, with our changes ([NOTICE](NOTICE)). Security reports: [SECURITY.md](SECURITY.md).
