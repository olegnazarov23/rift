<h1>
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="docs/logo/rift-logo-dark.svg">
    <img alt="rift" src="docs/logo/rift-logo.svg" height="64">
  </picture>
</h1>

Byte-exact recompression for container registry storage. rift sits between your registry and the
S3 bucket it already uses, and stores image layers in less space: 39–79% less in our tests. Every
byte comes back exactly as it was pushed, so digests, signatures and pull commands don't change.

| Data (every blob read back and checked against its digest) | Registry sees | Stored | Saved |
|---|---|---|---|
| 21 versions of nginx, node, postgres, python and redis | 1,295 MB | 466 MB | 64% |
| redis 7.4.0–7.4.2 and postgres 17.2–17.4, distribution on AWS S3 | 577.7 MB | 239.1 MB | 58.6% |
| redis 7.4.1–7.4.2 and postgres 17.4, Harbor 2.14 | 249.1 MB | 138.2 MB | 44.5% |
| 30 consecutive CI builds of grafana-dev (the chunk store alone, no registry) | 11,072.5 MB | 2,277.5 MB | 79.4% |

The more versions a registry keeps, the more it saves: GitLab's registry with just redis 7.4.2 and
postgres 17.4, on AWS S3, saved 39.4%.

## How it works

Image layers are gzip, so a bucket can't compress them, and two versions of an image share almost
nothing once compressed. rift unpacks each layer with
[preflate](https://github.com/microsoft/preflate-rs), which records how the encoder compressed it
so the exact bytes can be produced again. It keeps the content as deduplicated chunks in zstd
packs, in the same bucket. A blob is replaced by an empty stub only after rift has rebuilt it from
what it stored and checked its digest, and a read rebuilds it as a stream.

The full picture, with what it costs and where it falls short: [docs/HOW-IT-WORKS.md](docs/HOW-IT-WORKS.md).

## Try it: how much would your registry save?

`rift scan` estimates the saving on a registry's images. It only reads, and it rebuilds every blob
it recompresses and compares it with the original before counting it.

    rift scan registry://docker.io/library/redis --latest 10
    rift scan registry://ghcr.io/OWNER/IMAGE --tags v1.2.0,v1.3.0
    RIFT_REGISTRY_USER=me RIFT_REGISTRY_PASSWORD=token rift scan registry://registry.example.com/team/app

Each digest is counted once, the way the registry stores it, so the saving is on top of the layer
sharing you already have. On 136 blobs from 21 images (1.29 GB) the scan predicted 466.3 MB;
building the real store from the same blobs took 468.7 MB.

It also scans a directory, such as a copy of a bucket: `rift scan /mnt/bucket-copy --max-gb 50`.
There, besides gzip, it counts JPEG recompressed with
[Lepton](https://github.com/microsoft/lepton_jpeg_rust) (17.5% smaller on our test photos) and
other files deduplicated as they are, and it lists byte-for-byte copies separately so they don't
inflate the number.

    --tags a,b,c     registry tags to scan (default: the --latest N most recent, oldest first)
    --platform       which image of a multi-arch tag (default linux/amd64)
    --max-gb N       stop after N GB of input
    --threads N      default: all cores
    --price P        $/GB-month for the cost line (default 0.023, S3 Standard)
    --level L        zstd level for the packs (default 18, as the store; 9 is faster, ~4 points low)
    --no-verify      skip the rebuild check (faster)
    --json           machine-readable report

## Run it

`rift gateway` is the S3 endpoint your registry talks to, in front of its existing bucket:

    RIFT_BACKEND_ACCESS_KEY=… RIFT_BACKEND_SECRET_KEY=… \
    RIFT_CLIENT_ACCESS_KEY=… RIFT_CLIENT_SECRET_KEY=… \
    rift gateway --backend https://s3.us-east-1.amazonaws.com --bucket my-registry --cold-after 7d

Point the registry's S3 storage at it (distribution, Harbor and GitLab's registry all take an
endpoint), sign with the client keys, and keep the bucket name. Instead of backend keys, rift can
use an AWS role (IRSA, EKS Pod Identity, ECS task roles, EC2 instance profiles). The Helm chart in
`deploy/helm/rift` runs two serving gateways and one compactor, so losing a gateway doesn't
interrupt the registry. The image is `ghcr.io/olegnazarov23/rift:0.1.6`, for amd64 and arm64.

What the gateway does:

- Passes requests through to the bucket, re-signed with its own credentials.
- Compacts blobs nobody has written or read for `--cold-after`, about 19 MB/s on a 12-core server,
  the repositories with the most versions first.
- Rebuilds compacted blobs as they're read, byte ranges included, and keeps a hot copy while a blob
  is in use.
- Reclaims the space of deleted blobs with its own garbage collection.
- Checks every request's SigV4 signature and passes on only the S3 operations registries use.
- Serves Prometheus metrics at `/-/metrics` (stored against logical bytes, compaction, cold reads,
  GC) and health at `/-/healthz`.
- Warns at start about bucket settings that would delete its data or cancel the saving.
- Leaves on request: `rift eject` puts every original back and removes rift's data.

Running a pilot (Docker, Helm, registry settings, limits, leaving): [docs/PILOT.md](docs/PILOT.md).

## Status

Early: version 0.1.6, which also fixes a GC bug in earlier versions that could lose blobs after a
restart, so upgrade. Tested end to end with distribution 2.8, Harbor 2.14 and GitLab's registry
4.41 (both metadata modes) on MinIO, and with distribution and GitLab's registry on AWS S3, driven
from a laptop. Not yet run in-region at scale, and no outside security review yet. Only gzip
layers are compacted, a cold read costs CPU, and one compactor works on a bucket at a time.

## Documentation

- [How it works](docs/HOW-IT-WORKS.md): the design, results, costs and limits
- [Pilot guide](docs/PILOT.md): running rift in front of your registry
- [High availability](docs/HA.md): several gateways on one bucket
- [Threat model](docs/THREAT-MODEL.md): what rift protects, and how
- [Security](SECURITY.md): reporting vulnerabilities, verifying releases

## Build

    cargo build --release          # or: docker build -t rift .

preflate-rs is vendored in `vendor/preflate-rs`: our branch of microsoft/preflate-rs with restart
points, refreshed with `scripts/vendor-preflate.sh`. Lepton comes from crates.io. Pushing a `v*`
tag builds, signs and publishes a release image.

## License

Apache-2.0, see [LICENSE](LICENSE). `vendor/preflate-rs` is Microsoft's preflate-rs, also
Apache-2.0, with our changes ([NOTICE](NOTICE)).
