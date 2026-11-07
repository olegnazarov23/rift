# rift

Byte-exact recompression for container registries and object storage.

`rift scan` tells you how much smaller your data would be. It only reads. Every blob it
recompresses is rebuilt in memory and compared with the original before it counts.

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

## Build

    cargo build --release

preflate-rs and lepton_jpeg are path dependencies on the lab's clones
(`../recompress-lab/tools/`); rift needs the `restart-points` branch of preflate-rs.
