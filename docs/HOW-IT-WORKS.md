# How rift works

rift is an S3 endpoint you put between a container registry and the bucket it already uses. Blobs
nobody has touched for a while are stored in less space, and every byte comes back exactly as it
was pushed, so digests, signatures and pull commands don't change.

## The problem

Registry buckets only grow. Garbage collection reclaims what nothing refers to any more, and
everything else stays.

Two things make that storage hard to shrink:

- **Layers are gzip.** The bucket can't compress them further, and a storage layer that compresses
  or deduplicates blocks finds nothing to work with.
- **Versions share almost nothing once compressed.** Two builds of an image hold mostly the same
  files, but a compressed stream depends on everything before it, so their layers differ almost
  everywhere after the first change. The registry deduplicates whole blobs by digest, and anything
  short of identical is stored again. A registry with 500 CI builds of one app holds 500 nearly
  identical copies of it.

Unpacking the layers and storing the content would fix both, but recompressing them would change
their bytes, and with them their digests and every image that refers to them.

## The trick: undo gzip without losing it

[preflate](https://github.com/microsoft/preflate-rs) (Microsoft) decompresses a deflate stream and
records the choices the original encoder made: where it found its matches and how it built its
Huffman tables. That record, the recipe, is small, typically a few percent of the blob. Given the
plain content and the recipe, preflate produces the original compressed bytes again, bit for bit.

So rift can keep a layer as its plain content plus its recipe, and store the content any way it
likes. It cuts the content into chunks and keeps each distinct chunk once:

```
layer (gzip, 30 MB)
  │  preflate: unpack, and record how the encoder compressed it
  ├─► recipe (about 2% of the layer) ─────────────────┐
  └─► plain content (a tar, 90 MB)                     │
        │  FastCDC: cut into chunks of about 64 KB    │
        ├─► chunks already stored: references only     │
        └─► new chunks: added to packs (zstd)          │
                                                       ▼
            manifest: gzip header and trailer, the recipe, the list of chunks
```

Chunk boundaries come from the content (content-defined chunking), so an inserted or changed file
shifts only the chunks around it, and the rest of the layer matches what's already stored. Two
versions of an image, or two CI builds, mostly share chunks.

Recompression alone saves about a fifth of a layer. Most of the saving comes from deduplication,
so it grows with history: the more versions a registry keeps, the more each new one shares with
what's stored.

## What's in the bucket

Everything stays in the registry's own bucket:

| Object | What it is |
|---|---|
| `.rift/packs/NNNNNNNN.zst` | 16 MB of unique chunks, compressed with zstd |
| `.rift/packs/NNNNNNNN.idx` | where each chunk is in its pack, by its hash |
| `.rift/m/<key>@<size>@<date>` | a compacted blob's manifest |
| the blob's own key | an empty stub carrying the blob's size and date, so listings and the registry still see the blob |

Nothing else changes. The registry's manifests, tags and small blobs are left as they are.

## Compaction

A gateway compacts the bucket in rounds, every hour by default:

1. It lists the bucket and picks the blobs nobody has written or read for `--cold-after` (7 days by
   default), 1 MB and up.
2. It takes them in order of how much they're likely to save: first the layers of the repositories
   with the most layers (versions that share most of their content), then the biggest.
3. In batches of about 2 GB, it downloads each blob, checks it against its digest, unpacks it with
   preflate, and chunks it into the packs. Several blobs are unpacked at once, within a memory budget.
4. It rebuilds every blob of the batch from what's now in the bucket and checks it against its
   digest again. Only if it matches is the manifest written and the original replaced by a stub,
   and only if the original hasn't changed meanwhile.

A blob that isn't gzip, or that preflate can't rebuild exactly (some encoders, multi-member gzip),
stays as it is, and so does one that unpacks to more than `--max-unpacked-mb` or a hundred times
its size (a deflate bomb).

On a 12-core server (Ryzen 9 5900X), the layers of 21 versions of official images (1.3 GB) were
compacted in 67 seconds, 19 MB/s, to 64% less storage. Each batch commits as it finishes, so a
backfill saves as it goes, and an interruption loses one batch at most.

## Reads

The registry reads blobs through rift. rift forwards the read to the bucket and looks at what comes
back:

- **The blob as it is** (not compacted, or a hot copy): passed through.
- **A stub:** rift loads the blob's manifest and rebuilds the blob. The plain content is split
  into groups of about 8 MB that rebuild independently, so they rebuild in parallel and stream out
  in order. For typical layers the first byte goes out in under a second. On a 6-core laptop, a
  113 MB postgres layer streamed at 25 MB/s; downloads from Docker Hub on the same connection ran
  at 64–69 MB/s. Byte ranges work, and a range rebuilds only the groups it touches.

After a full cold read, rift puts the original back in the bucket, a hot copy, so later pulls are
plain downloads. Once nobody has read it for `--cold-after`, the stub goes back.

Listings and HEAD requests report each compacted blob's original size and date. With
`--redirect-endpoint`, a registry that redirects downloads to the bucket keeps doing so for blobs
stored as they are, and only cold ones stream through rift.

## Deletes and garbage collection

When the registry deletes a blob, rift drops its manifest. rift's own GC then reclaims chunks no
manifest uses:

- A pack nothing uses is retired.
- A pack less than half used has its live chunks copied to new packs and its blobs pointed there,
  and is then retired.
- Retired packs stay readable for `--gc-grace` (10 minutes by default), so reads already
  streaming finish, and are deleted only after a final check that no manifest uses them.

## Several gateways

Several gateways can serve one bucket (`--serve-only`) while one of them compacts it. A serving
gateway doesn't trust what it remembers, since another may have compacted, cooled or deleted a blob
a moment ago, so every blob read goes by what the bucket holds. The compactor holds a lock object in
the bucket, renewed every 20 seconds with conditional writes; a standby takes over if it goes quiet
for two minutes. The Helm chart runs two serving gateways and one compactor. Details:
[HA.md](HA.md).

## How it stays safe

- A blob's original is replaced only after rift has rebuilt it from what it stored and checked its
  sha256.
- Writes are conditional: a stub replaces only the version rift read, and packs and manifests are
  written only where nothing is.
- Deleted packs wait out a grace period and a final check that nothing uses them.
- One compactor at a time, enforced in the bucket.
- `rift eject` rebuilds every blob, puts every original back and removes rift's data.

Requests are authenticated with SigV4, and only the S3 operations registries use are passed on.
The full list, with evidence for each control: [THREAT-MODEL.md](THREAT-MODEL.md).

## Results

Every blob below was read back and checked against its digest.

| Data | Setup | Registry sees | Stored | Saved |
|---|---|---|---|---|
| 21 versions of nginx, node, postgres, python and redis (136 layers) | rift on a 12-core server, local S3 store | 1,295 MB | 466 MB | 64% |
| redis 7.4.0–7.4.2, postgres 17.2–17.4 | distribution → rift → AWS S3 | 577.7 MB | 239.1 MB | 58.6% |
| redis 7.4.1–7.4.2, postgres 17.4 | Harbor 2.14 → rift → MinIO | 249.1 MB | 138.2 MB | 44.5% |
| 30 consecutive CI builds of grafana-dev | the chunk store alone | 11,072.5 MB | 2,277.5 MB | 79.4% |

GitLab's registry, in its metadata-database mode on AWS S3 with its default checksums, stored redis
7.4.2 and postgres 17.4 in 39.4% less. The fewer versions there are, the less they share.

## What it costs

- **CPU.** Compaction ran at 19 MB/s on 12 cores. A cold read rebuilds the blob, about a quarter of
  a core-second per MB; hot copies keep that to the first read of a blob gone cold.
- **Requests.** A compaction round lists the bucket, one LIST per 1,000 objects. A cold read adds
  a request or two. Several gateways add about a dollar a month each in S3 requests.
- **Memory.** Compaction holds at most `--max-unpacked-mb` (2 GB by default) of plain content.

## Limits

- Only gzip layers are compacted; zstd and eStargz layers are left as they are.
- A cold read of a big layer on few cores is slower than a plain download.
- One compactor per bucket at a time: compaction scales with the cores of one machine.
- It's early. Tested end to end with distribution, Harbor and GitLab's registry on MinIO, and with
  distribution and GitLab's registry on AWS S3, driven from a laptop. Not yet run in-region at
  scale, and no outside review yet.

## Try it

- `rift scan registry://docker.io/library/redis --latest 10` estimates the saving on any
  registry's images. It only reads, and it rebuilds every blob it counts. On the 1.3 GB corpus
  above, the scan predicted 466.3 MB; the store took 468.7 MB.
- To put rift in front of a registry: [PILOT.md](PILOT.md).
