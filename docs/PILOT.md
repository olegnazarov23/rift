# rift pilot guide

rift sits between your registry and the S3 bucket it already uses. Your registry keeps working as
before. Blobs nobody has touched for a while are stored smaller, and every byte comes back exactly
as it was pushed, so digests never change. Nothing in the bucket is replaced until rift has rebuilt
the blob from what it stored and checked it against its digest.

## 1. Measure first

`rift scan` reads a registry and reports what rift would save. It never writes.

    RIFT_REGISTRY_USER=… RIFT_REGISTRY_PASSWORD=… \
      rift scan registry://registry.example.com/team/app --latest 20

On our tests: 64% less on official Docker Hub images, 79% on 30 consecutive CI builds.

## 2. What you need

- A registry that stores in S3: distribution (the Docker registry), Harbor, GitLab self-managed,
  or anything else with an S3 storage setting.
- The bucket, and a key for rift with GetObject, PutObject, DeleteObject, ListBucket and the
  multipart actions on it. rift keeps its data in the same bucket, under `.rift/`.
- Somewhere to run rift next to the registry, in the bucket's region: 2 or more cores, 4–8 GB of
  memory, temp space for your largest layer, and local disk for the chunk index (about 60 bytes per
  stored chunk, roughly 1 GB per TB stored). The index is rebuilt from the bucket if it's lost; keep
  it on a persistent volume (`state.persistence.enabled` in the chart) to skip that on restarts.

## 3. Run rift

Build the image with `docker build -t rift:0.1.0 .` and push it where your cluster can pull it
(set `image.repository` in the chart). Two sets of keys go in the environment:

| Variable | What it is |
|---|---|
| `RIFT_BACKEND_ACCESS_KEY`, `RIFT_BACKEND_SECRET_KEY` | rift's key to the bucket |
| `RIFT_CLIENT_ACCESS_KEY`, `RIFT_CLIENT_SECRET_KEY` | keys you make up; the registry signs its requests to rift with them |

With Helm:

    kubectl create secret generic rift-keys \
      --from-literal=RIFT_BACKEND_ACCESS_KEY=… --from-literal=RIFT_BACKEND_SECRET_KEY=… \
      --from-literal=RIFT_CLIENT_ACCESS_KEY=… --from-literal=RIFT_CLIENT_SECRET_KEY=…
    helm install rift deploy/helm/rift \
      --set backend.endpoint=https://s3.us-east-1.amazonaws.com \
      --set backend.bucket=my-registry --set credentials.existingSecret=rift-keys

With Docker:

    docker run -d --name rift -p 9100:9100 --env-file rift.env rift:0.1.0 \
      gateway --listen 0.0.0.0:9100 --backend https://s3.us-east-1.amazonaws.com --bucket my-registry

## 4. Point the registry at rift

Keep the bucket name, swap the endpoint for rift's, sign with the client keys, use path-style
addressing, and turn storage redirects off to start with.

distribution (`config.yml`):

```yaml
storage:
  s3:
    accesskey: <client access key>
    secretkey: <client secret key>
    region: us-east-1
    regionendpoint: http://rift:9100
    bucket: my-registry
    forcepathstyle: true
    secure: false
    v4auth: true
  redirect:
    disable: true
```

Harbor: the same keys under `storage_service.s3` in `harbor.yml` (then `./prepare` and
`docker compose up -d`), or under `persistence.imageChartStorage.s3` in Harbor's Helm chart.

GitLab (`gitlab.rb`). Use the `s3_v2` driver, the one GitLab is moving to:

```ruby
registry['storage'] = {
  's3_v2' => {
    'accesskey' => '<client access key>', 'secretkey' => '<client secret key>',
    'bucket' => 'my-registry', 'region' => 'us-east-1',
    'regionendpoint' => 'http://rift:9100', 'pathstyle' => true, 'secure' => false
  },
  'redirect' => { 'disable' => true }
}
```

`s3_v2` sends request checksums by default. Leave them on in front of AWS S3. If the bucket is on
MinIO, set `'checksum_disabled' => true` as GitLab's docs say: MinIO rejects the multipart copies
GitLab makes for blobs over 32 MB when checksums are on, with or without rift in between. Tested
with GitLab's registry 4.41 in its classic (non-database) metadata mode; the metadata-database
mode isn't tested yet.

**Redirects (optional, later).** Registries can send clients straight to storage for blob
downloads. To keep that, turn redirects back on and start rift with
`--redirect-endpoint https://s3.us-east-1.amazonaws.com`. Blobs stored as they are then download
from S3 directly; only cold ones stream through rift. Your nodes must be able to reach rift's
endpoint as well as the registry.

## 5. What happens next

- **Right away:** nothing changes. rift passes every request through to the bucket.
- **After `--cold-after` (default 7 days with no write or read):** blobs are compacted. preflate
  unpacks each gzip layer with a recipe to recreate it bit for bit; the content goes into
  deduplicated packs. Each blob is rebuilt from what was stored and checked against its digest,
  and only then is the original replaced with an empty stub.
- **A pull of a compacted blob** is rebuilt as it streams (first byte in under a second). rift then
  puts the original back in the bucket until the blob goes cold again, so repeat pulls are full
  speed.
- **Deletes and registry garbage collection** work as before. rift drops the deleted blobs and
  reclaims their packs after `--gc-grace` (default 10 minutes).

## 6. Watching it

- `GET /-/metrics` is Prometheus format (the chart adds scrape annotations). The ones that matter:
  - `rift_bucket_logical_bytes`, `rift_bucket_stored_bytes` and `rift_bucket_saved_ratio`: what the
    registry sees against what's stored, updated on every compactor round. This is the saving.
  - `rift_compactions_total`, `rift_compaction_failures_total`: progress, and blobs left as they were.
  - `rift_cold_reads_total`, `rift_cold_read_seconds_total`, `rift_hot_copies_made_total`,
    `rift_redirects_total`: how pulls are served.
  - `rift_backend_errors_total`, `rift_cold_read_errors_total`: should stay at zero.
- `GET /-/healthz` answers `{"status":"ok","compacted":N}`.
- The log has a line for every compaction batch, garbage collection, and hot copy stubbed again.

## 7. Leaving

1. Stop rift.
2. Run `rift eject --backend https://s3.us-east-1.amazonaws.com --bucket my-registry --yes`, with
   the backend keys in the environment. It rebuilds every compacted blob, checks it against its
   digest and puts the original back, then deletes `.rift/`. If any blob fails, nothing is deleted;
   fix the cause and run it again.
3. Point the registry back at the bucket.

## 8. Limits of this pilot build

- One rift per bucket; there's no high availability yet. While rift is down, pulls of compacted
  blobs wait for it (recent blobs are in the bucket as they were).
- Compaction runs at about 0.5 MB/s per core. A large registry takes a while to backfill; new
  pushes are a small daily stream.
- A cold pull of a big layer on few cores is slower than a plain download, about 2–3× on 6 cores.
- gzip layers only; zstd and eStargz layers are left as they are.
- Tested end to end with distribution 2.8.3, Harbor 2.14.6 and GitLab's registry 4.41 (classic
  metadata) on MinIO; not yet on AWS S3 at scale.
- No metrics endpoint yet, only logs.
