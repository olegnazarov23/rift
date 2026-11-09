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
- The bucket, and access to it for rift: an IAM role (IRSA, EKS Pod Identity, ECS task role or
  EC2 instance profile) or a key, with the policy below. rift keeps its data in the same bucket,
  under `.rift/`.
- Somewhere to run rift next to the registry, in the bucket's region: 2 or more cores; memory for
  compaction, which peaks near 2.5 times the largest unpacked blob it takes on (`--max-unpacked-mb`,
  default 2048) plus about 135 MB per thread, so 8 GB for the defaults with 4 threads; temp space for
  your largest layer, and local disk for the chunk index (about 60 bytes per
  stored chunk, roughly 1 GB per TB stored). The index is rebuilt from the bucket if it's lost; keep
  it on a persistent volume (`state.persistence.enabled` in the chart) to skip that on restarts.

## 3. Run rift

The image is `ghcr.io/olegnazarov23/rift:0.1.5`, for amd64 and arm64; the chart uses it by default.
To build your own, run `docker build -t rift:0.1.0 .` and set `image.repository`. Two sets of keys go
in the environment:

| Variable | What it is |
|---|---|
| `RIFT_BACKEND_ACCESS_KEY`, `RIFT_BACKEND_SECRET_KEY` | rift's key to the bucket; leave them out to use an IAM role |
| `RIFT_CLIENT_ACCESS_KEY`, `RIFT_CLIENT_SECRET_KEY` | keys you make up (the secret at least 16 characters: `openssl rand -hex 20`); the registry signs its requests to rift with them. rift won't start without them |

**IAM roles instead of a key.** Without `RIFT_BACKEND_*`, rift finds credentials the way AWS's SDKs
do: `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY` (with `AWS_SESSION_TOKEN`), then IRSA, EKS Pod
Identity or ECS, then the EC2 instance role. Temporary keys are fetched again 15 minutes before
they expire. With the chart, leave `credentials.backendAccessKey` empty, and either annotate the
service account for IRSA (`--set serviceAccount.annotations."eks\.amazonaws\.com/role-arn"=arn:aws:iam::<account>:role/rift`)
or add a Pod Identity association for it. The role or key needs:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {"Effect": "Allow",
     "Action": ["s3:ListBucket", "s3:ListBucketMultipartUploads", "s3:GetBucketVersioning",
                "s3:GetLifecycleConfiguration", "s3:GetBucketObjectLockConfiguration"],
     "Resource": "arn:aws:s3:::my-registry"},
    {"Effect": "Allow",
     "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:AbortMultipartUpload", "s3:ListMultipartUploadParts"],
     "Resource": "arn:aws:s3:::my-registry/*"}
  ]
}
```

With Helm:

    kubectl create secret generic rift-keys \
      --from-literal=RIFT_BACKEND_ACCESS_KEY=… --from-literal=RIFT_BACKEND_SECRET_KEY=… \
      --from-literal=RIFT_CLIENT_ACCESS_KEY=… --from-literal=RIFT_CLIENT_SECRET_KEY=…
    helm install rift deploy/helm/rift \
      --set backend.endpoint=https://s3.us-east-1.amazonaws.com \
      --set backend.bucket=my-registry --set credentials.existingSecret=rift-keys

The chart runs two gateways that only serve, behind the Service, and one compactor
([High availability](HA.md)): losing a serving gateway, or a rolling update, doesn't interrupt the
registry. `--set serve.replicas=0` runs the compactor alone, serving too.

With Docker:

    docker run -d --name rift -p 9100:9100 --env-file rift.env ghcr.io/olegnazarov23/rift:0.1.5 \
      gateway --listen 0.0.0.0:9100 --backend https://s3.us-east-1.amazonaws.com --bucket my-registry

More gateways for the same bucket take `--serve-only` (they need no state dir) and go behind one
load balancer; the one without it compacts. Only one gateway compacts at a time: a second one
without `--serve-only` stands by and takes over if the first stops for two minutes.

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
with GitLab's registry 4.41 in both metadata modes: classic, and the metadata database GitLab is
moving to, whose online garbage collection deletes through rift.

**Redirects (optional, later).** Registries can send clients straight to storage for blob
downloads. To keep that, turn redirects back on and start rift with
`--redirect-endpoint https://s3.us-east-1.amazonaws.com`. Blobs stored as they are then download
from S3 directly; only cold ones stream through rift. Your nodes must be able to reach rift's
endpoint as well as the registry.

## 5. Security

**What rift checks on every request**

- The signature: SigV4 with the client keys, in the header or a presigned URL. Anything unsigned or
  signed with other keys gets 403. A request signed more than 15 minutes from rift's clock gets 403
  `RequestTimeTooSkewed` (what S3 says; AWS SDKs correct their clock and retry), so a captured
  request can't be replayed later.
- The body against what was signed: a signed payload hash is checked (400
  `XAmzContentSHA256Mismatch`), every chunk of a signed `aws-chunked` upload is checked against its
  chunk signature, and trailing checksums (`x-amz-checksum-*`) go on to the bucket, which checks them.
- Headers that change what's stored or how a body is read (`x-amz-*`, `Content-MD5`,
  `Content-Encoding`) must be signed, as S3 requires; otherwise 403.
- Only the requests registries make are passed on: object reads, writes, copies and deletes,
  multipart uploads and listings. Everything else (tagging, ACLs, versions, bucket policy and other
  bucket settings) gets 501 `NotImplemented`, whatever the keys. Change bucket settings at the
  bucket itself.
- `.rift/` isn't reachable through rift, so clients can't read or overwrite packs and manifests.
  Keys that start with `/` or hold empty, `.` or `..` segments get 400; registries never use them.
- Logs never contain signatures or keys, also with `RIFT_DEBUG=1` (every request, with headers)
  and `--access-log` (a JSON audit line per request; see Watching it), and control characters in
  requests are escaped. Errors on rift's side tell the client only what failed; the details are in
  rift's log.

**Limits.** Past these, a client gets 413 or 503 `SlowDown`; registries and S3 clients retry 503s.

| Flag | Default | Bounds |
|---|---|---|
| `--max-body-mb` | 1024 | one request body (registries upload in parts far smaller) |
| `--max-requests` | 512 | requests in flight |
| `--max-cold-reads` | 4 | rebuilds at once; more wait up to a minute |
| `--max-spill-gb` | 16 | temp disk for rebuilt blobs waiting on slow clients; keep it under the temp volume (the chart's `/tmp` is 20Gi) |
| `--max-unpacked-mb` | 2048 | memory for unpacking one blob during compaction |
| `--client-timeout-secs` | 60 | how long a client may stall a read or write (the TLS handshake included), and may take to send a request's headers |
| `--max-connections` | 1024 | connections served at once; more are closed as they arrive |

Request headers are capped at 64 KB and 200 lines. A client that connects and stalls, or drips its
headers (slowloris), is dropped after `--client-timeout-secs`, so it can hold a connection only that
long; a flood past `--max-connections` is refused until those time out. A connection carries one
request at a time. When rift answers before reading a body (a refused upload, say), it reads and
drops at most 64 KB of it and otherwise closes the connection.

rift unpacks layers to compact them, which a registry never does, so a pushed layer could be a
deflate bomb (a few MB that unpack to gigabytes). Compaction stops unpacking at `--max-unpacked-mb`
or at 100 times the blob's size, whichever is less, and leaves such blobs as they are. A blob that
rift stopped twice while compacting (killed for memory, say) is left as it is too, so it can't
crash rift over and over. Blobs left as they are aren't downloaded and tried again on every round.

A rebuild writes to a temp file as fast as it can and the client reads it at its own pace, so a slow
download holds temp disk, never a rebuild slot. Temp files a killed rift leaves behind are deleted
after an hour.

**TLS.** `--tls-cert cert.pem --tls-key key.pem` serves HTTPS (the certificate file can carry
intermediates). With the chart: `kubectl create secret tls rift-tls --cert=… --key=…` and
`--set tls.secretName=rift-tls`. Then give the registry `https://rift:9100` and `secure: true`
(GitLab: `'secure' => true`), and make sure it trusts the CA. Without TLS, keep rift on a private
network: signatures stop tampering and replays, but blob contents cross the network in the clear.

**Keys and network**

- Prefer an IAM role to a backend key; either way it's the only thing that reaches the bucket
  itself, so scope it to that bucket (the policy in section 3). The client keys only work at rift.
  Rotate them by changing the Secret and the registry's config together; rift reads keys at start.
- `--state-dir` holds the chunk index: keep it writable only by rift's user (the chart's is). The
  default is `~/.cache/rift/<bucket>`, not the shared temp dir.
- For a local trial without client keys, `--no-client-auth` accepts unsigned requests, but only on
  a loopback address such as `127.0.0.1`.
- Only the registry needs to reach rift, plus the machines that pull if redirects are on (presigned
  URLs point at rift). Admit just those with a NetworkPolicy or security group; the limits above
  bound what anyone else could do.
- `/-/metrics` and `/-/healthz` answer without a signature (counts and sizes, no keys or names) on
  the same port; keep them inside too.
- Encryption at rest is the bucket's: see Bucket settings below.

## 6. Bucket settings

rift keeps its data in the registry's bucket, under `.rift/`, so some bucket settings change what
it saves or put its data at risk. It checks these when it starts and logs a warning for each problem
(`rift_bucket_setting_warnings` counts them). The three bucket reads it needs for that are in the
policy above.

- **Lifecycle rules must leave `.rift/` alone.** A rule that expires objects there deletes rift's
  packs and manifests, and the compacted blobs with them. A rule that moves them to Glacier Flexible
  Retrieval or Deep Archive makes compacted blobs unreadable until restored. Scope rules to your
  registry's own prefix (`docker/` for distribution and Harbor). Standard-IA, One Zone-IA, Glacier
  Instant Retrieval and Intelligent-Tiering without its archive tiers are fine for `.rift/`.
- **Versioning keeps what rift removes.** With versioning on, the originals rift replaces with stubs,
  and the packs its GC deletes, stay as noncurrent versions, so nothing is saved until they expire.
  Add a rule that expires noncurrent versions, say after 7 days. Until they expire, those versions are
  also a way back.
- **Object Lock** holds those versions until their retention ends; savings show up only after that.
- **Encryption:** rift's own writes (packs, manifests, stubs, hot copies) carry no encryption
  headers, so they get the bucket's default encryption (SSE-S3 or SSE-KMS). With SSE-KMS, the role
  needs `kms:GenerateDataKey` and `kms:Decrypt` on the key. A bucket policy that demands
  `x-amz-server-side-encryption` on every PUT refuses rift's writes; use default encryption instead.
  The registry's own encryption settings pass through unchanged.
- **Replication** copies packs, manifests and stubs like any other object. A registry reading a
  replica needs its own rift in front of it, since the stubs there are empty too.
- On the gateway host, cold reads and hot copies pass through temp files (`--max-spill-gb`) holding
  blob bytes. Put that volume on encrypted disk if your images hold anything sensitive.

## 7. What happens next

- **Right away:** nothing changes. rift passes every request through to the bucket.
- **After `--cold-after` (default 7 days with no write or read):** blobs are compacted. preflate
  unpacks each gzip layer with a recipe to recreate it bit for bit; the content goes into
  deduplicated packs. Each blob is rebuilt from what was stored and checked against its digest,
  and only then is the original replaced with an empty stub.
- **A pull of a compacted blob** is rebuilt as it streams (first byte in under a second). rift then
  puts the original back in the bucket until the blob goes cold again, so repeat pulls are full
  speed.
- **How often rift looks:** every `--interval` (default 1 hour) it lists the bucket to find cold
  blobs, stub idle hot copies again and reclaim deleted chunks. Each listing is one LIST request
  per 1,000 objects: on S3 Standard, about $3.60 a month per million objects at the default. Raise
  `--interval` for buckets with tens of millions of objects.
- **Deletes and registry garbage collection** work as before. rift drops the deleted blobs and
  reclaims their packs after `--gc-grace` (default 10 minutes).

## 8. Watching it

- `GET /-/metrics` is Prometheus format (the chart adds scrape annotations). The ones that matter:
  - `rift_bucket_logical_bytes`, `rift_bucket_stored_bytes` and `rift_bucket_saved_ratio`: what the
    registry sees against what's stored, updated on every compactor round (hourly by default). This is the saving.
  - `rift_compactions_total`, `rift_compaction_failures_total`: progress, and blobs left as they were.
  - `rift_cold_reads_total`, `rift_cold_read_seconds_total`, `rift_hot_copies_made_total`,
    `rift_redirects_total`: how pulls are served.
  - `rift_backend_errors_total`, `rift_cold_read_errors_total`: should stay at zero.
  - `rift_compactor`: 1 on the gateway compacting the bucket. The bucket and pack gauges come from
    it; alert if no gateway reports 1 for a while.
- `GET /-/healthz` answers `{"status":"ok","compacted":N}`.
- `--access-log` writes a JSON line per request to stdout: time, client address (and
  `X-Forwarded-For`), the access key id the request was signed with, method, path with signatures
  masked, status, bytes in and out, milliseconds, and how it was served: `pass` (from the bucket),
  `compacted` (rebuilt from packs), `redirect`, `local` (health and metrics) or `rift` (rift's own
  refusals and errors). rift's other messages go to stderr. With the chart: `--set accessLog=true`.
- The log has a line for every compaction batch, garbage collection, and hot copy stubbed again.

## 9. Leaving

1. Stop rift.
2. Run `rift eject --backend https://s3.us-east-1.amazonaws.com --bucket my-registry --yes`, with
   the backend keys in the environment. It rebuilds every compacted blob, checks it against its
   digest and puts the original back, then deletes `.rift/`. If any blob fails, nothing is deleted;
   fix the cause and run it again.
3. Point the registry back at the bucket.

## 10. Limits of this pilot build

- One compactor per bucket at a time. If it's down, serving goes on but compaction and GC wait;
  a standby compactor takes over after two minutes ([High availability](HA.md)).
- Compaction ran at 19 MB/s on a 12-core server (Ryzen 9 5900X; 1.3 GB of layers from 148 official
  images in 67 s, 64% saved) and 2.3 MB/s on a 6-core laptop. A backfill commits about 2 GB at a
  time, starting with the repositories with the most layers, where most is saved. New pushes are a
  small daily stream.
- A cold pull of a big layer on few cores is slower than a plain download, about 2–3× on 6 cores.
- gzip layers only; zstd and eStargz layers are left as they are.
- Blobs that unpack to more than `--max-unpacked-mb` stay as they are, so the biggest layers of ML
  images may not be compacted at the default.
- Tested end to end with distribution 2.8.3, Harbor 2.14.6 and GitLab's registry 4.41 (both
  metadata modes) on MinIO, and with distribution and GitLab's registry (metadata database, default
  checksums) on AWS S3 in us-east-1, driven from a laptop. Not yet run in-region at scale.
