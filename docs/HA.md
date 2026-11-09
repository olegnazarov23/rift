# High availability

From 0.1.5, several gateways can serve one bucket while one of them compacts it. The Helm chart
runs two serving gateways behind the Service and one compactor. A serving gateway can go at any
time (a crash, a node, a rolling update) without interrupting pushes or pulls. If the compactor
goes, compaction pauses until it's back, or until a standby takes over.

## Roles

- **Serving gateways** (`--serve-only`) answer the registry: pass-through, cold reads, hot copies,
  redirects. They keep no state on disk and never write packs; they delete a blob's manifest only
  when the registry deletes or replaces that blob through them.
- **The compactor** (a gateway without `--serve-only`) also lists the bucket, compacts cold blobs,
  stubs idle hot copies again and collects garbage. It keeps the chunk index in `--state-dir`.

## How serving gateways stay right

A gateway can't rely on what it remembers, because another gateway may have compacted, cooled or
deleted a blob a moment ago. So for blobs the bucket decides:

- **Reads go by what the bucket holds.** A GET or HEAD of a blob goes to the bucket. If the answer
  is a stub (an empty object carrying `x-amz-meta-rift-size`, and from 0.1.5 the blob's date), the
  gateway loads the blob's manifest and serves the rebuild. A stub is never served as the blob,
  and a blob deleted through another gateway isn't reported as present.
- **Redirects check first.** With `--redirect-endpoint`, a gateway checks the object (a HEAD)
  before redirecting a client to the bucket, so no client is sent to a stub.
- **Listings.** A listing shows an empty object at a blob's key for a blob compacted through
  another gateway; the gateway looks up its manifest by name and lists the original size.
- **Copies** of a blob are rebuilt when the bucket holds its stub.
- **Cached manifests** are checked against the bucket once they're 30 seconds old, because GC may
  have pointed them at new packs. Old packs stay readable for `--gc-grace`, so keep that at two
  minutes or more (the default is 10).
- **The catalog** (which blobs are compacted, for listings) is read again every `--interval`.

## One compactor at a time

Two compactors on one bucket would overwrite each other's packs and collect chunks the other just
started using. Every gateway that may compact takes turns holding `.rift/compactor`:

- The holder rewrites the object every 20 seconds, always with `If-Match` on its last version.
- Another gateway takes it over only after seeing it unchanged for two minutes, again with a
  conditional write, so two can't take it at once. It then brings its chunk index up to date with
  the packs in the bucket before it compacts.
- A holder that couldn't renew for a minute stops compacting and collecting, a minute before
  anyone may take over. It checks before each blob it compacts, each manifest GC changes, and each
  pack it deletes.
- A compactor that restarts (same state dir) takes its lock back at once.
- Packs, pack indexes and manifests are written with `If-None-Match: *`, so a second compactor
  would fail rather than overwrite.

At start, rift checks that the bucket refuses a write whose `If-None-Match` fails (AWS S3 and
MinIO do). If it doesn't, rift says so in its log: then run exactly one gateway without
`--serve-only`.

The compactor's own catalog follows the bucket too: each round it reads the manifests' names from
its listing, forgets blobs deleted or replaced through serving gateways, and GC counts as live
every manifest in the bucket, not what any gateway remembers.

## Reads on every gateway count

`--cold-after` means "not written or read for this long". Serving gateways hand the blobs they
served to the compactor every minute (`.rift/reads/`), so blobs read through any gateway count as
read.

## What failover looks like

| What goes | Effect |
|---|---|
| A serving gateway | Requests in flight on it fail and are retried by the registry; the Service sends new ones to the others |
| The compactor | Compaction, cooling and GC pause; serving goes on. After a crash Kubernetes restarts it and it carries on at once; a new pod (an upgrade, a lost node) waits up to two minutes for the old lock to go stale. A standby compactor, if you run one, takes over after about two minutes |
| The bucket | Everything waits for it, as without rift |

## Upgrading from 0.1.4 or earlier

The chart keeps the single gateway's Deployment as the compactor and adds the serving one, then
points the Service at the serving gateways. Expect a few seconds without a ready endpoint while
they first start. Stubs written before 0.1.5 carry no date; gateways find their manifests by name.

## Cost

Per gateway, about a dollar a month in S3 requests on S3 Standard: the lock's renewals (one PUT
every 20 seconds, from the compactor), the hourly catalog read (one LIST per 1,000 compacted
blobs), and at most one PUT a minute of reads. Redirects cost one HEAD each, and cold reads one
more request each.

## Tested

- **Soak on MinIO** (`rift-test/soak_ha.py`): two compactors (one standing by), two serving
  gateways behind a round-robin proxy, and a registry with redirects on. For 25 minutes it pushed
  131 images, deleted 39 tags and ran registry GC 30 times, killed serving gateways 32 times and
  the active compactor 9 times (3 during a compaction; 4 times it stayed down long enough for the
  standby to take over). 807 blobs (1.3 GB) were read back through the serving gateways and
  checked against their digests: no mismatch and no failed read. The bucket audit found nothing
  missing and nothing left over.
- **The same soak, later, found a GC bug** present since 0.1.0: after a restart or a takeover, GC
  could point blobs at a pack it was about to delete; it lost two blobs. Fixed in 0.1.6. Two
  15-minute soaks on the fix (13 compactor kills, 8 of them takeovers) read back 912 blobs, all
  byte-exact, and the audits were clean.
- **Deletes through serving gateways**: every tag deleted and the registry's GC run (304 blobs
  deleted through the serving gateways); the compactor noticed, collected, and after the grace
  period the bucket held no packs at all.
- **AWS S3** (us-east-1, gateways on a laptop): the same layout. S3 honours the conditional
  writes; the serving gateways rebuilt blobs the compactor had compacted and redirected the rest;
  after the compactor was killed the standby took over in 125 seconds and carried on. Every blob
  byte-exact, audit clean.

## Not yet

- **Fencing against a paused compactor.** A compactor frozen (a stopped VM, say) for longer than
  the lock's two minutes could finish a step it had started once it wakes. The chart runs one
  compactor, so this needs a second one running as well. An epoch carried by every destructive
  step would close it.
- **A standby compactor in the chart.** It works (see Tested), but the chart runs one compactor:
  Kubernetes restarts it within a minute, and a standby holds memory for nothing most of the time.
