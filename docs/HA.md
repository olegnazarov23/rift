# High availability: design proposal

Status: proposal, not built. Today rift runs as one gateway per bucket (the chart uses
`replicas: 1` and the `Recreate` strategy). While that gateway is down, pulls of compacted blobs
fail and the registry can't write through it.

## Why two gateways on one bucket aren't safe today

- **Pack numbers.** Each gateway picks the next pack number from what it saw at start. Two
  compactors can pick the same number and overwrite each other's pack: data loss.
- **Stale catalogs.** A gateway learns which blobs are compacted when it starts. A blob another
  gateway compacted later looks like an ordinary object, so its empty stub would be served as the
  blob, and listings would show it as 0 bytes.
- **Garbage collection.** Two collectors with different views could delete a pack the other just
  started using.

## Phase 1: several gateways serve, one compacts (about 2 days)

Serving (pass-through, cold reads, hot copies) is what pulls and pushes depend on; compaction and GC
are background work that can pause for a while without harm. So:

1. **One compactor.** A new `--compactor` flag (default on) decides whether a gateway compacts, cools
   hot copies and runs GC. The chart runs N serving replicas plus one compacting one.
2. **Stub-aware reads.** A stub carries `x-amz-meta-rift-size` (and will carry the blob's date too). Any
   gateway that reads a stub it doesn't know loads the blob's manifest and serves the rebuild, so
   no gateway can ever hand out a stub as content. This also guards the single-gateway case.
3. **Catalog updates.** After each batch, the compactor writes a small journal object under
   `.rift/journal/` listing the blobs it compacted, cooled or dropped. Serving gateways read new
   journal entries every few seconds, a single cheap LIST, and update their catalog, so listings
   stay right.
4. **Create-only writes.** Packs, indexes and manifests are written with `If-None-Match: *` (S3 and
   MinIO support it), so even a misconfigured second compactor fails loudly instead of overwriting.
   Stubs and hot copies already use `If-Match`.
5. **Chart.** `replicas` defaults to 2 serving gateways plus the compactor, with a
   PodDisruptionBudget and rolling updates, all behind one Service.

Result: losing any gateway, including the compactor, doesn't interrupt pushes or pulls. Losing the
compactor pauses compaction until it's back.

## Phase 2: the compactor fails over by itself (about 3 days)

1. **Lease.** Gateways that may compact compete for a lease object (`.rift/lease`), taken with
   `If-None-Match: *` and renewed with `If-Match` every 10 seconds. A lease not renewed for 60 seconds
   can be taken over. The lease carries an epoch number that increases with every takeover.
2. **Fencing destructive steps.** Before any GC delete, the holder re-reads the lease and stops if
   the epoch moved. Deletes happen in short batches, each well inside the lease time. Packs are
   retired first (written down under `.rift/retired/`, so every compactor stops deduplicating into
   them) and deleted only after the grace period, by whichever gateway holds the lease then, after
   it checks again that nothing refers to them.
3. **Index hand-over.** A new holder brings its local chunk index up to date from the packs' `.idx`
   files before compacting (this already happens at start).

## Testing

The soak test already pushes, deletes, collects and kills gateways while checking every blob
byte-exact. For HA it would run 3 gateways behind a round-robin proxy and kill a random one
(compactor included) every few seconds. The pass criteria: no mismatch, no failed pull other than
one in-flight on the killed gateway, and a clean bucket audit at the end.

## Cost

Phase 1 adds one LIST every 5 seconds per serving gateway (the journal), about 518,000 requests or
$2.60 a month each on S3 Standard, and one PUT per compaction batch. Phase 2 adds a lease PUT every 10 seconds (about 260,000 a month,
roughly $1.30 on S3 Standard).
