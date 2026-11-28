---
orphan: true
---

# Object upload and cleanup recovery records

This describes recovery records and physical reclamation for synchronous
object replicas, developed under
[RFC #4296](https://github.com/kvcache-ai/Mooncake/issues/4296).

## Ownership

The existing Master metadata remains the authority for logical keys, visible
values, replicas and commit. The records below do not form another logical-head
catalog. They track physical object obligations, including attempts whose
logical key has already been removed.

`ObjectUploadRecord` identifies an immutable backend object by its non-secret
storage target and object name. It also retains the tenant, logical key, writer,
replica ID, size and reclamation deadline. Credentials are process-local and
are not included. The target stays bound even after the last record is
forgotten; ordinary mutations cannot silently redirect old work.

The state is monotonic:

- `AUTHORIZED`: a single upload attempt is authorized; its outcome is unknown.
- `UPLOADED`: that attempt completed successfully. This alone does not publish
  the logical value.
- `RETIRED`: the object is no longer needed by that attempt/value. The caller
  supplies a conservative `not_before_ms` covering read protection.

`upload_finished` means that the PUT can no longer materialize late. A successful
DELETE alone does not establish this: a delayed PUT can recreate the object.
The Master separates pending-write admission from retired cleanup obligations.

## Bounded cleanup and orphan collection

New uploads use immutable names under `mc-gc-v1-<hex-cluster-id>-` within the
configured storage prefix. Cluster IDs must be unique for independent clusters
sharing a target. Do not reuse a cluster ID after discarding its HA state while
retaining that namespace's objects. Legacy names remain readable and normally
collectable; uncertain legacy uploads remain tracked because their ownership
cannot safely be inferred from a bucket listing.

The Master waits for retirement durability and read protection before DELETE.
For uncertain uploads in the new namespace, it retains the record for another
`object_storage_unknown_upload_retention_ms` (default 24 hours). After that
period, a successful final DELETE allows a durable `expired_forget`. This hands
the obligation to orphan GC; it does not set `upload_finished` or assert that a
remote PUT has stopped. Failed deletes retry with exponential backoff, bounded
by `object_storage_cleanup_max_backoff_ms` (default one hour).

A recovered, serving leader periodically lists its cluster namespace in bounded
pages. Objects with a backend LastModified timestamp older than
`object_storage_gc_min_object_age_ms` (default 24 hours) are candidates only
when the current upload registry has no record. Every valid object replica,
including a hidden Upsert value, must have a registry record. Missing or invalid
timestamps are skipped. Before DELETE the Master rechecks absence under the log
ordering mutex, appends an `orphan_deletes` authorization and waits for its
fenced durability. Replay rejects authorizations for tracked objects. This also
prevents a stale leader from deleting an object created by its successor.
Immutable names are never reused. Failed scans retry; successful scans repeat
after `object_storage_gc_scan_interval_ms` (default 24 hours), including when
there are no pending records. Thus PUTs arriving after forget are eventually
collected. List permissions are required in addition to read/write/delete.

`object_storage_max_pending_records` limits unfinished writes (default 10,000).
Retired records instead use `object_storage_max_gc_records` (default 100,000),
a high watermark that throttles new authorizations until cleanup progresses.
Existing writes can still finish or retire while throttled. Both indexes are
rebuilt after failover. Metrics expose `master_object_pending_writes`,
`master_object_gc_records`, `master_object_gc_oldest_age_ms`, and
`master_object_gc_failures_total`. The oldest-age metric measures time after
read protection, not time since the upload began. Legacy uncertain records can
still fill the separate GC watermark and require operator cleanup/migration;
they are never silently forgotten.

Deploy the GC-capable binary on all potential leaders and standbys before
using the new log extensions; older binaries cannot safely replay them.

## Ordered recovery boundary

`ObjectReplicaMutation` changes records and, optionally, the existing encoded
`MetadataPayload` or logical removal in one ordered-log entry. All validation
precedes publication under the standby store mutex. Invalid names, transitions,
namespace changes or cross-writer updates leave both stores unchanged.
Metadata replica IDs use the recovery checks: zero, the reserved maximum ID
and duplicate IDs within one object are rejected before either store changes.

The Master validates exact replica descriptors and attempt tokens
before producing a mutation. It uses the existing fenced
`OrderedOpLogWriter` and waits for durability before authorizing an
upload, publishing a committed value or acknowledging a logical removal.
Enqueue success is insufficient. Business locks must not be held while waiting.
Retries of metadata notifications must not authorize another physical PUT.
Logical commit/removal acknowledgments additionally wait for their local
metadata finalizers. New attempts use fresh immutable names scoped by cluster,
so late work cannot affect replacements. These are Master responsibilities;
the passive recovery store only validates and applies the resulting mutations.

`OpLogApplier` applies these entries in its existing global sequence, including
duplicate-delivery handling and checksum checks. Ordinary logical removal does
not discard upload records. Replica-ID validation includes records for deleted
keys, preventing recovery from overlooking their reserved IDs.

## Snapshots and promotion

Snapshot capture freezes log application using the existing capture lease, so
ordinary metadata and object obligations share one cursor. A v3 snapshot adds
an `object_storage.bin` artifact, with the same size/CRC verification and
publication rules as other artifacts. Restore validates the records before
installation. Snapshot GC and log pruning validate this artifact before deleting
recovery evidence. Existing v1/v2 snapshots remain readable; snapshots without
object state retain their existing version.

Before log pruning advances the compaction floor, it checks the object-state
artifact's size and CRC32C. If inspection supplies no CRC32C, pruning reads the
artifact back and computes it; ordinary object chunks retain their existing
inspection policy.

Both promotion export paths retain the records. This intermediate review unit
refuses to serve as a Master when a handoff contains object state: it cannot yet
resume uploads/reclamation. This avoids silently dropping obligations. The next
integration must restore these obligations and use the existing
`default_kv_lease_ttl` configuration for read protection, rather than storing a
second lease duration in the storage target. Participating Masters must use
the same read-lease duration. A shorter duration must not take effect until
previously granted longer leases have expired.

Before opening the serving gate, that integration must protect restored live
objects and retain retired objects until at least the restore time plus the
configured read-lease duration. Each retired object's `not_before_ms` must be
the maximum of its existing deadline and that recovery deadline. This review
unit does not execute that lifecycle, and there is no new public configuration
to create such state in this intermediate version.

## Verification and costs

Tests exercise ordered replay, the durable acknowledgement boundary, atomic
validation failure, uncertain-upload retention, target fencing, old snapshot
wire compatibility, and snapshot-only recovery after log compaction. Corrupt
object artifacts fail recovery. Existing snapshot/provider/GC/pruner tests
remain applicable. No HTTP data path changes, so MinIO throughput measurements
from a combined integration do not validate this smaller change.

There is no added ordinary Put/Get work besides the existing operation-type
dispatch. Applying an object mutation costs logarithmic record-map operations
plus constant-time replica-ID lookup and a scan of the submitted metadata's
replica IDs; snapshot capture copies retained records. Pruning adds an
object-state artifact download when the backend supplies no CRC32C. No threads,
timers or additional persistence service are added.
