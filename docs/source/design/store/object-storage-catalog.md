---
orphan: true
---

# Object upload and cleanup recovery records

This is review unit 2 of [RFC #4296](https://github.com/kvcache-ai/Mooncake/issues/4296).
It adds recovery records for object upload attempts and physical reclamation.
It does not enable object replicas in Store APIs. Client uploads, reads and
Master cleanup scheduling belong to the subsequent synchronous integration.

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

`upload_finished` means that the PUT can no longer materialize late. A retired
record with an uncertain outcome cannot be forgotten after one successful
DELETE: a delayed PUT can recreate the object. The subsequent integration must
repeat cleanup and retain/backpressure these obligations. Only terminal retired
records can be forgotten, after the caller has confirmed physical deletion and
satisfied protection. There is no bucket scan or DELETE implementation here.

## Ordered recovery boundary

`ObjectReplicaMutation` changes records and, optionally, the existing encoded
`MetadataPayload` or logical removal in one ordered-log entry. All validation
precedes publication under the standby store mutex. Invalid names, transitions,
namespace changes or cross-writer updates leave both stores unchanged.
Metadata replica IDs use the recovery checks: zero, the reserved maximum ID
and duplicate IDs within one object are rejected before either store changes.

The next integration must validate exact replica descriptors and attempt tokens
before producing a mutation. It must use the existing fenced
`OrderedOpLogWriter` and wait for `AwaitDurable(sequence)` before authorizing an
upload, publishing a committed value or acknowledging a logical removal.
Enqueue success is insufficient. Business locks must not be held while waiting.
Retries of metadata notifications must not authorize another physical PUT.
New attempts use fresh immutable names scoped by cluster/view, so late work
cannot affect replacements. These producer responsibilities are not implemented
by the passive recovery store.

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
