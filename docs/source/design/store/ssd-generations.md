# Object generations for SSD recovery

This draft implements the generation and HA recovery stage of RFC #5768.
It assigns each primary write a content generation, carries that generation
through SSD offload and promotion tasks, and records it in durable metadata.
The generation identifies the content incarnation even when an overwrite has
the same key and byte length.

## Write authority

PutStart and UpsertStart return the generation captured by the admitted write.
The client forwards it in PutEnd, PutRevoke, and their batch variants. The
master compares it with the admitted generation before changing replicas.
Session writes retain their Start grant until End or Revoke.

A query can describe a current object but cannot grant authority to finish
an older write. An old completion must not finalize a newer same-key write.
The tests retain the original grant to exercise this ordering.

## SSD records and tasks

The physical SSD key contains a length-delimited generation and the logical
key, inside the existing tenant scope. Different generations can coexist on
disk. Replica metadata supplies the physical key for reads, so the reader
uses the incarnation selected by its metadata response.

Offload tasks retain the object generation, task identity, and each owner's
source reference. Completion, rejection, and cancellation settle the matching
owner's reference. An unrelated task or a replacement generation cannot
consume that reference.

The owner keeps committed bucket data when notification of the master is
uncertain. Retrying the notification uses its captured task. Restart scans
report recoverable physical keys against existing master metadata. An old
scan cannot recreate a deleted object or restore a generation whose replicas
are all marked removed.

Promotion carries the same generation and task identity through allocation,
transfer, success, and failure. The master publishes the memory replica only
for the task it admitted.

## HA completion

The ordered log exposes separate durability and callback-finalization waits.
Write completion can wait for its captured record to become durable. Removal
and invalidation can wait for the callback that retires old allocations.

During HA upsert invalidation, old allocations retain their quota charge
until durable retirement. Another writer cannot reuse an allocation merely
because the serving metadata has become unreadable. A timeout does not cancel
an already accepted log record.

The synchronous wait budget is configured by
`oplog_durable_wait_timeout_ms`. Its default is 30 seconds. Invalid negative
or unrepresentable durations are rejected by the lower-level wait APIs.
The log's existing terminal error and shutdown state end waits for records
that cannot become durable.

## Draft upgrade boundary

All newly added authority fields on existing RPC and metadata payloads
(`Replica::Descriptor`, `LocalDiskDescriptor`, `ObjectMeta`,
`GetReplicaListResponse`, `PromotionAllocStartResponse`, `OffloadTaskItem`,
`PromotionTaskItem`, `StandbyObjectMetadata`, and `MetadataPayload`) use
`struct_pack::compatible` fields so older and newer binaries remain
serialization-compatible on the wire.

Existing `coro_rpc` method signatures (`PutRevoke`, `BatchPutRevoke`,
`UpsertRevoke`, `BatchUpsertRevoke`, `PromotionAllocStart`,
`NotifyPromotionSuccess`, `NotifyPromotionFailure`, `EvictDiskReplica`, and
`BatchEvictDiskReplica`) are preserved verbatim. Generation-aware calls use
additive `*V2` RPC handlers (`PutRevokeV2`, `BatchPutRevokeV2`,
`UpsertRevokeV2`, `BatchUpsertRevokeV2`, `PromotionAllocStartV2`,
`NotifyPromotionSuccessV2`, `NotifyPromotionFailureV2`, `EvictDiskReplicaV2`,
and `BatchEvictDiskReplicaV2`), and `MasterClient` routes tokenless/legacy calls
to the original handlers so newer clients can interop cleanly with older
masters during rolling upgrades.

At the protocol-policy layer, this draft still rejects tokenless primary write
completions (`PutEnd`/`PutRevoke` with an empty `write_generation`) on newer
masters. Supporting mixed old clients during a rolling upgrade requires an
explicit capability and compatibility-mode policy before turning strict
enforcement on by default.

Blocking durability/finalization wait timeouts return `ErrorCode::RPC_TIMEOUT`
to distinguish a timed-out wait on an accepted record from a terminal
`ErrorCode::PERSISTENT_FAIL` backend failure.

Persisted metadata uses compatible optional fields for the generation.
Snapshots that predate generations and legacy SSD records need dedicated
migration coverage. A missing physical generation cannot be treated as
authority to overwrite or resurrect a newer object.

## Validation

The generation tests cover captured grants, physical-key round trips,
same-writer stale completion, source reference accounting, pending deletion,
metadata replay, and recovery of old SSD notifications after same-key rewrite.
FileStorage tests cover carryover, retry after uncertain notification, and
physical file loss. HA tests hold persistence and callbacks independently
to check retirement ordering.

The implementation builds on the separately reviewed callback wait and
bucket metadata publication changes. The local streaming and owner-push
transport stages are independent of the generation protocol.
