# KVCacheStore Low Level backend

Select this backend with `MOONCAKE_DFS_FS_ADAPTER=kvcs-lowlevel`. Mooncake
Master remains the sole authority for object existence, size, replica identity,
and commit state. Clients read the COMPLETE DFS replica returned by Master and
then access KVCS through the node-local EFC. Mooncake does not query KVCS to
reconstruct metadata and does not keep a second client-side metadata cache.

The data path reuses the existing DFS `BatchWrite` and `BatchRead` entry points
and the shared `ObjectStorageAdapter` contract. EFC is deployed independently;
Mooncake neither starts it nor infers enablement from the socket.

## Initial scope

The initial implementation supports one configured KVCS target and one KVCS
value per Mooncake object. Mooncake treats that target as one DFS replica and
does not inspect its internal topology or replication. Multiple targets are
rejected during initialization.

The value must be nonempty and no larger than
`MOONCAKE_KVCS_MAX_VALUE_SIZE` (4 MiB by default). Oversize values are rejected
before provider I/O. The initial configuration is single-tenant, and the
adapter maps each logical key to one printable provider key.

Supported operations are Put, Get, Upsert, single-key Remove, and BatchRemove.
Upsert reuses Mooncake's existing Master state machine; at the adapter boundary
it is Delete followed by the insert-only KVCS Put. Replacement is not atomic:
if Put fails after Delete, the previous provider value is not restored.

Remove and BatchRemove first use the existing Master metadata removal, then
delete only the keys whose metadata removal succeeded. A provider delete error
is returned but does not recreate Master metadata, so an unreachable provider
value can remain until a later write pre-deletes the same key or KVCS reclaims
it. RemoveByRegex and RemoveAll return `NOT_SUPPORTED` because the Low Level
interface does not provide the required listing and conditional bulk-delete
contract.

Reads use the object size stored in Master metadata and reject a shorter or
longer KVCS value. An explicit KVCS `OBJECT_NOT_FOUND` conditionally removes
only the observed DFS `ReplicaID`; a timeout or other provider failure leaves
metadata unchanged. Put and Upsert finalization use the existing Mooncake
paths, with no KVCS-specific finalization retry or client-side uncertain-key
index.

The SDK is optional at build time. Without it, other storage backends still
build and explicit KVCS initialization returns `NOT_SUPPORTED`. KVCS-enabled
builds require SDK 0.6.0 or newer because capacity management uses the Low
Level `kvcs_ll_mp_stat` API introduced in 0.6.0. CMake enforces this minimum
through `pkg-config`.

## Configuration

The target comes from `MOONCAKE_KVCS_EFC_CONFIG` when set. The YAML file must
contain exactly one entry:

```yaml
mountpoints:
  - mountpoint_index: 1
    mountpoint_id: example-target
```

Otherwise Mooncake uses the single target in `KVCS_MOUNTPOINTS_JSON`; when that
deployment variable is absent, the defaults are index `1` and mountpoint ID
`kvcachestore-default`.

| Setting | Default |
| --- | --- |
| `MOONCAKE_KVCS_EFC_CONFIG` | unset |
| `MOONCAKE_KVCS_EFC_SOCKET` | `/var/run/kvcs/efc-grpc.sock` |
| `MOONCAKE_KVCS_MAX_VALUE_SIZE` | 4 MiB |
| `MOONCAKE_KVCS_MAX_KEY_SIZE` | 256 bytes |
| `MOONCAKE_KVCS_MAX_KEYS_PER_BATCH` | 8 |
| `MOONCAKE_KVCS_GET_WORKERS` | 1 |
| `MOONCAKE_KVCS_SET_WORKERS` | 1 |
| `MOONCAKE_KVCS_SIMPLE_WORKERS` | 1 |
| `MOONCAKE_KVCS_OPERATION_TIMEOUT_MS` | 30000 ms |

## Capacity source

Master calls the KVCS SDK 0.6.0+ `kvcs_ll_mp_stat` API and selects the entry
whose `index` and `id` match the configured target. Its `used_bytes` and
`total_bytes` are published through the existing `DfsUsageSnapshot`. DFS
remains the single place for watermark configuration, metrics, scheduling, and
eviction decisions; KVCS does not introduce a second capacity model.

`kvcs_ll_mp_stat` is a synchronous, uncached EFC control-plane RPC. Mooncake
therefore calls it only from the existing periodic capacity refresh worker,
not from the read or write path. A missing or duplicate target, an SDK/RPC
error, `total_bytes == 0`, or `used_bytes > total_bytes` makes capacity unknown.
The last successful value is cleared so stale capacity cannot drive a later
decision. When usage exceeds `MOONCAKE_DFS_EVICTION_HIGH_WATERMARK`, Master
uses the existing eviction index and its lease, pin, and managed-weight checks
to remove eligible KVCS DFS metadata until the logical bytes selected cover
the distance to `MOONCAKE_DFS_EVICTION_LOW_WATERMARK`. It then deletes those
logical keys through the same KVCS Low Level driver and commits the metadata
change under the same key lock, so a concurrent Put/Upsert cannot publish a
replacement between them. A failed provider delete leaves metadata unchanged
for the next refresh to retry. A later capacity refresh confirms the physical
watermark; if provider accounting includes non-Mooncake data, or too little
Mooncake data is eligible, usage can remain above the low watermark.

This first stage reuses the existing memory eviction index. An automatic KVCS
watermark candidate must therefore have both a completed memory replica and a
completed KVCS DFS replica, which matches the HiCache configuration validated
for this change. DFS-only KVCS objects are not selected by automatic watermark
eviction yet.

Automatic KVCS watermark eviction is disabled when OpLog durability is enabled
because provider deletion is not yet tied to the durable metadata finalizer.

Deployments should configure the KVCS GC watermark to 100%. Current KVCS
cannot fully disable automatic GC, so this only makes provider-side deletion
very unlikely; it is not a correctness guarantee. If KVCS removes a value,
the explicit Get miss reconciliation above removes the stale Mooncake replica.
When KVCS provides a way to disable automatic GC, a full provider should fail
writes and Mooncake can drive deletion through its DFS watermark mechanism.
