# Transfer Metadata Version Reliability

## Background

Transfer Engine uses segment metadata to describe remote devices, memory
regions, keys, and topology. In production, a segment name may be reused after a
node replacement. The old and new nodes are assumed not to overlap, but there is
usually a gap between the old node disappearing and the new node publishing its
metadata.

The reliability problem is not only node replacement. Dynamic memory
registration and deregistration can also change rkeys, address ranges, and
buffer availability. Readers need one simple way to know that cached metadata
and derived transport resources may be stale.

This design uses a single descriptor-level `metadata_version`.

```text
same segment name/id + different metadata_version = metadata changed
```

The transport layer does not need to classify the change as replacement,
topology update, or memory-region update. It invalidates resources derived from
the old descriptor and rebuilds from the new descriptor.

## Goals

- Use one version field for all segment metadata changes.
- Detect replacement of a node that reuses the same segment name/id.
- Detect dynamic memory-region changes.
- Keep legacy metadata readable.
- Keep the transport invalidation rule small and conservative.
- Avoid introducing backend leases, CAS, fencing, or writer heartbeats in this
  first step.

## Non-Goals

- Supporting overlapping old and new writers for the same segment name.
- Proving strict global ordering across multiple metadata backends.
- Adding metadata watch/subscription.
- Adding typed metadata errors.

## Data Model

`SegmentDesc` contains:

```cpp
uint64_t metadata_version;
```

Meaning:

- `0` means legacy or unknown version.
- Non-zero values identify a published descriptor revision.
- Every metadata publish should assign a new version.
- Readers treat any version change as invalidating cached transport resources
  derived from the previous descriptor.

`BufferDesc` keeps only lifecycle state:

```cpp
state = READY | DRAINING | REMOVED
```

Meaning:

- missing or empty state is treated as `READY`;
- `READY` buffers can be selected for new transfers;
- `DRAINING` buffers are not selected for new transfers;
- `REMOVED` is reserved for future explicit tombstones.

## Version Assignment

The implementation uses a timestamp-style version:

```text
metadata_version = max(now_ns, previous_metadata_version + 1)
```

This avoids the main problem with a per-process counter: after node replacement,
the new process might also start at version `1`, making replacement invisible to
clients that cached the old `1`.

The timestamp-style value is not intended to be a perfect distributed clock. It
is a compact monotonic freshness token. Under the current non-overlap model it
is sufficient to make a replacement publish differ from the old descriptor.

## Write Path

### Segment Startup

1. The transport creates the local `SegmentDesc`.
2. `addLocalSegment()` stores it in the local cache.
3. `updateLocalSegmentDesc()` bumps `metadata_version`.
4. The descriptor is encoded and published.

### Memory Registration

1. Add the local buffer to `SegmentDesc::buffers`.
2. Mark it `READY` if no state was provided.
3. If the operation publishes metadata, `updateLocalSegmentDesc()` bumps
   `metadata_version` once and publishes the descriptor.
4. Batch registration can mutate many local buffers and publish once; this
   results in one version bump.

### Memory Deregistration

For `update_metadata=true`, deregistration is two-phase:

1. Mark the buffer `DRAINING`.
2. Bump `metadata_version` and publish.
3. Wait for the deregistration grace period. By default this is aligned with
   `MC_METADATA_CACHE_TTL_MS`; `MC_METADATA_DEREG_GRACE_MS` can increase it but
   should not be lower than the metadata cache TTL while metacache is enabled.
4. Remove the buffer locally.
5. Bump `metadata_version` and publish again.

For `update_metadata=false`, local metadata changes are not published and do not
need a version bump.

## Read Path

Readers decode `metadata_version` from JSON. This implementation does not decode
older experimental `descriptor_version` or `buffer_version` fields.

Legacy descriptors without any version fields decode as version `0`.

## Cache Behavior

The metadata cache stores immutable descriptor snapshots by segment id/name.
When a refreshed descriptor replaces an older cached descriptor:

```text
if old.metadata_version != new.metadata_version:
    record metadata-version change
    replace cached descriptor
```

The cache also tracks a soft TTL (`MC_METADATA_CACHE_TTL_MS`, default `1000`).
When a cached descriptor expires, the first caller refreshes it; concurrent
callers keep using the existing snapshot instead of stampeding the metadata
backend. `MC_METADATA_CACHE_TTL_MS=0` keeps the historical non-expiring cache
behavior unless a caller explicitly requests `force_update=true`.

## RDMA Transport Behavior

Each worker remembers, per target segment:

```text
segment_id -> {
    metadata_version,
    peer_nic_paths
}
```

On first observation, the worker records the version and peer NIC paths.

On later observations:

```text
if metadata_version changed:
    delete old RDMA endpoints for old peer NIC paths
    clear rail state for old peer NIC paths
    remember new version and paths
```

This is intentionally conservative. A memory-region update may not strictly
require deleting endpoints, but using one invalidation rule keeps the first
implementation simple and avoids stale rkey/path coupling.

## Replacement Gap Behavior

During the gap between old and new nodes:

- metadata may be missing;
- endpoint setup may fail;
- in-flight work may complete with errors;
- force refresh may not find the new descriptor yet.

Expected behavior:

- workers use existing retry and redispatch logic;
- once the new descriptor appears, refresh observes a new `metadata_version`;
- old RDMA endpoints and rail state are invalidated;
- subsequent transfers use the new descriptor.

If the gap exceeds the retry budget, the transfer can still fail and the caller
or scheduler should retry later.

## Observability

The implementation exposes:

- `SegmentDesc::dump()` prints `metadata_version`;
- each buffer dump prints lifecycle state;
- metadata dump prints `segmentMetadataVersionChangeCount()`;
- RDMA workers log version-triggered endpoint invalidation.

Useful future metrics:

```text
metadata_version_change_total
rdma_endpoint_invalidated_by_metadata_version_total
metadata_refresh_total
metadata_refresh_failure_total
```

## Current Limitations

- There is no metadata watch/subscription yet.
- Metadata storage plugins still expose only get/set/remove, not typed errors.
- Two-phase deregistration relies on a grace period rather than explicit drain
  ACKs.
- Batch memory deregistration currently publishes one final descriptor after
  local removal. It does not publish per-buffer `DRAINING` state in this patch.
- If overlapping old and new writers become possible, this must be extended
  with backend leases, CAS, remove-if-owner, or fencing tokens.
