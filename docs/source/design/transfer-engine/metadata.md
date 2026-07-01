# Metadata Versioning and Replacement

Transfer Engine caches segment metadata locally. A segment name or id can be
reused after a remote process is replaced, and memory registration changes can
also change the buffers, keys, or devices behind the same handle. Cached RDMA
resources therefore need a small freshness signal.

## Segment Version

Each `SegmentDesc` carries a `metadata_version` field. New local descriptors are
normalized to version `1`; every published descriptor change advances the value
to:

```text
max(now_ns, previous_metadata_version + 1)
```

Descriptors without the field are treated as legacy metadata with version `0`.

## Cache Refresh

`MC_METADATA_CACHE_TTL_MS` controls when a cached remote descriptor becomes
eligible for refresh. The default is `1000` ms. Setting it to `0` keeps the
historical non-expiring behavior unless a caller requests `force_update=true`.

If refresh fails and the caller did not force it, Transfer Engine keeps using
the previous cached descriptor. This is a best-effort cache refresh, not a
metadata backend lease or watch protocol.

## Memory Deregistration

When memory is deregistered with metadata update enabled, Transfer Engine first
publishes the buffer as `DRAINING`. New RDMA path selection skips non-`READY`
buffers. After `MC_METADATA_DEREG_GRACE_MS` elapses, the buffer is removed from
the descriptor and the final descriptor is published.

The grace period defaults to `1000` ms and is clamped to at least the metadata
cache TTL.

## RDMA Resource Invalidation

RDMA workers remember the last observed metadata version and peer NIC paths for
each target segment. When a refreshed descriptor has a different version, the
worker deletes endpoints associated with the old peer NIC paths and clears their
rail state.

This rule is intentionally conservative:

```text
same segment handle + different metadata_version = invalidate derived RDMA resources
```

The mechanism is best effort. It does not support concurrent owners of the same
segment handle or provide strict fencing across metadata backends.
