# Immutable DFS bucket allocator

The immutable bucket allocator is an experimental alternative to the default
DFS shard allocator. It stores immutable objects by appending them to
fixed-capacity bucket files and reclaims space only by deleting a whole bucket.
The default remains `shard`; selecting `bucket` is an explicit deployment
choice.

## On-disk layout

Each bucket is named `bucket_<16-digit-id>.data` below
`MOONCAKE_DFS_ROOT_DIR`. Bucket IDs increase monotonically for the lifetime of
the master and are not reused.

An entry contains only its value bytes followed by zero padding to
`MOONCAKE_DFS_ALIGNMENT`:

```text
bucket_N.data
+----------------------+--------------------+----------------------+-----+
| object A value bytes | zero alignment pad | object B value bytes | ... |
+----------------------+--------------------+----------------------+-----+
^ descriptor.offset                         ^ next descriptor.offset
```

There is no per-entry header, footer, checksum, or persisted allocator
metadata in this first implementation. A DFS descriptor records the bucket
path and ID, value offset, object size, and aligned allocation size. The data
plane opens an existing bucket for each request and uses synchronous buffered
positional I/O. A successful request means that the requested `WriteAt` calls
completed; it is not an `fsync` durability guarantee.

Allocations append in request order. A rollback or removal changes the
in-memory entry to a tombstone but never rewinds the append cursor and never
reuses that range. This rule also applies when the same key is allocated again.
Batch allocation is atomic at allocator level: if any request fails, all ranges
reserved by that batch become tombstones and no descriptor is returned.

## Lifecycle and eviction

New entries are `PENDING` until `PutEnd` commits them. A bucket containing a
pending entry cannot be selected for eviction. Full buckets are sealed and the
allocator creates a new bucket, up to the configured maximum.

Eviction uses bucket-level LRU. Before deleting a candidate, the master locks
the affected metadata shards in a stable order and verifies that every live
entry still refers to the exact descriptor and is not leased or processing.
One failed check aborts the entire candidate; entries are never partially
evicted from a bucket. Metadata is removed only after every entry passes
validation. If physical deletion fails, the bucket remains charged against
capacity and is retained for a later deletion retry.

## Configuration

Set the same DFS root, adapter, allocator selection, alignment, and
`MOONCAKE_DFS_BUCKET_CAPACITY` on the master and every client that accesses
DFS replicas. `MOONCAKE_DFS_MAX_BUCKET_COUNT` and the bucket eviction settings
are consumed only by the master allocator.

| Environment variable | Default | Description |
| --- | --- | --- |
| `MOONCAKE_DFS_ALLOCATOR` | `shard` | Select `bucket` to enable this allocator, or `shard` for the existing allocator. |
| `MOONCAKE_DFS_BUCKET_CAPACITY` | `1073741824` (1 GiB) | Logical and preallocated size of every bucket; must be nonzero, aligned, fit in a signed 64-bit file offset, and match on the master and every DFS client. |
| `MOONCAKE_DFS_MAX_BUCKET_COUNT` | `64` | Maximum number of live bucket files; must be positive. Buckets awaiting a successful deletion still count. |
| `MOONCAKE_DFS_ROOT_DIR` | `/mnt/3fs/mooncake` | Absolute shared directory containing the bucket files. |
| `MOONCAKE_DFS_FS_ADAPTER` | `hf3fs` | Filesystem adapter (`hf3fs` or `posix`). This version uses its buffered synchronous interface. |
| `MOONCAKE_DFS_ALIGNMENT` | `4096` | Power-of-two entry alignment; must divide the bucket capacity. |
| `MOONCAKE_DFS_EVICTION_ENABLED` | `true` | Enable bucket-level eviction. |
| `MOONCAKE_DFS_EVICTION_HIGH_WATERMARK` | `0.9` | Usage ratio that starts normal eviction. |
| `MOONCAKE_DFS_EVICTION_LOW_WATERMARK` | `0.7` | Usage ratio at which normal eviction stops. |
| `MOONCAKE_DFS_EVICTION_CHECK_INTERVAL` | `5` | Background eviction check interval in seconds. |

For example:

```bash
export MOONCAKE_ENABLE_DFS=1
export MOONCAKE_DFS_ALLOCATOR=bucket
export MOONCAKE_DFS_ROOT_DIR=/mnt/3fs/mooncake
export MOONCAKE_DFS_FS_ADAPTER=posix
export MOONCAKE_DFS_BUCKET_CAPACITY=1073741824
export MOONCAKE_DFS_MAX_BUCKET_COUNT=64
export MOONCAKE_DFS_ALIGNMENT=4096
```

`MOONCAKE_DFS_SHARD_COUNT`, `MOONCAKE_DFS_SHARD_CAPACITY`, and online shard
expansion apply only to the `shard` allocator. Conversely, the two bucket
capacity variables have no effect while `MOONCAKE_DFS_ALLOCATOR=shard`.

## Current limitations

This initial backend intentionally provides only the minimal runtime path:

- Allocator and key metadata exist only in master memory. Startup fails closed
  when the DFS root already contains a `bucket_*.data` or `bucket_*.meta`
  artifact; restart recovery and persisted manifests are not implemented.
- Snapshot restore, OpLog recovery, standby promotion, and HA continuity do not
  reconstruct bucket state. Do not select `bucket` when those guarantees are
  required.
- I/O is synchronous and buffered. There is no asynchronous DFS pipeline,
  Direct I/O (`O_DIRECT`), read coalescing, prefetch, or bucket read cache.
- There is no public administrator API for changing the maximum bucket count
  while the service is running.
- The backend remains single-tenant and experimental. Only one active master
  may own a DFS bucket root, and operators must not create, truncate, rename,
  or delete bucket files behind it.

See the {ref}`Mooncake Store deployment guide <dfs-storage>` for the common DFS
replica setup and client requirements.
