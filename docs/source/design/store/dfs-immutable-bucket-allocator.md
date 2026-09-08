# DFS Immutable Bucket Allocator

This document describes the Bucket allocator introduced by
[RFC #3641](https://github.com/kvcache-ai/Mooncake/issues/3641). The goal is to
add dynamically created, append-only DFS files without changing the existing
DFS descriptor format or the default shard allocator.

## Allocator interface

`GlobalAllocatorInterface` is the Master-facing DFS allocation contract. It
covers initialization, single and batch allocation, free and access updates,
capacity reporting, and eviction configuration. The two implementations are:

- `DfsGlobalAllocator`: the existing fixed-shard allocator and the default.
- `ImmutableBucketAllocator`: the append-only Bucket allocator selected with
  `MOONCAKE_DFS_ALLOCATOR_TYPE=bucket`.

Both return `DistributedFSDescriptor`. In Bucket mode, the existing `shard_idx`
integer stores the Bucket ID. No field was added or reordered, so serialized
descriptors remain compatible.

## Bucket layout and ownership

Only the Master allocator creates Buckets and assigns offsets. One Bucket is
active at a time; allocation appends complete entries in request order. A batch
may cross Bucket boundaries, but an individual entry never does.

Each aligned entry contains:

1. an eight-byte encoded key length;
2. the key bytes;
3. the immutable value bytes;
4. zero padding up to the configured alignment.

The returned descriptor points to the value while `aligned_size` covers the
entire reserved entry. Clients reconstruct and validate the enclosing entry
from the descriptor and key before reading or writing. They cannot choose a
Bucket or overwrite an existing entry.

`Free` marks an entry as a tombstone. It does not reuse a hole in the middle of
a Bucket. This keeps offset ownership stable for concurrent readers and moves
physical reclamation to a single Bucket-level transaction.

## Write lifecycle

`PutStart` reserves an entry and publishes a `PROCESSING` DFS replica. Bucket
reservations initially exist only in allocator memory. A successful client
write calls `PutEnd(DFS)`, which changes the entry to committed under the same
Master metadata lock that marks the replica `COMPLETE`. A failed write calls
`PutRevoke(DFS)`, tombstoning the reservation and removing the processing
replica.

Bucket `BatchPut` reserves descriptors as a batch, allowing adjacent entries to
be emitted as bounded vectored writes. Before queuing an asynchronous write, the
client copies CPU buffers and stages GPU buffers into memory owned by the task.
This makes the caller's buffers reusable as soon as `BatchPut` returns. Upserts,
shard writes, and DFS requests combined with NoF remain synchronous.

Batch allocation rollback is all-or-nothing for allocation failure. Per-key
Master validation failures release only their unused preallocation, preserving
successful keys and the original request order.

## Metadata and recovery

A sealed Bucket owns one `<bucket>.meta` snapshot next to its data file. The
snapshot records layout, generation, and committed live entries. LRU order is
rebuilt conservatively from recovered bucket IDs; access timestamps are
runtime-only. The snapshot does not contain transient entry state, an
append-only log, or a rename-based publication sequence. Metadata rewrites are
deferred out of Master shard locks.

The active Bucket deliberately has no metadata snapshot. This keeps allocation
and commit free of metadata I/O. The consequence is explicit: after an abrupt
restart the active Bucket is discarded, while committed entries in sealed
Buckets are reconstructed as `COMPLETE` DFS replicas. Pending and tombstoned
entries are never restored. Corrupt metadata, orphan data files, incompatible
capacity, and interrupted-eviction markers are handled conservatively without
publishing a descriptor whose data file may be missing.

This recovery is specific to a standalone Master reopening the same DFS root.
Mooncake Master snapshots, oplog recovery, standby restore, and HA failover
remain incompatible with DFS.

## LRU eviction and capacity

Bucket access updates move the complete Bucket to the MRU side. The active
Bucket and Buckets with processing entries are not eligible for reclamation.
When usage crosses the high watermark, eviction continues toward the low
watermark. Allocation exhaustion may force one validated Bucket eviction even
when the high watermark has not been crossed.

Eviction is a two-phase transaction:

1. The allocator freezes one LRU Bucket and returns every live entry.
2. The Master locks all affected metadata shards in index order, verifies that
   every matching replica is complete, lease-expired, and not protected by hard
   or active soft pin, then removes all replicas while the locks remain held.
3. The allocator persists an eviction marker, drops the Bucket from its indexes,
   and deletes the metadata and data files.

Rejecting any entry aborts the whole Bucket transaction. The Bucket is unfrozen
and moved away from the cold edge so the same protected Bucket cannot starve the
rest of the LRU scan.

`MOONCAKE_DFS_MAX_BUCKET_COUNT` defines the logical capacity denominator and
limits new Bucket creation. `PUT /api/v1/dfs/max_bucket_count` can change the
positive limit online. Lowering the limit does not immediately delete existing
Buckets; raising it makes additional capacity available to subsequent
allocations.

## Batch reads and direct I/O

Bucket BatchRead validates every request, groups entries by Bucket, and runs
different Bucket groups in parallel. Adjacent entries may be merged into one
bounded read before their values are scattered to caller slices.

The POSIX adapter can open a read-only `O_DIRECT` handle. Because caller offsets
and buffers need not meet direct-I/O alignment, it reads the covering aligned
window into a bounded pool of aligned staging buffers and then copies only the
requested bytes. Pool exhaustion uses a transient aligned buffer. Filesystems
that reject `O_DIRECT` fall back to a buffered handle. Zero progress, EOF before
the requested range, and partial failures are errors; partial data is not
reported as success.

HF3FS keeps its adapter-specific batch read path and reports unsupported direct
handles through the common interface, allowing the backend to use the regular
path without changing descriptor semantics.

## Concurrency invariants

- Bucket allocation, commit, free, LRU updates, and limit changes share the
  allocator mutex.
- Filesystem I/O and metadata persistence do not run while holding that mutex.
- Master Bucket commit happens while holding the object's metadata shard lock.
- Whole-Bucket validation and replica removal happen under one set of ordered
  shard locks, closing the lease/pin race between those phases.
- A frozen Bucket accepts neither appends nor LRU refreshes until commit or
  abort resolves the transaction.
- Client destruction drains queued DFS writes before destroying the RPC client,
  backend, or staging pool.

These rules preserve immutable visibility: readers receive only descriptors for
complete data, and reclaimed files are removed from Master metadata before they
can disappear from the filesystem.
