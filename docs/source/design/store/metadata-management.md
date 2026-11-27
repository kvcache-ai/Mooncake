# Metadata Management

This document describes how Mooncake Store organizes object metadata for
`Put`/`Get` operations: where metadata lives, how it is indexed for lookup,
and how it relates to the actual object data.

The main object and replica types are defined in
[`object_metadata.h`](gh-file:mooncake-store/include/object_metadata.h),
[`object_part.h`](gh-file:mooncake-store/include/object_part.h), and
[`replica.h`](gh-file:mooncake-store/include/replica.h).

## Overview: Metadata/Data Plane Separation

Mooncake Store separates the metadata plane from the data plane:

- **Live metadata** is held in the Master Service's tenant routes, including
  object identity, replica locations, and lifecycle state. HA deployments can
  also persist metadata through snapshots and the OpLog.
- **Object data** lives in client-contributed memory segments, NVMe-oF targets,
  filesystem storage, or object storage. Clients use the Transfer Engine or
  the relevant storage backend to move bytes without routing them through the
  Master.

A `Get` therefore consists of two steps: a metadata query against the Master
(which returns replica descriptors plus a read lease), followed by a data
transfer that the Client performs on its own using those descriptors.

## Master-Side Index: Per-Tenant Object Routes

Object metadata is organized per tenant (`master_service.h`,
`metadata/namespace_table.h`, `metadata/object_route.h`):

```text
namespaces_.Lookup(tenant_id)                // level 1: one namespace per tenant
   -> Namespace.objects (route::ObjectRoute)
   -> stripe = hash(key) % 1024              // a stripe of the route's slot map
   -> KeySlot{lock, record}                  // level 2: one slot per key
   -> {generation, ObjectMetadata, ObjectState}  // level 3: the publication
```

A namespace is created on a tenant's first write and stays for as long as the
service runs. Its route keeps one `KeySlot` per key, holding that key's lock
and, while the key holds an object, the object's record. The lock belongs to
the key, not to a shard of keys: a stripe lock is held only to find, create or
drop a slot. Each publication gets a generation, so code that lets go of a key
and comes back later (a scan that evicts afterwards, a durable OpLog callback)
names the object with an `ObjectRef` and acts only while that same publication
is still there.

Point operations use RAII accessors (`MetadataAccessorRO` /
`MetadataAccessorRW`) that resolve the namespace and hold the key for their
scope. Walks over a tenant's objects use the route's `ReadCursor()` or
`WriteCursor()`, which hold one key at a time. Each namespace also tracks keys
with work in flight and maintains the eviction index.

## ObjectMetadata and Replica Descriptors

Each key maps to one `ObjectMetadata` record:

| Field | Meaning |
|---|---|
| `client_id`, `put_start_time` | Writer identity and write-start time (used to reap stale `PutStart`s) |
| `data_type` | Data-type classification |
| `tenant_id`, `user_key`, `group_id` | Identity (the key is stored again inside the record) and optional group for shared-TTL lifecycle |
| `lease_`, `soft_pin_timeout`, `hard_pinned` | Read lease and pin state that control eviction eligibility |
| `parts_` | `boost::container::small_vector<ObjectPart, 1>`. Each object currently has one part, which holds the length, optional checksum, and replicas in a `small_vector<Replica, 1>`. |

A serializable `Replica::Descriptor` contains the replica ID, status, and a
location variant determined by the storage tier.

| Replica type | Descriptor contents | Where the data actually is |
|---|---|---|
| `MEMORY` | `AllocatedBuffer::Descriptor` with address, size, protocol, and transport endpoint, plus optional segment locality in `MemoryDescriptor` | A mounted memory segment |
| `NOF_SSD` | `AllocatedBuffer::Descriptor` in `NoFDescriptor` | A mounted NVMe-oF segment |
| `DISK` | `{file_path, object_size}` | A file under the configured shared filesystem root, with a path derived by `ResolvePathFromKey` |
| `LOCAL_DISK` | `{client_id, object_size, transport_endpoint}` | A client-owned offload backend accessed through its owner |
| `DFS` | `DistributedFSDescriptor{file_path, offset, object_size, aligned_size, shard_idx}` | A shared filesystem range, or an object in KVCS when using the `kvcs-lowlevel` adapter |

The Master returns replicas that are `COMPLETE`, available, and ready to serve
after any required remount.

## Put Path: Two-Phase Metadata

`Put` is a two-phase protocol so that a partially written object is never
visible to readers:

1. **`PutStart`** (`master_service.cpp`): validates the request, takes the
   key's write lock, then checks for
   duplicates — a completed object or a fresh in-progress write returns
   `OBJECT_ALREADY_EXISTS`, while an expired in-progress write has its stale
   `PROCESSING` replicas moved to a deferred-release list. The Master then
   asks the namespace policy (the tenant quota, with multi-tenancy on) for the
   memory the write takes, allocates buffers on segments via the
   `AllocationStrategy` over the `SegmentManager`'s per-segment allocators,
   and **inserts the `ObjectMetadata` with replicas in `PROCESSING` state**,
   returning the replica descriptors to the client.
2. The client writes the object bytes to the returned locations through the
   Transfer Engine or the storage backend. The
   {ref}`store-put-start-replica-allocation` describe which allocations can
   proceed to this step.
3. **`PutEnd`** verifies the writer's client ID, marks the selected replicas
   `COMPLETE`, and applies any pending soft-pin action when an eligible
   replica completes. It updates the lease deadline with zero TTL, preserving
   any later deadline already in effect. On failure, **`PutRevoke`** removes
   the selected pending replicas and releases their resources. The object
   record can remain when another replica survives.

## Get Path: Query Then Transfer

1. The client issues `Query(key)`, an RPC to **`GetReplicaList`** on the
   Master. Using a read-only accessor, the Master collects the descriptors of
   all readable replicas, **grants a read lease** so the object cannot be
   evicted mid-read, and optionally records promotion-on-hit / dynamic
   replication signals. The response contains `replicas`, `lease_ttl_ms`, an
   optional `object_checksum`, and the compatible `resolved_tenant_id` field.
   The client uses the resolved tenant when it asks an owner to verify a
   local-disk replica.
2. The client fetches the bytes according to the descriptor type:
   - `MEMORY` / `NOF_SSD` reads use the buffer address, transport endpoint,
     and protocol from the descriptor.
   - For a remote `LOCAL_DISK` owner, `BatchGetOffloadObject` stages the object
     and replies with `batch_id`, `pointers`, `transfer_engine_addr`, and
     `gc_ttl_ms`. The requester fetches the bytes through the Transfer Engine.
   - `DISK` reads the shared file. `DFS` reads through the configured
     distributed storage adapter.
3. `ExistKey` looks up the key and extends the read lease on a readable hit.
   `GetReplicaListByRegex` walks the resolved tenant's route and grants a lease
   for each returned object. The compatibility admin endpoint `/get_all_keys`
   lists keys from the default tenant.

## Auxiliary Indexes

Besides the per-tenant routes, the Master maintains several secondary
structures:

- **`SoftPinDeadlineIndex`**: a min-heap keyed by soft-pin deadline plus a
  registration hash map for dedup/lazy deletion, letting a background thread
  pop expired soft-pins in batch without scanning all objects.
- **Group index** (one per route) maps each `group_id` to its member keys and
  shared lease. Publication and teardown update membership. Eviction consults
  the shared lease and checks each member's eligibility under its key lock.
- **`SegmentManager`** (`segment.h`): parallel maps from `segment_id`,
  `client_id`, segment name, and `host_id` to mounted segments and their
  allocators, used for allocation, preferred-segment selection, and
  segment-level operations (unmount, drain, eviction).

## SSD/Disk Backend Indexes (Client Side)

For `LOCAL_DISK` replicas, the Master records the owner, object size, and
transport endpoint. The owner's `FileStorage` resolves the physical location
through its `StorageBackendInterface`. It passes tenant-scoped storage keys to
the backend, including for the default tenant.

| Backend | On-disk layout | In-memory index |
|---|---|---|
| `kFilePerKey` | One file per object under two hash-derived directories. The filename is derived from the sanitized storage key, and the file serializes both key and value. | `file_queue_map_` indexes the FIFO eviction list. Point reads derive the path from the storage key. |
| `kBucket` | `<id>.bucket` stores packed key/value records. The companion `<id>.meta` persists keys, offsets, and sizes. | `object_bucket_map_` maps each storage key to its bucket and record location. Each disk has its own eviction indexes. Initialization rebuilds the index from bucket metadata, and `ScanMeta` reports recovered objects to the Master. |
| `kOffsetAllocator` | A shared data file stores a 24-byte record header, the key, padding up to the 4 KiB-aligned value start, and the value. A flag indicates whether the record has a CRC-32C. | 1024 shards map storage keys to offsets, sizes, allocation handles, and FIFO sequence numbers. Optional `relaxed` and `strict` persistence modes support restart recovery. |
| `kNvmeKv` | Key/value pairs stored directly in an NVMe KV namespace | Key codec/layout managed by the `nvme_kv_*` components |
| `kDistributed` (OSS/S3) | One object per storage key under an owner prefix. | The adapter derives the physical object name from the storage key. `ScanMeta` lists the owner's keys for registration. |

## Persistence and HA

With batch OpLog recovery enabled, the standby reconstructs the Master's
metadata from a snapshot baseline and the subsequent log entries.

- **Batch OpLog snapshots**: standby-generated snapshots persist a durable
  metadata baseline together with the OpLog cursor; the key and metadata are
  restored as `StandbyObjectEntry` records before suffix replay.
- **OpLog**: metadata mutations flow through the `OrderedOpLogWriter` to the
  configured etcd or Redis HA backend. The standby replays them into a
  `StandbyMetadataStore` (`tenant -> key -> StandbyObjectMetadata`). Snapshot
  entries are explicitly key+metadata bundles:
  `StandbyObjectEntry{tenant_id, key, metadata}` (`metadata_store.h`).

The `HttpMetadataServer` is a separate HTTP key-value service for Transfer
Engine connection metadata.

## Summary: Where Key, Metadata, and Data Live

- **Master memory**: the key and its metadata are stored together — the key
  is the slot-map key in the tenant's route, and `ObjectMetadata` also
  keeps a copy of `user_key`/`tenant_id` for serialization and reverse
  lookup.
- **Storage backends**: Bucket, Offset Allocator, and FilePerKey store the
  storage key alongside the value. FilePerKey can also derive a lookup path
  directly from that key. NVMe KV and object storage use backend-specific
  physical keys.
