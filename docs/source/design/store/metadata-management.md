# Metadata Management

This document describes how Mooncake Store organizes object metadata for
`Put`/`Get` operations: where metadata lives, how it is indexed for lookup,
and how it relates to the actual object data.

The primary sources for this document are `mooncake-store/include/master_service.h`,
`mooncake-store/include/replica.h`, and `mooncake-store/include/storage_backend.h`.

## Overview: Metadata/Data Plane Separation

Mooncake Store separates the metadata plane from the data plane:

- **Metadata** is centralized in the Master Service process and held entirely
  **in memory** as hash tables. The Master never stores object payloads; it only
  stores *location descriptors* that tell clients where the bytes are.
- **Object data** lives on the storage tier contributed by Clients: RDMA-visible
  memory segments, local SSD files, NVMe-oF targets, or a distributed
  filesystem (DFS). Transfers happen directly between Clients via the Transfer
  Engine, bypassing the Master.

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

All lookups go through RAII accessors (`MetadataAccessorRO` /
`MetadataAccessorRW`) that resolve the namespace and hold the key for their
scope; a thread holds one key at a time. Walks over a tenant's objects use the
route's cursors (`ReadCursor()` / `WriteCursor()`), which hold one key at a
time too. Next to the route, each namespace keeps the keys whose object has
work in flight and the eviction index eviction walks.

## ObjectMetadata and Replica Descriptors

Each key maps to one `ObjectMetadata` record:

| Field | Meaning |
|---|---|
| `client_id`, `put_start_time` | Writer identity and write-start time (used to reap stale `PutStart`s) |
| `size`, `object_checksum`, `data_type` | Object length, optional checksum, data-type classification |
| `tenant_id`, `user_key`, `group_id` | Identity (the key is stored again inside the record) and optional group for shared-TTL lifecycle |
| `lease`, `soft_pin_timeout`, `hard_pinned` | Read lease and pin state that control eviction eligibility |
| `replicas_` | `std::vector<Replica>` — the location descriptors |

A `Replica` is a `std::variant` over the storage media, with a serializable
`Descriptor` (`replica.h`):

| Replica type | Descriptor contents | Where the data actually is |
|---|---|---|
| `MEMORY` / `NOF_SSD` | `AllocatedBuffer::Descriptor{buffer_address, size, protocol, transport_endpoint}` | Remote segment memory, read directly over RDMA |
| `DISK` | `{file_path, object_size}` | A file on the Master's local filesystem (`ResolvePathFromKey`) |
| `LOCAL_DISK` | `{client_id, object_size, transport_endpoint}` | A store node's local SSD, proxied by that node |
| `DFS` | `{file_path, offset, object_size, aligned_size, shard_idx}` | Page offset in a distributed filesystem |

Replicas carry a status (`INITIALIZED -> PROCESSING -> COMPLETE | FAILED |
REMOVED`); only readable replicas (`COMPLETE` with a valid handle) are
returned to readers.

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
2. The client writes the object bytes directly into the referenced segment
   buffers through the Transfer Engine (RDMA), or to the disk/DFS paths.
3. **`PutEnd`**: after verifying that the caller is the original writer, the
   Master marks the replicas `COMPLETE`, grants the lease, and applies any
   soft-pin request. On failure the client calls **`PutRevoke`**, which
   removes the metadata and releases the buffers.

## Get Path: Query Then Transfer

1. The client issues `Query(key)`, an RPC to **`GetReplicaList`** on the
   Master. Using a read-only accessor, the Master collects the descriptors of
   all readable replicas, **grants a read lease** so the object cannot be
   evicted mid-read, and optionally records promotion-on-hit / dynamic
   replication signals. The response is
   `{replicas, lease_ttl_ms, object_checksum}` (`rpc_types.h`).
2. The client fetches the bytes according to the descriptor type:
   - `MEMORY` / `NOF_SSD`: RDMA-read `buffer_address` on `transport_endpoint`.
   - `LOCAL_DISK`: RPC `BatchGetOffloadObject` to the owning store node; the
     node loads the object from its SSD backend into staging buffers and
     replies `{batch_id, pointers, transfer_engine_addr}`, which the client
     then RDMA-reads.
   - `DISK` / `DFS`: read the file/range directly.
3. `ExistKey` walks the same index but only checks readability; admin
   interfaces (`GetAllKeys`, `GetReplicaListByRegex`) walk the tenant's route.

## Auxiliary Indexes

Besides the per-tenant routes, the Master maintains several secondary
structures:

- **`SoftPinDeadlineIndex`**: a min-heap keyed by soft-pin deadline plus a
  registration hash map for dedup/lazy deletion, letting a background thread
  pop expired soft-pins in batch without scanning all objects.
- **Group index** (one per route): `group_id -> {member_keys, shared lease}`.
  Lookups never use it; publishing and tearing down an object keep it in
  step, and group eviction reads it for all-or-none eviction.
- **`SegmentManager`** (`segment.h`): parallel maps from `segment_id`,
  `client_id`, segment name, and `host_id` to mounted segments and their
  allocators, used for allocation, preferred-segment selection, and
  segment-level operations (unmount, drain, eviction).

## SSD/Disk Backend Indexes (Client Side)

For `LOCAL_DISK` replicas the Master only records *which node holds the
object*; the key-to-on-disk-location mapping is maintained locally by that
node's `FileStorage` through a pluggable `StorageBackendInterface`
(`storage_backend.h`):

| Backend | On-disk layout | In-memory index |
|---|---|---|
| `kFilePerKey` | One file per object; the key is the filename | `file_queue_map_`: key -> iterator into a FIFO eviction list |
| `kBucket` | Objects packed into `<id>.bucket` data files as `[key bytes][value bytes]` records; per-bucket `<id>.meta` file persists the key list and offsets (struct_pack) | `object_bucket_map_`: key -> `{bucket_id, offset, key_size, data_size}`; per-bucket metadata map plus an LRU index ordered by last access; rebuilt by `ScanMeta` at startup |
| `kOffsetAllocator` | Single data file; each record is `[24B RecordHeader][key][4K padding][value]`, with a CRC covering header + key + value | Sharded (1024) maps of key -> `ObjectEntry{offset, sizes, allocation handle, fifo_seq}` plus a FIFO index `seq -> key`; allocator state can be persisted periodically for crash recovery |
| `kNvmeKv` | Key/value pairs stored directly in an NVMe KV namespace | Key codec/layout managed by the `nvme_kv_*` components |

Note the placement asymmetry: in the bucket and offset-allocator backends the
key is embedded *with the data* in the on-disk record (so a disk scan can
rebuild the index), while in the file-per-key backend the key is the filename
itself. In all cases the authoritative index used for lookups is the
in-memory hash map; persisted metadata files exist only to rebuild that index
after a restart.

## Persistence and HA

The Master's hash tables are not written to disk directly. Fault tolerance is
provided by:

- **Batch OpLog snapshots**: standby-generated snapshots persist a durable
  metadata baseline together with the OpLog cursor; the key and metadata are
  restored as `StandbyObjectEntry` records before suffix replay.
- **OpLog**: metadata mutations flow through the `OrderedOpLogWriter` to an
  HA KV backend (etcd or `HaKvBackend`), which the Standby replays into a
  `StandbyMetadataStore` (`tenant -> key -> StandbyObjectMetadata`). Snapshot
  entries are explicitly key+metadata bundles:
  `StandbyObjectEntry{tenant_id, key, metadata}` (`metadata_store.h`).

The `HttpMetadataServer` is unrelated to object metadata — it is a small
string-keyed HTTP KV service used by the Transfer Engine to exchange
connection information.

## Summary: Where Key, Metadata, and Data Live

- **Master memory**: the key and its metadata are stored together — the key
  is the slot-map key in the tenant's route, and `ObjectMetadata` also
  keeps a copy of `user_key`/`tenant_id` for serialization and reverse
  lookup.
- **Metadata vs. data**: always separated. The Master holds only location
  descriptors; object bytes stay on the storage nodes.
- **On disk**: bucket/offset-allocator backends embed the key inside the data
  file records (key stored with data) and keep the lookup index in memory,
  rebuilt from per-bucket metadata files or record scans; the file-per-key
  backend uses the key as the filename.
