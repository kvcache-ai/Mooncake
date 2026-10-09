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

## Master-Side Index: Sharded Tenant/Key Hash Tables

Object metadata is organized as a three-level hash index
(`master_service.h`):

```text
shard_idx = hash(tenant_id, key) % 1024      // default tenant: hash(key) only
   -> MetadataShard.tenants[tenant_id]       // level 1: tenant
   -> TenantState.metadata[key]              // level 2: user key
   -> ObjectMetadata                         // level 3: the metadata record
```

```cpp
static constexpr size_t kNumShards = 1024;

struct MetadataShard {
    SharedMutex mutex;
    std::unordered_map<TenantId, TenantState, TenantIdHash> tenants;
    long disk_object_count;  // objects with a completed LOCAL_DISK replica
};

struct TenantState {
    std::unordered_map<std::string, ObjectMetadata> metadata;
    std::unordered_set<std::string> processing_keys;
    std::unordered_map<std::string, ReplicationTask> replication_tasks;
    std::unordered_map<std::string, PromotionTask> promotion_tasks;
    // ... dynamic-replication and promotion-candidate state
};

std::array<MetadataShard, kNumShards> metadata_shards_;
```

Sharding keeps lock contention low: each shard has its own `SharedMutex`, and
all lookups go through RAII accessors (`MetadataAccessorRO` /
`MetadataAccessorRW`) that compute the shard index, take the appropriate
shared/exclusive lock, and resolve the tenant and key iterators in one shot.
Batch interfaces (`BatchExistKey`, `BatchGetReplicaList`, `BatchPutEnd`, ...)
first group keys by shard so each shard is locked only once per batch.

An additional stripe of 4096 object-operation mutexes serializes multi-step
write operations (e.g. `PutStart`) for the same key.

## ObjectMetadata and Replica Descriptors

Each key maps to one `ObjectMetadata` record:

| Field | Meaning |
|---|---|
| `client_id`, `put_start_time` | Writer identity and write-start time (used to reap stale `PutStart`s) |
| `size`, `object_checksum`, `data_type` | Object length, optional checksum, data-type classification |
| `tenant_id`, `user_key`, `group_id` | Identity (the key is stored again inside the record) and optional group for shared-TTL lifecycle |
| `lease`, `soft_pin_timeout`, `hard_pinned` | Read lease and pin state that control eviction eligibility |
| `quota_ledger` | Per-tenant quota accounting for this object |
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
   object-operation stripe lock and the shard write lock, then checks for
   duplicates — a completed object or a fresh in-progress write returns
   `OBJECT_ALREADY_EXISTS`, while an expired in-progress write has its stale
   `PROCESSING` replicas moved to a deferred-release list. The Master then
   charges tenant quota, allocates buffers on segments via the
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
   interfaces (`GetAllKeys`, `GetReplicaListByRegex`) scan all 1024 shards.

## Auxiliary Indexes

Besides the primary sharded tables, the Master maintains several secondary
structures:

- **`SoftPinDeadlineIndex`**: a min-heap keyed by soft-pin deadline plus a
  registration hash map for dedup/lazy deletion, letting a background thread
  pop expired soft-pins in batch without scanning all objects.
- **Group domain** (`group_domain_`): `group_id -> {member_keys, shared lease}`.
  Object routing never uses it (always `hash(tenant, key)`); it is consulted
  only at put/registration time and for all-or-none group eviction.
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

- **Snapshots**: each `ObjectMetadata` is serialized as a `MetadataPayload`
  (struct_pack/msgpack: `client_id, size, replicas, group_id, data_type,
  hard_pinned` — the key is *not* in the payload; it travels as the enclosing
  map key) via `MasterSnapshotManager`/`MasterSnapshotRepository`.
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
  is the hash-table key in `TenantState::metadata`, and `ObjectMetadata` also
  keeps a copy of `user_key`/`tenant_id` for serialization and reverse
  lookup.
- **Metadata vs. data**: always separated. The Master holds only location
  descriptors; object bytes stay on the storage nodes.
- **On disk**: bucket/offset-allocator backends embed the key inside the data
  file records (key stored with data) and keep the lookup index in memory,
  rebuilt from per-bucket metadata files or record scans; the file-per-key
  backend uses the key as the filename.
