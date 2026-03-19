# SSD Offload Design

## Overview

Mooncake Store supports offloading KV cache objects from distributed memory to local SSD. This extends the effective cache capacity beyond DRAM limits at lower cost, while preserving the performance characteristics of the hot path through zero-copy RDMA-based memory transfers.

SSD offload is implemented as a background subsystem within the **real client** process. It is transparent to the application: a `Put` that would otherwise be evicted from memory is persisted to disk, and a `Get` that finds no memory replica automatically falls back to reading from SSD.

---

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                  Application (vLLM, etc.)               │
└──────────────────────────┬──────────────────────────────┘
                           │ MooncakeDistributedStore API
                           ▼
┌─────────────────────────────────────────────────────────┐
│                      Real Client                        │
│                                                         │
│  ┌──────────────────────────────────────────────────┐   │
│  │                  FileStorage                     │   │
│  │  ┌────────────┐  ┌──────────────────────────┐    │   │
│  │  │  Heartbeat │  │   ClientBuffer (staging) │    │   │
│  │  │   Thread   │  └──────────────────────────┘    │   │
│  │  └─────┬──────┘                                  │   │
│  │        │ offload / load                          │   │
│  │        ▼                                         │   │
│  │  ┌─────────────────────────────────────────┐     │   │
│  │  │        StorageBackendInterface          │     │   │
│  │  │  ┌───────────┐ ┌──────────┐ ┌────────┐  │     │   │
│  │  │  │  Bucket   │ │FilePerKey│ │Offset  │  │     │   │
│  │  │  │  Backend  │ │ Backend  │ │Alloc.  │  │     │   │
│  │  │  └───────────┘ └──────────┘ └────────┘  │     │   │
│  │  └─────────────────────────────────────────┘     │   │
│  └──────────────────────────────────────────────────┘   │
│                                                         │
│  ┌──────────────────────────────────────────────────┐   │
│  │         In-memory distributed KV cache           │   │
│  │              (Transfer Engine / RDMA)            │   │
│  └──────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────┘
                           │
                    Local SSD / NVMe
```

The key components are:

- **FileStorage**: The top-level coordinator. It owns the storage backend, a staging buffer (`ClientBuffer`), and background threads for heartbeating and buffer garbage collection.
- **StorageBackendInterface**: An abstract interface implemented by three backends (see below). Responsible for the actual on-disk layout and I/O.
- **Heartbeat thread**: Periodically contacts the master. The master returns a list of objects to offload; the heartbeat thread writes them to SSD and notifies the master of completion.
- **ClientBuffer**: A pre-registered, O_DIRECT-aligned staging area used for zero-copy reads from SSD back into application memory.

---

## Data Flow

### Offload (memory → SSD)

The offload path is driven entirely by the heartbeat thread inside `FileStorage`. No write path from the application is involved.

```
Heartbeat Thread          Master                  Local Memory Segment
      │                     │                             │
      │─OffloadObjectHB ───▶│                             │
      │◀─ {key→size} map ───│  (objects to evict from     │
      │                     │   memory to SSD)            │
      │                     │                             │
      │─ BatchQuery(keys) ───────────────────────────────▶│
      │◀─ {key→Slice} ────────────────────────────────────│
      │                     │                             │
      │  [PrepareEviction: remove old buckets, notify master via BatchEvictDiskReplica]
      │─ BatchEvictDiskReplica(evicted_keys) ────────────▶│ (master removes stale replicas)
      │  [FinalizeEviction: delete evicted files]
      │                     │                             │
      │  BatchOffload(slices) → StorageBackend → SSD      │
      │                     │                             │
      │─ NotifyOffloadSuccess(keys, metadata) ───────────▶│
      │                     │  (master adds LOCAL_DISK    │
      │                     │   replica to object entry)  │
```

Step by step:

1. **Heartbeat**: The heartbeat thread wakes up every `MOONCAKE_OFFLOAD_HEARTBEAT_INTERVAL_SECONDS` seconds and calls `client_->OffloadObjectHeartbeat(enable_offloading_, offloading_objects)`. The master replies with a map of `{key → size}` for objects it has selected to evict from memory.
2. **Read slices from memory**: `FileStorage::OffloadObjects` groups the keys into buckets (for `BucketStorageBackend`) and calls `BatchQuerySegmentSlices` to obtain `{key → Slice}` from the local memory segment via `client_->BatchQuery`.
3. **Eviction** (if capacity limit is set): Before writing, `PrepareEviction` removes old buckets from metadata under the exclusive lock and collects their keys. The `eviction_handler` callback calls `client_->BatchEvictDiskReplica` to notify the master in a single RPC. `FinalizeEviction` then deletes the corresponding files.
4. **Write to SSD**: `StorageBackend::BatchOffload` serializes and writes the key-value data to disk.
5. **Notify master**: On success, the `complete_handler` calls `client_->NotifyOffloadSuccess(keys, metadatas)`. The master adds a `LOCAL_DISK` replica entry (carrying the real client's RPC address as `transport_endpoint`) to the object's replica list.

### Load (SSD → memory)

The load path involves three parties: the **requesting client**, the **target client** that holds the SSD data, and the **Transfer Engine** for zero-copy data movement.

```
Requesting Client                 Target Client                    Master
       │                                    │                         │
       │─ BatchGet(keys) ──────────────────────────────────────────▶│
       │◀─ QueryResult {replicas: [LOCAL_DISK(rpc_addr)]} ──────────│
       │                                    │                         │
       │  (no memory replica available)     │                         │
       │─ batch_get_offload_object(keys, sizes) ───────────────────▶│
       │                                    │                         │
       │                    FileStorage::BatchGet                     │
       │                    → StorageBackend::BatchLoad               │
       │                    → read from SSD into ClientBuffer         │
       │                                    │                         │
       │◀─ BatchGetOffloadObjectResponse ───│                         │
       │   {batch_id, pointers[], transfer_engine_addr, gc_ttl_ms}   │
       │                                    │                         │
       │─ Transfer Engine: BatchGetOffloadObject ──────────────────▶│
       │  (RDMA/TCP: pull data from ClientBuffer into app memory)     │
       │◀─ done ────────────────────────────│                         │
       │                                    │                         │
       │─ release_offload_buffer(batch_id) ────────────────────────▶│
       │                                    │ (free ClientBuffer slot)│
```

Step by step:

1. **Query master**: The requesting client calls `client_->BatchGet(keys, ...)` to query the master for replica locations. If the object has been offloaded, the master returns a `LOCAL_DISK` replica descriptor containing the target client's RPC address (`transport_endpoint`).
2. **RPC to target client**: The requesting client calls `batch_get_offload_object(keys, sizes)` on the target client identified by `transport_endpoint`. The target client calls `FileStorage::BatchGet`, which allocates slots in `ClientBuffer` and reads the requested objects from SSD via `StorageBackend::BatchLoad`.
3. **Response with buffer pointers**: The target client returns a `BatchGetOffloadObjectResponse` containing `batch_id`, a list of buffer `pointers` (addresses within `ClientBuffer`), the Transfer Engine address, and `gc_ttl_ms` (the buffer lease TTL).
4. **Zero-copy transfer**: The requesting client invokes `client_->BatchGetOffloadObject(transfer_engine_addr, keys, pointers, slices)`, which uses the Transfer Engine (RDMA or TCP) to pull the data directly from the target client's `ClientBuffer` into the application's target memory (DRAM or VRAM). No intermediate copy is made on the requesting client side.
5. **Release buffer**: After the transfer completes, the requesting client immediately calls `release_offload_buffer(batch_id)` on the target client to free the `ClientBuffer` slots. If the transfer takes longer than `gc_ttl_ms`, the buffer GC thread reclaims the slot automatically as a fallback.

---

## Storage Backends

### BucketStorageBackend (default)

Objects are grouped into **buckets** before being written to disk. Each bucket produces two files:

- **`.bucket`** — binary data file containing serialized key-value records
- **`.meta`** — metadata file describing the keys and byte offsets within the data file

Bucket IDs are monotonically increasing timestamps with a sequence suffix, so `buckets_` (a `std::map<int64_t, BucketMetadata>`) is always ordered by creation time.

**Grouping strategy** (`GroupOffloadingKeysByBucket`): objects are accumulated into a bucket until either `bucket_size_limit` (default 256 MB) or `bucket_keys_limit` (default 500) is reached. Objects that do not fill a complete bucket are held in `ungrouped_offloading_objects_` and retried on the next heartbeat.

**In-flight read tracking**: A `BucketReadGuard` RAII object increments `BucketMetadata::inflight_reads_` on construction and decrements it on destruction. This allows safe deletion of bucket files even when concurrent reads are in progress.

### StorageBackendAdaptor (FilePerKey)

Each object is stored as an individual file. The file path is derived from the key via a two-level hash-sharded directory structure to avoid large flat directories. This backend is simple and easy to inspect but does not scale well to millions of objects.

### OffsetAllocatorStorageBackend

A single pre-allocated file (`kv_cache.data`) is shared by all objects. Space within the file is managed by an `OffsetAllocator`. Metadata is sharded across 1024 independent maps to reduce lock contention under high concurrency. Records follow the layout `[key_len: u32 | value_len: u32 | key | value]`.

---

## Eviction (BucketStorageBackend)

When `MOONCAKE_BUCKET_MAX_TOTAL_SIZE` is set, the backend evicts existing buckets to make room before writing a new one. Eviction is disabled by default (`BucketEvictionPolicy::NONE`).

### Policies

| Policy | Candidate selection |
|--------|---------------------|
| `FIFO` | `buckets_.begin()` — always the oldest bucket, since `buckets_` is ordered by bucket ID |
| `LRU` | `std::min_element` over `BucketMetadata::last_access_ns_` — the bucket with the smallest last-read timestamp |

`last_access_ns_` is an atomic `int64_t` updated on every `BatchLoad` with relaxed ordering. Buckets that have never been read have `last_access_ns_ == 0` and are therefore always evicted first under LRU, giving FIFO-among-unread semantics.

### Two-phase eviction protocol

Eviction is split into two phases to ensure that the master is notified before files are deleted, and that no in-flight reads are interrupted.

**Phase 1 — `PrepareEviction(required_size)`** (called under exclusive lock):

1. Repeatedly call `SelectEvictionCandidate()` until `total_size_ + required_size <= max_total_size`.
2. For each selected bucket: remove it from `buckets_` and `object_bucket_map_`, subtract its size from `total_size_`.
3. Collect all evicted keys and bucket metadata into a `PendingEviction` struct and return it — no file I/O at this point.

**Between phases** — notify master:

The caller invokes the `eviction_handler` callback with the full list of evicted keys. The handler calls `MasterClient::BatchEvictDiskReplica`, which sends a single RPC to the master to remove the disk replicas for all evicted keys atomically.

**Phase 2 — `FinalizeEviction(pending)`** (called after master notification):

For each evicted bucket:
1. Spin-wait (with a 10-second timeout) until `inflight_reads_ == 0`.
2. Evict any stale file-handle cache entries.
3. Delete the `.bucket` and `.meta` files.

This ordering guarantees:
- The master never serves a stale disk-replica location for a file that has already been deleted.
- Ongoing reads complete successfully before their files are removed.
- Freed disk space is available for the incoming write before `WriteBucket` is called.

---

## io_uring File I/O

When `MOONCAKE_USE_URING=true`, the storage backends replace POSIX `pread`/`pwrite` calls with an io_uring-based implementation (`UringFile`). The design prioritizes eliminating inter-thread lock contention, which was the dominant latency source in the previous global-ring approach.

### Thread-local rings (`SharedUringRing`)

Each thread owns exactly one `io_uring` ring, stored in `thread_local` storage. This means:

- **No mutex between threads.** Each ring is accessed only by its owning thread, so concurrent I/O from multiple threads is fully parallel with zero synchronization overhead.
- **Within-thread batching.** Multiple SQEs can be enqueued before calling `io_uring_submit_and_wait`, exposing NVMe queue depth > 1 within a single thread. `batch_read` exploits this to issue up to `QUEUE_DEPTH` (32) independent reads in one submission.
- **File-descriptor registration is omitted.** The per-I/O `fdget()` overhead (~50 ns) is negligible compared to the lock contention (> 1 ms) the old global ring imposed, so `IOSQE_FIXED_FILE` is not used.

Rings are initialized lazily on first use and destroyed when the thread exits. If ring initialization fails (e.g., kernel too old), the backend falls back gracefully to POSIX I/O.

### Fixed-buffer registration

The `ClientBuffer` (the staging buffer used for SSD reads) is registered with io_uring as a **fixed buffer** via `io_uring_register_buffers`. When a read destination falls within the registered region, the backend uses `io_uring_prep_read_fixed` instead of `io_uring_prep_read`, which avoids a per-I/O `mmap`/`munmap` in the kernel and reduces system-call overhead.

Buffer registration is global but applied **lazily per thread**: `g_buf` stores the base address and length atomically; each thread-local ring calls `ensure_buf_registered()` on its first I/O and registers the buffer independently. This avoids a global barrier at startup.

To prevent `io_uring`'s `FOLL_LONGTERM` page pinning from failing on systems with Transparent Huge Pages (THP) enabled, `MADV_NOHUGEPAGE` is applied to the buffer region before registration. This forces the kernel to back the range with 4 KB pages, making long-term pinning reliable regardless of system THP policy.

### O_DIRECT and alignment

`UringFile` supports an optional `O_DIRECT` mode. When enabled:

- All file descriptors are opened with `O_DIRECT`.
- Buffers, lengths, and offsets must be aligned to 4 KB (`ALIGNMENT_ = 4096`).
- For unaligned writes (e.g., metadata serialized into a `std::string`), the backend allocates a temporary aligned bounce buffer via `posix_memalign`, copies the data, performs the aligned write, and frees the bounce buffer.
- `read_aligned` and `write_aligned` are the primary I/O paths; they assert alignment constraints and delegate directly to the ring.

### I/O operations

| Method | Description |
|--------|-------------|
| `read` / `write` | Contiguous read or write, chunked into up to `QUEUE_DEPTH` SQEs per submission |
| `read_aligned` / `write_aligned` | Same as above but with alignment preconditions for O_DIRECT |
| `batch_read` | Submits multiple independent reads (different offsets) in batches of up to `QUEUE_DEPTH`, maximizing NVMe queue utilization |
| `vector_read` / `vector_write` | Scatter/gather I/O: one SQE per `iovec`, submitted in batches |
| `datasync` | Issues `IORING_FSYNC_DATASYNC` and waits for completion |

### Integration with storage backends

- **BucketStorageBackend**: uses `UringFile` for both bucket data files and metadata files when `use_uring_` is set. A file-handle cache (`file_cache_`) avoids repeated `open`/`close` for hot buckets. On eviction, the cache entry is explicitly removed before the file is deleted to prevent stale handles.
- **OffsetAllocatorStorageBackend**: opens the single pre-allocated data file with `O_DIRECT` and `UringFile`, and uses `GetFileInstance()` to expose the file handle for external buffer registration.
- **StorageBackendAdaptor** (FilePerKey): uses `UringFile` for reads when `use_uring_` is set; writes use POSIX paths.

## Metadata Recovery on Restart

On startup, `FileStorage::Init` calls `StorageBackend::ScanMeta`, which reads all on-disk metadata and invokes a callback for each discovered object. The callback calls `MasterClient::NotifyOffloadSuccess` to re-register the objects with the master. This restores the full disk-replica view without any application-level intervention.
