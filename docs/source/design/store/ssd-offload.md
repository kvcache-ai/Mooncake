# SSD Offload Design

## Overview

Mooncake Store supports offloading KV cache objects from distributed memory to local SSD (and, via `DistributedStorageBackend`, to object storage). This extends the effective cache capacity beyond DRAM limits at lower cost, while preserving the performance characteristics of the hot path through zero-copy RDMA-based memory transfers.

SSD offload is implemented as a background subsystem within the **real client** process. It is transparent to the application: when `--enable_offload=true` is set on the master, objects with a DRAM replica on an offload-enabled client are queued for asynchronous disk persistence (eagerly at `PutEnd` by default, or on memory eviction when `--offload_on_evict=true`), and a `Get` that finds no memory replica automatically falls back to reading from SSD.

For multi-turn conversation benchmark results, see [Mooncake SSD Offload Benchmark](../../performance/mooncake/ssd-offload-benchmark-results.md).

---

## Architecture

```
┌───────────────────────────────────────────────────────────────────┐
│                    Application (vLLM, SGLang, etc.)               │
└───────────────────────────────┬───────────────────────────────────┘
                                │ MooncakeDistributedStore API
                                ▼
┌───────────────────────────────────────────────────────────────────┐
│                           Real Client                             │
│                                                                   │
│  ┌─────────────────────────────────────────────────────────────┐  │
│  │                        FileStorage                          │  │
│  │  ┌────────────┐  ┌──────────────────────────────────┐       │  │
│  │  │  Heartbeat │  │     ClientBuffer (staging)       │       │  │
│  │  │   Thread   │  └──────────────────────────────────┘       │  │
│  │  └─────┬──────┘                                             │  │
│  │        │ offload / load / promotion / watermark eviction    │  │
│  │        ▼                                                    │  │
│  │  ┌───────────────────────────────────────────────────────┐  │  │
│  │  │               StorageBackendInterface                 │  │  │
│  │  │ ┌──────┐ ┌──────────┐ ┌────────┐ ┌──────┐ ┌─────────┐ │  │  │
│  │  │ │Bucket│ │FilePerKey│ │Offset  │ │NVMe  │ │Distrib. │ │  │  │
│  │  │ │      │ │          │ │Alloc.  │ │KV    │ │Storage  │ │  │  │
│  │  │ └──────┘ └──────────┘ └────────┘ └──────┘ └─────────┘ │  │  │
│  │  └───────────────────────────────────────────────────────┘  │  │
│  └─────────────────────────────────────────────────────────────┘  │
│                                                                   │
│  ┌─────────────────────────────────────────────────────────────┐  │
│  │              In-memory distributed KV cache                 │  │
│  │                  (Transfer Engine / RDMA)                   │  │
│  └─────────────────────────────────────────────────────────────┘  │
└───────────────────────────────────────────────────────────────────┘
                                │
             Local SSD / NVMe (or Object Storage / DFS)
```

The key components are:

- **FileStorage**: The top-level coordinator. It owns the storage backend, a staging buffer (`ClientBuffer`), and background threads for heartbeating and buffer garbage collection.
- **StorageBackendInterface**: An abstract interface implemented by five backends (`StorageBackendType`): `BucketStorageBackend` (`kBucket`, default), `StorageBackendAdaptor` (`kFilePerKey`), `OffsetAllocatorStorageBackend` (`kOffsetAllocator`), `NvmeKvStorageBackend` (`kNvmeKv`; see [NVMe KV Storage Backend](nvme-kv-backend.md)), and `DistributedStorageBackend` (`kDistributed`, selecting `posix`, `hf3fs` when built with `USE_3FS`, `oss`/`s3` when built with libcurl and OpenSSL adapter support, or `kvcs-lowlevel`). Within `DistributedStorageBackend`, `posix`, `hf3fs`, and `kvcs-lowlevel` run in `DFS` mode (`FileStorage::Init()` skips `LOCAL_DISK` mounting and the offload heartbeat), whereas `oss` and `s3` run under the `LOCAL_DISK` offload model (see [OSS Backend](oss-backend.md) and [Object Storage Offload](../../deployment/object-storage-offload.md)).
- **Heartbeat thread**: Periodically contacts the master to fetch queued offload tasks, writes them to SSD, notifies the master of completion, drives best-effort L2→L1 promotions, and runs proactive disk-watermark eviction when enabled.
- **ClientBuffer**: A pre-registered, `O_DIRECT`-aligned staging area used for zero-copy reads from SSD back into application memory.

---

## Data Flow

### Offload (memory → SSD)

Offload is split into two stages: **task enqueueing on the master** and **asynchronous persistence by the heartbeat thread inside `FileStorage`**. No synchronous disk I/O occurs on the application write path.

Master offload flags default to `--enable_offload=false` and `--offload_on_evict=false`:

- **Eager at `PutEnd` (default when `--enable_offload=true`)**: When `PutEnd` completes an object that has a DRAM replica on an offload-enabled client, the master calls `PushOffloadingQueue` to enqueue an offload task for that owner.
- **Deferred until memory eviction (`--offload_on_evict=true`)**: `PutEnd` skips enqueueing; instead, `OffloadingReclaimer` queues objects selected by memory eviction. Do not combine `--offload_on_evict=true` with `--enable_oplog=true`, because memory eviction prioritizes `OpLogReclaimer` over `OffloadingReclaimer`, leaving both normal offload enqueue paths inactive.

```
Heartbeat Thread          Master                  Local Memory Segment
      │                     │                             │
      │─ OffloadObjectHB ──▶│                             │
      │◀─ [OffloadTaskItem]─│  (queued at PutEnd or       │
      │                     │   memory eviction)          │
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

1. **Heartbeat**: Every `MOONCAKE_OFFLOAD_HEARTBEAT_INTERVAL_SECONDS` seconds, the heartbeat thread calls `client_->OffloadObjectHeartbeat(enable_offloading_, offloading_objects)`. The master returns a list of `OffloadTaskItem` entries (`{tenant_id, key, size}`) queued for this client.
2. **Read slices from memory**: `FileStorage::OffloadObjects` groups the keys into buckets (for `BucketStorageBackend`) and calls `BatchQuerySegmentSlices` to obtain `{key → Slice}` from the local memory segment via `client_->BatchQuery`.
3. **Write-time eviction** (when enabled and capacity is reached): Before writing, the backend selects eviction candidates, invokes `eviction_handler` (`client_->BatchEvictDiskReplica`) to remove stale `LOCAL_DISK` replicas from the master in one batched RPC, and then frees the corresponding on-disk storage.
4. **Write to SSD**: `StorageBackend::BatchOffload` serializes and writes the key-value data to disk.
5. **Notify master**: On success, `complete_handler` calls `client_->NotifyOffloadSuccess(keys, metadatas)`. The master adds a `LOCAL_DISK` replica entry (carrying the real client's RPC address as `transport_endpoint`) to the object's replica list.

The heartbeat loop also coordinates two additional lifecycle paths:

- **L2→L1 promotion**: After offload work, `FileStorage::Heartbeat()` calls `ProcessPromotionTasks()` to promote hot SSD-only objects back into DRAM on a best-effort basis; see [SSD Prefetch and Promotion](ssd-prefetch.md).
- **Master restart recovery**: If `OffloadObjectHeartbeat` receives `SEGMENT_NOT_FOUND`, `FileStorage` re-mounts its `LOCAL_DISK` segment, re-reports configured SSD capacity, and launches an asynchronous `ScanMeta` rescan (`ReRegisterOffloadedObjects`) to restore master metadata.

### Load (SSD → memory)

The default load path involves three parties: the **requesting client**, the **target client** that holds the SSD data, and the **Transfer Engine** for data movement.

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

For a same-process GPU destination, `FileStorage` reads into a quota-bounded pinned restore arena and submits H2D copies to the caller's GPU slices. Tensor ranges use a source offset, avoiding another CPU staging allocation. This path requires local memcpy and a pinned quota; CPU and remote restores are unchanged.

Step by step:

1. **Query master**: The requesting client calls `client_->BatchGet(keys, ...)` to query the master for replica locations. If the object has been offloaded, the master returns a `LOCAL_DISK` replica descriptor containing the target client's RPC address (`transport_endpoint`).
2. **Select the owner path**: For a different process, the requesting client calls `batch_get_offload_object(keys, sizes)` on the target client identified by `transport_endpoint`. For a same-process GPU restore with pinned quota and local memcpy enabled, it calls the local `FileStorage::BatchGetLocal` path.
3. **Response with buffer pointers**: Remote requests receive pointers within `ClientBuffer`, plus a `batch_id`, Transfer Engine address, and buffer lease TTL. The local path receives pointers within the pinned restore arena and keeps their allocation owner in the requesting process.
4. **Transfer data**: Remote restores use the Transfer Engine (RDMA or TCP). The same-process pinned branch submits H2D copies from the local restore arena and supports per-object source offsets for tensor payloads.
5. **Release buffer**: Remote restores call `release_offload_buffer(batch_id)` and retain TTL GC as a failure fallback. A same-process allocation is held by an RAII owner through the synchronous H2D operation and released automatically on success or failure; it is never published in the remote batch map.

### Failed-Disk Tier Fencing

When a local SSD fails at runtime, `FileStorage` can fence the client's `LOCAL_DISK` tier so the master stops routing reads or offload work to the dead disk. Currently, only `BucketStorageBackend` wires disk I/O errors into `NotifyDiskError`.

- **Error classification (`FileStorage::ClassifyDiskErrno`)**:
  - `kFence` (`ENODEV`, `ENXIO`, `EROFS`, `ESHUTDOWN`): immediately fences the disk tier.
  - `kCheckFilesystem` (`EIO`, `EUCLEAN`, `EBADMSG`, `ENODATA`, `EREMOTEIO`, `EILSEQ`, `ETIMEDOUT`, `ENOLINK`): probes every configured disk root (calling `syncfs` when needed followed by `fstat`/`stat`) and fences only if a root reports a non-ignored filesystem error.
  - `kIgnore` (including `ENOSPC` and other non-fatal errors): does not fence.
- **Fence transition (`ApplyDiskFence`)**: `FileStorage` disables `enable_offloading_`, sets `draining_ = true`, clears carried-over ungrouped objects and NACKs deferred offload tasks so source DRAM replicas are not pinned until TTL expiry, fails disk reads with `FILE_READ_FAIL`, and retries `UnmountLocalDiskSegment` on subsequent heartbeats until the master removes the client's `LOCAL_DISK` replicas.

---

## Storage Backends

### BucketStorageBackend (default)

Objects are grouped into **buckets** before being written to disk. Each bucket produces two files named with a pure decimal integer ID (`<bucket_id>.bucket` and `<bucket_id>.meta`):

- **`<bucket_id>.bucket`** — binary data file containing serialized key-value records
- **`<bucket_id>.meta`** — metadata file describing the keys and byte offsets within the data file

Bucket IDs are monotonically increasing integers generated by `BucketIdGenerator` from a shifted timestamp mixed with `getpid() & SEQUENCE_MASK` (`SEQUENCE_BITS = 12`, so PIDs alias mod 4096), so `buckets_` (a `std::map<int64_t, BucketMetadata>`) is ordered by creation time. The PID salt only narrows the collision window; exclusive directory locking (`flock`) and `O_EXCL` on bucket creation remain the load-bearing guards.

**Multi-disk layout**: `MOONCAKE_OFFLOAD_FILE_STORAGE_PATH` accepts a comma-separated list of directories. `BucketStorageBackend` maintains an independent `DiskState` per disk (quota, physical usage cache, `fifo_index`, and `lru_index`) and selects target disks round-robin via `SelectDiskForWrite`.

**Grouping and carryover (`GroupOffloadingKeysByBucket`)**: Objects are accumulated into a bucket until either `bucket_size_limit` (default 256 MB) or `bucket_keys_limit` (default 500) is reached. Objects in an underfilled tail are held in `ungrouped_offloading_objects_`, while `FileStorage` retains their `OffloadTaskItem` state across heartbeats in `deferred_task_by_storage_key_`:

- On a subsequent heartbeat with **no new tasks**, `OffloadObjects` still runs and `GroupOffloadingKeysByBucket` (`replaying_complete_carryover`) forms a bucket from the carried-over objects so they are written even if below the bucket limits.
- When **new tasks arrive**, carried-over objects are merged into the first bucket; if a new underfilled tail remains and `MOONCAKE_OFFLOAD_FLUSH_PARTIAL_BUCKETS=0` (default), that tail continues to defer.
- Set `MOONCAKE_OFFLOAD_FLUSH_PARTIAL_BUCKETS=1` before starting a client to flush the final partial bucket in the **same** offload cycle instead of deferring it to the next heartbeat. Smaller buckets can increase file and metadata overhead and reduce write efficiency, and this setting does not guarantee persistence if the write itself fails.

**Aligned direct writes (`O_DIRECT`)**: Set `MOONCAKE_OFFLOAD_BUCKET_DIRECT_WRITE=aligned` to write bucket data files with `O_DIRECT` instead of through the page cache. Each object then takes whole 4 KiB blocks: zeros and the key, ending on a block boundary, then the value padded with zeros to a block boundary. The metadata offset still points at the key, so readers find the value at `offset + key_size` as before, and the logical sizes in the metadata exclude the padding. When every slice of a value starts on a 4 KiB boundary and every slice but the last is a 4 KiB multiple, its whole blocks are written from the caller's buffers; the key blocks, the last partial block of each value, and values with other slices are copied into an aligned per-thread buffer first. The writes go out with `pwritev`, at most `IOV_MAX` iovecs each, and `fdatasync` and the `.meta` write follow as before. If the file system rejects `O_DIRECT`, or its direct I/O alignment exceeds 4 KiB, the same aligned writes go through the page cache. A bucket being written reserves its padded size, which the `max_total_size`, free-space, and `max_physical_bytes` checks see; once it is written, the total size and the space an evicted bucket is expected to free count only keys and values, and so does `bucket_size_limit`. Offloaded data does not stay in the page cache, so reads of freshly offloaded objects go to the device.

**In-flight read tracking**: A `BucketReadGuard` RAII object increments `BucketMetadata::inflight_reads_` on construction and decrements it on destruction. This allows safe deletion of bucket files even when concurrent reads are in progress.

**Ownership contract (`storage_path`)**: `BucketStorageBackend` enforces **one live client per disk directory**. `BucketStorageBackend::Init()` opens `.mooncake_local_disk.lock` in each configured directory and takes a non-blocking exclusive `flock`; a second live client fails `Init()` immediately with an actionable error (`storage_path already held by another live client … use standalone store or per-client directories`). Do not delete the lock file while a client is running — that releases the `flock` for other processes. `flock` is only meaningful on local filesystems (NFS and similar may not honor exclusive locks the same way).

### StorageBackendAdaptor (FilePerKey)

Each object is stored as an individual file. The file path is derived from the key via a two-level hash-sharded directory structure to avoid large flat directories. This backend is simple and easy to inspect but does not scale well to millions of objects.

### OffsetAllocatorStorageBackend

A single pre-allocated file (`kv_cache.data`) is shared by all objects. Space within the file is managed by an `OffsetAllocator`. Metadata is sharded across 1024 independent maps to reduce lock contention under high concurrency. Records follow the layout `[key_len: u32 | value_len: u32 | seq: u64 | flags: u32 | crc32: u32 | key | zero padding | value]`. The value region always starts at a 4 KiB boundary within the record (padding is a pure function of `key_len`) so that DMA writers (e.g. GDS/cuFile) can use aligned file offsets. `seq` is a monotonic stamp used by restart recovery to drop post-checkpoint writes; `crc32` is a CRC-32C over header prefix + key + value and is present only when `flags & kFlagHasCrc` (see `enable_record_crc` in `OffsetAllocatorBackendConfig`). See [Eviction](#eviction) for write-time FIFO eviction and [Metadata Recovery on Restart](#metadata-recovery-on-restart) for persistence modes.

### Other Backends

- **NvmeKvStorageBackend (`nvme_kv_storage_backend`)**: Writes directly to an NVMe KV namespace; see [NVMe KV Storage Backend](nvme-kv-backend.md).
- **DistributedStorageBackend (`distributed_storage_backend`)**: Routes `LOCAL_DISK` offload to object storage when configured with `oss` or `s3` (see [OSS Backend](oss-backend.md) and [Object Storage Offload](../../deployment/object-storage-offload.md)), or runs in `DFS` mode for `posix`, `hf3fs`, and `kvcs-lowlevel` (skipping `LOCAL_DISK` offload).

---

## Eviction

### Write-time quota eviction

#### BucketStorageBackend

At runtime, `BucketBackendConfig::FromEnvironment()` defaults `MOONCAKE_OFFLOAD_BUCKET_EVICTION_POLICY` to **`fifo`** (even though the C++ struct member defaults to `BucketEvictionPolicy::NONE`). Supported values are `fifo`, `lru`, and `none`.

- **Per-disk logical quota**: Each configured disk has its own `DiskState::max_total_size`, set from `MOONCAKE_OFFLOAD_BUCKET_MAX_TOTAL_SIZE_LIST` (one entry per disk) or falling back to scalar `MOONCAKE_OFFLOAD_BUCKET_MAX_TOTAL_SIZE`. When the effective policy is `fifo` or `lru` and a disk's configured quota is `<= 0`, `Init()` automatically sets that disk's quota to **90% of its filesystem capacity**. Setting `MOONCAKE_OFFLOAD_BUCKET_EVICTION_POLICY=none` disables write-time eviction and skips this 90% auto-quota.
- **Per-disk candidate selection**: Eviction runs at **whole-bucket** granularity against the target disk's index, skipping buckets whose master notification is still pending (`offload_notification_pending_`):

| Policy | Candidate selection (per disk) |
|--------|--------------------------------|
| `FIFO` (runtime default) | `disk.fifo_index.begin()` — oldest bucket ID on that disk |
| `LRU` | `disk.lru_index`, ordered by `{last_access_ns_, bucket_id}` on that disk |

`last_access_ns_` is an atomic `int64_t` initialized to `steady_clock::now()` when a new bucket is committed and updated on every `BatchLoad` with relaxed ordering. Buckets recovered from disk on startup start with `last_access_ns_ == 0`, so unread recovered buckets are evicted before newly written or recently read buckets under LRU.

- **Physical disk cap (`MOONCAKE_OFFLOAD_BUCKET_MAX_PHYSICAL_BYTES`)**: A disk's `total_size_` is a *logical* per-instance byte counter (incremented on bucket write, decremented by the bucket's logical size on whole-bucket eviction), so it ignores filesystem block rounding. `MOONCAKE_OFFLOAD_BUCKET_MAX_PHYSICAL_BYTES` (default `0` = disabled) adds a hard cap on real on-disk usage per disk directory, measured by `ActualDiskBytesUsedLocked()`:
  - Scans the disk's `storage_path` with `std::filesystem::recursive_directory_iterator` and sums `stat.st_blocks * 512` (matching `du` and Kubernetes `emptyDir` `sizeLimit`, including block rounding and excluding sparse holes). Because `BucketStorageBackend` enforces one live client per directory via `flock`, each rank uses its own directory (or disk) and the physical cap bounds that rank's footprint.
  - Cached for `MOONCAKE_OFFLOAD_BUCKET_DISK_SCAN_CACHE_MS` (default 500 ms; `<= 0` re-scans on every check), snapshotted once per `PrepareEviction` call under the metadata lock, and invalidated by `FinalizeEviction` after file deletion.
  - Fails closed on scan or open errors (reports the cap as reached without caching a partial total).
  - Independent of the logical quota; when both are set, a write must satisfy both and eviction continues until projected post-eviction usage is under both limits.

#### OffsetAllocatorStorageBackend

`OffsetAllocatorStorageBackend` supports opt-in write-time FIFO eviction when `MOONCAKE_OFFSET_EVICTION_POLICY=fifo` (default `none`, under which writes fail once capacity is full). When byte or key usage crosses `MOONCAKE_OFFSET_HIGH_RATIO` (default `0.90`), it evicts oldest records in FIFO order down toward `MOONCAKE_OFFSET_LOW_RATIO` (default `0.80`), bounded by `MOONCAKE_OFFSET_MAX_EVICT_PER_OFFLOAD` (default `4096`) and `MOONCAKE_OFFSET_MAX_CAPACITY_NODES`. Like `BucketStorageBackend`, it notifies the master via `BatchEvictDiskReplica` before freeing the evicted extents. This path is separate from proactive disk-watermark eviction, which is a no-op for `OffsetAllocatorStorageBackend`.

### Proactive disk-watermark eviction

`FileStorage::Heartbeat()` also calls the backend-level proactive disk watermark path when `MOONCAKE_OFFLOAD_ENABLE_DISK_WATERMARK_EVICTION=true`. The high watermark decides when eviction starts and the low watermark decides the target usage after eviction. This path is independent of write admission, so disk usage can move back toward the low watermark even when no new write arrives.

| Backend | Candidate source |
|---------|------------------|
| `BucketStorageBackend` | The configured per-disk bucket eviction policy (`FIFO` or `LRU`) |
| `StorageBackendAdaptor` | The FIFO file queue rebuilt by `StorageBackend::Init()`; `ScanMeta()` restores object keys for recovered files |
| `OffsetAllocatorStorageBackend` | No-op in this version (use write-time FIFO eviction above instead) |

The watermark path uses the same master-notification ordering as write-time eviction: selected keys are reported through the eviction handler before files are deleted. If master notification fails, `BucketStorageBackend` restores the prepared metadata and `StorageBackendAdaptor` restores the selected file records to the FIFO queue.

For `BucketStorageBackend`, write-time eviction and watermark eviction both use `PrepareEviction()` and `FinalizeEviction()`, so candidate selection is serialized by the same metadata lock. For `StorageBackendAdaptor`, write-time reactive eviction and watermark eviction use the same FIFO queue and space-accounting locks.

### Two-phase eviction protocol (BucketStorageBackend)

Eviction is split into two phases so the master is notified before files are deleted and no in-flight reads are interrupted.

**Phase 1 — `PrepareEviction(required_size, disk_index)`** (called under exclusive lock):

1. Repeatedly call `SelectEvictionCandidate()` on the target disk until projected post-eviction usage is within every configured cap: `disk.total_size + required_size <= disk.max_total_size` (logical), and — when `max_physical_bytes > 0` — `physical_used_start + required_size - accumulated_freed_space <= max_physical_bytes` (physical), where `physical_used_start` is the `ActualDiskBytesUsedLocked()` snapshot taken once at the start of the call. If candidates are exhausted before the caps are satisfied, the write is rejected with `FILE_WRITE_FAIL`.
2. For each selected whole bucket: remove it from `buckets_`, the disk's FIFO/LRU index, and `object_bucket_map_`, and subtract its logical size from `disk.total_size` (and `total_size_`).
3. Collect all evicted keys and bucket metadata into a `PendingEviction` struct and return it — no file I/O at this point.

**Between phases** — notify master:

The caller invokes `eviction_handler` with the full list of evicted keys, which sends one batched `MasterClient::BatchEvictDiskReplica` RPC. If master notification fails, the prepared bucket metadata is restored.

**Phase 2 — `FinalizeEviction(pending)`** (called after master notification):

For each evicted bucket:
1. Attempt to delete the `.meta` file first, reducing the chance that a later restart recovers a bucket whose master replica has already been removed.
2. Spin-wait (with a 10-second timeout) until `inflight_reads_ == 0`.
3. Evict any stale file-handle cache entries.
4. Delete the `.bucket` file.

This ordering ensures that (when file operations succeed) the master does not serve a stale disk-replica location for a deleted file, ongoing reads finish before removal, and freed space is available before `WriteBucket`. Cleanup failures after the master update are logged and treated as best-effort; a crash or filesystem failure that leaves both files intact still relies on cross-node reconciliation.

---

## io_uring File I/O

When `MOONCAKE_OFFLOAD_USE_URING=true` (on a build with `USE_URING`), supported storage backends use `UringFile` instead of POSIX `pread`/`pwrite`. The design eliminates inter-thread lock contention, which was the dominant latency source in the previous global-ring approach.

### Thread-local rings (`SharedUringRing`)

Each thread owns one `io_uring` ring in `thread_local` storage, initialized lazily on first use and destroyed when the thread exits:

- **No mutex between threads**: Each ring is accessed only by its owning thread, so concurrent I/O across threads runs in parallel with zero lock contention.
- **Within-thread batching**: Multiple SQEs can be enqueued before calling `io_uring_submit_and_wait`, exposing NVMe queue depth > 1 within a single thread (`batch_read` submits up to `QUEUE_DEPTH` = 32 independent reads per submission; `read`/`write`, `read_aligned`/`write_aligned`, `vector_read`/`vector_write`, and `datasync` delegate through the same ring).
- **No fixed-file registration**: Per-I/O `fdget()` overhead (~50 ns) is negligible compared to global-ring lock contention, so `IOSQE_FIXED_FILE` is not used.
- **No runtime POSIX fallback**: If a thread's `io_uring` I/O ring fails to initialize (for example, on an unsupported kernel or restricted seccomp environment), `UringFile` operations on that thread return `ErrorCode::INTERNAL_ERROR`. To switch back to `PosixFile`, explicitly set `MOONCAKE_OFFLOAD_USE_URING=false` (so it does not fall back to the legacy `MOONCAKE_USE_URING` alias) and restart the client.

### Fixed-buffer registration

The `ClientBuffer` staging area can be registered with `io_uring` as a **fixed buffer** so reads targeting that region use `io_uring_prep_read_fixed` instead of `io_uring_prep_read`, avoiding per-I/O page pinning overhead:

- **Register once, clone lazily per thread**: `UringFile::register_global_buffer()` applies `MADV_NOHUGEPAGE` (preventing `FOLL_LONGTERM` pinning failures under Transparent Huge Pages), chunks the region into slices of at most 1 GiB, and registers it **once** on a dedicated source ring via `io_uring_register_buffers`. Each I/O thread lazily clones the buffer table on first use via `io_uring_clone_buffers` (requiring liburing 2.9+ and Linux 6.13+).
- **Fallback to unfixed `io_uring` I/O**: If source registration or `io_uring_clone_buffers` is unsupported or fails, `UringFile` still performs normal unfixed `io_uring` I/O as long as the thread's I/O ring initialized successfully.
- **Synchronous unregistration**: Unregistering the global buffer increments a generation counter and coordinates through `RingRegistry` to synchronously revoke cloned buffer tables before the memory is released.

### O_DIRECT and alignment

`UringFile` supports an optional per-file `O_DIRECT` mode. When `use_direct_io_` is enabled on a file handle:

- The file descriptor is opened with `O_DIRECT`, and buffers, lengths, and offsets must be aligned to 4 KiB (`ALIGNMENT_ = 4096`).
- `read_aligned` and `write_aligned` validate alignment and return `ErrorCode::FILE_INVALID_BUFFER` on mismatch.
- For unaligned writes (such as serialized metadata), `write` allocates a temporary aligned bounce buffer via `posix_memalign`, copies the data, writes it, and frees the buffer.

### Integration with storage backends

- **BucketStorageBackend**: When `use_uring_` is enabled (and `USE_URING` is compiled in), uses `UringFile` with `O_DIRECT` for **reads** (`.bucket` and `.meta`) and for `packed` `.bucket` writes (`MOONCAKE_OFFLOAD_BUCKET_DIRECT_WRITE=packed`). `.meta` writes and default/`aligned` `.bucket` writes use `PosixFile` (`aligned` enables `O_DIRECT` via `fcntl` when supported). A file-handle cache (`file_cache_`) avoids repeated `open`/`close` for hot buckets, and entries are evicted before deletion.
- **OffsetAllocatorStorageBackend**: When built with `USE_URING` and `MOONCAKE_OFFLOAD_USE_URING=true`, opens `kv_cache.data` with `O_DIRECT` and `UringFile`; otherwise uses buffered `PosixFile`. Global fixed-buffer registration is driven by `FileStorage` via `UringFile::register_global_buffer()`.
- **StorageBackendAdaptor (FilePerKey)**: When `use_uring_` is enabled, uses `UringFile` for **both** reads (with `O_DIRECT`) and writes (buffered, without `O_DIRECT`).

---

## Metadata Recovery on Restart

On startup, `FileStorage::Init()` calls `StorageBackend::ScanMeta()`, which reads recoverable on-disk metadata and invokes a callback (`MasterClient::NotifyOffloadSuccess`) to re-register existing objects with the master without application intervention. Recovery behavior depends on the backend:

| Backend | Restart recovery behavior |
|---------|---------------------------|
| `BucketStorageBackend` | Recovers `.meta` files located directly in each configured `storage_path` and removes orphaned `.bucket` files directly in `storage_path` that have no matching metadata. Subdirectories are neither descended nor cleaned (configure a client's own directory as `storage_path` to recover its buckets). Scanned buckets are pinned against eviction while re-registration batches are in flight. |
| `StorageBackendAdaptor` (FilePerKey) | Scans existing object files and re-registers their keys with the master, rebuilding the in-memory FIFO queue. |
| `OffsetAllocatorStorageBackend` | Controlled by `MOONCAKE_OFFSET_PERSIST_MODE` (default `disabled`):<br>• **`disabled`**: truncates `kv_cache.data` and clears metadata on startup; previously offloaded objects are not recoverable.<br>• **`relaxed`** (periodic write-triggered checkpoint after `MOONCAKE_OFFSET_PERSIST_INTERVAL_SECONDS` plus best-effort shutdown checkpoint) and **`strict`** (durable checkpoint after each completed `BatchOffload`; interrupted batches are not atomic): `Init()` calls `TryRecoverFromMetadata()`. `kRecovered` preserves data and index and re-registers keys via `ScanMeta`; `kTransientError` fails `Init()` to protect persisted data instead of wiping it; only `kNoMeta` or `kCorrupt` (or resizing `MOONCAKE_OFFLOAD_TOTAL_SIZE_LIMIT_BYTES`) falls back to truncating and starting fresh. Keep `MOONCAKE_OFFSET_RECORD_CRC=true` (default) for torn-write verification; when combined with FIFO eviction, neither `relaxed` nor `strict` guarantees a point-in-time snapshot across a crash before the next checkpoint. |
| `NvmeKvStorageBackend` | `ScanMeta()` is a no-op; previously written objects are not re-registered across restarts. |
| `DistributedStorageBackend` | `oss`/`s3` run under `LOCAL_DISK` offload (see [Object Storage Offload](../../deployment/object-storage-offload.md)); `posix`/`hf3fs`/`kvcs-lowlevel` run in `DFS` mode and skip `FileStorage::Init()`'s `LOCAL_DISK` recovery path. |
