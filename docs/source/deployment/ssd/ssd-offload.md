# SSD Offload

## Overview

Mooncake Store supports offloading KV cache objects from distributed memory to a local filesystem path, typically backed by local SSDs. With `--enable_offload=true`, for objects with a DRAM replica on an offload-enabled client, the master eagerly queues asynchronous disk persistence at `PutEnd` by default, or defers persistence until memory eviction when `--offload_on_evict=true`. On a memory cache miss, the client automatically falls back to reading from the local filesystem-backed offload path.

For measured TTFT and throughput impact in multi-turn workloads, see [Mooncake SSD Offload Benchmark](../../performance/mooncake/ssd-offload-benchmark-results.md).

SSD offload requires the **Real Client** and supports two deployment modes:

- **Mode A: Embedded Real Client** — the Python process embeds the Real Client, and SSD offload runs inside the Python process.
- **Mode B: Standalone Real Client + DummyClient** — a standalone `mooncake_client` process runs SSD offload, and the Python process connects via a DummyClient.

In both modes, all SSD reads and writes happen within the Real Client (embedded or standalone).

SSD offload does not use the master's `--root_fs_dir` option. Configure the local disk path on each Real Client with `MOONCAKE_OFFLOAD_FILE_STORAGE_PATH`; the master tracks offloaded objects as `LOCAL_DISK` replicas. `--root_fs_dir` is a legacy parameter from an older persistence path and may cause issues when used with `--enable_offload=true`.

## Startup Steps

### Step 1: Create the SSD storage directory

```bash
mkdir -p /nvme/mooncake_offload
```

### Step 2: Start the master

```bash
mooncake_master \
    --rpc_port=50051 \
    --enable_offload=true
```

### Step 3A (Mode A): Start the application with embedded Real Client

Pass `enable_ssd_offload=True` and `ssd_offload_path` to `MooncakeDistributedStore.setup(...)` (replace `<machine-ip>` and `<rdma-device>` with your node's reachable IP and RDMA device, e.g. `mlx5_0`), and optionally set environment variables to configure the storage backend:

```python
from mooncake.store import MooncakeDistributedStore

store = MooncakeDistributedStore()
store.setup(
    local_hostname="<machine-ip>",
    metadata_server="P2PHANDSHAKE",
    global_segment_size=4 * 1024 * 1024 * 1024,  # 4 GB
    local_buffer_size=512 * 1024 * 1024,  # 512 MB
    protocol="rdma",
    rdma_devices="<rdma-device>",
    master_server_addr="127.0.0.1:50051",
    enable_ssd_offload=True,
    ssd_offload_path="/nvme/mooncake_offload"
)
```

### Step 3B (Mode B): Start the standalone real client with SSD offload enabled and connect it with the dummy client

Set the SSD offload environment variables and start `mooncake_client` with `--enable_offload=true` (replace `<machine-ip>` and `<rdma-device>` with your node's reachable IP and RDMA device, e.g. `mlx5_0`):

```bash
export MOONCAKE_OFFLOAD_FILE_STORAGE_PATH=/nvme/mooncake_offload
export MOONCAKE_OFFLOAD_STORAGE_BACKEND_DESCRIPTOR=bucket_storage_backend

mooncake_client \
    --master_server_address="127.0.0.1:50051" \
    --host="<machine-ip>" \
    --protocol="rdma" \
    --device_names="<rdma-device>" \
    --port=50052 \
    --global_segment_size="4 GB" \
    --enable_offload=true \
    --metadata_server="P2PHANDSHAKE"
```

The application (e.g., SGLang) connects to the standalone real client on the **same host** (and in the same network namespace, since non-zero `local_buffer_size` exchanges shared-memory file descriptors over the local abstract Unix socket) via the `MooncakeDistributedStore` Python SDK (replace `<machine-ip>` with the host address of your local `mooncake_client`). The Python process acts as a DummyClient; SSD offload and fallback loading are handled by the standalone real client process.

```python
from mooncake.store import MooncakeDistributedStore

store = MooncakeDistributedStore()
store.setup_dummy(
    mem_pool_size=4 * 1024 * 1024 * 1024,  # 4 GB
    local_buffer_size=512 * 1024 * 1024,  # 512 MB
    server_address="<machine-ip>:50052"   # mooncake_client RPC address from Step 3B
)
```

> **Note:** When using `bucket_storage_backend` or `file_per_key_storage_backend`, the real client scans existing SSD metadata on startup and reports it to the master automatically. `offset_allocator_storage_backend` truncates its data file on startup by default (`MOONCAKE_OFFSET_PERSIST_MODE=disabled`), and recovers persisted metadata across restarts when `MOONCAKE_OFFSET_PERSIST_MODE` is set to `relaxed` or `strict`.

---

## Real Client Parameters

| Flag | Default | Description |
|------|---------|-------------|
| `--metadata_server` | `http://127.0.0.1:8080/metadata` | Metadata server connection string |
| `--master_server_address` | `127.0.0.1:50051` | Master address |
| `--host` | `0.0.0.0` | This machine's externally reachable IP. Accepts `ip:port` to specify the data plane port for TransferEngine |
| `--port` | `50052` | Real client RPC listening port |
| `--device_names` | ` ` | NIC name(s), e.g. `eth0` or `mlx5_0` |
| `--protocol` | `tcp` | Transport protocol: `tcp` or `rdma` |
| `--global_segment_size` | `4 GB` | Memory pool size allocated for this node |
| `--enable_offload` | `false` | **Must be set to `true` to enable SSD offload** |
| `--start_offload_rpc_server` | `true` | Start the dedicated offload TCP RPC server on an OS-assigned ephemeral port (`batch_get_offload_object`, `batch_get_offload_object_ranges`, `release_offload_buffer`, `verify_disk_replica`, `prefetch_offload_object`). Effective only when `--enable_offload=true`; peers must be able to reach this port over TCP (leave `MC_RPC_PROTOCOL` unset or `tcp` on clients issuing SSD restore RPCs, even when `--protocol=rdma` is used for data transfers, because neither offload listener enables RDMA RPC). Keep this enabled for deployments serving DummyClient SSD reads: setting `false` routes disk reads, buffer release, and delegated prefetch onto the fixed `--port` main client RPC listener (without `verify_disk_replica` for read-side stale-replica healing), where the default `--threads=1` can stall or deadlock on both local loopback and concurrent cross-node DummyClient SSD reads |
| `--enable_ssd_prefetch` | `false` | Enable best-effort SSD-to-DRAM prefetch on exist probes (callers must also pass `ExistOptions.prefetch_to_memory=true`, default `false`), demand-side get kicks (only `get_buffer`/`get`/`get_tensor` and `batch_get_into_multi_buffers`, not `batch_get_buffer`/`get_batch`, `get_into`, or `batch_get_into`), and remote delegations. For remote SSD replicas, both the requesting Real Client and the SSD holder must enable prefetch (see [SSD Prefetch](../../design/store/ssd-prefetch.md)) |
| `--ssd_prefetch_cooldown_sec` | `5` | Memory-pressure backoff window in seconds for SSD prefetch (`0` disables). Demand-side get kicks bypass the requester's cooldown, though a remote holder still honors its own cooldown |
| `--ssd_prefetch_dedup_ttl_sec` | `30` | Per-key prefetch trigger deduplication TTL in seconds (`0` disables) |
| `--threads` | `1` | Number of RPC server threads |

---

## Master Offload Parameters

These flags control when the master asks clients to persist objects to SSD, whether SSD-only objects can be promoted back to DRAM, and how much offload work can be queued during eviction.

| Flag | Default | Description |
|------|---------|-------------|
| `--enable_offload` | `false` | Enables the master-side SSD offload control plane. Set this on the master and on every real client that owns SSD storage |
| `--offload_on_evict` | `false` | Defer SSD persistence until memory eviction selects an object. When `false`, successful `Put` completion queues eager SSD persistence |
| `--offload_force_evict` | `false` | If offload-on-evict exceeds the per-cycle offload cap, force-evict excess objects instead of leaving them in DRAM |
| `--offloading_queue_limit` | `50000` | Maximum pending offload objects per local disk segment. Must be greater than `0` and at most `100000000` |
| `--offload_cap_ratio` | `0.5` | Per-eviction-cycle cap as a fraction of `offloading_queue_limit`; range `[0.0, 1.0]`. Default cap is `25000` objects per cycle |
| `--promotion_on_hit` | `false` | Promote SSD-resident objects back to DRAM after read hits |
| `--promotion_admission_threshold` | `2` | Minimum CountMinSketch count before promotion is admitted. Set `1` to disable second-touch gating |
| `--promotion_max_per_heartbeat` | `1` | Maximum promotion tasks returned to one client per heartbeat. Keep conservative for large objects because each task does SSD read plus memory write |
| `--promotion_queue_limit` | `50000` | Maximum in-flight promotion tasks tracked by the master |

Start with `--enable_offload=true` for eager SSD persistence. Add `--offload_on_evict=true` when SSD writes should happen only under memory pressure. For SSD-heavy workloads where offload-on-evict is dropping too many objects, raise both `--offloading_queue_limit` and `--offload_cap_ratio`; for example, `--offloading_queue_limit=500000 --offload_cap_ratio=0.8` allows up to `400000` objects to be queued in one eviction cycle.

> **Warning:** Do not combine `--offload_on_evict=true` with OpLog (`--enable_oplog=true`) if you rely on automatic `LOCAL_DISK` offload. `--offload_on_evict=true` disables `PutEnd` enqueueing, while OpLog takes precedence in memory eviction (`OpLogReclaimer` instead of `OffloadingReclaimer`), so neither normal offload enqueue path runs.

---

## SSD Offload Configuration

### Core settings

| Environment Variable | Default | Description |
|---|---|---|
| `MOONCAKE_OFFLOAD_FILE_STORAGE_PATH` | `/data/file_storage` | Absolute path to the SSD storage directory. `bucket_storage_backend` also accepts a comma-separated list of paths to spread offload across several disks — see {ref}`Multiple disks <ssd-offload-multiple-disks>` |
| `MOONCAKE_OFFLOAD_STORAGE_BACKEND_DESCRIPTOR` | `bucket_storage_backend` | Storage backend type: `bucket_storage_backend`, `file_per_key_storage_backend`, `offset_allocator_storage_backend`, `nvme_kv_storage_backend` (see [NVMe KV Local-Disk Backend](./nvme-kv.md)), or `distributed_storage_backend` (see [Object Storage Offload](../object-storage-offload.md)) |
| `MOONCAKE_OFFLOAD_LOCAL_BUFFER_SIZE_BYTES` | `1342177280` (1.25 GB) | Client-side staging buffer size |
| `MC_STORE_PINNED_RESTORE_ARENA_SIZE_BYTES` | `0` | Size of the additional preallocated pinned-host arena for same-process SSD-to-GPU restores and DFS ranged-session reads into device memory. See the constraints below |
| `MOONCAKE_OFFLOAD_SCANMETA_ITERATOR_KEYS_LIMIT` | `20000` | Max keys processed per iteration when scanning existing SSD metadata on startup |
| `MOONCAKE_OFFLOAD_TOTAL_SIZE_LIMIT_BYTES` | `2199023255552` (2 TB) | Maximum disk usage |
| `MOONCAKE_OFFLOAD_TOTAL_KEYS_LIMIT` | `10000000` | Maximum number of objects on disk |
| `MOONCAKE_OFFLOAD_HEARTBEAT_INTERVAL_SECONDS` | `10` | Interval for offload heartbeat to master (seconds) |
| `MOONCAKE_OFFLOAD_CLIENT_BUFFER_GC_INTERVAL_SECONDS` | `1` | Interval in seconds for reclaiming expired offload buffers |
| `MOONCAKE_OFFLOAD_CLIENT_BUFFER_GC_TTL_MS` | `5000` | Lease time for buffers returned by `batch_get_offload_object` before GC reclaims them |
| `MOONCAKE_OFFLOAD_USE_URING` | `false` | Enable io_uring for async file I/O |
| `MOONCAKE_OFFLOAD_ENABLE_DISK_WATERMARK_EVICTION` | `true` | Enable proactive local-disk eviction from the FileStorage heartbeat |
| `MOONCAKE_OFFLOAD_DISK_EVICTION_HIGH_WATERMARK_RATIO` | `0.90` | Trigger proactive disk eviction when backend usage exceeds this ratio of its quota |
| `MOONCAKE_OFFLOAD_DISK_EVICTION_LOW_WATERMARK_RATIO` | `0.80` | Target backend usage ratio for proactive disk eviction |

The `MOONCAKE_OFFLOAD_*` watermark names are preferred. Short aliases `MOONCAKE_DISK_EVICTION_HIGH_WATERMARK_RATIO` and `MOONCAKE_DISK_EVICTION_LOW_WATERMARK_RATIO` are also accepted. The high watermark must be greater than the low watermark.

The pinned restore quota is separate from `MOONCAKE_OFFLOAD_LOCAL_BUFFER_SIZE_BYTES`; it does not convert the normal FileStorage arena to pinned memory. It is allocated only when `MC_STORE_MEMCPY=1`. The arena is selected when the current process owns the SSD replica and the restore destination is GPU memory, and is also used as request-scoped staging for DFS ranged-session reads into device memory. Tensor payload ranges are copied from their source offset without an additional FileStorage staging allocation. Remote and CPU-destination reads continue to use the existing path. If the pinned quota is exhausted, the request uses the normal client buffer arena; if the quota cannot be pinned at startup, the optimization remains disabled. io_uring disables the pinned restore arena. The file-per-key backend may still use its own temporary pageable buffer internally; the bucket and offset-allocator backends can read into the supplied restore buffer directly.

### Bucket backend settings

Applies when `MOONCAKE_OFFLOAD_STORAGE_BACKEND_DESCRIPTOR=bucket_storage_backend`.

| Environment Variable | Default | Description |
|---|---|---|
| `MOONCAKE_OFFLOAD_BUCKET_SIZE_LIMIT_BYTES` | `268435456` (256 MB) | Max size per bucket |
| `MOONCAKE_OFFLOAD_BUCKET_KEYS_LIMIT` | `500` | Max keys per bucket |
| `MOONCAKE_OFFLOAD_FLUSH_PARTIAL_BUCKETS` | `false` | Flush trailing partial buckets in the same offload cycle instead of retaining them across heartbeats |
| `MOONCAKE_OFFLOAD_BUCKET_DIRECT_WRITE` | `0` | Write mode: `0`/`false` keeps buffered writes; `packed` enables packed O_DIRECT/io_uring writes (`true`/`1` are aliases). Packed writes require a build with io_uring support and `MOONCAKE_OFFLOAD_USE_URING=true`; otherwise writes remain buffered. `aligned` enables aligned O_DIRECT writes, which do not need io_uring (see below). Unknown values fail backend initialization. `.meta` writes stay buffered with their exact protobuf length. |
| `MOONCAKE_OFFLOAD_BUCKET_COPY_THREADS` | `1` | Threads used only by `packed` direct writes, clamped to 1–8; invalid values use 1. Buckets smaller than 16 MiB always use one thread. |
| `MOONCAKE_OFFLOAD_BUCKET_MAX_TOTAL_SIZE` | `0` | Eviction threshold in bytes, applied **per disk** when `MOONCAKE_OFFLOAD_BUCKET_MAX_TOTAL_SIZE_LIST` is empty. When set to `0` and an eviction policy (`fifo` or `lru`) is active, each disk defaults to **90% of its physical disk capacity**; with `MOONCAKE_OFFLOAD_BUCKET_EVICTION_POLICY=none`, no 90% default quota is synthesized. |
| `MOONCAKE_OFFLOAD_BUCKET_MAX_TOTAL_SIZE_LIST` | empty | Per-disk eviction thresholds as a comma-separated list, positionally aligned with the paths in `MOONCAKE_OFFLOAD_FILE_STORAGE_PATH`; when set, it needs exactly one positive entry per path. Empty applies `MOONCAKE_OFFLOAD_BUCKET_MAX_TOTAL_SIZE` to every disk. See {ref}`Multiple disks <ssd-offload-multiple-disks>` |
| `MOONCAKE_OFFLOAD_BUCKET_EVICTION_POLICY` | `fifo` | Eviction policy: `fifo` (default) / `lru` / `none` |
| `MOONCAKE_OFFLOAD_BUCKET_MAX_PHYSICAL_BYTES` | `0` (disabled) | Best-effort eviction threshold on **real on-disk** bytes (`du`-equivalent) under each configured offload root (`0` disables). Leave headroom below hard volume limits such as Kubernetes `emptyDir.sizeLimit` — see [Eviction](#eviction) below. |
| `MOONCAKE_OFFLOAD_BUCKET_DISK_SCAN_CACHE_MS` | `500` | How long the directory-scan result is cached before re-scanning, to bound the cost of the physical-usage check. `<= 0` scans on every check. |
| `MOONCAKE_OFFLOAD_RECLAIM_INTERVAL_SECONDS` | `10` | Interval between passes that reclaim buckets of objects the master removed; `0` disables reclaim. See {ref}`Reclaim of removed objects <ssd-offload-reclaim>` |
| `MOONCAKE_OFFLOAD_RECLAIM_CYCLE_SECONDS` | `120` | Target time for the passes to check every stored key against the master once. A lap takes longer when passes hit their time limit. |

For `packed` direct bucket writes, each writing thread retains a 4096-byte-aligned buffer
that grows to fit its largest bucket and is released when the thread exits.
Only the end of the data file is zero-padded to a 4096-byte boundary; key order,
metadata offsets, and logical lengths keep the existing format. Data writes and
`datasync` finish before metadata is written and the local index is committed.

To enable direct writes with parallel packing, set these variables before
starting the offload owner:

```bash
export MOONCAKE_OFFLOAD_USE_URING=true
export MOONCAKE_OFFLOAD_BUCKET_DIRECT_WRITE=packed
export MOONCAKE_OFFLOAD_BUCKET_COPY_THREADS=8
```

Unset `MOONCAKE_OFFLOAD_BUCKET_DIRECT_WRITE` or set it to `0`/`false` and restart the
owner to return to buffered writes. Set `MOONCAKE_OFFLOAD_BUCKET_COPY_THREADS=1`
to retain direct writes with serial packing. Previously written buckets remain
readable with either configuration.

Parallel packing competes for CPU and memory bandwidth and creates up to seven
helper threads per bucket. All copies finish before I/O submission. If a helper
thread cannot be created, existing copies finish before the whole bucket is
copied serially. The retained buffer increases resident memory per writer.
Filesystems that do not support O_DIRECT have no automatic buffered fallback;
disable the direct-write option on those filesystems.

`aligned` writes bucket data files with `O_DIRECT` instead of through the page
cache, each value at a 4 KiB-aligned file offset. Values in 4 KiB-aligned
buffers are written from those buffers; other values are copied into a
per-thread buffer that can grow to the bucket size. Each object takes up to
about 8 KiB more disk space (measured: 3% with 128 KiB values; about 12% with
64 KiB + 100 B values by the layout), and the bucket size limit and the total
size of written buckets count only keys and values, so it suits large values.
Offloaded data is not left in the page cache. Where the filesystem refuses
`O_DIRECT`, the same aligned writes go through the page cache. Buckets written
this way remain readable with the option off.

(ssd-offload-multiple-disks)=

### Multiple disks

`MOONCAKE_OFFLOAD_FILE_STORAGE_PATH` accepts a comma-separated list of storage
roots, so one real client can offload across every SSD on the machine instead of
being capped by the capacity and bandwidth of a single drive. Empty entries,
such as `/nvme0/a,,/nvme1/a` or a trailing comma, are rejected at startup
rather than skipped, so the client never runs on fewer disks than the list has
entries:

```bash
export MOONCAKE_OFFLOAD_STORAGE_BACKEND_DESCRIPTOR=bucket_storage_backend
export MOONCAKE_OFFLOAD_FILE_STORAGE_PATH=/nvme0/mooncake_offload,/nvme1/mooncake_offload
export MOONCAKE_OFFLOAD_BUCKET_EVICTION_POLICY=lru
# Optional: 200 GB on the first disk, 400 GB on the second
export MOONCAKE_OFFLOAD_BUCKET_MAX_TOTAL_SIZE_LIST=$((200 * 1024 * 1024 * 1024)),$((400 * 1024 * 1024 * 1024))
```

- **Bucket backend only.** `file_per_key_storage_backend` and
  `offset_allocator_storage_backend` use the configured path as a single
  directory and do not understand a comma list. Configure exactly one path for
  those backends.
- **Each root is validated independently.** Every entry must be an absolute path
  to an existing, writable directory; whitespace around entries is trimmed, and
  empty entries are rejected at startup. Roots must not nest inside one another,
  or startup fails — separate disks are the point, and overlapping roots would
  double-count usage.
- **Exclusive storage ownership.** For filesystem-backed `LOCAL_DISK` backends,
  always give each live Real Client its own offload directory (and its own
  directory on every disk when multiple disks are configured) — do not point
  multiple TP ranks at the same path. `bucket_storage_backend` enforces this at
  runtime with an exclusive `flock` on every configured root, failing `Init()`
  with `storage_path already held by another live client` if any directory is
  already held; other filesystem-backed backends do not acquire this lock, so
  directory isolation must still be maintained by configuration. Independent
  `nvme_kv_storage_backend` clients must use separate NVMe KV namespaces rather
  than relying on separate directories. See the ownership contract in
  [SSD Offload design](../../design/store/ssd-offload.md).
- **Writes are spread round-robin** across the disks that can admit the incoming
  bucket, so all drives stay in use. Selection is not free-space-greedy: that
  collapses onto a single disk once the drives equalize.
- **Adding disks to a node that already has data is a gradual rebalance, not an
  instant one.** Round-robin only steers *new* writes; it does not move
  existing buckets. The original disk keeps serving its existing buckets and
  keeps evicting under pressure while the new disks fill from empty, and under
  LRU a bucket that stays hot never migrates off the original disk. Usage
  evens out naturally as the original disk's older buckets age out and get
  replaced by round-robin writes, but this can take a while — treat the
  imbalance as expected during that window rather than a misconfiguration.
- **Quotas, eviction and the physical cap are per disk.** Each disk keeps its own
  usage counter, its own FIFO/LRU order and its own
  `MOONCAKE_OFFLOAD_BUCKET_MAX_PHYSICAL_BYTES` accounting, so a busy disk evicts
  without disturbing the others.
- **Per-disk quotas** come from `MOONCAKE_OFFLOAD_BUCKET_MAX_TOTAL_SIZE_LIST`
  when set. Entries are matched to paths by position, so the list must have
  exactly one positive entry per path. Because a wrong guess would silently put
  a disk on its neighbour's quota, the backend refuses to start on an empty or
  unparsable entry (such as `100,,300` or a trailing comma) and on a list whose
  length differs from the number of paths, logging the reason. To give every
  disk the same quota, use `MOONCAKE_OFFLOAD_BUCKET_MAX_TOTAL_SIZE` instead.
  When neither is set and an eviction policy is enabled, each disk defaults to
  90% of *its own* physical capacity.
- **On restart**, each root is scanned and every bucket is re-bound to the disk
  its metadata file was found on. The disk assignment is not persisted, so
  existing offloaded data stays readable with no migration step — but reordering
  or removing entries in the path list changes nothing about where existing
  buckets live, while removing a disk from the list makes its buckets
  unreachable.

(ssd-offload-file-per-key-settings)=

### File-per-key backend settings

Applies when `MOONCAKE_OFFLOAD_STORAGE_BACKEND_DESCRIPTOR=file_per_key_storage_backend`.

| Environment Variable | Default | Description |
|---|---|---|
| `MOONCAKE_OFFLOAD_FSDIR` | `file_per_key_dir` | Subdirectory name created under `MOONCAKE_OFFLOAD_FILE_STORAGE_PATH` |
| `MOONCAKE_OFFLOAD_ENABLE_EVICTION` | `true` | Enables local-storage eviction logic for this backend. Short alias: `ENABLE_EVICTION` |

---

## Storage Backends

### `bucket_storage_backend` (recommended)

Groups multiple objects into bucket files. Reduces filesystem overhead, supports efficient batch I/O, and supports FIFO and LRU eviction.

**File layout:**
```
/nvme/mooncake_offload/
├── 7004160000001.bucket   # data file (multiple KV pairs, integer bucket ID)
├── 7004160000001.meta     # metadata file
├── 7004160000002.bucket
└── ...
```

Best for: general-purpose use, large-scale deployments.

### `file_per_key_storage_backend`

Stores each object in an individual file. Simple and easy to inspect, but generates many small files at scale. See {ref}`File-per-key backend settings <ssd-offload-file-per-key-settings>` above for `MOONCAKE_OFFLOAD_FSDIR` and `MOONCAKE_OFFLOAD_ENABLE_EVICTION`.

Best for: debugging or small-scale deployments.

### `offset_allocator_storage_backend`

Pre-allocates a single large file and manages offset-based allocation within it. Highest concurrency via 1024-shard metadata.

- **Restart persistence:** Controlled by `MOONCAKE_OFFSET_PERSIST_MODE` (default `disabled`). In `disabled` mode, initialization truncates the data file and clears metadata, so previously offloaded objects are not recoverable after a restart. Set `relaxed` (checkpoints written during subsequent `BatchOffload` calls once `MOONCAKE_OFFSET_PERSIST_INTERVAL_SECONDS` has elapsed, plus a best-effort checkpoint on clean shutdown) or `strict` (durable checkpoint after each completed `BatchOffload`, though an interrupted batch is not atomic) to recover metadata and re-register objects on restart. Neither `relaxed` nor `strict` guarantees a point-in-time snapshot across a crash before the next checkpoint saves, because extents freed by FIFO eviction or key overwrites/upserts can be reused and overwritten by newer writes. Keep `MOONCAKE_OFFLOAD_TOTAL_SIZE_LIMIT_BYTES` unchanged across restarts: resizing it causes capacity-mismatch fallback, which deletes the checkpoint and truncates the data file (rebuilding the cache from empty).
- **Capacity and write-time eviction:** `MOONCAKE_OFFLOAD_TOTAL_SIZE_LIMIT_BYTES` sets the pre-allocated file size directly (100%, no safety margin), so set it below physical disk capacity. By default (`MOONCAKE_OFFSET_EVICTION_POLICY=none`), writes fail once capacity is full; set `MOONCAKE_OFFSET_EVICTION_POLICY=fifo` to enable write-time FIFO eviction.

| Environment Variable | Default | Description |
|---|---|---|
| `MOONCAKE_OFFSET_PERSIST_MODE` | `disabled` | Metadata persistence mode: `disabled`, `relaxed` (write-triggered checkpoint after interval + best-effort shutdown checkpoint), or `strict` (durable checkpoint after each completed `BatchOffload`; interrupted batches are not atomic) |
| `MOONCAKE_OFFSET_PERSIST_INTERVAL_SECONDS` | `60` | Minimum interval in seconds before a subsequent `BatchOffload` triggers a checkpoint in `relaxed` mode (must be `>= 5`; not a background timer, so idle writes before a crash may remain uncheckpointed) |
| `MOONCAKE_OFFSET_RECORD_CRC` | `true` | Attach CRC-32C checksums to newly written records (`0`/`false`/`off` disables for new writes; recovery checks each record's header flag, so existing checksummed records are still verified). Keep enabled for restart recovery unless storage guarantees prevent torn writes, because unchecksummed records skip payload verification and rely only on checkpoint sequence guards |
| `MOONCAKE_OFFSET_EVICTION_POLICY` | `none` | Write-time eviction policy: `none` or `fifo` |
| `MOONCAKE_OFFSET_HIGH_RATIO` | `0.90` | High usage ratio in `(0, 1]` that triggers write-time eviction (used for both byte and key watermarks) |
| `MOONCAKE_OFFSET_LOW_RATIO` | `0.80` | Low target usage ratio in `(0, high_ratio)` for write-time eviction (used for both byte and key watermarks) |
| `MOONCAKE_OFFSET_MAX_CAPACITY_NODES` | `0` (auto) | Override for allocator node capacity when initializing a fresh allocator (`0` auto-derives from pre-allocated file size; ignored on a successful `relaxed`/`strict` recovery, which restores the saved node limit) |
| `MOONCAKE_OFFSET_MAX_EVICT_PER_OFFLOAD` | `4096` | Maximum keys evicted in a single eviction pass (`> 0`); `BatchOffload` processes objects individually, so multiple objects in one batch can each trigger an eviction pass |

Best for: high-concurrency scenarios with many small objects.

### Other offload descriptors

- `nvme_kv_storage_backend`: writes directly to an NVMe KV device; see [NVMe KV Local-Disk Backend](./nvme-kv.md).
- `distributed_storage_backend`: selects an adapter from `MOONCAKE_DFS_FS_ADAPTER` (if set) or `MOONCAKE_DISTRIBUTED_FS_TYPE` (default `hf3fs`). Only `oss` and `s3` (when built with the corresponding adapter support) route `LOCAL_DISK` offload to object storage; `posix`, `hf3fs` (requires a `USE_3FS` build; otherwise client startup fails), and `kvcs-lowlevel` run in `DFS` mode and skip `LOCAL_DISK` offload, while unsupported values fail startup. Note that setting the effective adapter to `kvcs-lowlevel` automatically forces `distributed_storage_backend` and overrides `MOONCAKE_OFFLOAD_STORAGE_BACKEND_DESCRIPTOR`, so unset both `MOONCAKE_DFS_FS_ADAPTER` and `MOONCAKE_DISTRIBUTED_FS_TYPE` (or clear `kvcs-lowlevel`) when switching to local SSD offload. See [Object Storage Offload](../object-storage-offload.md) for full adapter setup.

---

## Eviction

### Write-time eviction

For `bucket_storage_backend`, `MOONCAKE_OFFLOAD_BUCKET_EVICTION_POLICY` defaults to `fifo`. With `fifo` or `lru` active, whenever a write would exceed a disk's effective quota (`MOONCAKE_OFFLOAD_BUCKET_MAX_TOTAL_SIZE_LIST` entry if set, otherwise `MOONCAKE_OFFLOAD_BUCKET_MAX_TOTAL_SIZE`, defaulting to 90% of that disk's physical capacity when `<= 0`) or `MOONCAKE_OFFLOAD_BUCKET_MAX_PHYSICAL_BYTES`, the backend evicts buckets on that disk before writing the new bucket.

| Policy | Behavior |
|--------|----------|
| `fifo` | Evict the oldest bucket first (default) |
| `lru` | Evict the least recently read bucket first |
| `none` | No eviction; no 90% default quota is synthesized, and writes fail or offload stops when the disk is full, an explicitly configured positive per-disk quota (`MAX_TOTAL_SIZE` / `MAX_TOTAL_SIZE_LIST`) would be exceeded, or the aggregate `MOONCAKE_OFFLOAD_TOTAL_SIZE_LIMIT_BYTES` / `MOONCAKE_OFFLOAD_TOTAL_KEYS_LIMIT` limit (minus one bucket's headroom) is reached |

Eviction is two-phase: the bucket is removed from metadata and master is notified first, then in-flight reads are drained before files are deleted.

**`MAX_TOTAL_SIZE` vs `MAX_PHYSICAL_BYTES`**

`MAX_TOTAL_SIZE` bounds a *logical* in-memory counter (object payloads, keys, and bucket metadata). Because bucket eviction removes whole buckets at a time and the counter does not include filesystem block rounding or files awaiting deletion, it can undercount actual disk consumption. `MAX_PHYSICAL_BYTES` instead tracks *real* on-disk bytes (`du`-equivalent) under each configured path as a best-effort threshold (using cached directory scans plus estimated incoming bucket sizes), so prefer it when targeting a physical limit — most importantly a Kubernetes `emptyDir` with a `sizeLimit`, which the kubelet enforces by actual `du` usage.

Because `MAX_PHYSICAL_BYTES` is enforced independently on each configured offload root directory (see {ref}`Multiple disks <ssd-offload-multiple-disks>`), budget it **below** each root's share of the volume to leave headroom for cached scans, block rounding, and in-flight writes (for example, comfortably below `sizeLimit / N` when `N` roots — such as `N` ranks with one directory each — share one volume, or below disk capacity when each root has its own disk).

The two limits are independent and may be combined — eviction fires when either would be exceeded. `MAX_PHYSICAL_BYTES` only takes effect when an eviction policy is active (`fifo`/`lru`); with `none`, eviction never runs.

---

### Proactive watermark eviction

When `MOONCAKE_OFFLOAD_ENABLE_DISK_WATERMARK_EVICTION=true`, the FileStorage heartbeat asks the backend to check local-disk usage every `MOONCAKE_OFFLOAD_HEARTBEAT_INTERVAL_SECONDS` seconds. If usage exceeds `MOONCAKE_OFFLOAD_DISK_EVICTION_HIGH_WATERMARK_RATIO`, the backend evicts toward `MOONCAKE_OFFLOAD_DISK_EVICTION_LOW_WATERMARK_RATIO`.

| Backend | Behavior |
|---------|----------|
| `bucket_storage_backend` | Reuses `MOONCAKE_OFFLOAD_BUCKET_EVICTION_POLICY` (`fifo` by default); setting it to `none` makes watermark eviction a no-op |
| `file_per_key_storage_backend` | Requires `MOONCAKE_OFFLOAD_ENABLE_EVICTION=true`; reuses the FIFO file eviction queue and recovered keys from startup metadata scan |
| `offset_allocator_storage_backend` | No-op for background heartbeat watermark eviction; use write-time FIFO eviction (`MOONCAKE_OFFSET_EVICTION_POLICY=fifo`) instead |

The watermark ratios apply to each backend's quota. For `bucket_storage_backend`, each disk uses its matching entry from `MOONCAKE_OFFLOAD_BUCKET_MAX_TOTAL_SIZE_LIST` if set, otherwise `MOONCAKE_OFFLOAD_BUCKET_MAX_TOTAL_SIZE`; only when an eviction policy (`fifo` or `lru`) is active and that disk's configured quota is `<= 0` does it default to 90% of that disk's physical capacity. With several disks configured, each disk is checked against its own quota and evicted toward the low watermark independently. For `file_per_key_storage_backend`, the underlying file backend uses its storage quota, which defaults to 90% of the physical disk capacity in SSD offload mode.

For watermark eviction, the real client notifies the master before deleting local files. If the notification fails, the selected files remain tracked locally and are retried by a later heartbeat.

(ssd-offload-reclaim)=
### Reclaim of removed objects

Removing an object on the master (remove, remove by regex, remove all, lease or TTL expiry) drops only its metadata. With `bucket_storage_backend`, the real client reclaims the disk space itself:

- Every `MOONCAKE_OFFLOAD_RECLAIM_INTERVAL_SECONDS`, the heartbeat asks the master about the next share of stored keys, sized so that all keys are checked about once per `MOONCAKE_OFFLOAD_RECLAIM_CYCLE_SECONDS`. A pass stops after about 2 seconds and the next one continues where it stopped, so the heartbeat is never held up for long.
- On the next tick, a bucket whose keys the master no longer has is deleted. A bucket that still holds one live key keeps all its bytes until it is fully dead or evicted.
- When a removed key is put again and offloaded to the same real client, its stale copy is dropped and the new value is written.
- Nothing is reclaimed while the real client re-registers its objects with the master, for example after the master lost its segment.

If the offload directory already holds objects when the real client starts, reclaim stays off for the lifetime of that process, and the client logs `Reclaim of removed objects is disabled: the offload directory was not empty at start`. Space from objects removed in that time is freed only by eviction, or by reclaim after the client restarts on an empty directory.

---

## Example

### Single-node example

The following example starts a master and a real client on a single machine.

#### Environment

- Machine IP: `192.168.1.10`
- RDMA device: `mlx5_0`
- SSD mount point: `/nvme`
- Memory pool size: 4 GB (smaller than the total data written, since this example enables `--offload_on_evict=true`)

#### Start the master

```bash
mooncake_master \
    --rpc_port=50051 \
    --enable_offload=true \
    --offload_on_evict=true \
    --promotion_on_hit=true \
    --promotion_admission_threshold=2
```

#### Start the real client (new terminal)

```bash
export MOONCAKE_OFFLOAD_FILE_STORAGE_PATH=/nvme/mooncake_offload
export MOONCAKE_OFFLOAD_STORAGE_BACKEND_DESCRIPTOR=bucket_storage_backend
export MOONCAKE_OFFLOAD_BUCKET_MAX_TOTAL_SIZE=$((200 * 1024 * 1024 * 1024))  # 200 GB
export MOONCAKE_OFFLOAD_BUCKET_MAX_PHYSICAL_BYTES=$((200 * 1024 * 1024 * 1024)) # optional; per-rank dir: per-rank cap
export MOONCAKE_OFFLOAD_BUCKET_DISK_SCAN_CACHE_MS=500 # optional
export MOONCAKE_OFFLOAD_BUCKET_EVICTION_POLICY=lru

mooncake_client \
    --master_server_address="192.168.1.10:50051" \
    --host="192.168.1.10" \
    --device_names="mlx5_0" \
    --port=50052 \
    --protocol="rdma" \
    --metadata_server="P2PHANDSHAKE" \
    --global_segment_size="4GB" \
    --enable_offload="true"
```

(ssd-offload-two-node-example)=

### Two-node example

Suppose Node A (`192.168.1.10`) runs the master and a real client, and Node B (`192.168.1.11`) runs a second real client. Both real clients must point to the same master and advertise their own externally reachable IP (not `127.0.0.1`):

```bash
# Node A — runs the master and its own real client (--host is Node A's reachable IP)
mooncake_master --rpc_port=50051 --enable_offload=true &

export MOONCAKE_OFFLOAD_FILE_STORAGE_PATH=/nvme/mooncake_offload
mooncake_client \
    --master_server_address="192.168.1.10:50051" \
    --host="192.168.1.10" \
    --device_names="mlx5_0" \
    --protocol="rdma" \
    --metadata_server="P2PHANDSHAKE" \
    --port=50052 \
    --global_segment_size="4GB" \
    --enable_offload="true"
```

```bash
# Node B — real client only; points to the master on Node A (--host is Node B's reachable IP)
export MOONCAKE_OFFLOAD_FILE_STORAGE_PATH=/nvme/mooncake_offload
mooncake_client \
    --master_server_address="192.168.1.10:50051" \
    --host="192.168.1.11" \
    --device_names="mlx5_0" \
    --protocol="rdma" \
    --metadata_server="P2PHANDSHAKE" \
    --port=50052 \
    --global_segment_size="4GB" \
    --enable_offload="true"
```

---

## Notes

- `MOONCAKE_OFFLOAD_FILE_STORAGE_PATH` must be an absolute path to an existing, writable directory. Symbolic links and paths containing `..` are rejected. For multi-disk paths and per-client directory ownership rules (including `bucket_storage_backend`'s exclusive `flock`), see {ref}`Multiple disks <ssd-offload-multiple-disks>`.
- On real client restart, `bucket_storage_backend` and `file_per_key_storage_backend` scan existing SSD metadata and report it to the master automatically; `offset_allocator_storage_backend` recovers persisted metadata only when `MOONCAKE_OFFSET_PERSIST_MODE` is `relaxed` or `strict` (default `disabled` truncates the file).
- Eviction only notifies the master and deletes local files; objects replicated on other nodes are unaffected.
- Each machine requires its own real client process with externally reachable `--host` and `--port`; see {ref}`Two-node example <ssd-offload-two-node-example>` above.

---

## Troubleshooting

### SSD offload is not triggering

- Confirm `--enable_offload=true` is passed to both `mooncake_client` and `mooncake_master`. Both binaries define the same gflags boolean; hyphenated aliases may work, but the underscored spelling matches the source.
- Check that `MOONCAKE_OFFLOAD_FILE_STORAGE_PATH` points to an existing, writable directory. If the path is invalid, real client setup fails during `FileStorageConfig::Validate()`.
- Check the offload trigger mode: with the default `--offload_on_evict=false`, eligible objects are queued at `PutEnd` without waiting for memory pressure; with `--offload_on_evict=true`, offload runs only when memory eviction selects objects, so a large `--global_segment_size` relative to written data may never trigger eviction.
- If `--offload_on_evict=true` is set, verify OpLog (`--enable_oplog=true`) is not also enabled on the master — see the warning under [Master Offload Parameters](#master-offload-parameters).
- In mixed deployments where only some clients enable SSD offload, confirm that objects place at least one DRAM replica on an offload-enabled client: the master queues offload work only to clients that own a source memory replica and does not route objects to a different SSD owner.

### "Permission denied" or "No such file or directory" on the storage path

- Ensure the directory exists before starting the client: `mkdir -p <path>`.
- Confirm the process user has read/write access to the directory.
- Symbolic links and paths containing `..` are rejected — use an absolute, canonical path.
- The path must already exist and must be a directory. Passing a regular file path also causes setup to fail.

### "Failed to register buffer with UringFile" warning in logs

This warning appears when `MOONCAKE_OFFLOAD_USE_URING=true` and the io_uring fixed-buffer registration fails. The most common cause is that `MOONCAKE_OFFLOAD_LOCAL_BUFFER_SIZE_BYTES` exceeds the process's locked-memory limit (`RLIMIT_MEMLOCK`). io_uring requires the registered buffer to be pinned in physical memory, which counts against this limit.

Check the current limit:

```bash
ulimit -l        # in KB; "unlimited" means no cap
```

To raise it for the current session:

```bash
ulimit -l unlimited
```

To raise it permanently, add the following to `/etc/security/limits.conf`:

```
* soft memlock unlimited
* hard memlock unlimited
```

Alternatively, reduce `MOONCAKE_OFFLOAD_LOCAL_BUFFER_SIZE_BYTES` to a value within the existing limit. Note that the warning does not abort startup — the client falls back to non-fixed-buffer I/O — but performance may be lower than expected.

### `action=disk_fence` in the logs: the SSD tier stopped on a disk error

A client fences its SSD tier when a disk call fails in a way that retrying cannot fix. It decides on the first failure, from the errno of the failed call:

- `ENODEV`, `ENXIO`, `EROFS`, `ESHUTDOWN`: fence at once.
- `EIO` and similar device errors: check the filesystem first (`syncfs` unless the failed call was a read, then `fstat` on each offload directory), and fence only if the check also fails. A single bad file on a healthy filesystem does not fence.
- Anything else, including `ENOSPC`: no fence.

After a fence, the client stops reading from and offloading to that disk, hands its queued offload tasks back to the master, and unmounts its `LOCAL_DISK` segment so the master stops handing out its disk replicas. The memory segment and other clients keep working. The fence lasts until the client restarts; the gauge `mooncake_ssd_disk_fenced` is `1` while it holds. Fix or replace the disk, then restart the client. With several disks in `storage_filepath`, the filesystem check covers every disk, and one dead disk fences the whole SSD tier of that client.

Only `bucket_storage_backend` (the default) reports disk errors; with the other backends the fence never triggers.
