# SSD Offload

## Overview

Mooncake Store supports offloading KV cache objects from distributed memory to a local filesystem path, typically backed by local SSDs. When memory pressure is high, the master instructs clients to persist selected objects to disk. On a cache miss, the client automatically falls back to reading from the local filesystem-backed offload path.

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
    --rpc_port=50051
```

### Step 3A (Mode A): Start the application with embedded Real Client

Use the `--enable_ssd_offload` flag to enable SSD offload, and set environment variables to specify the storage path and backend:

```python
from mooncake.store import MooncakeDistributedStore

store = MooncakeDistributedStore()
store.setup(
    local_hostname="<machine IP>",
    metadata_server="P2PHANDSHAKE",
    global_segment_size=4 * 1024 * 1024 * 1024,  # 4 GB
    local_buffer_size=512 * 1024 * 1024,  # 512 MB
    protocol="rdma",
    rdma_devices="eth0",
    master_server_addr="127.0.0.1:50051",
    enable_ssd_offload=True,
    ssd_offload_path="/nvme/mooncake_offload"
)
```

### Step 3B (Mode B): Start the standalone real client with SSD offload enabled and connect it with the dummy client

Set the same SSD offload environment variables as in Mode A.

```bash
export MOONCAKE_OFFLOAD_FILE_STORAGE_PATH=/nvme/mooncake_offload
export MOONCAKE_OFFLOAD_STORAGE_BACKEND_DESCRIPTOR=bucket_storage_backend

mooncake_client \
    --master_server_address=127.0.0.1:50051 \
    --host=<machine IP> \
    --protocol="rdma"  \
    --device_names=<NIC name, e.g. eth0> \
    --port=50052 \
    --global_segment_size="4 GB" \
    --enable_offload=true  \
    --metadata_server="P2PHANDSHAKE"
```

The application (e.g., SGLang) connects to the standalone real client via the `MooncakeDistributedStore` Python SDK. The Python process acts as a DummyClient; SSD offload and fallback loading are handled by the standalone real client process.

```python
from mooncake.store import MooncakeDistributedStore

store = MooncakeDistributedStore()
store.setup_dummy(
    mem_pool_size=4 * 1024 * 1024 * 1024,  # 4 GB
    local_buffer_size=512 * 1024 * 1024,  # 512 MB
    server_address="<machine IP>:50052"   # mooncake_client RPC address from Step 3
)
```

> **Note:** When using `bucket_storage_backend` or `file_per_key_storage_backend`, the real client scans existing SSD metadata on startup and reports it to the master automatically. `offset_allocator_storage_backend` is the exception: it truncates its data file during initialization and does not recover previously offloaded objects after a restart.

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
| `--start_offload_rpc_server` | `true` | Start the offload RPC server used by DummyClients for SSD reads. Effective only when `--enable_offload=true`; disable only for write-only owners |
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
| `--offload_on_evict_async` | `false` | Asynchronous offload-on-evict, see {ref}`below <ssd-offload-async-offload-on-evict>`. Needs `--offload_on_evict=true` and no OpLog |
| `--offload_on_evict_stall_ms` | `30000` | Async offload-on-evict: a store none of whose copies has ended for this long is stalled. Must be greater than `0` |
| `--offload_on_evict_backoff_ms` | `200` | Async offload-on-evict: how long a cycle that queued and freed nothing holds back the next one |
| `--promotion_on_hit` | `false` | Promote SSD-resident objects back to DRAM after read hits |
| `--promotion_admission_threshold` | `2` | Minimum CountMinSketch count before promotion is admitted. Set `1` to disable second-touch gating |
| `--promotion_max_per_heartbeat` | `1` | Maximum promotion tasks returned to one client per heartbeat. Keep conservative for large objects because each task does SSD read plus memory write |
| `--promotion_queue_limit` | `50000` | Maximum in-flight promotion tasks tracked by the master |

Start with `--enable_offload=true` for eager SSD persistence. Add `--offload_on_evict=true` when SSD writes should happen only under memory pressure. For SSD-heavy workloads where offload-on-evict is dropping too many objects, raise both `--offloading_queue_limit` and `--offload_cap_ratio`; for example, `--offloading_queue_limit=500000 --offload_cap_ratio=0.8` allows up to `400000` objects to be queued in one eviction cycle.

(ssd-offload-async-offload-on-evict)=

### Asynchronous offload-on-evict (`--offload_on_evict_async`)

Plain `--offload_on_evict=true` cannot keep up with a high put rate: an object queued for offload frees nothing until its copy lands, so every eviction cycle walks past its target and queues every expired object it meets; the trigger fires again every 10 ms while the copies are in flight; and the store's own read of the source grants the object a read lease, which moves it to the newest end of the eviction order, so its memory is freed only after the rest of DRAM has rotated through. Puts then wait on allocation failures.

`--offload_on_evict_async=true` (default `false`) changes how eviction drives the offloads:

| | `--offload_on_evict=true` | with `--offload_on_evict_async=true` |
|---|---|---|
| Cycle target | Queued objects do not count; the cycle queues every candidate | A queued object counts toward the target (`--eviction_ratio` of the objects, more when usage overshoots the watermark) |
| Trigger | Memory usage over `--eviction_high_watermark_ratio`, or an allocation failure | Usage **minus the memory whose copy is in flight to a store that is not stalled** over the watermark, or an allocation failure. An allocation failure while at least `--eviction_ratio` of DRAM is in flight to stores that ended a copy (or got their first) within `max(5 x --offload_on_evict_backoff_ms, 1 s)` (at most `--offload_on_evict_stall_ms`) waits for those copies (a cycle then frees only objects already on disk); a store quiet for longer is not waited on |
| Copies in flight | Unbounded (the mailbox limit only bounds what a store has not picked up yet) | At most `--offloading_queue_limit` x `--offload_cap_ratio` objects per store; a full store holds back only its own objects. A store none of whose copies has ended (landed, failed or been cancelled) for `--offload_on_evict_stall_ms` is stalled: it gets no more copies, and its copies in flight count as used memory, until one ends |
| Memory freed | By a later cycle, once the store's read lease ran out | When the copy lands (`NotifyOffloadSuccess`), unless the object was read, pinned or taken by another task meanwhile, in which case eviction judges it again later |
| `--offload_force_evict` | Drops past the per-cycle cap, also on watermark cycles | Drops an object it cannot queue only after a failed allocation (or with usage past halfway from the watermark to full) |
| A cycle that achieved nothing | Retried every 10 ms | The next one waits `--offload_on_evict_backoff_ms`, or until a copy ends |
| Victim order | Oldest lease first | Oldest lease first, also under sustained overload: an object kept because its store is full keeps its place for the next room, and a cycle ends once every store is full |

Requirements and fallback: the master needs `--enable_offload=true` and `--offload_on_evict=true` and must not run with the OpLog (`--enable_oplog`), which uses its own reclaimer. If any is missing, the master logs one warning at startup and keeps the configured mode. HA with leader election and no OpLog is supported (see the failover caveat below). With the requirements met, `--offloading_queue_limit` x `--offload_cap_ratio` must be at least `1` and `--offload_on_evict_stall_ms` above `0`, or the master refuses to start. Memory eviction with offload-on-evict, async or not, walks the eviction index on one thread: `--eviction_worker_threads` does not apply to it. The three master settings can also be set in the master's configuration file (`offload_on_evict_async`, `offload_on_evict_stall_ms`, `offload_on_evict_backoff_ms`).

| Setting | Where | Default | Description |
|---|---|---|---|
| `--offload_on_evict_async` | master | `false` | The behaviour above. The master hands each copy to its store with a token (`OffloadTaskItem::token`, a struct_pack compatible field, so older stores and masters still interoperate) and registers a landed copy only if its task still carries that token: the report of a copy whose object was removed or replaced, or whose task failed or expired, never creates or overwrites the key |
| `MOONCAKE_OFFLOAD_LEASE_FREE_SOURCE_READ` | real clients | `true` | A real client reads the source replica of a copy that carries a token without taking a read lease, so the copy does not push the object to the newest end of the eviction order; the offload task pins the source during the copy. Copies without a token (write-through, plain offload-on-evict, older masters) take the lease as before. `0` opts out. Real clients built before the token ignore it and take the lease, which works, but every landing then finds the lease live and keeps the memory until a later cycle |
| `--offload_on_evict_backoff_ms` | master | `200` | How long a cycle that queued and freed nothing holds back the next one, unless a copy ends sooner |
| `--offload_on_evict_stall_ms` | master | `30000` | A store that has copies in flight and has ended none (nor received its first) for this long is stalled: a hung store, or a heartbeat reply that never reached it. Keep it at least three times the stores' `MOONCAKE_OFFLOAD_HEARTBEAT_INTERVAL_SECONDS` plus one bucket write (see the caveats) |
| `--offloading_queue_limit` x `--offload_cap_ratio` | master | `50000` x `0.5` | Copies one store may have in flight. Each must cover what the store receives between two heartbeats, or eviction backs off (`master_offload_evict_no_room_total`) |
| `--eviction_ratio` | master | `0.05` | Share of the evictable objects one cycle queues, plus the overshoot past the watermark. Also the amount in flight that makes an allocation failure wait for landings |
| `--eviction_high_watermark_ratio` | master | `0.90` | The headroom above it (1 - ratio of DRAM) has to absorb the puts of one evict-to-land interval: the store's heartbeat pickup plus its bucket write |
| `--offload_force_evict` | master | `false` | See the table above. Not recommended with this mode (see the caveats) |
| `MOONCAKE_OFFLOAD_HEARTBEAT_INTERVAL_SECONDS` | real clients | `10` | How often a store picks up its copies. Eviction hands work out in bursts, so a short interval (e.g. `1`) keeps the copies in flight, and the memory they pin, small |

Example:

```bash
# master
mooncake_master \
    --rpc_port=50051 \
    --enable_offload=true \
    --offload_on_evict=true \
    --offload_on_evict_async=true \
    --offloading_queue_limit=20000 \
    --offload_cap_ratio=0.5 \
    --eviction_ratio=0.03 \
    --eviction_high_watermark_ratio=0.9

# every real client (lease-free source reads are on by default there)
export MOONCAKE_OFFLOAD_HEARTBEAT_INTERVAL_SECONDS=1
mooncake_client --enable_offload=true ...
```

Master metrics (only reported with the mode on):

- `master_offload_evict_pending_bytes`, `master_offload_evict_pending_objects`: memory queued for offload whose copy has not ended.
- `master_offload_evict_stalled_bytes`: of those, the bytes queued to stalled stores.
- `master_offload_evict_freed_total`, `master_offload_evict_freed_bytes_total`: memory freed as copies landed. These frees are not counted in `master_evicted_key_count` / `master_evicted_size_bytes`, which count what the eviction cycles themselves released.
- `master_offload_evict_kept_total{reason=read_again|pinned|busy}`: landed copies whose memory stayed.
- `master_offload_evict_write_latency_ms`, `master_offload_evict_free_latency_ms`: from eviction queueing the copy to the copy being registered, and to its memory being freed as it landed. A copy whose memory stayed is not in the second.
- `master_offload_evict_lease_wait_total`: landed copies whose memory stayed because a reader took a lease while the copy was written (`kept_total{reason=read_again}`); a later eviction frees it without another copy.
- `master_offload_evict_no_room_total`: pushes refused because the store had its full share in flight (or has stalled).
- `master_offload_evict_stale_landings_total`: copy reports whose token no task carries any more; never registered.
- `master_offload_evict_round_skipped_total{reason=no_room|wait_for_landings|backoff}`: eviction ticks over the watermark that did not run a full cycle (`wait_for_landings`: a cycle that queues nothing new and frees only objects already on disk).
- `master_offload_evict_waited_for_frees_total`: the `wait_for_landings` ticks alone.
- `master_offload_evict_parked_total`: sources kept until their copy ended because their object was removed meanwhile.

Caveats:

- An object is written to SSD only when it leaves DRAM, so after a master restart without the OpLog the DRAM-only hot set is gone; write-through keeps it readable from SSD.
- HA failover (leader election, no OpLog): the new leader does not know the copies the old one had in flight or their tokens. A store's later report of such a copy carries a token no task holds, so it is dropped as stale (`master_offload_evict_stale_landings_total`) and not registered: the new leader cannot tell whether the source stayed pinned while the store read it. The store's bucket still holds the copy, so a later copy of the same key is skipped by the store as a duplicate and reported as failed (the key stays in DRAM and is retried after 10 s) until the store's disk eviction removes that bucket, or the store re-registers its disk content after a restart or a re-mount (its rescan reports without tokens, which registers as before). The orphaned copies are at most what each store had in flight at the failover: `--offloading_queue_limit` x `--offload_cap_ratio` objects per store.
- A remove of an object whose copy a store already took (`Remove`, forced or not, `RemoveAll`, `RemoveByRegex`, `BatchRemove`, `BatchReplicaClear`) succeeds at once, and a copy still queued is cancelled. The source's memory of a taken copy stays allocated, and charged to that store, until the copy is reported, its store's disk is unmounted, or it would have expired; the report registers nothing (`master_offload_evict_parked_total`). An upsert of such a key still waits for the copy (`OBJECT_HAS_REPLICATION_TASK`), as before.
- Stall window: a store counts as stalled after `--offload_on_evict_stall_ms` in which none of its copies ended (landed, failed, or was cancelled or expired), and gets no new copies until one ends. A store's heartbeat alone does not count: a heartbeat reply lost on the way holds copies that will never land, and counting heartbeats would keep them in the in-flight discount until they expire. A store that took copies and then stays quiet longer than the window (several lost heartbeat replies, or a heartbeat interval at least as long as the window) is held there until a copy is reported or expires (`--put_start_release_timeout_sec`, 600 s by default); with a single store, eviction then frees only what is already on disk. The default window (30 s) is three times the default heartbeat interval (10 s); keep that ratio if you change either.
- A store that hangs (or never receives its tasks) keeps getting copies until it counts as stalled, so it can pin up to its room (`--offloading_queue_limit` x `--offload_cap_ratio` objects) until the copies expire; a smaller stall window bounds how long the pool runs short meanwhile.
- Overload: while every store is full and usage stays over the watermark, each eviction tick (10 ms) walks the expired objects again to find any already on disk, which keeps the eviction thread busy in proportion to the expired objects it walks.
- Without `--offload_force_evict`, a failed allocation waits for the landings once `--eviction_ratio` of DRAM is in flight, so under sustained put pressure puts go only as fast as the stores pick up and land the copies they are handed. A store that waits a full `MOONCAKE_OFFLOAD_HEARTBEAT_INTERVAL_SECONDS` after every tick, including one that wrote, lands about one batch per interval, and puts then go no faster than with plain offload-on-evict; a store that polls again right after a tick that wrote keeps up. Run the stores with such adaptive offload polling, and a short heartbeat interval.
- `--offload_force_evict` is not recommended with this mode. Under pressure a cycle stops counting queued copies toward its target and keeps queueing until every store is full, so SSD writes come in bursts, which raises read latency and the time from eviction to free; and, combined with a stall window too short for the heartbeat interval, a store judged stalled refuses copies that pressure cycles then drop.
- A failed copy (`NotifyOffloadSuccess` NACK) releases the object's slot at once, and eviction does not queue the object again for 10 seconds, except a cycle that may drop it. A copy that is never reported is released by the offload task timeout (`--put_start_release_timeout_sec`), and a copy its store never picked up leaves the store's queue with it; unmounting a store's disk ends the copies queued to it at once.
- A tenant's quota eviction subtracts what the tenant already has in flight to stores that are not stalled from its target, counts what it queues, and never drops.
- The `[EVICT] No memory freed` warnings of a cycle that only queued copies are expected: the memory is freed as the copies land.
- When the master declines to register a landed copy (stale token, removed object), the store's bucket still holds it until the store evicts the bucket; a later copy of the same key is then skipped by the store as a duplicate and reported as failed, so that key stays in DRAM (or is retried) until then. This is the same as for any registration the master refuses.

## SSD Offload Configuration

### Core settings

| Environment Variable | Default | Description |
|---|---|---|
| `MOONCAKE_OFFLOAD_FILE_STORAGE_PATH` | `/data/file_storage` | Absolute path to the SSD storage directory. `bucket_storage_backend` also accepts a comma-separated list of paths to spread offload across several disks — see {ref}`Multiple disks <ssd-offload-multiple-disks>` |
| `MOONCAKE_OFFLOAD_STORAGE_BACKEND_DESCRIPTOR` | `bucket_storage_backend` | Storage backend type (see below) |
| `MOONCAKE_OFFLOAD_LOCAL_BUFFER_SIZE_BYTES` | `1342177280` (1.25 GB) | Client-side staging buffer size |
| `MC_STORE_PINNED_RESTORE_ARENA_SIZE_BYTES` | `0` | Size of the additional preallocated pinned-host arena for same-process SSD-to-GPU restores and DFS ranged-session reads into device memory. See the constraints below |
| `MOONCAKE_OFFLOAD_SCANMETA_ITERATOR_KEYS_LIMIT` | `20000` | Max keys processed per iteration when scanning existing SSD metadata on startup |
| `MOONCAKE_OFFLOAD_TOTAL_SIZE_LIMIT_BYTES` | `2199023255552` (2 TB) | Maximum disk usage |
| `MOONCAKE_OFFLOAD_TOTAL_KEYS_LIMIT` | `10000000` | Maximum number of objects on disk |
| `MOONCAKE_OFFLOAD_HEARTBEAT_INTERVAL_SECONDS` | `10` | Interval for offload heartbeat to master (seconds) |
| `MOONCAKE_OFFLOAD_CLIENT_BUFFER_GC_INTERVAL_SECONDS` | `1` | Interval for reclaiming expired offload buffers; defaults to the heartbeat interval in the current implementation |
| `MOONCAKE_OFFLOAD_CLIENT_BUFFER_GC_TTL_MS` | `5000` | Lease time for buffers returned by `batch_get_offload_object` before GC reclaims them |
| `MOONCAKE_OFFLOAD_USE_URING` | `false` | Enable io_uring for async file I/O |
| `MOONCAKE_OFFLOAD_LEASE_FREE_SOURCE_READ` | `true` | Read the source of an async offload-on-evict copy without a read lease (see {ref}`Asynchronous offload-on-evict <ssd-offload-async-offload-on-evict>`); `0` opts out |
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
| `MOONCAKE_OFFLOAD_BUCKET_DIRECT_WRITE` | `0` | Write mode: `0`/`false` keeps buffered writes; `packed` enables packed O_DIRECT/io_uring writes (`true`/`1` are aliases). Packed writes require a build with io_uring support and `MOONCAKE_OFFLOAD_USE_URING=true`; otherwise writes remain buffered. `aligned` enables aligned O_DIRECT writes, which do not need io_uring (see below). Unknown values fail backend initialization. `.meta` writes stay buffered with their exact protobuf length. |
| `MOONCAKE_OFFLOAD_BUCKET_COPY_THREADS` | `1` | Threads used only by `packed` direct writes, clamped to 1–8; invalid values use 1. Buckets smaller than 16 MiB always use one thread. |
| `MOONCAKE_OFFLOAD_BUCKET_MAX_TOTAL_SIZE` | `0` | Eviction threshold in bytes, applied **per disk**. When set to `0`, the backend uses **90% of the physical disk capacity** as the quota — it does not mean unlimited. Set an explicit value to control disk usage precisely. |
| `MOONCAKE_OFFLOAD_BUCKET_MAX_TOTAL_SIZE_LIST` | empty | Per-disk eviction thresholds as a comma-separated list, positionally aligned with the paths in `MOONCAKE_OFFLOAD_FILE_STORAGE_PATH`; when set, it needs exactly one entry per path. Empty applies `MOONCAKE_OFFLOAD_BUCKET_MAX_TOTAL_SIZE` to every disk. See {ref}`Multiple disks <ssd-offload-multiple-disks>` |
| `MOONCAKE_OFFLOAD_BUCKET_EVICTION_POLICY` | `fifo` | Eviction policy: `none` / `fifo` / `lru` |
| `MOONCAKE_OFFLOAD_BUCKET_MAX_PHYSICAL_BYTES` | `0` (disabled) | Hard cap on **real on-disk** bytes (`du`-equivalent) under this backend's `ssd_offload_path`, applied **per disk** when several are configured. `0` disables it. With per-rank directories (required), the cap is per-rank — see below. |
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
  to an existing, writable directory; whitespace around entries is trimmed and
  empty entries are dropped. Roots must not nest inside one another, or startup
  fails — separate disks are the point, and overlapping roots would double-count
  usage.
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
├── 1710000000000-0.bucket   # data file (multiple KV pairs)
├── 1710000000000-0.meta     # metadata file
├── 1710000000001-0.bucket
└── ...
```

Best for: general-purpose use, large-scale deployments.

### `file_per_key_storage_backend`

Stores each object in an individual file. Simple and easy to inspect, but generates many small files at scale.

| Environment Variable | Default | Description |
|---|---|---|
| `MOONCAKE_OFFLOAD_FSDIR` | `file_per_key_dir` | Subdirectory name under `MOONCAKE_OFFLOAD_FILE_STORAGE_PATH` where objects are stored |
| `MOONCAKE_OFFLOAD_ENABLE_EVICTION` | `true` | Enable disk eviction for this backend. Short alias: `ENABLE_EVICTION` |

Best for: debugging or small-scale deployments.

### `offset_allocator_storage_backend`

Pre-allocates a single large file and manages offset-based allocation within it. Highest concurrency via 1024-shard metadata.

> **Warning:** This backend does **not** support metadata recovery on restart. On initialization, the data file is truncated and all in-memory metadata is cleared. Any previously offloaded objects become inaccessible after a process restart.

**Capacity:** `MOONCAKE_OFFLOAD_TOTAL_SIZE_LIMIT_BYTES` is used directly as the pre-allocated file size (100%, no safety margin). Unlike `bucket_storage_backend`, there is no separate quota variable — this is the sole disk usage control. Set it below the physical disk capacity to avoid filling the disk; writes are rejected once usage reaches this limit.

Best for: high-concurrency scenarios with many small objects where restart durability is not required.

---

## Eviction

### Write-time eviction

When `MOONCAKE_OFFLOAD_BUCKET_MAX_TOTAL_SIZE` or `MOONCAKE_OFFLOAD_BUCKET_MAX_PHYSICAL_BYTES` is set, the backend automatically evicts buckets before writing new ones if total disk usage would exceed the limit. With several disks configured, the limit and the eviction order are per disk: a write evicts only on the disk it is being placed on.

| Policy | Behavior |
|--------|----------|
| `none` | No eviction (default); writes fail when disk is full |
| `fifo` | Evict the oldest bucket first |
| `lru` | Evict the least recently read bucket first |

Eviction is two-phase: the bucket is removed from metadata and master is notified first, then in-flight reads are drained before files are deleted.

**`MAX_TOTAL_SIZE` vs `MAX_PHYSICAL_BYTES`**

`MAX_TOTAL_SIZE` bounds a *logical* in-memory counter (`data_size + meta_size` summed per object). It undercounts real disk usage: it ignores filesystem block rounding, and a bucket file stays on disk until *every* object in it is evicted while the counter drops per object. `MAX_PHYSICAL_BYTES` instead measures the *real* on-disk bytes (`du`-equivalent), so prefer it when you must not exceed a hard physical limit — most importantly a Kubernetes `emptyDir` with a `sizeLimit`, which the kubelet enforces by the volume's actual `du` usage and evicts the pod when exceeded.

**Ownership and scope.** LOCAL_DISK allows **one live client per** `ssd_offload_path` / `MOONCAKE_OFFLOAD_FILE_STORAGE_PATH`. A second live client fails `Init()` with `storage_path already held by another live client`. With several disks configured, every directory in the list is locked, so sharing any one of them fails the same way. Do **not** point several TP ranks at the same path — use **per-rank / per-client directories** (or separate disks). See the ownership contract in [SSD Offload design](../../design/store/ssd-offload.md).

`MAX_PHYSICAL_BYTES` scans this backend's own path. With the required per-rank layout, the cap bounds **each rank individually**. Set it to the per-rank budget (e.g. `sizeLimit / N` when N ranks share one volume via separate subdirectories, or the disk capacity when each rank has its own disk). A shared offload directory across live clients is no longer a supported deployment shape.

> A cap on the *global* sum across ranks that holds under any layout (including ranks on separate disks) cannot be done by a per-directory scan and would require master-side aggregation; it is not part of this feature.

The two limits are independent and may be combined — eviction fires when either would be exceeded. `MAX_PHYSICAL_BYTES` only takes effect when an eviction policy is set (`fifo`/`lru`); with `none`, eviction never runs.

---

### Proactive watermark eviction

When `MOONCAKE_OFFLOAD_ENABLE_DISK_WATERMARK_EVICTION=true`, the FileStorage heartbeat asks the backend to check local-disk usage every `MOONCAKE_OFFLOAD_HEARTBEAT_INTERVAL_SECONDS` seconds. If usage exceeds `MOONCAKE_OFFLOAD_DISK_EVICTION_HIGH_WATERMARK_RATIO`, the backend evicts toward `MOONCAKE_OFFLOAD_DISK_EVICTION_LOW_WATERMARK_RATIO`.

| Backend | Behavior |
|---------|----------|
| `bucket_storage_backend` | Reuses `MOONCAKE_OFFLOAD_BUCKET_EVICTION_POLICY`; set it to `fifo` or `lru` because the default policy `none` makes watermark eviction a no-op |
| `file_per_key_storage_backend` | Requires `MOONCAKE_OFFLOAD_ENABLE_EVICTION=true`; reuses the FIFO file eviction queue and recovered keys from startup metadata scan |
| `offset_allocator_storage_backend` | No-op in this version |

The watermark ratios apply to each backend's quota. For `bucket_storage_backend`, the quota is `MOONCAKE_OFFLOAD_BUCKET_MAX_TOTAL_SIZE` (or the matching entry of `MOONCAKE_OFFLOAD_BUCKET_MAX_TOTAL_SIZE_LIST`); when that value is `0`, the backend defaults it to 90% of the physical disk capacity. With several disks configured, each disk is checked against its own quota and evicted toward the low watermark on its own. For `file_per_key_storage_backend`, the underlying file backend uses its storage quota, which defaults to 90% of the physical disk capacity in SSD offload mode.

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

The following example starts a master and a real client on a single machine.

### Environment

- Machine IP: `192.168.1.10`
- NIC: `eth0`
- SSD mount point: `/nvme`
- Memory pool size: 4 GB (smaller than the total data written, to trigger offload)

### Start the master

```bash
mooncake_master \
    --rpc_port=50051 \
    --enable_offload=true \
    --offload_on_evict=true \
    --promotion_on_hit=true \
    --promotion_admission_threshold=2
```

### Start the real client (new terminal)

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
    --device_names="eth0" \
    --port=50052 \
    --protocol="rdma" \
    --global_segment_size="4GB" \
    --enable_offload="true"
```

---

## Notes

- `MOONCAKE_OFFLOAD_FILE_STORAGE_PATH` must be an absolute path to an existing, writable directory. Symbolic links and paths containing `..` are rejected. With `bucket_storage_backend` the value may be a comma-separated list of such directories, one per disk; see {ref}`Multiple disks <ssd-offload-multiple-disks>`.
- Each live Real Client needs its **own** offload directory — with several disks, its own directory on every disk. Sharing one path across TP ranks fails at `Init()` under the LOCAL_DISK ownership lock, which is taken on every configured directory.
- On real client restart, `bucket_storage_backend` and `file_per_key_storage_backend` scan existing SSD metadata and report it to the master, so previously offloaded objects remain accessible. `offset_allocator_storage_backend` does not support restart recovery.
- Eviction only notifies the master and deletes local files; objects replicated on other nodes are unaffected.
- Each machine requires its own real client process. In multi-node deployments, ensure `--host` and `--port` are correctly set so nodes can reach each other.

**2-node example:** suppose Node A (`192.168.1.10`) runs the master and Node B (`192.168.1.11`) is a second worker. Both real clients must point to the same master and advertise their own externally reachable IP:

```bash
# Node A — runs the master and its own real client
mooncake_master --rpc_port=50051 --enable_offload=true &

export MOONCAKE_OFFLOAD_FILE_STORAGE_PATH=/nvme/mooncake_offload
mooncake_client \
    --master_server_address="192.168.1.10:50051" \
    --host="192.168.1.10" \        # externally reachable IP of Node A
    --device_names="eth0" \
    --protocol="rdma" \
    --metadata_server="P2PHANDSHAKE" \
    --port=50052 \
    --global_segment_size="4GB" \
    --enable_offload="true"
```

```bash
# Node B — real client only; points to the same master on Node A
export MOONCAKE_OFFLOAD_FILE_STORAGE_PATH=/nvme/mooncake_offload
mooncake_client \
    --master_server_address="192.168.1.10:50051" \
    --host="192.168.1.11" \        # externally reachable IP of Node B, NOT 127.0.0.1
    --device_names="eth0" \
    --protocol="rdma" \
    --metadata_server="P2PHANDSHAKE" \
    --port=50052 \
    --global_segment_size="4GB" \
    --enable_offload="true"
```

---

## Troubleshooting

### SSD offload is not triggering

- Confirm `--enable_offload=true` is passed to both `mooncake_client` and `mooncake_master`. Both binaries define the same gflags boolean; hyphenated aliases may work, but the underscored spelling matches the source.
- Check that `MOONCAKE_OFFLOAD_FILE_STORAGE_PATH` points to an existing, writable directory. If the path is invalid, real client setup fails during `FileStorageConfig::Validate()`.
- Verify memory pressure is actually high enough for the master to trigger offload. If the memory pool (`--global_segment_size`) is large relative to the data written, offload may never activate.

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
