# SSD GDS (GPU Direct Storage) Offload

## Overview

GDS offload extends [SSD offload](ssd-offload.md) with GPU Direct Storage:
when a `Put` carries GPU-resident KV cache, the value bytes are written to
NVMe **directly from GPU memory** via PCIe P2P DMA (e.g. NVIDIA cuFile /
`nvidia-fs`), bypassing both the CPU and the DRAM staging path. Compared
with the regular SSD offload path (GPU → DRAM → pwrite), this removes one
full data movement and overlaps the SSD write with the RDMA transfer, so
`Put` latency approaches `max(RDMA, DMA)` instead of `RDMA + SSD write`.

```{note}
**Status: quick, usable integration — not the final form.**
The current GDS support is intentionally built *on top of* the existing
SSD offload framework (the same data file, the same `RecordHeader` disk
layout, the same master metadata, plus a small reservation protocol).
This makes it easy to deploy and fully interoperable with non-GDS reads,
but it also means GDS reuses concepts that were designed for the DRAM
tier. A future iteration may introduce a **dedicated GDS replica type**
as a first-class citizen in the master metadata and replica lifecycle
(see [Future Work](#future-work)). Deployments should treat the current
integration as functional and supported, while expecting the
configuration surface and internal protocol to evolve.
```

---

## How It Works

### Hardware constraint: co-location

cuFile DMA uses PCIe P2P — the GPU directly addresses the local NVMe
controller. The GPU and the NVMe must therefore be on the **same
machine**, which means the store process that owns the SSD pool and the
inference-engine ranks (e.g., vLLM / SGLang) holding GPU KV cache are
co-located on each GPU node.
Cross-node reads are served by the store's regular offload RPC server.

### Write modes: standalone vs. separated store

GDS has two integration modes, depending on whether the store and the
inference engine share a process.

**Standalone mode (same process).** The engine and the store live in one
process, so GPU pointers and the `OffsetAllocator` are both local — no
RPC is involved. A `Put` submits the GDS task on the client's worker
pool (`DirectGdsOffload` → `BatchOffload` → `WriteRecord`) in parallel
with the RDMA `BatchPut`, then waits for the DMA future.

**Normal mode (separated store, `gds_client_only`).** The store service
is a separate process that owns the single `OffsetAllocator` and the
data file (`kv_cache.data`); engine ranks hold GPU pointers but no
allocator, so allocation and I/O are separated:

```
Put (engine rank)                                 store_service
═══════════════                                   ═════════════
1. RPC: ReserveOffloadSpace(keys, sizes)  ────►   allocate offsets,
                                                  insert entry (dirty),
                                                  stamp placeholder header
2. cuFileWrite DMA → kv_cache.data          (parallel with step 3)
3. BatchPut → RDMA → store DRAM                 (regular path)
4. wait DMA (120 s timeout)
5. RPC: CompleteOffloadSpace(keys)        ────►   clear dirty flag
6. NotifyOffloadSuccess                   ────►   master records DISK replica
```

- **Two-phase commit.** A reserved entry is `dirty` (invisible to reads)
  until the client acknowledges DMA completion. A store-side heartbeat
  reclaims reservations whose client died mid-write (quarantine timeout).
- **Placeholder header + header-last commit.** Both
  `ReserveOffloadSpace` (normal mode) and the `BatchOffload` GDS branch
  (standalone mode) stamp a placeholder header carrying an unknown flag
  bit before the write path starts; `WriteRecord` then writes the value
  first and the real header last. Any failure (crash, timeout, I/O
  error) leaves the placeholder on disk, so crash recovery rejects the
  torn record instead of serving stale data.
- **Slice coalescing.** Adjacent GPU slices from the same allocation
  (e.g. KV blocks inside one per-layer tensor) are merged and submitted
  as a single `cuFileWrite`, cutting DMA calls from O(blocks) to
  O(layers), and cutting `cuFileBufRegister` calls likewise via
  range-aware extent registration (one registration per merged extent
  instead of one per slice). Set `MOONCAKE_GDS_NO_MERGE=1` to disable
  merging and revert to per-slice writes (escape hatch / A-B comparison).

### Read path

- Local hit (same node): `DirectGdsBatchLoad` → `ReadRecord` →
  `cuFileRead` DMA straight into GPU memory.
- Cross-node hit: the master points the requester at the store's offload
  RPC server, which reads from disk and returns the data over RDMA.

### Vendor abstraction

Vendor-specific GDS APIs are isolated behind `GdsDeviceOps`
(`include/gds/gds_device_ops.h`), mirroring the existing
`src/device/` accelerator abstraction: one pure-virtual interface, one
`.cpp` per vendor, a single factory dispatch point. NVIDIA (cuFile) is
implemented; Hygon / Ascend / Moore Threads are stubs; a fallback
implementation always compiles, so the code builds and cleanly reports
`GDS_NOT_AVAILABLE` on machines without GDS hardware.

### Backend support

Only `offset_allocator_storage_backend` supports GDS. The blocker is
not offset precision — bucket offsets are equally deterministic — but
that GDS DMA requires the raw GPU pointer to reach `cuFileWrite`
without any CPU staging. The other two backends destroy that
precondition on their write paths:

- **FilePerKey** merges all slices into a CPU `std::string`
  (`ConcatSlicesToString`) before writing.
- **Bucket** assembles objects into CPU-side bucket buffers
  (`BuildBucket` / `WriteBucket`).

Because both backends only accept host pointers, the offload path
performs a D2H copy into pinned staging buffers *before* the backend
ever sees the data (`FileStorage::OffloadObjects`), so there is no GPU
pointer left to DMA from. `OffsetAllocatorStorageBackend` is the only
backend that forwards `Slice.ptr` untouched to the I/O layer
(`WriteRecord` → `cuFileWrite`).

---

## Usage

### Build

```bash
cmake -DUSE_GDS_BACKEND=ON ...
```

**NVIDIA (cuFile).** The `libcufile` userspace library and the
`nvidia_fs` kernel module must be installed.  The kernel module is
not auto-loaded — load it explicitly before starting any GDS process:

```bash
sudo modprobe nvidia_fs
```

Verify the device nodes appear:

```bash
ls /dev/nvidia-fs*   # should show /dev/nvidia-fs or /dev/nvidia-fs0
```

If the module is installed but not loaded, `ProbeDeviceNode()` returns
false, `GDS_NOT_AVAILABLE` is reported, and the entire GDS path falls
back to CPU pwrite.

**Other vendors (Hygon / Ascend / Moore Threads).** Each vendor has
its own kernel driver and userspace library, loaded and configured
according to the vendor's documentation.  The `GdsDeviceOps` factory
(`gds_device_factory.cpp`) probes the corresponding device node at
startup; the fallback path is shared.  Contact your GPU vendor for
GDS driver installation instructions.

### store_service

```bash
export MOONCAKE_ENABLE_GDS=1
export MOONCAKE_OFFLOAD_STORAGE_BACKEND_DESCRIPTOR=offset_allocator_storage_backend
export MOONCAKE_OFFLOAD_FILE_STORAGE_PATH=/mnt/nvme/offload
export MOONCAKE_OFFLOAD_TOTAL_SIZE_LIMIT_BYTES=96636764160   # e.g. 90 GiB
```

### Inference engine (each rank, same node)

```bash
export MOONCAKE_ENABLE_GDS=1
export MOONCAKE_OFFLOAD_STORAGE_BACKEND_DESCRIPTOR=offset_allocator_storage_backend
export MOONCAKE_OFFLOAD_FILE_STORAGE_PATH=/mnt/nvme/offload  # same path as store
export MOONCAKE_GDS_STORE_SERVICE_ADDR=127.0.0.1:PORT      # store coro_rpc address
```

Engine ranks run with `global_segment_size=0` (normal mode: the rank
contributes no memory segment and runs no allocator); the separated
store owns the memory pool and the SSD tier.

### Environment variables

| Variable | Used by | Description |
|----------|---------|-------------|
| `MOONCAKE_ENABLE_GDS` | store + engine | Enable GDS DMA (default off). |
| `MOONCAKE_OFFLOAD_STORAGE_BACKEND_DESCRIPTOR` | store + engine | Must be `offset_allocator_storage_backend`. |
| `MOONCAKE_OFFLOAD_FILE_STORAGE_PATH` | store + engine | Shared data-file directory (same value on both sides). |
| `MOONCAKE_OFFLOAD_TOTAL_SIZE_LIMIT_BYTES` | store | Data-file capacity (pre-allocated via `posix_fallocate`). |
| `MOONCAKE_GDS_STORE_SERVICE_ADDR` | engine | store coro_rpc address for reserve/complete. |
| `MOONCAKE_GDS_NUM_WORKERS` | engine | DMA worker pool size (default `min(4, hw_concurrency)`). |
| `MOONCAKE_GDS_NO_MERGE` | store + engine | `1` disables slice coalescing (fallback / A-B testing). |
| `MOONCAKE_GDS_RETRY_DRIVER` | store + engine | `1` retries `cuFileDriverOpen` after a failed first probe. |
| `MOONCAKE_GDS_ALLOW_REOPEN` | store | **Test only.** Deletes the existing data file on restart. |

### Production notes

- **Never set `MOONCAKE_GDS_ALLOW_REOPEN=1` in production.** Without it,
  the store refuses to start when a data file exists, protecting your
  data; with it, restart silently discards the SSD tier.
- The data file must sit on a local ext4 (mounted with `data=ordered`)
  or xfs filesystem. Linux software RAID **RAID-0 is supported** by
  `nvidia-fs.ko` (its block-layer callbacks resolve the stripe mapping);
  other RAID levels are not. Note that RAID-0 works via the `nvidia-fs`
  path — GDS in PCI P2PDMA mode is not supported with RAID. Loop devices
  are not supported. See NVIDIA's
  [GDS troubleshooting guide](https://docs.nvidia.com/gpudirect-storage/troubleshooting-guide/index.html)
  for the authoritative compatibility list.
- The offload RPC handlers are **unauthenticated** and bind `0.0.0.0`
  (cross-node reads require reachability). Restrict the offload RPC port
  to a trusted network via firewall / security-group rules.
- When diagnosing DMA failures (`cuFileWrite` errors), check
  `/proc/driver/nvidia-fs/stats` and run with `GLOG_v=1` — the write
  path logs coalescing decisions, registration failures, and `errno`.
- Keep `MOONCAKE_GDS_NO_MERGE` **unset** in normal production. It exists
  for A/B comparison of the coalescing write path and as an escape
  hatch if a driver version misbehaves with merged writes; leaving it
  on permanently costs ~4× more `cuFileWrite` calls and buffer
  registrations per record.

---

## Limitations

- Only `offset_allocator_storage_backend`; 4 GiB max object size.
- store and inference engine must be co-located (GDS hardware constraint).
- GDS-written records do not carry a CRC: the value never passes
  through the CPU, so there is nothing to checksum on the write side,
  and the header is written with `flags=0` so recovery skips the CRC
  check for these records. Recovery correctness relies on the
  placeholder + checkpoint-ordering mechanism instead.
- `BatchPutWhenPreferSameNode` does not trigger GDS offload.

---

## Future Work

### Dedicated GDS replica

The current integration registers GDS-written data as ordinary DISK
replicas through the existing offload metadata path, and coordinates
clients with a Reserve/Complete RPC protocol on the side. This was a
deliberate "quick and usable" choice: zero master changes and full
interoperability with the non-GDS read path.

A dedicated **GDS replica type** is the natural next step: making
GPU-direct disk writes a first-class replica in the master's metadata
and lifecycle (allocation, eviction, recovery) rather than an overlay on
the DRAM-tier offload framework. That would simplify the reservation
protocol, give the master direct visibility into GDS capacity and
placement, and allow GDS-specific policies (e.g. replica priorities
between DRAM, CPU SSD, and GPU-direct SSD). Configuration and internal
protocols described in this document may change when that lands.
