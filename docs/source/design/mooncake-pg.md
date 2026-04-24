# Mooncake Process Group (mooncake-pg)

## Overview

`mooncake-pg` is a custom PyTorch distributed process-group backend built on top of the Mooncake Transfer Engine. It provides `torch.distributed` collective and point-to-point (P2P) communication primitives that exploit high-speed interconnects (RDMA, NVLink, etc.) while remaining fully compatible with the standard `torch.distributed` API.

It was designed for large-scale expert-parallelism and disaggregated inference scenarios such as those described in the [Kimi K2 deployment blog post](https://lmsys.org/blog/2025-07-20-k2-large-scale-ep/), where Mooncake replaced NCCL for all-to-all expert routing across 128 H200 GPUs.

## Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│                       Python / torch.distributed                  │
│  dist.send / dist.recv / dist.broadcast / dist.all_reduce / …    │
└───────────────────────────────┬──────────────────────────────────┘
                                │
                    ┌───────────▼────────────┐
                    │    MooncakeBackend     │  (c10d::Backend subclass)
                    │  (mooncake_backend.h)  │
                    └───┬──────────┬─────────┘
                        │          │
          ┌─────────────▼──┐  ┌───▼────────────────┐
          │ ConnectionPoller│  │  MooncakeWorker     │
          │(connection_     │  │  (mooncake_worker.  │
          │  poller.h)      │  │   cu / .cuh)        │
          └────────────────┘  └───────┬─────────────┘
                                      │
                           ┌──────────▼──────────┐
                           │    P2PProxy          │
                           │    (p2p_proxy.h)     │
                           └──────────┬──────────┘
                                      │
                           ┌──────────▼──────────┐
                           │  TransferEngine      │
                           │  (RDMA / NVLink /    │
                           │   TCP …)             │
                           └─────────────────────┘
```

### MooncakeBackend

`MooncakeBackend` is registered with PyTorch as two backends:
- `"mooncake"` — for CUDA tensors.
- `"mooncake-cpu"` — for CPU tensors.

It subclasses `c10d::Backend` and overrides all collective operations: `send`, `recv`, `broadcast`, `allreduce`, `allgather`, `alltoall`, `scatter`, `reduce`, `gather`, `barrier`, and `batch_isend_irecv`.

Under the hood it maintains:
- A singleton `TransferEngine` instance shared across all backend instances within the same process, initialized with the RDMA/NVLink device configuration.
- A `MooncakeWorkerManager` (one per backend instance) that owns a per-GPU CUDA worker thread queue.
- A `P2PProxy` that serialises and routes send/recv payloads through the Transfer Engine.
- A `ConnectionPoller` that continuously polls Transfer Engine for completed transfers and signals waiting Work objects.

`MooncakeBackendOptions` carries an `activeRanks_` tensor (a boolean mask) that enables **elastic group membership**: ranks can be added dynamically via `extendGroupSizeTo()`, queried with `getActiveRanks()`, or recovered after a failure with `recoverRanks()`.

### ConnectionPoller

`ConnectionPoller` runs on a dedicated thread. It polls `TransferEngine::getTransferStatus()` for pending batch IDs and sets the `std::atomic<bool> completed` flag on the associated `MooncakeP2PWork` object, which unblocks `Work::wait()` in the calling Python thread.

### MooncakeWorker (CUDA Worker)

Each GPU rank spawns a `MooncakeWorker` CUDA thread (via a CUDA stream / host thread) to execute device-side work items — primarily memory copies involving GPU tensors (`cudaMemcpyAsync`, slicing, casting). This separation keeps the Python GIL thread free during GPU-side operations.

### P2PProxy

`P2PProxy` translates high-level send/recv requests (tensor + rank + tag) into one or more `TransferRequest` entries that the Transfer Engine can process. It handles:
- Buffer registration (delegated to `TransferEngine::registerLocalMemory`).
- Multi-slice splitting for large tensors.
- Tag-based demultiplexing when multiple P2P streams are in flight.

## Supported Operations

| Operation | Supported | Notes |
|-----------|-----------|-------|
| `send` / `recv` | ✅ | Single-tensor P2P |
| `batch_isend_irecv` | ✅ | Async batch P2P |
| `broadcast` | ✅ | Rank 0 → all |
| `allreduce` | ✅ | SUM only |
| `allgather` | ✅ | |
| `allgather_into_tensor` | ✅ | |
| `alltoall` | ✅ | Equal-size exchange |
| `alltoall_base` | ✅ | Variable-size exchange |
| `scatter` | ✅ | |
| `gather` | ✅ | |
| `reduce` | ✅ | SUM only |
| `barrier` | ✅ | Via allreduce on a dummy tensor |
| Sparse tensors | ❌ | Not supported |
| Non-SUM reduce ops | ❌ | Only SUM is implemented |

## Elastic Group Membership

`MooncakeBackend` exposes several extension APIs beyond the standard `c10d::Backend` interface:

| Function | Description |
|----------|-------------|
| `getActiveRanks()` | Returns a boolean `torch.Tensor` of size `world_size` |
| `getNumSyncedRanks()` | Number of currently synced (alive) ranks |
| `extendGroupSizeTo(size)` | Grow the process group without restart |
| `getPeerState(ranks)` | Check liveness of a list of ranks |
| `recoverRanks(ranks)` | Re-admit previously failed ranks |
| `getPreferredHca(location)` | Query optimal RDMA HCA for a memory location |

These enable fault-tolerant and elastic training scenarios where nodes may join, leave, or fail during a run.

## Build

`mooncake-pg` requires a CUDA-capable build. It is an optional component enabled via the main CMake configuration:

```bash
mkdir build && cd build
cmake .. \
    -DWITH_PG=ON \
    -DUSE_CUDA=ON \
    -DCMAKE_BUILD_TYPE=RelWithDebInfo
make -j$(nproc) mooncake_pg
```

The build produces `mooncake_pg.cpython-*.so`, which can be installed with pip via the wheel:

```bash
pip install -e ../mooncake-wheel --no-build-isolation
```

## Use Cases

- **Expert parallelism (MoE)**: All-to-all expert token routing between GPU ranks using RDMA/NVLink instead of NCCL, as demonstrated in the Kimi K2 deployment.
- **Pipeline parallelism**: Low-latency activation transfers between pipeline stages.
- **Disaggregated prefill/decode**: Offloading KV-cache transfers from NCCL to Mooncake's topology-aware engine.
- **Elastic training**: Dynamic rank management without process restart.

## See Also

- [Mooncake PG Usage Guide](../getting_started/examples/mooncake-pg-usage)
- [Transfer Engine Architecture](transfer-engine/index)
- [Kimi K2 deployment blog post](https://lmsys.org/blog/2025-07-20-k2-large-scale-ep/)
