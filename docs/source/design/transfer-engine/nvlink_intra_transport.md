# Intra-Node NVLink Transport

The Intra-Node NVLink transport (`IntraNodeNvlinkTransport`) enables **zero-copy GPU-to-GPU data transfers within a single host** using the NVLink high-speed interconnect, without going through the PCIe bus or system memory.

## Overview

Modern NVIDIA servers (DGX A100/H100/H200) connect all GPUs on the same node via NVLink. For within-node transfers this provides substantially higher bandwidth and lower latency than PCIe-based copies or RDMA loopback.

`IntraNodeNvlinkTransport` uses CUDA IPC handles and the **UBShmem Fabric Allocator** to map peer GPU memory directly into the local GPU's address space, then issues `cudaMemcpyAsync` on the NVLink path.

## Hardware Requirements

- NVIDIA GPUs with intra-node NVLink (e.g., A100 / H100 / H200 SXM variants in DGX systems).
- CUDA 11.0+ with peer access support (`cudaDeviceEnablePeerAccess`).

## Build

```bash
cmake .. \
    -DUSE_INTRA_NVLINK=ON \
    -DUSE_CUDA=ON \
    -DCMAKE_BUILD_TYPE=RelWithDebInfo
make -j$(nproc)
```

`USE_INTRA_NVLINK` enables the `IntraNodeNvlinkTransport` class in the `transfer_engine` library.

## Usage

### Protocol String

Use `"nvlink_intra"` as the protocol string:

```python
from mooncake.engine import TransferEngine

te = TransferEngine()
te.initialize("localhost:12345", "P2PHANDSHAKE", "nvlink_intra", "cuda:0")
```

### Automatic Selection

When `MooncakeDistributedStore` is used with `protocol="rdma"` and both the source and destination are on the same node, the topology-aware path selector may automatically prefer the intra-node NVLink path over RDMA when `USE_INTRA_NVLINK` is enabled and peer access is available.

## Memory Registration

`IntraNodeNvlinkTransport` uses the **UBShmem Fabric Allocator**, which:
1. Creates a shareable CUDA IPC memory handle via `cudaIpcGetMemHandle`.
2. Exports the handle to other CUDA contexts on the same host via a shared-memory segment.
3. Maps the remote handle into the local GPU's address space with `cudaIpcOpenMemHandle`.

Only memory in the VRAM of GPUs that are NVLink-connected can be mapped this way. CPU (DRAM) transfers fall through to the standard memcpy path.

## Comparison with Inter-Node NVLink

| Feature | Intra-Node (`nvlink_intra`) | Inter-Node (`nvlink`) |
|---------|-----------------------------|-----------------------|
| Scope | Single host, multiple GPUs | Multiple hosts (MNNVL fabric) |
| Build flag | `USE_INTRA_NVLINK=ON` | `USE_MNNVL=ON` |
| Memory mechanism | CUDA IPC handles | CUDA Virtual Memory Management + MNNVL fabric export |
| Bandwidth | Full NVLink bandwidth (e.g., 900 GB/s total on H100 SXM) | Full MNNVL bandwidth |
| Hardware needed | Any DGX / server with NVLink switch | MNNVL-capable nodes |

## See Also

- [NVLink Transport (MNNVL / Inter-Node)](nvlink_transport) — cross-node NVLink transfers
- [Supported Protocols](../../getting_started/supported-protocols) — protocol selection guide
- [Transfer Engine Architecture](index)
