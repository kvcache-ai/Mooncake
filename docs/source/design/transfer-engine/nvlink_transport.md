# NVLink Transport (MNNVL / Inter-Node)

The NVLink transport (`NvlinkTransport`) enables high-bandwidth, low-latency GPU-to-GPU data transfers across nodes using **NVIDIA Multi-Node NVLink (MNNVL)**. It bypasses the PCIe bus and the network stack entirely, delivering bandwidth that approaches the raw NVLink fabric speed.

## Overview

MNNVL is available on NVIDIA platforms that physically link GPUs across multiple nodes via NVLink cables (e.g., DGX SuperPOD / DGX GB200 NVL72). Within such a fabric, all GPUs share a unified address space exposed through the NVLink Allocator, making remote GPU memory directly addressable from any node in the fabric.

`NvlinkTransport` registers GPU memory into the NVLink fabric via `cuMemCreate` / `cuMemMap` (CUDA Virtual Memory Management) and performs transfers through direct remote memory writes — no RDMA NIC is involved.

## Hardware Requirements

- NVIDIA MNNVL-capable hardware (e.g., H100 / H200 / B200 in a NVL72 configuration).
- CUDA 12.0+ with NVLink fabric support (`cuMemGetAllocationGranularity` with `CU_MEM_ALLOC_GRANULARITY_RECOMMENDED`).
- NVLink fabric initialised by the system firmware before the application starts.

## Build

Enable MNNVL support at CMake configure time:

```bash
cmake .. \
    -DUSE_MNNVL=ON \
    -DUSE_CUDA=ON \
    -DCMAKE_BUILD_TYPE=RelWithDebInfo
make -j$(nproc)
```

The `USE_MNNVL` flag compiles `NvlinkTransport` into the `transfer_engine` library.

## Usage

### Protocol String

Use `"nvlink"` as the protocol string when initialising the Transfer Engine:

```python
from mooncake.engine import TransferEngine

te = TransferEngine()
te.initialize("node1:12345", "P2PHANDSHAKE", "nvlink", "")
```

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `MC_FORCE_MNNVL` | `false` | Force NVLink even when RDMA NICs are present. When RDMA HCAs are detected and `MC_FORCE_MNNVL` is not set, the engine prefers RDMA. Set `MC_FORCE_MNNVL=true` to override. |

### When to Use

- Use `"nvlink"` only when the deployment hardware has MNNVL connectivity and you want maximum GPU-to-GPU bandwidth between nodes.
- On clusters without MNNVL hardware, fall back to `"rdma"` or `"tcp"`.
- For **intra-node** NVLink transfers (within a single host), use `"nvlink_intra"` instead (see [Intra-Node NVLink Transport](nvlink_intra_transport)).

## Memory Registration

`NvlinkTransport` uses a custom **NVLink Allocator** that:
1. Allocates GPU memory via CUDA Virtual Memory Management (`cuMemCreate`).
2. Exports the allocation handle through the NVLink fabric (`cuMulticastAddMemory` / `cuMemExportToShareableHandle`).
3. Registers the allocation with the Transfer Engine metadata service so remote peers can map it.

All memory registered through `TransferEngine::registerLocalMemory` with a `cuda:N` location tag is automatically handled by the NVLink Allocator when `NvlinkTransport` is active.

## Tuning Tips

- **Buffer alignment**: Allocate buffers in multiples of the NVLink granularity returned by `cuMemGetAllocationGranularity`. Misaligned allocations will be rounded up internally.
- **Pinning**: Keep GPU buffers pinned for the lifetime of a Transfer Engine session; frequent re-registration degrades performance.
- **Topology**: MNNVL works best when the NVLink fabric is fully populated. Partial fabric configurations will fall back to slower paths for non-adjacent GPUs.

## Test

```bash
./build/mooncake-transfer-engine/tests/nvlink_transport_test
```

The test suite requires MNNVL hardware. On systems without MNNVL, the test will skip automatically.

## See Also

- [Intra-Node NVLink Transport](nvlink_intra_transport) — within-node NVLink transfers
- [Supported Protocols](../../getting_started/supported-protocols) — protocol selection guide
- [Transfer Engine Architecture](index)
