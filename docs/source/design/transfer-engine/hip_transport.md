# HIP Transport (AMD ROCm)

The HIP transport (`HipTransport`) enables GPU-to-GPU data transfers on **AMD ROCm** platforms, using either IPC handles or Shareable handles for intra-node transfers between AMD GPUs.

## Overview

`HipTransport` is the AMD equivalent of the CUDA-based transfer paths. It is designed for **intra-node** GPU communication: moving data between AMD GPU VRAM buffers on the same host without routing through system memory or a network NIC.

The transport uses the HIP runtime (`hip_runtime.h`) and supports two handle types for mapping peer GPU memory:

| Handle Type | Use Case |
|-------------|----------|
| **IPC handle** (`hipIpcMemHandle_t`) | Mapping GPU memory from another process on the same host |
| **Shareable handle** | Used when IPC handles are unavailable or for cross-device mappings |

## Hardware Requirements

- AMD GPU with ROCm 5.0 or later (e.g., MI200 / MI300 series).
- Peer access must be supported between the source and destination GPUs (`hipDeviceCanAccessPeer`).

## Build

```bash
cmake .. \
    -DUSE_HIP=ON \
    -DCMAKE_BUILD_TYPE=RelWithDebInfo
make -j$(nproc)
```

The `USE_HIP` flag enables HIPification of the Transfer Engine sources (`hipify_files`) and links `hip::host` and the ROCm runtime library.

> **Note:** `USE_HIP` and `USE_CUDA` are mutually exclusive. Do not enable both in the same build.

## Usage

### Protocol String

Use `"hip"` as the protocol string:

```python
from mooncake.engine import TransferEngine

te = TransferEngine()
te.initialize("localhost:12345", "P2PHANDSHAKE", "hip", "")
```

### Memory Registration

Register AMD GPU memory using the standard `TransferEngine::registerLocalMemory` API. Specify the location as `"cuda:N"` (the HIP transport reuses the same location tag convention as the CUDA transport, since HIP mirrors the CUDA device numbering):

```python
import ctypes, mooncake

te = TransferEngine()
te.initialize("localhost:12345", "P2PHANDSHAKE", "hip", "")

# Allocate 256 MB on GPU 0 (HIP device 0)
buf = mooncake.allocate_gpu_memory(256 * 1024 * 1024, device=0)
te.register_memory(buf, 256 * 1024 * 1024, "cuda:0")
```

### Environment Variables

`HipTransport` respects the same topology and configuration environment variables as the RDMA transport:

| Variable | Default | Description |
|----------|---------|-------------|
| `MC_MS_AUTO_DISC` | `1` | Auto-discover GPU topology |
| `MC_MS_FILTERS` | — | NIC / device whitelist |

## Limitations

- Intra-node only: `HipTransport` does not support transfers across network links. Combine with `"rdma"` (via the standard RDMA transport) for cross-node transfers in a heterogeneous setup.
- IPC handles require both processes to be on the same OS instance and the same AMD GPU driver version.
- Inter-GPU peer access must be enabled at the OS / driver level.

## See Also

- [Supported Protocols](../../getting_started/supported-protocols) — protocol selection guide
- [Transfer Engine Architecture](index)
- [ROCm Documentation](https://rocm.docs.amd.com/)
