# MLU Transport (Cambricon)

Mooncake Transfer Engine supports **Cambricon MLU** (Machine Learning Unit) accelerators through an extension to the standard `RdmaTransport`. There is no separate `mlu` protocol string; MLU support adds MLU-aware memory registration and topology discovery on top of the normal RDMA data path.

## Overview

Cambricon MLU devices expose their on-device memory via the **DMA-BUF** kernel interface, the same mechanism used by NVIDIA GPUDirect RDMA. When `USE_MLU=ON` is enabled, Mooncake:

1. Detects MLU devices during topology discovery.
2. Registers MLU VRAM buffers with the RDMA NIC using DMA-BUF handles.
3. Performs RDMA read/write operations directly to/from MLU VRAM — no intermediate CPU bounce buffer is needed.

The result is a zero-copy transfer path between MLU device memory and remote DRAM / another MLU.

## Hardware Requirements

- Cambricon MLU accelerator (MLU370, MLU590, etc.).
- Cambricon Neuware SDK installed (`neuware` package).
- RDMA-capable NIC (InfiniBand or RoCE) in the same server.
- Linux kernel with DMA-BUF support (5.6+).

## Build

```bash
cmake .. \
    -DUSE_MLU=ON \
    -DUSE_CUDA=OFF \
    -DCMAKE_BUILD_TYPE=RelWithDebInfo
make -j$(nproc)
```

### Environment Variables (build-time)

| Variable | Default | Description |
|----------|---------|-------------|
| `NEUWARE_HOME` | `/usr/local/neuware` | Path to the Neuware installation |
| `NEUWARE_ROOT` | — | Alternative Neuware root (checked if `NEUWARE_HOME` is unset) |

The CMake scripts use these to locate `libcnrt`, `libcndrv`, and the MLU headers.

### Runtime Libraries

`USE_MLU=ON` links the following Neuware libraries into `transfer_engine`:

| Library | Purpose |
|---------|---------|
| `cnrt` | Cambricon Runtime (memory allocation, device management) |
| `cndrv` | Cambricon Driver (low-level device access, DMA-BUF export) |

## Usage

### Protocol String

Use the standard `"rdma"` protocol string. MLU support is transparent at the API level:

```python
from mooncake.engine import TransferEngine

te = TransferEngine()
te.initialize("node1:12345", "P2PHANDSHAKE", "rdma", "mlx5_0")
```

To register MLU memory:

```python
import ctypes

# Allocate MLU memory via cnrt (or use a framework allocator)
mlu_ptr = cnrt_malloc(256 * 1024 * 1024)

# Register with Transfer Engine — specify the MLU device location
te.register_memory(mlu_ptr, 256 * 1024 * 1024, "mlu:0")
```

The location tag `"mlu:N"` tells the topology engine to associate the buffer with MLU device N and select an RDMA NIC that has direct PCIe connectivity to that device.

### Topology Discovery

When `USE_MLU=ON` and MLU devices are present, the Transfer Engine topology module:
- Enumerates all MLU devices and their PCIe BDF addresses.
- Maps each MLU to the nearest RDMA NIC(s) based on PCIe topology.
- Populates the `priority_matrix` with `"mlu:N"` location entries.

This ensures that DMA-BUF transfers use the NIC closest to the MLU, minimising PCIe switch hops.

## Limitations

- MLU support uses the RDMA data path; it does not add a dedicated protocol endpoint.
- Cross-node MLU-to-MLU transfers require an RDMA NIC on each node.
- Intra-node MLU-to-MLU transfers use the RDMA loopback path (not a direct NVLink / PCIe peer copy); direct peer-to-peer MLU copy is not yet implemented in Mooncake.
- `USE_MLU=ON` and `USE_CUDA=ON` can coexist in the same build (the DMA-BUF registration code is independent), but has not been tested extensively in mixed-accelerator environments.

## Troubleshooting

### DMA-BUF export fails

```
cnDrvMemExportToDmaBuf: CNDRV_ERROR_NO_SUPPORT
```

Ensure the Cambricon driver version supports DMA-BUF export (driver ≥ 4.9) and that the kernel DMA-BUF interface is enabled:

```bash
zcat /proc/config.gz | grep DMA_BUF
# CONFIG_DMA_BUF=y
```

### MLU not detected in topology

```bash
cnrt-diagnose   # should list MLU devices
lspci | grep Cambricon
```

Verify that `NEUWARE_HOME` points to the correct Neuware installation.

## See Also

- [Transfer Engine Architecture](index) — RDMA transport and topology-aware path selection
- [Supported Protocols](../../getting_started/supported-protocols) — `rdma` protocol usage with MLU
- [Cambricon Neuware Documentation](https://developer.cambricon.com)
