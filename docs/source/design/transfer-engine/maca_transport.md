# MACA Transport (MetaX / Muxi)

Mooncake Transfer Engine supports **MetaX (Muxi) MACA** GPU accelerators. MACA (MetaX Architecture for Computing Acceleration) is the GPU compute platform from MetaX Integrated Circuits.

## Overview

MACA support in Mooncake mirrors the CUDA code path: memory allocations on MetaX GPUs are registered with the Transfer Engine and transferred via the RDMA NIC that is closest to the device in the PCIe topology. MACA-specific runtime libraries replace the CUDA runtime.

## Hardware Requirements

- MetaX (Muxi) GPU accelerator.
- MACA runtime installed at `MACA_HOME` (default `/opt/maca`).
- RDMA-capable NIC in the same server.

## Build

```bash
cmake .. \
    -DUSE_MACA=ON \
    -DCMAKE_BUILD_TYPE=RelWithDebInfo
make -j$(nproc)
```

### Environment Variables (build-time)

| Variable | Default | Description |
|----------|---------|-------------|
| `MACA_HOME` | `/opt/maca` | Root of the MACA installation (headers and libraries) |

CMake resolves `MACA_INCLUDE_DIR` as `${MACA_HOME}/include`.

### Runtime Libraries

By default, `USE_MACA=ON` links:

```cmake
mcruntime      # MACA compute runtime
mxc-runtime64  # MetaX cross-platform runtime
rt             # POSIX real-time library
```

Override the library list at configure time if your MACA installation uses different names:

```bash
cmake .. \
    -DUSE_MACA=ON \
    -DMACA_RUNTIME_LIBS="mcruntime;mxc-runtime64;rt"
```

## Usage

### Protocol String

Use `"rdma"` as the protocol string. MACA support extends the RDMA path with MACA-aware memory registration:

```python
from mooncake.engine import TransferEngine

te = TransferEngine()
te.initialize("node1:12345", "P2PHANDSHAKE", "rdma", "mlx5_0")
```

Register MACA GPU memory:

```python
# Allocate MACA GPU memory via mcMalloc (or framework allocator)
maca_ptr = mc_malloc(256 * 1024 * 1024)

# Register with Transfer Engine using "cuda:N" location tag
# (MACA devices follow the same device-numbering convention as CUDA)
te.register_memory(maca_ptr, 256 * 1024 * 1024, "cuda:0")
```

### Topology Discovery

When `USE_MACA=ON`, the topology discovery module enumerates MetaX GPUs via the MACA runtime and maps them to their nearest RDMA NIC(s). The resulting `priority_matrix` entries use `"cuda:N"` location tags (shared convention with CUDA devices).

## Environment Variables (run-time)

| Variable | Default | Description |
|----------|---------|-------------|
| `MC_MS_AUTO_DISC` | `1` | Auto-discover MACA GPU topology. Set `0` to disable. |
| `MC_MS_FILTERS` | — | Comma-separated NIC whitelist |
| `MACA_HOME` | `/opt/maca` | Used at runtime if the MACA shared libraries are not on `LD_LIBRARY_PATH` |

Ensure the MACA runtime libraries are on `LD_LIBRARY_PATH`:

```bash
export LD_LIBRARY_PATH=$MACA_HOME/lib:$LD_LIBRARY_PATH
```

## Build with Both MACA and Other Features

`USE_MACA=ON` can be combined with other flags such as `USE_EFA`, `USE_CXL`, or `USE_BAREX`. It cannot be combined with `USE_CUDA=ON` (both define the same CUDA-like runtime aliases).

## Limitations

- Cross-node MACA GPU transfers require an RDMA NIC; direct MACA peer-to-peer transfers between nodes are not supported.
- MACA and CUDA cannot be enabled in the same build.

## Troubleshooting

### MACA runtime library not found

```
error while loading shared libraries: libmcruntime.so
```

Add the MACA library directory to `LD_LIBRARY_PATH`:

```bash
export LD_LIBRARY_PATH=/opt/maca/lib:$LD_LIBRARY_PATH
```

### Incorrect `MACA_HOME`

```
CMake Error: MACA_INCLUDE_DIR not found
```

Set `MACA_HOME` explicitly:

```bash
cmake .. -DUSE_MACA=ON -DMACA_HOME=/path/to/maca
```

## See Also

- [Transfer Engine Architecture](index) — RDMA transport and topology-aware path selection
- [Supported Protocols](../../getting_started/supported-protocols)
- [Build Guide](../../getting_started/build) — `USE_MACA` flag in the build matrix
