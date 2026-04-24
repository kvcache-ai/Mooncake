# CXL Transport

The CXL transport (`CxlTransport`) enables memory transfers over **Compute Express Link (CXL)**, a high-speed interconnect that allows CPUs and accelerators to share a unified memory pool across the PCIe bus.

## Overview

CXL memory pooling disaggregates physical DRAM from compute nodes: a CXL memory expander can be mounted into the address space of one or more host CPUs, making remote memory appear as local DRAM. `CxlTransport` leverages this to move data between CXL-attached memory regions across nodes without involving a network NIC.

Typical use cases include:
- **Memory disaggregation**: Offload KV cache tensors to a large, shared CXL memory pool that multiple inference nodes can access.
- **Bandwidth aggregation**: Use CXL memory expanders to increase the total memory bandwidth available to a single node.

## Hardware Requirements

- CXL 2.0 or later capable host CPU (e.g., Intel Xeon Scalable 4th Gen / AMD EPYC 9004).
- CXL memory expander (e.g., Samsung CMM-D, Micron CZ120, Ayar Labs).
- CXL device must be mounted and visible as a NUMA node (verify with `numactl -H`).

## Build

```bash
cmake .. \
    -DUSE_CXL=ON \
    -DCMAKE_BUILD_TYPE=RelWithDebInfo
make -j$(nproc)
```

`USE_CXL` compiles `CxlTransport` into the `transfer_engine` library. No additional runtime library is required beyond the standard Linux kernel CXL driver (enabled by default in Linux 5.15+).

## Usage

### Protocol String

Use `"cxl"` as the protocol string:

```python
from mooncake.engine import TransferEngine

te = TransferEngine()
te.initialize("localhost:12345", "P2PHANDSHAKE", "cxl", "")
```

### Memory Registration

CXL memory regions are registered with a `cpu:N` location tag where `N` is the NUMA node corresponding to the CXL device:

```bash
# Find the CXL memory NUMA node
numactl -H | grep -A5 "node distances"
```

```python
import ctypes

# Allocate on CXL NUMA node (e.g., node 2)
buf = ctypes.create_string_buffer(256 * 1024 * 1024)
te.register_memory(ctypes.addressof(buf), len(buf), "cpu:2")
```

### Base Address

`CxlTransport` exposes a `getCxlBaseAddr()` method that returns the virtual base address of the mapped CXL region. This is useful when constructing scatter-gather lists that reference CXL memory directly.

### CXL Segment Allocation

`CxlTransport` maintains an internal segment table with a fixed number of local segments (controlled by `allocateLocalSegmentID()`). Each registered CXL buffer occupies one slot. The segment limit is set at compile time; contact the Mooncake team if you need to increase it.

## Limitations

- CXL memory regions must be on the same host or connected through a CXL switch; this transport does not use a network NIC.
- CXL 1.1 devices (non-pooled) appear as standard DRAM NUMA nodes — no special transport is needed for them.
- CXL 2.0 pooling (shared memory across multiple hosts) requires a CXL switch and OS-level support (Linux 6.6+).

## Troubleshooting

### CXL device not visible as NUMA node

```bash
ls /sys/bus/cxl/devices/
daxctl list
```

If the device appears as a DAX device but not a NUMA node, use `daxctl reconfigure-device` to online it as system RAM.

### Permission denied on CXL device file

```bash
sudo chmod 660 /dev/dax0.0
sudo chown root:$(id -gn) /dev/dax0.0
```

## See Also

- [Transfer Engine Architecture](index)
- [Supported Protocols](../../getting_started/supported-protocols)
- [SSD Offload](../../deployment/ssd-offload) — multi-tier storage with NVMe
- [Multi-Tier Storage](../../deployment/multi-tier-storage) — G1/G2/G3 tier configuration
