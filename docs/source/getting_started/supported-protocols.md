# Supported Communication Protocols

Mooncake Transfer Engine supports multiple communication protocols for data transfer between nodes in a cluster. The protocol selection depends on your hardware capabilities and performance requirements.

## Quick Reference

| Protocol | Hardware Required | Use Case | Python API Support |
|----------|-------------------|----------|-------------------|
| **tcp** | Standard network | General purpose, works everywhere | ✅ Primary |
| **rdma** | RDMA-capable NIC | High-performance, low-latency | ✅ Primary |
| **efa** | AWS EFA-capable instance | High-performance on AWS (libfabric SRD) | ✅ Primary |
| **nvmeof** | NVMe-oF capable storage | Direct NVMe storage access | ⚠️ Advanced |
| **nvlink** | NVIDIA MNNVL | Inter-node GPU communication | ⚠️ Advanced |
| **nvlink_intra** | NVIDIA NVLink | Intra-node GPU communication | ⚠️ Advanced |
| **hip** | AMD ROCm/HIP | AMD GPU communication | ⚠️ Advanced |
| **barex** | RDMA-capable NIC | Bare-metal RDMA extension | ⚠️ Advanced |
| **cxl** | CXL-capable hardware | Memory pooling and sharing | ⚠️ Advanced |
| **ascend** | Huawei Ascend NPU | Ascend NPU communication | ⚠️ Advanced |

## Commonly Used Protocols (Python API)

### TCP (Default)

**Description:** Standard TCP/IP network protocol.

**Use When:**
- No special hardware is available
- Testing or development environments
- Compatibility is more important than performance

**Configuration:**
```python
# Python API
engine.initialize(
    hostname="localhost",
    metadata_server="P2PHANDSHAKE",
    protocol="tcp",  # No device_name needed
    device_name=""
)
```

```bash
# Environment variables
export MOONCAKE_PROTOCOL="tcp"
```

**Advantages:**
- Works in all environments
- No special hardware required
- Simple setup

**Limitations:**
- Lower throughput compared to RDMA
- Higher CPU overhead
- Higher latency

### RDMA (Recommended for Production)

**Description:** Remote Direct Memory Access protocol providing high-performance, low-latency data transfer with minimal CPU overhead. Supports GPUDirect RDMA for zero-copy GPU memory transfers.

**Hardware Support:**
- InfiniBand
- RoCE (RDMA over Converged Ethernet)
- eRDMA (Elastic RDMA)
- NVIDIA GPUDirect RDMA
- Non-NVIDAI GPUDirect RDMA (e.g., Intel E810 RDMA NIC)

**Use When:**
- High-performance networking is required
- RDMA-capable NICs are available
- Low latency is critical (e.g., distributed inference, KV cache transfer)

**Note:** If no RDMA HCA (Host Channel Adapter) is detected on the system, the Transfer Engine will automatically fall back to TCP protocol for compatibility.

**Configuration:**
```python
# Python API - With specific device
engine.initialize(
    hostname="node1",
    metadata_server="etcd://10.0.0.1:2379",
    protocol="rdma",
    device_name="mlx5_0"  # Specify your RDMA device
)

# Python API - With auto-discovery
engine.initialize(
    hostname="node1",
    metadata_server="P2PHANDSHAKE",
    protocol="rdma",
    device_name="auto-discovery"  # Automatically detect optimal device
)
```

```bash
# Environment variables
export MOONCAKE_PROTOCOL="rdma"
export MOONCAKE_DEVICE="mlx5_0"  # or "auto-discovery"
```

**Device Discovery:**
To find available RDMA devices on your system:
```bash
ibv_devices  # List InfiniBand/RDMA devices
# Example output: mlx5_0, mlx5_1, erdma_0, etc.
```

**Advantages:**
- Very high throughput (up to 200 Gbps per NIC)
- Ultra-low latency (sub-microsecond)
- Minimal CPU overhead
- Supports GPUDirect RDMA for zero-copy GPU transfers
- Multi-NIC bandwidth aggregation
- Topology-aware path selection

**Limitations:**
- Requires RDMA-capable hardware
- May require elevated permissions (sudo)
- More complex network configuration

**Performance Tips:**
- Use multiple RDMA NICs for bandwidth aggregation
- Enable GPUDirect RDMA for GPU memory transfers
- Configure proper NUMA affinity for optimal performance
- See [Transfer Engine Benchmark Tuning](../design/transfer-engine/transfer-engine-bench-tuning.md) for detailed optimization

### EFA (AWS Elastic Fabric Adapter)

**Description:** AWS EFA transport using libfabric's Scalable Reliable Datagram (SRD) protocol, providing high-bandwidth RDMA-like performance on AWS instances without traditional RDMA support.

**Use When:**
- Running on AWS EFA-enabled instances (e.g., p5e.48xlarge, p6-b200.48xlarge, p4d.24xlarge)
- High-performance networking is required on AWS
- Traditional RDMA (ibverbs QP) is not supported by the hardware

**Configuration:**
```python
# Python API
engine.initialize(
    hostname="localhost",
    metadata_server="P2PHANDSHAKE",
    protocol="efa",
    device_name=""
)
```

**Build Requirements:**
```bash
cmake .. -DUSE_EFA=ON -DUSE_CUDA=ON
```

> **Note:** `-DUSE_CUDA=ON` is required when transferring GPU memory. Without it, fallback to TCP protocol will fail with "Bad address" errors on GPU buffers.

**Advantages:**
- High throughput (~170 GB/s with 8 EFA devices, tuned)
- Bypasses kernel network stack
- Available on all AWS EFA-enabled instances

**Limitations:**
- AWS-only
- Software-emulated RDMA writes (higher CPU overhead than true RDMA)
- ~88% of RoCE RDMA throughput

**Documentation:** See [EFA Transport](../design/transfer-engine/efa_transport.md) for build instructions, benchmarks, and tuning.

## Advanced Protocols (C++ Transfer Engine)

The following protocols are available at the C++ Transfer Engine level for specialized use cases. They are not commonly used through the Python API.

### NVMe over Fabric (nvmeof)

**Description:** Direct data transfer between NVMe storage and DRAM/VRAM using GPUDirect Storage, bypassing the CPU for zero-copy operations.

**Use When:**
- Direct NVMe storage access is needed
- Implementing multi-tier storage (DRAM/VRAM/NVMe)
- Working with large datasets that don't fit in memory

**Requirements:**
- NVMe-oF capable storage
- Properly mounted remote storage nodes

### NVLink (nvlink)

**Description:** NVIDIA MNNVL (Multi-Node NVLink) protocol for high-bandwidth, low-latency GPU-to-GPU communication across nodes.

**Use When:**
- Inter-node GPU communication is required
- Using NVIDIA MNNVL (Multi-Node NVLink)
- Maximum GPU bandwidth is needed

**Requirements:**
- NVIDIA MNNVL hardware
- Compiled with `USE_MNNVL=ON`

**Configuration:**
```bash
# Set MC_FORCE_MNNVL=true to use MNNVL even when RDMA NICs are present
export MC_FORCE_MNNVL=true
```

**Note:** When `protocol="rdma"` is set and RDMA NICs exist, you must explicitly set `MC_FORCE_MNNVL=true` to use MNNVL instead of RDMA. If no RDMA HCA is detected, MNNVL will be used automatically.

### Intra-Node NVLink (nvlink_intra)

**Description:** NVIDIA NVLink for GPU-to-GPU communication within a single node.

**Use When:**
- Local GPU-to-GPU transfers are needed
- Maximizing intra-node GPU bandwidth

**Requirements:**
- NVIDIA NVLink hardware
- Compiled with `USE_INTRA_NVLINK=ON`

### HIP Transport (hip)

**Description:** AMD ROCm/HIP transport for GPU communication using IPC handles or Shareable handles.

**Use When:**
- Working with AMD GPUs
- Need intra-node GPU communication on AMD hardware

**Requirements:**
- AMD ROCm/HIP runtime
- AMD GPUs

### Barex Transport (barex)

**Description:** Bare-metal RDMA extension protocol for specialized RDMA configurations.

**Use When:**
- Advanced RDMA features are required
- Custom RDMA configurations

**Requirements:**
- RDMA-capable hardware
- Specialized configuration

### CXL Transport (cxl)

**Description:** Compute Express Link for memory pooling and sharing across devices.

**Use When:**
- CXL memory pooling is available
- Memory disaggregation is needed

**Requirements:**
- CXL-capable hardware

### Ascend Transport (ascend)

**Description:** Huawei Ascend NPU communication using HCCL (Huawei Collective Communication Library) or direct transport.

**Use When:**
- Working with Huawei Ascend NPUs
- Distributed inference on Ascend hardware

**Requirements:**
- Huawei Ascend NPU hardware
- HCCL runtime

**Documentation:**
- [Heterogeneous Ascend](../design/transfer-engine/heterogeneous_ascend.md)
- [Ascend Transport](../design/transfer-engine/ascend_transport.md)

## Configuration Examples

### Configuration File (JSON)

**TCP Configuration:**
```json
{
    "local_hostname": "localhost",
    "metadata_server": "localhost:8080",
    "protocol": "tcp",
    "device_name": "",
    "master_server_address": "localhost:8081"
}
```

**RDMA Configuration:**
```json
{
    "local_hostname": "node1",
    "metadata_server": "etcd://10.0.0.1:2379",
    "global_segment_size": "3GB",
    "local_buffer_size": "1GB",
    "protocol": "rdma",
    "device_name": "mlx5_0",
    "master_server_address": "10.0.0.1:8081"
}
```

### Environment Variables

```bash
# TCP (Default)
export MOONCAKE_PROTOCOL="tcp"

# RDMA with specific device
export MOONCAKE_PROTOCOL="rdma"
export MOONCAKE_DEVICE="mlx5_0"

# RDMA with auto-discovery
export MOONCAKE_PROTOCOL="rdma"
export MOONCAKE_DEVICE="auto-discovery"

# Other configuration
export MOONCAKE_MASTER="10.0.0.1:50051"
export MOONCAKE_TE_META_DATA_SERVER="P2PHANDSHAKE"
export MOONCAKE_LOCAL_HOSTNAME="node1"
```

## Choosing the Right Protocol

| Scenario | Recommended Protocol | Notes |
|----------|---------------------|-------|
| Development/Testing | tcp | Simple setup, no special hardware |
| Production Inference | rdma | Best performance and latency |
| AWS Cloud (EFA instances) | efa | High performance on p5e, p6-b200, p4d, etc. |
| Cloud Environments | tcp or rdma (if available) | Check cloud provider support |
| Multi-tier Storage | rdma + nvmeof | Combine protocols for different layers |
| AMD GPU Clusters | rdma + hip | Use HIP for local GPU communication |
| Ascend NPU Clusters | rdma + ascend | Use Ascend for NPU-specific operations |

## Troubleshooting

### RDMA Connection Issues

1. **Check RDMA devices:**
   ```bash
   ibv_devices
   ibv_devinfo
   ```

2. **Verify network connectivity:**
   ```bash
   # Test RDMA connectivity (requires rdma-core tools)
   rping -s  # On server
   rping -c -a <server_ip> -v  # On client
   ```

3. **Check permissions:**
   - RDMA may require elevated permissions
   - Run with `sudo` if necessary
   - Configure proper udev rules for non-root access

4. **Firewall configuration:**
   - Ensure RDMA ports are not blocked
   - Check InfiniBand subnet manager is running

### Protocol Selection

If a protocol fails to initialize:
1. Verify hardware support
2. Check that required drivers are installed
3. Ensure compile-time flags are set correctly (for C++ protocols)
4. Fall back to TCP for basic functionality

## See Also

- [Quick Start Guide](quick-start.md) - Getting started with Mooncake
- [Transfer Engine Design](../design/transfer-engine/index.md) - Detailed architecture
- [Transfer Engine Benchmark](../design/transfer-engine/transfer-engine-bench-tuning.md) - Performance tuning
- [Python API Reference](../python-api-reference/transfer-engine.md) - API documentation
- [Deployment Guide](../deployment/mooncake-store-deployment-guide.md) - Production deployment
