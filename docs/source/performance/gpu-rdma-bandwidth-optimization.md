# GPUDirect RDMA Bandwidth Optimization Guide

## Issue Overview

When using GPUDirect RDMA for GPU memory transfers, you may observe significantly lower bandwidth (~15 GB/s) compared to CPU RDMA transfers (~47 GB/s) on the same hardware. This performance degradation occurs when PCIe relaxed ordering is not enabled.

## Symptoms

- GPU RDMA transfers: **~15 GB/s** bandwidth
- CPU RDMA transfers: **~47 GB/s** bandwidth (near line rate for 400 Gbps NICs)
- Same hardware configuration (8x 400Gbps NICs, 8x GPUs)
- No errors or warnings in logs

## Root Cause

The performance issue is caused by **missing `IBV_ACCESS_RELAXED_ORDERING` flag** during RDMA memory registration. This flag is critical for optimal PCIe performance when transferring GPU memory over RDMA.

By default, Mooncake Transfer Engine disables relaxed ordering (for backward compatibility). When disabled, RDMA operations use strict PCIe ordering, which significantly limits bandwidth for GPU memory transfers.

## Solution

### Quick Fix

Set the following environment variable **before** starting your application:

```bash
export MC_IB_PCI_RELAXED_ORDERING=1
```

This enables PCIe relaxed ordering when the RDMA driver supports it (specifically, when `ibv_reg_mr_iova2` is available).

### Values

- `0` (default): Disabled - strict PCIe ordering (backward compatible but slower for GPU)
- `1`: Enable if supported - automatically detects driver support  
- `2`: Auto (functionally identical to mode 1) - enable if supported

### Example Usage

#### vLLM with Mooncake Transfer Engine
```bash
export MC_IB_PCI_RELAXED_ORDERING=1
python -m vllm.entrypoints.api_server \
    --model meta-llama/Llama-2-70b-hf \
    --disaggregated-prefill
```

#### SGLang with Mooncake
```bash
export MC_IB_PCI_RELAXED_ORDERING=1
python -m sglang.launch_server \
    --model-path meta-llama/Llama-2-70b-hf \
    --disaggregated-prefill
```

#### Transfer Engine Benchmark
```bash
# Target node
export MC_IB_PCI_RELAXED_ORDERING=1
./transfer_engine_bench --mode=target --auto_discovery --metadata_server=P2PHANDSHAKE

# Initiator node
export MC_IB_PCI_RELAXED_ORDERING=1
./transfer_engine_bench --mode=initiator --auto_discovery \
    --metadata_server=P2PHANDSHAKE --segment_id=target_host:15428
```

## Verification

After enabling relaxed ordering, you should see bandwidth improvements:

### Expected Logs

When relaxed ordering is successfully enabled, you'll see:
```
[RDMA TENT] Relaxed ordering is supported; IBV_ACCESS_RELAXED_ORDERING will be enabled for optimal GPUDirect RDMA performance.
```

If your driver doesn't support it:
```
[RDMA TENT] Relaxed ordering is NOT supported (ibv_reg_mr_iova2 missing). Falling back to strict ordering.
```

### Performance Comparison

| Configuration | GPU RDMA Bandwidth | CPU RDMA Bandwidth |
|--------------|-------------------|-------------------|
| Without `MC_IB_PCI_RELAXED_ORDERING=1` | ~15 GB/s | ~47 GB/s |
| With `MC_IB_PCI_RELAXED_ORDERING=1` | ~47 GB/s | ~47 GB/s |

## Requirements

### Software Requirements

1. **Modern RDMA Driver**: Your RDMA driver must support `ibv_reg_mr_iova2` function
   - **Mellanox OFED**: Version 4.9 or later
   - **inbox drivers**: Recent kernels (5.x+) with updated `rdma-core`

2. **Mooncake Version**: This optimization is available in Mooncake v0.4.0 and later

### Hardware Requirements

- RDMA-capable NICs (InfiniBand, RoCE, eRDMA, etc.)
- NVIDIA GPUs with GPUDirect RDMA support
- Proper PCIe topology (GPUs and NICs on appropriate PCIe switches)

### Verification Commands

Check if your driver supports relaxed ordering:
```bash
# Check for ibv_reg_mr_iova2 symbol
nm -D /usr/lib64/libibverbs.so.1 | grep ibv_reg_mr_iova2

# If found, relaxed ordering is supported
# If not found, you may need to upgrade your RDMA driver
```

Check RDMA driver version:
```bash
# For Mellanox OFED
ofed_info -s

# For inbox drivers
rdma --version
```

## Troubleshooting

### Issue: Still seeing low bandwidth after enabling the flag

**Possible Causes:**
1. Driver doesn't support `ibv_reg_mr_iova2` - check logs for "NOT supported" message
2. PCIe topology issues - ensure GPUs and NICs are on the same PCIe root complex when possible
3. Other bottlenecks - CPU affinity, memory bandwidth, or network congestion

**Solution:**
- Upgrade to latest MLNX_OFED or RDMA driver
- Use `nvidia-smi topo -m` to verify GPU-NIC topology
- Check `MC_LOG_LEVEL=TRACE` for detailed diagnostics

### Issue: Application crashes after enabling relaxed ordering

**Possible Causes:**
Incompatibility with older drivers or buggy implementations

**Solution:**
- Verify driver version meets minimum requirements (MLNX_OFED 4.9+)
- Check `dmesg` for kernel errors
- Note: Modes 1 and 2 are functionally equivalent (both auto-detect support)

### Issue: Different bandwidth on different GPU-NIC pairs

**Possible Causes:**
PCIe topology - some GPU-NIC pairs may cross QPI/UPI links

**Solution:**
- Use Mooncake's topology-aware path selection (enabled by default)
- Manually pin buffers to specific NUMA nodes
- Review PCIe topology with `lstopo` or `nvidia-smi topo -m`

## Technical Background

### Why Relaxed Ordering Matters

PCIe has two ordering modes:

1. **Strict Ordering** (default without flag):
   - All transactions must complete in order
   - High latency for GPU memory access
   - Limited parallelism
   - Results in ~15 GB/s for GPU RDMA

2. **Relaxed Ordering** (with `IBV_ACCESS_RELAXED_ORDERING`):
   - Allows out-of-order completion when safe
   - Lower latency for GPU memory access
   - Better PCIe utilization
   - Achieves ~47 GB/s for GPU RDMA

### When is Relaxed Ordering Safe?

Relaxed ordering is safe for RDMA operations because:
- RDMA verbs API already provides proper synchronization
- Completion semantics ensure data consistency
- Modern RDMA drivers handle ordering correctly

The flag only affects PCIe transaction ordering, not RDMA protocol semantics.

## Alternative Solutions

### Using nvidia-peermem Kernel Module

If you compile Mooncake with `WITH_NVIDIA_PEERMEM=ON`, the traditional path through nvidia-peermem kernel module is used instead of DMA-BUF. This path may have different performance characteristics.

```bash
# Check if nvidia_peermem is loaded
lsmod | grep nvidia_peermem

# If not loaded and available
sudo modprobe nvidia_peermem
```

However, the DMA-BUF path (default when `WITH_NVIDIA_PEERMEM=OFF`) is recommended for newer kernels and CUDA versions.

### Using UCX or NCCL

As mentioned in the issue, UCX and NCCL with UCX backend may also achieve good GPU RDMA performance. However, Mooncake Transfer Engine with relaxed ordering enabled should match or exceed their performance.

## Performance Tuning Tips

Beyond enabling relaxed ordering, consider these optimizations:

1. **Multi-NIC Aggregation**: Use multiple RDMA NICs for bandwidth aggregation
   ```bash
   export MC_SLICE_SIZE=65536  # Adjust for your workload
   ```

2. **NUMA Affinity**: Pin memory and processes to appropriate NUMA nodes
   ```bash
   numactl --membind=0 --cpunodebind=0 your_application
   ```

3. **Topology Awareness**: Mooncake automatically selects optimal GPU-NIC pairs, but you can verify with:
   ```bash
   nvidia-smi topo -m  # Check GPU-NIC affinity
   ```

4. **Parallel Registration**: Enable parallel memory registration for faster startup
   ```bash
   export MC_ENABLE_PARALLEL_REG_MR=1
   ```

## References

- [Mooncake Transfer Engine Documentation](../design/transfer-engine/index.md)
- [Supported Protocols](../getting_started/supported-protocols.md)
- [Troubleshooting Guide](../troubleshooting/troubleshooting.md)
- [Transfer Engine Benchmark Tuning](../design/transfer-engine/transfer-engine-bench-tuning.md)

## Related Issues

- GPUDirect RDMA setup and verification: See [Troubleshooting Guide](../troubleshooting/troubleshooting.md#how-to-make-sure-gpudirect-rdma-gdr-is-supported)
- Low memory registration performance: See environment variable `MC_ENABLE_PARALLEL_REG_MR`
