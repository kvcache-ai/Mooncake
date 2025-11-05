# Heterogeneous Ascend Transport

## Overview
Heterogeneous Ascend Transport is a high-performance data transmission library designed for heterogeneous inference scenarios, specifically optimized to leverage the advantages of heterogeneous computing architectures. Key features and use cases include:

- **910B Device**: Executes PREFILL operations for efficient inference computation
- **H20 Device**: Handles DECODE operations for data decoding
- **Heterogeneous Ascend Transport**: Manages cross-device KVCACHE transmission, enabling efficient data exchange between 910B NPU memory and H20 GPU memory

> Current version only supports WRITE semantics. READ semantics will be implemented in future releases.

## Enhanced HBM-to-DRAM Data Transfer Optimization
The copy bandwidth from HBM to DRAM is constrained by the size of data blocks. Small data blocks smaller than 2MB result in underutilized bandwidth. We have implemented an optimization using "data aggregation + pipeline parallelism": first, small data blocks are aggregated into 8MB blocks within HBM before being transferred to DRAM, while data copying and RDMA transmission are executed in parallel. This solution effectively hides the HBM-DRAM copy latency and significantly reduces the overall transmission time.

## Build Instructions
The `USE_ASCEND_HETEROGENEOUS` compilation option has been added to `mooncake-common/common.cmake` to control this feature:

- **PREFILL Side (910B)**:
  - Enable `USE_ASCEND_HETEROGENEOUS` option
  - Recompile Mooncake project

- **DECODE Side (H20)**:
  - Use Mooncake RDMA Transport directly
  - Enable GPU Direct functionality (supports direct GPU memory access via RDMA)

## Parameter Configuration
The following key parameters are required:

| Parameter | Description |
|-----------|-------------|
| `source` | Source address (910B NPU memory address) |
| `target_offset` | Target address offset (H20 GPU memory address) |
| `opcode` | Operation code (currently only `WRITE` supported) |

## Testing Guide

### Test Cases
Available test programs:
- **Initiator (910B)**: `transfer_engine_heterogeneous_ascend_perf_initiator.cpp`
- **Target (H20)**: `rdma_transport_test.cpp` (reusing existing test)

After compilation, test programs are located at:
```
build/mooncake-transfer-engine/example/
build/mooncake-transfer-engine/test/
```

Test programs can be configured via `DEFINE_string` parameters. Refer to the parameter list at the beginning of each test file.

### Test Procedure
When `metadata_server=P2PHANDSHAKE` is configured, the system automatically selects available ports to avoid conflicts. Testing steps:

1. **Start Target Node**
   - Check target node logs (in `transfer_engine.cpp`)
   - Record listening information in this format:
     ```
     Transfer Engine RPC using <protocol> listening on <IP>:<port>
     ```

2. **Configure Initiator Node**
   - Set `--segment_id` parameter to the actual listening address recorded in step 1 (format: `<IP>:<port>`)

3. **Start Initiator Node** to complete connection test

### Example Commands
**Target (H20)**:
```bash
./rdma_transport_test --mode=target --local_server_name=10.10.10.10 --metadata_server=P2PHANDSHAKE --operation=write --protocol=rdma --device_name=mlx5_1 --use_vram=true --gpu_id=0
```

**Initiator (910B)**:
```bash
./transfer_engine_heterogeneous_ascend_perf_initiator --mode=initiator --local_server_name=10.10.10.10 --metadata_server=P2PHANDSHAKE --operation=write --npu_id=1 --segment_id=10.10.10.10:12345 --device_name=mlx5_1 --block_size=65536 --batch_size=128
```