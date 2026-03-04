# Ascend Direct Transport

Ascend Direct Transport (referred to as Transport throughout this document) is located at `Mooncake/mooncake-transfer-engine/src/transport/ascend_transport/ascend_direct_transport`.

## Overview

Transport is a transmission adapter layer built on CANN's ADXL capabilities, providing direct compatibility with the Mooncake Transfer Engine. With Transport, users can perform Host-to-Device, Device-to-Host, and Device-to-Device transfers in Ascend environments using various communication protocols such as HCCS and RDMA. Future enhancements will enable data transfer between heterogeneous Ascend and GPU environments.

To build and use the Transport library, please follow the process described in `build.md` and enable the `USE_ASCEND_DIRECT` option.

### Additional Dependencies

**Ascend Compute Architecture for Neural Networks (CANN)**

Please install the latest version of Ascend Compute Architecture for Neural Networks.

### Test Cases

Transport provides a performance test file at `mooncake-transfer-engine/example/transfer_engine_ascend_direct_perf.cpp`.

When the `metadata_server` is configured as `P2PHANDSHAKE`, Mooncake randomly selects listening ports from the new RPC port mapping to avoid port conflicts. Therefore, testing requires the following steps:

1. Start the target node first and observe the logs printed in `mooncake-transfer-engine/src/transfer_engine.cpp`. Look for a statement in the following format:
   ```
   Transfer Engine RPC using <protocol> listening on <IP>:<actual_port>
   ```
   Record the actual listening port number of the target node.

2. Modify the startup command for the initiator node: Change the `--segment_id` parameter value to the target node's IP + actual listening port number (format: `<IP>:<port>`).

3. Start the initiator node to complete the connection test.

Complete command format is shown below:

**Start target node:**
```shell
./transfer_engine_ascend_direct_perf --metadata_server=P2PHANDSHAKE --local_server_name=127.0.0.1:12345 --operation=write --device_logicid=0 --mode=target --block_size=16384 --batch_size=32 --block_iteration=10
```

**Start initiator node:**
```shell
./transfer_engine_ascend_direct_perf --metadata_server=P2PHANDSHAKE --local_server_name=127.0.0.1:12346 --operation=write --device_logicid=1 --mode=initiator --block_size=16384 --batch_size=32 --block_iteration=10  --segment_id=127.0.0.1:real_port
```

### Environment Variables Configuration

The following environment variables can be configured to control Ascend Direct Transport behavior:

| Variable | Description | Default Value | Example |
|----------|-------------|---------------|---------|
| `ASCEND_AUTO_CONNECT` | Enable automatic connection management | 0 (disabled) | `ASCEND_AUTO_CONNECT=1` |
| `ASCEND_ENABLE_USE_FABRIC_MEM` | Enable fabric memory transfer mode in Mooncake Store (A3 only) | 0 (disabled) | `ASCEND_ENABLE_USE_FABRIC_MEM=1` |
| `ASCEND_USE_ASYNC_TRANSFER` | Enable asynchronous transfer mode | 0 (disabled) | `ASCEND_USE_ASYNC_TRANSFER=1` |
| `ASCEND_GLOBAL_RESOURCE_CONFIG` | Global resource configuration | - | `ASCEND_GLOBAL_RESOURCE_CONFIG="{\"fabric_memory.max_capacity\":32}"` |
| `ASCEND_CONNECT_TIMEOUT` | Link establishment timeout in milliseconds | 3000 | `ASCEND_CONNECT_TIMEOUT=5000` |
| `ASCEND_TRANSFER_TIMEOUT` | Data transfer timeout in milliseconds | 3000 | `ASCEND_TRANSFER_TIMEOUT=10000` |
| `ASCEND_THREAD_POOL_SIZE` | Number of worker threads in the transfer thread pool | 8 (1 for buffer pool mode) | `ASCEND_THREAD_POOL_SIZE=16` |
| `ASCEND_USE_SHORT_CONNECTION` | Enable short connection mode (disconnect after each transfer) | 0 (disabled) | `ASCEND_USE_SHORT_CONNECTION=1` |
| `ASCEND_BUFFER_POOL` | Buffer pool configuration for intermediate transfer mode (BUFFER_NUM:BUFFER_SIZE_MB) | "0:0" (disabled) | `ASCEND_BUFFER_POOL=4:8` |
| `ASCEND_BASE_PORT` | Base port for ADXL engine port allocation | 11000 | `ASCEND_BASE_PORT=20000` |
| `HCCL_INTRA_ROCE_ENABLE` | Enable RDMA protocol for intra-node communication | 0 (disabled) | `HCCL_INTRA_ROCE_ENABLE=1` |
| `HCCL_RDMA_TIMEOUT` | RDMA packet retransmission timeout coefficient | - | `HCCL_RDMA_TIMEOUT=14` |
| `HCCL_RDMA_RETRY_CNT` | RDMA packet retransmission count | - | `HCCL_RDMA_RETRY_CNT=7` |

**Detailed Descriptions:**

- **ASCEND_AUTO_CONNECT**: Requires CANN 9.0 or later. Default is 0, recommended to enable on supported versions: link can be automatically disconnected when the remote end goes offline abnormally.
- **ASCEND_ENABLE_USE_FABRIC_MEM**: Requires CANN 9.0+ and HDK 26.0+. Recommended when using Mooncake Store on supported A3 platforms: it can significantly improve transmission performance.
- **ASCEND_USE_ASYNC_TRANSFER**: Requires CANN 8.5+. Enables HIXL asynchronous transfer mode, defaults to synchronous mode.
- **ASCEND_GLOBAL_RESOURCE_CONFIG**: Configures HIXL global resources. Refer to HIXL documentation for `OPTION_GLOBAL_RESOURCE_CONFIG` settings.

### Important Notes

1. **Device Setup Required**: Before calling `TransferEngine.initialize()`, you must set the device (e.g., `torch.npu.set_device(0)`).

2. **TCP Connection**: Transport internally establishes a host-side TCP connection and automatically searches for available ports. An error will be reported if no ports are available.

3. **Memory Alignment for HCCS**: When using the `HCCS` communication protocol, registered memory addresses must be aligned with page tables (device memory: 2MB alignment).

4. **Configuration File**: Ensure `/etc/hccn.conf` exists, especially in containers. Mount `/etc/hccn.conf` or copy the host file to the container's `/etc` path.

5. **Timeout Configuration**: 
   - Use the `ASCEND_CONNECT_TIMEOUT` environment variable to control link establishment timeout (default: 3 seconds)
   - Use the `ASCEND_TRANSFER_TIMEOUT` environment variable to control data transfer timeout (default: 3 seconds)

6. **RDMA Timeout and Retry Configuration**: 
   - Use `HCCL_RDMA_TIMEOUT` to configure the RDMA NIC packet retransmission timeout coefficient. The actual packet retransmission timeout is `4.096us * 2 ^ $HCCL_RDMA_TIMEOUT`
   - Use `HCCL_RDMA_RETRY_CNT` to configure the RDMA NIC retransmission count
   - It is recommended to configure `ASCEND_TRANSFER_TIMEOUT` to be slightly larger than `retransmission_timeout * HCCL_RDMA_RETRY_CNT`

7. **Communication Protocol Selection**: Within A2 servers/A3 supernodes, the default communication protocol is HCCS. You can specify RDMA by setting `export HCCL_INTRA_ROCE_ENABLE=1`.

8. **RDMA Configuration**: When using the `RDMA` communication protocol, in scenarios where switch and NIC default configurations are inconsistent or traffic planning is required, you may need to modify the RDMA NIC's Traffic Class and Service Level configuration:
   - Set Traffic Class using the `ASCEND_RDMA_TC` environment variable
   - Set Service Level using the `ASCEND_RDMA_SL` environment variable

9. **Buffer Pool Configuration**: Under constrained scenarios, transmission can be achieved by using intermediate buffers. The specific way to enable this is by configuring the ASCEND_BUFFER_POOL environment variable in the format BUFFER_NUM:BUFFER_SIZE (in MB). The recommended size is 4:8, which can be adjusted to the most suitable configuration based on actual scenarios.

10. **Async transfer**: The asynchronous transfer mode can be enabled by configuring the ASCEND_USE_ASYNC_TRANSFER environment variable.

12. **Fabric Memory mode**: On the A3, with the latest drivers and CANN installed, when using Mooncake store, the ASCEND_ENABLE_USE_FABRIC_MEM environment variable can be set to enable fabric memory transfer mode (which allows direct access remote HOST memory).

13. **Auto Connect**: The auto connect feature can be enabled by configuring the `ASCEND_AUTO_CONNECT` environment variable. The default value is 0 (disabled).
