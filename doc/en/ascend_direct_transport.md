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

### Important Notes

1. **Device Setup Required**: Before calling `TransferEngine.initialize()`, you must set the device (e.g., `torch.npu.set_device(0)`).

2. **TCP Connection**: Transport internally establishes a host-side TCP connection and automatically searches for available ports. An error will be reported if no ports are available.

3. **Memory Alignment for HCCS**: When using the `HCCS` communication protocol, registered memory addresses must be aligned with page tables (host memory: 4KB alignment, device memory: 2MB alignment).

4. **Configuration File**: Ensure `/etc/hccn.conf` exists, especially in containers. Mount `/etc/hccn.conf` or copy the host file to the container's `/etc` path.

5. **Timeout Configuration**: 
   - Use the `ASCEND_CONNECT_TIMEOUT` environment variable to control link establishment timeout (default: 3 seconds)
   - Use the `ASCEND_TRANSFER_TIMEOUT` environment variable to control data transfer timeout (default: 3 seconds)

6. **RDMA Timeout and Retry Configuration**: 
   - Use `HCCL_RDMA_TIMEOUT` to configure the RDMA NIC packet retransmission timeout coefficient. The actual packet retransmission timeout is `4.096us * 2 ^ $HCCL_RDMA_TIMEOUT`
   - Use `HCCL_RDMA_RETRY_CNT` to configure the RDMA NIC retransmission count
   - It is recommended to configure `ASCEND_TRANSFER_TIMEOUT` to be slightly larger than `retransmission_timeout * HCCL_RDMA_RETRY_CNT`

7. **Communication Protocol Selection**: Within A2 servers/A3 supernodes, the default communication protocol is HCCS. You can specify RDMA by setting `export HCCL_INTRA_ROCE_ENABLE=1`. For KV Cache transfer scenarios, RDMA transfer is recommended to avoid conflicts with model collective communication traffic that could impact inference performance.

8. **RDMA Configuration**: When using the `RDMA` communication protocol, in scenarios where switch and NIC default configurations are inconsistent or traffic planning is required, you may need to modify the RDMA NIC's Traffic Class and Service Level configuration:
   - Set Traffic Class using the `ASCEND_RDMA_TC` environment variable
   - Set Service Level using the `ASCEND_RDMA_SL` environment variable