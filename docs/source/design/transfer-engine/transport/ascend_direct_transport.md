# Ascend Direct Transport

Ascend Direct Transport (referred to as Transport throughout this document) is located at `Mooncake/mooncake-transfer-engine/src/transport/ascend_transport/ascend_direct_transport`.

## Overview

Transport is a transmission adapter layer built on CANN's ADXL capabilities, providing direct compatibility with the Mooncake Transfer Engine. With Transport, users can perform Host-to-Device, Device-to-Host, and Device-to-Device transfers in Ascend environments using various communication protocols such as HCCS and RDMA. Future enhancements will enable data transfer between heterogeneous Ascend and GPU environments.

To build and use the Transport library, please follow the process described in `build.md` and enable the `USE_ASCEND_DIRECT` option (the legacy `USE_ASCEND` option now selects the same Ascend Direct transport).

### Additional Dependencies

**Ascend Compute Architecture for Neural Networks (CANN)**

Please install the latest version of Ascend Compute Architecture for Neural Networks.

The TENT transport backend for Ascend (`MC_USE_TENT=1`) uses HIXL's AutoConnect
session mode and keeps peer sessions open until teardown, so it requires
**CANN/HIXL 9.2.0 or newer**. On 9.1.x and earlier, deregistering a memory
region while AutoConnect sessions to that peer are still open fails; use the
classic `ascend` transport on those versions.

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

The following environment variables can be configured to control Ascend Direct Transport behavior. For all `ASCEND_*` variables defined by Mooncake, the `MC_ASCEND_*` form is preferred and takes precedence, while the unprefixed `ASCEND_*` form remains supported for backward compatibility. Under TENT (`MC_USE_TENT=1` or native `tent::TransferEngine`), supported environment variables are automatically converted into TENT `Config` keys.

In the table below, variables supported by **both Classic TE and TENT** are listed first as primary options, followed by CANN/HCCL native variables and Classic-TE-only / deprecated options.

| Variable | Description | Default Value | TENT Support | Example |
|----------|-------------|---------------|--------------|---------|
| `MC_ASCEND_TRANSFER_TIMEOUT` / `ASCEND_TRANSFER_TIMEOUT` | Data transfer timeout in milliseconds | 10000 | Supported, auto-converted to `transports/ascend_direct/transfer_timeout_ms` config | `MC_ASCEND_TRANSFER_TIMEOUT=10000` |
| `MC_ASCEND_THREAD_POOL_SIZE` / `ASCEND_THREAD_POOL_SIZE` | Number of worker threads in the transfer thread pool (1–32) | 16 (1 for buffer pool mode) | Supported, auto-converted to `transports/ascend_direct/thread_pool_size` config | `MC_ASCEND_THREAD_POOL_SIZE=32` |
| `MC_ASCEND_BASE_PORT` / `ASCEND_BASE_PORT` | Base port for ADXL/HIXL engine port allocation | 20000 | Supported, auto-converted to `transports/ascend_direct/base_port` config | `MC_ASCEND_BASE_PORT=20000` |
| `MC_ASCEND_ENABLE_USE_FABRIC_MEM` / `ASCEND_ENABLE_USE_FABRIC_MEM` | Enable fabric memory transfer mode in Mooncake Store (A3 only) | 0 (disabled) | Supported (Store TE only), auto-converted to `transports/ascend_direct/fabric_mem` config | `MC_ASCEND_ENABLE_USE_FABRIC_MEM=1` |
| `MC_ASCEND_GLOBAL_RESOURCE_CONFIG` / `ASCEND_GLOBAL_RESOURCE_CONFIG` | Global resource configuration | - | Supported, auto-converted to `transports/ascend_direct/global_resource_config` config | `MC_ASCEND_GLOBAL_RESOURCE_CONFIG='{"comm_resource_config":{"protocol_desc":"roce:device"}}'` |
| `MC_ASCEND_RDMA_TC` / `ASCEND_RDMA_TC` | RDMA Traffic Class (falls back to `HCCL_RDMA_TC`) | - | Supported, auto-converted to `transports/ascend_direct/rdma_tc` config | `MC_ASCEND_RDMA_TC=106` |
| `MC_ASCEND_RDMA_SL` / `ASCEND_RDMA_SL` | RDMA Service Level (falls back to `HCCL_RDMA_SL`) | - | Supported, auto-converted to `transports/ascend_direct/rdma_sl` config | `MC_ASCEND_RDMA_SL=3` |
| `MC_ASCEND_LOCAL_COMM_RES` / `ASCEND_LOCAL_COMM_RES` | HIXL local communication resource JSON (`LocalCommRes`) | `{"version":"1.3"}` (in CS/TENT mode) | Supported, auto-converted to `transports/ascend_direct/local_comm_res` config | `MC_ASCEND_LOCAL_COMM_RES='{"version":"1.3"}'` |
| `MC_ASCEND_USE_SHMEM` / `ASCEND_USE_SHMEM` | Enable same-host shared memfd fast path for host buffers | 0 (disabled) | Supported, auto-converted to `transports/ascend_direct/use_shmem` config | `MC_ASCEND_USE_SHMEM=1` |
| `MC_ASCEND_STANDALONE_SINGLE_SEGMENT` / `ASCEND_STANDALONE_SINGLE_SEGMENT` | Share one Store segment across NPUs to reduce connections | 0 (disabled) | Supported, auto-converted to `transports/ascend_direct/standalone_single_segment` config | `MC_ASCEND_STANDALONE_SINGLE_SEGMENT=1` |
| `MC_CUSTOM_TOPO_JSON` | Path to NIC topology JSON for `comm_resource_config.nic_topo_path` | - | Supported, auto-converted to `topology/custom_json_path` config | `MC_CUSTOM_TOPO_JSON=/etc/mooncake/nic_topo.json` |
| `HCCL_RDMA_TIMEOUT` | RDMA packet retransmission timeout coefficient | - | CANN/HCCL native | `HCCL_RDMA_TIMEOUT=14` |
| `HCCL_RDMA_RETRY_CNT` | RDMA packet retransmission count | - | CANN/HCCL native | `HCCL_RDMA_RETRY_CNT=7` |
| `MC_ASCEND_AUTO_CONNECT` / `ASCEND_AUTO_CONNECT` | Enable automatic connection management | 1 (enabled) | Enabled by default, no need to specify | `MC_ASCEND_AUTO_CONNECT=0` |
| `MC_ASCEND_USE_ASYNC_TRANSFER` / `ASCEND_USE_ASYNC_TRANSFER` | Enable asynchronous transfer mode | 0 (disabled) | Async enabled by default, no need to specify | `MC_ASCEND_USE_ASYNC_TRANSFER=1` |
| `MC_ASCEND_CONNECT_TIMEOUT` / `ASCEND_CONNECT_TIMEOUT` | Link establishment timeout in milliseconds | 10000 | Uses AutoConnect by default, no need to specify | `MC_ASCEND_CONNECT_TIMEOUT=5000` |
| `MC_ASCEND_USE_SHORT_CONNECTION` / `ASCEND_USE_SHORT_CONNECTION` | Enable short connection mode (disconnect after each transfer) | 0 (disabled) | Not supported | `MC_ASCEND_USE_SHORT_CONNECTION=1` |
| `MC_ASCEND_BUFFER_POOL` / `ASCEND_BUFFER_POOL` *(Deprecated)* | Buffer pool configuration for intermediate transfer mode (`BUFFER_NUM:BUFFER_SIZE_MB`) | `"0:0"` (disabled) | Not supported | `MC_ASCEND_BUFFER_POOL=4:8` |
| `HCCL_INTRA_ROCE_ENABLE` *(Deprecated)* | Enable intra-node RDMA protocol when not using `hixl_cs` before CANN 9.2 | 0 (disabled) | Fallback detection only | `HCCL_INTRA_ROCE_ENABLE=1` |

**Detailed Descriptions:**

- **MC_ASCEND_TRANSFER_TIMEOUT / ASCEND_TRANSFER_TIMEOUT**: Controls data transfer timeout in milliseconds (default: `10000`). Supported in both classic TE and TENT (auto-converted to `transports/ascend_direct/transfer_timeout_ms`).
- **MC_ASCEND_THREAD_POOL_SIZE / ASCEND_THREAD_POOL_SIZE**: Number of worker threads (range `1`–`32`, default `16`; `1` in classic buffer pool mode). Supported in both classic TE and TENT (auto-converted to `transports/ascend_direct/thread_pool_size`).
- **MC_ASCEND_BASE_PORT / ASCEND_BASE_PORT**: Base TCP port for ADXL/HIXL engine port allocation (default `20000`). Supported in both classic TE and TENT (auto-converted to `transports/ascend_direct/base_port`).
- **MC_ASCEND_ENABLE_USE_FABRIC_MEM / ASCEND_ENABLE_USE_FABRIC_MEM**: Requires CANN 9.0+ and HDK 26.0+. Recommended when using Mooncake Store on supported A3 platforms: it can significantly improve transmission performance. In TENT, this is auto-converted to `transports/ascend_direct/fabric_mem` only for Store TE (`store_te_init=true`) so P2P TEs in the same process stay isolated.
- **MC_ASCEND_GLOBAL_RESOURCE_CONFIG / ASCEND_GLOBAL_RESOURCE_CONFIG**: Configures HIXL global resources (auto-converted to `transports/ascend_direct/global_resource_config` in TENT), such as `comm_resource_config.protocol_desc`. Refer to HIXL documentation for `OPTION_GLOBAL_RESOURCE_CONFIG` settings. Supports an optional `"store"` sub-object to separate Store TE and P2P TE configs in the same process.
- **MC_ASCEND_RDMA_TC / ASCEND_RDMA_TC** and **MC_ASCEND_RDMA_SL / ASCEND_RDMA_SL**: Configures RDMA NIC Traffic Class (`RdmaTrafficClass`) and Service Level (`RdmaServiceLevel`), falling back to `HCCL_RDMA_TC` / `HCCL_RDMA_SL` if unset. Auto-converted to `transports/ascend_direct/rdma_tc` and `transports/ascend_direct/rdma_sl` in TENT.
- **MC_ASCEND_LOCAL_COMM_RES / ASCEND_LOCAL_COMM_RES**: Overrides HIXL `LocalCommRes` JSON. When unset, Mooncake auto-injects `{"version":"1.3"}` in HIXL Client-Server mode and TENT (auto-converted to `transports/ascend_direct/local_comm_res`).
- **MC_ASCEND_USE_SHMEM / ASCEND_USE_SHMEM**: Enables allocating host Store segments as shared memfds so co-located peer processes can map them directly (auto-converted to `transports/ascend_direct/use_shmem` in TENT).
- **MC_ASCEND_STANDALONE_SINGLE_SEGMENT / ASCEND_STANDALONE_SINGLE_SEGMENT**: Set to `1` for standalone Store (classic or TENT, auto-converted to `transports/ascend_direct/standalone_single_segment`) to share one memory segment across all NPU engines and reduce client-to-server connections. Default: `0`. Requires `MC_ASCEND_BUFFER_POOL=0:0` / `ASCEND_BUFFER_POOL=0:0` (or unset). Clients route using `S - 1 - (physical_device_id % S)`, so single-device processes isolated with `ASCEND_RT_VISIBLE_DEVICES` still balance across server engines as long as ACL reports distinct physical device IDs.
- **MC_CUSTOM_TOPO_JSON**: Path to a NIC topology JSON file; Transfer Engine also uses it to configure HIXL's `comm_resource_config.nic_topo_path` (`npu:<user_device_id>` key) for Host RoCE endpoint generation. Under TENT, this is auto-converted to `topology/custom_json_path`; when both `MC_TENT_CONF` and `MC_CUSTOM_TOPO_JSON` are set, `MC_CUSTOM_TOPO_JSON` takes precedence.
- **MC_ASCEND_AUTO_CONNECT / ASCEND_AUTO_CONNECT**: Requires CANN 9.0 or later. Default is `1`: links can be automatically disconnected when the remote end goes offline abnormally. Set `MC_ASCEND_AUTO_CONNECT=0` (or `ASCEND_AUTO_CONNECT=0`) to disable in classic TE. **In TENT, AutoConnect (`AutoConnect=1`) is enabled by default and does not need to be specified** (this variable is ignored).
- **MC_ASCEND_USE_ASYNC_TRANSFER / ASCEND_USE_ASYNC_TRANSFER**: Requires CANN 8.5+. Enables HIXL asynchronous transfer mode in classic TE (defaults to synchronous mode). **In TENT, async transfer is enabled by default and does not need to be specified** (this variable is ignored).
- **MC_ASCEND_CONNECT_TIMEOUT / ASCEND_CONNECT_TIMEOUT**: Controls link establishment timeout (ms) in classic TE. **In TENT, AutoConnect is used by default so no explicit connect timeout needs to be specified** (this variable is ignored).
- **MC_ASCEND_USE_SHORT_CONNECTION / ASCEND_USE_SHORT_CONNECTION**: Enables disconnecting after each transfer in classic TE. **Not supported in TENT**, which keeps peer sessions open until teardown or timeout disconnect.
- **MC_ASCEND_BUFFER_POOL / ASCEND_BUFFER_POOL *(Deprecated)***: Deprecated intermediate-buffer transfer mode (`BUFFER_NUM:BUFFER_SIZE_MB`) for classic TE only (incompatible with HIXL Client-Server mode and async transfer). **Not supported in TENT** (in single-segment mode, setting a non-`0:0` value fails explicitly).
- **HCCL_INTRA_ROCE_ENABLE *(Deprecated)***: Deprecated. Only needed before CANN 9.2 when not using `hixl_cs` (Client-Server mode) to enable intra-node RDMA communication depending on the scenario. On CANN 9.2+ / `hixl_cs`, configure `comm_resource_config.protocol_desc` via `MC_ASCEND_GLOBAL_RESOURCE_CONFIG` / `ASCEND_GLOBAL_RESOURCE_CONFIG` instead.

### TENT Native JSON Configuration

TENT uses **JSON** configuration (reference template: `mooncake-transfer-engine/tent/config/transfer-engine.json`). In addition to environment variable conversion, you can configure `ascend_direct` natively in TENT via either:

1. **`MC_TENT_CONF` environment variable**: set to a JSON file path or an inline JSON string (loaded by `ConfigHelper::loadFromEnv()`):
   ```shell
   export MC_TENT_CONF=/path/to/tent_ascend.json
   ```
2. **Programmatic C++ API**: load JSON via `tent::Config::load()` / `tent::Config::loadFile()` or set keys via `tent::Config::set()` before constructing `tent::TransferEngine`.

Example JSON configuration for Ascend Direct under TENT:

```json
{
  "topology": {
    "custom_json_path": "/etc/mooncake/nic_topo.json"
  },
  "transports": {
    "ascend_direct": {
      "enable": true,
      "transfer_timeout_ms": 10000,
      "base_port": 20000,
      "thread_pool_size": 16,
      "rdma_tc": "106",
      "rdma_sl": "3",
      "local_comm_res": "{\"version\":\"1.3\"}",
      "global_resource_config": "{\"comm_resource_config\":{\"protocol_desc\":\"roce:device\"}}",
      "fabric_mem": false,
      "use_shmem": false,
      "standalone_single_segment": false,
      "agent_mode": false
    }
  }
}
```

**Notes:**
- `global_resource_config` and `local_comm_res` must be JSON-encoded **strings** (for example `"{\"comm_resource_config\":{\"protocol_desc\":\"roce:device\"}}"`), and `rdma_tc` / `rdma_sl` are string values.
- `transfer_timeout_ms`, `base_port` (range `1024`–`65435`), and `thread_pool_size` (range `1`–`32`) are integers; `enable`, `fabric_mem`, `use_shmem`, `standalone_single_segment`, and `agent_mode` are booleans.
- If `MC_ASCEND_*` / `ASCEND_*` or `MC_CUSTOM_TOPO_JSON` environment variables are also set at the same time, `ConfigHelper::loadFromEnv()` converts and applies those environment variables on top of `MC_TENT_CONF` (with `MC_ASCEND_ENABLE_USE_FABRIC_MEM` only auto-converting when `store_te_init` is `true`, whereas `"fabric_mem": true` in JSON applies directly to that engine).

### Important Notes

1. **Device Setup Required**: Before calling `TransferEngine.initialize()`, you must set the device (e.g., `torch.npu.set_device(0)`).

2. **TCP Connection**: Transport internally establishes a host-side TCP connection and automatically searches for available ports. An error will be reported if no ports are available.

3. **Memory Alignment for HCCS**: When using the `HCCS` communication protocol, registered memory addresses must be aligned with page tables (device memory: 2MB alignment). Registering a remote-accessible device buffer whose base address is not 2MB-aligned (for example, a sub-block from the torch_npu caching allocator) fails immediately with `ERR_INVALID_ARGUMENT` instead of failing later at connection time.

4. **Configuration File**: Ensure `/etc/hccn.conf` exists, especially in containers. Mount `/etc/hccn.conf` or copy the host file to the container's `/etc` path.

5. **Timeout Configuration**:
   - Use `MC_ASCEND_CONNECT_TIMEOUT` / `ASCEND_CONNECT_TIMEOUT` to control link establishment timeout in classic TE (default: 10 seconds; TENT uses AutoConnect by default, no need to specify).
   - Use `MC_ASCEND_TRANSFER_TIMEOUT` / `ASCEND_TRANSFER_TIMEOUT` to control data transfer timeout (default: 10 seconds).
   - When the caller drives transfers through the Python API, keep the outer `MC_TRANSFER_TIMEOUT` (seconds, default 30) **strictly greater than twice** `MC_ASCEND_TRANSFER_TIMEOUT` / `ASCEND_TRANSFER_TIMEOUT` (milliseconds, default 10000). A transfer that hits the inner timeout is retried, so the inner budget can be spent twice; the outer deadline has to cover both attempts. If the outer timeout fires first, the transfer can still be in flight inside the Ascend transport, and the caller cannot tell whether it has quiesced — so do not reuse or unregister the target buffer until the transfer is known to have drained. Ascend Direct checks this when it initializes a transfer executor and logs a warning naming both values if the outer deadline is too small.

6. **RDMA Timeout and Retry Configuration**:
   - Use `HCCL_RDMA_TIMEOUT` to configure the RDMA NIC packet retransmission timeout coefficient. The actual packet retransmission timeout is `4.096us * 2 ^ $HCCL_RDMA_TIMEOUT`
   - Use `HCCL_RDMA_RETRY_CNT` to configure the RDMA NIC retransmission count
   - It is recommended to configure `MC_ASCEND_TRANSFER_TIMEOUT` / `ASCEND_TRANSFER_TIMEOUT` to be slightly larger than `retransmission_timeout * HCCL_RDMA_RETRY_CNT`

7. **Communication Protocol Selection**: Within A2 servers/A3 supernodes, the default communication protocol is HCCS. On CANN 9.2+ (`hixl_cs`), configure `comm_resource_config.protocol_desc` in `MC_ASCEND_GLOBAL_RESOURCE_CONFIG` / `ASCEND_GLOBAL_RESOURCE_CONFIG` to select RDMA. Before CANN 9.2 without `hixl_cs`, `export HCCL_INTRA_ROCE_ENABLE=1` *(deprecated)* can be used per scenario to specify RDMA.

8. **RDMA Configuration**: When using the `RDMA` communication protocol, in scenarios where switch and NIC default configurations are inconsistent or traffic planning is required, you may need to modify the RDMA NIC's Traffic Class and Service Level configuration:
   - Set Traffic Class using `MC_ASCEND_RDMA_TC` / `ASCEND_RDMA_TC` (falls back to `HCCL_RDMA_TC`)
   - Set Service Level using `MC_ASCEND_RDMA_SL` / `ASCEND_RDMA_SL` (falls back to `HCCL_RDMA_SL`)

9. **Buffer Pool Configuration *(Deprecated)***: In classic TE under constrained scenarios where direct host memory transfer is unavailable, transmission can be achieved via intermediate buffers by configuring `MC_ASCEND_BUFFER_POOL` / `ASCEND_BUFFER_POOL` in the format `BUFFER_NUM:BUFFER_SIZE` (in MB, e.g. `4:8`). This option is deprecated and is **not supported in TENT** or HIXL Client-Server mode.

10. **Async transfer**: In classic TE, asynchronous transfer mode can be enabled via `MC_ASCEND_USE_ASYNC_TRANSFER` / `ASCEND_USE_ASYNC_TRANSFER`. In TENT, async transfer is enabled by default and does not need to be specified.

11. **Fabric Memory mode**: On the A3, with the latest drivers and CANN installed, when using Mooncake Store, `MC_ASCEND_ENABLE_USE_FABRIC_MEM` / `ASCEND_ENABLE_USE_FABRIC_MEM` can be set to enable fabric memory transfer mode (which allows direct access to remote HOST memory).

12. **Auto Connect**: In classic TE, auto connect is enabled by default (`MC_ASCEND_AUTO_CONNECT=1` / `ASCEND_AUTO_CONNECT=1`) when supported, and can be disabled with `0`. In TENT, auto connect is enabled by default and does not need to be specified.
