# Ascend Direct Transport
Ascend Direct Transport(以下简称Transport)源代码路径为Mooncake/mooncake-transfer-engine/src/transport/ascend_transport/ascend_direct_transport。

## 概述
Transport是一个基于CANN提供的ADXL能力构建的传输适配层，直接兼容Mooncake Transfer Engine。
用户使用Transport可以实现在Ascend环境进行Host To Device, Device To Host, Device To Device传输，并且可使用多种通信协议，如HCCS、RDMA，未来还可实现Ascend和GPU异构环境的数据传输。
要编译使用Transport库，请参考build.md所述流程，并开启USE_ASCEND_DIRECT选项。

### 新增依赖
**昇腾Compute Architecture for Neural Networks**
请安装昇腾Compute Architecture for Neural Networks最新版本。

Ascend 的 TENT 传输后端（`MC_USE_TENT=1`）使用 HIXL 的 AutoConnect 会话模式并将对端会话保持至析构阶段，因此要求 **CANN/HIXL 9.2.0 或更新版本**。在 9.1.x 及更早版本中，当 AutoConnect 会话仍处于打开状态时注销内存会失败；在这些版本上请使用经典 `ascend` 传输后端。

### 测试用例
Transport提供性能测试文件mooncake-transfer-engine/example/transfer_engine_ascend_direct_perf.cpp。

当 metadata_server 配置为 P2PHANDSHAKE 时，Mooncake 会在新的 RPC 端口映射中随机选择监听端口，以避免端口冲突。因此，测试时需要按以下步骤操作：
1. 先启动目标节点，观察其在 mooncake-transfer-engine/src/transfer_engine.cpp 中打印的日志，找到如下格式的语句：
Transfer Engine RPC using <协议> listening on <IP>:<实际端口>，记录目标节点实际监听的端口号。
2. 修改发起节点的启动命令： 将 --segment_id 参数的值改为目标节点的 IP + 实际监听的端口号（格式为 <IP>:<端口>）。
3. 启动发起节点，完成连接测试。

完整命令格式见下文：

启动目标节点:
```shell
./transfer_engine_ascend_direct_perf --metadata_server=P2PHANDSHAKE --local_server_name=127.0.0.1:12345 --operation=write --device_logicid=0 --mode=target --block_size=16384 --batch_size=32 --block_iteration=10
```
启动发起节点:
```shell
./transfer_engine_ascend_direct_perf --metadata_server=P2PHANDSHAKE --local_server_name=127.0.0.1:12346 --operation=write --device_logicid=1 --mode=initiator --block_size=16384 --batch_size=32 --block_iteration=10  --segment_id=127.0.0.1:real_port
```

### 环境变量配置

以下环境变量可用于控制 Ascend Direct Transport 的行为。对于 Mooncake 定义的所有 `ASCEND_*` 环境变量，均优先读取带 `MC_` 前缀的 `MC_ASCEND_*` 版本，同时兼容不加前缀的 `ASCEND_*` 版本。在 TENT 模式（`MC_USE_TENT=1` 或原生 `tent::TransferEngine`）下，支持的配置项会自动转换为 TENT `Config` 中的对应配置。

下表将 **经典 TE 与 TENT 均支持** 的常用重要配置项排在前面，随后为 CANN/HCCL 原生配置及仅经典 TE 支持或已弃用的配置项。

| 变量名 | 描述 | 默认值 | TENT 支持 | 示例 |
|--------|------|--------|-----------|------|
| `MC_ASCEND_TRANSFER_TIMEOUT` / `ASCEND_TRANSFER_TIMEOUT` | 数据传输超时时间（毫秒） | 10000 | 支持，自动转换为 `transports/ascend_direct/transfer_timeout_ms` 配置 | `MC_ASCEND_TRANSFER_TIMEOUT=10000` |
| `MC_ASCEND_THREAD_POOL_SIZE` / `ASCEND_THREAD_POOL_SIZE` | 传输线程池的工作线程数（1–32） | 16（缓冲池模式下为 1） | 支持，自动转换为 `transports/ascend_direct/thread_pool_size` 配置 | `MC_ASCEND_THREAD_POOL_SIZE=32` |
| `MC_ASCEND_BASE_PORT` / `ASCEND_BASE_PORT` | ADXL/HIXL 引擎端口分配的基础端口 | 20000 | 支持，自动转换为 `transports/ascend_direct/base_port` 配置 | `MC_ASCEND_BASE_PORT=20000` |
| `MC_ASCEND_ENABLE_USE_FABRIC_MEM` / `ASCEND_ENABLE_USE_FABRIC_MEM` | 在 Mooncake Store 中启用 fabric 内存传输模式（仅 A3） | 0（禁用） | 支持（仅 Store TE 生效），自动转换为 `transports/ascend_direct/fabric_mem` 配置 | `MC_ASCEND_ENABLE_USE_FABRIC_MEM=1` |
| `MC_ASCEND_GLOBAL_RESOURCE_CONFIG` / `ASCEND_GLOBAL_RESOURCE_CONFIG` | 全局资源配置 | - | 支持，自动转换为 `transports/ascend_direct/global_resource_config` 配置 | `MC_ASCEND_GLOBAL_RESOURCE_CONFIG='{"comm_resource_config":{"protocol_desc":"roce:device"}}'` |
| `MC_ASCEND_RDMA_TC` / `ASCEND_RDMA_TC` | RDMA Traffic Class（未设置时回退到 `HCCL_RDMA_TC`） | - | 支持，自动转换为 `transports/ascend_direct/rdma_tc` 配置 | `MC_ASCEND_RDMA_TC=106` |
| `MC_ASCEND_RDMA_SL` / `ASCEND_RDMA_SL` | RDMA Service Level（未设置时回退到 `HCCL_RDMA_SL`） | - | 支持，自动转换为 `transports/ascend_direct/rdma_sl` 配置 | `MC_ASCEND_RDMA_SL=3` |
| `MC_ASCEND_LOCAL_COMM_RES` / `ASCEND_LOCAL_COMM_RES` | HIXL 本地通信资源 JSON 配置（`LocalCommRes`） | `{"version":"1.3"}`（CS/TENT 模式） | 支持，自动转换为 `transports/ascend_direct/local_comm_res` 配置 | `MC_ASCEND_LOCAL_COMM_RES='{"version":"1.3"}'` |
| `MC_ASCEND_USE_SHMEM` / `ASCEND_USE_SHMEM` | 为 Host 内存启用同机共享 memfd 快速拷贝路径 | 0（禁用） | 支持，自动转换为 `transports/ascend_direct/use_shmem` 配置 | `MC_ASCEND_USE_SHMEM=1` |
| `MC_ASCEND_STANDALONE_SINGLE_SEGMENT` / `ASCEND_STANDALONE_SINGLE_SEGMENT` | 在多个 NPU 间共享同一 Store Segment 以减少建链数 | 0（禁用） | 支持，自动转换为 `transports/ascend_direct/standalone_single_segment` 配置 | `MC_ASCEND_STANDALONE_SINGLE_SEGMENT=1` |
| `MC_CUSTOM_TOPO_JSON` | 网卡拓扑 JSON 文件路径（配置 `comm_resource_config.nic_topo_path`） | - | 支持，自动转换为 `topology/custom_json_path` 配置 | `MC_CUSTOM_TOPO_JSON=/etc/mooncake/nic_topo.json` |
| `HCCL_RDMA_TIMEOUT` | RDMA 数据包重传超时时间系数 | - | CANN/HCCL 原生配置 | `HCCL_RDMA_TIMEOUT=14` |
| `HCCL_RDMA_RETRY_CNT` | RDMA 数据包重传次数 | - | CANN/HCCL 原生配置 | `HCCL_RDMA_RETRY_CNT=7` |
| `MC_ASCEND_AUTO_CONNECT` / `ASCEND_AUTO_CONNECT` | 启用自动连接管理 | 1（启用） | 默认开启，不需要指定 | `MC_ASCEND_AUTO_CONNECT=0` |
| `MC_ASCEND_USE_ASYNC_TRANSFER` / `ASCEND_USE_ASYNC_TRANSFER` | 启用异步传输模式 | 0（禁用） | 默认开启异步传输，不需要指定 | `MC_ASCEND_USE_ASYNC_TRANSFER=1` |
| `MC_ASCEND_CONNECT_TIMEOUT` / `ASCEND_CONNECT_TIMEOUT` | 链路建链超时时间（毫秒） | 10000 | 默认使用 AutoConnect，不需要指定 | `MC_ASCEND_CONNECT_TIMEOUT=5000` |
| `MC_ASCEND_USE_SHORT_CONNECTION` / `ASCEND_USE_SHORT_CONNECTION` | 启用短连接模式（每次传输后断开） | 0（禁用） | 不支持 | `MC_ASCEND_USE_SHORT_CONNECTION=1` |
| `MC_ASCEND_BUFFER_POOL` / `ASCEND_BUFFER_POOL` *(已弃用 / Deprecated)* | 中转模式缓冲池配置（`BUFFER_NUM:BUFFER_SIZE_MB`） | "0:0"（禁用） | 不支持 | `MC_ASCEND_BUFFER_POOL=4:8` |
| `HCCL_INTRA_ROCE_ENABLE` *(已弃用 / Deprecated)* | CANN 9.2 之前使用非 `hixl_cs` 模式时按场景使能节点内 RDMA 通信协议 | 0（禁用） | 仅做兼容识别 | `HCCL_INTRA_ROCE_ENABLE=1` |

详细说明：
- **MC_ASCEND_TRANSFER_TIMEOUT / ASCEND_TRANSFER_TIMEOUT**：数据传输超时时间（毫秒，默认 `10000`）。经典 TE 与 TENT 均支持（TENT 下自动转换为 `transports/ascend_direct/transfer_timeout_ms` 配置）。
- **MC_ASCEND_THREAD_POOL_SIZE / ASCEND_THREAD_POOL_SIZE**：传输线程池的工作线程数（范围 `1`–`32`，默认 `16`，经典缓冲池模式下为 `1`）。经典 TE 与 TENT 均支持（TENT 下自动转换为 `transports/ascend_direct/thread_pool_size` 配置）。
- **MC_ASCEND_BASE_PORT / ASCEND_BASE_PORT**：ADXL/HIXL 引擎端口分配的基础端口（默认 `20000`）。经典 TE 与 TENT 均支持（TENT 下自动转换为 `transports/ascend_direct/base_port` 配置）。
- **MC_ASCEND_ENABLE_USE_FABRIC_MEM / ASCEND_ENABLE_USE_FABRIC_MEM**：需要 CANN 9.0+ 和 HDK 26.0+，在支持该功能的平台使用 Mooncake Store 时推荐启用，可显著提升传输性能。在 TENT 中仅对 Store TE（`store_te_init=true`）自动转换为 `transports/ascend_direct/fabric_mem` 配置，以保证同进程内 P2P TE 的隔离。
- **MC_ASCEND_GLOBAL_RESOURCE_CONFIG / ASCEND_GLOBAL_RESOURCE_CONFIG**：配置 HIXL 全局资源（如 `comm_resource_config.protocol_desc`；TENT 下自动转换为 `transports/ascend_direct/global_resource_config` 配置），具体配置项参考 HIXL 的 `OPTION_GLOBAL_RESOURCE_CONFIG` 文档。支持可选的 `"store"` 子对象以区分同进程内的 Store TE 与 P2P TE 配置。
- **MC_ASCEND_RDMA_TC / ASCEND_RDMA_TC** 与 **MC_ASCEND_RDMA_SL / ASCEND_RDMA_SL**：设置 RDMA 网卡的 Traffic Class（`RdmaTrafficClass`）与 Service Level（`RdmaServiceLevel`），未设置时会回退读取 `HCCL_RDMA_TC` / `HCCL_RDMA_SL`。TENT 下分别自动转换为 `transports/ascend_direct/rdma_tc` 和 `transports/ascend_direct/rdma_sl` 配置。
- **MC_ASCEND_LOCAL_COMM_RES / ASCEND_LOCAL_COMM_RES**：覆盖 HIXL `LocalCommRes` JSON 配置；未设置时，Mooncake 在 HIXL Client-Server 模式和 TENT 下默认自动注入 `{"version":"1.3"}`（TENT 下自动转换为 `transports/ascend_direct/local_comm_res` 配置）。
- **MC_ASCEND_USE_SHMEM / ASCEND_USE_SHMEM**：启用后，Host 侧 Store 内存通过共享 memfd 分配，同机对端进程可直接映射并通过本地拷贝传输（TENT 下自动转换为 `transports/ascend_direct/use_shmem` 配置）。
- **MC_ASCEND_STANDALONE_SINGLE_SEGMENT / ASCEND_STANDALONE_SINGLE_SEGMENT**：在独立 Store 模式下设为 `1`（经典 TE 与 TENT 均支持，TENT 下自动转换为 `transports/ascend_direct/standalone_single_segment` 配置），可在所有 NPU 引擎间共享同一内存 Segment 以减少客户端到服务端的连接数，默认为 `0`。要求 `MC_ASCEND_BUFFER_POOL=0:0` / `ASCEND_BUFFER_POOL=0:0`（或未设置）。客户端按 `S - 1 - (physical_device_id % S)` 进行路由。
- **MC_CUSTOM_TOPO_JSON**：网卡拓扑 JSON 文件路径，TE 也用它为 HIXL 配置 `comm_resource_config.nic_topo_path`（`npu:<user_device_id>`）以生成 Host RoCE endpoint。TENT 模式下自动转换为 `topology/custom_json_path` 配置；当 `MC_TENT_CONF` 与 `MC_CUSTOM_TOPO_JSON` 同时设置时，以 `MC_CUSTOM_TOPO_JSON` 环境变量为准。
- **MC_ASCEND_AUTO_CONNECT / ASCEND_AUTO_CONNECT**：需要 CANN 9.0 及以上版本。默认值为 `1`：当对端异常下线后可自动断链；经典 TE 下可设置 `MC_ASCEND_AUTO_CONNECT=0`（或 `ASCEND_AUTO_CONNECT=0`）关闭。**TENT 默认开启（`AutoConnect=1`），不需要指定**（该环境变量会被忽略）。
- **MC_ASCEND_USE_ASYNC_TRANSFER / ASCEND_USE_ASYNC_TRANSFER**：需要 CANN 8.5+，经典 TE 下用于开启 HIXL 异步传输模式（默认同步模式）。**TENT 默认开启异步传输，不需要指定**（该环境变量会被忽略）。
- **MC_ASCEND_CONNECT_TIMEOUT / ASCEND_CONNECT_TIMEOUT**：经典 TE 下控制建链超时时间（毫秒）。**TENT 默认使用 AutoConnect 模式，不需要指定**（该环境变量会被忽略）。
- **MC_ASCEND_USE_SHORT_CONNECTION / ASCEND_USE_SHORT_CONNECTION**：经典 TE 下启用短连接模式（每次传输后断开）。**TENT 不支持**：TENT 会保持对端会话直至析构或超时断链。
- **MC_ASCEND_BUFFER_POOL / ASCEND_BUFFER_POOL *(已弃用 / Deprecated)***：已标记为 deprecated 的中转模式缓冲池配置（格式 `BUFFER_NUM:BUFFER_SIZE_MB`），仅用于经典 TE（且不兼容 HIXL Client-Server 模式与异步传输）。**TENT 不支持**（在单 segment 模式下若设置了非 `0:0` 值会显式报错）。
- **HCCL_INTRA_ROCE_ENABLE *(已弃用 / Deprecated)***：已标记为 deprecated，仅在 CANN 9.2 之前未使用 `hixl_cs`（Client-Server 模式）时按场景使能节点内 RDMA 通信。CANN 9.2+ / `hixl_cs` 模式下请改用 `MC_ASCEND_GLOBAL_RESOURCE_CONFIG` / `ASCEND_GLOBAL_RESOURCE_CONFIG` 中的 `comm_resource_config.protocol_desc` 进行配置。

### TENT 原生 JSON 配置方式

TENT 原生使用 **JSON** 配置文件（而非 YAML；完整模板见 `mooncake-transfer-engine/tent/config/transfer-engine.json`）。除上述环境变量自动转换外，还可以通过以下两种方式直接使用 TENT 原生配置：

1. **通过 `MC_TENT_CONF` 环境变量**：指定 JSON 配置文件路径，或直接传入内联 JSON 字符串（由 `ConfigHelper::loadFromEnv()` 加载）：
   ```shell
   export MC_TENT_CONF=/path/to/tent_ascend.json
   ```
2. **通过 C++ API 编程配置**：在构造 `tent::TransferEngine` 前，调用 `tent::Config::load()` / `tent::Config::loadFile()` 加载 JSON，或通过 `tent::Config::set()` 写入对应配置键。

Ascend Direct 在 TENT 下的原生 JSON 配置示例：

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

**说明：**
- `global_resource_config` 和 `local_comm_res` 必须填写为 **JSON 字符串**（需要转义引号，例如 `"{\"comm_resource_config\":{\"protocol_desc\":\"roce:device\"}}"`），`rdma_tc` / `rdma_sl` 也为字符串类型。
- `transfer_timeout_ms`、`base_port`（有效范围 `1024`–`65435`）和 `thread_pool_size`（有效范围 `1`–`32`）为整数类型；`enable`、`fabric_mem`、`use_shmem`、`standalone_single_segment`、`agent_mode` 为布尔类型。
- 若同时设置了 `MC_ASCEND_*` / `ASCEND_*` 或 `MC_CUSTOM_TOPO_JSON` 环境变量，`ConfigHelper::loadFromEnv()` 会在加载 `MC_TENT_CONF` 后将环境变量转换覆盖到对应配置项上（其中 `MC_ASCEND_ENABLE_USE_FABRIC_MEM` 仅在 `store_te_init=true` 时自动转换，而在 JSON 中直接配置 `"fabric_mem": true` 会对该引擎直接生效）。

### 注意事项（必看）
1. 调用 TransferEngine initialize 前需要 set device，比如 `torch.npu.set_device(0)`。

2. Transport 内部会建立一个 host 侧的 tcp 连接，会自动寻找可使用的端口，没有可用的时候会报错。

3. 当使用 `HCCS` 通信协议时，注册的内存地址需要和页表对齐（device 内存是 2MB 对齐）。

4. 请确保 `/etc/hccn.conf` 存在，特别是在容器内，请挂载 `/etc/hccn.conf` 或者把宿主机的文件复制到容器的 `/etc` 路径下。

5. 超时配置：
   - 经典 TE 下通过 `MC_ASCEND_CONNECT_TIMEOUT` / `ASCEND_CONNECT_TIMEOUT` 环境变量控制链路建链超时时间，默认超时时间为 10s（TENT 默认使用 AutoConnect，不需要指定）。
   - 通过 `MC_ASCEND_TRANSFER_TIMEOUT` / `ASCEND_TRANSFER_TIMEOUT` 环境变量控制数据传输超时时间，默认超时时间为 10s。
   - 通过 Python API 发起传输时，需确保外层 `MC_TRANSFER_TIMEOUT`（单位秒，默认 30）**严格大于两倍** `MC_ASCEND_TRANSFER_TIMEOUT` / `ASCEND_TRANSFER_TIMEOUT`（单位毫秒，默认 10000），因为传输超时后会重试，内层超时预算可能消耗两次。

6. 通过 `HCCL_RDMA_TIMEOUT` 用于配置 RDMA 网卡数据包重传超时时间系数，真实的数据包重传超时时间为 `4.096us * 2 ^ $HCCL_RDMA_TIMEOUT`，通过 `HCCL_RDMA_RETRY_CNT` 来配置 RDMA 网卡的重传次数，建议配置 `MC_ASCEND_TRANSFER_TIMEOUT` / `ASCEND_TRANSFER_TIMEOUT` 略大于 `重传时间 * HCCL_RDMA_RETRY_CNT`。

7. A2 server 内 / A3 超节点内默认通信协议为 `HCCS`。在 CANN 9.2+（`hixl_cs`）下可通过 `MC_ASCEND_GLOBAL_RESOURCE_CONFIG` / `ASCEND_GLOBAL_RESOURCE_CONFIG` 中的 `comm_resource_config.protocol_desc` 指定走 `RDMA`；仅在 CANN 9.2 之前使用非 `hixl_cs` 模式时，可按场景通过 `export HCCL_INTRA_ROCE_ENABLE=1` *(已弃用)* 指定走 `RDMA`。

8. 当使用 `RDMA` 通信协议时，在交换机和网卡默认配置不一致场景 / 需要流量规划场景下，可能需要修改 RDMA 网卡的 Traffic Class 和 Service Level 配置，通过 `MC_ASCEND_RDMA_TC` / `ASCEND_RDMA_TC`（或回退 `HCCL_RDMA_TC`）来设置 Traffic Class，通过 `MC_ASCEND_RDMA_SL` / `ASCEND_RDMA_SL`（或回退 `HCCL_RDMA_SL`）来设置 Service Level。

9. *(已弃用)* 在经典 TE 向 Host 内存直接传输不通的场景下，可通过中转 Buffer 的方式进行传输，具体开启方式是配置 `MC_ASCEND_BUFFER_POOL` / `ASCEND_BUFFER_POOL` 环境变量，格式为 `BUFFER_NUM:BUFFER_SIZE`（单位 MB），推荐大小为 `4:8`。该配置已标记为 deprecated，且 **TENT 与 HIXL Client-Server 模式均不支持**。

10. 在经典 TE 中可以通过配置 `MC_ASCEND_USE_ASYNC_TRANSFER` / `ASCEND_USE_ASYNC_TRANSFER` 环境变量来开启异步传输（TENT 默认开启异步传输，不需要通过该变量指定）。

11. 在 A3 上，在获取最新驱动和 CANN 的前提下，在使用 Mooncake Store 时，可以设置 `MC_ASCEND_ENABLE_USE_FABRIC_MEM` / `ASCEND_ENABLE_USE_FABRIC_MEM` 环境变量来开启 fabric mem 传输模式（能直接访问远端的 HOST 内存）。

12. 在经典 TE 中自动建链断链特性默认开启（`MC_ASCEND_AUTO_CONNECT=1` / `ASCEND_AUTO_CONNECT=1`），如需关闭可设为 `0`；在 TENT 中默认开启，不需要指定。
