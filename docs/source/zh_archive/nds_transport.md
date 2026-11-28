---
orphan: true
---

# NDS Transport

[English documentation](../design/transfer-engine/transport/nds_transport.md)

## 背景

当前 TENT 在 Ascend NPU 设备上缺少存储介质与 HBM 之间的直通传输路径。通过主机内存与页缓存中转会带来额外的数据拷贝、CPU 开销和端到端延迟。NDS（NPU Direct Storage）Transport 通过公开标准 C 接口 `ascend_nds.h` / `libascend_nds`，为 Ascend HBM 与存储介质之间提供直接 DMA 读写能力。

## 概述

NDS Transport 位于 `mooncake-transfer-engine/tent/src/transport/nds`，通过外部标准 C 接口 `ascend_nds.h` / `libascend_nds.so` 为 TENT 提供存储直通读写能力。

核心特性：

- **标准 TENT 接入**：复用既有 `Request`、内存注册、File Segment、批量提交与状态查询接口；需同时开启 `USE_NDS` 编译开关与 `transports/nds/enable` 运行时开关。
- **默认 HBM 内存注册**：通过 `registerLocalMemory` 注册 NPU HBM 时默认调用 `ndsFileBufRegister`（进程级引用计数共享），并在提交时附带 `NDS_FILE_IO_REGISTERED_MEM`。
- **多 `iovec` 合并与滚动提交**：同批内指向同一文件、相同方向且偏移连续的请求自动合并为单条 I/O，受 `io_bytes` 与 `NDS_FILE_MAX_IOV` 约束；队列深度超出 `io_batch_depth` 时通过轮询持续补交。
- **安全排空与选路**：失败、注销或退出前先排空在途 DMA；默认文件选路顺序为 `GDS -> NDS -> IOURING`，自动跳过不可用候选。

> 职责边界：NDS 负责存储直通读写，Ascend Direct 负责 NPU 间内存传输。

## 前置依赖

### 硬件与软件

| 组件 | 要求 |
| --- | --- |
| 主机与驱动 | 配套的 OS、Kernel、CANN、HDK 及内核模块（`.ko`）；具体支持的硬件型号及版本约束见 [UBSIO NDS SDK](https://gitcode.com/openeuler/ubs-io/tree/develop/ubsio-nds) |
| 存储 | NDS 支持的存储文件系统上、已初始化 extent 的普通文件 |
| NDS SDK | [UBSIO](https://gitcode.com/openeuler/ubs-io/tree/develop/ubsio-nds)，包含提供向量批接口的 `ascend_nds.h` 及配套 `libascend_nds.so` |
| Mooncake 构建依赖 | 按[编译指南](../getting_started/build.md)准备，并安装所选 Ascend 平台的构建依赖 |

- 头文件与动态库须来自同一批 SDK 构建产物，并与所加载的内核模块（`.ko`）版本保持配套。

### 安装 NDS SDK

按 [UBSIO NDS SDK 指南](https://gitcode.com/openeuler/ubs-io/tree/develop/ubsio-nds) 安装 SDK 并配置驱动，然后将 `NDS_ROOT` 指向 SDK 安装目录并确认文件可读：

```bash
export NDS_ROOT=/path/to/ubsio-nds/dist
test -r "$NDS_ROOT/include/ascend_nds.h"
test -r "$NDS_ROOT/lib/libascend_nds.so"
```

## 编译

在目标 Python 环境中，从 Mooncake 仓库根目录执行：

```bash
source /usr/local/Ascend/ascend-toolkit/set_env.sh
export NDS_INSTALL_PREFIX="$HOME/.local/mooncake-nds"

cmake -S . -B build-nds -G Ninja \
  -DUSE_TENT=ON -DUSE_NDS=ON -DNDS_ROOT="$NDS_ROOT" \
  -DUSE_ASCEND_DIRECT=ON -DUSE_CUDA=OFF \
  -DWITH_STORE=OFF -DWITH_STORE_RUST=OFF \
  -DBUILD_UNIT_TESTS=OFF -DBUILD_EXAMPLES=OFF -DBUILD_BENCHMARK=OFF \
  -DTENT_METRICS_ENABLED=ON -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_INSTALL_PREFIX="$NDS_INSTALL_PREFIX"
cmake --build build-nds --parallel 2 --target tent_shared tentpy tent_cli
cmake --install build-nds/mooncake-common/src
cmake --install build-nds/mooncake-transfer-engine/tent
```

| CMake 选项 | 默认值 | 说明 |
| --- | --- | --- |
| `USE_TENT` | `OFF` | 构建 TENT，启用 `USE_NDS` 的前提 |
| `USE_NDS` | `OFF` | 构建并链接 NDS Transport |
| `NDS_ROOT` | 未设置 | 包含 `include/` 和 `lib/` 或 `lib64/` 的 SDK 前缀 |
| `NDS_INCLUDE_DIR` / `NDS_LIBRARY` | 自动查找 | 分别显式指定头文件目录和库路径（优先级高于 `NDS_ROOT`） |
| `TENT_METRICS_ENABLED` | `OFF` | 编译 TENT 指标支持 |

编译说明：

- 示例通过 `USE_ASCEND_DIRECT=ON` 启用 Ascend 平台内存探测（需配套 CANN/ADXL/HIXL 依赖）；仅设 `USE_NDS=ON` 不会启用 Ascend 平台探测，运行时可关闭 `ascend_direct` Transport。
- 显式指定 Python 时，将 `PYTHON_EXECUTABLE` 和 `Python3_EXECUTABLE` 均指向目标解释器。
- 安装产物包含 `libtent_shared.so` 与 `tent` 诊断命令；C/C++ 使用 `mooncake-transfer-engine/tent/include` 头文件，Python 应用直接使用 `build-nds/mooncake-transfer-engine/tent/src/python` 下的绑定，无需独立 NDS wheel。

## 环境变量配置

| 变量 | 使用条件 | 说明 |
| --- | --- | --- |
| `LD_LIBRARY_PATH` | 库不在动态加载器默认路径时 | 包含 TENT、NDS SDK 与 CANN 的库目录 |
| `PYTHONPATH` | 使用 Python 绑定时 | 指向构建生成的 `tent` 模块目录 |
| `MC_TENT_CONF` | 配置 TENT 应用时 | TENT JSON 配置文件路径或内联 JSON |
| `MC_USE_TENT` | 使用经典 TE 兼容入口时 | 设为 `1`；直接调用 `tent.TransferEngine()` 不需要 |

```bash
export LD_LIBRARY_PATH="$NDS_INSTALL_PREFIX/lib:$NDS_ROOT/lib:${LD_LIBRARY_PATH:-}"
export PYTHONPATH="$PWD/build-nds/mooncake-transfer-engine/tent/src/python:${PYTHONPATH:-}"
export MC_TENT_CONF=/absolute/path/nds-tent.json
```

### TENT 配置文件

将以下内容保存为 `nds-tent.json`，用于固定通过 NDS 执行 NPU 存储直通 I/O：

```json
{
  "metadata_type": "p2p",
  "rpc_server_hostname": "127.0.0.1",
  "rpc_server_port": 0,
  "merge_requests": false,
  "transports": {
    "tcp": {"enable": false},
    "rdma": {"enable": false},
    "shm": {"enable": false},
    "io_uring": {"enable": false},
    "ascend_direct": {"enable": false},
    "nds": {
      "enable": true,
      "io_batch_depth": 128,
      "io_bytes": 16777216
    }
  },
  "policy": [
    {
      "name": "nds_storage",
      "segment_type": "file",
      "transports": ["nds"]
    }
  ]
}
```

| NDS 配置项 | 默认值 | 取值范围与说明 |
| --- | --- | --- |
| `enable` | `false` | 是否在当前 engine 中加载 NDS Transport |
| `io_batch_depth` | `128` | 单个 batch 允许同时在途的最大底层 I/O 数，范围 `1`–`1024` |
| `io_bytes` | `16777216` (16 MiB) | 合并/切分后的单条底层 I/O 最大字节数，按 4 KiB 对齐，范围 `4 KiB`–`64 MiB` |

> 配置在创建 `TransferEngine` 时生效，修改后需重新创建 engine。NPU HBM 缓冲区默认启用注册（`ndsFileBufRegister`），无需额外配置开关。

## 功能验证

### 安装检查

```bash
"$NDS_INSTALL_PREFIX/bin/tent" diagnostics version --json
python3 -c 'import tent, torch, torch_npu; print(tent.__file__); print(torch.npu.is_available())'
```

确认 Python 加载了目标构建目录的 `tent` 模块且包含 `tent.TransportType.NDS`。`engine.available()` 反映整个 engine 状态，NDS 实际初始化与数据通路通过业务 I/O 验证确认。

## 注意事项

1. **4 KiB 对齐与边界**：HBM 地址、传输长度与存储偏移均须按 4 KiB（4096 B）对齐；每个分片须落在已注册 HBM 内，且请求范围同时满足 File Segment 与实际文件容量。
2. **文件准备与独占**：使用前须实际写入完整目标数据区域并执行 `fsync`。仅靠 `fallocate` 或 `truncate` 产生未初始化 extent 时，NDS 回环可能成功但 POSIX 读取仍返回零。传输期间避免截断、打洞、并发 buffered I/O 与重叠写；交叉校验须使用 `O_DIRECT` 绕过页缓存。
3. **缓冲区与会话生命周期**：在传输完成或注销成功前保持 HBM 有效；错误清理与注销会等待在途 DMA 排空，若注销返回失败应重试成功后再释放显存。若同一进程内外部应用已自行打开 NDS 驱动会话，需先关闭再安装 `NdsTransport`。
4. **失败处理与 Failover**：短提交场景下已接收的前缀可能已进入 DMA 队列；NDS 会先排空所有已提交 DMA 再报告失败，已落盘的部分写入不回滚。读写失败均遵循既有文件传输 failover 策略，如需禁止回退请使用仅包含 `["nds"]` 的策略。
5. **可观测性**：在启用指标的构建中打开 `metrics/enabled`，可观察 `transport="nds"` 的请求、字节、失败与尝试计数；日志级别通过 TENT 的 `log_level` 控制。

## 故障排查

| 现象 | 检查与处理 |
| --- | --- |
| CMake 找不到 NDS SDK | 检查 `NDS_ROOT`，或显式设置 `NDS_INCLUDE_DIR` 与 `NDS_LIBRARY` |
| `import tent` 失败或加载了其他版本 | 检查 Python 版本、`PYTHONPATH`、`tent.__file__` 及动态库依赖 |
| `ndsFileDriverOpen` 失败 | 检查 SDK 与驱动版本兼容性，以及 NDS 设备访问权限 |
| NDS 不可用，或注册/选路失败 | 检查 `USE_NDS` 与 `transports/nds/enable`、Ascend 平台构建、当前 NPU、内存位置、4 KiB 对齐与选路策略 |
| 请求失败或完成字节数不足 | 检查传输状态、文件容量、提交范围与驱动日志；排空完成前保持缓冲区有效 |
| 小粒度带宽偏低 | 先确认数据校验通过，再对照分片布局、是否触发多 `iovec` 合并、batch 大小、`io_batch_depth` 与设备负载 |

## 相关文档

- [UBSIO NDS SDK](https://gitcode.com/openeuler/ubs-io/tree/develop/ubsio-nds)
