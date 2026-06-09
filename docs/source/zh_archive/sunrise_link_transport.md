# Sunrise Link Transport

## 概述
Sunrise Link 是 Mooncake TENT 传输框架中的一个 GPU 传输后端。它通过 Tang Runtime（`tangrt`）提供设备内存分配、指针属性查询、Peer Copy 与 IPC 句柄相关能力，并在 TENT 中以 `SUNRISE_LINK` 传输类型注册。

在运行时，当编译开启 `USE_SUNRISE` 且配置项 `transports/sunrise_link/enable=true` 时，`TransferEngineImpl` 会加载 Sunrise Link Transport。

---

## 新增依赖
Sunrise Link Transport 在 Mooncake 基础依赖外，额外依赖 Tang Runtime：

- **头文件路径**：`/usr/local/tangrt/include`
- **库路径**：`/usr/local/tangrt/lib/linux-x86_64`
- **动态库**：`libtangrt_shared.so`（运行时按默认安装路径加载）

建议确认 `tangrt` 与 `ptml` 相关运行库可被动态链接器找到（例如通过 `LD_LIBRARY_PATH` 或系统库路径配置）。

---

## 构建与编译

**前置条件**

- 已安装可用的 Tang Runtime（默认安装到 `/usr/local/tangrt`）
- 编译环境可访问 Mooncake 及其基础依赖

**CMake 配置**

```bash
# 克隆 Mooncake 仓库
git clone https://github.com/kvcache-ai/Mooncake.git
cd Mooncake

# 启用 TENT + Sunrise Link
mkdir build && cd build
cmake .. -DUSE_TENT=ON -DUSE_SUNRISE=ON

# 编译
make -j$(nproc)
```

---

## 运行与测试

`transfer_engine_bench` 支持 `sunrise_link` 协议，可用于基本连通性与性能验证。

```bash
# 终端 1：目标端（Target）
./transfer_engine_bench \
  --mode=target \
  --protocol=sunrise_link \
  --local_server_name=10.0.0.2 \
  --metadata_server=P2PHANDSHAKE \
  --gpu_id=0

# 终端 2：发起端（Initiator）
./transfer_engine_bench \
  --mode=initiator \
  --protocol=sunrise_link \
  --metadata_server=P2PHANDSHAKE \
  --segment_id=10.0.0.2:$PORT \
  --gpu_id=0 \
  --block_size=8388608 \
  --batch_size=32
```

> 说明：当 `metadata_server=P2PHANDSHAKE` 时，目标端实际监听端口可能为动态分配端口。请以目标端日志中打印的实际端口替换 `--segment_id` 中的 `$PORT`。

---

## 配置项说明（TENT）

Sunrise Link 支持通过配置文件调整行为，常用项包括：

- `transports/sunrise_link/enable`：是否启用该传输（默认 `true`）
- `transports/sunrise_link/async_memcpy_threshold`：异步拷贝阈值（单位 MiB）

可根据业务负载与设备拓扑微调上述参数。
