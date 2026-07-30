# Sunrise Link Transport

## 概述
Sunrise Link 是曦望（Sunrise）开发的 GPU 间高速直连互连技术。在 Mooncake 中，它作为 GPU 传输后端集成于 TENT 传输框架。它通过 Tang Runtime（`tangrt`）提供设备内存分配、指针属性查询、Peer Copy 与 IPC 句柄相关能力，并在 TENT 中以 `SUNRISE_LINK` 传输类型注册。

在运行时，当编译开启 `USE_SUNRISE` 且配置项 `transports/sunrise_link/enable=true` 时，`TransferEngineImpl` 会加载 Sunrise Link Transport。

### 传输流水线

1. **初始化**：`dlopen` 加载 `libptml_shared.so` + `libtangrt_shared.so`，初始化 PTML。
2. **拓扑检测**：`ptmlPtlinkPhytopoDetect` 发现芯片间端口，构建 C2C 映射（`local_chipid, remote_chipid → local_port`）。
3. **内存注册**：`tangIpcGetMemHandle` 生成 GPU 缓冲区 IPC 句柄。
4. **传输执行**：同设备 → `tangMemcpy`；跨设备 → `tangMemcpyPeer` / `tangMemcpyPeer_v2`，回退 `tangDeviceGetPeerPointer` + C2C；远程 → `tangIpcOpenMemHandle` + peer copy；主机 ↔ 设备 → `tangMemcpy` + 方向。
5. **异步路径**：传输大小超过 `async_memcpy_threshold` 时，使用 `tangMemcpyAsync` + 每设备 stream。

---

## 新增依赖
Sunrise Link Transport 在 Mooncake 基础依赖外，额外依赖 Tang Runtime：

- **头文件路径**：`/usr/local/tangrt/include`
- **库路径**：`/usr/local/tangrt/lib/linux-x86_64`（或扁平布局 `/usr/local/tangrt/lib`）
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

如果 Tang Runtime 安装在非默认路径：

```bash
cmake .. -DUSE_TENT=ON -DUSE_SUNRISE=ON -DMC_TANGRT_ROOT=/opt/tangrt
```

---

## 运行与测试

### 使用 `transfer_engine_bench`（经典 TE 后端）

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

### 使用 `tebench`（TENT 后端）

角色由 `--target_seg_name` 决定：为空 → 目标端，否则 → 发起端。

```bash
# 终端 1：目标端
./tebench --backend=tent --metadata_type=p2p --xport_type=sunrise_link \
  --seg_type=VRAM --local_gpu_id=0

# 终端 2：发起端
./tebench --backend=tent --metadata_type=p2p --xport_type=sunrise_link \
  --seg_type=VRAM --target_seg_name=192.168.172.52:168 \
  --local_gpu_id=1 --target_gpu_id=0
```

---

## 环境变量

| 变量 | 描述 | 默认值 |
|------|------|--------|
| `MC_TANGRT_ROOT` | Tang Runtime 根目录 | `/usr/local/tangrt` |
| `MC_TANGRT_LIB_DIR` | `.so` 目录路径，优先于 `MC_TANGRT_ROOT` + 架构查找 | （未设置） |
| `MC_TANGRT_LIB_ARCH` | `<root>/lib/` 下的架构子目录 | `linux-x86_64` |

---

## 配置项说明（TENT）

Sunrise Link 支持通过配置文件调整行为，常用项包括：

| 选项 | 类型 | 描述 | 默认值 |
|------|------|------|--------|
| `transports/sunrise_link/enable` | bool | 是否启用该传输 | `true` |
| `transports/sunrise_link/async_memcpy_threshold` | int | 异步 memcpy 最小传输大小（MiB）。设为 0 禁用。 | `0` |
| `transports/sunrise_link/enable_port_striping` | bool | 为大型跨设备拷贝（>4 MiB）启用多端口条带化 | `false` |

可根据业务负载与设备拓扑微调上述参数。

---

## 注意事项

1. **设备上下文**：内部管理 `tangSetDevice`。直接调用 Tang API 时，需在查询指针属性前设置正确设备。
2. **IPC 句柄生命周期**：IPC 句柄缓存复用，在 `uninstall()` 中释放。
3. **并发 Peer Copy**：每设备互斥锁保护上下文操作。跨设备拷贝按固定顺序（较低设备 ID 优先）获取源/目标锁以防死锁。
4. **映射主机指针规范化**：`tangHostAlloc` 的主机指针若有对应设备指针，自动规范化以选择正确的 peer copy 路径。
