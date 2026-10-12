# MemFabric Transport

MemFabric Transport 的源代码位于 `Mooncake/mooncake-transfer-engine/tent/src/transport/memfabric`，是 TENT 框架下的数据传输后端，通过 dlopen 动态加载 memfabric 共享库（`libmf_smem.so`），为 Ascend NPU 提供 SDMA/RDMA 数据传输能力。

## 概述

MemFabric 是华为提供的共享内存传输框架，基于 smem_trans API 实现跨节点/跨设备的批量数据拷贝。MemFabric Transport 将其封装为 TENT Transport 接口，具有以下特点：

- **动态加载**：通过 dlopen/dlsym 加载 `libmf_smem.so`，编译时不依赖 memfabric 头文件
- **批量传输**：支持 `smem_trans_batch_copy` 批量异步传输，减少调用开销
- **TENT 集成**：作为 TENT transport 的一种，自动参与传输选择和故障切换
- **多协议支持**：支持 `device_sdma`、`device_rdma`、`host_rdma` 等底层操作类型

### 工作原理

```
┌─────────────────────────────────────────────┐
│  TENT TransferEngine                        │
│  - transport_selector 选择 memfabric        │
├─────────────────────────────────────────────┤
│  MemFabricTransport                         │
│  - install: 加载库 → 创建 smem_trans 实例   │
│  - submitTransferTasks: 批量提交 batch_copy │
│  - getTransferStatus: 轮询 batch 状态       │
│  - addMemoryBuffer: 注册本地内存            │
├─────────────────────────────────────────────┤
│  MemFabricUnderApi (dlopen 封装)            │
│  - LoadLibrary / CleanupLibrary             │
│  - 13 个 smem_trans API 函数指针            │
├─────────────────────────────────────────────┤
│  libmf_smem.so (memfabric 共享库)           │
│  - smem_trans_init / create / batch_copy    │
└─────────────────────────────────────────────┘
```

### 关键组件

| 组件 | 说明 |
|------|------|
| `MemFabricTransport` | 实现 TENT Transport 接口，管理 smem_trans 实例生命周期 |
| `MemFabricUnderApi` | 全静态类，封装 dlopen/dlsym，提供 13 个 API 的 inline 转发 |
| `MemFabricSubBatch` | 批量任务容器，包含 task_list、batch_status_cache、pending_batch_count |

## 前置条件

### 硬件与软件

- **硬件**：Atlas A2/A3/A5 系列 NPU
- **CANN**：>= 9.0.0
- **memfabric**：`libmf_smem.so` 已安装并在 `LD_LIBRARY_PATH` 中

### 安装 memfabric

确保 `libmf_smem.so` 已安装并在 `LD_LIBRARY_PATH` 中。安装方式请参考 memfabric 项目文档。

验证：
```bash
# 确认库可被找到
ldconfig -p | grep libmf_smem
```

## 编译

### 启用 MemFabric Transport

```bash
cmake -S . -B build -G Ninja \
    -DUSE_TENT=ON \
    -DUSE_MEMFABRIC=ON \
    -DUSE_HTTP=ON \
    -DBUILD_SHARED_LIBS=ON \
    -DBUILD_UNIT_TESTS=ON \
    -DCMAKE_BUILD_TYPE=Release

cmake --build build --parallel
sudo cmake --install build
```


## 环境变量配置

| 变量 | 必填 | 默认值 | 说明 |
|------|------|--------|------|
| `MC_USE_TENT` | 是 | — | 设为 `1` 启用 TENT 模式 |
| `MC_TENT_CONF` | 是 | — | TENT 配置 JSON 文件路径，需包含 `transports/memfabric/enable: true` |
| `LD_LIBRARY_PATH` | 是 | — | 包含 `libmf_smem.so` 所在目录 |
| `MC_STORE_USE_HUGEPAGE` | 否 | — | 设为 `1` 启用 Store 大页分配（使用 Store 池化时建议开启） |
| `MC_STORE_HUGEPAGE_SIZE` | 否 | `2MB` | 大页大小，可选 `2MB` / `512MB` / `1GB`，需与系统实际分配的大页大小匹配 |

### TENT 配置文件示例

```json
{
    "transports": {
        "memfabric": {
            "enable": true,
            "data_op_type": "device_rdma"
        }
    }
}
```

`data_op_type` 可选：`device_sdma` / `device_rdma` / `host_rdma` / `device_urma` / `device_uboe`。

### 基础配置

```bash
export MC_USE_TENT=1
export MC_TENT_CONF=/path/to/memfabric_tent.json
export LD_LIBRARY_PATH=/usr/local/memfabric_hybrid/latest/aarch64-linux/lib64:$LD_LIBRARY_PATH
source /usr/local/Ascend/ascend-toolkit/set_env.sh
```

## 验证

### 检查 Transport 注册

启动后查看日志，应出现以下信息：

```
I ... memfabric_transport.cpp: ...] MemFabricTransport installed: url=...
```

### 单元测试

```bash
# 编译时需 USE_MEMFABRIC=ON + BUILD_UNIT_TESTS=ON
ctest --test-dir build -R 'memfabric' --output-on-failure
```

测试覆盖：

| 测试 | 说明 |
|------|------|
| `InstallSucceeds` | install 成功，smem_trans 实例创建 |
| `InstallFailsWhenCreateReturnsNull` | 创建失败时的错误处理 |
| `SubmitAndGetStatusCompleted` | 批量提交 + 状态查询 + 缓存命中 |
| `SubmitAndGetStatusPending` | 传输进行中状态 |
| `FailedBatchPropagatesToAllTasks` | 失败传播到同 batch 所有 task |
| `PendingBatchDestructorWarning` | 析构时未完成 batch 告警 |
| `AddRemoveMemoryBuffer` | 内存注册/注销 |
| `AddMemoryBufferFailsBeforeInstall` | 未安装时的错误处理 |
| `SubmitFailsWhenBatchCopyFails` | batch_copy 失败时的错误处理 |

测试使用 mock ACL 函数和 mock `MemFabricUnderApi`，不依赖真实 memfabric 硬件。

## 注意事项

1. **`libmf_smem.so` 必须在 `LD_LIBRARY_PATH` 中**：MemFabric Transport 通过 dlopen 动态加载，找不到库会返回 `DeviceNotFound`。

2. **`data_op_type` 必须在 TENT 配置中设置**：未设置时 install 返回 `InvalidArgument`。

3. **`device_rdma` 模式需配置 NIC**：当 `data_op_type=device_rdma` 或 `host_rdma` 时，`cfg.nic` 字段会设为 `tcp://ip:port` 格式，IP 从 `HCCL_IF_IP` 环境变量获取，端口从 segment name 获取。

4. **大页配置**：使用 Store 池化（`protocol=ascend`）时需要预留大页。
   页大小取决于系统配置，可通过 `cat /proc/meminfo | grep Hugepagesize` 查看：
   ```bash
   # 查看系统支持的大页大小
   ls /sys/kernel/mm/hugepages/
   # 例如: hugepages-2048kB  hugepages-524288kB

   # 分配 2MB 大页（例如 200GB → 102400 页）
   echo 102400 > /sys/kernel/mm/hugepages/hugepages-2048kB/nr_hugepages

   # 或分配 512MB 大页（例如 200GB → 400 页）
   echo 400 > /sys/kernel/mm/hugepages/hugepages-524288kB/nr_hugepages

    # 持久化（重启不丢失）
    echo 'vm.nr_hugepages = 102400' > /etc/sysctl.d/99-hugepages.conf
    sysctl -p /etc/sysctl.d/99-hugepages.conf

    # 启用 Store 大页分配
    export MC_STORE_USE_HUGEPAGE=1
    export MC_STORE_HUGEPAGE_SIZE=2MB
    ```
   `MC_STORE_HUGEPAGE_SIZE` 可选 `2MB` / `512MB` / `1GB`，需与系统实际分配的大页大小匹配。

5. **与 `USE_ASCEND_DIRECT` 解耦**：`USE_MEMFABRIC` 可独立启用，不需要同时启用 `USE_ASCEND` 或 `USE_ASCEND_DIRECT`。

6. **日志级别**：MemFabric Transport 的日志级别跟随 TENT 的 `log_level` 配置，不需要单独的环境变量。

7. **设备设置**：调用 `TransferEngine.initialize()` 之前需设置设备（如 `torch.npu.set_device(0)`），`aclrtGetDevice` 用于获取设备 ID。

## 故障排查

### `DeviceNotFound: libmf_smem.so unavailable`

```
MemFabricUnderApi::LoadLibrary failed, libmf_smem.so unavailable
```

**解决**：确认 `libmf_smem.so` 在 `LD_LIBRARY_PATH` 中：
```bash
find / -name "libmf_smem.so" 2>/dev/null
export LD_LIBRARY_PATH=/usr/local/memfabric_hybrid/latest/aarch64-linux/lib64:$LD_LIBRARY_PATH
```

### `InvalidArgument: data_op_type must be set`

**解决**：在 TENT 配置文件中设置 `data_op_type`：
```json
{
    "transports": {
        "memfabric": {
            "enable": true,
            "data_op_type": "device_rdma"
        }
    }
}
```

### `session not found` (smem_trans 内部错误)

```
ERROR [SMEM smem_trans_connection.cpp] session:(...) not found
```

**原因**：两端 `smem_trans_role` 相同导致不记录对端内存映射。

**解决**：确保两端使用不同的 role（由 memfabric 库内部处理）。

### `mmap failed, errno=12 (Cannot allocate memory)`

**原因**：大页不足。

**解决**：
```bash
# 查看系统支持的大页大小
ls /sys/kernel/mm/hugepages/
# 按需分配对应大小的页
echo <页数> > /sys/kernel/mm/hugepages/hugepages-<页大小>kB/nr_hugepages
```
