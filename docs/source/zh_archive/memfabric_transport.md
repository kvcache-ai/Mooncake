# MemFabric Transport

MemFabric Transport 的源代码位于 `Mooncake/mooncake-transfer-engine/tent/src/transport/memfabric`，是 TENT 框架下的数据传输后端，通过 dlopen 动态加载 memfabric 共享库（`libmf_smem.so`），为 Ascend NPU 提供 SDMA/RDMA 数据传输能力。

## 概述

MemFabric 是华为提供的共享内存传输框架，基于 smem_trans API 实现跨节点/跨设备的批量数据拷贝。MemFabric Transport 将其封装为 TENT Transport 接口，具有以下特点：

- **动态加载**：通过 dlopen/dlsym 加载 `libmf_smem.so`，编译时不依赖 memfabric 头文件
- **批量传输**：支持 `smem_trans_batch_copy` 批量异步传输，减少调用开销
- **TENT 集成**：作为 TENT transport 的一种，自动参与传输选择和故障切换
- **多协议支持**：支持 `device_rdma`、`device_urma`、`device_sdma`、等底层操作类型

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

安装由兼容版本构建的 memfabric wheel（`MEMFABRIC_WHEEL` 指向该文件）：

```bash
pip install "$MEMFABRIC_WHEEL"  # 必须支持下文约定的四参数查询 ABI
```

安装后 `libmf_smem.so` 位于 Python site-packages 目录下。将其加入 `LD_LIBRARY_PATH`：

```bash
SITE_PKG=$(python3 -c "import site; print(site.getsitepackages()[0])")
export LD_LIBRARY_PATH=$SITE_PKG/memfabric_hybrid/lib:$LD_LIBRARY_PATH
```

验证：
```bash
ls $SITE_PKG/memfabric_hybrid/lib/libmf_smem.so
```

## 编译

### 启用 MemFabric Transport

```bash
cmake -S . -B build -G Ninja \
    -DUSE_TENT=ON \
    -DUSE_MEMFABRIC=ON \
    -DBUILD_UNIT_TESTS=OFF \
    -DBUILD_SHARED_LIBS=ON \
    -DCMAKE_BUILD_TYPE=Release

cmake --build build --parallel
cmake --install build
```

默认仅启用 Ascend / Ascend Direct（HIXL）或 MemFabric 中的一种。

## 环境变量配置

| 变量 | 必填 | 默认值 | 说明 |
|------|------|--------|------|
| `LD_LIBRARY_PATH` | 是 | — | 包含 `libmf_smem.so` 所在目录 |
| `MF_PROTOCOL` | 是 | — | 传输协议：`device_sdma` / `device_rdma` / `host_rdma` / `device_urma` / `device_uboe` |
| `MF_TRANSFER_TIMEOUT` | 否 | `10000` | quiesce 轮询超时时间（毫秒） |
| `MC_USE_TENT` | 是 | — | 设为 `1` 启用 TENT 模式 |
| `MC_TENT_CONF` | 是 | — | TENT 配置 JSON 文件路径，需包含 `transports/memfabric/enable: true` |
| `MC_STORE_USE_HUGEPAGE` | 否 | — | 设为 `1` 启用 Store 大页分配（使用 Store 池化时建议开启） |
| `MC_STORE_HUGEPAGE_SIZE` | 否 | `2MB` | 大页大小，可选 `2MB` / `512MB` / `1GB`，需与系统实际分配的大页大小匹配 |

### TENT 配置文件示例

```json
{
    "transports": {
        "memfabric": {
            "enable": true
        }
    }
}
```

### 基础配置

```bash
export MF_PROTOCOL=device_rdma
export MF_TRANSFER_TIMEOUT=10000
export MC_USE_TENT=1
export MC_TENT_CONF=/path/to/memfabric_tent.json
SITE_PKG=$(python3 -c "import site; print(site.getsitepackages()[0])")
export LD_LIBRARY_PATH=$SITE_PKG/memfabric_hybrid/lib:$LD_LIBRARY_PATH
source /usr/local/Ascend/ascend-toolkit/set_env.sh
```

## 验证

### 单元测试

```bash
# 在仓库根目录执行
cmake -S . -B build-memfabric-mock -G Ninja \
    -DUSE_TENT=ON -DUSE_MEMFABRIC=ON \
    -DBUILD_UNIT_TESTS=ON \
    -DWITH_STORE=OFF -DWITH_STORE_RUST=OFF
cmake --build build-memfabric-mock --parallel 2 --target \
    tent_memfabric_transport_test
ctest --test-dir build-memfabric-mock/mooncake-transfer-engine/tent/tests \
    -R '^tent_memfabric_transport_test$' --output-on-failure --no-tests=error --timeout 120
```


## 注意事项

1. **`libmf_smem.so` 必须在 `LD_LIBRARY_PATH` 中**：MemFabric Transport 通过 dlopen 动态加载，找不到库会返回 `DeviceNotFound`。

2. **`MF_PROTOCOL` 必须设置**：未设置时 install 返回 `InvalidArgument`。

3. **大页配置**：使用 Store 池化（`protocol=ascend`）时需要预留大页。
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

4. **日志级别**：MemFabric Transport 的日志级别跟随 TENT 的 `log_level` 配置，不需要单独的环境变量。

## 故障排查

### `DeviceNotFound: libmf_smem.so unavailable`

```
MemFabricUnderApi::LoadLibrary failed, libmf_smem.so unavailable
```

**解决**：确认 `libmf_smem.so` 在 `LD_LIBRARY_PATH` 中：
```bash
SITE_PKG=$(python3 -c "import site; print(site.getsitepackages()[0])")
ls $SITE_PKG/memfabric_hybrid/lib/libmf_smem.so
export LD_LIBRARY_PATH=$SITE_PKG/memfabric_hybrid/lib:$LD_LIBRARY_PATH
```

### `InvalidArgument: MF_PROTOCOL must be set`

**解决**：设置 `MF_PROTOCOL` 环境变量：
```bash
export MF_PROTOCOL=device_rdma
```

### `mmap failed, errno=12 (Cannot allocate memory)`

**原因**：大页不足。

**解决**：
```bash
# 查看系统支持的大页大小
ls /sys/kernel/mm/hugepages/
# 按需分配对应大小的页
echo <页数> > /sys/kernel/mm/hugepages/hugepages-<页大小>kB/nr_hugepages
```


## 完成状态约定

适配层依赖带执行结果输出参数的四参数接口
`smem_trans_batch_status_query(handle, batch_id, &result, flags)`。

`SM_NEED_RETRY` 表示未完成；`SM_OK` 表示结果可读取且该批次的全部内存访问
已经停止，包括执行失败的情况。成功任务计入请求长度，失败任务计入零个确认成功字节；
后者不表示目标内存未被部分修改，也不表示回滚。底层终态只查询一次，并在适配层缓存。

查询本身失败时返回查询错误，任务保持 PENDING，待完成计数不变。

`quiesce()` 拦截新提交，并在 `MF_TRANSFER_TIMEOUT` 到期前主动轮询在途任务，
复用正常状态查询更新任务状态和 `inflight_`。提交与查询使用同一把锁，
两轮查询之间的等待会释放锁。quiesce 返回后仍拒绝新提交，包括超时返回的情况。

超时返回错误，但不会将待完成任务改为终态，也不会清空在途计数；后续查询或再次
调用 quiesce 仍可确认完成。该超时不会取消底层操作，也无法抢占阻塞的底层调用。
当前引擎记录 quiesce 错误后仍继续清理，因此超时不代表缓冲区和批次已经可以安全回收。
