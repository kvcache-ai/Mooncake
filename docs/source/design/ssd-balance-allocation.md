# SSD负载均衡分配策略设计文档

## 1. 概述

### 1.1 问题背景

现有 `FreeRatioFirstAllocationStrategy` 在选择segment时只考虑DDR空闲比例，忽略了SSD水位。这导致以下问题：

- 一个segment的DDR空闲但SSD已满时，数据仍被分配到该segment
- 后续eviction时无法offload到SSD（因为SSD已满），DDR产生backpressure
- 最终DDR被填满，整个节点无法接受新写入

### 1.2 解决方案

新增 `SsdBalanceAllocationStrategy`，按SSD空闲比例做负载均衡：

- 默认只看SSD水位（alpha=0），优先选择SSD空闲的节点
- SSD达到高水位时禁止向该节点写入，但不驱逐SSD数据
- DDR达到驱逐水位时临时禁止写入，水位下降后自动恢复

### 1.3 适用场景

多节点集群中每个节点有DDR+本地SSD的分层存储环境。

## 2. 设计目标

| 目标 | 说明 |
|------|------|
| SSD比例均衡 | 按SSD空闲比例选择segment，优先写入SSD空闲的节点 |
| SSD驱逐保护 | SSD达到高水位时禁止写入，绝不驱逐SSD数据（避免数据丢失） |
| DDR准入控制 | 每个segment的DDR达到准入水位时禁止向该segment分配，自动fallback到其他segment |
| 全满暂停 | 所有节点DDR都满时暂停所有put，返回 DDR_ADMISSION_REJECTED（-201），不触发eviction |

## 3. 核心算法

### 3.1 SSD比例计算

```
ssd_free_ratio = (ssd_total_capacity - ssd_used_bytes) / ssd_total_capacity
```

- 无SSD信息的segment：`ssd_free_ratio = 1.0`（不约束）
- `ssd_used_bytes` 通过 `std::atomic<int64_t>` 跟踪，在offload成功时递增，磁盘驱逐时递减

### 3.2 候选采样与排序

```
1. 采样 min(6 * replica_num, total_segments) 个候选segment
2. 排除SSD使用率 >= ssd_high_watermark_ratio 的segment
3. 按ssd_free_ratio降序排序
4. 从top-N候选中尝试分配
5. 如果replica_num未满足，fallback到随机分配
```

### 3.3 SSD高水位保护

当segment的SSD使用率 >= `ssd_high_watermark_ratio`（默认0.90）时：

- **禁止**向该segment分配新数据
- **绝不驱逐**SSD上的已有数据（驱逐意味着数据不可恢复丢失）
- SSD数据只能通过以下方式释放：
  - 正常promotion（访问命中后提升回DDR）
  - TTL过期（软pin到期后自动清理）
- SSD水位下降后，节点自动恢复可写状态

### 3.4 DDR写入准入控制（per-segment）

通过 `--ddr_admission_watermark_ratio`（默认 0.0，即禁用）设定每个 segment 的 DDR 准入水位。
当 segment 的 DDR 使用率 >= 该水位时：

- 分配策略**跳过**该 segment，尝试分配到其他 segment
- 所有 segment 都被跳过时，返回 `DDR_ADMISSION_REJECTED`（-201）
- **不设置** `need_mem_eviction_`（避免触发 eviction 驱逐已有数据，DDR 数据零丢失）
- 其他 segment DDR 下降（eviction 释放空间或 offload 完成）后自动恢复

与 eviction 的关系：
- `ddr_admission_watermark_ratio`（如 0.90）应设得**低于** `eviction_high_watermark_ratio`（0.95）
- 准入阻写先于 eviction 驱逐发生，保护 DDR 数据不被驱逐
- 如果所有 segment 都超过准入水位也无 eviction 触发，put 暂停直到有 segment 释放空间

使用方式：

```bash
./mooncake_master --allocation_strategy=ssd_balance \
    --ddr_admission_watermark_ratio=0.90
```

## 4. 决策流程

### 4.1 AllocateAndInsertMetadata流程

```
AllocateAndInsertMetadata()
│
├── 获取AllocatorManager和SsdMetricsProvider
│
├── 调用 SsdBalanceAllocationStrategy::Allocate()
│   │
│   ├── 处理preferred segments
│   │   ├── 检查SSD水位，跳过高水位segment
│   │   └── 检查DDR准入水位，跳过超标segment
│   │
│   ├── 候选采样 + SSD比例排序
│   │   ├── 排除excluded/used segments
│   │   ├── 排除SSD高水位segments
│   │   ├── 排除DDR准入水位超标的segments
│   │   └── 按ssd_free_ratio降序排序，取top-N
│   │
│   ├── Fallback随机分配
│   │   └── 同样排除SSD高水位和DDR准入超标segments
│   │
│   └── 返回结果
│       ├── 有可用segment → replicas
│       ├── 被DDR准入拒绝 → DDR_ADMISSION_REJECTED（-201）
│       │   └── 不设need_mem_eviction_，保护DDR数据
│       └── 其他原因失败 → NO_AVAILABLE_HANDLE（-200）
│           └── 设need_mem_eviction_，触发eviction释放空间
│
└── 返回结果给客户端
```

### 4.2 SSD水位检查

```
isSsdHighWatermark(segment_name)
│
├── 查询SsdMetricsProvider
│   ├── total = getSsdTotalCapacity(segment_name)
│   └── used = getSsdUsedBytes(segment_name)
│
├── total <= 0?
│   └── 返回false（无SSD信息，不阻塞）
│
└── used/total >= ssd_high_watermark_ratio?
    ├── YES → 排除该segment
    └── NO  → 允许分配
```

### 4.3 DDR准入水位检查

```
isDdrHighWatermark(segment_name)
│
├── ddr_admission_watermark_ <= 0.0?
│   └── 返回false（未启用DDR准入）
│
├── ddr_admission_watermark_ >= 1.0?
│   └── 返回false（显式禁用）
│
├── 查询SsdMetricsProvider
│   └── ratio = getDdrUsedRatio(segment_name)
│       └── MasterMetricManager.get_segment_mem_used_ratio()
│
└── ratio >= ddr_admission_watermark_?
    ├── YES → 排除该segment
    └── NO  → 允许分配
```

## 5. SSD使用量追踪

### 5.1 数据结构

`LocalDiskSegment` 新增字段：

```cpp
std::atomic<int64_t> ssd_used_bytes{0};
```

### 5.2 更新时机

| 事件 | 操作 | 触发位置 |
|------|------|----------|
| offload成功 | `ssd_used_bytes += data_size` | `NotifyOffloadSuccess` |
| 磁盘replica被驱逐 | `ssd_used_bytes -= object_size` | `EvictDiskReplica` |

### 5.3 暴露接口

通过 `SsdMetricsProvider` 接口：

```cpp
class SsdMetricsProvider {
    virtual int64_t getSsdTotalCapacity(const std::string& segment_name) const = 0;
    virtual int64_t getSsdUsedBytes(const std::string& segment_name) const = 0;
    virtual double getDdrUsedRatio(const std::string& segment_name) const {
        return 0.0;  // 默认不检查DDR
    }
};
```

`ScopedLocalDiskSegmentAccess` 实现该接口，通过 segment_name → client_id → LocalDiskSegment 查找。

## 6. 配置参数

### 6.1 Master 启动参数

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `--allocation_strategy` | `random` | 设为 `ssd_balance` 启用本策略 |
| `--ssd_high_watermark_ratio` | `0.90` | SSD使用率上限，超过则禁止向该节点写入 |
| `--ddr_admission_watermark_ratio` | `0.0` | DDR准入水位（0.0 = 禁用），低于此值则禁止向该segment分配 |

### 6.2 环境变量（存储后端驱逐保护）

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `MOONCAKE_OFFLOAD_DISABLE_SSD_EVICTION` | `false` | 强制禁止SSD驱逐，即使 eviction_policy 非 NONE 也不驱逐 |

### 6.3 错误码

| 错误码 | 值 | 触发条件 |
|--------|-----|----------|
| `NO_AVAILABLE_HANDLE` | -200 | 分配失败（段满或其他原因），触发 eviction |
| `DDR_ADMISSION_REJECTED` | -201 | DDR准入水位拒绝分配，**不触发** eviction |

启用方式：

```bash
./mooncake_master --allocation_strategy=ssd_balance \
    --ssd_high_watermark_ratio=0.90 \
    --ddr_admission_watermark_ratio=0.90
```

## 7. 代码结构

### 7.1 新增/修改文件

| 文件 | 变更类型 | 说明 |
|------|----------|------|
| `include/allocation_strategy.h` | 修改 | 新增 `SsdMetricsProvider` 接口（含 `getDdrUsedRatio`）、`SsdBalanceAllocationStrategy` 类（含 `isDdrHighWatermark`）、更新工厂函数 |
| `include/types.h` | 修改 | `AllocationStrategyType` 枚举新增 `SSD_BALANCE`；新增 `DDR_ADMISSION_REJECTED` 错误码 |
| `include/segment.h` | 修改 | `LocalDiskSegment` 新增 `ssd_used_bytes`；`ScopedLocalDiskSegmentAccess` 实现 `SsdMetricsProvider`（含 `getDdrUsedRatio`） |
| `src/segment.cpp` | 修改 | 实现 `getSsdTotalCapacity`、`getSsdUsedBytes`、`getDdrUsedRatio` |
| `include/master_config.h` | 修改 | 新增 `ssd_high_watermark_ratio`、`ddr_admission_watermark_ratio` 配置字段 |
| `src/master.cpp` | 修改 | 新增 `--ssd_high_watermark_ratio`、`--ddr_admission_watermark_ratio` gflag |
| `include/master_service.h` | 修改 | 新增 `ssd_high_watermark_ratio_` 成员 |
| `src/master_service.cpp` | 修改 | 分配策略传 SSD/DDR provider、SSD使用量追踪、分发 DDR_ADMISSION_REJECTED（不触发 eviction） |
| `src/client_service.cpp` | 修改 | 处理 `DDR_ADMISSION_REJECTED` 错误码（日志 + 重试） |
| `include/storage_backend.h` | 修改 | `BucketBackendConfig` 新增 `disable_ssd_eviction` 字段 |
| `src/storage_backend.cpp` | 修改 | `PrepareEviction` 检查 `disable_ssd_eviction`；`IsEnableOffloading` 跳过eviction分支 |

### 7.2 类继承关系

```
AllocationStrategy (抽象基类)
├── RandomAllocationStrategy
│   └── FreeRatioFirstAllocationStrategy
│       └── SsdBalanceAllocationStrategy  ← 新增
└── CxlAllocationStrategy

SsdMetricsProvider (抽象接口)
└── ScopedLocalDiskSegmentAccess  ← 新增实现
```

## 8. 验证方案

详见 `mooncake-wheel/tests/verify_ssd_balance.py` 和 `tests/ssd_balance_test_guide.md`。

| 测试 | 验证内容 |
|------|----------|
| `load_balancing` | 2个Client不对称SSD，验证数据按SSD空闲比例分布 |
| `ssd_high_watermark_blocking` | SSD达到90%高水位后offload完成，验证新分配被拒绝 + 初始数据可读 |
| `ssd_eviction_protection` | 启用FIFO驱逐+`MOONCAKE_OFFLOAD_DISABLE_SSD_EVICTION=true`，验证已有SSD数据不被驱逐 |
| `ddr_admission` | 设置 `--ddr_admission_watermark_ratio=0.90`，DDR满时拒绝写入，不触发eviction |
| `all_ssd_full` | 所有节点SSD满后全局拒绝，释放后恢复 |
