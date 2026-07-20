# Offload + Promotion 冷热交换机制验证指南

## 验证目标

验证 Mooncake 的完整冷热数据交换循环：

```
MEMORY ──Offload──→ LOCAL_DISK ──Promotion──→ MEMORY
  │                      │                      │
  └── Eviction           └── Load (SSD→Client)  └── 热数据回到内存
```

## 前置条件

- 编译完成 mooncake_store（含 `mooncake_master` 可执行文件）
- 编译完成 mooncake-wheel（含 Python `mooncake.store` 模块）
- 安装 Python 3

## 验证脚本

```bash
python tests/verify_offload_promotion.py --test exchange
```

## 默认规模

DDR = **自动计算**（`800 × 1MB × 0.6` ≈ 480MB），Value = 1MB，NumKeys = 800（≈800MB），每批 30 个 key 后暂停 3s。显式设 `SEGMENT_SIZE_BYTES` 环境变量则跳过自动计算。

## 两个流水线瓶颈

### Offload 瓶颈：KEYS_ULTRA_LIMIT 永久关闭

`file_storage.cpp:469-474`：`BatchOffload` 返回 `KEYS_ULTRA_LIMIT` 时，`enable_offloading_` 被**永久设为 false**。后续所有心跳向 Master 发 `enable_offloading=false`，Master 直接清空队列不返回 key。在队列中的 key 永远失去落盘机会。

### Promotion 瓶颈：kMaxPerHeartbeat=1

`master_service.cpp:2991`：每次心跳只返回 1 个 promotion 任务。注释说明"多于一个可能阻塞超过 client-liveness 窗口"。180s 等待最多 ~180 次 promotion，覆盖 160 个 hot key 绰绰有余。

### Phase 3 冷读副作用

冷 key 的随机采样读可能触发 promotion（Count-Min Sketch 达到 `promotion_admission_threshold`），导致冷 key 进入 MEMORY。减小 `min(3, ...)` 可缓解。

## 注意事项

- **每次测试前清空 SSD 目录**：`rm -rf <SSD_PATH> && mkdir -p <SSD_PATH>`
- **MOONCAKE_OFFLOAD_HEARTBEAT_INTERVAL_SECONDS 必须设为 1**：默认 10s 会导致 offload/promotion 延迟过长
- **MOONCAKE_OFFLOAD_BUCKET_SIZE_LIMIT_BYTES 设小（如 10MB）**：默认 256MB 太大，测试数据量不够一个桶，不会落盘
- **put() 不抛异常**：`store.put()` 返回整数状态码（0=成功，非0=失败）
- **promotion_on_hit 必须在 Master 端启用**：不是 Client 端参数
- **LOCAL_HOSTNAME 无须设置**：与 ssd_balance 测试一样使用默认值 `localhost`。`127.0.0.1` 反而会导致 `Client::Create` 在 Windows 上失败（`real_client.cpp:710`）

---

## 验证：冷热交换闭环

混合写入热 key（20%）和冷 key（80%）→ overflow DDR → eviction + offload 将所有 key 推入 LOCAL_DISK → 热 key 被反复访问触发 promotion 回到 MEMORY → 冷 key 留在 SSD。

### 机制说明

1. 写入 800 个 key（160 hot, 640 cold），分 27 批，批间 3s 暂停
2. Eviction + offload：DDR 超过 70% 水位触发 eviction，候选 key 入 offload 队列 → 心跳落盘
3. 热 key 各读 4 次 → `TryPushPromotionQueue` 触发 → promotion 队列
4. 冷 key 各读 0-1 次 → 不触发 promotion
5. 等待 180s promotion 心跳结束后：
   - 热 key：应恢复 MEMORY 副本（promoted）
   - 冷 key：应保持在 LOCAL_DISK-only 状态

**Terminal 1** — 启动 Master：

```bash
mooncake_master \
    --port=50053 \
    --http_metadata_server_port=8880 \
    --enable_http_metadata_server=true \
    --metrics_port=9104 \
    --enable_offload=true \
    --offload_on_evict=true \
    --promotion_on_hit=true \
    --promotion_admission_threshold=2 \
    --default_kv_lease_ttl=2000 \
    --eviction_high_watermark_ratio=0.70
```

**Terminal 2** — 运行验证脚本：

```bash
MC_METADATA_SERVER=http://127.0.0.1:8880/metadata \
MOONCAKE_OFFLOAD_HEARTBEAT_INTERVAL_SECONDS=1 \
MOONCAKE_OFFLOAD_FILE_STORAGE_PATH=/tmp/mooncake_offload_promotion \
MOONCAKE_OFFLOAD_BUCKET_SIZE_LIMIT_BYTES=10485760 \
python tests/verify_offload_promotion.py --test exchange --master 127.0.0.1:50053
```

### 预期观察

```
After promotion cycle (160 hot, 640 cold):
  Hot  keys — MEMORY_only: X, LOCAL_DISK_only: Y, BOTH: Z, none: 0
  Cold keys — MEMORY_only: A, LOCAL_DISK_only: B, BOTH: C, none: 0
```

- 热 key 大部分获得 MEMORY 副本（`MEMORY_only + BOTH`）
- 冷 key 大部分停在 LOCAL_DISK-only
- 可能有少量热 key 未提升（promotion 队列排队中，受 `kMaxPerHeartbeat=1` 限制）
- 可能有少量冷 key 被提升到 MEMORY（Phase 3 随机采样读意外触发 promotion）

### 判断标准

- 至少有一些 hot key 被提升到 MEMORY
- 冷 key 大部分留在 LOCAL_DISK-only
- 脚本输出 `[PASS] Cold-Hot Exchange`

## 关键环境变量

| 变量 | 推荐值 | 说明 |
|------|--------|------|
| `MC_METADATA_SERVER` | `http://127.0.0.1:8880/metadata` | HTTP 元数据服务器地址 |
| `MOONCAKE_OFFLOAD_HEARTBEAT_INTERVAL_SECONDS` | `1` | **必须设为 1**，默认 10s 太慢 |
| `MOONCAKE_OFFLOAD_FILE_STORAGE_PATH` | 测试专用目录 | 每次测试前清空 |
| `MOONCAKE_OFFLOAD_BUCKET_SIZE_LIMIT_BYTES` | `10485760`（10MB） | 必须设小，默认 256MB 导致不足一桶不落盘 |
| `SEGMENT_SIZE_BYTES` | 自动计算（默认 ≈480MB） | DDR = `num_keys × value_size × 0.6`，显式设置则优先 |

## 关键 Master 启动参数

| 参数 | 推荐值 | 说明 |
|------|--------|------|
| `--enable_offload` | `true` | 总开关 |
| `--offload_on_evict` | `true` | eviction 时触发 offload |
| `--promotion_on_hit` | `true` | 启用热数据提升 |
| `--promotion_admission_threshold` | `2` | 访问频率阈值 |
| `--eviction_high_watermark_ratio` | `0.70` | 降低水位提前触发 eviction |
| `--default_kv_lease_ttl` | `2000` | 缩短 lease 加速淘汰 |

## 日志观察方法

**Client 侧 offload 日志**（`GLOG_v=1`）：

```
V... file_storage.cpp:...] Group objects with total object count: ...
V... file_storage.cpp:...] OffloadObjects completed: keys=..., time=...us
```

**Client 侧 promotion 日志**（`GLOG_v=1`）：

```
V... file_storage.cpp:...] ProcessPromotionTasks: got N promotion tasks
V... file_storage.cpp:...] Promotion completed for key=...
```

**Master 侧 promotion 日志**（`--v=1`）：

```
V... master_service.cpp:...] Promotion task enqueued for key=...
V... master_service.cpp:...] PromotionAllocStart: key=..., size=...
V... master_service.cpp:...] NotifyPromotionSuccess: key=... promoted to MEMORY
```

**观察 offload 落盘进度**：

```bash
watch -n 1 'find /tmp/mooncake_offload_promotion -name "*.bucket" -exec du -sh {} \;'
```

## 故障排查

| 现象 | 可能原因 | 解决方案 |
|------|----------|----------|
| `Failed to create client on port`（real_client.cpp:710） | `LOCAL_HOSTNAME` 设为了不可解析的地址或端口冲突 | **不要设置 `LOCAL_HOSTNAME`**，使用默认 `localhost` |
| No LOCAL_DISK replicas after offload wait | 数据量不足一个 bucket / 心跳间隔太长 | 设 `MOONCAKE_OFFLOAD_BUCKET_SIZE_LIMIT_BYTES=10485760` 和 `MOONCAKE_OFFLOAD_HEARTBEAT_INTERVAL_SECONDS=1` |
| No LOCAL_DISK-only keys（全部 memory_only） | DDR 太大未触发 eviction | 增加 `--num-keys` 或降低 `SEGMENT_SIZE_BYTES` |
| No promotion even after repeated reads | Master 未启动 `promotion_on_hit` / 等待不够 | 确认 `--promotion_on_hit=true`，增加 `PROMOTION_WAIT_SECONDS` |
| `store.get()` 返回错误 | Lease 已过期被 eviction 且未成功 offload | 降低 `eviction_high_watermark_ratio` |
| Segmentation fault (core dump) in teardown | 大量 key 逐条 remove RPC 与 heartbeat 并发竞争 | 脚本已改用 `safe_cleanup` 分批删除 |

## 可调参数速查

### Offload 频率

心跳间隔 = `FileStorageConfig::heartbeat_interval_seconds`，环境变量 `MOONCAKE_OFFLOAD_HEARTBEAT_INTERVAL_SECONDS`（默认 10s）。

```
export MOONCAKE_OFFLOAD_HEARTBEAT_INTERVAL_SECONDS=1   # 每 1s 拉一次 offload 任务
```

**原理**：`file_storage.cpp:286-292`，Init 中启动 heartbeat 线程，每次 `Heartbeat()` 后 `sleep(interval)`。`Heartbeat()` 内部**同步**执行：`OffloadObjectHeartbeat` RPC → `OffloadObjects()` SSD 写入 → `NotifyOffloadSuccess` RPC。

### Offload 每轮吞吐量

`OffloadObjectHeartbeat`（`master_service.cpp:2670-2677`）**无数量限制**——直接 `std::move` 整个 `offloading_objects` 返回。吞吐量实际受以下因素约束：

| 参数 | 环境变量 | 默认值 | 位置 |
|------|----------|--------|------|
| 桶大小上限 | `MOONCAKE_OFFLOAD_BUCKET_SIZE_LIMIT_BYTES` | 256MB | `storage_backend.h:181-182` |
| 桶 key 上限 | `MOONCAKE_OFFLOAD_BUCKET_KEYS_LIMIT` | 500 | `storage_backend.h:184` |
| 总 key 上限 | `MOONCAKE_OFFLOAD_TOTAL_KEYS_LIMIT` | 10M | `FileStorageConfig` |
| 总容量上限 | `MOONCAKE_OFFLOAD_TOTAL_SIZE_LIMIT_BYTES` | 2TB | `FileStorageConfig` |
| Staging buffer | `MOONCAKE_OFFLOAD_LOCAL_BUFFER_SIZE_BYTES` | 1280MB | `FileStorageConfig` |

**桶分组机制**（`storage_backend.cpp:1880`, `GroupOffloadingKeysByBucket`）：心跳返回的 key 按桶大小和数量分组。每次写满一桶就 `BuildBucket` → 落盘 → `NotifyOffloadSuccess`。**凑不满一桶的留在 `ungrouped_offloading_objects_`，等下次心跳凑满**。

### KEYS_ULTRA_LIMIT 保护机制

`file_storage.cpp:469-474`：`BatchOffload` 返回 `KEYS_ULTRA_LIMIT` 时 `enable_offloading_` **永久 false**。触发条件来自 `IsEnableOffloading()`（bucket backend 检查 `total_size + bucket_size_limit > total_size_limit`）。

**后果**：后续所有心跳向 Master 发 `enable_offloading=false` → Master 清空队列 → 仍在队列中的 key **永久失去落盘机会**。

### Promotion 频率

`master_service.cpp:2991` — **硬编码** `constexpr size_t kMaxPerHeartbeat = 1`。每次 `PromotionObjectHeartbeat` 最多返回 1 个任务。**不能通过配置修改**，需改 C++ 源码后重新编译。

**变通方案**：增大 `PROMOTION_WAIT_SECONDS`。每个心跳处理 1 个 key，N 个 hot key 需 ~N 秒。

### Promotion 准入门控

`master_service.cpp:2876-2930`（`TryPushPromotionQueue`）四重门控：

| 门控 | 参数 | 说明 |
|------|------|------|
| 频率 | `--promotion_admission_threshold`（默认 2） | Count-Min Sketch 访问次数 |
| 水位 | `--eviction_high_watermark_ratio`（默认 0.85） | DRAM 使用率低于此阈值才允许 promotion |
| 去重 | 无 | 已有 MEMORY 副本或进行中 task → 跳过 |
| 容量 | `--promotion_queue_limit`（默认 50000） | 全局进行中 task 上限 |

### Offload 与 Eviction 频率（Master 侧）

| 线程 | 触发方式 | 周期 | 位置 |
|------|----------|------|------|
| Eviction 线程 | DDR 水位 > `eviction_high_watermark_ratio` | `kEvictionThreadSleepMs` | `master_service.cpp:3212` |
| NOF heartbeat 线程 | 编译时 `USE_NOF` 宏 | `DEFAULT_NOF_HEARTBEAT_INTERVAL_SEC` | `master_service.cpp:287` |
