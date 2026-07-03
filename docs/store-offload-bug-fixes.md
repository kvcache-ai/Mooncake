# Mooncake Store SSD Offload Bug 修复记录

> 分支: `fix/store-offload-main` (基于 upstream `main` @ `85e0ed69`)
> 仓库: `pingzhuu/Mooncake`
> 最后更新: 2026-07-03

## 背景

store-nvme 部署（.15 master + 2 store-server, .18 2 store-server, 4× NVMe SSD）
在 AI_ERA_PRESSURE 压测中出现缓存命中率从 ~80% 断崖式下跌到 ~27% 的问题。
经多轮诊断后确认根因为 LRU eviction 在提交新 bucket 时使用了时间戳 0，
并伴有一组并发性能 / 元数据正确性 / 串行 I/O 瓶颈等问题。

本分支将旧分支 (`fix/store-offload-clean` 基于 `b57d18a4`) 的修复方案
逐 commit 重新应用到 upstream main，且每个修复独立成 commit 以便审阅与
cherry-pick。本文件按 commit 顺序记录每个修复。

## 已被上游覆盖（无需在本分支修改）

| Bug | 上游 PR / 说明 |
|-----|---------------|
| A — BatchEvict 重复迭代 / 过量淘汰 | PR #2286 (`be1ceba6`) — Phase 1/2 重构并列扫描候选 |
| B — offload cap 绑定 offload_force_evict_ | PR #2599 (`077b696f`) — `--offloading_queue_limit` / `--offload_cap_ratio` 已为 gflag |
| C — 跳过的 offload key 静默丢弃 → orphan task | PR #2658 (`70ad6c7d`) — `data_size=-1` NACK + NotifyOffloadSuccess 处理 |
| 0ca9f054 等价的 SSD 容量 re-report | upstream `file_storage.cpp` Heartbeat 中已有 MountLocalDiskSegment + ReportSsdCapacity 逻辑 |

## 本分支提交列表

### Bug D: partial bucket 永久滞留于 ungrouped_offloading_objects_

**commit**: `81ed7c08`
**文件**: `mooncake-store/src/storage_backend.cpp` `GroupOffloadingKeysByBucket`

**根因**: 当输入 offloading_objects 在凑满 `bucket_keys_limit` 之前耗尽，
旧逻辑把剩余 key 倒回 `ungrouped_offloading_objects_` 并 `return {}`。压测
high-watermark 之后每个 heartbeat 只有 ~151-491 key 到达，永远凑不满
1000 key/bucket → key 永久滞留 → master 端 offloading_task TTL 超
`put_start_release_timeout_sec` 而 expired。

**修复**: 输入耗尽时立即把已收集的 partial bucket 推入 `buckets_keys`
（带 `[OFFLOAD-PARTIAL]` 日志），不再回灌 ungrouped 池。代价是少量额外
bucket 元数据开销，换得正确性：不会无限积压。

---

### Bug H: AddReplica 不替换 stale LOCAL_DISK replica

**commit**: `7f57409c`
**文件**: `mooncake-store/src/master_service.cpp` `AddReplica`

**根因**: store-server 重启后 ScanMeta 回告时，`AddReplica` 的 `VisitReplicas`
predicate 要求 `client_id` 匹配；旧 client_id 的 replica 不匹配 → 新 replica
被静默丢弃，master 中残留 dead transport_endpoint。后续 GET 取不到新 transport
→ `INVALID_KEY` / RDMA 错误。

**修复**: 在检查是否已有 LOCAL_DISK replica 之前，erase 掉所有
`client_id != caller` 的 LOCAL_DISK replica（`[ADDREPLICA-STALE]` 日志），
并通过 `EraseReplicasWithCacheTotalAccounting` + `OnDiskReplicaRemoved` 保持
shard-level disk_object_count 与本地 SSD usage 计数一致。剩余同 client_id
的 replica 走原路径 update endpoint / size。

---

### Bug I: PutStart OBJECT_ALREADY_EXISTS 阻塞 LOCAL_DISK-only metadata

**commit**: `05835124`
**文件**: `mooncake-store/src/master_service.cpp` `PutStart`

**根因**: PutStart 用 `metadata.HasReplica(&Replica::fn_is_completed)` 判断是否
已存在写入——`fn_is_completed` 匹配任意已完成 replica（含 LOCAL_DISK）。
store 重启 + ScanMeta 后 metadata 仅含一个 completed LOCAL_DISK replica，导致
每次相同 key 的 PutStart 都返回 `OBJECT_ALREADY_EXISTS`，把已写到 SSD 上的
数据永久 gate 掉。

**修复**: 改为 `metadata.HasReplica([](r){ r.is_memory_replica() && r.is_completed(); })`。
LOCAL_DISK-only metadata 不应阻塞新的 PutStart——它的作用是服务 cache 读而不是
gate 写。

**验证**: `tests/test_store_restart_cache.py` Step 6 PUT 阶段 300000 key
`ok=300000 fail=0`。

---

### Bug K: PutEnd offload 路径 double-pin + emplace 失败不回退

**commit**: `d2048ae6`
**文件**: `mooncake-store/src/master_service.cpp` `PutEnd`

**根因**: PutEnd 的 `!offload_on_evict_` 分支内 VisitReplicas 缺少
`if (task_created) return` early-exit，且 `emplace` 返回值未检查。`replica_num=2`
时第二个 replica 仍然 `PushOffloadingQueue + inc_refcnt`，但第二份 emplace
被同 key 已存在的 task 排斥 → refcnt 永久泄漏。即使 `replica_num=1`，
若 `offloading_tasks[key]` 已存在（与 BatchEvict 路径的 race），也会泄漏。

**修复**: 进 lambda 前加 `offloading_tasks.count(key)==0` 前置检查；
lambda 内 `if (task_created) return` 早退；`emplace` 返回 `inserted=false`
时立即 `dec_refcnt` 回退（`[PUTEND-OFFLOAD-EMPLACE-FAIL]` 日志）。

**验证**: 旧分支在生产压测下 OFFLOAD-COMPLETE gap 从 87623 → 0；
新分支压测 5 分钟 OFFLOAD-COMPLETE gap=0。

---

### Bug J part 1: BatchEvict 跨周期重复 pin → refcnt 泄漏

**commit**: `77b43718`
**文件**: `mooncake-store/src/master_service.cpp` `BatchEvict::try_evict_or_offload`

**根因**: `try_evict_or_offload` 在把 key 推入 offloading queue 之前没有检查
`offloading_tasks.count(key) > 0`。BatchEvict 第 N 轮把 replica_A 推入队列并 pin；
FetchOffloadingTasks 把 master side `offloading_objects` 清空后，第 N+1 轮可以
对同一 key 的 replica_B 再 pin 一次。`emplace(key, T2)` 静默失败（key 已有 T1），
但 replica_B 的 `inc_refcnt` 不会回退 → replica 永久 pin，不可淘汰。

**修复**: VisitReplicas 前加 `count(key)==0` 前置检查；emplace 返回 `inserted=false`
时立即 `dec_refcnt` 回退（`[BATCHEVICT-OFFLOAD-EMPLACE-FAIL]` 日志）。双重防御。

---

### Bug J part 2: EvictTenantMemoryForQuota 同一泄漏路径

**commit**: `d3690852`
**文件**: `mooncake-store/src/master_service.cpp` `EvictTenantMemoryForQuota::try_evict_or_offload`

**根因**: 与 Bug J part 1 同形状，tenant-quota 驱动 evict 路径同样存在
`emplace` 失败不回退与缺少 count 前置检查的问题。在 BatchEvict 已插入 task 后
紧接着触发 tenant-quota evict，会对同 key 的第二个 replica 造成 refcnt 泄漏。

**修复**: 完全对称的两项防护（count 前置 + emplace 返回值 + `dec_refcnt` 回退，
`[TENANT-EVICT-OFFLOAD-EMPLACE-FAIL]` 日志）。

---

### LRU bug: BatchOffload 提交新 bucket 时把 LRU 时间戳写为 0

**commit**: `40ab7c7d`
**文件**: `mooncake-store/src/storage_backend.cpp` `BatchOffload`

**根因**: BatchOffload 提交新 bucket 时执行
`buckets_.emplace(bucket_id, std::move(bucket)); lru_index_.emplace(0LL, bucket_id);`。
`0` 严格小于任何真实 `steady_clock::now()` 时间戳，所以新写入的 bucket 在
`SelectEvictionCandidate` 的 LRU 路径下永远是首选项。一旦 SSD 写满触发
PrepareEviction，新写入的 cache 还没被读到就被淘汰掉，命中率从 ~80% 跌至 ~27%。

诊断日志确认：
```
[LRU-BUG] BatchOffload commit:  bucket_id=X lru_ts=0 keys=5
[LRU-BUG] EvictCandidate:        bucket_id=X ts=0 (matches actual)
```
同一个 bucket_id 刚提交就被选为淘汰候选。

**修复**: 提交时
```cpp
int64_t now_ns = std::chrono::steady_clock::now().time_since_epoch().count();
bucket->last_access_ns_.store(now_ns, std::memory_order_relaxed);
buckets_.emplace(bucket_id, std::move(bucket));
lru_index_.emplace(now_ns, bucket_id);
```
ScanMeta 恢复路径（line ~1592）有意保持 `0`，因为重启后从磁盘扫出的 bucket
确实应当被视为最旧。

**影响**: 仅当 `MOONCAKE_OFFLOAD_BUCKET_EVICTION_POLICY=lru` 时触发；默认 `fifo`
按 bucket_id 单调递增淘汰，不受影响。本分支生产配置使用 LRU，必须修。

**验证**: `tests/test_lru_bug_repro.py` 在 30MB SSD + 100MB DRAM 的测试 store 上
通过（`b15 survived — older data was evicted first`）。

---

### Promotion worker pool（PR #2529 commit 1/3，本分支独立重做）

**commit**: `dc40e854`
**文件**: `mooncake-store/include/file_storage.h`、`include/storage_backend.h`、`src/file_storage.cpp`

**根因**: 上游 `ProcessPromotionTasks` 在 store-server 的 heartbeat 线程内
同步执行每个 promotion task（SSD read + RDMA write）。`promotion_max_per_heartbeat=1`
时每 tick 只 2/s 准入；调到 64 后同一 tick 内 batch_get + TransferWrite 64 个 task
会阻塞 heartbeat 线程几百毫秒，拖垮心跳节奏导致积压。

**修复**: 把 per-key body 拆出为 `ProcessPromotionTask(task, preferred_segments)`
返回 `PromotionExecutionResult`，并用 mutex/cv 守护的 `promotion_task_queue_`
+ 后台 worker pool 异步消费。`ProcessPromotionTasks` 持续从 master
`PromotionObjectHeartbeat` 拉 task 直到本地队列达到 soft budget。

新增 env vars:
- `MOONCAKE_OFFLOAD_PROMOTION_WORKER_THREADS` (默认 1，生产建议 4)
  - `0` 走旧 inline 路径，便于测试与回退
- `MOONCAKE_OFFLOAD_PROMOTION_QUEUE_CAPACITY` (默认 1024，0 = 无限)
- `MOONCAKE_OFFLOAD_PROMOTION_DRAIN_BATCH_SIZE` (默认 64)

Init 时 spawn worker；~FileStorage join worker。

---

### Promotion 默认值调整

**commit**: `5cb97e1d`
**文件**: `mooncake-store/include/master_config.h`、`mooncake-store/include/storage_backend.h`

**根因**: PR #2529 把 promotion 从 heartbeat 同步路径剥离后，老保守默认值
(`promotion_max_per_heartbeat=1`、`promotion_worker_threads=1`) 仍然存在，导致
64 并发压测下 25259/~70000 admitted task TTL expired——准入 2/s 满足不了 70/s
的实际需求。

**修复**:
- `master_config.h` 4 个 config struct 的 `promotion_max_per_heartbeat`: 1 → 64
- `FileStorageConfig::promotion_worker_threads`: 1 → 4
- 64×4/5s ≈ 51/s/server，4 server 聚合约 204/s 远超 70/s 需求

调优后 promotion task expired 从 25259 → 0。

---

### Bug F: OffloadObjects 串行 WriteBucket → SSD 写吞吐瓶颈

**commit**: `e738cfd9`
**文件**: `mooncake-store/src/file_storage.cpp`、`mooncake-store/include/storage_backend.h`

**根因**: `OffloadObjects` 串行遍历每个 bucket 调 `BatchOffload` (→ WriteBucket →
pwritev)。每个 bucket 是独立文件，无共享写状态，串行化纯属人为瓶颈。单盘 NVMe
~1.6 GB/s vs 实测可达 ~4.7 GB/s，跟不上 high-watermark 触发后 cache 涌入速率。

**修复**: 把单个 bucket 的处理体捕获为返回 `std::optional<ErrorCode>` 的 lambda
（nullopt = 成功/可恢复；含值为不可恢复）。
- `offload_write_threads <= 1` 或仅 1 bucket → 走旧串行路径，零开销
- 否则 spawn `write_threads` 个 std::thread，从原子 `next_index` 抢 bucket，
  任何 bucket 返回错误即设 `first_error_set` 让其他 worker 立即退出；
  caller join 全部 worker 后再返回，保证捕获的引用生命周期安全

数据安全：
- `failed_tasks` / `all_bucket_keys` 用 `failed_tasks_mutex` 合并
- staging_bufs 是 per-bucket RAII 局部
- complete_handler 的 ssd_metric_ counters 与 client NotifyOffloadSuccess RPC
  均为独立路径

新增 env: `MOONCAKE_OFFLOAD_WRITE_THREADS` (默认 4)；=1 退化到串行。

---

### Bug E: orphan task 诊断日志

**commit**: `64863cb0`
**文件**: `mooncake-store/src/master_service.cpp`

修复 silent orphan-task 路径，加 3 个 tag：
- `[OFFLOAD-NOTASK]` — NotifyOffloadSuccess 收到 complete 但 master 没有
  offloading_task（已被 expired / 清理）。之前静默 AddReplica，现在可见
- `[CLEANUP-ORPHAN]` — EraseMetadata 删 metadata 时该 key 仍挂 offloading_task
  （BatchEvict 第二遍 vs store worker in-flight race）
- `[PUSH-EMPTY]` — PushOffloadingQueue 收到 segment_names 为空的 replica
  （之前静默 no-op）

与 Bug D 的 `[OFFLOAD-PARTIAL]`、Bug J/K 的 emplace-fail 日志一起构成
orphan-task 诊断套件。

---

### pwritev/preadv 按 UIO_MAXIOV 分块 + errno 诊断

**commit**: `7936a3b4`
**文件**: `mooncake-store/src/posix_file.cpp`

**根因**: `vector_write` / `vector_read` 直接把整个 iovec 数组传给 `pwritev` /
`preadv`，没有按 `UIO_MAXIOV` (1024) 分块。`BUCKET_KEYS_LIMIT=1000` × 每 key
2 iovec > 1024 → pwritev 返回 `EINVAL` → 转译成 `FILE_WRITE_FAIL`。

新分支部署时第一波压测立刻暴露：
```
vector_write failed for: <bucket_id>, error: FILE_WRITE_FAIL
Deleted corrupted file: .../X.bucket
```
SSD 使用率保持 0，cache hit 为 0。

**修复**:
1. 分块循环 `for (int idx = 0; idx < iovcnt; idx += UIO_MAXIOV)`，每块
   `int chunk_cnt = std::min(iovcnt - idx, UIO_MAXIOV)`；累计 offset 与 totals
2. 总字节不匹配 → `FILE_WRITE_FAIL`
3. 失败时打 `errno(strerror) + fd + iovcnt + total_bytes + offset +
   chunk_start/chunk_cnt`，便于后续 FILE_WRITE_FAIL 排查直接看到 syscall 错误

未在上游 main 中找到对应修复，故本 commit 是必需的。

## 配置参数（生产推荐）

### store-server 端（`launch_store_server.sh`）

| Env | 值 | 说明 |
|-----|----|------|
| `MOONCAKE_OFFLOAD_HEARTBEAT_INTERVAL_SECONDS` | 5 | 压测下更快处理积压 |
| `MOONCAKE_OFFLOAD_LOCAL_BUFFER_SIZE_BYTES` | 21474836480 (20GB) | batch-get SSD 默认 1.2GB 在并发下 BUFFER_OVERFLOW |
| `MOONCAKE_OFFLOAD_BUCKET_KEYS_LIMIT` | 1000 | 每桶最多 1000 key |
| `MOONCAKE_OFFLOAD_BUCKET_SIZE_LIMIT_BYTES` | 2GB | |
| `MOONCAKE_OFFLOAD_BUCKET_EVICTION_POLICY` | lru | 需配合 LRU bug 修复 |
| `MOONCAKE_OFFLOAD_PROMOTION_WORKER_THREADS` | 4 (新默认) | promotion worker 池 |
| `MOONCAKE_OFFLOAD_PROMOTION_DRAIN_BATCH_SIZE` | 64 (新默认) | |
| `MOONCAKE_OFFLOAD_WRITE_THREADS` | 4 (新默认) | parallel WriteBucket |

### master 端（`launch_master.sh`）

| Flag | 值 | 说明 |
|------|----|------|
| `--eviction_high_watermark_ratio` | 0.7 | DRAM 触发 BatchEvict 的水位 |
| `--eviction_ratio` | 0.2 | 目标：DRAM 使用率降到 20% |
| `--put_start_release_timeout_sec` | 120 | 测试时缩短，加速过期诊断 |
| `--promotion_max_per_heartbeat` | 64 (新默认) | 每 tick 准入上限 |
| `--promotion_admission_threshold` | 2 | CountMinSketch 二次访问准入 |
| `--offloading_queue_limit` (upstream PR #2599) | 默认 50000 | 如调高需同步 offload_cap_ratio |
| `--offload_cap_ratio` (upstream PR #2599) | 默认 0.5 | offload_cap = queue_limit × ratio |

## 验证

### test_lru_bug_repro.py
小容量 store (100MB DRAM + 30MB SSD) 上写 30 batch、READ 全部、再写
b15/b16 触发 eviction。
- buggy wheel: `b15 evicted` + `[LRU-BUG] lru_ts=0` 日志
- fixed wheel: `b15 survived — older data was evicted first`

### test_store_restart_cache.py
写 300000 key → 等 offload → baseline GET → restart store + master → ScanMeta →
再次 GET → PutStart 同批 key。
- Step 1 WRITE: ok=299592 fail=408（DRAM 满，非 bug）
- Step 5 GET after restart: **hit=299592 miss=0**（验证 Bug H + I）
- Step 6 PUT after restart: **ok=300000 fail=0**（验证 Bug I）

### AI_ERA_PRESSURE 压测
4 store-server × 128GB DRAM / 128GB SSD = 512GB / 512GB 总量；
64 并发；glm-5.2 prefill + decoder；5 分钟采样。
- hit_rate (window): **88.76%**（修复前 27%）
- FILE_WRITE_FAIL: 0
- save queue full / BUFFER_OVERFLOW: 0
- Promotion expired / OFFLOAD-EXPIRED: 0
- OFFLOAD-COMPLETE gap: 0
- req_err: 0

## 测试脚本

| 脚本 | 用途 |
|------|------|
| `tests/test_lru_bug_repro.py` | LRU ts=0 bug 复现 / 验证 |
| `tests/run_restart_cache_test.sh` | store restart cache 恢复完整流程 |
| `tests/test_store_restart_cache.py` | 上述脚本调用的 Python 测试本体 |
