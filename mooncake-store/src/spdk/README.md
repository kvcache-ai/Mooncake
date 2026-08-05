
# SPDK NVMe-oF Multi-Qpair I/O Subsystem

## 概述

Mooncake Store 的 NoF (NVMe-over-Fabrics) 子系统负责通过 RDMA 协议与远端
NVMe SSD 进行高性能块 I/O。本模块基于 Intel SPDK 库，在原生单 qpair 的基础
上实现了多 qpair 并发 I/O、Pipeline 流水线、自适应降级和动态恢复等机制。

## 架构层次

```
┌──────────────────────────────────────────────────────────┐
│  transfer_task.cpp — SpdkNofWorkerPool                   │
│    · 任务队列 (per-worker)  · 流控背压                   │
│    · Per-seg QOS   · Inflight 管理   · 周期性再均衡      │
├──────────────────────────────────────────────────────────┤
│  spdk_wrapper.cpp — SpdkWrapper (Singleton)              │
│    · SPDK env 初始化  · Open/Close Segment               │
│    · QidPressureGauge 全局压力感知                        │
│    · ProbeNofSegment 心跳探测                             │
├──────────────────────────────────────────────────────────┤
│  nof_connection.cpp — NofConnection / NofQpairPool       │
│    · Controller + Namespace 绑定                         │
│    · 多 qpair 分配 / Round-Robin 分发                    │
│    · 连续分配 + 退避重试 + TryGrow 恢复                  │
├──────────────────────────────────────────────────────────┤
│  nof_segment.cpp — NofSegment / PipelineIO               │
│    · 单次 Submit API  · Pipeline 批量 I/O                │
├──────────────────────────────────────────────────────────┤
│  SPDK lib — spdk_nvme_probe / alloc_io_qpair / cmd_rw    │
└──────────────────────────────────────────────────────────┘
```

## 核心组件

### 1. NofConfig (`include/spdk/nof_config.h`)

NVMe-oF I/O 配置结构，所有字段通过 `MC_NVME_*` 环境变量设置。

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `num_io_queues` | 16 | 请求的 I/O qpair 数量 |
| `io_queue_size` | 256 | 每条 qpair 的队列深度 |
| `io_queue_requests` | 512 | 队列请求条目数 |
| `max_inflight_per_qpair` | 64 | 每 qpair 最大在途 I/O 数 |
| `chunk_blocks` | 512 | I/O chunk 大小 (512 blk = 2 MiB @4KiB) |
| `keep_alive_timeout_ms` | 10000 | Keep-alive 超时 (ms) |
| `min_io_queues` | 1 | 降级模式下最少允许的 qpair 数 |
| `retry_max_attempts` | 5 | 分配失败最大重试次数 |
| `retry_backoff_ms` | 100 | 重试退避基础间隔 (指数增长) |
| `enable_degradation` | true | 启用自适应降级 |
| `max_queue_depth` | 256 | Worker 任务队列深度上限 (0=不限制) |
| `adaptive_inflight` | true | 根据 qpair 数自适应调整 inflight 上限 |
| `raid0_stripe_size_kb` | 2048 | RAID-0 条带大小 (KiB) |

### 2. NofQpairPool (`include/spdk/nof_connection.h`)

管理单个 NVMe Controller 上的 N 条 I/O qpair。

**Round-Robin 分发**：
```cpp
spdk_nvme_qpair *GetNextQpair() {
    uint32_t idx = round_robin_idx_.fetch_add(1, memory_order_relaxed);
    return qpairs_[idx % qpairs_.size()];
}
```
每次 I/O 提交通过原子自增索引轮转到不同 qpair，保证负载均匀分布。

**统一 Polling**：
```cpp
int32_t PollAll(uint32_t max_completions = 0) {
    for (auto *qp : qpairs_)
        total += spdk_nvme_qpair_process_completions(qp, ...);
    return total;
}
```
一次性收割所有 qpair 的完成事件。

**Inflight 跟踪**：`inflight_count_` 原子计数器，配合 `max_inflight_per_qpair`
计算池总容量：
```
MaxInflight = qpairs_.size() × max_inflight_per_qpair
```

**TryGrow 恢复**：当其他连接断开释放 QID 后，降级池可逐步恢复到初始 `target_count_`：
```cpp
uint32_t TryGrow(uint32_t target_total) {
    for (uint32_t i = qpairs_.size(); i < target_total; i++) {
        auto *qp = spdk_nvme_ctrlr_alloc_io_qpair(ctrlr_, nullptr, 0);
        if (!qp) break;  // QID 池耗尽，等待下一轮
        qpairs_.push_back(qp);
    }
    return added;
}
```

### 3. NofConnection (`include/spdk/nof_connection.h`)

每个连接 = 1 个 NVMe-oF Controller + 1 个 Namespace + 1 个 QpairPool。

**连接建立** (`Connect()`)：
1. 通过 `spdk_nvme_probe()` 探测 target
2. 设置唯一 `hostnqn`（`nqn.2024-08.mooncake:c<N>`），确保不同连接获得独立 Controller
3. 连续分配 I/O qpair：尝试拿满 `num_io_queues` 条，能拿多少拿多少
4. 如实际分配数 < `min_io_queues`，则释放已完成的部分并返回失败

**关键设计 — hostnqn 唯一化**：
```cpp
// 全局计数器，每个连接递增
static atomic<uint32_t> g_hostnqn_counter{0};
// probe callback 中设置唯一 hostnqn
snprintf(opts->hostnqn, sizeof(opts->hostnqn),
         "nqn.2024-08.mooncake:c%u", pctx->hostnqn_id);
```
不使用 SPDK 默认的 UUID-based hostnqn（同进程内所有连接共享同一 UUID），
确保 target 为每个连接创建独立 Controller。

### 4. NofSegment (`include/spdk/nof_segment.h`)

一个连续的 LBA 范围，绑定到一个 NofConnection。

**单次 I/O API**（兼容旧接口）：
```cpp
int SubmitRead(void *buf, uint64_t lba, uint32_t num_blocks, ...);
int SubmitWrite(void *buf, uint64_t lba, uint32_t num_blocks, ...);
```

**Pipeline I/O**（高吞吐批量传输）：
```cpp
ssize_t PipelineRead(void *buf, uint64_t lba, uint32_t total_blocks);
ssize_t PipelineWrite(const void *buf, uint64_t lba, uint32_t total_blocks);
```

Pipeline I/O 循环：
```
while (未完成所有块 或 还有 inflight I/O):
    1. 提交阶段：只要 inflight < max_inflight 且有剩余块:
       - 将数据拆分为 chunk_blocks 大小的 chunk
       - GetNextQpair() 选择 qpair
       - spdk_nvme_ns_cmd_read/write() 提交
    2. Poll 阶段：PollAll(0) 收割所有 qpair 的完成事件
    3. 错误处理：如有错误，排空所有 inflight I/O 后返回 -1
```

### 5. SpdkWrapper (`include/spdk/spdk_wrapper.h`)

全局单例，管理 SPDK 环境初始化和所有打开的 NoF 连接。

**连接管理**：
- `open_segments_`：handle → NofConnection 映射
- `endpoint_to_handle_`：transport string → handle 反向索引（去重用）
- `connect_mutex_`：序列化 `spdk_nvme_probe()` 调用（SPDK probe 非线程安全）

**QID 压力感知** (`QidPressureGauge`)：
- 滑动窗口 (16 样本) 记录 (requested, allocated) 分配结果
- 三级压力：Green (>75%) / Yellow (50-75%) / Red (<50%)
- 新连接根据压力等级自适应减少 qpair 请求数

**心跳探测** (`ProbeNofSegment`)：
- 使用 `config_probe_` (num_io_queues=1) 的独立临时连接
- 不与 I/O 路径共享 qpair，避免竞争
- 函数返回时连接自动析构 → QID 回收

### 6. SpdkNofWorkerPool (`include/transfer_task.h`)

管理多个 worker 线程，每个线程独立执行 SPDK I/O 操作。

**任务分发** — seg 到 worker 的亲和性绑定：
```cpp
// 同一 seg_handle 的所有任务固定路由到同一 worker 线程
if (seg_to_worker_.find(seg) != seg_to_worker_.end())
    worker_idx = seg_to_worker_[seg];          // 已有绑定：复用
else
    worker_idx = (seg_num++ % worker_count_);  // 新绑定：round-robin 分配
```
原因：SPDK qpair 非线程安全，同一连接的 qpair 池必须由单一 worker 访问。

**Per-Seg QOS** (`SpdkNofQos`)：
- 读写各一条 FIFO 任务链表
- `inflight_blocks_limit` 控制该 seg 上同时在途的 I/O block 数
- `blocks_per_chunk` 控制每次提交的最大 chunk 大小
- 自适应 Inflight：当 qpair 数降级时自动缩小上限

**流控背压**：
```cpp
if (task_queue_[worker_idx].size() >= max_queue_depth_)
    queue_not_full_cv_[worker_idx].wait(lock);  // 阻塞调用方
```
防止降级场景下请求无界积压导致内存溢出。

**周期性再均衡** (每 30 秒)：
```cpp
if (pool.Size() < pool.GetTargetCount()) {
    uint32_t added = pool.TryGrow(target);
    if (added > 0) nof_qos->UpdateInflightLimit(pool.Size(), ...);
}
```

## 多 Qpair 并发 I/O 流程

### 单客户端 I/O 路径

```
Client.put(key, data)
  → TransferSubmitter::submitSpdkNofOperation()
    → 查 nof_handle_cache_ (endpoint → seg_handle)
    → SpdkNofWorkerPool::submitTask()
      → seg 绑定到 worker
      → 入队 + notify worker
        → workerThread():
          1. 从 task_queue_ 取任务
          2. 绑定 seg_to_qos (per-seg QOS)
          3. 拆分为 chunk_blocks 大小的子任务
          4. GetNextQpair() → round-robin 选 qpair
          5. spdk_nvme_ns_cmd_read/write() 提交
          6. PollAll() 收割 completion
          7. SpdkNofTaskCompletion() → set_completed()
```

### 多 Qpair 并行原理

```
Thread 1 ────┐
Thread 2 ────┤
Thread 3 ────┼──→ SpdkNofWorkerPool (4 workers)
Thread 4 ────┘         │
                       ├── Worker 0 → seg_A → NofQpairPool[qp0, qp1, qp2, qp3]
                       │                     Round-Robin: qp0 → qp1 → qp2 → qp3
                       │
                       ├── Worker 1 → (idle)
                       ├── Worker 2 → (idle)
                       └── Worker 3 → (idle)

Pipeline I/O 循环:
  [qp0 ████░░░░] [qp1 ████░░░░] [qp2 ████░░░░] [qp3 ████░░░░]
      ↑ 提交          ↑ 提交          ↑ 提交          ↑ 提交
      ↓ 完成          ↓ 完成          ↓ 完成          ↓ 完成
  PollAll() 一次性收割所有 qpair 的 completion
```

## QID 管理

### QID 消耗模型

```
单连接 → 同一 target 的 QID = 1 (admin qpair) + num_io_queues (io qpairs)

默认配置 (满血):
  Admin Queue Pair: 1
  IO Queue Pairs:   16  (MC_NVME_NUM_IO_QUEUES)
  合计:             17  QID
```

每个 Client (TransferSubmitter) 持有独立的 `nof_handle_cache_`，
同一 Client 内的多次 put/get 复用同一连接，不增加 QID。

### 连接建立的退避重试

```
第一次尝试：请求完整 num_io_queues
  ├─ 成功 → 连接建立，Record(requested, allocated)
  └─ 失败 (qpair_alloc_fail):
      ├─ QidPressureGauge::GetRecommended() 评估全局压力
      ├─ 渐进降级：target = min(gauge建议, target/2)
      ├─ 退避等待：retry_backoff_ms × 2^attempt
      └─ 重新 Connect() (最多 retry_max_attempts 次)
```

### 降级与恢复

**降级**：QID 池不足时，能拿多少拿多少（≥ min_io_queues）：
```
请求 16 qpair → 实际 4 条 → 降级状态 (4/16)
qpair_pool_ 内部记录 target_count_=16
日志: "QID degraded: allocated 4/16 qpairs"
```

**恢复**：Worker 线程每 30 秒调用 TryGrow：
```
检查 pool.Size() < pool.GetTargetCount()
  → TryGrow(target_count)
    → 成功 → 更新 inflight 上限
```
## 配置调优

### 满带宽配置

```bash
# qp=4, 大 block, 大 chunk
MC_NVME_NUM_IO_QUEUES=4
MC_NVME_MAX_INFLIGHT_PER_QPAIR=16   # 总 inflight = 4×16 = 64
MC_NVME_CHUNK_BLOCKS=2048            # 8 MiB chunk
```

### 避免 Target 过载

| 场景 | 推荐配置 |
|------|---------|
| 稳定生产 | `MAX_INFLIGHT_PER_QPAIR=16`, `CHUNK_BLOCKS=2048` |
| 小块 I/O | `MAX_INFLIGHT_PER_QPAIR=32`, `CHUNK_BLOCKS=512` |
| 大块 I/O (32MB+) | `MAX_INFLIGHT_PER_QPAIR=8`, `CHUNK_BLOCKS=4096` |
| 多 qpair 扫参 | `MAX_INFLIGHT_PER_QPAIR=16`, 总 inflight ≤ 64 |

**关键公式**：`总 inflight = num_io_queues × max_inflight_per_qpair`
保持总 inflight ≤ 64 可避免 FORINN target 过载。

## 性能基准

以下数据来自 `nof_worker_pool_bench` 工具，测试环境为 **FORINN HWE62P447T6L00LN**
NVMe-oF target，单 SSD，随机读 (`--rw randread`)，`--io_size 8388608 --iodepth 64
--duration_sec 20 --warmup_sec 5`。

通过 shell 循环依次设置 `MC_NVME_NUM_IO_QUEUES=1,2,4,8,16`，
同时动态调整 `MC_NVME_MAX_INFLIGHT_PER_QPAIR` 以保持总 inflight 恒定为 64：

```bash
for qp in 1 2 4 8 16; do
    MC_NVME_NUM_IO_QUEUES=$qp \
    MC_NVME_MAX_INFLIGHT_PER_QPAIR=$(expr 64 / $qp) \
    ./nof_worker_pool_bench \
        --endpoints "traddr:10.10.10.100 trsvcid:4420 \
                     subnqn:nqn.ForinnBase5000.lsjs:nvme.1 \
                     trtype:RDMA adrfam:IPv4 ns:1" \
        --rw randread --io_size 8388608 --iodepth 64 \
        --duration_sec 20 --warmup_sec 5
    sleep 5
done
```

**恒定总 inflight = 64** 的设计意图：消除 inflight 变化对吞吐的干扰，使 qpair 数成为唯一变量。

| 请求 qpair | 实际 qpair | `MAX_INFLIGHT` | 总 inflight | BW | IOPS | 说明 |
|-----------|-----------|----------------|-------------|-----|------|------|
| 1 | 1 | 64 | 64 | 420.00 MiB/s | 52.50 | 基线：单路深度 64 |
| 2 | 2 | 32 | 64 | 861.20 MiB/s | 107.65 | 2.05× 扩展 |
| 4 | 4 | 16 | 64 | 1.87 GiB/s | 238.95 | 4.56× 扩展 (超线性, 114% 效率) |
| 8 | 4¹ | 8 | 32¹ | **3.21 GiB/s** | **411.30** | nn=4 截断，inflight 被动缩减后吞吐反升 |
| 16 | 4¹ | 4 | 16¹ | **3.28 GiB/s** | **419.60** | nn=4 截断，低 inflight 下达到最优 |

> ¹ 请求 8 或 16 qpair 时，FORINN target per-Controller I/O Queue 配额 (`nn=4`)
> 将实际分配数截断为 4。此时 `MAX_INFLIGHT_PER_QPAIR=64/qp` 公式导致总 inflight
> 被动缩减为 32（qp=8）或 16（qp=16）。吞吐反而从 1.87 GiB/s 跃升至 3.28 GiB/s，
> 说明 **总 inflight=64 已导致 target 端过载**——适度降低 inflight 释放了 target
> 内部 completion 处理瓶颈，实际有效吞吐反而更高。

**结论**：
1. 在当前 target (nn=4) 下，4 qpair 已达物理上限；吞吐扩展效率 114%（超线性）。
2. **总 inflight 并非越大越好**：64 → 32 → 16 的递减过程中，吞吐从 1.87 → 3.21 → 3.28 GiB/s 持续上升，说明 target 端存在 inflight 过载拐点。最优总 inflight 约 16（4 qpair × 4 per-qpair）。
3. 推荐生产配置：`MC_NVME_NUM_IO_QUEUES=4`，`MC_NVME_MAX_INFLIGHT_PER_QPAIR=4`，总 inflight=16。

## 常见问题

### 1. `No free I/O queue IDs`

**原因**：Target per-controller I/O Q 配额已满 (FORINN nn=4)。
**解决**：
- 检查是否有残留连接 (等待 keep-alive 超时或重启 target)
- 减少 `MC_NVME_NUM_IO_QUEUES`
- 使用多个不同 NQN 的 subsystem

### 2. `poll completion error: ret -6` (ENXIO)

**原因**：Qpair 进入断开/错误状态。
- 在 QP_SWEEP 中：前一个子进程的 QID 还未被 target 回收
- 在 Benchmark 中：Inflight 过大导致 target 端 RDMA 连接断开

**解决**：
- 增大扫参间隔 `QP_SWEEP_GAP_SEC=60`
- 减小 `MC_NVME_MAX_INFLIGHT_PER_QPAIR`

### 3. `RDMA connect error -99` + `Ctrlr is in error state`

**原因**：FORINN target 的 NVMe 子系统已崩溃，必须重启 target。
**预防**：控制总 inflight ≤ 64。

### 4. 多客户端无法同时连接同一 target

**原因**：FORINN target 将同一 NQN 的不同 hostnqn 连接合并为同一 Controller，
per-Controller I/O Queue 配额 (`nn=4`) 被第一个 Client 占满。

**解决**：
- 使用单 Client 模式 + 多线程并发
- 配置多个 subsystem (不同 NQN)
- 使用不同物理 target

## 文件索引

| 文件 | 说明 |
|------|------|
| `include/spdk/nof_config.h` | 配置结构 + 环境变量解析 |
| `include/spdk/nof_connection.h` | NofQpairPool + NofConnection 声明 |
| `include/spdk/nof_segment.h` | NofSegment + PipelineCtx 声明 |
| `include/spdk/nof_raid0.h` | NofRaid0 条带化声明 |
| `include/spdk/spdk_wrapper.h` | SpdkWrapper + QidPressureGauge 声明 |
| `src/spdk/nof_config.h` | (inline 实现，见 include) |
| `src/spdk/nof_connection.cpp` | Connection 工厂 + QpairPool 实现 |
| `src/spdk/nof_segment.cpp` | PipelineIO 核心循环 |
| `src/spdk/nof_raid0.cpp` | RAID-0 Stripe 映射 + 聚合 Pipeline |
| `src/spdk/spdk_wrapper.cpp` | 单例管理 + 自适应连接 + 心跳探测 |
| `include/transfer_task.h` | SpdkNofWorkerPool + SpdkNofQos |
| `src/transfer_task.cpp` | Worker 线程 + 流控 + 再均衡 |

## 修改履历

- **2026-07-31** — 初始多 qpair 实现 (NofQpairPool, NofConnection, NofSegment, PipelineIO)
- **2026-08-03** — 新增连续分配 + 退避重试 + TryGrow 再均衡 + QidPressureGauge + 流控背压
- 
---
> 修改人：绿算技术
