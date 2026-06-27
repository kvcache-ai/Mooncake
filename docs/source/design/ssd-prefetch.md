# SSD Prefetch-on-Exist 设计文档

> 关联：RFC #2213（prefetch-on-exist）、PR #2071（L2→L1 promotion-on-hit）
> 适用分支：`Mooncake`（main）。本特性最初在 v0.3.11 上实现并验证，现已同步至 main。


## 1. 背景与目标

### 1.1 问题来源
在 vLLM-Ascend 的 KV 三级池化（HBM / DRAM / SSD）场景下，KV cache 会随 DRAM 容量不足被驱逐（offload）到 SSD（`LOCAL_DISK` 副本）。当请求命中一个**只在 SSD、不在 DRAM** 的 key 时，`get()` 需要从 SSD 读盘，延迟显著高于 DRAM 命中。

vLLM 的请求处理是多阶段流水线，`exists()`（调度阶段的 cache 探测）与 `get()`（worker 真正加载 KV）之间存在一个时间窗口（实测中位数 15~17s，因调度排队而被放大）：

```
阶段1: Scheduler 调度            阶段2: 当前 batch 前向计算        阶段3: Worker 加载 KV
 get_num_new_matched_tokens()     GPU/NPU forward (耗时)            start_load_kv()
   └─ batch_is_exist() ◄─探测                                         └─ get() ◄─真正读数据
        │                                                                  │
        └────────────── 这个窗口内可以偷偷把 SSD→DRAM 预热 ──────────────┘
```

### 1.2 目标
利用 `exists()` 到 `get()` 的时间窗口，在探测阶段**异步、尽力而为（best-effort）**地把 SSD-only 的 key 预取（promote）到 DRAM，使后续 `get()` 命中 DRAM，降低 TTFT。

### 1.3 设计约束
- prefetch 必须是 `exists()` 的**附加动作**，不能改变 `exists()` 的语义、不能阻塞调度。
- 不能污染既有 promotion-on-hit / eviction / 指标系统的行为。
- 失败可丢弃（best-effort），绝不能影响正常 offload / get。
- 需支持 **PD 分离 / 跨节点**：发起预取的节点 A 发现 key 的 `LOCAL_DISK` 副本在节点 B，应让 B 从自己的 SSD 读入自己的 DRAM（“方案 B：跨节点委托”），而非把数据先搬到 A。

## 2. 当前实现的方案（采用）

### 2.1 总览：prefetch 专用路径，复用 promotion 的“执行底座”，绕开 promotion-on-hit 的“准入与节奏”

```
is_exist(keys, ExistOptions{prefetch_to_memory=true})
    └─ triggerSsdPrefetch(keys) → prefetch_pool_（固定大小 4）
        └─ 按 chunk（128 key）BatchQueryForPrefetch(chunk)     # 批量只读元数据，1 次 RPC/chunk
            └─ ClassifySsdPrefetchRoute：过滤 SSD-only（有 LOCAL_DISK、无 MEMORY）且 size>0
                ├─ 本节点持有 LOCAL_DISK：RunLocalPrefetchRegisterAndPromote(chunk_local)   # 立即 register + PrefetchKeys（流水线，不等待后续 chunk）
                └─ 远端节点持有：收集到 remote_keys，全部 chunk 完成后 prefetch_offload_object RPC 委托 holder
                      └─ holder 侧 runLocalPrefetch → RunLocalPrefetchRegisterAndPromote   # 在 holder 本地完成
```

**阶段 1 批量化 + 流水线化（2026-06）**：早期实现是对每个 key 单独 `QueryForPrefetch`，且**等整批 metadata 查完**才统一 `RegisterPrefetchTask` + `PrefetchKeys`。大批量（如 954 key）时，首个 register 可晚于首个 `get()` 约百毫秒，导致 `prefetch_miss_race` 偏高。现改为：
- **批量化**：`BatchQueryForPrefetch` / `BatchGetReplicaListForPrefetch`，每 chunk 最多 128 key，RPC 次数从 O(N) 降为 O(N/128)。
- **流水线化**：每个 chunk 查完 metadata 后**立即**对本 chunk 的 local key 执行 register + promote，不再阻塞后续 chunk 的 register。
- **defer reserve**：`PrefetchThrottle::reserve()` 从 exist 同步路径移到 **BatchQuery 确认 SSD-only 之后**，避免「key 已在 DRAM 仍记 trigger」的误统计与无效 async 工作。

`RunLocalPrefetchRegisterAndPromote` 封装「逐 key RegisterPrefetchTask → markInFlight → PrefetchKeys → cooldown」的本地提升逻辑，供 `triggerSsdPrefetch` 各 chunk 与 `runLocalPrefetch`（RPC 委托入口）共用。

`PrefetchKeys()` 内部复用 Mooncake 既有的 promotion 执行链：
`PromotionAllocStart`（master 分配 PROCESSING MEMORY 副本）→ `FileStorage::AllocateBatch`（本地暂存）→ `BatchLoad`（SSD 读入）→ `PromotionWrite`（Transfer Engine 写入 DRAM）→ `NotifyPromotionSuccess`（标记 MEMORY 副本 COMPLETE）。

### 2.2 关键设计点：为什么“专用路径”而不是直接复用 promotion-on-hit

prefetch **只复用 promotion 的执行底座**（`promotion_tasks` / `PromotionAllocStart` / `PromotionWrite`），而**不复用 promotion-on-hit 的触发与调度**。为此新增了 prefetch 专用 master RPC：

| RPC | 作用 | 关键：不做什么 |
|---|---|---|
| `GetReplicaListForPrefetch(key)` | 只读取 replica 元数据用于判定 SSD-only | **不**发 lease、**不**记 sketch、**不**累加 `valid_get_nums` 等指标、**不**入 promotion 队列 |
| `BatchGetReplicaListForPrefetch(keys)` | 同上，批量版；client 侧 `BatchQueryForPrefetch` 封装 | 同上；master 侧当前为循环调 `GetReplicaListForPrefetch`（与 `BatchGetReplicaList` 同模式），后续可优化为真批量 |
| `RegisterPrefetchTask(client_id, key)` | 在 master 登记一个 `promotion_tasks`（供 `PromotionAllocStart` 用） | **不**走 `TryPushPromotionQueue` 的准入闸、**不**推 holder 的 `promotion_objects` 心跳队列 |

与原生 `GetReplicaList`（main, master_service.cpp）对照可见区别：原生路径会 `inc_*_cache_hit_nums` / `inc_valid_get_nums` / `GrantLease` / `TryPushPromotionQueue`，而 `GetReplicaListForPrefetch` 全部不做。

### 2.3 节流：防止 prefetch 风暴（cooldown + dedup）
vLLM 调度的 busy-loop 会对 waiting 队列里的同一请求**反复探测**同一批 SSD-only block，导致 prefetch 被高频触发。为此引入两个客户端侧节流参数（`PrefetchThrottle`）：

| 参数（mooncake.json，单位秒） | 默认值 | 含义 |
|---|---|---|
| `ssd_prefetch_dedup_ttl_sec` | 30 | 同一 key 在该 TTL 内最多触发一次 prefetch，抑制并发重复探测（**主要起效项**） |
| `ssd_prefetch_cooldown_sec` | 5 | DRAM 饱和后的退避窗口：窗口内跳过 prefetch，让 eviction/offload 先回收内存，避免与 promotion 抢内存 |

> 实测中 `dedup_ttl` 是消除 prefetch 风暴、解决 offload 饿死的主要因素；`cooldown` 在本数据集规模下未触发（DRAM 未被打满，offload 跟得上）。

### 2.4 并发边界：固定线程池
`triggerSsdPrefetch` / `runLocalPrefetch` 使用固定大小的 `prefetch_pool_`（`mooncake::ThreadPool`，大小 4，对齐 `ClientService::task_thread_pool_(4)`），替代早期每次 detached `std::thread` 的写法，**限制并发 SSD 读 + DRAM 分配数量**，避免海量 detached 线程争抢同一物理 SSD 的 IOPS/带宽并向 master 狂申请 DRAM。

`submitPrefetchJob` 统一入池；当池不可用（未初始化 / 正在关闭）时**直接丢弃该 best-effort 任务，绝不退化为 detached `std::thread`**——后者正是 B1 prefetch 风暴的反模式。由门控保证 `triggerSsdPrefetch` 仅在 `file_storage_` 存在时触发，而 `initPrefetchRuntime()` 与 `file_storage_` 同在 `enable_ssd_offload` 分支内建立，故池不可用分支在正常运行下不可达，丢弃仅作为不变式兜底。

#### 线程使用规范（本特性及后续修改一律遵循）
对照 Mooncake 原生设计，线程分两类，**严禁随负载（每请求 / 每 Key）创建线程**：

| 类别 | 适用场景 | 正确做法 | Mooncake 原生先例 |
|---|---|---|---|
| A. 固定角色的长生命周期守护循环 | eviction、heartbeat、monitor、ipc server、GC 等单例后台循环 | 每个角色一个专用 `std::thread`/`jthread`，存为成员，进程生命周期内常驻（线程数 = O(角色数)，小常数，与负载无关） | `master_service.cpp` 的 eviction/monitor/cleanup/dispatch；`client_service.cpp` 的 leader monitor/storage heartbeat/task poll；`file_storage.cpp` 的 heartbeat/GC |
| B. 随负载增长的高频短任务 | prefetch promotion、分段重试等数量随请求量/Key 数膨胀的工作 | 一律走固定大小 `ThreadPool::enqueue` 封顶并发，**不得**裸起线程；池满/不可用时丢弃或排队，不绕过上限 | `client_service.cpp` 的 `task_thread_pool_(4)` |

> 判定口诀：线程数会不会随请求量/Key 数增长？会 → 必须用 ThreadPool（B 类）；不会、是固定角色的常驻循环 → 用专用 `std::thread` 成员（A 类）。prefetch 属 B 类，故用 `prefetch_pool_`。B1 的根因正是把 B 类工作误用了 A 类（每次探测 detached 线程）手法。

### 2.5 get()-side “等一次” 机制
若 `get()` 命中 SSD-only key（`LOCAL_DISK` 为最优副本），可在预算内轮询等待 promotion 完成后再选 DRAM 副本：
- 配置项 `ssd_get_wait_ms`（mooncake.json）或环境变量 `MOONCAKE_SSD_GET_WAIT_MS`（默认 **10ms**；`0` = 关闭）。
- 观测与等待逻辑挂在 **`batch_get_into_multi_buffers_internal`**（vLLM-Ascend 实际批量 get 路径），合并输出 `[GET-SRC]` + `[PREFETCH-OUTCOME]` 日志。
- **TP0（local，`prefetch_wait_mode=local`）**：本进程 `PrefetchThrottle` 有 trigger 记录时，轮询 throttle 完成状态 + `TryRefreshBestMemoryReplica` 再 Query master。
- **TP1~7（master，`prefetch_wait_mode=master`）**：本进程无 trigger 时（exist 在 TP0 触发、get 在其它 rank），仍对 SSD-only key **轮询 Query master**，直到出现 COMPLETE 的 MEMORY 副本或预算耗尽；不增加 exist 次数。
- 等待后若出现 MEMORY 副本则走 DRAM；否则回退到原 SSD 读路径。
- `ClassifyPrefetchOutcome` 输出（`[PREFETCH-OUTCOME]`，见分析脚本 `analyze_prefetch.sh` 指标说明）：
  - `prefetch_hit`：promotion 在 get 前完成（`done_ms<=get_ms`）且 source=DRAM
  - `prefetch_promoted_untracked`：曾 RegisterPrefetchTask（BatchQuery 时为 SSD-only），get 时已是 DRAM 但 done 未记入 throttle
  - `prefetch_miss_race`：**预取失败/超时**——本 rank 已 trigger（`prefetch_trigger_ms>=0`）或 `promote_attempted=true`，get 仍 source=SSD
  - `prefetch_evicted_after_exist`：**驱逐后读SSD**——本 rank 无 trigger 且 `promote_attempted=false`，get 仍 source=SSD（exist 时可能在 DRAM，窗口内被驱逐；或 TP1~7 master 等待超时）
  - `prefetch_dram_was_resident`（脚本称 **exist误trigger**）：get 读 DRAM，但 key 本来就在 DRAM、本 rank **未** RegisterPrefetchTask；误入 prefetch 等待路径。**defer reserve 后应≈0**
  - `dram_resident`：get 读 DRAM，且**从未**参与 prefetch（无 trigger、无 wait，`prefetch_wait_mode=none`）
  - `prefetch_failed`：`PrefetchKeys` promotion 失败（throttle 状态 `kFailed`）
  - ~~`ssd_no_prefetch`~~：已并入 `prefetch_evicted_after_exist`（旧日志仍可能出现）
- `[GET-SRC]` 日志另含 `prefetch_promote_attempted=0|1`（本 rank 是否已走 RegisterPrefetchTask / markInFlight），供 outcome 分类与脚本交叉统计（如驱逐子项「无 register / 有 register」）。
- **SSD 预取有效读 DRAM**（分析脚本核心指标）= `prefetch_hit + prefetch_promoted_untracked`，占 GET 总条数比例；`prefetch_dram_was_resident` / `dram_resident` 不计入。
- 批量化流水线缩短「exist → 首个 register」延迟后，10ms 预算内 `prefetch_hit` 比例预期上升。若 `exist→get` 间隔仍大于 `default_kv_lease_ttl`，预取 key 仍可能在 get 前被驱逐（见 5.3）；此时应调大 master 的 `--default_kv_lease_ttl` 而非单独 prefetch 参数。

### 2.6 配置开关（vLLM-Ascend 侧 mooncake.json）
| 字段 | 含义 |
|---|---|
| `enable_ssd_offload` | 必须 true，否则无 SSD-only key，prefetch 无意义 |
| `ssd_offload_path` | SSD 存储目录（绝对路径，需与 master `--root_fs_dir` 一致） |
| `enable_ssd_prefetch` | 开启 prefetch-on-exist |
| `ssd_prefetch_cooldown_sec` / `ssd_prefetch_dedup_ttl_sec` | 可选；不配置则用 Mooncake 默认（5s/30s），vLLM-Ascend 侧不设默认 |
| `ssd_get_wait_ms` | get 侧等待 prefetch 完成的最大预算（毫秒）；默认 10，`0` 关闭 |

### 2.7 预取提升后的 lease 保护
`NotifyPromotionSuccess` 在 `from_prefetch=true` 时，对提升出的 MEMORY 副本发放与 `exist`/`get` **相同**的租约：`GrantLease(default_kv_lease_ttl_, default_kv_soft_pin_ttl_)`（grouped key 走 `GrantLeaseForGroup`，逻辑与 `GetReplicaList` 一致）。

| 参数 | 配置方式 | 默认值 | 含义 |
|---|---|---|---|
| `default_kv_lease_ttl` | master 启动参数 `--default_kv_lease_ttl`（ms） | 5000 | 硬租约：窗口内不会被容量驱逐 |
| `default_kv_soft_pin_ttl` | master 启动参数 `--default_kv_soft_pin_ttl`（ms） | 见 master 配置 | soft-pin 续期，与 exist/get 一致 |

> **已废弃**：早期曾用环境变量 `MOONCAKE_SSD_PREFETCH_PROTECT_SEC`（默认 2s soft-pin、**lease=0**）单独保护预取 key；现统一复用 master 的 `default_kv_lease_ttl` / `default_kv_soft_pin_ttl`，该环境变量**不再读取**。

### 2.8 硬 lease 调优与 Put 容量风险

**背景**：早期实现中，预取提升出的 MEMORY 副本在 `NotifyPromotionSuccess` 时若仅 `GrantLease(0, soft_pin_ttl)`，硬 lease 立即过期，key 在 `exist→get` 窗口（实测中位 15~17s）内易被容量驱逐回 SSD，`prefetch_evicted_after_exist` 偏高。

**变更 1（代码，2026-06）——预取提升后的 lease 对齐 exist/get**

| 维度 | 变更前 | 变更后 |
|---|---|---|
| 触发点 | `NotifyPromotionSuccess`，`from_prefetch=true` | 同左 |
| 租约 | `GrantLease(0, protect_ms)` 或等价 soft-pin-only | `GrantLeaseForGroup` → **`GrantLease(default_kv_lease_ttl_, default_kv_soft_pin_ttl_)`** |
| 与 exist/get | 不一致（硬 lease=0） | **与 `GetReplicaList` / `BatchExist` 续租行为一致** |

实现见 `master_service.cpp` 中 `NotifyPromotionSuccess` 的 `from_prefetch` 分支（§5.3）。

**变更 2（部署配置）——按需调大 master 硬 lease TTL**

| 参数 | 默认值 | 推荐调优场景 | 目的 |
|---|---|---|---|
| `--default_kv_lease_ttl` | 5000 ms | `exist→get` 中位数接近或超过 5s 时，可增至 **10000 ms** 或更高 | 覆盖更长调度窗口，降低预取 key 在 get 前被驱逐概率 |
| `--default_kv_soft_pin_ttl` | master 默认 | 与 exist/get 一致 | soft-pin 续期，与变更 1 配套 |

> **注意**：`GetReplicaListForPrefetch` / `BatchGetReplicaListForPrefetch` **仍不发 lease**（§2.2）；上述 lease 仅作用于 **预取 promotion 完成**（`NotifyPromotionSuccess`）以及后续 **exist/get 路径上的续租**。预取 metadata 查询本身不会 pin 对象。

**Put 失败风险（容量竞争，属预期内副作用）**

硬 lease + soft-pin 会延长 MEMORY 副本的驱逐豁免期。在 **DRAM 高水位**（如 `memory_ratio>0.9`）、**高并发长上下文**、且 **`offload_on_evict` 队列 defer**（Master 日志 `[EVICT] No memory freed … deferred for disk offload`）时，Master 当轮可能腾不出 segment handle，Client 侧出现：

```
BatchPut failed for N keys due to insufficient space  (NO_AVAILABLE_HANDLE)
```

| 维度 | 说明 |
|---|---|
| **是否异常** | 高压 + 大 lease 下的**容量竞争**，非功能 bug |
| **是否影响当前请求推理** | **一般不影响**：上层 `put()` 通常仅记录错误、不中断已完成的 prefill；Put 在后台线程异步执行 |
| **实际影响** | 失败 block **未写入 Mooncake 外部池**，后续同 prefix 请求的 external hit 可能下降，需 NPU 重算或走 SSD get |
| **缓解** | 增大 `global_segment_size`、降低 `eviction_high_watermark_ratio`、压测轮次间 idle 等待 offload drain、或权衡缩短 `default_kv_lease_ttl`（会增加驱逐后读 SSD 风险） |

**与 offload 提交顺序（§8 B10）的关系**：lease 对齐解决预取 key **被容量驱逐**；`BatchOffload` 先 commit 本地索引再 `NotifyOffloadSuccess` 解决 **Master 已登记 LOCAL_DISK、Client 索引未就绪** 导致的 get `INVALID_KEY`。二者正交，生产环境应同时合入。

## 3. 为什么这样设计（决策理由）

### 3.1 复用 promotion 执行底座
`PromotionAllocStart → PromotionWrite → NotifyPromotionSuccess` 已是 Mooncake 成熟的、带 PROCESSING 中间态保护（`is_completed()==false` 期间 eviction 不会动它）的 SSD→DRAM 数据通路。重复造一套传输/分配/状态机既冗余又易错。

### 3.2 绕开 promotion-on-hit 的触发与节奏（核心）
直接复用 promotion-on-hit 队列存在 5 个问题，故另起专用路径（见第 4 节详述）。

### 3.3 跨节点委托（方案 B）
KV 副本可能在远端节点的 SSD 上。让 holder 节点“就地”把自己的 SSD 读入自己的 DRAM，避免无谓的跨节点数据搬运；原生 `get` 已支持从远端 DRAM 取数，预取只需把数据暖到 holder 的 DRAM 即可。

## 4. 不采用的错误方案及原因

### 方案 X（错误）：直接复用 promotion-on-hit 队列触发 prefetch
即 `exist → Query() 入队 → 循环调 ProcessPromotionTasks()`。能跑通，但长期有 5 个问题，故否决：

| # | 问题 | 说明 |
|---|---|---|
| 1 | **准入闸挡住 prefetch** | promotion-on-hit 入队前有频率门槛（Count-Min sketch）、DRAM 水位、队列上限。prefetch 的典型场景是“第一次 exist 就要暖”，但 `promotion_admission_threshold>1` 时第一次根本不入队，prefetch 形同未发生 |
| 2 | **`Query` 副作用污染系统** | prefetch 内用 `Query()` 走的是 `GetReplicaList`：会发 lease（pin 住对象、扰乱 eviction/offload 节奏）、抬高 sketch 热度、污染 `valid_get_nums`/cache hit 指标。一次“只查存在”被做成“假装 Get” |
| 3 | **执行节奏是为‘懒提升’设计** | promotion-on-hit 每次心跳最多吐 1 个 task、默认 10s 心跳一次（UT 要等 25s）。prefetch 需要亚秒~数秒内完成，靠心跳来不及。手动循环调 `ProcessPromotionTasks()` 是在“慢车道开快车”，与原设计拧着且易与心跳线程并发处理同一队列 |
| 4 | **与专用 `PrefetchKeys` 双路径打架** | 若 `Query 入队` 与 `PrefetchKeys 直调` 同时跑，会对同一 key 各做一次 promotion，触发 `REPLICA_IS_NOT_READY` |
| 5 | **资源争抢** | promotion 队列上限 / DRAM alloc / in-flight slot 全局共享。大批量 prefetch 会挤掉真正 Get 触发的 hot-key promotion，或在 DRAM 紧张时互相阻塞 |

**采用方案如何规避：** 用只读 `GetReplicaListForPrefetch`（解决 1、2）+ 立即执行的 `PrefetchKeys`（解决 3）+ `RegisterPrefetchTask` 不推心跳队列（解决 4）+ 独立 RPC 不蹭 admission（解决 5）。

### 其它否决的小方案
- **每次 detached `std::thread` 触发预取**：高频探测会爆出海量线程争抢 SSD IOPS/带宽并狂申请 DRAM → 改为固定线程池。
- **整批 metadata 查完才 register**：大批量时首个 register 过晚、与 get 竞态 → 改为 chunk 批量 Query + 逐 chunk 流水线 register（见 2.1）。

## 5. 可能需要优化的点

### 5.3 预取提升后的 lease 保护（已实现）

**问题（历史）**：`PutEnd`/普通 promote 出来的新 MEMORY 副本初始 `GrantLease(0, soft_pin_ttl)`，lease 立即过期，直到首次 `get()` 才续 `default_kv_lease_ttl`。prefetch 路径在 `NotifyPromotionSuccess` 完成后同样面临此问题：叠加 `exist→get` 间隔可达 15s，大量预取好的 key 会在被使用前因容量驱逐回 SSD（早期压测中约 82% 的“触发过预取却仍走 SSD”属于此类）。

**实现（2026-06）**：`from_prefetch=true` 时在 `NotifyPromotionSuccess` 内调用 `GrantLeaseForGroup`（等价于 `GrantLease(default_kv_lease_ttl_, default_kv_soft_pin_ttl_)`），与 `GetReplicaList` / `BatchExist` 续租行为对齐。保护时长由 master 的 `--default_kv_lease_ttl` / `--default_kv_soft_pin_ttl` 统一控制（默认 lease 5s）。

**调优建议**：若 `prefetch_evicted_after_exist` 仍偏高且 `exist→get` 中位数 > `default_kv_lease_ttl`，优先调大 master lease TTL（长上下文场景常用 **10000 ms**，见 §2.8）；若间隔极长（>15s），lease 无法覆盖全窗口，需结合 DRAM 容量或调度侧优化。调大 lease 时注意 §2.8 所述 **Put 失败（NO_AVAILABLE_HANDLE）** 风险。

**废弃项**：`MOONCAKE_SSD_PREFETCH_PROTECT_SEC` 不再使用。

1. **prefetch 与原生 promotion-on-hit 的 in-flight 计量弱共享**
   两者共用 `promotion_tasks` map、`promotion_in_flight_` 计数与 `promotion_queue_limit_` 上限。共用 map 带来跨路径去重（正向收益），但共用上限意味着大批量 prefetch 可能挤占原生 promotion-on-hit 的名额（反之亦然）。如需隔离，可为 prefetch 增设独立的 in-flight 计数与上限。该共享是 v0.3.11 既有行为，非 main 同步引入。

2. **prefetch 路径不感知多租户（tenant_id）**
   main 引入了 `tenant_id`（多租户），但 `GetReplicaListForPrefetch` / `RegisterPrefetchTask` 当前固定走 `"default"` 租户（`MakeObjectIdentity(key, "default")`）。若要支持多租户 prefetch，需让 prefetch 的 RPC 透传 `tenant_id`。当前对单租户/default 场景完全正确。

3. **节流参数自适应**
   `ssd_prefetch_cooldown_sec` 理想值应与“DRAM 从高水位驱逐到目标水位的实际耗时”挂钩，目前为静态默认 5s。可考虑按运行时驱逐速率自适应。

4. **prefetch 命中率可观测性**
   压测验证时 P 侧日志 + `analyze_prefetch.sh`（仓库内分析脚本）统计 `[GET-SRC]` / `[PREFETCH-OUTCOME]`。主要指标：
   - GET 读盘：DRAM / SSD 条数及占比
   - **SSD预取有效读DRAM** = `prefetch_hit + promoted_untracked`
   - 子项：`prefetch_hit`、`promoted_untracked`、`预取失败/超时`（`prefetch_miss_race`）、`驱逐后读SSD`（`prefetch_evicted_after_exist`，含无 register / 有 register 子项）、`exist误trigger`（`prefetch_dram_was_resident`）、`dram_resident`
   - 可选：`SHOW_DETAIL=1` 输出 `prefetch_wait_mode`（local / master / none）分布

5. **metadata 查询与 register 延迟（阶段 1 已缓解，阶段 2 待做）**
   阶段 1：`BatchGetReplicaListForPrefetch` 减少 client↔master RPC 往返；流水线 register 缩短 exist→首个 task 登记时间。阶段 2：master 侧 `BatchGetReplicaListForPrefetch` 仍逐 key 调 `GetReplicaListForPrefetch`，可改为单次批量查元数据以进一步降延迟。

6. **get()-side 等待与 lease 驱逐的权衡**
   `ssd_get_wait_ms` 默认 10ms；对 TP1~7 等非 exist 触发 rank，get 侧会通过 **master Query 轮询**（日志 `prefetch_wait_mode=master`）等待 promotion 完成。预取完成后 key 已获 `default_kv_lease_ttl` 保护（5.3）；若 `exist→get` 仍超过 lease 窗口，仍可能被驱逐，需调大 master lease 或优化调度间隔。

## 6. 涉及的主要代码位置（Mooncake）

| 文件 | 改动要点 |
|---|---|
| `mooncake-store/include/types.h` | 新增 `CONFIG_KEY_SSD_PREFETCH_*` / `CONFIG_KEY_SSD_GET_WAIT_MS` 及默认值 |
| `mooncake-store/include/real_client.h` / `src/real_client.cpp` | `PrefetchThrottle`（含 `promote_attempted`、`defer reserve` 至 BatchQuery 后）、`prefetch_pool_`、`triggerSsdPrefetch`（chunk 批量 + 流水线）、`runLocalPrefetch`、`RunLocalPrefetchRegisterAndPromote`、`ClassifySsdPrefetchRoute`、`TryRefreshBestMemoryReplica`、`ClassifyPrefetchOutcome`；`batch_get_into_multi_buffers_internal` 内 get-wait（local / master 双路径）与 `[GET-SRC]`/`[PREFETCH-OUTCOME]` 日志（含 `prefetch_promote_attempted`） |
| `mooncake-store/include/master_service.h` / `src/master_service.cpp` | `GetReplicaListForPrefetch`（只读）、`RegisterPrefetchTask`（不入心跳队列）、`NotifyPromotionSuccess`（`from_prefetch` 时 `GrantLeaseForGroup` 对齐 exist/get lease），适配 main 的租户作用域 |
| `mooncake-store/include/master_client.h` / `src/master_client.cpp` | prefetch RPC 客户端；含 `BatchGetReplicaListForPrefetch` |
| `mooncake-store/include/rpc_service.h` / `src/rpc_service.cpp` | `WrappedMasterService` 封装与 RPC handler 注册（含 batch prefetch） |
| `mooncake-store/include/client_service.h` / `src/client_service.cpp` | `QueryForPrefetch` / `BatchQueryForPrefetch` / `RegisterPrefetchTask` |
| `mooncake-store/include/file_storage.h` / `src/file_storage.cpp` | `PrefetchKeys()`：封装完整 promotion 执行链；`AllocateBatch`/`BatchLoad` 使用 `MakeTenantScopedStorageKey` 作为 staging map 键（修复 B8 INVALID_KEY） |
| `mooncake-store/include/storage_backend.h` / `src/storage_backend.cpp` | `BucketStorageBackend::BatchOffload` 先 commit `object_bucket_map_` 再 `NotifyOffloadSuccess`；notify 失败时 `RollbackCommittedBucket` + `CleanupOrphanedBucket`（见 §8 B10、`ssd-offload.md` 写盘流程） |
| `mooncake-store/include/replica.h` / `store_c.*` / `pyclient.h` / `dummy_client.*` | 签名/描述符/ C API / Python binding 适配（`ExistOptions.prefetch_to_memory`、`prefetch` 参数） |
| `mooncake-integration/store/store_py.cpp` | pybind：`setup` 增加 `ssd_prefetch_*` 参数、`is_exist`/`batch_is_exist` 的 prefetch 选项 |

## 7. 从 v0.3.11 同步到 main 的注意事项（已处理）
- main 引入 `tenant_id`，所有 `setup_real` 签名把 `ssd_prefetch_cooldown_sec` / `ssd_prefetch_dedup_ttl_sec` **追加到 `tenant_id` 之后**（带默认值，不影响 main 既有调用点）。
- master 端 prefetch 实现按 main 的租户作用域元数据模型改写：`MetadataAccessor(this, key)` → `MetadataAccessor(this, MakeObjectIdentity(key, "default"))`；`accessor.GetShard()->promotion_tasks` → `accessor.GetTenantState().promotion_tasks`。
- `get_config_size` 在 main 返回 `std::optional<size_t>`，读取节流配置处改用 `.value_or(默认值)`。
- **第 4 节 5 个问题对应的‘专用路径’规避在 main 上完整保留**（只读 RPC 不发 lease/sketch/不入队、`RegisterPrefetchTask` 不推心跳队列、`PrefetchKeys` 立即执行）。
- 待办：在 Linux/Ascend 环境实际编译验证 `Mooncake`（Windows 无法编译）。

## 8. 历史 Bug 与修复记录（排障日志）

> 本节按“观察到的现象 → 根因 → 修复/结论”记录开发过程中真实踩过的坑，作为换会话后对齐上下文的依据。第 4 节侧重“方案选型理由”，本节侧重“实际故障与定位”。

### B1. prefetch 风暴 → offload 饿死（最严重，催生第 2.3/2.4 节的节流与线程池）
- **现象**：压测时日志大量出现 `REPLICA_IS_NOT_READY`、`INVALID_KEY`、`NO_AVAILABLE_HANDLE`，伴随 `KV load failure`；`mooncake_master` 持续刷 DRAM eviction 日志，`memory_ratio` 卡在 ~0.81 反复空转驱逐，offload 迟迟写不进 SSD。
- **根因**：vLLM 调度 busy-loop 会对 waiting 队列里同一请求的**同一批 SSD-only block 反复探测 `exist`**；早期每次探测都 `detached std::thread` 直接发起预取 → 海量并发预取抢占**单 IO 线程 / SSD IOPS / DRAM 分配名额**，把正常的 offload（DRAM→SSD）挤到饿死；DRAM 一直在高水位 → master 反复尝试驱逐却腾不出空间。
- **修复**：① `ssd_prefetch_dedup_ttl_sec`（默认 30s）同一 key 窗口内只预取一次——**主要起效项**，直接消除风暴；② `ssd_prefetch_cooldown_sec`（默认 5s）DRAM 饱和退避；③ detached 线程改为固定大小 `prefetch_pool_`。
- **结论**：本数据集规模下 cooldown 实际未触发（dedup 已足够、DRAM 未打满）。

### B2. 双路径双提升 → `REPLICA_IS_NOT_READY`
- **现象**：早期同时存在“`Query()` 入 promotion-on-hit 队列”与“`PrefetchKeys` 直调”两条路，对同一 key 各做一次 promotion，报 `REPLICA_IS_NOT_READY`。
- **根因/修复**：见第 4 节方案 X 问题 #4。最终只保留专用路径：`RegisterPrefetchTask` **不推 holder 心跳 `promotion_objects` 队列**，避免与直调重复提升。

### B3. `setup_real` 抽象类编译错误
- **现象**：编译报 `invalid new-expression of abstract class RealClient`（无法 new 抽象类）。
- **根因**：给 `setup` 追加 `ssd_prefetch_cooldown_sec`/`ssd_prefetch_dedup_ttl_sec` 参数时，只改了部分声明，导致 `real_client.h`、`pyclient.h`、`dummy_client.h` 之间 `setup` 纯虚函数签名不一致 → `RealClient` 未完全覆盖基类纯虚函数，仍是抽象类。
- **修复**：保持三处（基类声明 / `RealClient` / `DummyClient` / pybind）`setup` 签名**完全一致**，新参数统一追加在 `tenant_id` 之后并带默认值。后续任何透传新参数都要同步全链路签名。

### B4. `QueryResult` const 成员 → 移动赋值被删除（get-wait 复查路径）
- **现象**：实现 get()-side “等一次后复查 DRAM 副本”时编译失败，提示 `tl::expected` 的移动赋值被 `delete`（因 `QueryResult` 含 const 成员，无法对已存在变量重新赋值）。
- **修复**：用 `std::optional<QueryResult>` 承载复查结果（`.emplace()` 而非赋值），绕开不可移动赋值的限制。该写法用于 2.5 节 get-wait 中 **master 轮询复查**路径（`prefetch_wait_mode=master`）。

### B5. get 来源/等待日志埋点放错函数
- **现象**：在 `get_buffer_internal` 加的 “GET-SRC / 等待” 日志在部分集成用例里始终不打印。
- **根因**：vLLM-Ascend 实际调用的是 `batch_get_into_multi_buffers_internal`（批量多 buffer 路径），而非 `get_buffer_internal`。
- **结论/修复**：日志与 get-wait 逻辑须挂在 `batch_get_into_multi_buffers_internal`。这是判断“get 走 DRAM 还是 SSD”“是否撞上在途预取”的正确观测点——也是得出 “exist→get ~15s、SSD→DRAM ~2ms、~82% 预取后被驱逐” 等结论的数据来源函数。

### B6. `LEASE_EXPIRED`（已知项，非本特性引入）
- **现象**：大批量请求日志出现 `LEASE_EXPIRED`（如多 TP rank 上若干 key 批量传输超时）。
- **根因**：客户端 lease 默认约 5s；当一次 query→transfer 的批量很大、整体耗时超过 lease 有效期时，对象 lease 在传输完成前过期。PD 分离下 P 从 D 远程拉 DRAM 时更易触发。属客户端租约时序问题，与 prefetch 专用路径无直接因果（专用只读 RPC 本就**不发 lease**）。
- **处置**：记录为已知项；与 5.3 节 master 侧容量驱逐（lease 保护）是相关但不同的两个问题（前者是客户端取数租约超时，后者是 master 侧 DRAM 容量驱逐）。

### B7. metadata 串行查询 + exist 同步 reserve → `prefetch_miss_race` / `prefetch_dram_was_resident` 偏高
- **现象（批量化 / defer reserve 前）**：首个 `registered task` 比首个 `[GET-SRC]` 晚约 **143ms**；`prefetch_hit` ~40%；`prefetch_miss_race` ~58%（含大量虚高）；修正后约 **25%** 的 GET 为 `prefetch_dram_was_resident`（exist 误 trigger）。
- **根因 1（B7 原）**：早期 `triggerSsdPrefetch` 对每个 key 单独 `QueryForPrefetch`，且**等全部 key 查完**才统一 register；大批量 metadata 阶段耗时长，get 侧 10ms 预算内 master 尚未登记 task。
- **根因 2（defer reserve）**：`PrefetchThrottle::reserve()` 曾在 exist 同步路径执行，对**已在 DRAM 的 key** 也记 trigger；get 读 DRAM 却误入 prefetch 统计（`prefetch_dram_was_resident`）。exist 只知「存在」，不知副本层级，须等 BatchQuery 过滤。
- **修复（阶段 1）**：`BatchQueryForPrefetch`（128 key/chunk）+ 每 chunk 完成后立即 `RunLocalPrefetchRegisterAndPromote`（流水线 register）；**reserve 移到 BatchQuery 确认 SSD-only 之后**。
- **验证（批量化 + defer reserve + outcome 拆分后）**：`prefetch_dram_was_resident`≈**0**；`prefetch_hit` **86.4%**；`prefetch_miss_race` **0.7%**；`prefetch_evicted_after_exist` **10.0%**；exist→首个 register ~**83ms**（较修复前 143ms 缩短）。

### B8. `PrefetchKeys` staging 键不匹配 → `INVALID_KEY`
- **现象**：预取路径偶发 `INVALID_KEY` / staging slice missing。
- **根因**：`PrefetchKeys` 内 `AllocateBatch`/`BatchLoad` 的 map 键须为 tenant-scoped **storage key**（`MakeTenantScopedStorageKey`），与 logical object key 不一致时查不到 slice。
- **修复**：`PrefetchKeys` 对 staging 使用 `storage_key`，logical key 仍用于 master promotion RPC。

### B9. `analyze_prefetch.sh` 统计 `prefetch_hit` 漏计（grep 顺序）
- **现象**：分析脚本曾显示 `prefetch_hit_master/local=0`，与日志肉眼不符。
- **根因**：脚本用单向 grep（要求 `prefetch_wait_mode=master` 在 `outcome=prefetch_hit` 之前），而实际日志字段顺序相反。
- **修复**：改为双向 grep（`| outcome=prefetch_hit.*prefetch_wait_mode=master`）。

### B10. `BatchOffload` 先 NotifyOffloadSuccess、后 commit 本地索引 → get 路径 `INVALID_KEY`
- **现象**：高并发 offload + get/prefetch 并发时，大量 `Failed to get key`（`res: -400` = `INVALID_KEY`）；`BatchLoad` / `SSD read failed` 与 prefetch `BatchLoad failed` 并存；Master `SSD Storage` 远未满、`EvictDiskReplica=0`；部分请求级 load 失败。
- **根因**：Mooncake `BucketStorageBackend::BatchOffload` 在 **写盘成功后** 先调 `complete_handler`（内部 `NotifyOffloadSuccess`，Master 登记 `LOCAL_DISK` 并可驱逐 MEMORY），**之后**才 commit Client 侧 `object_bucket_map_`。
           并发 get/prefetch 在此窗口内走 SSD 读路径，但本地索引尚无该 storage key → `Key not found` → `INVALID_KEY`。与 SSD 容量无关，是 **Master 可见性早于 Client 本地索引** 的一致性问题（`publish-before-commit`）。
- **错误顺序（修复前）**：
  ```
  WriteBucket → NotifyOffloadSuccess → [竞态窗口] → commit object_bucket_map_
  ```
- **修复（2026-06，`storage_backend.cpp`）**：
  ```
  WriteBucket → commit object_bucket_map_ → NotifyOffloadSuccess
  ```
  若 `NotifyOffloadSuccess` 失败：`RollbackCommittedBucket` 回滚本地索引 + `CleanupOrphanedBucket` 删除 orphan 文件，避免「Client 能读、Master 不知道」或反向 ghost replica。
- **与 B8 区别**：B8 是 PrefetchKeys **staging 键名**错误；B10 是 offload **提交顺序**导致 get/offload 读路径索引缺失。B8 修复不能消除 B10。
- **与 §2.8 lease 变更关系**：lease 对齐解决预取 key **被驱逐**；B10 解决 **索引未就绪** 导致的 `INVALID_KEY`，二者正交，需同时合入。

### 关键实测数据（驱动上述决策的数字，便于对齐）
- `exist→get` 间隔中位 **15~17s**（受调度排队放大）。
- SSD→DRAM 单次预取 **~2ms（p90 2.65ms）**。
- 约 **82%** 的“触发过预取却仍走 SSD”（lease 对齐前），原因是提升出的 MEMORY 副本 lease=0、在 `get()` 前就被容量驱逐回 SSD；§5.3 实现后预取路径已与 exist/get 对齐 lease。
- 节流：`dedup_ttl` 为消除风暴的主要因素，`cooldown` 未触发；预取带宽未打满。
- 批量化 / defer reserve **前**：metadata 阶段致首个 register 比首个 get 晚 **~143ms**；`prefetch_hit` ~40% / 有效读 DRAM ~64%；`exist误trigger` ~25%。
- 批量化 + defer reserve + outcome 拆分**后**：`prefetch_hit` **86.4%**；`exist误trigger` **0**；`prefetch_miss_race` **0.7%**；`prefetch_evicted_after_exist` **10.0%**（主因 TP1~7 master 等待超时 / lease 窗口不足，见 §5.3、§2.8）。
- B10 修复后：get 路径 `INVALID_KEY` 归零；高压尾部仍可能出现 `BatchPut insufficient space`（§2.8 Put 风险），一般不阻断单次推理成功。
- `ssd_get_wait_ms` 默认 **10ms**（get 侧等待已默认开启）；`0` 可关闭。
