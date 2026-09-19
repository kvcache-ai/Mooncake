# RFC: 面向 Agent × 多模态推理场景的 EPD 分离与协同调度方案

**RFC-EPD-2026-FINAL**

| 项目 | 内容 |
|------|------|
| 标题 | 以统一多模态状态对象为核心的 EPD 三阶段分离、跨 Agent 步骤 KV 复用与协同调度架构 |
| 状态 | Active |
| 日期 | 2026-07-04 |
| 依赖 | Mooncake Transfer Engine / MooncakeStore, vLLM (PagedAttention + MooncakeConnector), Qwen3-VL |
| 目标模型 | Qwen3-VL 系列多模态大语言模型（DeepStack 多层视觉特征注入） |
| 目标硬件 | 阶段一 A6000（无 RDMA，TCP 主 + SHM）；阶段二 A6000 Pro（RoCE/IB RDMA + GPUDirect，RDMA 加速 P→D） |
| 仓库 | [Pinoeer-kingxi/Mooncake](https://github.com/Pinoeer-kingxi/Mooncake) |

---

## 1. 问题定义

### 1.1 两股趋势的交汇

**多模态大模型推理的 EPD 三阶段化。** 多模态大语言模型（MLLM）的推理天然包含计算特征差异显著的三个阶段：Vision Encoder 将图像编码为视觉特征（含 DeepStack 多层特征束），LLM 对完整输入序列执行一次前向计算生成 KV Cache（Prefill），随后逐 token 自回归生成（Decode）。传统 PD（Prefill-Decode）分离把 Encoder 与 Prefill 耦合在同一节点，忽略了视觉编码阶段的可缓存性与计算模式差异——同一张图在多轮对话中反复出现时无法跨请求复用编码结果。

**Agent 工作流的状态化。** Agent 工作流对推理基础设施提出四类新需求：(a) 多分支推理（Tree-of-Thought / Beam Search）需要从同一中间状态派生并行分支并共享 KV；(b) 多轮对话与工具调用需要跨轮次持久化状态、在函数调用阻塞期间卸载显存；(c) A2A（agent-to-agent）状态交接需要在协作智能体间共享上下文（含视觉上下文）；(d) **同一 Agent 工作流的多个子步骤往往共享上下文前缀（系统提示、历史轮次、共享图像），如何跨 Step 复用 KV 以减少重复 Prefill** 是降低 Agent 端到端延迟的关键。

### 1.2 交叉空白与本方案定位

据多轮调研，现有工作各自占据一个象限，但 **Agent 状态管理 × 多模态 EPD 分离 × 跨步骤 KV 复用** 的三者交集基本空白：

- **多模态 EPD 分离侧**：EPD-Serve、Efficiently Serving Large Multimodal Models via EPD Disaggregation、Nova、ModServe、vLLM-Omni 实现了三段分离、异步特征预取与跨阶段并行，但**无 Agent fork / 共享 / 多轮状态语义**。
- **Agent 状态共享侧**：KVCOMM、TokenDance、PolyKV、DroidSpeak 实现了多 Agent 间 KV Cache 锚点对齐、集体共享与不对称压缩，但**只面向纯文本 KV，未涉及视觉特征束的跨节点传输与多层注入**。
- **多模态多轮缓存侧**：Kamera、MPIC、VL-Cache、FastCache 解决了多模态 KV 的位置无关重用与压缩，但**停留在单进程单请求视角，无分布式 fork 与 Agent 调度**。
- **跨步骤复用侧**：RelayCaching、RKSC、CachedAttention、SwiftCache 探索了跨步骤 / 跨轮 KV 复用与选择性重算，但**未与多模态 EPD 传输、Agent 状态协同统一**。

本方案的目标是填补这一三者交集：以**统一多模态状态对象**为枢纽，把 EPD 分离的传输能力、Agent 状态的克隆 / 共享能力、多模态特征的可缓存能力、跨步骤 KV 复用能力，统一在同一套「引用计数 + 分层传输 + 弹性调度 + 步骤间前缀复用」基础设施之上。

---

## 2. 设计思路

### 2.1 状态是一等公民，计算围绕状态编排

Agent 的每一步操作（fork 一个分支、发起一次工具调用、把视觉上下文交接给协作 Agent、推进到下一个子步骤）都被建模为对 *MultimodalState* 的引用增减与跨节点迁移，而非对张量的复制。这带来三个直接收益：

- **克隆开销与分支数解耦**：fork 一个带视觉上下文的分支只需增加引用计数（O(1)），不拷贝 DeepStack 特征束。
- **传输策略与数据语义对齐**：KV 页与视觉特征束的分布、可压缩性、延迟敏感性不同，统一对象让传输层按字段选择策略。
- **跨步骤 / 多轮 / A2A 自然统一**：跨步骤复用 = 命中前一步骤状态的前缀；多轮持久化 = 把状态的 KV 页集打 TTL 标签并可能卸载；A2A 交接 = 把状态的引用从一个 Agent 迁移到另一个。三者复用同一套引用、迁移与 TTL 机制。

### 2.2 三个维度的设计原则

- **工程设计原则**：所有共享状态以页（block）为最小粒度引用计数；引用计数原子化、生命周期受控（先收集后释放，杜绝迭代中删除）；前缀缓存以 token 序列内容寻址（RadixAttention 范式），杜绝采样哈希碰撞；视觉特征束以图像内容哈希寻址。
- **数据传输原则**：传输策略由 *TransferPolicy* 按数据类型 + 链路 + 硬件能力自动决策；协议无关（TCP / SHM / RDMA 可切换）；流式优先以重叠传输与计算；RDMA 用于 P→D 大块 KV 的带宽加速，TCP 为功能完备基线。
- **调度原则**：EPD 三段比例弹性可调并支持角色切换（append-prefill on decode）；Agent 图节点携带优先级与截止时间；函数调用阻塞时主动卸载 KV 腾出显存；跨步骤前缀命中时跳过重复 Prefill。

### 2.3 硬件约束的现实主义与演进路径

阶段一目标环境为 A6000 集群（无 RDMA）：以 **TCP 为主、SHM（同机 CUDA IPC）为加速、RDMA 为预留**，所有传输接口协议无关，功能完整性与基准对比在 TCP 下即可达成。阶段二迁移至 A6000 Pro（具备 RoCE/IB RDMA + GPUDirect）：在协议无关接口下把 P→D 大块 KV 切换为 RDMA 直写 GPU PagedAttention 页（绕过主机内存中转），作为带宽加速路径；其余链路按硬件能力自动选择。这避免方案依赖阶段性不可用硬件，同时为硬件升级预留清晰加速点。

### 2.4 系统假设与一致性模型（工程约束层）

本节是后续所有设计的语义地基。任何与本节冲突的实现细节以本节为准。

**一致性模型 = per-workflow snapshot isolation + 跨工作流 bounded staleness。**

- **工作流内 snapshot isolation**：每个 Agent 工作流（workflow_id）持有一个单调递增的 `snapshot_epoch`。fork、跨步骤复用查询、A2A 读 都在该工作流当前 epoch 的**一致快照**上执行；写（decode 追加、CoW 新页）创建新版本，**不破坏旧 epoch 快照**。这保证 fork 看到的是一个稳定的、因果完整的状态版本，而非"计算到一半的 live state"。
- **跨工作流 bounded staleness**：KV Directory（§6.7）的元数据跨工作流最终一致，落后量有界（目录条目在 `staleness_bound`，默认 ≤1s 内全集群可见）。跨工作流共享 KV（如两个 Agent 复用同一图像 FeatureBundle）允许读到略旧的目录条目，但读到的条目一定是一个**曾经真实存在且 refcount>0 的稳定版本**——不会读到半提交或已回收的块。
- **不追求全局 linearizability**：KV 页的读写不要求全局线性一致。强一致只施加在「同一 workflow 内的元数据版本序」与「物理页释放的原子性」两点上，其余放宽。这是把 GDKR 从全局热点锁中解放出来的关键（§6.7）。

**KV 复用语义 = 近似缓存 + 永远可回退（approximate, fallback-safe）。**

- 跨步骤 / 非连续 KV 复用是**性能优化**，不是 correctness 依赖。任何复用路径都存在一条 **full-recompute fallback**：当复用预判不满足正确性闸门（§6.5 KV Validity Check、§6.8 发散阈值熔断）时，退化为标准全量 Prefill。
- **硬约束**：复用不得改变模型语义输出超出容差 ε（默认 ε：任务成功率回退 ≤2%，与 B0 baseline 对齐）。correctness **永远不依赖**复用命中——即使所有复用全部失效退回重算，系统仍输出正确结果，仅损失性能。
- 受限场景下复用可达 **bit-exact 等价**（§6.8 Provable Equivalence 定理：后缀复用 + 纯追加插入 + attention mask 不变 → 复用与全量重算的 attention 输出逐位一致）；条件破坏时回退到近似 + verify，再不满足则全量重算。三层降级。

**控制面语义 = 最终一致目录 + 分片权威。** GDKR 拆为 KV Directory（最终一致、分片）、Scheduler（本地去中心化）、per-shard Consistency（仅 shard 内元数据 Raft），见 §6.7。热路径（refcount 增减、fork、CoW 提权）不经 Raft。

**硬件不依赖。** RDMA 是加速路径；correctness、一致性、调度正确性均不依赖 RDMA 可用。RDMA 失效退回 TCP，语义不变。

---

## 3. 架构方案

### 3.1 总体架构（四层 + 控制面/数据面解耦）

系统在纵向上分为四层，自底向上：

- **传输层（Transfer Layer）**：封装 Mooncake Transfer Engine，提供协议无关的 `transfer(refs, plan, target)` 原语。按链路类型暴露 `E2P` / `P2D` / `A2A` / `Offload` 四类通道，每类通道内部按硬件能力选择 TCP / SHM / RDMA。`TransferPolicy` 是这层的策略大脑，负责为每次传输选择 mode / compress / precision / prefetch（详见 §5）。
- **状态层（State Layer）**：管理 *MultimodalState* 的生命周期——页级引用计数表、页级 CoW、RadixTree 内容寻址前缀树、FeatureBundle 内容哈希缓存、TTL 与卸载。所有 Agent 状态操作都落到这里。
- **流水线层（Pipeline Layer）**：编排 Encoder / Prefill / Decode 三类 Worker，支持 chunked-prefill、stage 间流水线重叠、缓存命中时跳过 Encoder、Omni 多阶段（AR→Generation→Diffusion）扩展。
- **协同层（Coordination Layer）**：在流水线之上提供 Agent fork / A2A 交接、Agent-EPD 协同调度（图优先级 + 弹性比例 + 角色切换 + 阻塞卸载）、跨步骤 KV 复用决策，面向上层 Agent 框架暴露接口。

依赖关系单向：协同层 → 流水线层 → 状态层 → 传输层。每一层可独立替换（如传输层从 Mooncake 换为自研 RDMA 库）而不影响上层。

**横向上控制面/数据面解耦（Control/Data Plane Disaggregation）。** 为在每秒数千次并发的 Agent 图调度中保持微秒级元数据同步，并把分布式一致性问题从张量计算路径上剥离，系统再横切为两个正交平面：

- **控制面（Control Plane）— GDKR 三件拆分**：由 **KV Directory**（最终一致、按 `workflow_id` 分片的元数据索引）、**Scheduler**（本地去中心化调度）、**Consistency Manager**（仅 per-shard 元数据强一致）三件组成（详见 §6.7）。**热路径（refcount 增减、fork、CoW 提权 CAS）下沉到 shard-local 本地内存，不经 Raft**；Raft 只介入跨分片 2PC handoff、epoch 提交与批量物理释放。这避免控制面成为 Raft log 爆炸与 CAS 热点瓶颈。
- **数据面（Data Plane）— 计算与传输**：Encoder / Prefill / Decode Worker 与传输层。数据面**不自行推断跨节点引用计数**，仅在控制面下发物理释放/复制指令后执行张量操作。

二者通过轻量 RPC 解耦：控制面下发「Block 租约 / 提权 / 路由 / 释放」指令，数据面上报「写入触发 / 传输完成 / 异常」事件。这一解耦把分布式一致性（CoW 竞态、Use-After-Free、孤儿块回收）集中在控制面用 per-shard CAS + Raft 解决，数据面保持纯计算语义，详见 §6.7。

### 3.2 各阶段 Worker 的职责

**Encoder Worker**：在指定 GPU 上运行 Vision Encoder，输出 *DeepStackFeatureBundle*——包含 ViT 末层 hidden states（注入 LLM 第 0 层 embedding）以及若干中间层特征图（分别注入 LLM 不同 transformer 层）。FeatureBundle 是 MultimodalState 的一个字段。命中图像内容哈希缓存时直接返回缓存的 FeatureBundle，跳过 ViT 计算。图像哈希采用三段式极速策略（URL→元数据→全量，详见 §6.9），避免大图全量哈希拖慢 TTFT。

**Prefill Worker**：接收 FeatureBundle + 文本 token IDs，按 DeepStack 协议将各层特征注入到对应 LLM transformer 层（非单层投影拼接），执行一次前向（`use_cache=True`），产出 KV Cache（以页表形式，非整张量）与首 token logits。支持 chunked-prefill；支持从 RadixTree 命中前缀直接续接（O(Δ) 增量 Prefill）。**跨步骤精确前缀发散时，采用虚拟 RoPE 与非连续 KV 复用（Virtual-PagedAttention）零精度损失地复用后缀块，而非强制重算**（详见 §6.8）。特征注入采用异步层级流水线 + 乱序容忍调度，消除多层等待气泡（详见 §6.10）。

**Decode Worker**：以首 token logits 为起点自回归解码；每步将新 KV 追加为页，写时若该页被多分支共享则触发页级 CoW（由控制面 GDKR 通过 CAS 提权仲裁，详见 §6.7）。**角色切换（append-prefill on decode）采用双引擎算力隔离**——Decode CUDA Graph 与增量 Prefill 分属不同 CUDA Stream/MPS 池，互不破坏（详见 §6.11），而非简单软切换。

### 3.3 与 vLLM 的集成路径

选择 vLLM 作集成框架：v0.23.0+ 内置 `MooncakeConnector`，提供 P→D 的 KV 传输基础设施与 `kv_transfer_config`（`kv_producer` / `kv_consumer`）角色接口，且已对齐 PagedAttention block 粒度。集成策略是**复用 MooncakeConnector 处理 P→D，在其之上插拔 Encoder 与 Agent 协同**：

1. P→D：直接用 vLLM MooncakeConnector 的页级 KV 传输（天然支持本方案的页级引用计数）。
2. E→P：在 vLLM serving 层之上插入 Encoder Worker，通过 TransferPolicy 把 FeatureBundle 传到 Prefill 节点，再作为 `inputs_embeds` + 多层 DeepStack hooks 注入。
3. Agent 协同：作为 vLLM 之外的一层 Adapter 暴露 `fork(parent_state) -> child_state` / `handoff(state, to_agent)` / `release(state)` / `advance_step(state, new_tokens)` 接口，内部操作状态层；不改 vLLM 推理核心，便于上游 PR。

### 3.4 数据通路总览

```
Image ──► [Encoder Worker] ──FeatureBundle──► (E→P: SHM/RDMA/stream) ──► [Prefill Worker]
                                                                       │ (DeepStack 多层注入 + chunked-prefill + RadixTree 前缀续接)
                                                                       ▼
                                                                  KV 页表 (paged)
                                                                       │
                                       ┌───────────────────────────────┤ (P→D: RDMA 直写/TCP, 页级增量)
                                       ▼                               ▼
                            [Decode Worker A]                  [Decode Worker B]   (角色切换/弹性比例)
                                       │                               │
                                       └──────────┬────────────────────┘
                                                  ▼
                                     MultimodalState (KV页集 + FeatureBundle + meta)
                                          │   │   │   │
                              fork(引用+1)│   │   │   │ advance_step(跨步骤前缀复用)
                                ┌─────────┘   │   │   └────────┐
                                ▼             ▼            ▼
                          分支 KV增量    TTL/卸载(工具调用阻塞)  下一步骤状态(命中前缀→O(Δ))
                                              │
                                              ▼ handoff(A2A, 引用迁移)
                                        协作 Agent
```

---

## 4. 关键技术方案一：MultimodalState 统一状态对象

### 4.1 状态对象的定义（带版本化）

> **设计修正（呼应 §2.4 snapshot isolation）**：v1 的 MultimodalState 是无版本的扁平对象，fork 与 decode 写并发时无法保证 fork 看到一致快照。v2 引入版本化：fork 必须在 `snapshot_epoch` 的一致快照上执行，写创建新版本不破坏旧快照。

```python
@dataclass
class BlockRef:
    global_block_id: str     # KV Directory 全局唯一寻址 ID（owner shard 分配）
    physical_node_id: str    # 当前驻留的物理节点 ID
    logical_index: int       # 在当前 Sequence 中的逻辑位置
    virtual_offset: int      # 虚拟位置偏移（Virtual-RoPE 修正系数，见 §6.8）

@dataclass
class MultimodalState:
    state_id: str
    workflow_id: str         # 所属 Agent 工作流（跨步骤复用范围 + 分片键）
    step_index: int          # 工作流内步骤序号
    # --- 版本化（snapshot isolation 核心）---
    version_id: str          # 本版本唯一 ID（不可变）
    parent_version_id: str   # 父版本（fork/advance_step 的前驱）
    snapshot_epoch: int      # 所属快照 epoch（单调递增；同 epoch 内读到一致快照）
    approximate: bool = False  # 是否经近似复用产出（§6.5 layer 3）
    # --- 内容引用（指针，非数据）---
    kv_pages: List[BlockRef] # KV 页引用列表
    feature_bundle_id: str   # DeepStack 多层特征束引用
    token_ids: List[int]     # 文本 token 序列（RadixTree 寻址）
    image_ids: List[str]     # 图像内容哈希（FeatureBundle 缓存寻址）
    # --- 生命周期 ---
    status: str              # ACTIVE | OFFLOADING | HANDING_OVER（状态锁，见 §6.7）
    ttl_deadline: float      # TTL（monotonic clock）
```

关键点：

- `kv_pages` 与 `feature_bundle_id` 都是**引用**（BlockRef / FeatureBundleRef），指向 owner shard 登记的物理页 / 特征束。状态对象本身轻量（仅指针 + 元数据），因此 fork 一个状态 = 复制指针 + 在 owner shard 中对所指 GID 引用计数 +1，开销 O(1)。`BlockRef.virtual_offset` 是跨步骤非连续复用的核心字段（§6.8）。
- **版本不可变**：一个 `version_id` 对应的 `kv_pages` 引用集合一旦提交即不可变。decode 追加新 token 不就地改旧版本，而是产出 `parent_version_id = 旧版本` 的新版本；CoW 新页也是新版本的一部分。这使旧版本可作为稳定快照被 fork / 跨步骤复用读取，不受并发写影响。
- **fork 在快照上执行**：`fork(parent)` 创建子状态时，子状态 `snapshot_epoch` 继承父状态当前 epoch、`parent_version_id = parent.version_id`。父状态后续的 decode 写不会改变子状态已绑定的版本——子状态看到的是 fork 时刻的一致快照，杜绝"读到计算到一半的 live state"。
- **执行模型**：decode = 对不可变 KV 的只读 attention + append-only 写新页；跨 fork 无 in-place mutation。CoW 是"复制后写"，从不覆写共享页。

### 4.2 页级引用计数 + Copy-on-Write（分布式版）

借鉴 ForkKV 的 DualRadixTree（物理共享 + 逻辑私有）与 PagedAttention 的 block 级管理，**引用计数与 CoW 都在页（block）粒度**；在分布式环境下，引用计数与 CoW 仲裁**上移到控制面 GDKR 的 owner shard**，规避跨节点竞态（详见 §6.7）：

- **物理页池 + 全局 GID**：每个物理 KV 页（如 16 token / 页）在 KV Directory 中有全局唯一 GID 与 `refcount`，由 owner shard（按 `workflow_id` 分片）权威持有。多个 MultimodalState 共享同一页时 `refcount` 累加；fork 只增加 `refcount`，不拷贝数据。数据面节点本地仅缓存「本节点持有该 GID 指针的实例数」，**不作回收判定依据**。
- **页级 CoW + CAS 提权（shard-local）**：当某分支 Decode 产生新 token 落入一个 `refcount > 1` 的共享页时，节点向 owner shard 发起 `PromoteBlock(GID, NodeID)` 提权请求。owner shard 在**本地内存**用原子 CAS 判定（不经 Raft）：率先到达者获得写权限、指向本地新分配物理页、旧页 `refcount -= 1`；稍后到达者收到 `STATUS_COW_REQUIRED`，在本地复制该页后修改。**仅复制被写入的页**，代价 O(单页)；并发擦写冲突由 shard 本地串行化仲裁，杜绝 Use-After-Free。
- **多级 fork 传递性**：A→B→C 链式 fork 中，每级只独立持有对底层 GID 的引用；释放 B 时 owner shard 递减其引用，A 与 C 的引用不受影响（各自独立持引用，而非通过 B 间接持有）。

### 4.3 DeepStack 特征束的引用计数

FeatureBundle 同样以引用计数管理：多轮对话中反复引用同一张图时，多个 MultimodalState 的 `feature_bundle_id` 指向同一份物理特征束，owner shard 中 `refcount` 累加。这统一了「视觉特征缓存复用」与「KV 页共享」两套原本割裂的机制。

### 4.4 生命周期安全：主动异常传播 + Reaper 兜底

Agent 工作流极不稳定（第三方工具超时、网络中断），高频 fork 的分支若因异常挂掉而数秒不释放，会瞬间 OOM 或显存碎片化。本方案采用**作用域绑定的 RAII + 主动异常传播为主、Reaper 为兜底**的两级回收：

- **确定性事件驱动 GC（主动异常传播）**：上层协同层把 Agent 的 Context Lifespan 与 MultimodalState 绑定（RAII 语义）。Agent Step 结束正常产出 `State_N` 时注册到 KV Directory 并绑定 TTL；**一旦监听到任何 Async Task Cancel（如 `asyncio.CancelledError`、工具 API 超时、代码执行崩溃），框架层 `on_error` Hook 立即向 owner shard 发 RPC，毫秒级下发指令清空该链路独占的 KV Block**，杜绝显存碎片堆积。
- **Reaper 降级为保底**：后台 Reaper 周期扫描仅作为「防止内存死锁 / 处理 RPC 丢失」的最终兜底，不再是主要释放路径。
- **原子引用计数（shard-local）**：`refcount` 增减在 owner shard 本地内存原子化执行，不经 Raft，杜绝非原子竞态。
- **受控释放**：释放分支时 owner shard 先递减引用、再判断 `refcount == 0` 才向数据面下发物理释放指令（物理释放持久化走 per-shard Raft，批量异步）；**遍历与删除分离**——先收集待释放 GID，遍历结束后批量下发，杜绝「迭代中删除」bug。

### 4.5 前缀缓存：内容寻址前缀树

采用 **RadixAttention（SGLang）内容寻址前缀树**：

- **键 = token id 序列前缀**：沿 RadixTree 匹配最长公共前缀，命中则复用对应 KV 页（`refcount + 1`），未命中部分才计算。天然支持「共享系统 prompt」「共享图像描述前缀」「跨步骤共享上下文前缀」，且无碰撞。
- **强制锚点对齐（Anchor Point）**：在 Agent 推理框架层（LangGraph / AutoGen）规范序列拼接顺序——共享静态数据（系统词、长期不变的图像描述）严格排在头部，动态生成的步骤思考排在尾部。这使 RadixTree 最长公共前缀命中率最大化，发散点永远发生在尾部，把选择性重算/Virtual-RoPE 的触发率压到 < 15%，从根源规避位置编码错位。
- **FeatureBundle 寻址 = 三段式极速哈希**：视觉特征束以三段式哈希寻址（URL/path → 元数据 → 全量），独立于 token 树，避免大图全量哈希拖慢 TTFT（详见 §6.9）。
- **淘汰**：LRU + TTL（monotonic clock）。TTL 同时服务于多轮持久化与跨步骤复用（见 §6.3 / §6.5）。


---

## 5. 关键技术方案二：TransferPolicy 分层传输策略

### 5.1 传输策略的四维参数化

传输层暴露一个统一的协议无关原语：

```python
def transfer(
    refs:    List[BlockRef | FeatureBundleRef],  # 待传输的状态引用
    plan:    TransferPolicy,                      # 策略（mode/compress/precision/prefetch）
    target:  NodeID,                              # 目标节点
) -> TransferHandle:                              # 异步句柄，可 await
```

`TransferPolicy` 由四个正交维度组成，由策略大脑根据「数据类型 × 链路类型 × 硬件能力 × 目标延迟」自动选择：

| 维度 | 取值 | 决策依据 |
|------|------|----------|
| **mode** | `stream` / `pull` / `push_batch` / `shm` | 链路 + 数据量：同机→shm；跨机大块→stream 重叠计算；小元数据→pull 按需 |
| **compress** | `none` / `cacheGen` / `splitzip` / `kvcodec` / `per_level` | 数据类型：KV 页→cacheGen 流式或 per_level 按层；FeatureBundle→per_level；元数据→none |
| **precision** | `bf16` / `fp8` / `spectrum` / `q4` | 带宽瓶颈 vs 质量：P→D 大块在带宽受限时降精度；FeatureBundle 保持 bf16 |
| **prefetch** | `none` / `next_step` / `speculative` | Agent 语义：跨步骤可预测前缀→next_step 预取；分支 fork→speculative |

### 5.2 按链路类型的策略分配

四类通道各有默认策略画像，再由硬件能力（`hw_caps` 探测结果）微调：

- **E→P 通道（FeatureBundle，小而延迟敏感）**：
  - 同机：`shm` + `none` + `bf16`（CUDA IPC 零拷贝，μs 级）。
  - 跨机：`stream` + `per_level` + `bf16`。FeatureBundle 按 DeepStack 注入层分片流式发送，Prefill 节点边收边注入对应层，重叠传输与可用层的计算。压缩用 `per_level`（不同层特征图重要性不同，末层保精度、中间层可轻压缩），避免一刀切。
- **P→D 通道（KV 页表，大而带宽敏感）**：
  - TCP 环境：`stream` + `cacheGen`（3.5–4.3× 流式压缩）+ `bf16`/`fp8`（按带宽余量）。页级增量传输，Decode 节点边收边解码。
  - RDMA 环境：`push_batch` + `none`/`fp8` + 直写 GPU PagedAttention 页（绕过主机内存中转）。带宽充裕时优先 `none` 保精度，仅带宽饱和时降 `fp8`。
- **A2A 通道（多 Agent 状态交接，元数据 + KV 增量）**：
  - 元数据（state_id / 页表指针 / token_ids）走 `send`（tiny）。
  - KV 增量走 KVDirect 风格的 `pull`：目标 Agent 仅按需拉取自己 RadixTree 缺失的页，避免全量推送。压缩用 PolyKV 风格不对称压缩（发送方压缩、接收方持有压缩态），`spectrum` 精度。
- **Offload 通道（工具调用阻塞时卸载到 MooncakeStore）**：
  - `push_batch` 到远端主机内存池；工具返回时 `pull` 拉回。带 TTL，超时由状态层回收。

### 5.3 per_level 压缩与多层特征束的对齐

DeepStack 把 ViT 的不同层特征注入 LLM 不同 transformer 层，这些特征图的重要性分布不均——注入靠近输出的层对最终生成影响更大。`per_level` 压缩据此对每层特征图采用不同策略：末层 hidden state 保 `bf16`、中间层特征图可用 `fp8` 或轻量量化，使 FeatureBundle 整体传输量下降而质量损失可控。这与 KV 页的 `spectrum`（按 token 重要性的混合精度）思路一致，但作用对象是特征束层而非 token。

### 5.4 硬件能力探测与协议切换

启动时 `hw_caps` 探测：NIC 类型（普通以太 / RoCE / IB）、是否 GPUDirect RDMA、是否同机 NVLink、是否支持 CUDA IPC。传输层据此为每条通道选择实现：

- 无 RDMA（A6000）：E→P 同机走 SHM/CUDA IPC、跨机走 TCP；P→D 走 TCP + cacheGen 压缩；A2A 走 TCP + pull。
- 有 RDMA（A6000 Pro）：P→D 切换为 RDMA 直写 GPU 页（注册 PagedAttention block 内存为 MR，RC QP，write-with-imm / 消费端 read）；E→P 跨机走 RDMA send；A2A 走 RDMA read 按需拉取。

**关键约束**：所有路径都实现同一 `transfer()` 接口，无任何代码路径硬依赖 RDMA 存在。切换仅在配置层 `transfer.protocol: tcp|rdma|auto` 完成。A6000 阶段的功能验证、基准对比全部在 TCP 下达成；A6000 Pro 阶段作为带宽加速路径打开 RDMA，预期 P→D 带宽从 10–25 Gbps 提升至 100–200 Gbps。

### 5.5 策略空间爆炸的治理：Cost Model + Adaptive Controller

> **设计修正**：v1 的四维参数化 `mode × compress × precision × prefetch` = 4×5×4×4 = 320 种策略，纯靠静态启发式选择会退化为次优路由。v2 引入在线 cost model + 自适应控制器，把 320 维空间压缩到每链路 3–4 个候选。

- **Cost Model**：对每次传输在线估计 `cost ≈ α·compute_bound + β·transfer_bytes/bandwidth + γ·queue_delay`，参数从历史传输实测回归（每条链路维护 EWMA）。候选策略只从该链路画像的 3–4 个默认策略（§5.2）中选 cost 最小者，不做全空间搜索。
- **Adaptive Controller**：控制器周期性对比"所选策略的实际耗时"与"次优候选的预测耗时"，若次优连续 N 次预测更优则切换默认画像（bandit 风格探索-利用）。这避免静态画像在负载变化时僵化，同时把决策复杂度从 O(320) 降到 O(常数)。
- **降级保底**：cost model 不可用（冷启动）时回退 §5.2 的静态默认画像，功能不受影响。


---

## 6. 关键技术方案三：Agent 协同调度与跨步骤复用

### 6.1 EPD 弹性比例与角色切换

借鉴 EPD-Serve 的阶段级解耦 + 异步特征预取、Prefill-Deflection / PPD 的 append-prefill-on-decode、以及 xLLM 的动态 PD/EPD 分解，本方案对 EPD 三段比例与角色做弹性管理：

- **基线比例 E:P:D ≈ 1:2:4**：Encoder 计算量小且可缓存（命中率高的工作流甚至可缩到 1 个 Encoder Worker），Prefill 计算密集需较多算力，Decode 访存密集且是吞吐瓶颈需最多节点。比例随负载动态调整（Encoder 命中率高时把更多 GPU 让给 Prefill/Decode）。
- **异步特征预取**：Encoder 产出的 FeatureBundle 在 Prefill 真正需要前就开始 E→P 流式传输（stream mode），Prefill 到达时特征已就绪，掩盖 E→P 延迟。这是 EPD-Serve 的核心收益来源（+57% 吞吐、−71% TTFT）。
- **角色切换（append-prefill on decode）**：当 Decode 节点空载或跨节点 KV 传输代价过高时，把"续接已有 KV 的增量 Prefill"路由到 Decode 节点本地执行，而非回送到 Prefill 节点。这避免了一次 P→D 大块 KV 传输，借鉴 Prefill-Deflection 的 −68%/−81% TTFT 收益。多轮对话的 Turn-2+ TTFT 因此显著下降。**角色切换采用双引擎算力隔离（§6.11）**——Decode CUDA Graph 与增量 Prefill 分属不同 CUDA Stream/MPS 池，仅当增量 token <64 时触发本地 Prefill，否则 deflect 回 Prefill 节点，保护 TPOT 不恶化。
- **动态分解**：xLLM 风格的全局 KV 管理器监控三类 Worker 负载，当某类成为瓶颈时触发角色切换或弹性扩缩。

### 6.2 Agent 图优先级调度

借鉴 ProServe（优先级 + 抢占式调度）、QLLM（查询级 DAG 规划）、Halo（DAG 查询计划），在协同层维护 Agent 工作流的有向图：

- **图节点**：每个 Agent 子任务是一个节点，携带 `priority`（思考型/交互型/混合型）、`deadline`（截止时间）、`workflow_id`、`parent_state`（父状态引用，用于 fork 或跨步骤复用）。
- **路由**：THINKING 节点路由到高算力 Prefill 资源（长复杂 prompt）；INTERACTIVE 节点路由到低延迟 Decode 资源（快速响应）；HYBRID 走均衡路径。加权评分函数综合剩余算力、队列深度、GPU 利用率、平均延迟，并支持 `prefer_high_capacity` / `prefer_low_latency` 偏好。
- **批量保序**：批量请求按优先级排序路由，但结果按原始索引重排返回，避免乱序。
- **抢占**：高优先级节点可抢占低优先级节点的 Decode 资源；被抢占节点的 KV 通过 Offload 通道卸载到 MooncakeStore（带 TTL），恢复时拉回。**抢占式 Offload/Restore 采用非阻塞异步流水化 + In-flight State Lock**，保证高优任务立即获得 Slot 而不被卸载 IO 阻塞，极速返回时原地恢复（时序见 §6.12）。

### 6.3 工具调用阻塞与多轮 TTL

借鉴 Continuum（KV TTL + 程序级 FCFS，8× JCT）与 CachedAttention（分层 GPU/CPU/磁盘 KV + preload + 异步 save，−87% TTFT）：

- **工具调用阻塞卸载**：Agent 子任务发起函数调用（如检索、代码执行）时，其 KV 短时间内不再被访问。协同层主动把该状态的 KV 页通过 Offload 通道卸载到 MooncakeStore，腾出 GPU 显存承接新请求；工具返回时通过 preload + 异步拉回，掩盖恢复延迟。FeatureBundle 因体积小且很可能复用，优先留在 GPU 或迁到 CPU 而非远端磁盘。**卸载/恢复闭环时序与 In-flight 极速返回处理见 §6.12**。
- **多轮 TTL**：每个 MultimodalState 带 `ttl_deadline`（monotonic clock）。多轮对话的上下文 TTL 较长（覆盖整个会话），工具调用阻塞的中间状态 TTL 较短（覆盖调用窗口）。TTL 到期且 `refcount == 0` 时 owner shard 下发物理页回收。
- **O(Δ) 增量**：新一轮对话只对新增 token 做 Prefill，复用前几轮的 KV 页（RadixTree 命中前缀），增量计算代价 O(Δ) 而非 O(全序列)。

### 6.4 Agent State Cloning（fork）

基础任务 2 的核心。fork 在状态层实现，开销 O(1)。**关键：fork 在父状态当前 `snapshot_epoch` 的一致快照上执行**（§4.1），子状态绑定父状态的 `version_id`，父状态后续的 decode 写创建新版本而不影响子状态已绑定的快照。

```python
def fork(parent_state: MultimodalState, child_agent_id: str) -> MultimodalState:
    """从父状态的一致快照派生分支，零拷贝。fork 看到的是 snapshot, 不是 live state。"""
    child = shallow_copy(parent_state)            # 复制指针 + meta，不复制数据
    child.state_id = new_uuid()
    child.version_id = new_uuid()                 # 子状态新版本
    child.parent_version_id = parent_state.version_id  # 绑定父状态当前快照版本
    child.snapshot_epoch = parent_state.snapshot_epoch  # 继承快照 epoch
    child.meta.agent_id = child_agent_id
    for page_ref in parent_state.kv_pages:
        kvdir.incref(page_ref.global_block_id)   # owner shard 本地 refcount+1（不经 Raft）
    feature_store.incref(parent_state.feature_bundle_id)
    register_reaper(child.state_id, ttl=child.meta.ttl_deadline)  # 异常泄漏兜底
    return child
```

分支 Decode 产生新 token 落入共享页时触发页级 CoW（§4.2），仅复制被写入的单页。多级 fork（A→B→C）中每级独立持引用，释放 B 不影响 A、C。跨节点 fork 通过 A2A 通道把父状态引用迁移到目标节点（引用迁移 + 物理页 pull 按需）。

### 6.5 跨 Agent 步骤 KV 复用（核心创新方向）

这是用户指定的深入方向，也是本方案相对现有工作的主要创新点。**同一 Agent 工作流中，多个子步骤往往共享上下文前缀**——系统提示、前几轮对话历史、共享图像及其描述。若每一步都从头 Prefill，重复计算开销显著；若能跨步骤复用前一步骤的 KV，则后续步骤只需对增量 token 做 Prefill。

> **正确性总纲（呼应 §2.4）**：跨步骤 KV 复用是**性能优化**，永远存在 full-recompute fallback。复用决策由 **KV Validity Check** 闸门把关：估计复用引人的 attention 偏差，超容差 ε 即回退全量重算。correctness **绝不依赖**复用命中。

#### 6.5.1 复用的正确性约束：为什么"裸拼接"是错的

Transformer attention 是 **path-dependent** 的：某 token 的 attention 输出同时依赖 (a) 其 KV 内容、(b) 其位置编码、(c) 与所有前缀 token 的相对位置关系（attention mask + RoPE）。原版 RelayCaching 直接拼接历史 KV 块有两类 correctness 破坏：

- **位置破坏**：中间插入新内容后，后缀 token 的绝对位置整体顺延，但被复用的旧 KV 块仍带旧位置编码 → RoPE 角度错位。
- **attention 归一化破坏**：softmax 分母 = 对所有前缀 key 的 exp 求和。插入新 token 改变了 key 集合，**即使位置编码修正完美，softmax 归一化分布仍不同**——被复用块的 attention 权重被新 key 稀释。

因此本方案的复用不是"裸拼接 + offset 修正"，而是 **segment-level cache with verification gate**：复用前先估计偏差，可证等价时直接复用，近似可接受时复用并标记，否则全量重算。

#### 6.5.2 三层复用决策

按"能精确命中就不重算、能可证复用就不近似、能近似就不全算、任何一层都有 fallback"分四层降级：

1. **精确前缀复用（首选，零损失）**：步骤 N 的输入与步骤 N−1 输出存在公共前缀。RadixTree 最长公共前缀匹配，命中部分 KV 页 `refcount + 1`（O(1)），仅对增量 token 做 Prefill（O(Δ)）。FeatureBundle 按内容哈希命中跳过 Encoder。**此层 attention mask 与位置完全不变，是严格等价复用。**

2. **Virtual-RoPE 非连续复用（可证等价场景，见 §6.8）**：前缀发散但满足受限条件（**纯追加插入 + 后缀 token 集合不变 + attention mask 不变**）时，对后缀块直接复用旧 KV，Attention 内核读 VPV 实时修正 RoPE 角度。§6.8 给出 bit-exact 等价定理与证明。**条件破坏即触发熔断，不可侥幸。**

3. **近似复用 + Verify Gate（发散更深的回退）**：当发散打破 §6.8 的可证条件（如中间插入改变后缀 token 集合，或 mask 变化），进入近似层。复用前运行 **KV Validity Check**：
   - **attention divergence estimator**：用前一步骤的 attention 分布作参考，对受影响段预估 attention entropy drift（基于被插入 key 数量与原 attention 熵的闭式上界估计，无需重算 attention）。
   - **recompute gate**：若估计 drift ≤ ε（默认对应任务成功率回退 ≤2%），允许近似复用并在状态上标记 `approximate=true`；否则回退全量重算。
   - 借鉴 RKSC 的 attention-similarity sharing + confidence-gated early exit 作为本层的可选加速。

4. **Full-recompute fallback（兜底，永远存在）**：以上任一闸门不满足，或复用预判失败，执行标准 Chunked-Prefill 全量重算发散点之后的所有文本，仅保留最长公共绝对前缀。**此路径保证输出与不启用复用的系统逐位一致。**

#### 6.5.3 跨步骤状态链与 TTL

为支撑跨步骤复用，状态层为每个 `workflow_id` 维护一条**步骤状态链**：步骤 N 产出的 MultimodalState 不立即释放，按 `workflow_id + step` 索引存入 RadixTree 与 FeatureBundle 缓存，TTL 覆盖整个工作流预期步数。步骤 N+1 发起时，调度器先查询链上前驱状态，命中前缀则复用。工作流结束后整链释放（或按 LRU 淘汰）。

```python
def advance_step(prev_state: MultimodalState, new_tokens: List[int], new_images: List[str]) -> MultimodalState:
    """推进到下一步骤。四层降级复用，correctness 永不依赖复用命中。"""
    # 1. 精确前缀匹配（在 prev_state 的 snapshot_epoch 一致快照上）
    hit_blocks, matched_length = kvdir.match_prefix(new_tokens, scope=prev_state.workflow_id,
                                                     epoch=prev_state.snapshot_epoch)
    reused_features = [feature_store.get(h) for h in new_images if feature_store.has(h)]
    delta_tokens = new_tokens[matched_length:]

    # 2. 发散分析与可证条件检查（§6.8）
    vpv, provable = analyze_divergence(new_tokens, matched_length, prev_state)

    if provable:  # 纯追加 + 后缀不变 + mask 不变 -> bit-exact 等价
        new_kv = engine.execute_virtual_rope_prefill(
            tokens=delta_tokens, prefix_blocks=hit_blocks, offsets=vpv[matched_length:],
            delta_features=[img for img in new_images if not feature_store.has(img)],
            inject_features=reused_features)
    else:
        # 3. 近似层：KV Validity Check
        drift = estimate_attention_drift(prev_state, hit_blocks, delta_tokens)  # 闭式上界,无需重算
        if drift <= EPS:
            new_kv = engine.execute_virtual_rope_prefill(  # 近似复用,标记
                tokens=delta_tokens, prefix_blocks=hit_blocks, offsets=vpv[matched_length:],
                delta_features=[img for img in new_images if not feature_store.has(img)],
                inject_features=reused_features, approximate=True)
        else:
            # 4. Full-recompute fallback(永远存在)
            new_kv = engine.execute_standard_prefill(new_tokens[matched_length:], prefix_kv=hit_blocks)

    # 5. 注册新状态(新 snapshot_epoch),前驱状态 TTL 续期
    new_state = kvdir.commit_new_state(prev_state.workflow_id, prev_state.snapshot_epoch,
                                       hit_blocks + new_kv, new_tokens)
    state_layer.refresh_ttl(prev_state, step_window=EXPECTED_STEPS)
    return new_state
```

#### 6.5.4 复用收益与正确性的度量

- **步骤复用率**（性能）：工作流中步骤 2..N 的 Prefill 计算量被前驱缓存命中的比例，及步骤 2..N TTFT 相对步骤 1 的下降。
- **复用安全率**（correctness）：复用请求中通过可证等价 / verify gate 的比例，以及回退全量重算的比例。**回退比例过高只意味着性能损失，不影响正确性**——这是 §2.4 fallback-safe 语义的直接体现。
- **质量回归**（correctness 硬指标）：启用复用 vs 关闭复用，在全部数据集上的任务成功率差，须 ≤ ε（2%）。

### 6.6 A2A 状态交接

多 Agent 协作中，一个 Agent 把上下文（含视觉上下文）交接给另一个 Agent。这本质是 MultimodalState 的引用迁移，由控制面 GDKR 以**两阶段提交（2PC）**保证跨节点一致性（边界情况 E，详见 §6.7）：

- **Phase 1（Prepare）**：源节点向 owner shard 的 Consistency Manager 发起交接申请，跨分片 2PC 将目标物理页元数据置为 `HANDING_OVER`（per-shard Raft 记录）——此时目标页对源节点只读、对目标节点不可见。
- **Phase 2（Commit）**：目标节点完成物理增量拉取（KVDirect 风格 pull，仅拉取自己 RadixTree 缺失的页）并向 Consistency Manager 回报 `ACK_SUCCESS`；2PC 原子变更所有权，向源节点异步发送 `RELEASE_DECREMENT`。
- **超时回滚**：若规定窗口（如 200ms）内目标节点未响应（网络分区/节点宕机），Consistency Manager 强行将元数据回滚至源 shard，状态树复原，避免产生全网无法回收的「孤儿 KV 块」。
- FeatureBundle 按 `image_ids` 命中；借鉴 KVCOMM 的 anchor-pool 在线维护跨上下文 KV 偏移锚点（纯文本场景 7.8× 加速），本方案把锚点扩展到包含 FeatureBundle 引用，使视觉上下文同样可锚点复用。

### 6.7 控制面：GDKR 三件拆分与分片（去中心化）

> **设计修正（呼应 §2.4）**：原 v1 的 GDKR 把「KV 注册表 + 调度器 + 一致性管理」三合一，并以全局 Raft 处理每次 fork/CoW 的元数据更新，在 10K+ QPS 的 Agent 工作流下会成为 Raft log 爆炸与 CAS 热点瓶颈。v2 将控制面拆为三个职责正交、一致性要求不同的组件，**热路径不经 Raft**。

#### 6.7.1 三件拆分

| 组件 | 职责 | 一致性 | 部署 |
|------|------|--------|------|
| **KV Directory** | GID → 物理节点 + refcount + 状态锁 的元数据索引 | **最终一致**，按 `workflow_id` 分片；跨分片只读复制 | 内存型，分片集群 |
| **Scheduler** | EPD 弹性比例、Agent 图优先级、抢占、角色切换路由 | **本地去中心化**，每节点独立决策 + gossip 同步负载视图 | per-worker |
| **Consistency Manager** | 仅 per-shard 元数据的强一致（状态机版本序、2PC handoff、物理释放原子性） | **per-shard Raft**，仅管单分片内元数据 | 分片内 3 副本 |

**关键：热路径下沉到 shard-local。** fork 的 refcount+1、CoW 提权的 CAS 都在 **owner shard 本地内存**完成（同一 workflow 的状态由同一 shard 权威持有），不写 Raft log。Raft 只在以下情形介入：(a) 跨分片 A2A handoff 的 2PC；(b) shard 内状态机 epoch 提交（罕见，每 step 一次）；(c) 物理页释放的持久化（批量、异步）。这把 Raft ops/sec 从"每 fork/decode 一次"降到"每 step 一次 + 批量释放"，远离 10K–50K ops/sec 的 Raft 天花板。

#### 6.7.2 分片与权威所有权

- **按 workflow_id 分片**：同一工作流的所有 MultimodalState、KV 页元数据落在同一 shard，由该 shard 权威持有 refcount 与状态锁。Tree-of-Thought 的 128 路分支同属一个 workflow，故其并发 CoW CAS 全在单 shard 本地内存解决，无跨节点锁。
- **跨工作流共享**（如两 Agent 复用同一图像 FeatureBundle）：共享块的元数据在 owner shard 权威，其他 shard 持最终一致只读副本（bounded staleness，§2.4）。读副本可能略旧，但只读到 refcount>0 的稳定版本，不会读到半提交块。
- **Directory 与 Consistency 解耦**：Directory 是可重建的缓存索引（数据面 worker 上报重建），Consistency Manager 才是权威。Directory 全损可从数据面重建，不丢数据。

#### 6.7.3 边界情况（v1 边界 A/E/D 的去中心化重述）

**边界 A：多分支并发写入同一共享页的提权竞态。** 128 路 Beam 分支同属一个 workflow，共享末页 `Block_Shared_0`（refcount=128）落在 owner shard 本地。各分支并发 `PromoteBlock(GID, NodeID)`：shard 本地原子 CAS 串行提权，率先者（branch_1）获写、旧页 refcount−1（127）；其余 127 路 CAS 失败获 `STATUS_COW_REQUIRED`，各自本地复制该页后修改。**全程 shard 本地内存，无 Raft、无跨节点锁**。彻底规避 Use-After-Free 与页内容互相覆盖（压测见 §8.5）。

**边界 E：A2A 交接的网络分区/节点宕机。** 跨分片 handoff 走 §6.6 的 2PC + 超时回滚。Consistency Manager 在 prepare 阶段把目标页置 `HANDING_OVER`（per-shard Raft 记录），200ms 未 ACK 则回滚，源分片保留所有权。保证无孤儿 KV 块。

**边界 D：工具调用阻塞期间的状态颠簸（In-flight State Lock）。** State 进入卸载流程时 owner shard 将其置 `OFFLOADING` 并加本地锁（shard-local，不经 Raft）；极速返回触发 Restore 时拦截请求、下发 `ABORT_DMA`、就地解锁恢复（In-place Reclamation）。锁语义（re-entrancy、concurrent-fork-during-offload、restore-after-fork race）见 §6.13。

### 6.8 计算面创新一：虚拟 RoPE 与非连续 KV 复用（核心突破）

**设计目标**：跨步骤精确前缀发散时，实现高比例 KV 复用且**严格不破坏 attention 正确性**。针对原版 RelayCaching 裸拼接导致位置错位与 softmax 归一化破坏两类 correctness 缺陷（§6.5.1），重构 PagedAttention 内核，引入 **Virtual-PagedAttention**，并给出可证等价定理与近似回退闸门。

#### 6.8.1 Virtual-PagedAttention 机制

**逻辑位置解耦**：允许一个请求的逻辑 token 序列映射到物理上非连续、且具有离散绝对位置编码的 KV Block。当 Agent 工作流中间插入新内容（如工具调用 JSON 返回），系统不再强制重算后缀，而是对后缀 token 直接复用旧 KV Block，但在 Attention 计算时传入一维 **虚拟位置偏移向量（Virtual Position Vector, VPV）** `position_offsets`。

**Attention 修正**：CUDA Kernel 在计算 $Q \cdot K^T$ 时实时读取 VPV 修正 RoPE 角度：

$$\mathbf{q}_m = \mathbf{W}_q \mathbf{x}_m \, e^{i (m + \Delta_m) \theta}$$

其中 $\Delta_m$ 是该 token 在 VPV 中注册的虚拟位置偏移量。对后缀复用块，其位置角度保持原样；对新插入块，其位置角度顺延。

#### 6.8.2 Provable Equivalence 定理（可证等价场景）

> **定理**：设步骤 N−1 产出 KV 块序列 $S = [b_1, \dots, b_p \| b_{p+1}, \dots, b_n]$（前缀 $b_{1..p}$，后缀 $b_{p+1..n}$）。步骤 N 的输入为 $S' = [b_1, \dots, b_p \| c_1, \dots, c_q \| b_{p+1}, \dots, b_n]$，即在前后缀之间**纯追加**插入新块 $c_{1..q}$，且：
> (C1) 后缀块 $b_{p+1..n}$ 的 KV 内容不变（只读复用，未被重算）；
> (C2) attention mask 为标准因果 mask，插入不改变后缀 token 对前缀的可见性（新块 $c$ 只增可见、不删可见）；
> (C3) 后缀 token 在 $S'$ 中的逻辑位置 = 原位置 + q（VPV 恰补偿这一偏移）。
>
> 则对后缀任一 token $m \in [p+q+1, n+q]$，其在 $S'$ 上复用旧 KV + VPV 修正后的 attention 输出，**与在 $S'$ 上全量重算的 attention 输出逐位等价（bit-exact）**。

**证明草图**：
- **位置编码**：RoPE 对 query/key 施加 $e^{i\,pos\cdot\theta}$。后缀 token 在 $S'$ 中真实逻辑位置为 $m+q$；VPV 给其注册偏移 $\Delta_m = q$，使 kernel 实际使用角度 $(m+\Delta_m)\theta = (m+q)\theta$，与全量重算一致。被复用块 $b$ 的 key 角度同理由其 VPV 项补偿到真实位置。✓ C3。
- **KV 内容**：C1 保证后缀 key/value 张量逐位不变，复用即等价于重算产出。✓
- **softmax 归一化**：标准因果 mask 下，token $m$ 的 attention 分母 $Z_m = \sum_{j \le m} \exp(q_m \cdot k_j)$。$S'$ 相对 $S$ 在 $m$ 之前**仅新增** $c_{1..q}$ 的 key（C2 不删可见性）。全量重算时分母含 $c$；复用路径同样把 $c$ 的新算 key 纳入分母（$c$ 是本轮新 Prefill 产物，非复用）。故两条路径的分母集合相同。分子同理。softmax 输出逐位一致。✓

**结论**：满足 C1–C3 的"纯追加 + 后缀不变 + mask 不变"场景，Virtual-RoPE 复用与全量重算**bit-exact 等价**，零精度损失。

#### 6.8.3 近似回退与熔断闸门

定理的条件一旦破坏即不可侥幸，按 §6.5.2 的四层降级处理：

- **C2 破坏**（mask 变化，如分支裁剪改变可见性）：不可证等价 → 进入近似层，运行 KV Validity Check 估计 attention drift，超 ε 回退全量重算。
- **C1 破坏**（后缀 KV 被改写，如该段已被 CoW 修改）：直接回退全量重算。
- **性能熔断（Divergence Threshold Gate）**：即使可证等价，非连续块过多会导致 GPU Warp 分支分歧（Branch Divergence）拉低吞吐。设硬阈值——单次发散断裂片段 >4，或发散 token 占比 <30%，自动退化为标准 Chunked-Prefill 尾部重算。**这是性能闸门，不是 correctness 闸门**：熔断只损失复用率，不影响输出正确性。

> 一句话：**可证场景 bit-exact 复用，不可证场景近似 + verify，verify 不过则全量重算；任何路径都不会产生错误输出。**

### 6.9 计算面创新二：极速内容哈希（三段式）

避免大图全量哈希拖慢 TTFT（原 §4.5 单次精确哈希在大图上可达数毫秒，落在 TTFT 关键路径）。采用三级策略：

- **L1**：URL / Filepath 字符串哈希（CityHash64）——命中直接跳过 Encoder，O(1)。
- **L2**：若无路径，提取图像分辨率 + 通道数 + 首尾各 1KB 像素做 MurmurHash3——区分绝大多数不同图像，亚毫秒。
- **L3**：前两者都失效（流式上传原始图片且无 URL）时，才异步计算全量特征哈希，不阻塞 TTFT 关键路径。

### 6.10 计算面创新三：多模态异步层级流水线（Async Layer-Pipelining）

**设计目标**：掩盖 DeepStack E→P 的传输延迟。原 §3.2 的串行层级注入会在第 12 层挂起等网络，产生流水线气泡。

- **多路 TCP/RDMA 传输**：视觉特征束不再整体打包。ViT 第 0、12、24 层特征分为 3 个独立 Stream（A6000 阶段走 Multi-stream TCP，A6000 Pro 走 RDMA Send/Recv），不同层特征在不同 socket 上并发传输，防止第 0 层巨量数据阻塞高层关键特征及时到达。
- **静态时序错位预取（Offset Prefetch Queue）**：Encoder 以极高优先级连续把所有层特征压入网络，传输层根据 LLM 各层计算延迟建立带前向时间差的预取队列。
- **乱序容忍调度**：Prefill Worker 维持 Sequence 队列。若 Sequence A 计算到第 12 层但对应特征未到，立刻把 A 挂起（Context Switch），调度 Sequence B 的底层计算；待 A 的特征块到达再恢复 A。彻底消除网络微小抖动造成的整个大模型前向阻塞气泡。

### 6.11 计算面创新四：双引擎算力隔离池（Dual-Engine Isolation）

**设计目标**：消除 Append-Prefill on Decode 导致的 CUDA Graph 崩溃（原 §6.1 软切换会让形状多变的 Prefill 破坏 Decode 的 CUDA Graph，TPOT 飙升）。摒弃直接的角色混合，采用算力槽隔离：

- **Slot A（Decode 专用）**：独占一个高优 CUDA Stream，固定 Batch Size 与形状，始终被 CUDA Graph 捕获，保证极低 TPOT。
- **Slot B（增量 Prefill 专用）**：在同一 GPU 上独占另一 CUDA Stream，动态接收长度 ≤128 的增量 Prefill，不使用 Graph 或使用有限尺寸的 Chunked-Prefill Graph。
- **MPS 资源编排**：通过 NVIDIA MPS 把单卡物理算力严格划分为两个不重叠隔离池（如 Decode 60% / Prefill 40%）。Prefill 算子被切碎为细粒度 Chunk（每 32 token 强制 Yield 一次），Slot A 的 Decode 请求始终挂 `cudaStreamNonBlocking` 最高优先级，利用 Decode 访存瓶颈期间闲置的 ALU 算力吞吐 Prefill，确保 **TPOT 波动率 <5%**。
- **粒度限制**：仅当增量 token <64 时触发本地 Prefill，否则依然 deflect 回 Prefill 节点。

### 6.12 数据面细节：Offload/Restore 异步流水线时序与 In-flight State Lock

§6.3 的工具调用阻塞卸载、§6.7 边界 D 的状态颠簸，统一在本节给出确定性时序。目标是让"高优抢占立即拿 Slot"与"被抢占状态极速返回"两条路径都不被卸载 IO 阻塞。

**正常异步流水线（无中断）**：

```
Agent step N          Offload Worker (BG stream)         MooncakeStore
   |---- offload(refs)---->|                                  |
   |                        |--- owner_shard.cas STATUS=OFFLOADING-->| (持锁, refcount 仍 >0)
   |   step N 立即让出 Slot  |--- RDMA write page@host -------->|
   |   (不等 DMA 完成)        |<-- ack (page persisted) ---------|
   |                        |--- owner_shard.cas STATUS=OFFLOADED ---->| (解锁, refcount-- , 挂 TTL)
   |<-- offload_done -------|                                  |
   ... 工具执行 / 等待 ...
   |---- restore(refs)---->|                                  |
   |                        |--- owner_shard.cas STATUS=RESTORING --->|
   |                        |<-- RDMA read page@host ----------|
   |                        |--- owner_shard.cas STATUS=ACTIVE ------->|
   |<-- restore_done -------|                                  |
   |   step N+1 resume on Slot                                 |
```

**In-flight State Lock（极速返回路径）**：当 Restore 在 Offload 的 DMA 仍未完成（状态仍为 `OFFLOADING`）时到达，走以下短路，不触发任何跨网络换入换出：

```
Agent step N+1 (50ms 内极速返回)
   |---- restore(refs)----> owner_shard
                             | (锁内查 STATUS==OFFLOADING, DMA 仍 in-flight)
                             |--- ABORT_DMA to Offload Worker (丢弃未完成 RDMA write)
                             |--- In-place Reclamation:
                             |       物理页仍在 GPU 显存(尚未被覆盖擦除)
                             |       就地置 STATUS=ACTIVE, refcount 回滚
                             |<-- restore_done (响应延迟 ≤5ms) --|
   |<-- resume immediately |  (跳过 host 换入)
```

**状态机**：`ACTIVE -> OFFLOADING(locked) -> OFFLOADED -> RESTORING(locked) -> ACTIVE`。`OFFLOADING`/`RESTORING` 为 In-flight 锁态：期间任何对该 State 的写操作（fork CoW、A2A handoff）被阻塞或重定向到回滚路径。锁超时（默认 30s）触发 Reaper 兜底回收，防死锁。

**原地回收的可行性前提**：卸载采用 RDMA write 直读 GPU MR（GPUDirect），不预先擦除源页；源页仅在 `OFFLOADED` 确认后才允许被新请求覆写。因此 `OFFLOADING` 态下源页内容完整可恢复。

### 6.13 失败模型与并发语义（Failure Model）

> **设计修正**：v1 的状态机只定义了正常路径，缺少 partial failure semantics、并发 race 与回滚正确性。本节补齐。

#### 6.13.1 In-flight State Lock 的并发语义

`OFFLOADING`/`RESTORING`/`HANDING_OVER` 为锁态，定义以下并发规则，杜绝 split-brain：

- **可重入性**：锁以 `(state_id, version_id)` 为粒度。同一 version 的 owner 可重入；不同 version（已 CoW 出新页的分支）持各自锁，互不阻塞。
- **concurrent fork during OFFLOADING**：父状态处于 `OFFLOADING` 时收到 fork 请求。fork 在快照上执行（§4.1）——子状态绑定父状态 `OFFLOADING` 前的 `version_id`，对该版本引用计数 +1。父状态的卸载 DMA 针对的是物理页，而子状态额外持引用使 `refcount > 0`，故 owner shard 在 DMA 完成后**不得释放物理页**，只标记 `OFFLOADED` 并保留物理页直到所有引用释放。fork 不阻塞卸载，卸载不破坏 fork。
- **restore-after-fork race**：fork 增加了 `refcount`，使 In-place Reclamation（§6.12）更安全——即便 Restore 短路跳过换入，物理页因 fork 仍被持有，不会被覆写。若 Restore 已走完整换入路径而 fork 同时发生，二者都基于不可变快照版本，结果一致。
- **锁超时**：`OFFLOADING`/`RESTORING` 锁超时（默认 30s）触发 Reaper 兜底：强制置 `ABORTED`，回滚 refcount，回收可能残留的半完成 DMA 缓冲，防死锁。

#### 6.13.2 失败语义与幂等性

| 失败类型 | 语义 | 恢复 |
|----------|------|------|
| **fork 失败** | fork 是 refcount+1 + 浅拷贝；若中途失败（如 owner shard 不可达），已 incref 的页必须回滚 decref | 幂等：重试 fork 前先检查是否已有同 `parent_version_id` 的悬挂子状态，有则复用 |
| **partial KV transfer**（A2A handoff 传到一半断网） | 2PC prepare 已置 `HANDING_OVER`，目标节点收到部分页 | 超时回滚（§6.6）：源 shard 保留所有权，目标节点丢弃半接收页；retry 时目标节点只 pull 自己 RadixTree 缺失的页（KVDirect 风格），天然幂等 |
| **restore 幂等** | 重复 Restore 请求 | 以 `(state_id, version_id)` 去重；已在 `RESTORING` 则合并等待，不重复发起 DMA |
| **节点宕机** | owner shard 由 Consistency Manager 的 per-shard Raft 选主恢复 | 新主从 Raft log 重建权威元数据；数据面 worker 上报重建 KV Directory 缓存；宕机节点持有的物理页 refcount 由新主标记 stale，Reaper 周期清理 |
| **GDKR 分片分裂**（网络分区） | 分区致 shard 间不可达 | 跨分片 2PC handoff 超时回滚；分片内操作（fork/CoW）继续可用（shard-local）；分区恢复后 KV Directory 最终一致同步 |

#### 6.13.3 孤儿块 GC 协议

孤儿块 = 物理页存在但无任何 MultimodalState 引用、且非 RadixTree/FeatureBundle 缓存命中的页。来源：RPC 丢失、节点宕机、回滚残留。回收协议：

1. **权威判定**：owner shard 的 Consistency Manager 是 refcount 唯一权威。`refcount == 0` 且 TTL 过期的 GID 进入待回收队列。
2. **数据面确认**：owner shard 向物理驻留节点下发 `RELEASE(GID)`；节点确认页未被任何活跃 sequence 引用后释放。若节点报告"仍被引用"，回退 refcount（保守策略，宁可漏回收不可误回收）。
3. **Reaper 周期扫描**：后台 Reaper 周期扫描数据面物理页池，对"物理存在但 Directory 无记录"的页上报 owner shard 复核；确认为孤儿后释放。这是 Directory 与数据面不一致的最终兜底。

### 6.14 性能模型、反压与尾延迟（Performance Model）

> **设计修正**：v1 只优化 TTFT 均值，缺少 queueing model、TTFT 分解、尾延迟目标与 backpressure/queue-collapse 模型。本节补齐。目标：把"性能"从经验直觉变为可预测、可降级的工程模型，并显式定义过载下的系统行为，避免 queue collapse。

#### 6.14.1 排队模型（Queueing Model）

把每个 stage worker 池建模为带优先级的 M/M/k 排队系统（到达近似 Poisson，服务时间近似指数）：

- **每 stage 一个独立队列**：Encoder / Prefill / Decode 各自的 worker 池是独立的 M/M/k。Stage 间通过 transfer 耦合，整体为 tandem queue。优先级队列（§6.2 THINKING/INTERACTIVE/HYBRID）使每个 M/M/k 退化为 M/M/k with priority classes——高优请求的非抢占优先级平均延迟 `W_high = 1/(kμ − λ_high)`，低优 `W_low` 在高优之后排队。
- **稳定性判据**：每 stage 稳定条件 `ρ = λ/(kμ) < 1`。Scheduler 实时监测各 stage 的 `ρ`；任一 stage `ρ ≥ ρ_warn`（默认 0.85）触发弹性扩容或角色切换（§6.1）；`ρ ≥ ρ_critical`（默认 0.95）触发反压（§6.14.3）。
- **耦合传播**：tandem queue 中下游 stage 阻塞会反压上游。Prefill 产出 KV 后若 Decode 队列满，KV 暂存于 MooncakeStore（带 TTL），Prefill worker 不阻塞——这是 EPD 解耦相对 colocated 的核心增益，把"队列满→整条流水线阻塞"降级为"单 stage 暂存等待"。

#### 6.14.2 TTFT 分解（TTFT Decomposition）

把端到端 TTFT 分解为可独立优化、可独立降级的分量，任一分量超预算即定位瓶颈：

```
TTFT = T_encoder + T_transfer(E→P) + T_prefill_reuse + T_transfer(P→D) + T_queue
```

- **T_encoder**：Vision Encoder 前向。命中率高的工作流可缩到单 Encoder（§6.1），FeatureBundle 命中时 `T_encoder ≈ 0`（内容哈希直出）。
- **T_transfer(E→P)**：FeatureBundle 传输。同机 SHM us 级；跨机被 async feature prefetch（§6.1）掩盖至接近 0。
- **T_prefill_reuse**：增量 Prefill 计算。命中前缀时只算 `delta_tokens`（§6.5 advance_step），命中率高时该分量大幅下降；这是本方案相对 B0/B1 的核心 TTFT 收益来源。
- **T_transfer(P→D)**：P→D 大块 KV 传输。带宽关键链路（§5/§11），RDMA 下被算力隐藏；角色切换（§6.1）可令该项为 0（deflect 到 decode 本地）。
- **T_queue**：各 stage 排队等待之和。由 §6.14.1 排队模型决定，是尾延迟的主导项（§6.14.4）。

每个分量带预算（budget），Scheduler 在路由时优先把请求送入"剩余预算仍充足"的 stage 实例，避免单一慢节点拖垮 P99。

#### 6.14.3 反压、过载与队列崩塌防护（Backpressure / Overload / Queue-Collapse）

queue collapse 的典型形态：到达率持续 > 服务率 → 队列无限增长 → 内存撑爆 → 全量驱逐/重启 → 雪崩。本方案三层防护：

1. **有界队列 + 显式拒绝**：每个 stage 队列有界（默认容量 = `k × 4`）。队列满时新请求走 **admission control**：低优请求直接 503-reject（client 退避重试），高优请求触发抢占（§6.2）或弹性扩容。绝不让队列无界增长。
2. **反压传播**：下游 stage `ρ ≥ ρ_critical` 时，Scheduler 对上游 stage 的该类请求降速（token-bucket 限流注入），而非堆积。EPD 解耦使反压可停在单 stage：Decode 过载时只对"目标为 Decode"的请求限流，Encoder/Prefill 仍服务其他工作流，避免全流水线停摆。
3. **过载降级梯度**：`ρ_critical` 触发时按梯度降级——(a) 关闭跨步骤近似复用（§6.5 只走 exact prefix，省算力）；(b) 提升压缩等级（§5 SpectrumKV/CacheGen 降带宽）；(c) KV 更激进 Offload 腾 GPU 显存；(d) 最后才全局 reject。每级降级都用 §6.14.1 的 `ρ` 反馈决定进退，形成闭环，防止"降级后欠收 → 过早恢复 → 再过载"振荡。
4. **内存闸门**：MooncakeStore 与 GPU 显存各设 high-watermark。超过 high-watermark 时 Scheduler 拒绝新 fork / 新 KV-cache 注册（已活跃请求不受影响），从根上切断"queue 增长 → 内存撑爆"链路。

#### 6.14.4 尾延迟目标（Tail Latency）

均值优化掩盖尾延迟；本方案显式给 P99 目标并隔离长尾来源：

- **目标**：单步 TTFT P99 ≤ 3 × P50，Turn-2+ TTFT P99 ≤ P50（角色切换 + 前缀命中消除跨节点传输长尾）。尾延迟预算分配到 §6.14.2 各分量，T_queue 占 P99 增量的大头。
- **长尾隔离**：长尾主因是 (a) 跨节点 KV 传输抖动、(b) 单慢节点、(c) 抢占恢复。对策：跨节点传输走 RDMA/SHM 与角色切换消除 (a)；Scheduler 基于各 worker EWMA 延迟做 load-aware 路由，规避 (b) 慢节点；抢占式 Offload/Restore 走 In-flight State Lock 异步流水（§6.12）掩盖 (c)。
- **尾延迟预算守门**：每个请求携带 `deadline`（§6.2），Scheduler 估算各 stage `W_p99`（由排队模型在线拟合），若预测超 deadline 则触发降级梯度（§6.14.3 第 3 条）或 reject——宁可早拒不可拖垮全体。这把"尾延迟"从被动观测变为主动调度约束。


---

## 7. 关键技术方案四：Omni 多阶段流水线扩展（进阶任务 1）

借鉴 vLLM-Omni（阶段抽象 + 分布式执行 + 阶段间 connector，−91.4% JCT）、ModServe（模态 + 阶段感知资源分离）、Nova（自适应跨阶段并行化，−23.3% 延迟）、TridentServe（diffusion 流水线阶段级服务，−4.1× 延迟），把 EPD 的"三阶段 + 阶段间传输"抽象推广到 Omni 模型（AR 声码器 → Generation LLM → Diffusion 图像生成）的多阶段流水线：

- **统一阶段抽象**：每个阶段是一个 Worker，暴露 `run(input_refs) -> output_refs`。阶段间通过统一的 `transfer()` 原语传输中间状态（AR→Generation 的隐状态、Generation→Diffusion 的条件嵌入），与 E→P / P→D 复用同一套 TransferPolicy。
- **worker 级跨阶段传输优化**：阶段间数据小而延迟敏感，优先 SHM（同机）与 RDMA send（跨机），配合 stream 重叠。Nova 的自适应跨阶段并行化用于在 AR/Generation/Diffusion 速度不匹配时动态调整并行度，减少气泡。
- **复用 EPD 基础设施**：Omni 的阶段间隐状态复用、条件嵌入缓存，复用 §4 的引用计数 + RadixTree + FeatureBundle 缓存机制。Diffusion 阶段的 dKV-Cache（diffusion 步骤间 KV 复用）类比 Decode 的页级 CoW。

本方案把 Omni 作为 EPD 抽象的自然推广，而非独立子系统——同一套状态对象、传输策略、调度框架同时服务 EPD 与 Omni。

---

## 8. 评估方案：指标体系、数据集构建与 Baseline 对比

### 8.1 评估目标

本方案的评估不只验证“端到端吞吐是否提升”，而是要拆解验证以下六类能力：

1. EPD 三阶段分离收益：验证 Encoder、Prefill、Decode 分离后，是否相对单机同置和传统 PD 分离降低 TTFT、提升吞吐。
2. 视觉特征缓存收益：验证同图、多图、多轮复用场景下，FeatureBundle 是否能有效跳过 Vision Encoder。
3. Agent State Cloning 收益：验证 fork 多个思考分支时，页级引用计数 + CoW 是否相对深拷贝显著降低延迟和显存占用。
4. 跨步骤 KV 复用收益：验证同一 Agent workflow 的后续 step 是否能通过前缀命中减少重复 Prefill。
5. 调度与卸载收益：验证 THINKING / INTERACTIVE / HYBRID 类型 Agent 的动态路由、抢占、工具调用阻塞卸载是否改善 JCT 与尾延迟。
6. 正确性与稳定性：验证缓存复用、压缩传输、offload/restore、A2A handoff 不引入输出质量回退、KV 泄漏、Use-After-Free、孤儿 block、死锁等问题。

因此，本评估采用四层矩阵：

层级	目的	主要问题
L0 单请求多模态推理	验证 EPD 基本收益	Encoder / Prefill / Decode 分离是否有效
L1 多轮同图 / 多图会话	验证 FeatureBundle 与前缀缓存	能否减少重复 Encoder / Prefill
L2 多步骤 Agent 工作流	验证跨步骤 KV 复用、工具调用卸载、JCT	workflow step 2..N 是否显著加速
L3 多分支 / A2A / 故障压测	验证 fork、handoff、CoW、一致性	分布式状态系统是否稳定可靠

⸻

### 8.2 数据集选择与使用原则

公开数据集分为两类使用：真实任务数据集用于衡量最终任务质量与真实负载；可控派生数据集用于精确评估缓存命中、fork、A2A、offload 等系统能力。

#### 8.2.1 主数据集

数据集	用途	使用方式
m&m’s	多步多模态工具调用 Agent	用作核心 Agent workflow 数据集，构造多 step、多工具、多图像输入；该 benchmark 包含 4K+ multi-step multimodal tasks 和 33 个工具，官方还提供 1,565 条人工验证的可执行计划，可作为高置信子集。
GAIA	通用助手型 Agent 任务	用作多步推理 + 工具调用 + 多模态上下文测试集，评估端到端任务成功率、JCT、工具调用正确性。
MMMU	多学科多模态推理	用作强推理型多模态任务，主要测试 Prefill-heavy 场景和长上下文场景。
MMBench	标准 VLM 能力评估	用作单轮 / 多轮视觉问答基础能力回归测试，防止缓存和压缩策略导致模型质量下降。
DocVQA	文档图像理解	用作文档图像重复引用、多轮问答、长图像 token 场景，重点验证 Encoder 跳过率和 FeatureBundle 复用收益。

m&m’s、GAIA、MMMU、MMBench、DocVQA 的角色要区分清楚：m&m’s 和 GAIA 主要服务 Agent workflow 评估，MMMU / MMBench / DocVQA 主要服务 多模态推理质量与 Encoder 复用评估。m&m’s 官方说明其包含 4K+ 多步多模态任务和 33 个工具，适合构造多 step 工具调用型 workflow。(arXiv) GAIA 是面向通用 AI assistant 的 benchmark，适合评估带工具能力、搜索能力、多模态处理能力的下一代 LLM agent。(Hugging Face) MMMU 包含 11.5K 道多学科多模态问题，覆盖 art & design、business、science、health & medicine、humanities & social science、tech & engineering 等领域，适合测试复杂多模态推理场景。(arXiv) MMBench 是面向 VLM 综合能力评估的客观多选 benchmark，包含中英文设置，适合做质量回归。(OpenReview) DocVQA 面向 document image visual question answering，适合测试文档图像场景下的视觉特征复用和多轮问答。(DocVQA)

#### 8.2.2 候选数据集准入规则

RFC 中提到的 M³-Bench、EpiBench、ToolVQA 等新数据集可以作为候选，但不能直接作为主评估集。纳入主评估前必须满足：

1. 数据集公开可下载，或者提供可复现构造脚本。
2. 任务有明确输入、输出、评分函数。
3. 至少包含以下一种结构：
    * 多 step workflow；
    * 多图 / 多轮输入；
    * 工具调用轨迹；
    * 可构造 fork / verifier / planner 子任务。
4. 能统一转换为本 RFC 的 WorkflowTrace 格式。
5. 能在 baseline 和本方案上使用同一 prompt、同一工具集、同一模型权重复现。

未满足条件的数据集只放入“扩展实验”，不作为主结论依据。

⸻

### 8.3 数据集构建：从公开 benchmark 到 Agent WorkflowTrace

公开 benchmark 通常不是专门为 KVCache 系统设计的，因此需要构造统一的 workload trace。每条任务转换为如下格式：

{
  "workflow_id": "uuid",
  "source_dataset": "mnms|gaia|mmmu|mmbench|docvqa|synthetic",
  "task_type": "single_turn|multi_turn|tool_use|fork|a2a|offload",
  "priority_class": "THINKING|INTERACTIVE|HYBRID",
  "images": [
    {
      "image_id": "content_hash",
      "path_or_url": "...",
      "reuse_group": "same_image_group_id"
    }
  ],
  "steps": [
    {
      "step_id": 0,
      "agent_role": "planner|solver|tool_executor|verifier",
      "input_prompt": "...",
      "input_token_len": 0,
      "shared_prefix_token_len": 0,
      "new_token_len": 0,
      "expected_tool_call": null,
      "expected_answer": "...",
      "parent_state_id": null,
      "fork_group_id": null
    }
  ],
  "gold_answer": "...",
  "scoring": {
    "type": "exact_match|multiple_choice|f1|anls|llm_judge|tool_success"
  }
}

#### 8.3.1 构造五类 workload

W0：单轮多模态推理 workload

来源：MMBench、MMMU、DocVQA。

目的：验证 EPD 相对单机同置的基础收益。

构造方式：

* 每个样本只包含一个 step。
* 不启用 Agent fork。
* 不启用跨步骤复用。
* 图像每次首次出现时必须经过 Encoder。
* 记录 Encoder、E→P、Prefill、P→D、Decode 各阶段耗时。

主要验证：

* EPD 是否降低 TTFT；
* P→D 传输是否成为瓶颈；
* 图像大小、图像 token 数、文本 token 数对 TTFT 的影响。

W1：多轮同图 / 多图会话 workload

来源：DocVQA、MMBench、MMMU 派生。

目的：验证 FeatureBundle 缓存与多轮 KV 前缀缓存。

构造方式：

* 对同一张图构造 3–5 轮连续问题。
* 第 1 轮必须完整执行 Encoder + Prefill。
* 第 2..N 轮复用同一图像 FeatureBundle。
* prompt 采用固定结构：

System Prompt
Shared Image Context
Conversation History
Current User Question

主要验证：

* Encoder 跳过率；
* FeatureBundle 命中延迟；
* Turn-2+ TTFT 降低比例；
* 多轮 KV TTL 是否稳定；
* 多轮质量是否相对 B0 回退。

W2：多步骤工具调用 Agent workload

来源：m&m’s、GAIA。

目的：验证跨步骤 KV 复用、工具调用阻塞卸载、workflow JCT。

构造方式：

* 将原始任务转换为 planner → tool_call → observation → solver → verifier 的多 step workflow。
* 如果数据集已有 tool trace，则保留原始 trace。
* 如果只有最终答案，则用固定 planner 模板派生 3-step 或 5-step 工作流。
* 工具调用 step 引入可控阻塞时间：50ms、200ms、1s、5s 四档。
* 工具返回内容分为三档：
    * short observation：≤128 tokens；
    * medium observation：512–1K tokens；
    * long observation：2K–8K tokens。

主要验证：

* 跨步骤复用率；
* step 2..N TTFT 降低；
* workflow JCT；
* offload/restore 收益；
* 工具快速返回时的 in-flight lock 是否避免 IO 颠簸。

W3：多分支 fork workload

来源：m&m’s、GAIA、MMMU 派生。

目的：验证 Agent State Cloning 和页级 CoW。

构造方式：

* 对同一个 parent state fork 出 2、4、8、16、32、64、128 个分支。
* 每个分支生成不同 reasoning path。
* 分支共享相同系统 prompt、历史上下文、图像上下文。
* 分支 decode 到第一个新 token 时触发 CoW 边界。
* 可设置 branch merge：选择 top-k 分支进入 verifier。

主要验证：

* fork 延迟是否接近 O(1)；
* fork 显存增长是否接近 O(branch_delta)，而非 O(branch × full_context)；
* CoW page copy 次数；
* 共享页被并发写入时是否出现 refcount mismatch；
* 128 分支极端情况下是否 OOM。

W4：A2A handoff workload

来源：GAIA、m&m’s 派生。

目的：验证多 Agent 状态交接。

构造方式：

* 将 workflow 切为 planner agent、vision agent、tool agent、verifier agent。
* 每次 agent role 变化时执行 handoff。
* handoff 分为同机、跨机 TCP、跨机 RDMA 三档。
* 目标 agent 只按需 pull 缺失 KV pages。

主要验证：

* A2A handoff latency；
* handoff 后首 token TTFT；
* pull 增量字节数；
* handoff 失败回滚；
* 网络断开时是否出现 orphan blocks。

W5：混合在线负载 workload

来源：W0–W4 按比例混合。

目的：验证真实 serving 场景下的调度、公平性、尾延迟和 goodput。

推荐默认混合比例：

Workload	占比
W0 单轮 VQA	25%
W1 多轮同图会话	20%
W2 多步骤工具 Agent	25%
W3 fork 多分支推理	15%
W4 A2A handoff	10%
系统故障 / offload 压测流量	5%

到达过程采用 Poisson arrival + burst arrival 两种模式：

* Poisson arrival：模拟稳定线上服务。
* Burst arrival：模拟热点事件或批量 Agent 调用。
* 负载强度设置为 30%、50%、70%、90%、110% 集群容量。

⸻

### 8.4 数据划分与复现实验协议

#### 8.4.1 数据划分

每个数据源划分为：

Split	用途
dev-small	功能调试，100–300 条
dev	策略调参，1K–3K 条
test	最终报告，固定随机种子，禁止调参
stress	fork、handoff、offload、故障注入专项压测

如果原数据集已有官方 dev/test 划分，则保留官方划分。派生 workload 只允许在 dev 上调参，最终 test 必须固定 prompt、固定工具、固定随机种子、固定采样参数。

#### 8.4.2 公平性协议

所有 baseline 必须满足：

1. 使用同一模型权重、同一 tokenizer、同一图像预处理。
2. 使用同一 prompt 模板。
3. 使用同一最大输入长度、最大输出长度。
4. greedy / temperature=0 用于质量对比；固定 temperature 和 seed 用于压力测试。
5. 相同 GPU 数量、相同 batch admission policy、相同并发上限。
6. 相同精度配置，除非实验明确比较压缩 / fp8 / bf16。
7. 所有系统预热 5–10 分钟后开始计数。
8. 报告 cold-cache 和 warm-cache 两种结果。
9. 主结论以 P50 / P95 / P99 同时报出，不能只报平均值。
10. 如果某方案无法完成某类 workload，必须报告 failure rate，而不能静默丢弃。

⸻

### 8.5 Baseline 设计

Baseline 必须分层，不能只和单机 vLLM 对比。否则无法证明每个组件的独立贡献。

#### 8.5.1 主 Baseline

编号	Baseline	配置	对比目的
B0	单机同置 vLLM / SGLang	Encoder、Prefill、Decode 同机同进程；无 EPD；原生 prefix cache 可按默认开启或关闭并分别报告	赛题要求的单机端到端基线
B1	原生 PD 分离	Prefill / Decode 分离；Encoder 与 Prefill 同置；P→D 走 Mooncake / vLLM connector	验证 EPD 相对传统 PD 的增益
B2	PD + Prefix Cache	B1 + 文本 prefix cache；无 Encoder 分离；无 FeatureBundle cache	隔离文本前缀缓存收益
B3	PD + Feature Cache	B1 + 图像 FeatureBundle cache；Encoder 仍逻辑同置	隔离视觉特征缓存收益
B4	Naive EPD	Encoder / Prefill / Decode 三阶段分离；E→P 使用普通 tensor copy / TCP；无页级 CoW；无 Agent state	验证真实 TransferPolicy 与页级状态管理的收益
B5	EPD + 深拷贝 fork	支持 Agent fork，但每个分支深拷贝完整 KV	验证页级引用计数 + CoW 的收益
B6	EPD + 页级 CoW，无跨步骤复用	支持 fork O(1)，但 step 间不复用 KV	隔离跨步骤复用收益
B7	EPD + 精确前缀复用	只允许 RadixTree exact prefix reuse；发散后一律重算	验证安全复用上界
B8	EPD + 近似 / 非连续复用	在 B7 上加入 Virtual-RoPE 或选择性重算策略	验证激进复用策略收益与质量风险
B9	Full System	EPD + FeatureBundle cache + RadixTree + page CoW + cross-step reuse + Agent scheduler + offload + A2A	最终方案

#### 8.5.2 外部系统 Baseline

如果工程时间允许，增加以下外部 baseline：

Baseline	用途
vLLM 原生 prefix caching	对比 RadixTree / block-level reuse 效果
SGLang RadixAttention	对比成熟 prefix cache 系统
Mooncake PD disaggregation	对比 KVCache-centric PD 架构
CachedAttention / 类似 offload 系统	对比分层 KV offload
Tokencake / agent-aware KV serving 系统	对比 Agent 场景中的缓存调度与 offload

Mooncake 是 KVCache-centric disaggregated serving 系统，核心能力包括分离 prefill / decode cluster，并利用 CPU、DRAM、SSD、NIC 等资源构建 disaggregated KVCache pool，因此它应作为 PD 分离与 KVCache 池化方向的重要外部参照。(arXiv)

#### 8.5.3 消融实验

Full System 需要做以下消融：

消融项	设置	预期观察
w/o EPD	退回 PD 或单机同置	TTFT / 吞吐下降
w/o FeatureBundle cache	每轮重新跑 Encoder	多轮同图 TTFT 上升
w/o RadixTree prefix cache	每 step 完整 Prefill	跨步骤复用率归零，JCT 上升
w/o page-level CoW	fork 深拷贝 KV	fork 延迟和显存随分支数线性增长
w/o offload	工具阻塞期间 KV 留在 GPU	HBM 峰值升高，高负载下 admission 下降
w/o scheduler	使用 FIFO / random routing	高优任务 P95/P99 恶化
w/o compression	P→D / A2A 不压缩	网络瓶颈下 TTFT 上升
w/o RDMA	TCP only	量化 RDMA 对 P→D 和 A2A 的贡献
exact-prefix only	关闭近似 / 非连续 reuse	验证激进复用带来的额外收益与质量风险
no prefetch	关闭 E→P / next-step prefetch	流水线气泡增大

⸻

### 8.6 指标体系

指标分为七类：延迟、吞吐、缓存复用、资源、调度、质量、一致性可靠性。

#### 8.6.1 延迟指标

指标	定义	说明
TTFT	request 到首 token 输出的时间	报告 P50/P95/P99
TPOT	decode 阶段每输出 token 的平均时间	报告平均与 P95
ITL	inter-token latency	更细粒度观察 decode 抖动
E2E Latency	request 到完整答案输出	单请求端到端
JCT	workflow 第一个 step 开始到最后一个 step 完成	Agent 工作流核心指标
Step TTFT	每个 step 的 TTFT	用于验证 step 2..N 加速
Handoff Latency	A2A 状态交接耗时	包含元数据提交与增量 KV pull
Offload Latency	KV 从 GPU 卸载到 Store 的耗时	不应阻塞高优任务 admission
Restore Latency	KV 从 Store 恢复到 GPU 的耗时	区分 normal restore 与 in-flight restore
Control-plane Latency	fork / incref / decref / CoW promote / handoff commit 的元数据耗时	观察 GDKR 是否成为瓶颈

TTFT 需要拆解为：

TTFT = T_queue
     + T_encoder_miss
     + T_E2P_transfer
     + T_prefill
     + T_P2D_transfer
     + T_first_decode
     + T_control_plane

JCT 需要拆解为：

JCT = Σ StepLatency
    + Σ ToolWait
    + Σ Handoff
    + Σ OffloadRestore
    + SchedulerQueueing

报告中必须同时给出端到端值和阶段拆解值，否则无法判断收益来自 EPD、缓存、调度还是负载波动。

#### 8.6.2 吞吐与 Goodput 指标

指标	定义
Request Throughput	每秒完成请求数
Workflow Throughput	每秒完成 Agent workflow 数
Output Token Throughput	每秒输出 token 数
Prefill Token Throughput	每秒处理 prompt token 数
Effective Goodput	满足 SLO 的有效完成请求数 / 秒
SLO Attainment	TTFT、JCT、TPOT 同时满足 SLO 的请求比例
Deadline Miss Ratio	超过 deadline 的 workflow 比例
Admission Success Rate	高负载下被系统接收并完成的请求比例
Early Rejection Rate	系统主动拒绝请求的比例，不能混入 failure rate

Goodput 比 raw throughput 更重要。一个系统如果通过排队堆积换来更高 tokens/s，但 P99 TTFT 或 JCT 崩溃，不能算有效提升。

#### 8.6.3 缓存与复用指标

1. FeatureBundle 命中率

FeatureBundle Hit Rate
= 命中 FeatureBundle cache 的图像请求数 / 总图像请求数

2. Encoder 跳过率

Encoder Skip Rate
= 被跳过的 Encoder 调用次数 / 理论应执行的 Encoder 调用次数

3. Token 前缀命中率

Prefix Token Hit Rate
= 命中的 prefix token 数 / 总 input token 数

4. KV Page 命中率

KV Page Hit Rate
= 复用的 KV pages / 总需要 KV pages

5. 跨步骤复用率

这是本方案核心指标，必须分别按 token、page、FLOPs 三种口径报告。

Step Reuse Rate_token
= Σ_{workflow} Σ_{step=2..N} reused_prefix_tokens(step)
  / Σ_{workflow} Σ_{step=2..N} total_input_tokens(step)
Step Reuse Rate_page
= Σ reused_kv_pages(step)
  / Σ total_required_kv_pages(step)
Step Reuse Rate_flops
= saved_prefill_flops
  / baseline_prefill_flops

其中 saved_prefill_flops 不能简单等于 token 数比例，长序列 attention 的计算量随上下文长度变化，因此需要按实际 prefill kernel 统计或用近似模型估算。

6. Turn-2+ TTFT 降低率

Turn2+ TTFT Reduction
= 1 - TTFT_turn_2plus_with_cache / TTFT_turn_2plus_without_cache

7. Fork 放大系数

Fork Memory Amplification
= peak_HBM_after_fork / peak_HBM_before_fork

理想情况下，fork 后显存增长应接近增量 token 的 KV，而不是完整上下文 × 分支数。

8. CoW 触发率

CoW Rate
= cow_copied_pages / shared_pages_touched_by_decode

9. 复用安全指标

指标	定义
Reuse Fallback Rate	由于发散过大、位置不安全、相似度不足而回退重算的比例
Unsafe Reuse Blocked Rate	被 correctness gate 阻止的复用比例
Reuse-induced Quality Delta	开启复用相对 full recompute 的质量差
Exact Reuse Coverage	exact prefix reuse 覆盖的 token/page 比例
Approximate Reuse Coverage	近似 / 非连续 reuse 覆盖的 token/page 比例

如果使用 Virtual-RoPE 或任何非连续 KV 复用，必须单独报告 approximate reuse 的质量变化，不能和 exact prefix reuse 混在一起。

#### 8.6.4 资源指标

指标	定义
Peak HBM	GPU 显存峰值
Average HBM	平均 GPU 显存占用
KV Cache Residency	KV 留在 GPU / CPU / Store 的比例
HBM Fragmentation	页池碎片率
Eviction Count	KV / FeatureBundle 被淘汰次数
Offload Bytes	卸载字节数
Restore Bytes	恢复字节数
E→P Bandwidth	FeatureBundle 传输带宽
P→D Bandwidth	KV 传输带宽
A2A Pull Bytes	handoff 时目标 agent 拉取的增量字节数
GPU Utilization	SM utilization、memory bandwidth utilization
NIC Utilization	TCP / RDMA 网络利用率
CPU Overhead	控制面和传输线程 CPU 占用

#### 8.6.5 调度指标

指标	定义
Priority-aware TTFT	THINKING / INTERACTIVE / HYBRID 分类 TTFT
Priority-aware JCT	不同任务类型的 workflow JCT
Preemption Latency	高优任务触发抢占到获得资源的时间
Victim Recovery Latency	被抢占任务恢复执行的时间
Starvation Rate	低优任务超过最大等待窗口仍未执行的比例
Queueing Delay	各阶段队列等待时间
Routing Accuracy	任务是否被路由到预期资源类型
Load Balance Score	E/P/D worker 负载均衡程度
Jain Fairness Index	多租户或多 workflow 公平性

8.6.6 质量指标

质量指标必须和具体数据集绑定：

数据集 / 任务	指标
MMBench	accuracy
MMMU	accuracy / subject-wise accuracy
DocVQA	ANLS / exact match
m&m’s	tool plan success、tool execution success、final answer correctness
GAIA	final answer accuracy、tool-use correctness、step success rate
Synthetic fork / verifier	branch success rate、best-of-k answer accuracy

另外必须报告“相对 full recompute 的质量差”：

Quality Delta
= Score(system_with_reuse) - Score(full_recompute_baseline)

通过标准：

|Quality Delta| <= 1% 作为默认安全线
|Quality Delta| <= 2% 可作为压缩 / 近似复用的宽松线

如果质量下降超过 2%，对应复用策略不能作为默认开启，只能作为 optional aggressive mode。

#### 8.6.7 一致性与可靠性指标

指标	定义
Refcount Mismatch Count	GDKR refcount 与实际 page 引用不一致次数
Orphan Block Count	无 owner 但未释放 block 数
Use-after-free Count	已释放 block 被访问次数
Double-free Count	同一 block 被重复释放次数
CoW Race Failure Count	并发写共享页导致错误次数
2PC Rollback Success Rate	A2A handoff 失败时成功回滚比例
Offload Abort Success Rate	in-flight offload 被 restore 中止成功比例
Deadlock Count	状态锁超过超时时间次数
Recovery Time	故障注入后系统恢复到可服务状态的时间
Data Corruption Count	KV / FeatureBundle 校验失败次数

这些指标必须自动化断言。只要出现 Use-after-free、double-free、数据损坏，实验即判定失败，不能只报告平均性能。

⸻

### 8.7 压力测试与边界场景

#### 8.7.1 Fork 压测

参数	取值
branch width	2 / 4 / 8 / 16 / 32 / 64 / 128
context length	1K / 4K / 16K / 64K tokens
image count	0 / 1 / 4 / 8
decode length	32 / 128 / 512 tokens
shared prefix ratio	50% / 80% / 95%

通过标准：

* fork latency 随 branch width 增长接近 O(branch_metadata)，不能出现完整 KV 深拷贝级增长。
* HBM 增长不应接近 branch_width × full_context_kv_size。
* refcount mismatch = 0。
* Use-after-free = 0。
* 输出质量相对 B0 不下降。

#### 8.7.2 跨步骤复用压测

参数	取值
step count	2 / 4 / 8 / 16
shared prefix ratio	30% / 50% / 70% / 90%
insertion location	tail / middle / head
inserted observation length	128 / 512 / 2K / 8K tokens
image reuse ratio	0% / 50% / 100%

通过标准：

* exact prefix reuse 在 tail insertion 场景下稳定生效。
* middle insertion / head insertion 触发 fallback 或安全复用 gate。
* 近似 / 非连续复用必须单独报告质量 delta。
* Step 2..N TTFT 必须相对无复用 baseline 下降。
* 如果 shared prefix ratio <30%，系统应自动退化到标准 chunked prefill。

#### 8.7.3 Offload / Restore 压测

参数	取值
tool wait time	50ms / 200ms / 1s / 5s / 30s
KV size	512MB / 2GB / 8GB
restore timing	before DMA done / after DMA done / after TTL
pressure	normal / HBM full / network congested

通过标准：

* 工具等待较长时，offload 能降低 HBM 峰值。
* 工具 50ms 内极速返回时，in-flight restore 不应触发完整网络 restore。
* offload abort success rate 接近 100%。
* 无死锁、无脏页、无状态丢失。

#### 8.7.4 A2A Handoff 压测

参数	取值
topology	same GPU / same node / cross node TCP / cross node RDMA
missing page ratio	0% / 25% / 50% / 100%
network fault	no fault / delay / packet loss / target node down
handoff frequency	low / medium / high

通过标准：

* 增量 pull 字节数随 missing page ratio 变化。
* 目标节点失败时 2PC rollback 成功。
* 源节点状态不会提前释放。
* orphan block count = 0。

#### 8.7.5 Overload 压测

参数	取值
load	50% / 70% / 90% / 110% / 150%
workload mix	W0-W5 默认混合 / Agent-heavy / VQA-heavy
priority mix	interactive-heavy / thinking-heavy / balanced

通过标准：

* 90% 负载下 P95 TTFT 和 P95 JCT 满足 SLO。
* 110% 以上负载下系统允许 early rejection，但不能无限排队导致 P99 崩溃。
* 高优任务不应被低优任务长期阻塞。
* low-priority starvation rate 必须可控。
* **不发生 queue collapse**：150% 负载下队列有界、内存不撑爆、降级梯度（§6.14.3）触发后 `ρ` 回落且无振荡；fallback 路径（§11.7）在过载降级全程保持可达。

⸻

### 8.8 报告格式

最终报告至少包含以下表格。

#### 8.8.1 主结果表

System	TTFT P50	TTFT P95	TTFT P99	TPOT	JCT P50	JCT P95	req/s	workflows/s	goodput	quality
B0 Single-node										
B1 PD										
B4 Naive EPD										
B6 EPD + CoW										
B7 Exact Reuse										
B9 Full										

#### 8.8.2 阶段拆解表

System	Queue	Encoder	E→P	Prefill	P→D	Decode First Token	Control Plane	Total TTFT
B0								
B1								
B9								

#### 8.8.3 缓存复用表

System	Feature Hit	Encoder Skip	Prefix Token Hit	KV Page Hit	Step Reuse Token	Step Reuse FLOPs	Fallback Rate	Quality Delta
B3								
B6								
B7								
B9								

8.8.4 Fork 与 CoW 表

Branch Width	Fork Latency	Peak HBM	CoW Pages	Refcount Mismatch	UAF	Quality Delta
2						
8						
32						
128						

#### 8.8.5 调度与 Offload 表

System	Interactive TTFT P95	Thinking JCT P95	Preemption Latency	Restore Latency	HBM Saved	Starvation Rate	SLO Attainment
FIFO							
Priority Routing							
Full Scheduler + Offload							

#### 8.8.6 可靠性表

Test	Refcount Mismatch	Orphan Blocks	UAF	Double Free	Deadlock	Rollback Success	Recovery Time	Pass
Fork Race								
A2A Network Cut								
Offload Restore Race								
Overload								

⸻

### 8.9 通过标准

本方案要宣称优于 baseline，必须同时满足：

1. 性能收益
    * 相对 B0 单机同置，端到端 throughput 或 goodput 有显著提升。
    * 相对 B1 PD，W1/W2/W3/W4 中 TTFT 或 JCT 有显著下降。
    * 在 W2 多步骤 Agent workflow 中，Step 2..N TTFT 明显低于无跨步骤复用 baseline。
    * 在 W3 fork workload 中，fork latency 与 HBM 增长显著低于深拷贝 baseline。
2. 质量不回退
    * MMBench / MMMU / DocVQA / GAIA / m&m’s 上任务质量相对 full recompute baseline 回退不超过 1%。
    * 如果启用近似复用或压缩，质量回退不超过 2%，否则不得作为默认策略。
3. 稳定性达标
    * Use-after-free = 0。
    * Double-free = 0。
    * Refcount mismatch = 0。
    * Orphan block 在 GC 窗口后必须为 0。
    * A2A 失败 rollback success rate 接近 100%。
    * Offload/restore race 不产生脏页或死锁。
4. SLO 达标
    * 70% 负载下 P95 TTFT、P95 JCT 满足配置 SLO。
    * 90% 负载下 P99 不出现队列崩溃。
    * 110% 以上负载下允许 early rejection，但不能出现无限排队和系统雪崩。
5. 可复现
    * 所有 workload trace、随机种子、prompt 模板、工具 mock、模型配置、硬件拓扑、batch 配置必须保存。
    * 所有结果必须同时报告 cold-cache 和 warm-cache。
    * 所有主表必须包含 P50 / P95 / P99，而不是只报告平均值。
---

## 9. 与赛题六项任务的对应关系

| 赛题任务 | 本方案对应设计 |
|----------|----------------|
| 基础任务 1：EPD 三阶段分离原型（SGLang/vLLM，Vision Encoder Hidden State → Prefill via Mooncake，Prefill KVCache → Decode） | §3 架构 + §3.3 vLLM 集成 + §5 TransferPolicy：Encoder/Prefill/Decode Worker 职责划分，E→P（FeatureBundle via Mooncake）、P→D（KV 页表 via MooncakeConnector）两段链路，DeepStack 多层注入 |
| 基础任务 2：Agent State Cloning（fork 并行思考分支，零拷贝克隆 + 跨节点共享 via Mooncake Store） | §4 MultimodalState（§4.1 版本化、不可变快照）+ §4.2 页级引用计数 + CoW + §6.4 fork（O(1)，version 继承）+ §6.7 GDKR 三件拆分 / 分片，分布式 CAS 串行提权下沉 shard-local 保证跨节点一致性 + A2A 跨节点引用迁移 + §6.13 失败模型（fork 失败回滚） |
| 基础任务 3：Qwen-VL 端到端 Demo（相对单机吞吐增益） | §3.4 数据通路 + §8 评估：以 Qwen3-VL 在 m&m's/GAIA 等数据集上对比 B0 单机，报告吞吐/TTFT/JCT 增益 |
| 进阶任务 1：Omni Pipeline Worker 级跨阶段传输优化（AR→Generation→Diffusion，RDMA/SHM 低延迟） | §7 Omni 多阶段流水线扩展：统一阶段抽象 + worker 级跨阶段 transfer（SHM/RDMA/stream） |
| 进阶任务 2：Agent PD 分离调度（THINKING→高算力 Prefill，INTERACTIVE→低延迟 Decode，动态路由） | §6.1 EPD 弹性比例 + 角色切换 + §6.2 Agent 图优先级调度（任务分类 + 加权评分 + 动态路由）+ §6.14 性能模型（排队模型 + TTFT 分解 + 反压/过载防护 + 尾延迟目标） |
| 进阶任务 3：Hidden State 前缀缓存（多模态 omni 前缀复用，减少重复 Encoder） | §4.5 RadixTree 内容寻址 + FeatureBundle 内容哈希缓存 + §5.3 per_level 压缩 + §6.5 跨步骤复用（四层降级：精确前缀 / Virtual-RoPE 可证等价 / 近似+verify gate / full-recompute 兜底，§6.8 可证等价定理） |

---

## 10. 技术难点与取舍

### 10.1 EPD vs PD

EPD 引入额外 E→P 传输，纯文本场景无收益。选择 EPD 的理由是多模态场景图像输入量大、重复率高，FeatureBundle 缓存可显著降低 Encoder 计算；命中率高的工作流几乎消除 Encoder 阶段，仅保留传输开销（且 E→P 数据量小、可流式重叠）。当图像几乎不重复时，EPD 退化为 PD + 一次小传输，代价可控。

### 10.2 页级 CoW（分布式 CAS）vs 整张量 CoW

整张量 CoW 实现简单但代价 O(序列长度)（30MB KV 拷 8 分支 = 240MB + 数毫秒）。页级 CoW 把代价降到 O(单页)——fork 只增引用（O(1)），仅被写入的页复制。代价是页表与引用计数管理的复杂度，但在 Tree-of-Thought 4–16 分支场景下净收益显著。**进一步，分布式页级 CoW 由 owner shard 的 shard-local CAS 串行提权保证一致性**（§6.7 边界 A，热路径下沉分片不走 Raft）：多路分支并发写入同一物理页时，仅第一个 CAS 成功者原地写入，其余被标记 `STATUS_COW_REQUIRED` 的分支触发物理页复制，杜绝 Use-After-Free 与页内容互相覆盖。本地独立引用计数在跨节点场景下无法保证这一致性，故以全局 GID + shard-local CAS 取代之。

### 10.3 内容寻址前缀树 vs 采样哈希

采样哈希做 FeatureBundle 缓存键有碰撞风险且无法天然支持前缀复用。本方案改用 RadixTree 内容寻址（token 序列）+ 图像内容哈希（FeatureBundle）：前者天然支持前缀复用且无碰撞，后者单次哈希可接受（ViT 计算更贵）。代价是 RadixTree 的内存与查找开销，但在千级条目下可忽略。

### 10.4 跨步骤复用的精确 vs 近似

精确前缀复用零质量损失但覆盖率受前缀发散限制；**发散场景下直接的 RelayCaching 裸拼接会破坏 RoPE 位置编码导致注意力失真，本方案以 Virtual-RoPE（§6.8）的非连续复用 + 发散阈值熔断取代**；选择性重算（RelayCaching ResidualAttention）覆盖更严重发散但需补全跨片段注意力；注意力相似度复用（RKSC）覆盖率最高但引入近似误差。本方案以精确前缀为首选、Virtual-RoPE 非连续复用为发散回退（>4 片段或增量比 <30% 时熔断退化为标准重算）、相似度为可选，按 `divergence_count` 与 `confidence gate` 自动切换，在质量与复用率间取动态平衡。

### 10.5 TCP 优先 vs RDMA 优先

阶段一 A6000 无 RDMA，TCP + 压缩即可完成功能验证与基准对比，方案不依赖阶段性不可用硬件。阶段二 A6000 Pro 有 RDMA，作为 P→D 带宽加速路径在协议无关接口下打开。这避免方案被硬件可用性绑架，同时为硬件升级预留清晰加速点。

### 10.6 双引擎算力隔离 vs 角色软切换

角色软切换（增量 Prefill 与 Decode 共享同一 CUDA Stream/CUDA Graph）实现最省事，但 Prefill 形状多变会反复破坏 Decode 的 CUDA Graph，导致 TPOT 大幅抖动。本方案采用 MPS 双引擎隔离：Decode 独占高优 Stream + 固定 Graph，Prefill 走独立 Stream + Chunked-Yield。代价是 MPS 隔离带来 ~10–15% 总算力损耗（隔离开销）及调度复杂度，换来 **TPOT 波动率 <5%** 这一硬指标——对长时解码的 Agent 交互场景，稳定性比峰值吞吐更重要。仅当增量 token <64 时才触发本地 Prefill，大块增量仍 deflect 回 Prefill 节点，避免隔离池被打满。

### 10.7 异步层级流水线 vs 串行层级注入

DeepStack 多层 ViT 特征注入若串行执行（计算 layer 0 → 注入 → 计算 layer 12 → 注入 …），计算与跨层传输相互等待，形成大量同步气泡。本方案采用多流异步层级流水线（§6.10）：layer 0/12/24 各走独立 stream，配合偏移预取与乱序容忍调度，把跨层传输藏在前一层计算背后。代价是 stream 切换与上下文保存的开销及乱序调度的实现复杂度，但多模态 Agent 场景 ViT 层数多、注入频繁，流水线收益远超切换开销。

### 10.8 KV 复用正确性 vs 复用率（生死线取舍）

复用率越高越省算力，但跨片段拼接破坏 RoPE 位置编码与 softmax 归一化（§6.5.1）。本方案以**四层降级**（§6.5/§6.8）划清生死线：精确前缀与 Virtual-RoPE 可证等价（C1/C2/C3 满足时 bit-exact）零质量损失；近似+verify gate 以 ε=2% 容忍度换更高复用率，但**永远保留 full-recompute 兜底，正确性从不依赖复用**（§11.7）。取舍偏向：宁可复用率低，不可正确性塌。代价是 verify gate 的 attention divergence 估算开销，但远低于全量重算。

### 10.9 GDKR 去中心化（三件拆分 + 分片）vs 单点强一致

单点 GDKR 强一致实现简单但 Raft 无法承载 10K+ QPS 的 fork/CoW 热路径，是单点瓶颈与可用性风险。本方案三件拆分（§6.7）：KV Directory 最终一致分片、Scheduler 本地去中心化、Consistency Manager 仅 per-shard Raft；热路径（refcount/fork/CoW CAS）下沉 shard-local owner shard，不走 Raft；Raft 只用于跨分片 2PC handoff、epoch commit、批量物理释放。代价是跨分片操作的 2PC 延迟与最终一致窗口内的 staleness，但热路径 QPS 与可用性显著提升，符合 §2.4 一致性模型（per-workflow 快照隔离 + 有界 staleness）。

### 10.10 反压降级 vs 峰值吞吐

为防 queue collapse（§6.14.3），过载时有界队列 + 显式 reject + 反压传播会主动牺牲部分吞吐（低优 503）。代价是峰值吞吐低于"无界队列硬扛"的理论上限，但换来系统稳定不雪崩、尾延迟可控（§6.14.4）。对 Agent 长时交互场景，稳定可预测优于瞬时高吞吐后崩溃。降级梯度（复用降级→压缩升档→Offload→reject）保证降级不牺牲正确性（§11.7）。



---

## 11. A6000 Pro RDMA 硬件利用设计

迁移到 A6000 Pro（具备 RoCE/IB RDMA NIC + GPUDirect RDMA）后，按下述利用硬件：

### 11.1 P->D 大块 KV（带宽关键链路）

- 在 Worker 初始化时把 vLLM PagedAttention block 内存注册为 RDMA Memory Region（pinned、GPUDirect-aware），一次性注册复用。
- 使用 RC（Reliable Connection）QPs；以 LIFO 批次发送页表，通过 RDMA write-with-imm 或消费端 RDMA read 拉取。
- GPUDirect RDMA 直写 GPU PagedAttention 页，绕过主机内存中转，降低 P->D 延迟并饱和链路。对齐 Mooncake 的 RDMA 路径（`kvcache_prefix_bench --protocol rdma`）。

### 11.2 E->P FeatureBundle（小而延迟敏感）

- 同机：SHM / CUDA IPC（零拷贝，us 级）。
- 跨机：RDMA send（小消息、低延迟）。per_level 压缩在 MR 注册前完成。

### 11.3 A2A 状态交接（元数据 + KV 增量）

- 元数据走 RDMA send（tiny）。
- KV 增量走 KVDirect 风格 RDMA read 按需拉取——仅拉取目标 RadixTree 缺失的页。

### 11.4 Offload 到远端 MooncakeStore

- 卸载走 RDMA write 到远端主机内存池；工具返回时 RDMA read 拉回。

### 11.5 缓冲区与验证

- 预注册 pinned MR 池，容量按 `prefix_cache.max_size_gb` 配置；GPUDirect RDMA 直写 GPU 页（无 CPU 暂存）。QP 数 = min(cores-2, 16) 每链路类型；完成通过 CQ 轮询线程 + 有界环形缓冲区。
- 验证指标：P->D 带宽（目标饱和 100-200 Gbps）、TTFT 相对 TCP 的下降、GPUDirect vs 主机中转的延迟差。
- **协议无关约束**：RDMA 路径是同一 `transfer(refs, plan, target)` 接口的实现。`hw_caps` 探测到 NIC + GPUDirect 时选 rdma，否则 tcp/shm。无代码路径硬依赖 RDMA 存在。

### 11.6 RDMA / TCP / SHM 三路内存所有权语义统一

> **设计修正**：v1 把 RDMA/TCP/SHM 当作并列传输实现，但未定义"页归谁所有、谁负责回收、跨路径迁移时所有权如何交接"。这三路对内存的生命周期假设不同（RDMA 直写 GPU pinned MR、TCP 经主机 bounce buffer、SHM 是 IPC 句柄），若所有权语义不统一，路径切换会导致 double-free 或 use-after-free。本节统一为协议无关的 ownership 模型。

#### 11.6.1 所有权权威：owner shard，而非传输路径

内存所有权的唯一权威是 **owner shard 的 Consistency Manager**（§6.7），通过 GID + refcount 表达。物理页无论经 RDMA / TCP / SHM 哪条路径传输，其所有权状态都映射到同一个 GID + refcount——**传输路径是数据的搬运方式，不是所有权的载体**。这保证：换路径不换所有权，回收判定只看 refcount，与传输路径解耦。

#### 11.6.2 各路径的缓冲区归属与回收责任

| 路径 | 缓冲区形态 | 归属 | 回收责任 |
|------|-----------|------|---------|
| **RDMA** | 预注册 pinned MR（GPUDirect 直写 GPU 页） | MR 池由数据面 worker 持有，但**逻辑所有权在 owner shard**；MR 只是 GID 的物理载体 | DMA 完成后 refcount-1；MR 槽位归还池，**不释放底层 GPU 页**（页由 owner shard 按 refcount 决定） |
| **TCP** | 主机 bounce buffer（接收侧临时） | 接收节点临时持有；拷贝入 GPU 页后 bounce buffer 即可释放 | bounce buffer 拷贝完成即释放；目标 GPU 页成为新 GID 实例，向 owner shard incref |
| **SHM/IPC** | 共享内存句柄 + 偏移 | 多进程共享同一物理页；**无拷贝，零成本** | 句柄关闭不释放页；页释放仍由 owner shard refcount 决定；SHM 段生命周期独立于单次 transfer |

关键不变量：**任何路径完成后，目标侧必须向 owner shard `incref`（除非是 borrow-only 的零拷贝 SHM，由发起方持有引用）。回收一律走 owner shard `RELEASE(GID)`，禁止任何路径自行 `free` 底层页。**

#### 11.6.3 路径迁移时的所有权交接

运行中可能发生路径迁移（如 RDMA NIC 故障 → 降级 TCP；跨机 → 同机 SHM）。交接协议保证不丢页、不 double-free：

1. **迁移原子性**：路径切换发生在 transfer 原语层，对 owner shard 透明。原路径的 in-flight DMA 若未完成，按 partial transfer 失败语义回滚（§6.13.2）——源侧保留所有权，目标侧丢弃半接收缓冲，retry 走新路径。
2. **borrow vs own 标记**：每个 GID 实例标记 `borrow`（零拷贝 SHM，引用计在发起方）或 `own`（拷贝得到，引用计在自身）。迁移发生时新路径按其缓冲特性重新打标；owner shard 只看 refcount 总和，不关心各实例是 borrow 还是 own。
3. **RDMA MR 失效**：若 GPU 页被 owner shard 决定释放（refcount=0），而该页仍注册为 RDMA MR，数据面 worker 先 `deregister_mr` 再释放 GPU 页，杜绝 RDMA 对已释放页的远程 write（use-after-free）。MR dereg 与页释放的顺序由 worker 本地保证，owner shard 通过 `RELEASE` 确认回执。

#### 11.6.4 跨路径一致性

- **路径透明的不变量**：无论经哪条路径，目标侧拿到的 KV 内容必须 bit-exact 一致（RDMA 直写、TCP bounce 拷贝、SHM 零拷贝都不改变内容）。压缩/量化是 TransferPolicy（§5）层的可选变换，与 ownership 无关，且在注册/传输前完成。
- **失败等价性**：任一路径失败都映射到统一的失败语义（§6.13.2），不因路径不同而行为分叉。这使上层调度无需感知"当前走哪条路"，只需面对统一的 transfer 抽象。

### 11.7 安全约束（Safety Constraints）

> **设计修正**：v1 未显式声明"KV 复用不能改变语义输出"与"fallback 永远存在"等不可逾越的红线。本节把工程安全边界写成显式约束，作为系统的不可妥协契约。

1. **语义保真红线（ε-tolerance）**：任何 KV 复用路径（exact prefix / Virtual-RoPE provable / approximate+verify）的输出，相对全量重算 baseline 的任务成功率/准确率回归不得超过 ε=2%（与 §8.6 quality 指标一致）。**正确性永远不依赖于复用**——复用是性能优化，不是正确性假设。
2. **fallback 永远可达**：full-recompute 路径（§6.5 第 4 层降级）必须始终可用且独立于复用机制。任一复用路径（含 Virtual-RoPE、近似 verify gate）出问题或证据不足时，系统必须能无条件回退到 full-recompute，且该回退不依赖 RDMA/SHM/任何特定硬件。fallback 是 correctness 的兜底，不可被降级梯度（§6.14.3）关闭——过载时可关复用，但不可关 fallback。
3. **硬件无关正确性**：系统正确性不依赖 RDMA 存在。RDMA/TCP/SHM（§11.6）只影响性能与延迟，不影响 KV 内容与所有权语义。A6000（无 RDMA，TCP 主）与 A6000 Pro（RDMA）跑同一工作流，输出必须一致。
4. **所有权不可旁路**：任何路径不得绕过 owner shard 直接释放物理页（§11.6.2）。违反此约束会导致 double-free / use-after-free，属严重缺陷。
5. **过载下的安全降级序**：过载时降级顺序固定为 复用降级 → 压缩升档 → Offload 腾显存 → reject（§6.14.3），**绝不**为保吞吐而牺牲正确性（如放宽 ε、关闭 verify gate、强制走近似复用）。
6. **fork 不可破坏父状态**：fork（§6.4）必须基于不可变快照版本（§4.1），fork 失败必须回滚 incref（§6.13.2），父状态的活跃计算绝不因 fork 而被改写或释放。


---

## 12. 附录 A：核心接口与伪代码

### A.1 状态层接口

```python
class StateLayer:
    def fork(self, parent: MultimodalState, child_agent_id: str) -> MultimodalState: ...
    def handoff(self, state: MultimodalState, to_agent: str) -> MultimodalState: ...
    def release(self, state: MultimodalState) -> None: ...           # 引用计数 -1，归零回收
    def advance_step(self, prev: MultimodalState, new_tokens, new_images) -> MultimodalState: ...
    def cow_page(self, shared_page: BlockRef) -> BlockRef: ...       # 页级写时复制
    def refresh_ttl(self, state: MultimodalState, step_window: int) -> None: ...
    def offload(self, state: MultimodalState, store: MooncakeStore) -> Handle: ...
    def restore(self, handle: Handle) -> MultimodalState: ...

class RadixTree:
    def match_longest_prefix(self, token_ids: List[int], scope: str) -> Tuple[List[BlockRef], List[int]]: ...
    def insert(self, token_ids: List[int], kv_pages: List[BlockRef]) -> None: ...
```

### A.2 fork 伪代码（O(1) 零拷贝）

```python
def fork(parent_state, child_agent_id):
    child = shallow_copy(parent_state)               # 复制指针 + meta，不复制数据
    child.state_id = new_uuid()
    child.meta.agent_id = child_agent_id
    for page_ref in parent_state.kv_pages:
        page_table.incref(page_ref.physical_id)      # 物理页引用计数 +1
    feature_store.incref(parent_state.feature_bundle.id)  # 特征束引用计数 +1
    register_reaper(child.state_id, ttl=child.meta.ttl_deadline)  # 异常泄漏兜底
    return child
```

### A.3 跨步骤复用伪代码（advance_step）

```python
def advance_step(prev_state, new_tokens, new_images):
    # 1. 精确前缀复用
    hit_pages, delta_tokens = radix_tree.match_longest_prefix(new_tokens, scope=prev_state.workflow_id)
    reused_features = [feature_store.get(h) for h in new_images if feature_store.has(h)]
    # 2. 增量 Prefill（仅 delta_tokens + 未命中特征）
    new_kv, new_features = prefill_worker.run(
        delta_tokens,
        delta_features=[img for img in new_images if not feature_store.has(img)],
        prefix_kv=hit_pages, inject_features=reused_features)
    # 3. 若 delta 发散严重 -> RelayCaching 选择性重算 + ResidualAttention 补全
    if divergence_score(delta_tokens) > THRESHOLD:
        new_kv = relay_recompute(new_tokens, prev_state, residual_attn=True)
    # 4. 组装新状态，前驱状态 TTL 续期
    state_chain.extend(prev_state.workflow_id, new_state)
    state_layer.refresh_ttl(prev_state, step_window=EXPECTED_STEPS)
    return new_state
```

### A.4 传输层接口

```python
def transfer(refs, plan: TransferPolicy, target: NodeID) -> TransferHandle:
    impl = pick_impl(plan.mode, hw_caps)   # shm / tcp_stream / rdma_write / rdma_read
    return impl.transfer(refs, plan, target)  # 异步句柄，可 await

@dataclass
class TransferPolicy:
    mode: str         # stream | pull | push_batch | shm
    compress: str     # none | cacheGen | splitzip | kvcodec | per_level
    precision: str    # bf16 | fp8 | spectrum | q4
    prefetch: str     # none | next_step | speculative
```

---

## 13. 附录 B：实施路线图

### 阶段一（A6000，TCP 主 + SHM）

1. 集成 vLLM MooncakeConnector 跑通 P->D 基础链路（B1 baseline）。
2. 在 serving 层之上插入 Encoder Worker + E->P 传输（SHM 同机 / TCP 跨机），实现 DeepStack 多层注入，完成基础任务 1 的 EPD 原型。
3. 实现状态层：MultimodalState + 页级引用计数 + 页级 CoW + RadixTree + FeatureBundle 内容哈希缓存，完成基础任务 2 的 Agent State Cloning。
4. 实现 TransferPolicy 四维参数化与策略大脑，跑通 E->P / P->D / A2A / Offload 四类通道。
5. 实现协同层：fork / handoff / advance_step / 图优先级调度 / 工具调用阻塞卸载 / 跨步骤复用（精确前缀 + 选择性重算回退）。
6. 在 m&m's / GAIA / M3-Bench 等数据集上对比 B0-B3，产出基础任务 3 的端到端 Demo 与吞吐/TTFT/JCT 增益报告。

### 阶段二（A6000 Pro，RDMA 加速）

7. `hw_caps` 探测 RDMA + GPUDirect；注册 PagedAttention block 为 MR；P->D 切换 RDMA 直写。
8. E->P 跨机切 RDMA send；A2A 切 RDMA read 按需拉取；Offload 切 RDMA write/read。
9. 对比 TCP vs RDMA 的 P->D 带宽与 TTFT，验证硬件加速点。
10. Omni 多阶段流水线扩展（进阶任务 1）作为 EPD 抽象的推广接入。

---

## 14. 参考资料

1. Mooncake: A KVCache-centric Disaggregated Architecture for LLM Serving. https://github.com/kvcache-ai/Mooncake
2. vLLM: Easy, Fast, and Cheap LLM Serving and RL with PagedAttention. https://github.com/vllm-project/vllm
3. DeepStack: Multi-layer Visual Feature Injection for Multimodal Large Language Models (Qwen3-VL). arXiv:2511.21631
4. ForkKV: Dual-RadixTree physical-shared + logical-private KV with page-level copy-on-write for multi-agent reasoning.
5. EPD-Serve / Efficiently Serving Large Multimodal Models via EPD Disaggregation: stage-level decoupling + async feature prefetch.
6. PPD / Prefill-Deflection: append-prefill routed to decode node, eliminating cross-node KV transfer.
7. xLLM: decoupled serving-engine + dynamic PD/EPD decomposition + global KV cache management.
8. KVCOMM: online anchor-pool for cross-context KV-cache reuse (text-only, 7.8x speedup).
9. RelayCaching: training-free selective KV recomputation for cross-step LLM collaboration (>80% reuse).
10. RKSC: Attention-Similarity KV sharing + confidence-gated early exit for multi-step/multi-branch reasoning.
11. CachedAttention: hierarchical GPU/CPU/disk KV + preload + async save (-87% TTFT).
12. Continuum: KV TTL + program-level FCFS (8x JCT).
13. SpectrumKV (per-token mixed precision), CacheGen (3.5-4.3x streaming), SplitZip (lossless), KVTC (PCA+quant 20x), KVDirect (pull-based).
14. Kamera (position-invariant multimodal KV reuse), MPIC (position-independent), VL-Cache (modality-aware token scoring).
15. PolyKV (asymmetrically-compressed shared KV pool, -97.7% mem), TokenDance (All-Gather collective sharing).
16. vLLM-Omni (stage abstraction + inter-stage connector, -91.4% JCT), ModServe (modality+stage-aware), Nova (adaptive cross-stage parallelization), TridentServe (diffusion stage-level).
17. ProServe (priority + preemptive scheduling), QLLM (DAG query plan), Halo (DAG query plan).
18. SGLang RadixAttention: content-addressed prefix tree for automatic prefix caching.
19. DroidSpeak, Q-KVComm: cross-LLM KV sharing.
20. 数据集: m&m's (arXiv:2403.11085), GTA (arXiv:2412.15606), GAIA, M3-Bench (arXiv:2511.17729), EpiBench (arXiv:2604.05557), ToolVQA (arXiv:2508.03284), MMBench, MMMU, DocVQA.
