# DeepSeek Engram：论文与 Demo 阅读笔记

本文基于 **2026-04-02** 读取到的官方材料整理：

- 论文：[Conditional Memory via Scalable Lookup: A New Axis of Sparsity for Large Language Models](https://arxiv.org/abs/2601.07372)
- 仓库：[deepseek-ai/Engram](https://github.com/deepseek-ai/Engram)
- Demo：[`engram_demo_v1.py`](https://github.com/deepseek-ai/Engram/blob/main/engram_demo_v1.py)

这份笔记重点回答三个问题：

1. Engram 想解决什么问题
2. 论文里的 Engram 结构到底怎么工作
3. 官方 demo 实际实现了什么，以及它和 Mooncake 的边界在哪里

## TL;DR

Engram 可以理解成一种 **conditional memory（条件记忆）**，它给大模型增加了一个区别于 Dense 和 MoE 的新稀疏轴：

- **Dense**：参数和计算一起增长。
- **MoE**：总参数增长，但每个 token 只激活少量专家，主要解决的是 **conditional computation（条件计算）**。
- **Engram**：把大量容量放进可查表的 memory 中，而每个 token 的检索开销基本保持常数级，主要解决的是 **conditional memory（条件记忆）**。

论文里的主路径可以概括成四步：

1. 先对 token 做压缩映射（tokenizer compression）。
2. 用局部 N-gram 构造确定性的 hash 地址。
3. 按地址从多头 memory table 中查出 embedding。
4. 再通过 gating、value projection、short convolution 和 residual 把这部分记忆融合回隐藏状态。

从系统视角看，Engram 最有价值的一点是：**它的检索地址由 token 序列决定，而不是由运行时 hidden state 动态路由决定。** 这使得预取、缓存、分层存储和数据面优化都更容易做。

## 1. Engram 想解决什么问题

论文的出发点很清楚：大模型既需要计算能力，也需要记忆能力。

- MoE 很擅长扩展条件计算。
- 但 MoE 并不是显式的 lookup memory。
- 语言里有很多高频、可复用、偏局部的模式，其实更适合“查出来”，而不是每次都重新算一遍。

因此作者提出，除了 dense 和 MoE 之外，还应该给模型加入一个新的能力维度：

- memory 容量可以通过扩大表规模继续增长；
- 每个 token 实际只查固定数量的 slot，所以计算开销不随 memory 容量线性上升；
- 检索地址由 token 模式决定，天然更适合系统优化。

论文把这种机制叫做 **Conditional Memory via Scalable Lookup**。

## 2. 论文里的 Engram 是怎么工作的

从论文和 demo 一起看，Engram 的主干可以画成下面这样：

```text
input_ids
  -> tokenizer compression
  -> N-gram hashing
  -> per-head embedding lookup
  -> 拼接成 Engram 特征
  -> value projection + context-aware gates
  -> short convolution
  -> residual 融合回 hidden states
```

注意，论文不是把 Engram 放到每一层，而是只插入到一部分 transformer layer 中。

### 2.1 Tokenizer compression

论文在哈希之前先做 tokenizer compression，用归一化后的 token 空间来降低词表规模。

官方 demo 里的 `CompressedTokenizer` 做了这些事：

- 从 `deepseek-ai/DeepSeek-V3` 加载 tokenizer；
- 对 token 对应的字符串做 Unicode 归一化；
- 去掉 accent、统一大小写、压缩空白；
- 把多个原始 token ID 映射到更小的 normalized token space。

论文给出的结论是，这一步可以把有效词表规模压缩大约 **23%**，同时又不会明显破坏 lookup 语义。

这一步的意义主要有三个：

- 降低 hash 冲突压力；
- 提高 memory table 的复用率；
- 让不同表面形式更容易共享同一类 lookup 行。

### 2.2 N-gram hash mapping

Engram 不是做 ANN，也不是做相似度搜索。它本质上是一个 **确定性的 N-gram 到 row id 的映射**。

在每个插入 Engram 的 layer 上，流程大致是：

1. 对输入序列做 shift，构造不同阶数的 N-gram 窗口；
2. 用每层独立的 odd multiplier 做混合；
3. 再对每个 head 对应的表大小取模；
4. 得到每个 token、每个 head 的 row index。

这个逻辑在 demo 中由 `NgramHashMapping` 实现。

demo 的默认配置是：

- `max_ngram_size=3`，也就是使用 2-gram 和 3-gram；
- `n_head_per_ngram=8`；
- 因此总头数是 `(3 - 1) * 8 = 16`。

另外，demo 不直接把 `engram_vocab_size` 当成每个 head 的最终表大小，而是会为每个 head 生成不同的 **prime-sized table**。这样做的目的，是尽量减少不同 head 上同步碰撞的概率。

可以把它和 MoE 做一个直观对比：

- **MoE** 是 hidden-state-driven expert routing；
- **Engram** 是 token-pattern-driven memory lookup。

这也是为什么论文强调，Engram 不是 MoE 的替代品，而是互补品。

### 2.3 Memory lookup

哈希地址出来之后，Engram 就按 head 去查 embedding table。

抽象上看：

- 每个 head 对应一张 `[N_h, D]` 的表；
- 所有 head 合起来，输出形状是 `[B, L, H, D]`；
- 再把 head 维展平，形成每个 token 的 Engram 特征。

demo 默认参数下：

- `n_embed_per_ngram=512`
- `n_head_per_ngram=8`
- 所以每个 head 的 embedding 维度是 `512 / 8 = 64`
- 总 Engram 特征宽度是 `(max_ngram_size - 1) * 512 = 1024`

这个 `1024` 恰好也和 demo 里的 `BackBoneConfig.hidden_size=1024` 对齐，说明 demo 是特意把 Engram 输出宽度和 backbone hidden size 配平了。

在实现上，demo 用 `MultiHeadEmbedding` 把所有 head 的表打包进一个大的 `nn.Embedding`，再通过 offset 定位到每个 head 自己的那一段。

### 2.4 Context-aware gating 和 value fusion

查出来的 embedding 并不是直接加回模型，而是先经过一个带上下文条件的融合路径。

论文和 demo 都包含这几个关键部件：

- Engram embedding 先投影成共享的 value；
- 再为每个 hyper-connection branch 生成各自的 key；
- 当前 hidden state 充当 query 信号；
- 用 branch-wise gate 决定要注入多少 memory。

在 demo 的 `Engram.forward(...)` 里，大致就是：

1. `multi_head_embedding(hash_input_ids)` 取出 `[B, L, H, D]`；
2. `flatten(start_dim=-2)` 把多个 head 拼起来；
3. 计算 gate；
4. 用 gate 调制 `value_proj(embeddings)`；
5. 经过 short conv；
6. 最后在 `TransformerBlock` 里通过 residual 加回去。

这一步说明 Engram 不是“静态词表增强”，而是一个 **被当前上下文调制的条件记忆模块**。

### 2.5 Short convolution

论文在 gating/value projection 之后还加了一层 short convolution，用来补足纯查表可能缺失的短程连续性。

demo 里的 `ShortConv` 有几个特征：

- depthwise；
- 沿 token 维做卷积；
- `kernel_size=4`；
- dilation 取 `max_ngram_size`。

可以把它理解成：查表负责把局部模式“拿出来”，short conv 负责把相邻 token 的短程结构再揉一下。

## 3. 论文里最值得记住的结论

### 3.1 Engram 和 MoE 是互补关系，不是替代关系

论文做了一个很重要的 ablation：在固定总参数和固定 activated FLOPs 的前提下，研究 sparse budget 应该怎么在 MoE experts 和 Engram memory 之间分配。

结论是一个 **U 型曲线**：

- 纯 MoE 不是最优；
- 纯 memory 也不是最优；
- 最好的点通常是把一部分 sparse 容量从 expert 挪给 Engram memory。

这说明 conditional computation 和 conditional memory 确实在解决不同的问题。

### 3.2 Engram-27B 明显优于同激活规模的 MoE-27B

论文 Table 1 对比了：

- `Dense-4B`
- `MoE-27B`
- `Engram-27B`
- `Engram-40B`

这些模型都对齐到了 **3.8B activated parameters**，训练 token 数也都对齐在 **262B**。

从 Table 1 挑几组最能说明问题的数据：

- validation loss：`1.634 -> 1.622`（`MoE-27B -> Engram-27B`）
- MMLU：`57.4 -> 60.4`
- BBH：`50.9 -> 55.9`
- ARC-Challenge：`70.1 -> 73.8`
- DROP：`55.7 -> 59.0`
- HumanEval：`37.8 -> 40.8`
- GSM8K：`58.4 -> 60.6`

这说明 Engram 的收益并不只体现在“死记硬背”类任务上，也会外溢到推理、阅读理解、代码和数学任务。

### 3.3 更大的 Engram memory 往往更强，但不是所有指标都单调上升

`Engram-40B` 把 Engram memory 从 **5.7B** 提高到 **18.5B**，同时仍然保持 activated compute 预算不变。

论文报告它在大多数任务上继续提升，例如：

- validation loss：`1.622 -> 1.610`
- AGIEval：`41.8 -> 45.9`
- ARC-Challenge：`73.8 -> 76.4`
- GSM8K：`60.6 -> 62.6`

但它也不是对所有 benchmark 都严格单调更强：

- `C3`
- `HumanEval`
- `MBPP`

这些点上，`Engram-40B` 没有完全压过 `Engram-27B`。论文自己的解释是：在固定 262B token 预算下，更大的 memory 可能还没有被训练到充分收敛。

### 3.4 Long-context 能力也有明显提升

论文 Table 2 做了 32k 长上下文扩展实验。

两个信息最关键：

- `Engram-27B (41k steps)` 已经能在 LongPPL 上接近甚至匹配 `MoE-27B (50k)`，同时在很多 RULER 子任务上更好；
- `Engram-27B (46k)` 被当作 **iso-loss** 对比点时，依然在大多数长上下文指标上优于 MoE 基线。

比如 Table 2 里的几组数字：

- RULER `MQ`：`84.2 -> 97.0`
- RULER `VT`：`77.0 -> 87.2`
- LongPPL（books）：`4.38 -> 4.19`

论文的解释是：显式 lookup memory 帮模型接住了大量局部模式，从而把更多表达能力留给真正的长程依赖建模。

### 3.5 Engram 放在哪些层，很重要

论文的 layer ablation 有两个稳定趋势：

- 把 Engram 放在后层，通常比放在前层更有效；
- 多层 Engram 连续放在一段后层里，通常比打散放置略好。

这对系统设计也很有启发：

- 放得更深，可以给 lookup/prefetch 留出更多前置计算时间去做重叠；
- 但放得太晚，又可能减少检索结果影响后续层的空间。

所以 layer placement 本质上是一个 **建模收益和系统延迟联合优化** 的问题。

### 3.6 推理吞吐开销不大

论文 Table 4 给出了在 H100、序列长度 **2048** 下的吞吐数据：

- `Dense-4B`：`9031.62 tok/s`
- `Engram-4B`：`8858.28 tok/s`
- `Dense-8B`：`6315.52 tok/s`
- `Engram-8B`：`6140.02 tok/s`

按表中的吞吐直接计算，Engram 带来的额外开销大约是：

- **1.9%**（4B）
- **2.8%**（8B）

这个量级并不夸张，前提是 serving 系统能把 lookup 路径做好。

## 4. 官方 Demo 实际实现了什么

官方 demo 明确声明自己是一个教学性质的实现，而不是生产实现。它自己就写明了：

- 只用于演示；
- 不是 production-ready；
- 没有 custom CUDA kernel，也没有分布式训练支持；
- attention、MoE、norm、hyper-connection 等大量细节都被简化或 mock 了。

所以 demo 更适合回答的是：

- 结构上要发生什么；
- 张量怎样流动；
- 模块边界在哪里。

它并不回答：

- 训练时怎么稳定收敛；
- 多机多卡怎么切分；
- 存储层怎么 offload；
- cache/prefetch 怎么做；
- serving latency 怎么压到最好。

### 4.1 Demo 代码结构

如果按逻辑拆，demo 基本可以分成六块：

1. `EngramConfig` / `BackBoneConfig`
2. `CompressedTokenizer`
3. `NgramHashMapping`
4. `MultiHeadEmbedding`
5. `ShortConv`
6. `Engram` + `TransformerBlock`

### 4.2 Demo 的最小 forward path

demo 的前向路径可以概括成：

```text
text
  -> tokenizer(input_ids)
  -> backbone token embedding
  -> mock hyper-connection expansion
  -> 在选中的 layer 上执行 Engram
       -> hash(input_ids)
       -> multi-head embedding lookup
       -> gates from hidden_states
       -> value projection
       -> short conv
       -> residual add
  -> mock attention / mock moe
  -> lm head
```

### 4.3 几个特别值得注意的实现细节

#### 细节 1：论文和 demo 很接近，但不是逐字逐句一模一样

论文描述的是一个 30-layer 模型，并把 Engram 放在 **layer 2 和 layer 15**。

而 demo 里的默认配置是：

```python
layer_ids = [1, 15]
```

这很可能只是索引习惯或演示简化上的差异，但在把论文和代码逐项对照时，这个点值得记一下。

#### 细节 2：真实的 per-head 表大小不是原始 `engram_vocab_size`

demo 不是拿 `engram_vocab_size` 直接当每个 head 的最终大小，而是：

- 先给每个 N-gram 阶数设一个 base size；
- 再为每个 head 生成不同的 prime-sized table；
- 同时尽量避免不同 layer/head 上重复使用同一个素数大小。

这意味着，真正参与取模的空间是 **per-head prime size**，不是原始配置里的 base vocab size。

#### 细节 3：gate 是 branch-specific 的

demo 里 value projection 是共享的，但 key projection 和 norm 是按 hyper-connection branch 分开的。

这很符合论文的直觉：

- memory 可以共享；
- 但不同 branch 如何解释和注入这份 memory，是上下文相关的。

#### 细节 4：demo 只做了一次最小 forward sanity check

脚本最后只跑了一个很小的例子：

```python
text = "Only Alexander the Great could tame the horse Bucephalus."
```

然后打印输入和输出的 shape。

所以它证明的是：

- 模块可以端到端串起来；
- 维度设计是自洽的。

它没有证明的是：

- 训练稳定性；
- 分布式正确性；
- 热点缓存收益；
- offload/prefetch 效果；
- 生产环境推理性能。

## 5. 论文、Demo 和 Mooncake 的关系

如果站在 Mooncake 的上下文里读 Engram，最重要的是把 **模型侧融合逻辑** 和 **数据面 lookup 逻辑** 分开看。

| 组件 | 论文 | 官方 demo | Mooncake 当前分支 |
| --- | --- | --- | --- |
| token compression | 有 | `CompressedTokenizer` | 不在 Mooncake backend 边界内；由调用方/runtime 自己处理 |
| hashing | 确定性的 N-gram lookup | `NgramHashMapping.hash()` | 不在 Mooncake backend 边界内；调用方先算好 `row_ids` 再调用 `lookup()` |
| embedding storage | 可扩展、可分片、可 offload 的 memory | 进程内单个 `nn.Embedding` | Mooncake Store 中的 per-layer / per-head table |
| query 输出 | Engram embeddings | 在 `Engram.forward(...)` 中直接继续融合 | `lookup()` 返回 `[B, L, H, D]` |
| gating / value projection | 架构核心之一 | 已实现 | 明确不在 Mooncake 边界内 |
| short convolution | 架构核心之一 | 已实现 | 明确不在 Mooncake 边界内 |
| 系统优化 | 强调 offload、overlap、cache hierarchy | 基本没做 | Mooncake 现在提供 same-node local-direct、通用 sparse range read 和 Store 级数据搬运能力 |

这张表基本就说明了三者的角色边界：

- **论文** 定义的是完整模型架构；
- **demo** 展示的是一个简化后的端到端参考实现；
- **Mooncake** 更适合作为 deterministic embedding lookup 的数据面，而不是把完整 Engram layer 都塞进来。

## 6. 这对 Mooncake 的启发

把论文和 demo 放到 Mooncake 的语境里看，有一个非常清晰的职责划分。

### 6.1 适合 Mooncake 负责的部分

Mooncake 天然适合承接这些特征明显的数据面工作：

- 确定性；
- 表驱动；
- 读多写少；
- 易缓存；
- 可按 head / row 做 batch 化。

对应到 Engram，就是：

- per-head table 存储；
- hash 到 row 的地址计算；
- batched metadata query；
- same-node local-direct fast path；
- range merge 后的远端稀疏拉取；
- 多级缓存和放置策略。

### 6.2 更适合留在模型 runtime 里的部分

下面这些本质上属于模型语义，而不是存储语义：

- tokenizer compression 策略；
- branch-specific gating；
- value projection；
- short convolution；
- residual / hyper-connection 融合。

这些更适合由真正消费 Engram embeddings 的模型 runtime 或框架来负责。

### 6.3 为什么论文对 Mooncake 是利好信号

论文里有几条结论，和 Mooncake 的优势是天然对齐的：

1. **Deterministic lookup**
   - 检索地址由 token 序列决定；
   - 这比 hidden-state-driven routing 更容易做预取和缓存。

2. **Zipfian access distribution**
   - 高频 N-gram 会形成热点；
   - 这很适合做 host-memory cache 和多级缓存。

3. **Layer placement 能帮助 latency hiding**
   - Engram 放在后层，可以给系统留更大的重叠窗口；
   - Mooncake 可以用这个窗口做 prefetch 或通用 sparse read overlap。

4. **Memory 容量可以远大于 active compute**
   - Engram 想要的是大 memory、小额外计算；
   - Mooncake 擅长的正是高效服务大对象和稀疏数据访问。

## 7. 怎么正确地读这个 Demo

如果目标是理解或实现 Engram，demo 很适合当作一张“概念地图”：

- `CompressedTokenizer` 解释了词表压缩；
- `NgramHashMapping` 解释了地址生成；
- `MultiHeadEmbedding` 解释了到底在查什么；
- `Engram.forward(...)` 解释了查出来的 memory 怎样影响 hidden states。

但如果目标是设计 production serving 系统，就不能直接照搬 demo，因为它：

- 把 memory 都放在进程内；
- 没有 async prefetch；
- 没有跨设备/跨节点切分；
- 没有 cache hierarchy 管理；
- 没有 kernel 级优化。

所以更准确地说，demo 回答的是：

- **“功能上需要发生什么？”**

而不是：

- **“生产系统应该怎么搭？”**

## 8. 总结

结合官方论文和 demo，我对 Engram 的结论是：

- Engram 不是“另一种 MoE”；
- 它是一个以 lookup 为核心的条件记忆模块；
- 它真正新增的能力，是把 **memory capacity** 和 **per-token compute** 拆开；
- 官方 demo 足够解释架构思路，但明显还不是 serving 参考实现；
- Mooncake 当前把自己定位成 Engram 的 **lookup backend** 是合理的：负责分布式存储、row-id lookup 和高效数据搬运，而把 hashing / gating / conv / residual 等模型语义留给 runtime。

## Related Mooncake Documents

- [Engram: Mooncake Lookup Backend](./engram.md)

## References

- 论文：[Conditional Memory via Scalable Lookup: A New Axis of Sparsity for Large Language Models](https://arxiv.org/abs/2601.07372)
- 仓库：[deepseek-ai/Engram](https://github.com/deepseek-ai/Engram)
- Demo：[`engram_demo_v1.py`](https://github.com/deepseek-ai/Engram/blob/main/engram_demo_v1.py)
