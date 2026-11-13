# CachePilot 设计文档

## 1. 背景

大模型推理中，长上下文 Prefill 成本高，KVCache 的跨请求复用与跨节点检索调度直接影响 TTFT 与集群带宽。Mooncake 提供了分布式 KVCache Store（`MooncakeDistributedStore`）与官方 benchmark（`store_kv_bench.py`），适合作为底层存储与性能基线。

CachePilot 的目标不是 fork Mooncake，而是在其之上构建一层**非侵入式评测套件（non-invasive evaluation suite）**：

- 用官方 Store Bench 建立**真实 IO 基线**（实机实测）；
- 用可控 Prefix Reuse workload 分析 prefix 命中率对 TTFT 的影响；
- 用离线策略评估比较不同 Retrieval Scheduler 策略。

---

## 2. CachePilot 架构

```
                 ┌────────────────────┐
                 │   scripts/run_*.sh │
                 └─────────┬──────────┘
                           │
         ┌─────────────────┼─────────────────┐
         ▼                 ▼                 ▼
 ┌───────────────┐ ┌───────────────┐ ┌──────────────────┐
 │ Store Runner  │ │ Prefix Reuse  │ │ Retrieval Sched  │
 │ (真实实测)     │ │ Evaluation   │ │ Evaluation       │
 │ subprocess →  │ │ 可控 workload │ │ workload gen     │
 │ 官方 bench    │ │ 参数化评估     │ │ (Monte Carlo)    │
 └───────┬───────┘ └───────┬───────┘ └────────┬─────────┘
         │                 │                  │
         ▼                 ▼                  ▼
   parse_store_output   metrics.py        metrics.py
         │                 │                  │
         └────────────┬────┴──────────────────┘
                      ▼
              results/csv/*.csv
                      │
                      ▼
              plot_results.py
                      │
                      ▼
              results/figures/*.png
```

设计原则：

1. **非侵入**：不改 Mooncake 源码；真实调用官方 Store API / benchmark。
2. **Store Bench 建立真实 IO 基线**：通过 `MooncakeDistributedStore` + `store_kv_bench.py` 实机实测。
3. **Prefix Reuse 使用可控 workload**：分析 prefix 命中率对 TTFT 的影响，参数可与真实 Store 延迟对齐。
4. **Retrieval Scheduler 使用离线策略评估**：在同一批 workload 上比较不同调度策略。
5. **路径相对项目根**：脚本 `cd` 到仓库根再执行。

---

## 3. 真实运行链路

Store Benchmark 的端到端链路：

```text
mooncake_master
  （--enable_http_metadata_server=true, port=8080）
        │
        ▼
HTTP metadata server
  （http://HOST_IP:8080/metadata）
        │
        ▼
MooncakeDistributedStore
  （Python binding / mooncake-transfer-engine-non-cuda, protocol=tcp）
        │
        ▼
mooncake-store/benchmarks/store_kv_bench.py
  （verify_write / read_perf 等官方 scenario）
        │
        ▼
benchmark/mooncake_store_runner.py
  （CachePilot wrapper：参数矩阵、日志落盘、失败隔离）
        │
        ▼
benchmark/parse_store_output.py
  （解析 req/s、MiB/s、lat_p50/p95/p99 等）
        │
        ▼
results/csv/store_benchmark.csv
  （及 store_verify_real.csv）
        │
        ▼
benchmark/plot_results.py
  → results/figures/*.png
```

本链路已在 AutoDL RTX 4090 实例上完整跑通：`verify_write` 与 16 组 `read_perf` 全部 `return_code=0`。

---

## 4. Store Benchmark 设计

### 4.1 包装官方真实 Benchmark

`mooncake_store_runner.py` 通过 `subprocess` 调用：

```text
{MOONCAKE_ROOT}/mooncake-store/benchmarks/store_kv_bench.py
```

不自行实现 put/get，避免与官方语义漂移。底层由 `MooncakeDistributedStore` 完成真实 Store IO。

### 4.2 Mooncake Root 解析

优先级：

1. `--mooncake-root`
2. 环境变量 `MOONCAKE_ROOT`
3. 候选路径：`../Mooncake`、`../../Mooncake`、`/root/autodl-tmp/mooncake_competition/Mooncake`

校验条件：存在 `mooncake-store/benchmarks/store_kv_bench.py`。

### 4.3 参数矩阵

遍历 `value_sizes × batch_sizes`，按 scenario 组装官方参数：

| scenario | 关键参数 |
|----------|----------|
| verify_write | `--verify --pattern 0xab`，较小 `nr-objects` |
| write_perf | `--runtime` |
| read_perf | `--prepare-mode auto --phase-gap-mode sleep --phase-gap-sec 1 --verify --pattern 0xee` |
| mixed_rw | `--prepare-mode auto --rwmixread 70` |

### 4.4 输出解析

`parse_store_output.py` 用正则提取 `req/s`、`kv/s`、`MiB/s`、延迟分位、`misses`、`verify_failures`、`errors`；缺失字段填 `None`，不崩溃。

---

## 5. Prefix Reuse Evaluation 设计

目标：在给定 prefix 长度与 cache hit ratio 下，基于可控 workload 与可标定 Store get latency，评估相对「无缓存 Prefill」的 TTFT 改善趋势。

公式：

```text
prefix_kv_mb = prefix_tokens * kv_bytes_per_token / 1024 / 1024
no_cache_ttft = prefix_tokens * prefill_ms_per_token + decode_first_token_ms
cache_get_latency = store_get_base_ms + prefix_kv_mb * store_get_ms_per_mb
prefix_cache_ttft = hit_ratio * (cache_get_latency + decode_first_token_ms)
                  + (1 - hit_ratio) * no_cache_ttft
ttft_reduction_ms = no_cache_ttft - prefix_cache_ttft
ttft_reduction_pct = ttft_reduction_ms / no_cache_ttft * 100
```

含义：

- hit 时：从 Store 取回 Prefix KV + 首 token decode；
- miss 时：完整 Prefill + 首 token decode；
- `store_get_*` 参数可与真实 Store 延迟（如实测 p50/p99）标定对齐。

这是**参数化评估 / 可控 workload 评估**，不是对 Store API 的替代实现。

---

## 6. Retrieval Scheduler Evaluation 设计

### 6.1 Block Metadata

每个 KV block：

| 字段 | 含义 |
|------|------|
| block_id | 块 ID |
| prefix_id | 所属 prefix |
| reuse_count | 历史复用次数 |
| importance_score | 重要性 |
| location / source_node | 主副本节点 |
| replica_list | 可用副本节点 |
| estimated_fetch_latency_ms | 各副本拉取延迟 |
| size_mb | 块大小 |

### 6.2 策略

1. **Random**：在副本中均匀随机；
2. **Nearest**：优先本地副本，否则选最低延迟副本；
3. **Reuse-aware**：高复用块尽量本地命中，否则选最近；
4. **CachePilot**：

```text
score = alpha * importance_score
      + beta  * normalized_reuse_count
      - gamma * normalized_fetch_latency
      - load_penalty
      + local_bonus
```

选 score 最高的副本；`hotspot_ratio = max_source_load / mean_source_load`。

### 6.3 公平对比

- 固定 `--seed`；
- 同一批 blocks / requests 喂给所有策略；
- workload 可用 Monte Carlo 方法生成，用于离线策略评估；
- 输出 summary CSV + decision log。

---

## 7. 数据流

```text
run_all.sh
  ├─ store_verify / store_perf   ← 真实 Mooncake Store 实机实测
  │    └─ logs/store_*.log → parse → csv/store_benchmark.csv
  ├─ prefix_reuse                ← 可控 Prefix Reuse workload 评估
  │    → csv/prefix_reuse.csv + logs/prefix_reuse.log
  ├─ scheduler evaluation        ← Retrieval Scheduler 离线策略评估
  │    → csv/retrieval_scheduler.csv
  │    + logs/retrieval_scheduler_decisions.csv
  └─ plot_results → figures/*.png
```

---

## 8. 和 Mooncake 的关系

| 项目 | 关系 |
|------|------|
| Mooncake Store / `MooncakeDistributedStore` | 真实依赖；通过官方 bench 实机测量 |
| mooncake_master + HTTP metadata | Store 实测前置服务 |
| 官方 `store_kv_bench.py` | Store 性能与正确性底座 |
| CachePilot | 独立仓库；benchmark extension + 策略评估 + 可视化 |

CachePilot **不** vendoring Mooncake，也不 patch 其 wheel / C++ 代码。

---

## 9. 局限性

1. Prefix Reuse / Retrieval Scheduler 为离线策略评估，未接入真实 LLM serving runtime，不作为端到端集群性能声明。
2. Store 输出解析依赖官方日志文本格式；上游格式变更需更新正则。
3. CachePilot 分数中的 load_penalty / local_bonus 为启发式，需结合真实拓扑再标定。
4. 当前实机验证为单机 TCP；RDMA、多网卡、跨机延迟模型待扩展。
