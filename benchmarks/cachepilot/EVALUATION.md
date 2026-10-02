# CachePilot 评测文档

> **数据性质说明**  
> - **Store Benchmark**：真实 Mooncake Store 实机实测数据（AutoDL RTX 4090）。  
> - **Prefix Reuse / Retrieval Scheduler**：基于可控 workload 的策略评估结果，不作为真实分布式集群端到端性能声明。

---

## 1. 实验环境

| 项目 | 配置 |
|------|------|
| 平台 | AutoDL 容器实例 |
| GPU | RTX 4090 24GB |
| CPU | 16 核 |
| 内存 | 120GB |
| OS | Ubuntu 22.04 |
| Python | 3.10 |
| CUDA | 11.8 |
| Mooncake package | mooncake-transfer-engine-non-cuda |
| Protocol | TCP |
| Master | `HOST_IP:50051` |
| Metadata | `http://HOST_IP:8080/metadata` |
| Store API | `MooncakeDistributedStore` |
| 官方 Benchmark | `mooncake-store/benchmarks/store_kv_bench.py` |
| CachePilot Wrapper | `benchmark/mooncake_store_runner.py` |

启动示例：

```bash
mooncake_master \
  --enable_http_metadata_server=true \
  --http_metadata_server_host=0.0.0.0 \
  --http_metadata_server_port=8080 \
  --eviction_high_watermark_ratio=0.95
```

---

## 2. 指标定义

### 2.1 Store Benchmark（真实实测）

| 指标 | 含义 |
|------|------|
| req/s | 请求吞吐 |
| kv/s | KV 操作吞吐 |
| MiB/s | 有效带宽 |
| lat_mean / p50 / p95 / p99 | 延迟均值与分位（ms） |
| misses | 未命中次数 |
| verify_failures | 校验失败次数 |
| errors | 错误次数 |
| return_code | 子进程退出码（0 成功） |

### 2.2 Prefix Reuse Evaluation（可控 workload）

| 指标 | 含义 |
|------|------|
| no_cache_ttft_ms | 无缓存 TTFT |
| prefix_cache_ttft_ms | 有 Prefix Cache 时的期望 TTFT |
| store_get_latency_ms | 从 Store 拉取 Prefix KV 的估计延迟（可与实测标定） |
| ttft_reduction_ms / pct | 相对无缓存的绝对 / 相对收益 |

### 2.3 Retrieval Scheduler Evaluation（离线策略评估）

| 指标 | 含义 |
|------|------|
| avg / p50 / p95 / p99 latency | 检索延迟 |
| remote_traffic_mb | 跨节点传输量 |
| hotspot_ratio | `max_source_load / mean_source_load` |
| avg_source_load / max_source_load | 源节点负载 |

---

## 3. Store Benchmark 真实结果

### 3.1 verify_write

数据源：`results/csv/store_verify_real.csv`  
结论：`return_code=0`，`misses=0`，`verify_failures=0`。

| value_size | batch_size | req/s | kv/s | MiB/s | p50 | p95 | p99 | misses | verify_failures |
|------------|------------|-------|------|-------|-----|-----|-----|--------|-----------------|
| 4096 | 4 | 2482.98 | 9931.94 | 38.8 | 0.260 | 0.718 | 0.781 | 0 | 0 |

另：`lat_mean = 0.377 ms`。

### 3.2 read_perf

数据源：`results/csv/store_benchmark.csv`  
结论：共 **16 组**真实运行，全部 `return_code=0`。  
矩阵：value_size ∈ {4096, 65536, 1048576, 4194304} × batch_size ∈ {1, 4, 8, 16}。

代表性结果：

| value_size | batch_size | MiB/s | p50 (ms) | p95 (ms) | p99 (ms) |
|------------|------------|-------|----------|----------|----------|
| 4096 | 1 | 28.78 | 0.106 | 0.225 | 0.380 |
| 4096 | 16 | 139.86 | 0.352 | 0.661 | 0.703 |
| 65536 | 4 | 694.43 | 0.304 | 0.503 | 0.917 |
| 1048576 | 4 | **1399.08** | 2.574 | 3.785 | 5.956 |
| 4194304 | 4 | 1227.74 | 12.251 | 14.809 | 22.456 |
| 4194304 | 16 | 1015.36 | 52.517 | 88.238 | **93.267** |

观察：

- 小对象（4KB）带宽随 batch 提升明显（28.78 → 139.86 MiB/s）。
- 峰值带宽出现在 1MB、batch=4：**1399.08 MiB/s**。
- 4MB 大对象下延迟随 batch 上升；batch=16 时 p99 达 **93.267 ms**，吞吐仍保持约 1 GiB/s 量级。

---

## 4. Prefix Reuse 结果表模板

数据源：`results/csv/prefix_reuse.csv`  
性质：可控 Prefix Reuse workload 评估（可与真实 Store get 延迟参数对齐）。

| prefix_tokens | cache_hit_ratio | no_cache_ttft_ms | prefix_cache_ttft_ms | ttft_reduction_pct |
|---------------|-----------------|------------------|----------------------|--------------------|
| 512 | 0.0 | | | |
| 512 | 0.5 | | | |
| 4096 | 0.75 | | | |
| 8192 | 0.9 | | | |

---

## 5. Scheduler 结果表模板

数据源：`results/csv/retrieval_scheduler.csv`  
性质：Retrieval Scheduler 离线策略评估。

| scheduler | avg_latency_ms | p99_latency_ms | remote_traffic_mb | hotspot_ratio |
|-----------|----------------|----------------|-------------------|---------------|
| Random | | | | |
| Nearest | | | | |
| Reuse-aware | | | | |
| CachePilot | | | | |

---

## 6. 图表说明

| 文件 | 内容 |
|------|------|
| `store_bandwidth_by_value_size.png` | 真实 Store：不同 value size 下带宽（按 batch 分组） |
| `store_p99_by_batch_size.png` | 真实 Store：不同 batch size 下 p99 延迟 |
| `prefix_length_vs_ttft_reduction.png` | Prefix 长度 vs TTFT 降低比例（可控 workload） |
| `cache_hit_ratio_vs_ttft.png` | Hit ratio vs TTFT（可控 workload） |
| `scheduler_latency.png` | 各策略延迟对比（离线策略评估） |
| `scheduler_remote_traffic.png` | 各策略远程流量 |
| `scheduler_hotspot.png` | 各策略热点比 |

---

## 7. 复现实验步骤

```bash
cd CachePilot
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

export MOONCAKE_ROOT=/path/to/Mooncake
# 安装 Mooncake Python binding（如 mooncake-transfer-engine-non-cuda）
# 启动 mooncake_master（见上文）

bash scripts/run_all.sh
```

期望产物：

- `results/csv/store_benchmark.csv`（真实 read_perf）
- `results/csv/store_verify_real.csv` 或 verify 日志对应 CSV
- `results/csv/prefix_reuse.csv`
- `results/csv/retrieval_scheduler.csv`
- `results/figures/store_bandwidth_by_value_size.png`
- `results/figures/prefix_length_vs_ttft_reduction.png`
- `results/figures/scheduler_latency.png`
- `results/logs/store_*.log`

Prefix Reuse 与 Scheduler 也可在无 Store 服务时作为离线策略评估模块独立运行。

---

## 8. 结果分析

### 8.1 Store（真实实测）

- 带宽是否随 value size 上升，并在大对象接近平台上限？本实验峰值约 **1399 MiB/s**（1MB, batch=4）。
- p99 是否随 batch size 恶化？4MB batch=16 时 p99≈**93 ms**，需在调度侧权衡吞吐与尾延迟。
- verify_write 是否 `verify_failures=0`？本实验为 **0**。

### 8.2 Prefix Reuse（可控 workload）

- 固定 hit ratio 时，更长 prefix 的相对收益是否更大？
- hit ratio 从 0→0.9，TTFT 曲线如何变化？
- 将 `store_get_ms_per_mb` 换成实测 Store 延迟后，结论是否仍成立？

### 8.3 Scheduler（离线策略评估）

- CachePilot 相对 Random 的 p99 / remote traffic / hotspot 改善幅度？
- Nearest 是否已能显著降延迟？CachePilot 的额外收益来自哪里（复用 / 负载）？
- 调整 `alpha/beta/gamma` 后，热点与延迟的权衡如何变化？

### 8.4 结论摘要

> 在 AutoDL RTX 4090 + Mooncake TCP Store 环境下，CachePilot 已完成真实 `verify_write` 与 16 组 `read_perf` 实机实测，峰值带宽约 1399 MiB/s，校验零失败。  
> Prefix Reuse 与 Retrieval Scheduler 提供基于可控 workload 的策略评估，用于分析复用收益与调度权衡；后续可将 Store 实测延迟进一步标定进评估参数，并扩展多机 / RDMA 场景。
