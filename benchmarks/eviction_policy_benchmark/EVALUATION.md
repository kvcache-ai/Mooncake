# Mooncake Store Hidden State Type-Aware Eviction Benchmark Report

## 1. Overview

This report evaluates the Hidden State type-aware eviction mechanism added to
Mooncake Store. The benchmark targets multimodal EPD
(Encoder-Prefill-Decode) style workloads where Hidden State objects and KV
Cache objects have different access patterns, object sizes, and recomputation
costs.

The benchmark measures Store object-level cache behavior only. It does not
measure end-to-end inference latency.

The evaluation focuses on three mechanisms:

- a logical Hidden State memory budget before global eviction;
- a Hidden-specific lease TTL independent from the KV lease TTL;
- a soft-pinned Hidden lease for high-value Hidden objects, especially video
  Hidden objects.

## 2. Test Scope

The benchmark covers in-memory Mooncake Store eviction behavior under a mixed
Hidden/KV workload. It does not cover multi-replica behavior, NoF, disk
offload, snapshot restore, or end-to-end inference serving.

The benchmark workload is scaled down with `scale=0.0025` while preserving the
relative pressure between Hidden State objects and KV Cache objects.

## 3. Environment and Fixed Parameters

| Item | Value |
| --- | --- |
| Deployment | Single-node Docker environment |
| Store capacity | 64 MiB |
| Storage backend | In-memory cache only |
| Proxy mode | `session` |
| Execution mode | `request_eviction` |
| Workload fingerprint | `ab69f2dc33841c64` |
| `scale` | `0.0025` |
| `sessions` | `240` |
| `mixed_write_sessions` | `260` |
| `eviction_high_watermark_ratio` | `0.95` |
| `eviction_ratio` | `0.05` |

The request replay preserves timestamp ordering. Session arrival intervals are
sampled with `rng.expovariate`, which is equivalent to a Poisson arrival
process. Intra-session turn intervals are sampled with `rng.lognormvariate`.
The replay uses `--request-timing-scale 0.05` to compress real sleep time while
preserving the event order.

## 4. Workload Model

The benchmark models a mixed multimodal workload with text, image, multi-image,
and video sessions. Video sessions account for about 10% of the mixed workload.

Object-level access totals in the reported three-run average are:

| Object type | Total accessed objects |
| --- | ---: |
| KV Cache | 113,747 |
| Hidden State | 293 |
| Image Hidden | 275 |
| Video Hidden | 18 |

Hidden State objects are modeled as larger, lower-frequency objects with high
reuse value. KV Cache objects are modeled as high-frequency, fine-grained cache
objects and remain the dominant source of object accesses.

`Saved compute ms` is a model-based estimate used only for relative policy
comparison. It should not be interpreted as measured end-to-end latency
improvement.

## 5. Evaluated Cases

The report uses 14 benchmark cases:

| Case | Purpose |
| --- | --- |
| `original_baseline_500ms` | Native unified TTL baseline, TTL=500ms |
| `original_baseline_1s` | Native unified TTL baseline, TTL=1s |
| `original_baseline_5s` | Native unified TTL baseline, TTL=5s |
| `original_baseline_10s` | Native unified TTL baseline, TTL=10s |
| `hidden_budget_0p04_h10s_kv10s` | Hidden budget sweep, budget=0.04 |
| `hidden_budget_0p06_h10s_kv10s` | Hidden budget sweep, budget=0.06 |
| `hidden_budget_0p08_h10s_kv10s` | Hidden budget sweep, budget=0.08 |
| `hidden_budget_0p10_h10s_kv10s` | Hidden budget sweep, budget=0.10 |
| `hidden_budget_0p12_h10s_kv10s` | Hidden budget sweep, budget=0.12 |
| `hidden_budget_0p08_h500ms_kv10s` | Hidden lease sweep, Hidden TTL=500ms |
| `hidden_budget_0p08_h1s_kv10s` | Hidden lease sweep, Hidden TTL=1s |
| `hidden_budget_0p08_h5s_kv10s` | Hidden lease sweep, Hidden TTL=5s |
| `hidden_budget_1p0_h10s_kv10s` | Fallback behavior, budget=1.0 |
| `soft_pin_budget_0p08_h10s_soft20s_kv10s` | Full policy with video Hidden soft pin |

The PR summary uses the following policy groups:

| Group | Case |
| --- | --- |
| Baseline best | `original_baseline_10s` |
| Policy-Budget | `hidden_budget_0p08_h10s_kv10s` |
| Policy-Lease | `hidden_budget_0p08_h1s_kv10s` |
| Policy-Full | `soft_pin_budget_0p08_h10s_soft20s_kv10s` |
| Policy-Fallback | `hidden_budget_1p0_h10s_kv10s` |

## 6. Three-Run Average Results

This section records the summarized three-run average used by this PR. Raw
benchmark result files are intentionally not committed to keep the upstream PR
focused. The complete raw benchmark outputs and supporting experimental
materials will be provided as part of the contest report submission.

### 6.1 PR Summary Groups

| Group | KV hit rate | Hidden hit rate | Saved compute ms |
| --- | --- | --- | ---: |
| Baseline best, unified TTL=10s | 83,592.67 / 113,747 = 73.49% | 78.00 / 293 = 26.62% | 152,198.10 |
| Policy-Budget, budget=0.08, Hidden/KV TTL=10s | 83,412.00 / 113,747 = 73.33% | 138.00 / 293 = 47.10% | 168,635.00 |
| Policy-Lease, budget=0.08, Hidden TTL=1s, KV TTL=10s | 83,404.67 / 113,747 = 73.32% | 138.00 / 293 = 47.10% | 168,624.52 |
| Policy-Full, budget=0.08, Hidden TTL=10s, Soft Pin TTL=20s, KV TTL=10s | 83,724.33 / 113,747 = 73.61% | 136.67 / 293 = 46.64% | 180,909.52 |
| Policy-Fallback, budget=1.0, Hidden/KV TTL=10s | 83,435.33 / 113,747 = 73.35% | 77.33 / 293 = 26.39% | 149,991.67 |

Policy-Full keeps the KV hit rate nearly unchanged while improving the Hidden
hit rate from 26.62% to 46.64%. With soft pin enabled, the video Hidden hit
rate is 14 / 18 = 77.78%.

### 6.2 Native Unified TTL Baseline

| Case | KV hit rate | Hidden hit rate | Image Hidden hit rate | Video Hidden hit rate | Saved compute ms |
| --- | --- | --- | --- | --- | ---: |
| `original_baseline_500ms` | 83,245.67 / 113,747 = 73.18% | 79.00 / 293 = 26.96% | 72.67 / 275 = 26.42% | 6.33 / 18 = 35.19% | 151,289.05 |
| `original_baseline_1s` | 83,161.00 / 113,747 = 73.11% | 79.67 / 293 = 27.19% | 73.00 / 275 = 26.55% | 6.67 / 18 = 37.04% | 151,006.43 |
| `original_baseline_5s` | 83,200.00 / 113,747 = 73.14% | 77.00 / 293 = 26.28% | 70.33 / 275 = 25.58% | 6.67 / 18 = 37.04% | 151,075.48 |
| `original_baseline_10s` | 83,592.67 / 113,747 = 73.49% | 78.00 / 293 = 26.62% | 71.00 / 275 = 25.82% | 7.00 / 18 = 38.89% | 152,198.10 |

The best baseline by KV hit rate and saved compute estimate is
`original_baseline_10s`, so it is used as the PR baseline.

### 6.3 Hidden Budget Sweep

All cases in this sweep use Hidden TTL=10s, KV TTL=10s, and soft pin disabled.

| Hidden budget | KV hit rate | Hidden hit rate | Video Hidden hit rate | Saved compute ms |
| --- | --- | --- | --- | ---: |
| 0.04 | 83,592.33 / 113,747 = 73.49% | 69.00 / 293 = 23.55% | 2.00 / 18 = 11.11% | 141,202.62 |
| 0.06 | 83,485.67 / 113,747 = 73.40% | 101.00 / 293 = 34.47% | 7.00 / 18 = 38.89% | 157,965.24 |
| 0.08 | 83,412.00 / 113,747 = 73.33% | 138.00 / 293 = 47.10% | 8.00 / 18 = 44.44% | 168,635.00 |
| 0.10 | 76,063.00 / 113,747 = 66.87% | 180.00 / 293 = 61.43% | 10.00 / 18 = 55.56% | 174,841.43 |
| 0.12 | 75,941.67 / 113,747 = 66.76% | 267.00 / 293 = 91.13% | 15.00 / 18 = 83.33% | 206,718.10 |

Increasing the Hidden budget improves the Hidden hit rate, but budgets 0.10 and
0.12 significantly reduce the KV hit rate. Budget 0.08 is the best balanced
point in this benchmark: it raises the Hidden hit rate to 47.10% while keeping
the KV hit rate close to the baseline.

### 6.4 Hidden Lease TTL Sweep

All cases in this sweep use Hidden budget=0.08, KV TTL=10s, and soft pin
disabled.

| Hidden TTL | KV hit rate | Hidden hit rate | Video Hidden hit rate | Saved compute ms |
| --- | --- | --- | --- | ---: |
| 500ms | 83,403.33 / 113,747 = 73.32% | 138.00 / 293 = 47.10% | 8.00 / 18 = 44.44% | 168,622.62 |
| 1s | 83,404.67 / 113,747 = 73.32% | 138.00 / 293 = 47.10% | 8.00 / 18 = 44.44% | 168,624.52 |
| 5s | 83,537.67 / 113,747 = 73.44% | 138.00 / 293 = 47.10% | 8.00 / 18 = 44.44% | 168,814.52 |
| 10s | 83,412.00 / 113,747 = 73.33% | 138.00 / 293 = 47.10% | 8.00 / 18 = 44.44% | 168,635.00 |

Hidden lease TTL has limited impact in this high-pressure benchmark because
capacity-driven eviction dominates.

### 6.5 Soft Pin

| Case | Soft-pinned Hidden objects | KV hit rate | Hidden hit rate | Video Hidden hit rate | Saved compute ms |
| --- | ---: | --- | --- | --- | ---: |
| Policy-Budget, no soft pin | 0 | 83,412.00 / 113,747 = 73.33% | 138.00 / 293 = 47.10% | 8.00 / 18 = 44.44% | 168,635.00 |
| Policy-Full, video Hidden soft pin | 18 | 83,724.33 / 113,747 = 73.61% | 136.67 / 293 = 46.64% | 14.00 / 18 = 77.78% | 180,909.52 |

Soft pin improves video Hidden retention in this workload while keeping the KV
hit rate nearly unchanged. Soft pin is weak protection and is not a permanent
eviction exemption.

### 6.6 Fallback Behavior

| Case | KV hit rate | Hidden hit rate | Video Hidden hit rate | Saved compute ms |
| --- | --- | --- | --- | ---: |
| Baseline best | 83,592.67 / 113,747 = 73.49% | 78.00 / 293 = 26.62% | 7.00 / 18 = 38.89% | 152,198.10 |
| Policy-Fallback, budget=1.0 | 83,435.33 / 113,747 = 73.35% | 77.33 / 293 = 26.39% | 6.00 / 18 = 33.33% | 149,991.67 |

With budget=1.0, the policy falls back close to the native baseline behavior.
The small difference is treated as benchmark noise from eviction ordering.

## 7. Recommended PR Configuration

For this benchmark workload, the balanced configuration is:

| Parameter | Value |
| --- | --- |
| `enable_hidden_type_aware_eviction` | `true` |
| Hidden logical budget | `0.08` |
| Hidden TTL | `10s` |
| KV TTL | `10s` |
| Video Hidden soft pin TTL | `20s` |

This configuration is represented by
`soft_pin_budget_0p08_h10s_soft20s_kv10s`.

## 8. Limitations

- Hidden recognition depends on writers setting
  `ReplicateConfig.data_type = HIDDEN_STATE`.
- Hidden budget eviction currently targets object count rather than exact
  bytes.
- TTL has limited impact in sustained high-pressure, capacity-driven eviction.
- Soft pin is weak protection, not permanent eviction exemption.
- `Saved compute ms` is a model-based estimate for relative comparison only.
  It is not measured end-to-end latency.
- Multi-replica, NoF, disk offload, and snapshot-restore combinations still
  need broader follow-up coverage.

## 9. Reproduction

From a Mooncake development container:

```bash
cd /workspace/mooncake
export PYTHONPATH=/workspace/mooncake/mooncake-wheel${PYTHONPATH:+:$PYTHONPATH}
export LD_LIBRARY_PATH=/workspace/mooncake/build-docker/mooncake-common${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}

cd /workspace/mooncake/benchmarks/eviction_policy_benchmark
BENCH_SCALE=0.0025 \
OUT_DIR=/tmp/mooncake_eviction_policy_benchmark_results \
  bash run_final_benchmark.sh
```

Use `RUN_CASE_FILTER=<case_name>` to reproduce a single benchmark case.
