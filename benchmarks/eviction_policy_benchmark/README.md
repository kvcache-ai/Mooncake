# Hidden State Eviction Policy Benchmark

This benchmark package supports the Mooncake Store Hidden State type-aware
eviction PR. It evaluates object-level eviction behavior under a mixed
Hidden/KV workload and is intended for policy comparison only. It does not
measure end-to-end inference latency.

## Directory Layout

```text
benchmarks/eviction_policy_benchmark/
|-- README.md
|-- EVALUATION.md
|-- proxy_aware_hidden_kv_bench.py
`-- run_final_benchmark.sh
```

## Contents

- `EVALUATION.md`: benchmark report and analysis.
- `proxy_aware_hidden_kv_bench.py`: benchmark driver.
- `run_final_benchmark.sh`: runner for the PR evaluation cases.

Raw benchmark result files are intentionally not committed to keep the upstream
PR focused. The complete raw benchmark outputs and supporting experimental
materials will be provided as part of the contest report submission.

## Fixed Parameters

- `sessions = 240`
- `mixed_write_sessions = 260`
- `scale = 0.0025`
- Store capacity: 64 MiB
- `eviction_high_watermark_ratio = 0.95`
- `eviction_ratio = 0.05`
- Proxy mode: `session`
- Execution mode: `request_eviction`
- Workload fingerprint: `ab69f2dc33841c64`

## Reported Policy Groups

| Group | Case |
| --- | --- |
| Baseline best | `original_baseline_10s` |
| Policy-Budget | `hidden_budget_0p08_h10s_kv10s` |
| Policy-Lease | `hidden_budget_0p08_h1s_kv10s` |
| Policy-Full | `soft_pin_budget_0p08_h10s_soft20s_kv10s` |
| Policy-Fallback | `hidden_budget_1p0_h10s_kv10s` |

## Three-Run Summary

| Case | KV hit rate | Hidden hit rate |
| --- | --- | --- |
| Baseline best | 83,592.67 / 113,747 = 73.49% | 78.00 / 293 = 26.62% |
| Policy-Budget | 83,412.00 / 113,747 = 73.33% | 138.00 / 293 = 47.10% |
| Policy-Lease | 83,404.67 / 113,747 = 73.32% | 138.00 / 293 = 47.10% |
| Policy-Full | 83,724.33 / 113,747 = 73.61% | 136.67 / 293 = 46.64% |
| Policy-Fallback | 83,435.33 / 113,747 = 73.35% | 77.33 / 293 = 26.39% |

Under this benchmark workload, Policy-Full keeps the KV hit rate nearly
unchanged while improving the Hidden hit rate from 26.62% to 46.64%. Its video
Hidden hit rate is 14 / 18 = 77.78%.

## Reproduction

From a Mooncake development container, run:

```bash
cd /workspace/mooncake
export PYTHONPATH=/workspace/mooncake/mooncake-wheel${PYTHONPATH:+:$PYTHONPATH}
export LD_LIBRARY_PATH=/workspace/mooncake/build-docker/mooncake-common${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}

cd /workspace/mooncake/benchmarks/eviction_policy_benchmark
BENCH_SCALE=0.0025 \
OUT_DIR=/tmp/mooncake_eviction_policy_benchmark_results \
  bash run_final_benchmark.sh
```

Use `RUN_CASE_FILTER=<case_name>` to reproduce a single case.

## Metric Notes

- `Saved compute ms` is a model-based estimate for relative comparison between
  policies. It should not be interpreted as absolute end-to-end latency
  improvement.
- The benchmark evaluates Store object-level cache eviction behavior only.
