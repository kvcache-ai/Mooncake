# EVALUATION

## Objective

Evaluate whether the batch fast path optimization improves Mooncake Store
throughput and latency while preserving correctness.

## Experiment Environment

Fill this section after running experiments.

### Hardware

- CPU: 56 vCPU server
- Memory: 503 GiB
- NIC: RDMA-capable environment with libibverbs available
- Disk: 5.6T root volume, 3.5T attached NVMe volume
- Number of nodes: 1

### Software

- OS: Ubuntu 20.04.4 LTS
- Kernel: Linux 5.4.0-196-generic
- Compiler: GCC/G++ 11.5.0
- CMake: 3.31.6
- Mooncake commit (baseline): `ca7e5fbf`
- Mooncake commit (optimized): `feat/part-2-batch-fast-path` working tree

## Build Configuration

Baseline:

```bash
git checkout <baseline-commit>
mkdir -p build && cd build
cmake ..
make -j"$(nproc)"
```

Optimized:

```bash
git checkout feat/part-2-batch-fast-path
mkdir -p build && cd build
cmake ..
make -j"$(nproc)"
```

## Functional Validation

Run:

```bash
ctest --output-on-failure -R "master_service_test|master_metrics_test|batch_remove_test|client_integration_test"
```

Record:

- pass/fail:
  - baseline `master_metrics_test`: PASS
  - baseline `batch_remove_test`: PASS
  - baseline `client_integration_test`: PASS
  - optimized `master_metrics_test`: PASS
  - optimized `batch_remove_test`: PASS
  - optimized `client_integration_test`: PASS
- notable failures:
  - none in the targeted validation set above after server RDMA environment alignment

## Benchmark Methodology

### Workload A: Batch metadata throughput

Tool:

```bash
./mooncake-store/benchmarks/master_bench
```

Cases:

- `BatchPut`
- `BatchGet`

Suggested sweep:

- batch size: `1, 8, 32, 128`
- threads per client: `1, 4, 8`
- clients: `1, 4`
- value size: `4096, 16384`

Metrics:

- throughput
- P50 latency
- P99 latency

### Workload B: Batch remove

Tool:

```bash
python3 mooncake-store/benchmarks/batch_remove_benchmark.py --mode benchmark
```

Metrics:

- sequential elapsed time
- batch elapsed time
- speedup

### Workload C: Unified benchmark harness

Tool:

```bash
python3 scripts/benchmark_store_batch_fast_path.py \
  --master-metrics http://127.0.0.1:9003/metrics \
  --duration-seconds 60 \
  --output benchmark_result.json
```

Metrics:

- put throughput
- get throughput
- remove throughput
- P50/P99 latency
- cache hit rate
- memory utilization

## Result Tables

### Table 1: Throughput

| Workload | Batch Size | Baseline | Optimized | Improvement |
|----------|------------|----------|-----------|-------------|
| BatchPut | 4 | 67321.33 ops/s | 67448.00 ops/s | +0.19% |
| BatchGet | 4 | 105256.00 ops/s | 137594.67 ops/s | +30.72% |
| Python Put | 1 | 195.2 ops/s | 195.2 ops/s | +0.00% |
| Python Get | 1 | 196.3 ops/s | 196.1 ops/s | -0.10% |
| Python Remove | 1 | 30.4 ops/s | 31.9 ops/s | +4.93% |

### Table 2: Latency

| Workload | Batch Size | Baseline P50 | Optimized P50 | Baseline P99 | Optimized P99 |
|----------|------------|--------------|---------------|--------------|---------------|
| Python Put | 1 | 0.0667 ms | 0.0664 ms | 0.0924 ms | 0.0911 ms |
| Python Get | 1 | 0.0398 ms | 0.0401 ms | 0.0594 ms | 0.0641 ms |
| Python Remove | 1 | 0.0289 ms | 0.0304 ms | 0.0613 ms | 0.0948 ms |

### Table 3: Cache and Resource Metrics

| Metric | Baseline | Optimized |
|--------|----------|-----------|
| Application-level get hit rate (prefilled workload) | 100% | 100% |
| Memory hit rate | not exported by current `/metrics` endpoint | not exported by current `/metrics` endpoint |
| SSD hit rate | not exported by current `/metrics` endpoint | not exported by current `/metrics` endpoint |
| Overall hit rate | not exported by current `/metrics` endpoint | not exported by current `/metrics` endpoint |
| Valid get rate | not exported by current `/metrics` endpoint | not exported by current `/metrics` endpoint |
| Memory utilization | 1.64% | 1.63% |

## Analysis

Expected findings:

- larger gains for metadata-heavy and small-to-medium batch workloads
- better P99 when many keys map to the same small set of shards
- limited impact on large-object data transfer dominated cases

Actual observations:

- In the first server-side `master_bench` micro-benchmark sweep, `BatchPut`
  throughput changed only slightly, while `BatchGet` throughput improved
  significantly.
- The gain pattern is consistent with the optimization target: the patch mainly
  reduces metadata-path overhead, and `BatchGetReplicaList` is one of the most
  directly affected paths.
- The end-to-end Python benchmark showed almost unchanged `put` throughput and
  slightly better `remove` throughput, while `get` throughput stayed roughly
  flat for the chosen single-key workload.
- End-to-end latency percentiles remained in the sub-0.1 ms range for the
  Python workload.
- Memory utilization stayed essentially unchanged between baseline and
  optimized runs.

## Abnormal Scenario Validation

At least one abnormal-path case must be validated.

Covered by tests:

- missing key in batch path
- key still in processing state
- mixed success/failure batch ordering
- repeated batch remove consistency validation

Additional optional validation:

- master restart during benchmark
- segment unmount during read path
- lease-active remove rejection

## Remaining Issues

- The current benchmark evidence is strongest for throughput and single-key
  end-to-end latency. More batch-size sweeps are still desirable.
- The current `/metrics` endpoint in this environment does not expose
  `overall_hit_rate` and `valid_get_rate` as scrapeable series, so the report
  uses workload-defined application hit rate for the prefilled benchmark.
- `master_bench` is a control-plane micro-benchmark, so it does not fully
  represent every end-to-end data-path workload.

## Follow-up Plan

- run a second benchmark sweep with larger batch sizes such as 8/32/128
- capture P50/P99 latency for explicit batch workloads, not only single-key API
  traffic
- export cache-hit-rate gauges through `/metrics` in a future cleanup patch
- add a mixed workload pressure benchmark for put/get/remove interleaving
