# Mooncake Store Batch Fast Path Optimization

## Overview

This submission focuses on improving the control-plane performance of Mooncake
Store batch operations. The work keeps public APIs backward compatible while
reducing metadata-path overhead for:

- `put` / `batch put`
- `get` / `batch get`
- `remove` / `batch remove`
- batch metadata queries such as `BatchExistKey` and
  `BatchGetReplicaList`

The implementation is on branch:

```text
feat/part-2-batch-fast-path
```

## Core Contribution

The main idea is to convert API-level batch operations into execution-level
batch operations on the master side.

Key changes:

- group batch keys by metadata shard before processing
- reduce repeated shard lock acquisition and repeated hash lookups
- route wrapped batch RPC handlers directly to batch fast paths
- reduce temporary container reshuffling in client-side `put_batch`

## Files Changed

Code:

- `mooncake-store/include/master_service.h`
- `mooncake-store/src/master_service.cpp`
- `mooncake-store/src/rpc_service.cpp`
- `mooncake-store/src/real_client.cpp`

Tests and benchmark materials:

- `mooncake-store/tests/master_service_test.cpp`
- `docs/source/performance/store-batch-fast-path-benchmark.md`
- `mooncake-store/benchmarks/README_batch_fast_path.md`
- `scripts/prepare_windows_dev_env.ps1`
- `scripts/benchmark_store_batch_fast_path.py`
- `submission/DESIGN.md`
- `submission/EVALUATION.md`

## Build

Recommended environment:

- Ubuntu 22.04+
- GCC 10+
- CMake 3.20+

Build:

```bash
bash dependencies.sh
mkdir -p build
cd build
cmake ..
make -j"$(nproc)"
```

## Test

Targeted validation:

```bash
cd build
ctest --output-on-failure -R "master_service_test|master_metrics_test|batch_remove_test|client_integration_test"
```

Additional Python-side validation:

```bash
bash scripts/run_tests.sh
```

## Benchmark

### Metadata path

```bash
./mooncake-store/benchmarks/master_bench \
  --operation=BatchGet \
  --batch_size=32 \
  --num_clients=4 \
  --num_threads=4 \
  --duration=60
```

```bash
./mooncake-store/benchmarks/master_bench \
  --operation=BatchPut \
  --batch_size=32 \
  --num_clients=4 \
  --num_threads=4 \
  --duration=60
```

### Batch remove

```bash
python3 mooncake-store/benchmarks/batch_remove_benchmark.py \
  --mode benchmark \
  --key-counts 10 100 1000 \
  --iterations 5
```

### Unified JSON benchmark harness

```bash
python3 scripts/benchmark_store_batch_fast_path.py \
  --master-metrics http://127.0.0.1:9003/metrics \
  --put-qps 500 \
  --get-qps 500 \
  --duration-seconds 60 \
  --output benchmark_result.json
```

## Result Summary

This repository now includes:

- optimized source changes
- functional tests
- abnormal-path validation
- benchmark methodology and scripts
- submission-ready design and evaluation documents

Current measured results on the remote Ubuntu server already show:

- `BatchPut` throughput: `67321.33 ops/s -> 67448.00 ops/s` (`+0.19%`)
- `BatchGet` throughput: `105256.00 ops/s -> 137594.67 ops/s` (`+30.72%`)
- Python single-key end-to-end `put/get/remove` latency remains sub-`0.1 ms`
  at P99 for the evaluated workload

More detailed tables and notes are recorded in `submission/EVALUATION.md`.
