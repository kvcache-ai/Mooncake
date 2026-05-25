# Mooncake Store Batch Fast Path Benchmark

This document describes how to evaluate the batch metadata fast path
optimization for Mooncake Store.

## Goal

The optimization targets the control-plane overhead of the following
operations:

- `BatchExistKey`
- `BatchGetReplicaList`
- `BatchPutEnd`
- `BatchPutRevoke`
- client-side `put_batch` request preparation

The expected benefit is lower per-request metadata overhead, better throughput,
and lower P50/P99 latency under small-to-medium batch sizes.

## What Changed

Compared with the baseline implementation, the optimized branch introduces:

- shard-grouped batch processing in `MasterService`
- direct batch RPC dispatch in `WrappedMasterService`
- reduced temporary container reshuffling in `RealClient::put_batch_internal`

These changes preserve the public API and keep request ordering unchanged.

## Recommended Environment

- OS: Ubuntu 22.04 or newer
- Compiler: GCC 10+ or Clang 14+
- CMake: 3.20+
- CPU cores: 16+ recommended
- Memory: 32 GB+

For consistency, run baseline and optimized builds on the same machine and
with the same build flags.

## Build

From the repository root:

```bash
bash dependencies.sh
mkdir -p build
cd build
cmake ..
make -j"$(nproc)"
```

## Functional Validation

Run the tests most relevant to this optimization first:

```bash
cd build
ctest --output-on-failure -R "master_service_test|master_metrics_test|batch_remove_test|client_integration_test"
```

Recommended additional validation:

```bash
ctest --output-on-failure -R "client_tcp_local_memcpy_test|client_metrics_test"
```

## Benchmark 1: Master Metadata Throughput

Use `master_bench` to measure metadata-path throughput.

### BatchPut

```bash
./mooncake-store/benchmarks/master_bench \
  --master_server=127.0.0.1:50051 \
  --operation=BatchPut \
  --num_clients=4 \
  --num_threads=4 \
  --batch_size=32 \
  --value_size=4096 \
  --duration=60
```

### BatchGet

```bash
./mooncake-store/benchmarks/master_bench \
  --master_server=127.0.0.1:50051 \
  --operation=BatchGet \
  --num_clients=4 \
  --num_threads=4 \
  --batch_size=32 \
  --num_keys=100000 \
  --duration=60
```

### Suggested Sweep

Run each case with:

- `batch_size`: `1, 8, 32, 128`
- `num_threads`: `1, 4, 8`
- `value_size`: `4096, 16384`

Record:

- throughput (ops/s)
- objects/s
- P50 latency
- P99 latency

## Benchmark 2: Batch Remove API

Use the existing Python benchmark for `BatchRemove`.

```bash
python3 mooncake-store/benchmarks/batch_remove_benchmark.py \
  --mode benchmark \
  --key-counts 10 100 1000 \
  --iterations 5
```

Record:

- sequential remove time
- batch remove time
- speedup

## Benchmark 3: End-to-End Client Batch Query

Use the client path that exercises `BatchGetReplicaList` through the RPC
wrapper. Measure:

- `BatchQuery` throughput
- P50/P99 query latency
- error rate under mixed ready/missing keys

If you have an internal harness, use it. Otherwise, the recommended minimum is
to collect:

- 100% hit workload
- mixed hit/miss workload
- in-flight processing workload

## Reporting Template

Include a table like this in `EVALUATION.md`:

| Workload | Batch Size | Baseline Throughput | Optimized Throughput | Gain |
|----------|------------|---------------------|----------------------|------|
| BatchGetReplicaList | 8 | X | Y | Z% |
| BatchGetReplicaList | 32 | X | Y | Z% |
| BatchPutEnd | 32 | X | Y | Z% |
| BatchExistKey | 128 | X | Y | Z% |
| BatchRemove | 1000 keys | X | Y | Z% |

Also include latency tables:

| Workload | Batch Size | Baseline P50 | Optimized P50 | Baseline P99 | Optimized P99 |
|----------|------------|--------------|---------------|--------------|---------------|

## Notes

- Warm up the service before recording results.
- Run each benchmark at least 3 times and report the mean.
- Keep background load stable across baseline and optimized runs.
- If you use SSD offload or promotion-on-hit, report that configuration
  explicitly because it changes metadata behavior.
