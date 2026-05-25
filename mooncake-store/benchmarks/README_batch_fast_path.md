# Batch Fast Path Evaluation Guide

This note is intended for the `feat/part-2-batch-fast-path` optimization work.

## Scope

The current optimization focuses on metadata-heavy batch paths:

- `BatchExistKey`
- `BatchGetReplicaList`
- `BatchPutEnd`
- `BatchPutRevoke`
- client-side batch put preparation

## Build and Test Prerequisites

Mooncake Store C++ tests and benchmarks should be run on Linux or a healthy
WSL2 environment.

### Windows host helpers

On Windows, you can prepare the local developer tools with:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\prepare_windows_dev_env.ps1
```

This installs user-local `cmake` and `ninja`, and checks WSL status.

### Linux / WSL build

```bash
bash dependencies.sh
mkdir -p build
cd build
cmake ..
make -j"$(nproc)"
```

## Targeted Tests

Run the tests most relevant to this optimization:

```bash
cd build
ctest --output-on-failure -R "master_service_test|master_metrics_test|batch_remove_test|client_integration_test"
```

## Benchmarks

### Metadata path benchmark

```bash
./mooncake-store/benchmarks/master_bench --operation=BatchGet --batch_size=32 --num_clients=4 --num_threads=4 --duration=60
./mooncake-store/benchmarks/master_bench --operation=BatchPut --batch_size=32 --num_clients=4 --num_threads=4 --duration=60
```

### Batch remove benchmark

```bash
python3 mooncake-store/benchmarks/batch_remove_benchmark.py --mode benchmark --key-counts 10 100 1000 --iterations 5
```

## Recommended Result Collection

Collect the following for both `main` and the optimized branch:

- throughput
- P50 latency
- P99 latency
- failure rate
- batch size
- number of clients
- number of threads
- value size

Keep all hardware and runtime settings unchanged between baseline and optimized
runs.
