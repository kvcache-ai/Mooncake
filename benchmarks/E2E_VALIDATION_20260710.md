# E2E SGLang + HiCache + Mooncake Validation - 2026-07-10

## Summary

This run extends the 2026-07-08 C++ correctness validation into a real
SGLang + HiCache + Mooncake Store end-to-end smoke benchmark.

The full 7B benchmark did not run because the local Hugging Face snapshot for
`Qwen2.5-7B-Instruct` contains tokenizer/config files but no actual weight
shards. A smaller complete local model, `Qwen2.5-0.5B`, was used to validate
the integration path.

## Remote Environment

| Item | Value |
|------|-------|
| Server | A40 GPU server |
| Hostname | `<gpu-hostname>` |
| GPU | 8 x NVIDIA A40 visible |
| Source tree | `/tmp/mooncake_competition_codex` |
| Python env | `/tmp/mooncake-e2e-bfcl` |
| Base env | Cloned from `${BASE_CONDA_ENV}` |
| Python | 3.10.20 |
| SGLang | 0.5.15 |
| Torch | 2.11.0+cu130 |
| Mooncake wheel | `mooncake-transfer-engine==0.3.10` |
| ABI build dir | `/tmp/mooncake_competition_codex/build-py310-abi` |
| Result dir | `/tmp/mooncake_competition_codex/benchmarks/e2e_results_20260710_py310_0p5b` |
| Cleanup-fix rerun dir | `/tmp/mooncake_competition_codex/benchmarks/e2e_results_20260710_cleanupfix_0p5b` |

## Environment Preparation

```bash
${CONDA_ROOT}/bin/conda create \
  --clone ${BASE_CONDA_ENV} \
  -p /tmp/mooncake-e2e-bfcl \
  -y

source ${CONDA_ROOT}/etc/profile.d/conda.sh
conda activate /tmp/mooncake-e2e-bfcl

export PIP_CACHE_DIR=/tmp/pip-cache-mooncake
pip install 'sglang[all]'
```

SGLang installation completed in the isolated env and did not modify the
original `BFCL` env.

## Mooncake Build And Wheel

The first wheel attempt exposed an ABI trap: the existing `build/` directory
used system Python 3.12, which produced `engine.cpython-312...so`. The E2E
env is Python 3.10, so the wheel must be built from a Python 3.10 CMake tree.

```bash
cd /tmp/mooncake_competition_codex

cmake -S . -B build-py310-abi \
  -DCMAKE_BUILD_TYPE=Release \
  -DPython_EXECUTABLE=/tmp/mooncake-e2e-bfcl/bin/python \
  -DPython3_EXECUTABLE=/tmp/mooncake-e2e-bfcl/bin/python \
  -DPYBIND11_FINDPYTHON=ON

cmake --build build-py310-abi \
  --target engine store mooncake_master mooncake_client transfer_engine_bench \
  -j$(nproc)

BUILD_DIR=build-py310-abi ./scripts/build_wheel.sh 3.10 dist-py310-e2e

pip install --force-reinstall \
  mooncake-wheel/dist-py310-e2e/mooncake_transfer_engine-0.3.10-cp310-cp310-manylinux_2_39_x86_64.whl
```

Wheel artifact:

```text
/tmp/mooncake_competition_codex/mooncake-wheel/dist-py310-e2e/mooncake_transfer_engine-0.3.10-cp310-cp310-manylinux_2_39_x86_64.whl
```

## Smoke Gates

| Gate | Result |
|------|--------|
| `import sglang` | Pass |
| `import torch` and CUDA available | Pass |
| `import mooncake.engine` | Pass |
| `import mooncake.store` | Pass |
| `which mooncake_master` | `/tmp/mooncake-e2e-bfcl/bin/mooncake_master` |
| `which transfer_engine_bench` | `/tmp/mooncake-e2e-bfcl/bin/transfer_engine_bench` |
| `mooncake_master` RPC/metadata/metrics smoke | Pass |
| SGLang launch flags include HiCache Mooncake backend | Pass |

Mooncake master smoke confirmed listening on RPC, metadata, and metrics ports.
The bare `/metadata` endpoint returns HTTP 400 without a `key` parameter, but
the service is up and `/metrics` returns Prometheus metrics.

## 7B Model Blocker

The intended model path was:

```text
${HF_CACHE}/models--Qwen--Qwen2.5-7B-Instruct/snapshots/a09a35458c702b33eeacc393d103063234e8bc28
```

SGLang reached Mooncake initialization but stopped at model loading because no
weight shards were present in that snapshot. The directory contains files such
as `config.json`, `tokenizer.json`, and `model.safetensors.index.json`, but no
`*.safetensors` shards.

Observed error:

```text
RuntimeError: Cannot find any model weights
```

## E2E Smoke Run

The completed smoke run used the local complete model:

```text
${HF_CACHE}/models--Qwen--Qwen2.5-0.5B/snapshots/060db6499f32faf8b98477b0a26969ef7d8b9987
```

Command:

```bash
cd /tmp/mooncake_competition_codex
source ${CONDA_ROOT}/etc/profile.d/conda.sh
conda activate /tmp/mooncake-e2e-bfcl

export CUDA_VISIBLE_DEVICES=0
export PYTHONUNBUFFERED=1

./benchmarks/e2e_hicache_benchmark.sh \
  ${HF_CACHE}/models--Qwen--Qwen2.5-0.5B/snapshots/060db6499f32faf8b98477b0a26969ef7d8b9987 \
  benchmarks/e2e_results_20260710_py310_0p5b
```

Key SGLang log evidence:

- Transfer Engine initialized and discovered RDMA device `mlx5_0`.
- KV cache was allocated on GPU.
- SGLang allocated 73.61 GB host memory for hierarchical KV cache.
- Mooncake storage backend was created.
- Mooncake store setup and warmup completed successfully.
- `HiRadixCache` was initialized with `hierarchical=True`.
- Uvicorn served OpenAI-compatible requests on `127.0.0.1:30000`.

## Results

| Workload | Requests | Avg Latency | P50 | P99 | Throughput |
|----------|----------|-------------|-----|-----|------------|
| multi_turn_conversation | 20 | 1197.7 ms | 101.3 ms | 14522.8 ms | 286.0 tok/s |
| prefix_sharing | 20 | 224.6 ms | 147.9 ms | 389.1 ms | N/A |
| cache_miss_heavy | 20 | 94.5 ms | 94.8 ms | 95.2 ms | N/A |

The result JSON files are:

```text
benchmarks/e2e_results_20260710_py310_0p5b/bench_multiturn.json
benchmarks/e2e_results_20260710_py310_0p5b/bench_prefix_sharing.json
benchmarks/e2e_results_20260710_py310_0p5b/bench_cache_miss.json
benchmarks/e2e_results_20260710_py310_0p5b/REPORT.md
```

## Cleanup-Fix Rerun

After the initial smoke run, the E2E script was hardened in two ways:

- It resolves and launches the direct `mooncake_master` binary when available,
  avoiding the Python console wrapper that can leave a child process behind.
- It starts Mooncake Master and SGLang in their own process groups and cleans
  up the whole group on exit.

The verification rerun used:

```bash
cd /tmp/mooncake_competition_codex
source ${CONDA_ROOT}/etc/profile.d/conda.sh
conda activate /tmp/mooncake-e2e-bfcl

export CUDA_VISIBLE_DEVICES=0
export PYTHONUNBUFFERED=1

./benchmarks/e2e_hicache_benchmark.sh \
  ${HF_CACHE}/models--Qwen--Qwen2.5-0.5B/snapshots/060db6499f32faf8b98477b0a26969ef7d8b9987 \
  benchmarks/e2e_results_20260710_cleanupfix_0p5b
```

Key run evidence:

- Script resolved `Mooncake Master binary: ./build/mooncake-store/src/mooncake_master`.
- Inline result summaries printed successfully instead of `parsing failed`.
- Result files were generated under
  `benchmarks/e2e_results_20260710_cleanupfix_0p5b/`.
- Post-run cleanup check found no `mooncake_master`, no SGLang server process,
  no listeners on ports `50051`, `8080`, `9003`, or `30000`, and all eight A40
  GPUs reported `0 MiB` memory used.

Cleanup-fix rerun results:

| Workload | Requests | Avg Latency | P50 | P99 | Throughput |
|----------|----------|-------------|-----|-----|------------|
| multi_turn_conversation | 20 | 102.7 ms | 100.9 ms | 192.4 ms | 307.6 tok/s |
| prefix_sharing | 20 | 137.0 ms | 136.2 ms | 146.7 ms | N/A |
| cache_miss_heavy | 20 | 94.8 ms | 94.7 ms | 100.4 ms | N/A |

Cleanup-fix Mooncake metrics still confirmed real backend traffic:

```text
Mem Storage: 27.75 MB / 4.00 GB (0.7%)
Keys: 75
Clients: 1
PutStart=1/1 single + batch Req=36/36 Item=74/74
PutEnd=1/1 single + batch Req=36/36 Item=74/74
ExistKey=1/1 single + batch Req=36/36 Item=74/74
Ping=8/8
FetchTasks=8/8
```

## Mooncake Metrics

At the end of the smoke run, Mooncake metrics confirmed real backend traffic:

| Metric | Value |
|--------|-------|
| Memory capacity | 4.00 GB |
| Allocated memory | 27.75 MB |
| Managed keys | 75 |
| Active clients | 1 |
| `PutStart` | 1/1 single + 36/36 batch |
| `PutEnd` | 1/1 single + 36/36 batch |
| `GetReplicaList` | 1/1 |
| `ExistKey` | 1/1 single + 36/36 batch |
| Batch items written | 74/74 |
| `Ping` | 30/30 |
| `FetchTasks` | 30/30 |

## Notes

- This is an integration smoke result, not a final performance comparison.
  Final competition numbers still need a comparable baseline and optimized run
  on the same complete 7B or larger model.
- The initial smoke run exposed an inline summary parsing bug. The E2E script
  now uses safe `.format(...)` rendering for those summaries, and the
  cleanup-fix rerun verified the live log output.
- The E2E script now prefers the direct `mooncake_master` binary and performs
  process-group cleanup for both Mooncake Master and SGLang.
- `scripts/build_wheel.sh` now honors `BUILD_DIR` for artifact copying and
  installs `patchelf`, which auditwheel needs for repaired wheel generation.
