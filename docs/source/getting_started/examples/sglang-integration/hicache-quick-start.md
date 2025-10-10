# Quick Start: SGLang HiCache with Mooncake Backend

Follow this streamlined workflow to get SGLang HiCache running with Mooncake as the L3 storage backend.

> Need more background or tuning options? See the [Complete Guide](hicache-integration-v1.md).

## Deployment

Before you begin, make sure that:

- SGLang is installed with HiCache support on the machine hosting your SGLang server. Refer to the [official installation guide](https://docs.sglang.ai/get_started/install.html) if needed.
- Mooncake is installed and accessible as the hierarchical cache backend. Detailed build steps live in the [Mooncake documentation](https://kvcache-ai.github.io/Mooncake/getting_started/build.html).
- The router package is installed to provide the `sglang_router` entrypoint:

```bash
pip install sglang-router
```

### 1. Launch the Mooncake master service

```bash
mooncake_master --enable_http_metadata_server=true
```

### 2. Launch SGLang with Mooncake L3 storage

```bash
MOONCAKE_MASTER=127.0.0.1:50051 python -m sglang.launch_server \
    --model-path [model_path] \
    --page-size 64 \
    --enable-hierarchical-cache \
    --hicache-storage-prefetch-policy timeout \
    --hicache-storage-backend mooncake
```

**Key flag:** `--hicache-storage-prefetch-policy {best_effort,wait_complete,timeout}` determines when prefetching from storage should stop. `timeout` usually offers the best balance when Mooncake is the backend.

## Prefill/Decode Disaggregation

The disaggregated setup runs three processes—prefill worker, decode worker, and router. Launch each command below in its own terminal window.

### Prefill worker

```bash
MOONCAKE_MASTER=127.0.0.1:50051 python -m sglang.launch_server \
    --model-path [model_path] \
    --page-size 64 \
    --enable-hierarchical-cache \
    --hicache-storage-prefetch-policy timeout \
    --hicache-storage-backend mooncake \
    --disaggregation-mode prefill \
    --disaggregation-ib-device "mlx5_1" \
    --base-gpu-id 0 \
    --port 30000
```

### Decode worker

```bash
python -m sglang.launch_server \
    --model-path [model_path] \
    --page-size 64 \
    --disaggregation-mode decode \
    --disaggregation-ib-device "mlx5_1" \
    --base-gpu-id 1 \
    --port 30001
```

### Router

```bash
python -m sglang_router.launch_router \
    --pd-disaggregation \
    --prefill "http://127.0.0.1:30000" \
    --decode "http://127.0.0.1:30001" \
    --host 0.0.0.0 \
    --port 8000
```

### Smoke test

```bash
curl -X POST http://127.0.0.1:8000/generate \
    -H "Content-Type: application/json" \
    -d '{
  "text": "Let me tell you a long story ",
  "sampling_params": {
    "temperature": 0
  }
}'
```

### Quick tips

- `--disaggregation-ib-device` is optional—SGLang autodetects devices, but you can set it explicitly (comma-separated, no spaces) when multiple NICs are available.
- Use `--tp-size` to enable tensor-parallel execution across GPUs if required.
- Optional flags to experiment with once you need them (some still have compatibility gaps):
  - `--disaggregation-decode-enable-offload-kvcache` writes the decode worker's outputs back into Mooncake; enable it when you want decoded KV to persist in L3.
- Launch dedicated Mooncake `store service` nodes when you want to scale L3 capacity beyond what the SGLang servers contribute.
- HuggingFace timeouts can be mitigated with `export SGLANG_USE_MODELSCOPE=true`.
