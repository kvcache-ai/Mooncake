# SGLang Disaggregated Serving with MooncakeTransferEngine

## Overview

SGLang uses Mooncake's Transfer Engine to enable disaggregated prefill-decode (PD) serving across nodes via RDMA, with support for EP and EPD backends. This integration is based on [PR 4654](https://github.com/sgl-project/sglang/pull/4654) and [PR 4880](https://github.com/sgl-project/sglang/pull/4880).

In benchmarks, PD disaggregation with Mooncake achieves **~30% lower ITL** while maintaining comparable throughput ([details](../../performance/sglang-benchmark-results-v1)).

```
  +-----------+   Transfer Engine (RDMA)    +-----------+
  | SGLang    | ◄━━━━━━━━━━━━━━━━━━━━━━► | SGLang     |
  | Prefill   |     KV cache blocks        | Decode     |
  +-----------+                            +-----------+
```

## Prerequisites

```bash
pip3 install mooncake-transfer-engine
```

If you encounter missing `lib*.so` errors, uninstall and build from source instead:

```bash
pip3 uninstall mooncake-transfer-engine
# Build from source — see build instructions
```

### Install SGLang

It is recommended to use uv for faster installation:

```bash
pip install --upgrade pip
pip install uv
uv pip install sglang
```

The major version of Cuda is 13 by default. To install sglang under Cuda 12 with pip or uv, please try the following commands:

```bash
pip install --upgrade pip
pip install uv
uv pip install sglang
uv pip install --force-reinstall  torch==2.11.0 torchaudio==2.11.0 torchvision --index-url https://download.pytorch.org/whl/cu129
uv pip install --force-reinstall sglang-kernel --index-url https://docs.sglang.ai/whl/cu129/
uv pip install --force-reinstall sgl-deep-gemm --index-url https://docs.sglang.ai/whl/cu129/ --no-deps
```

See the [SGLang official compilation guide](https://docs.sglang.ai/start/install.html) if you encounter issues.

## Configuration

Key arguments:

| Argument | Description |
|----------|-------------|
| `--disaggregation-mode` | `prefill` or `decode` — node role |
| `--disaggregation-ib-device` | RDMA device(s). Auto-detected, comma-separated for multi-NIC |
| `--tp-size` | Tensor parallelism size (optional) |
| `--base-gpu-id` | Starting GPU index for same-node deployments |
| `--host` / `--port` | SGLang service address |

## Run PD Disaggregation

### Multi-Node

```bash
# Prefill node (192.168.0.137)
python -m sglang.launch_server \
  --model-path Qwen/Qwen2.5-7B-Instruct-GPTQ-Int4 \
  --disaggregation-mode prefill \
  --port 30000 --host 192.168.0.137 --tp-size 2

# Decode node (192.168.0.140)
python -m sglang.launch_server \
  --model-path Qwen/Qwen2.5-7B-Instruct-GPTQ-Int4 \
  --disaggregation-mode decode \
  --port 30001 --host 192.168.0.140 --tp-size 2

# Router
python3 -m sglang_router.launch_router \
  --pd-disaggregation \
  --prefill "http://192.168.0.137:30000" 8998 \
  --decode  "http://192.168.0.140:30001" \
  --policy round_robin \
  --host 0.0.0.0 --port 8000
```

Test:

```bash
curl -X POST http://127.0.0.1:8000/generate \
  -H "Content-Type: application/json" \
  -d '{"text": "Tell me a long story", "sampling_params": {"temperature": 0}}'
```

### Single-Node

```bash
# Prefill
python -m sglang.launch_server \
  --model-path Qwen/Qwen2.5-7B-Instruct-GPTQ-Int4 \
  --disaggregation-mode prefill \
  --port 30000 --host 192.168.0.137 --tp-size 2

# Decode (skip first 2 GPUs, use cards 2+)
python -m sglang.launch_server \
  --model-path Qwen/Qwen2.5-7B-Instruct-GPTQ-Int4 \
  --disaggregation-mode decode \
  --port 30001 --base-gpu-id 2 --host 192.168.0.137 --tp-size 2

# Router
python3 -m sglang_router.launch_router \
  --pd-disaggregation \
  --prefill "http://192.168.0.137:30000" 8998 \
  --decode  "http://192.168.0.137:30001" \
  --policy round_robin \
  --host 0.0.0.0 --port 8000

# Router for multi node.
# Here is an example for 2 decode node running on 192.168.0.137 and 192.168.0.140
python3 -m sglang_router.launch_router \
  --pd-disaggregation \
  --prefill "http://192.168.0.137:30000" 8998 \
  --decode "http://192.168.0.137:30001,http://192.168.0.140:30001" \
  --policy round_robin \
  --host 0.0.0.0 --port 8000
```

TP is supported but not required — omit `--tp-size 2` for single-GPU setups.

Multiple decode instances per prefill are supported. Multiple prefills on the same node are not supported due to bootstrap server port conflicts.

```{tip}
If you encounter HuggingFace timeouts, set `export SGLANG_USE_MODELSCOPE=true`.
```

## Advanced Backends

### EP Backend — Expert Parallelism for MoE Models

For Mixture-of-Experts models (e.g., DeepSeek-V3), different nodes hold different expert weights. During inference, Mooncake transfers expert activations between prefill and decode nodes via RDMA, replacing the NCCL-based all-to-all with a faster, disaggregation-aware transfer path.

```bash
# Prefill
python -m sglang.launch_server \
  --model-path deepseek-ai/DeepSeek-V3-0324 \
  --disaggregation-mode prefill \
  --port 30000 --host 192.168.0.137 \
  --tp-size 8 --dp-size 8 \
  --elastic-ep-backend mooncake \
  --moe-a2a-backend mooncake

# Decode
python -m sglang.launch_server \
  --model-path deepseek-ai/DeepSeek-V3-0324 \
  --disaggregation-mode decode \
  --port 30001 --host 192.168.0.140 \
  --tp-size 8 --dp-size 8 \
  --elastic-ep-backend mooncake \
  --moe-a2a-backend mooncake
```

Set `--mooncake-ib-device` to the same value as `--disaggregation-ib-device` if explicit device specification is needed.

### EPD Backend — Encoder-Prefill-Decode for Multimodal Models

For multimodal models (e.g., LLaVA, InternVL), the **encoder** (Vision Transformer) processes images, the **prefill** node handles text + encoded visual tokens, and the **decode** node generates output. Mooncake transfers encoder outputs (visual embeddings) between these nodes via RDMA.

The three node roles are:

| Role | Flag | Responsibility |
|------|------|----------------|
| Encoder | `--encoder-only` | Processes images, produces visual embeddings. Mooncake sends embeddings to prefill via `--encoder-transfer-backend mooncake` |
| Prefill | `--disaggregation-mode prefill` | Receives encoder embeddings, runs prefill with text + visual context. Mooncake transfers KV cache to decode |
| Decode | `--disaggregation-mode decode` | Receives KV cache from prefill, generates output tokens |

```bash
# Encoder-only node
python -m sglang.launch_server \
  --model-path $MODEL \
  --encoder-only \
  --encoder-transfer-backend mooncake \
  --port 30002

# Prefill node
python -m sglang.launch_server \
  --model-path $MODEL \
  --disaggregation-mode prefill \
  --disaggregation-transfer-backend mooncake \
  --encoder-transfer-backend mooncake \
  --tp $TP \
  --mem-fraction-static $MEM_FRACTION \
  --chunked-prefill-size $CHUNK_SIZE \
  --language-only \
  --encoder-urls http://127.0.0.1:30002 http://127.0.0.1:30003 \
  --port $PORT

# Decode node
python -m sglang.launch_server \
  --model-path $MODEL \
  --disaggregation-mode decode \
  --disaggregation-transfer-backend mooncake \
  --port $PORT
```
