# Mooncake EPD Evaluation

This report summarizes the public, reproducible evaluation of Mooncake EPD.
The committed evidence snapshot contains the claim scope, metrics, and source
digests required for review.

## Evaluation goals

The public evaluation matrix covers the following dimensions:

1. Validate real Encoder-Prefill-Decode serving with a Qwen-VL model.
2. Verify that Vision Hidden State can bypass repeated Prefill-side encoding.
3. Verify grouped, layered Prefill-to-Decode KV transfer on the vLLM hot path.
4. Measure scale-out request and output-token throughput against a colocated
   vLLM baseline.
5. Measure Agent KV state cloning, page-reference sharing, Copy-on-Write, and
   release cleanup.
6. Gate all published results on real services, direct-path counters, HTTP
   success, transfer integrity, and explicit resource metadata.

## Test environment

The published measurements were collected on one server with the following
configuration:

| Component | Configuration |
| --- | --- |
| GPU | 8 x NVIDIA RTX A6000, 49,140 MiB each |
| CPU | 2 x Intel Xeon Gold 6226R, 64 logical CPUs |
| Host memory | 503 GiB |
| Python | 3.10.12 |
| PyTorch | 2.11.0 |
| vLLM | 0.23.0 |
| Transformers | 5.12.1 |
| CUDA runtime | 13.0 |
| NVIDIA driver | 580.159.03 |
| Mooncake Python package | 0.3.11.post1 |
| Multimodal model | Qwen3-VL-8B-Instruct |

The same host provided a validated TCP direct path and an intra-node
`nvlink_intra` topology. RDMA is not required to reproduce the published
correctness results.

## Workloads and baselines

### W0: real multimodal EPD serving

| Field | Value |
| --- | --- |
| Topology | 1 Encoder GPU + 1 Prefill GPU + 2 Decode GPUs |
| Input | Qwen-VL multimodal request |
| Transport | Mooncake direct peer-buffer over TCP |
| Warmup / measured | 8 / 16 requests |
| Concurrency | 8 |
| Maximum output | 128 tokens |
| Validation | Strict real-EPD and no-fallback gates |

This workload verifies the full E-to-P-to-D data flow, vLLM Vision Encoder
skip, layered KV handoff, streaming responses, and resource cleanup.

### W1: colocated baseline and 2P2D scale-out

| Field | Colocated baseline | EPD scale-out |
| --- | --- | --- |
| GPU topology | 1 GPU | 2 Prefill + 2 Decode GPUs |
| Input | Same synthetic long prompt and request fingerprint | Same |
| Prompt length | Approximately 1,970 tokens | Same |
| Request variation | Unique prefix | Same |
| Warmup / measured | 8 / 16 requests | Same |
| Concurrency | 8 | Same |
| Maximum output | 32 tokens | Same |
| Generation config | vLLM | vLLM |

This experiment measures **scale-out capacity**. Because the EPD deployment
uses four GPUs and the colocated baseline uses one GPU, it is not presented as
an equal-resource efficiency comparison.

### W2: Agent State Cloning

| Field | Value |
| --- | --- |
| Pages / branches | 32 / 8 |
| Page size | 16 tokens |
| KV shape | 8 layers, 4 KV heads, head dimension 64 |
| Dtype / device | float32 / CPU |
| Baseline | Deep-copy each branch |
| EPD operation | Clone immutable page references, then Copy-on-Write |

This workload isolates clone latency, copied bytes, CoW materialization, and
post-release ownership cleanup.

## Metrics

| Metric | Purpose |
| --- | --- |
| Request throughput | Completed requests per second |
| Output throughput | Generated output tokens per second |
| TTFT | Client request start to first streamed token |
| End-to-end latency | Client request start to stream completion |
| Goodput | Successful requests satisfying the configured gate |
| Peer-buffer batches / bytes | Direct P-to-D transfer volume |
| Fallback and failure counters | Direct-path integrity |
| Clone latency / copied bytes | Agent branch creation efficiency |
| Orphan blocks after release | Ownership and cleanup correctness |

## Results

### Real multimodal EPD

| Metric | Result |
| --- | ---: |
| HTTP success | 16 / 16 |
| EPD route success | 16 / 16 |
| Request throughput | 8.143 RPS |
| Output throughput | 232.08 tok/s |
| Mean latency | 953.08 ms |
| Mean TTFT | 288.82 ms |
| P95 TTFT | 344.01 ms |
| P99 TTFT | 354.51 ms |
| Vision Encoder skip | Observed |
| P-to-D peer-buffer batches | 46 |
| P-to-D peer-buffer bytes | 301,989,888 |
| Transfer fallback batches | 0 |
| Layered send failures | 0 |
| Layered receive failures | 0 |

The strict real-EPD gate passed. The result demonstrates a complete real-model
serving path with direct feature reuse and direct layered KV transport.

### Scale-out comparison

| Metric | 1-GPU colocated | 4-GPU 2P2D EPD | Change |
| --- | ---: | ---: | ---: |
| Request throughput | 2.580 RPS | 3.685 RPS | +42.8% |
| Output throughput | 74.83 tok/s | 108.24 tok/s | +44.6% |
| Mean latency | 2917.69 ms | 2143.09 ms | -26.5% |
| Mean TTFT | 1271.71 ms | 1413.93 ms | +11.2% |
| P95 TTFT | 2006.39 ms | 1481.26 ms | -26.2% |
| P-to-D fallback / failures | N/A | 0 / 0 | Direct path clean |
| Prefill-to-Decode affinity | N/A | 24 / 24 hits | 0 fallback |

The measured EPD topology increased request and output-token throughput while
reducing mean end-to-end latency and P95 TTFT. The paired deterministic-text
check produced exact text identity for 12 of 16 measured requests; all requests
completed successfully and the transport integrity gates passed.

### Agent State Cloning

| Metric | Deep copy | Page-reference clone |
| --- | ---: | ---: |
| Mean clone latency | 1.6996 ms | 0.1694 ms |
| P50 clone latency | 1.3749 ms | 0.1552 ms |
| P95 clone latency | 4.1569 ms | 0.2108 ms |
| Branch-copy bytes | 67,108,864 | 0 |

Measured mean clone speedup was **10.03x**. One-page CoW materialization had a
262,144-byte upper bound. After all branches were released, the state store,
page manager, and owner directory reported zero retained states and zero
orphan blocks.

## Correctness and stability gates

| Gate | Published result |
| --- | --- |
| Real model and real vLLM services | Pass |
| Mock path disabled | Pass |
| Strict no-fallback mode | Pass |
| HTTP success | Pass |
| Vision Hidden State injection | Pass |
| Direct P-to-D backend observed | Pass |
| Layered send / receive integrity | Pass |
| Direct-buffer reference cleanup | Pass |
| Agent clone release cleanup | Pass |
| Resource-count metadata present | Pass |

Repository regression verification after the final runtime-boundary changes:

| Suite | Result |
| --- | ---: |
| `mooncake_epd/tests` | 371 passed, 1 skipped |
| Mooncake Store service API | 72 passed |
| Runtime/security boundary subset | 52 passed |
| Python compile, JSON validation, diff whitespace | Pass |

## Reproduction

Run from the Mooncake repository root. Paths and GPU assignments are expressed
as environment variables so the same commands can be used on another host.

### Environment

```bash
python3.10 -m venv .venv
source .venv/bin/activate
python -m pip install -r mooncake_epd/requirements.txt

export PYTHONPATH=$PWD
export MODEL_PATH="${MODEL_PATH:-models/Qwen3-VL-8B-Instruct}"
export DATASET_ROOT="${DATASET_ROOT:-datasets/mooncake_test_dataset}"
export ARTIFACT_ROOT="${ARTIFACT_ROOT:-.epd-eval}"
export ENCODER_DEVICE="${ENCODER_DEVICE:-cuda:0}"
export PREFILL_GPU="${PREFILL_GPU:-1}"
export PREFILL_GPUS="${PREFILL_GPUS:-0 1}"
export DECODE_GPUS="${DECODE_GPUS:-2 3}"
export BASELINE_GPU="${BASELINE_GPU:-4}"
export NO_PROXY=127.0.0.1,localhost
unset http_proxy https_proxy HTTP_PROXY HTTPS_PROXY
mkdir -p "$ARTIFACT_ROOT"
```

Install the vLLM, Transformers, Qwen-VL utility, CUDA, and Mooncake package
versions compatible with the target GPU environment.

### Build the reusable evaluation dataset

```bash
python mooncake_epd/scripts/build_qwenvl_epd_eval_dataset.py \
  --dataset-root "$DATASET_ROOT" \
  --output "$ARTIFACT_ROOT/qwenvl_epd_eval.jsonl" \
  --samples 100 \
  --seed 2026
```

### Run real multimodal EPD

```bash
python mooncake_epd/scripts/run_real_qwenvl_epd_demo.py \
  --workdir "$ARTIFACT_ROOT/real-multimodal-epd" \
  --model "$MODEL_PATH" \
  --encoder-device "$ENCODER_DEVICE" \
  --prefill-gpu "$PREFILL_GPU" \
  --decode-gpus $DECODE_GPUS \
  --gpu-memory-utilization 0.75 \
  --max-model-len 4096 \
  --max-num-batched-tokens 4096 \
  --max-num-seqs 32 \
  --warmup-requests 8 \
  --warmup-concurrency 8 \
  --repeat-requests 16 \
  --concurrency 8 \
  --max-tokens 128 \
  --temperature 0.0 \
  --demo-image room \
  --mooncake-protocol tcp \
  --layers-per-group 32 \
  --max-group-bytes 67108864 \
  --max-transfer-descriptors 512 \
  --max-transfer-bytes 67108864
```

### Generate the scale-out prompt

```bash
export PROMPT_FILE="$ARTIFACT_ROOT/long_prompt.txt"
python - <<'PY'
import os
from pathlib import Path

sentence = (
    "The context describes distributed inference scheduling, KV cache "
    "transfer, and request admission. "
)
prompt = sentence * 128 + "State the key tradeoff in one sentence.\n"
Path(os.environ["PROMPT_FILE"]).write_text(prompt, encoding="utf-8")
PY
```

### Run the colocated baseline

```bash
python mooncake_epd/scripts/run_vllm_single_baseline_e2e.py \
  --workdir "$ARTIFACT_ROOT/single-baseline" \
  --model "$MODEL_PATH" \
  --gpu "$BASELINE_GPU" \
  --gpu-memory-utilization 0.65 \
  --max-model-len 4096 \
  --max-num-batched-tokens 4096 \
  --max-num-seqs 32 \
  --generation-config vllm \
  --warmup-requests 8 \
  --warmup-concurrency 8 \
  --repeat-requests 16 \
  --concurrency 8 \
  --prompt-file "$PROMPT_FILE" \
  --request-variation unique_prefix \
  --max-tokens 32 \
  --temperature 0.0 \
  --text-only
```

### Run 2P2D EPD

```bash
python mooncake_epd/scripts/run_vllm_online_direct_e2e.py \
  --workdir "$ARTIFACT_ROOT/2p2d-epd" \
  --model "$MODEL_PATH" \
  --text-only \
  --prefill-gpus $PREFILL_GPUS \
  --decode-gpus $DECODE_GPUS \
  --gpu-memory-utilization 0.65 \
  --max-model-len 4096 \
  --max-num-batched-tokens 4096 \
  --max-num-seqs 32 \
  --generation-config vllm \
  --warmup-requests 8 \
  --warmup-concurrency 8 \
  --repeat-requests 16 \
  --concurrency 8 \
  --prompt-file "$PROMPT_FILE" \
  --request-variation unique_prefix \
  --max-tokens 32 \
  --temperature 0.0 \
  --mooncake-protocol nvlink_intra \
  --layers-per-group 32 \
  --max-group-bytes 268435456 \
  --max-transfer-descriptors 4096 \
  --max-transfer-bytes 268435456 \
  --transfer-workers 2 \
  --prefill-decode-affinity prefill-0=decode-0 prefill-1=decode-1
```

### Run Agent State Cloning

```bash
python mooncake_epd/scripts/run_agent_state_clone_benchmark.py \
  --output "$ARTIFACT_ROOT/agent_state_clone.json" \
  --device cpu \
  --dtype float32 \
  --branches 8 \
  --pages 32 \
  --page-size 16 \
  --num-layers 8 \
  --num-kv-heads 4 \
  --head-dim 64
```

### Run regressions and artifact gates

```bash
PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 \
MOONCAKE_EPD_ENABLE_VLLM_PATCHES=0 \
python -m pytest -q mooncake_epd/tests

PYTHONPATH=$PWD/mooncake-wheel \
PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 \
python -m pytest -q mooncake-wheel/tests/test_mooncake_store_service_api.py

python mooncake_epd/scripts/check_real_epd_gate.py \
  --summary "$ARTIFACT_ROOT/real-multimodal-epd/online_direct_e2e_summary.json" \
  --json
```

## Public evidence

The committed evidence record is
[`artifacts/2026-07-10/benchmark_summary.json`](artifacts/2026-07-10/benchmark_summary.json).
It contains the environment, workload boundaries, summarized metrics, and
SHA-256 digests for these logical source artifacts:

| Source ID | SHA-256 |
| --- | --- |
| `real-multimodal-epd` | `2ef83636ab1b6ddb09b5983a29f0ddee7ea601eed171d42719fdfb149afc3c8d` |
| `scale-out-2p2d-epd` | `a6fe23c1d2fc2773400823393138376f6f68ff03a5013519a4d5e150366a1df4` |
| `single-gpu-colocated-baseline` | `68416afc83d4394e5b12284162fd5ce41a47a99d0ee6ffe5dd573735032a7b7c` |
| `agent-state-clone` | `8a73f3280e0dc95054d323a3942cd7f70c967380eed4863536478099f380672c` |

## Interpretation and next evaluation

The current results establish real EPD serving, direct Vision Hidden State
reuse, layered KV transfer integrity, scale-out throughput, topology affinity,
and zero-copy Agent branch creation. The next public matrix will add matched
1-GPU and 4-GPU resource pairs, cold/warm multimodal cache workloads, mixed
Agent scheduling loads, and longer stability runs while preserving the same
strict artifact gates.
