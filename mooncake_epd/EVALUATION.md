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

The same host provided a validated TCP direct path. A separately staged
`USE_INTRA_NVLINK` engine also completed a strict text-only 1P1D P→D run on
the NV4-connected GPU1↔GPU3 pair, with both worker logs proving native
`nvlink_intra` selection. That is a transport-path result only: it does not
validate full multimodal E→P→D, 2P2D NVLink, dual-node operation, or an
installed-release package. RDMA is not required to reproduce the published TCP
correctness results.

The current environment has GPUs but no verified second host or RDMA fabric.
Consequently, the newest hardware refresh below is deliberately labeled as a
single-host TCP result; it is not relabeled as RDMA, NVLink EPD, or a
dual-node result.

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

The current runner also supports a cold multi-image variant.  When all images
route to one Prefill worker, E→P feature descriptors are coalesced into one
Mooncake peer-buffer write, and the Prefill EngineCore groups same-session
remote handles into one direct batch read into final hidden-state tensors.  The
artifact records `image_count` and the shared publish timing; with multimodal
trace enabled, `feature_handle_direct_remote_batch_resolved` records the read
batch.  This is a hot-path optimization, not a published throughput claim
until it has been measured against the same multi-image colocated baseline on
equal resources.

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

### B/C/D Decode token-path diagnostic

The current worktree completed a strict Qwen3-VL-8B-Instruct W3/C1 diagnostic
on one host with Encoder GPU 5, Prefill GPU 1, Decode GPU 3, and the validated
NVLink P-to-D path. Both arms used prompt-only Prefill; the only intended
variable was shadow versus active Decode token-envelope dispatch.

| Metric | Shadow / legacy Decode | Active token envelope | Change |
| --- | ---: | ---: | ---: |
| Decode request bytes | 1,145,006 | 16,718 | -98.54% |
| Decode JSON encode | 18.361 ms | 0.766 ms | -95.83% |
| Decode stream open | 61.347 ms | 8.239 ms | -86.57% |
| TTFT | 378.245 ms | 341.123 ms | -9.81% |
| End-to-end latency | 738.778 ms | 705.759 ms | -4.47% |
| Fast path / media stripped / envelope validated | shadow only | 1 / 1 / 1 | pass |

The token-path response exactly matched the shadow response and all six
measured responses in the existing colocated baseline. Their common response
SHA-256 is
`c29089b34feccc282a94d04d4e6f1f6f350031cc8ecd79e436ea0d7a35f92c4c`.
No fallback was enabled.

Local artifacts:

- `/tmp/mooncake_epd_20260719_qwen3_w3_c1_decode_shadow_bcd_r2/online_direct_e2e_summary.json`
- `/tmp/mooncake_epd_20260719_qwen3_w3_c1_decode_token_ids_bcd_r5/online_direct_e2e_summary.json`
- `/tmp/mooncake_baseline_20260716_qwen3_w3_isolate_2k_tokens_c1_gpu3_refresh/single_baseline_summary.json`

This diagnostic used one warmup and one measured request per EPD arm. It
validates semantics and proves that the compact path is exercised, but it is
not used as a throughput claim. C4/C16 repeated trials with confidence
intervals, Qwen2.5-VL coverage, and equal-resource comparisons remain required
before publication.

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

The strict real-EPD artifact gate passed. It verifies the captured direct
Encoder-to-Prefill-to-Decode path with feature reuse and layered KV transport
on that host. It does **not** establish the later producer-restart fence,
multi-node/RDMA behavior, or deterministic-golden acceptance; those require
their own current-hardware gates.

### 2026-07-14 strict single-host TCP refresh

This worktree refresh validates the current public demo facade after it was
expanded to forward the online runner's topology, scheduler, cache, transfer,
and dispatch settings. In particular, it closes a facade/runner compatibility
defect where the lower-level runner required `scheduler_policy` but the public
demo parser did not define it.

| Field | Value |
| --- | --- |
| Model | Qwen3-VL-8B-Instruct |
| Topology | Encoder GPU 0, Prefill GPU 1, Decode GPU 3 on one host |
| Transport | E→P TCP direct peer-buffer; P→D TCP direct peer-buffer |
| Strict dispatch | `render_generate` |
| Warmup / measured | 2 / 4 requests |
| Concurrency / output budget | 1 / 16 tokens |
| Gate | Strict no-fallback |

| Metric | Result |
| --- | ---: |
| HTTP success | 4 / 4 measured requests |
| Mean / p95 TTFT | 255.93 / 257.65 ms |
| Mean latency | 614.15 ms |
| Request / completion throughput | 1.628 RPS / 26.04 tok/s |
| Measured P→D peer-buffer batches / bytes | 8 / 9,437,184 |
| Measured P→D write bandwidth | 2.44 Gbit/s |
| Fallback / layered send / receive failures | 0 / 0 / 0 |
| Direct-feature cache hits | 4 / 4 measured requests |
| Direct-buffer references after release | 0 |

The measured timing split is the actionable outcome of this refresh:

| Segment | Mean |
| --- | ---: |
| E→P direct-cache lookup | 4.84 ms |
| Prefill `render_generate` | 167.84 ms |
| Proxy Prefill→Decode dispatch | 1.58 ms |
| P→D receive worker | 14.52 ms |
| Decode engine request→first token | 69.27 ms |
| Proxy Decode first content | 75.90 ms |

For this stable, cache-warm, single-request workload, strict Prefill control
processing is the largest measured TTFT component. The safe next step is to
reduce that path only with output-equivalence coverage; historical
`openai_prompt_only` probes are not sufficient for strict serving because they
did not produce exact text for every request. This refresh is not a baseline
comparison, equal-resource result, throughput-improvement claim, RDMA result,
or two-node validation. Its raw artifact is intentionally outside the
committed 2026-07-10 public evidence snapshot until repeated and reviewed.

### 2026-07-15 strict rendered-Prefill-cache ablation

The follow-up implements that safe control-path optimization without changing
the strict dispatch protocol. The Proxy caches only the deterministic vLLM
`/render` output, bounds it to 64 entries / 256 MiB / 300 seconds, scopes it
to a Prefill worker-generation fence, and injects fresh FeatureHandles and
P→D KV-transfer metadata into every `/inference/v1/generate` call. Requests
with mutable external `http(s)://`, `file://`, or `ftp://` media bypass the
cache.

Both arms used Qwen3-VL-8B-Instruct; Encoder GPU 0, Prefill GPU 1, Decode GPU
3; same-host TCP direct peer-buffer; `render_generate`; two warmups and six
measured requests; concurrency one; and a 16-token output budget. Both passed
the strict real-EPD gate, all 12 measured HTTP responses were 200, and the six
cache-on outputs exactly matched the six cache-off outputs.

| Metric | Cache off | Cache on | Change |
| --- | ---: | ---: | ---: |
| Mean TTFT | 276.90 ms | 210.19 ms | -24.1% (-66.71 ms) |
| Mean latency | 635.12 ms | 555.19 ms | -12.6% |
| Request throughput | 1.574 RPS | 1.800 RPS | +14.4% |
| Completion throughput | 25.18 tok/s | 28.80 tok/s | +14.4% |
| Prefill render control step | 87.33 ms | 1.69 ms | -98.1% |
| Measured render-cache hits | 0 / 6 | 6 / 6 | pass |
| Fallback / layered receive failures | 0 / 0 | 0 / 0 | pass |

The cache held one 3.07 MiB artifact. This is a strict EPD cache ablation, not
a comparison to colocated vLLM or another system, an equal-resource result, a
multi-node result, or RDMA evidence. It remains outside the committed public
snapshot pending repeated-run and artifact-digest review.

An independent concurrency-four control (two warmups, 16 measured requests
per arm, otherwise identical) also passed both strict gates with exact 16/16
output equality and zero fallback/receive failures. Cache-on changed mean
TTFT **469.05→379.13 ms** (-19.2%), request throughput **4.485→4.935 RPS**
(+10.0%), completion throughput **71.77→78.97 tok/s** (+10.0%), and mean
latency **866.18→779.01 ms** (-10.1%). Its measured render-cache hit rate was
16/16; this is concurrent EPD ablation evidence, not a standalone or
strong-system comparison.

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

The archived scale-out topology observed higher request and output-token
throughput and lower mean end-to-end latency/P95 TTFT than its 1-GPU baseline.
It is not an equal-resource result and is not a deterministic-golden release:
the paired text check produced exact identity for only 12 of 16 measured
requests. All requests completed and the transport-integrity gates passed, but
the observation must not be generalized into an equal-resource throughput
claim.

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

Current-checkout CPU regression verification (2026-07-13; real-model/GPU/RDMA
gates are explicitly skipped when their resources are unavailable):

| Suite | Result |
| --- | ---: |
| `mooncake_epd/tests` | 439 passed, 19 skipped |
| Mooncake Store service API | 72 passed |
| Runtime/security boundary subset | 52 passed |
| Python compile, JSON validation, diff whitespace | Pass |
| Public-demo facade / online-runner focused regression (2026-07-14) | 73 passed |
| Rendered-Prefill-cache semantics/config/runner regression (2026-07-15) | 91 passed |

### Current hardware-gated status

The current development environment does not provide a verified RDMA fabric.
The RDMA, two-node, CUDA IPC/NVLink Omni, and real-model GPU gates are kept as
explicit skips/unavailable checks. In particular, a POSIX-SHM CPU process
smoke result is not recorded as TCP, RDMA, or a real-Omni performance result.
Use the following only to validate the local descriptor-only worker boundary:

```bash
PYTHONPATH=$PWD python mooncake_epd/scripts/run_omni_worker_pipeline_e2e.py \
  --transport posix_shm --elements 64
PYTHONPATH=$PWD python mooncake_epd/benchmarks/omni_pipeline_benchmark.py \
  --transport posix_shm --warmup 1 --rounds 3
```

Both artifacts set `claim_supported=false`. The runner also has an actual local
TCP-relay compatibility edge (`--transport tcp`); this is not a two-node
result. `--transport rdma` emits a machine-readable skip until an actual
adapter plus NIC/GID/remote-host evidence is available.

### Current evidence gaps

The CPU suite verifies contracts, fault handling, and local process transport;
it does not replace the following release gates: a version-locked real vLLM
Agent connector acknowledgement, repeated single-node GPU validation across a
workload matrix, two-node TCP continuation, RDMA registration/GID/remote-host
evidence, CUDA IPC/NVLink Omni semantic stages, 10k-request soak, complete
A0--A9 ablations, or equal-resource comparison against the planned strong
systems. These remain explicitly unavailable rather than inferred from the
archived scale-out or CPU results.

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

The facade accepts the lower-level runner options used by current deployments,
including `--prefill-gpus`, `--decode-gpus`, `--scheduler-policy`,
`--transfer-workers`, direct-buffer release/cache switches, and
`--prefill-dispatch-mode`. Keep `render_generate` for strict serving. The
rendered-Prefill cache is enabled by default; use
`--no-enable-rendered-prefill-cache` only for a matched cache ablation.
`openai_prompt_only` requires an explicit compatibility override only after a
version- and workload-specific output-equivalence artifact has passed.

### Measure the multi-image E→P batching path

Give both the EPD runner and the colocated runner the same ordered image list;
the entries must be distinct assets when measuring cold publication rather than
an exact-cache hit.  `--image-url` and `--image-urls` are mutually exclusive.

```bash
# Run the same command shape for EPD and the colocated baseline, changing only
# the deployment-specific flags/GPU topology.
python mooncake_epd/scripts/run_vllm_online_direct_e2e.py \
  --workdir "$ARTIFACT_ROOT/multi-image-epd" \
  --model "$MODEL_PATH" \
  --image-urls "$IMAGE_URL_A" "$IMAGE_URL_B" \
  --encoder-device "$ENCODER_DEVICE" \
  --prefill-gpu "$PREFILL_GPU" --decode-gpu "${DECODE_GPU:-2}" \
  --mooncake-protocol tcp

python mooncake_epd/scripts/run_vllm_single_baseline_e2e.py \
  --workdir "$ARTIFACT_ROOT/multi-image-colocated" \
  --model "$MODEL_PATH" --gpu "$BASELINE_GPU" \
  --image-urls "$IMAGE_URL_A" "$IMAGE_URL_B"
```

Verify that both summaries have the same request fingerprint and
`benchmark_config.request.image_count`; in the EPD request timing metadata,
`direct_publish_engine_batch_feature_count=2` and only one shared batch timing
must be counted.  With `MOONCAKE_EPD_VLLM_MM_HIDDEN_TRACE=1`, the Prefill trace
must also contain one `feature_handle_direct_remote_batch_resolved` event with
`batch_feature_count=2` for that Encoder session.  Do not publish an
improvement without the H5 equal-resource, repeat, confidence-interval, and
raw-artifact gates.

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

`nvlink_intra` is valid here only if every paired Prefill→Decode GPU pair has a
verified NVLink path and the deployed engine exposes
`SUPPORT_INTRA_NVLINK=true`. The artifact-backed host currently has one tested
GPU1↔GPU3 NV4 pair, so this 2P2D command is a topology-specific template, not
evidence for a 2P2D NVLink result on that host.

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
reuse, layered KV transfer integrity, a strict rendered-Prefill-cache TTFT
ablation, scale-out throughput, topology affinity, and zero-copy Agent branch
creation. The next public matrix will add matched 1-GPU and 4-GPU resource
pairs, cold/warm multimodal cache workloads, mixed Agent scheduling loads, a
strict Prefill-dispatch output-equivalence gate, and longer stability runs
while preserving the same strict artifact gates.
