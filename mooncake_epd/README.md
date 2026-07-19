# Mooncake EPD: Multimodal and Agent-State Disaggregation

Mooncake EPD is a **fail-closed research prototype** for
Encoder-Prefill-Decode (EPD) disaggregation integrated into the Mooncake
source tree. It extends Mooncake and vLLM for multimodal serving and stateful
Agent workflows through three coordinated data paths:

1. **E→P hidden-state transfer**: a dedicated Encoder stage produces Qwen-VL visual hidden states and transfers them to Prefill through FeatureHandle/direct peer-buffer paths.
2. **P→D layered KV transfer**: Prefill generates paged KV cache and hands it to Decode through a repo-local vLLM `MooncakeConnector` with grouped/layered transfer metadata.
3. **Agent state reuse**: workflow state, KV page descriptors, feature handles, clone/release lifecycle, and scheduler metadata are tracked as first-class control-plane objects.

The implementation reuses Mooncake Transfer Engine, Mooncake Store, vLLM's KV
connector lifecycle, and repository-native tests. Public performance summaries
are archived in
[`artifacts/2026-07-10/benchmark_summary.json`](artifacts/2026-07-10/benchmark_summary.json).
They are not a production-readiness or equal-resource throughput claim. New
claims must pass `scripts/check_epd_performance_claims.py` from complete raw
artifacts.

## Public links

- Upstream Mooncake repository: <https://github.com/kvcache-ai/Mooncake>
- Public submission repository: <https://github.com/Pinoeer-kingxi/Mooncake>
- EPD branch: <https://github.com/Pinoeer-kingxi/Mooncake/tree/epd-vllm-serve-20260709-a6000>
- Upstream PR: to be opened from the public EPD branch after the review split described in `PR_DESCRIPTION.md`.

The proposed upstream review structure is documented in `PR_DESCRIPTION.md`.

## Task coverage

### Foundation tasks

| Task | Delivered capability |
| --- | --- |
| vLLM EPD three-stage serving | A real Encoder service transfers Vision Hidden State to Prefill through Mooncake; Prefill transfers paged KV cache to Decode through the vLLM save/load connector path. |
| Agent State Cloning | Same-node page refs can be cloned without KV payload copy. Cross-node branches are Store-backed manifest clones and perform an explicit materialized copy before write; they are not physical cross-node zero-copy. |
| Qwen-VL end-to-end demo | Real-model launchers, strict direct-path gates, baseline runners, and benchmark artifacts cover Qwen3-VL serving. |

### Advanced tasks

| Task | Delivered capability |
| --- | --- |
| Worker-level stage transfer | CPU Omni stages have a real independent-process POSIX-SHM descriptor transport. CUDA IPC/NVLink/RDMA remain capability-gated work, not aliases for `.to()` copies. |
| Agent PD scheduling | Thinking, interactive, and hybrid tasks are routed with stage-specific pools, deadline/rho-aware admission, topology affinity, and measurable backpressure. |
| Hidden State Prefix Caching | Exact reuse is supported; partial Omni prefix reuse is experimental and disabled by default until a model-specific oracle passes. |
| Upstream contribution | The implementation follows Mooncake's tree layout and is packaged as reviewable Transfer Engine, FeatureHandle, serving/control-plane, and benchmark/documentation changes. |

## What is implemented

| Area | Current implementation evidence |
| --- | --- |
| EPD pipeline | `core/epd_pipeline.py`, `core/epd_workers.py`, `scripts/run_real_qwenvl_epd_demo.py`, `scripts/run_vllm_online_direct_e2e.py` |
| Qwen-VL Encoder FeatureBundle | `core/epd_workers.py`, `core/state/feature_store.py`, `core/state/direct_feature_buffer.py`, `scripts/epd_encoder_service.py`, `scripts/direct_feature_buffer_service.py` |
| P→D layered KV transfer | `core/control/vllm_mooncake_connector.py`, `core/control/vllm_transfer_primitives.py`, `core/state/kv_transfer_manifest_v2.py`, `demo/vllm_integration.py` |
| Direct peer-buffer transport planning | `core/transfer/engine.py`, `tests/test_transfer_engine_peerbuffer_plan.py` |
| Serving control plane | `core/control/serving_controller.py`, `core/control/kv_directory.py`, `core/state/workflow_registry.py` |
| Agent PD routing/backpressure | `agent/coordination/scheduler.py`, serving metrics in `ServingControlPlane` |
| Agent state clone / CoW descriptors | `agent/state_clone.py`, `core/state/kv_manifest.py`, `core/state/mooncake_kv_page_store.py`, `core/state/agent_state_catalog.py`, `core/state/vllm_kv_materializer.py`, `core/state/kv_state_store.py`, `core/state/page_manager.py` |
| Explicit vLLM adapter | `integrations/vllm/adapter.py`, `integrations/vllm/launcher.py`, `docs/vllm_upstream_pr_plan.md` |
| Prompt-only Prefill and compact Decode | `sitecustomize.py`, `core/control/decode_token_envelope.py`, `scripts/vllm_disagg_proxy.py`, `tests/test_vllm_decode_token_mm_patch.py` |
| Omni process worker transport | `core/omni/`, `scripts/run_omni_worker_pipeline_e2e.py`, `benchmarks/omni_pipeline_benchmark.py`, `tests/test_omni_process_pipeline.py` |
| MM cache and event prefetch | `core/state/mm_store.py`, `core/state/vllm_mm_hidden_cache.py`, `core/state/vllm_feature_handle_provider.py` |
| Artifact gates | `scripts/check_real_epd_gate.py`, `scripts/check_epd_artifact_gates.py` |

## Core contributions

- **Multimodal EPD serving path**: separates visual encoding, LLM prefill, and decode for Qwen3-VL-style workloads while preserving OpenAI-compatible vLLM request handling.
- **Direct FeatureHandle path for E→P**: Prefill allocates destination feature buffers; Encoder publishes hidden-state tensor bytes through Mooncake direct peer-buffer metadata; Prefill validates descriptors before skipping vision recompute.
- **Layered P→D KV transfer**: vLLM `MooncakeConnector` extension batches KV descriptors by layer group, records peer-buffer backend metrics, and enforces strict no-fallback gates for measured runs. `KVTransferManifestV2` binds the transfer/workflow, source engine and destination execution fence, source/destination generations, KV-layout checksum, declared transport, block groups, and an expiry. Generated workers atomically publish their current generation fence; strict Decode workers reject a missing, stale, tampered, wrong-target, or destination-generation-mismatched envelope before initiating a pull.
- **State-centric Agent runtime**: versioned workflow records, KV directory entries, handoff IDs, and WAL-backed registry events make Agent fork/handoff/release observable.
- **Fail-closed Agent consumption bridge**: Store-backed Agent materialization
  produces a checked vLLM block envelope and, in strict mode, requires a
  version-locked Decode connector acknowledgement before the state is reported
  consumable. The CPU package supplies the contract and tests; deployment must
  wire the callback to the compatible vLLM worker adapter.
- **Precise Agent clone semantics**: `same_node_physical_zero_copy` refers only to a retained local page/block reference. `cross_node_manifest_clone` is metadata-only at fork time; `cross_node_materialized_copy` moves bytes into target blocks and must never be called zero-copy.
- **Type-aware scheduler and backpressure**: thinking, interactive, and hybrid Agent hints influence Prefill/Decode routing; rho/deadline backpressure and reject counters are exposed in metrics.
- **TTFT-oriented request representation and scheduling**: strict prompt-only
  Prefill returns an integrity-bound token/MM envelope; Decode consumes exact
  token IDs and locally reconstructed structural MM metadata without original
  media bytes. Worker scheduling tracks queued, running, and first-token
  states and combines telemetry confidence with queue/service/transfer costs
  instead of a fixed affinity bonus.

## Dependencies

The optional package boundary and supported vLLM minor are declared in
[`pyproject.toml`](pyproject.toml) and
[`constraints-vllm-0.23.0.txt`](constraints-vllm-0.23.0.txt). Minimal Python
dependencies from `requirements.txt` remain a compatibility entry point:

```text
torch>=2.0
mooncake-transfer-engine
numpy
Pillow
pyyaml
requests
aiohttp
```

The artifact-backed real serving run used this environment (`benchmark_summary.json`):

| Component | Version / value |
| --- | --- |
| GPU host | 8 × NVIDIA RTX A6000, 49140 MiB each |
| CPU / RAM | 2 × Intel Xeon Gold 6226R, 64 logical CPUs; 503 GiB RAM |
| Python | 3.10.12 |
| PyTorch | 2.11.0 |
| vLLM | 0.23.0 |
| Transformers | 5.12.1 |
| CUDA runtime / driver | CUDA 13.0 / NVIDIA 580.159.03 |
| Mooncake Python package | 0.3.11.post1 |
| Model | Qwen3-VL-8B-Instruct |

Real multimodal serving additionally requires the artifact-compatible vLLM,
Transformers, and `qwen-vl-utils` versions listed in `EVALUATION.md`.

## Build, start, and test commands

Run commands from the Mooncake repository root unless noted otherwise.

### Install / build the Python environment

```bash
python3.10 -m venv .venv
source .venv/bin/activate
python -m pip install -e mooncake_epd[serving,test] \
  -c mooncake_epd/constraints-vllm-0.23.0.txt
```

`mooncake_epd` is an optional package inside this repository. Generated
Prefill/Decode scripts invoke the explicit adapter launcher; they no longer
require global `sitecustomize` mutation or edits to `site-packages`.

### Generate vLLM/Mooncake launch scripts

```bash
export PYTHONPATH=$PWD
export MOONCAKE_EPD_VENV_ROOT="${MOONCAKE_EPD_VENV_ROOT:-$VIRTUAL_ENV}"
export MOONCAKE_EPD_MODEL="${MOONCAKE_EPD_MODEL:-models/Qwen3-VL-8B-Instruct}"
python mooncake_epd/demo/vllm_integration.py
```

This generates or refreshes scripts under `mooncake_epd/config/` for metadata, master, Prefill, Decode, and proxy processes.

### Start vLLM EPD services

```bash
bash mooncake_epd/scripts/start_vllm_disagg.sh
```

The command above regenerates the local configuration and prints the startup
order. Start each generated process in its own terminal:

```bash
bash mooncake_epd/config/start_metadata.sh
bash mooncake_epd/config/start_master.sh
bash mooncake_epd/config/start_prefill.sh
bash mooncake_epd/config/start_decode.sh
bash mooncake_epd/config/start_proxy.sh
```

For Store API or standalone Store integration, use
`mooncake_epd/scripts/start_mooncake.sh` instead of separately starting the
generated metadata and master processes. Do not start both variants on the
same ports. The real EPD benchmark artifacts used the self-contained strict
runners `scripts/run_real_qwenvl_epd_demo.py` and
`scripts/run_vllm_online_direct_e2e.py`.

For a cold multi-image E→P measurement, both
`run_vllm_online_direct_e2e.py` and `run_vllm_single_baseline_e2e.py` accept
the same `--image-urls URL_A URL_B ...` workload flag (mutually exclusive with
`--image-url`). The EPD encoder batches publish-required features for one
Prefill session into one Mooncake peer-buffer write, and Prefill batches the
same session's remote `epd-direct://` reads directly into final tensors. Use
the identical ordered list for the colocated baseline; do not turn this
optimization into a throughput claim without the documented equal-resource
hardware gate.

### Run the strict public demo facade

`run_real_qwenvl_epd_demo.py` is the supported public facade for the strict
online-direct runner. It exposes the runner's topology, scheduler, cache,
transfer-worker, and dispatch controls, so a benchmark cannot fail after
startup because the wrapper omitted a lower-level required option.

```bash
export PYTHONPATH=$PWD
export MODEL_PATH="${MODEL_PATH:-models/Qwen3-VL-8B-Instruct}"
export ARTIFACT_ROOT="${ARTIFACT_ROOT:-.epd-eval}"

python mooncake_epd/scripts/run_real_qwenvl_epd_demo.py \
  --workdir "$ARTIFACT_ROOT/strict-tcp-refresh" \
  --model "$MODEL_PATH" \
  --encoder-device cuda:0 \
  --prefill-gpu 1 --decode-gpu 3 \
  --mooncake-protocol tcp --direct-engine-protocol tcp \
  --scheduler-policy agent_aware \
  --warmup-requests 2 --warmup-concurrency 1 \
  --repeat-requests 4 --concurrency 1 \
  --max-tokens 16 --temperature 0.0 \
  --strict-no-fallback
```

The default `render_generate` Prefill dispatch remains the strict serving
path. `openai_prompt_only` is an explicit experiment only: it must remain
disabled for strict serving until the exact model, vLLM version, and workload
have recorded output-equivalence evidence.

`render_generate` also enables a bounded Proxy-side rendered-request cache by
default. It caches only vLLM's deterministic `/render` response, scopes the
entry to the Prefill worker generation fence, and injects fresh FeatureHandle
and P→D KV-transfer metadata before every `/inference/v1/generate` call.
External `http(s)://`/`file://` media bypass it, so mutable remote media is
never reused. The default bound is 64 entries, 256 MiB, and 300 seconds; use
`--no-enable-rendered-prefill-cache` for the strict cache-off ablation.

### Run regression tests

```bash
PYTHONPATH=$PWD \
PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 \
MOONCAKE_EPD_ENABLE_VLLM_PATCHES=0 \
python -m pytest -q mooncake_epd/tests

PYTHONPATH=$PWD/mooncake-wheel \
PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 \
python -m pytest -q mooncake-wheel/tests/test_mooncake_store_service_api.py
```

Run the CPU suite from the current checkout rather than relying on historical
counts. Real-model/GPU cases are explicitly skipped when model files or GPUs
are unavailable and must not access model hubs.

### Explicit vLLM adapter probe

```bash
PYTHONPATH=$PWD MOONCAKE_EPD_ENABLE_VLLM_PATCHES=0 \
  python mooncake_epd/scripts/check_vllm_feature_handle_patch.py --json

PYTHONPATH=$PWD MOONCAKE_EPD_ENABLE_VLLM_PATCHES=0 \
  python -c 'from mooncake_epd.integrations.vllm.adapter import install_vllm_epd_adapter; print(install_vllm_epd_adapter(strict=True))'
```

The first command must report no hooks. The second probes the vLLM version and
private method signatures, then installs hooks only in that worker process.

### Run the Omni worker-process smoke path

```bash
PYTHONPATH=$PWD python mooncake_epd/scripts/run_omni_worker_pipeline_e2e.py \
  --transport posix_shm --elements 64

PYTHONPATH=$PWD python mooncake_epd/benchmarks/omni_pipeline_benchmark.py \
  --transport posix_shm --warmup 1 --rounds 3
```

Both commands report the actual `posix_shm` backend, distinct worker PIDs,
descriptor-only stage timings, and `claim_supported=false`. `--transport tcp`
uses an authenticated local TCP relay and reports `actual_transport="tcp"`; it
is a compatibility path, not a two-node result. On this checkout `--transport
rdma` emits a machine-readable `status="skipped"` result unless a real RDMA
adapter is installed; it never relabels TCP or CPU SHM as RDMA.

### Check real artifact gates

On the evaluation host where raw `/tmp` artifacts are present:

```bash
export ARTIFACT_ROOT="${ARTIFACT_ROOT:-.epd-eval}"
export SERVING_SUMMARY="$ARTIFACT_ROOT/real-multimodal-epd/online_direct_e2e_summary.json"
export AGENT_CLONE_SUMMARY="$ARTIFACT_ROOT/agent_state_clone.json"

PYTHONPATH=$PWD python mooncake_epd/scripts/check_real_epd_gate.py \
  --summary "$SERVING_SUMMARY" \
  --json

PYTHONPATH=$PWD python mooncake_epd/scripts/check_epd_artifact_gates.py \
  --serving-summary "$SERVING_SUMMARY" \
  --agent-clone-summary "$AGENT_CLONE_SUMMARY"
```

Recorded gate results: `{"ok": true}` and `{"ok": true, "failures": []}`.

## Results summary

### Real multimodal EPD serving

Source: [`artifacts/2026-07-10/benchmark_summary.json`](artifacts/2026-07-10/benchmark_summary.json).

| Metric | Value |
| --- | ---: |
| Model | Qwen3-VL-8B-Instruct |
| Topology | 1 Encoder + 1 Prefill + 2 Decode GPUs |
| Transport | Mooncake direct peer-buffer over TCP |
| Measured requests | 16 |
| HTTP 200 responses | 16/16 |
| Request throughput | 8.143 RPS |
| Output throughput | 232.08 tok/s |
| Mean latency | 953.08 ms |
| Mean TTFT | 288.82 ms |
| p95 TTFT | 344.01 ms |
| P→D peer-buffer batches | 46 |
| Transfer fallback batches | 0 |
| Layered transfer failures | 0 |
| Layered receive failures | 0 |

### Latest strict TCP refresh (not a published comparison)

The current worktree was also refreshed on 2026-07-14 with Qwen3-VL-8B,
one Encoder, one Prefill, and one Decode GPU on a single host. This is a
small strict smoke/performance-diagnosis run, not a replacement for the
archived 16-request result and not an EPD-versus-baseline comparison.

| Metric | Value |
| --- | ---: |
| Warmup / measured requests | 2 / 4 |
| HTTP 200 responses | 4 / 4 |
| Transport | E→P and P→D TCP direct peer-buffer |
| Mean / p95 TTFT | 255.93 / 257.65 ms |
| Request / completion throughput | 1.628 RPS / 26.04 tok/s |
| Measured P→D peer-buffer batches / bytes | 8 / 9,437,184 |
| Transfer fallback / layered receive failures | 0 / 0 |
| Direct-feature cache hits | 4 / 4 measured requests |
| Direct-buffer live references after release | 0 |

The measured timing split attributes about 167.84 ms to strict Prefill
`render_generate`, 14.52 ms to P→D receive work, and 69.27 ms to Decode's
first-token engine segment. It identifies the strict Prefill control path as
the next optimization target while ruling out a blind transfer-parameter
change for this workload. The raw refresh artifact is intentionally not added
to the committed 2026-07-10 evidence snapshot until it has the corresponding
repeat, baseline, and artifact-digest review.

### 2026-07-15 strict rendered-Prefill-cache ablation (local, not published)

Using the same Qwen3-VL-8B model, same single-host Encoder GPU 0 / Prefill GPU
1 / Decode GPU 3 topology, TCP direct peer-buffer transport, two warmups, six
measured requests, concurrency one, 16 output tokens, and
`render_generate`, the leading control-plane bottleneck was isolated and
removed without changing the output. The cache-off and cache-on arms both
passed `check_real_epd_gate.py`; all six measured outputs were exact matches.

| Metric | Cache off | Cache on | Change |
| --- | ---: | ---: | ---: |
| Mean TTFT | 276.90 ms | 210.19 ms | -24.1% (-66.71 ms) |
| Mean end-to-end latency | 635.12 ms | 555.19 ms | -12.6% |
| Request throughput | 1.574 RPS | 1.800 RPS | +14.4% |
| Completion throughput | 25.18 tok/s | 28.80 tok/s | +14.4% |
| Strict Prefill render step | 87.33 ms | 1.69 ms | -98.1% |
| Measured render-cache hits | 0 / 6 | 6 / 6 | pass |
| Fallback / layered receive failures | 0 / 0 | 0 / 0 | pass |

The cache held one 3.07 MiB rendered artifact and was generation-fenced. This
is a same-workload control-path ablation, **not** an EPD-versus-colocated or
strong-system comparison, equal-resource claim, multi-node result, or RDMA
result. The raw local artifacts remain outside the committed public snapshot
until repeated-run and artifact-review gates are completed.

The same cache-off/cache-on control was also run at concurrency four with two
warmups and 16 measured requests per arm. Cache-on preserved exact 16/16
output equality and zero fallback/failure counters while reducing mean TTFT
469.05→379.13 ms (-19.2%), raising request throughput 4.485→4.935 RPS
(+10.0%), and raising completion throughput 71.77→78.97 tok/s (+10.0%). This
confirms that the benefit remains under this small concurrent workload; it is
still not a strong-system or equal-resource comparison.

### Scale-out capacity comparison

Source: [`artifacts/2026-07-10/benchmark_summary.json`](artifacts/2026-07-10/benchmark_summary.json).

This is a **4-GPU EPD scale-out capacity comparison against a 1-GPU colocated baseline**. It is **not** an equal-resource efficiency comparison.

| Metric | 1-GPU baseline | 2P2D EPD, 4 GPUs | Change |
| --- | ---: | ---: | ---: |
| Request throughput | 2.580 RPS | 3.685 RPS | +42.8% |
| Output throughput | 74.83 tok/s | 108.24 tok/s | +44.6% |
| Mean latency | 2917.69 ms | 2143.09 ms | -26.5% |
| Mean TTFT | 1271.71 ms | 1413.93 ms | +11.2% |
| p95 TTFT | 2006.39 ms | 1481.26 ms | -26.2% |
| P→D fallback / failure | N/A | 0 / 0 | pass |

### Agent state clone

Source: [`artifacts/2026-07-10/benchmark_summary.json`](artifacts/2026-07-10/benchmark_summary.json).

| Metric | Value |
| --- | ---: |
| Device | CPU |
| Pages / branches | 32 / 8 |
| Deep-copy mean | 1.6996 ms |
| Page-reference clone mean | 0.1694 ms |
| Mean speedup | 10.03× |
| Deep-copy bytes | 67,108,864 |
| Zero-copy clone bytes | 0 |
| CoW upper-bound bytes | 262,144 |
| States after release | 0 |
| Directory orphans after release | 0 |

## Evaluation scope

- The published comparison demonstrates EPD scale-out capacity: the 2P2D
  deployment uses four GPUs and the colocated baseline uses one GPU.
- Published transport measurements cover TCP direct peer-buffer and same-host
  `nvlink_intra`. The transport abstraction also exposes SHM and RDMA selection
  for environments with the required topology.
- The current development environment has no verified RDMA fabric. RDMA,
  multi-node, CUDA IPC/NVLink Omni, and real-Omni semantic gates therefore
  remain unavailable/skipped rather than being substituted with TCP or SHM.
- The 2026-07-14 strict refresh is a single-host TCP result. It does not
  validate a second node, RDMA registration/GID/remote-host setup, or a
  throughput improvement against a colocated baseline.
- The 2026-07-15 rendered-Prefill-cache ablation is also single-host TCP and
  concurrency-one only. It proves a strict control-path optimization against
  its cache-off arm; it does not replace the required equal-resource and
  strong-system comparisons.
- The archived 4-GPU-versus-1-GPU result is scale-out capacity evidence only.
  It contains 12/16 exact deterministic-text matches and does not satisfy the
  equal-resource, three-independent-run, 95% CI, A0--A9 ablation, or
  strong-system-baseline release gates. No general throughput-improvement
  claim is made from it.
- Agent clone measurements isolate descriptor/page-reference cloning and
  release correctness. Full methodology is documented in
  [`EVALUATION.md`](EVALUATION.md).
- `KVTransferManifestV2` is covered by CPU/control-plane regression gates in
  this checkout. The archived serving artifact predates its generation fence;
  it is not evidence of a producer-restart or two-node RDMA fence.

## Key evidence files

- [`DESIGN.md`](DESIGN.md) — architecture, interfaces, data flow, and lifecycle semantics.
- [`EVALUATION.md`](EVALUATION.md) — environment, methodology, baseline, metrics, and conclusions.
- [`artifacts/2026-07-10/benchmark_summary.json`](artifacts/2026-07-10/benchmark_summary.json) — compact public claim record with source digests.
- [`tests/`](tests/) — regression coverage for connectors, control plane, direct buffers, and artifact gates.
