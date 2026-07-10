# Mooncake EPD: Multimodal and Agent-State Disaggregation

Mooncake EPD is a production-oriented **Encoder-Prefill-Decode (EPD)
disaggregation** implementation integrated directly into the Mooncake source
tree. It extends Mooncake and vLLM for multimodal serving and stateful Agent
workflows through three coordinated data paths:

1. **E→P hidden-state transfer**: a dedicated Encoder stage produces Qwen-VL visual hidden states and transfers them to Prefill through FeatureHandle/direct peer-buffer paths.
2. **P→D layered KV transfer**: Prefill generates paged KV cache and hands it to Decode through a repo-local vLLM `MooncakeConnector` with grouped/layered transfer metadata.
3. **Agent state reuse**: workflow state, KV page descriptors, feature handles, clone/release lifecycle, and scheduler metadata are tracked as first-class control-plane objects.

The implementation reuses Mooncake Transfer Engine, Mooncake Store, vLLM's KV
connector lifecycle, and repository-native tests. Public performance summaries
are backed by
[`artifacts/2026-07-10/benchmark_summary.json`](artifacts/2026-07-10/benchmark_summary.json).

## Public links

- Upstream Mooncake repository: <https://github.com/kvcache-ai/Mooncake>
- Public submission repository: <https://github.com/Pinoeer-kingxi/Mooncake>
- EPD branch: <https://github.com/Pinoeer-kingxi/Mooncake/tree/feature/epd-vllm-multimodal-agent>
- Main upstream PR: [Mooncake EPD: Multimodal and Agent-State Disaggregation](https://github.com/kvcache-ai/Mooncake/pull/2836)

The subsystem-oriented review guide for the single integration PR is
documented in `PR_DESCRIPTION.md`.

## Task coverage

### Foundation tasks

| Task | Delivered capability |
| --- | --- |
| vLLM EPD three-stage serving | A real Encoder service transfers Vision Hidden State to Prefill through Mooncake; Prefill transfers paged KV cache to Decode through the vLLM save/load connector path. |
| Agent State Cloning | Agent branches share immutable KV page descriptors with reference counting and page-level Copy-on-Write instead of deep-copying complete state. |
| Qwen-VL end-to-end demo | Real-model launchers, strict direct-path gates, baseline runners, and benchmark artifacts cover Qwen3-VL serving. |

### Advanced tasks

| Task | Delivered capability |
| --- | --- |
| Worker-level stage transfer | Reusable registered-pointer, peer-buffer, grouped-transfer, and topology-affinity primitives support low-latency stage handoff and provide an integration base for AR/Generation/Diffusion pipelines. |
| Agent PD scheduling | Thinking, interactive, and hybrid tasks are routed with stage-specific pools, deadline/rho-aware admission, topology affinity, and measurable backpressure. |
| Hidden State Prefix Caching | Stable multimodal keys, FeatureHandles, vLLM hidden-state cache integration, event-driven prefetch, and precomputed `image_embeds` injection enable Vision Encoder skip on cache hits. |
| Upstream contribution | The implementation follows Mooncake's tree layout and is packaged as reviewable Transfer Engine, FeatureHandle, serving/control-plane, and benchmark/documentation changes. |

## What is implemented

| Area | Current implementation evidence |
| --- | --- |
| EPD pipeline | `core/epd_pipeline.py`, `core/epd_workers.py`, `scripts/run_real_qwenvl_epd_demo.py`, `scripts/run_vllm_online_direct_e2e.py` |
| Qwen-VL Encoder FeatureBundle | `core/epd_workers.py`, `core/state/feature_store.py`, `core/state/direct_feature_buffer.py`, `scripts/epd_encoder_service.py`, `scripts/direct_feature_buffer_service.py` |
| P→D layered KV transfer | `core/control/vllm_mooncake_connector.py`, `core/control/vllm_transfer_primitives.py`, `demo/vllm_integration.py` |
| Direct peer-buffer transport planning | `core/transfer/engine.py`, `tests/test_transfer_engine_peerbuffer_plan.py` |
| Serving control plane | `core/control/serving_controller.py`, `core/control/kv_directory.py`, `core/state/workflow_registry.py` |
| Agent PD routing/backpressure | `agent/coordination/scheduler.py`, serving metrics in `ServingControlPlane` |
| Agent state clone / CoW descriptors | `agent/state_clone.py`, `core/state/kv_state_store.py`, `core/state/page_manager.py` |
| MM cache and event prefetch | `core/state/mm_store.py`, `core/state/vllm_mm_hidden_cache.py`, `core/state/vllm_feature_handle_provider.py` |
| Artifact gates | `scripts/check_real_epd_gate.py`, `scripts/check_epd_artifact_gates.py` |

## Core contributions

- **Multimodal EPD serving path**: separates visual encoding, LLM prefill, and decode for Qwen3-VL-style workloads while preserving OpenAI-compatible vLLM request handling.
- **Direct FeatureHandle path for E→P**: Prefill allocates destination feature buffers; Encoder publishes hidden-state tensor bytes through Mooncake direct peer-buffer metadata; Prefill validates descriptors before skipping vision recompute.
- **Layered P→D KV transfer**: vLLM `MooncakeConnector` extension batches KV descriptors by layer group, records peer-buffer backend metrics, and enforces strict no-fallback gates for measured runs.
- **State-centric Agent runtime**: versioned workflow records, KV directory entries, handoff IDs, and WAL-backed registry events make Agent fork/handoff/release observable.
- **Zero-copy Agent clone semantics**: same-node KV clone increments page references instead of copying tensor bytes; CoW materializes only modified pages.
- **Type-aware scheduler and backpressure**: thinking, interactive, and hybrid Agent hints influence Prefill/Decode routing; rho/deadline backpressure and reject counters are exposed in metrics.

## Dependencies

Minimal Python dependencies from `requirements.txt`:

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
python -m pip install -r mooncake_epd/requirements.txt
```

`mooncake_epd` is a Python module inside this repository. For local development and tests, use `PYTHONPATH=$PWD` from the repository root.

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

Current local verification: EPD `371 passed, 1 skipped`; Mooncake Store API
`72 passed`.

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
- Agent clone measurements isolate descriptor/page-reference cloning and
  release correctness. Full methodology is documented in
  [`EVALUATION.md`](EVALUATION.md).

## Key evidence files

- [`DESIGN.md`](DESIGN.md) — architecture, interfaces, data flow, and lifecycle semantics.
- [`EVALUATION.md`](EVALUATION.md) — environment, methodology, baseline, metrics, and conclusions.
- [`artifacts/2026-07-10/benchmark_summary.json`](artifacts/2026-07-10/benchmark_summary.json) — compact public claim record with source digests.
- [`tests/`](tests/) — regression coverage for connectors, control plane, direct buffers, and artifact gates.
