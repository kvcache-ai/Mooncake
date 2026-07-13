# Mooncake EPD Submission

## Track

**KVCache-centric disaggregation and coordinated scheduling for multimodal and
Agent inference.**

Mooncake EPD extends Mooncake and vLLM with a real Encoder-Prefill-Decode
serving pipeline, reusable multimodal Hidden State, layered KV transfer, and
state-aware Agent scheduling. The implementation is integrated into the
Mooncake repository rather than maintained as a standalone demo.

## Completed foundation tasks

### 1. Three-stage EPD serving in vLLM

- Vision Encoder output is represented as a validated `FeatureBundle`.
- Mooncake direct FeatureHandles move the bundle from Encoder to Prefill.
- Prefill injects precomputed `image_embeds` into vLLM and skips the Vision
  Encoder when the handle is valid.
- Prefill-generated paged KV cache is transferred to Decode through the vLLM
  save/load connector path using grouped, layered Mooncake transfer.
- Real Qwen3-VL launchers and strict no-fallback gates validate the complete
  E-to-P-to-D path.

### 2. Agent State Cloning

- Agent branches clone immutable KV page descriptors by incrementing page
  references instead of copying complete tensors.
- Page-level Copy-on-Write materializes only pages modified by a branch.
- Owner-directory, workflow-registry, handoff, and release metadata preserve
  lifecycle visibility across nodes and steps.
- Clone and release gates verify copied bytes, reference counts, and orphan-free
  cleanup.

### 3. Qwen-VL end-to-end demonstration

- A real-model benchmark runner launches Encoder, Prefill, Decode, proxy, and
  Mooncake components.
- A colocated vLLM baseline and a 2P2D EPD runner share workload metadata and
  fail-closed comparison gates.
- Public artifacts record throughput, TTFT, direct-path counters, cache reuse,
  topology affinity, and correctness signals.

## Completed advanced tasks

### Worker-level transfer optimization

- Registered-pointer peer-buffer transfer avoids repeated hot-path memory
  registration for persistent KV regions.
- Layer grouping reduces metadata handshakes and aligns transfer with vLLM
  layer save/load events.
- TCP provides a portable direct path; SHM, RDMA, and intra-node NVLink
  selections are exposed through the same transport policy and telemetry.
- Prefill-to-Decode affinity maps workers to favorable physical links while
  preserving load-aware routing.

### Agent PD Disaggregation scheduling

- `THINKING`, `INTERACTIVE`, and `HYBRID` requests use stage-specific worker
  pools and scoring.
- Admission uses queue depth, service rate, GPU utilization, deadlines, and
  rho-based backpressure.
- Tool-wait, handoff, commit, rollback, and release transitions are observable
  through workflow and connector metrics.

### Hidden State Prefix Caching

- Stable multimodal keys identify reusable Vision Hidden State.
- Event-driven prefetch coalesces concurrent fetches and bounds queue pressure.
- vLLM's multimodal hot path consumes precomputed hidden state, including
  Qwen3-VL DeepStack intermediates and `grid_thw` metadata.
- Strict validation checks model, processor, shape, dtype, checksum, and
  storage-root compatibility before reuse.

### Upstream-ready contribution structure

The implementation follows Mooncake ownership boundaries and is organized as
four focused review areas inside one integration PR:

1. Transfer Engine and intra-node transport.
2. Encoder-to-Prefill FeatureHandle and hidden-state integration.
3. Serving, Agent state, scheduler, and control plane.
4. Benchmarks, gates, tests, and documentation.

## Core innovations

- **State-centric EPD protocol**: tensor data, ownership metadata, workflow
  version, and transfer lifecycle are coordinated instead of routed as
  unrelated RPC payloads.
- **Prefill-owned feature destinations**: Encoder writes into registered target
  buffers described by lightweight FeatureHandles, reducing serialization and
  allocation work on the hot path.
- **Layer-aware KV handoff**: transfer descriptors preserve layer grouping and
  integrate with vLLM's producer/consumer lifecycle.
- **Reference-based Agent branching**: immutable KV pages can be shared across
  parallel reasoning branches, with bounded CoW materialization on mutation.
- **Fail-closed performance evidence**: benchmark gates distinguish real EPD,
  direct transfer, cache reuse, resource scope, and output evidence before a
  result is published.

## Mooncake integration

| Mooncake area | Contribution |
| --- | --- |
| `mooncake-transfer-engine/` | Registered-pointer transfer and intra-node synchronization improvements |
| `mooncake-wheel/` | Store service API lifecycle updates and tests |
| `sitecustomize.py` | Explicitly enabled vLLM hot-path adapters for generated EPD workers |
| `mooncake_epd/core/` | State, transfer, connector, control-plane, and scheduling semantics |
| `mooncake_epd/scripts/` | Real serving, baseline, dataset, matrix, and gate runners |
| `mooncake_epd/tests/` | Unit, integration, control-plane, and artifact-gate coverage |

## Source and documentation

- Upstream project: <https://github.com/kvcache-ai/Mooncake>
- Submission repository: <https://github.com/Pinoeer-kingxi/Mooncake>
- Submission branch: `feature/epd-vllm-multimodal-agent`
- Main upstream PR: <https://github.com/kvcache-ai/Mooncake/pull/2836>
- Overview: [`README.md`](README.md)
- Design: [`DESIGN.md`](DESIGN.md)
- Evaluation: [`EVALUATION.md`](EVALUATION.md)
- Pull request text: [`PR_DESCRIPTION.md`](PR_DESCRIPTION.md)

## Run and verify

Run from the Mooncake repository root after installing a compatible Python
3.10, Mooncake, CUDA, vLLM, Transformers, and Qwen-VL environment.

```bash
export PYTHONPATH=$PWD
export MOONCAKE_EPD_VENV_ROOT="${MOONCAKE_EPD_VENV_ROOT:-$VIRTUAL_ENV}"
export MOONCAKE_EPD_MODEL="${MOONCAKE_EPD_MODEL:-models/Qwen3-VL-8B-Instruct}"

python mooncake_epd/demo/vllm_integration.py
bash mooncake_epd/scripts/start_vllm_disagg.sh
```

The launcher prints the generated metadata, master, Prefill, Decode, proxy,
and verification commands. Each service is started in a separate terminal.

Regression verification:

```bash
PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 \
MOONCAKE_EPD_ENABLE_VLLM_PATCHES=0 \
python -m pytest -q mooncake_epd/tests

PYTHONPATH=$PWD/mooncake-wheel \
PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 \
python -m pytest -q mooncake-wheel/tests/test_mooncake_store_service_api.py
```

The full benchmark commands use configurable relative paths and GPU lists in
[`EVALUATION.md`](EVALUATION.md).

## Experiment summary

### Real multimodal EPD

| Metric | Result |
| --- | ---: |
| Request throughput | 8.143 RPS |
| Output throughput | 232.08 tok/s |
| Mean TTFT | 288.82 ms |
| P95 TTFT | 344.01 ms |
| HTTP / EPD route success | 16/16 / 16/16 |
| P-to-D peer-buffer traffic | 46 batches / 301,989,888 bytes |
| Transfer fallback / send / receive failures | 0 / 0 / 0 |

### Scale-out capacity

The comparison below uses four GPUs for 2P2D EPD and one GPU for the colocated
baseline. It demonstrates scale-out capacity rather than equal-resource
efficiency.

| Metric | 1-GPU colocated | 4-GPU 2P2D EPD | Change |
| --- | ---: | ---: | ---: |
| Request throughput | 2.580 RPS | 3.685 RPS | +42.8% |
| Output throughput | 74.83 tok/s | 108.24 tok/s | +44.6% |
| Mean latency | 2917.69 ms | 2143.09 ms | -26.5% |
| P95 TTFT | 2006.39 ms | 1481.26 ms | -26.2% |

### Agent State Cloning

| Metric | Result |
| --- | ---: |
| Mean clone speedup | 10.03x |
| Deep-copy bytes | 67,108,864 |
| Page-reference clone bytes | 0 |
| CoW upper-bound bytes | 262,144 |
| States / orphan blocks after release | 0 / 0 |

## Verification summary

| Verification | Result |
| --- | ---: |
| Full EPD regression | 371 passed, 1 skipped |
| Mooncake Store service API | 72 passed |
| Runtime/security boundary subset | 52 passed |
| Strict real-EPD gate | Pass |
| Python compile, JSON validation, diff whitespace | Pass |

The compact public evidence record is
[`artifacts/2026-07-10/benchmark_summary.json`](artifacts/2026-07-10/benchmark_summary.json).

## Community contribution

This submission contributes reusable Mooncake infrastructure rather than a
single fixed deployment: direct buffer lifecycle, layered transfer semantics,
vLLM hot-path integration, Agent state ownership, scheduling/backpressure,
artifact gates, and reproducible benchmark entry points. The primary PR keeps
the end-to-end contract together while subsystem-oriented commits support
focused upstream review; smaller follow-up PRs can be extracted when requested.
