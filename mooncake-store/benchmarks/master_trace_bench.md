# `master_trace_bench`

`master_trace_bench` benchmarks the Mooncake Master metadata and allocator path
using deterministic, SGLang-derived `ModelProfile` workloads. It requires no
inference engine or traffic recorder at runtime, scales linearly to 100M+ keys
via multi-stream cloning (`--amplify`), and ships with an A/B regression runner
(`master_trace_runner.py`) and gate (`master_trace_gate.py`).

## Scope

- **What it measures**: Master RPC, metadata index, and virtual Memory Segment
  allocator throughput/latency under causal `BatchExistKey` →
  `BatchGetReplicaList` / `BatchPutStart` + `BatchPutEnd` workloads, including
  100M-key scale (`--amplify=94`) and optional eviction churn.
- **What it does not measure**: KV payload data-plane transfer (segments are
  virtual address ranges). For end-to-end Store data-plane benchmarks, see
  [store_kv_bench.md](store_kv_bench.md).

## Quick Start

### 1. Build

Configure CMake with `-DBUILD_BENCHMARK=ON` (if not already enabled) and build
both `mooncake_master` and `master_trace_bench`:

```bash
cmake --build build --target mooncake_master master_trace_bench -j
```

### 2. List available model profiles

`master_trace_bench` auto-detects `mooncake-store/benchmarks/profiles/` from
any working directory:

```bash
./build/mooncake-store/benchmarks/master_trace_bench --list_models
```

### 3. Run an A/B regression check (base vs. current Master)

`master_trace_runner.py` starts a fresh loopback Master per run, interleaves
base and current runs, writes medians, and evaluates the regression gate:

```bash
python3 mooncake-store/benchmarks/master_trace_runner.py \
  --base-master ./build-base/mooncake-store/src/mooncake_master \
  --current-master ./build/mooncake-store/src/mooncake_master \
  --bench ./build/mooncake-store/benchmarks/master_trace_bench \
  --result-dir /tmp/trace-run \
  --model qwen3.5-9b --amplify 2 --speed 0 --runs 3
```

## Common Scenarios

### 1. Single-clone or small multi-clone standalone run

Start a Master on loopback:

```bash
./build/mooncake-store/src/mooncake_master \
  --rpc_port=50051 --rpc_address=127.0.0.1 \
  --metrics_port=51051 --metrics_host=127.0.0.1 \
  --rpc_thread_num=16 --memory_allocator=offset \
  --eviction_high_watermark_ratio=1.0 \
  --allow_evict_soft_pinned_objects=false
```

Run in unpaced saturation mode (`--speed=0`, recommended for throughput/A/B
comparisons) or paced mode (`--speed=1`, preserves synthesized arrival cadence):

```bash
./build/mooncake-store/benchmarks/master_trace_bench \
  --master=127.0.0.1:50051 --model=qwen3.5-9b \
  --seed=42 --amplify=1 --speed=0
```

For multi-stream concurrency (`--amplify=2` or `3`), add `--clone_spread_us=1000`
so clones do not issue identical batches in lockstep.

### 2. 100M-key Memory Segment run (`--amplify=94`)

With `qwen3.5-9b` (`1,073,077` keys/clone), `--amplify=94` creates
**100,869,238 keys** across 94 disjoint 64 GiB virtual Memory Segments
(5.88 TiB virtual capacity). Start Master with 64 RPC threads on a host with
at least ~128 GiB free RAM (~116 GiB peak Master RSS observed on Linux x86_64):

```bash
./build/mooncake-store/src/mooncake_master \
  --rpc_port=50051 --rpc_address=127.0.0.1 \
  --metrics_port=51051 --metrics_host=127.0.0.1 \
  --rpc_thread_num=64 --memory_allocator=offset \
  --eviction_high_watermark_ratio=1.0 \
  --allow_evict_soft_pinned_objects=false

./build/mooncake-store/benchmarks/master_trace_bench \
  --master=127.0.0.1:50051 --model=qwen3.5-9b \
  --seed=42 --amplify=94 --speed=0 \
  --clone_spread_us=1000 --rpc_timeout_ms=60000 \
  --result_file=/tmp/qwen3.5-9b-94clones.json
```

Progress is logged to stderr every 30 seconds (`--progress_interval_sec`).
An unpaced 94-clone run takes ~27 minutes on a 180-core Linux host; graceful
`mooncake_master` shutdown after 100M keys can take several minutes, so
`master_trace_runner.py` escalates to `SIGKILL` after `--stop-timeout`
(default 60 s) to avoid hanging multi-run sweeps.

### 3. Eviction-churn A/B regression

With `qwen3.5-9b`'s default 24 KiB object size, each clone writes ~24.6 GiB of
virtual objects. Shrinking `--segment-bytes` to 16 GiB (`17179869184`) and
setting `--eviction-high-watermark-ratio 0.7` together with `--allow-eviction`
triggers continuous Master eviction during mixed reads and writes:

```bash
python3 mooncake-store/benchmarks/master_trace_runner.py \
  --base-master ./build-base/mooncake-store/src/mooncake_master \
  --current-master ./build/mooncake-store/src/mooncake_master \
  --bench ./build/mooncake-store/benchmarks/master_trace_bench \
  --result-dir /tmp/trace-evict \
  --model qwen3.5-9b --amplify 2 --speed 0 --runs 3 \
  --segment-bytes 17179869184 \
  --eviction-high-watermark-ratio 0.7 \
  --allow-eviction
```

When `--allow_eviction=true`:
- `BatchExistKey` filters out evicted keys before issuing `BatchGetReplicaList`.
- Fully-evicted read batches are skipped and counted in `skipped_ops`.
- Race-window `OBJECT_NOT_FOUND` errors during `Get` are counted in
  `evicted_misses` rather than fatal `missing`, while real RPC/transport errors
  still fail the run.

## Built-in Profile: `qwen3.5-9b`

`mooncake-store/benchmarks/profiles/qwen3.5-9b.json` encodes empirical
distributions extracted offline from real Qwen3.5-9B SGLang HiCache runs
(TP=1, 64-token pages, 96 unique 3.8K-token prompts + 16 reused prefixes):

- **Geometry & sizing**: 32 layers (8 full-attention), 4 KV heads, 256 head dim,
  bfloat16, 2 MiB KV page, 64-byte hash-length keys, and 24 KiB (`24576` B)
  default allocated object size (matching the captured `44.46 GiB / 1,851,985`
  keys average; override with `--object_bytes`).
- **Batch-size PMFs**:
  - `put_batch_pmf`: 256-page chunks (91.4%) plus tail chunks (2, 114, 116, 192,
    244).
  - `get_batch_pmf`: 256-page chunks (93.5%) plus 2, 128, 88.
  - `prefix_probe_pmf`: prefix-tree `BatchExistKey` bursts (7,578–7,586 keys,
    85% prefix hit ratio).
- **Causal mixed read/write stream**:
  - **Miss + Write**: `BatchExistKey` on fresh keys (miss) → `BatchPutStart` +
    `BatchPutEnd`. On partial `BatchPutStart` failure, all allocated keys across
    the full response are revoked; on partial `BatchPutEnd` failure, only
    failed keys are revoked.
  - **Hit + Read**: `BatchExistKey` on live keys with hot-prefix skew (80% of
    reads target the hot 15% prefix) → `BatchGetReplicaList`.
  - **Prefix-tree Probe**: large `BatchExistKey` batches mixing hot live keys
    and uncached tail keys.
- **Burst arrival rhythm**: intra-burst p50 = 2 ms / p99 = 15 ms with 663 ms
  inter-prompt burst gaps (1.5% probability).

With a fixed `--seed` (default `42`), each clone `i` derives its own
deterministic stream seed (`SplitMix64(seed, i)`) so concurrent clones
naturally interleave `Put`, `Get`, and large `Exist` probes without lockstep
spikes, while every clone writes the exact `keys_per_clone` into its own
disjoint client UUID, segment UUID/address range, and key prefix
(`<key_prefix>/<clone>/...`).

## Key CLI Options

### `master_trace_bench`

| Flag | Default | Description |
|---|---|---|
| `--master` | `127.0.0.1:50051` | Master RPC `host:port` |
| `--model` | `qwen3.5-9b` | Profile name under `profiles/<model>.json` |
| `--profile_dir` | *(auto-detected)* | Override profile directory |
| `--list_models` | `false` | Print available model names and exit |
| `--seed` | `42` | Deterministic PRNG seed |
| `--amplify` | `1` | Number of concurrent clone streams (`1..256`; `94` ≈ 100M keys) |
| `--speed` | `1` | `0` = unpaced max throughput; `1` = synthesized cadence; `>1` = compressed |
| `--clone_spread_us` | `0` | Deterministic start offset (`clone * spread`) between clones |
| `--object_bytes` | `0` | Allocated bytes per object (`0` uses profile default) |
| `--segment_bytes` | `64 GiB` | Virtual capacity per clone Memory Segment |
| `--rpc_timeout_ms` | `60000` | Per-request Master RPC timeout (`0` keeps env default) |
| `--allow_eviction` | `false` | Tolerate evicted reads on causal `Exist` → `Get` |
| `--progress_interval_sec` | `30` | Stderr progress log interval (`0` disables) |
| `--result_file` | `""` | Also write the JSON summary to this path |

Exit codes: `0` = complete run with 0 errors/missing reads, `1` = runtime/RPC
failure, `2` = invalid flags or profile.

### `master_trace_runner.py` & `master_trace_gate.py`

- **Runner defaults**: `--gate`, `--profile-dir`, and `--workdir` default
  automatically; pass `--bench` when both sides share one benchmark binary (or
  `--base-bench` / `--current-bench` separately). `--runs` defaults to `5` (use
  `3` for quick local checks; must be odd). `--rpc-threads` defaults to `32`,
  `--speed` to `0`, `--clone-spread-us` to `1000`, and `--master-arg` can be
  repeated to forward extra Master flags.
- **Result directory contents**:
  - `baseline.json`, `current.json`: aggregated median reports (`min` for
    completion counts and `max` for failure counts so a bad run is never masked).
  - `gate.json`: pass/fail verdict with absolute throughput (`baseline_ops_per_second`,
    `current_ops_per_second`), `rps_drop_pct`, per-op `baseline_p50_us` /
    `current_p50_us` / `p50`/`p95`/`p99` deltas, and `gated_p50_ops` /
    `advisory_p50_ops`.
  - `{base,current}-{i}.json`, `{base,current}-master-{i}.log`,
    `{base,current}-bench-{i}.log`: per-run raw JSON and logs.
- **Gate rules** (`master_trace_gate.py`, exit `0` pass / `1` regression / `2`
  invalid or mismatched workload plan):
  - Rejects any mismatch across `model`, `profile_hash`, `seed`, `amplify`,
    `object_bytes`, `key_bytes`, `keys_per_clone`, `speed`, `clone_spread_us`,
    `segment_bytes`, or `allow_eviction`.
  - Fails on any `errors`, `missing`, `failed_ops`, incomplete trace, or
    missing keys.
  - Fails if `ops_per_second` drops by more than `--max-rps-drop-pct` (default
    `10.0%`).
  - Fails if `p50` increases by more than `--max-p50-increase-pct` (default
    `10.0%`) for any op kind with at least `--min-latency-samples` samples
    (default `30`). With `qwen3.5-9b`, `put`, `get`, and `exist` all exceed 30
    samples even at `--amplify=1`. `p95`, `p99`, and low-sample ops are
    reported as advisory.

## Adding a New Model Profile

Drop a plain JSON file at `mooncake-store/benchmarks/profiles/<model>.json`
with `format: "mooncake-master-profile-v1"`, `model: "<model>"`,
`desensitized: true`, `key_bytes >= 24`, `object_bytes > 0`,
`keys_per_clone > 0`, `read_hit_ratio + prefix_probe_ratio < 0.95`,
`hot_key_fraction > 0`, non-empty `put_batch_pmf` / `get_batch_pmf` /
`prefix_probe_pmf` (`{"size", "weight"}` arrays), and `arrival_us`
(`intra_burst_p50`, `intra_burst_p99 >= intra_burst_p50`, `burst_gap_prob`,
`burst_gap_us`). No C++ recompilation is needed; verify with `--list_models`.

## Tool Unit Tests

Run the self-contained Python unit tests (zero external dependencies):

```bash
python3 -m unittest -v mooncake-store/benchmarks/test_master_trace_tools.py
```
