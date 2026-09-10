# Mooncake Store Benchmarks

This directory contains benchmark tools for Mooncake Store internals.

## Allocation Strategy Benchmark

`allocation_strategy_bench` evaluates Store allocation behavior across segment
counts, replica counts, allocation strategies, and workload patterns.

Build the benchmark from an existing CMake build directory:

```bash
cmake --build build --target allocation_strategy_bench -j$(nproc)
```

### Size-Class Churn Fragmentation Benchmark

The `size_class_churn` workload measures fragmentation under mixed-size
KVCache-like allocation pressure. It pre-fills the simulated cluster when
`--prefill_pct` is set, then repeatedly allocates objects from weighted size
classes. On allocation failure it randomly evicts a fraction of live objects and
retries.

When prefill is enabled, the prefill attempt cap is auto-derived from target
utilization, total cluster capacity, weighted average object size, and replica
count, with a 5000-attempt minimum for small cases.

This is an allocation-strategy-layer benchmark. It complements the existing
`dsa` workload by adding explicit fragmentation sampling and configurable
weighted size-class patterns. It is not a replacement for `allocator_bench`,
which remains the low-level `OffsetAllocator` microbenchmark.

Run a small local validation:

```bash
./build/mooncake-store/benchmarks/allocation_strategy_bench \
  --workload=size_class_churn \
  --segment_capacity=1024 \
  --num_allocations=10000 \
  --prefill_pct=70
```

Run a larger baseline:

```bash
./build/mooncake-store/benchmarks/allocation_strategy_bench \
  --workload=size_class_churn \
  --segment_capacity=1024 \
  --num_allocations=100000 \
  --prefill_pct=80
```

Supported size-class patterns:

- `kv_mixed`: 4KB at 70%, 256KB at 20%, and 3.12MB at 10%.
- `dsa_pair`: 3.12MB KV pages at 50% and 643KB indexer entries at 50%.
- `rl`: RL data-plane offload stand-in. Sizes are drawn log-uniformly from
  `[--rl_min_kib, --rl_max_mib]` (default 64 KiB to 512 MiB), split into one
  equally weighted class per octave so the per-class breakdown shows which
  size decade fails first.
- `all`: run all three patterns.

#### RL offload workload, trace replay, and large-allocation probe

RL offload traffic differs from KV cache traffic in two ways that matter for
the allocator: object sizes span several orders of magnitude, and objects are
consumed and released at roughly the rate they are written. The following
flags model that and measure whether a large object can still be placed:

- `--size_class_release_prob=<p>`: before each measured allocation, release
  one random live object with probability `p`. `1.0` gives a
  free-one/allocate-one steady state that holds utilization near
  `--prefill_pct`; `0` (default) keeps the allocate-only behavior.
- `--probe_mib=<N>`: at every fragmentation sample point, attempt one `N` MiB
  single-replica allocation with no eviction retry, release it immediately,
  and report `success/attempts` together with the free space and largest free
  region observed at probe time. A low success rate with plenty of free space
  is the "20 GB free but 1.25 GiB fails" symptom.
- `--rl_trace_file=<path>`: replay recorded allocation sizes (one size in
  bytes per line, `#` starts a comment) in file order instead of a synthetic
  pattern. Size classes for the breakdown are derived from the trace, one per
  octave, weighted by observed counts.
- `--segment_counts=16`: override the default `1,10,100` segment matrix, for
  example to match a production cluster of sixteen 4 GiB segments.

Reproduce the RL symptom with the synthetic pattern:

```bash
./build/mooncake-store/benchmarks/allocation_strategy_bench \
  --workload=size_class_churn --size_class_pattern=rl \
  --segment_capacity=4096 --segment_counts=16 \
  --prefill_pct=70 --size_class_release_prob=1.0 \
  --num_allocations=20000 --probe_mib=1280
```

If no RL job is at hand, `mooncake-rl/examples/rl_dataproto_trace_driver.py`
replays the per-step DataProto data flow of a GRPO-style trainer (rollout put,
log-prob/reward/advantage appends, micro-batch reads, cleanup or eviction)
through the structured object API with configurable batch geometry, so the
object sizes and lifetimes seen by the master are the real ones for that
geometry.

Capture a real trace from a running master. `mooncake_master --v=1` logs one
`action=put_start_begin` line per PutStart with `key=` and `value_length=`,
one `action=remove_object` line per successful Remove, one
`action=evict_object` line per evicted object, and on allocation failure an
`action=put_start_alloc_failed` line with the requested size, total free
space, and largest free region across segments. Capture until cumulative
writes reach three to four times the cluster capacity so the pool is in
steady-state eviction, then extract sizes with:

```bash
python3 mooncake-store/benchmarks/extract_alloc_trace.py master.INFO \
  -o rl_sizes.txt --events rl_events.txt
```

The script prints a per-octave histogram of the captured sizes; cross-check it
against the `master_value_size_bytes` metric. Replay the sizes with:

```bash
./build/mooncake-store/benchmarks/allocation_strategy_bench \
  --workload=size_class_churn --rl_trace_file=rl_sizes.txt \
  --segment_capacity=4096 --segment_counts=16 \
  --prefill_pct=70 --size_class_release_prob=1.0 \
  --num_allocations=20000 --probe_mib=1280
```

Replay the event log with the real object lifetimes instead of sampling
sizes. Puts allocate, removes and evicts free, and there is no eviction retry,
so every failed put is reported together with the free space and largest free
region at that moment, mirroring `put_start_alloc_failed`:

```bash
./build/mooncake-store/benchmarks/allocation_strategy_bench \
  --workload=size_class_churn --rl_trace_events=rl_events.txt \
  --segment_capacity=4096 --segment_counts=16 --replica_counts=1 \
  --size_class_strategies=random,free_ratio_first,best_fit,hybrid,reserved \
  --probe_mib=1280
```

`--trace_events_repeat=N` replays a short log several times back to back.
`--size_class_strategies` selects the placement policies to compare:

- `random`, `free_ratio_first`, `best_fit`: the production strategies
  (`best_fit` places the object in the segment whose largest free region is
  the smallest one that still fits, so large holes are preserved).
- `largest_hole_first`: bench-only; always pick the segment with the largest
  free region.
- `hybrid`: bench-only; `best_fit` below `--large_object_mib`,
  `largest_hole_first` at or above it.
- `reserved`: bench-only; the last `--reserved_segments` segments accept only
  objects of at least `--large_object_mib`, the others only smaller ones.

The bench-only strategies rank every segment (no candidate sampling and no
random fallback) so that the effect of the placement rule is measured in
isolation; they are experiments, not proposals for the master as written.
`best_fit` is the production `BestFitAllocationStrategy`, selectable on the
master with `--allocation_strategy=best_fit`.

The driver also has `--api put_parts`, which stores each field as one whole
object through `put_parts`, reads it back with `get_buffer`, and frees it with
a forced `remove`, for data planes that use only those calls.

Key output columns:

- `Throughput`, `Avg(ns)`, `P50(ns)`, `P90(ns)`, and `P99(ns)` measure
  allocation performance.
- `Frag_avg`, `Frag_p50`, `Frag_p90`, and `Frag_p99` summarize sampled
  fragmentation ratios.
- `LargestFreeMB` shows the final largest contiguous free region.
- `Evictions` counts fail-triggered eviction rounds during measurement.
- `Full/Partial/Fail/Total` reports allocation outcomes. Only results with
  `result->size() == replica_num` count as full success; shorter replica
  results are counted as partial allocations.
- `Probe summary` (when `--probe_mib` is set) reports probe success,
  free space at probe time, and the p10/p50/max largest free region.
- `released=` in the fragmentation summary counts objects released by
  `--size_class_release_prob`.

Fragmentation is computed per `OffsetBufferAllocator` and then averaged by free
space:

```text
1 - largest_free_region / total_free_space
```

The weighted average avoids treating free space in different Store segments as
one mergeable region. `LargestFreeMB` still reports the final largest contiguous
free region across all segments.

The benchmark also prints a one-line `Prefill summary`, `Fragmentation summary`,
and `Size-class breakdown` after each result row, so reviewers can read the
actual prefill utilization, fragmentation, and per-size-class latency numbers
without manually deriving them from the table.
