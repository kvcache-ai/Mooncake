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
- `all`: run both patterns.

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

## Variable-Length Allocation Benchmark

`variable_length_allocation_bench` covers the workloads whose object sizes are
data-dependent instead of fixed. The common KV cache workload hands the
allocator one object size, so segment choice only has to balance capacity. A
variable-length workload (RL data-plane offload is the one this was written for)
hands it sizes spanning several octaves, and there segment choice also decides
whether a contiguous region large enough for the next big object still exists.
The fixed-size workloads stay in `allocation_strategy_bench`.

Build it from an existing CMake build directory:

```bash
cmake --build build --target variable_length_allocation_bench -j$(nproc)
```

The benchmark runs mixed-size churn (optionally trace-driven), replay of a
recorded allocation event log, and a large-allocation probe, sweeping
`--strategies` x `--replica_counts` x `--segment_counts` x `--capacity_skew`.

Cluster shape and workload flags:

- `--segment_capacity=<MiB>`, `--segment_counts=16`: the simulated cluster. The
  default segment matrix is `1,10,100`; override it to match a production
  cluster, for example sixteen 4 GiB segments.
- `--capacity_skew=uniform|skewed|both`: whether segments are all equal, half
  at base + 50% and half at base - 50%, or both cases in turn.
- `--size_pattern=octave`: draw sizes log-uniformly from
  `[--min_object_kib, --max_object_mib]` (default 64 KiB to 512 MiB), split into
  one equally weighted class per octave, so the per-class breakdown shows which
  size decade fails first. This is the most hostile mix for large objects.
- `--release_prob=<p>`: before each measured allocation, release one random
  live object with probability `p`. `1.0` gives a free-one/allocate-one steady
  state that holds utilization near `--prefill_pct`; `0` (default) keeps the
  allocate-only behavior.
- `--probe_mib=<N>`: at every fragmentation sample point, attempt one `N` MiB
  single-replica allocation with no eviction retry, release it immediately,
  and report `success/attempts` together with the free space and largest free
  region observed at probe time. A low success rate with plenty of free space
  is the "20 GB free but 1.25 GiB fails" symptom.

Reproduce that symptom with the synthetic pattern:

```bash
./build/mooncake-store/benchmarks/variable_length_allocation_bench \
  --segment_capacity=4096 --segment_counts=16 --replica_counts=1 \
  --prefill_pct=70 --release_prob=1.0 \
  --num_allocations=20000 --probe_mib=1280
```

### Replaying a recorded trace

If no RL job is at hand, `mooncake-rl/examples/rl_dataproto_trace_driver.py`
replays the per-step DataProto data flow of a GRPO-style trainer (rollout put,
log-prob/reward/advantage appends, micro-batch reads, cleanup or eviction)
through the structured object API with configurable batch geometry, so the
object sizes and lifetimes seen by the master are the real ones for that
geometry. The driver also has `--api put_parts`, which stores each field as one
whole object through `put_parts`, reads it back with `get_buffer`, and frees it
with a forced `remove`, for data planes that use only those calls.

Capture a real trace from a running master. `mooncake_master --v=1` logs one
`action=put_start_allocated` line with `key=` and `value_length=` every time a
request actually reserves space, one `action=remove_object` line per successful
Remove, one `action=evict_object` line per evicted object, and on allocation
failure an `action=put_start_alloc_failed` line with the requested size, total
free space, and largest free region across segments.

Extract from `put_start_allocated`, not from the `action=put_start_begin` line
PutStart logs on entry: `put_start_begin` precedes the duplicate-key and quota
checks, so a request rejected with `OBJECT_ALREADY_EXISTS` emits a begin record
without allocating, and replaying it invents pressure the master never saw.
`put_start_begin` also misses UpsertStart, which allocates through the same
path without logging one.

Capture until cumulative writes reach three to four times the cluster capacity
so the pool is in steady-state eviction, then extract sizes with:

```bash
python3 mooncake-store/benchmarks/extract_alloc_trace.py master.INFO \
  -o rl_sizes.txt --events rl_events.txt
```

The script prints a per-octave histogram of the captured sizes; cross-check it
against the `master_value_size_bytes` metric. Replay the sizes with
`--trace_sizes`, which draws from the recorded sizes in file order and derives
the breakdown classes from the trace, one per octave, weighted by observed
counts:

```bash
./build/mooncake-store/benchmarks/variable_length_allocation_bench \
  --trace_sizes=rl_sizes.txt \
  --segment_capacity=4096 --segment_counts=16 --replica_counts=1 \
  --prefill_pct=70 --release_prob=1.0 \
  --num_allocations=20000 --probe_mib=1280
```

Replay the event log with the real object lifetimes instead of sampling sizes.
Puts allocate, removes and evicts free, and there is no eviction retry, so
every failed put is reported together with the free space and largest free
region at that moment, mirroring `put_start_alloc_failed`:

```bash
./build/mooncake-store/benchmarks/variable_length_allocation_bench \
  --trace_events=rl_events.txt \
  --segment_capacity=4096 --segment_counts=16 --replica_counts=1 \
  --strategies=random,free_ratio_first,best_fit,hybrid,reserved \
  --probe_mib=1280
```

`--trace_events_repeat=N` replays a short log several times back to back.
Because `random` and `free_ratio_first` sample candidate segments and `best_fit`
breaks ties randomly, failure counts move run to run; compare medians over a few
repetitions rather than single runs.

### Strategies

`--strategies` selects the placement policies to compare:

- `random`, `free_ratio_first`, `best_fit`: the production strategies
  (`best_fit` places the object in the segment whose largest free region is
  the smallest one that still fits, so large holes are preserved). It is the
  production `BestFitAllocationStrategy`, selectable on the master with
  `--allocation_strategy=best_fit`.
- `largest_hole_first`: bench-only; always pick the segment with the largest
  free region.
- `hybrid`: bench-only; `best_fit` below `--large_object_mib`,
  `largest_hole_first` at or above it.
- `reserved`: bench-only; the last `--reserved_segments` segments accept only
  objects of at least `--large_object_mib`, the others only smaller ones.
- `best_fit_bucketed`: bench-only; segments whose slack falls in the same
  `--best_fit_bucket_mib` bucket are treated as equal and picked at random,
  trading a little contiguity for less traffic concentration.

The bench-only strategies rank every segment (no candidate sampling and no
random fallback) so that the effect of the placement rule is measured in
isolation; they are experiments, not proposals for the master as written.

### Output

The result table and the `Prefill summary`, `Fragmentation summary` and
`Size-class breakdown` lines are the same as in the allocation strategy
benchmark above, including the weighted fragmentation ratio. Additionally:

- `Failure summary` reports, for the failed allocations, the requested size
  min/p50/max plus the free space and largest free region seen at failure time;
  that pair tells a genuinely full cluster apart from a fragmented one.
- `Probe summary` (when `--probe_mib` is set) reports probe success, free space
  at probe time, and the p10/p50/max largest free region.
- `Traffic summary` (event replay only) reports write skew across segments,
  overall and within a sliding window, plus the mean per-segment utilization
  stddev, so contiguity gains can be weighed against traffic concentration.
- `released=` in the fragmentation summary counts objects released by
  `--release_prob`.
