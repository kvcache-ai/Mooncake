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

### RL Data-Plane Offload Benchmark

The `rl_offload` workload replays the object lifecycle of RL training data
offloaded through the object store. Every step writes bundles shaped like a
captured GRPO trainer bundle: three payloads of data-dependent size (10-12 MiB
by default) and 31 small Arrow buffers of 256 B to 9896 B. Objects are removed
explicitly `--rl_lifetime_steps` later and nothing is evicted, so a failed put
is lost data rather than a cache miss. The number of bundles per step is
derived from each `--rl_target_utils` entry, the share of capacity the live set
should occupy.

```bash
./build/mooncake-store/benchmarks/allocation_strategy_bench \
  --workload=rl_offload \
  --rl_segments=8 \
  --rl_segment_mib=147 \
  --rl_target_utils=70,80,85,90,95
```

- `Live%` is requested bytes live over capacity, averaged over steps;
  `Charged%` is what the allocator reserved for them, size-class round-up
  included.
- `Failed/Puts` counts puts that found no segment with a large enough
  contiguous region, summed over `--rl_seeds`.
- `FreeMB@Fail` and `LargestMB@Fail` are cluster free space and the largest
  contiguous free region at those failures. A large gap between them means the
  failures come from placement, not from a full pool.
