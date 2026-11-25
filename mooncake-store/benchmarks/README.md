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

## Existence Lookup and Eviction Benchmark

`exists_evict_bench` measures `BatchExistKey` latency while memory eviction
runs, and whether eviction keeps up with a stream of new keys. It drives an
in-process `MasterService` over one synthetic segment: metadata and allocator
handles are real, but there is no RPC and no payload I/O.

The benchmark prefills `--num_objects` 1 KiB keys, measures lookups alone for
`--baseline_sec`, then runs eviction every `--eviction_interval_sec` for
`--duration_sec`. Each successful `BatchExistKey` grants a read lease, so this
also shows how lease renewal by readers interacts with eviction.

By default, readers look up 80% of keys (spread over write order, including
new keys), so the other 20% go cold and can be evicted, while writers add 50k
new keys/s for the whole run. The run reports failed puts when eviction falls
behind.

```bash
cmake --build build --target exists_evict_bench -j$(nproc)

# Defaults: 10M keys, 8 reader threads, 200-key batches, 80% of keys read,
# 50k new keys/s, 5% eviction every 10 s. Use a production-length key prefix.
./build/mooncake-store/benchmarks/exists_evict_bench \
  --key_prefix=$(printf 'p%.0s' $(seq 150))

# Every key is read and nothing new is written: lookup latency during eviction
# alone.
./build/mooncake-store/benchmarks/exists_evict_bench \
  --key_prefix=$(printf 'p%.0s' $(seq 150)) \
  --lookup_fraction=1 --put_rate=0
```

Workload flags:

- `--lookup_fraction` (default 0.8): readers only draw from this fraction of
  keys; the rest are never read after they are written.
- `--lookup_subset` (default `interleaved`): `interleaved` reads a hash-chosen
  subset spread over write order, including keys added by writers; `prefix`
  reads the first keys written and never the new ones.
- `--put_rate` (default 50000), `--put_threads` (default 4): write new keys at
  this rate for the whole run; 0 disables writes.
- `--segment_headroom`: segment capacity beyond `num_objects` x 1 KiB. With
  `--put_rate`, this is how far writes can run ahead of eviction before
  `PutStart` fails.
- `--eviction_worker_threads`: eviction workers (default 0, automatic: one per
  16M filed keys, so 1 at the default 10M keys).
- `--lookup_mode=probe`: use `BatchProbeKey` instead, without granting a lease.
- `--skip_eviction`: never evict; a control for lookup and put latency.
- `--key_prefix`: prepended to every key; use a production-length prefix so
  keys are heap-allocated like real keys.

Output lines:

- `EVICTION`: one line per eviction pass, with its duration and key counts.
- `RESULT`: lookup batch latency (p50, p95, p99, max), throughput, and hit
  rate for the `baseline`, `periodic_eviction`, `overlapping_eviction` (batches
  that overlap a pass), and `between_evictions` phases.
- `PUTS`: with `--put_rate`, attempted and failed puts, put latency, and when
  the first put failed.
- `FINAL`: keys written, live keys, and eviction passes.
