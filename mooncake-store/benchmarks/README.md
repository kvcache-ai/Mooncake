# Mooncake Store Benchmarks

This directory contains benchmark tools for Mooncake Store internals.

## Tenant Quota Mutex versus CAS Benchmark

`tenant_quota_bench` is a standalone microbenchmark for comparing the current
mutex-based tenant quota state machine with the proposed unified
`charged_bytes` CAS state.

The mutex baseline models the current quota-table shard lock and
`used_bytes`/`reserved_bytes` transitions. The CAS implementation models the
proposed handle-based hot path, including the admission bit, effective-quota
load, policy-sequence validation, guarded release, and CAS retry accounting.
The benchmark uses direct tenant indexing, so the mutex result does not include
tenant map lookup overhead. It also excludes metadata-object counter updates
and all non-quota Master work, making it a conservative comparison focused on
charge, settlement, and release.

Build through CMake:

```bash
cmake --build build --target tenant_quota_bench -j$(nproc)
```

The source has no Mooncake runtime dependencies and can also be copied to
another server and compiled directly:

```bash
g++ -O3 -DNDEBUG -std=c++17 -pthread \
  mooncake-store/benchmarks/tenant_quota_bench.cpp \
  -o tenant_quota_bench
```

Run a same-tenant, successful-lifecycle comparison:

```bash
./tenant_quota_bench \
  --threads=32 \
  --tenants=1 \
  --workload=commit \
  --iterations=1000000 \
  --warmup=100000 \
  --rounds=5 \
  --pin-threads
```

Run an abort/refund comparison:

```bash
./tenant_quota_bench \
  --threads=32 \
  --tenants=1 \
  --workload=abort \
  --iterations=1000000 \
  --rounds=5 \
  --pin-threads
```

Sweep thread counts:

```bash
for threads in 1 2 4 8 16 32 64; do
  ./tenant_quota_bench \
    --threads="${threads}" \
    --tenants=1 \
    --workload=commit \
    --iterations=1000000 \
    --rounds=5 \
    --pin-threads
done
```

The `commit` workload runs `Reserve+Commit+Release` for the mutex baseline and
`TryCharge+no-op settlement+Release` for CAS. The `abort` workload runs
`Reserve+Abort` versus `TryCharge+Release`. `--work=N` adds CPU work between
quota calls to model lower operation density.

Use one tenant to measure maximum same-tenant contention. Use
`--tenants=<thread count> --tenant-pattern=sticky` to measure the uncontended
case, or `--tenant-pattern=round_robin` to distribute every worker across
tenants. Keep `quota-bytes >= threads * charge-bytes` when measuring the hot
success path; rejected admissions are reported separately.

Each `RESULT` line reports lifecycle Mops, average ns/op, charge and release CAS
retries, rejected admissions, accounting errors, and final charged bytes.
`SUMMARY` reports median mutex and CAS throughput and the CAS speedup. A
non-zero exit status indicates an accounting mismatch or leaked final charge.

For cross-server comparisons, use the same compiler, optimization flags,
thread affinity, NUMA placement, CPU frequency policy, and command line. The
benchmark alternates mutex/CAS execution order between rounds to reduce
order-dependent bias.

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
