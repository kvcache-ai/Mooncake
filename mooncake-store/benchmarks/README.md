# Mooncake Store Benchmarks

This directory contains benchmark tools for Mooncake Store internals.

## DFS Storage Backend Benchmark

`storage_backend_bench --backend=dfs` (also `--backend=distributed`) measures
`DistributedStorageBackend::BatchWrite` and `BatchRead` using real DFS
descriptors. It supports the `hf3fs` USRBIO adapter and the `posix` adapter,
with either the `shard` or immutable `bucket` allocator. It runs the allocator
inside the benchmark process; no Mooncake master or client service is needed.

Build from a configured Mooncake tree with `BUILD_BENCHMARK=ON`. HF3FS runs also
require `USE_3FS=ON`, the 3FS headers/library, and a writable 3FS mount:

```bash
cmake --build build --target storage_backend_bench -j 8
```

Run a small local check using the POSIX adapter:

```bash
./build/mooncake-store/benchmarks/storage_backend_bench \
  --backend=dfs --dfs_adapter=posix --dfs_allocator=shard \
  --test=all --storage_path=/tmp/mooncake-dfs-bench \
  --dfs_file_count=2 --dfs_shard_capacity_mb=512 \
  --value_size=4097 --batch_size=19 \
  --num_operations=10 --warmup_operations=2 --num_threads=2
```

Measure native 3FS writes, then reads:

```bash
./build/mooncake-store/benchmarks/storage_backend_bench \
  --backend=dfs --dfs_adapter=hf3fs --dfs_allocator=shard \
  --test=offload --storage_path=/mnt/3fs/mooncake-bench \
  --dfs_file_count=4 \
  --value_size=131072 --batch_size=16 \
  --num_operations=1000 --warmup_operations=100

# Use the same arguments with --test=load for reads, or
# --test=concurrent_load --num_threads=4 for concurrent reads.
# Add files or reduce operation count for the larger concurrent dataset.
```

Use `--dfs_allocator=bucket` to measure the immutable DFS bucket path. This is
separate from the local backend selected by `--backend=bucket`.

- `--dfs_adapter` defaults to `posix`. Pass `--dfs_adapter=hf3fs` to measure
  USRBIO; it fails explicitly in a build without 3FS support or when
  `--storage_path` is not on a 3FS mount. POSIX checks exercise DFS descriptor
  handling but do not measure USRBIO batching.
- DFS capacity is `--dfs_file_count` (shard count or maximum bucket count,
  default 4) times the file size. File sizes default to the Mooncake
  defaults: `--dfs_shard_capacity_mb=4096` (`MOONCAKE_DFS_SHARD_CAPACITY`)
  and `--dfs_bucket_capacity_mb=1024` (`MOONCAKE_DFS_BUCKET_CAPACITY`).
  `--capacity_gb` applies only to local backends and is rejected for DFS.
  Shards are preallocated at init, so all shard files must fit on the target
  filesystem; buckets are created as the dataset is allocated. Eviction is
  disabled.
- `--num_operations` and `--warmup_operations` count batches per thread.
  `offload` and `load` use one thread; `concurrent_load` uses `--num_threads`.
  The dataset contains `(operations + warmup) * batch_size * threads` objects
  and must fit the DFS files; values are 4 KiB-aligned and never span files.
  DFS traverses distinct keys in generation order; `--pattern` is not used.
- Allocation, request construction, and payload generation are outside the
  operation timer. Read tests populate the dataset first. Each worker warms up
  its own I/O resources before measurement. Write verification runs afterward;
  read verification runs outside the operation timer.
- Latencies describe a whole batch. `Throughput` uses the sum of call latencies;
  `Wall throughput` uses elapsed time across all threads and includes request
  preparation and in-loop verification. Use the wall figure for aggregate
  concurrency scaling. Failed I/O or verification makes the DFS run exit nonzero.
- Every test creates and prints a unique `dfs-bench-*` subdirectory below
  `--storage_path`. Only that subdirectory is removed afterward; use
  `--skip_cleanup` to retain it. Existing files under the supplied path are
  preserved.
- DFS supports `init`, `offload`, `load`, `concurrent_load`, and `all` (these four
  tests). Metadata lookup, churn, restart, mixed read/write, and `--run_all`
  sweeps are not implemented for DFS. Cache-control modes and per-operation
  fsync are also unsupported; HF3FS uses USRBIO regardless of the default
  `--cache_mode=buffered` flag. POSIX uses its normal buffered path.

Start the batching comparison with one thread and batch sizes `1, 4, 16, 64`.
Keep the total object count constant by adjusting `--num_operations` and
`--warmup_operations`. For a comparison against an earlier adapter commit,
build the same benchmark changes against both revisions and use identical
adapter, allocator, capacity, and workload settings.

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
