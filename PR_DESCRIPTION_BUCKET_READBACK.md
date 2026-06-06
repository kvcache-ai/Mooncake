# PR: Harden Bucket Readback Fallbacks and Follow-up Store Fixes

## Description

This PR is the follow-up version of the bucket readback optimization work. It keeps the original goal of stabilizing `BucketStorageBackend::BatchLoad` when `use_uring=true`, and also incorporates the code review fixes that were raised against the first draft.

The main focus is still the bucket readback path, but the current version also fixes two related store-side issues discovered while iterating on the patch:

- The bucket read path no longer uses a shared mutable aligned scratch buffer.
- `ScanMeta()` now restarts its iterator state on each call instead of relying on `FileStorage::ReRegisterOffloadedObjects()` to do that externally.
- The deprecated `distributed_storage_backend` descriptor now logs a warning and falls back to `bucket_storage_backend` instead of selecting an unsupported backend mode.

There is no public API signature change. The only externally visible behavior change is that the deprecated `MOONCAKE_OFFLOAD_STORAGE_BACKEND_DESCRIPTOR=distributed_storage_backend` path now degrades safely to the bucket backend.

## What Changed

### 1. Bucket readback hardening under `use_uring=true`

- Detect hard `io_uring + O_DIRECT` read failures and stop retrying the same failing direct path for the remainder of the backend lifetime.
- Reopen the bucket file on a buffered path and retry reads instead of repeatedly paying `EINVAL` / `Invalid argument` costs.
- Keep the logical `batch_read` fast path first, then fall back to span-read + scatter, then finally to per-key reads.
- Keep bucket-file padding aligned with the read-side assumptions used by the `io_uring` path.

### 2. Review follow-up: remove shared scratch buffer races

- Remove the shared `aligned_io_buffer_` member from `BucketStorageBackend`.
- Replace it with `AcquireScratchBuffer()`, which provides:
  - a thread-local aligned scratch buffer for common-sized operations, and
  - a per-call aligned allocation for larger requests.
- Apply the same aligned scratch-buffer strategy to both:
  - `BatchLoad()` span reads, and
  - `WriteBucket()`.

This addresses the two critical concerns from the first review round:

- concurrent readers/writers touching the same shared buffer, and
- unaligned temporary buffers on `O_DIRECT`-backed paths.

### 3. ScanMeta restart semantics

- Move the iterator reset into `BucketStorageBackend::ScanMeta()` itself.
- Remove the external reset from `FileStorage::ReRegisterOffloadedObjects()`.

This makes repeated `ScanMeta()` calls self-contained and avoids depending on a caller-side reset sequence.

### 4. Deprecated backend descriptor handling

- Treat `distributed_storage_backend` as unsupported.
- Log a warning and fall back to `bucket_storage_backend`.
- Add a test for that env-driven fallback behavior.

## Module

- [ ] Transfer Engine (`mooncake-transfer-engine`)
- [x] Mooncake Store (`mooncake-store`)
- [ ] Mooncake EP (`mooncake-ep`)
- [ ] Integration (`mooncake-integration`)
- [ ] P2P Store (`mooncake-p2p-store`)
- [ ] Python Wheel (`mooncake-wheel`)
- [ ] PyTorch Backend (`mooncake-pg`)
- [ ] Mooncake RL (`mooncake-rl`)
- [ ] CI/CD
- [ ] Docs
- [ ] Other

## Type of Change

- [x] Bug fix
- [ ] New feature
- [x] Refactor
- [ ] Breaking change
- [ ] Documentation update
- [ ] Other

## Baseline vs Current Version

### Methodology

- Same server: `121.48.165.75:7027`
- OS observed during benchmark: Linux `5.4.0-196-generic`
- Compiler used for the rebuilt comparison binaries: `gcc-12` / `g++-12`
- Same benchmark workload on both builds
- Each configuration was run 3 times; the tables below report the median
- Default benchmark cache mode is still `buffered` page-cache mode

### Harness note

`storage_backend_bench` does not currently expose a dedicated `use_uring` flag, and it does not wire `MOONCAKE_OFFLOAD_USE_URING` into `FileStorageConfig::use_uring` by itself.

To make the `use_uring=false/true` comparison meaningful, I applied the same tiny harness-only patch to both compared builds so that:

```cpp
config.use_uring = BenchmarkUseUringEnabled();
```

reads `MOONCAKE_OFFLOAD_USE_URING` from the environment.

That harness patch was used only for measurement and is **not** part of the production diff described in this PR.

### Production files compared

The production comparison for the current implementation was based on the current working-tree versions of:

- `mooncake-store/include/storage_backend.h`
- `mooncake-store/src/storage_backend.cpp`
- `mooncake-store/src/file_storage.cpp`

against `origin/main`.

### Benchmark command

```bash
./mooncake-store/benchmarks/storage_backend_bench \
  --backend=bucket \
  --test=load \
  --value_size=131072 \
  --batch_size=32 \
  --num_operations=200 \
  --warmup_operations=20 \
  --verify=false
```

For the two modes, the benchmark was run as:

```bash
MOONCAKE_OFFLOAD_USE_URING=0 ./mooncake-store/benchmarks/storage_backend_bench ...
```

```bash
MOONCAKE_OFFLOAD_USE_URING=1 ./mooncake-store/benchmarks/storage_backend_bench ...
```

## Results

### 1. `use_uring=false`

| Branch | Status | Throughput (MB/s) | Ops/sec | Mean (ms) | P50 (ms) | P99 (ms) | Max (ms) |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `main` | Success | 4548.04 | 1137.01 | 0.88 | 0.64 | 1.72 | 1.75 |
| `this PR` | Success | 5180.67 | 1295.17 | 0.77 | 0.77 | 0.84 | 0.85 |

Relative change vs `main`:

| Metric | Change |
| --- | ---: |
| Throughput | +13.9% |
| Ops/sec | +13.9% |
| Mean latency | -12.5% |
| P50 latency | +20.3% |
| P99 latency | -51.2% |
| Max latency | -51.4% |

Interpretation:

- The new scratch-buffer path improves throughput and materially improves tail latency.
- `P50` is slightly higher than `main`, so this is not a uniform latency win across every percentile.
- The more important result for this workload is that mean, `P99`, and max latency all improve while throughput also increases.

### 2. `use_uring=true`

| Branch | Status | Throughput (MB/s) | Ops/sec | Mean (ms) | P50 (ms) | P99 (ms) | Max (ms) |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `main` | Invalid result | N/A | N/A | N/A | N/A | N/A | N/A |
| `this PR` | Success | 4605.91 | 1151.48 | 0.87 | 0.87 | 0.92 | 0.94 |

`main` behavior under `use_uring=true` on this server:

- All 3 runs failed to produce valid load results
- Each run completed with `200/200` operation errors
- stderr repeatedly reported `CQE error: Invalid argument`
- The benchmark therefore reported zero useful data loaded

`this PR` behavior under `use_uring=true` on this server:

- All 3 runs completed successfully
- stderr still shows the initial `Invalid argument` event from the direct path
- after that first hard failure, the backend falls back to a stable buffered path and completes the workload successfully

Interpretation:

- On this server, `main` does not provide a valid `bucket + use_uring` readback result for this workload.
- This PR turns that scenario into a successful readback path.
- The current version therefore improves correctness first, and then delivers usable performance where `main` does not.

## How Has This Been Tested?

### Passed on the comparison server

Benchmark build:

```bash
cmake -S /root/bench-current -B /root/bench-current/build-bench -G Ninja \
  -DCMAKE_C_COMPILER=/usr/bin/gcc-12 \
  -DCMAKE_CXX_COMPILER=/usr/bin/g++-12 \
  -DBUILD_BENCHMARK=ON \
  -DBUILD_UNIT_TESTS=OFF \
  -DBUILD_EXAMPLES=OFF \
  -DSTORE_USE_ETCD=OFF \
  -DSTORE_USE_REDIS=OFF \
  -DSTORE_USE_K8S_LEASE=OFF \
  -DUSE_CUDA=OFF \
  -DUSE_MUSA=OFF \
  -DUSE_NOF=OFF
cmake --build /root/bench-current/build-bench --target storage_backend_bench -j4
```

Repeated benchmark comparisons against `origin/main`:

```bash
MOONCAKE_OFFLOAD_USE_URING=0 ./mooncake-store/benchmarks/storage_backend_bench ...
MOONCAKE_OFFLOAD_USE_URING=1 ./mooncake-store/benchmarks/storage_backend_bench ...
```

Targeted `storage_backend_test` run for the current implementation:

```bash
./mooncake-store/tests/storage_backend_test \
  --gtest_filter=StorageBackendTest.BucketStorageBackend_BatchLoadWithMixedBuckets:StorageBackendTest.BucketStorageBackend_ScanMetaRestartsFromBeginningEachCall:StorageBackendTest.BucketStorageBackend_ConcurrentReadsNoBlocking
```

Observed result:

- `StorageBackendTest.BucketStorageBackend_BatchLoadWithMixedBuckets`: passed
- `StorageBackendTest.BucketStorageBackend_ScanMetaRestartsFromBeginningEachCall`: passed
- `StorageBackendTest.BucketStorageBackend_ConcurrentReadsNoBlocking`: passed

### Not fully runnable on this server

I also attempted to build `file_storage_test` for the current implementation, but on this server the target currently fails to link because of an RDMA / ibverbs symbol issue unrelated to the bucket-readback logic:

```text
undefined reference to `_ibv_query_gid_ex`
```

Because of that environment-level link failure, I am **not** claiming a passed `file_storage_test` run in this PR.

### Local test updates included in this branch

This branch also updates / adds tests for:

- deprecated `distributed_storage_backend` env fallback
- repeated `ScanMeta()` restart behavior
- concurrent bucket reads validating returned payloads instead of counting success on return code alone

## Checklist

- [x] I have performed a self-review of my own code.
- [x] I have formatted my own code using `./scripts/code_format.sh` before submitting.
- [ ] I have updated the documentation.
- [x] I have added or updated tests for the changed behavior.
