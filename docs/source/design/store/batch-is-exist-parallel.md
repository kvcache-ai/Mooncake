# Parallel `batch_is_exist` (server-side chunked lookup)

Related: `ExistOptions` (replica.h), SSD prefetch-on-exist (ssd-prefetch.md).

## Goal

`batch_is_exist` on a large key set (case B: the server-side serial metadata
loop is the bottleneck, not the network RTT) should split the keys into
chunks and query them in parallel inside the master, cutting end-to-end
latency. The feature is opt-in via a new `ExistOptions.chunked_parallel` flag
and off by default, so the serial path stays byte-for-byte unchanged.

## Current path

```
Python batch_is_exist                  python/mooncake/store/rs/store.py
  → pybind batch_is_exist(keys, opts)  mooncake-integration/store/store_py.cpp
    → RealClient::batchIsExist         mooncake-store/src/real_client.cpp:3107
      → batchIsExist_internal(keys)    mooncake-store/src/real_client.cpp:7094
        → Client::BatchIsExist         mooncake-store/src/client_service.cpp:4249
          → MasterClient::BatchExistKey  mooncake-store/src/master_client.cpp:621
            → invoke_batch_rpc         mooncake-store/src/master_client.cpp:453
              → WrappedMasterService::BatchExistKey  src/rpc_service.cpp:172
                → MasterService::BatchExistKey       src/master_service.cpp:3007
                  → BatchExistKeyImpl  src/master_service.cpp:3064  ← serial loop
```

`BatchExistKey` is a pooled handler on `RpcWorkerPool`
(asio thread pool, `handler_worker_thread_num` threads), but one request runs
on exactly one worker thread, so `BatchExistKeyImpl`'s `for` loop is serial.

## Why parallel reads are safe

- Per-key lookup takes a stripe lock: `ns->objects.Read(user_key)`
  (`master_service.h` `MetadataAccessorRO::Hold`).
- `GrantReadLease` is an atomic CAS (`include/lease.h`).
- `master_state_mutex_` is taken as `std::shared_lock` (read), so concurrent
  readers are allowed.
- `results` is pre-sized; each worker writes a disjoint `[begin, end)` index
  range, so there is no data race.

## Parameter

Add to `ExistOptions` (`mooncake-store/include/replica.h`):

```cpp
struct ExistOptions {
    bool prefetch_to_memory = false;
    bool chunked_parallel  = false;  // NEW: chunk + parallel lookup
};
```

Only a `bool parallel` crosses the RPC boundary (not the whole struct). To keep
the old `BatchExistKey(keys, tenant_id)` wire signature intact, the parallel
path is a separate **additive** RPC method `BatchExistKeyParallel(keys,
tenant_id)` (the project's "purely additive for old peers" convention).

## Implementation steps

1. `replica.h`: add `chunked_parallel`; update `operator<<`.
2. Server core — `BatchExistKeyImpl` (`master_service.cpp:3064`): add a
   `bool parallel` param. If `parallel && keys.size() >= kParallelThreshold`
   (1024), split into sub-ranges, `post` them to an internal
   `asio::thread_pool`, each worker taking its own `shared_lock` and writing
   `results[i]` for its range, joined with `std::latch`. Otherwise run the
   existing serial loop.
3. Executor: add a lazily-created `std::unique_ptr<asio::thread_pool>`
   member to `MasterService` (self-contained; avoids starving
   `RpcWorkerPool`). Worker count defaults to
   `min(hardware_concurrency, 32)` — benchmarked on a 256-core host, 16–64
   workers is the sweet spot and the default 256 was slower for mid-size
   batches. `MOONCAKE_BATCH_EXIST_WORKERS` overrides it.
4. Thread the bool: `RealClient::batchIsExist_internal(keys, parallel)` →
   `Client::BatchIsExist(keys, parallel)` →
   `MasterClient::BatchExistKey(keys, parallel)` → dispatch to
   `BatchExistKeyParallel` (parallel) or `BatchExistKey` (serial) RPC →
   `WrappedMasterService::BatchExistKeyInternal(keys, tenant_id, parallel)` →
   `MasterService::BatchExistKey(keys, tenant_id, parallel)` →
   `BatchExistKeyImpl(keys, tenant_id, grant_lease, parallel)`.
5. Python: `rs/store.py` `batch_is_exist(..., chunked_parallel=False)`; pybind
   already accepts `ExistOptions`.

Note: `BatchProbeKey` reuses `BatchExistKeyImpl` with `grant_lease=false`;
keep it serial (pass `parallel=false`).

## Verification

- C++ unit test `BatchReadTest.ParallelBatchExistMatchesSerial`
  (`master_service_batch_read_test.cpp`): parallel result vector == serial
  vector (all-hit / all-miss / mixed / duplicate / large), order preserved.
- C++ integration test `RealClientTest.BatchIsExistParallelMatchesSerial`
  (`pybind_client_test.cpp`): real client → `BatchExistKeyParallel` RPC →
  embedded master, result == serial.
- Python e2e `TestStandaloneStoreE2E.test_batch_is_exist_parallel_matches_serial`
  (`mooncake-wheel/tests/test_standalone_store_e2e.py`): `batch_is_exist(keys)`
  == `batch_is_exist(keys, ExistOptions(chunked_parallel=True))`.
- TSAN: parallel path runs race-free.
- Keep green: `master_service_tenant_scenario_test.cpp`
  (`BatchExistKeyTenantAwarePreservesOrder`), `master_service_batch_read_test.cpp`,
  `mooncake-wheel/tests/test_batch_probe_policy.py`,
  `test_distributed_object_store.py`.

## Performance comparison

Two benchmarks (`mooncake-store/benchmarks/`):

- `batch_exist_bench` (C++, server-only): constructs `MasterService` directly,
  no network/client — measures the `BatchExistKeyImpl` loop.
- `batch_exist_bench.py` (Python, end-to-end): embedded master +
  `MooncakeDistributedStore` — measures pybind → client → RPC → master.

Both sweep N, hit ratio, worker count and report p50 + keys/s.

### Measured (256-core host, TCP loopback, hit_ratio 0.5)

| N keys | server-only speedup | end-to-end speedup |
|---|---|---|
| 10,000 | ~4x | ~1.7x |
| 100,000 | ~3x | ~1.6x |

- Worker sweet spot 16–64; the default is capped at 32
  (`min(hardware_concurrency, 32)`); `MOONCAKE_BATCH_EXIST_WORKERS` overrides.
- End-to-end gains are lower than server-only because pybind serialization,
  the client RPC round-trip, and the network do not scale with the parallel
  loop.
- Control: N=1000 (< 1024 threshold) stays ~1.0x (serial path).

## Risks

- Worker starvation if the pool is shared — mitigated by a dedicated pool.
- RPC compatibility — solved with an additive `BatchExistKeyParallel` method
  (old `BatchExistKey` wire signature untouched), following the project's
  "purely additive for old peers" convention.
- Duplicate keys across chunks: extra read, still correct.
