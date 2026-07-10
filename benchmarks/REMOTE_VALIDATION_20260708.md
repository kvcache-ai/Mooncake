# Mooncake Store Remote Validation - 2026-07-08

## Context

This validation run was performed after wiring the prefix-aware radix index into
the real `MasterService::GetReplicaList` path and hardening the related Bloom
filter and deferred-lease semantics.

The goal was not only to prove the new prefix fallback behavior, but also to
make sure existing lease, remove, replica-clear, copy, move, and eviction
semantics still pass on the GPU server build environment.

## Remote Environment

| Item | Value |
|------|-------|
| Server | A40 GPU server |
| Hostname | `<gpu-hostname>` |
| GPU state | 8 x NVIDIA A40 visible, idle at preflight |
| Remote validation tree | `/tmp/mooncake_competition_codex` |
| Compiler | GCC/G++ 13.3.0 |
| CMake | 3.28.3 |
| yaml-cpp | 0.8.0 via pkg-config |
| Build type | Debug |

The source tree was synced into `/tmp` instead of `/home` because `/home` was
near full during validation. The original remote tree under
`${REMOTE_DIR}` was left untouched as a reference.

## Fixes Validated

1. `GetReplicaList` now tries exact lookup first when Bloom says the full key
   may exist, then falls back to `PrefixRadixTree::LongestPrefixMatch` when the
   exact key is absent.

2. Bloom filter registration now happens when object metadata is created in
   `PutStart`, not only at `PutEnd`. This prevents in-flight objects from being
   misreported as `OBJECT_NOT_FOUND`; they correctly return
   `REPLICA_IS_NOT_READY`.

3. `PutEnd` no longer adds the same key to the Counting Bloom Filter again,
   avoiding double-counting and stale positives after remove.

4. Deferred lease touch now stores the access timestamp. Flush converts
   `access_time + ttl` into the hard lease timeout, preserving TTL semantics
   even if the flush happens later in remove, eviction, or replica-clear paths.

5. Delete-like slow paths that make lease decisions now flush deferred leases
   before checking expiration:
   - `Remove`
   - `RemoveByRegex`
   - `RemoveAll`
   - `BatchReplicaClear`

## Commands

```bash
cd /tmp/mooncake_competition_codex

cmake -S . -B build \
  -DCMAKE_BUILD_TYPE=Debug \
  -DCMAKE_POLICY_VERSION_MINIMUM=3.5

cmake --build build --target master_service_test -j$(nproc)
```

Focused prefix fallback regression:

```bash
./build/mooncake-store/tests/master_service_test \
  --gtest_filter='MasterServiceTest.GetReplicaListFallsBackToLongestStoredPrefix'
```

Related `GetReplicaList` regression group:

```bash
GLOG_minloglevel=2 ./build/mooncake-store/tests/master_service_test \
  --gtest_filter='MasterServiceTest.GetReplicaList*'
```

Deferred lease and cleanup regression group:

```bash
GLOG_minloglevel=2 ./build/mooncake-store/tests/master_service_test \
  --gtest_filter='MasterServiceTest.RemoveByRegex:MasterServiceTest.CopyStart:MasterServiceTest.MoveStart:MasterServiceTest.RemoveByRegexComplex:MasterServiceTest.RemoveAll:MasterServiceTest.ConcurrentReadAndRemoveAll:MasterServiceTest.SingleSliceMultiReplicaFlow:MasterServiceTest.RemoveLeasedObject:MasterServiceTest.GetReplicaListFallsBackToLongestStoredPrefix'
```

Final full suite:

```bash
GLOG_minloglevel=2 ./build/mooncake-store/tests/master_service_test
```

## Results

| Check | Result | Evidence |
|-------|--------|----------|
| CMake configure | Pass | Build files generated under `/tmp/mooncake_competition_codex/build` |
| `master_service_test` build | Pass | `[100%] Built target master_service_test` |
| Prefix fallback single test | Pass | 1/1 passed, 1174 ms |
| `GetReplicaList*` group | Pass | 4/4 passed, 4598 ms |
| Deferred lease cleanup group | Pass | 9/9 passed, 15944 ms |
| `BatchReplicaClearWithLeaseActive` | Pass | 1/1 passed, 1164 ms |
| Full `master_service_test` | Pass | 73/73 passed, 256850 ms |

Remote log files retained on the server:

```text
/tmp/mooncake_cmake_configure.log
/tmp/mooncake_master_service_build.log
/tmp/mooncake_get_replica_list_after_fix.log
/tmp/mooncake_regression_cluster_after_timestamp.log
/tmp/mooncake_batch_clear_lease_after_fix.log
/tmp/mooncake_master_service_test_full_final.log
```

## Bugs Caught During Validation

| Symptom | Root Cause | Fix |
|---------|------------|-----|
| In-flight `PutStart` object returned `OBJECT_NOT_FOUND` instead of `REPLICA_IS_NOT_READY` | Bloom filter only tracked completed keys added at `PutEnd` | Add Bloom entry when metadata is created, and remove duplicate `PutEnd` Add |
| Immediate `Remove` after `ExistKey` or `GetReplicaList` could delete a leased object | Deferred lease used only a boolean touch and had not yet been flushed | Flush deferred lease on single-key remove path |
| Batch remove/regex/removeAll skipped too many keys after delayed flush | Boolean-only deferred touch extended lease from flush time, not access time | Store deferred access timestamp and apply `access_time + ttl` |
| `BatchReplicaClearWithLeaseActive` cleared an actively leased object | Replica clear path checked lease without flushing deferred touch | Flush deferred lease before the replica-clear lease check |

## Competition Readiness Note

This run upgrades the correctness story behind the optimization stack:

- Counting Bloom Filter remains usable for fast negative lookup without hiding
  in-flight metadata.
- Prefix-aware radix fallback is now exercised through the real master service
  API, not just as an isolated data-structure test.
- Deferred lease keeps the read hot path lightweight while preserving observable
  lease behavior for remove, regex remove, remove-all, eviction, and replica
  clear paths.

The next performance step is to rerun E2E SGLang + HiCache on the same A40 node
with two comparable trees:

1. Baseline: upstream or pre-optimization Mooncake Store.
2. Optimized: current `feat/optimize-mooncake-store`.

Use identical model path, request count, concurrency, warmup, and server process
lifetime for both runs. The final report should present side-by-side latency,
throughput, cache hit behavior, and Mooncake metric counters.

## E2E Preflight Status

An E2E run was not launched in this validation pass because the remote Python
environments were not ready for SGLang + HiCache:

| Check | Status |
|-------|--------|
| A40 GPUs | Ready, 8 GPUs idle at preflight |
| Model cache | Qwen model directories exist under Hugging Face cache and `${MODEL_DIR}` |
| System Python | Missing `sglang`, `torch`, and `mooncake` |
| Existing conda envs | Several envs have `torch`/`vllm`, but none have `sglang` or `mooncake` |

Before launching the next E2E benchmark, prepare one isolated environment under
`/tmp` or an agreed conda env with:

```bash
pip install 'sglang[all]'
pip install -e mooncake-wheel/
```

Then run the E2E guide from `benchmarks/E2E_DEPLOY_GUIDE.md`.

## E2E Follow-Up - 2026-07-10

The Python/SGLang environment blocker was resolved in a follow-up pass by
cloning the existing `BFCL` conda env into `/tmp/mooncake-e2e-bfcl`, installing
`sglang[all]`, building a Python 3.10 ABI-aligned Mooncake wheel from
`build-py310-abi`, and installing the repaired wheel into the isolated env.

The intended `Qwen2.5-7B-Instruct` snapshot was still incomplete because it
contained tokenizer/config files but no weight shards. A complete local
`Qwen2.5-0.5B` snapshot was used for an end-to-end smoke run instead.

Result:

| Check | Result |
|-------|--------|
| `import mooncake.engine` / `import mooncake.store` | Pass |
| `mooncake_master` RPC/metadata/metrics smoke | Pass |
| SGLang HiCache launch with `--hicache-storage-backend mooncake` | Pass |
| OpenAI-compatible request workloads | 60/60 successful requests |
| Mooncake backend metrics | 75 keys, 1 active client, 74/74 batch items written |

Full E2E evidence is in:

```text
benchmarks/E2E_VALIDATION_20260710.md
```
