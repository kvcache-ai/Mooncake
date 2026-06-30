# SSD Prefetch-on-Exist Design Document

> Related: RFC #2213 (prefetch-on-exist), PR #2071 (L2→L1 promotion-on-hit)
> Branch: Mooncake (main). Initially implemented on v0.3.11, now synced to main.


## 1. Background and Goals

### 1.1 Problem Origin
In vLLM-Ascend's three-tier KV pooling scenario (HBM / DRAM / SSD), KV cache is evicted (offloaded) to SSD (`LOCAL_DISK` replicas) when DRAM capacity is insufficient. When a request hits a key that exists **only on SSD, not in DRAM**, `get()` must read from SSD, with latency significantly higher than a DRAM hit.

vLLM request processing is a multi-stage pipeline. There is a time window between `exists()` (cache probing in the scheduling phase) and `get()` (worker actually loading KV) — measured median 15–17s, amplified by scheduling queueing:

```
Phase 1: Scheduler              Phase 2: Current batch forward      Phase 3: Worker loads KV
 get_num_new_matched_tokens()    GPU/NPU forward (time-consuming)    start_load_kv()
   └─ batch_is_exist() ◄─probe                                       └─ get() ◄─actual read
        │                                                                  │
        └────────────── within this window, silently warm SSD→DRAM ────────┘
```

### 1.2 Goals
Leverage the time window from `exists()` to `get()` to **asynchronously, best-effort** prefetch (promote) SSD-only keys to DRAM during the probe phase, so subsequent `get()` hits DRAM and TTFT is reduced.

### 1.3 Design Constraints
- Prefetch must be an **additional action** of `exists()`; it must not change the semantics of `exists()` or block scheduling.
- Must not pollute existing promotion-on-hit / eviction / metrics behavior.
- Failures are discardable (best-effort); must never affect normal offload / get.
- Must support **PD disaggregation / cross-node**: when node A initiates prefetch and finds a key's `LOCAL_DISK` replica on node B, B should read from its own SSD into its own DRAM ("Option B: cross-node delegation"), rather than moving data to A first.

## 2. Current Implementation (Adopted)

### 2.1 Overview: Dedicated Prefetch Path, Reusing Promotion "Execution Substrate", Bypassing Promotion-on-Hit "Admission and Pacing"

```
is_exist(keys, ExistOptions{prefetch_to_memory=true})
    └─ triggerSsdPrefetch(keys) → prefetch_pool_ (fixed size 4)
        └─ per chunk (128 keys) BatchQueryForPrefetch(chunk)     # batch read-only metadata, 1 RPC/chunk
            └─ ClassifySsdPrefetchRoute: filter SSD-only (has LOCAL_DISK, no MEMORY) and size>0
                ├─ local node holds LOCAL_DISK: RunLocalPrefetchRegisterAndPromote(chunk_local)   # immediate register + PrefetchKeys (pipelined, no wait for later chunks)
                └─ remote node holds: collect into remote_keys, after all chunks complete prefetch_offload_object RPC delegates to holder
                      └─ holder side runLocalPrefetch → RunLocalPrefetchRegisterAndPromote   # completed locally on holder
```

**Phase 1 batching + pipelining (2026-06)**: Early implementation called `QueryForPrefetch` per key and **waited for the entire batch of metadata queries** before unified `RegisterPrefetchTask` + `PrefetchKeys`. With large batches (e.g., 954 keys), the first register could be ~100ms later than the first `get()`, causing elevated `prefetch_miss_race`. Now changed to:
- **Batching**: `BatchQueryForPrefetch` / `BatchGetReplicaListForPrefetch`, up to 128 keys per chunk, RPC count reduced from O(N) to O(N/128).
- **Pipelining**: After each chunk's metadata query completes, **immediately** register + promote local keys in that chunk; no longer block subsequent chunk registers.
- **Defer reserve**: `PrefetchThrottle::reserve()` moved from the exist synchronous path to **after BatchQuery confirms SSD-only**, avoiding false trigger stats and useless async work for keys already in DRAM.

`RunLocalPrefetchRegisterAndPromote` encapsulates the local promotion logic of "per-key RegisterPrefetchTask → markInFlight → PrefetchKeys → cooldown", shared by `triggerSsdPrefetch` chunks and `runLocalPrefetch` (RPC delegation entry).

`PrefetchKeys()` internally reuses Mooncake's existing promotion execution chain:
`PromotionAllocStart` (master allocates PROCESSING MEMORY replica) → `FileStorage::AllocateBatch` (local staging) → `BatchLoad` (SSD read) → `PromotionWrite` (Transfer Engine writes to DRAM) → `NotifyPromotionSuccess` (marks MEMORY replica COMPLETE).

### 2.2 Key Design Point: Why a "Dedicated Path" Instead of Directly Reusing Promotion-on-Hit

Prefetch **only reuses the promotion execution substrate** (`promotion_tasks` / `PromotionAllocStart` / `PromotionWrite`), and **does not reuse promotion-on-hit triggering and scheduling**. Dedicated prefetch master RPCs were added:

| RPC | Purpose | Key: What it does NOT do |
|---|---|---|
| `GetReplicaListForPrefetch(key)` | Read-only replica metadata to determine SSD-only | Does **not** grant lease, record sketch, increment `valid_get_nums` metrics, or enqueue promotion |
| `BatchGetReplicaListForPrefetch(keys)` | Same as above, batch version; client-side `BatchQueryForPrefetch` wrapper | Same; master side currently loops `GetReplicaListForPrefetch` (same pattern as `BatchGetReplicaList`), can be optimized to true batch later |
| `RegisterPrefetchTask(client_id, key)` | Register a `promotion_tasks` entry on master (for `PromotionAllocStart`) | Does **not** go through `TryPushPromotionQueue` admission gate, does **not** push holder's `promotion_objects` heartbeat queue |

Compared with native `GetReplicaList` (main, master_service.cpp): the native path does `inc_*_cache_hit_nums` / `inc_valid_get_nums` / `GrantLease` / `TryPushPromotionQueue`, while `GetReplicaListForPrefetch` does none of these.

### 2.3 Throttling: Preventing Prefetch Storms (cooldown + dedup)
vLLM scheduling busy-loop repeatedly probes the same batch of SSD-only blocks for the same request in the waiting queue, causing high-frequency prefetch triggers. Two client-side throttling parameters were introduced (`PrefetchThrottle`):

| Parameter (mooncake.json, seconds) | Default | Meaning |
|---|---|---|
| `ssd_prefetch_dedup_ttl_sec` | 30 | Same key triggers prefetch at most once within this TTL, suppressing concurrent duplicate probes (**primary effective item**) |
| `ssd_prefetch_cooldown_sec` | 5 | Backoff window after DRAM saturation: skip prefetch within window, let eviction/offload reclaim memory first, avoid competing with promotion for memory |

> In practice, `dedup_ttl` is the main factor eliminating prefetch storms and resolving offload starvation; `cooldown` was not triggered at this dataset scale (DRAM not full, offload kept up).

### 2.4 Concurrency Boundary: Fixed Thread Pool
`triggerSsdPrefetch` / `runLocalPrefetch` use a fixed-size `prefetch_pool_` (`mooncake::ThreadPool`, size 4, aligned with `ClientService::task_thread_pool_(4)`), replacing the early pattern of detached `std::thread` per call, **limiting concurrent SSD reads + DRAM allocations**, avoiding massive detached threads contending for the same physical SSD's IOPS/bandwidth and flooding master with DRAM allocation requests.

`submitPrefetchJob` uniformly enqueues to the pool; when the pool is unavailable (not initialized / shutting down), **directly discard the best-effort task, never fall back to detached `std::thread`** — the latter is exactly the B1 prefetch storm anti-pattern. Gating ensures `triggerSsdPrefetch` only fires when `file_storage_` exists, and `initPrefetchRuntime()` is established in the same `enable_ssd_offload` branch as `file_storage_`, so the pool-unavailable branch is unreachable in normal operation; discard serves only as an invariant fallback.

#### Thread Usage Guidelines (this feature and all subsequent modifications must follow)
Per Mooncake native design, threads fall into two categories; **never create threads per load (per request / per key)**:

| Category | Use Case | Correct Approach | Mooncake Native Precedent |
|---|---|---|---|
| A. Fixed-role long-lived daemon loops | eviction, heartbeat, monitor, ipc server, GC and other singleton background loops | One dedicated `std::thread`/`jthread` per role, stored as member, resident for process lifetime (thread count = O(roles), small constant, load-independent) | `master_service.cpp` eviction/monitor/cleanup/dispatch; `client_service.cpp` leader monitor/storage heartbeat/task poll; `file_storage.cpp` heartbeat/GC |
| B. Load-scaling high-frequency short tasks | prefetch promotion, segmented retries, etc., work whose count scales with request volume/key count | Always use fixed-size `ThreadPool::enqueue` to cap concurrency; **must not** spawn bare threads; when pool full/unavailable, discard or queue, do not bypass the cap | `client_service.cpp` `task_thread_pool_(4)` |

> Rule of thumb: Will thread count grow with request volume/key count? Yes → must use ThreadPool (Category B); No, fixed-role resident loop → use dedicated `std::thread` member (Category A). Prefetch is Category B, hence `prefetch_pool_`. B1's root cause was applying Category A technique (detached thread per probe) to Category B work.

### 2.5 get()-Side "Wait Once" Mechanism
If `get()` hits an SSD-only key (`LOCAL_DISK` is the best replica), poll within budget waiting for promotion to complete, then select DRAM replica:
- Config `ssd_get_wait_ms` (mooncake.json) or env var `MOONCAKE_SSD_GET_WAIT_MS` (default **10ms**; `0` = disabled).
- Observation and wait logic hooked in **`batch_get_into_multi_buffers_internal`** (vLLM-Ascend actual batch get path), combined output of `[GET-SRC]` + `[PREFETCH-OUTCOME]` logs.
- **TP0 (local, `prefetch_wait_mode=local`)**: When this process's `PrefetchThrottle` has a trigger record, poll throttle completion status + `TryRefreshBestMemoryReplica` then Query master.
- **TP1~7 (master, `prefetch_wait_mode=master`)**: When this process has no trigger (exist triggered on TP0, get on other ranks), still **poll Query master** for SSD-only keys until a COMPLETE MEMORY replica appears or budget exhausted; does not increase exist count.
- After wait, if MEMORY replica appears, use DRAM; otherwise fall back to original SSD read path.
- `ClassifyPrefetchOutcome` output (`[PREFETCH-OUTCOME]`, see analysis script `analyze_prefetch.sh` metric descriptions):
  - `prefetch_hit`: promotion completed before get (`done_ms<=get_ms`) and source=DRAM
  - `prefetch_promoted_untracked`: RegisterPrefetchTask was called (SSD-only at BatchQuery time), get time already DRAM but done not recorded in throttle
  - `prefetch_miss_race`: **prefetch failed/timeout** — this rank triggered (`prefetch_trigger_ms>=0`) or `promote_attempted=true`, get still source=SSD
  - `prefetch_evicted_after_exist`: **read SSD after eviction** — this rank has no trigger and `promote_attempted=false`, get still source=SSD (may have been in DRAM at exist time, evicted within window; or TP1~7 master wait timeout)
  - `prefetch_dram_was_resident` (script label **exist false trigger**): get reads DRAM, but key was already in DRAM, this rank did **not** RegisterPrefetchTask; incorrectly entered prefetch wait path. **Should be ≈0 after defer reserve**
  - `dram_resident`: get reads DRAM, and **never** participated in prefetch (no trigger, no wait, `prefetch_wait_mode=none`)
  - `prefetch_failed`: `PrefetchKeys` promotion failed (throttle state `kFailed`)
  - ~~`ssd_no_prefetch`~~: merged into `prefetch_evicted_after_exist` (may still appear in old logs)
- `[GET-SRC]` log also includes `prefetch_promote_attempted=0|1` (whether this rank went through RegisterPrefetchTask / markInFlight), for outcome classification and script cross-stats (e.g., eviction sub-items "no register / has register").
- **SSD prefetch effective DRAM reads** (core analysis script metric) = `prefetch_hit + prefetch_promoted_untracked`, as proportion of total GET entries; `prefetch_dram_was_resident` / `dram_resident` not counted.
- After batching pipelining shortens "exist → first register" latency, `prefetch_hit` proportion within 10ms budget expected to rise. If `exist→get` interval still exceeds `default_kv_lease_ttl`, prefetched keys may still be evicted before get (see 5.3); in that case tune up master's `--default_kv_lease_ttl` rather than prefetch-specific parameters alone.

### 2.6 Configuration Switches (vLLM-Ascend side mooncake.json)
| Field | Meaning |
|---|---|
| `enable_ssd_offload` | Must be true, otherwise no SSD-only keys, prefetch meaningless |
| `ssd_offload_path` | SSD storage directory (absolute path, must match master `--root_fs_dir`) |
| `enable_ssd_prefetch` | Enable prefetch-on-exist |
| `ssd_prefetch_cooldown_sec` / `ssd_prefetch_dedup_ttl_sec` | Optional; if unset use Mooncake defaults (5s/30s), vLLM-Ascend side sets no defaults |
| `ssd_get_wait_ms` | Max budget (ms) for get-side wait for prefetch completion; default 10, `0` disables |

### 2.7 Lease Protection After Prefetch Promotion
When `NotifyPromotionSuccess` has `from_prefetch=true`, grant the promoted MEMORY replica the **same** lease as `exist`/`get`: `GrantLease(default_kv_lease_ttl_, default_kv_soft_pin_ttl_)` (grouped keys use `GrantLeaseForGroup`, logic consistent with `GetReplicaList`).

| Parameter | Configuration | Default | Meaning |
|---|---|---|---|
| `default_kv_lease_ttl` | master startup arg `--default_kv_lease_ttl` (ms) | 5000 | Hard lease: not evicted for capacity within window |
| `default_kv_soft_pin_ttl` | master startup arg `--default_kv_soft_pin_ttl` (ms) | see master config | soft-pin renewal, consistent with exist/get |

> **Deprecated**: Early implementation used env var `MOONCAKE_SSD_PREFETCH_PROTECT_SEC` (default 2s soft-pin, **lease=0**) for separate prefetch key protection; now unified to reuse master's `default_kv_lease_ttl` / `default_kv_soft_pin_ttl`, that env var is **no longer read**.

### 2.8 Hard Lease Tuning and Put Capacity Risk

**Background**: In early implementation, promoted MEMORY replicas at `NotifyPromotionSuccess` with only `GrantLease(0, soft_pin_ttl)` had hard lease immediately expired; keys within the `exist→get` window (measured median 15–17s) were easily evicted back to SSD by capacity, causing elevated `prefetch_evicted_after_exist`.

**Change 1 (code, 2026-06) — Prefetch promotion lease aligned with exist/get**

| Dimension | Before | After |
|---|---|---|
| Trigger point | `NotifyPromotionSuccess`, `from_prefetch=true` | Same |
| Lease | `GrantLease(0, protect_ms)` or equivalent soft-pin-only | `GrantLeaseForGroup` → **`GrantLease(default_kv_lease_ttl_, default_kv_soft_pin_ttl_)`** |
| vs exist/get | Inconsistent (hard lease=0) | **Consistent with `GetReplicaList` / `BatchExist` lease renewal behavior** |

Implementation in `master_service.cpp` `NotifyPromotionSuccess` `from_prefetch` branch (§5.3).

**Change 2 (deployment config) — Tune up master hard lease TTL as needed**

| Parameter | Default | Recommended tuning scenario | Purpose |
|---|---|---|---|
| `--default_kv_lease_ttl` | 5000 ms | When `exist→get` median approaches or exceeds 5s, increase to **10000 ms** or higher | Cover longer scheduling window, reduce probability of prefetched key evicted before get |
| `--default_kv_soft_pin_ttl` | master default | Consistent with exist/get | soft-pin renewal, paired with Change 1 |

> **Note**: `GetReplicaListForPrefetch` / `BatchGetReplicaListForPrefetch` **still do not grant lease** (§2.2); the above lease applies only to **prefetch promotion completion** (`NotifyPromotionSuccess`) and subsequent **exist/get path lease renewal**. Prefetch metadata queries themselves do not pin objects.

**Put Failure Risk (capacity contention, expected side effect)**

Hard lease + soft-pin extends MEMORY replica eviction exemption. Under **high DRAM watermark** (e.g., `memory_ratio>0.9`), **high-concurrency long context**, and **`offload_on_evict` queue defer** (Master log `[EVICT] No memory freed … deferred for disk offload`), Master may fail to free segment handles in the current round; Client side shows:

```
BatchPut failed for N keys due to insufficient space  (NO_AVAILABLE_HANDLE)
```

| Dimension | Description |
|---|---|
| **Is it abnormal?** | **Capacity contention** under high pressure + large lease, not a functional bug |
| **Affects current request inference?** | **Generally no**: upper-layer `put()` typically only logs error, does not interrupt completed prefill; Put runs asynchronously in background thread |
| **Actual impact** | Failed blocks **not written to Mooncake external pool**, subsequent same-prefix requests may see lower external hit rate, requiring NPU recompute or SSD get |
| **Mitigation** | Increase `global_segment_size`, lower `eviction_high_watermark_ratio`, idle wait between benchmark rounds for offload drain, or trade off shortening `default_kv_lease_ttl` (increases post-eviction SSD read risk) |

**Relationship with offload commit order (§8 B10)**: lease alignment resolves prefetched key **capacity eviction**; `BatchOffload` commit local index before `NotifyOffloadSuccess` resolves get `INVALID_KEY` from **Master registered LOCAL_DISK but Client index not ready**. The two are orthogonal; both should be merged in production.

## 3. Why This Design (Decision Rationale)

### 3.1 Reuse Promotion Execution Substrate
`PromotionAllocStart → PromotionWrite → NotifyPromotionSuccess` is Mooncake's mature SSD→DRAM data path with PROCESSING intermediate-state protection (eviction won't touch it while `is_completed()==false`). Duplicating transport/allocation/state machine is redundant and error-prone.

### 3.2 Bypass Promotion-on-Hit Triggering and Pacing (Core)
Directly reusing promotion-on-hit queue has 5 problems, hence a dedicated path (detailed in Section 4).

### 3.3 Cross-Node Delegation (Option B)
KV replicas may be on a remote node's SSD. Have the holder node "in-place" read its own SSD into its own DRAM, avoiding unnecessary cross-node data movement; native `get` already supports fetching from remote DRAM, prefetch only needs to warm data to holder's DRAM.

## 4. Rejected Wrong Approaches and Reasons

### Approach X (Wrong): Directly Reusing Promotion-on-Hit Queue to Trigger Prefetch
i.e., `exist → Query() enqueue → loop calling ProcessPromotionTasks()`. It works, but has 5 long-term problems, hence rejected:

| # | Problem | Description |
|---|---|---|
| 1 | **Admission gate blocks prefetch** | promotion-on-hit has frequency threshold (Count-Min sketch), DRAM watermark, queue limit before enqueue. Prefetch typical scenario is "warm on first exist", but when `promotion_admission_threshold>1` first time never enqueues, prefetch effectively never happens |
| 2 | **`Query` side effects pollute system** | prefetch using `Query()` goes through `GetReplicaList`: grants lease (pins object, disrupts eviction/offload pacing), raises sketch heat, pollutes `valid_get_nums`/cache hit metrics. A "existence-only check" becomes a "pretend Get" |
| 3 | **Execution pacing designed for 'lazy promotion'** | promotion-on-hit emits at most 1 task per heartbeat, default 10s heartbeat (UT waits 25s). Prefetch needs sub-second to few-second completion, heartbeat too slow. Manually looping `ProcessPromotionTasks()` is "driving fast on the slow lane", conflicts with original design and easily races with heartbeat thread on same queue |
| 4 | **Dual path conflict with dedicated `PrefetchKeys`** | If `Query enqueue` and `PrefetchKeys direct call` run simultaneously, same key gets two promotions, triggering `REPLICA_IS_NOT_READY` |
| 5 | **Resource contention** | promotion queue limit / DRAM alloc / in-flight slot globally shared. Large batch prefetch squeezes out real Get-triggered hot-key promotion, or mutual blocking when DRAM tight |

**How adopted approach avoids:** read-only `GetReplicaListForPrefetch` (fixes 1, 2) + immediately executed `PrefetchKeys` (fixes 3) + `RegisterPrefetchTask` does not push heartbeat queue (fixes 4) + independent RPC does not piggyback admission (fixes 5).

### Other Rejected Minor Approaches
- **Detached `std::thread` per prefetch trigger**: high-frequency probing spawns massive threads contending for SSD IOPS/bandwidth and flooding DRAM allocation → changed to fixed thread pool.
- **Register only after entire batch metadata query**: large batches cause first register too late, races with get → changed to chunk batch Query + per-chunk pipelined register (see 2.1).

## 5. Potential Optimization Points

### 5.3 Lease Protection After Prefetch Promotion (Implemented)

**Problem (historical)**: New MEMORY replicas from `PutEnd`/normal promote initially get `GrantLease(0, soft_pin_ttl)`, lease immediately expires, only renewed to `default_kv_lease_ttl` on first `get()`. Prefetch path faces same issue after `NotifyPromotionSuccess` completes: combined with `exist→get` interval up to 15s, many prefetched keys evicted back to SSD before use (early benchmarks ~82% of "prefetch triggered but still SSD" were this type).

**Implementation (2026-06)**: When `from_prefetch=true`, call `GrantLeaseForGroup` in `NotifyPromotionSuccess` (equivalent to `GrantLease(default_kv_lease_ttl_, default_kv_soft_pin_ttl_)`), aligned with `GetReplicaList` / `BatchExist` lease renewal. Protection duration unified under master's `--default_kv_lease_ttl` / `--default_kv_soft_pin_ttl` (default lease 5s).

**Tuning recommendation**: If `prefetch_evicted_after_exist` still high and `exist→get` median > `default_kv_lease_ttl`, prioritize tuning up master lease TTL (long context scenarios commonly use **10000 ms**, see §2.8); if interval extremely long (>15s), lease cannot cover full window, need DRAM capacity or scheduling-side optimization. When tuning up lease, note §2.8 **Put failure (NO_AVAILABLE_HANDLE)** risk.

**Deprecated**: `MOONCAKE_SSD_PREFETCH_PROTECT_SEC` no longer used.

1. **Weak sharing of in-flight metering between prefetch and native promotion-on-hit**
   Both share `promotion_tasks` map, `promotion_in_flight_` count and `promotion_queue_limit_` cap. Shared map brings cross-path dedup (positive benefit), but shared cap means large batch prefetch may squeeze native promotion-on-hit slots (and vice versa). For isolation, prefetch could get independent in-flight count and cap. This sharing is v0.3.11 existing behavior, not introduced by main sync.

2. **Prefetch path not aware of multi-tenant (tenant_id)**
   main introduced `tenant_id` (multi-tenant), but `GetReplicaListForPrefetch` / `RegisterPrefetchTask` currently fixed to `"default"` tenant (`MakeObjectIdentity(key, "default")`). To support multi-tenant prefetch, prefetch RPCs need to pass through `tenant_id`. Currently fully correct for single-tenant/default scenarios.

3. **Adaptive throttling parameters**
   Ideal `ssd_prefetch_cooldown_sec` should tie to "actual time for DRAM to evict from high watermark to target watermark"; currently static default 5s. Could consider adaptive tuning by runtime eviction rate.

4. **Prefetch hit rate observability**
   Benchmark validation uses P-side logs + `analyze_prefetch.sh` (in-repo analysis script) to stat `[GET-SRC]` / `[PREFETCH-OUTCOME]`. Main metrics:
   - GET read source: DRAM / SSD count and proportion
   - **SSD prefetch effective DRAM reads** = `prefetch_hit + promoted_untracked`
   - Sub-items: `prefetch_hit`, `promoted_untracked`, prefetch failed/timeout (`prefetch_miss_race`), read SSD after eviction (`prefetch_evicted_after_exist`, with no register / has register sub-items), exist false trigger (`prefetch_dram_was_resident`), `dram_resident`
   - Optional: `SHOW_DETAIL=1` outputs `prefetch_wait_mode` (local / master / none) distribution

5. **Metadata query and register latency (Phase 1 mitigated, Phase 2 pending)**
   Phase 1: `BatchGetReplicaListForPrefetch` reduces client↔master RPC round trips; pipelined register shortens exist→first task registration time. Phase 2: master-side `BatchGetReplicaListForPrefetch` still loops per-key `GetReplicaListForPrefetch`, can change to single batch metadata query for further latency reduction.

6. **get()-side wait vs lease eviction tradeoff**
   `ssd_get_wait_ms` default 10ms; for TP1~7 and other non-exist-trigger ranks, get side waits via **master Query polling** (log `prefetch_wait_mode=master`) for promotion completion. After prefetch completes key has `default_kv_lease_ttl` protection (5.3); if `exist→get` still exceeds lease window, may still be evicted, need tune up master lease or optimize scheduling interval.

## 6. Main Code Locations (Mooncake)

| File | Change Highlights |
|---|---|
| `mooncake-store/include/types.h` | Added `CONFIG_KEY_SSD_PREFETCH_*` / `CONFIG_KEY_SSD_GET_WAIT_MS` and defaults |
| `mooncake-store/include/real_client.h` / `src/real_client.cpp` | `PrefetchThrottle` (includes `promote_attempted`, `defer reserve` after BatchQuery), `prefetch_pool_`, `triggerSsdPrefetch` (chunk batch + pipeline), `runLocalPrefetch`, `RunLocalPrefetchRegisterAndPromote`, `ClassifySsdPrefetchRoute`, `TryRefreshBestMemoryReplica`, `ClassifyPrefetchOutcome`; get-wait (local / master dual path) and `[GET-SRC]`/`[PREFETCH-OUTCOME]` logs (includes `prefetch_promote_attempted`) in `batch_get_into_multi_buffers_internal` |
| `mooncake-store/include/master_service.h` / `src/master_service.cpp` | `GetReplicaListForPrefetch` (read-only), `RegisterPrefetchTask` (no heartbeat queue), `NotifyPromotionSuccess` (`GrantLeaseForGroup` when `from_prefetch` aligns exist/get lease), adapted to main tenant scope |
| `mooncake-store/include/master_client.h` / `src/master_client.cpp` | prefetch RPC client; includes `BatchGetReplicaListForPrefetch` |
| `mooncake-store/include/rpc_service.h` / `src/rpc_service.cpp` | `WrappedMasterService` wrapper and RPC handler registration (includes batch prefetch) |
| `mooncake-store/include/client_service.h` / `src/client_service.cpp` | `QueryForPrefetch` / `BatchQueryForPrefetch` / `RegisterPrefetchTask` |
| `mooncake-store/include/file_storage.h` / `src/file_storage.cpp` | `PrefetchKeys()`: wraps full promotion execution chain; `AllocateBatch`/`BatchLoad` use `MakeTenantScopedStorageKey` as staging map key (fixes B8 INVALID_KEY) |
| `mooncake-store/include/storage_backend.h` / `src/storage_backend.cpp` | `BucketStorageBackend::BatchOffload` commits `object_bucket_map_` before `NotifyOffloadSuccess`; on notify failure `RollbackCommittedBucket` + `CleanupOrphanedBucket` (see §8 B10, `ssd-offload.md` write path) |
| `mooncake-store/include/replica.h` / `store_c.*` / `pyclient.h` / `dummy_client.*` | Signature/descriptor/C API/Python binding adaptation (`ExistOptions.prefetch_to_memory`, `prefetch` parameter) |
| `mooncake-integration/store/store_py.cpp` | pybind: `setup` adds `ssd_prefetch_*` params, `is_exist`/`batch_is_exist` prefetch options |

## 7. Notes for Syncing from v0.3.11 to main (Handled)
- main introduced `tenant_id`; all `setup_real` signatures append `ssd_prefetch_cooldown_sec` / `ssd_prefetch_dedup_ttl_sec` **after `tenant_id`** (with defaults, no impact on existing main call sites).
- master-side prefetch implementation rewritten per main tenant-scoped metadata model: `MetadataAccessor(this, key)` → `MetadataAccessor(this, MakeObjectIdentity(key, "default"))`; `accessor.GetShard()->promotion_tasks` → `accessor.GetTenantState().promotion_tasks`.
- `get_config_size` on main returns `std::optional<size_t>`; throttling config reads use `.value_or(default)`.
- **Section 4's 5 problems' 'dedicated path' mitigations fully preserved on main** (read-only RPC no lease/sketch/enqueue, `RegisterPrefetchTask` no heartbeat queue, `PrefetchKeys` immediate execution).
- TODO: actual compile verification on Linux/Ascend environment for `Mooncake` (Windows cannot compile).

## 8. Historical Bug and Fix Records (Troubleshooting Log)

> This section records real pitfalls encountered during development in "observed symptom → root cause → fix/conclusion" format, for context alignment when switching sessions. Section 4 focuses on "approach selection rationale"; this section focuses on "actual failures and diagnosis".

### B1. Prefetch Storm → Offload Starvation (Most Severe, Drove Section 2.3/2.4 Throttling and Thread Pool)
- **Symptom**: Benchmark logs flooded with `REPLICA_IS_NOT_READY`, `INVALID_KEY`, `NO_AVAILABLE_HANDLE`, accompanied by `KV load failure`; `mooncake_master` continuously spamming DRAM eviction logs, `memory_ratio` stuck at ~0.81 repeatedly spinning eviction, offload unable to write to SSD.
- **Root cause**: vLLM scheduling busy-loop repeatedly probes `exist` on the **same batch of SSD-only blocks for the same request** in waiting queue; early implementation used detached `std::thread` per probe to initiate prefetch → massive concurrent prefetch seizing **single IO thread / SSD IOPS / DRAM allocation quota**, starving normal offload (DRAM→SSD); DRAM always at high watermark → master repeatedly tries eviction but cannot free space.
- **Fix**: ① `ssd_prefetch_dedup_ttl_sec` (default 30s) prefetch same key at most once per window — **primary effective item**, directly eliminates storm; ② `ssd_prefetch_cooldown_sec` (default 5s) DRAM saturation backoff; ③ detached threads replaced with fixed-size `prefetch_pool_`.
- **Conclusion**: At this dataset scale cooldown actually not triggered (dedup sufficient, DRAM not full).

### B2. Dual Path Double Promotion → `REPLICA_IS_NOT_READY`
- **Symptom**: Early coexistence of "`Query()` into promotion-on-hit queue" and "`PrefetchKeys` direct call" paths, each doing one promotion on same key, reporting `REPLICA_IS_NOT_READY`.
- **Root cause/fix**: See Section 4 Approach X problem #4. Finally only dedicated path retained: `RegisterPrefetchTask` **does not push holder heartbeat `promotion_objects` queue**, avoiding duplicate promotion with direct call.

### B3. `setup_real` Abstract Class Compile Error
- **Symptom**: Compile error `invalid new-expression of abstract class RealClient` (cannot new abstract class).
- **Root cause**: When appending `ssd_prefetch_cooldown_sec`/`ssd_prefetch_dedup_ttl_sec` params to `setup`, only partial declarations changed, causing `setup` pure virtual signatures inconsistent between `real_client.h`, `pyclient.h`, `dummy_client.h` → `RealClient` did not fully override base pure virtuals, still abstract.
- **Fix**: Keep all three (base declaration / `RealClient` / `DummyClient` / pybind) `setup` signatures **fully consistent**, new params uniformly appended after `tenant_id` with defaults. Any future new param passthrough must sync full chain signatures.

### B4. `QueryResult` const Member → Deleted Move Assignment (get-wait Re-check Path)
- **Symptom**: Implementing get()-side "wait once then re-check DRAM replica" compile failed, `tl::expected` move assignment deleted (because `QueryResult` has const members, cannot reassign to existing variable).
- **Fix**: Use `std::optional<QueryResult>` to hold re-check result (`.emplace()` not assignment), bypassing non-movable assignment limit. Used in Section 2.5 get-wait **master polling re-check** path (`prefetch_wait_mode=master`).

### B5. get Source/Wait Log Instrumentation in Wrong Function
- **Symptom**: "GET-SRC / wait" logs added in `get_buffer_internal` never printed in some integration cases.
- **Root cause**: vLLM-Ascend actually calls `batch_get_into_multi_buffers_internal` (batch multi-buffer path), not `get_buffer_internal`.
- **Conclusion/fix**: Logs and get-wait logic must hook in `batch_get_into_multi_buffers_internal`. This is the correct observation point for "get uses DRAM or SSD" and "whether in-flight prefetch was hit" — also the data source function for conclusions like "exist→get ~15s, SSD→DRAM ~2ms, ~82% evicted after prefetch".

### B6. `LEASE_EXPIRED` (Known Item, Not Introduced by This Feature)
- **Symptom**: Large batch request logs show `LEASE_EXPIRED` (e.g., several keys on multiple TP ranks batch transfer timeout).
- **Root cause**: Client lease default ~5s; when one query→transfer batch is very large and total time exceeds lease validity, object lease expires before transfer completes. More likely under PD disaggregation when P pulls DRAM remotely from D. Client lease timing issue, no direct causality with prefetch dedicated path (dedicated read-only RPC **does not grant lease**).
- **Disposition**: Recorded as known item; related but distinct from Section 5.3 master-side capacity eviction (lease protection) — former is client fetch lease timeout, latter is master-side DRAM capacity eviction.

### B7. Serial Metadata Query + exist Synchronous reserve → Elevated `prefetch_miss_race` / `prefetch_dram_was_resident`
- **Symptom (before batching / defer reserve)**: First `registered task` ~**143ms** later than first `[GET-SRC]`; `prefetch_hit` ~40%; `prefetch_miss_race` ~58% (includes much inflated); after correction ~**25%** of GETs were `prefetch_dram_was_resident` (exist false trigger).
- **Root cause 1 (original B7)**: Early `triggerSsdPrefetch` called `QueryForPrefetch` per key and **waited for all keys queried** before unified register; large batch metadata phase long, within get-side 10ms budget master had not registered task.
- **Root cause 2 (defer reserve)**: `PrefetchThrottle::reserve()` used to run on exist synchronous path, recording trigger for **keys already in DRAM**; get reads DRAM but incorrectly counted in prefetch stats (`prefetch_dram_was_resident`). exist only knows "exists", not replica tier; must wait for BatchQuery filter.
- **Fix (Phase 1)**: `BatchQueryForPrefetch` (128 key/chunk) + immediately `RunLocalPrefetchRegisterAndPromote` after each chunk (pipelined register); **reserve moved to after BatchQuery confirms SSD-only**.
- **Verification (after batching + defer reserve + outcome split)**: `prefetch_dram_was_resident`≈**0**; `prefetch_hit` **86.4%**; `prefetch_miss_race` **0.7%**; `prefetch_evicted_after_exist` **10.0%**; exist→first register ~**83ms** (shortened from 143ms pre-fix).

### B8. `PrefetchKeys` Staging Key Mismatch → `INVALID_KEY`
- **Symptom**: Prefetch path occasionally `INVALID_KEY` / staging slice missing.
- **Root cause**: In `PrefetchKeys`, `AllocateBatch`/`BatchLoad` map keys must be tenant-scoped **storage key** (`MakeTenantScopedStorageKey`); when inconsistent with logical object key, slice not found.
- **Fix**: `PrefetchKeys` uses `storage_key` for staging, logical key still used for master promotion RPC.

### B9. `analyze_prefetch.sh` Undercounting `prefetch_hit` (grep Order)
- **Symptom**: Analysis script once showed `prefetch_hit_master/local=0`, inconsistent with manual log inspection.
- **Root cause**: Script used one-direction grep (requiring `prefetch_wait_mode=master` before `outcome=prefetch_hit`), but actual log field order is opposite.
- **Fix**: Changed to bidirectional grep (`| outcome=prefetch_hit.*prefetch_wait_mode=master`).

### B10. `BatchOffload` NotifyOffloadSuccess Before commit Local Index → get Path `INVALID_KEY`
- **Symptom**: Under high-concurrency offload + get/prefetch concurrency, massive `Failed to get key` (`res: -400` = `INVALID_KEY`); `BatchLoad` / `SSD read failed` coexisting with prefetch `BatchLoad failed`; Master `SSD Storage` far from full, `EvictDiskReplica=0`; some request-level load failures.
- **Root cause**: Mooncake `BucketStorageBackend::BatchOffload` after **successful disk write** first calls `complete_handler` (internally `NotifyOffloadSuccess`, Master registers `LOCAL_DISK` and can evict MEMORY), **then** commits Client-side `object_bucket_map_`.
           Concurrent get/prefetch within this window takes SSD read path, but local index lacks that storage key → `Key not found` → `INVALID_KEY`. Unrelated to SSD capacity; **Master visibility ahead of Client local index** consistency issue (`publish-before-commit`).
- **Wrong order (pre-fix)**:
  ```
  WriteBucket → NotifyOffloadSuccess → [race window] → commit object_bucket_map_
  ```
- **Fix (2026-06, `storage_backend.cpp`)**:
  ```
  WriteBucket → commit object_bucket_map_ → NotifyOffloadSuccess
  ```
  If `NotifyOffloadSuccess` fails: `RollbackCommittedBucket` rolls back local index + `CleanupOrphanedBucket` deletes orphan files, avoiding "Client can read, Master doesn't know" or reverse ghost replica.
- **Distinction from B8**: B8 is PrefetchKeys **staging key name** error; B10 is offload **commit order** causing get/offload read path index missing. B8 fix does not eliminate B10.
- **Relationship with §2.8 lease change**: lease alignment resolves prefetched key **eviction**; B10 resolves **index not ready** causing `INVALID_KEY`; orthogonal, both need to be merged.

### Key Measured Data (Numbers Driving Above Decisions, for Alignment)
- `exist→get` interval median **15–17s** (amplified by scheduling queueing).
- SSD→DRAM single prefetch **~2ms (p90 2.65ms)**.
- ~**82%** of "prefetch triggered but still SSD" (before lease alignment) because promoted MEMORY replica had lease=0, evicted back to SSD by capacity before `get()`; after §5.3 implementation prefetch path aligned lease with exist/get.
- Throttling: `dedup_ttl` main factor eliminating storm, `cooldown` not triggered; prefetch bandwidth not saturated.
- Before batching / defer reserve: metadata phase caused first register **~143ms** later than first get; `prefetch_hit` ~40% / effective DRAM reads ~64%; exist false trigger ~25%.
- After batching + defer reserve + outcome split: `prefetch_hit` **86.4%**; exist false trigger **0**; `prefetch_miss_race` **0.7%**; `prefetch_evicted_after_exist` **10.0%** (main cause TP1~7 master wait timeout / insufficient lease window, see §5.3, §2.8).
- After B10 fix: get path `INVALID_KEY` zeroed; under high pressure tail may still see `BatchPut insufficient space` (§2.8 Put risk), generally does not block single inference success.
- `ssd_get_wait_ms` default **10ms** (get-side wait enabled by default); `0` disables.
