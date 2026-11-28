# SSD Prefetch-on-Exist

Related: RFC #2213, promotion-on-hit (PR #2071).

## Goal

`is_exist` / `batch_is_exist` with `ExistOptions.prefetch_to_memory = true`
triggers a best-effort SSD→DRAM promotion for keys that only have a
`LOCAL_DISK` replica so a follow-up `get()` can hit DRAM. Prefetch never blocks
the caller, does not alter exist/get semantics, and may be dropped on failure.
For a remote disk replica, the holder reads its local SSD and writes into a
cluster DRAM segment allocated by the master (`PromotionAllocStart` passes an
empty `preferred_segments` list, not necessarily the holder's own DRAM).

Off by default (`enable_ssd_prefetch = false`).

> **Backend scope:** On the disk holder, client prefetch queries on-disk size
> via `StorageBackendInterface::GetObjectDataSize()`, currently implemented
> **only by `BucketStorageBackend`**. Other backends return empty and skip
> prefetch. (Master `promotion-on-hit` uses the task's `size` directly and is
> unaffected.)

---

## Architecture & Trigger Flow

### Component architecture

```mermaid
classDiagram
    class SsdPrefetcher {
        +TriggerPrefetch(keys)
        +RunLocalPrefetch(keys, sizes)
        -ThreadPool prefetch_pool_
    }
    class PrefetchThrottle {
        +reserve(keys) keys
        +enterCooldown()
        +inCooldown() bool
        -Shard shards_
    }
    class FileStorage {
        +PrefetchKeys(keys, sizes, dram_pressure, on_key_done)
        +LookupLocalObjectSize(key) size
        -PromoteOneKeyFromLocalDisk(key, tenant_id, size, dram_pressure)
    }
    class MasterService {
        +RegisterPrefetchTask(client_id, key, tenant_id)
        +NotifyPromotionSuccess(client_id, key, tenant_id)
        -promotion_tasks
    }
    class Client {
        +RegisterPrefetchTask(key)
        +BatchQueryReadOnly(keys)
    }
    SsdPrefetcher --> PrefetchThrottle
    SsdPrefetcher --> Client : metadata and register
    SsdPrefetcher --> FileStorage : execute promotion
    SsdPrefetcher ..> SsdPrefetcher : prefetch_offload_object RPC to remote holder
    Client --> MasterService : additive RPCs
    FileStorage --> Client : promotion chain calls
```

### End-to-end trigger flow (exist probe)

On an embedded `RealClient`, `TriggerPrefetch` adds **no synchronous RPC** on
the caller's thread — it deduplicates via `PrefetchThrottle` and enqueues onto
a 4-worker pool. (`DummyClient` forwards `ExistOptions.prefetch_to_memory` to
its standalone `mooncake_client` via additive RPCs.) Metadata classification
uses read-only admin RPCs (`GetReplicaListForAdmin` /
`BatchGetReplicaListForAdmin`), so it grants no lease, never triggers
promotion-on-hit, and does not increment `valid_get`.

```mermaid
sequenceDiagram
    participant E as Engine
    participant P as SsdPrefetcher
    participant T as PrefetchThrottle
    participant M as Master
    participant H as Holder

    E->>P: TriggerPrefetch(keys), sync on RealClient
    P->>T: reserve(keys), TTL dedup for local and remote alike
    T-->>P: unseen subset
    P->>P: enqueue pool job, caller returns here
    loop per 128-key chunk
        P->>M: BatchQueryReadOnly, no lease, no promotion, no metrics
        M-->>P: replica descriptors
        P->>P: ClassifySsdPrefetchRoute, SSD-only, COMPLETE, size above 0
    end
    alt local holder
        P->>H: LookupLocalObjectSize (BucketStorageBackend)
        H->>M: RegisterPrefetchTask with holder client_id and tenant
        M-->>H: OK, or PROMOTION_ALREADY_EXISTS (skip quietly)
        H->>H: PrefetchKeys then PromoteOneKeyFromLocalDisk
        H->>M: NotifyPromotionSuccess, grant read lease from_prefetch
    else remote holder
        P->>H: prefetch_offload_object RPC, additive, old peers drop
        Note over H: holder runs the same local branch<br/>with its own client_id
    end
```

During client teardown, `SsdPrefetcher::Shutdown()` rejects new jobs, drains
queued and in-flight work, and joins the 4 workers before releasing client or
segment resources.

---

## PrefetchThrottle

16-shard per-key state machine (`reserve()` locks the target shard; cooldown
and disabled-TTL checks are lock-free atomics) with lazy per-entry expiry and
amortized per-shard sweeps (`O(batch)` per probe):

```mermaid
stateDiagram-v2
    [*] --> kTriggered : reserve()
    kTriggered --> kInFlight : RegisterPrefetchTask OK
    kTriggered --> kAlreadyResident : query shows MEMORY
    kTriggered --> kDelegated : remote holder RPC sent
    kInFlight --> kCompleted : NotifyPromotionSuccess
    kInFlight --> kFailed : any step failed
    kFailed --> [*] : cooldown-length backoff, then retryable
    kCompleted --> [*] : dedup TTL, then re-prefetchable
    kAlreadyResident --> [*] : dedup TTL
    kDelegated --> [*] : dedup TTL, holder owns execution state
```

- **`ssd_prefetch_dedup_ttl_sec` (default 30 s)**: Suppresses re-triggering for
  healthy states (`kCompleted`, `kAlreadyResident`, `kDelegated`) at trigger
  time, preventing metadata-query storms on DRAM-resident keys.
- **`ssd_prefetch_cooldown_sec` (default 5 s)**: Shorter retry window for
  `kFailed` keys, and global backoff when `PromotionAllocStart` returns
  `NO_AVAILABLE_HANDLE` so prefetch yields DRAM to eviction/offload (`0`
  disables).

---

## Master: RegisterPrefetchTask

Additive RPC (older masters reject it and prefetch degrades to a no-op):

```mermaid
flowchart TD
    A[client_id, key, tenant] --> B{serving guard?}
    B -- no --> X1[UNAVAILABLE_IN_CURRENT_STATUS]
    B -- yes --> C{object exists?}
    C -- no --> X2[OBJECT_NOT_FOUND]
    C -- yes --> D{Put in flight?<br/>InProcessing}
    D -- yes --> X3[REPLICA_IS_NOT_READY]
    D -- no --> E{MEMORY replica or<br/>promotion task exists?}
    E -- yes --> X4[PROMOTION_ALREADY_EXISTS<br/>caller skips quietly]
    E -- no --> F{COMPLETE LOCAL_DISK source?}
    F -- no --> X3
    F -- yes --> G{"holder_id == client_id?"}
    G -- no --> X5[INVALID_PARAMS]
    G -- yes --> H{promotion_in_flight<br/>queue limit?}
    H -- full --> X6[KEYS_ULTRA_LIMIT]
    H -- pass --> I[pin source refcnt<br/>emplace PromotionTask from_prefetch=true]
```

- Skips promotion-on-hit's frequency sketch, DRAM watermark gate, and heartbeat
  mailbox, but **shares** `promotion_in_flight_` / `promotion_queue_limit_` and
  per-key `promotion_tasks` so the two paths cannot double-promote a key.
- `NotifyPromotionSuccess` on a `from_prefetch` task grants the standard
  `default_kv_lease_ttl` read lease so the promoted replica survives until the
  follow-up `get()`.

---

## FileStorage Execution

`PromoteOneKeyFromLocalDisk` is shared verbatim by `ProcessPromotionTasks`
(promotion-on-hit) and `PrefetchKeys` (prefetch):

```mermaid
sequenceDiagram
    participant F as FileStorage
    participant C as Client
    participant M as Master
    F->>C: PromotionAllocStart(key, tenant, size)
    alt NO_AVAILABLE_HANDLE
        C-->>F: error, set dram_pressure, throttle enters cooldown
    end
    Note over F: PromotionStateGuard armed (RAII) —<br/>any later failure triggers NotifyPromotionFailure
    F->>F: AllocateBatch staging, then BatchLoad SSD read
    F->>C: PromotionWrite, TE write into staged replica
    F->>C: NotifyPromotionSuccess
    C->>M: replica COMPLETE, lease granted from_prefetch
    Note over F: guard.Dismiss()
```

Failures past `PromotionAllocStart` release staged master state immediately via
`NotifyPromotionFailure` (RAII guard). Per-key errors never propagate; they
update `SsdMetric.prefetch_complete/fail_total` and `PrefetchThrottle`.

---

## exist / get Wiring

- **`exist`**: `is_exist` / `batch_is_exist` with
  `ExistOptions.prefetch_to_memory = true` and `enable_ssd_prefetch = true`
  calls `TriggerPrefetch` (or forwards via `DummyClient`).
- **`get` demand-side kick**: Supported read entrypoints whose best replica is
  `LOCAL_DISK` fire `TriggerPrefetch(disk_keys, ignore_cooldown=true)` before
  starting the SSD transfer, never blocking or waiting on promotions. The
  bypass applies only to the requester's client-side cooldown (master gates,
  dedup TTL, and remote holder cooldown still apply).

| Read API family | Client demand-side kick | Master `promotion-on-hit` |
|---|---|---|
| `get_buffer` (Python `get`, `get_tensor`; `DummyClient::get_buffer` shared-buffer fallback) | Yes (prefetch-enabled `RealClient`) | Yes |
| `batch_get_into_multi_buffers` (C / Python / `DummyClient` wrappers) | Yes (prefetch-enabled `RealClient`) | Yes |
| `batch_get_buffer` (Python `get_batch`, `batch_get_tensor`), `batch_get_into`, single-object `get_into` (`get_tensor_into`, `mooncake_store_get_into`), `batch_get_into_cuda_ipc`, `get_into_ranges*`, `batch_get_into_multi_buffer_ranges` | No | Yes |

---

## Compatibility & Configuration

- **Rolling-upgrade compatibility**: No existing RPC signature changes.
  `RegisterPrefetchTask`, `prefetch_offload_object`, and `DummyClient`
  exist-with-options RPCs are additive (older peers reject or fall back to
  ordinary exist queries). Read-only queries reuse existing admin RPCs.

| Key | Default | Meaning |
|---|---|---|
| `enable_ssd_prefetch` | `false` | Master switch (client config) |
| `ssd_prefetch_cooldown_sec` | `5` | DRAM-pressure backoff in seconds; `0` disables |
| `ssd_prefetch_dedup_ttl_sec` | `30` | Per-key trigger dedup TTL in seconds; `0` disables |

- **Requester vs. Holder**: Both sides must enable prefetch. The SSD holder
  needs `enable_ssd_offload` and a `BucketStorageBackend` storage directory; a
  requesting `RealClient` can enable `enable_ssd_prefetch=true` via
  `setup(dict)` without local offload or a DRAM segment.
- **Standalone `mooncake_client` & `DummyClient`**: Set
  `MOONCAKE_OFFLOAD_FILE_STORAGE_PATH` and enable `--enable_ssd_prefetch=true`
  (replace `192.168.1.10` with your addresses). Keep
  `--start_offload_rpc_server=true` for the dedicated offload RPC listener
  (when `false`, the main client RPC listener serves remote disk reads and
  prefetch). `DummyClient` (`setup_dummy()`) forwards
  `ExistOptions.prefetch_to_memory=true` to its `mooncake_client`.

```bash
mooncake_client \
  --host="192.168.1.10" \
  --master_server_address="192.168.1.10:50051" \
  --metadata_server="P2PHANDSHAKE" \
  --enable_offload=true \
  --enable_ssd_prefetch=true \
  --ssd_prefetch_cooldown_sec=5 \
  --ssd_prefetch_dedup_ttl_sec=30
```

---

## Code Map

- `prefetch_throttle.h` — sharded throttle state machine.
- `master_service.*` / `object_runtime_state.h` — `RegisterPrefetchTask`,
  `PromotionTask::from_prefetch`, prefetch lease in `NotifyPromotionSuccess`.
- `rpc_service.*` / `master_client.*` / `client_service.*` — RPC wiring.
- `file_storage.*` — `PrefetchKeys`, `PromoteOneKeyFromLocalDisk`,
  `LookupLocalObjectSize`.
- `storage_backend.*` — `GetObjectDataSize` (`BucketStorageBackend`).
- `ssd_prefetcher.*` — trigger/dedup/classify/delegate and worker pool.
- `real_client.*` — exist-path trigger, get-side kick.
- `replica.h` (`ExistOptions`), `store_c`, `store_py`, `dummy_client` — API
  plumbing.
