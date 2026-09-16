# SSD Prefetch-on-Exist

Related: RFC #2213, promotion-on-hit (PR #2071).

## Goal

`is_exist` / `batch_is_exist` with `ExistOptions.prefetch_to_memory = true` may
start a best-effort SSD→DRAM promotion for keys that only have a `LOCAL_DISK`
replica, so a follow-up `get()` can be served from DRAM. Prefetch must not
change exist/get semantics, must not block the caller, and may be dropped on
any failure. If the disk replica is remote, the holder node reads its own SSD
into its own DRAM.

The feature is off by default (`enable_ssd_prefetch = false`); with the switch
off the exist/get paths behave exactly as before.

## Path

```
is_exist(..., prefetch_to_memory=true)          # synchronous, no RPC added
  → SsdPrefetcher::triggerSsdPrefetch           # throttle.reserve() dedup
    → prefetch thread pool (fixed size 4)
      → 128-key chunks: BatchQueryReadOnly      # no lease, no promotion-on-hit
        → classify: SSD-only (LOCAL_DISK complete, no MEMORY), size > 0
          → local holder:  RegisterPrefetchTask + FileStorage::PrefetchKeys
          → remote holder: prefetch_offload_object RPC → holder promotes locally
```

`PrefetchKeys` shares the promotion execution chain with promotion-on-hit
(`PromoteOneKeyFromLocalDisk`: `PromotionAllocStart` → staging `AllocateBatch`
→ `BatchLoad` → `PromotionWrite` → `NotifyPromotionSuccess`). It does **not**
reuse promotion-on-hit admission (frequency sketch, DRAM watermark gate), the
lease-on-query, or the holder's promotion heartbeat mailbox.

Metadata queries use the read-only admin replica RPCs
(`GetReplicaListForAdmin` / `BatchGetReplicaListForAdmin`, also registered on
the master RPC server), so prefetch never grants a lease, never triggers
promotion-on-hit, and never counts `valid_get` metrics as a side effect.

## Master side: RegisterPrefetchTask

New additive RPC (older masters reject it and prefetch degrades to a no-op).
Semantics differ from promotion-on-hit admission:

- tenant-aware: the key is looked up under the caller's tenant;
- holder-only: `holder_id == client_id`, others get `INVALID_PARAMS`;
- no frequency/watermark admission, no heartbeat mailbox push;
- shares the `promotion_in_flight_` / `promotion_queue_limit_` cap with
  promotion-on-hit (both compete for the same DRAM);
- idempotency is explicit: if a MEMORY replica or an in-flight promotion task
  already exists, the call fails with `PROMOTION_ALREADY_EXISTS`, which the
  caller treats as "skip quietly", not as an error. Prefetch and
  promotion-on-hit therefore cannot double-promote the same key: both paths
  allocate from the same per-key `promotion_tasks` entry.

`NotifyPromotionSuccess` with `from_prefetch = true` grants the same read
lease (`default_kv_lease_ttl`) as exist/get, so the promoted DRAM replica
survives until the follow-up `get()`. If the exist→get gap can exceed the hard
lease, raise `--default_kv_lease_ttl`; note that larger leases can raise Put
`NO_AVAILABLE_HANDLE` under a high DRAM watermark (a capacity trade-off, not a
prefetch bug).

## Throttle, threads, failure handling

Per-client `PrefetchThrottle` (sharded, lock-free fast path per shard):

- `ssd_prefetch_dedup_ttl_sec` (default 30): a key prefetches at most once per
  window. Applies at trigger time (before any RPC) so hot probes on
  DRAM-resident keys do not cause metadata-query storms; the async job still
  re-classifies precisely after `BatchQueryReadOnly`.
- Failed keys retry after the (shorter) cooldown window, not the full TTL.
- `ssd_prefetch_cooldown_sec` (default 5): when promotion fails with
  `NO_AVAILABLE_HANDLE` (DRAM saturated), prefetch backs off so it stops
  competing with eviction/offload, which are what actually frees DRAM. 0
  disables the backoff.
- All prefetch work runs on a fixed-size thread pool (4 workers). If the pool
  is down (shutdown), the job is dropped — prefetch never spawns a thread per
  probe and never falls back to unbounded detached threads.
- Every failure past `PromotionAllocStart` releases the master-side staged
  state eagerly (`NotifyPromotionFailure`, RAII guard) instead of waiting for
  the reaper TTL.

## get() wait (opt-in)

`ssd_get_wait_ms` (default 0 = off) in the batch-get path: when the selected
replica is `LOCAL_DISK`-only and there is evidence of an in-flight promotion —
the local throttle shows the key triggered, or a single read-only re-query
shows a `PROCESSING` MEMORY replica — wait up to the budget for it to
complete, then serve from DRAM; otherwise read from SSD immediately. With the
default (0) there is no extra RPC and no added latency on any get.

The re-query is read-only and grants no lease; between observing a COMPLETE
MEMORY replica and the actual transfer the replica could in principle be
evicted. That race is accepted (best-effort): the transfer simply fails and
the caller retries/falls back per the existing error path.

## Rolling-upgrade compatibility

- No existing RPC signature changes.
- `RegisterPrefetchTask` and `prefetch_offload_object` are additive: old
  masters/holders reject them, and prefetch degrades to a no-op.
- The read-only replica queries reuse the pre-existing admin RPC handlers.

## Configuration

| key | default | meaning |
|---|---|---|
| `enable_ssd_prefetch` | false | master switch (client config) |
| `ssd_prefetch_cooldown_sec` | 5 | DRAM-pressure backoff; 0 disables |
| `ssd_prefetch_dedup_ttl_sec` | 30 | per-key trigger dedup TTL; 0 disables |
| `ssd_get_wait_ms` | 0 | get-side wait budget; 0 disables |

Requires `enable_ssd_offload` + `ssd_offload_path` (prefetch promotes from the
local SSD tier; without offload there are no LOCAL_DISK replicas).

## Code map

- `prefetch_throttle.h` — throttle state machine (sharded).
- `master_service.*` — `RegisterPrefetchTask`, `PromotionTask::from_prefetch`,
  prefetch lease in `NotifyPromotionSuccess`.
- `rpc_service.*` / `master_client.*` / `client_service.*` — RPC wiring.
- `file_storage.*` — `PrefetchKeys`, `PromoteOneKeyFromLocalDisk` (shared with
  promotion-on-hit), `LookupLocalObjectSize`.
- `storage_backend.*` — `GetObjectDataSize`.
- `ssd_prefetcher.*` — trigger/dedup/classify/delegate and the prefetch pool.
- `real_client.*` — exist-path trigger, get-path wait.
- `replica.h` (`ExistOptions`), `store_c`, `store_py`, `dummy_client` — API
  plumbing.
