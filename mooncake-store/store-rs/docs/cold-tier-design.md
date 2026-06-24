# Storage Backend Hot/Cold Design

This document defines the target backend semantics for `mooncake-store-rs` after switching from the current locator-as-replica model to a hot/cold storage model.

## Goals

- Keep DRAM as the serving tier for normal reads and writes
- Use backend storage as a cold durability tier instead of a peer replica medium
- Reuse the existing storage-owner eviction scheduler for DRAM pressure relief
- Add backend offload and restore paths without breaking the current transport-backed DRAM path
- Keep route transitions explicit and correctness-preserving under CAS races

## Non-goals

- Do not make backend objects first-class hot replicas
- Do not replace the existing DRAM segment allocator or TE/TENT data path
- Do not redesign the existing background eviction scheduler from scratch
- Do not require every read miss to perform a globally optimal placement decision

## Current behavior summary

Today the runtime can publish `ReplicaLocator::BackendObject` directly into `ObjectRoute.replicas`. That means backend objects are treated as ordinary replicas:

- writes may publish backend-object replicas directly
- reads may read backend-object replicas directly
- route and reclaim logic still reason in terms of peer replicas

That behavior is not aligned with the desired backend semantics.

## Target behavior summary

The new model is:

- write to DRAM first
- asynchronously offload from DRAM to backend
- evict DRAM only after backend offload is materialized
- read from DRAM first
- on DRAM miss, restore from backend and repopulate DRAM
- clean up old backend objects during overwrite/delete reclaim

In short:

- DRAM = hot serving tier
- backend = cold backing tier

## Reuse of the existing eviction scheduler

The current storage-owner eviction implementation already provides a background watermark-driven scheduler for reclaiming DRAM replicas:

- it polls local allocator usage
- it evicts from high watermark down to low watermark
- it uses storage-owner CLOCK selection
- it removes the victim replica from route state via route-owner CAS
- it releases allocator bytes only after CAS succeeds

This existing scheduler should be kept.

The required change is semantic, not architectural:

- eviction remains DRAM eviction
- eviction must only apply to objects whose cold backing is already materialized
- if the last hot replica is evicted but cold backing exists, the route must remain as a cold-only route instead of being deleted

## Data model changes

## Hot replicas remain in `replicas`

`ObjectRoute.replicas` should represent only hot, directly serviceable replicas in DRAM segments.

Those replicas still use the existing segment-backed fields and are still owned by storage runtimes.

## Add explicit cold backing state

Add a dedicated cold-backing description to `ObjectRoute` instead of treating backend objects as ordinary replicas.

Current shape:

```rust
pub struct ObjectRoute {
    pub key: ObjectKey,
    pub version: RouteVersion,
    pub state: RouteState,
    pub compatibility: CompatibilityDescriptor,
    pub replicas: Vec<ReplicaRoute>,
    pub cold_backing: Option<ColdBackingRoute>,
}
```

```rust
pub struct ColdBackingRoute {
    pub owner: ClientRuntimeId,
    pub cold_tier_id: String,
    pub object_locator: String,
    pub length: u64,
    pub checksum: Option<u64>,
    pub state: ColdBackingState,
}
```

`cold_tier_id` on `ColdBackingRoute` is kept for route compatibility, but the current implementation uses it as the registered cold tier device key for backend lookup. Bootstrap/Admin-created devices currently use `device_id == cold_tier_id`; if those identities diverge later, persisted routes should continue to identify the concrete `device_id`.

```rust
pub enum ColdBackingState {
    PendingOffload,
    Materialized,
    PendingDelete,
}
```

### Why not keep `BackendObject` inside `replicas`

Keeping backend objects inside `replicas` preserves the wrong mental model:

- the read path naturally treats backend as a direct replica source
- the write path naturally publishes backend as a peer replica target
- eviction cannot distinguish hot serviceability from cold durability
- route state keeps mixing serving location with backup state

Splitting hot replicas from cold backing makes the desired semantics explicit.

## Object lifecycle

## 1. Write

For `put(key, value)`:

1. place and write the object into DRAM
2. publish a hot route containing DRAM replicas
3. set `cold_backing = PendingOffload`
4. return success to the caller
5. enqueue background offload

Foreground write success depends on DRAM write success and route publication, not backend completion.

## 2. Offload

A background offload worker:

1. reloads the current route for `(key, version)`
2. confirms the object still needs offload
3. verifies the referenced cold tier device is still schedulable and has capacity
4. reserves cold tier bytes in metadata
5. reads bytes from a hot DRAM replica
6. writes them to backend via `PersistentStorageBackend::put_object(...)`
7. CAS-updates the route to `cold_backing = Materialized`
8. commits reserved bytes into used bytes in metadata

If backend write succeeds but route CAS loses a race:

- re-read the route
- if the version is stale, delete the orphan backend object
- if the same version still needs materialization, retry the route update

### Persisted pending-offload source

The current implementation persists a pending offload source during background offload while `cold_backing.state == PendingOffload`.

That extra persisted source is used only as a recovery seam:

- foreground writes still succeed on the DRAM-first path and do not synchronously write LocalDir pending-source files
- background offload first reads the bytes from live DRAM, then stages the pending source before materializing the backend object
- heartbeat repair can still materialize the object from a staged pending source if the original hot segment is no longer readable
- once backend materialization succeeds, the pending source is deleted

Operationally this means the backend now has two temporary states during offload:

- final materialized cold object
- transient `__pending__` source payload used for restart/recovery

## 3. Eviction

The existing watermark eviction scheduler remains responsible for DRAM pressure.

A victim is eligible only when:

- the object still has a valid route
- the route is active
- the chosen local hot replica still exists
- `cold_backing.state == Materialized`

Eviction then:

1. removes the hot replica from `replicas` via route-owner CAS
2. releases allocator bytes after CAS success

If the evicted replica was the last hot replica:

- keep the route if `cold_backing` is materialized
- transition to a cold-only route with `replicas = []`
- delete the route only when neither hot replicas nor cold backing remain

## 4. Read / restore

For `get(key)`:

1. resolve route
2. attempt DRAM hot replicas first
3. if DRAM misses and `cold_backing = Materialized`, read from backend
4. verify payload length/checksum
5. restore a hot replica into DRAM
6. CAS-update the route to include the restored hot replica
7. return payload

A backend read should not fail the user request just because DRAM re-promotion fails. The restore path should prefer returning correct data over requiring immediate route promotion.

### Local and remote restore paths

Restore has two execution shapes:

1. **Local restore**: the reader is also the storage owner for the selected materialized cold backing. The reader resolves the backend locally, reads the cold payload, verifies length/checksum, returns the payload, and enqueues best-effort DRAM promotion.
2. **Remote owner restore**: the reader sees a materialized cold backing owned by another runtime. The reader asks that owner to stage the cold payload into owner DRAM through the control plane, then reads the staged bytes through the existing transport path.

The remote path keeps cold backend authority with the owner runtime that owns the cold backing. A reader should not resolve another runtime's local cold directory directly. The control-plane request therefore carries the namespace and owner authority and the owner validates that the request is addressed to its local route namespace and stable identity before serving a cold read.

### Cold read control-plane RPCs

Remote restore uses dedicated owner-side RPCs:

- `ReadFromCold`: stage one materialized cold backing and return a temporary segment/offset plus transport metadata.
- `BatchReadFromCold`: stage multiple cold backings owned by the same authority; backends should use batch/read-into APIs where available instead of thin per-object loops.
- `AckColdReadComplete`: reader notifies the owner that staged bytes are no longer needed.
- `PinForRead`: verifies staged slots when needed by compatibility paths; it must not create an unbounded capability to pin arbitrary owner memory.

`ReadFromCold` may return backpressure when restore admission or the staging pool is saturated. Backpressure must be preserved across protobuf/transport boundaries as `StoreError::Backpressure` so callers can retry or fall back without treating the condition as data loss.

### Staging pool, read pins, and ACK lifecycle

Remote owner restore stages cold bytes into a bounded DRAM staging pool registered with the transport layer. The owner returns transport-readable coordinates only after the SSD/NFS read has completed and the payload has passed length/checksum validation.

The staging slot lifecycle is:

1. owner allocates a staging slot from the bounded pool
2. owner reads the cold backend into that slot
3. owner pins the `(segment, offset)` while a remote reader may still be reading it
4. owner replies with segment/offset, target chunks, transport endpoint, descriptor, checksum, and timing fields
5. reader performs the normal remote segment read
6. reader sends `AckColdReadComplete`
7. owner unpins the slot and recycles it when no reader pin remains

Promotion is intentionally decoupled from the remote read critical path. After staging data is ready, the owner may asynchronously copy the staged bytes into a normal hot replica allocation and CAS the route to add a DRAM replica. Promotion failure must not invalidate the successful cold read; it only means a later read may have to restore again.

The staging pool must be bounded and reclaimable. Expired pending staging slots can be swept, but the timeout is a last-resort cleanup mechanism; the normal lifecycle is reader ACK followed by unpin and slot recycling.

### Restore singleflight and batching

Cold restore should deduplicate concurrent work for the same logical cold payload:

- reader/local restore singleflight should include route identity and cold backing identity, not only the object name, so overwrite races do not reuse stale payloads
- owner-side remote restore should avoid issuing duplicate SSD reads for concurrent requests while preserving route/cold-backing correctness
- batch restore should group requests by cold tier device/backend and use backend batch-read interfaces where supported

A successful cold read must verify the restored length and checksum against the materialized `ColdBackingRoute` before returning bytes to the caller or publishing a restored hot replica.

## 5. Overwrite / delete cleanup

Overwrite and delete must reclaim both tiers:

- old DRAM replicas continue through route-aware segment reclaim
- old cold objects are marked `PendingDelete`
- backend cleanup removes old `object_locator` entries after the route change is authoritative

The cleanup path is not eviction. It is cold-object reclaim / garbage collection.

## Schedulers

## Existing eviction scheduler

Keep the current scheduler and CLOCK victim selection.

Required semantic changes:

- gate victim eligibility on `cold_backing = Materialized`
- preserve cold-only routes when the last hot replica is evicted

## New offload scheduler

Add a background offload worker responsible for pending cold materialization.

Suggested idempotency key:

```text
(owner, key, route_version)
```

Suggested responsibilities:

- deduplicate repeated enqueue attempts
- retry transient backend failures with backoff
- delete stale orphan backend objects after lost CAS races
- rebuild pending work on restart from route state
- reuse persisted pending offload source when DRAM bytes are no longer readable during rebuild

## New restore coordinator

Restore should be front-path driven and coordinated with per-key singleflight rather than being delegated to a generic low-priority worker.

Suggested idempotency key:

```text
(owner, key, route_version)
```

Responsibilities:

- prevent duplicate concurrent restores
- perform backend read on hot miss
- attempt DRAM re-promotion
- allow successful reads even if re-promotion fails

## Current implementation status

The current tree has landed the production-facing cold tier device registry and data-path integration:

- `StoreClientBuilder::cold_tier_target(...)` accepts an explicit cold tier target and bootstraps it as a metadata-backed device
- `ColdTierTargetConfig` supports `directory(...)` and `uuid(...)` forms for SSD intent
- `ColdTierKind::Nfs` is validated as an `nfs` / `nfs4` mount and currently accepts directory targets only
- Python compatibility config accepts `cold_tier_targets=[...]`
- the standalone client accepts `--cold-tier-id`, `--cold-tier-kind`, `--cold-tier-directory`, `--cold-tier-uuid`, `--cold-tier-tags`, and `--cold-tier-capacity-bytes`
- `MC_STORE_RS_COLD_TIER_TARGETS` provides the env fallback using JSON array syntax
- admin HTTP manages `/v1/cold-tier/devices` with create/list/get/register/unregister/disable/enable operations
- metadata stores device lifecycle, capacity, used bytes, reserved bytes, target spec, root dir, tags, and failure state
- the membership worker refreshes a cold tier device cache used by write/offload admission
- new cold backings are admitted only onto schedulable healthy devices with enough capacity
- background offload reserves, commits, and releases device usage accounting in metadata
- overwrite/delete reclaim decrements cold tier usage for materialized objects
- unregister without `force` moves a device with live cold backings to `Draining`; `force` unregister only succeeds for hot-replicated materialized backings and marks them `PendingDelete`
- placement can choose a non-local schedulable device and route the local directory backend to that device root
- cold-only routes are readable through local restore or remote owner restore
- the owner control plane exposes cold read RPCs for staging, batched staging, ACK, and read-pin verification
- remote cold reads stage bytes through a bounded transport-registered staging pool before the reader performs the normal remote segment read
- restore promotion is asynchronous and best-effort; user-visible read success does not depend on route promotion success
- restore timing and backpressure are propagated through the control-plane response

Current scope limitations:

- the compatibility layer currently resolves a single startup cold tier target, even though the env / Python shape is a list for forward compatibility
- admin-managed additional devices can participate in placement when their metadata record includes a resolvable `root_dir`
- SSD intent is validated structurally (directory / UUID resolution) and does not attempt hardware-media certification
- NFS validation is mount-type based and does not attempt deeper storage benchmarking

## SSD end-to-end validation status

The current e2e binary now validates the directory-backed SSD flow end to end:

- explicit SSD cold tier target wiring through `ColdTierTargetConfig::directory(...)`
- hot write -> pending cold backing -> materialized cold backing transition
- backend object materialization on disk
- overwrite path with new cold backing identity
- tenant-scoped delete and backend cleanup
- benchmark phases followed by benchmark object cleanup so later shrink validation is not polluted by leftover benchmark routes
- true client shrink validation with convergence-aware waiting in the full e2e environment

This was validated with a full local run of `cargo run -p mooncake-store-e2e` on the current host using a directory-backed SSD target root.

## Compatibility strategy

The codebase already has history and compatibility paths for `ReplicaLocator::BackendObject`.

Migration should therefore be phased:

### Phase A: read compatibility

- legacy backend-object replicas remain decodable
- runtime treats them as legacy cold backing for fallback reads
- DRAM remains the preferred serving path

### Phase B: stop generating new backend replicas

- new writes publish hot DRAM replicas only
- backend state is expressed through `cold_backing`

### Phase C: lazy convergence

- overwrite, restore, or route rewrite paths rewrite legacy routes into the new model

## Required invariants

- DRAM bytes are never released before route CAS removes the corresponding hot replica
- a hot replica is not eligible for eviction until cold backing is materialized
- backend objects created for stale route versions are eventually cleaned up
- the last hot replica can disappear without losing the object if cold backing exists
- read correctness does not depend on restore promotion succeeding synchronously
- remote staged restore memory is protected by read pins until the reader ACKs completion or last-resort staging-slot expiry reclaims it
- cold read RPCs must validate the route namespace and owner authority before exposing staging coordinates
- cold read backpressure must remain distinguishable from data corruption or not-found errors
- cold tier usage accounting must reserve before backend write and must release or commit on every outcome
- unregister cannot strand cold-only objects on a removed device; safe force unregister only marks hot-replicated materialized backings for deletion

## Implementation sequence

1. extend route schema with `cold_backing`
2. update write path to DRAM-first + `PendingOffload`
3. add offload worker and route materialization
4. update eviction eligibility and cold-only route handling
5. add restore-on-miss path
6. add overwrite/delete cold cleanup
7. add legacy route compatibility and recovery/repair

## Files most likely to change

- `crates/mooncake-store-core/src/route.rs`
- `crates/mooncake-store-client/src/client/runtime_write.rs`
- `crates/mooncake-store-client/src/client/runtime_io.rs`
- `crates/mooncake-store-client/src/client/state_store.rs`
- `crates/mooncake-store-client/src/client/runtime_alloc.rs`
- `crates/mooncake-store-client/src/client/state_core.rs`
- `crates/mooncake-store-client/src/client/backend.rs`
- `crates/mooncake-store-client/src/client/posix_backend.rs`

## Testing focus

- write returns after DRAM publish even before backend materialization
- successful offload marks `cold_backing = Materialized`
- eviction never removes a hot replica without materialized cold backing
- last-hot eviction produces a cold-only route instead of dropping the object
- read miss restores from backend and can return data even if promotion fails
- two-client remote restore covers an embedded storage client that writes, evicts to a materialized cold-only route, reads back from cold tier, and allows a second client to trigger owner-side cold read
- owner-side staging ACK releases read pins and recycles staging slots without leaking or prematurely reusing memory
- batch cold restore uses backend batch read/read-into paths when objects share a backend/device
- overwrite/delete clean up old cold objects
- startup/heartbeat rebuild can materialize pending offloads from persisted pending source
- a full e2e flow covers write -> materialize -> cold-only eviction -> backend restore -> overwrite -> delete
- legacy backend-object routes remain readable during migration

## Admin HTTP control plane

The Admin HTTP API uses `/v1/cold-tier` as its base path. It manages Mooncake cold tier devices, not operating-system block devices or mounts. Operating-system provisioning remains the responsibility of deployment tooling.

Device identity fields:

| Field | Meaning |
|---|---|
| `device_id` | Mooncake-managed logical device identity used by routes and placement |
| `cold_tier_id` | Human-readable cold tier alias, for example `ssd-0` |
| `stable_id` | Storage runtime stable identity |
| `epoch` | Current live runtime incarnation for fencing |
| `kind` | Cold tier kind, for example `ssd` or `nfs` |
| `target` | Runtime-resolved target spec such as a directory or filesystem UUID |
| `root_dir` | Resolved cold root directory |
| `state` | Device lifecycle state |
| `schedulable` | Whether the device accepts new offload placement |

Implemented device operations include create, list, get, register, unregister, disable, enable, drain, blockers query, manual GC, manual free, object cold backing query, pending offload trigger, offload task status/listing, and quarantine reporting.

`register` and `unregister` are Mooncake registry operations. `unregister` without force must not strand cold-only objects on the removed device. Safe forced unregister is allowed only when materialized objects still have hot replicas and can be marked `PendingDelete`.

## Scheduling, capacity, and backpressure

Cold tier scheduling is storage-owner local and bounded:

- pending offload materialization uses a bounded queue and retry/backoff state
- cold device placement checks device lifecycle, schedulability, capacity, and watermarks
- usage accounting reserves bytes before cold object write and commits or releases reservation on every outcome
- foreground writes remain DRAM-first and should not synchronously materialize cold payloads
- free and cleanup paths are route-gated and must not delete data that is still authoritative in route metadata

The first production target is LocalDir/SSD. The traits and target model leave room for NFS, UUID-resolved mounts, and other storage engines, but correctness and capacity safety take priority over adding more backends.

## Offload priority modes

Cold tier has two offload modes:

- `Passthrough`: writes publish `PendingOffload`; background materialization drains the pending offload queue.
- `EvictTriggered`: writes do not publish a materialized cold backing immediately; eviction selects a hot replica victim and forces materialization before releasing DRAM.

Priority policy is mode-aware:

- pending offload policy supports FIFO and size-based ordering for passthrough materialization
- eviction policy supports CLOCK and bounded coldest-largest-first selection
- forced eviction materialization bypasses ordinary queue priority by claiming the selected route directly
- all scans remain bounded; no global sort or unbounded metadata lookup should be introduced on the hot path

## Reliability and observability priorities

Cold tier correctness depends on these operational invariants:

- LocalDir writes use the binary object format; runtime JSON fallback is not part of the production path
- pending sources are transient recovery seams and are deleted after materialization succeeds
- Redis/Lua metadata mutations must preserve JSON compatibility for empty vector fields
- metadata reconnect, route CAS races, and timeout-after-success cases must not leak usage accounting or orphan authoritative routes
- admin and metrics surfaces must expose device state, used/reserved bytes, failure state, offload progress, and cleanup outcomes

## SSD KV engine direction

The directory-backed LocalDir cold tier remains the reference/debug backend, but it is not the desired fast path for KVCache-oriented SSD cold tier use. A production SSD path should avoid one filesystem object per cached object, JSON payload encoding, temp-file/rename metadata traffic, page-cache copies, and per-object `fsync`.

The target SSD KV engine should approximate the useful system properties of NVMe KV SSDs on ordinary NVMe devices without depending on the NVMe KV command set:

- append many cold objects into large segment files instead of per-object files
- encode payloads with the binary cold object format only
- use extent-style locators in `ColdBackingRoute.object_locator`
- read and write through direct I/O / `io_uring` where available
- make batch offload and batch restore first-class paths
- track segment liveness so delete/overwrite can mark extents dead and reclaim by segment
- keep route metadata authoritative for object visibility instead of duplicating a full engine-level key index
- throttle cleaning so GC does not dominate restore tail latency

The intended route integration stays the same as the rest of the hot/cold model:

```text
write -> hot DRAM route + PendingOffload
background SSD append -> Materialized cold backing with SSD extent locator
eviction -> remove hot replica only after materialized cold backing
hot miss -> restore from SSD and best-effort re-promote into DRAM
delete/overwrite -> route-gated mark-dead of the old SSD extent
```

For the first implementation, the SSD engine can keep the existing string `object_locator` field and encode an extent locator such as:

```text
ssd-v1:<segment_id_hex>:<offset_hex>:<record_len_hex>:<value_offset_hex>:<value_len_hex>:<generation_hex>
```

The engine should return the final materialized locator after append succeeds. A pending route may use a provisional locator keyed by object/version, but the materialized route must identify the concrete segment extent.

Recommended initial engine shape:

```text
StoreClient cold tier
  -> PersistentStorageBackend
      -> SsdKvPersistentStorageBackend
          -> SsdKvEngine
              -> io_uring worker
              -> segment manager
              -> binary record encoder/decoder
              -> liveness accounting
              -> background cleaner
```

Recommended first implementation phases:

1. segment backend with binary records, append-only writes, extent locators, read-into-buffer support, mark-dead delete, and simple whole-segment recycling
2. dedicated `io_uring` worker with bounded queue depth, batched offload, batched restore, aligned buffer handling, and direct reads into restore buffers when possible
3. lifecycle-aware placement by tenant/namespace hash, size class, TTL/session bucket where available, plus throttled cleaner policies
4. optional persistent recovery with manifest/checkpoint metadata, segment generation tracking, lazy validation, and orphan reclamation

Suggested modules for the SSD fast path are:

```text
crates/mooncake-store-client/src/client/ssd_kv_backend.rs
crates/mooncake-store-client/src/client/ssd_kv_engine.rs
```

Design invariants for the SSD KV engine:

- foreground write success must not depend on SSD materialization
- DRAM bytes must not be evicted before cold backing is materialized
- route state remains authoritative for object visibility
- CAS-lost SSD writes must be marked orphan/dead and reclaimed later
- delete/overwrite should not synchronously rewrite segment files or punch holes per object
- SSD reads may satisfy user requests even if DRAM re-promotion fails
- batch paths should not be thin loops around single-object operations
- no RocksDB/LSM, per-object JSON files, runtime JSON fallback, or public `backend` terminology for cold-tier APIs

## ExtentStore performance review and optimization direction

The community Mooncake `io_uring` implementations and this ExtentStore direction operate at different abstraction layers:

- community `io_uring` file/transport code is a simple low-risk offset I/O accelerator
- ExtentStore is a KVCache-aware cold storage engine that owns object layout, extent locators, route-authoritative visibility, restore/offload semantics, liveness, and recovery boundaries

The optimization direction is therefore not to replace ExtentStore with a generic `io_uring` transport. ExtentStore should keep the KV-like engine semantics, while absorbing the community design lessons that avoid global contention and keep the I/O fast path simple, observable, and fallback-friendly.

Key architectural conclusions:

- ExtentStore's main advantage is KVCache lifecycle awareness, not fixed-file or fixed-buffer registration by itself.
- Append-only segment layout can create restore locality only if runtime restore uses backend batch reads instead of issuing mostly single-object restores.
- Route metadata remains the authoritative index; the engine should not introduce an independent LSM-style object index for visibility.
- Restore correctness must not depend on DRAM promotion succeeding.
- Cleaner/GC is a storage-engine responsibility once cold objects are packed into shared segments.
- Delete journal durability should support a strict mode for tests/strong durability and a normal group-commit mode for high-churn cache workloads.
- Worker/ring design should evolve toward per-device/per-queue sharding when profiling shows the current dedicated worker is a bottleneck; avoid a global ring or global lock hot path.
- Direct I/O, fixed file, fixed buffer, and GDS paths are optimizations, not the core design. They must expose hit/fallback counters and should not dominate the control flow before batch restore and copy reduction are addressed.

Profiling must come before structural optimization. The minimum profiling surface should split restore/offload latency into route lookup, locator parse, singleflight wait/leader work, backend queue wait, physical I/O submit/complete, checksum/decode, copy-to-caller, promotion allocation/copy/CAS, delete journal append/sync, and total p50/p95/p99. It should also expose object size distribution, batch size distribution, coalesced read ratio, average physical read size, SQE count per logical object, direct/scratch/buffered ratios, scratch copy bytes, worker busy ratio, queue depth, and route CAS retry counts.

The prioritized optimization sequence is:

1. Add fine-grained profiling and benchmark coverage for restore/offload and worker contention.
2. Connect runtime cold batch restore to backend batch read/read-into-buffer by grouping locators by cold device/backend and preserving object-level singleflight initially.
3. Reduce restore copies with read-into-destination singleflight: single waiter reads into caller or promotion buffer; multiple waiters share a leader buffer and copy only when necessary.
4. Implement a minimal conservative cleaner for sealed segments: choose high-dead-ratio candidates, verify each live record against the current route, copy-forward live records, CAS routes to new locators, and treat CAS-lost copies as dead.
5. Add delete journal batching/group commit with strict and normal modes; normal mode may conservatively leak space across a crash window but must not make live data unreachable.
6. Add minimal KVCache-aware segment placement, starting with size class and TTL/session bucket before adding more dimensions.
7. Shard I/O workers/rings by device and queue class only if profiling shows the single worker/queue is limiting throughput or p99.
8. Clarify GPU/GDS restore paths with large aligned direct reads, pooled pinned-host fallback, and success/fallback counters.

For architecture-level changes in these phases, use profiling data and compare against mature storage/runtime patterns before implementation. Relevant references include log-structured segment cleaning, RocksDB-style compaction tradeoffs where applicable, `io_uring` per-thread/per-device queue patterns, and cache admission/promotion strategies. Do not introduce speculative abstractions or complex fast paths without a measured bottleneck and a clear fallback.

## Consolidated development scope

This document is the single cold tier design reference. Detailed research notes, local findings, SSD KV engine notes, and previous Chinese design/review drafts have been consolidated or removed from the PR to avoid splitting the same design across multiple documents.
