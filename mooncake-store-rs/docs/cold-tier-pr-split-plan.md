# Cold tier PR split plan

This document records the current split plan for landing the cold tier work as a reviewable PR series.

## Guiding principles

- Keep each PR independently reviewable and testable.
- Preserve the current implementation semantics: cold offload is owned by the runtime that owns the hot object replica.
- Avoid introducing public API names that expose internal `backend` terminology; use `cold_tier_id` / `device_id` for external surfaces.
- Land the LocalDir path first to validate semantics, then add the ExtentStore engine as the performance/recovery backend.
- Do not split tightly coupled admission, offload, restore, and recovery logic into artificial PRs that leave dead configuration or untestable hooks.

## PR 1: Core route and metadata schema

Scope:

- Add cold backing route model: `ColdBackingState`, `ColdBackingRoute`, `ColdBackingReplica`, and cold-target helpers.
- Add cold tier device model: `ColdTierDeviceRecord`, `ColdTierDeviceState`, `ColdTierTargetSpec`, `ColdTierDeviceFilter`, `ColdTierDeviceUpdate`, and `ColdTierUsageDelta`.
- Add metadata APIs for listing cold-backed routes and managing cold tier devices/usages.
- Implement the metadata APIs for in-memory, Redis, and Etcd backends.

Key files:

- `crates/mooncake-store-core/src/route.rs`
- `crates/mooncake-store-core/src/traits.rs`
- `crates/mooncake-store-core/src/lib.rs`
- `crates/mooncake-metadata/src/in_memory.rs`
- `crates/mooncake-metadata/src/redis_backend.rs`
- `crates/mooncake-metadata/src/etcd_backend.rs`
- `crates/mooncake-metadata/src/keyspace.rs`

Out of scope:

- Runtime offload/restore hooks.
- Storage backend implementation.
- Admin HTTP or control-plane operations.
- ExtentStore engine.

## PR 2: Backend abstraction, LocalDir backend, and startup config

Scope:

- Add `PersistentStorageBackend` and `ColdTierBackendResolver`.
- Add binary-only LocalDir backend implementation.
- Add cold tier target startup config and builder wiring.
- Resolve cold tier targets from directories or UUIDs.

Key files:

- `crates/mooncake-store-client/src/client/cold_tier_storage_backend.rs`
- `crates/mooncake-store-client/src/client/types.rs`
- `crates/mooncake-store-client/src/client/builder.rs`

Out of scope:

- Full offload/restore scheduling.
- Device lifecycle API.
- ExtentStore engine.

## PR 3: Device manager, lifecycle, and basic admission

Scope:

- Add shared cold tier device cache and device manager.
- Bootstrap/register/probe devices.
- Track device capacity, usage, reservations, and schedulability.
- Add basic runtime/device admission buckets needed by later offload and restore paths.

Key files:

- `crates/mooncake-store-client/src/client/cold_tier_state.rs`
- `crates/mooncake-store-client/src/client/cold_tier/device.rs`
- `crates/mooncake-store-client/src/client/builder.rs`

Out of scope:

- Complex offload pressure tuning.
- Restore throttling behavior that requires the restore path to exist.

## PR 4: Runtime skeleton and owner-side offload

Scope:

- Add cold tier fields to `StorageOwnerState`.
- Add `ColdTierHandle` and background tick skeleton.
- Hook route publication into owner-side initial cold backing publication.
- Add pending offload queue and materialization.
- Rebuild pending offload work after startup/metadata recovery.
- Add foreground offload kick before allocator eviction.

Key files:

- `crates/mooncake-store-client/src/client/state_core.rs`
- `crates/mooncake-store-client/src/client/cold_tier/scheduler.rs`
- `crates/mooncake-store-client/src/client/cold_tier/offload.rs`
- `crates/mooncake-store-client/src/client/runtime_write.rs`

Out of scope:

- Cold restore read path.
- GC/watermark reclaim.
- Admin offload task API.

## PR 5: Restore path, staging pool, and cold read RPC

Scope:

- Add local cold restore reads.
- Add remote owner cold restore.
- Add staging pool, restore promotion queue, read pins, and ACK handling.
- Add `ReadFromCold` / `BatchReadFromCold` control-plane RPCs.
- Add cold-only miss behavior.

Key files:

- `crates/mooncake-store-client/src/client/cold_tier/restore.rs`
- `crates/mooncake-store-client/src/client/cold_tier/staging_pool.rs`
- `crates/mooncake-store-client/src/control_plane/mod.rs`
- `crates/mooncake-store-client/src/control_plane/client.rs`
- `crates/mooncake-store-client/src/control_plane/server.rs`
- `crates/mooncake-store-client/src/control_plane/codec.rs`
- `crates/mooncake-store-client/src/client/runtime_io.rs`

Out of scope:

- ExtentStore-specific pinned/batch optimizations beyond the backend abstraction.
- Admin HTTP orchestration.

## PR 6: GC, reclaim, watermark, and manual free

Scope:

- Add pending-delete cold backing GC.
- Add device watermark cleanup and free scheduling.
- Add manual GC/free runtime operations.
- Release cold tier usage after successful backend deletion and route CAS.

Key files:

- `crates/mooncake-store-client/src/client/cold_tier/cleanup.rs`
- `crates/mooncake-store-client/src/client/cold_tier/scheduler.rs`
- `crates/mooncake-store-client/src/client/runtime_io.rs`

Out of scope:

- Admin HTTP wrappers if the runtime operation itself is not needed for validation.
- ExtentStore compaction implementation unless required by the backend API.

## PR 7: ExtentStore engine, backend, and recovery

Scope:

- Add `ExtentStoreEngine`.
- Add extent IO worker and batch/pinned read paths.
- Add ExtentStore-backed `PersistentStorageBackend`.
- Add delete journal and startup recovery.
- Wire `ColdTierSsdEngine::ExtentStore` to the backend resolver.

Key files:

- `crates/mooncake-store-client/src/client/extent_store_engine.rs`
- `crates/mooncake-store-client/src/client/extent_store_io_worker.rs`
- `crates/mooncake-store-client/src/client/extent_store_cold_backend.rs`
- `crates/mooncake-store-client/src/client/extent_store_recovery.rs`
- `crates/mooncake-store-client/src/client/types.rs`

Out of scope:

- New user-facing admin API unless needed to select the engine.

## PR 8: Admin operations, observability, benchmarks, and docs

Scope:

- Add Admin HTTP models/routes for cold tier device and object operations.
- Add asynchronous Admin offload task orchestration.
- Add manual GC/free/probe/drain/enable/disable/unregister/create operations.
- Add Prometheus metrics and diagnostics for devices, pending offload, reclaim, SSD IO, restore, and ExtentStore IO.
- Add cold tier benchmarks and final user-facing documentation/config examples.

Key files:

- `crates/mooncake-store-py/src/admin/cold_tier_http.rs`
- `crates/mooncake-store-py/src/admin/cold_tier_service.rs`
- `crates/mooncake-store-py/src/admin/models.rs`
- `crates/mooncake-store-client/src/observability/registry.rs`
- `crates/mooncake-store-client/benches/cold_tier_e2e.rs`
- `crates/mooncake-store-client/benches/cold_tier_kvcache.rs`
- `docs/cold-tier-design.md`

Out of scope:

- Core runtime semantics that should already have landed in earlier PRs.
