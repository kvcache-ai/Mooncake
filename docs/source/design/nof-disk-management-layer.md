# NoF / NVMe-oF Disk Management Layer

## Purpose

This document defines the disk-management layer for Mooncake's NoF / NVMe-oF storage work.

The goal is to recover useful **disk-management** ideas from the reset implementation and integrate them with the current Mooncake NoF/NVMe-oF implementation. The data-path read/write strategy is already implemented and must not be copied from the reset shared-disk code.

## Production Shape

Production NVMe-oF deployments can expose storage in two different ways.

### Opaque NoF Pool

Many physical SSDs, disk nodes, RAID groups, or SPDK bdevs may be aggregated by a lower-level storage system and exposed to Mooncake as one logical NVMe-oF target or namespace.

In this mode Mooncake sees only a logical pool:

```text
many physical disks
  -> lower-level storage system / SPDK / array / SDK
  -> one NVMe-oF target or namespace
  -> Mooncake NoF segment / logical pool
```

Mooncake can manage only what it can observe:

- logical NoF segment mount/remount/unmount
- target endpoint reachability
- logical capacity and usage
- heartbeat health
- allocation eligibility based on lifecycle status and device metadata
- NoF eviction / logical GC
- logical pool failure and recovery

Mooncake cannot directly manage hidden physical disks unless the lower layer exposes those details.

### SDK-managed Disk Pool

A future shared disk SDK or NVMe-oF SDK may expose more detailed metadata:

- disk node state
- physical disk state
- namespace mapping
- capacity and watermarks
- rebuild progress
- failed disk events
- provider-specific metadata

In this mode the SDK should feed metadata into Mooncake through the disk-management layer instead of modifying NoF internals directly.

## Architecture

```text
MasterService
  ├── existing NoF offset read/write path
  ├── existing NoF logical-pool allocation / eviction
  ├── NVMe KV backend data path
  │     └── NvmeKvCommandExecutor           // KV Store / Retrieve / Delete / Iterate
  └── storage device management view
        ├── NoF logical-pool metadata       // current NoFSegmentManager
        └── NVMe KV namespace metadata      // management-only view of KV namespaces
```

The abstraction boundary is management-only. It does not own the data path.
NVMe-oF is a transport under this model: one NVMe-oF namespace may be a normal offset-addressed SSD pool, while another may be a KV namespace that must use NVMe KV opcodes.

## Core Interfaces

The current code adds `mooncake-store/include/storage_device_metadata.h` as a metadata model with these concepts:

- `StorageDeviceIdentity`
- `StorageDeviceMetadata`
- `StorageDeviceMetadataUpdate`
- `StorageDeviceMaintenancePlan`

`StorageDeviceIdentity` supports different granularities:

- `LOGICAL_POOL`
- `TARGET`
- `NAMESPACE`
- `PHYSICAL_DISK`
- `DISK_NODE`

The current NoF implementation uses `LOGICAL_POOL`. A future SDK adapter may report `PHYSICAL_DISK` or `DISK_NODE` updates.

## Current NoF Adapter

`NoFSegmentManager` acts as the current adapter for the disk-management layer.

It already owns:

- mounted NoF segments
- NoF segment owner client IDs
- allocators
- segment lifecycle state

The management layer adds metadata alongside each mounted NoF segment:

- health
- readable / writable / schedulable flags
- capacity total / used / available
- consecutive failures
- last error
- endpoint and transport
- opaque provider metadata

Mounting a NoF segment initializes a logical-pool metadata record. Heartbeat success and failure update the metadata. Unmount transitions the metadata to an unmounting state before the segment is removed.

## Current Admin Observability

The current management surface exposes NoF and NVMe KV device metadata through:

```text
GET /api/v1/devices/metadata
PUT /api/v1/devices/metadata?device_id=<uuid>
GET /api/v1/devices/maintenance
GET /api/v1/devices/gc_candidates
POST /api/v1/devices/recovery?device_id=<uuid>&action=start|complete|fail
POST /api/v1/devices/probe?device_id=<uuid>&action=run|success|fail[&reason=<text>]
```

The metadata `GET` endpoint currently combines `NoFSegmentManager::ListDeviceMetadata()` with any storage backend that implements `StorageBackendInterface::ListStorageDeviceMetadata()`. Mounted NoF pools use `kind=LOGICAL_POOL` and `provider=nof`. NVMe KV namespaces are reported by `NvmeKvStorageBackend` as `kind=NAMESPACE`, `provider=nvme_kv`, and `transport=nvme-of` or `stub` depending on the executor.

The `PUT` endpoint currently accepts only an `enabled` update for one owned device record. For mounted NoF logical pools, operators can use `{"enabled":false}` to set health to `DISABLED` and remove the pool from new allocation, or `{"enabled":true}` to restore `HEALTHY`, `readable`, `writable`, and `schedulable` when the segment lifecycle status is still `OK`. For NVMe KV namespace records, the wrapper routes matching device ids to `NvmeKvStorageBackend::ApplyStorageDeviceMetadataUpdate()`, which maps the enabled/disabled state onto the backend's device selection state. The backend also implements provider probe hooks: probe failure increments the namespace failure counter and records the probe reason; before the failure threshold is reached the namespace is reported as `DEGRADED` and appears in the maintenance view, once the threshold is reached it becomes `FAILED` / non-schedulable, and probe success reuses `RefreshDevices()` / `ReconcileDevices()` to recover without overriding an operator `DISABLED` state.

The maintenance endpoint derives recovery and GC candidates from the combined NoF and storage-backend metadata view. It is a management view only: it does not perform cleanup, rebuild, recovery, or read/write invalidation by itself.

The `gc_candidates` endpoint is a focused, read-only projection of the same maintenance plan: it returns just the GC candidates (devices whose metadata yields a GC reason such as `not_writable`, `not_schedulable`, `unmounting`, or `full`). It reuses the same plan and does not trigger any actual GC.

The recovery endpoint advances provider state through the owning device adapter. For mounted NoF logical pools, `action=start` marks a pool `REBUILDING` and removes it from new allocation, `action=complete` restores `HEALTHY` / readable / writable / schedulable, and `action=fail` marks recovery failure. For NVMe KV namespaces, the wrapper routes the action to `NvmeKvStorageBackend`: `start` and `fail` make the namespace maintenance-visible as failed/non-schedulable, while `complete` reuses `RefreshDevices()` / `ReconcileDevices()` to reconnect or refresh the namespace without overriding an operator `DISABLED` state. The endpoint still does not copy data or rebuild NoF offset state.

The probe endpoint records or runs a provider probe for one device. For NVMe KV namespaces, `action=run` executes a lightweight KV namespace probe through the NVMe KV connector and executor, currently by issuing an `Iterate()` command with an empty visitor. `action=fail` increments the namespace failure counter and records the supplied reason, while `action=success` reuses the provider refresh path to recover and clear errors. For NoF logical pools, the endpoint applies the equivalent metadata update. This endpoint is a manual/control-plane hook; a future background probe worker can call the same provider hooks.

NVMe KV namespace metadata reports probe-related observability so operators can see how close a namespace is to being marked `FAILED`. The first-class `consecutive_failures` and `last_error` fields carry the probe failure count and last probe error, and `opaque_provider_metadata` additionally carries `consecutive_failures`, `failure_threshold`, `device_serial`, and `namespace_uuid`. These are all derived from existing device runtime state — no extra data-path calls are made to populate them.

The admin server also hosts an optional background probe worker (off by default, enabled via `MasterAdminServer::SetDeviceProbeIntervalSeconds(interval)` before `Start()`). When enabled, it periodically calls `WrappedMasterService::ProbeStorageBackendDevicesForAdmin()`, which runs the same provider probe hook (`ProbeStorageDevice`) used by the manual `action=run` path against every storage-backend-owned device. This drives health transitions (`HEALTHY` → `DEGRADED` → `FAILED`, and recovery back to `HEALTHY`) without operator action, while still respecting an operator `DISABLED` state. NoF logical pools are not probed by this worker. The worker shares the same start/stop/semaphore lifecycle as the metric report thread and is joined on shutdown.

Allocator eligibility is driven by the management metadata: `writable=false`, `schedulable=false`, `FAILED`, `DISABLED`, and `UNMOUNTING` remove the logical pool from new allocation. If the lifecycle status remains `OK`, a later update can restore eligibility.

`readable=false` is currently reported for observability and lifecycle state, but the NoF read path is not changed by this layer. Existing completed NoF replicas continue to follow the existing metadata/descriptor read strategy until later recovery or invalidation work explicitly changes that behavior.

Capacity fields are refreshed from allocator snapshots when metadata is listed, so admin consumers do not need to call `QuerySegments` first to observe current usage.

## Recovery with Data Rebuild

This section describes the recovery workflow that rebuilds data when a storage device fails. The core job model, planning, copy execution, recovery gating, and progress metadata are implemented. The first validation target is a plain SSD device (a local `LOCAL_DISK` / file-backed storage backend), not NVMe-oF or an SDK-managed pool, so the rebuild path can be exercised without special hardware.

### Goal and non-goals

Goal: when a storage device is recovered, re-establish the configured replica redundancy for objects that lost a replica on that device, so that reads can be served again and the device returns to `HEALTHY` only after its data is consistent.

Non-goals for the first iteration:

- No NVMe-oF / SDK-managed rebuild. Validate on plain SSD first.
- No change to the NVMe KV executor data path. NVMe KV namespace recovery stays reconnect/refresh; KV objects are not block-rebuilt by this workflow.
- No new replica placement policy. Rebuild reuses the existing `ReplicateConfig` and the existing copy/move task machinery.

### Why a job, not an inline call

A full rebuild can touch many objects and take a long time, so it must be asynchronous, observable, cancelable, and resumable. The existing `DrainJob` framework (`JobType` / `JobStatus` / per-unit progress counters in `QueryJobResponse`, plus the copy-task machinery used by drain) already models exactly this. The design reuses that framework rather than inventing a parallel one:

- add `JobType::REBUILD` alongside `JobType::DRAIN`
- a `RebuildJob` reuses the same `JobStatus` lifecycle (`CREATED → PLANNING → RUNNING → SUCCEEDED/FAILED/CANCELED`) and the same `succeeded_units / failed_units / blocked_units / active_units / migrated_bytes` progress counters
- a rebuild "unit" is one object replica that must be re-created elsewhere
- per-object rebuild reuses the existing copy task (`CreateCopyTask` / replica add) — recovery does not get its own data-movement path

### Recovery → rebuild flow

The recovery endpoint keeps the same `start | complete | fail` verbs, but `complete` is no longer "reconnect and declare healthy". The proposed semantics:

```text
POST /api/v1/devices/recovery?device_id=<uuid>&action=start
  -> mark device REBUILDING, remove from new allocation (unchanged)
  -> create a RebuildJob for objects whose replicas lived on this device
  -> return the job id in the response

POST /api/v1/devices/recovery?device_id=<uuid>&action=complete
  -> only allowed once the RebuildJob is SUCCEEDED
  -> if rebuild is still RUNNING, return UNAVAILABLE_IN_CURRENT_STATUS
  -> on success, restore HEALTHY / readable / writable / schedulable

POST /api/v1/devices/recovery?device_id=<uuid>&action=fail
  -> cancel the RebuildJob, mark recovery failed
```

Job progress is observed through the existing job query endpoint, so no new progress API is needed.

### Plan phase (which objects to rebuild)

In `PLANNING`, the job enumerates objects that had a replica on the recovering device and whose current live replica count is below the configured redundancy:

- for a NoF logical pool device, this is the set of NoF replicas whose segment maps to that device id
- candidate objects come from the existing replica list / metadata, filtered by device id
- objects already at full redundancy are skipped (counted as `succeeded_units` immediately)

The plan output is a list of rebuild units `{key, tenant_id, source_replica, needed_targets}`.

### Run phase (rebuild each unit)

Each unit is processed with bounded concurrency (`max_concurrency`, same as drain):

1. pick a healthy source replica for the object (must be `readable`)
2. pick eligible target device(s) using existing allocation eligibility (the recovering device is excluded until it is `HEALTHY` again)
3. issue a copy task to create the missing replica (reuse `CreateCopyTask`)
4. on copy success, add the replica and increment `succeeded_units` + `migrated_bytes`
5. on copy failure, increment `failed_units`; if no healthy source exists, increment `blocked_units`

The job is `SUCCEEDED` when every unit is either rebuilt or already redundant, `FAILED` if any unit is permanently unrebuildable (no healthy source), and `CANCELED` on `action=fail`.

### Interaction with the read path

This is the part that must be reviewed carefully, because the current layer explicitly does **not** touch the NoF read path.

- While a device is `REBUILDING`, its replicas are treated as not a valid new-allocation target, but existing readable replicas on *other* devices continue to serve reads through the existing strategy.
- A replica on the failed device is marked unreadable only when it is actually gone; the rebuild creates a fresh replica elsewhere rather than rewriting the failed device's descriptors in place.
- `complete` flips the device back to `readable/writable/schedulable` only after the job confirms redundancy is restored, so the read path never has to special-case a half-rebuilt device.

### Rebuild progress metadata

`StorageDeviceMetadata` gains rebuild observability while a job is active, surfaced via `opaque_provider_metadata` (no new first-class fields needed for the first iteration):

- `rebuild_job_id`
- `rebuild_total_units`, `rebuild_succeeded_units`, `rebuild_failed_units`
- `rebuild_migrated_bytes`

These mirror the job's own counters so the device view and the job query stay consistent.

### First-iteration validation (plain SSD)

The first implementation targets a plain SSD / file-backed storage backend:

- a key has a memory replica plus an SSD replica
- the SSD device is marked failed (probe failure or operator action)
- `action=start` creates a `RebuildJob` that copies the lost SSD replica to another eligible SSD/segment from a healthy source
- the device is only restored to `HEALTHY` after the job reports `SUCCEEDED`
- an end-to-end test asserts the object is fully readable with restored redundancy after recovery

NVMe-oF and SDK-managed rebuild are explicitly out of scope for this iteration and remain reconnect/refresh until a later phase.

### Implementation staging (for post-review work)

To keep the implementation low-risk and modular, the rebuild work should be landed in small phases that reuse existing job/task code rather than introducing a new orchestration framework.

#### Phase 1 — Job model only (no copy execution yet)

Goal: make rebuild observable without moving data yet.

Planned code touchpoints:

- `mooncake-store/include/rpc_types.h`
  - add `JobType::REBUILD`
  - reuse `QueryJobResponse` as-is (no new query schema in phase 1)
- `mooncake-store/include/master_service.h`
  - add `RebuildJob` parallel to `DrainJob`
  - add `CreateRebuildJobRequest` only if `CreateDrainJobRequest` shape is insufficient; otherwise reuse the existing request shape plus a device id
- `mooncake-store/src/master_service.cpp`
  - add `CreateRebuildJob`, `QueryRebuildJob`, `CancelRebuildJob`
  - extend the existing `job_dispatch_thread_` / `ProcessDrainJobs()` pattern to iterate rebuild jobs too, or rename the helpers to generic job helpers if the refactor stays small

Output of phase 1:

- `POST /api/v1/devices/recovery?action=start` can create a rebuild job and return a job id
- `QueryJobResponse` shows `type=REBUILD`, progress counters, and message text
- no real copy is issued yet; every rebuild unit remains `blocked` until later phases

#### Phase 2 — Plain SSD rebuild planning

Goal: enumerate exactly which object replicas need rebuilding.

Planned code touchpoints:

- reuse existing replica enumeration (`GetReplicaList`, batch replica lookups, object metadata)
- add a planner helper that maps `device_id -> affected object replicas`
- only include objects whose live replica count is below redundancy and that still have a healthy readable source

Output of phase 2:

- `RebuildJob` enters `PLANNING` then `RUNNING`
- `blocked_units` / `succeeded_units` reflect whether each candidate unit has a healthy source and still needs work
- still no real copy execution yet

#### Phase 3 — Execute rebuild with existing copy task path

Goal: create the missing replica using the existing data movement machinery.

Planned code touchpoints:

- reuse `CreateCopyTask` as the execution primitive
- add `ActiveRebuildTask` parallel to `ActiveDrainTask`, or generalize the task record if the shared fields remain clean
- reuse the existing refresh / retry / completion accounting style from:
  - `RefreshDrainJobTasks`
  - `ScheduleDrainJobTasks`
  - `MaybeCompleteDrainJob`

Output of phase 3:

- each rebuild unit creates a copy task to a healthy eligible SSD/segment
- success increments `succeeded_units` and `migrated_bytes`
- failed copies increment `failed_units`
- unrebuildable objects (no healthy source) stay `blocked_units`

#### Phase 4 — Gate device recovery completion on successful rebuild

Goal: make `action=complete` semantically correct.

Planned code touchpoints:

- `WrappedMasterService::RecoverDeviceForAdmin`
- `MasterAdminServer::HandleDeviceRecoveryAction`
- NoF device metadata transitions in `MasterService` / `NoFSegmentManager`

Semantics:

- `complete` returns `UNAVAILABLE_IN_CURRENT_STATUS` while the rebuild job is not `SUCCEEDED`
- only after job success does the device return to `HEALTHY/readable/writable/schedulable`
- `fail` cancels the job and leaves the device failed / maintenance-visible

#### Phase 5 — Rebuild progress metadata on the device view

Goal: expose rebuild state without inventing a second progress model.

Planned code touchpoints:

- `StorageDeviceMetadata.opaque_provider_metadata`
- NoF device metadata snapshot construction

Expose:

- `rebuild_job_id`
- `rebuild_total_units`
- `rebuild_succeeded_units`
- `rebuild_failed_units`
- `rebuild_migrated_bytes`

The device view remains a projection of the job state, not a separate source of truth.

#### Phase ordering rationale

This ordering is intentional:

- phases 1–2 are design-safe and mostly metadata/job plumbing
- phase 3 is the first phase that actually moves data
- phase 4 changes externally visible recovery semantics only after execution exists
- phase 5 is observability polish and can land after the functional path

This keeps each reviewable change small and avoids a single "big bang" rebuild patch.

## Provider Feedback

Future lower-level integrations should report state through `StorageDeviceMetadata` snapshots and `StorageDeviceMetadataUpdate`.

Adapters should translate provider-specific state into Mooncake metadata:

```text
provider event / poll result / backend snapshot
  -> StorageDeviceMetadata or StorageDeviceMetadataUpdate
  -> owning NoF / NVMe KV provider metadata state
```

Provider-specific fields should go into `opaque_provider_metadata` unless they are common enough to become first-class fields. Update fields are optional so providers can update one dimension without overwriting the rest, including explicitly clearing string fields such as `last_error`.

Provider updates affect scheduling only when routed to the owning data-path provider. For a mounted NoF logical pool, `writable=false`, `schedulable=false`, `FAILED`, `DISABLED`, or `UNMOUNTING` removes the allocator from new allocation. For an NVMe KV namespace, the same metadata should later route to the NVMe KV backend's device selection state instead of the NoF allocator.

Effective state should be derived from multiple sources, with precedence similar to:

```text
manual disable > provider failed > heartbeat failed > provider degraded > healthy
```

## Mooncake-managed Logical Pool

When the lower layer exposes only one opaque NoF pool, Mooncake manages the logical pool itself.

### Mount

On `MountNoFSegment`:

1. Create the existing NoF allocator.
2. Register the segment in `NoFSegmentManager`.
3. Create `StorageDeviceMetadata` with:
   - kind = `LOGICAL_POOL`
   - provider = `nof`
   - health = `HEALTHY`
   - readable / writable / schedulable = true
   - capacity_total = segment size
   - mount_endpoint = `te_endpoint`

### Heartbeat

On heartbeat success:

- clear failure count
- clear last error
- set health to `HEALTHY`

On heartbeat failure:

- increment failure count
- record last error
- set health to `DEGRADED`
- existing heartbeat logic may still unmount the NoF segment after the configured threshold

### Unmount

On prepare unmount:

- set health to `UNMOUNTING`
- set readable / writable / schedulable to false
- remove allocator from allocation path

On commit unmount:

- remove the mounted NoF segment
- clear heartbeat state
- existing metadata cleanup keeps using current NoF invalid-handle cleanup

### Capacity / Usage

Capacity and usage are derived from existing NoF allocators and metrics. The disk-management layer records them in `StorageDeviceMetadata` so GC and recovery policy have a single management-facing view. Metadata snapshots refresh allocator-derived capacity before returning to callers, so management consumers do not depend on a prior query call to observe current usage.

## Reset Code: What to Reuse

The reset implementation should be used only as a reference for disk management.

Useful concepts:

- device pool / target pool structure
- runtime device state
- failure counters
- capacity-disabled state
- config-disabled state
- usage accounting
- startup catalog / scan coordination
- cleanup and GC metadata
- rebuild / recovery metadata
- stable device identity such as serial / namespace / attachment identity

## Reset Code: What Not to Reuse

Do not copy or restore the reset shared-disk data-path strategy.

Specifically do not reintroduce:

- reset shared-disk read target resolution
- reset heartbeat/offload write decision flow
- reset `LOCAL_DISK` descriptor rewrite as the current read strategy
- reset shared-disk read/write policy
- NVMe KV-specific control-plane naming as the generic disk-management layer

The existing NoF/NVMe-oF read/write strategy remains authoritative.

## NVMe KV over NVMe-oF Relationship

NVMe-oF is the transport; NVMe KV is the namespace data model. A remote KV namespace such as `/dev/ng0n1` should be managed by the disk-management layer but must not be mounted as a normal NoF offset segment.

The data path remains:

```text
remote NVMe KV namespace
  -> NVMe-oF connect
  -> /dev/ng0n1
  -> NvmeKvCommandExecutor
  -> Store / Retrieve / Delete / Iterate
```

It must not enter:

- Transfer Engine `nvmeof_transport`
- CUFile / GDS offset read/write
- NoF segment allocator offset path
- `MountNoFSegment` as a fake logical pool

The management view should represent the namespace as metadata similar to:

```text
provider       = "nvme_kv"
kind           = NAMESPACE
transport      = "nvme-of"
mount_endpoint = "/dev/ng0n1"
namespace_id   = nsid / nguid / uuid
target_id      = NQN / controller identity
device_id      = Mooncake NVMe KV device id
capacity_total = KV namespace capacity
capacity_used  = NVMe KV backend observed used bytes
readable       = true / false
writable       = true / false
schedulable    = true / false
health         = HEALTHY / DEGRADED / FAILED / DISABLED
```

NVMe KV-specific concerns stay in the NVMe KV backend:

- limited physical key size
- hashed physical keys
- collision slots
- chunk keys and chunk manifests
- catalog and object layout
- backend-specific key conflict policy

Those details should not define the generic disk-management abstraction. The generic layer only tracks health, readability, writability, schedulability, capacity, target / namespace identity, and maintenance state.

### Rollout Plan for NVMe KV Namespaces

1. Keep NVMe KV data I/O on `NvmeKvCommandExecutor`.
2. Add NVMe KV device snapshots converted to `StorageDeviceMetadata`.
3. Expose those snapshots as `provider=nvme_kv`, `transport=nvme-of`, `kind=NAMESPACE` metadata so admin and maintenance views can see them.
4. Route `enabled` updates for NVMe KV namespace device ids to the NVMe KV backend so operator disable / enable affects NVMe KV device selection.
5. Longer term, split the management view into a composite manager over NoF logical pools, NVMe KV namespaces, and future catalog / SDK providers.

## Future Work

The recovery-with-data-rebuild workflow (job model, planning, copy execution, recovery gating, progress metadata) is implemented. Remaining future work:

- real startup catalog scan workflow and lease coordination (blocked on external SDK/catalog availability; not scheduled)
- end-to-end rebuild validation on plain SSD with real data (current tests validate the job lifecycle but do not exercise multi-node copy tasks)
- NVMe-oF and SDK-managed rebuild (explicitly out of scope; NVMe KV namespace recovery stays reconnect/refresh)

Those extensions should continue to preserve the existing NoF offset read/write path and the NVMe KV executor data path.
