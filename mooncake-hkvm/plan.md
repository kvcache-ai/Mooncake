# HKVM — Hierarchical KVcache Management: Design & Implementation Plan

**Module:** `mooncake-hkvm`
**Status:** Design draft
**Scope:** Scale Mooncake Master to 10,000+ GPU cards (10,000 dummy clients) by
hierarchizing KV-cache allocation, RPC routing, and HA/recovery.

---

## 1. Problem statement

In AntGroup's Theta KVPool practice, a single LLM service reaches **10,000 cards
(≈ 10,000 dummy clients)**. At that scale the **Mooncake Master** becomes the
bottleneck and fails in four concrete ways (mirrored in `README.md`):

1. **Allocator management is too heavy.** One global `SegmentManager` +
   `AllocatorManager` owns every mounted segment and every live allocator's
   free-space view, serialized under a single shared-mutex region.
2. **RPC processing exhausts CPU threads.** One `coro_rpc_server` + one
   `WrappedMasterService` funnel all ~60 RPC methods into one process; metadata
   sharding exists but lives inside a single process, so it yields no CPU
   fan-out.
3. **HA recovery time is unacceptable.** Promotion re-materializes the entire
   cluster's object table from snapshot/OpLog one object at a time.
4. **Single active Master.** Even with HA on, multiple `MasterServiceSupervisor`
   instances run, but only the leader serves allocation; standbys are
   read-only OpLog followers.

The goal of HKVM is to make the management layer **hierarchical** so that each
tier owns a disjoint, independently serviceable subset of the cluster, and only
tier-local work touches a hot lock or a hot RPC handler.

---

## 2. Current architecture (baseline)

Dimensions confirmed against code at `git HEAD` (`1e35557f init project`).

### 2.1 Master

- **Object / process:** `mooncake_master`, built from
  `mooncake-store/src/master.cpp`. Single process, one `coro_rpc_server`.
- **Core object:** `MasterService` (`mooncake-store/include/master_service.h:107`),
  a monolithic per-process object holding all state:
  - Single global `SegmentManager segment_manager_`
    (`mooncake-store/include/segment.h:443`) with one
    `std::shared_mutex segment_mutex_` (`segment.h:499`) guarding *all* segment
    state and the entire `AllocatorManager`.
  - `AllocatorManager` (`allocation_strategy.h:26`): maps
    `segment_name -> vector<BufferAllocatorBase>`. Doc comment states external
    synchronization is required, provided today by `segment_mutex_`.
  - Metadata sharded into `std::array<MetadataShard, 1024> metadata_shards_`
    (`master_service.h:1313`), each shard having its own `SharedMutex` — *already
    horizontally sharded, but within one process only.*
  - Liveness: `ok_client_` set + a single lock-free
    `client_ping_queue_{128*1024}` (`master_service.h:1987`).
- **RPC layer:** `WrappedMasterService` (`rpc_service.h:22`) — exactly one
  `MasterService`, ~60 methods, all registered on one `coro_rpc_server` in
  `RegisterRpcService` (`rpc_service.cpp:1637`).

### 2.2 Allocation path (fully centralized)

`MasterService::PutStart` → `AllocateAndInsertMetadata`
(`master_service.cpp:3335`):

1. `ScopedAllocatorAccess = segment_manager_.getAllocatorAccess()`
   (`master_service.cpp:3375`) — acquires `segment_mutex_` (shared) for the
   **entire allocation**.
2. `allocation_strategy_->Allocate(allocator_manager, ...)`
   (`master_service.cpp:3417`); strategies chosen by `AllocationStrategyType`
   (`RandomAllocationStrategy`, `FreeRatioFirstAllocationStrategy`,
   `SsdFreeRatioFirstAllocationStrategy`, `CxlAllocationStrategy`).
3. Concrete segment allocators (`CachelibBufferAllocator` /
   `OffsetBufferAllocator`) take their own internal bulk-allocation locks.

Clients do **not** learn placement from a distributed source of truth — they ask
the one master, which holds the full global view under one lock.

### 2.3 Client path

- `MasterClient` ctor (`include/master_client.h:76`) creates one
  `RpcClientPool` against a **single configured master address**.
- `MasterClient::Connect` (`master_client.cpp:421`) — talks to exactly one
  master; a client does not discover a leader itself.
- RPC fan-in: per-process single pool → single master.
- After failover, `Ping` returns `NEED_REMOUNT` and clients must
  `ReMountSegment` against the (new) leader — a potential reconnect storm.

### 2.4 HA / recovery

- `MasterServiceSupervisor::RunSupervisorLoop`
  (`ha/leadership/master_service_supervisor.cpp:218`):
  standbys replicate the leader's OpLog (`HotStandbyService::ReplicationLoop`),
  then on leadership acquisition promote via
  `PromoteStandbyAndExport()` → `HotStandbyService::PromoteAndExportSnapshot`
  → `MasterService::RestoreFromStandbySnapshot` (`master.cpp:2506`), which
  re-inserts every `StandbyObjectEntry` one by one.
- `FinalCatchUpForPromotionLocked` bounds catch-up to a **30-second window**
  (`hot_standby_service.cpp:520-561`).
- Followers serialize on a single `std::mutex` in `StandbyMetadataStore`
  (`hot_standby_service.cpp:54`).

---

## 3. Design goals

| Goal | Why | Current failure |
|---|---|---|
| **G1 — Decentralize allocation** | Remove cluster-wide lock | `segment_mutex_` serializes every `PutStart` |
| **G2 — Shard RPC processing across processes** | Distribute CPU fan-out | One `MasterService` + one RPC server |
| **G3 — Partial (regional) failover** | Bounded recovery time | promotion re-exports whole cluster |
| **G4 — Serve reads/allocations from multiple instances** | "Single active Master" is a ceiling | leader only; standbys read-only |
| **G5 — Backward compatible** | Keep existing client API where possible | — |

---

## 4. Proposed architecture

Introduce a **three-tier hierarchy**, reusing names already present in the
codebase where possible.

```
                    +----------------------------+
                    |   Root Master (HkvmRouter)  |
                    |  - routing / membership    |
                    |  - global view_version     |
                    |  - cross-region placement  |
                    |  - tiny: no hot state      |
                    +-------------+--------------+
                    ping          | lookup(key / worker id)
                                  v
       +----------------+  +----------------+  +----------------+
       | Region Master A |  | Region Master  |  | Region Master N|
       |  RegionalAlloc  |  | ...            |  | ...            |
       |  per-region HA  |  |                |  |                |
       +----------------+  +----------------+  +----------------+
                |  data placement descriptors            |
                v                                        v
        workers/clients (KVCacheClient / DummyClient) served by one region
```

### 4.1 Tier 0 — Root Master / Router (`HkvmRouter`)

A **thin, stateless(ish)** object that:

- Maintains a `MasterView` mapping `worker group / workgroup key → region`.
- Answers `Ping`, `LookupRegion(key)`, and membership `RegisterRegion`.
- Holds **no** per-object metadata, **no** allocators, **no** hot segment state.
- Is the *only* component with which a brand-new client first communicates.

Because it is stateless w.r.t. objects, it can run on any leader/standby set with
negligible recovery (G2, G4).

### 4.2 Tier 1 — Regional Master / `RegionalAllocator`

- Owns a **disjoint subset** of segments / `AllocatorManager`s, partitioned by
  worker region or group.
- Serves all existing master RPCs **scoped to its region**: `PutStart`,
  `GetReplicaList`, `CopyStart`, `MoveStart`, lifetime management.
- Each region has its **own `SegmentManager`-equivalent** with **region-local
  `segment_mutex_`**, so allocation contention is bounded to the region, not the
  cluster (G1).
- Each region runs as its own `coro_rpc_server` → real CPU fan-out across
  processes (G2).

### 4.3 Tier routing object — `HkvmRouter`

A new lightweight component (mirroring `SegmentManager` naming style):

```cpp
class HkvmRouter {
  // group_id / tenant / key-hash prefix -> RegionalAllocator address
  Shimmap<KeyRange, Endpoint> region_table_;
  uint64_t view_version_ = 0;
  // tiny membership state only
};
```

The root answers `Lookup(key, worker_id)` → returns the owning `RegionalAllocator` endpoint; the client's existing `MasterClient::RpcClientPool` (`master_client.h:669`, a per-address pool) then serves the region directly — no new client transport needed.

### 4.4 What the client config looks like

Preserve a venmo of the current single-master client:

- Client still starts with **one well-known bootstrap address** (the Root).
- After bootstrap it is handed a *regional* endpoint and pins its `RpcClientPool`
  to that region for the bulk of traffic.
- On `NEED_REMOUNT` or region-leader change, `Ping` path returns the new region
  endpoint (or Root re-resolves).

This keeps G5 (backward compatibility) for the client protocol: the *routing*
object is new, the *leaf* client-to-region protocol is the existing master RPC.

---

## 5. Sharding / partitioning scheme (G1)

Reuse `AllocationStrategy` to pick a **region first**, then allocate inside the
delegated `RegionalAllocator`'s own `AllocatorManager`, **without holding a
cluster-wide lock**.

### Proposal: hash-of-key(group) + capacity-driven region selection

1. Group by worker/instance (`group_id`), mirroring the existing
   `workgroup`/peer-group notion.
2. Root route allows region assignment by group:
   - **Hash-based fallback:** `region = hash(key) mod NUM_REGIONS` for even,
     deterministic split.
   - **Capacity-aware:** Root tracks each region's aggregate `free_segment_bytes`
     (updated lazily / near-realtime) and steer new mounts to a region with
     headroom, avoiding global free-space recompute on every Put.
3. Within a region, the existing `FreeRatioFirstAllocationStrategy` (or any
   retained strategy) applies unchanged; that lookup touches only
   region-local state.

### Cross-region exception (RW)
- Store-wide ops that genuinely span regions (rare: cross-region `CopyMove`,
  global `quota`, `GetReplicaList` for multi-region replicas) escalate up to the
  Root, which orchestrates but does **not** enumerate object tables.

---

## 5. HA / recovery (G3, G4)

Make the failover **per-region** instead of whole-cluster:

| Failure domain today | HKVM behavior |
|---|---|
| Single leader owns RPC port; standbys read-only | Each **region owns a leader**. Region leaders are fans-out; multiple regions serve concurrently (G4). |
| Promotion re-exports whole object table | `RestoreFromStandbySnapshot` becomes **region-scoped** — only the affected region's objects/segments re-export; bounded by region size (G3). |
| Global 30s catch-up window | Per-region OpLog tail is far smaller; catch-up window applies per region and is comfortably met. |
| Reconnect storm (10k `NEED_REMOUNT`) | `Ping` returns the *new region leader* endpoint directly; clients only remount into one region. |

**Root availability:** Root is tiny (routing + `view_version` only). It can run
with the existing `LeaderCoordinator` / `MasterView` machinery
(`ha_types.h: MasterView`, `ViewVersionId`) so its failover cost is negligible.

---

## 6. Key symbols to build against (existing code)

| Symbol | File | Role in HKVM |
|---|---|---|
| `MasterService` | `include/master_service.h:107` | base surface to regionalize |
| `WrappedMasterService` | `include/rpc_service.h:22` | 1:1 wrapper → become per-region wrappers |
| `RegisterRpcService` | `src/rpc_service.cpp:1637` | register region-scoped handlers |
| `SegmentManager::segment_mutex_` | `include/segment.h:499` | cluster lock → region-local lock |
| `AllocatorManager` | `include/allocation_strategy.h:26` | partitioned by region |
| `AllocationStrategy` | `include/allocation_strategy.h:167` | pick region first, then local |
| `ScopedAllocatorAccess` | `include/segment.h:343` | RAII lock entry, keep but region-local |
| `MasterClient::RpcClientPool` | `include/master_client.h:669` | reusable to talk to a region |
| `MasterServiceSupervisor::RunSupervisorLoop` | `ha/leadership/master_service_supervisor.cpp:218` | drive per-region leaders |
| `RestoreFromStandbySnapshot` | `src/master.cpp:2506` | scope to a region |
| `HotStandbyService` / `StandbyMetadataStore` | `ha/...` | per-region replica tailing |
| `MasterView` / `ViewVersionId` | `include/ha/ha_types.h` | reuse for root membership |

---

## 7. Module build & integration plan

1. **Wire into top-level build** — `/data/zyk/20260804_dev/mooncake/CMakeLists.txt`:
   ```cmake
   option(WITH_HKVM "build mooncake hkvm" OFF)
   if (WITH_HKVM)
     add_subdirectory(mooncake-hkvm)
     include_directories(mooncake-hkvm/include)
   endif()
   ```
   (Currently `mooncake-hkvm` is not referenced anywhere in the build/CI.)

2. **`mooncake-hkvm/src/CMakeLists.txt`** — define `mooncake_hkvm` target
   linking `mooncake_common`, `glog::glog`, `gflags::gflags`, `asio_shared`,
   `yalantinglibs::yalantinglibs`, `JsonCpp::JsonCpp`, plus the already-declared
   `Torch` / `CUDAToolkit`.

3. **Headers** under `include/` ↔ `.cpp` under `src/` (mirror the `mooncake-store`
   layout; headers `.h`, impl `.cpp`/`.cu`; no `.hpp`).

4. **Minimal first slice** (vertical proof):
   - `HkvmRouter` + `RegionalAllocator` header/src stubs.
   - A single regional server that still does *all* existing master RPC but is
     already federated from the root (validates routing and client
     confinement).
   - Wire `Ping`/`Lookup` path.

5. **Tests** — `tests/CMakeLists.txt` + `*_test.cpp` registered via a helper
   (mirror `add_store_test`), gated by `BUILD_UNIT_TESTS`; benchmarks gated by
   `BUILD_BENCHMARK`.

6. **Python / wheel** — if a Python API is needed: build `.so` and copy into
   `mooncake-wheel/mooncake/`, extend `scripts/build_wheel.sh`, add console
   entry in `mooncake-wheel/pyproject.toml`.

7. **Docs** — add usage/API/build sections to `mooncake-hkvm/README.md` once the
   MVP exists.

---

## 8. Milestones

- **M0 — Scaffolding:** topology wiring in root CMake; `HkvmRouter`
  + `RegionalAllocator` stubs compile; `WITH_HKVM=ON`.
- **M1 — Routing MVP:** root answers `Ping`/`Lookup`; clients pinned to a region;
  single region end-to-end put/get passes parity tests against current master.
- **M2 — Horizontal sharding:** multiple regions serve disjoint segments
  concurrently; allocation lock is region-local; benchmark shows contention
  scaling.
- **M3 — Per-region HA:** per-region leader election + region-scoped recovery;
  `view_version` routing on failover; cross-region ops via root.
- **M4 — Scale/validity:** 10k-client stress; recovery-time SLA validation;
  backward-compat client guard.

---

## 9. Open questions (for review)

1. **Region granularity:** partition by *worker instance* vs *GDD/network
   topology-island*. Topology-aware regions (as in `FAST25-release`/CXL work)
   minimize cross-region transfers but complicate routing; instance-level is the
   simpler first cut. **Propose: start instance/tenant-level, add topology later.**
2. **Replica placement** (`ReplicateConfig`) can span regions. Decide whether
   the region leader pushes cross-region replicas to Root for coordination, or a
   dedicated cross-region replica channel exists. **Propose: route through Root.**
3. **Backward compatibility bar:** how much of `MasterClient` must stay
   unchanged for C-API (`store_c.cpp`) and wheel consumers? Propose keeping the
   client-facing protocol stable and adding the routing hop only on first
   bootstrap.
4. **Quota / capacity global invariants:** a tenant's total budget is currently
   cluster-wide. Decide whether quota is enforced per-region (fence) or globally
   (Root rescans on escalate). Recommend per-region budgets summing to tenant
   budget.

---

*This plan is a living document; update milestone checkboxes as implementation
progresses.*