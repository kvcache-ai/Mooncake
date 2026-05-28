# Architecture

This document describes the runtime architecture implemented in `mooncake-store-rs`.

## Design Goals

- Keep the public store programming model close to Mooncake
- Reuse Mooncake TE/TENT for the data path
- Avoid a dedicated master on the route hot path
- Keep metadata backends responsible for leases, segments, route policy, handoff, and `MetadataOnly` route persistence
- Make lifecycle changes, reclaim, and routing explicit in the client

## Hot-Path Invariant

Steady-state request hot paths must not depend on backend round trips.

- Redis, etcd, admin RPC, and other backend-owned control surfaces must stay off normal per-request read and write latency paths
- hot paths may use local snapshots, maintained caches, authority indirection, or exact maintained indexes instead
- `MetadataOnly` remains a bring-up and debugging mode rather than the normal performance target
- if a feature proposal would reintroduce direct backend reads or writes on every request, the design is wrong until the hot-path dependency is removed

## Component Model

| Component | Responsibility | Hot Path |
|-----------|----------------|----------|
| `StoreClient` | User-facing API, local state, batching, reclaim scheduling | Yes |
| `RouteOperations` / `RouteDirectory` | Operation-level route table facade plus lookup/CAS implementation | Yes |
| `ControlPlaneClient` | Peer-to-peer control transport for route, allocator, eviction, and migration RPC | Yes |
| `StorageOwnerState` | Local replica tracking, CLOCK eviction, route-aware reclaim | Yes on storage nodes |
| `MetadataBackend` | Leases, segments, route policy, handoff, and `MetadataOnly` route persistence | No for default `EmbeddedWrh` route lookups |
| `TentEngine` / `TentTransportFactory` | Data transfer and remote segment access | Yes |
| `LocalAllocatorState` | Local segment reservation and release | Yes for local storage |

```mermaid
graph LR
    A["Store API"] --> B["StoreClient"]
    B --> C["RouteOperations / RouteDirectory"]
    B --> D["Allocator"]
    B --> E["ControlPlaneClient"]
    B --> J["StorageOwnerState"]
    B --> F["TentEngine / Transport"]
    C --> G["MetadataBackend"]
    D --> G
    J --> C
    J --> D
    E --> H["Peer client control plane"]
    E --> J
    F --> I["Peer client segment"]

    style B fill:#e3f2fd
    style C fill:#e8f5e9
    style D fill:#fff3e0
    style E fill:#ede7f6
    style J fill:#fce4ec
    style G fill:#f3e5f5
```

## Route Control

### Default mode: `EmbeddedWrh`

In the default route mode, the client chooses route authorities with embedded weighted rendezvous hashing.

- candidates come from live client leases
- only compatible, route-capable clients are considered
- the top `route_topk` authorities are selected per key
- the highest-ranked authority is the CAS primary
- the remaining `route_topk - 1` authorities are mirrored authorities
- route reads and route CAS go to authorities through the client-owned control-plane transport
- if the mirrored set does not resolve a key, lower-ranked authorities in the same WRH ordering can still be queried

This keeps object route lookups on the authority mesh and off the metadata hot path in normal `EmbeddedWrh` deployments.

`mooncake-store-route` owns the route-control request semantics. It defines the
route control request/response envelope, dispatches inbound route-authority
requests to the local authority service, and turns outbound transport replies
back into route lookup/CAS/list results. `mooncake-store-client` owns the
control-plane transport implementation: tonic channels, control streams,
unary fallback, timeouts, and protobuf encoding. Non-route control services
such as allocator, eviction, and migration remain in `mooncake-store-client`.

Route authority policy is bootstrap-validated through metadata:

- every client starts with a local `route_control` (cluster-level, fixed at startup) and `route_topk` (fallback, overridable via tenant policy)
- the first client in a metadata keyspace writes the default cluster route policy with create-if-absent semantics
- runtimes then resolve the effective route policy from the stored metadata key for their default tenant, otherwise the default cluster policy
- later clients must match that effective policy or startup fails

This keeps route-authority fanout deterministic across the cluster instead of letting each client silently pick a different authority set size.

### Alternative mode: `MetadataOnly`

`MetadataOnly` bypasses client authorities and stores object routes directly in the metadata backend.

| Route Mode | Route Read Path | Route Write Path | Best For |
|------------|-----------------|------------------|----------|
| `EmbeddedWrh` | Authority RPC across ranked WRH authorities | Authority CAS with mirrored top-k publication | Normal deployments |
| `MetadataOnly` | Metadata backend | Metadata backend | Simpler debugging and bring-up |

## Membership Snapshot Model

`StoreClientBuilder::build(...)` publishes the client lease, prewarms a live-client snapshot, and then starts a background membership sync worker.

The shared snapshot is the membership truth used by the hot request path.

- route-authority selection reads the cached snapshot
- routed placement reads the same cached snapshot
- runtime lease lookup and preferred-storage resolution read the same cached snapshot
- successful background refresh replaces the whole snapshot
- failed refresh keeps the last successful snapshot available

This keeps `list_live_clients()` off the normal request path. Metadata still owns the durable lease set, but request-path consumers read a locally cached view that is refreshed asynchronously.

The same background worker now also maintains the tenant-root quota-policy hot cache used by write admission:

- tenant quota policy is cached per tenant root instead of being re-read from metadata for every object write
- the default tenant's root quota policy or cached absence is seeded during client build, so the first steady-state request does not need a backend lookup just to learn that default-tenant policy state
- the cache shares the same worker thread and timer as membership refresh
- the worker prunes idle tenant-policy entries and opportunistically refreshes recently used stale entries
- hot writes still refresh a missing or stale tenant policy on demand when needed

This keeps membership and tenant-policy maintenance on one metadata-cache loop without turning tenant policy into a global full-scan snapshot.

The membership snapshot is runtime cache, not protocol configuration:

- membership snapshots are refreshed in the background
- route policy is durable cluster configuration stored in metadata
- request-level tenants affect scoped object keys, not the cluster-wide route-authority policy

## Write Path

A write is split into route resolution, allocation, transfer, and route publication.

For routed batch writes, remote replica transfers are coalesced by tenant and scratch-window
capacity before route publication. Large same-tenant batches therefore pay one transfer-completion
wait per chunk instead of one wait per object.

If a routed write fails during remote transfer before the object route is published, the client
releases the prepared allocations, quarantines the failed remote storage runtime, refreshes its
membership snapshot, and retries placement when the request is not pinned to a hard required
segment. This keeps transient storage-owner transport failures from escaping into Python backup
threads as fatal exceptions.

Remote transfer planning splits each request at both local registration limits and the storage
target chunks published in the target segment announcement. The route still records one logical
replica, but the transport only receives slices that fit inside one registered storage target
chunk, matching classic RDMA transfer-engine requirements when a segment is backed by multiple
contiguous registrations.

Classic TE P2P deployments also publish the TE segment descriptor in the Store-RS segment
announcement. The logical `segment_name` remains the allocator and route identity, while
`transport_endpoint` names the peer RPC endpoint and `transport_segment_descriptor` carries the
full TE RAM descriptor that Redis-backed TE metadata would otherwise store under
`mooncake/ram/[segment_name]`. Before `openSegment`, Store-RS preloads that descriptor into the
local TE cache so P2P opens have the same device/GID/buffer view as Redis metadata opens without
adding a backend lookup to the request path.

Replica routes keep two separate coordinates. `ReplicaRoute::offset` is the actual transport
target address used by TE/TENT and is the byte location of the stored payload in the segment
storage target map. `segment_offset` belongs to the local allocator and reclaim path. Segment
announcements publish the exact `logical_offset -> target_offset` storage chunks derived during
local registration; writers use those chunks as the only allocator-to-transport mapping source.
Scratch buffers are registered for staging only and are never published as object-addressable
storage. Local direct reads, batch reads, and drain migration translate `offset` through the
selected segment's storage target map before copying from local storage, so local and remote reads
observe the same bytes even when the transport target coordinate is not the process virtual address.

```mermaid
sequenceDiagram
    participant App
    participant Client as StoreClient
    participant Route as RouteOperations
    participant Alloc as Allocator/ControlPlane
    participant Evict as Storage Owner State
    participant Peer as Remote Storage Client
    participant TE as TE/TENT

    App->>Client: put / batch_put
    Client->>Client: resolve replication policy
    Client->>Alloc: reserve local or remote segment space
    Alloc-->>Client: reservations
    Client->>TE: write buffers into chosen segments
    TE-->>Peer: transfer payload
    Client->>Route: publish ObjectRoute
    Route-->>Client: route version / CAS result
    Client->>Evict: track local replicas
    Client->>Peer: batch track remote replica routes (best effort)
```

### Placement rules

The current implementation supports:

- local-only writes
- routed writes to remote storage nodes
- local-first placement with remote spillover
- explicit preferred segment hints
- explicit preferred storage owner hints
- multi-replica placement

The default request policy prefers local storage when available. When local capacity is insufficient, the client allocates on remote storage nodes selected by the planner and the replication policy.

### Remote storage-owner tracking

Route publication and eviction are intentionally decoupled across two owners:

- the route owner is responsible for the authoritative object route version
- the storage owner is responsible for the actual bytes and local capacity

After a write publishes a route, the writer groups replicas by remote storage owner and sends the published routes through `BatchTrackReplicaRoutes`.

This lets each storage owner update its local eviction clock from the published route directly, instead of rebuilding state from metadata during normal write traffic.

## Read Path

For reads, the client first resolves the object route, then groups reads by remote segment and submits transfer requests through TE/TENT.
When the selected replica is local, the client still resolves the same `ReplicaRoute::offset` target coordinate that remote TE reads use, then maps that coordinate back to the local storage registration.
Successful route lookup APIs are also access signals: `query_route`, `get_size`, `is_exist`,
and `batch_is_exist` report active route hits to the storage owners best-effort, using the same
local/CLOCK and control-plane hit-report path as completed reads. This keeps prefix-probe
workloads from letting recently observed replicas age out before the caller issues the matching
restore read, without adding metadata-backend traffic to the request path.

```mermaid
sequenceDiagram
    participant App
    participant Client as StoreClient
    participant Route as RouteOperations
    participant Evict as Storage Owner State
    participant TE as TE/TENT
    participant Peer as Remote Storage Client

    App->>Client: get / batch_get
    Client->>Route: lookup route
    Route-->>Client: ObjectRoute
    Client->>Evict: report active route hit
    Client->>TE: open segment and submit read batch
    TE-->>Peer: fetch bytes
    TE-->>Client: completion status
    Client->>Evict: mark local hits
    Client->>Peer: batch report remote hits (best effort)
    Client-->>App: value or copied buffer
```

The read path supports:

- single get and batch get
- direct copy into caller buffers
- multi-buffer reads for fragmented targets
- registered-buffer reads for reduced copy overhead

### Read-hit reporting

Successful reads update eviction heat at the storage owner:

- local hits are reported directly into the local `StorageOwnerState`
- remote hits are grouped by replica owner and sent through `BatchReportRouteHits`

Hit reporting is best-effort. It improves eviction quality but is not part of correctness.

## Allocation and Reclaim

Allocation is split by storage ownership.

- local allocations use `LocalAllocatorState`
- remote allocations use control-plane RPC to the owning client
- remote allocator capacity errors remain owner-local results; transport or protocol failures quarantine that owner and let placement move on

Local memory supports two backing strategies:

- transport-managed memory through the TE/TENT transport
- hugepage-backed native memory allocated directly by store-rs and then registered into the transport

The Python host allocator uses shared `memfd` + `mmap`, and can also request hugepage-backed shm regions when the host kernel is configured for hugepages.

Reclaim is explicit and route-aware.

- overwrites schedule release for the old replicas
- deletes schedule reclaim after route removal
- `reclaim_grace_ms` controls delayed release behavior
- remote release uses batched allocator RPC

### Storage-owner CLOCK with route-owner CAS

Capacity pressure is resolved by the storage owner, but correctness is still enforced by the route owner.

```mermaid
sequenceDiagram
    participant Client as StoreClient
    participant Alloc as LocalAllocatorState
    participant Evict as StorageOwnerState
    participant Route as RouteOperations

    Client->>Alloc: reserve local space
    Alloc-->>Client: Allocator error
    Client->>Evict: evict_one(preferred_segment?)
    Evict->>Evict: pick CLOCK victim
    Evict->>Route: CAS route without victim replica
    Route-->>Evict: applied / current route
    Evict->>Alloc: release victim bytes after CAS success
    Evict-->>Client: reclaimed or exhausted
    Client->>Alloc: retry reservation
```

The important invariants are:

- a replica is never released before its route entry is removed
- a failed CAS refreshes local eviction state instead of guessing
- a storage owner can rebuild its CLOCK from route state if best-effort tracking falls behind
- local CLOCK eviction is enabled only on clients labeled `storage=true`
- routed writers without local storage keep the spill-remote behavior

Remote allocator RPC follows the same pattern on the storage owner side. A live remote storage owner returns a real allocator error when it still cannot free capacity. The caller does not silently downgrade that case to metadata allocation.

### Background watermarks plus synchronous fallback

The local storage owner now has two reclaim modes:

- background mode: poll local allocator usage and reclaim from the high watermark down to the low watermark
- synchronous mode: when a reserve still fails, evict immediately and retry the allocation

This keeps steady-state pressure off the front path while still preserving a deterministic fallback under bursty writes.

Default local-memory thresholds are:

- high watermark: `90%`
- low watermark: `80%`
- poll interval: `100ms`

Startup also normalizes the storage role:

- `storage=true` is rejected when `storage_bytes=0`
- `storage_bytes=0` without an explicit storage label is normalized to `storage=false`

## Control Plane

The control plane is implemented with protobuf and tonic.

It handles three categories of RPC:

- route RPC: batch get, compare-and-swap, replace
- allocator RPC: batch reserve any, reserve specific, release
- eviction RPC: batch report route hits, batch track replica routes

Single-item calls are intentionally folded into the batch path so that the implementation can reuse streaming sessions and keep the control-plane logic uniform.

For route control, that batch substrate serves both the CAS primary and the mirrored remainder of the `route_topk` authority set.

## Metadata Model

Metadata backends store four persistent categories of data:

- client leases
- segment announcements and segment lifecycle state
- object routes, when metadata persistence is needed
- tenant policy and strict-quota metadata

For membership specifically, metadata is the authoritative lease store, while the client runtime keeps a prewarmed and background-refreshed snapshot for request-path reads.

Client-lease registration uses `allocate_client_lease`: the metadata backend atomically derives the next epoch from `max(active epochs for stable_id, persistent HWM) + 1`, writes the lease under that epoch, and returns the assigned `ClientRuntimeId`. Republishing the same `(stable_id, epoch)` through `upsert_client_lease` is a refresh and extends the lease TTL in place — heartbeat and lifecycle-state updates use that path. The backends implement the allocation differently: the in-memory backend holds the active-epoch set and HWM under its write lock, Redis uses a dedicated Lua script over a `by-stable` set and an HWM key, and etcd uses a txn loop over per-epoch marker keys and the HWM key. Callers never supply an epoch; the `ClientEpoch` type remains internal to serialization, handoff plans, and metadata keys.

For strict quota rollout, the metadata model now also includes tenant-root quota primitives:

- `TenantQuotaState` for committed and pending usage
- `TenantObjectAccounting` for authoritative committed object size/version
- `TenantQuotaReservation` for reserve/finalize/abort coordination

In Phase 1, these primitives exist in the shared contracts and all three metadata backends. The in-memory backend applies them under one write lock, Redis uses backend-atomic Lua reserve/finalize/abort scripts, and etcd uses multi-key compare-and-swap txn loops.

Phase 3 now wires those primitives into the single-object client path: `put` reserves tenant quota before allocation/write, publishes the route, then finalizes quota before returning success. `remove` now reserves and finalizes the negative delta at delete CAS time, so quota/accounting state tracks authoritative object visibility rather than delayed storage reclaim.

Phase 4 extends the same model to routed `batch_put`: the client sorts entries by scoped key before quota admission, reserves quota for every item before storage reservation proceeds, aborts already-acquired reservations if later admission fails, and finalizes quota only after each route CAS is authoritative. That keeps routed batch admission deterministic and prevents partial failures from leaving pending quota behind.

### Backend support

| Backend | Use Case |
|---------|----------|
| Redis | local development, e2e, route and segment persistence |
| etcd | store metadata for distributed deployments |
| in-memory | unit tests |

## Lifecycle and Elasticity

`StoreClient` also exposes lifecycle operations that are used by the compatibility facade.

| Capability | Description |
|------------|-------------|
| `activate` / `enter_standby` / `enter_draining` | Drive client lifecycle state |
| `plan_handoff` | Prepare successor handoff metadata |
| `expand_local_memory` | Add new local storage capacity |
| `drain_segment` / `retire_segment` | Drain and retire individual local segments |
| `evacuate_owned_replicas` / `evacuate_owned_replicas_via` | Rewrite live routes away from a draining client and retire emptied segments |
| remove + reclaim | Delete route state and release segment space |

These APIs are what the e2e suite uses to validate dynamic membership, elastic expansion, true client shrink, and hot-upgrade handoff.

During full client shrink, a draining route authority mirrors any locally held route records to active route authorities before shutdown. That final durability pass refreshes the live-client snapshot if the cached membership view cannot protect a route, so graceful exit does not depend on a stale local membership cache.

## Observability

The client contains a built-in typed metrics registry and a lightweight HTTP exporter.

Observability is split into three stages:

- request-path code records facts into typed families close to the state transition
- `snapshot_metrics()` builds one immutable view of registry + process facts
- the exporter renders Prometheus text from that snapshot without mutating state

This avoids continuing to grow one branchy `operation -> counters` map and keeps `/metrics` deterministic under concurrent request traffic.

### Metrics

Metrics are recorded per operation and status.

Examples include:

- route lookups
- local copy and remote transfer work
- control-plane batch operations
- bytes in and bytes out
- accumulated latency and max latency
- metadata backend operation latency, inflight calls, and typed outcomes for Redis, etcd, or another `MetadataBackend`
- tenant quota reservation / finalize / abort / reconcile outcomes
- route-authority publish latency and storage-owner read/write traffic observed from control-plane route CAS, route-hit, and replica-track handlers

The same tracker framework also covers eviction-related work such as:

- storage-owner eviction attempts
- route-hit reporting RPC
- replica-route tracking RPC
- strict tenant quota protocol transitions

### Tracing

Log-oriented tracing is built on `tracing` + `tracing-subscriber` and can be enabled from code or by environment variables.

Jaeger profiling uses the same operation boundaries as the metrics registry:

- `OperationTracker` creates semantic profiling spans for request APIs and internal stages when profiling is enabled; spans can go to OTLP/Jaeger, a local JSONL file, or both
- request API spans such as `store.put`, `store.batch_put_from`, `store.get`, and `store.batch_get_into` become the root of the in-process waterfall
- local JSONL profiling can also emit `store.api_items.v1` records for `batch_put_from` and `batch_get_into` when `MC_STORE_RS_TRACE_ITEM_METADATA=1`; these records keep per-item key hash/prefix, namespace, runtime id, SGLang TP rank, KV kind, byte count, and status in the offline artifact instead of high-cardinality metrics labels
- control-plane stages use names such as `control.route_lookup`, `control.allocate`, `control.route_publish`, and `control.replica_track`
- data-plane stages use names such as `data.transfer_write`, `data.transfer_read`, `data.local_write`, and `data.local_read`
- the metadata backend wrapper creates child spans for Redis, etcd, or another `MetadataBackend`
- spans carry `mooncake.phase`, `mooncake.flow`, `mooncake.request_id`, `mooncake.item_count`, byte counts, and target-count attributes so Jaeger traces can be correlated with metrics time series
- disabled profiling is an atomic fast path and does not build spans or touch the exporter
- `/tracing` on the metrics HTTP server can turn profiling on or off dynamically after an OTLP endpoint or JSONL file is configured
- high-throughput profiling can set a sample ratio before exporter initialization to keep the collector from becoming the bottleneck

## Python Compatibility Architecture

The Python package exposes two runtime paths over the same Rust implementation.

### Real path

`MooncakeDistributedStore.setup(...)` builds a native `StoreClient`.

That means:

- route lookups use the same `RouteOperations` facade and `RouteDirectory` implementation
- allocation uses the same local / remote allocator logic
- data transfer uses the same TE/TENT transport path
- lifecycle, reclaim, tracing, and metrics share the same implementation as Rust callers

### Dummy path

`MooncakeDistributedStore.setup_dummy(...)` connects to a standalone `mooncake-store-client` process.

That path is split as follows:

- gRPC carries compatibility operations such as put/get/batch RPCs
- a Unix-domain side channel passes shm file descriptors for registered buffers
- the standalone server resolves those shm registrations and forwards operations into the native store runtime

This preserves compatibility for integrations that expect a dummy client / external server split while keeping the real store logic inside the Rust runtime.

## Code Map

| Path | Role |
|------|------|
| `crates/mooncake-store-core` | Shared contracts and store model |
| `crates/mooncake-metadata` | Backend implementations for metadata |
| `crates/mooncake-store-route` | route table operation facade and implementation modules: `operations`, `control`, `local_authority`, `directory`, `mesh`, `traits`, `metrics`, and `util`; `mesh` remains crate-internal |
| `crates/mooncake-store-client/src/client/mod.rs` | `StoreClient` assembly and module composition |
| `crates/mooncake-store-client/src/client/builder.rs` | builder defaults, lease publication, membership prewarm |
| `crates/mooncake-store-client/src/client/runtime_core.rs` | runtime lookup, placement, lifecycle, allocator helpers |
| `crates/mooncake-store-client/src/client/runtime_io.rs` | get/batch-get, route scans, migration reads |
| `crates/mooncake-store-client/src/client/runtime_write.rs` | put/batch-put and route publication |
| `crates/mooncake-store-client/src/client/runtime_alloc.rs` | local and remote allocation helpers |
| `crates/mooncake-store-client/src/client/membership_sync.rs` | background live-client snapshot refresh |
| `crates/mooncake-store-client/src/client/facade.rs` | Mooncake-compatible surface methods |
| `crates/mooncake-store-client/src/route_directory.rs` | route crate adapters for membership snapshots, local authority binding, and metrics |
| `crates/mooncake-store-client/src/control_plane/mod.rs` | control-plane module entry and exports |
| `crates/mooncake-store-client/src/control_plane/client.rs` | protobuf RPC client and stream-session reuse |
| `crates/mooncake-store-client/src/control_plane/server.rs` | protobuf RPC server and dispatch |
| `crates/mooncake-store-client/src/memory.rs` | local memory and segment tracking |
| `crates/mooncake-store-client/src/transport.rs` | transfer submission helpers |
| `crates/mooncake-store-py/src/lib.rs` | Python bindings and top-level compatibility API |
| `crates/mooncake-store-py/src/runtime.rs` | real runtime construction from Python setup args |
| `crates/mooncake-store-py/src/dummy_client.rs` | dummy compatibility client and shm registration RPC |
| `crates/mooncake-store-py/src/dummy_service.rs` | standalone dummy compatibility service |
| `crates/mooncake-store-py/src/shm.rs` | shm region ownership, fd passing, shared mapping helpers |
| `crates/mooncake-store-e2e` | runnable system validation |
| `crates/mooncake-store-test-utils` | test-only fixtures, counting / faulty backend decorators, `TestTransport` and `FaultyTransport` |

## When to Read Which Document

- Start with `README.md` for setup and a first run
- Read `docs/deployment.md` for local scripts and deployment roles
- Read `docs/rust.md` for Rust integration
- Read `docs/configuration.md` for defaults and tuning knobs
- Read `docs/python.md` for the Python layer
- Read `docs/testing.md` for the test suite layout, fault-injection model, and CI entry point
- Read `crates/mooncake-store-e2e/src/main.rs` for a complete runnable example
