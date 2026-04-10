# Features

This document explains the implemented capabilities by function rather than by crate.

## Store API

### Basic object operations

- `put`
- `get`
- `remove`
- `is_exist`
- `get_size`
- `query_route`

These APIs are the simplest entry points for applications that treat the store as an object service.

### Batch operations

- `batch_put`
- `batch_get`
- `batch_remove`
- batch existence checks

Batch APIs are first-class paths rather than thin wrappers around repeated single-item calls.

## Buffer-Oriented I/O

### Registered-buffer path

The runtime supports registering caller-owned buffers and then using them for put/get operations with less copy overhead.

Main capabilities:

- register and unregister buffers
- `put_from`
- `batch_put_from`
- `batch_get_into`
- registered subranges inside a larger caller-owned buffer

### Multi-buffer path

For fragmented payloads or caller-managed buffer layouts, the runtime supports multi-buffer request forms.

Main capabilities:

- `batch_put_from_multi_buffers`
- `batch_get_into_multi_buffers`
- HiCache-compatible dummy and real paths for the Python layer

## Routing

### Embedded weighted rendezvous routing

Default route control uses embedded weighted rendezvous hashing.

What it provides:

- route owner selection without a dedicated master
- primary and secondary route authorities
- authority reads and compare-and-swap through the control plane
- replica removal through route-owner compare-and-swap
- metadata fallback when authority RPC is unavailable

### Metadata-only routing

For simpler bring-up or debugging, the runtime can store and resolve routes directly through the metadata backend.

## Placement and Replication

### Local-first placement

The default request behavior prefers local storage when possible.

When local storage cannot satisfy the request, the client can spill over to remote storage nodes.

### Routed remote placement

Router nodes can place data onto storage nodes selected by the placement planner.

The planner filters candidate nodes by labels and compatibility.

### Request-level replication policy

A request can control placement behavior with:

- replica count
- preferred segment
- preferred segment list
- preferred storage owner
- preferred storage owner list
- `prefer_local`
- `prefer_alloc_in_same_node`
- `with_soft_pin`

## Allocation and Segment Management

### Local allocation

Local segment reservation and release are handled inside the client process.

The current implementation supports:

- transport-managed local memory
- hugepage-backed native memory for Rust store segments
- hugepage-backed shared memory for Python host buffers

### Remote allocation

Remote storage reservation uses allocator RPC to the owning client.

Remote storage owners apply the same reserve loop as local storage owners:

- try allocator reservation
- if the failure is capacity-related, trigger storage-owner eviction and retry
- return an allocator error when capacity still cannot be recovered

### Metadata fallback

Allocator fallback is intentionally narrow.

The client falls back to metadata-backed allocation only when allocator RPC fails with:

- transport errors
- unsupported control-plane endpoints

Allocator failures returned by a live remote storage owner are treated as real capacity errors and are not silently downgraded to metadata allocation.

## Storage-Owner Eviction

### Local CLOCK state

Each storage owner maintains a local CLOCK over the replicas it currently stores.

The tracked identity is route-oriented rather than segment-only:

- route key
- segment name
- segment offset

This lets the storage owner evict a precise replica instead of reclaiming blind segment regions.

### Read-hit reporting

Successful reads update eviction heat without putting metadata back on the hot path.

The current implementation does this in batch:

- local hits update the local storage-owner state directly
- remote hits are grouped by storage owner
- grouped hits are sent through `BatchReportRouteHits`

Hit reporting is best-effort. A lost hit can reduce eviction quality, but it does not affect correctness.

### Route tracking after publish

After a writer successfully publishes a route, it groups the remote replica owners and sends the published routes through `BatchTrackReplicaRoutes`.

This gives each storage owner a current route view for the replicas it stores without requiring a metadata scan on the write hot path.

### Route-owner CAS reclaim

When capacity pressure forces eviction:

- the storage owner picks a CLOCK victim
- the matching route owner removes that replica with CAS
- only after CAS succeeds does the storage owner release the local allocation

This preserves route correctness during eviction and keeps reclaim coupled to the authoritative route version.

### Where local eviction runs

Local CLOCK eviction is enabled for clients labeled `storage=true`.

This keeps the roles clean:

- storage nodes reclaim their own local capacity
- routed rw nodes keep the existing spill-remote behavior
- scratch-only rw nodes can run with `storage_bytes=0`

## Delete and Reclaim

### Overwrite reclaim

When a key is overwritten, old replicas are scheduled for release.

### Delete reclaim

When a key is removed, route state is removed and segment space is reclaimed.

### Graceful reclaim

A reclaim grace window can delay release to smooth transitions or handoff behavior.

## Multi-Tenancy

The client supports tenant-scoped keys and tenant-aware request builders.

This keeps API usage explicit while still allowing a default tenant for simpler applications.

## Dynamic Membership and Lifecycle

### Client lifecycle

The runtime models explicit client states:

- standby
- active
- draining

### Handoff and upgrade

The client can publish handoff plans and participate in successor upgrade flows.

### Elastic capacity

The client can expand local storage, drain segments, retire empty segments, and evacuate all replicas owned by a draining client.

This allows segment-level shrink, full client shrink, and dynamic storage growth without changing the public API.

## Control Plane

The runtime includes a dedicated control plane for two domains:

- route operations
- allocator operations

Important properties:

- protobuf schema
- tonic RPC implementation
- batched operations
- streaming session reuse
- eviction-specific RPCs for hit reporting and route tracking

## Observability

### Tracing

Tracing can be enabled from code or environment variables.

### Metrics

The runtime records:

- operation counts
- input and output bytes
- total latency
- max latency
- status labels such as `ok` and `error`

### HTTP export

Metrics can be served through an in-process HTTP endpoint.

## Python Compatibility

The Python layer mirrors the same runtime instead of wrapping a separate implementation.

What this means in practice:

- the same route, allocator, and reclaim logic is reused
- the same transport path is reused
- the same metrics and tracing capabilities are available
- the same replication controls are exposed through `ReplicateConfig`
- the same standalone compatibility server can serve dummy clients
- the wheel packaging flow ships the native extension with bundled runtime libraries

### Dummy and real compatibility paths

The compatibility layer exposes two integration modes:

- real mode uses the native `StoreClient` directly from Python
- dummy mode talks to a standalone `mooncake-store-client` process and exchanges shm buffer registrations over a side channel

Both paths are covered by repository validation scripts.
