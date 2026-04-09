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

### Multi-buffer path

For fragmented payloads or caller-managed buffer layouts, the runtime supports multi-buffer request forms.

Main capabilities:

- `batch_put_from_multi_buffers`
- `batch_get_into_multi_buffers`

## Routing

### Embedded weighted rendezvous routing

Default route control uses embedded weighted rendezvous hashing.

What it provides:

- route owner selection without a dedicated master
- primary and secondary route authorities
- authority reads and compare-and-swap through the control plane
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

### Remote allocation

Remote storage reservation uses allocator RPC to the owning client.

### Metadata fallback

If allocator RPC fails, the client can fall back to metadata-backed allocation behavior.

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

The client can expand local storage, drain segments, and retire segments.

This allows soft shrink and dynamic storage growth without changing the public API.

## Control Plane

The runtime includes a dedicated control plane for two domains:

- route operations
- allocator operations

Important properties:

- protobuf schema
- tonic RPC implementation
- batched operations
- streaming session reuse

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
