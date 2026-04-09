# Components

This document explains the repository by module and by runtime role.

## Layer View

| Layer | Main Crates | Responsibility |
|------|-------------|----------------|
| Store model | `mooncake-store-core` | Shared types, traits, lifecycle, and route model |
| Metadata | `mooncake-metadata` | Persistent leases, segments, and route fallback state |
| Runtime | `mooncake-store-client` | User-facing API and all runtime decisions |
| Transport binding | `mooncake-transport-sys`, `mooncake-transport` | Native TE/TENT linkage and Rust wrappers |
| Compatibility | `mooncake-store-py` | Python binding and compatibility API |
| Validation | `mooncake-store-e2e` | End-to-end correctness and benchmark runs |

## Core Model

### `mooncake-store-core`

Purpose:

- define the shared store vocabulary
- keep metadata, client, and bindings on one consistent type system

Main content:

- client identities and endpoint sets
- leases and lifecycle states
- routes, replicas, segments, and versions
- store traits such as `MetadataBackend` and `RouteDirectory`

Read this crate first if you want to understand the data model before reading the runtime.

## Metadata Backends

### `mooncake-metadata`

Purpose:

- store durable coordination state outside the client process
- provide one backend interface with multiple implementations

Backends:

| Backend | When It Is Used |
|---------|------------------|
| `RedisMetadataBackend` | default local runs and e2e |
| `EtcdMetadataBackend` | distributed store metadata |
| `InMemoryMetadataBackend` | tests |

Main responsibilities:

- live client lease storage
- segment announcement storage
- segment lifecycle updates
- route persistence for fallback and metadata-only mode

## Client Runtime

### `mooncake-store-client`

This is the main crate that applications use.

It contains the public builder, the request API, the allocator logic, the route directory, the control plane, and the transport helpers.

#### Public entry points

- `StoreClient`
- `StoreClientBuilder`
- `PutRequest`, `GetRequest`
- `ReplicationPolicy`
- `PlacementPlanner`

#### Internal runtime areas

| Area | Files | Responsibility |
|------|-------|----------------|
| API and lifecycle | `src/client.rs` | public API, batching, reclaim, lifecycle, registration |
| Control plane | `src/control_plane.rs` | route RPC and allocator RPC between clients |
| Route control | `src/route_directory.rs` | embedded weighted rendezvous and metadata route mode |
| Memory tracking | `src/memory.rs` | local storage segments, scratch, registered buffers |
| Placement planning | `src/placement.rs` | storage-node filtering and selection helpers |
| Observability | `src/observability.rs` | tracing, metrics registry, HTTP export |
| Transport helpers | `src/transport.rs` | transfer submission and completion helpers |

## Transport Layer

### `mooncake-transport-sys`

Purpose:

- link Rust against upstream Mooncake native libraries
- build upstream TE/TENT when native artifacts are missing

What it does:

- locates the Mooncake submodule
- configures `cmake`
- links `libtransfer_engine.so` and `libtent_shared.so`
- exports FFI for classic TE and TENT

### `mooncake-transport`

Purpose:

- provide a safer Rust-facing transport API over the FFI layer

Main objects:

- `TentEngine`
- `TentEngineConfig`
- `TentTransportFactory`
- transport request and status types

## Python Compatibility

### `mooncake-store-py`

Purpose:

- expose the Rust runtime to Python without reimplementing store logic

What it contains:

- `MooncakeDistributedStore`
- compatibility-style `setup(...)`
- Python-facing replication config handling
- metadata URL parsing for Redis and etcd

### `python/mooncake`

Purpose:

- package convenience layer around the native extension
- find and load the built shared library from the repository checkout

It also provides Python-friendly wrappers for batch and buffer-oriented methods.

## Validation and Examples

### `mooncake-store-e2e`

Purpose:

- prove that the runtime behaves correctly in realistic multi-client flows
- print benchmark numbers for batch put and get

The e2e binary validates:

- single and batch I/O
- replication policy handling
- routed remote writes
- overwrite reclaim and delete reclaim
- multi-tenant behavior
- dynamic membership and elastic segment changes
- hot-upgrade handoff flows

## Suggested Reading Order

1. `README.md`
2. `docs/deployment.md`
3. `docs/rust.md`
4. `docs/configuration.md`
5. `crates/mooncake-store-core`
6. `docs/features.md`
7. `docs/architecture.md`
8. `crates/mooncake-store-e2e/src/main.rs`
