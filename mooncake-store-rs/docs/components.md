# Components

This document explains the repository by module and by runtime role.

## Layer View

| Layer | Main Crates | Responsibility |
|------|-------------|----------------|
| Store model | `mooncake-store-core` | Shared types, traits, lifecycle, and route model |
| Metadata | `mooncake-store-rs-metadata` | Persistent leases, segments, route policy, handoff, and `MetadataOnly` route state |
| Runtime | `mooncake-store-client` | User-facing API and all runtime decisions |
| Python compatibility runtime | `mooncake-store-rs-runtime` | Python-facing setup, dispatcher, dummy service, and shm compatibility path |
| Admin | `mooncake-store-rs-admin` | Admin service and standalone `mooncake-store-rs-admin` command |
| CLI | `mooncake-store-rs-cli` | Standalone `mooncake-store-rs-client` and `mooncake-store-rs-bench` commands |
| Transport binding | `mooncake-store-rs-transport-sys`, `mooncake-store-rs-transport` | Native TE/TENT linkage and Rust wrappers |
| Python binding | `mooncake-store-py` | Private `mooncake._store_rs` extension and PyO3 bindings |
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
- hugepage configuration parsing shared by Rust and Python entry points
- store traits such as `MetadataBackend` and `RouteDirectory`

Read this crate first if you want to understand the data model before reading the runtime.

## Metadata Backends

### `mooncake-store-rs-metadata`

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
- segment announcement storage under the owning runtime; Redis stores these records in the same TTL-backed client resource hash as the lease
- segment lifecycle updates
- route policy and handoff storage
- route storage for `MetadataOnly` mode

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
| Client assembly | `src/client/mod.rs`, `src/client/builder.rs` | public builder, client composition, startup defaults |
| Client runtime | `src/client/runtime_core.rs`, `src/client/runtime_io.rs`, `src/client/runtime_write.rs`, `src/client/runtime_alloc.rs`, `src/client/facade.rs` | request-path logic, lifecycle, batching, reclaim, compatibility surface |
| Membership sync | `src/client/membership_sync.rs`, `src/client/state_core.rs` | prewarmed live-client snapshot and background refresh |
| Control plane | `src/control_plane/mod.rs`, `src/control_plane/client.rs`, `src/control_plane/server.rs` | route RPC and allocator RPC between clients |
| Route control | `src/route_directory.rs` | embedded weighted rendezvous and metadata route mode |
| Memory tracking | `src/memory.rs` | local storage segments, scratch, registered buffers |
| Placement planning | `src/placement.rs` | storage-node filtering and selection helpers |
| Observability | `src/observability.rs` | tracing, metrics registry, HTTP export |
| Transport helpers | `src/transport.rs` | transfer submission and completion helpers |

#### Explicit route migration runtime

The explicit route-migration runtime is implemented in the same
`mooncake-store-client` crate and is split across three pieces:

- `src/client/runtime_io.rs`
  - source-replica selection
  - explicit `copy` / `move` route-delta construction
  - payload copy, CAS route publish, and best-effort reclaim
- `src/client/state_adapters.rs`
  - executor-side task adapter
  - background worker that turns a control-plane request into one runtime
    migration execution
- `src/control_plane/*`
  - `SubmitMigrationTask` and `GetMigrationExecutionStatus`
  - migration request validation and client/server wiring

This runtime layer is intentionally lower-level than the admin queue and HTTP
operator surface. It is the executor-side kernel that later stacked PRs build
on top of.

## Transport Layer

The top-level CMake project owns the native shim targets
`mooncake_store_rs_classic_shim` and `mooncake_store_rs_tent_shim`. CMake links
them to the existing `transfer_engine` and `tent_shared` targets, passes their
target-file paths into Cargo, and installs the shim libraries with the Python
component. The Rust sys crate owns the FFI surface and validates those explicit
paths; it does not configure or build CMake targets.

### `mooncake-store-rs-transport-sys`

Purpose:

- expose Rust FFI over the classic-TE and TENT shims built by the top-level CMake project

What it does:

- validates the explicit Mooncake source, CMake build, and shim-library paths passed by CMake
- binds Rust transport code to the CMake-owned classic-TE and TENT shim libraries
- leaves upstream TE/TENT targets and both shim implementations under CMake ownership

### `mooncake-store-rs-transport`

Purpose:

- provide a safer Rust-facing transport API over the FFI layer

Main objects:

- `TentEngine`
- `TentEngineConfig`
- `ClassicTransferEngine`
- `ClassicEngineConfig`
- `TentTransportFactory`
- transport request and status types

## Python Compatibility

### `mooncake-store-rs-runtime`

Purpose:

- provide the Rust compatibility runtime used by Python's real and dummy modes

It owns setup configuration, dispatcher and hot-cache state, shared-memory
registration, and the dummy client/service protocol. The Python bindings and
CLI use this runtime; the admin crate owns its separate operator layer.

### `mooncake-store-py`

Purpose:

- expose the Rust runtime to Python without reimplementing store logic

What it contains:

- PyO3 bindings for the Python-facing Store classes and allocator wrappers
- the private `mooncake._store_rs` extension module
- binding-specific tensor and buffer wrappers

The root Python package exposes the selected backend through `mooncake.store`;
this crate provides the Store-RS native module consumed by that facade.

## Admin and CLI

### `mooncake-store-rs-admin`

This crate owns the admin HTTP/service layer and the fixed-backend
`mooncake-store-rs-admin` operator command.

### `mooncake-store-rs-cli`

This crate provides the fixed-backend `mooncake-store-rs-client` command and
`mooncake-store-rs-bench` benchmark command. These commands launch Store-RS
directly; `MOONCAKE_STORE_BACKEND` selects the Python facade only.

### `python/mooncake/store/rs`

Purpose:

- Store-RS implementation selected by the root `mooncake.store` facade
- load the private `_store_rs` extension installed in the root wheel

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

### `mooncake-store-rs-bench`

Purpose:

- provide a standalone, production-grade benchmark and verification binary built from the Rust workspace

The bench binary provides three subcommands:

- `bench` — concurrent single-key or batch read/write throughput and latency measurement with p50/p90/p99/p999 percentiles, configurable mode, interfaces, concurrency, batch size, and duration
- `verify` — sequential correctness checks including configurable single-key or batch read/write paths, `get_into`, `is_exist`, overwrite correctness, delete/reclaim, and tenant isolation
- `soak` — long-duration stability test with per-operation fault injection (Redis jitter, metadata drop, transport delay, transport error) and optional read verification

See `docs/bench.md` for full usage and design.

## Suggested Reading Order

1. `README.md`
2. `docs/deployment.md`
3. `docs/rust.md`
4. `docs/configuration.md`
5. `crates/mooncake-store-core`
6. `docs/features.md`
7. `docs/architecture.md`
8. `crates/mooncake-store-e2e/src/main.rs`
