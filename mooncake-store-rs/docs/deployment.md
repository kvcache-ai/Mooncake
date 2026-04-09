# Deployment Guide

This document explains how to run `mooncake-store-rs` locally and how to map the runtime to common deployment roles.

## Requirements

- Rust toolchain
- `cmake` and a C++ toolchain
- `redis-server` and `redis-cli`
- Git submodule support
- Python 3, if you want to run the Python compatibility layer

## Prepare the Repository

Fetch the upstream Mooncake submodule before building the transport layer:

```bash
git submodule update --init --recursive
```

The repository expects the upstream sources at `third_party/Mooncake`.

## Local Validation

### Rust e2e

Run the full Rust end-to-end suite and the built-in batch benchmark:

```bash
./scripts/run-local-e2e.sh
```

What the script does:

- checks that `third_party/Mooncake` exists
- starts a local Redis instance on port `6380` when needed
- sets `LD_LIBRARY_PATH` for the upstream TE/TENT artifacts
- runs `cargo run --release -p mooncake-store-e2e`

Supported script inputs:

| Variable | Default | Used By |
|----------|---------|---------|
| `MC_STORE_RS_REDIS_PORT` | `6380` | local Redis port |
| `MC_STORE_RS_BENCH_ITERS` | `512` | batch benchmark loop count passed into e2e |
| `MC_STORE_RS_VALUE_SIZE` | `4096` | payload size used by e2e |
| `MOONCAKE_UPSTREAM_DIR` | `third_party/Mooncake` | upstream source location |
| `MOONCAKE_UPSTREAM_BUILD_DIR` | `third_party/Mooncake/build-rust` | upstream build output location |

### Python compatibility e2e

Run the Python compatibility validation:

```bash
./scripts/run-python-compat-e2e.sh
```

What the script does:

- builds `mooncake-store-py`
- exports `PYTHONPATH="$PWD/python"`
- creates two Python clients
- validates single-object, batch, zero-copy, multi-buffer, route, and metrics paths

### HiCache compatibility validation

Run the compatibility checks for both Python execution modes:

```bash
./scripts/run-sglang-hicache-dummy-compat.sh
./scripts/run-sglang-hicache-real-compat.sh
```

What they validate:

- dummy path through the standalone compatibility server plus shm registration
- real path through the native distributed store runtime plus registered-buffer I/O

### Wheel packaging

Build the Python wheel and stage the standalone client binary:

```bash
./scripts/build-wheel.sh
```

Default outputs:

- `dist/wheels/`
- `dist/bin/mooncake-store-client`

## Deployment Roles

The runtime is assembled from regular clients with different configuration.

### Storage node

A storage node owns local segments and can accept local or remote allocation requests.

Typical settings:

- `state(ClientLifecycleState::Active)`
- `label("storage", "true")`
- non-zero `LocalMemoryConfig`
- `register_local_memory()` after `build(...)`
- optional hugepage-backed local memory through `LocalMemoryConfig`

### Routed writer

A routed writer accepts write requests and places replicas onto storage nodes selected by `PlacementPlanner`.

Typical settings:

- `label("storage", "false")`
- `routed_writes(planner, replica_count)`
- local memory for scratch space and optional local placement

### Reader or stateless client

A reader can resolve routes and fetch objects without acting as a storage target.

Typical settings:

- `label("storage", "false")`
- no `routed_writes(...)` unless it also routes writes
- transport enabled if it needs remote data transfer

## Metadata and Transport

`mooncake-store-rs` separates store metadata from transport metadata.

### Store metadata

Supported backends:

- Redis
- etcd
- in-memory backend for tests

Store metadata is responsible for:

- live client leases
- segment announcements and lifecycle state
- fallback route persistence
- handoff plans

### Transport metadata

The transport layer is configured through `TentEngineConfig`.

Current repository scripts and examples use Redis-backed TENT metadata. In the Python compatibility layer, etcd for store metadata still requires Redis for TENT metadata through `transport_metadata_url` or `MC_STORE_RS_TENT_REDIS_URL`.

## Routing Modes

### `EmbeddedWrh`

This is the default mode.

- route ownership is selected on the client with weighted rendezvous hashing
- normal route reads and compare-and-swap stay off the metadata hot path
- metadata remains the fallback when authority RPC is unavailable

### `MetadataOnly`

This mode is useful for simpler bring-up and debugging.

- route reads and writes go directly to the metadata backend
- fewer moving parts
- higher dependence on metadata latency

## Label Conventions

The current implementation relies on a few label conventions.

| Label | Meaning |
|-------|---------|
| `storage=true` | marks a client as a placement candidate for routed writes |
| `pool=<name>` | scopes placement planning; this is the default placement scope key |
| `route_scope=<name>` | optionally scopes route authority selection |

The runtime also manages some labels internally, such as route capability and control-plane address publication.

## Operational Conventions

- keep `stable_id` stable across restarts and upgrades
- increment `epoch` for successor processes during hot-upgrade flows
- mount local memory before serving data traffic
- use a dedicated metadata keyspace per environment or test run
- if hugepage mode is enabled, preallocate matching hugepages on the host before starting clients

## Validation Coverage

The current end-to-end binary covers:

- single and batch put/get
- registered-buffer and multi-buffer paths
- request-level replication policy
- overwrite reclaim and delete reclaim
- routed writes and multi-replica publication
- multi-tenant access
- dynamic expansion, true client shrink, and hot-upgrade handoff

The entry point is `crates/mooncake-store-e2e/src/main.rs`.

## Next Reading

- `docs/rust.md` for Rust integration
- `docs/configuration.md` for knobs and defaults
- `docs/architecture.md` for request paths and control-plane behavior
