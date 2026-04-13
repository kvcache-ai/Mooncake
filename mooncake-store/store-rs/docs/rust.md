# Rust Guide

This document shows how to assemble and use the Rust client from the current workspace.

## Before You Start

Bring `MooncakeCompatibilityFacade` into scope. Most store operations are exposed through that trait.

```rust
use mooncake_store_client::MooncakeCompatibilityFacade;
```

You also need:

- a metadata backend
- a transport engine
- a transport factory
- a `StoreClientBuilder`

## Build a Local Client

```rust
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use mooncake_metadata::{MetadataKeyspace, RedisMetadataBackend, RedisMetadataConfig};
use mooncake_store_client::{
    LocalMemoryConfig, MooncakeCompatibilityFacade, StoreClientBuilder, TentTransportFactory,
};
use mooncake_store_core::{ClientEpoch, ClientLifecycleState, CompatibilityDescriptor, Result};
use mooncake_transport::{TentEngine, TentEngineConfig};

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time should move forward")
        .as_millis() as u64
}

fn main() -> Result<()> {
    let metadata = Arc::new(RedisMetadataBackend::new(
        RedisMetadataConfig::new("redis://127.0.0.1:6380/0")
            .keyspace(MetadataKeyspace::new("mc/store-rs/demo")),
    )?);

    let tent_config = TentEngineConfig::new()
        .set("metadata_type", "redis")
        .set("metadata_servers", "127.0.0.1:6380")
        .set("redis_db_index", "0")
        .set("rpc_server_hostname", "127.0.0.1")
        .set("rpc_server_port", "0")
        .set("log_level", "warning")
        .set("transports/tcp/enable", "true")
        .set("transports/shm/enable", "false")
        .set("transports/rdma/enable", "false")
        .set("transports/io_uring/enable", "false");

    let engine = Arc::new(TentEngine::new(
        &tent_config.clone().set("local_segment_name", "demo-segment"),
    )?);
    let factory = Arc::new(TentTransportFactory::new(tent_config));

    let client = StoreClientBuilder::new(metadata, "demo-store")
        .epoch(ClientEpoch(1))
        .state(ClientLifecycleState::Active)
        .compatibility(CompatibilityDescriptor::default())
        .tenant("default")
        .route_topk(2)
        .local_memory(
            LocalMemoryConfig::new()
                .storage_bytes(128 * 1024 * 1024)
                .scratch_bytes(16 * 1024 * 1024)
                .location("cpu:0"),
        )
        .with_tent(engine)
        .transport_factory(factory)
        .build(now_ms() + 600_000)?;

    client.register_local_memory()?;
    client.put("hello", b"world")?;
    assert_eq!(client.get("hello")?, b"world");
    Ok(())
}
```

`rpc_server_port` is the TENT TCP data-plane port. Leaving it at `0` lets TENT choose a random local port, which is fine for single-host demos. For cross-host or cross-container deployments, set a fixed port and make sure peers can reach `rpc_server_hostname:rpc_server_port`.

## Transport Backends

The Rust transport layer exposes two explicit choices:

- `TentEngine` + `TentEngineConfig`
- `ClassicTransferEngine` + `ClassicEngineConfig`

This guide uses TENT for the concrete examples because it is the default repository path, but Rust applications can construct either backend directly.

Runtime backend selection is implemented by the compatibility layer, not by `StoreClientBuilder` itself:

- `MC_STORE_RS_TRANSPORT_BACKEND=tent|classic_te`
- `mooncake-store-client --transport-backend tent|classic-te`
- `MooncakeDistributedStore.setup(..., transport_backend="tent"|"classic_te")`

Redis authentication can come from URL-embedded credentials or from `MC_REDIS_USERNAME` / `MC_REDIS_PASSWORD`. Prefer environment variables when passwords contain URL-reserved characters such as `@`.

## Enable Routed Writes

Use `PlacementPlanner` and `routed_writes(...)` when a client should route writes to storage nodes.

```rust
use mooncake_store_client::PlacementPlanner;

let planner = PlacementPlanner::new(metadata.clone()).require_label("storage", "true");
let router = StoreClientBuilder::new(metadata, "router-a")
    .state(ClientLifecycleState::Active)
    .label("storage", "false")
    .with_tent(engine)
    .transport_factory(factory)
    .local_memory(local_memory)
    .routed_writes(planner, 2)
    .build(now_ms() + 600_000)?;
```

In this mode:

- placement candidates come from live compatible clients
- candidates are filtered by planner labels
- replica owners are ranked with rendezvous hashing

## Use Request Policies

`ReplicationPolicy` changes placement behavior for one request.

```rust
use mooncake_store_client::ReplicationPolicy;

let policy = ReplicationPolicy::new()
    .replica_count(2)
    .prefer_local(false)
    .preferred_storage_owner("store-b")
    .with_soft_pin(true);

client.put_with_policy("replicated", b"payload", &policy)?;
```

Useful knobs:

- `replica_count(...)`
- `prefer_local(...)`
- `prefer_alloc_in_same_node(...)`
- `preferred_segment(...)`
- `preferred_segments(...)`
- `preferred_storage_owner(...)`
- `preferred_storage_owners(...)`
- `with_soft_pin(...)`

## Use Batch and Buffer-Oriented APIs

The Rust client supports both object-oriented and buffer-oriented paths.

### Basic and batch APIs

- `put`, `get`, `remove`
- `batch_put`, `batch_get`, `batch_remove`
- `is_exist`, `batch_is_exist`, `get_size`

### Registered-buffer APIs

- `register_buffer`
- `unregister_buffer`
- `put_from`
- `batch_put_from`
- `batch_get_into`

### Multi-buffer APIs

- `batch_put_from_multi_buffers`
- `batch_get_into_multi_buffers`

Use the buffer-oriented forms when the application already owns stable memory and wants to avoid extra copies.

## Hugepage-Backed Local Memory

`LocalMemoryConfig` can request hugepage-backed local storage and scratch memory.

```rust
let local_memory = LocalMemoryConfig::new()
    .storage_bytes(128 * 1024 * 1024)
    .scratch_bytes(16 * 1024 * 1024)
    .use_hugepage(true)
    .hugepage_size_bytes(2 * 1024 * 1024);
```

Supported sizes:

- `2 MiB`
- `1 GiB`

If these fields are not set explicitly, the runtime can also use `MC_STORE_USE_HUGEPAGE` and `MC_STORE_HUGEPAGE_SIZE`.

## Multi-Tenant Access

Use tenant-aware request builders when the default tenant is not enough.

Examples:

- `put_in_tenant(...)`
- `get_in_tenant(...)`
- `remove_in_tenant(...)`
- `query_route_in_tenant(...)`

Batch request builders also accept `ObjectRef::tenant(...)`.

## Lifecycle and Elasticity

The compatibility facade also exposes lifecycle and memory-management operations.

Common calls:

- `heartbeat(expires_at_ms)`
- `enter_standby()`
- `activate()`
- `enter_draining()`
- `plan_handoff(...)`
- `list_segments()`
- `expand_local_memory(storage_bytes)`
- `drain_segment(segment)`
- `retire_segment(segment)`
- `evacuate_owned_replicas()`
- `evacuate_owned_replicas_via(writer)`

`evacuate_owned_replicas()` drains the local client, rewrites every live route that still references it, immediately reclaims old allocations, and retires emptied local segments.

Use `evacuate_owned_replicas_via(writer)` when you want a separate routed client to publish replacement routes during shrink.

These operations are the current control surface for hot-upgrade, elastic growth, segment drain/retire, and full client shrink flows.

## Route Inspection

Use these methods when debugging placement or route state:

- `query_route(...)`
- `cas_route(...)`
- `list_segments()`
- `runtime_id()`
- `lease()`

The default route control mode is `EmbeddedWrh`. Use `route_control(RouteControlMode::MetadataOnly)` if you want metadata-backed route control for bring-up or debugging.

Use `route_topk(...)` when you want a different WRH route-authority fanout. The runtime defaults to `2`, rejects values below `2`, and validates the setting against the route policy already stored in the active metadata keyspace.

## Observability

Tracing and metrics are part of the Rust client crate.

```rust
use mooncake_store_client::{
    init_tracing, render_prometheus_metrics, start_metrics_http_server,
};

init_tracing(Some("info"))?;
let addr = start_metrics_http_server("127.0.0.1:0")?;
println!("metrics: http://{addr}/metrics");
println!("{}", render_prometheus_metrics());
```

Key functions:

- `init_tracing(...)`
- `init_tracing_from_env(...)`
- `render_prometheus_metrics()`
- `start_metrics_http_server(...)`
- `stop_metrics_http_server()`

## Full Example

The most complete runnable example is `crates/mooncake-store-e2e/src/main.rs`.

It covers:

- local and routed writes
- request-level replication
- batch put/get
- registered-buffer and multi-buffer paths
- delete reclaim and overwrite reclaim
- dynamic expansion, true client shrink, and hot-upgrade

## Next Reading

- `docs/configuration.md` for defaults and knobs
- `docs/deployment.md` for local and multi-role deployment
- `docs/architecture.md` for runtime behavior
