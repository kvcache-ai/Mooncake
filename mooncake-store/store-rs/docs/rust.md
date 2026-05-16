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
use mooncake_store_core::{ClientLifecycleState, CompatibilityDescriptor, Result};
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

`tenant(...)` remains the normal way to select the default scope used for startup policy lookup and request builders. Startup policy resolution reads exact tenant-policy scopes for `tenant -> tenant/domain -> tenant/domain/object_set` instead of listing all tenant policies. `route_topk(...)`, `route_control(...)`, `namespace_quota(...)`, `execution_fairness(...)`, and `bandwidth_shaping(...)` are compatibility fallbacks; admin-managed tenant policy in metadata is the preferred authoring surface when those settings are tenant-scoped.

When tenant quota policy is present, single-object `put` and `remove` now use metadata-backed reservation/finalize semantics instead of relying only on the older runtime-local preflight check. The write call returns success only after route publication and quota finalization both succeed. Overwrites are charged on committed byte delta, and deletes refund quota when the route delete CAS becomes authoritative rather than waiting for later segment reclaim.

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

Preferred storage owners are placement hints. If a hinted owner is not in the compatible live
snapshot or is locally quarantined as suspect, the writer skips it and continues with normal
placement candidates instead of failing the cache write before fallback can run.

## Use Batch and Buffer-Oriented APIs

The Rust client supports both object-oriented and buffer-oriented paths.

### Basic and batch APIs

- `put`, `get`, `remove`
- `batch_put`, `batch_get`, `batch_remove`
- `is_exist`, `batch_is_exist`, `get_size`

`query_route`, `get_size`, `is_exist`, and `batch_is_exist` are treated as access
signals when they find active routes. They report route hits to the storage owners
best-effort through the same CLOCK hit-report path used by reads, so applications that
probe keys before restore do not lose hot pages to background eviction between the probe
and the later read.

`StoreClient::batch_is_readable` is the cache-facing variant used by the Python
compatibility layer. It keeps the same bounded route lookup but returns `true` only
when at least one route replica belongs to a live, non-suspect readable runtime.

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

Behavior boundary:

- `put_from` and `batch_put_from` send remote writes from the registered source buffer directly
- `batch_get_into` reads remote payloads directly into registered destination buffers
- routed `batch_put_from` treats per-key route CAS conflicts as successful cache insert races when metadata returns or an exact bounded recheck finds an already published active route, releases its own temporary reservation, and returns that route entry without failing the batch
- `segment_offset` is the durable storage coordinate for the payload; remote reads, local reads, and drain migration all derive the live TE target offset from `segment_offset` and the segment's published storage target chunks, then verify the target range is present in the current TE segment buffers before touching storage
- unregistered `get_into` targets and `batch_put_from_multi_buffers` still fall back to the staged copy paths

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

During evacuation, payload copy is pinned to the exact local replica being removed. The writer does not satisfy the migration read from another live mirror, because that would publish the wrong bytes when replicas temporarily diverge during a shrink.
The migration read uses the replica's durable `segment_offset`, derives the live transport target offset through the selected segment's published storage target chunks, verifies the range against the current TE segment buffers, and only then touches local memory.

Before returning, the draining client also mirrors any route-authority records that were only present locally to active route authorities. If its cached live-client snapshot cannot find a mirror, it refreshes membership and retries the mirror pass.

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

Use `route_topk(...)` when you want a different WRH route-authority fanout. The runtime defaults to `2`, rejects values below `2`, and validates the setting against the effective route policy already stored in the active metadata keyspace: tenant override for the builder's default tenant first, otherwise the default cluster policy.

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

Every Prometheus sample exported by a `StoreClient` process includes `tenant="<default tenant>"`, matching the process-bound tenant selected by `StoreClientBuilder::tenant(...)`. The exported metrics also include strict tenant quota counters for reservation, finalize, abort, and reconcile outcomes, in addition to the existing request, route-CAS, replication, eviction, and process families. Sparse operational counters such as tenant quota, tenant-local eviction, preferred-segment skip, rebalance, and segment lifecycle are exported with zero-valued baseline series so dashboards show an explicit zero rate during steady state instead of `No data`.

## Full Example

The most complete runnable example is `crates/mooncake-store-e2e/src/main.rs`.

It covers:

- local and routed writes
- request-level replication
- batch put/get
- strict tenant quota on single-object and routed batch writes
- strict tenant quota e2e validation for Redis-backed admit/finalize/reject/delete-refund state
- registered-buffer and multi-buffer paths
- delete reclaim and overwrite reclaim
- dynamic expansion, true client shrink, and hot-upgrade

## Benchmarking

Use `mooncake-store-bench` for throughput measurement, correctness verification, and long-duration stability testing. It is built from `crates/mooncake-store-py/src/bin/mooncake_store_bench/` and ships in the wheel.

Quick start:

```bash
# Correctness check
# Defaults use batch_put + batch_get; override interfaces when needed.
mooncake-store-bench --metadata-url redis://127.0.0.1:6379/0 verify

# 30-second mixed benchmark, 8 concurrent workers
# Defaults measure batch_put + batch_get; override interfaces when needed.
# Start storage=true daemons first, and pass the same --keyspace they use
# when you are not using the default `mc/store-rs/v2` namespace.
mooncake-store-bench --metadata-url redis://127.0.0.1:6379/0 bench \
  --keyspace mc/store-rs/bench-prod \
  --mode mixed --concurrency 8 --duration 30

# 1-hour soak test with Redis jitter fault injection
mooncake-store-bench --metadata-url redis://127.0.0.1:6379/0 soak \
  --duration 3600 --fault redis-jitter:5:50 --verify-reads
```

See `docs/bench.md` for the full CLI reference and architecture description.

## Next Reading

- `docs/configuration.md` for defaults and knobs
- `docs/deployment.md` for local and multi-role deployment
- `docs/architecture.md` for runtime behavior
