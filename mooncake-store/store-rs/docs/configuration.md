# Configuration Reference

This document collects the main runtime knobs exposed by the current implementation.

## `StoreClientBuilder`

`StoreClientBuilder` is the main construction surface for Rust clients.

| Method or Field | Default | Notes |
|-----------------|---------|-------|
| `new(metadata, stable_id)` | required | `stable_id` is the long-lived node identity |
| `epoch(ClientEpoch(1))` | `1` | increment for successor processes |
| `compatibility(...)` | `CompatibilityDescriptor::default()` | controls compatibility matching |
| `rpc_address(...)` | empty | filled from transport when possible |
| `segment_name(...)` | none | filled from transport when possible |
| `state(...)` | `Standby` | use `Active` for serving clients |
| `tenant(...)` | `"default"` | default tenant for request builders |
| `local_memory(...)` | `LocalMemoryConfig::default()` | storage and scratch memory layout |
| `transport(...)` / `with_tent(...)` | none | required for remote transfer paths |
| `transport_factory(...)` | none | used to create transports for peers |
| `routed_writes(...)` | disabled | enables routed placement |
| `route_control(...)` | `EmbeddedWrh` | selects route control mode |

System-managed behavior:

- the builder adds `route=true` when missing
- the control-plane address label is published automatically
- if transport is present, RPC address and segment name are inferred when possible

## `LocalMemoryConfig`

`LocalMemoryConfig` controls the memory that a client exposes to the store.

| Field | Default | Meaning |
|-------|---------|---------|
| `storage_bytes` | `64 MiB` | capacity for object storage |
| `scratch_bytes` | `4 MiB` | temporary space for transfer staging |
| `location` | `"cpu:0"` | placement string passed into transport registration |
| `tags` | `["dram"]` | tags published with the segment |
| `alignment` | `64` | allocation alignment |
| `reclaim_grace_ms` | `1000` | delayed reclaim window |

Validation rules:

- `storage_bytes` must be greater than zero
- `scratch_bytes` must be greater than zero
- `location` must not be empty

## Route Control

| Mode | Default | Behavior |
|------|---------|----------|
| `RouteControlMode::EmbeddedWrh` | yes | client-side route authority selection with metadata fallback |
| `RouteControlMode::MetadataOnly` | no | route reads and writes go directly to metadata |

Use `MetadataOnly` for bring-up and debugging. Use `EmbeddedWrh` for normal deployments.

## Routed Placement

Routed placement is enabled through:

```rust
let planner = PlacementPlanner::new(metadata.clone()).require_label("storage", "true");
let client = StoreClientBuilder::new(metadata, "router")
    .routed_writes(planner, 2);
```

### `PlacementPlanner`

| Method | Default | Meaning |
|--------|---------|---------|
| `scope_label("pool")` | `pool` | label key used to scope candidate sets |
| `require_label(key, value)` | none | filters placement candidates |

Planner behavior:

- only active leases are considered
- compatibility descriptors must match
- candidates are ranked with rendezvous hashing

## `ReplicationPolicy`

`ReplicationPolicy` overrides placement behavior for a single request.

| Field | Default | Meaning |
|-------|---------|---------|
| `replica_count` | `None` | use the client default |
| `with_soft_pin` | `false` | request soft pin behavior |
| `preferred_segments` | empty | prefer specific segment names |
| `preferred_storage_owners` | empty | prefer specific storage owners |
| `prefer_alloc_in_same_node` | `false` | bias allocation toward the same node |
| `prefer_local` | `true` | prefer the local node before remote spillover |

If `replica_count` is not set:

- local-only clients default to one replica
- routed clients default to the replica count passed to `routed_writes(...)`

## Labels and Naming

The runtime and examples use these conventions:

| Key | Purpose |
|-----|---------|
| `storage` | opt a client into routed placement candidate sets |
| `pool` | default placement scope label |
| `route_scope` | optional route-authority scope |

Recommended practice:

- treat `stable_id` as the persistent node identity
- treat `segment_name` as the current process-owned segment identity
- assign a distinct metadata keyspace per deployment

## Metadata Backends

### Rust

Rust code constructs metadata backends directly:

- `RedisMetadataBackend`
- `EtcdMetadataBackend`
- `InMemoryMetadataBackend`

Both Redis and etcd backends accept `MetadataKeyspace`.

### Python compatibility layer

The Python compatibility layer accepts metadata URLs:

| Scheme | Meaning |
|--------|---------|
| `redis://host:port/db` | Redis store metadata |
| `etcd://host1:2379,host2:2379` | etcd store metadata |

Notes:

- HTTP metadata endpoints are not supported
- when store metadata uses etcd, TENT metadata still needs Redis
- set `transport_metadata_url` or `MC_STORE_RS_TENT_REDIS_URL` for that Redis endpoint

## Environment Variables

The current repository uses these environment variables.

| Variable | Used By | Meaning |
|----------|---------|---------|
| `MC_STORE_RS_TRACE` | e2e and applications | enable tracing initialization from env |
| `MC_STORE_RS_TRACE_FILTER` | e2e and applications | `tracing_subscriber` filter string |
| `MC_STORE_RS_METRICS_ADDR` | e2e and applications | bind address for the in-process metrics server |
| `MC_STORE_RS_REDIS_URL` | Rust e2e | metadata Redis URL |
| `MC_STORE_RS_REDIS_PORT` | local scripts and e2e | local Redis port |
| `MC_STORE_RS_VALUE_SIZE` | Rust e2e | payload size for validation and benchmark loops |
| `MC_STORE_RS_BATCH_BENCH_ITERS` | Rust e2e | benchmark iteration count |
| `MC_STORE_RS_BENCH_ITERS` | `scripts/run-local-e2e.sh` | input that the script maps to `MC_STORE_RS_BATCH_BENCH_ITERS` |
| `MC_STORE_RS_PRINT_METRICS` | Rust e2e | print the Prometheus text snapshot at the end of the run |
| `MC_STORE_RS_TENT_REDIS_URL` | Python compatibility layer | Redis URL used by TENT when store metadata is etcd |
| `MOONCAKE_UPSTREAM_DIR` | local scripts | upstream Mooncake source tree |
| `MOONCAKE_UPSTREAM_BUILD_DIR` | local scripts | upstream Mooncake build output tree |

## Observability

The Rust client exposes:

- `init_tracing(...)`
- `init_tracing_from_env(...)`
- `render_prometheus_metrics()`
- `start_metrics_http_server(...)`
- `start_metrics_http_server_from_env(...)`
- `stop_metrics_http_server()`

The metrics HTTP server exposes:

- `GET /metrics`
- `GET /healthz`

## Practical Defaults

For local development:

- use Redis metadata
- keep `route_control` at `EmbeddedWrh`
- label storage targets with `storage=true`
- keep `prefer_local=true` unless you want remote-first behavior

For debugging:

- switch to `MetadataOnly` if you want all route state to flow through metadata

For upgrade and elasticity flows:

- preserve `stable_id`
- increment `epoch`
- use `expand_local_memory`, `drain_segment`, `retire_segment`, and `evacuate_owned_replicas`
