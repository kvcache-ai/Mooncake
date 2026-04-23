# Configuration Reference

This document collects the main runtime knobs exposed by the current implementation.

## Tenant Policy Precedence

Tenant-scoped routing and resource policy should be authored through `mooncake-store-admin policy ...`
and stored in durable metadata. Runtime-local knobs remain available as compatibility fallbacks.

Precedence is:

1. admin-managed tenant policy in metadata
2. legacy compatibility metadata reads where still supported during migration
3. runtime-local builder / Python / standalone client fallback values when metadata does not provide the relevant section

Keep this distinction clear:

- `tenant` selects the default scope used for request builders and startup policy lookup
- request-scoped APIs and per-request `ReplicationPolicy` remain normal execution-time inputs
- local route/resource knobs are not the preferred long-term policy authoring surface
- strict quota usage/accounting inspection now comes from admin/metadata surfaces, not builder fields

## Admin strict-quota inspection and repair

The admin surface now exposes metadata-backed strict quota inspection plus explicit repair commands:

- `mooncake-store-admin quota state --tenant <tenant> [--domain <domain>] [--object-set <set>]`
- `mooncake-store-admin quota object --tenant <tenant> [--domain <domain>] [--object-set <set>] --key <logical-key>`
- `mooncake-store-admin quota reservations --tenant <tenant> [--domain <domain>] [--object-set <set>] [--state pending|finalized|aborted]`
- `mooncake-store-admin quota abort --tenant <tenant> [--domain <domain>] [--object-set <set>] --reservation-id <id> [--dry-run]`
- `mooncake-store-admin quota reconcile --tenant <tenant> [--domain <domain>] [--object-set <set>] [--dry-run]`

Operational notes:

- quota state and reservation queries resolve to the tenant-root scope because mutable quota state is tenant-root metadata in Phase 1
- object-accounting lookups still accept nested selectors so operators can specify the logical object namespace they care about
- reconcile currently aborts expired pending reservations and finalizes pending reservations whose authoritative route + accounting state is already visible
- finalized and aborted reservations are retained for a bounded terminal window, then expire from Redis automatically; the default terminal TTL is `24h`
- `Pending` reservations still rely on reconcile / abort paths rather than Redis TTL so an in-flight write is never deleted out from under finalize
- `crates/mooncake-metadata` exposes `RedisMetadataConfig::tenant_quota_terminal_ttl(...)` for deployments that want a different terminal retention window
- reservation listing prunes dangling reservation index entries opportunistically when the backing reservation record is already gone
- other mismatches remain operator-visible and are reported as skipped rather than repaired speculatively
- the admin HTTP surface rejects requests whose declared `Content-Length` exceeds `1 MiB` with `413 Payload Too Large`; because the server does not drain the remaining body, it responds with `Connection: close`

## Admin maintenance

Redis-backed and etcd-backed deployments now support a stateless admin maintenance loop for stale segment cleanup.
The same admin server can also run tenant-quota reservation reconcile for an explicit tenant list.

Relevant surfaces:

- `mooncake-store-admin cleanup-stale-segments`
- `mooncake-store-admin server --cleanup-interval-ms <ms> --cleanup-batch-size <n>`
- `mooncake-store-admin server --quota-reconcile-interval-ms <ms> --quota-reconcile-tenant <tenant>`

Operational model:

- Redis lease keys still use TTL as the liveness signal
- etcd lease keys do not expire automatically; admin re-checks the stored `expires_at_ms` field when due work arrives
- segment keys still do **not** use TTL in either backend
- lease publish and heartbeat refresh now also update a backend-native expiry work index under the same metadata keyspace: Redis uses one sorted set, etcd uses `by-runtime` + lexicographically ordered `by-time` keys
- the admin maintenance loop consumes due entries from that work index, re-checks lease liveness, and only then removes dead-owner segment metadata through owner-scoped segment metadata instead of a hidden global scan in the steady-state worker
- same-epoch lease reclaim after an expired lease key is allowed when that epoch is still the historical HWM and no higher live epoch exists, so heartbeat repair does not fail with `StaleEpoch` after a TTL gap
- tenant quota reconcile is intentionally opt-in per tenant because the current metadata model has no bounded global tenant-work index for the admin plane to consume safely

Server maintenance defaults:

- `--cleanup-interval-ms 5000`
- `--cleanup-batch-size 128`
- `--cleanup-interval-ms 0` disables the background maintenance worker
- `--quota-reconcile-interval-ms 0` disables background tenant quota reconcile
- `--quota-reconcile-tenant <tenant>` can be repeated; when unset, no tenant quota reconcile worker is started

## `StoreClientBuilder`

`StoreClientBuilder` is the main construction surface for Rust clients.

| Method or Field | Default | Notes |
|-----------------|---------|-------|
| `new(metadata, stable_id)` | required | `stable_id` is the long-lived node identity; the metadata backend assigns the epoch atomically on every `build(...)` |
| `compatibility(...)` | `CompatibilityDescriptor::default()` | controls compatibility matching |
| `rpc_address(...)` | empty | filled from transport when possible |
| `segment_name(...)` | none | filled from transport when possible |
| `state(...)` | `Standby` | use `Active` for serving clients |
| `tenant(...)` | `"default"` | default tenant for request builders and startup policy lookup |
| `local_memory(...)` | `LocalMemoryConfig::default()` | storage and scratch memory layout |
| `transport(...)` / `with_tent(...)` | none | required for remote transfer paths; low-level Rust transport is wired explicitly |
| `transport_factory(...)` | none | used to create transports for peers |
| `routed_writes(...)` | disabled | enables routed placement |
| `route_control(...)` | `EmbeddedWrh` | compatibility fallback route-control mode; metadata tenant policy wins when present |
| `route_topk(...)` | `2` | compatibility fallback WRH route-authority fanout; metadata tenant policy wins when present; must be `>= 2` |
| `live_client_sync_interval(...)` | `1s` | background refresh interval for the live-client membership snapshot; `0` disables the worker |

System-managed behavior:

- the builder adds `route=true` when missing
- the control-plane address label is published automatically
- if transport is present, RPC address and segment name are inferred when possible
- `build(...)` prewarms the live-client membership snapshot before serving requests
- the background membership worker refreshes that snapshot after startup

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
| `eviction_high_watermark_percent` | `90` | start background eviction when local usage reaches this percentage |
| `eviction_low_watermark_percent` | `80` | stop background eviction after usage falls to this percentage |
| `eviction_poll_interval` | `100ms` | background storage-owner eviction polling interval; `0` disables the worker |
| `hugepage_enabled` | `None` | override hugepage enablement for local storage and scratch |
| `hugepage_size_bytes` | `None` | override hugepage size; `2 MiB` and `1 GiB` are supported |

Validation rules:

- `scratch_bytes` must be greater than zero
- `location` must not be empty
- `eviction_high_watermark_percent` must be in `1..=100`
- `eviction_low_watermark_percent` must be lower than `eviction_high_watermark_percent`
- hugepage size must be either `2 MiB` or `1 GiB`

`storage_bytes` may be `0`. This is how scratch-only rw clients are configured when storage and inference are deployed as separate roles.

Storage-role normalization:

- `storage=true` requires `storage_bytes > 0`
- if `storage_bytes=0` and the storage label is missing, the runtime normalizes it to `storage=false`

Hugepage behavior:

- if `hugepage_enabled` is `Some(true)`, the client allocates native local memory with `MAP_HUGETLB`
- if `hugepage_size_bytes` is set, hugepage mode is implicitly enabled
- if both fields are `None`, the runtime falls back to `MC_STORE_USE_HUGEPAGE` and `MC_STORE_HUGEPAGE_SIZE`

Startup registration behavior:

- `classic_te` + RDMA keeps startup storage-region planning NUMA-aware when `location` targets host memory and `numa_aware` remains enabled
- only transports that opt into parallel startup registration fan out initial storage into multiple segments; other transports keep the historical single-segment startup layout
- when startup planning produces multiple initial storage segments, the runtime allocates and registers those extra segments in parallel, then publishes the full local segment set after local registration completes
- under `classic_te` + RDMA, startup storage registration pre-touches pages before MR registration when the total startup storage registration volume reaches `4 GiB` or more
- ordinary scratch-region registration still follows the transport registration limit split logic; the startup fast path is only for storage segments

## Transport Backend Selection

The compatibility layer supports two real data-plane backends:

| Backend | Compatibility Value | Notes |
|---------|---------------------|-------|
| TENT | `tent` | opt-in compatibility backend |
| Classic TE | `classic_te` | runtime-selectable compatibility backend |

Selection surfaces:

| Surface | Knob |
|---------|------|
| Environment | `MC_STORE_RS_TRANSPORT_BACKEND=tent|classic_te` |
| Standalone client | `--transport-backend tent|classic-te` |
| Python compatibility | `transport_backend="tent"` or `transport_backend="classic_te"` |

Notes:

- explicit CLI or Python values override the environment variable
- when no explicit selection is present, the compatibility layer defaults to `classic_te`
- `classic`, `classic-te`, and `te` are accepted as compatibility aliases by the parser
- set `MC_STORE_RS_GID_INDEX=<n>` when `classic_te` over RDMA must use a non-default RoCE GID index; Store-RS forwards it to upstream `MC_GID_INDEX`
- low-level Rust transport construction remains explicit; runtime backend selection is only a compatibility-layer feature
- current upstream SGLang Mooncake integration does not forward `transport_backend` from `--hicache-storage-backend-extra-config`; use `MC_STORE_RS_TRANSPORT_BACKEND` when SGLang real mode must select `tent` or `classic_te`

Reconnect behavior:

- both backends repair local transport metadata after Redis connectivity returns and the heartbeat repair path runs
- `classic_te` recreates its transport runtime before republishing local buffers
- `tent` also recreates its transport runtime before re-registering the local buffers it still owns
- peer restarts that invalidate a cached remote segment handle are repaired on demand: the read path drops the stale handle, reopens by segment name, and only quarantines that runtime if the fresh reopen still fails
- routed writes apply the same stale-handle refresh once before escalating to outer soft-pin retry or failover, so a restarted live peer does not poison the cached remote-segment state
- if Redis restarts from an empty dataset, surviving storage clients republish both lease state and segment metadata during recovery
- requests that arrive while Redis is unavailable can still fail fast; recovery is designed for self-healing after metadata service returns, not for serving through a metadata blackout

## Route Control

| Mode | Default | Behavior |
|------|---------|----------|
| `RouteControlMode::EmbeddedWrh` | yes | client-side route authority selection with mirrored top-k publication, ranked authority reads, and a prewarmed background-refreshed membership snapshot |
| `RouteControlMode::MetadataOnly` | no | route reads and writes go directly to metadata |

Use `MetadataOnly` for bring-up and debugging. Use `EmbeddedWrh` for normal deployments.

### Route authority policy

`route_topk` controls how many WRH-ranked route authorities each key uses.

- the highest-ranked authority is the CAS primary
- the remaining `route_topk - 1` authorities are mirrors used for read repair and mirror publication
- `route_topk` is **not** the same as write-side `replica_count`
- the runtime rejects `route_topk < 2`

Startup bootstrap is metadata-authoritative:

- admin-managed tenant policy is the preferred source for tenant-scoped routing
- runtime-local `route_control + route_topk` values are bootstrap/compatibility fallbacks only
- if the metadata keyspace has no default route policy yet, the first successful client writes it with create-if-absent semantics
- startup then resolves the effective policy for the client's default tenant: tenant override first, otherwise the default cluster policy
- later clients must match that effective policy or startup fails immediately

Use `mooncake-store-admin policy set --tenant <tenant> --route-topk <n> --route-control <mode>` when one tenant in a shared metadata keyspace needs a different route-authority policy.

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
| `preferred_segments` | empty | request-scoped segment preference; hard by default, best-effort when `with_soft_pin=true` |
| `preferred_storage_owners` | empty | prefer specific storage owners |
| `prefer_alloc_in_same_node` | `false` | bias allocation toward the same node |
| `prefer_local` | `true` | prefer the local node before remote spillover |

If `replica_count` is not set:

- local-only clients default to one replica
- routed clients default to the replica count passed to `routed_writes(...)`

Keep the distinction clean:

- `replica_count` controls how many data replicas a write publishes
- `route_topk` controls how many route authorities keep mirrored route metadata
- request-level `preferred_segments` is the explicit pinning surface for a single write
- tenant policy `placement.preferred_segments` is only a default hint and falls back when the segment is missing or stale
- tenant-policy `preferred_segments` must use the exact active `segment_name`; a `stable_id` is not a valid segment identifier

## Labels and Naming

The runtime and examples use these conventions:

| Key | Purpose |
|-----|---------|
| `storage` | opt a client into routed placement candidate sets and enable local background/synchronous eviction when it owns storage memory |
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
- Redis store metadata accepts URL-embedded credentials or `MC_REDIS_USERNAME` / `MC_REDIS_PASSWORD`
- when store metadata uses etcd, transport metadata still needs Redis
- set `transport_metadata_url` or `MC_STORE_RS_TENT_REDIS_URL` for that Redis endpoint

### Redis authentication

Use `MC_REDIS_PASSWORD` for password-only Redis deployments, including cloud Redis instances that authenticate the default user:

```bash
export MC_REDIS_PASSWORD='<redis-password>'
mooncake-store-client \
  --local-hostname 127.0.0.1 \
  --metadata-url redis://redis.example.com:6379/0 \
  --stable-id store-a
```

Use `MC_REDIS_USERNAME` together with `MC_REDIS_PASSWORD` when Redis ACLs require a named user:

```bash
export MC_REDIS_USERNAME='<redis-username>'
export MC_REDIS_PASSWORD='<redis-password>'
```

URL-embedded credentials are also accepted:

```text
redis://username:password@redis.example.com:6379/0
```

If both forms are present, credentials in the URL take precedence. Prefer environment variables when passwords contain reserved URL characters such as `@`, `/`, `:` or `#`; URL-embedded credentials must be percent-encoded. Route namespaces redact URL credentials before they are used for routing metadata identity.

For Redis 5 password-only deployments, the metadata backend also tolerates connections where a username was supplied by configuration but the server accepts only legacy `AUTH <password>`; the client retries with password-only auth after the server rejects the username form.

## Python Compatibility Configuration

`MooncakeDistributedStore.setup(...)` accepts the core store knobs plus Python-specific convenience parameters.

Important Python-only compatibility knobs:

`tenant` remains a normal default scope selector. `route_topk` and `route_control` are kept for compatibility, but admin-managed tenant policy in metadata is the preferred place to author tenant-scoped routing policy.


| Parameter | Meaning |
|-----------|---------|
| `stable_id` | persistent client identity |
| `tenant` | default tenant scope |
| `labels` | lease labels such as `pool` and `storage` |
| `routed_writes` | enable routed placement from Python |
| `replica_count` | default replica count when routed writes are enabled |
| `route_topk` | WRH route-authority fanout; must match the policy already stored in the metadata keyspace |
| `transport_backend` | choose `tent` or `classic_te` for the real transport runtime |
| `transport_metadata_url` | Redis endpoint for transport metadata when store metadata uses etcd |
| `transport_rpc_port` | fixed real data-plane TCP port for real-mode peers |
| `use_hugepage` | enable hugepage-backed local store memory |
| `hugepage_size` | hugepage size for local store memory; accepts `2MB` or `1GB` |

The config-dict path accepts the same compatibility knobs. For port pinning, both `transport_rpc_port` and `rpc_server_port` map to the real-mode backend data-plane port.

`local_hostname` accepts either:

- a plain hostname such as `10.0.0.15`
- `host:port`, which is normalized into `local_hostname=host` plus `transport_rpc_port=port`

If `local_hostname` already embeds a port and `transport_rpc_port` or `rpc_server_port` is also provided, the values must match. The compatibility layer rejects mismatches instead of silently publishing an invalid real-mode endpoint.

Port role reminder:

- `transport_rpc_port` / `rpc_server_port` is used by real clients and maps to the selected backend `rpc_server_port`
- `client_server_address` belongs to the dummy compatibility server and is not used by real-mode peers
- `metrics_addr` only exposes `/metrics`

For cross-host or cross-container real-mode deployments, set a reachable `local_hostname` together with a fixed `transport_rpc_port`.

`master_server` / `master_server_addr` remains accepted on the Python compatibility entry points for upstream API parity, but the current store-rs runtime does not use a master-based control path. Real deployments should configure metadata with `redis://...` or `etcd://...`.

The compatibility client defaults `lease_ttl_ms` to `30000`. Keep the heartbeat interval comfortably below that TTL so dead peers converge quickly without triggering avoidable churn.

Timeouts now converge on one shared compatibility helper so the Python wrapper,
standalone client, dummy client, and transport builder all resolve the same
knobs with the same precedence rules:

- CLI override, when a standalone binary exposes the flag
- explicit environment variable
- built-in default

The compatibility layer exposes five timeout scopes:

| Knob | Default | Scope | Meaning |
|------|---------|-------|---------|
| `request_timeout_ms` / `--request-timeout-ms` / `MC_STORE_RS_REQUEST_TIMEOUT_MS` | `65000` | dispatcher request budget, routed read/write request budget, dummy fallback | outer per-request deadline shared across replica failover |
| `startup_timeout_ms` / `--startup-timeout-ms` / `MC_STORE_RS_STARTUP_TIMEOUT_MS` | `max(20000, ceil(registration_bytes / 1 GiB) * 1000)` | startup `register_local_memory`, real-mode `register_buffer`, and compatibility-side buffer unregister | registration-specific timeout budget; explicit override wins over the adaptive default |
| `heartbeat_timeout_ms` / `--heartbeat-timeout-ms` / `MC_STORE_RS_HEARTBEAT_TIMEOUT_MS` | `15000` | standalone client heartbeat / state publish | dedicated health-channel publish budget |
| `transfer_stall_timeout_ms` / `--transfer-stall-timeout-ms` / `MC_STORE_RS_TRANSFER_STALL_TIMEOUT_MS` | `10000` | TENT / classic transfer engine | inner stall detector for one transfer slice or batch wait |
| `MC_STORE_RS_DUMMY_RPC_TIMEOUT_MS` | `65000` | dummy gRPC client | dummy RPC budget; falls back to `request_timeout_ms` when unset |

Design intent:

- request timeout is the outer deadline for one logical store request
- startup timeout is the registration budget for compatibility-managed memory registration work; when unset, the runtime derives it from the current registration size with a conservative `20s` floor so small scratch-only startup does not fail under host-side contention
- heartbeat timeout is independent, so a slow health publish does not block the shared data path
- transfer stall timeout is not a whole-request timeout; it only detects no-progress transport stalls
- dummy RPC timeout follows request timeout unless explicitly overridden, so compatibility scripts do not hang forever on one slow server

Standalone clients now retry failed heartbeat publishes instead of exiting on
the first timeout. When cloud metadata links or long transfers need more head
room, raise the matching timeout scope instead of stretching every timeout.

Control-plane RPCs now run on a dedicated shared Tokio runtime instead of a
single global caller lock. Use `MC_STORE_RS_CONTROL_PLANE_THREADS` to tune the
worker count when a deployment needs more concurrent route / allocator RPCs or
when an embedded client should keep its thread footprint smaller. The default
is `2`; values must be positive integers.

## Python Local Hot Cache

The local hot cache lives inside the Python compatibility runtime and the standalone dummy daemon. It stays disabled unless `MC_STORE_LOCAL_HOT_CACHE_SIZE` is set to a positive integer.

| Variable | Default | Meaning |
|----------|---------|---------|
| `MC_STORE_LOCAL_HOT_CACHE_SIZE` | disabled | total cache capacity in bytes; unset or invalid values disable the cache |
| `MC_STORE_LOCAL_HOT_BLOCK_SIZE` | `16777216` (`16 MiB`) | block size in bytes and the maximum payload size that can be cached |
| `MC_STORE_LOCAL_HOT_CACHE_USE_SHM` | disabled | set to `1` to back cached payloads with shm so dummy clients on the same daemon can reuse them |

Behavior notes:

- effective cache capacity is `floor(total_size / block_size)` blocks
- values larger than `block_size` bypass the cache instead of being partially cached
- successful read misses populate the cache from the fetched value
- successful local writes and deletes invalidate the matching local cache entry on that daemon
- Python compatibility runtimes partition local entries by effective metadata keyspace, so different keyspaces do not reuse the same cached value even inside one process
- shm mode shares payload bytes with dummy clients connected to the same `mooncake-store-client`, while LRU metadata, generations, and pins remain private to the daemon
- cache entries are daemon-local only and are never published to Redis or etcd
- metadata keyspace remains the authoritative read/write isolation boundary; cache partitioning does not make objects visible across keyspaces

## Read-side Membership and Failure Semantics

The client keeps membership refresh out of the steady-state request path.

- `build(...)` prewarms a live-client lease snapshot before serving reads
- a background worker refreshes that snapshot after startup
- steady-state reads reuse the cached snapshot instead of issuing inline membership refreshes
- owners in `Active` and `Draining` state remain readable
- owners that become offline or hit transport/metadata failures are marked suspect and skipped on later reads
- suspect owners observe a minimum quarantine, then recover only after a fresh lease heartbeat or control-plane endpoint change appears in the membership snapshot
- when fallback to another replica succeeds, the client best-effort prunes unreadable owners from the route with CAS

`MooncakeHostMemAllocator(...)` exposes:

| Parameter | Meaning |
|-----------|---------|
| `use_hugepage` | request hugepage-backed shm regions |
| `hugepage_size` | hugepage size for shm regions; accepts `2MB` or `1GB` |

When the native extension is unavailable, the pure-Python allocator falls back to `mmap` and does not support hugepages.

## Environment Variables

The current repository uses these environment variables.

| Variable | Used By | Meaning |
|----------|---------|---------|
| `MC_STORE_RS_TRANSPORT_BACKEND` | compatibility layer, standalone client, Python wrapper | select `tent` or `classic_te` as the default real transport backend |
| `MC_STORE_RS_KEYSPACE` | Python wrapper setup fallback | metadata keyspace used when SGLang cannot pass `keyspace`; this also defines Python compatibility read/write visibility and local hot-cache partitioning |
| `MC_STORE_RS_STABLE_ID` | Python wrapper setup fallback | stable client id used when SGLang cannot pass `stable_id` |
| `MC_STORE_RS_INITIAL_STATE` | Python wrapper setup fallback | initial lifecycle state, for example `active`, `standby`, `draining`, or `offline` |
| `MC_STORE_RS_TENANT` | Python wrapper setup fallback | default tenant used when SGLang cannot pass `tenant` |
| `MC_STORE_RS_LABELS` | Python wrapper setup fallback | labels as JSON object or comma-separated `key=value` pairs |
| `MC_STORE_RS_ROUTED_WRITES` | Python wrapper setup fallback | enable routed writer mode when set to `1`, `true`, `yes`, or `on` |
| `MC_STORE_RS_REPLICA_COUNT` | Python wrapper setup fallback | default routed-writer replica count |
| `MC_STORE_RS_ROUTE_TOPK` | Python wrapper setup fallback | WRH route-authority fanout; must be `>= 2` |
| `MC_STORE_RS_ROUTE_CONTROL` | Python wrapper setup fallback | route control mode, usually `embedded_wrh` |
| `MC_STORE_RS_TRANSPORT_METADATA_URL` | Python wrapper setup fallback | transport Redis URL when store metadata uses etcd |
| `MC_STORE_RS_GID_INDEX` | compatibility layer, standalone client, Python wrapper, and bench | `classic_te` RDMA GID index override; forwarded to upstream `MC_GID_INDEX` |
| `MC_STORE_RS_TRANSPORT_RPC_PORT` | Python wrapper setup fallback | fixed real data-plane transport port |
| `MC_STORE_RS_LOCAL_SEGMENT_NAME` | Python wrapper setup fallback | explicit local segment name |
| `MC_STORE_RS_EXPIRES_AT_MS` | Python wrapper setup fallback | absolute lease expiry timestamp in milliseconds |
| `MC_STORE_RS_REQUEST_TIMEOUT_MS` | standalone client, Python compatibility runtime, applications | outer per-request deadline for dispatcher requests and routed client operations |
| `MC_STORE_RS_STARTUP_TIMEOUT_MS` | standalone client, Python compatibility runtime, applications | explicit override for compatibility-managed memory registration work; when unset the runtime uses `max(20s, ceil(registration_bytes / 1 GiB))` |
| `MC_STORE_RS_HEARTBEAT_TIMEOUT_MS` | standalone client, Python compatibility runtime, applications | dedicated dispatcher timeout for heartbeat publish |
| `MC_STORE_RS_TRANSFER_STALL_TIMEOUT_MS` | standalone client, Python compatibility runtime, applications | inner transfer stall detector for TENT / classic TE |
| `MC_STORE_RS_TRANSFER_TIMEOUT_MS` | legacy compatibility alias | deprecated alias of `MC_STORE_RS_TRANSFER_STALL_TIMEOUT_MS` |
| `MC_STORE_RS_DUMMY_RPC_TIMEOUT_MS` | dummy compatibility clients | dummy gRPC timeout; falls back to `MC_STORE_RS_REQUEST_TIMEOUT_MS` when unset |
| `MC_STORE_RS_CONTROL_PLANE_THREADS` | standalone client, Python compatibility runtime, applications | worker thread count for the shared control-plane RPC runtime; default `2`; must be `> 0` |
| `MC_STORE_LOCAL_HOT_CACHE_SIZE` | Python compatibility runtime, standalone dummy daemon, and local e2e | total byte budget for the daemon-local hot read cache; unset disables it |
| `MC_STORE_LOCAL_HOT_BLOCK_SIZE` | Python compatibility runtime, standalone dummy daemon, and local e2e | cache block size and maximum cached object size; default `16777216` (`16 MiB`) |
| `MC_STORE_LOCAL_HOT_CACHE_USE_SHM` | standalone dummy daemon, dummy compatibility clients, and local e2e | set to `1` to back cached payloads with shm so dummy clients attached to the same daemon can reuse them |
| `MC_STORE_RS_TRACE` | Python wrapper setup fallback, e2e, and applications | enable tracing initialization from env |
| `MC_STORE_RS_TRACE_FILTER` | e2e and applications | `tracing_subscriber` filter string |
| `MC_STORE_RS_TRACE_FILE` | Python real mode, standalone client, e2e, and applications | append Rust tracing logs to this file; also auto-enables Python real-client tracing |
| `MC_BENCH_TRACE_FILE` | `mooncake-store-bench` | append bench tracing logs to this file; bench otherwise logs to `stderr` and does not use `MC_STORE_RS_TRACE_FILE` for its own output |
| `MC_STORE_RS_METRICS_ADDR` | Python wrapper setup fallback, e2e, and applications | bind address for the in-process metrics server |
| `MC_STORE_RS_REDIS_URL` | Rust e2e | metadata Redis URL |
| `MC_STORE_RS_REDIS_PORT` | local scripts and e2e | local Redis port |
| `MC_REDIS_USERNAME` | Redis metadata backends and transport Redis plugins | optional Redis ACL username |
| `MC_REDIS_PASSWORD` | Redis metadata backends and transport Redis plugins | optional Redis password; enables auth when set |
| `MC_STORE_RS_REDIS_CONNECT_TIMEOUT_MS` | Redis metadata backend | connection timeout for sync Redis metadata calls |
| `MC_STORE_RS_REDIS_IO_TIMEOUT_MS` | Redis metadata backend | read/write timeout for sync Redis metadata calls |
| `MC_STORE_RS_REDIS_RETRY_ATTEMPTS` | Redis metadata backend | retry count for transient Redis reconnect / IO failures on readonly and idempotent metadata operations |
| `MC_STORE_RS_REDIS_RETRY_DELAY_MS` | Redis metadata backend | delay between transient Redis retry attempts |
| `MC_STORE_RS_VALUE_SIZE` | Rust e2e | payload size for validation and benchmark loops |
| `MC_STORE_RS_BENCH_ITERS` | Rust e2e and local scripts | benchmark iteration count; default `64` |
| `MC_STORE_RS_PRINT_METRICS` | Rust e2e | print the Prometheus text snapshot at the end of the run |
| `MC_STORE_RS_TENT_REDIS_URL` | Python compatibility layer | Redis URL used by TENT when store metadata is etcd |
| `MC_STORE_USE_HUGEPAGE` | local memory and Python shm allocator | enable hugepage-backed allocation |
| `MC_STORE_HUGEPAGE_SIZE` | local memory and Python shm allocator | hugepage size; `2MB` or `1GB` |
| `MOONCAKE_UPSTREAM_DIR` | local scripts | upstream Mooncake source tree |
| `MOONCAKE_UPSTREAM_BUILD_DIR` | local scripts | upstream Mooncake build output tree |

`mooncake-store-bench` reuses `MC_STORE_RS_TRACE_FILTER` for level control, but
it has its own trace-file surface. Use `MC_BENCH_TRACE_FILE` for bench logs and
keep `MC_STORE_RS_TRACE_FILE` for standalone-client or Python real-client
logging.

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

Exporter families:

- `mooncake_store_operation_total`, `mooncake_store_operation_bytes_in_total`, `mooncake_store_operation_bytes_out_total`, `mooncake_store_operation_latency_microseconds_total`, `mooncake_store_operation_latency_microseconds_max`
- `mooncake_store_request_total`, `mooncake_store_request_inflight`, `mooncake_store_request_bytes_total`
- `mooncake_store_request_duration_seconds_bucket`
- `mooncake_store_segment_capacity_bytes`, `mooncake_store_segment_used_bytes`
- `mooncake_store_runtime_status`, `mooncake_store_runtime_lease_expires_at_ms`
- `mooncake_store_route_cas_total`
- `mooncake_store_segment_lifecycle_total`, `mooncake_store_eviction_total`, `mooncake_store_eviction_duration_seconds_bucket`
- `mooncake_store_transport_bytes_total`, `mooncake_store_rebalance_routes_total`, `mooncake_store_rebalance_bytes_total`
- `mooncake_store_tenant_quota_reservation_total`, `mooncake_store_tenant_quota_finalize_total`, `mooncake_store_tenant_quota_abort_total`, `mooncake_store_tenant_quota_reconcile_total`
- `mooncake_store_heartbeat_consecutive_failures`, `mooncake_store_heartbeat_last_success_ms`
- `mooncake_store_membership_refresh_total`, `mooncake_store_membership_refresh_duration_seconds`
- `mooncake_store_checksum_validation_total`, `mooncake_store_replication_publish_duration_seconds`
- `process_cpu_seconds_total`, `process_resident_memory_bytes`, `process_open_fds`

All metrics use the `mooncake_store_` prefix to reflect the store cluster perspective. Each runtime instance exports its own view through the `/metrics` endpoint regardless of its role (storage node or routed client).

Recommended recording queries:

```promql
rate(mooncake_store_request_total{operation="get",result="error"}[5m])
histogram_quantile(0.99, sum by (le, operation) (rate(mooncake_store_request_duration_seconds_bucket[5m])))
mooncake_store_segment_used_bytes / mooncake_store_segment_capacity_bytes
```

Infrastructure split:

- process-scoped facts come from this crate
- host CPU, disk, and network should come from `node_exporter` or `cAdvisor`

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
- start the successor with the same `stable_id`; the metadata backend allocates the next epoch atomically
- use `expand_local_memory`, `drain_segment`, `retire_segment`, and `evacuate_owned_replicas`
