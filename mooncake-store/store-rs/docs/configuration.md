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

Redis-backed and etcd-backed deployments support explicit stale segment maintenance.
The same admin server can also run tenant-quota reservation reconcile for an explicit tenant list.

Relevant surfaces:

- `mooncake-store-admin cleanup-stale-segments`
- `mooncake-store-admin server --cleanup-interval-ms <ms> --cleanup-batch-size <n>`
- `mooncake-store-admin server --quota-reconcile-interval-ms <ms> --quota-reconcile-tenant <tenant>`

Operational model:

- the default metadata keyspace is `mc/store-rs/v2`; `v2` is the Redis schema boundary for
  TTL-backed client resource hashes and intentionally does not read or migrate old `v1` keys
- Redis client resource hashes use TTL as the liveness signal
- Redis stores the lease and owned segment records in that single client resource hash; when the lease TTL expires, Redis removes both the lease and the segment records
- etcd lease keys do not expire automatically; admin re-checks the stored `expires_at_ms` field when due work arrives
- etcd segment keys do not use TTL, but they are stored under the owning client runtime namespace
- lease publish and heartbeat refresh now also update a backend-native expiry work index under the same metadata keyspace: Redis uses one sorted set, etcd uses `by-runtime` + lexicographically ordered `by-time` keys
- the admin maintenance loop consumes due entries from that work index, re-checks lease liveness, and removes owner-scoped resource metadata only when the backend still has stale state to repair
- same-epoch lease reclaim after an expired lease key is allowed when that epoch is still the historical HWM and no higher live epoch exists, so heartbeat repair does not fail with `StaleEpoch` after a TTL gap
- tenant quota reconcile is intentionally opt-in per tenant because the current metadata model has no bounded global tenant-work index for the admin plane to consume safely

Server maintenance defaults:

- `--cleanup-interval-ms 5000`
- `--cleanup-batch-size 128`
- `--cleanup-interval-ms 0` disables the background maintenance worker
- `--quota-reconcile-interval-ms 0` disables background tenant quota reconcile
- `--quota-reconcile-tenant <tenant>` can be repeated; when unset, no tenant quota reconcile worker is started

`mooncake-store-admin` accepts every CLI parameter through an environment
variable as a fallback. Explicit CLI flags override environment values.

Global admin knobs:

| CLI flag | Environment variable | Default | Meaning |
|----------|----------------------|---------|---------|
| `--metadata-url` (alias `--metadata_url`) | `MC_STORE_RS_METADATA_URL` | required | Store-RS metadata URL (`redis://...` or `etcd://...`). Same naming as `mooncake-store-client` / `mooncake-store-bench`. |
| `--admin-url` | `MC_STORE_ADMIN_URL` | command-dependent | admin HTTP endpoint used by route-migration client commands |
| `--keyspace` | `MC_STORE_RS_KEYSPACE` | default keyspace | metadata keyspace |
| `--trace-filter` | `MC_STORE_ADMIN_TRACE_FILTER` | tracing default | tracing filter for the admin process |

Server and migration knobs:

| CLI flag | Environment variable | Default | Meaning |
|----------|----------------------|---------|---------|
| `server --bind-addr` | `MC_STORE_ADMIN_BIND_ADDR` | `127.0.0.1:0` | admin HTTP bind address |
| `server --cleanup-interval-ms` | `MC_STORE_ADMIN_CLEANUP_INTERVAL_MS` | `5000` | stale-segment maintenance interval; `0` disables it |
| `server --cleanup-batch-size` | `MC_STORE_ADMIN_CLEANUP_BATCH_SIZE` | `128` | stale-segment cleanup batch size |
| `server --quota-reconcile-interval-ms` | `MC_STORE_ADMIN_QUOTA_RECONCILE_INTERVAL_MS` | `0` | tenant-quota reconcile interval; `0` disables it |
| `server --quota-reconcile-tenant` | `MC_STORE_ADMIN_QUOTA_RECONCILE_TENANTS` | none | comma-separated tenants for background quota reconcile |
| `migrate --authority` | `MC_STORE_ADMIN_AUTHORITY` | required | route authority runtime id |
| `migrate --tenant` | `MC_STORE_ADMIN_TENANT` | required | tenant scope |
| `migrate --domain` | `MC_STORE_ADMIN_DOMAIN` | none | optional domain scope |
| `migrate --object-set` | `MC_STORE_ADMIN_OBJECT_SET` | none | optional object-set scope |
| `migrate --key` | `MC_STORE_ADMIN_KEY` | required | logical object key |
| `migrate --source-segment` | `MC_STORE_ADMIN_SOURCE_SEGMENT` | required | source segment for route migration |
| `migrate copy --target-segment` | `MC_STORE_ADMIN_TARGET_SEGMENTS` | required | comma-separated copy target segments |
| `migrate move --target-segment` | `MC_STORE_ADMIN_TARGET_SEGMENT` | required | move target segment |
| `migrate --task-executor` | `MC_STORE_ADMIN_TASK_EXECUTOR` | required | executor runtime id |
| `migrate --max-retries` | `MC_STORE_ADMIN_MIGRATION_TASK_MAX_RETRIES` | admin queue default | per-task retry override |
| `migrate task get --task-id` | `MC_STORE_ADMIN_TASK_ID` | required | route-migration task id |

Policy and quota knobs:

| CLI flag | Environment variable | Default | Meaning |
|----------|----------------------|---------|---------|
| `policy --tenant` / `quota --tenant` | `MC_STORE_ADMIN_TENANT` | command-dependent | tenant scope |
| `policy --domain` / `quota --domain` | `MC_STORE_ADMIN_DOMAIN` | none | optional domain scope |
| `policy --object-set` / `quota --object-set` | `MC_STORE_ADMIN_OBJECT_SET` | none | optional object-set scope |
| `policy get --effective` | `MC_STORE_ADMIN_EFFECTIVE` | `false` | resolve effective policy fallback chain |
| `policy set/delete --expected-version` | `MC_STORE_ADMIN_EXPECTED_VERSION` | none | optimistic policy version guard |
| `policy set --updated-by` | `MC_STORE_ADMIN_UPDATED_BY` | `admin` | policy audit author |
| `policy list --tenant` | `MC_STORE_ADMIN_TENANT` | none | optional tenant filter |
| `policy set --route-topk` | `MC_STORE_ADMIN_ROUTE_TOPK` | unchanged | WRH route-authority fanout |
| `policy set --route-control` | `MC_STORE_ADMIN_ROUTE_CONTROL` | unchanged | route control mode |
| `policy set --max-bytes` | `MC_STORE_ADMIN_MAX_BYTES` | unchanged | tenant quota byte limit |
| `policy set --max-objects` | `MC_STORE_ADMIN_MAX_OBJECTS` | unchanged | tenant quota object limit |
| `policy set --max-remote-batch-items-per-tenant` | `MC_STORE_ADMIN_MAX_REMOTE_BATCH_ITEMS_PER_TENANT` | unchanged | routed batch per-tenant item cap |
| `policy set --max-remote-batch-bytes` | `MC_STORE_ADMIN_MAX_REMOTE_BATCH_BYTES` | unchanged | routed batch byte cap |
| `policy set --max-remote-batch-burst-items` | `MC_STORE_ADMIN_MAX_REMOTE_BATCH_BURST_ITEMS` | unchanged | routed batch burst item cap |
| `policy set --max-inflight-bytes-per-batch` | `MC_STORE_ADMIN_MAX_INFLIGHT_BYTES_PER_BATCH` | unchanged | per-batch inflight byte cap |
| `policy set --default-replica-count` | `MC_STORE_ADMIN_DEFAULT_REPLICA_COUNT` | unchanged | default placement replica count |
| `policy set --prefer-local` | `MC_STORE_ADMIN_PREFER_LOCAL` | unchanged | default local placement preference |
| `policy set --prefer-alloc-in-same-node` | `MC_STORE_ADMIN_PREFER_ALLOC_IN_SAME_NODE` | unchanged | default same-node allocation preference |
| `policy set --preferred-storage-owners` | `MC_STORE_ADMIN_PREFERRED_STORAGE_OWNERS` | unchanged | comma-separated preferred storage owners |
| `policy set --preferred-segments` | `MC_STORE_ADMIN_PREFERRED_SEGMENTS` | unchanged | comma-separated preferred segments |
| `quota object --key` | `MC_STORE_ADMIN_KEY` | required | logical object key |
| `quota reservations --state` | `MC_STORE_ADMIN_RESERVATION_STATE` | none | reservation state filter |
| `quota abort --reservation-id` | `MC_STORE_ADMIN_RESERVATION_ID` | required | quota reservation id |
| `quota abort/reconcile --dry-run` | `MC_STORE_ADMIN_DRY_RUN` | `false` | inspect without changing metadata |

Boolean admin environment variables use falsey parsing: `0`, `false`, `no`,
and `off` disable the flag; other non-empty values enable it.

## Admin route-migration queue

The standalone admin HTTP server keeps explicit route-migration tasks in process memory and retries transient executor failures automatically while the server remains alive.

Environment knobs:

| Variable | Default | Meaning |
|----------|---------|---------|
| `MC_STORE_ADMIN_MIGRATION_MAX_RETRIES` | `5` | default retry budget for tasks that do not override `max_retries` |
| `MC_STORE_ADMIN_MIGRATION_RETRY_BASE_DELAY_MS` | `3000` | base delay used by the admin retry backoff |
| `MC_STORE_ADMIN_MIGRATION_RETRY_MAX_DELAY_MS` | `30000` | maximum delay cap for the admin retry backoff |
| `MC_STORE_ADMIN_MIGRATION_POLL_INTERVAL_MS` | `100` | background admin polling interval for task dispatch and executor status refresh |

Notes:

- these knobs are read by `mooncake-store-admin server` through `AdminService::from_config(...)`
- the same defaults apply to tasks submitted through the long-lived admin HTTP server
- task state is not persisted in metadata, so these settings control a live in-memory queue rather than a durable scheduler

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
- a scratch-only client may still enable `routed_writes`; placement will choose live `storage=true` runtimes and will not use stale segment metadata as a candidate source

Hugepage behavior:

- if `hugepage_enabled` is `Some(true)`, the client allocates native local memory with `MAP_HUGETLB`
- if `hugepage_size_bytes` is set, hugepage mode is implicitly enabled
- if both fields are `None`, the runtime falls back to `MC_STORE_USE_HUGEPAGE` and `MC_STORE_HUGEPAGE_SIZE`

Compatibility entrypoints expose the same background eviction watermarks:

| Surface | High watermark | Low watermark |
|---------|----------------|---------------|
| Standalone client | `--eviction-high-watermark-percent` / `MC_STORE_RS_EVICTION_HIGH_WATERMARK_PERCENT` | `--eviction-low-watermark-percent` / `MC_STORE_RS_EVICTION_LOW_WATERMARK_PERCENT` |
| Python `setup(...)` | `eviction_high_watermark_percent=` or `MC_STORE_RS_EVICTION_HIGH_WATERMARK_PERCENT` | `eviction_low_watermark_percent=` or `MC_STORE_RS_EVICTION_LOW_WATERMARK_PERCENT` |
| Python config dict | `eviction_high_watermark_percent` | `eviction_low_watermark_percent` |
| `mooncake-store-bench` | `--eviction-high-watermark-percent` / `MC_STORE_RS_EVICTION_HIGH_WATERMARK_PERCENT` | `--eviction-low-watermark-percent` / `MC_STORE_RS_EVICTION_LOW_WATERMARK_PERCENT` |

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
- for `classic_te` with Redis-backed transport metadata, Store-RS derives the upstream TE Redis key prefix from the default tenant and forwards `tenants/<tenant>` as the Mooncake metadata cluster id
- for `classic_te` with `P2PHANDSHAKE`, segment metadata keeps the configured logical `segment_name` and also publishes the TE `transport_endpoint` (`ip:rpc_port`) used when peers open the segment
- low-level Rust transport construction remains explicit; runtime backend selection is only a compatibility-layer feature
- current upstream SGLang Mooncake integration does not forward `transport_backend` from `--hicache-storage-backend-extra-config`; use `MC_STORE_RS_TRANSPORT_BACKEND` when SGLang real mode must select `tent` or `classic_te`

Reconnect behavior:

- both backends repair local transport metadata after Redis connectivity returns and the heartbeat repair path runs
- `classic_te` recreates its transport runtime before republishing local buffers
- `tent` also recreates its transport runtime before re-registering the local buffers it still owns
- peer restarts that invalidate a cached remote segment handle are repaired on demand: the read path drops the stale handle, reopens by the segment's transport open name, and only quarantines that runtime if the fresh reopen still fails
- segment metadata lookup starts from live clients. In Redis, segment records share the owner client hash TTL with the lease; in every backend, a leftover segment record whose owner lease is gone is ignored for allocation, preferred-segment resolution, and HTTP remote-runtime resolution.
- routed writes apply the same stale-handle refresh once before escalating to outer soft-pin retry or failover, so a restarted live peer does not poison the cached remote-segment state
- if Redis restarts from an empty dataset, surviving storage clients republish both lease state and segment metadata into their client resource hash during recovery
- local memory registration refreshes the runtime lease for at least 30 seconds before publishing segment metadata, so slow startup registration does not publish into an expired Redis client resource hash
- requests that arrive while Redis is unavailable can still fail fast; recovery is designed for self-healing after metadata service returns, not for serving through a metadata blackout

## Route Control

| Mode | Default | Behavior |
|------|---------|----------|
| `RouteControlMode::EmbeddedWrh` | yes | client-side route authority selection with mirrored top-k publication, ranked authority reads, and a prewarmed background-refreshed membership snapshot |
| `RouteControlMode::MetadataOnly` | no | route reads and writes go directly to metadata |

Use `MetadataOnly` for bring-up and debugging. Use `EmbeddedWrh` for normal deployments.

Embedded route authorities maintain in-memory indexes for scope and reuse-identity lookups. Hot-path existence and prefix-reuse queries resolve through exact route-authority reads instead of scanning the metadata backend or the full local route map. Batched existence checks stop at the mirrored `route_topk` authority set, so common misses do not fan out to every live authority.

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
- when `metadata_url` (arg7) is etcd, the Transfer Engine still needs its own metadata at `transport_metadata_url` (arg2): pass `P2PHANDSHAKE` for `classic_te` (default) or `redis://...` for `tent`

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

## Standalone Client Configuration

`mooncake-store-client run` accepts its runtime knobs either as CLI flags or as
environment variables. Explicit CLI flags override environment values. For
environment-only startup, keep the `run` subcommand because the root command with
no arguments still renders help.

| CLI flag | Environment variable | Default | Meaning |
|----------|----------------------|---------|---------|
| `--local-hostname` | `MOONCAKE_LOCAL_HOSTNAME` | required | hostname or IP published for this runtime |
| `--metadata-url` (alias `--metadata_url`) | `MC_STORE_RS_METADATA_URL` | required | Store-RS metadata URL (`redis://...` or `etcd://...`). |
| `--transport-metadata-url` (alias `--transport_metadata_url`) | `MC_STORE_RS_TRANSPORT_METADATA_URL` | `P2PHANDSHAKE` | Transfer Engine metadata input (`redis://...` or `P2PHANDSHAKE`). Defaults to `P2PHANDSHAKE` (classic_te peer handshake); `tent` requires `redis://...`. |
| `--storage-bytes` | `MC_STORE_RS_STORAGE_BYTES` | `67108864` | local storage capacity published by this runtime |
| `--scratch-bytes` | `MC_STORE_RS_SCRATCH_BYTES` | `4194304` | local scratch capacity for transfer staging |
| `--protocol` | `MOONCAKE_PROTOCOL` | `tcp` | transport protocol such as `tcp` or `rdma` |
| `--rdma-devices` | `MC_STORE_RS_RDMA_DEVICES` | empty | RDMA device list passed through compatibility setup |
| `--transport-rpc-port` / `--rpc-server-port` | `MC_STORE_RS_TRANSPORT_RPC_PORT` | backend chooses | fixed real data-plane transport port |
| `--transport-backend` | `MC_STORE_RS_TRANSPORT_BACKEND` | `classic_te` | real transport backend; accepts `tent`, `classic_te`, `classic-te`, `classic`, or `te` |
| `--stable-id` | `MC_STORE_RS_STABLE_ID` | generated | persistent runtime identity |
| `--initial-state` | `MC_STORE_RS_INITIAL_STATE` | `active` | startup lifecycle state |
| `--tenant` | `MC_STORE_RS_TENANT` | `default` | default tenant scope |
| `--domain` | `MC_STORE_RS_DOMAIN` | `default` | default domain scope for scoped object identity |
| `--object-set` | `MC_STORE_RS_OBJECT_SET` | `default` | default object-set scope for scoped object identity |
| `--label key=value` | `MC_STORE_RS_LABELS` | none | comma-separated runtime labels such as `pool=a,storage=true` |
| `--routed-writes` | `MC_STORE_RS_ROUTED_WRITES` | `false` | enable routed writer mode; env accepts falsey values such as `0`, `false`, `no`, or `off`, and treats other non-empty values as true |
| `--replica-count` | `MC_STORE_RS_REPLICA_COUNT` | `1` | default routed-writer replica count |
| `--route-topk` | `MC_STORE_RS_ROUTE_TOPK` | `2` | WRH route-authority fanout; must be `>= 2` |
| `--keyspace` | `MC_STORE_RS_KEYSPACE` | default keyspace | metadata keyspace |
| `--local-segment-name` | `MC_STORE_RS_LOCAL_SEGMENT_NAME` | generated from transport/runtime | explicit local segment name |
| `--lease-ttl-ms` | `MC_STORE_RS_LEASE_TTL_MS` | `30000` | lease TTL published by the standalone heartbeat loop |
| `--heartbeat-interval-ms` | `MC_STORE_RS_HEARTBEAT_INTERVAL_MS` | `30000`, normalized below TTL | heartbeat loop interval |
| `--request-timeout-ms` | `MC_STORE_RS_REQUEST_TIMEOUT_MS` | `65000` | dispatcher and routed operation timeout |
| `--startup-timeout-ms` | `MC_STORE_RS_STARTUP_TIMEOUT_MS` | adaptive | compatibility-managed registration timeout override |
| `--heartbeat-timeout-ms` | `MC_STORE_RS_HEARTBEAT_TIMEOUT_MS` | `15000` | heartbeat publish timeout |
| `--transfer-stall-timeout-ms` | `MC_STORE_RS_TRANSFER_STALL_TIMEOUT_MS` | `10000` | transfer no-progress timeout |
| `--metrics-addr` | `MC_STORE_RS_METRICS_ADDR` | disabled | in-process metrics bind address |
| `--client-server-address` | `MC_STORE_RS_CLIENT_SERVER_ADDRESS` | disabled | dummy compatibility gRPC server address |
| `--use-hugepage` | `MC_STORE_USE_HUGEPAGE` | disabled | enable hugepage-backed local store memory; standalone env accepts falsey values such as `0`, `false`, `no`, or `off` |
| `--hugepage-size` | `MC_STORE_HUGEPAGE_SIZE` | backend default | hugepage size such as `2M` or `1G` |
| `--trace-filter` | `MC_STORE_RS_TRACE_FILTER` | tracing default | standalone tracing filter |
| `--route-control` | `MC_STORE_RS_ROUTE_CONTROL` | `embedded_wrh` | route-control fallback; accepts `embedded_wrh` / `embedded-wrh` or `metadata_only` / `metadata-only` |
| `--drain-on-exit` | `MC_STORE_RS_DRAIN_ON_EXIT` | `false` | drain owned routes during graceful shutdown; env accepts falsey values such as `0`, `false`, `no`, or `off`, and treats other non-empty values as true |

`mooncake-store-client stats` also supports `MC_STORE_RS_STATS_SERVER` for
`--server` and `MC_STORE_RS_STATS_JSON` for `--json`; the JSON switch uses the
same falsey-value parsing as the standalone runtime boolean flags.

## Python Compatibility Configuration

`MooncakeDistributedStore.setup(...)` accepts the core store knobs plus Python-specific convenience parameters.

Important Python-only compatibility knobs:

`tenant`, `domain`, and `object_set` form the default namespace scope used by Python compatibility read/write operations when a request does not provide a more specific scope. `route_topk` and `route_control` are kept for compatibility, but admin-managed tenant policy in metadata is the preferred place to author tenant-scoped routing policy.


| Parameter | Meaning |
|-----------|---------|
| `stable_id` | persistent client identity |
| `tenant` | default tenant scope |
| `domain` | default domain scope; falls back to `default` when omitted or empty |
| `object_set` | default object-set scope; falls back to `default` when omitted or empty |
| `labels` | lease labels such as `pool` and `storage` |
| `routed_writes` | enable routed placement from Python |
| `replica_count` | default replica count when routed writes are enabled |
| `route_topk` | WRH route-authority fanout; must match the policy already stored in the metadata keyspace |
| `transport_backend` | choose `tent` or `classic_te` for the real transport runtime |
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

### Setup positional layout

`setup(local_hostname, transport_metadata_url, global_segment_size, local_buffer_size, protocol, rdma_devices, metadata_url)`.

- **`transport_metadata_url`** — Transfer Engine metadata input. Accepts `redis://...` or `P2PHANDSHAKE`. **Default is `P2PHANDSHAKE`** wherever a default is expressible: the Python dict-form (the `MC_STORE_RS_TRANSPORT_METADATA_URL` env is honored as fallback before the default), the standalone CLI (no `--transport-metadata-url` flag and no env set), and the bench. The Python positional `setup(...)` requires it explicitly because Python disallows a defaulted positional before a required positional; pass the literal string `"P2PHANDSHAKE"` to use the default. The dict-form also accepts the upstream Mooncake key `metadata_server` as an alias.
- **`metadata_url`** — Store-RS metadata URL. Required. Accepts `redis://...` or `etcd://...`. The dict-form `setup({...})` also accepts the upstream Mooncake keys `master_server`, `master_server_addr`, and `master_server_address` interchangeably as aliases; `setup()` raises a clear `TypeError` if none is provided.

`transport_metadata_url` is for the Transfer Engine only and never influences Store-RS routing or metadata decisions.

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

Control-plane RPC clients run on a dedicated shared Tokio runtime instead of a
single global caller lock. Use `MC_STORE_RS_CONTROL_PLANE_THREADS` to tune the
client worker count when a deployment needs more concurrent route / allocator
RPCs or when an embedded client should keep its thread footprint smaller. The
default is `2`; values must be positive integers.

Each runtime also serves peer control-plane requests on a separate multi-thread
Tokio runtime. Use `MC_STORE_RS_CONTROL_PLANE_SERVER_THREADS` to tune the
server worker count when routed writers create high allocator / route-authority
fanout. The default is `4`; values must be positive integers.

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
- Python compatibility runtimes partition local entries by effective namespace scope, including `tenant`, `domain`, and `object_set`, so different object sets do not reuse the same cached value even inside one process
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
| `MOONCAKE_LOCAL_HOSTNAME` | standalone client and bench | hostname or IP published by the runtime |
| `MOONCAKE_PROTOCOL` | standalone client and bench | transport protocol such as `tcp` or `rdma` |
| `MC_STORE_RS_TRANSPORT_BACKEND` | compatibility layer, standalone client, Python wrapper | select `tent` or `classic_te` as the default real transport backend |
| `MC_STORE_RS_METADATA_URL` | standalone client, standalone admin, and bench | Store-RS metadata URL. Backs `--metadata-url` on `mooncake-store-client`, `mooncake-store-admin`, and `mooncake-store-bench`. The Python wrapper `setup({...})` dict-form also honors it as a fallback when neither `metadata_url` nor any of the upstream aliases (`master_server` / `master_server_addr` / `master_server_address`) is provided. |
| `MC_STORE_RS_TRANSPORT_METADATA_URL` | standalone client, bench, Python wrapper dict-form fallback | Transfer Engine metadata input. Backs `--transport-metadata-url` on `mooncake-store-client` and `mooncake-store-bench`. The Python wrapper `setup({...})` dict-form honors it as a fallback when neither `transport_metadata_url` nor the upstream alias `metadata_server` is provided (default value `P2PHANDSHAKE`). |
| `MC_STORE_RS_STORAGE_BYTES` | standalone client | local storage bytes for `mooncake-store-client run` |
| `MC_STORE_RS_SCRATCH_BYTES` | standalone client and bench | local scratch bytes for compatibility-managed clients |
| `MC_STORE_RS_RDMA_DEVICES` | standalone client and Rust e2e | RDMA device list |
| `MC_STORE_RS_KEYSPACE` | standalone client, standalone admin, and Python wrapper setup fallback | metadata keyspace used when SGLang cannot pass `keyspace`; this also defines Python compatibility read/write visibility and local hot-cache partitioning |
| `MC_STORE_RS_STABLE_ID` | standalone client and Python wrapper setup fallback | stable client id used when SGLang cannot pass `stable_id` |
| `MC_STORE_RS_INITIAL_STATE` | standalone client and Python wrapper setup fallback | initial lifecycle state, for example `active`, `standby`, `draining`, or `offline` |
| `MC_STORE_RS_TENANT` | standalone client, Python wrapper setup fallback, and bench | default tenant used when SGLang cannot pass `tenant` |
| `MC_STORE_RS_DOMAIN` | standalone client and Python wrapper setup fallback | default domain used when SGLang cannot pass `domain` |
| `MC_STORE_RS_OBJECT_SET` | standalone client and Python wrapper setup fallback | default object set used when SGLang cannot pass `object_set`; treated as an opaque namespace component |
| `MC_STORE_RS_LABELS` | standalone client and Python wrapper setup fallback | standalone labels as comma-separated `key=value` pairs; Python also accepts a JSON object |
| `MC_STORE_RS_ROUTED_WRITES` | standalone client and Python wrapper setup fallback | enable routed writer mode when set to `1`, `true`, `yes`, or `on`; standalone also treats `0`, `false`, `no`, or `off` as false |
| `MC_STORE_RS_REPLICA_COUNT` | standalone client, Python wrapper setup fallback, and bench | default routed-writer replica count |
| `MC_STORE_RS_ROUTE_TOPK` | standalone client, Python wrapper setup fallback, and bench | WRH route-authority fanout; must be `>= 2` |
| `MC_STORE_RS_ROUTE_CONTROL` | standalone client, Python wrapper setup fallback, and bench | route control mode, usually `embedded_wrh` |
| `MC_STORE_RS_GID_INDEX` | compatibility layer, standalone client, Python wrapper, and bench | `classic_te` RDMA GID index override; forwarded to upstream `MC_GID_INDEX` |
| `MC_STORE_RS_TRANSPORT_RPC_PORT` | standalone client and Python wrapper setup fallback | fixed real data-plane transport port |
| `MC_STORE_RS_LOCAL_SEGMENT_NAME` | standalone client and Python wrapper setup fallback | explicit local segment name |
| `MC_STORE_RS_EXPIRES_AT_MS` | Python wrapper setup fallback | absolute lease expiry timestamp in milliseconds |
| `MC_STORE_RS_LEASE_TTL_MS` | standalone client | lease TTL for the standalone heartbeat loop |
| `MC_STORE_RS_HEARTBEAT_INTERVAL_MS` | standalone client | heartbeat interval for the standalone heartbeat loop |
| `MC_STORE_RS_REQUEST_TIMEOUT_MS` | standalone client, Python compatibility runtime, applications | outer per-request deadline for dispatcher requests and routed client operations |
| `MC_STORE_RS_STARTUP_TIMEOUT_MS` | standalone client, Python compatibility runtime, applications | explicit override for compatibility-managed memory registration work; when unset the runtime uses `max(20s, ceil(registration_bytes / 1 GiB))` |
| `MC_STORE_RS_HEARTBEAT_TIMEOUT_MS` | standalone client, Python compatibility runtime, applications | dedicated dispatcher timeout for heartbeat publish |
| `MC_STORE_RS_TRANSFER_STALL_TIMEOUT_MS` | standalone client, Python compatibility runtime, applications | inner transfer stall detector for TENT / classic TE |
| `MC_STORE_RS_TRANSFER_TIMEOUT_MS` | legacy compatibility alias | deprecated alias of `MC_STORE_RS_TRANSFER_STALL_TIMEOUT_MS` |
| `MC_STORE_RS_DUMMY_RPC_TIMEOUT_MS` | dummy compatibility clients | dummy gRPC timeout; falls back to `MC_STORE_RS_REQUEST_TIMEOUT_MS` when unset |
| `MC_STORE_RS_CLIENT_SERVER_ADDRESS` | standalone client | dummy compatibility gRPC server address |
| `MC_STORE_RS_DRAIN_ON_EXIT` | standalone client | drain owned routes during graceful shutdown; accepts falsey values such as `0`, `false`, `no`, or `off` |
| `MC_STORE_RS_STATS_SERVER` | standalone client stats command | server address used by `mooncake-store-client stats --server` |
| `MC_STORE_RS_STATS_JSON` | standalone client stats command | emit compact JSON from the stats command; accepts falsey values such as `0`, `false`, `no`, or `off` |
| `MC_STORE_RS_CONTROL_PLANE_THREADS` | standalone client, Python compatibility runtime, applications | worker thread count for the shared control-plane RPC runtime; default `2`; must be `> 0` |
| `MC_STORE_RS_CONTROL_PLANE_SERVER_THREADS` | standalone client, Python compatibility runtime, applications | worker thread count for the embedded control-plane gRPC server; default `4`; must be `> 0` |
| `MC_STORE_LOCAL_HOT_CACHE_SIZE` | Python compatibility runtime, standalone dummy daemon, and local e2e | total byte budget for the daemon-local hot read cache; unset disables it |
| `MC_STORE_LOCAL_HOT_BLOCK_SIZE` | Python compatibility runtime, standalone dummy daemon, and local e2e | cache block size and maximum cached object size; default `16777216` (`16 MiB`) |
| `MC_STORE_LOCAL_HOT_CACHE_USE_SHM` | standalone dummy daemon, dummy compatibility clients, and local e2e | set to `1` to back cached payloads with shm so dummy clients attached to the same daemon can reuse them |
| `MC_STORE_RS_TRACE` | Python wrapper setup fallback, e2e, and applications | enable tracing initialization from env |
| `MC_STORE_RS_TRACE_FILTER` | standalone client, Python wrapper setup fallback, e2e, and applications | `tracing_subscriber` filter string; `--trace-filter` overrides it for the standalone client |
| `MC_STORE_RS_TRACE_FILE` | Python real mode, standalone client, e2e, and applications | append Rust tracing logs to this file; also auto-enables Python real-client tracing |
| `MC_STORE_RS_TRACE_SPAN_EVENTS` | standalone client, Python real mode, e2e, and applications | tracing span lifecycle events; unset suppresses synthetic span close lines, `close` enables operation close timing logs |
| `MC_BENCH_TRACE_FILE` | `mooncake-store-bench` | append bench tracing logs to this file; bench otherwise logs to `stderr` and does not use `MC_STORE_RS_TRACE_FILE` for its own output |
| `MC_BENCH_INTERFACES` | `mooncake-store-bench` | combined write/read interface selector; accepts `<write>,<read>`, `<write>:<read>`, or `write=<...>,read=<...>`; when set it overrides non-CLI interface defaults |
| `MC_BENCH_WRITE_INTERFACE` | `mooncake-store-bench` | measured write-side bench API; `put`, `batch_put`, or `batch_put_from`; default `batch_put_from` |
| `MC_BENCH_READ_INTERFACE` | `mooncake-store-bench` | measured read-side bench API; `get`, `batch_get`, or `batch_get_into`; default `batch_get_into` |
| `MC_STORE_RS_METRICS_ADDR` | Python wrapper setup fallback, e2e, and applications | bind address for the in-process metrics server |
| `MC_STORE_RS_REDIS_URL` | Rust e2e | metadata Redis URL |
| `MC_STORE_RS_REDIS_PORT` | local scripts and e2e | local Redis port |
| `MC_REDIS_USERNAME` | Redis metadata backends and transport Redis plugins | optional Redis ACL username |
| `MC_REDIS_PASSWORD` | Redis metadata backends and transport Redis plugins | optional Redis password; enables auth when set |
| `MC_STORE_RS_REDIS_CONNECT_TIMEOUT_MS` | Redis metadata backend | connection timeout for sync Redis metadata calls |
| `MC_STORE_RS_REDIS_IO_TIMEOUT_MS` | Redis metadata backend | read/write timeout for sync Redis metadata calls |
| `MC_STORE_RS_REDIS_CONNECTION_POOL_SIZE` | Redis metadata backend | bounded per-backend Redis connection pool size; default `16`, capped at `256` |
| `MC_STORE_RS_REDIS_RETRY_ATTEMPTS` | Redis metadata backend | retry count for transient Redis reconnect / IO failures on readonly and idempotent metadata operations |
| `MC_STORE_RS_REDIS_RETRY_DELAY_MS` | Redis metadata backend | delay between transient Redis retry attempts |
| `MC_STORE_RS_VALUE_SIZE` | Rust e2e | payload size for validation and benchmark loops |
| `MC_STORE_RS_BENCH_ITERS` | Rust e2e and local scripts | benchmark iteration count; default `64` |
| `MC_STORE_RS_PRINT_METRICS` | Rust e2e | print the Prometheus text snapshot at the end of the run |
| `MC_STORE_USE_HUGEPAGE` | local memory, standalone client, and Python shm allocator | enable hugepage-backed allocation; standalone client treats `0`, `false`, `no`, or `off` as an explicit disable |
| `MC_STORE_HUGEPAGE_SIZE` | local memory and Python shm allocator | hugepage size; `2MB` or `1GB` |
| `MOONCAKE_UPSTREAM_DIR` | local scripts | upstream Mooncake source tree |
| `MOONCAKE_UPSTREAM_BUILD_DIR` | local scripts | upstream Mooncake build output tree |

`mooncake-store-bench` reuses `MC_STORE_RS_TRACE_FILTER` for level control, but
it has its own trace-file surface. Use `MC_BENCH_TRACE_FILE` for bench logs and
keep `MC_STORE_RS_TRACE_FILE` for standalone-client or Python real-client
logging.

Standalone-client heartbeat success refreshes are trace-level routine events.
Heartbeat recovery and failure messages remain explicit stderr output.
Standalone-client local segment publication is an info-level startup registration event.
Background storage-owner eviction completion is an info-level aggregate state change.
Standalone-client startup emits one debug-level state snapshot after ready.
Debug output keeps bounded per-key summary samples.
Trace output keeps full per-key route and eviction details.
Control-plane stream open events are trace-level connection churn.

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
- `mooncake_store_metadata_operation_total`, `mooncake_store_metadata_operation_inflight`, `mooncake_store_metadata_operation_duration_seconds_bucket`
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

All Store-RS and process metrics exported by this endpoint include `tenant="<default tenant>"`, where the value is the process-bound tenant selected by `StoreClientBuilder::tenant(...)` or the corresponding compatibility-layer startup option. All Store-RS metric families use the `mooncake_store_` prefix to reflect the store cluster perspective. Each runtime instance exports its own view through the `/metrics` endpoint regardless of its role (storage node or routed client).

Sparse operational counter families emit zero-valued baseline series for their known label set. This makes steady-state dashboards report an explicit zero for tenant quota, tenant-local eviction, preferred-segment skip, rebalance, and segment lifecycle activity until the corresponding real event occurs and increments the counter.

Metadata backend metrics are recorded around Store-RS calls into the configured `MetadataBackend` and are labeled by `backend`, `operation`, and `result`. They show whether Store-RS is waiting on or failing calls to Redis, etcd, or another metadata implementation; they are not a replacement for Redis or etcd server-internal exporters.

Storage and route-authority clients also record control-plane-derived traffic: route CAS handlers observe replication publish latency, replica-route tracking records storage-owner write bytes, and route-hit reports record storage-owner read bytes plus successful checksum-validated hits. Scratch-only benchmark workers therefore do not need to expose their own metrics endpoint for the long-running store clients to show benchmark traffic.

Recommended recording queries:

```promql
rate(mooncake_store_request_total{tenant="$tenant",operation="get",result="error"}[5m])
histogram_quantile(0.99, sum by (le, tenant, operation) (rate(mooncake_store_request_duration_seconds_bucket{tenant="$tenant"}[5m])))
rate(mooncake_store_metadata_operation_total{tenant="$tenant",backend=~"redis|etcd",result!="ok"}[5m])
histogram_quantile(0.99, sum by (le, tenant, backend, operation) (rate(mooncake_store_metadata_operation_duration_seconds_bucket{tenant="$tenant"}[5m])))
mooncake_store_segment_used_bytes{tenant="$tenant"} / mooncake_store_segment_capacity_bytes{tenant="$tenant"}
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
