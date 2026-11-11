# Store-RS Deployment Guide

This document explains how to run `mooncake-store-rs` locally and how to map the runtime to common deployment roles.

## Recommended Tenant Policy Workflow

For tenant-scoped routing and resource policy, prefer this operator workflow:

1. write tenant policy through `mooncake-store-rs-admin policy ...`
2. launch runtimes with tenant identity plus transport/memory configuration
3. let Store-RS resolve and enforce the effective policy from metadata at bootstrap and on request paths

Runtime-local CLI, Python, and environment route/resource knobs remain available as compatibility fallbacks, but they are not the preferred long-term policy authoring surface.

## Requirements

- Rust toolchain when building the optional Store-RS wheel component
- `cmake` and a C++ toolchain
- `redis-server` and `redis-cli`
- Git submodule support for native dependencies
- Python 3, if you want to run the Python compatibility layer

## Prepare the Repository

Set the Store-RS source, Mooncake source, and CMake build paths explicitly. Each
path must be absolute. The top-level CMake project builds Transfer Engine,
TENT, the Store-RS native shims, the Python extension, and the three command
bins when `WITH_STORE_RS=ON`:

```bash
export MOONCAKE_STORE_RS_DIR=/path/to/Mooncake/mooncake-store-rs
export MOONCAKE_ROOT_DIR=/path/to/Mooncake
export MOONCAKE_BUILD_DIR=/path/to/Mooncake-build
cmake -S "${MOONCAKE_ROOT_DIR}" -B "${MOONCAKE_BUILD_DIR}" \
  -DWITH_TE=ON -DUSE_TENT=ON -DWITH_STORE=OFF -DWITH_STORE_RUST=OFF \
  -DWITH_STORE_RS=ON
cmake --build "${MOONCAKE_BUILD_DIR}" --target build_store_rs
```

The default build leaves `WITH_STORE_RS=OFF` and does not require Cargo. CMake
owns the TE/TENT shim targets; `mooncake-store-rs-transport-sys` validates and
links to those explicit build-tree artifacts.

## Large-Memory Classic RDMA Bring-Up

For `classic_te` deployments on RDMA hosts with very large local storage:

- keep `LocalMemoryConfig::numa_aware(true)` unless you have a measured reason to collapse registration onto one CPU location
- expect startup registration to fan out into multiple initial storage segments only on transports that opt into parallel startup registration; today that means `classic_te` + RDMA
- the runtime now pre-touches startup storage automatically before RDMA MR registration once the total startup storage registration volume reaches `4 GiB`
- metadata publication happens after the local startup registration phase finishes, so operators should treat the segment set as appearing in one startup wave rather than one segment at a time

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

Operational behavior:

- `storage=true` opts the client into routed placement candidate sets
- the same label also enables local storage-owner CLOCK eviction when reservation pressure appears
- the same label enables background watermark eviction when local storage is configured
- writers publish remote replica routes back to the storage owner automatically, so eviction does not need metadata scans on the hot path
- `storage=true` requires `storage_bytes > 0`

### Routed writer

A routed writer accepts write requests and places replicas onto storage nodes selected by `PlacementPlanner`.

Typical settings:

- `label("storage", "false")`
- `routed_writes(planner, replica_count)`
- local memory for scratch space and optional local placement

Operational behavior:

- routed writers can run with `storage_bytes=0` in inference/storage split deployments
- when they do not own storage, they do not run local CLOCK eviction
- remote placement still benefits from remote storage-owner eviction and route tracking
- when `storage_bytes=0` and no explicit storage label is provided, the runtime normalizes the label to `storage=false`

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
- route policy and handoff plans
- object routes in `MetadataOnly` deployments
- handoff plans

Redis metadata supports two authentication forms:

- set `MC_REDIS_PASSWORD` for password-only Redis deployments
- set both `MC_REDIS_USERNAME` and `MC_REDIS_PASSWORD` for Redis ACL users

Credentials embedded in `redis://username:password@host:port/db` also work and take precedence over the environment variables. Prefer environment variables for cloud Redis passwords or any password containing URL-reserved characters.

### Clean stale segment registrations

Redis client resource hashes expire automatically. The Redis lease payload and owned
segment records live in the same hash, so the lease TTL removes both. etcd client leases
do not expire automatically; admin relies on the stored `expires_at_ms` field plus a
backend work index, and etcd segment registration keys remain owner-scoped explicit keys.

That means a hard-killed etcd-backed storage client can leave stale `segments/...`
metadata and owner-scoped segment bookkeeping behind even after its lease has disappeared.
Redis can also need a repair sweep after abnormal metadata edits or legacy state. Strict
tenant quota reservations can outlive a killed writer until an explicit reconcile repairs
the state. Store-RS supports one-shot repair plus stateless background maintenance in the
same admin binary:

- a one-shot sweep through `cleanup-stale-segments`
- a stateless background maintenance loop in `mooncake-store-rs-admin server`
- optional tenant quota reservation reconcile for an explicit tenant list in that same admin process

Run the admin sweep when you want to remove dead-owner segment metadata. The same shape
works with either `redis://...` or `etcd://...` metadata URLs:

```bash
mooncake-store-rs-admin \
  --metadata-url redis://127.0.0.1:6380/0 \
  cleanup-stale-segments
```

Run the stateless admin container shape when cleanup should happen continuously:

```bash
mooncake-store-rs-admin \
  --metadata-url redis://127.0.0.1:6380/0 \
  server \
  --bind-addr 0.0.0.0:8080 \
  --cleanup-interval-ms 5000 \
  --cleanup-batch-size 128 \
  --quota-reconcile-interval-ms 10000 \
  --quota-reconcile-tenant tenant-a \
  --quota-reconcile-tenant tenant-b
```

The background maintenance loop works like this:

- lease create and heartbeat refresh both update a backend-native lease-expiry work index
- Redis stores due work in one sorted set; etcd stores a `by-runtime` pointer plus a lexicographically ordered `by-time` queue
- the admin worker polls due entries from that backend-native work index
- each due owner is re-checked against the live lease key before cleanup
- dead-owner segment deletion stays owner-scoped instead of falling back to a hidden full keyspace walk in the steady-state worker
- if a lease key disappeared briefly, same-epoch reclaim is accepted as long as that epoch is still the stable-id HWM and no higher live epoch exists
- tenant quota reconcile remains explicit: the worker only runs for tenants named on the command line, then internally reuses the same repair logic as `mooncake-store-rs-admin quota reconcile`

This keeps the admin pod stateless. If the pod restarts, the next reconcile loop resumes from Redis or etcd metadata instead of relying on in-memory work queues.

You can also manage tenant route policy and inspect strict-quota metadata through the same binary:

```bash
mooncake-store-rs-admin \
  --metadata-url redis://127.0.0.1:6380/0 \
  policy list

mooncake-store-rs-admin \
  --metadata-url redis://127.0.0.1:6380/0 \
  policy list \
  --tenant tenant-a

mooncake-store-rs-admin \
  --metadata-url redis://127.0.0.1:6380/0 \
  policy set \
  --tenant tenant-a \
  --route-topk 3 \
  --route-control embedded-wrh

mooncake-store-rs-admin \
  --metadata-url redis://127.0.0.1:6380/0 \
  quota state \
  --tenant tenant-a

mooncake-store-rs-admin \
  --metadata-url redis://127.0.0.1:6380/0 \
  quota reservations \
  --tenant tenant-a \
  --state pending

mooncake-store-rs-admin \
  --metadata-url redis://127.0.0.1:6380/0 \
  quota reconcile \
  --tenant tenant-a \
  --dry-run
```

Useful options:

- `--keyspace <prefix>` to target a non-default metadata namespace for either policy management or stale cleanup
- `policy list --tenant <tenant>` narrows Redis / etcd metadata reads to that tenant's root policy and nested policy subtree
- `MC_REDIS_USERNAME` / `MC_REDIS_PASSWORD` for Redis ACL authentication
- terminal tenant-quota reservations (`Finalized` / `Aborted`) expire automatically after `24h` by default; if a deployment needs a different retention window, configure `RedisMetadataConfig::tenant_quota_terminal_ttl(...)`
- `server --cleanup-interval-ms 0` to run the admin HTTP surface without the maintenance worker
- `server --quota-reconcile-tenant <tenant>` repeated for each tenant whose strict quota reservations should be repaired automatically

### Local e2e validation

The repository's main end-to-end validation binary is `crates/mooncake-store-e2e/src/main.rs`, and the standard local entrypoint is:

```bash
mooncake-store-rs/scripts/e2e/run-local-e2e.sh
```

For fast local verification while iterating on quota behavior, reduce benchmark noise and disable RDMA probing:

```bash
MC_STORE_RS_ENABLE_RDMA=0 \
MC_STORE_RS_BENCH_ITERS=1 \
mooncake-store-rs/scripts/e2e/run-local-e2e.sh
```

The local e2e now includes a focused strict tenant quota scenario that proves all of the following against Redis-backed metadata state:

- a tenant quota policy is applied before the quota-scoped writer starts
- an admitted write consumes quota and leaves `pending_reserved_* == 0` after finalize
- a second over-limit write is rejected without drifting quota state or reservation count
- authoritative delete refunds quota immediately and makes the same capacity reusable
- object accounting and finalized reservation records are visible in metadata for the test tenant

The command removes:

- stale Redis client resource hashes whose `lease` field is gone but resource fields remain
- stale etcd segment keys owned by clients with no live lease
- stale backend index entries used by the maintenance scheduler

### Transport metadata

The transport layer is selected by the compatibility runtime and then configured through the matching backend config.

Current repository scripts and examples default to `classic_te`; set `MC_STORE_RS_TRANSPORT_BACKEND=tent` only when a deployment explicitly wants TENT.

Runtime selection:

- `MC_STORE_RS_TRANSPORT_BACKEND=tent|classic_te`
- `mooncake-store-rs-client --transport-backend tent|classic-te`
- `MooncakeDistributedStore.setup(..., transport_backend="tent"|"classic_te")`
- `MC_STORE_RS_GID_INDEX=<n>` when `classic_te` over RDMA must pick a non-default RoCE GID index; Store-RS forwards it to upstream `MC_GID_INDEX`

When `metadata_url` (arg7) is an etcd URL, the Transfer Engine still needs its own metadata at `transport_metadata_url` (arg2). For `classic_te`, `P2PHANDSHAKE` is the default; for `tent`, supply a `redis://...` value explicitly.

Port roles stay the same across backends:

- `transport_rpc_port` is the real data-plane TCP port published to peer real clients
- `client_server_address` is the dummy compatibility gRPC port
- `metrics_addr` / `MC_STORE_RS_METRICS_ADDR` is the Prometheus `/metrics` listener

Backend reconnect behavior:

- when Redis metadata connectivity returns, the heartbeat recovery path republishes both store metadata and transport metadata
- `classic_te` repairs fresh Redis restarts by recreating its engine and republishing local buffers
- `tent` repairs fresh Redis restarts by recreating its engine and re-registering the local buffers it still owns
- if a live peer restart leaves callers with a stale cached remote segment handle, reads and routed writes refresh that handle by segment name before treating the peer as dead
- runtime quarantine still applies to true reopen or transfer failures; stale cached handles alone do not demote an otherwise live peer
- the process must remain alive across the outage for automatic recovery; a dead client still needs normal restart and lease takeover semantics

## Routing Modes

### `EmbeddedWrh`

This is the default mode.

- route ownership is selected on the client with weighted rendezvous hashing
- the top `route_topk` authorities are selected per key; the first authority is primary and the rest are mirrors
- the client prewarms a live-client membership snapshot during `build(...)`
- background membership sync refreshes that snapshot after startup
- steady-state reads use that cached snapshot instead of refreshing membership inline
- route reads and CAS stay off the metadata hot path in steady state
- storage owners manage local eviction separately from route ownership
- read paths keep `Draining` owners readable for handoff, fail fast on suspect or offline owners, keep suspect owners quarantined until membership shows a fresh lease/control-plane refresh, and best-effort prune unreadable replicas after fallback

Cluster policy is metadata-authoritative:

- every client starts with a local `route_control` (cluster-level) and `route_topk` (fallback, overridable per-tenant)
- the first client in a metadata keyspace persists the cluster route policy
- later clients must match the stored policy or startup fails
- `route_control` is uniform across all tenants in a metadata keyspace

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
- start successor processes with the same `stable_id` during hot-upgrade flows; the metadata backend allocates the next epoch and the runtime prints it as `epoch=<n>` on startup
- mount local memory before serving data traffic
- keep `heartbeat_interval_ms` comfortably below the default `lease_ttl_ms=30000`
- use a dedicated metadata keyspace per environment or test run
- if hugepage mode is enabled, preallocate matching hugepages on the host before starting clients

## Next Reading

- [Rust API](../../api-reference/rust/store-rs.md) for Rust integration
- [Configuration](configuration.md) for knobs and defaults
- [Architecture](../../design/store/store-rs/architecture.md) for request paths and control-plane behavior
