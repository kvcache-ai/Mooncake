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
Successful route queries and reads count as hot-object access. The client reports active route hits
to the storage owners best-effort, so storage-owner CLOCK eviction sees prefix probes and completed
reads before later pressure arrives. Existence checks stay boolean probes and do not report hits.

With tenant quota policy configured, single-object `put` uses metadata-backed reserve/finalize/abort semantics and `remove` applies the matching refund when the delete becomes authoritative. This gives create, overwrite, and delete a tenant-root quota state that survives concurrent writers better than the older best-effort namespace scan.

`remove` and `batch_remove` also retry when route delete CAS observes a fresher still-active
current route that metadata can prove is the same hot-upgrade or standby-promotion successor
lineage. Cleanup traffic that races with rollout-era route migration therefore converges onto the
newer handoff route, while ordinary delete-versus-rewrite races, including same-payload rewrites,
still fail with conflict instead of deleting the fresher rewrite.

The performance boundary stays the same: backend-owned metadata is not allowed to become a normal steady-state request-path dependency. When quota or policy needs backend truth, the runtime is expected to hide that behind exact local caches, authority indirection, or maintained indexes instead of paying a backend round trip per request.

### Batch operations

- `batch_put`
- `batch_get`
- `batch_remove`
- batch existence checks

Batch APIs are first-class paths rather than thin wrappers around repeated single-item calls.

With tenant quota policy configured, routed `batch_put` now acquires metadata-backed quota reservations in deterministic scoped-key order, admits the batch all-or-nothing, publishes route CAS updates, and only finalizes quota for entries whose authoritative route publish succeeds. Reservation or publish failures abort unfinished quota state so batch retries do not inherit stale pending usage.

## Buffer-Oriented I/O

### Registered-buffer path

The runtime supports registering caller-owned buffers and then using them for put/get operations with less copy overhead.

Main capabilities:

- register and unregister buffers
- `put_from`
- `batch_put_from`
- `batch_get_into`
- registered subranges inside a larger caller-owned buffer

Remote read behavior:

- when a `batch_get_into` destination range is already registered, the runtime issues direct remote batch reads into that buffer instead of staging through local scratch first
- unregistered destination buffers still use the existing scratch-window planner and direct single-object fallback path when needed
- `put_from` and `batch_put_from` now preserve the same registered-buffer zero-copy contract for remote writes: the transport request sources point at the caller-registered buffer ranges instead of a scratch copy
- `batch_put_from` uses the same registered-buffer batch writer in both default local-write mode and explicit routed-write mode when the batch has a shared replication policy
- routed `batch_put_from` refreshes membership and re-ranks placement candidates on retry, so rollout-era write failures can fall forward onto newly visible live storage runtimes instead of reusing only the stale pre-retry ranking
- routed `batch_put_from` applies the same retry envelope to reserve-stage placement gaps, so brief hot-upgrade windows with no active `storage=true` owner wait for successor promotion instead of failing the batch immediately
- routed `batch_put_from` handles concurrent cache writers per key by treating route-CAS conflicts as successful cache insert races when an active route is already published; when the CAS response omits that active route, the client uses one exact bounded recheck for the conflicted key before surfacing a write failure
- Python compatibility `batch_put_from` reports best-effort per-key statuses; route-CAS conflicts are success, while true per-key write failures stay isolated to the affected item
- routed `batch_put_from` skips the pre-publish route preload on the accept-existing cache-write path when no tenant quota policy needs previous-route accounting; route CAS still resolves active conflicts and version-floor retries through the route authorities
- Python compatibility `batch_put_from` reports best-effort per-key statuses produced by the Rust batch writer; route-CAS conflicts with an active published route are success, while true per-key write failures stay isolated to the affected item without replaying the batch serially in Python
- Python compatibility registered-buffer batch restores also report best-effort per-key statuses; a route miss or dead storage owner for one entry does not poison other entries in the same restore batch
- compatibility `get_into_ranges` accepts a request-scoped route-query cache from higher-level planners, so tensor reconstruction can reuse one batch route lookup across prefix reads and final range reads instead of resolving the same shard keys repeatedly
- remote storage segment announcements publish explicit storage target chunks, and writers translate allocator segment offsets through those chunks instead of inferring storage from backend buffer ordering
- local reads and drain migration translate the published replica transport target offset through the storage target map before copying from local storage; allocator segment offsets remain reclaim bookkeeping only
- scratch buffers are registered for local transfer staging only and are not published as remote storage extents
- local replicas still copy into owned local segment memory; the zero-copy contract applies to the remote transport source buffer, not to local placement
- payload integrity uses a fast stable 64-bit checksum, and large batch validation fans out across multiple CPU workers so registered-buffer restore traffic does not serialize the verification tail on one core

### Multi-buffer path

For fragmented payloads or caller-managed buffer layouts, the runtime supports multi-buffer request forms.

Main capabilities:

- `batch_put_from_multi_buffers`
- `batch_get_into_multi_buffers`
- HiCache-compatible dummy and real paths for the Python layer

## Python Read Acceleration

### Daemon-local hot cache

The Python compatibility runtime can keep recently fetched remote values in a daemon-local hot cache.

What it provides:

- populate on successful read paths such as `get`, `get_into`, `batch_get`, `batch_get_into`, and multi-buffer reads
- serve repeated reads from local memory without repeating the remote transfer
- invalidate local entries after successful writes or deletes for the same key on that daemon
- partition local entries by Python compatibility scope so different metadata keyspaces do not reuse the same cached value
- optional shm-backed payload storage so multiple dummy clients attached to the same standalone daemon can reuse the cached bytes
- bounded LRU eviction with pin protection while dummy readers hold an acquired handle

Design boundary:

- the cache is a local read accelerator only
- it does not publish route state, replica state, or metadata
- a miss falls back to the normal remote read path
- values larger than the configured cache block size bypass the cache
- metadata keyspace remains the authoritative read/write isolation boundary, and runtime coverage now includes a regression test that verifies the same object key can be written and read independently in separate keyspaces

### Tensor parallel reconstruction

The Python tensor-parallel read path reconstructs missing full or shard targets from stored shard objects when no direct object already matches the requested layout.

What it provides:

- manifest-guided shard discovery for writer partitions and tensor parallelism layouts
- metadata-prefix reads for shard planning, so reconstruction can inspect dtype, shape, split dimension, and payload length without reading each full shard payload first
- request-scoped route-query reuse between shard discovery and `get_into_ranges` data transfer
- batch full `_into` reconstruction that combines multiple caller buffers into one ranged-read request where possible
- contiguous range planning for split dimension `0` and strided range planning for non-leading split dimensions

## Routing

### Embedded weighted rendezvous routing

Default route control uses embedded weighted rendezvous hashing.

What it provides:

- route owner selection without a dedicated master
- primary and secondary route authorities
- authority reads and compare-and-swap through the control plane
- replica removal through route-owner compare-and-swap
- mirrored-authority publication plus ranked authority reads for repair and convergence
- drain, migration, and shrink route updates through the object route CRUD/CAS facade once the affected key is known

### Metadata-only routing

For simpler bring-up or debugging, the runtime can store and resolve routes directly through the metadata backend.

### Explicit route migration tasks

The current implementation also exposes admin-driven explicit route migration tasks for key-level `copy` and `move`.

See [Route Migration Guide](./route-migration-usage.md) for the current
operator workflow, task semantics, and request examples.

What it provides:

- explicit `source_segment -> target_segment(s)` selection
- namespace-aware `tenant/domain/object_set/key` selection
- multi-target `copy`, including multiple explicit target segments on the same storage runtime
- single-target `move`
- admin HTTP queue plus `mooncake-store-admin migrate ...` operator commands
- a task-executor model where admin submits control-plane RPC and the executor runtime performs the transfer
- in-memory admin task queue with `pending`, `dispatching`, `running`, `retry_wait`, `succeeded`, and `failed` states
- admin-side retry with configurable backoff and a default retry budget of `5`
- route-based completion checks so admin can mark a task successful after executor loss when the final route is already authoritative

Design boundary:

- the admin queue is runtime memory only and does not recover across admin restart
- the CLI is only an HTTP client for this queue; `migrate ...` commands must talk to a long-lived `mooncake-store-admin server`
- the authoritative durable state remains the object route, not the admin task record
- retry is for executor loss or transient RPC failure, not for preserving a persistent migration backlog

## Placement and Replication

### Local-first placement

The default request behavior prefers local storage when possible.

When local storage cannot satisfy the request, the client can spill over to remote storage nodes.

### Routed remote placement

Router nodes can place data onto storage nodes selected by the placement planner.

The planner filters candidate nodes by labels and compatibility.
Replica owner ranking uses stable rendezvous input from the object scope and the
storage runtime's stable identity, so a storage-node epoch change does not
reshuffle placement by itself.

### Multi-replica reads

When an object route has multiple readable remote replicas and no local replica,
readers choose a remote replica with a stable score over the scoped route key,
reader identity, and replica identity. This spreads one reader's multi-key read
traffic across replicas while keeping selection deterministic for a given key.

### Request-level replication policy

A request can control placement behavior with:

- replica count
- preferred segment
- preferred segment list
- preferred storage owner
- preferred storage owner list
- `prefer_local`
- `prefer_alloc_in_same_node`

## Admin Maintenance

### Stateless stale-segment cleanup

The admin plane now supports Redis-backed and etcd-backed stale-segment cleanup without reintroducing a master on the request path.

What it provides:

- explicit one-shot stale cleanup through `mooncake-store-admin cleanup-stale-segments`
- continuous stateless cleanup through `mooncake-store-admin server`
- optional tenant-quota reservation reconcile in that same admin server for an explicit tenant list
- backend-native lease-expiry indexing for recoverable maintenance scheduling (`ZSET` in Redis, `by-runtime` + `by-time` keys in etcd)
- owner-scoped cleanup through backend-native owner metadata instead of a hidden global segment scan in the steady-state worker
- same-epoch lease reclaim after a lease TTL gap when no higher live epoch exists, so heartbeat repair and predecessor drain pinning do not fail with stale-epoch rejection
- `with_soft_pin`

## Observability

The client Prometheus exporter reports both request-level and internal operation-level store
health. Remote data-plane transfers now expose explicit transport operation outcomes through
`mooncake_store_transport_operation_total{direction,peer_kind,result}` in addition to successful
byte volume through `mooncake_store_transport_bytes_total{direction,peer_kind}`.

Route publication also exposes `mooncake_store_replication_publish_total{result}` alongside the
existing publish latency histogram. Operators can therefore distinguish publish failures from slow
successful publishes without inferring result counts from histogram internals.

Jaeger tracing can be controlled dynamically through the store metrics HTTP endpoint and through
the admin server fanout layer. The per-node metrics endpoint still owns the actual tracing switch,
while `mooncake-store-admin tracing ...` provides the cluster-level operator entry point. Admin
discovers live store nodes from metadata leases, derives the host from existing endpoint data, and
uses the store-published `metrics_port` label for the metrics HTTP port.

The lease extension is intentionally label-based. Older metadata readers ignore the extra label,
and newer admin servers fall back to the default metrics port for leases written before the label
existed. When tracing is disabled, request-path tracing work remains guarded by the runtime tracing
filter and dynamic exporter state rather than adding metadata calls to put/get hot paths.

## Allocation and Segment Management

### Local allocation

Local segment reservation and release are handled inside the client process.

The current implementation supports:

- transport-managed local memory
- hugepage-backed native memory for Rust store segments
- hugepage-backed shared memory for Python host buffers
- background async eviction on storage nodes

Startup behavior for large local storage is explicit:

- `classic_te` + RDMA keeps initial storage-region planning NUMA-aware for host memory, so RDMA-capable hosts can start with multiple local storage segments instead of collapsing everything into one CPU location
- startup registration of extra initial storage segments is transport-gated; non-RDMA and non-opted-in transports keep the historical single-segment startup layout
- when a transport enables startup fanout, the runtime registers extra initial storage segments in parallel and publishes them only after the local registration phase completes
- `classic_te` + RDMA pre-touches startup storage pages once total startup storage registration reaches `4 GiB`, reducing cold-page MR registration stalls on very large hosts

### Remote allocation

Remote storage reservation uses allocator RPC to the owning client.

Remote storage owners apply the same reserve loop as local storage owners:

- try allocator reservation
- if the failure is capacity-related, trigger storage-owner eviction and retry
- return an allocator error when capacity still cannot be recovered

### Remote allocator semantics

Allocator ownership stays with the storage owner.

- reserve and release go through allocator RPC on the owning client
- capacity failures returned by a live storage owner remain capacity failures
- transport or protocol failures quarantine the remote storage owner and let routed placement skip to the next soft candidate

## Storage-Owner Eviction

### Local CLOCK state

Each storage owner maintains a local CLOCK over the replicas it currently stores.

The tracked identity is route-oriented rather than segment-only:

- route key
- segment name
- segment offset

This lets the storage owner evict a precise replica instead of reclaiming blind segment regions.

### Background watermarks

Storage nodes reclaim proactively in the background.

The current behavior is:

- a background worker polls local usage
- when used bytes reach the high watermark, eviction starts
- eviction continues until used bytes fall to the low watermark or no more victims can be reclaimed
- synchronous reserve-fail eviction still remains as the last-resort fallback

Default local-memory settings are:

- high watermark: `90%`
- low watermark: `80%`
- poll interval: `100ms`

### Read-hit reporting

Successful reads update eviction heat without putting metadata back on the hot path.

The current implementation does this in batch:

- local hits update the local storage-owner state directly
- remote hits are grouped by storage owner
- grouped hits are sent through `BatchReportRouteHits`

Hit reporting is best-effort. A lost hit can reduce eviction quality, but it does not affect correctness.

### Route tracking after publish

After a writer successfully publishes a route, it groups the remote replica owners and sends the published routes through `BatchTrackReplicaRoutes`.

This gives each storage owner a current route view for the replicas it stores without requiring a metadata scan on the write hot path.
Freshly published replicas get one CLOCK grace pass on the storage owner. Completed reads and
route-query access signals still set the stronger read-hot bit; boolean existence probes remain
pure probes and do not report hits.

### Route-owner CAS reclaim

When capacity pressure forces eviction:

- the storage owner picks a CLOCK victim
- the matching route owner removes that replica with CAS
- only after CAS succeeds does the storage owner release the local allocation

This preserves route correctness during eviction and keeps reclaim coupled to the authoritative route version.

### Where local eviction runs

Local CLOCK eviction is enabled for clients labeled `storage=true`.

This keeps the roles clean:

- storage nodes reclaim their own local capacity
- routed rw nodes keep the existing spill-remote behavior
- scratch-only rw nodes can run with `storage_bytes=0`

Startup also enforces the role boundary:

- `storage=true` requires `storage_bytes > 0`
- when `storage_bytes=0` and no explicit storage label is supplied, the runtime defaults the label to `storage=false`

## Delete and Reclaim

### Overwrite reclaim

When a key is overwritten, old replicas are scheduled for release.

### Delete reclaim

When a key is removed, route state is removed and segment space is reclaimed.

For tenant-scoped quota policy, delete now also finalizes a negative quota delta when the route delete CAS succeeds. That means quota refund is tied to authoritative object deletion, not to later background reclaim of the freed segment space.

### Graceful reclaim

A reclaim grace window can delay release to smooth transitions or handoff behavior.
Queued reclaim cleanup now treats duplicate local or remote release as idempotent
cleanup noise, while direct non-cleanup release paths still return allocator
errors.

## Multi-Tenancy

The client supports tenant-scoped keys and tenant-aware request builders.

This keeps API usage explicit while still allowing a default tenant for simpler applications.

Route-authority policy has a default cluster policy per metadata keyspace. `route_control` is a cluster-level deployment decision fixed at startup (determines whether WRH authority mesh or Redis-only route storage is used). `route_topk` can be overridden per-tenant via admin policy. Runtime bootstrap resolves the effective `route_topk` from the tenant policy if present, otherwise uses the CLI default.

## Dynamic Membership and Lifecycle

### Client lifecycle

The runtime models explicit client states:

- standby
- active
- draining

### Handoff and upgrade

The client can publish handoff plans and participate in successor upgrade flows.

### Elastic capacity

The client can expand local storage, drain segments, retire empty segments, and evacuate all replicas owned by a draining client.

This allows segment-level shrink, full client shrink, and dynamic storage growth without changing the public API.

During full-client drain, replacement routes are published through the same object route CRUD/CAS facade used by normal writes. The client does not perform a separate route-authority fanout for the draining authority. Explicit drain migration reads the selected source replica through an exact readable lease lookup instead of the suspect-runtime placement cache, so allocator quarantine for new writes does not block copying the source bytes that are being evacuated. Migration placement excludes the draining source owner even when a separate writer performs the copy, and the source owner releases its old allocations after publication. Read selection also prefers a local readable replica before remote replicas with lower route priority, so replica-protected reads avoid a dead remote primary when the survivor already has the data locally.

## Control Plane

The runtime includes a dedicated control plane for route and non-route control
traffic:

- route operations
- allocator operations
- eviction-specific hit reporting and route tracking
- migration task submission and status queries

Important properties:

- protobuf schema
- tonic RPC implementation
- batched operations
- streaming session reuse

Route-control semantics live in `mooncake-store-route`; the client control
plane remains responsible for transport concerns such as protobuf encoding,
tonic channels, stream reuse, unary fallback, and request timeouts.

## Observability

### Tracing

Tracing can be enabled from code or environment variables.
Jaeger profiling exports OTLP spans for the same operation and stage boundaries
used by metrics, including request APIs, route lookup, allocation, transfer,
route publication, metadata backend calls, heartbeat, membership refresh, and
storage-owner eviction. The spans are named by profiling phase rather than raw
implementation hook: API roots use `store.*`, control-plane stages use
`control.*`, data-plane stages use `data.*`, and backend calls use
`metadata.*`. A put/get request therefore appears in Jaeger as one in-process
waterfall with `mooncake.phase`, `mooncake.flow`, request id, item count, byte
count, replica count, and local/remote target-count attributes.

The in-process metrics HTTP server exposes `/tracing` to turn profiling on or
off dynamically when a sink is configured. Use `endpoint=...` for OTLP/Jaeger
or `file=/tmp/store-rs.trace.jsonl` for a local JSONL artifact. Set
`MC_STORE_RS_OTLP_SAMPLE_RATIO` or the `/tracing?sample_ratio=...` query before
enabling export when profiling a high-throughput workload against a
memory-backed Jaeger collector.

When local JSONL profiling is enabled, `MC_STORE_RS_TRACE_ITEM_METADATA=1`
adds `store.api_items.v1` records for `batch_put_from` and `batch_get_into`.
These records keep per-item namespace, runtime id, key hash/prefix, parsed
SGLang TP rank and `k`/`v` suffix, object size or read length, and item status
out of Prometheus labels while still making offline per-key put/get counts
possible. `MC_STORE_RS_TRACE_KEY_MODE` defaults to `hash`; `full` is only for
local debug captures.

### Metrics

The runtime records:

- operation counts
- input and output bytes
- total latency
- max latency
- histogram percentiles for request APIs and internal stages
- metadata backend operation counts and latency
- transport operation counts and bytes

The metrics HTTP server also exposes `GET /breakdown`, a structured diagnostic
snapshot for SGLang/HiCache runs. It groups API calls, internal phases, metadata
backend work, transport volume, runtime state, and segment usage, then ranks
bottleneck candidates by observed total latency and p99 latency. The ranking is
evidence for where time was spent, not proof of root cause.
- status labels such as `ok` and `error`
- strict tenant quota counters for reservation, finalize, abort, and reconcile outcomes

### HTTP export

Metrics can be served through an in-process HTTP endpoint.

## Python Compatibility

The Python layer mirrors the same runtime instead of wrapping a separate implementation.

What this means in practice:

- the same route, allocator, and reclaim logic is reused
- the same transport path is reused
- the same metrics and tracing capabilities are available
- the same replication controls are exposed through `ReplicateConfig`
- the same standalone compatibility server can serve dummy clients
- the wheel packaging flow ships the native extension with bundled runtime libraries

### Dummy and real compatibility paths

The compatibility layer exposes two integration modes:

- real mode uses the native `StoreClient` directly from Python
- dummy mode talks to a standalone `mooncake-store-client` process and exchanges shm buffer registrations over a side channel

Both paths are covered by repository validation scripts.

The standard read/write entry point is:

```bash
./scripts/tests/client/test-client-rw-cli.sh
```

Manual path-specific validators stay available as:

- `scripts/clients/real_client_rw.py`
- `scripts/clients/dummy_client_rw.py`
