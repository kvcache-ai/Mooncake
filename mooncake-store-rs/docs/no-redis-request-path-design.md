# No-Redis Request-Path Design

## Background

Store-RS multi-tenant support already separates operator policy authoring from runtime enforcement in principle:

- operators write tenant-scoped policy through `mooncake-store-admin`
- runtimes enforce routing, quota, placement, and QoS on bootstrap and request paths

However, the current implementation still allows Redis-backed metadata access to appear in `put` / `get` request execution.

That is acceptable for bootstrap, admin inspection, and explicit repair, but it is not acceptable for the steady-state request path we want going forward.

This document captures:

1. the current Redis usage that still appears in multi-tenant request flows
2. the target requirement: **`put` / `get` must not trigger Redis access**
3. a concrete refactor plan to remove Redis from request serving while preserving correctness and operability

---

## Requirement

### Hard requirement

For steady-state runtime request serving:

- `get` must not trigger Redis access
- `put` must not trigger Redis access
- `remove` and routed `batch_put` should follow the same rule because they share the same admission / routing machinery

"No Redis access" here means:

- no direct Redis metadata reads in the request thread
- no direct Redis metadata writes in the request thread
- no request-path fallback that conditionally goes to Redis on miss / conflict / pressure / repair

A request path may still use:

- local in-memory state
- authority RPC / control-plane RPC
- transport RPC / storage RPC

But Redis must be excluded from synchronous request execution.

### What Redis may still do

Redis remains allowed for:

- admin policy storage
- startup snapshot loading
- background refresh / reconciliation
- explicit operator inspection
- explicit operator repair
- crash recovery source of truth or checkpoint store

So the requirement is **not** "remove Redis from the system".
The requirement is: **remove Redis from request serving**.

---

## Current State

## Summary

Today, Redis still appears in request-path-relevant multi-tenant logic in two broad areas:

1. **route lookup / route CAS when using `MetadataOnly`**
2. **strict tenant quota enforcement on write paths**

The second category is the more important one because it is present even under the preferred multi-tenant enforcement model.

---

## Current request-path Redis accesses

### 1. Route lookup in `MetadataOnly`

Relevant code:

- `crates/mooncake-store-client/src/route_directory.rs`
- `crates/mooncake-metadata/src/redis_backend.rs`

Key call path:

- `MetadataRouteDirectory::get_object_route(...)`
- `metadata.get_object_route(...)`
- Redis `HGET route payload`

Evidence:

- `crates/mooncake-store-client/src/route_directory.rs:69`
- `crates/mooncake-metadata/src/redis_backend.rs:1870`

Impact:

- `get` can hit Redis for route lookup when route control runs in `MetadataOnly`
- `put` / `batch_put` can also hit Redis when loading current route state before route publish

Implication:

- As long as `MetadataOnly` is a valid request-serving mode, `put` / `get` cannot guarantee no Redis access

---

### 2. Tenant policy lookup on write path

Relevant code:

- `crates/mooncake-store-client/src/client/runtime_io.rs`
- `crates/mooncake-metadata/src/redis_backend.rs`

Key call path:

- `tenant_quota_policy_for_object(...)`
- `self.metadata.get_tenant_policy(...)`
- Redis `GET tenant policy`

Evidence:

- `crates/mooncake-store-client/src/client/runtime_io.rs:24`
- `crates/mooncake-metadata/src/redis_backend.rs:2040`

Impact:

- every strict-quota write currently resolves effective tenant policy via metadata access in the request path

Implication:

- control-plane policy is still leaking into synchronous admission logic

---

### 3. Tenant object-accounting lookup on write path

Relevant code:

- `crates/mooncake-store-client/src/client/runtime_io.rs`
- `crates/mooncake-metadata/src/redis_backend.rs`

Key call path:

- `self.metadata.get_tenant_object_accounting(scoped_key)`
- Redis `GET tenant object accounting`

Evidence:

- `crates/mooncake-store-client/src/client/runtime_io.rs:81`
- `crates/mooncake-store-client/src/client/runtime_io.rs:212`
- `crates/mooncake-metadata/src/redis_backend.rs:2184`

Impact:

- every strict-quota `put` / `remove` reads authoritative object accounting through Redis-backed metadata today

---

### 4. Tenant quota reserve on write path

Relevant code:

- `crates/mooncake-store-client/src/client/runtime_io.rs`
- `crates/mooncake-metadata/src/redis_backend.rs`

Key call path:

- `self.metadata.reserve_tenant_quota(...)`
- Redis Lua script `RESERVE_TENANT_QUOTA_SCRIPT`

Evidence:

- `crates/mooncake-store-client/src/client/runtime_io.rs:109`
- `crates/mooncake-store-client/src/client/runtime_io.rs:230`
- `crates/mooncake-metadata/src/redis_backend.rs:2320`

Impact:

- write admission is synchronously Redis-backed today

---

### 5. Tenant quota finalize on write path

Relevant code:

- `crates/mooncake-store-client/src/client/runtime_io.rs`
- `crates/mooncake-metadata/src/redis_backend.rs`

Key call path:

- `self.metadata.finalize_tenant_quota(...)`
- Redis Lua script `FINALIZE_TENANT_QUOTA_SCRIPT`

Evidence:

- `crates/mooncake-store-client/src/client/runtime_io.rs:240`
- `crates/mooncake-store-client/src/client/runtime_io.rs:267`
- `crates/mooncake-metadata/src/redis_backend.rs:2443`

Impact:

- write completion still depends on synchronous Redis-backed finalize logic

---

### 6. Tenant quota abort on failure path

Relevant code:

- `crates/mooncake-store-client/src/client/runtime_io.rs`
- `crates/mooncake-metadata/src/redis_backend.rs`

Key call path:

- `self.metadata.abort_tenant_quota(...)`
- Redis Lua script `ABORT_TENANT_QUOTA_SCRIPT`

Evidence:

- `crates/mooncake-store-client/src/client/runtime_io.rs:292`
- `crates/mooncake-metadata/src/redis_backend.rs:2576`

Impact:

- request failure handling still synchronously depends on Redis-backed reservation state

---

### 7. Tenant eviction candidate lookup under quota pressure

Relevant code:

- `crates/mooncake-store-client/src/client/runtime_io.rs`
- `crates/mooncake-metadata/src/redis_backend.rs`

Key call path:

- `list_tenant_eviction_candidates(...)`
- Redis `ZRANGE` frontier + `GET` object accounting

Evidence:

- `crates/mooncake-store-client/src/client/runtime_io.rs:165`
- `crates/mooncake-metadata/src/redis_backend.rs:2222`

Impact:

- even if quota admission is not exceeded in the common case, pressure handling still falls back to Redis

Implication:

- "no Redis in request path" must include miss / pressure branches, not only the common success path

---

## What is not part of the target hot path

The following Redis-backed operations exist but are not themselves the primary request-path problem:

- startup policy load
- startup route policy bootstrap
- startup lease allocation / publication
- background live-client membership refresh
- admin inspection APIs
- admin reconcile / maintenance APIs

These may continue to use Redis as long as they stay outside synchronous request serving.

---

## Design Goals

The refactor should achieve all of the following:

1. `put` / `get` never touch Redis synchronously
2. strict multi-tenant quota remains authoritative, not best-effort
3. route lookup / route publish remain correct under concurrency
4. operators still have a durable admin-visible state store
5. crash recovery remains possible
6. maintenance and reconciliation remain explicit and inspectable

---

## Non-Goals

This design does **not** require:

- deleting Redis support entirely
- removing admin metadata inspection
- removing durable metadata checkpoints
- replacing all control-plane RPC with local state only

This is specifically a request-path isolation effort.

---

## Target Architecture

## High-level split

### Request path uses only:

- local runtime memory
- in-memory route authority
- in-memory tenant quota authority
- control-plane RPC between runtimes / authorities
- transport / storage RPC

### Redis is moved to:

- bootstrap snapshot source
- async checkpoint sink
- admin policy store
- admin inspection backend
- reconcile / repair backend
- crash recovery input

---

## Core principle

### Route state and quota state must stop being Redis-authoritative for request serving

As long as route resolution or quota admission depends on Redis-backed metadata during request execution, request-path Redis elimination is impossible.

So the key change is:

- **Redis-backed metadata stays durable/control-plane oriented**
- **request-path authority becomes in-memory / RPC-oriented**

---

## Implementation Status

The following pieces are implemented as the first slice of this design:

- tenant-policy listing now accepts an optional tenant filter at the metadata trait boundary, so admin `policy list --tenant <T>` can push the filter into Redis / etcd instead of scanning every tenant policy and filtering in process
- default-scope object keys keep the legacy `tenant::logical_key` format
- non-default namespace object keys now include tenant, domain, object_set, and logical_key in a deterministic encoded key, allowing the same tenant and logical key to coexist across different domain / object_set scopes
- client point and batch route lookups derive deterministic scoped keys and do not perform Redis fallback probing
- scoped route listing and reuse-candidate listing now go through `RouteDirectory`; `MetadataOnly` delegates to metadata for compatibility, while `EmbeddedWrh` serves local authority route state without metadata route scans
- hot-path tests now fail if routed request paths call metadata route lookup, route CAS, or full route listing through `NoHotPathMetadataBackend`

Remaining work is still required for the full no-Redis request-path target, especially tenant-policy snapshots and tenant-root quota authority / local quota slices.

---

## Proposed Components

## 1. Route authority as the only request-serving route source

### Target behavior

For normal request serving:

- `get` route lookup must resolve through route authority, not Redis metadata
- `put` current-route load and route CAS must resolve through route authority, not Redis metadata

### Required changes

- treat `EmbeddedWrh` as the only valid request-serving route mode
- demote `MetadataOnly` to bootstrap / admin / debug / compatibility use only
- add startup validation so request-serving runtimes cannot accidentally run in `MetadataOnly`
- route scoped scans and reuse-candidate scans through `RouteDirectory`, not directly through `MetadataBackend`, so WRH serving paths read local authority state instead of Redis-backed route metadata

### Why this is necessary

`MetadataOnly` directly maps request route lookup to Redis-backed metadata.
As long as it is available in normal serving mode, `get` / `put` can still hit Redis.

---

## 2. Tenant policy snapshot in runtime memory

### Target behavior

Request-path policy resolution must use local snapshot state, not Redis-backed metadata reads.

### Required changes

- resolve effective tenant policy at bootstrap
- store effective policy snapshots in runtime memory
- refresh snapshots asynchronously or via explicit control-plane update propagation
- remove request-path calls to `metadata.get_tenant_policy(...)`

### Notes

This is the smallest and safest first removal because tenant policy is configuration-like and already conceptually belongs to the admin/control plane.

---

## 3. Tenant quota authority for authoritative admission

### Target behavior

The following request-path operations must move out of Redis-backed metadata and into quota authority logic:

- object-accounting lookup
- quota reservation
- quota finalize
- quota abort
- eviction frontier lookup

The quota path should follow a **CFS bandwidth-control style** model:

- the quota authority owns the tenant-global authoritative quota state
- each runtime / writer receives a preallocated local quota slice
- the hot path consumes local slice budget only
- refill happens only when local slice reaches a low-watermark or is exhausted

This removes per-write quota chattiness from the request path without giving up authoritative tenant-wide control.

### Authority-owned state

For each tenant root scope, the quota authority should own at least:

- effective tenant quota policy snapshot
- committed quota state
- distributed local slices
- pending refill / reclaim state
- object accounting state
- eviction frontier / victim ordering
- anti-thrashing hysteresis state near tenant limit

### Refill policy

Refill should not use a single dimension.
It should choose the larger of:

- a **fixed minimum grant**
- a **ratio-based grant** derived from tenant limit, recent consumption rate, or configured share

Conceptually:

- `grant = max(fixed_min_grant, ratio_based_grant)`

This avoids both extremes:

- tiny low-throughput refill requests that thrash near the limit
- refill grants that are too small for high-throughput writers and immediately force another request

### Anti-thrashing requirements

To avoid repeated refill requests near the limit:

- use a **low-watermark refill trigger**, not only refill-on-empty
- enforce a **minimum refill quantum**, rather than refilling exactly the missing amount
- apply **hysteresis near the tenant hard limit**, so the system does not oscillate between tiny grants and immediate rejections
- prevent multiple concurrent refill requests from the same runtime when one refill is already in flight

### Request-path effect

`put` / `remove` / `batch_put` should consume local quota slice on the hot path and only enter authority refill logic on the slow path. The request path should call quota authority over in-memory access or RPC, not Redis.

### Durability model

Quota authority may still asynchronously checkpoint to Redis or another durable store, but the checkpoint must not be required to synchronously complete the request.

---

## 4. Explicit separation between serving traits and metadata traits

### Current problem

The request path currently reaches into `MetadataBackend` directly for both:

- durable control-plane state
- request-serving authority decisions

That creates accidental Redis coupling.

### Required change

Split interfaces conceptually into:

- **serving-time authority traits**
  - route lookup / route CAS
  - tenant quota admission / finalize / abort
  - object accounting access
- **durable metadata traits**
  - policy CRUD
  - checkpoint I/O
  - admin inspection
  - recovery / reconciliation state

### Why this matters

Without an interface split, Redis-backed metadata remains too easy to call from request code.

---

## 5. Scoped object identity must be deterministic

### Target behavior

The same tenant must be able to store the same logical key in different domain / object_set scopes without route-key collision.

### Required changes

- default namespace routes keep the legacy `tenant::logical_key` key for compatibility
- non-default namespace routes use a deterministic full-scope key derived from tenant, domain, object_set, and logical_key
- request paths must compute the key directly from the logical object id
- request paths must not try one Redis key and then fallback to another Redis key on miss

### Why this matters

A runtime Redis fallback would reintroduce metadata reads on `put` / `get`. Compatibility must come from deterministic key construction, not request-time probing.

---

## 6. No Redis fallback inside miss / pressure / repair branches

### Target behavior

The no-Redis rule must include:

- cache-miss branches
- quota conflict branches
- quota pressure branches
- route repair branches triggered by request execution

### Required change

If request-time repair is still needed, it must go through route/quota authorities or local cached state, not Redis.

If that is too expensive or too risky, request-time repair should fail fast and push reconciliation to explicit background/admin flows.

---

## Proposed Phases

## Phase 1: Eliminate Redis from `get`

### Scope

- enforce request-serving use of route authority only
- disallow `MetadataOnly` for normal serving mode
- make route lookup and route CAS authority-only in the request path
- ensure get-time membership / authority resolution does not Redis-fallback

### Deliverables

- builder/runtime validation that rejects request-serving `MetadataOnly`
- documentation update describing `MetadataOnly` as non-serving / compatibility mode
- tests proving `get` route resolution does not call Redis-backed metadata in serving mode

### Expected result

`get` no longer performs Redis route lookup.

---

## Phase 2: Eliminate Redis policy lookup from `put`

### Scope

- move effective tenant policy resolution to bootstrap snapshot
- add async refresh or explicit reload path
- remove request-path `get_tenant_policy(...)`

### Deliverables

- runtime-held effective tenant policy snapshot
- refresh protocol for updated admin policy
- tests proving write-path policy resolution no longer calls metadata backend synchronously

### Expected result

`put` no longer reads Redis for tenant policy.

---

## Phase 3: Move strict quota admission to quota authority

### Scope

Replace request-time Redis-backed strict quota operations with quota authority operations:

- object accounting read
- reserve
- finalize
- abort
- eviction frontier read

### Deliverables

- tenant quota authority abstraction
- request-path integration in `runtime_io.rs` / `runtime_write.rs`
- background durability/checkpoint mechanism
- admin inspection compatibility over durable snapshot / checkpoint state
- tests for correctness under overwrite, delete, failure, conflict, and batch write cases

### Expected result

`put` / `remove` / `batch_put` no longer depend on Redis in synchronous admission / completion paths.

---

## Phase 4: Remove request-path Redis fallback and tighten invariants

### Scope

- audit all request branches for Redis fallback
- move pressure handling and repair to authority or explicit maintenance flow
- make any Redis request-path fallback a test failure / invariant violation

### Deliverables

- explicit fast-path invariant checks
- tests that fail if request code calls Redis-backed metadata
- observability showing authority-path usage vs background checkpoint usage

### Expected result

The no-Redis request-path property becomes enforceable, not just aspirational.

---

## Risks and Trade-offs

## 1. In-memory authority increases recovery complexity

If route/quota authority becomes in-memory authoritative for serving, crash recovery needs:

- checkpointing
- replay / rebuild
- reconciliation tools

This is acceptable because recovery complexity belongs in control-plane design, not request latency.

---

## 2. Authority RPC may replace Redis latency with control-plane latency

That is acceptable under this requirement.
The target is not "no remote hop"; the target is "no Redis in request path".

Still, route/quota authority placement should minimize unnecessary cross-node RPC.

---

## 3. Admin inspection may become snapshot-based rather than live-serving-state-based

That is also acceptable as long as:

- docs state the semantics clearly
- explicit reconcile tools exist
- operators can inspect both checkpointed state and live authority state when needed

---

## Testing Strategy

We should add dedicated tests for the property itself, not only functional correctness.

### 1. Request-path backend accounting tests

Use a counting / failing metadata backend to prove:

- serving-mode `get` does not call Redis-backed route metadata
- serving-mode `put` does not call Redis-backed route metadata
- serving-mode scoped route scans do not call Redis-backed full route listing in WRH mode
- serving-mode `put` does not call Redis-backed policy/quota/object-accounting metadata once quota authority is implemented

### 2. Failure-path tests

Verify that these branches also avoid Redis:

- quota exceeded
- overwrite conflict
- route CAS conflict
- batch put failure
- eviction path / pressure path

### 3. Recovery / checkpoint tests

Verify that removing Redis from request execution does not break:

- restart recovery
- admin inspection
- explicit reconcile

---

## Recommended Immediate Decisions

1. Treat **"no Redis in `put` / `get`"** as a hard serving invariant.
2. Treat **`MetadataOnly` as non-serving mode**.
3. Move **tenant policy resolution out of request path first**.
4. Prioritize **tenant quota authority** as the main architectural change.
5. Keep Redis as **durable admin/recovery storage**, not serving-time authority.

---

## Appendix: Most Important Current Evidence

### Route lookup

- `crates/mooncake-store-client/src/route_directory.rs:69`
- `crates/mooncake-metadata/src/redis_backend.rs:1870`

### Request-path tenant policy lookup

- `crates/mooncake-store-client/src/client/runtime_io.rs:24`
- `crates/mooncake-metadata/src/redis_backend.rs:2040`

### Request-path tenant object-accounting lookup

- `crates/mooncake-store-client/src/client/runtime_io.rs:81`
- `crates/mooncake-store-client/src/client/runtime_io.rs:212`
- `crates/mooncake-metadata/src/redis_backend.rs:2184`

### Request-path quota reserve/finalize/abort

- `crates/mooncake-store-client/src/client/runtime_io.rs:109`
- `crates/mooncake-store-client/src/client/runtime_io.rs:230`
- `crates/mooncake-store-client/src/client/runtime_io.rs:249`
- `crates/mooncake-store-client/src/client/runtime_io.rs:274`
- `crates/mooncake-store-client/src/client/runtime_io.rs:300`
- `crates/mooncake-metadata/src/redis_backend.rs:2320`
- `crates/mooncake-metadata/src/redis_backend.rs:2443`
- `crates/mooncake-metadata/src/redis_backend.rs:2576`

### Request-path quota-pressure eviction candidate lookup

- `crates/mooncake-store-client/src/client/runtime_io.rs:165`
- `crates/mooncake-metadata/src/redis_backend.rs:2222`
