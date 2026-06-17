# Tenant Quota Consistency Design

## Background

Store-RS already supports tenant-scoped quota policy through metadata-backed tenant policy and runtime enforcement hooks. Today, however, quota enforcement still happens as a request-path preflight inside the client runtime.

Relevant current path:

- `StoreClient::put_object_with_policy_current(...)` in `crates/mooncake-store-client/src/client/runtime_io.rs`
- routed and batch write allocation / publication logic in:
  - `crates/mooncake-store-client/src/client/runtime_io.rs`
  - `crates/mooncake-store-client/src/client/runtime_write.rs`
- tenant quota policy model in:
  - `crates/mooncake-store-core/src/route.rs`
- metadata-backed tenant policy storage in:
  - `crates/mooncake-metadata`

Current write flow is roughly:

1. load current route / object state
2. evaluate quota from that snapshot
3. reserve local or remote capacity
4. write bytes into storage segments
5. publish the new route with route CAS
6. reclaim previous route allocations

This leaves a time-of-check/time-of-use gap:

- two writers can both read the same tenant usage snapshot
- both can pass quota admission
- both can write bytes
- both can attempt route publication
- quota correctness depends on timing rather than protocol guarantees

That is acceptable for best-effort quota, but it is not sufficient for hard multi-tenant isolation.

## Goal

Make tenant quota enforcement **strongly consistent at the metadata authority level** so concurrent writes cannot over-admit tenant storage usage beyond configured quota, except in explicitly bounded transitional states.

The design should:

- preserve tenant-scoped policy as the source of truth for limits
- work with current Store-RS architecture:
  - embedded WRH route authorities
  - metadata-backed tenant policy
  - allocator reservations and route publication
  - remote storage-owner tracking
- correctly handle:
  - create
  - overwrite
  - delete
  - conflict retry
  - client crash during write publication
- keep the request-path semantics understandable and debuggable

## Non-Goals

This design does **not** try to solve:

- global fairness / rate limiting
- bandwidth shaping
- allocator-space accounting beyond quota admission correctness
- exact real-time reconciliation of every derived usage view
- elimination of all background reconciliation work
- redesigning route ownership or WRH authority selection

This design is specifically about **hard tenant quota admission** for stored objects.

---

## External Design References

The research across Ceph, Kubernetes, HDFS, Swift, MinIO, SeaweedFS, JuiceFS, and transactional KV systems led to a clear conclusion:

- systems that want **hard quota semantics** perform quota accounting in the same authoritative metadata transaction as object state mutation, with optimistic conflict retry
- systems that keep quota checks outside the authoritative commit path usually become best-effort or eventual

The most relevant patterns for Store-RS are:

1. **Kubernetes ResourceQuota**
   - admission-time enforcement against authoritative versioned state
   - optimistic conflict retry on concurrent updates
2. **HDFS quota enforcement**
   - namespace mutation and quota accounting share a single metadata authority path
3. **FoundationDB transactional counters**
   - read current counter, compute delta, update counter and entity record in one serializable transaction
4. **JuiceFS hybrid practice**
   - hard admission on authoritative state, background repair only for derived accounting / cleanup

This design follows that family of solutions.

---

## Current Architecture Constraints

From `docs/architecture.md`, the current Store-RS write path has these important properties:

- `StoreClient` performs request-path admission, allocation, transfer, and route publication
- `RouteDirectory` owns route lookup and route CAS
- route authority is usually **not** the metadata backend directly in `EmbeddedWrh` mode
- allocator reservation and data transfer can happen before the final route CAS commits
- storage ownership and route ownership are intentionally decoupled

Those constraints matter because a strict quota design must answer:

1. where quota usage becomes authoritative
2. how overwrite delta is computed
3. what happens if data bytes are written but final publication does not commit
4. how retries avoid double-charging or double-refunding

---

## Problem Statement

### Why the current flow is insufficient

Today the runtime can ask “does this tenant still have room?” before writing, but that answer is based on a snapshot that may already be stale by the time route publication happens.

This creates several correctness gaps:

### 1. Concurrent create overshoot

Two writers can both observe usage below limit and both admit writes that together exceed quota.

### 2. Overwrite mis-accounting

Quota should be charged on **delta**:

- create: `+new_size`
- overwrite larger value: `+(new_size - old_size)`
- overwrite smaller value: `-(old_size - new_size)` or immediate refund to usage
- delete: `-old_size`

A preflight check on `new_size` alone is not sufficient.

### 3. Publish-after-write crash window

The current write path can write bytes before final route publication succeeds. If the client crashes after writing bytes but before authoritative metadata commit, quota state and stored bytes can diverge.

### 4. Conflict retry ambiguity

Retries must not:

- charge quota twice
- leak reserved quota
- reclaim quota for a write that never became authoritative

A correct solution needs idempotent state transitions.

---

## Design Summary

Introduce a new **authoritative tenant quota state** in metadata and move quota admission into the same metadata mutation protocol that commits object state.

The recommended model is:

1. derive the current object accounting state from authoritative metadata
2. compute the requested quota delta
3. atomically update:
   - tenant quota usage state
   - authoritative object accounting state
   - route publication intent / committed object metadata
4. only after that authoritative commit succeeds should the write become visible as committed
5. if data bytes must be written before final commit, represent that with an explicit reservation / pending-write record rather than with an implicit preflight check

In short:

- **quota becomes part of the metadata commit protocol**
- not a separate runtime-local admission hook

---

## Proposed Metadata Model

### 1. Tenant quota state

Add a new metadata entity per tenant scope:

```text
TenantQuotaState {
  scope: TenantPolicyScope,
  version: u64,
  used_bytes: u64,
  used_objects: u64,
  pending_reserved_bytes: u64,
  pending_reserved_objects: u64,
  updated_at_ms: u64,
  updated_by: String,
}
```

Notes:

- `scope` should initially be tenant-level only
- domain/object_set quota usage can be added later if we want nested enforcement
- `version` is required for optimistic concurrency / CAS
- `pending_reserved_*` is only needed if we adopt the reservation-based protocol described below

### 2. Authoritative object accounting record

Quota logic must know the committed size of each object.

We can satisfy that in one of two ways:

#### Option A: use the authoritative committed route/object record directly

If the route record is always the committed source of object length, we can derive old size from it.

Pros:

- less metadata duplication

Cons:

- route ownership in `EmbeddedWrh` mode is authority-based rather than globally centralized in metadata
- tying strict quota to route-authority-local state is harder to reason about

#### Option B: add a metadata-backed object accounting record

```text
TenantObjectAccounting {
  key: ObjectKey,
  tenant_scope: TenantPolicyScope,
  version: u64,
  committed_length: u64,
  route_version: Option<u64>,
  state: Active | Deleted,
  last_writer: String,
  updated_at_ms: u64,
}
```

Pros:

- quota is anchored in one durable metadata plane
- overwrite/delete delta is explicit
- route authority remains responsible for route ownership while quota correctness remains metadata-authoritative

Cons:

- additional metadata writes and consistency protocol

### Recommendation

Use **Option B**.

It cleanly separates:

- route ownership / replica placement authority
- tenant quota accounting authority

This matches the current Store-RS architecture better than forcing hard quota semantics into WRH route authorities.

### Phase 1 status

The Phase 1 metadata foundation now exists in the shared model and in-memory backend:

- `TenantQuotaState`
- `TenantObjectAccounting`
- `TenantQuotaReservation`
- `TenantQuotaReservationRequest` / `TenantQuotaFinalizeRequest`
- backend trait methods for get/list/reserve/finalize/abort

Current implementation status:

- in-memory backend: implemented and covered by unit tests
- Redis backend: implemented with backend-atomic Lua reserve/finalize/abort scripts plus quota/accounting lookup helpers
- etcd backend: implemented with multi-key compare-and-swap txn loops for reserve/finalize/abort plus quota/accounting lookup helpers

Phase 1 backend parity tests now cover:

- create / overwrite / delete reservation state transitions
- finalize idempotency
- abort idempotency
- byte and object limit admission
- reservation listing and quota-state lookup

The first implementation remains tenant-root only. `TenantPolicyScope` values with domain/object-set components are rejected for quota-state mutation APIs until nested quota accounting is implemented.

---

## Write Protocol Options

There are two realistic protocol families.

## Option 1: Atomic metadata commit before data publish

### Flow

1. read `TenantQuotaState` and `TenantObjectAccounting`
2. compute `delta_bytes` and `delta_objects`
3. CAS / transact metadata to:
   - validate limit
   - update quota usage
   - update object accounting state to new committed size/version
   - create a pending publish marker or next route intent
4. write bytes to storage
5. publish route
6. mark pending publish committed

### Why it is not the best fit today

This gives strong quota semantics, but it means metadata can claim usage before data bytes are durably published. On crash/failure, we then need rollback or orphan recovery anyway.

Given Store-RS currently writes bytes before route publication, this option does not reduce protocol complexity enough.

---

## Option 2: Reservation + finalize/abort protocol

### Summary

This is the recommended design for Store-RS.

Instead of doing a plain preflight quota check, the writer first obtains an authoritative **quota reservation** in metadata. That reservation is then either finalized into committed usage or aborted.

### New metadata entity

```text
TenantQuotaReservation {
  reservation_id: UUID,
  tenant_scope: TenantPolicyScope,
  object_key: ObjectKey,
  object_version_hint: Option<u64>,
  requested_bytes_delta: i64,
  requested_objects_delta: i64,
  state: Pending | Finalized | Aborted,
  expires_at_ms: u64,
  created_at_ms: u64,
  writer_runtime: ClientRuntimeId,
}
```

### Flow

#### Phase A: reserve quota

1. read committed `TenantObjectAccounting` for the object
2. compute tentative delta against current committed size
3. metadata transaction atomically:
   - verifies tenant policy limit
   - increments `pending_reserved_bytes` / `pending_reserved_objects`
   - inserts `TenantQuotaReservation`
   - version-checks the object accounting row used for delta calculation

If this step fails, the write is rejected before bytes are written.

#### Phase B: write bytes and publish route

4. reserve storage allocations
5. write bytes to target segments
6. publish the route through existing route authority flow

#### Phase C: finalize reservation

7. metadata transaction atomically:
   - verifies reservation is still `Pending`
   - verifies committed object accounting version still matches expected predecessor
   - applies the committed object size/version
   - moves reserved quota into committed usage
   - decrements `pending_reserved_*`
   - marks reservation `Finalized`

#### Abort path

If route publication fails or the writer gives up:

- metadata transaction marks reservation `Aborted`
- decrements `pending_reserved_*`
- leaves committed usage unchanged
- local/remote allocations are released or scheduled for reclaim

### Why this fits Store-RS best

It matches the current architecture:

- allocations and data transfer still happen before final route visibility
- quota state becomes authoritative in metadata rather than in route authorities
- failures become explicit protocol states instead of hidden races
- recovery is auditable and repairable

---

## Authoritative Semantics

### Committed usage

`used_bytes` and `used_objects` reflect only **finalized committed objects**.

### Pending usage

`pending_reserved_*` reflects in-flight writes that passed quota admission but have not yet finalized.

### Admission rule

New reservations are allowed only if:

```text
used_bytes + pending_reserved_bytes + requested_positive_bytes_delta <= max_bytes
used_objects + pending_reserved_objects + requested_positive_objects_delta <= max_objects
```

Non-positive deltas from shrink / overwrite-same / overwrite-smaller / delete should not require
quota headroom on that dimension.

### Why include pending reservations in admission

Without this, concurrent in-flight writes can all reserve against the same free space and oversubscribe before finalization.

---

## Delta Computation Rules

Quota correctness depends on consistent delta rules.

Assume authoritative committed state for object `O` is:

- `old_size`
- `old_exists`

Then:

### Create

If object does not exist:

- `delta_bytes = new_size`
- `delta_objects = +1`

### Overwrite same object

If object exists:

- `delta_bytes = new_size - old_size`
- `delta_objects = 0`

This means:

- larger overwrite may require headroom
- same-size overwrite needs no quota expansion
- smaller overwrite frees quota on finalize

### Delete

If object exists:

- `delta_bytes = -old_size`
- `delta_objects = -1`

### Idempotent retry

A retry of the same logical write must either:

- reuse the same reservation id, or
- fail version checks and recompute against latest committed state

The protocol must never assume retries are harmless without version checks.

---

## Interaction with Route Authorities

The current architecture deliberately keeps route control off the metadata hot path for normal reads/writes in `EmbeddedWrh` mode.

This design preserves that.

### Principle

- metadata is authoritative for quota accounting
- route authorities remain authoritative for route publication and mirrored route convergence

### Implication

Finalization must bridge both worlds:

- route publication result determines whether the write became visible
- metadata finalization determines whether quota usage becomes committed

### Required invariant

A route must not be treated as fully committed for quota purposes unless the matching reservation is finalized.

That means we need one of these two implementation rules:

#### Rule A: finalize immediately after successful route CAS, before returning success

This is the recommended rule.

The user-visible `put(...)` only returns success once:

1. route CAS succeeded
2. reservation finalization succeeded

If finalization fails after route CAS succeeds, the client must enter repair logic instead of blindly returning success.

#### Rule B: return success after route CAS and reconcile finalization asynchronously

Not recommended for the first implementation.

It creates user-visible ambiguity and can temporarily diverge committed route visibility from committed quota usage.

---

## Failure Handling and Recovery

This is the main reason to prefer an explicit protocol over a hidden preflight check.

## Failure matrix

### 1. Reservation fails

Result:

- write rejected
- no bytes written
- no route published

### 2. Data write fails after reservation

Result:

- reservation is aborted
- pending reservation counters are released
- allocations are released
- no committed usage change

### 3. Route CAS fails after data write

Result:

- reservation is aborted
- newly written allocations are released or reclaimed
- no committed usage change

### 4. Client crashes after reservation, before finalize/abort

Result:

- reservation remains `Pending`
- background reconciler inspects expired reservations
- for each expired reservation:
  - if no matching committed route/object accounting exists, abort it
  - if route publication succeeded and object accounting can be proven, finalize it

### 5. Finalize transaction conflicts

Result:

- retry finalization if the predecessor version still matches
- otherwise re-read current object accounting and route state
- if another write already committed a newer object version, abort stale reservation and reclaim bytes

---

## Background Reconciliation

A strict protocol still needs bounded repair for crash recovery.

Add an explicit reconciliation loop or admin-triggered tool for:

- expired pending reservations
- orphaned written allocations with no finalized reservation
- finalized accounting with missing route visibility
- route visibility with stale pending reservation state

### Initial implementation recommendation

Keep reconciliation simple and operator-visible:

- add admin inspection commands first
- background automation can come after the protocol is stable

Suggested admin surface:

- `mooncake-store-admin quota state --tenant <t>`
- `mooncake-store-admin quota reservations --tenant <t>`
- `mooncake-store-admin quota abort --tenant <t> --reservation-id <id> [--dry-run]`
- `mooncake-store-admin quota reconcile --tenant <t> [--dry-run]`

The current implementation also exposes matching HTTP endpoints:

- `GET /v1/tenant-quotas/<scope>`
- `GET /v1/tenant-quotas/<scope>/reservations?state=pending|finalized|aborted`
- `POST /v1/tenant-quotas/<scope>/reservations/<reservation_id>/abort`
- `POST /v1/tenant-quotas/<scope>/reconcile`

Current reconcile behavior is intentionally conservative:

- expired pending reservations are aborted
- pending reservations whose route and object-accounting state already match an authoritative active object are finalized
- other mismatches remain inspectable and are reported as skipped for operator review

Reservation retention is now split by lifecycle state:

- `Pending` reservations are kept until they are finalized or aborted by normal write flow or explicit reconcile
- `Finalized` and `Aborted` reservations are retained only for a bounded terminal window, then expire from Redis automatically
- the default terminal retention window is `24h`
- terminal retention uses Redis TTL on both the reservation record and its per-tenant index entry so completed history does not accumulate indefinitely

This matches the existing admin philosophy in `docs/multi-tenant-admin-control-plane-design.md`. Runtimes now also export tenant-quota reservation/finalize/abort/reconcile counters through the built-in Prometheus metrics registry.

---

## API / Trait Changes

## Metadata traits

Add new metadata operations in `mooncake-store-core` and implement them in all backends.

### New operations

```text
get_tenant_quota_state(scope)
compare_and_swap_tenant_quota_state(scope, expected_version, next)
get_object_accounting(key)
compare_and_swap_object_accounting(key, expected_version, next)
create_quota_reservation(...)
finalize_quota_reservation(...)
abort_quota_reservation(...)
list_quota_reservations(scope, filter)
```

### Preferred backend shape

Where backend capabilities allow it, expose higher-level atomic methods instead of forcing multi-step client orchestration:

```text
reserve_quota_for_object_write(...)
finalize_quota_write(...)
abort_quota_write(...)
```

This is especially important for Redis / etcd so the client does not emulate multi-key transactions unsafely.

## Runtime changes

### `StoreClient`

Current `enforce_namespace_quota(...)` evolves from:

- preflight policy check

to:

- reservation acquisition / finalize / abort coordinator

### `runtime_io.rs`

Single-object write path should change from:

- check quota
n- allocate
- write
- route CAS
- reclaim old route

to:

- compute predecessor state
- reserve quota in metadata
- allocate
- write
- route CAS
- finalize quota reservation
- reclaim old route
- on failure: abort reservation and reclaim new allocations

### `runtime_write.rs`

Batch writes should not reserve quota per item independently without metadata protection.

Recommended batch rule:

- reserve per object in a deterministic key order, or
- add a backend-level batch reservation transaction for a set of object deltas in one tenant

The second option is preferable for throughput, but the first option is easier to ship first.

---

## Batch Write Semantics

Batch writes need explicit policy because quota admission can partially succeed.

## Recommended v1 behavior

For `batch_put` and related batch APIs:

- process reservations in stable key order
- if any reservation fails:
  - abort already-acquired reservations in the batch
  - return a conflict / quota exceeded error for the batch
- only after all reservations succeed do we proceed to data write and route publication

Pros:

- preserves all-or-nothing batch admission semantics
- simpler than partial batch acceptance

Cons:

- more metadata coordination

This is acceptable for a correctness-first v1.

---

## Delete Path

Delete must participate in authoritative quota accounting too.

### Required flow

1. load authoritative object accounting
2. route delete CAS succeeds
3. metadata finalization applies negative delta:
   - `used_bytes -= old_size`
   - `used_objects -= 1`
   - object accounting moves to `Deleted`
4. reclaim route allocations

Delete should not rely on asynchronous reclaim to refund tenant quota. The quota refund should happen when the delete becomes authoritative, not when bytes are later garbage-collected.

---

## Overwrite Path

Overwrite is the most important correctness case.

### Required invariant

Quota is charged on **committed object delta**, not on raw incoming payload size.

Example:

- current object size = 64 MiB
- max tenant bytes = 100 MiB
- current committed usage = 90 MiB
- overwrite with 65 MiB should succeed
  - delta = +1 MiB
- overwrite with 120 MiB should fail
  - delta = +56 MiB

This is one of the main reasons an explicit object accounting row is useful.

---

## Redis / etcd / in-memory backend implications

## Redis

Redis can support this design if the multi-key reserve/finalize/abort steps are implemented as Lua scripts with version checks.

Needed properties:

- atomic read-check-write across:
  - quota state key
  - object accounting key
  - reservation key
- deterministic conflict result
- no client-side race window between independent Redis commands

## etcd

etcd can support this design through compare-and-swap style transactions over versioned keys.

Needed properties:

- compare object accounting version
- compare quota state version
- write reservation and next state in one txn

## In-memory

In-memory backend should implement the same protocol directly under a mutex for test determinism.

---

## Observability

Add explicit metrics and logs so quota behavior is diagnosable.

### Suggested metrics

- `store_quota_reservation_total{result=ok|conflict|error}`
- `store_quota_finalize_total{result=ok|conflict|error}`
- `store_quota_abort_total{reason=write_failed|route_conflict|expired|repair}`
- `store_quota_pending_reservations`
- `store_quota_used_bytes`
- `store_quota_used_objects`
- `store_quota_reconcile_total{result=finalized|aborted|error}`

### Suggested logs

Every reservation/finalize/abort should log:

- tenant
- object key
- reservation id
- predecessor version
- delta bytes / objects
- final outcome

---

## Rollout Plan

## Phase 0: design-only preparation

- add this design doc
- align on metadata model and semantics
- decide whether object accounting is route-derived or separate

## Phase 1: metadata model and admin inspection

- add `TenantQuotaState`
- add `TenantQuotaReservation`
- add `TenantObjectAccounting`
- add backend support in in-memory, Redis, and etcd
- add admin read-only inspection commands

## Phase 2: single-object strict quota path

- implement reserve/finalize/abort for single `put`
- implement delete refund path
- gate behind an opt-in runtime/config flag if needed during bring-up
- add crash/retry tests

### Current single-object runtime behavior

For positive-delta writes, the current single-object path adds a bounded
tenant-local recovery step on top of metadata-authoritative
reserve/finalize/abort semantics:

- quota policy resolves from `LogicalObjectId.scope.tenant`, not only from
  the writer's default tenant
- when reservation returns `tenant quota bytes exceeded` or
  `tenant quota objects exceeded`, runtime may evict one older object from
  the same tenant and retry
- recovery is bounded by `MAX_TENANT_LOCAL_EVICTION_ATTEMPTS`
- victim selection stays tenant-local: skip the object being written,
  consider only `RouteState::Active`, and order candidates by older version
  first and then larger committed length
- if no same-tenant victim can free enough room, the original quota conflict
  is returned unchanged
- cross-tenant data is never evicted to satisfy another tenant's write

This keeps strict quota semantics intact: metadata reservation/finalize is
still authoritative, and self-eviction only frees capacity inside the same
tenant before the next retry.

## Phase 3: batch strict quota path

- implement deterministic multi-object reservation flow
- add batch rollback / abort semantics
- add conflict-heavy tests

## Phase 4: reconciliation automation

- background cleanup for expired reservations
- admin-triggered repair commands
- metrics / dashboards

## Phase 5: remove legacy best-effort path

- stop treating runtime-local preflight quota checks as authoritative
- keep them only as early rejection hints, or remove them entirely

---

## Alternatives Considered

## 1. Keep current preflight check and add more retries

Rejected.

Why:

- retry reduces visible failures
- it does **not** eliminate quota TOCTOU
- concurrent writers can still over-admit quota

## 2. Make route authorities own quota directly

Rejected for now.

Why:

- route authority in `EmbeddedWrh` is intentionally off-metadata and mirrored
- quota state wants a single durable authoritative plane
- tying quota correctness to mirrored route authority convergence adds unnecessary coupling

## 3. Use only periodic reconciliation/scanner-based enforcement

Rejected.

Why:

- this becomes best-effort quota, not strict quota
- unsuitable for hard multi-tenant isolation

## 4. Use distributed local credits from day one

Deferred.

Why:

- improves scalability under hotspot tenants
- but adds bounded over-reservation / debt complexity
- premature before we prove a single tenant quota row is a bottleneck

---

## Open Questions

1. Should the first strict implementation cover only tenant-level quota, or also domain/object_set scopes?
2. Should object accounting reuse route metadata for committed length, or keep a distinct object-accounting row? This doc recommends a distinct row.
3. Do we need a separate logical write id to make finalize/abort fully idempotent across client retries?
4. For batch writes, do we want all-or-nothing semantics only, or eventually partial acceptance with per-item results?
5. Should the initial reconciler be admin-driven only, or should we also run a lightweight background worker in the client/runtime?
6. Do we want the old `enforce_namespace_quota(...)` hook to remain as an early fast-fail hint even after strict metadata reservation exists?

---

## Recommended Decision

Adopt a **metadata-authoritative reservation + finalize/abort quota protocol** with these principles:

- tenant policy remains the source of quota limits
- metadata becomes the source of truth for quota usage
- object accounting is explicit and versioned
- route publication stays in the existing WRH authority path
- the user-visible write only succeeds after both route publication and quota finalization succeed
- background reconciliation is used only for crash recovery and cleanup, not for normal correctness

This gives Store-RS a quota model aligned with the mature systems that provide hard guarantees, while still fitting the current split between metadata, route authority, allocator, and storage owner.
