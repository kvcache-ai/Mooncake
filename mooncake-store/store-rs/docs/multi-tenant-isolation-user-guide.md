# Multi-Tenant Isolation User Guide

This guide explains how to use Store-RS multi-tenant isolation as an operator or application integrator.

The current implementation follows one principle:

- author tenant-scoped policy through `mooncake-store-admin`
- let Store-RS runtimes enforce that policy on bootstrap and on request paths

Runtime-local builder, Python, CLI, and environment knobs still exist as compatibility fallbacks, but they are no longer the preferred place to author tenant policy.

## What Multi-Tenant Isolation Covers

Store-RS multi-tenant isolation currently spans these areas:

- tenant-scoped routing defaults such as `route_control` and `route_topk`
- tenant-scoped quota defaults such as `max_bytes` and `max_objects`
- placement defaults such as replica count, preferred storage owners, and preferred segment hints
- QoS-related defaults such as fairness and shaping knobs
- tenant namespace selection through `tenant`, `domain`, and `object_set`
- strict quota admission backed by authoritative metadata rather than best-effort request-local checks
- Python compatibility worker isolation through `keyspace` and `worker_scope`

Current scope model:

- tenant
- tenant + domain
- tenant + domain + object_set

Object identity is deterministic across that scope model: the default namespace keeps the legacy route key shape `tenant::logical_key`, while non-default `domain` / `object_set` scopes use a full-scope route key so the same tenant can store the same logical key in different scopes without collision.

Python compatibility clients can now select a default namespace scope at setup time with `tenant`, `domain`, and `object_set`, or through `MC_STORE_RS_TENANT`, `MC_STORE_RS_DOMAIN`, and `MC_STORE_RS_OBJECT_SET` when an integration layer such as SGLang cannot forward those fields directly. The compatibility API applies that default scope to both read and write paths unless the caller supplies an explicit tenant override. Treat `object_set` as an opaque namespace component; URI-like values such as checkpoint paths are percent-encoded in route keys rather than parsed or inferred from object key patterns.

In Phase 1 strict quota rollout, mutable quota state is still tenant-root metadata. Nested selectors remain useful for object-accounting lookups and for future policy expansion.

## Recommended Control-Plane Model

Use this split of responsibilities:

- `mooncake-store-admin` writes tenant policy and runs explicit inspection / repair
- Store-RS runtimes enforce routing, quota, placement, and request-path isolation
- `tenant` remains the default namespace selector used for startup policy lookup and request builders

For metadata backends that support tenant binding, each Store-RS runtime binds metadata to its default tenant during bootstrap. Runtime control-plane metadata such as client leases, client resource records, object routes, and handoff plans is stored under a tenant-rooted keyspace like `<prefix>/tenants/<tenant>/...`; route-authority discovery uses that tenant-bound metadata namespace too. This is separate from request-path object identity: route keys still carry deterministic tenant/domain/object-set identity, but metadata listing and lookup for runtime state starts from the tenant root instead of scanning a global root and filtering in process.

This keeps management explicit while avoiding control-plane policy decisions in the fast path.

## Python Compatibility Isolation Model

The Python compatibility layer has two separate isolation knobs and they should not be treated as synonyms:

- `keyspace` isolates metadata-backed routing, policy lookup, and object visibility
- `worker_scope` isolates compat-local worker state such as the Python dispatcher runtime, local hot cache domain, tracked-key registry, and dummy side-channel namespace

Current defaulting behavior:

- if `worker_scope` is set explicitly, that value is used
- otherwise Python derives `worker_scope` from `keyspace` when `keyspace` is present
- otherwise real-mode Python setups allocate a unique per-setup worker scope so separate setups do not silently share compat-local state; dummy-mode setups without an explicit scope fall back to the legacy `worker-1` compat scope so they stay compatible with standalone daemon side-channel aliases

This means two Python setups can intentionally share metadata namespace through the same `keyspace` while still keeping their compat-local caches and worker state isolated by different `worker_scope` values.

## Policy Precedence

Tenant-scoped policy resolution is ordered like this:

1. admin-managed tenant policy stored in metadata
2. legacy compatibility metadata reads where still supported during migration
3. runtime-local builder / Python / standalone client fallback values when metadata does not provide the relevant section

Practical takeaway:

- if a tenant policy exists in metadata, treat it as the source of truth
- local `route_topk`, `route_control`, `namespace_quota`, `execution_fairness`, and `bandwidth_shaping` are fallback knobs, not the preferred authoring surface

## Basic Operator Workflow

### 1. Write tenant policy

Set tenant-scoped routing, quota, and QoS policy through admin:

```bash
mooncake-store-admin \
  --metadata-url redis://127.0.0.1:6380/0 \
  policy set \
  --tenant tenant-a \
  --route-topk 3 \
  --route-control embedded-wrh \
  --max-bytes 1048576 \
  --max-objects 10 \
  --max-remote-batch-items-per-tenant 16 \
  --max-remote-batch-bytes 1048576 \
  --max-remote-batch-burst-items 32 \
  --max-inflight-bytes-per-batch 4194304
```

Common related commands:

```bash
mooncake-store-admin \
  --metadata-url redis://127.0.0.1:6380/0 \
  policy list

mooncake-store-admin \
  --metadata-url redis://127.0.0.1:6380/0 \
  policy get \
  --tenant tenant-a
```

`policy list --tenant <tenant>` pushes the tenant filter into metadata backends, so Redis and etcd only read the tenant-root policy plus that tenant's nested policy subtree instead of listing every tenant policy and filtering in process.

Use `--keyspace <prefix>` when the deployment keeps multiple environments or tenants in separate metadata namespaces.

## 2. Start runtimes with tenant identity

A runtime should still declare its tenant identity even when policy lives in metadata.

Standalone client example:

```bash
mooncake-store-client \
  --metadata-url redis://127.0.0.1:6380/0 \
  --stable-id tenant-a-store-1 \
  --tenant tenant-a \
  --state active \
  --storage-bytes 268435456 \
  --scratch-bytes 16777216
```

Rust example:

```rust
let client = StoreClientBuilder::new(metadata, "tenant-a-store-1")
    .tenant("tenant-a")
    .state(ClientLifecycleState::Active)
    .local_memory(
        LocalMemoryConfig::new()
            .storage_bytes(256 * 1024 * 1024)
            .scratch_bytes(16 * 1024 * 1024)
            .location("cpu:0"),
    )
    .with_tent(engine)
    .transport_factory(factory)
    .build(now_ms() + 600_000)?;
```

Important points:

- `tenant(...)` selects the default scope used for startup policy lookup and request builders
- request-scoped APIs and per-request replication settings are still normal execution-time inputs
- transport and memory setup remain runtime responsibilities even when route / quota policy is metadata-managed

Python real-mode example:

```python
from mooncake import MooncakeDistributedStore

store = MooncakeDistributedStore()
store.setup({
    "local_hostname": "127.0.0.1",
    # transport_metadata_url omitted: defaults to P2PHANDSHAKE in the dict-form path.
    "global_segment_size": 256 * 1024 * 1024,
    "local_buffer_size": 16 * 1024 * 1024,
    "protocol": "tcp",
    "rdma_devices": "",
    "metadata_url": "redis://127.0.0.1:6380/0",            # arg7: Store-RS metadata backend
    "tenant": "tenant-a",
    "domain": "sglang-chat",
    "object_set": "deepseek-r1__2026-04-19-build-44",
    "keyspace": "tenant-a-prod",
    "worker_scope": "py-worker-a",
})
```

Use real mode when Python should participate directly in the same distributed runtime as Rust clients. If the caller cannot pass `domain` / `object_set`, set `MC_STORE_RS_DOMAIN` and `MC_STORE_RS_OBJECT_SET` before startup so compatibility reads, writes, route queries, removes, size checks, and local hot-cache keys all use the same default namespace scope.

Python dummy-mode example:

```python
from mooncake import MooncakeDistributedStore

store = MooncakeDistributedStore()
store.setup_dummy(
    256 * 1024 * 1024,
    16 * 1024 * 1024,
    "127.0.0.1:50051",
    keyspace="tenant-a-prod",
    worker_scope="dummy-worker-a",
)
```

Use dummy mode when Python should attach to a standalone `mooncake-store-client` daemon. In dummy mode, clients only share shm-backed hot-cache hits and side channels when they intentionally use the same worker-scoped dummy server boundary.

## Placement policy notes

Tenant placement policy can provide default replica and routing hints, including `preferred_storage_owners` and `preferred_segments`.

Important semantics:

- request-level `ReplicationPolicy.preferred_segments` is still the explicit pinning surface
- request-level `preferred_segments` stays hard by default and only becomes best-effort when `with_soft_pin=true`
- tenant policy `placement.preferred_segments` is a default placement hint, not a mandatory constraint
- tenant-policy preferred segments are tried first when they exist, but missing / stale entries only emit a warning and then fall back to normal placement
- `preferred_segments` must contain the exact active segment name, not a stable id such as `store-a`
- because segment names are process-scoped and can churn across restarts, stale tenant-policy segment hints are expected to happen occasionally
- placement patch merge is sticky: omitting `preferred_segments` from a later policy patch does not clear an older stored value

Operational takeaway:

- prefer `preferred_storage_owners` when you want a more stable long-lived hint
- use `preferred_segments` only when you intentionally want to bias toward a specific currently active segment
- clear or overwrite bad `preferred_segments` explicitly if an earlier policy write stored the wrong value

## QoS and bandwidth policy notes

The current tenant policy surface includes these QoS-related defaults:

- `execution_fairness.max_remote_batch_items_per_tenant`
- `bandwidth_shaping.max_remote_batch_bytes`
- `bandwidth_shaping.max_remote_batch_burst_items`
- `bandwidth_shaping.max_inflight_bytes_per_batch`

A practical reading of them is:

- `max_remote_batch_items_per_tenant` limits how many items one tenant can occupy in a remote batch
- `max_remote_batch_bytes` caps total bytes per remote batch
- `max_remote_batch_burst_items` caps short burst size in item count
- `max_inflight_bytes_per_batch` caps in-flight batch bytes and is part of bandwidth shaping / isolation behavior

Keep this distinction clear:

- strict quota governs admission and authoritative metadata accounting
- fairness and shaping govern request-path batching, burst control, and bandwidth occupancy

They are all part of the multi-tenant isolation model, but they are not the same mechanism.

If you inspect tenant policy through admin, these values should appear as metadata-authored policy rather than only runtime-local fallback knobs.

## 3. Inspect authoritative tenant state

Inspect strict quota and object-accounting state through admin:

```bash
mooncake-store-admin \
  --metadata-url redis://127.0.0.1:6380/0 \
  quota state \
  --tenant tenant-a

mooncake-store-admin \
  --metadata-url redis://127.0.0.1:6380/0 \
  quota object \
  --tenant tenant-a \
  --key object-a

mooncake-store-admin \
  --metadata-url redis://127.0.0.1:6380/0 \
  quota reservations \
  --tenant tenant-a \
  --state pending
```

Interpretation notes:

- `quota state` and `quota reservations` reflect authoritative metadata state, not best-effort local counters
- `quota object` is the fastest way to inspect one logical object's committed accounting record
- when inspecting non-default `domain` / `object_set` scopes, admin uses the same deterministic full-scope object key as request serving; it does not probe legacy keys as a fallback
- in the current strict-quota rollout, quota state and reservations collapse to the tenant-root scope

## 4. Run explicit repair when needed

When operators suspect an interrupted write left visible pending reservations, inspect the repair plan first:

```bash
mooncake-store-admin \
  --metadata-url redis://127.0.0.1:6380/0 \
  quota reconcile \
  --tenant tenant-a \
  --dry-run
```

Repair guidance:

- use `--dry-run` first to see what admin would finalize or abort
- healthy completed runs should usually leave no reconcile work
- admin repair is explicit by design; Store-RS does not silently guess on tenant metadata cleanup

## Strict Quota Semantics

Strict quota is part of the multi-tenant isolation model, not a separate optional add-on.

Current behavior:

- single-object `put` reserves quota in metadata before allocation/write
- route publication happens before quota finalization becomes visible as success
- `remove` refunds quota when the route delete CAS becomes authoritative
- overwrites are charged on committed byte delta
- routed `batch_put` reserves quota for every item before storage reservation proceeds and finalizes only after each route CAS is authoritative

This avoids the older best-effort model where concurrent writers could over-admit on stale snapshots.

## Metadata Model Summary

Strict quota enforcement now relies on tenant metadata primitives:

- `TenantQuotaState` for committed and pending usage
- `TenantObjectAccounting` for authoritative committed object size/version
- `TenantQuotaReservation` for reserve/finalize/abort coordination

Backend behavior:

- in-memory backend applies the model under one write lock
- Redis uses backend-atomic Lua reserve/finalize/abort scripts
- etcd uses multi-key compare-and-swap transaction loops

## QoS and bandwidth operational tips

- Prefer writing fairness and shaping defaults through admin policy instead of scattering them across runtime startup commands
- If metadata already provides tenant-scoped QoS policy, treat that as the source of truth and use builder / Python knobs only as compatibility fallbacks
- Current strict quota authoritative state is visible through quota / reservation / object-accounting inspection; QoS and bandwidth shaping are request-path execution behaviors, so they do not show up as the same quota metadata
- `rdma bandwidth isolation` in the e2e suite is a separate validation signal for bandwidth-isolation behavior, not a substitute for strict quota accounting checks

## Operational Tips

- Prefer `mooncake-store-admin policy ...` over embedding tenant route/quota settings into every runtime startup path
- Keep `tenant` explicit on runtime startup so policy lookup and request builders use the intended namespace
- Treat local route/resource knobs as bootstrap or compatibility fallbacks only
- For Redis ACL deployments, prefer `MC_REDIS_USERNAME` / `MC_REDIS_PASSWORD` when passwords contain URL-reserved characters
- Use `cleanup-stale-segments` for dead-owner segment metadata cleanup; this is separate from tenant quota reconcile

## Validation Entry Points

If you want to validate the feature end-to-end:

- `scripts/e2e/run-local-e2e.sh` covers multi-tenant behavior and strict tenant quota in the Rust e2e harness
- `scripts/e2e/run-python-compat-e2e.sh` covers the Python compatibility API surface
- `scripts/e2e/run-local-hot-cache-e2e.sh` validates real-mode local hot-cache reuse and dummy-mode shm-backed hot-cache reuse
- `scripts/sglang/run-sglang-hicache-dummy-compat.sh` and `scripts/sglang/run-sglang-hicache-real-compat.sh` validate the two Python execution modes against the HiCache compatibility flows
- `docs/deployment.md` describes the recommended operator workflow from policy authoring to local validation
- `docs/python.md` documents the current Python `keyspace` / `worker_scope` behavior in more detail

## Related Documents

- `docs/deployment.md` — runtime and operator deployment workflow
- `docs/python.md` — Python real-mode and dummy-mode isolation semantics
- `docs/configuration.md` — precedence and fallback behavior for tenant-scoped settings
- `docs/multi-tenant-admin-control-plane-design.md` — control-plane design and admin command model
- `docs/tenant-quota-consistency-design.md` — strict quota design and metadata protocol
- `docs/architecture.md` — metadata model and runtime integration details
- `docs/rust.md` — Rust client usage and tenant-scoped runtime notes
