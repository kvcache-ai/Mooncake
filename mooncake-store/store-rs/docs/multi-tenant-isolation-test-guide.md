# Multi-Tenant Isolation Test Guide

This guide explains how to validate the full multi-tenant isolation feature in Store-RS.

The current feature spans more than one mechanism, so validation should be layered:

- end-to-end multi-tenant behavior in the main e2e harness
- strict quota admission, rejection, refund, and reuse checks
- QoS / fairness / bandwidth-shaping behavior checks
- admin-side inspection of authoritative tenant metadata
- targeted repair-path verification when interrupted runs leave pending reservations behind

## Validation Goals

A complete multi-tenant isolation validation should demonstrate all of the following:

- tenant policy is authored in metadata before tenant-scoped writers start
- runtimes resolve tenant-scoped policy from metadata instead of relying only on local fallback knobs
- multi-tenant data paths behave correctly in the main e2e harness
- strict quota admission is authoritative and rejects over-limit writes without drifting committed state
- delete refunds quota at authoritative delete time and makes capacity reusable immediately
- admin inspection surfaces expose the same authoritative tenant state operators need for debugging and repair
- QoS and bandwidth-related policy is distinguishable from strict quota and can be validated through the right runtime and lower-level paths

## Main Validation Layers

### 1. Main local e2e

Primary command:

```bash
scripts/e2e/run-local-e2e.sh
```

Recommended fast iteration command:

```bash
MC_STORE_RS_ENABLE_RDMA=0 \
MC_STORE_RS_BENCH_ITERS=1 \
scripts/e2e/run-local-e2e.sh
```

What this validates for multi-tenant isolation:

- general multi-tenant behavior in the real local harness
- strict tenant quota in the Redis-backed runtime path
- routed writes and multi-replica publication alongside tenant scoping
- lifecycle flows such as expansion, true client shrink, and hot-upgrade without regressing tenant behavior

Expected success signal includes both:

```text
strict tenant quota
```

and

```text
multi-tenant
```

in the final `e2e ok:` line.

### 2. Focused strict tenant quota scenario

The strict quota scenario lives in:

- `crates/mooncake-store-e2e/src/main.rs`

Relevant helpers:

- `put_tenant_quota_policy(...)`
- `verify_strict_tenant_quota(...)`

The scenario uses a dedicated tenant:

- `tenant-quota-e2e`

What it proves in order:

1. tenant quota policy is written to metadata before the quota-scoped writer starts
2. one write within quota succeeds
3. metadata quota state becomes committed after finalize
4. a second over-limit write is rejected
5. quota state and reservation counts do not drift on rejection
6. delete refunds quota at authoritative delete time
7. the refunded quota can be reused immediately
8. object accounting and reservation records remain visible for inspection

## Preconditions

Before running validation locally, ensure:

- Redis is available on the configured port
- the workspace builds successfully
- the local environment can run the standard e2e harness
- `mooncake-store-admin` can reach the same metadata namespace if you want follow-up inspection after the run

## Recommended Operator Validation Workflow

### Step 1: run the local e2e

```bash
MC_STORE_RS_ENABLE_RDMA=0 \
MC_STORE_RS_BENCH_ITERS=1 \
scripts/e2e/run-local-e2e.sh
```

### Step 2: confirm the success line

The final success line should include at least these markers:

- `strict tenant quota`
- `multi-tenant`

On RDMA-capable hosts, also watch for:

- `rdma bandwidth isolation`

A current successful run looks like:

```text
e2e ok: single put/get, strict tenant quota, batch put/get, request-level replication policy, true delete reclaim, routed remote write, multi-replica publish, registered-buffer path, overwrite reclaim, multi-tenant, scale-out, elastic expand-shrink, true client shrink, hot-upgrade, rdma bandwidth isolation
```

### Step 3: inspect authoritative tenant metadata

After the run, inspect the strict quota tenant through admin:

```bash
mooncake-store-admin \
  --metadata-url redis://127.0.0.1:6380/0 \
  quota state \
  --tenant tenant-quota-e2e

mooncake-store-admin \
  --metadata-url redis://127.0.0.1:6380/0 \
  quota reservations \
  --tenant tenant-quota-e2e
```

Optional object-level inspection:

```bash
mooncake-store-admin \
  --metadata-url redis://127.0.0.1:6380/0 \
  quota object \
  --tenant tenant-quota-e2e \
  --key <logical-key>
```

Expected interpretation after a clean success:

- `pending_reserved_*` should be `0`
- finalized reservations should explain the admitted write and the later refund path
- object accounting should match the final visible object state

### Step 4: inspect repair path if the run was interrupted

If the run crashes or is interrupted mid-flight and pending reservations remain visible, inspect the repair plan first:

```bash
mooncake-store-admin \
  --metadata-url redis://127.0.0.1:6380/0 \
  quota reconcile \
  --tenant tenant-quota-e2e \
  --dry-run
```

If operators want to explicitly terminate one visible reservation instead of asking for a reconcile plan, also inspect:

```bash
mooncake-store-admin \
  --metadata-url redis://127.0.0.1:6380/0 \
  quota abort \
  --tenant tenant-quota-e2e \
  --reservation-id <reservation-id> \
  --dry-run
```

Expected interpretation:

- a healthy completed run should usually leave no reconcile work
- `--dry-run` should be used first so operators can see what would be finalized, aborted, or explicitly terminated
- repair behavior is explicit; the system does not silently guess on tenant metadata cleanup
- `quota abort` is an operator-directed repair path, not the same thing as route repair or stale-authority recovery

## Additional Implemented Coverage To Validate

### Scope-selector isolation: `tenant` / `domain` / `object_set`

What to validate:

- different scope combinations do not bleed into each other
- routing and object identity reflect the effective scope
- `tenant-a/domain-a/set-a` remains distinct from `tenant-a/domain-b/set-b`

Current supporting anchors:

- `crates/mooncake-store-client/src/client/tests.rs` with `.domain(...)`, `.object_set(...)`, and `NamespaceScope::with_defaults(...)`

### Metadata-authored defaults: routing / placement / fairness / shaping

What to validate:

- runtimes resolve effective defaults from metadata-authored tenant policy
- behavior does not silently fall back to builder-local defaults when policy exists
- placement, fairness, and shaping match the authored policy fields

Current supporting anchors:

- `crates/mooncake-store-client/src/client/tests.rs` coverage around effective defaults such as `default_replica_count`, `preferred_storage_owners`, `preferred_segments`, and QoS-related knobs

### Placement defaults and preferences

What to validate:

- `default_replica_count` affects replica count in resulting routes
- `preferred_storage_owners` and `preferred_segments` influence placement outcomes when satisfiable
- placement falls back cleanly when preferences cannot be satisfied

### Route repair / stale-authority recovery

What to validate:

- dead-owner or stale-authority cases recover to a correct live route
- route repair is interpreted separately from quota repair
- tests do not treat route recovery as quota reconcile behavior

### Explicit `quota abort` operator path

What to validate:

- operators can inspect a visible reservation and use `quota abort --dry-run` before applying an abort
- this path is documented separately from `quota reconcile`

## QA Checklist

When handing this feature to QA, ask them to verify:

- the main e2e script exits successfully
- the final success line includes both `strict tenant quota` and `multi-tenant`
- there is no error indicating quota state drift, missing accounting, or missing reservations
- the fast validation command above passes consistently
- metadata inspection after the run matches the expected tenant-root strict quota behavior
- scope-selector behavior across `tenant` / `domain` / `object_set` remains isolated
- metadata-authored placement / fairness / shaping defaults are reflected in runtime behavior
- `quota reconcile --dry-run` shows no unexpected work after a healthy completed run
- `quota abort --dry-run` is available as an explicit operator repair path when inspecting visible pending reservations

## Lower-Level Supporting Coverage

The e2e suite is the process-level proof, but it is not the only coverage.

Supporting lower-level tests already cover strict quota semantics and several additional parts of multi-tenant isolation in:

- `crates/mooncake-store-client/src/client/tests.rs`

Examples include:

- over-limit write rejection
- overwrite delta charging
- delete refund behavior
- routed batch all-or-nothing quota admission
- scope-selector isolation across `tenant` / `domain` / `object_set`
- metadata-authored defaults for routing, placement, fairness, and shaping
- placement defaults and preferences, including replica count, preferred owners, and preferred segments
- route repair / stale-authority recovery paths
- fairness limits on remote batch item counts
- shaping limits on remote batch bytes and inflight bytes
- QoS-tier reclaim ordering, including `qos_tier_reclaims_low_priority_before_high_priority()`

These tests are complementary. The main e2e proves the Redis-backed runtime path exercises the logic in a real harness, while several scope-, placement-, repair-, and QoS-specific behaviors are still primarily proven at the lower-level runtime layer rather than through one single end-to-end scenario.

## What to Watch For

Common failure classes when testing this area:

- tenant policy missing from metadata before the tenant-scoped writer starts
- local fallback knobs masking missing admin-authored policy during manual runs
- pending reservations left behind after interrupted writes
- quota state drift between reservation/finalize/refund transitions
- object accounting not matching final authoritative object visibility
- `domain` / `object_set` carried on requests but not actually isolating namespace or route behavior
- metadata-authored placement / fairness / shaping defaults present in policy but not reflected in runtime behavior
- preferred owner / preferred segment / replica-count defaults not affecting placement outcomes
- route-repair and quota-repair semantics being conflated in test interpretation
- fairness / shaping knobs present in policy but not reflected in runtime batching behavior
- qos tier attached to objects or routes but not changing reclaim priority

## Mental Model for Testers

Use this model when interpreting results:

- policy is authored through admin
- runtime enforces policy on request paths
- strict quota state is authoritative in metadata
- admin inspection and repair commands are the operator-facing source of truth
- scope selectors, placement defaults, route repair, and QoS eviction priority each have their own validation surface and should not be collapsed into one generic “multi-tenant passed” claim
- QoS eviction-priority coverage currently includes lower-level reclaim ordering tests and should not automatically be read as full black-box eviction proof

Do not treat runtime-local quota configuration alone as the primary validation target anymore.

## Related Documents

- `docs/multi-tenant-isolation-user-guide.md` — operator and integrator usage guide
- `docs/deployment.md` — deployment workflow and admin command examples
- `docs/strict-tenant-quota-e2e-test-guide.md` — focused strict quota QA handoff guide
- `docs/tenant-quota-consistency-design.md` — strict quota protocol and repair semantics
- `docs/multi-tenant-admin-control-plane-design.md` — admin command model and control-plane design
