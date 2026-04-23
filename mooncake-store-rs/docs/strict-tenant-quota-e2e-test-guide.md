# Strict Tenant Quota E2E Test Guide

This document is for QA handoff of the new strict tenant quota end-to-end coverage in Store-RS.

## Goal

Validate that Store-RS exercises the new metadata-authoritative tenant quota logic in the real local e2e harness, not only in unit tests.

The e2e scenario proves all of the following in one run:

- tenant quota policy is authored in metadata before the quota-scoped writer starts
- a write within quota succeeds
- quota usage becomes committed metadata state after finalize
- a second over-limit write is rejected
- the rejection does not drift quota state or reservation count
- delete refunds quota at authoritative delete time
- the refunded quota can be reused immediately
- object accounting and reservation records are visible in metadata for the quota test tenant

## Code Location

The strict quota e2e scenario lives in:

- `crates/mooncake-store-e2e/src/main.rs`

Relevant helpers:

- `put_tenant_quota_policy(...)`
- `verify_strict_tenant_quota(...)`

## Preconditions

Before running the e2e locally, ensure:

- Redis is available on the configured port
- the workspace builds successfully
- the local environment can run the standard e2e harness
- `mooncake-store-admin` can reach the same metadata namespace if you want follow-up inspection after the run

The default local script uses the same conventions as the rest of the repository.

## Recommended Fast Validation Command

Use this command for QA verification with minimal benchmark noise:

```bash
MC_STORE_RS_ENABLE_RDMA=0 \
MC_STORE_RS_BENCH_ITERS=1 \
scripts/e2e/run-local-e2e.sh
```

## Expected Success Signal

The run should finish with an `e2e ok:` line that includes:

```text
strict tenant quota
```

Current success output includes:

```text
e2e ok: single put/get, strict tenant quota, batch put/get, request-level replication policy, true delete reclaim, routed remote write, multi-replica publish, registered-buffer path, overwrite reclaim, multi-tenant, scale-out, elastic expand-shrink, true client shrink, hot-upgrade, rdma bandwidth isolation
```

## What the Scenario Checks

The strict quota scenario uses a dedicated tenant:

- `tenant-quota-e2e`

It then performs these checks in order.

### 1. Seed tenant quota policy

A tenant-root quota policy is written to metadata before constructing the quota-specific writer.

Test shape:

- `max_bytes = value_size`
- `max_objects = 1`

Expected result:

- the subsequent writer for `tenant-quota-e2e` resolves and uses this strict quota policy

### 2. Successful admitted write

The scenario writes one object that exactly fits the quota.

Expected result:

- write succeeds
- another client can read the object back from the same tenant
- metadata quota state shows:
  - `used_bytes == value_size`
  - `used_objects == 1`
  - `pending_reserved_bytes == 0`
  - `pending_reserved_objects == 0`
- metadata object accounting exists and shows:
  - active state
  - committed length equal to payload size
- metadata reservations contain a finalized positive reservation for the object

### 3. Strict rejection without state drift

The scenario attempts a second object of the same size in the same tenant.

Expected result:

- write fails with a tenant quota conflict
- quota state remains unchanged from the successful first write
- reservation count remains unchanged

This proves the new metadata-authoritative admission path rejects without corrupting committed state.

### 4. Authoritative delete refund

The scenario deletes the first object.

Expected result:

- quota state returns to zero used bytes and zero used objects
- pending counters remain zero
- object accounting for the deleted object disappears
- reservations contain a finalized negative reservation for the refund

This proves quota is refunded at authoritative delete time, not only after later reclaim.

### 5. Immediate quota reuse

The scenario writes a new object of the same size after delete.

Expected result:

- write succeeds immediately
- another client can read it back

This is the final proof that delete refund made quota reusable right away.

## Suggested QA Checklist

When coordinating with QA, ask them to verify:

- the script exits successfully
- the final success line includes `strict tenant quota`
- there is no error indicating quota state drift, missing accounting, or missing reservations
- the scenario passes with the recommended fast command above
- metadata inspection after the run matches the expected tenant-root strict quota behavior

## Recommended Operator Follow-Up

After a successful or failed run, operators can inspect the same tenant through admin:

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

If the run is interrupted mid-flight and pending reservations remain visible, inspect the repair plan first:

```bash
mooncake-store-admin \
  --metadata-url redis://127.0.0.1:6380/0 \
  quota reconcile \
  --tenant tenant-quota-e2e \
  --dry-run
```

Expected operator interpretation:

- after a clean success, `pending_reserved_*` should be `0`
- finalized reservations should explain the admitted write and the later refund path during the terminal retention window
- `quota reconcile --dry-run` should usually report no work on a healthy completed run
- `Finalized` / `Aborted` reservations are retained for `24h` by default and then age out of Redis automatically

## Optional Follow-Up Checks

If QA needs deeper inspection, they can also confirm the repository already has supporting lower-level tests for strict quota semantics in:

- `crates/mooncake-store-client/src/client/tests.rs`

Examples include coverage for:

- over-limit write rejection
- overwrite delta charging
- delete refund behavior
- routed batch all-or-nothing quota admission
- tenant-local quota eviction observability via `mooncake_store_tenant_local_eviction_total`
- successful eviction-triggered writes remaining readable while tenant usage stays within quota
- cross-tenant isolation during tenant-local quota recovery

Those tests are complementary. The e2e described here is the process-level proof that the Redis-backed runtime path exercises the new logic in a real harness.

## Notes

- The e2e is intentionally focused and does not try to cover every admin repair flow.
- Its purpose is to prove the newly added strict quota runtime path is wired into the real end-to-end execution path.
- Repair-specific behaviors such as admin reconcile/abort remain covered by targeted tests elsewhere in the repository.
- The recommended mental model is now admin-authored policy plus runtime enforcement, not runtime-local quota configuration alone.
