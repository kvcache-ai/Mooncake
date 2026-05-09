# Testing

This document describes the test suite that ships with `mooncake-store-rs`:
what it covers, how it is organised, and how to run it.

Everything in this document reflects the state of the current tree. Absolute
test counts are a point-in-time snapshot; the categorisation, infrastructure,
and conventions are the contract.

## Design Goals

- Prove that every public surface has at least one round-trip assertion, and
  that every known failure path is exercised as a negative test.
- Keep the hot code paths covered by property-based tests where a meaningful
  invariant exists, not just by hand-written cases.
- Simulate distributed-inference failure modes — transport disconnect,
  metadata unavailability, concurrent CAS, segment exhaustion — in-process
  rather than relying on external fault injectors.
- Keep the default test path hermetic: no real network, no stray files, and
  live Redis / etcd only in explicitly scoped integration tests that skip
  cleanly when the required binary or service is absent.

## Test Inventory

Workspace `cargo test --lib` runs 879 tests across the workspace crates listed
below. The Redis-backed integration tests inside `mooncake-metadata` and
`mooncake-store-py` skip silently when no `redis-server` binary is on `PATH`;
the etcd-backed integration tests skip when no local `etcd` binary is
available; all other tests run unconditionally.

| Crate | `--lib` tests | Notes |
|---|---:|---|
| `mooncake-store-client` | 537 | Dominant runtime; includes property tests and fault-injection integration |
| `mooncake-store-core` | 112 | Pure-type contracts: identity, route, compat, error, codec |
| `mooncake-metadata` | 111 | In-memory backend + keyspace + segment state + Redis / etcd integration |
| `mooncake-store-py` | 94 | PyO3 bindings, admin service, setup helpers (3 `#[ignore]`, including Redis-backed and etcd-backed admin maintenance tests) |
| `mooncake-transport` | 21 | Transport-core trait behaviour |
| `mooncake-transport-sys` | 4 | FFI shim sanity checks |
| `mooncake-store-test-utils` | 0 | Test-only crate — no self-tests |
| `mooncake-store-transport-core` | 0 | Trait definitions only |

Wall time on a warm cache stays in the low-20-second range, with
`mooncake-store-client` still responsible for most of that time because it
owns the fault-injection and property suites.

### Per-file breakdown — client tests

The client test suite is split across ten files under
`crates/mooncake-store-client/src/client/tests/`:

| File | `#[test]` fns | proptest fns | Focus |
|---|---:|---:|---|
| `mod.rs` | 118 | 0 | Large multi-client integration scenarios (membership, drain, hot-upgrade, evacuation, metrics) |
| `unit_tests.rs` | 101 | 0 | Pure-type contracts: `ObjectRef`/`PutRequest`/`ReplicationPolicy` builder chains, `flatten_slices` / `scatter_into_buffers`, zero-clamp, payload-checksum invariants, error display |
| `store_client_tests.rs` | 47 | 0 | `StoreClient` lifecycle (put/get/remove), batch APIs, dual-node read path, `FaultyTransport` integration, boundary inputs, benchmark logging |
| `adversarial.rs` | 35 | 6 | `align_up_u64` property tests, metadata-failure data-integrity scenarios |
| `fault_injection_prop.rs` | 42 | 8 | `FaultConfig` state machine + `FaultyTransport` contract (disconnect priority, counter semantics, injection roundtrip) |
| `lifecycle_tests.rs` | 34 | 0 | Hot-upgrade handoff flow, lifecycle state machine, segment drain/retire/expand, evacuation |
| `routing_tests.rs` | 20 | 0 | CAS route conflicts, query_route tenant scope, RouteVersion monotonicity, `get_into`/`get_size` buffer sizing, placement validation |
| `routing_prop.rs` | 20 | 6 | Route CAS/get property tests against counting and faulty backends |
| `transport_prop.rs` | 26 | 6 | `TestTransport` batch-id monotonicity, memory-registration idempotence, payload roundtrip |
| `quota_prop.rs` | 15 | 4 | Tenant quota state machine property tests |

Property tests run 32 / 64 / 512 cases depending on per-file
`ProptestConfig::with_cases(...)`.

### Inline module tests

Some `src/` modules carry `#[cfg(test)] mod tests` directly:

| Module | Tests | Focus |
|---|---:|---|
| `mooncake-store-client/src/memory.rs` | 23 | `LocalMemoryConfig` validation, watermark matrix, NUMA region distribution, startup registration hook coverage |
| `mooncake-store-client/src/route_directory.rs` | 14 | Embedded-WRH route selection |
| `mooncake-store-client/src/transport.rs` | 12 | Transfer request assembly and registration chunk boundaries |
| `mooncake-store-client/src/observability/mod.rs` | 12 | Metrics rendering, prometheus / stats JSON snapshots |
| `mooncake-store-client/src/control_plane/tests.rs` | 10 | Control-plane RPC dispatch and stream-session reuse |
| `mooncake-store-client/src/placement.rs` | 4 | Placement planner deterministic ranking |
| `mooncake-store-client/src/client/runtime_io.rs` | 3 | Local I/O helpers |
| `mooncake-store-client/src/client/membership_sync.rs` | 1 | Background cache refresh |

## Test Categories

Seven cross-cutting categories describe how a given test exercises the code
under test. A single file often mixes several categories.

### 1. Functional round-trip

Straightforward "set up → call → assert" tests that prove a single API
contract. Covers construction, CRUD, and basic error translation. Forms the
bulk of `unit_tests.rs` and the `mod.rs` integration scenarios.

### 2. Boundary and equivalence-class

Empty / zero / extreme / Unicode / truncated inputs. Examples:

- `payload_checksum(...)` remains deterministic and distinguishes order/length changes.
- `BandwidthShaping::max_remote_batch_bytes(0)` clamps to `Some(1)`.
- `parse_legacy_scoped_key` handles 10 000-character logical keys.
- `release_at_u64_max_offset_overflows_gracefully` rejects without panic.
- `encode_decode_round_trip_unicode` and `_all_bytes` cover percent encoding.

### 3. Adversarial

Crafted inputs that attack parser, codec, or validator paths:

- `decode_rejects_truncated_percent_at_end`, `decode_rejects_non_hex_after_percent`.
- `tenant_policy_scope_rejects_object_set_without_domain`.
- `tenant_quota_finalize_request_validates_state_length_combinations`
  (full state × length matrix).
- `reserve_on_retired_segment_is_rejected`.

### 4. Concurrency

Multi-thread races against shared state. `mooncake-metadata::in_memory`
carries three of these; `mooncake-metadata::redis_backend` has one more
(skipped without `redis-server`):

| Test | What it stresses |
|---|---|
| `concurrent_lease_upserts_for_distinct_stable_ids_all_land` | 20 threads upsert distinct `stable_id` leases; all 20 must be visible |
| `concurrent_cas_insert_on_same_key_exactly_one_winner` | 10 threads race `compare_and_swap_object_route(key, None, Some(route))`; exactly one winner, nine conflicts |
| `concurrent_segment_reserves_are_serialized_without_oversubscribing` | 10 threads × 64-byte reserve against a 640-byte segment; all succeed, the 641st byte fails with `Allocator` |
| `redis_backend_concurrent_route_cas_only_one_wins` | Same 5-thread race as above, but against real Redis Lua scripts |

### 5. Fault injection

Tests that drive code paths through injected failures. Three layers of
injection, all provided by `mooncake-store-test-utils`:

1. **Transport layer** — `FaultyTransport` wraps a `TestTransport` and lets
   a test disconnect the wire, fail the next N `open_segment` calls, fail
   the next N `submit_with_hints` calls, or add latency / jitter. Used by
   `fault_injection_prop.rs` (42 tests + 8 proptests) and the Phase-6 tests
   in `store_client_tests.rs`.
2. **Metadata layer** — `FaultyMetadataBackend` wraps any backend and
   returns a configured error after N successful calls. Used by
   `routing_prop.rs` and `quota_prop.rs` property tests.
3. **Observation layer** — `CountingMetadataBackend` records per-method
   call counts so tests can assert "we did not do two roundtrips".

### 6. Property-based

`proptest` generates inputs across a domain; the test asserts the
invariant must hold for every generated case.

- `align_up_u64` — result is a multiple of alignment, ≥ input, overshoot < one
  alignment unit, idempotent (512 cases).
- `FaultConfig::fail_next_opens(n)` — subsequent open calls fail exactly n
  times, then succeed (64 cases).
- `TestTransport` batch IDs are strictly increasing for every batch sequence
  length (32 cases).
- `compare_and_swap_object_route` — stale version always rejects (64 cases).
- Tenant policy version increments on every update (64 cases).

### 7. Integration

End-to-end flows that compose ≥2 real components without mocks.

- `mooncake-metadata::redis_backend::tests::redis_backend_*` — 35+ tests
  spin up a local `redis-server` (falling back to no-op if absent), exercise
  the full metadata surface, Lua-script atomicity, transient-error retry, and
  TTL-backed client resource hashes.
- `mooncake-metadata::etcd_backend::tests::etcd_backend_*` — 7 tests gated
  on a local `etcd` binary, covering round-trip metadata behavior plus expiry
  work-index and owner-scoped cleanup semantics.
- `mooncake-store-py::admin::service::tests::*stale_segments*` — 4 backend-integrated
  admin-maintenance scenarios verify dead-owner cleanup and live-owner skip
  semantics against both Redis and etcd metadata backends.
- `mooncake-store-client::client::tests::mod.rs` — 116 scenarios build
  multi-client topologies sharing an `InMemoryMetadataBackend` and verify
  cross-client handoff, drain, evacuation, metrics rendering.
  Restart-recovery regressions now also pin the stale cached remote-segment
  path: cached-handle reads must reopen by segment name, remote writes must
  refresh a stale cached handle once, and transport submit/open failures must
  still bubble into the outer soft-pin retry path instead of being mistaken
  for recoverable stale-cache noise.
  Registered-buffer regressions also pin the zero-copy data path: remote
  `batch_put_from` must submit caller buffer addresses directly, while
  registered-buffer `batch_get_into` must issue remote reads directly into
  the destination buffers instead of staging through scratch.

## Test Infrastructure: `mooncake-store-test-utils`

A `dev-dependencies`-only crate providing the shared primitives that let
integration and adversarial tests stay hermetic. It is consumed only by
`mooncake-store-client` today.

| Module | Provides | Purpose |
|---|---|---|
| `transport` | `TestTransport`, `TestTransportFactory`, `TestSegment`, `FaultConfig`, `FaultyTransport` | In-memory `StoreTransport` implementation so the client never needs real RDMA or TCP; fault-injection decorator on top |
| `metadata` | `CountingMetadataBackend`, `FaultyMetadataBackend`, `OperationCounts` | Observation and fault-injection decorators around any `MetadataBackend` |
| `fixtures` | `now_ms()`, `test_future_expiry_ms()` | Deterministic time helpers (lease expiry 30 s in the future) |
| `assertions` | `wait_for(timeout, interval, predicate)` | Polling assertion for async events (membership convergence, background sync) |

### `FaultConfig` — fault injection state

```rust
pub struct FaultConfig {
    pub disconnected: AtomicBool,
    pub submit_failures_remaining: AtomicU64,
    pub open_failures_remaining: AtomicU64,
    pub submit_call_count: AtomicU64,
    pub open_call_count: AtomicU64,
    pub jitter_ms: AtomicU64,
    // submit_latency, open_latency: parking_lot::Mutex<Duration>
}
```

Semantics:

- `disconnect()` sets `disconnected` to `true`; while disconnected, every
  `open_segment` and `submit_with_hints` returns `Transport(...)`.
- Disconnect takes priority over counters — a disconnected transport
  does not consume `fail_next_opens` / `fail_next_submits` credits.
- `fail_next_opens(n)` overwrites the counter (does not add). Each call
  decrements until zero, then subsequent calls succeed.
- Jitter is deterministic: `(call_count * 7 + 13) % jitter_ms`.
- `reset_counters()` zeros call counts but preserves injection state;
  `reset_all()` clears everything back to default.

## Route Migration Layered E2E

Route-migration verification is split across three layers so that runtime,
control-plane, and operator regressions are caught at the narrowest possible
scope first.

### Runtime / control-plane layer

Use
[scripts/tests/route-migration/test-route-migration-runtime-e2e.sh](../scripts/tests/route-migration/test-route-migration-runtime-e2e.sh)
for the runtime PR layer.

It validates:

- control-plane submit -> executor worker -> explicit move completion
- worker panic recovery without involving the admin queue

### Admin / operator layer

Use
[scripts/tests/route-migration/test-route-migration-admin-e2e.sh](../scripts/tests/route-migration/test-route-migration-admin-e2e.sh)
for the admin PR layer.

It validates:

- admin HTTP submit -> list -> status transitions
- `mooncake-store-admin` as the operator-side HTTP client for route migration

### Unit and in-process integration

- `crates/mooncake-store-client/src/client/tests/route_migration_tests.rs`
  covers explicit `copy` / `move` route transitions, CAS conflict handling,
  worker-failure recovery, and the main control-plane request / reply contracts.
- `crates/mooncake-store-client/src/control_plane/tests.rs`
  covers migration RPC validation and client-side decoding failures.
- `crates/mooncake-store-py/src/admin/*.rs`
  keeps admin HTTP / queue / CLI behaviour under in-process tests.

### Scripted E2E

Use [scripts/e2e/run-route-migration-e2e.sh](../scripts/e2e/run-route-migration-e2e.sh)
when the host already has a usable `cargo`, Python, Redis, and upstream build
environment.

Supported scenarios:

- `move`
- `copy`
- `copy-multi`

Important knobs:

- `MC_STORE_RS_ROUTE_MIGRATION_SUBMITTER=http|cli`
- `MC_STORE_RS_ROUTE_MIGRATION_SEQUENCE=copy-then-move|move-then-copy`
- `MC_STORE_RS_ROUTE_MIGRATION_REPEAT=<n>`
- `MC_STORE_RS_ROUTE_MIGRATION_KILL_EXECUTOR_AT=dispatching|running`

This script is the canonical black-box entry point for route-migration E2E in
this repository. It validates the full operator path: admin server, executor
selection, control-plane RPC, route publication, and post-migration readback.

### Dual-node fault-injection topology

Transport faults only affect the remote path (`open_segment` + `submit`).
Local `put()` writes to local memory and never touches the transport. Tests
that want to prove a transport fault actually blocks reads therefore use a
two-client topology:

```text
┌──────────────────┐                 ┌───────────────────┐
│ Writer           │  shared state   │ Reader            │
│ TestTransport    │◀───────────────▶│ FaultyTransport   │
│ local_memory=64K │                 │ storage_bytes=0   │
│ → put(k, v)      │                 │ → get(k) reads    │
│                  │                 │   remotely, hits  │
│                  │                 │   injected faults │
└──────────────────┘                 └───────────────────┘
```

See `store_client_tests::make_faulty_reader` for the pattern.

## Redis-backed Integration: Conditional Skip

`RedisTestServer::start()` in both `mooncake-metadata/src/redis_backend.rs`
and `crates/mooncake-store-py/src/admin/service.rs` spawns a local
`redis-server` child process on an ephemeral port. Every Redis-backed
integration test begins with:

```rust
let Some(server) = RedisTestServer::start() else { return; };
```

When the binary is absent the test returns `Ok(())` silently. This lets
the suite run on a laptop without Redis, while still exercising the
Redis paths on any CI image that includes it.

Metadata backend scenarios covered (9 new + 26 pre-existing):

- Backend round-trip for the full metadata surface.
- Concurrent route CAS — 5-thread race yields exactly one winner.
- Route CAS delete with wrong version is rejected.
- Route CAS version chain 1 → 2 → 3 via `expected-version` CAS.
- Multi-keyspace isolation (two backends sharing a URL but different
  prefixes see only their own leases).
- Handoff `put` / `get` roundtrip preserving all `HandoffPlan` fields.
- Segment lifecycle: draining segments reject new reservations; exhausted
  segments return `Allocator`; unpublish / republish cycle preserves state.
- Idempotent-write retry on transient connection loss.
- Stale lease expiry enforcement via TTL.
- Redis hash-tag slot verification (all keyspace-derived keys map to the
  same cluster slot — critical for Redis Cluster Lua scripts).

Admin service scenarios covered:

- due stale-owner entries remove orphaned segment metadata through the owner
  resource namespace and clear the consumed expiry-queue entries
- live owners that happen to appear in the expiry queue are re-checked and
  skipped instead of being cleaned speculatively

## Etcd-backed Integration: Conditional Skip

`EtcdTestServer::start()` in `mooncake-metadata/src/etcd_backend.rs` and
`crates/mooncake-store-py/src/admin/service.rs` spawns a local single-node
`etcd` child process on ephemeral client/peer ports. The readiness probe waits
for the server to accept traffic before the test continues.

When the binary is absent the test returns early. This keeps developer laptops
usable while still exercising the etcd-specific maintenance path anywhere the
binary is installed.

Etcd scenarios covered:

- same-epoch reclaim after a missing lease key
- backend-native due-expiry work indexing via `by-runtime` + `by-time` keys
- owner-scoped stale-segment cleanup without a steady-state full scan
- admin reconcile against real etcd metadata for both dead-owner cleanup and
  live-owner skip behavior

Redis scenarios covered:

- client leases and owned segment records share one TTL-backed resource hash
- owner-scoped cleanup deletes abnormal resource hashes whose lease field is gone

### Transient-error classifier

`should_retry_transient_redis_error` classifies connection-level failures
as retryable. The classifier is unit-tested without a live server:

- Every `std::io::ErrorKind` that represents a transport failure
  (`Interrupted`, `TimedOut`, `BrokenPipe`, `ConnectionRefused`,
  `ConnectionReset`, `ConnectionAborted`, `NotConnected`) classifies as
  transient.
- Message-pattern matching covers variants that do not travel through
  `io::ErrorKind` (`"connection refused"`, `"broken pipe"`, `"resource
  temporarily unavailable"`, etc.).
- Authentication, protocol, and read-only errors do NOT retry.

## Module Coverage Matrix

A qualitative view of which testing dimensions each module carries.
"✓" means at least one dedicated test; blank means coverage relies on
dependents.

| Module | Functional | Boundary | Adversarial | Concurrency | Fault | Property |
|---|:---:|:---:|:---:|:---:|:---:|:---:|
| `store-core::identity` | ✓ | ✓ | ✓ |   |   |   |
| `store-core::identity_codec` | ✓ | ✓ | ✓ |   |   |   |
| `store-core::compat` | ✓ | ✓ | ✓ |   |   |   |
| `store-core::route` | ✓ | ✓ | ✓ |   |   |   |
| `store-core::error` | ✓ | ✓ |   |   |   |   |
| `store-core::lifecycle` | ✓ |   |   |   |   |   |
| `metadata::in_memory` | ✓ | ✓ | ✓ | ✓ |   | via client |
| `metadata::redis_backend` | ✓ | ✓ | ✓ | ✓ | ✓ |   |
| `metadata::keyspace` | ✓ | ✓ | ✓ |   |   |   |
| `metadata::segment_state` | ✓ | ✓ | ✓ |   |   |   |
| `client::helpers` | ✓ | ✓ | ✓ |   |   | ✓ |
| `client::types` | ✓ | ✓ |   |   |   |   |
| `client::memory` | ✓ | ✓ | ✓ |   |   |   |
| `client::facade` | ✓ | ✓ |   |   | ✓ |   |
| `client::runtime_*` | ✓ | ✓ | ✓ | ✓ | ✓ |   |
| `client::route_directory` | ✓ |   |   |   |   | ✓ |

## Running Tests

### Full lib suite

```bash
cargo test --lib -- --test-threads=4
```

Matches the command CI runs. Completes in ≈17 s on a warm cache.

### Single crate

```bash
cargo test -p mooncake-store-client --lib
cargo test -p mooncake-metadata --lib
cargo test -p mooncake-store-core --lib
cargo test -p mooncake-store-py --lib
cargo test -p mooncake-store-py --bin mooncake-store-admin
```

### Single test file or pattern

```bash
cargo test -p mooncake-store-client --lib lifecycle_tests
cargo test -p mooncake-store-client --lib faulty_transport
cargo test -p mooncake-metadata --lib transient_error
```

### Property-test case count

Override the default case count for a one-off deep-dive:

```bash
PROPTEST_CASES=5000 cargo test -p mooncake-store-client --lib prop_align_up
```

### Remote workflow

Per `docs/skills/mooncake-store-rs-dev/SKILL.md`, the canonical remote
build-and-test environment is on `sg`:

```bash
rsync -avz crates/ sg:/root/mooncake-store-rs/crates/
ssh sg 'cd /root/mooncake-store-rs && source /root/.cargo/env \
  && cargo test -p mooncake-store-client --lib'
```

## CI Integration

`.aoneci/ci.yaml` keeps the broader merge-request checks such as `rust-lint`
and the full CI build/test flow.

`.aoneci/build-wheel.yaml` defines the wheel-only release flow as separate
native, wheel, and test stages:

- `build-native-artifacts` — builds the upstream Mooncake native shared
  libraries plus `libmooncake_classic_shim.so` and
  `libmooncake_tent_shim.so`, then uploads the static `native-artifacts`
  artifact for the current pipeline. The artifact also carries the upstream
  Mooncake Python wheel assets needed by `build-wheel`.
- `build-wheel` — downloads the current pipeline's static `native-artifacts`
  artifact, sets `MOONCAKE_REUSE_NATIVE_ARTIFACTS=1`, and builds the
  Rust/Python wheels without rerunning the upstream CMake build.
- `unit-test` — downloads the current pipeline's static `native-artifacts`
  artifact and runs
  `cargo test --lib -- --test-threads=4` with native builds disabled.
- `build-release-package-and-image` in `.aoneci/build-wheel.yaml` — reuses
  the `build-wheel` `build-artifacts` wheel output for OSS upload and
  nightly image assembly instead of starting a separate ABS wheel build.
  The runtime wheel is uploaded through the AoneCI `upload-oss` component to
  the `mooncake-pro` bucket under `nightly/$build_id/`.

Redis-backed lib tests inside `mooncake-metadata` and `mooncake-store-py`
run only if the CI image has a `redis-server` binary. Etcd-backed tests run
only if the image also includes a local `etcd` binary. Redis coverage runs on
the current CI image; etcd coverage depends on the builder image contents.

Property-test case budgets (32 / 64 / 512) are the per-file defaults; they
do not expand under CI.

## Adding Tests

### Where to put a new test

Decision tree:

1. **Pure type construction / ordering / serde?** → inline test module in
   the file that defines the type (`mooncake-store-core/src/identity.rs`,
   `route.rs`, `error.rs`, ...).
2. **Single-module internal helper?** → inline test module in the module
   file (`mooncake-store-client/src/memory.rs`,
   `mooncake-metadata/src/segment_state.rs`).
3. **Client builder / API surface contract?** →
   `mooncake-store-client/src/client/tests/unit_tests.rs`.
4. **Client + metadata integration scenario?** →
   `mooncake-store-client/src/client/tests/mod.rs` if it needs ≥2 clients,
   otherwise `store_client_tests.rs`.
5. **New adversarial boundary for an existing module?** → append to the
   matching `*_tests.rs` file under `client/tests/`.
6. **Property invariant?** → add to the nearest `*_prop.rs` file. Keep
   proptest case counts reasonable: 32 for expensive setups, 512 for
   cheap arithmetic.
7. **Metadata-backend scenario?** → inline test module in the backend
   file (`in_memory.rs` / `redis_backend.rs` / `etcd_backend.rs`). Use
   `RedisTestServer::start()` and the `Some(...) else { return; }`
   skip pattern for integration cases.

### Avoiding common pitfalls

- **Do not assert `is_ok() || matches!(err, ...)` as a hedge.** If the
  contract is "must succeed", assert success. If the contract is "two
  shapes are both valid", say so in the test name (see
  `remove_nonexistent_returns_not_found_or_ok` for the documented-ambiguity
  pattern).
- **Do not test a default value with `is_none() || == Some(N)`.** Pin the
  default with `assert_eq!(x, expected)`.
- **Local `put()` does not hit the transport.** Tests that want to prove
  a transport fault blocks a write must use the dual-node topology with
  a remote reader holding the `FaultyTransport`.
- **`InMemoryMetadataBackend::upsert_client_lease` enforces strictly-
  increasing epoch per `stable_id`.** When a test needs to seed an
  arbitrary epoch, call it in insertion order or use
  `allocate_client_lease` which auto-allocates.
- **Never add `#[ignore]` to make a flaky test pass.** Diagnose the race
  or remove the test.

## When to Read Which Document

- This file if you are adding tests or reviewing a test-heavy PR.
- `docs/architecture.md` for the runtime architecture under test.
- `docs/rust.md` for the public Rust API that tests exercise.
- `crates/mooncake-store-test-utils/src/` for the injection primitives.
