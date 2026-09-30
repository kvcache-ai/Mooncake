# Store-RS Test and Validation Guide

Run component validation scripts from the Mooncake repository root unless a
procedure says otherwise. Each scenario lists its required CMake artifacts,
Python environment, and services. Run Rust unit-test commands from the
`mooncake-store-rs` directory. Hardware-specific procedures state their
topology requirements.

## Local Validation

### Rust e2e

Run the full Rust end-to-end suite and the built-in batch benchmark:

```bash
mooncake-store-rs/scripts/e2e/run-local-e2e.sh
```

What the script does:

- requires the three explicit absolute path variables from the prepare step
- starts a local Redis instance on port `6380` when needed
- sets `LD_LIBRARY_PATH` from `MOONCAKE_BUILD_DIR`
- runs `cargo run --release -p mooncake-store-e2e`

Supported script inputs:

| Variable | Default | Used By |
|----------|---------|---------|
| `MC_STORE_RS_REDIS_PORT` | `6380` | local Redis port |
| `MC_STORE_RS_BENCH_ITERS` | `64` | batch benchmark loop count passed into the local Rust e2e |
| `MC_STORE_RS_VALUE_SIZE` | `4096` | payload size used by e2e |
| `MOONCAKE_STORE_RS_DIR` | required absolute path | Store-RS source location |
| `MOONCAKE_ROOT_DIR` | required absolute path | Mooncake source location |
| `MOONCAKE_BUILD_DIR` | required absolute path | Mooncake CMake build output |
| `MOONCAKE_PYTHON_BIN` | required for Python e2e | virtualenv interpreter with the root wheel built using `WITH_STORE_RS=ON` |

### Python compatibility e2e

Run the Python compatibility validation with a root wheel installed in a
virtualenv. Set `MOONCAKE_PYTHON_BIN` to that environment's interpreter; the
script selects `MOONCAKE_STORE_BACKEND=rs` and imports the installed package.

```bash
MOONCAKE_STORE_RS_DIR=/path/to/Mooncake/mooncake-store-rs \
MOONCAKE_PYTHON_BIN=/path/to/venv/bin/python \
mooncake-store-rs/scripts/e2e/run-python-compat-e2e.sh
```

What the script does:

- requires a root wheel built with `WITH_STORE_RS=ON`
- creates two Python clients
- validates single-object, batch, zero-copy, multi-buffer, route, and metrics paths

### Local hot-cache e2e

Run the daemon-local hot-cache validation:

```bash
MOONCAKE_STORE_RS_DIR=/path/to/Mooncake/mooncake-store-rs \
MOONCAKE_PYTHON_BIN=/path/to/venv/bin/python \
mooncake-store-rs/scripts/e2e/run-local-hot-cache-e2e.sh
```

The script selects Store-RS from the installed root wheel.

What the script does:

- runs the installed Store-RS Python extension and standalone client command
- Phase A validates that a real-mode reader reuses daemon-local cached bytes after the origin key is removed remotely
- Phase B validates that two dummy clients attached to one standalone daemon reuse a shm-backed hot-cache hit
- starts a temporary Redis instance automatically and tears it down after the run

Important inputs:

| Variable | Default | Used By |
|----------|---------|---------|
| `MC_STORE_RS_LOCAL_HOT_CACHE_E2E_REDIS_PORT` | auto | temporary Redis port |
| `MC_STORE_RS_LOCAL_HOT_CACHE_E2E_STORAGE_BYTES` | `64 MiB` | storage bytes for the local standalone daemon |
| `MC_STORE_RS_LOCAL_HOT_CACHE_E2E_SCRATCH_BYTES` | `16 MiB` | scratch bytes per local client |
| `MC_STORE_RS_LOCAL_HOT_CACHE_E2E_CACHE_BYTES` | `1 MiB` | hot-cache capacity under test |
| `MC_STORE_RS_LOCAL_HOT_CACHE_E2E_BLOCK_BYTES` | `8192` | hot-cache block size under test |
| `MOONCAKE_PYTHON_BIN` | required | virtualenv interpreter with a root wheel built with `WITH_STORE_RS=ON` |

### Real-mode read/write validation

Run the real-mode black-box validator:

```bash
MOONCAKE_STORE_BACKEND=rs "$MOONCAKE_PYTHON_BIN" \
  ./python/tests/store/rs/clients/real_client_rw.py --help
```

What the script does:

- validates the current store-rs compatibility path instead of an upstream master-based path
- accepts `--local_host host:port` and normalizes the embedded port into `transport_rpc_port`
- can act as a storage process with `--mode idle`
- can validate routed rw-only write and read flows with `--storage-bytes 0 --routed-writes`
- supports single-op and batch put/get validation through `--batch_size`

Recommended split-deployment pattern:

- storage node: `--storage-bytes > 0 --mode idle`
- routed writer: `--storage-bytes 0 --routed-writes --mode write`
- reader: `--storage-bytes 0 --routed-writes --mode read`

### Same-binary hot-upgrade validation

Native CLI hot-upgrade validation:

```bash
mooncake-store-rs/scripts/tests/client/test-client-hot-upgrade-cli.sh
```

What the script does:

- builds the standalone `mooncake-store-rs-client` binary
- starts an active predecessor and a standby successor with the same `stable_id`
- writes a real payload through an external routed client
- sends `SIGTERM` to trigger graceful handoff
- verifies that the successor promotes itself and can still read the original payload

Python hot-upgrade argument and wrapper compatibility validation:

```bash
mooncake-store-rs/scripts/tests/client/test-python-client-hot-upgrade-args.sh
```

What the script verifies:

- PyO3 native `setup(..., stable_id, initial_state)` argument parsing; the metadata backend assigns the epoch
- Python wrapper forwarding of hot-upgrade startup arguments into the Rust runtime

### Versioned V1-to-V2 rolling upgrade

Run `mooncake-store-rs/scripts/tests/rolling/test-rolling-upgrade-e2e.sh` from
the repository root. The script temporarily patches Store-RS source and the
protobuf definition, builds V1 and V2 client binaries in the checkout's
`target` directory, and verifies data preservation through rolling handoff.
Its exit cleanup restores the modified source files.

The test requires an existing root CMake build and an installed root-package
Python environment:

| Variable | Required value |
|----------|----------------|
| MOONCAKE_ROOT_DIR | Absolute Mooncake repository root |
| MOONCAKE_STORE_RS_DIR | Absolute path to the repository's mooncake-store-rs directory |
| MOONCAKE_BUILD_DIR | Existing root CMake build with TE and TENT |
| MOONCAKE_CLASSIC_SHIM_LIB_PATH | Existing classic-TE shim library |
| MOONCAKE_TENT_SHIM_LIB_PATH | Existing TENT shim library |
| MOONCAKE_PYTHON_BIN | Python interpreter with the root package installed |
| MC_STORE_RS_REDIS_PORT | Dedicated local Redis port; defaults to 6380 |

Run it from the repository root with those paths exported and a dedicated local
Redis port available:

:::{code-block} bash
mooncake-store-rs/scripts/tests/rolling/test-rolling-upgrade-e2e.sh
:::

The procedure in [Rolling upgrades](rolling-upgrade.md) describes the operator
handoff; this component script exercises the versioned V1/V2 path.

### Eviction validation

Native CLI eviction validation:

```bash
mooncake-store-rs/scripts/tests/client/test-client-eviction-cli.sh
```

What the script does:

- builds the standalone `mooncake-store-rs-client` binary
- starts a storage client with `/metrics` enabled
- uses an external routed Python client to issue `put`, `get`, and `batch_get`
- warms one key, then waits for background storage-owner eviction to reclaim the cold replica
- verifies both Prometheus metrics and tracing logs for the eviction path

For production dashboards, pair the in-process exporter with infrastructure exporters:

- use Mooncake Store RS for request, lease, route, capacity, lifecycle, and process metrics
- use `node_exporter` or `cAdvisor` for host CPU, disk, filesystem, and network saturation

### HiCache compatibility validation

Run the compatibility checks for both Python execution modes:

```bash
```

What they validate:

- dummy path through the standalone compatibility server plus shm registration
- real path through the native distributed store runtime plus registered-buffer I/O

Deployment note:

- dummy mode needs a reachable `client_server_address`
- real mode needs a reachable `local_hostname + transport_rpc_port`
- `client_server_address` does not carry real-mode TENT traffic
- Python real-mode validation can provide `local_hostname + transport_rpc_port` either explicitly or through `--local_host host:port`

### SGLang HiCache integration

Example launch patterns:

- real-mode SGLang with an in-process rw-only client (`global_segment_size=0`)

```bash
MC_STORE_RS_TRANSPORT_BACKEND=classic_te \
SGLANG_HICACHE_MOONCAKE_REUSE_TE=0 \
python -m sglang.launch_server \
  --model-path /models/Qwen3-0.6B \
  --host 0.0.0.0 \
  --port 30000 \
  --enable-hierarchical-cache \
  --hicache-size 4 \
  --hicache-write-policy write_through \
  --hicache-io-backend direct \
  --hicache-mem-layout page_first_direct \
  --hicache-storage-backend mooncake \
  --hicache-storage-prefetch-policy wait_complete \
  --hicache-storage-backend-extra-config '{
    "local_hostname": "10.0.0.21:17121",
    "metadata_server": "P2PHANDSHAKE",
    "master_server_address": "redis://10.0.0.10:6379/0",
    "global_segment_size": 0,
    "protocol": "tcp",
    "device_name": "",
    "check_server": false
  }'
```

  The upstream sglang JSON keys `metadata_server` and `master_server_address` are accepted as aliases that map to `transport_metadata_url` and `metadata_url` respectively. `metadata_server` is forwarded to the Transfer Engine only (defaults to `P2PHANDSHAKE`; omitting the key from the JSON has the same effect). `master_server_address` carries the Store-RS metadata URL (`redis://...` or `etcd://...`, required); `master_server` and `master_server_addr` are equivalent aliases. `setup()` raises a `TypeError` when no metadata URL is supplied.

  Current upstream SGLang only forwards the legacy Mooncake fields from `--hicache-storage-backend-extra-config`: `local_hostname`, `metadata_server`, `global_segment_size`, `protocol`, `device_name`, `master_server_address`, `check_server`, `standalone_storage`, and `client_server_address`.

  Store-RS compatibility extensions such as `transport_backend`, `keyspace`, `stable_id`, `tenant`, `domain`, `object_set`, `labels`, `routed_writes`, `replica_count`, and `route_topk` are not forwarded by the current SGLang parser. The Python compatibility layer therefore treats environment variables as setup fallbacks when SGLang does not pass the new fields. Explicit Python `setup(...)` arguments still win over environment values.

Use these environment variables for SGLang real mode:

These are compatibility bridges because current upstream SGLang does not forward the full Store-RS setup surface. Prefer admin-managed tenant policy in metadata whenever the integration path allows it.

- `MC_STORE_RS_METADATA_URL` (dict-form `metadata_url` fallback) and `MC_STORE_RS_TRANSPORT_METADATA_URL` (dict-form `transport_metadata_url` fallback; defaults to `P2PHANDSHAKE`)
- `MC_STORE_RS_TRANSPORT_BACKEND=tent|classic_te`; default `classic_te`
- `MC_STORE_RS_KEYSPACE`, `MC_STORE_RS_STABLE_ID`, `MC_STORE_RS_TENANT`, `MC_STORE_RS_DOMAIN`, `MC_STORE_RS_OBJECT_SET`, `MC_STORE_RS_LABELS`
- `MC_STORE_RS_ROUTED_WRITES=1`, `MC_STORE_RS_REPLICA_COUNT=<n>`, `MC_STORE_RS_ROUTE_TOPK=<n>`
- `MC_STORE_RS_ROUTE_CONTROL=embedded_wrh|metadata_only`
- `MC_STORE_RS_TRANSPORT_RPC_PORT`, `MC_STORE_RS_LOCAL_SEGMENT_NAME`
- `MC_STORE_RS_INITIAL_STATE`, `MC_STORE_RS_EXPIRES_AT_MS`
- `MC_STORE_RS_METRICS_ADDR=host:port` to auto-start the Python real-client `/metrics` endpoint
- `MC_STORE_RS_TRACE_FILE=/path/to/real-client.log` to append real-client Rust logs to a dedicated file instead of the SGLang process stream
- `MC_STORE_RS_CONTROL_PLANE_THREADS=<n>` to tune concurrent control-plane RPC client capacity; default `2`
- `MC_STORE_RS_CONTROL_PLANE_SERVER_THREADS=<n>` to tune embedded control-plane gRPC server capacity; default `4`

`MC_STORE_RS_OBJECT_SET` is treated as an opaque namespace component. For model-serving deployments it can carry the active weight-version boundary, and compatibility reads/writes, including KVCache keys, will use that object set until the process is restarted or a future runtime hot-update API changes it. `MC_STORE_RS_LABELS` accepts either a JSON object or comma-separated `key=value` pairs, for example `MC_STORE_RS_LABELS='storage=false,pool=rw'`.

- dummy-mode SGLang through a standalone routed gateway

```bash
mooncake-store-rs-client \
  --local-hostname 10.0.0.21 \
  --metadata-url redis://10.0.0.10:6379/0 \
  --storage-bytes 0 \
  --scratch-bytes 16777216 \
  --protocol tcp \
  --transport-rpc-port 17121 \
  --stable-id sglang-gateway \
  --tenant default \
  --label pool=pool-a \
  --label storage=false \
  --routed-writes \
  --replica-count 2 \
  --route-topk 2 \
  --client-server-address 0.0.0.0:16590 \
  --metrics-addr 0.0.0.0:19101

SGLANG_HICACHE_MOONCAKE_REUSE_TE=0 \
python -m sglang.launch_server \
  --model-path /models/Qwen3-0.6B \
  --host 0.0.0.0 \
  --port 30000 \
  --enable-hierarchical-cache \
  --hicache-size 4 \
  --hicache-write-policy write_through \
  --hicache-io-backend direct \
  --hicache-mem-layout page_first_direct \
  --hicache-storage-backend mooncake \
  --hicache-storage-prefetch-policy wait_complete \
  --hicache-storage-backend-extra-config '{
    "standalone_storage": true,
    "client_server_address": "10.0.0.21:16590",
    "check_server": false,
    "prefetch_threshold": 32
  }'
```

- real mode uses `setup(...)` and does not use `client_server_address`
- dummy mode uses `setup_dummy(...)` and only needs `client_server_address`
- dummy mode also consumes `MC_STORE_RS_KEYSPACE` as a Python wrapper fallback when `setup_dummy(...)` does not pass `keyspace`, aligning the dummy side-channel namespace with the routed gateway

### Multi-client stress benchmark

Run the process-per-client stress benchmark:

```bash
mooncake-store-rs/scripts/e2e/run-multi-client-stress.sh
```

What the script does:

- auto-starts local Redis when needed
- builds the release Python compatibility runtime
- starts dedicated storage processes
- starts dedicated rw processes
- runs `put`, `get`, `batch-put`, and `batch-get` phases
- prints a final steady-state bandwidth summary

Example summary:

```text
steady-state bandwidth:
- put: 24.82 MiB/s
- get: 27.22 MiB/s
- batch-put: 45.31 MiB/s
- batch-get: 109.52 MiB/s
```

Use this summary as the primary throughput readout. The longer `stress phase=...` lines still include setup and prepare timing for debugging, but they are not the main throughput signal.

Important stress-benchmark inputs:

| Variable | Default | Meaning |
|----------|---------|---------|
| `MC_STORE_RS_STRESS_STORAGE_CLIENTS` | `4` | number of dedicated storage processes |
| `MC_STORE_RS_STRESS_WRITER_CLIENTS` | `8` | number of concurrent rw worker processes |
| `MC_STORE_RS_STRESS_WRITER_STORAGE_BYTES` | `0` | local storage bytes owned by rw workers |
| `MC_STORE_RS_STRESS_VALUE_SIZE` | `4096` | payload size per object |
| `MC_STORE_RS_STRESS_BATCH_SIZE` | `32` | objects per batch request |
| `MC_STORE_RS_STRESS_SINGLE_ITERS` | `256` | steady-state single-request iterations per worker |
| `MC_STORE_RS_STRESS_BATCH_ITERS` | `128` | steady-state batch iterations per worker |
| `MC_STORE_RS_STRESS_WARMUP_ITERS` | `16` | pre-measurement warmup iterations per worker |
| `MC_STORE_RS_STRESS_PHASES` | `put,get,batch-put,batch-get` | comma-separated phase list |
| `MC_STORE_RS_STRESS_ROUTE_CONTROL` | `metadata_only` | route mode used by the benchmark |

For the shipped operator-facing benchmark, correctness checker, and soak runner,
use mooncake-store-rs-bench; see the [benchmark guide](../../performance/mooncake/store-rs-benchmark.md). Its default mode is scratch-only
RW benchmarking (`MC_BENCH_STORAGE_BYTES=0`) against separate `storage=true`
daemons, and it joins `mc/store-rs/v2` when no explicit keyspace is provided.

## Validation Coverage

The current end-to-end binary covers:

- single and batch put/get
- registered-buffer and multi-buffer paths
- request-level replication policy
- overwrite reclaim and delete reclaim
- routed writes and multi-replica publication
- multi-tenant access
- dynamic expansion, true client shrink, and hot-upgrade handoff

Additional dedicated validation scripts cover:

- CLI-driven real/dummy read-write validation against standalone daemons
- CLI-driven hot-upgrade handoff with payload preservation
- CLI-driven eviction with metrics and tracing validation
- Python hot-upgrade startup argument parsing and wrapper forwarding

The entry point is `crates/mooncake-store-e2e/src/main.rs`.


## Test inventory and harness

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

`cargo test --lib` runs the workspace library tests. Redis-backed integration
tests in the metadata and admin crates skip when `redis-server` is unavailable;
etcd-backed tests skip when local `etcd` is unavailable. Unit tests in the
runtime, CLI, and Python-binding crates remain in their owning packages.

| Crate | Test ownership |
|---|---|
| `mooncake-store-client` | Store SDK runtime, routing, allocator, and fault-injection/property tests |
| `mooncake-store-rs-runtime` | Python compatibility configuration, dispatcher, dummy service, and shared-memory runtime tests |
| `mooncake-store-rs-admin` | Admin service, HTTP queue, maintenance, and admin command tests |
| `mooncake-store-rs-cli` | Standalone client and benchmark command tests |
| `mooncake-store-py` | PyO3 bindings and Python-facing buffer/tensor wrapper tests |
| `mooncake-store-core` | Pure-type contracts: identity, route, compat, error, codec |
| `mooncake-store-rs-metadata` | In-memory backend, keyspace, segment state, and Redis / etcd integration |
| `mooncake-store-rs-transport` | Transport-core trait behaviour |
| `mooncake-store-rs-transport-sys` | Rust FFI boundary checks for CMake-built shims |
| `mooncake-store-test-utils` | Test-only fixtures and decorators |
| `mooncake-store-transport-core` | Trait definitions |

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

Multi-thread races against shared state. `mooncake-store-rs-metadata::in_memory`
carries three of these; `mooncake-store-rs-metadata::redis_backend` has one more
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

- `mooncake-store-rs-metadata::redis_backend::tests::redis_backend_*` — 35+ tests
  spin up a local `redis-server` (falling back to no-op if absent), exercise
  the full metadata surface, Lua-script atomicity, transient-error retry, and
  TTL-backed client resource hashes.
- `mooncake-store-rs-metadata::etcd_backend::tests::etcd_backend_*` — 7 tests gated
  on a local `etcd` binary, covering round-trip metadata behavior plus expiry
  work-index and owner-scoped cleanup semantics.
- `mooncake-store-rs-admin::admin::service::tests::*stale_segments*` — 4 backend-integrated
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
[mooncake-store-rs/scripts/tests/route-migration/test-route-migration-runtime-e2e.sh](https://github.com/kvcache-ai/Mooncake/blob/main/mooncake-store-rs/scripts/tests/route-migration/test-route-migration-runtime-e2e.sh)
for the runtime PR layer.

It validates:

- control-plane submit -> executor worker -> explicit move completion
- worker panic recovery without involving the admin queue

### Admin / operator layer

Use
[mooncake-store-rs/scripts/tests/route-migration/test-route-migration-admin-e2e.sh](https://github.com/kvcache-ai/Mooncake/blob/main/mooncake-store-rs/scripts/tests/route-migration/test-route-migration-admin-e2e.sh)
for the admin PR layer.

It validates:

- admin HTTP submit -> list -> status transitions
- `mooncake-store-rs-admin` as the operator-side HTTP client for route migration

### Unit and in-process integration

- `crates/mooncake-store-client/src/client/tests/route_migration_tests.rs`
  covers explicit `copy` / `move` route transitions, CAS conflict handling,
  worker-failure recovery, and the main control-plane request / reply contracts.
- `crates/mooncake-store-client/src/control_plane/tests.rs`
  covers migration RPC validation and client-side decoding failures.
- `crates/mooncake-store-rs-admin/src/admin/*.rs`
  keeps admin HTTP / queue / CLI behaviour under in-process tests.

### Scripted E2E

Use [mooncake-store-rs/scripts/e2e/run-route-migration-e2e.sh](https://github.com/kvcache-ai/Mooncake/blob/main/mooncake-store-rs/scripts/e2e/run-route-migration-e2e.sh)
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

`RedisTestServer::start()` in both `mooncake-store-rs-metadata/src/redis_backend.rs`
and `crates/mooncake-store-rs-admin/src/admin/service.rs` spawns a local
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

`EtcdTestServer::start()` in `mooncake-store-rs-metadata/src/etcd_backend.rs` and
`crates/mooncake-store-rs-admin/src/admin/service.rs` spawns a local single-node
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
cargo test -p mooncake-store-rs-metadata --lib
cargo test -p mooncake-store-core --lib
cargo test -p mooncake-store-rs-runtime --lib
cargo test -p mooncake-store-rs-admin --lib
cargo test -p mooncake-store-rs-cli --bins
cargo test -p mooncake-store-py --lib
cargo test -p mooncake-store-rs-admin --bin mooncake-store-rs-admin
```

### Single test file or pattern

```bash
cargo test -p mooncake-store-client --lib lifecycle_tests
cargo test -p mooncake-store-client --lib faulty_transport
cargo test -p mooncake-store-rs-metadata --lib transient_error
```

### Property-test case count

Override the default case count for a one-off deep-dive:

```bash
PROPTEST_CASES=5000 cargo test -p mooncake-store-client --lib prop_align_up
```

Redis-backed lib tests inside `mooncake-store-rs-metadata` and `mooncake-store-rs-admin`
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
   `mooncake-store-rs-metadata/src/segment_state.rs`).
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
- [Architecture](../../design/store/store-rs/architecture.md) for the runtime architecture under test.
- [Rust API](../../api-reference/rust/store-rs.md) for the public API that tests exercise.
- `crates/mooncake-store-test-utils/src/` for the injection primitives.

## Cold Tier Validation Scenarios

- write returns after DRAM publish even before backend materialization
- successful offload marks `cold_backing = Materialized`
- eviction never removes a hot replica without materialized cold backing
- last-hot eviction produces a cold-only route instead of dropping the object
- read miss restores from backend and can return data even if promotion fails
- two-client remote restore covers an embedded storage client that writes, evicts to a materialized cold-only route, reads back from cold tier, and allows a second client to trigger owner-side cold read
- owner-side staging ACK releases read pins and recycles staging slots without leaking or prematurely reusing memory
- batch cold restore uses backend batch read/read-into paths when objects share a backend/device
- overwrite/delete clean up old cold objects
- startup/heartbeat rebuild can materialize pending offloads from persisted pending source
- a full e2e flow covers write -> materialize -> cold-only eviction -> backend restore -> overwrite -> delete
- legacy backend-object routes remain readable during migration


The scenarios above describe correctness coverage for the cold-tier design. Use the root Store-RS smoke entry and the scenario-specific commands in this guide to run validation.

## Python Validation Scenarios

## Standard Read/Write Validation

Use the repository-standard entry point to validate both compatibility paths in one run:

```bash
mooncake-store-rs/scripts/tests/client/test-client-rw-cli.sh
```

This script:

- builds the standalone `mooncake-store-rs-client` binary
- starts two storage daemons against a temporary Redis metadata backend
- validates dummy single-item, shm batch, and shm multi-buffer read/write
- validates real routed write/read across separate real clients

For path-specific manual checks, keep using the paired helper scripts below.

## Local Hot-Cache Validation

Run the dedicated local hot-cache e2e:

```bash
MOONCAKE_STORE_RS_DIR=/path/to/Mooncake/mooncake-store-rs \
MOONCAKE_PYTHON_BIN=/path/to/venv/bin/python \
mooncake-store-rs/scripts/e2e/run-local-hot-cache-e2e.sh
```

This script validates two phases:

- Phase A: a real-mode reader reuses daemon-local cached bytes after the origin key is removed remotely
- Phase B: two dummy clients attached to one standalone daemon reuse a shm-backed hot-cache hit

Useful inputs:

- `MC_STORE_RS_LOCAL_HOT_CACHE_E2E_REDIS_PORT` to pin the temporary Redis port
- `MC_STORE_RS_LOCAL_HOT_CACHE_E2E_STORAGE_BYTES` and `MC_STORE_RS_LOCAL_HOT_CACHE_E2E_SCRATCH_BYTES` to size the local runtime
- `MC_STORE_RS_LOCAL_HOT_CACHE_E2E_CACHE_BYTES` and `MC_STORE_RS_LOCAL_HOT_CACHE_E2E_BLOCK_BYTES` to tune the cache under test

## Real-Mode Validation Script

Use `python/tests/store/rs/clients/real_client_rw.py` for black-box real-mode validation against the current store-rs compatibility stack.

Storage node:

```bash
MOONCAKE_STORE_BACKEND=rs "$MOONCAKE_PYTHON_BIN" ./python/tests/store/rs/clients/real_client_rw.py \
  --local_host 10.0.0.11:17111 \
  --metadata_url redis://10.0.0.10:6379/0 \
  --storage-bytes $((128 * 1024 * 1024)) \
  --mode idle \
  --hold-seconds 600
```

RW-only writer:

```bash
MOONCAKE_STORE_BACKEND=rs "$MOONCAKE_PYTHON_BIN" ./python/tests/store/rs/clients/real_client_rw.py \
  --local_host 10.0.0.21:17121 \
  --metadata_url redis://10.0.0.10:6379/0 \
  --storage-bytes 0 \
  --routed-writes \
  --mode write \
  --key_prefix smoke
```

RW-only reader:

```bash
MOONCAKE_STORE_BACKEND=rs "$MOONCAKE_PYTHON_BIN" ./python/tests/store/rs/clients/real_client_rw.py \
  --local_host 10.0.0.22:17122 \
  --metadata_url redis://10.0.0.10:6379/0 \
  --storage-bytes 0 \
  --routed-writes \
  --mode read \
  --key_prefix smoke
```

Current script behavior:

- validates only the current `redis://...` and `etcd://...` metadata modes
- accepts `host:port` in `--local_host` and normalizes that port into the real-mode backend config
- supports `idle`, `write`, `read`, and `both`
- supports batch put/get validation through `--batch_size`
- supports `--route-control`, `--route-topk`, and `--transport-backend`
- treats `--master_addr` as a deprecated compatibility alias and ignores it
- exits with a regular error code on validation failure

## Dummy-Mode Validation Script

Use `python/tests/store/rs/clients/dummy_client_rw.py` for black-box dummy-mode validation against a standalone compatibility daemon.

Single-item mode:

```bash
MOONCAKE_STORE_BACKEND=rs "$MOONCAKE_PYTHON_BIN" ./python/tests/store/rs/clients/dummy_client_rw.py \
  --daemon_addr 127.0.0.1:16590 \
  --key_prefix dummy-smoke
```

Shared-memory batch mode:

```bash
MOONCAKE_STORE_BACKEND=rs "$MOONCAKE_PYTHON_BIN" ./python/tests/store/rs/clients/dummy_client_rw.py \
  --daemon_addr 127.0.0.1:16590 \
  --key_prefix dummy-batch \
  --batch_size 8
```

Shared-memory multi-buffer mode:

```bash
MOONCAKE_STORE_BACKEND=rs "$MOONCAKE_PYTHON_BIN" ./python/tests/store/rs/clients/dummy_client_rw.py \
  --daemon_addr 127.0.0.1:16590 \
  --key_prefix dummy-multi \
  --batch_size 4 \
  --batch_api multi_buffer
```

Current script behavior:

- waits for `health_check()` before issuing traffic
- uses high-level `put` / `get` in single-item mode
- uses registered-buffer shm APIs in batched mode
- can validate raw multi-buffer shm get/put with `--batch_api multi_buffer`
- uses `remove_all()` for cleanup because dummy mode does not expose per-key delete parity


### SGLang HiCache integration

Example `sglang.launch_server` patterns:

- real-mode SGLang with an in-process rw-only client (`global_segment_size=0`)

```bash
MC_STORE_RS_TRANSPORT_BACKEND=classic_te \
SGLANG_HICACHE_MOONCAKE_REUSE_TE=0 \
python -m sglang.launch_server \
  --model-path /models/Qwen3-0.6B \
  --host 0.0.0.0 \
  --port 30000 \
  --enable-hierarchical-cache \
  --hicache-size 4 \
  --hicache-write-policy write_through \
  --hicache-io-backend direct \
  --hicache-mem-layout page_first_direct \
  --hicache-storage-backend mooncake \
  --hicache-storage-prefetch-policy wait_complete \
  --hicache-storage-backend-extra-config '{
    "local_hostname": "10.0.0.21:17121",
    "metadata_server": "P2PHANDSHAKE",
    "master_server_address": "redis://10.0.0.10:6379/0",
    "global_segment_size": 0,
    "protocol": "tcp",
    "device_name": "",
    "check_server": false
  }'
```

  Use this when SGLang should build the real store runtime directly inside the serving process. The `global_segment_size: 0` setting keeps the process rw-only while still allowing remote storage placement through the current compatibility path.
  The upstream sglang JSON keys `metadata_server` and `master_server_address` are accepted as aliases that map to `transport_metadata_url` and `metadata_url` respectively. `metadata_server` is forwarded to the Transfer Engine only (defaults to `P2PHANDSHAKE`; omitting the key from the JSON has the same effect). `master_server_address` carries the Store-RS metadata URL (`redis://...` or `etcd://...`, required); `master_server` and `master_server_addr` are equivalent aliases. `setup()` rejects the call if no metadata URL is provided.

  Current upstream SGLang only forwards the legacy Mooncake fields from `--hicache-storage-backend-extra-config`: `local_hostname`, `metadata_server`, `global_segment_size`, `protocol`, `device_name`, `master_server_address`, `check_server`, `standalone_storage`, and `client_server_address`.

  Store-RS compatibility extensions such as `transport_backend`, `keyspace`, `stable_id`, `tenant`, `labels`, `routed_writes`, `replica_count`, and `route_topk` are not forwarded by the current SGLang parser. For real-mode compatibility today:

- use `MC_STORE_RS_TRANSPORT_BACKEND=tent|classic_te` to override the backend; the default is `classic_te`
- use `MC_STORE_RS_TRANSPORT_METADATA_URL=P2PHANDSHAKE` only with `classic_te` when the transfer engine should use peer handshake instead of Redis-backed transport metadata; or set `transport_metadata_url` / `metadata_server` in the JSON dict to `P2PHANDSHAKE` (default for `classic_te`) or `redis://...` (required for `tent`)
- with `P2PHANDSHAKE`, Store-RS keeps the logical segment name in route metadata and publishes a separate `transport_endpoint` (`ip:rpc_port`) so peer opens do not depend on DNS resolution of that logical segment name
- keep SGLang real clients and storage peers on the default metadata keyspace `mc/store-rs/v2`
- treat `--hicache-storage-backend-extra-config` as a legacy field bridge, not a full Store-RS setup dictionary
- if a deployment needs custom `keyspace`, explicit `stable_id`, or per-process route labels, use the dummy gateway path or a patched SGLang fork

- dummy-mode SGLang through a standalone routed gateway

```bash
mooncake-store-rs-client \
  --local-hostname 10.0.0.21 \
  --metadata-url redis://10.0.0.10:6379/0 \
  --storage-bytes 0 \
  --scratch-bytes 16777216 \
  --protocol tcp \
  --transport-rpc-port 17121 \
  --stable-id sglang-gateway \
  --tenant default \
  --label pool=pool-a \
  --label storage=false \
  --routed-writes \
  --replica-count 2 \
  --route-topk 2 \
  --client-server-address 0.0.0.0:16590 \
  --metrics-addr 0.0.0.0:19101

SGLANG_HICACHE_MOONCAKE_REUSE_TE=0 \
python -m sglang.launch_server \
  --model-path /models/Qwen3-0.6B \
  --host 0.0.0.0 \
  --port 30000 \
  --enable-hierarchical-cache \
  --hicache-size 4 \
  --hicache-write-policy write_through \
  --hicache-io-backend direct \
  --hicache-mem-layout page_first_direct \
  --hicache-storage-backend mooncake \
  --hicache-storage-prefetch-policy wait_complete \
  --hicache-storage-backend-extra-config '{
    "standalone_storage": true,
    "client_server_address": "10.0.0.21:16590",
    "check_server": false,
    "prefetch_threshold": 32
  }'
```

  Use this when SGLang should behave like the upstream dummy client. The standalone `mooncake-store-rs-client` process owns the real runtime and exposes a dummy-compatible gRPC endpoint through `client_server_address`.
  Because upstream SGLang still does not forward `worker_scope`, the standalone `mooncake-store-rs-client --client-server-address` path now defaults its dummy side-channel scope to `worker-1` when `keyspace` is absent. That matches omitted-scope `setup_dummy(...)` clients in the serving process, so the default SGLang gateway topology can register shared-memory buffers without requiring a patched SGLang fork.
  The standalone gateway may still bind `client_server_address` on `0.0.0.0` while dummy clients dial a concrete service IP. The dummy side channels now publish matching aliases for that concrete host string, so this wildcard-bind topology keeps working for registered-buffer and hot-cache paths.

Port role summary:

- real mode publishes `local_hostname[:transport_rpc_port]` to peers and does not use `client_server_address`
- dummy mode only needs `client_server_address`

Current coverage includes:

- setup and native loading
- single-key and batch operations
- registered-buffer and multi-buffer paths
- dummy-path shm registration
- real-path registered-buffer reads and writes
- hot-upgrade startup argument parsing and wrapper forwarding
- CLI-driven hot-upgrade handoff with payload preservation
- CLI-driven eviction with metrics and tracing validation
- route query behavior
- replication policy handling
- metrics exposure

## Regression Entry Point

See [Local Validation](#local-validation) for the installed-wheel
compatibility entry and scenario-specific framework and hardware checks.

## Cold-Tier Validation Scenarios

- write returns after DRAM publish even before backend materialization
- successful offload marks `cold_backing = Materialized`
- eviction never removes a hot replica without materialized cold backing
- last-hot eviction produces a cold-only route instead of dropping the object
- read miss restores from backend and can return data even if promotion fails
- two-client remote restore covers an embedded storage client that writes, evicts to a materialized cold-only route, reads back from cold tier, and allows a second client to trigger owner-side cold read
- owner-side staging ACK releases read pins and recycles staging slots without leaking or prematurely reusing memory
- batch cold restore uses backend batch read/read-into paths when objects share a backend/device
- overwrite/delete clean up old cold objects
- startup/heartbeat rebuild can materialize pending offloads from persisted pending source
- a full e2e flow covers write -> materialize -> cold-only eviction -> backend restore -> overwrite -> delete
- legacy backend-object routes remain readable during migration
