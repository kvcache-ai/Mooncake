# Python Guide

`mooncake-store-rs` ships a Python compatibility layer in `crates/mooncake-store-py` and a convenience package in `python/mooncake`.

The Python package does not wrap a second store implementation. It reuses the same Rust runtime, control plane, allocator, reclaim logic, tracing, and metrics that the Rust client uses.

## Package Model

The repository now builds two Python wheels with different responsibilities:

- `mooncake-*.whl` is the real runtime package
- `mooncake_pro-*.whl` is the user-facing Pro metapackage

Recommended installation flow:

```bash
./scripts/build/install-pro-wheel.sh
```

Equivalent raw pip flow:

```bash
pip install --find-links dist/wheels dist/wheels/mooncake_pro-*.whl
```

The install helper also supports bundle-local wheelhouses on another machine:

```bash
./scripts/build/install-pro-wheel.sh --wheel-dir /tmp/sglang-true-e2e-bundle/dist/wheels
```

After installation:

- Python code still imports `mooncake`
- `pip list` clearly shows `mooncake-pro`
- the installed runtime version is pinned through `mooncake-pro -> mooncake==...+pro...`
- upgrading from an older `mooncake` install does not require `--force-reinstall`

## What the Python Layer Provides

- `MooncakeDistributedStore` as the main compatibility API
- `MooncakeHostMemAllocator` for caller-owned registered buffers
- real and dummy execution modes for Mooncake / HiCache-style integration
- batch I/O, registered-buffer I/O, and multi-buffer I/O
- the same storage-owner CLOCK eviction and route-owner CAS reclaim as Rust callers
- the same background watermark eviction defaults as Rust callers
- route query, lifecycle, and metrics helpers
- wheel packaging for the native extension plus the bundled runtime libraries

## Execution Modes

The Python compatibility layer supports two execution styles.

### Real mode

Use `setup(...)` when Python should talk to the native distributed store runtime directly.

For tenant-scoped routing and resource policy, prefer `mooncake-store-admin policy ...` and durable metadata. Python `setup(...)` route knobs are kept as compatibility/bootstrap fallbacks.

Isolation knobs:

- `keyspace` isolates metadata-backed routing, policy lookup, and object visibility
- `tenant`, `domain`, and `object_set` select the default namespace scope applied to Python compatibility read/write operations
- with `classic_te` and Redis-backed transport metadata, the default `tenant` also scopes upstream transfer-engine Redis keys under `mooncake/tenants/<tenant>/...`
- `worker_scope` isolates compat-local worker state such as the dispatcher executor and local hot-cache domain
- when `worker_scope` is omitted, the Python layer derives it from `keyspace` when present; otherwise dummy mode falls back to the legacy `worker-1` scope so omitted-scope clients keep matching standalone daemon side-channel aliases

This mode:

- constructs a Rust `StoreClient`
- dispatches compatibility calls directly into the native store object so concurrent Python threads are not serialized behind a single Python worker queue
- registers local memory and participates in route / allocator control plane RPC
- starts a background heartbeat loop after setup so long-lived Python runtimes keep their lease live and repair local metadata after Redis connectivity returns without requiring explicit `heartbeat()` calls
- participates in the same hit-report and replica-route tracking RPC used by Rust clients
- supports routed writes, replication policy, segment lifecycle, metrics, and tracing
- is the normal path for Python clients that should behave like store-rs nodes

Registered-buffer batch restores are best-effort per item in the Python compatibility layer. If
the fast batch restore path fails because one route is missing or one storage owner is unavailable,
the dispatcher retries the missed entries one by one and returns a per-key status: positive byte
counts for restored entries and negative soft-miss codes for the entries that still failed. A dead
cache replica therefore does not turn healthy keys in the same HiCache batch into misses.

Real-mode `batch_put_from(...)` keeps the caller's batch together by default so the Rust write path
can coalesce placement, remote transfer, route publication, and replica tracking. Operators that
prefer Python-side sharding for a specific workload can set
`MC_STORE_RS_PY_BATCH_PUT_FROM_FANOUT` to an explicit positive fanout width.

`setup(...)` accepts `eviction_high_watermark_percent=` and
`eviction_low_watermark_percent=` for storage-role runtimes. The same values can come from
`MC_STORE_RS_EVICTION_HIGH_WATERMARK_PERCENT` and
`MC_STORE_RS_EVICTION_LOW_WATERMARK_PERCENT`. They feed the Rust `LocalMemoryConfig` directly;
the high watermark must be in `1..=100`, and the low watermark must be lower than the high
watermark.

### Dummy mode

Use `setup_dummy(...)` when Python should behave like the upstream dummy compatibility client.

`setup_dummy(...)` now also accepts optional keyword-only `keyspace=` and `worker_scope=` arguments. They do not change the standalone server's underlying distributed-store namespace by themselves; they control the Python compat worker boundary used by the dummy side channels. In practice:

- `keyspace` remains the metadata/data-plane namespace knob for the real runtime that the standalone daemon was started with
- `worker_scope` isolates dummy compat-local state such as hot-cache SHM, registered-region side channels, and worker-local dispatcher state
- when `worker_scope` is omitted, the Python layer derives it from `keyspace` when present; otherwise it allocates a unique per-setup worker scope
- a daemon started with an explicit `keyspace` or `worker_scope` also exposes a legacy `worker-1` side-channel alias so omitted-scope dummy buffer clients can still register SHM regions
- a daemon started with a wildcard `client_server_address` such as `0.0.0.0:16590` or `[::]:16590` also exposes dummy shm and hot-cache side-channel aliases for its advertised hostname plus loopback forms, so clients may connect through a concrete host address without breaking buffer registration

This mode:

- connects to a standalone `mooncake-store-client` process over gRPC
- registers shm regions by passing file descriptors over a Unix socket
- only reports dummy `register_buffer(...)` success after the standalone daemon has installed the shared region in its dispatcher, so callers can issue `batch_put_from(...)` or `batch_get_into(...)` immediately without adding an extra sleep/retry fence
- derives short hashed Unix socket filenames for dummy shm and hot-cache side channels so long worker scopes stay below AF_UNIX path limits
- keeps the Python process out of the distributed control plane
- is the compatibility path used by the HiCache dummy flow

Dummy mode is intentionally narrower than real mode. It exists to preserve compatibility for callers that expect the old dummy client / standalone server split.

The standalone server behind dummy mode still uses the same Rust runtime internally, so route publication, reclaim, eviction, tracing, and metrics stay aligned with the real path.

Compatibility note:

- legacy Python alias names such as `put_batch(...)` and `get_batch(...)` remain available
- dummy mode also supports high-level `batch_put(...)` and `batch_get(...)` compatibility calls for legacy black-box tests
- registered-buffer and multi-buffer batch APIs are still the preferred throughput path for dummy-mode validation

## Local Hot Cache

The Python compatibility layer can enable a daemon-local hot read cache for both real-mode clients and standalone dummy daemons.

In real mode:

- the first remote read populates the local cache
- later reads from the same runtime can reuse the cached bytes without another remote transfer
- successful writes or deletes issued by that same runtime invalidate the matching local cache entry

In dummy mode:

- start the standalone daemon with `MC_STORE_LOCAL_HOT_CACHE_USE_SHM=1`
- each dummy client maps the daemon hot-cache shm region on connect
- a dummy read first asks the daemon for a hot-cache handle and falls back to the regular dummy RPC path on miss
- dummy clients only share hot-cache SHM hits when they connect through the same worker-scoped dummy server boundary
- different worker scopes do not reuse each other's cached bytes, even when they point at the same underlying store client/runtime

Configuration uses the upstream environment variable names:

```bash
export MC_STORE_LOCAL_HOT_CACHE_SIZE=$((256 * 1024 * 1024))
export MC_STORE_LOCAL_HOT_BLOCK_SIZE=$((4 * 1024 * 1024))
export MC_STORE_LOCAL_HOT_CACHE_USE_SHM=1   # only needed when dummy clients should share hits
```

Design boundary:

- the cache is a local read accelerator only
- it is not written into Redis or etcd
- it does not change route ownership, replica ownership, or placement policy
- values larger than the configured block size bypass the cache

## Build From a Checkout

```bash
git submodule update --init --recursive
cargo build -p mooncake-store-py
export PYTHONPATH="$PWD/python"
```

The package loads the native extension from the local `target` directory and preloads the upstream Mooncake TE/TENT shared libraries from `third_party/Mooncake/build-rust`.

## Build a Wheel

Use the repository packaging script:

```bash
./scripts/build/build-wheel.sh
```

By default the script:

- creates or reuses `.venv-wheel`
- installs `maturin`
- installs `build` for the Pro metapackage
- embeds the standalone `mooncake-store-client` and `mooncake-store-admin` binaries into the runtime wheel package
- embeds the build Python `libpython*.so` needed by those standalone binaries and restores that dependency after `auditwheel repair`, because the binaries run as subprocesses from a wheel install rather than as Python extension modules
- builds both wheels into `dist/wheels/`
- copies the standalone `mooncake-store-client` and `mooncake-store-admin` artifacts into `dist/bin/`

Repository packaging rule:

- `scripts/build/build-wheel.sh` is the single owner of wheel asset injection and `auditwheel repair`
- outer wrappers such as `abs_scripts/build.sh` only prepare the environment and collect the wheels already produced in `dist/wheels/`

Common variants:

```bash
./scripts/build/build-wheel.sh --interpreter python3.11
DIST_DIR=artifacts ./scripts/build/build-wheel.sh
```

CI jobs that already downloaded native artifacts can skip the upstream CMake
portion and reuse `MOONCAKE_UPSTREAM_BUILD_DIR` directly:

```bash
MOONCAKE_REUSE_NATIVE_ARTIFACTS=1 \
MOONCAKE_SKIP_NATIVE_BUILD=1 \
MOONCAKE_UPSTREAM_BUILD_DIR="$PWD/third_party/Mooncake/build-wheel-compat" \
MOONCAKE_CLASSIC_SHIM_LIB_PATH="$PWD/dist/lib/libmooncake_classic_shim.so" \
MOONCAKE_TENT_SHIM_LIB_PATH="$PWD/dist/lib/libmooncake_tent_shim.so" \
./scripts/build/build-wheel.sh --interpreter python3.10
```

When the host OS is missing build dependencies, use the Ubuntu Docker wrapper
instead. It reuses `scripts/build/build-wheel.sh` inside the container and
produces the same `dist/wheels/` and `dist/bin/` outputs:

```bash
./scripts/build/build-wheel-ubuntu-docker.sh
PYTHON_VERSION=3.11 ./scripts/build/build-wheel-ubuntu-docker.sh
PYTHON_VERSION=3.12 UBUNTU_VERSION=24.04 ./scripts/build/build-wheel-ubuntu-docker.sh
```

Docker wheel notes:

- `PYTHON_VERSION=system|3.10|3.11|3.12` selects the interpreter installed in the builder image
- `UBUNTU_VERSION` selects the base image used for the build environment
- `CN_MIRROR=1` is enabled by default for rustup, cargo, and pip downloads; set `CN_MIRROR=0` to use the upstream endpoints
- `HTTP_PROXY`, `HTTPS_PROXY`, and `NO_PROXY` are forwarded into the Docker build/run steps for local proxy setups

Install the wheel into any compatible virtualenv:

```bash
./scripts/build/install-pro-wheel.sh
```

Or with raw pip:

```bash
pip install --find-links dist/wheels dist/wheels/mooncake_pro-*.whl
```

After installation, both interfaces are available:

- `python -c "import mooncake"` loads the native extension
- `python -c "import mooncake; print(mooncake.__version__, mooncake.__edition__)"` shows the active Pro runtime
- `python -c "import mooncake; print(mooncake.__build_info__)"` shows the packaged build branch, commit, and build time without loading the native extension
- `mooncake-store-client --help` runs the packaged standalone client command
- `mooncake-store-client -v` prints the packaged Pro version plus wheel build branch, commit, and build time
- `mooncake-store-admin --help` runs the packaged metadata maintenance and route-policy management command
- if your environment still exposes the upstream compatibility alias, `mooncake_master --version` prints the same packaged Pro version banner

## Standalone Client Binary

You can build the standalone compatibility server directly:

```bash
cargo build -p mooncake-store-py --bin mooncake-store-client --release
```

Or use `./scripts/build/build-wheel.sh`, which also copies the binary to `dist/bin/`.

When the client is installed from a wheel, the same binary is also embedded inside the package and exposed through the `mooncake-store-client` console script, matching the upstream Mooncake packaging style.

The same wheel also exposes `mooncake-store-admin` for explicit metadata maintenance and admin-managed tenant policy operations.

Start a storage client:

```bash
./dist/bin/mooncake-store-client \
  --local-hostname 127.0.0.1 \
  --metadata-url redis://127.0.0.1:6380/0 \
  --storage-bytes $((128 * 1024 * 1024)) \
  --scratch-bytes $((16 * 1024 * 1024)) \
  --stable-id store-a \
  --tenant default \
  --label pool=pool-a \
  --label storage=true \
  --metrics-addr 127.0.0.1:9091
```

Useful flags:

- `--transport-metadata-url` to override the Transfer Engine metadata input (default `P2PHANDSHAKE`; `tent` requires `redis://...`)
- `--transport-backend tent|classic-te` to choose the real data-plane backend
- `--transport-rpc-port <port>` to pin the real data-plane TCP port used by real clients
- `--routed-writes` and `--replica-count` to enable routed writer mode
- `--route-topk <n>` as a compatibility fallback for WRH route-authority fanout; it must be `>= 2`, should match any policy already stored in metadata, and admin-managed tenant policy is preferred
- `--route-control metadata-only|embedded-wrh` as a compatibility fallback for route authority mode; prefer admin-managed tenant policy in metadata
- `--heartbeat-interval-ms`, `--heartbeat-timeout-ms`, and `--lease-ttl-ms` to tune lease refresh; `--lease-ttl-ms` defaults to `30000`
- `--request-timeout-ms` to set the outer request deadline for routed operations and dispatcher calls
- `--startup-timeout-ms` to override the compatibility registration timeout used by startup local-memory registration plus real-mode `register_buffer` / `unregister_buffer`; when unset the runtime uses `max(20s, ceil(registration_bytes / 1 GiB))`
- `--transfer-stall-timeout-ms` to set the inner transport stall detector for TENT / classic TE
- `--drain-on-exit` to enter draining mode and evacuate owned replicas before shutdown
- `--client-server-address host:port` to expose the standalone compatibility server for dummy clients only
- `--use-hugepage` and `--hugepage-size 2MB|1GB` to enable hugepage-backed local memory

Role reminder:

- use `--label storage=true` on storage nodes that should accept routed placement and run local CLOCK eviction
- use `--label storage=false` on routed rw nodes that should place remotely without owning local storage
- `--label storage=true` requires `--storage-bytes > 0`
- when `--storage-bytes 0` is used without an explicit storage label, the runtime defaults to `storage=false`

Port reminder:

- `transport_rpc_port` / `--transport-rpc-port` is the real-mode data-plane port for the selected backend
- `client_server_address` / `--client-server-address` is the dummy compatibility gRPC port
- `metrics_addr` / `--metrics-addr` is only for `/metrics`
- cross-host real-mode deployments should set both a reachable `local_hostname` and a fixed `transport_rpc_port`
- `local_hostname` may also be passed as `host:port`; the compatibility layer will split the port into `transport_rpc_port`

Heartbeat behavior:

- timeout knobs now come from one shared helper across the standalone client, Python compatibility runtime, and dummy client
- `--request-timeout-ms` / `MC_STORE_RS_REQUEST_TIMEOUT_MS` sets the outer request deadline
- `--startup-timeout-ms` / `MC_STORE_RS_STARTUP_TIMEOUT_MS` sets the registration-specific timeout budget; when unset the runtime derives it from the current registration size with a `20s` floor
- `--heartbeat-timeout-ms` / `MC_STORE_RS_HEARTBEAT_TIMEOUT_MS` sets the dedicated heartbeat publish timeout
- `--transfer-stall-timeout-ms` / `MC_STORE_RS_TRANSFER_STALL_TIMEOUT_MS` sets the inner transfer stall detector
- `MC_STORE_RS_DUMMY_RPC_TIMEOUT_MS` controls dummy gRPC calls and falls back to `MC_STORE_RS_REQUEST_TIMEOUT_MS`
- heartbeat publish uses a dedicated timeout instead of the generic request deadline
- a single failed heartbeat no longer exits the standalone client process
- failed heartbeat publishes are retried on a short backoff
- `MC_STORE_RS_CONTROL_PLANE_THREADS` sets the worker count of the shared control-plane RPC client runtime; default `2`
- `MC_STORE_RS_CONTROL_PLANE_SERVER_THREADS` sets the worker count of the embedded control-plane gRPC server runtime; default `4`

## Metadata Maintenance

Use the packaged admin binary when metadata still contains stale segment registrations
from dead storage owners:

```bash
mooncake-store-admin \
  --metadata-url redis://127.0.0.1:6380/0 \
  cleanup-stale-segments
```

## Admin HTTP Migration Tasks

`mooncake-store-admin server` exposes an in-memory route-migration task queue over HTTP.

For the operator workflow, task semantics, and request examples, see
[Route Migration 使用手册](./route-migration-usage.md).

The packaged `mooncake-store-admin` binary acts as an operator client for that
HTTP surface. Route-migration tasks are not kept in the CLI process, so
`migrate ...` commands must point at a long-lived admin server with `--admin-url`:

```bash
mooncake-store-admin \
  --metadata-url redis://127.0.0.1:6380/0 \
  --admin-url http://127.0.0.1:18080 \
  migrate copy \
  --authority source-store \
  --tenant tenant-a \
  --domain domain-a \
  --object-set set-a \
  --key object-a \
  --source-segment source-segment \
  --target-segment target-segment-a \
  --target-segment target-segment-b \
  --task-executor executor-store \
  --max-retries 5

mooncake-store-admin \
  --metadata-url redis://127.0.0.1:6380/0 \
  --admin-url http://127.0.0.1:18080 \
  migrate task list
```

Current endpoints:

- `POST /v1/route-migrations/copy`
- `POST /v1/route-migrations/move`
- `GET /v1/route-migrations`
- `GET /v1/route-migrations/<task_id>`

Task request fields:

- `authority`
- `tenant`
- optional `domain`
- optional `object_set`
- `key`
- `source_segment`
- `target_segments`
- `task_executor`
- optional `max_retries`

Operational notes:

- the admin server does not move bytes itself; it submits migration RPC to the chosen `task_executor`
- `copy` supports multiple targets, while `move` currently requires exactly one target
- scoped migration requests may carry `domain` and `object_set`; omitted values fall back to the default namespace
- submit bodies accept only the documented fields; legacy `mode` or other unknown fields are rejected with `400`
- admin keeps task state only in process memory, so queued tasks are lost if the admin server restarts
- admin retry is automatic while the server stays alive; route visibility is used as the authoritative completion check when executor status is lost
- current CLI support covers `migrate copy`, `migrate move`, `migrate task list`, and `migrate task get`
- `migrate` commands require `--admin-url`; the CLI no longer starts a private in-process task queue for these asynchronous operations

Manage tenant policy or clean up stale segment registrations with the packaged admin binary:

```bash
mooncake-store-admin \
  --metadata-url redis://127.0.0.1:6380/0 \
  policy set \
  --tenant tenant-a \
  --route-topk 3 \
  --route-control embedded-wrh

mooncake-store-admin \
  --metadata-url redis://127.0.0.1:6380/0 \
  policy get \
  --tenant tenant-a

mooncake-store-admin \
  --metadata-url redis://127.0.0.1:6380/0 \
  quota reservations \
  --tenant tenant-a \
  --state pending

mooncake-store-admin \
  --metadata-url redis://127.0.0.1:6380/0 \
  quota reconcile \
  --tenant tenant-a \
  --dry-run
```

Notes:

- Redis stores the client lease and owned segment metadata in one TTL-backed client resource hash, so a normal lease expiry removes both
- cleanup consumes the lease-expiry work index and repairs stale owner metadata only after re-checking liveness; this is the normal path for etcd and a fallback path for abnormal Redis metadata
- tenant-scoped routing, quota, fairness, shaping, and placement defaults should be authored through admin-managed metadata policy
- Python `setup(...)`, standalone client flags, and related env vars remain compatibility/bootstrap fallbacks when metadata does not provide the relevant section
- `--keyspace <prefix>` scopes both policy operations and stale-segment cleanup to one metadata namespace
- `MC_REDIS_USERNAME` / `MC_REDIS_PASSWORD` also apply here

## Structured Object Store Helper

`mooncake.structured_object_store` provides a higher-level helper for one logical object that contains multiple named members.
It is designed for cases such as rollout / batch transfer where callers want to keep their own object semantics locally while using Mooncake for fast payload movement.

The helper separates two concepts:

- **structured object path**: named members with metadata-aware materialization;
- **generic bundle path**: manifest + named payloads when the caller only needs raw grouped objects.

### Main types

```python
from mooncake.structured_object_store import (
    MooncakeBundleTransfer,
    StructuredMemberSlice,
    StructuredObjectPayload,
)
```

- `MooncakeBundleTransfer`: public helper facade built on a `MooncakeDistributedStore`.
- `StructuredObjectPayload`: structured object to write. Members are passed in `buffers`, and optional object metadata is passed in `metadata`.
- `StructuredMemberSlice`: slice selection for one structured member during reads.

### Structured object write and full read

Use `put_structured_object()` to write one structured object. The default read path is `read_spec(ref)`, and full-object materialization is just the default case of the partial-read API.

```python
import numpy as np
from mooncake.store import MooncakeDistributedStore
from mooncake.structured_object_store import MooncakeBundleTransfer, StructuredObjectPayload

store = MooncakeDistributedStore()
transfer = MooncakeBundleTransfer(store, key_prefix="demo/structured")

payload = StructuredObjectPayload(
    metadata={"step": 7, "layout": "rollout"},
    buffers={
        "tokens": np.array(range(24), dtype=np.int32).reshape(6, 4),
        "mask": np.ones((6, 4), dtype=np.int8),
        "prompt_ids": b"sample-ids",
    },
)

ref = transfer.put_structured_object(payload)
result = transfer.materialize(transfer.read_spec(ref))

tokens = result.objects["tokens"]
prompt_ids = result.objects["prompt_ids"]
metadata = result.metadata
```

### Partial reads

Read narrowing happens on top of `read_spec(ref)`:

- `select_members([...])` keeps only selected members;
- `slice_member(name, axis=0, start=..., end=...)` slices one ndarray member;
- `materialize(spec)` returns newly materialized objects.

```python
spec = (
    transfer.read_spec(ref)
    .select_members(["tokens"])
    .slice_member("tokens", axis=0, start=2, end=5)
)
result = transfer.materialize(spec)

selected_tokens = result.objects["tokens"]
```

Current scope:

- byte members support full-member reads;
- ndarray members support full reads and sliced reads;
- full read is the default `read_spec(ref)` case.

### Reusing caller-owned destinations

Use `materialize_into()` when the caller already owns the destination ndarray buffers and wants Mooncake to fill them directly.

```python
destination = np.empty((3, 4), dtype=np.int32)
spec = (
    transfer.read_spec(ref)
    .select_members(["tokens"])
    .slice_member("tokens", axis=0, start=2, end=5)
)
result = transfer.materialize_into(spec, {"tokens": destination})

assert result.objects["tokens"] is destination
```

`materialize_into()` is only for members whose destination layout is already known to the caller. For byte members or default object reconstruction, use `materialize()`.

### Generic bundle fallback

If the caller does not need structured member semantics, the same helper also supports raw named bundles:

- `put_bundle(...)`
- `remove_bundle(...)`

Use the bundle path when the object is just a manifest plus named payloads, and use the structured object path when callers want member selection, slicing, and ndarray-aware materialization.

## Basic Real-Mode Example

```python
from mooncake.store import MooncakeDistributedStore

store = MooncakeDistributedStore()
store.setup(
    "127.0.0.1",
    "P2PHANDSHAKE",                      # arg2 transport_metadata_url: TE input (default for classic_te)
    128 * 1024 * 1024,
    16 * 1024 * 1024,
    "tcp",
    "",
    "redis://127.0.0.1:6380/0",          # arg7 metadata_url: Store-RS metadata backend (required)
    stable_id="py-store-a",
    tenant="default",
    domain="sglang-chat",
    object_set="deepseek-r1__2026-04-19-build-44",
    labels={"pool": "pool-a", "storage": "true"},
    transport_backend="classic_te",
    transport_rpc_port=17111,
)

store.put("hello", b"world")
assert store.get("hello") == b"world"
```

When `domain` and `object_set` are omitted from `setup(...)`, the Python wrapper also accepts `MC_STORE_RS_DOMAIN` and `MC_STORE_RS_OBJECT_SET` as startup fallbacks. The selected default scope is applied consistently to real-mode compatibility reads, writes, route queries, removes, existence checks, size checks, batch operations, registered-buffer operations, and local hot-cache keys.

## Routed Writes

```python
from mooncake.store import MooncakeDistributedStore, ReplicateConfig

store = MooncakeDistributedStore()
store.setup(
    "127.0.0.1",
    "P2PHANDSHAKE",                      # arg2 transport_metadata_url: TE input (default for classic_te)
    128 * 1024 * 1024,
    16 * 1024 * 1024,
    "tcp",
    "",
    "redis://127.0.0.1:6380/0",          # arg7 metadata_url: Store-RS metadata backend
    stable_id="router-a",
    labels={"pool": "pool-a", "storage": "false"},
    routed_writes=True,
    replica_count=2,
    route_topk=2,
)

policy = ReplicateConfig(
    replica_num=2,
    prefer_local=False,
    preferred_storage_owners=["store-b"],
)
store.put("key", b"payload", config=policy)
```

## Standard Read/Write Validation

Use the repository-standard entry point to validate both compatibility paths in one run:

```bash
./scripts/tests/client/test-client-rw-cli.sh
```

This script:

- builds the standalone `mooncake-store-client` binary
- starts two storage daemons against a temporary Redis metadata backend
- validates dummy single-item, shm batch, and shm multi-buffer read/write
- validates real routed write/read across separate real clients

For path-specific manual checks, keep using the paired helper scripts below.

## Local Hot-Cache Validation

Run the dedicated local hot-cache e2e:

```bash
./scripts/e2e/run-local-hot-cache-e2e.sh
```

This script validates two phases:

- Phase A: a real-mode reader reuses daemon-local cached bytes after the origin key is removed remotely
- Phase B: two dummy clients attached to one standalone daemon reuse a shm-backed hot-cache hit

Useful inputs:

- `MC_STORE_RS_REFRESH_WHEEL=0` to reuse the current `.venv-wheel`
- `MC_STORE_RS_LOCAL_HOT_CACHE_E2E_REDIS_PORT` to pin the temporary Redis port
- `MC_STORE_RS_LOCAL_HOT_CACHE_E2E_STORAGE_BYTES` and `MC_STORE_RS_LOCAL_HOT_CACHE_E2E_SCRATCH_BYTES` to size the local runtime
- `MC_STORE_RS_LOCAL_HOT_CACHE_E2E_CACHE_BYTES` and `MC_STORE_RS_LOCAL_HOT_CACHE_E2E_BLOCK_BYTES` to tune the cache under test

## Real-Mode Validation Script

Use `scripts/clients/real_client_rw.py` for black-box real-mode validation against the current store-rs compatibility stack.

Storage node:

```bash
python3 ./scripts/clients/real_client_rw.py \
  --local_host 10.0.0.11:17111 \
  --metadata_url redis://10.0.0.10:6379/0 \
  --storage-bytes $((128 * 1024 * 1024)) \
  --mode idle \
  --hold-seconds 600
```

RW-only writer:

```bash
python3 ./scripts/clients/real_client_rw.py \
  --local_host 10.0.0.21:17121 \
  --metadata_url redis://10.0.0.10:6379/0 \
  --storage-bytes 0 \
  --routed-writes \
  --mode write \
  --key_prefix smoke
```

RW-only reader:

```bash
python3 ./scripts/clients/real_client_rw.py \
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

Use `scripts/clients/dummy_client_rw.py` for black-box dummy-mode validation against a standalone compatibility daemon.

Single-item mode:

```bash
python3 ./scripts/clients/dummy_client_rw.py \
  --daemon_addr 127.0.0.1:16590 \
  --key_prefix dummy-smoke
```

Shared-memory batch mode:

```bash
python3 ./scripts/clients/dummy_client_rw.py \
  --daemon_addr 127.0.0.1:16590 \
  --key_prefix dummy-batch \
  --batch_size 8
```

Shared-memory multi-buffer mode:

```bash
python3 ./scripts/clients/dummy_client_rw.py \
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

## Basic Dummy-Mode Example

Start the standalone compatibility server first:

```bash
./dist/bin/mooncake-store-client \
  --local-hostname 127.0.0.1 \
  --metadata-url redis://127.0.0.1:6380/0 \
  --storage-bytes $((64 * 1024 * 1024)) \
  --scratch-bytes $((16 * 1024 * 1024)) \
  --stable-id dummy-server \
  --client-server-address 127.0.0.1:16590
```

Then connect from Python:

```python
from mooncake.store import MooncakeDistributedStore, MooncakeHostMemAllocator

store = MooncakeDistributedStore()
store.setup_dummy(
    64 * 1024 * 1024,
    16 * 1024 * 1024,
    "127.0.0.1:16590",
    keyspace="tenant-a",
    worker_scope="tenant-a-dummy-worker",
)

allocator = MooncakeHostMemAllocator()
ptr = allocator.alloc(4096)
store.register_buffer(ptr, 4096)
```

`setup_dummy(...)` only needs `client_server_address` for the remote endpoint. It does not consume `transport_rpc_port`, because the standalone server owns the real store runtime and data-plane endpoint on behalf of the dummy client. Use `keyspace` to align with the intended metadata namespace and `worker_scope` when you need an explicit compat worker boundary for dummy-side cache and shm isolation. When callers omit `keyspace`, the Python wrapper now also falls back to `MC_STORE_RS_KEYSPACE` before deriving the dummy worker scope, so upstream SGLang dummy mode can share the same scoped side-channel namespace as a `mooncake-store-client --keyspace ... --client-server-address ...` gateway. A standalone daemon that binds `client_server_address` on a wildcard host also publishes side-channel aliases for its advertised host address, `127.0.0.1`, `::1`, and `localhost`, so dummy buffer clients do not need the daemon to bind the exact same host string they dial.

## Host Allocator and Hugepages

`MooncakeHostMemAllocator` is the Python-side allocator for stable registered buffers.

Default behavior:

- when the native extension is available, it allocates shm regions with `memfd` and shared `mmap`
- when the native extension is not available, it falls back to Python `mmap`
- the pure-Python fallback does not support hugepages

Hugepage options:

- `use_hugepage=True`
- `hugepage_size="2MB"` or `hugepage_size="1GB"`

Example:

```python
from mooncake.store import MooncakeHostMemAllocator

allocator = MooncakeHostMemAllocator(use_hugepage=True, hugepage_size="2MB")
ptr = allocator.alloc(2 * 1024 * 1024)
allocator.free(ptr)
```

The same hugepage knobs are also accepted by `MooncakeDistributedStore.setup(...)` and the config-dict path. They control the store client's local storage and scratch allocator.

Supported hugepage sizes:

- `2MB`
- `1GB`

Related environment variables:

- `MC_STORE_USE_HUGEPAGE`
- `MC_STORE_HUGEPAGE_SIZE`

If hugepage mode is requested, the host kernel must already have compatible hugepages reserved.

## Supported Operations

### Basic I/O

- `put`, `get`, `remove`
- `batch_put`, `batch_get`, `batch_remove`
- `query_route`, `is_exist`, `get_size`

### Buffer-Oriented I/O

- `register_buffer`, `unregister_buffer`
- `put_from`
- `batch_put_from`
- `batch_put_from_multi_buffers`
- `batch_get_into`
- `batch_get_into_multi_buffers`

## Cache Soft-Fail Semantics

The Python compatibility API treats KV cache data-plane failures as best-effort misses or failed cache operations so optional HiCache traffic does not crash an inference process. Strict setup, configuration, metrics, lifecycle, and admin APIs still return normal exceptions.

- `get(...)` and `batch_get(...)` return empty byte payloads on soft failures
- `get_into(...)`, `batch_get_into(...)`, and `batch_get_into_multi_buffers(...)` return copied lengths and use `-1` for soft misses
- `put(...)`, `put_from(...)`, `batch_put_from(...)`, and related buffer write APIs return `-1` or per-item negative statuses on true write failures
- `batch_put_from(...)` reports per-key best-effort statuses; route-CAS conflicts are success, preserving insert-if-absent cache semantics under concurrent writers without adding a metadata recheck
- `batch_is_exist(...)` returns `1` for hit, `0` for miss, and negative status codes for soft backend failures
- soft-fail downgrade covers transient native exceptions, including metadata and transport errors, at the Python compatibility boundary

### Lifecycle and Capacity

- `activate`, `enter_standby`, `enter_draining`
- `expand_local_memory`
- `drain_segment`, `retire_segment`
- `evacuate_owned_replicas`
- `plan_handoff`

### Observability

- `init_tracing()`
- `metrics_text()`
- `start_metrics_server()`
- `stop_metrics_server()`
- `metrics_server_address()`

Python real clients also auto-initialize Rust tracing before `setup(...)` when `MC_STORE_RS_TRACE=1` or `MC_STORE_RS_TRACE_FILE` is set; use `MC_STORE_RS_TRACE_FILTER` to pass a `tracing_subscriber` filter.

When `MC_STORE_RS_TRACE_FILE=/path/to/real-client.log` is set, Rust tracing appends to that file instead of writing to the process stdout/stderr stream. This is the recommended way to keep SGLang real-client logs separate from SGLang server logs.

For short profiling windows, set `MC_STORE_RS_TRACE_JSONL_FILE=/path/to/store-rs.trace.jsonl`
or use the metrics server `/tracing?enabled=on&file=...` control endpoint. The
JSONL sink records one structured span per Store-RS API or phase and can be
used alongside Jaeger OTLP export. Set `MC_STORE_RS_TRACE_ITEM_METADATA=1` to
also write `store.api_items.v1` records for `batch_put_from` and
`batch_get_into`, including per-item namespace, runtime id, hashed key,
readable key prefix, parsed SGLang TP rank and `k`/`v` suffix, size/read
length, and status. `MC_STORE_RS_TRACE_KEY_MODE=hash` is the default; use
`full` only for local debug captures.

The same metrics HTTP server also exposes `/breakdown` for SGLang/HiCache
diagnosis. Use `mooncake-store-client stats --breakdown --server <host:port>`
for a readable summary, or add `--json` to keep the machine-readable API,
phase, metadata, transport, runtime, and segment snapshot. The
`scripts/sglang/sglang_true_e2e.py` workflow saves that endpoint as
`sglang-true-e2e-breakdown-<stamp>.json` after the real SGLang run.

## Metadata URLs

The compatibility layer accepts these metadata URL forms:

| Scheme | Meaning |
|--------|---------|
| `redis://host:port/db` | Redis metadata backend |
| `etcd://host1:2379,host2:2379` | etcd metadata backend |

Redis authentication follows the same environment variables as the native store:

```bash
export MC_REDIS_PASSWORD='<redis-password>'
# Optional when Redis ACLs require a named user:
export MC_REDIS_USERNAME='<redis-username>'
```

Credentials embedded in `redis://username:password@host:port/db` are also accepted and take precedence over the environment variables. Prefer environment variables when passwords contain URL-reserved characters such as `@`.

### Setup positional layout

`setup(local_hostname, transport_metadata_url, global_segment_size, local_buffer_size, protocol, rdma_devices, metadata_url)`.

- `transport_metadata_url` is forwarded to the Transfer Engine only. Accepts `redis://...` or `P2PHANDSHAKE`. **Default value is `P2PHANDSHAKE`** everywhere it can be defaulted: the Python dict-form (resolution order: dict key → `MC_STORE_RS_TRANSPORT_METADATA_URL` env → `P2PHANDSHAKE`), the standalone CLI bins (`mooncake-store-client` / `mooncake-store-bench` without `--transport-metadata-url` or env), and other defaultable surfaces. The Python positional `setup(...)` requires it explicitly because Python disallows defaults on a positional that precedes required positionals; pass `"P2PHANDSHAKE"` to opt into the default. The dict-form also accepts the upstream Mooncake key `metadata_server` as an alias.
- `metadata_url` is the Store-RS metadata URL. Accepts `redis://...` or `etcd://...` and is required. The dict-form `setup({...})` also accepts the upstream Mooncake keys `master_server`, `master_server_addr`, and `master_server_address` interchangeably as aliases, with `MC_STORE_RS_METADATA_URL` env honored as a final fallback. The standalone CLI bins use the same env for their `--metadata-url` flag.

### Important note for etcd

When `metadata_url` is an etcd URL, the Transfer Engine still needs its own metadata: pass either `redis://...` or `P2PHANDSHAKE` at `transport_metadata_url`. For `classic_te`, `P2PHANDSHAKE` (the transfer-engine peer-handshake mode) is the default; `tent` always needs an explicit `redis://...`.

## Transport Backend Selection

The compatibility layer can choose the transport backend at runtime.

Supported values:

- `tent`
- `classic_te`

Selection order:

1. explicit `transport_backend=...` argument to `setup(...)`
2. `MC_STORE_RS_TRANSPORT_BACKEND`
3. default `classic_te`

The standalone client follows the same rule, except the explicit override is `--transport-backend`.

## SGLang Environment Fallbacks

Current upstream SGLang only forwards legacy Mooncake setup fields. When SGLang cannot pass Store-RS setup extensions, the Python wrapper reads these environment variables as fallbacks:

- `MC_STORE_RS_METADATA_URL` (dict-form `metadata_url` fallback) and `MC_STORE_RS_TRANSPORT_METADATA_URL` (dict-form `transport_metadata_url` fallback; defaults to `P2PHANDSHAKE` when unset)
- `MC_STORE_RS_TRANSPORT_BACKEND=tent|classic_te`
- `MC_STORE_RS_KEYSPACE`, `MC_STORE_RS_STABLE_ID`, `MC_STORE_RS_TENANT`, `MC_STORE_RS_LABELS`
- `MC_STORE_RS_ROUTED_WRITES=1`, `MC_STORE_RS_REPLICA_COUNT=<n>`, `MC_STORE_RS_ROUTE_TOPK=<n>`
- `MC_STORE_RS_ROUTE_CONTROL=embedded_wrh|metadata_only`
- `MC_STORE_RS_TRANSPORT_RPC_PORT`, `MC_STORE_RS_LOCAL_SEGMENT_NAME`
- `MC_STORE_RS_INITIAL_STATE`, `MC_STORE_RS_EXPIRES_AT_MS`
- `MC_STORE_RS_METRICS_ADDR=host:port`
- `MC_STORE_RS_CONTROL_PLANE_THREADS=<n>` to tune concurrent control-plane RPC client capacity; default `2`
- `MC_STORE_RS_CONTROL_PLANE_SERVER_THREADS=<n>` to tune embedded control-plane gRPC server capacity; default `4`

Explicit `setup(...)` arguments still take precedence. `MC_STORE_RS_METRICS_ADDR` starts the Python real-client `/metrics` endpoint after `setup(...)`. `MC_STORE_RS_LABELS` accepts either JSON (`{"storage":"false","pool":"rw"}`) or comma-separated pairs (`storage=false,pool=rw`).

## Replication Policy

`ReplicateConfig` currently supports:

- `replica_num`
- `preferred_segment`
- `preferred_segments`
- `preferred_storage_owner`
- `preferred_storage_owners`
- `prefer_local`
- `with_soft_pin`
- `prefer_alloc_in_same_node`

These values map to the Rust `ReplicationPolicy` used by `StoreClient`.

## Validation Scripts

Validation entry points are now grouped by purpose:

- `scripts/run-all-tests.sh` — unified discovery + execution entrypoint for shell-based regressions
- `scripts/build/` — wheel build, wheel install, and coverage helpers
- `scripts/clients/` — black-box real/dummy read-write validators
- `scripts/e2e/` — generic compatibility and stress runners
- `scripts/lib/` — shared shell bootstrap helpers used by script entrypoints
- `scripts/sglang/` — SGLang-specific compatibility and true e2e runners
- `scripts/tests/client/` — standalone client CLI regressions
- `scripts/tests/rolling/` — rolling-upgrade and rollback regressions

Run the full shell-based scripts regression suite:

```bash
./scripts/run-all-tests.sh
```

Inspect what the runner will execute:

```bash
./scripts/run-all-tests.sh --list
```

Typical scoped runs:

```bash
./scripts/run-all-tests.sh --skip-tag sglang
./scripts/run-all-tests.sh --tag rolling
```

Run the Python compatibility validation:

```bash
./scripts/e2e/run-python-compat-e2e.sh
```

Run the black-box real-mode reader / writer validator:

```bash
python3 ./scripts/clients/real_client_rw.py --help
```

Run the hot-upgrade startup validation:

```bash
./scripts/tests/client/test-python-client-hot-upgrade-args.sh
```

This script verifies:

- PyO3 native `setup(..., stable_id, initial_state)` argument parsing; the metadata backend assigns the epoch
- Python wrapper forwarding of hot-upgrade startup arguments into the Rust runtime

Run the native CLI hot-upgrade black-box validation:

```bash
./scripts/tests/client/test-client-hot-upgrade-cli.sh
```

This script verifies:

- an active predecessor and a standby successor can share the same `stable_id`
- `SIGTERM` triggers graceful handoff
- the promoted successor preserves and serves the original payload after takeover

Run the native CLI eviction black-box validation:

```bash
./scripts/tests/client/test-client-eviction-cli.sh
```

This script verifies:

- standalone `mooncake-store-client` storage process startup
- routed writes from the Python compatibility layer into the CLI process
- `put`, `get`, and `batch_get` around a real background eviction cycle
- `/metrics` exposure for `storage_owner_background_eviction` and `storage_owner_evict_one`
- tracing logs for route-owner CAS reclaim

Run the HiCache compatibility validations:

```bash
./scripts/sglang/run-sglang-hicache-dummy-compat.sh
./scripts/sglang/run-sglang-hicache-real-compat.sh
```

Run the full SGLang HiCache e2e from a checkout:

```bash
./scripts/sglang/run-sglang-true-e2e.sh --model-path /models/Qwen3-0.6B
```

Build a portable bundle for another machine:

```bash
./scripts/build/build-sglang-e2e-bundle.sh
```

Then on machine B run:

```bash
cd /path/to/sglang-true-e2e-bundle
./scripts/sglang/run-sglang-true-e2e-bundle.sh --model-path /models/Qwen3-0.6B
```

These runners verify:

- two real storage `mooncake-store-client` processes plus one routed rw-only gateway
- two `python -m sglang.launch_server` processes using the packaged Mooncake backend
- baseline cross-process put/get through Mooncake HiCache
- one lightweight drain request after each writer phase before put metric assertions, matching SGLang's asynchronous write-through backup timing
- storage expansion while requests are still served
- forced storage kill with retry-based recovery instead of persistent request failure
- graceful storage shrink with retry-based recovery instead of persistent request failure
- per-phase completion wall time plus gateway-side operation latency breakdowns for TTFT triage

The true e2e runner prints, for each phase:

- completion wall time in milliseconds for both SGLang servers
- gateway aggregate put/get call counts, total latency, average latency, and peak latency
- focused gateway operation deltas for `batch_put_from`, `batch_get_into`, hot-cache probes, and the Python compat dispatcher bridge

Portable bundle notes:

- machine B installs `mooncake_pro` from `dist/wheels/` inside the bundle
- machine B only needs `python3`, working `python3 -m venv`, `redis-server`, `redis-cli`, and a compatible GPU/SGLang stack
- machine B does not need a source checkout, `git`, `cargo`, `cmake`, or `third_party/Mooncake`
- logs default to `target/sglang-true-e2e/`, overridable with `--workdir` or `MC_STORE_RS_SGLANG_TRUE_E2E_WORKDIR`

Model selection is explicit:

- use `--model-path` or `MC_STORE_RS_SGLANG_MODEL_PATH` for a local model directory
- use `--auto-download-model --model-id <repo>` only when the runner is allowed to download from Hugging Face
- use `--model-cache` or `MC_STORE_RS_SGLANG_MODEL_CACHE` to control the optional download cache

Manual `sglang.launch_server` patterns:

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
mooncake-store-client \
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

  Use this when SGLang should behave like the upstream dummy client. The standalone `mooncake-store-client` process owns the real runtime and exposes a dummy-compatible gRPC endpoint through `client_server_address`.
  Because upstream SGLang still does not forward `worker_scope`, the standalone `mooncake-store-client --client-server-address` path now defaults its dummy side-channel scope to `worker-1` when `keyspace` is absent. That matches omitted-scope `setup_dummy(...)` clients in the serving process, so the default SGLang gateway topology can register shared-memory buffers without requiring a patched SGLang fork.
  The standalone gateway may still bind `client_server_address` on `0.0.0.0` while dummy clients dial a concrete service IP. The dummy side channels now publish matching aliases for that concrete host string, so this wildcard-bind topology keeps working for registered-buffer and hot-cache paths.

Port role summary:

- real mode publishes `local_hostname[:transport_rpc_port]` to peers and does not use `client_server_address`
- dummy mode only needs `client_server_address`
- the current `run-sglang-true-e2e.sh` validation path uses the dummy/gateway topology

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
