# Deployment Guide

This document explains how to run `mooncake-store-rs` locally and how to map the runtime to common deployment roles.

## Recommended Tenant Policy Workflow

For tenant-scoped routing and resource policy, prefer this operator workflow:

1. write tenant policy through `mooncake-store-admin policy ...`
2. launch runtimes with tenant identity plus transport/memory configuration
3. let Store-RS resolve and enforce the effective policy from metadata at bootstrap and on request paths

Runtime-local CLI, Python, and environment route/resource knobs remain available as compatibility fallbacks, but they are not the preferred long-term policy authoring surface.

## Requirements

- Rust toolchain
- `cmake` and a C++ toolchain
- `redis-server` and `redis-cli`
- Git submodule support
- Python 3, if you want to run the Python compatibility layer

## Prepare the Repository

Fetch the upstream Mooncake submodule before building the transport layer:

```bash
git submodule update --init --recursive
```

The repository expects the upstream sources at `third_party/Mooncake`.

## Large-Memory Classic RDMA Bring-Up

For `classic_te` deployments on RDMA hosts with very large local storage:

- keep `LocalMemoryConfig::numa_aware(true)` unless you have a measured reason to collapse registration onto one CPU location
- expect startup registration to fan out into multiple initial storage segments only on transports that opt into parallel startup registration; today that means `classic_te` + RDMA
- the runtime now pre-touches startup storage automatically before RDMA MR registration once the total startup storage registration volume reaches `4 GiB`
- metadata publication happens after the local startup registration phase finishes, so operators should treat the segment set as appearing in one startup wave rather than one segment at a time

## Local Validation

### Rust e2e

Run the full Rust end-to-end suite and the built-in batch benchmark:

```bash
./scripts/e2e/run-local-e2e.sh
```

What the script does:

- checks that `third_party/Mooncake` exists
- starts a local Redis instance on port `6380` when needed
- sets `LD_LIBRARY_PATH` for the upstream TE/TENT artifacts
- runs `cargo run --release -p mooncake-store-e2e`

Supported script inputs:

| Variable | Default | Used By |
|----------|---------|---------|
| `MC_STORE_RS_REDIS_PORT` | `6380` | local Redis port |
| `MC_STORE_RS_BENCH_ITERS` | `64` | batch benchmark loop count passed into the local Rust e2e |
| `MC_STORE_RS_VALUE_SIZE` | `4096` | payload size used by e2e |
| `MOONCAKE_UPSTREAM_DIR` | `third_party/Mooncake` | upstream source location |
| `MOONCAKE_UPSTREAM_BUILD_DIR` | `third_party/Mooncake/build-rust` | upstream build output location |

### Python compatibility e2e

Run the Python compatibility validation:

```bash
./scripts/e2e/run-python-compat-e2e.sh
```

What the script does:

- builds `mooncake-store-py`
- exports `PYTHONPATH="$PWD/python"`
- creates two Python clients
- validates single-object, batch, zero-copy, multi-buffer, route, and metrics paths

### Local hot-cache e2e

Run the daemon-local hot-cache validation:

```bash
./scripts/e2e/run-local-hot-cache-e2e.sh
```

What the script does:

- rebuilds or reuses `.venv-wheel`, then builds the standalone `mooncake-store-client` binary
- Phase A validates that a real-mode reader reuses daemon-local cached bytes after the origin key is removed remotely
- Phase B validates that two dummy clients attached to one standalone daemon reuse a shm-backed hot-cache hit
- starts a temporary Redis instance automatically and tears it down after the run

Important inputs:

| Variable | Default | Used By |
|----------|---------|---------|
| `MC_STORE_RS_REFRESH_WHEEL` | `1` | rebuild and reinstall the latest wheel into `.venv-wheel` |
| `MC_STORE_RS_LOCAL_HOT_CACHE_E2E_REDIS_PORT` | auto | temporary Redis port |
| `MC_STORE_RS_LOCAL_HOT_CACHE_E2E_STORAGE_BYTES` | `64 MiB` | storage bytes for the local standalone daemon |
| `MC_STORE_RS_LOCAL_HOT_CACHE_E2E_SCRATCH_BYTES` | `16 MiB` | scratch bytes per local client |
| `MC_STORE_RS_LOCAL_HOT_CACHE_E2E_CACHE_BYTES` | `1 MiB` | hot-cache capacity under test |
| `MC_STORE_RS_LOCAL_HOT_CACHE_E2E_BLOCK_BYTES` | `8192` | hot-cache block size under test |

### Real-mode read/write validation

Run the real-mode black-box validator:

```bash
python3 ./scripts/clients/real_client_rw.py --help
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

### Hot-upgrade validation

Native CLI hot-upgrade validation:

```bash
./scripts/tests/client/test-client-hot-upgrade-cli.sh
```

What the script does:

- builds the standalone `mooncake-store-client` binary
- starts an active predecessor and a standby successor with the same `stable_id`
- writes a real payload through an external routed client
- sends `SIGTERM` to trigger graceful handoff
- verifies that the successor promotes itself and can still read the original payload

Python hot-upgrade argument and wrapper compatibility validation:

```bash
./scripts/tests/client/test-python-client-hot-upgrade-args.sh
```

What the script verifies:

- PyO3 native `setup(..., stable_id, initial_state)` argument parsing; the metadata backend assigns the epoch
- Python wrapper forwarding of hot-upgrade startup arguments into the Rust runtime

### Eviction validation

Native CLI eviction validation:

```bash
./scripts/tests/client/test-client-eviction-cli.sh
```

What the script does:

- builds the standalone `mooncake-store-client` binary
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
./scripts/sglang/run-sglang-hicache-dummy-compat.sh
./scripts/sglang/run-sglang-hicache-real-compat.sh
```

What they validate:

- dummy path through the standalone compatibility server plus shm registration
- real path through the native distributed store runtime plus registered-buffer I/O

Deployment note:

- dummy mode needs a reachable `client_server_address`
- real mode needs a reachable `local_hostname + transport_rpc_port`
- `client_server_address` does not carry real-mode TENT traffic
- Python real-mode validation can provide `local_hostname + transport_rpc_port` either explicitly or through `--local_host host:port`

### True SGLang e2e

Run the full HiCache end-to-end validation:

```bash
./scripts/sglang/run-sglang-true-e2e.sh --model-path /models/Qwen3-0.6B
```

What the script does:

- builds and installs the current Pro wheel into a dedicated SGLang venv unless reuse is requested
- starts two real storage clients plus one routed rw-only gateway client
- starts two `python -m sglang.launch_server` processes against that standalone gateway
- verifies baseline cross-process HiCache write/read through gateway `/metrics`
- issues a one-token drain request after each writer phase before checking put metrics, because SGLang write-through backup is finalized on a later scheduler tick
- starts an extra storage node and verifies SGLang keeps serving requests after expansion
- hard-kills one storage node and retries completions until recovery, validating that requests do not stay broken after forced shrink
- gracefully drains one storage node and retries completions until recovery, validating that requests do not stay broken after shrink
- prints per-phase completion wall time and gateway latency breakdowns so TTFT regressions can be attributed to put/get, hot-cache probe, or compat bridge overhead

Important inputs:

- `--model-path` or `MC_STORE_RS_SGLANG_MODEL_PATH` must point to the local model directory used by SGLang
- `--auto-download-model` or `MC_STORE_RS_SGLANG_AUTO_DOWNLOAD_MODEL=1` opts into Hugging Face download when no local model path is provided
- `--model-id` or `MC_STORE_RS_SGLANG_MODEL_ID` selects the download target; the default is `Qwen/Qwen3-0.6B`
- `--model-cache` or `MC_STORE_RS_SGLANG_MODEL_CACHE` selects the Hugging Face cache directory for optional downloads
- `MC_STORE_RS_SGLANG_SERVER_A_GPU` and `MC_STORE_RS_SGLANG_SERVER_B_GPU` control `--base-gpu-id`; when server B is unset, the runner now picks a different GPU automatically when `nvidia-smi` reports more than one visible device
- `MC_STORE_RS_SGLANG_MEM_FRACTION_STATIC` controls the SGLang `--mem-fraction-static` used by this true e2e path; the default is `0.25`
- `MC_STORE_RS_SGLANG_HICACHE_SIZE_GB` controls the SGLang `--hicache-size` used by this true e2e path; the default is `20` so the host-side HiCache stays larger than the device-side pool on the current dual-A10 validation host
- `SGLANG_HICACHE_MOONCAKE_REUSE_TE` defaults to `0` for this validation path
- logs are written to `target/sglang-true-e2e-*.log`

Manual launch patterns:

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

- real mode uses `setup(...)` and does not use `client_server_address`
- dummy mode uses `setup_dummy(...)` and only needs `client_server_address`
- dummy mode also consumes `MC_STORE_RS_KEYSPACE` as a Python wrapper fallback when `setup_dummy(...)` does not pass `keyspace`, which is how the true SGLang e2e aligns the dummy side-channel namespace with the routed gateway
- the current `run-sglang-true-e2e.sh` workflow validates the dummy/gateway topology

The runner intentionally does not scan local model caches. A missing model path is a configuration error unless auto-download is explicitly enabled.

### Multi-client stress benchmark

Run the process-per-client stress benchmark:

```bash
./scripts/e2e/run-multi-client-stress.sh
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
use `mooncake-store-bench`; see `docs/bench.md`. Its default mode is scratch-only
RW benchmarking (`MC_BENCH_STORAGE_BYTES=0`) against separate `storage=true`
daemons, and it joins `mc/store-rs/v2` when no explicit keyspace is provided.

### Wheel packaging

Build the Python wheel and stage the standalone client binary:

```bash
./scripts/build/build-wheel.sh
```

If the host OS is missing wheel-build dependencies, use the Ubuntu Docker
wrapper and pin the Python runtime explicitly:

```bash
PYTHON_VERSION=3.11 ./scripts/build/build-wheel-ubuntu-docker.sh
```

The Docker wrapper writes the same wheelhouse outputs as the host build script.
It defaults to CN mirrors for rustup, cargo, and pip; set `CN_MIRROR=0` to use
the upstream endpoints instead.

Default outputs:

- `dist/wheels/mooncake-*.whl`
- `dist/wheels/mooncake_pro-*.whl`
- `dist/bin/mooncake-store-client`
- `dist/bin/mooncake-store-bench`

Recommended installation flow:

```bash
python3 -m venv .venv-wheel-test
. .venv-wheel-test/bin/activate
pip install --find-links dist/wheels dist/wheels/mooncake_pro-*.whl
```

Or use the repository helper:

```bash
./scripts/build/install-pro-wheel.sh
```

Operational meaning:

- `mooncake-pro` is the product-facing package users install
- `mooncake` remains the runtime compatibility package imported by Python and expected by integrations such as SGLang
- installing `mooncake-pro` upgrades an existing `mooncake` install to the matching Pro runtime without `--force-reinstall`

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
- a stateless background maintenance loop in `mooncake-store-admin server`
- optional tenant quota reservation reconcile for an explicit tenant list in that same admin process

Run the admin sweep when you want to remove dead-owner segment metadata. The same shape
works with either `redis://...` or `etcd://...` metadata URLs:

```bash
mooncake-store-admin \
  --metadata-url redis://127.0.0.1:6380/0 \
  cleanup-stale-segments
```

Run the stateless admin container shape when cleanup should happen continuously:

```bash
mooncake-store-admin \
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
- tenant quota reconcile remains explicit: the worker only runs for tenants named on the command line, then internally reuses the same repair logic as `mooncake-store-admin quota reconcile`

This keeps the admin pod stateless. If the pod restarts, the next reconcile loop resumes from Redis or etcd metadata instead of relying on in-memory work queues.

You can also manage tenant route policy and inspect strict-quota metadata through the same binary:

```bash
mooncake-store-admin \
  --metadata-url redis://127.0.0.1:6380/0 \
  policy list

mooncake-store-admin \
  --metadata-url redis://127.0.0.1:6380/0 \
  policy list \
  --tenant tenant-a

mooncake-store-admin \
  --metadata-url redis://127.0.0.1:6380/0 \
  policy set \
  --tenant tenant-a \
  --route-topk 3 \
  --route-control embedded-wrh

mooncake-store-admin \
  --metadata-url redis://127.0.0.1:6380/0 \
  quota state \
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
scripts/e2e/run-local-e2e.sh
```

For fast local verification while iterating on quota behavior, reduce benchmark noise and disable RDMA probing:

```bash
MC_STORE_RS_ENABLE_RDMA=0 \
MC_STORE_RS_BENCH_ITERS=1 \
scripts/e2e/run-local-e2e.sh
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
- `mooncake-store-client --transport-backend tent|classic-te`
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

## Next Reading

- `docs/rust.md` for Rust integration
- `docs/configuration.md` for knobs and defaults
- `docs/architecture.md` for request paths and control-plane behavior
