# Python Guide

`mooncake-store-rs` ships a Python compatibility layer in `crates/mooncake-store-py` and a convenience package in `python/mooncake`.

The Python package does not wrap a second store implementation. It reuses the same Rust runtime, control plane, allocator, reclaim logic, tracing, and metrics that the Rust client uses.

## Package Model

The repository now builds two Python wheels with different responsibilities:

- `mooncake-*.whl` is the real runtime package
- `mooncake_pro-*.whl` is the user-facing Pro metapackage

Recommended installation flow:

```bash
pip install --find-links dist/wheels dist/wheels/mooncake_pro-*.whl
```

For local wheelhouse installs you can also use:

```bash
./scripts/install-pro-wheel.sh
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

This mode:

- constructs a Rust `StoreClient`
- registers local memory and participates in route / allocator control plane RPC
- participates in the same hit-report and replica-route tracking RPC used by Rust clients
- supports routed writes, replication policy, segment lifecycle, metrics, and tracing
- is the normal path for Python clients that should behave like store-rs nodes

### Dummy mode

Use `setup_dummy(...)` when Python should behave like the upstream dummy compatibility client.

This mode:

- connects to a standalone `mooncake-store-client` process over gRPC
- registers shm regions by passing file descriptors over a Unix socket
- keeps the Python process out of the distributed control plane
- is the compatibility path used by the HiCache dummy flow

Dummy mode is intentionally narrower than real mode. It exists to preserve compatibility for callers that expect the old dummy client / standalone server split.

The standalone server behind dummy mode still uses the same Rust runtime internally, so route publication, reclaim, eviction, tracing, and metrics stay aligned with the real path.

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
./scripts/build-wheel.sh
```

By default the script:

- creates or reuses `.venv-wheel`
- installs `maturin`
- installs `build` for the Pro metapackage
- embeds the standalone `mooncake-store-client` binary into the runtime wheel package
- builds both wheels into `dist/wheels/`
- copies the standalone `mooncake-store-client` artifact into `dist/bin/`

Common variants:

```bash
./scripts/build-wheel.sh --interpreter python3.11
DIST_DIR=artifacts ./scripts/build-wheel.sh
```

Install the wheel into any compatible virtualenv:

```bash
pip install --find-links dist/wheels dist/wheels/mooncake_pro-*.whl
```

After installation, both interfaces are available:

- `python -c "import mooncake"` loads the native extension
- `python -c "import mooncake; print(mooncake.__version__, mooncake.__edition__)"` shows the active Pro runtime
- `mooncake-store-client --help` runs the packaged standalone client command
- if your environment still exposes the upstream compatibility alias, `mooncake_master --version` prints the same packaged Pro version banner

## Standalone Client Binary

You can build the standalone compatibility server directly:

```bash
cargo build -p mooncake-store-py --bin mooncake-store-client --release
```

Or use `./scripts/build-wheel.sh`, which also copies the binary to `dist/bin/`.

When the client is installed from a wheel, the same binary is also embedded inside the package and exposed through the `mooncake-store-client` console script, matching the upstream Mooncake packaging style.

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

- `--transport-metadata-url` for TENT Redis when the metadata backend uses etcd
- `--transport-rpc-port <port>` to pin the TENT TCP data-plane port used by real clients
- `--routed-writes` and `--replica-count` to enable routed writer mode
- `--route-topk <n>` to control WRH route-authority fanout; it must be `>= 2` and match the policy already stored in the metadata keyspace
- `--route-control metadata-only|embedded-wrh` to select the route authority mode
- `--heartbeat-interval-ms` and `--lease-ttl-ms` to tune lease refresh; `--lease-ttl-ms` defaults to `30000`
- `--drain-on-exit` to enter draining mode and evacuate owned replicas before shutdown
- `--client-server-address host:port` to expose the standalone compatibility server for dummy clients only
- `--use-hugepage` and `--hugepage-size 2MB|1GB` to enable hugepage-backed local memory

Role reminder:

- use `--label storage=true` on storage nodes that should accept routed placement and run local CLOCK eviction
- use `--label storage=false` on routed rw nodes that should place remotely without owning local storage
- `--label storage=true` requires `--storage-bytes > 0`
- when `--storage-bytes 0` is used without an explicit storage label, the runtime defaults to `storage=false`

Port reminder:

- `transport_rpc_port` / `--transport-rpc-port` is the real-mode TENT data-plane port
- `client_server_address` / `--client-server-address` is the dummy compatibility gRPC port
- `metrics_addr` / `--metrics-addr` is only for `/metrics`
- cross-host real-mode deployments should set both a reachable `local_hostname` and a fixed `transport_rpc_port`

## Basic Real-Mode Example

```python
from mooncake.store import MooncakeDistributedStore

store = MooncakeDistributedStore()
store.setup(
    "127.0.0.1",
    "redis://127.0.0.1:6380/0",
    128 * 1024 * 1024,
    16 * 1024 * 1024,
    "tcp",
    "",
    "",
    stable_id="py-store-a",
    tenant="default",
    labels={"pool": "pool-a", "storage": "true"},
    transport_rpc_port=17111,
)

store.put("hello", b"world")
assert store.get("hello") == b"world"
```

## Routed Writes

```python
from mooncake.store import MooncakeDistributedStore, ReplicateConfig

store = MooncakeDistributedStore()
store.setup(
    "127.0.0.1",
    "redis://127.0.0.1:6380/0",
    128 * 1024 * 1024,
    16 * 1024 * 1024,
    "tcp",
    "",
    "",
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
store.setup_dummy(64 * 1024 * 1024, 16 * 1024 * 1024, "127.0.0.1:16590")

allocator = MooncakeHostMemAllocator()
ptr = allocator.alloc(4096)
store.register_buffer(ptr, 4096)
```

`setup_dummy(...)` only needs `client_server_address`. It does not consume `transport_rpc_port`, because the standalone server owns the real store runtime and TENT endpoint on behalf of the dummy client.

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

## Batch Read Soft-Miss Semantics

The real backend keeps batch read behavior compatible with upstream Mooncake while avoiding Python-side crashes on transient backend failures.

- `batch_get_into(...)` returns copied lengths and uses `-1` for soft misses
- `batch_get_into_multi_buffers(...)` returns copied lengths and uses `-1` for soft misses
- `batch_is_exist(...)` returns `1` for hit, `0` for miss, and negative status codes for soft backend failures
- soft-miss downgrade covers `NotFound`, `InvalidState`, metadata errors, and transport errors on the real backend
- single-key `get(...)` and `get_into(...)` keep normal exception behavior

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

### Important note for etcd

When the store metadata backend is etcd, TENT transport metadata still uses Redis. Provide that Redis endpoint through:

- `transport_metadata_url=...`, or
- `MC_STORE_RS_TENT_REDIS_URL`

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

Run the Python compatibility validation:

```bash
./scripts/run-python-compat-e2e.sh
```

Run the hot-upgrade startup validation:

```bash
./scripts/test-python-client-hot-upgrade-args.sh
```

This script verifies:

- PyO3 native `setup(..., stable_id, epoch, initial_state)` argument parsing
- Python wrapper forwarding of hot-upgrade startup arguments into the Rust runtime

Run the native CLI hot-upgrade black-box validation:

```bash
./scripts/test-client-hot-upgrade-cli.sh
```

This script verifies:

- an active predecessor and a standby successor can share the same `stable_id`
- `SIGTERM` triggers graceful handoff
- the promoted successor preserves and serves the original payload after takeover

Run the native CLI eviction black-box validation:

```bash
./scripts/test-client-eviction-cli.sh
```

This script verifies:

- standalone `mooncake-store-client` storage process startup
- routed writes from the Python compatibility layer into the CLI process
- `put`, `get`, and `batch_get` around a real background eviction cycle
- `/metrics` exposure for `storage_owner_background_eviction` and `storage_owner_evict_one`
- tracing logs for route-owner CAS reclaim

Run the HiCache compatibility validations:

```bash
./scripts/run-sglang-hicache-dummy-compat.sh
./scripts/run-sglang-hicache-real-compat.sh
```

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
