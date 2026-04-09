# Python Guide

`mooncake-store-rs` ships a Python compatibility layer in `crates/mooncake-store-py` and a convenience package in `python/mooncake`.

## What the Python Layer Provides

- `MooncakeDistributedStore`: the main client wrapper
- `ReplicateConfig`: request-level replication policy
- tracing and metrics helpers
- batch and zero-copy style operations exposed through Python-friendly APIs

## Build

```bash
git submodule update --init --recursive
cargo build -p mooncake-store-py
export PYTHONPATH="$PWD/python"
```

The Python package loads the native extension from the local `target` directory and preloads the upstream Mooncake TE/TENT shared libraries from `third_party/Mooncake/build-rust`.

## Build a Wheel

```bash
python3 -m venv .venv-build
. .venv-build/bin/activate
python -m pip install -U pip maturin
maturin build --release
```

The wheel is written to `target/wheels/`. It contains:

- the `mooncake` Python package
- the `mooncake._store_rs` native extension
- the repaired runtime shared libraries needed by the extension on Linux

Install the wheel into any compatible virtualenv:

```bash
pip install target/wheels/mooncake_store_rs-*.whl
```

## Standalone Client Binary

Build the standalone client runtime:

```bash
cargo build -p mooncake-store-py --bin mooncake-store-client --release
```

Start a storage client:

```bash
./target/release/mooncake-store-client \
  --local-hostname 127.0.0.1 \
  --metadata-url redis://127.0.0.1:6380/0 \
  --storage-bytes $((128 * 1024 * 1024)) \
  --scratch-bytes $((16 * 1024 * 1024)) \
  --stable-id store-a \
  --tenant default \
  --label pool=pool-a \
  --metrics-addr 127.0.0.1:9091
```

Useful flags:

- `--transport-metadata-url` for TENT Redis when the metadata backend uses etcd
- `--routed-writes` and `--replica-count` to enable routed writer mode
- `--route-control metadata-only|embedded-wrh` to select the route authority mode
- `--heartbeat-interval-ms` and `--lease-ttl-ms` to tune lease refresh
- `--drain-on-exit` to enter draining mode and evacuate owned replicas before shutdown

## Basic Example

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
)

policy = ReplicateConfig(
    replica_num=2,
    prefer_local=False,
    preferred_storage_owners=["store-b"],
)
store.put("key", b"payload", config=policy)
```

## Supported Operations

### Basic I/O

- `put`, `get`, `remove`
- `batch_put`, `batch_get`, `batch_remove`
- `query_route`, `is_exist`, `get_size`

### Buffer-oriented I/O

- `register_buffer`, `unregister_buffer`
- `put_from`
- `batch_put_from`
- `batch_put_from_multi_buffers`
- `batch_get_into`
- `batch_get_into_multi_buffers`

### Lifecycle and capacity

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

## Local Development

Run the compatibility validation script:

```bash
./scripts/run-python-compat-e2e.sh
```

This validates:

- setup and native loading
- single-key operations
- batch operations
- route query behavior
- replication policy handling
- metrics exposure
