# Structured RL Data Transfer Helpers

Mooncake provides Python helpers for transferring structured RL training data through Mooncake Store without flattening the whole object into a single serialized blob. The helpers target common RL data containers used by training, rollout, reward, and data-processing components across RL systems.

## Scope

`mooncake.dataproto_transfer` transfers three logical parts of a structured data object:

- `batch`: dense tensors stored as zero-copy Mooncake objects.
- `non_tensor_batch`: object arrays encoded leaf-by-leaf with specialized codecs for tensors, numeric ragged data, bytes/media, JSON-compatible values, and trusted pickle fallback.
- `meta_info`: metadata stored separately and restored during materialization.

This represents the full structured RL data object, not only tensor fields.

## Container Contract

The object passed to `put_dataproto()` should expose:

- `batch`: mapping from field name to tensor-like values, or `None`.
- `non_tensor_batch`: mapping from field name to `numpy.ndarray(dtype=object)`.
- `meta_info`: metadata dictionary.
- `__len__()` returning row count.
- `slice(start, end)` for row sharding.

Materialization requires passing a compatible container class through `data_cls`:

```python
backend = MooncakeDataProtoTransferBackend(store, data_cls=MyStructuredData)
```

The class must provide:

- `from_dict(tensors=..., non_tensors=..., meta_info=...)`
- `concat(list_of_parts)` when materializing sharded references
- constructor compatibility for metadata-only objects with no tensor batch

## Basic Usage

```python
from mooncake.dataproto_transfer import (
    DataProtoMaterializePolicy,
    DataProtoShardPolicy,
    MooncakeDataProtoTransferBackend,
    create_mooncake_store_from_env,
)

backend = MooncakeDataProtoTransferBackend(
    create_mooncake_store_from_env(),
    key_prefix="rl/dataproto",
    data_cls=MyStructuredData,
)

ref = backend.put_dataproto(
    structured_data,
    partition="scheduler-0",
    shard_policy=DataProtoShardPolicy(adaptive=True),
)

try:
    materialized = backend.materialize_dataproto(
        ref,
        materialize_policy=DataProtoMaterializePolicy(max_inflight_get=4),
    )
finally:
    backend.remove_dataproto(ref)
```

The class and method names keep `DataProto` for API compatibility, but the implementation is framework-neutral and only relies on the container contract above.

## Store Setup from Environment

`create_mooncake_store_from_env()` is a convenience helper around `MooncakeDistributedStore.setup()`.

| Environment variable | Default | Meaning |
| --- | --- | --- |
| `LOCAL_HOSTNAME` | `localhost` | Local node address passed to Mooncake Store. |
| `MC_METADATA_SERVER` | `127.0.0.1:2379` | Metadata server address. Use the deployment-specific endpoint in real clusters. |
| `MOONCAKE_GLOBAL_SEGMENT_SIZE` | `3200 MiB` | Global segment size. |
| `MOONCAKE_LOCAL_BUFFER_SIZE` | `512 MiB` | Local buffer size used during store setup. |
| `PROTOCOL` | `tcp` | Transport protocol. Use `rdma` for performance measurements. |
| `DEVICE_NAME` | `ibp6s0` | RDMA device name when using RDMA. |
| `MASTER_SERVER` | `127.0.0.1:50051` | Mooncake master address. |

The defaults are for local smoke tests. Performance reports should explicitly list the runtime configuration and should not treat TCP or in-memory runs as RDMA baselines.

## Registered Buffer Pool

Materialization uses `RegisteredBufferPool` from `mooncake.registered_buffer_pool` for reusable scratch buffers. The pool keeps buffers registered with Mooncake so repeated range reads avoid registration on the hot path.

Configuration is controlled by:

| Environment variable | Default | Meaning |
| --- | --- | --- |
| `MOONCAKE_REGISTERED_BUFFER_POOL_BYTES` | `2 GiB` | Maximum bytes managed by the process-local registered scratch pool. |
| `MOONCAKE_REGISTERED_BUFFER_POOL_MAX_BUFFER_BYTES` | `512 MiB` | Largest reusable size class. Larger temporary requests are registered and released as oversize buffers. |
| `MOONCAKE_REGISTERED_BUFFER_POOL_PREWARM_BYTES` | `0` | Optional prewarm buffer size. |
| `MOONCAKE_REGISTERED_BUFFER_POOL_PREWARM_COUNT` | `0` | Optional number of buffers to pre-register for the prewarm size class. |

Registration and prewarm costs are setup/fixed costs. When benchmarking online transfer or materialization latency, report pool registration/prewarm time separately from online read time.

Use `get_registered_buffer_pool(store)` to reuse the process-local pool for a store, and `close_registered_buffer_pool(store)` when a process is shutting down or when a store is no longer used. Active leases must be released before closing the pool.

For direct use, import the pool helper from the dedicated module:

```python
from mooncake.registered_buffer_pool import RegisteredBufferPool

with RegisteredBufferPool(store, max_bytes=2 * 1024**3) as pool:
    with pool.acquire(64 * 1024 * 1024) as lease:
        store.get_into(key, lease.ptr, lease.size)
```

## Sharding

Large or row-skewed objects can be split into row shards:

```python
policy = DataProtoShardPolicy(
    adaptive=True,
    target_shard_bytes=2 * 1024**3,
    max_shards=16,
    max_inflight_get=4,
)
ref = backend.put_dataproto(structured_data, shard_policy=policy)
```

Sharded references use a parent manifest plus child manifests. Cleanup removes child shards before the parent manifest so a failed cleanup does not orphan child objects behind a missing parent manifest.

## Failure Handling and Ownership

The caller owns the returned remote reference and should remove it after successful or failed consumption:

```python
ref = backend.put_dataproto(structured_data)
try:
    use(backend.materialize_dataproto(ref))
finally:
    backend.remove_dataproto(ref)
```

The helper cleans up partial writes when local write failures are observed, including partial `batch_put_from()` and native `put_parts()` failures. If a producer process dies after writing an object but before returning the reference to the caller, external lifecycle management such as a scheduler-side registry or TTL cleanup is still required.

## Codec Trust Boundary

Specialized codecs are used when possible. The pickle fallback is only appropriate for trusted internal data where both producer and consumer are in the same trust domain. Do not materialize untrusted Mooncake keys with pickle fallback enabled by data shape alone; use an application-level trust boundary or a safer codec policy if untrusted producers can write to the store.

## Validation

Relevant unit tests:

```bash
PYTHONPATH="/path/to/mooncake-wheel" python -m pytest -q \
  mooncake-wheel/tests/test_registered_buffer_pool.py

PYTHONPATH="/path/to/mooncake-wheel" python -m pytest -q \
  mooncake-wheel/tests/test_dataproto_transfer.py
```

The structured data transfer tests use a minimal framework-neutral container defined in the test file. Pure registered-buffer pool tests only require Mooncake's Python package.
