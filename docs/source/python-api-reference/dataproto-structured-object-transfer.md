# DataProto and flat dict structured object usage

Mooncake can store DataProto-like objects and flat dictionaries as structured objects so callers can pass a lightweight handle between stages and materialize only the fields they need.

A DataProto-like object is any object with these mapping-like attributes:

- `batch`: tensor or ndarray fields indexed by batch row.
- `non_tensor_batch`: per-row non-tensor fields.
- `meta_info`: small metadata for the whole batch.

Plain dictionaries are also accepted on the DataProto path. A dictionary with only `batch`, `non_tensor_batch`, and `meta_info` keys is treated as an envelope; other dictionaries are treated as `batch` fields. Use `type="dict"` for the flat dictionary path when the input is an ordinary dictionary and should be routed into `batch`, `non_tensor_batch`, and `meta_info` by Mooncake.

## Public API

```python
from mooncake.structured_object_store import (
    BundleTransferPolicy,
    MooncakeBundleTransfer,
    export_dataproto_ref,
    import_dataproto_ref,
    tensor_object_buffer,
)

transfer = MooncakeBundleTransfer(store, key_prefix="rl")

ref = transfer.put(data, type="dataproto")
subset = transfer.get(ref, type="dataproto", fields=["input_ids", "old_log_probs"])

dict_ref = transfer.put(payload_dict, type="dict")
dict_result = transfer.get(dict_ref, type="dict")

# Compatibility helpers remain available for callers that prefer typed names.
ref = transfer.put_dataproto(data)
subset = transfer.get_dataproto(ref, fields=["input_ids", "old_log_probs"])
dict_ref = transfer.put_dict(payload_dict)
dict_result = transfer.get_dict(dict_ref)

ref = transfer.append_dataproto_fields(
    ref,
    logprob_data,
    stage="old_log_prob",
)

handle = export_dataproto_ref(ref)
ref = import_dataproto_ref(handle)
transfer.cleanup_dataproto(ref)
```

## Lightweight handles

`MooncakeDataProtoRef` contains only DataProto-level routing information:

- `stage_refs`: stage name to structured object reference.
- `field_index`: field name to `(stage, member, section)`.
- `batch_size`, `namespace`, `partition`, `meta_info`, and optional `global_indexes`.

It does not duplicate dtype, shape, chunk layout, or range metadata. Those details remain in the structured object manifest. Use `dataproto_manifest_view(ref)` when a caller needs an introspection view derived from the manifests.

For process boundaries, use `export_dataproto_ref(ref)`. The exported handle is JSON-safe and contains manifest keys instead of embedded manifest payloads. `get_dataproto()`, `append_dataproto_fields()`, `dataproto_manifest_view()`, and `cleanup_dataproto()` accept either an in-memory ref or an exported handle.

## Writing fields

`put(..., type="dataproto")` writes one structured object for the requested stage. `append_dataproto_fields()` writes another structured object and updates the handle. Existing fields are not rewritten.

`put(..., type="dict")` writes a flat dictionary through the same structured object path. Numeric tensor-like values are stored as `batch` fields, row-aligned non-tensor sequences are stored as `non_tensor_batch`, and scalar or global metadata stays in `meta_info`. Pass `field_schemas` when list-valued metadata would otherwise be ambiguous.

`put_dataproto()` / `get_dataproto()` and `put_dict()` / `get_dict()` are thin compatibility helpers over the unified `put()` / `get()` API. Older `put_legacy_dict()` / `get_legacy_dict()` names are kept as aliases.

Duplicate field names are rejected by default. Use `overwrite=True` only when replacing all fields from an existing stage; after the new stage object is written successfully, the old stage object is removed.

Field names are global within a ref. A `batch` field and a `non_tensor_batch` field cannot use the same name.

Store write configuration is passed through with `config`. Mooncake does not interpret this object in the structured layer; it forwards it to the lower-level write APIs such as `put`, `put_from`, and `batch_put_from`. For example, callers can use the same `ReplicateConfig` object they would pass to the lower-level store APIs:

```python
from mooncake.store import ReplicateConfig

config = ReplicateConfig()
config.replica_num = 2

ref = transfer.put(data, type="dict", config=config)
```

## Reading fields

`get_dataproto()` supports:

- `fields`: mixed batch and non-tensor field selection.
- `batch_fields`: batch-only selection.
- `non_tensor_fields`: non-tensor-only selection.
- `meta_info_keys`: metadata selection.
- `data_cls`: return a DataProto-like class instead of a plain dict.
- `destinations`: caller-provided output buffers.
- `rows`: row selection with a Python `slice`, `StructuredMemberSlice`, or an integer row-index sequence.

Use `rows` to materialize the same batch rows across all selected `batch` and `non_tensor_batch` fields:

```python
subset = transfer.get_dataproto(
    ref,
    fields=["input_ids", "text", "rewards"],
    rows=slice(128, 256),
)

gathered = transfer.get_dataproto(
    ref,
    batch_fields=["input_ids"],
    rows=[7, 3, 9],
)
```

Row selection supports `axis=0`. Tensor and ndarray batch fields, including native Mooncake tensor fields, are read by byte ranges from the stored payload. Structured object `non_tensor_batch` fields read only the selected row metadata and payload ranges. `destinations` may be combined with `rows` for fields that support caller-provided output buffers. Use `raw_destination(ptr, size, owner, pre_registered=True)` for BufferPool or otherwise pre-registered destination memory.

The result is a plain dictionary when `data_cls` is omitted:

```python
{
    "batch": {...},
    "non_tensor_batch": {...},
    "meta_info": {...},
}
```

If `data_cls` is provided, Mooncake first tries `data_cls.from_dict(batch, non_tensor_batch, meta_info=meta_info)`, then falls back to `data_cls(batch=..., non_tensor_batch=..., meta_info=...)`.

## Tensor and ndarray behavior

Tensor fields are stored through the best available Mooncake path:

1. `tensor_object_buffer(ptr, size, owner, batch_size=...)` with `copy_mode="zero_copy"` uses `put_tensor_from()` directly.
2. Torch tensors use the store tensor API when available.
3. If a tensor-native path is unavailable, Mooncake falls back to a serialized tensor payload.
4. Scalar tensors use a correctness fallback until the native tensor codec preserves zero-dimensional shape.

Numeric numpy arrays are stored as structured ndarray members. Contiguous arrays can be passed through without copying; non-contiguous arrays are made contiguous before storage. Row slices are materialized through structured range reads when the backend supports them.

`non_tensor_batch` object arrays are structured-encoded according to their contents. Numeric scalar object arrays, ragged tensors, bytes, strings, JSON-like values, and selected media payloads have explicit codecs. These fields are serialized by design and should not be treated as zero-copy tensor data.

## Materializing into caller buffers

`destinations` can reuse caller-provided buffers:

```python
dst = np.empty((rows, width), dtype=np.int64)
result = transfer.get_dataproto(ref, batch_fields=["input_ids"], destinations={"input_ids": dst})
assert result["batch"]["input_ids"] is dst
```

For tensor payloads stored as Mooncake tensor objects or tensor-object buffers, pass a `tensor_object_buffer` destination:

```python
lease = pool.acquire(nbytes)
result = transfer.get_dataproto(
    ref,
    batch_fields=["hidden_states"],
    destinations={
        "hidden_states": tensor_object_buffer(
            lease.ptr,
            lease.size,
            lease,
            batch_size=batch_size,
        )
    },
)
```

The destination owner or lease must remain alive for as long as the materialized data may be used.

## Copy policy

Control-plane metadata and manifests are small and always use the copy path. Tensor and ndarray payloads use the configured payload policy:

```python
policy = BundleTransferPolicy(copy_mode="zero_copy")
transfer.put_structured_object(payload, policy=policy)
```

`copy_mode="zero_copy"` requires tensor payloads to be provided as `tensor_object_buffer`; plain torch tensors are rejected because they do not expose a registered tensor-object buffer.

`policy` controls how structured payload buffers are copied or transferred. `config` is separate: it is forwarded to the underlying Mooncake Store write calls and can carry store-level placement or replication options.

## Cleanup

`cleanup_dataproto(ref)` removes all stage objects referenced by the handle. It accepts both in-memory refs and exported transport handles.
