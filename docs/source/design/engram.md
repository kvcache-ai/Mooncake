# Engram: Mooncake Lookup Backend

## Overview

This PR adds an Engram lookup backend to Mooncake Store.

The backend manages per-layer, per-head embedding tables in Mooncake Store and
serves row-id based lookup requests for those tables.

The integration contract is simple:

- the caller provides the physical table layout
- the caller uploads one embedding table per head
- the caller provides precomputed row IDs for each lookup
- Mooncake returns the requested embedding rows in `[B, L, H, D]`

## Configuration

`EngramConfig` describes the physical layout of one Engram layer backend.

Fields:

- `table_vocab_sizes`
  - final table size of each head `[N_0, N_1, ..., N_{H-1}]`
- `embedding_dim`
  - row width `D`

For `layer_id`, Mooncake generates one store key per head:

```text
engram:l{layer_id}:h{head_idx}
```

Each key stores one 2D table:

```text
[N_h, D]
```

## Public API

The Python-facing API consists of four parts.

### 1. Metadata helpers

- `get_table_vocab_sizes()`
- `get_num_heads()`
- `get_embedding_dim()`
- `get_store_keys()`

These helpers expose the backend layout owned by the current `Engram` instance.

### 2. Populate

```python
engram.populate(embedding_buffers)
```

`embedding_buffers` contains one NumPy `float32` array per head.

For head `h`, the expected shape is:

```text
[N_h, D]
```

The populate path:

1. validates the number of buffers and their shapes
2. registers the source buffers with Mooncake Store
3. uploads them with `batch_put_from(...)`
4. refreshes cached metadata for future reads

### 3. Lookup

```python
output = engram.lookup(row_ids)
```

`row_ids` is a 3D tensor-like object with shape:

```text
[B, L, H]
```

Where:

- `B` is batch size
- `L` is sequence length
- `H` is number of heads

The output is a NumPy `float32` array with shape:

```text
[B, L, H, D]
```

For each token position and head, Mooncake returns the row selected by the
corresponding row ID.

### 4. Cleanup

```python
removed = engram.remove_from_store(force=False)
```

This removes the store objects owned by the current layer instance and returns
how many head tables were removed.

## Read Path

A lookup request follows this flow:

1. validate `row_ids` shape and bounds
2. resolve per-head metadata with `batch_query(...)`
3. use direct local reads when a replica is locally mapped
4. otherwise fetch full table buffers with `batch_get_buffer(...)`
5. materialize the final result in `[B, L, H, D]`

The implementation keeps a cache of query metadata so repeated reads can reuse
resolved placement information while leases remain valid.

## Validation

The backend validates the following invariants:

- `table_vocab_sizes` is non-empty and every entry is positive
- `embedding_dim` is positive
- `populate(...)` receives exactly one table per head
- each populated table matches `[N_h, D]`
- `lookup(row_ids)` receives shape `[B, L, H]`
- every row ID satisfies `0 <= row_ids[..., h] < N_h`

## Implemented in This PR

This PR implements:

- a physical table layout for Engram data in Mooncake Store
- a batch upload path for per-head embedding tables
- a row-id based lookup API
- local-read and full-buffer fallback paths
- per-layer cleanup of owned store objects
- Python bindings, tests, and benchmark scripts for this backend

## Related Documents

- [Engram Python API](../python-api-reference/engram.md)
- [DeepSeek Engram: Paper and Demo Notes](./deepseek-engram-paper-demo-notes.md)
- [Mooncake Store](./mooncake-store.md)

## References

- **Paper**: [Conditional Memory via Scalable Lookup: A New Axis of Sparsity for Large Language Models](https://arxiv.org/abs/2601.07372)
- **Reading Notes**: [DeepSeek Engram: Paper and Demo Notes](./deepseek-engram-paper-demo-notes.md)
