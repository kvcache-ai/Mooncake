# Engram Backend

Mooncake provides Engram support as a storage backend for embedding tables.

The scope is intentionally narrow:

- the caller defines the physical table layout
- the caller uploads one table per head
- the caller provides precomputed `row_ids [B, L, H]`
- Mooncake returns the selected rows as `[B, L, H, D]`

Mooncake does not implement tokenizer compression, N-gram hashing, query logic,
or any other model-side Engram algorithm.

## Configuration

`EngramConfig` contains the physical layout for one Engram layer:

- `table_vocab_sizes`: per-head table sizes `[N_0, N_1, ..., N_{H-1}]`
- `embedding_dim`: row width `D`

For `layer_id`, Mooncake generates one store key per head:

```text
engram:l{layer_id}:h{head_idx}
```

Each key stores a `float32` table with shape `[N_h, D]`.

## Public Interface

Python:

- `Engram(layer_id, config, store=None)`
- `populate(embedding_buffers)`
- `lookup(row_ids)`
- `remove_from_store(force=False)`
- `get_table_vocab_sizes()`
- `get_store_keys()`
- `get_num_heads()`
- `get_embedding_dim()`

C++:

- constructor `Engram(int layer_id, const EngramConfig&, std::shared_ptr<PyClient>)`
- `populate(...)`
- `lookup_rows(...)`
- `remove_from_store(...)`
- metadata getters matching the Python surface

## Data Contract

Populate expects one NumPy `float32` array per head:

```text
embedding_buffers[h].shape == [N_h, D]
```

Lookup expects precomputed row ids:

```text
row_ids.shape == [B, L, H]
```

Lookup returns:

```text
output.shape == [B, L, H, D]
```

## Lookup Flow

Each lookup follows the same backend flow:

1. validate `row_ids` shape and bounds
2. query per-head metadata with `batch_query(...)`
3. directly read tables that are locally mapped
4. fetch missing tables with `batch_get_buffer(...)`
5. materialize the requested rows into the output buffer

Query metadata is cached while the corresponding leases remain valid.

## Validation

The backend enforces these invariants:

- `table_vocab_sizes` is non-empty and every entry is positive
- `embedding_dim` is positive
- `populate(...)` receives exactly one table per head
- every populated table matches `[N_h, D]`
- `lookup(...)` receives a non-empty `[B, L, H]` input
- every row id satisfies `0 <= row_ids[..., h] < N_h`

## Validation Status

This backend is covered by:

- single-node correctness tests in `scripts/test_engram.py`
- single-node benchmark coverage in `scripts/bench_engram_27b.py`

The current implementation is optimized for correctness and a clean backend
boundary. Transfer-engine optimization can be improved independently.
