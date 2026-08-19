# EngramStore Backend

Mooncake provides EngramStore as the storage backend for Engram embedding tables.

The scope is intentionally narrow:

- the caller defines the physical table layout
- the caller uploads one table per head
- the caller provides precomputed row ids with shape `[B, L, H]`
- Mooncake returns the selected rows as `[B, L, H, D]`

Mooncake does not implement tokenizer compression, N-gram hashing, query logic,
or any other model-side Engram algorithm.

## Current Backend Boundary

The current implementation is intentionally conservative. It keeps EngramStore
on top of the existing Store interfaces and does not depend on:

- transfer scatter read
- grouped transfer task
- `get_into_range`
- `batch_query`
- local direct mapping
- query cache
- remote gather control-plane changes

Those optimizations are deferred to follow-up PRs so that the EngramStore backend can
land first as a small, reviewable unit.

## Configuration

`EngramStoreConfig` contains the physical layout for one EngramStore layer:

- `table_vocab_sizes`: per-head table sizes `[N_0, N_1, ..., N_{H-1}]`
- `embedding_dim`: row width `D`

For `layer_id`, Mooncake generates one store key per head:

```text
engram:l{layer_id}:h{head_idx}
```

Each key stores a `float32` table with shape `[N_h, D]`.

## Public Interface

Python:

- `EngramStore(layer_id, config, store=None)`
- `populate(embedding_buffers)`
- `lookup(row_ids)`
- `remove_from_store(force=False)`
- `get_table_vocab_sizes()`
- `get_store_keys()`
- `get_num_heads()`
- `get_embedding_dim()`

The Python `store` argument accepts the existing `MooncakeDistributedStore`
wrapper, or `None` for metadata-only construction.

C++:

- constructor `EngramStore(int layer_id, const EngramStoreConfig&, std::shared_ptr<PyClient>)`
- `populate(...)`
- `lookup_rows(...)`
- `lookup_rows_contiguous(...)`
- `remove_from_store(...)`
- metadata getters matching the Python surface

## Data Contract

Populate expects one NumPy `float32` array per head:

```text
embedding_buffers[h].shape == [N_h, D]
```

Lookup accepts either:

- nested Python lists with logical shape `[B, L, H]`, or
- a contiguous NumPy `int64` array with shape `[B, L, H]`

Lookup returns:

```text
output.shape == [B, L, H, D]
```

## Populate Flow

Populate follows the existing Store write path:

1. validate that exactly one table is provided for each head
2. validate that every table matches `[N_h, D]`
3. verify that the target head-table keys do not already exist
4. register each embedding table buffer
5. upload all head tables with `batch_put_from(...)`
6. unregister the staging buffers

`populate(...)` is defined as a create-only operation for one EngramStore layer. To
reuse a `layer_id`, first remove the old tables with `remove_from_store(...)`.

If upload fails after some head tables have already been written, or if publish
finishes but post-write buffer cleanup fails, the backend best-effort removes
the keys written by the failed populate attempt before returning an error.

## Lookup Flow

Each lookup follows the same simplified backend flow:

1. validate the `row_ids` shape and bounds
2. build per-head byte ranges for the requested rows
3. issue one `get_into_ranges(...)` call to materialize those rows into the output buffer

For NumPy `row_ids`, the binding uses a contiguous fast path and builds ranges
directly from the input tensor without first converting the entire input into a
nested C++ container.

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

- correctness tests in `scripts/test_engram_store.py`
- benchmark coverage in `scripts/bench_engram_store_27b.py`

`scripts/test_engram_store.py` can run against an existing Mooncake deployment through
`MOONCAKE_CONFIG_PATH` / `MOONCAKE_MASTER`, or it can start a local
`mooncake_master` instance automatically for a self-contained TCP test run.

By default, the benchmark exercises `engram_store.populate(...)` directly. Its
fallback populate paths are gated behind `ENGRAM_ALLOW_POPULATE_FALLBACK=1` so
they do not silently mask regressions in the current implementation.
