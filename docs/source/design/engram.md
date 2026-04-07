# Engram Backend

Mooncake provides Engram support as a storage backend for embedding tables.

The scope is intentionally narrow:

- the caller defines the physical table layout
- the caller uploads one table per head
- the caller provides precomputed row ids with shape `[B, L, H]`
- Mooncake returns the selected rows as `[B, L, H, D]`

Mooncake does not implement tokenizer compression, N-gram hashing, query logic,
or any other model-side Engram algorithm.

## Current Backend Boundary

The current implementation is intentionally conservative. It keeps the Engram
backend on top of the existing Store interfaces and does not depend on:

- transfer scatter read
- grouped transfer task
- `get_into_range`
- `batch_query`
- local direct mapping
- query cache
- remote gather control-plane changes

Those optimizations are deferred to follow-up PRs so that the Engram backend can
land first as a small, reviewable unit.

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

The Python `store` argument accepts the existing `MooncakeDistributedStore`
wrapper, or `None` for metadata-only construction.

C++:

- constructor `Engram(int layer_id, const EngramConfig&, std::shared_ptr<PyClient>)`
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
3. register each embedding table buffer
4. upload all head tables with `batch_put_from(...)`
5. unregister the staging buffers

If upload fails after some head tables have already been written, the backend
best-effort removes the partially populated keys before returning an error.

## Lookup Flow

Each lookup follows the same simplified backend flow:

1. validate the `row_ids` shape and bounds
2. fetch per-head tables with the existing `batch_get_buffer(...)` interface
3. materialize the requested rows into the output buffer

For NumPy `row_ids`, the binding uses a contiguous fast path and copies rows
directly into the output tensor without first converting the entire input into a
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

- correctness tests in `scripts/test_engram.py`
- benchmark coverage in `scripts/bench_engram_27b.py`

`scripts/test_engram.py` can run against an existing Mooncake deployment through
`MOONCAKE_CONFIG_PATH` / `MOONCAKE_MASTER`, or it can start a local
`mooncake_master` instance automatically for a self-contained TCP test run.

By default, the benchmark exercises `engram.populate(...)` directly. Its
fallback populate paths are gated behind `ENGRAM_ALLOW_POPULATE_FALLBACK=1` so
they do not silently mask regressions in the current implementation.
