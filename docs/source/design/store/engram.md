# EngramStore Backend

Mooncake provides EngramStore as the storage backend for Engram embedding tables.

The scope is intentionally narrow:

- the caller defines the physical table layout
- the caller uploads one table per head
- the caller provides precomputed row ids with shape `[B, L, H]`
- Mooncake writes selected rows into caller-owned `[B, L, H, row_bytes]` memory

Mooncake does not implement tokenizer compression, N-gram hashing, query logic,
or any other model-side Engram algorithm.

## Current Backend Boundary

EngramStore uses the existing `batch_put_from`, `batch_query`, and
`get_into_ranges` Store interfaces. It stores one object per head and reads only
the requested byte ranges. Lookup is synchronous; the Python binding releases
the GIL during I/O. There is no model-specific hashing or GPU execution here.

## Configuration

`EngramStore` manages all Engram layers of a model. Its constructor accepts a
map from layer ID to `EngramStoreConfig`; each config describes one layer:

- `table_vocab_sizes`: per-head table sizes `[N_0, N_1, ..., N_{H-1}]`
- `row_bytes`: required positive byte width of each row; the default `0` must be set before constructing the store

```python
layer1 = EngramStoreConfig()
layer1.table_vocab_sizes = [17, 19]
layer1.row_bytes = 264
layer14 = EngramStoreConfig()
layer14.table_vocab_sizes = [23, 29]
layer14.row_bytes = 264
table = EngramStore({1: layer1, 14: layer14}, store)
```

The constructor copies layouts and creates no stored data. Each layer may have
different head counts, table sizes and row widths. For `layer_id`, Mooncake
generates one store key per head:

```text
engram:l{layer_id}:h{head_idx}
```

Each key stores a contiguous `uint8` table with shape `[N_h, row_bytes]`.
The backend treats rows as opaque bytes. Callers pack and interpret embedding
values and any quantization scales. For example, a DeepSeek-V4.1 hash-head row
contains 256 FP8 bytes followed by 8 E8M0 scale bytes, so `row_bytes = 264`.
There is no dtype mode or configurable key prefix. Models with overlapping layer
IDs need separate Store deployments or explicit removal before replacement.

## Public Interface

Python:

- `EngramStore(layers, store=None)`
- `populate(layer_id, embedding_buffers, config=ReplicateConfig())`
- `bind_local(layer_id, embedding_buffers)`
- `lookup_into(layer_id, row_ids, output)`
- `remove_from_store(layer_id, force=False)`
- `get_layer_ids()`
- `get_table_vocab_sizes(layer_id)`
- `get_store_keys(layer_id)`
- `get_num_heads(layer_id)`
- `get_row_bytes(layer_id)`

The Python `store` argument accepts the existing `MooncakeDistributedStore`
wrapper, or `None` for metadata-only construction or local table binding.

### Local table mode

Construct without a Store client and bind immutable, contiguous `uint8` tables
before starting lookup workers:

```python
table = EngramStore({1: layer1})
heads = [np.memmap(path, mode="r", dtype=np.uint8, shape=(rows, layer1.row_bytes))
         for path, rows in zip(paths, layer1.table_vocab_sizes)]
table.bind_local(1, heads)
table.lookup_into(1, row_ids, output)
```

This mode copies selected rows directly from CPU-addressable memory into output;
it performs no Store metadata queries, registration, or network transfers. The
Python binding retains the supplied list and arrays. Do not change that list,
modify/resize the arrays, or truncate/unmap their backing files while bound.
Binding a layer twice, binding with a Store client, and lookup of an unbound
layer are rejected. C++ callers retain ownership of the bound memory.

Same-host ranks can map the same immutable tmpfs files to share physical pages
without RDMA. Each rank still owns its staging output. Merely placing data in
another process on the same host does not make that process's pointers locally
addressable. Use separate directories for separate model instances.

C++:

- constructor `EngramStore(const std::map<int, EngramStoreConfig>& layers, std::shared_ptr<PyClient>)`
- `populate(...)`
- `lookup_into(int layer_id, const int64_t* row_ids, int B, int L, void* output, size_t output_size)`
- `remove_from_store(...)`
- metadata getters matching the Python surface

## Data Contract

Populate expects one C-contiguous NumPy `uint8` array per head; it does not
cast numeric arrays to bytes:

```text
embedding_buffers[h].shape == [N_h, row_bytes]
```

`lookup_into` requires a C-contiguous NumPy `int64` row-ID array with shape
`[B, L, H]` and caller-owned, writable C-contiguous `uint8` output:

```text
output.shape == [B, L, H, row_bytes]
```

It writes output in place and returns `None`. Neither argument is implicitly
converted. The explicit `layer_id` selects a configured layer; position
`h` in the last row-ID dimension selects `engram:l{layer_id}:h{h}`.

For Store-backed reads, the caller must register the entire output buffer with the same Store client
before the first nonempty lookup, keep it registered throughout each call, and
unregister it when finished. `lookup_into` does not allocate, register, or
unregister output; there is no registration flag or automatic-registration mode.
An empty batch is a no-op and does not require registration.

```python
output = np.empty((*row_ids.shape, table.get_row_bytes(layer_id)), dtype=np.uint8)
assert store.register_buffer(output.ctypes.data, output.nbytes) == 0
try:
    table.lookup_into(layer_id, row_ids, output)  # Repeat using this buffer as needed.
finally:
    assert store.unregister_buffer(output.ctypes.data) == 0
```

Store registration does not allocate CUDA pinned memory. SGLang allocates
persistent pinned host buffers separately, fills them outside CUDA Graph, and
captures the per-layer H2D copy and dequantization inside the graph. The caller
must wait for both Store writes and any GPU reads before reusing or releasing
these buffers.

To store existing floating-point arrays, explicitly expose their bytes, e.g.
`table.view(np.uint8).reshape(num_rows, row_bytes)`. The caller is responsible for
the dtype and layout when interpreting lookup results.

## Populate Flow

Populate follows the existing Store write path:

1. validate that exactly one table is provided for each head
2. validate that every table matches `[N_h, row_bytes]`
3. verify that the target head-table keys do not already exist
4. register each embedding table buffer
5. upload all head tables with `batch_put_from(...)`
6. unregister the staging buffers

`populate(...)` is defined as a create-only operation for the selected layer. To
reuse a `layer_id`, first remove that layer's old tables with `remove_from_store(layer_id, ...)`. Other layers are unaffected.

If upload fails after some head tables have already been written, or if publish
finishes but post-write buffer cleanup fails, the backend best-effort removes
the keys written by the failed populate attempt before returning an error.

## Lookup Flow

Each Store-backed lookup follows this flow (local mode validates IDs and copies
rows directly):

1. validate the `row_ids` shape and bounds
2. build per-head byte ranges for the requested rows
3. query head-table locations with `batch_query(...)`
4. issue one `get_into_ranges(...)` call to write the rows into the registered output buffer

The binding builds ranges directly from contiguous NumPy row IDs.
It rejects Python lists and implicit dtype or layout conversion.

## Validation

The backend enforces these invariants:

- the layer map is nonempty, IDs are nonnegative, and operations reject unknown layers

- `table_vocab_sizes` is non-empty and every entry is positive
- `row_bytes` is positive and table/output byte sizes do not overflow
- `populate(...)` receives exactly one table per head
- every populated table matches `[N_h, row_bytes]`
- `lookup_into(...)` receives matching `[B, L, H]` IDs and registered output
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

## Client Instances and Storage

SGLang shares one EngramStore and one Store client across all Engram layers in
each rank. Each layer still owns its fixed pinned output buffers for CUDA Graph.
Different ranks access the same backend keys; creating additional clients or
EngramStore handles does not replicate table data. Storage replicas are controlled
by the Store replication configuration used during upload.

Per-rank reads currently duplicate network traffic and fetched rows in staging
buffers. Reducing that traffic requires a separate optimization such as one reader
per TP group followed by broadcast, or coordinated shared host buffers. Sharing a
multi-layer EngramStore instance alone does not deduplicate reads across processes.
