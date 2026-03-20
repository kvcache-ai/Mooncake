# Engram Python API

## Overview

The current Python API exposes Mooncake's **query-only** Engram integration.

It is designed for the data plane only:

- hash token IDs into deterministic per-head row indices
- populate Mooncake Store with per-head embedding tables
- query embedding rows from Mooncake Store

It does **not** execute Engram's model-side forward path. In particular, Mooncake no longer provides:

- context-aware gating
- value projection
- short convolution
- residual fusion

Use Mooncake to retrieve Engram embeddings, then do any model-side fusion in your runtime/framework.

## Quick Start

```python
import numpy as np
from mooncake.store import (
    BackboneConfig,
    Engram,
    EngramConfig,
    MooncakeDistributedStore,
)
from mooncake.mooncake_config import MooncakeConfig

# 1. Setup Mooncake Store
store = MooncakeDistributedStore()
mc_cfg = MooncakeConfig.load_from_env()
store.setup(
    mc_cfg.local_hostname,
    mc_cfg.metadata_server,
    mc_cfg.global_segment_size,
    mc_cfg.local_buffer_size,
    mc_cfg.protocol,
    mc_cfg.device_name,
    mc_cfg.master_server_address,
)

# 2. Configure Engram
cfg = EngramConfig()
cfg.tokenizer_name_or_path = ""
cfg.engram_vocab_size = [1000, 1000]
cfg.max_ngram_size = 3
cfg.n_embed_per_ngram = 64
cfg.n_head_per_ngram = 4
cfg.layer_ids = [1, 15]
cfg.pad_id = 2
cfg.seed = 0
cfg.kernel_size = 4  # retained for API compatibility; unused by query path

bb = BackboneConfig()
bb.hidden_size = 256
bb.hc_mult = 4
bb.vocab_size = 128000
bb.num_layers = 6

# 3. Create Engram for one layer
engram = Engram(layer_id=1, config=cfg, backbone_cfg=bb, store=store)

# 4. Inspect per-head table sizes
num_heads = engram.get_num_heads()
embed_dim = engram.get_embedding_dim()
table_vocab_sizes = engram.get_table_vocab_sizes()
assert len(table_vocab_sizes) == num_heads

# IMPORTANT: allocate each head table from get_table_vocab_sizes(),
# not directly from cfg.engram_vocab_size.
embedding_buffers = []
for N_h in table_vocab_sizes:
    table = np.random.randn(N_h, embed_dim).astype(np.float32)
    embedding_buffers.append(table)

# 5. Upload tables to Mooncake Store
engram.populate_store_from_buffers(embedding_buffers)

# 6. Hash and query
input_ids = [[1, 2, 3, 4], [4, 3, 2, 1]]
hash_ids = engram.hash_input_ids(input_ids)
print(hash_ids.shape)  # (2, 4, num_heads)

# query() works without a caller-managed workspace.
# Engram may still reuse an internal registered scratch workspace.
output = engram.query(input_ids)
print(output.shape)  # (2, 4, num_heads, embed_dim)

# Optional: provide a caller-managed scratch workspace.
ws_bytes = engram.get_query_workspace_size(len(input_ids), len(input_ids[0]))
workspace = np.empty(ws_bytes, dtype=np.uint8)
output = engram.query(input_ids, workspace)

# Optional: remove this layer's Engram tables from Mooncake Store.
removed = engram.remove_from_store()
print("removed_heads =", removed)

store.close()
```

## Important Notes

- `forward()` has been removed from Mooncake's Engram integration.
- `hash_input_ids()` returns an `int64` NumPy array shaped `[B, L, H]`.
- `query()` returns a NumPy array shaped `[B, L, H, D]`.
- `H = (max_ngram_size - 1) * n_head_per_ngram`.
- `D = n_embed_per_ngram // n_head_per_ngram`.
- Each head has its own real table size `N_h`; use `get_table_vocab_sizes()` when preparing tables.
- `get_query_workspace_size(B, L)` is batch-dependent and is often much smaller than `get_embedding_tables_workspace_size()`.
- `remove_from_store(force=False)` is the dedicated cleanup API for deleting the current layer's Engram tables.

## API Reference

### EngramConfig

Configuration for Engram hashing and table layout.

**Attributes:**

- `tokenizer_name_or_path` (`str`): Tokenizer path/name kept for API compatibility
  - In the current store-backed C++ path, hashing uses the provided `input_ids` directly.
- `engram_vocab_size` (`List[int]`): Base table sizes for each N-gram order
  - Length must be `max_ngram_size - 1`
  - Example: `[1000, 1000]` for 2-gram and 3-gram
- `max_ngram_size` (`int`): Maximum N-gram size
  - Example: `3` means 2-gram and 3-gram heads are created
- `n_embed_per_ngram` (`int`): Total embedding width per N-gram group
  - Per-head width is `n_embed_per_ngram // n_head_per_ngram`
- `n_head_per_ngram` (`int`): Number of heads for each N-gram order
- `layer_ids` (`List[int]`): Transformer layer IDs that own Engram tables
- `pad_id` (`int`): Padding token ID used when shifting the sequence for N-gram hashing
- `seed` (`int`): Random seed used to generate the per-layer hash multipliers
- `kernel_size` (`int`): Retained for compatibility with older/full Engram configs
  - It is not used by the current Mooncake query-only path

**Note:** the per-head table sizes actually used by Mooncake are prime-sized expansions derived from `engram_vocab_size`. Use `Engram.get_table_vocab_sizes()` to see the real values.

### BackboneConfig

Backbone model configuration kept for constructor compatibility.

**Attributes:**

- `hidden_size` (`int`)
- `hc_mult` (`int`)
- `vocab_size` (`int`)
- `num_layers` (`int`)

These fields are currently preserved for API consistency, but the Mooncake query-only Engram path does not use them for store population or embedding lookup.

### Engram

Main Engram storage/query helper.

#### Constructor

```python
Engram(
    layer_id: int,
    config: EngramConfig,
    backbone_cfg: BackboneConfig,
    store: MooncakeDistributedStore | None = None,
)
```

**Parameters:**

- `layer_id`: Layer ID whose per-head tables will be addressed by this instance
- `config`: Engram configuration
- `backbone_cfg`: Backbone configuration kept for API compatibility
- `store`: Mooncake Store instance
  - Required for `populate_store_from_buffers()` and `query()`
  - Optional when you only need `hash_input_ids()` and metadata helpers such as `get_table_vocab_sizes()`
  - Lifecycle note: destroy the `Engram` instance before calling `store.close()`
    so its background services and registered buffers can be released cleanly;
    the destructor is best-effort defensive, but the recommended order is still
    `del engram` first, then `store.close()`

#### get_num_heads()

```python
num_heads = engram.get_num_heads()
```

Returns the total head count:

```text
(max_ngram_size - 1) * n_head_per_ngram
```

#### get_embedding_dim()

```python
embed_dim = engram.get_embedding_dim()
```

Returns the per-head embedding width:

```text
n_embed_per_ngram // n_head_per_ngram
```

#### get_table_vocab_sizes()

```python
table_vocab_sizes = engram.get_table_vocab_sizes()
```

Returns the real per-head table sizes `N_h`.

**Use this method when allocating embedding tables.** Do not assume that each head's table size is equal to the raw `cfg.engram_vocab_size[...]` value.

#### get_store_keys()

```python
keys = engram.get_store_keys()
```

Returns the Mooncake Store keys for the current layer, for example:

```text
engram:l1:h0
engram:l1:h1
...
```

#### get_embedding_tables_workspace_size()

```python
bytes_needed = engram.get_embedding_tables_workspace_size()
```

Returns the workspace size in bytes needed for temporary embedding-table buffers in the zero-copy/range-read path.

#### get_query_workspace_size()

```python
bytes_needed = engram.get_query_workspace_size(B, L)
```

Returns the workspace size in bytes for `query()`.

This is the batch-dependent scratch size for the query path. It is based on the number of rows that can be touched by the current `B x L` request, so it is often much smaller than `get_embedding_tables_workspace_size()`.

#### hash_input_ids()

```python
hash_ids = engram.hash_input_ids(input_ids)
```

**Parameters:**

- `input_ids`: token IDs shaped `[B, L]`, typically a Python list of lists

**Returns:**

- `np.ndarray` shaped `[B, L, H]`, dtype `int64`

**Example:**

```python
input_ids = [[10, 11, 12, 13]]
hash_ids = engram.hash_input_ids(input_ids)
assert hash_ids.shape == (1, 4, engram.get_num_heads())
```

#### query()

```python
output = engram.query(input_ids, workspace=None)
```

**Parameters:**

- `input_ids`: token IDs shaped `[B, L]`
- `workspace`: optional NumPy array used as scratch space
  - Allocate with `np.empty(engram.get_query_workspace_size(B, L), dtype=np.uint8)`

**Returns:**

- `output`: `np.ndarray` shaped `[B, L, H, D]`, dtype `float32`

**Behavior:**

- computes `hash_input_ids(input_ids)` internally
- queries the corresponding rows from Mooncake Store
- tries the same-node local-direct path first when the tables are mounted locally
- otherwise prefers a range-read path backed by reusable scratch buffers
- on RDMA cross-node reads, may use the remote-gather control plane to have the
  producer gather rows and write them back into the caller scratch space
- may reuse an internal registered workspace even when `workspace=None`
- falls back to `batch_get_buffer()` when the access pattern is too fragmented or a bulk fetch is cheaper

**Cross-node control-plane knobs:**

- `MC_ENGRAM_REMOTE_GATHER`: enable or disable the RDMA remote-gather path
- `MC_ENGRAM_SG_RING_SLOT_BYTES`: base descriptor-slot size for remote-gather requests
- `MC_ENGRAM_SG_RING_SLOTS`: number of request-ring slots kept per Engram instance
- `MC_ENGRAM_SG_RING_MAX_SLOT_BYTES`: upper bound when a single request needs a larger slot
- `MC_ENGRAM_REMOTE_GATHER_HOT_POLLS`: producer-side hot-poll budget before sleeping

If remote gather is disabled or cannot be provisioned for the current request,
`query()` transparently falls back to the existing range-read/bulk-fetch path.

#### populate_store_from_buffers()

```python
engram.populate_store_from_buffers(embedding_buffers)
```

Upload per-head embedding tables into Mooncake Store.

**Parameters:**

- `embedding_buffers`: list of NumPy arrays, one per head
  - each array must be `float32`
  - each array must be 2D
  - each array must have shape `[N_h, D]`, where:
    - `N_h` comes from `get_table_vocab_sizes()[h]`
    - `D` comes from `get_embedding_dim()`

**Example:**

```python
tables = [
    np.random.randn(N_h, engram.get_embedding_dim()).astype(np.float32)
    for N_h in engram.get_table_vocab_sizes()
]
engram.populate_store_from_buffers(tables)
```

#### remove_from_store()

```python
removed = engram.remove_from_store(force=False)
```

Remove all Mooncake Store tables owned by this Engram layer.

**Parameters:**

- `force`: whether to skip lease checks during removal
  - default is `False`
  - in production, prefer `False` unless traffic has already been drained and you explicitly need forced cleanup
  - `force=True` does not mean "ignore all safety checks"; concurrent write/replication safety checks still apply at the store layer

**Returns:**

- number of removed head tables
- missing keys are ignored, so calling the method repeatedly is idempotent from the caller's perspective

**Notes:**

- this removes only the current Engram layer instance's keys, not the whole store
- the implementation invalidates Engram's cached query metadata after cleanup
- with `force=False`, active leases can still block deletion; this is expected in online traffic
- after cleanup, create/populate again before serving queries from the same layer
- `remove_all()` remains a store-level test/debug API and is not the recommended production cleanup path for Engram

## Storage Format

Embedding tables are stored in Mooncake Store under keys:

```text
engram:l{layer_id}:h{head_idx}
```

Each key stores one contiguous `float32` table shaped `[N_h, D]`.

This means the head dimension is represented in the key space rather than packed into one giant tensor. The queried result is still materialized as `[B, L, H, D]`.

## Removed API

`forward()` is no longer part of Mooncake's Engram API.

If you still need model-side fusion, use `query()` to fetch `[B, L, H, D]` embeddings and implement the downstream fusion in your model runtime.

## See Also

- [Engram Design Documentation](../design/engram.md)
- [Mooncake Store API](./mooncake-store.md)
