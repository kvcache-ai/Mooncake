# Engram Python API

Mooncake exposes a Python API for the Engram lookup backend.

This API manages per-head embedding tables in Mooncake Store and serves lookup
requests using caller-provided row IDs.

## Quick Start

```python
import numpy as np
import store

cfg = store.EngramConfig()
cfg.table_vocab_sizes = [1000, 1200, 1500, 1800]
cfg.embedding_dim = 64

engram = store.Engram(layer_id=1, config=cfg, store=store_obj)

embedding_tables = [
    np.random.randn(N_h, cfg.embedding_dim).astype(np.float32)
    for N_h in cfg.table_vocab_sizes
]
engram.populate(embedding_tables)

row_ids = [
    [[0, 1, 2, 3], [4, 5, 6, 7]],
    [[8, 9, 10, 11], [12, 13, 14, 15]],
]
output = engram.lookup(row_ids)

assert output.shape == (
    2,
    2,
    engram.get_num_heads(),
    engram.get_embedding_dim(),
)

removed = engram.remove_from_store(force=False)
```

## EngramConfig

`EngramConfig` describes the physical layout of one Engram layer backend.

Fields:

- `table_vocab_sizes: List[int]`
  - per-head table sizes `[N_0, N_1, ..., N_{H-1}]`
- `embedding_dim: int`
  - embedding row width `D`

Example:

```python
cfg = store.EngramConfig()
cfg.table_vocab_sizes = [2048, 2048, 4096, 4096]
cfg.embedding_dim = 64
```

## Engram

Constructor:

```python
Engram(layer_id: int, config: EngramConfig, store=None)
```

Arguments:

- `layer_id`
  - logical layer identifier used in key naming
- `config`
  - physical table layout for this backend instance
- `store`
  - `PyClient` or `MooncakeDistributedStore`

Generated store keys follow:

```text
engram:l{layer_id}:h{head_idx}
```

## Metadata Helpers

### get_table_vocab_sizes()

Returns the configured per-head table sizes.

```python
sizes = engram.get_table_vocab_sizes()
```

### get_num_heads()

Returns the number of heads.

```python
num_heads = engram.get_num_heads()
```

### get_embedding_dim()

Returns the row width `D`.

```python
dim = engram.get_embedding_dim()
```

### get_store_keys()

Returns all store keys owned by this layer instance.

```python
keys = engram.get_store_keys()
```

## populate()

```python
engram.populate(embedding_buffers)
```

Parameters:

- `embedding_buffers`
  - list of NumPy `float32` arrays
  - one array per head
  - head `h` must have shape `[N_h, D]`

Behavior:

- validates buffer count and shapes
- registers source buffers with Mooncake
- uploads tables with `batch_put_from(...)`
- refreshes cached metadata after a successful upload

## lookup()

```python
output = engram.lookup(row_ids)
```

Parameters:

- `row_ids`
  - shape `[B, L, H]`
  - `H` must match `get_num_heads()`
  - for each head `h`, every row ID must be in `[0, N_h)`

Returns:

- NumPy `float32` array with shape `[B, L, H, D]`

Behavior:

- validates lookup shape and row-id bounds
- reads local replicas directly when available
- otherwise falls back to full-buffer fetch through Mooncake Store

## remove_from_store()

```python
removed = engram.remove_from_store(force=False)
```

Parameters:

- `force`
  - forwarded to the underlying store removal path

Returns:

- number of head tables removed

Behavior:

- removes keys owned by the current layer instance
- ignores already-missing keys
- refreshes cached metadata after cleanup

## Validation Rules

The Python API enforces these rules:

- `table_vocab_sizes` must be non-empty and positive
- `embedding_dim` must be positive
- `populate(...)` must receive one table per head
- each table must match `[N_h, D]`
- `lookup(...)` must receive a non-empty `[B, L, H]` input
- each row ID must be within the corresponding table range

## Related Documents

- [Engram: Mooncake Lookup Backend](../design/engram.md)
- [DeepSeek Engram: Paper and Demo Notes](../design/deepseek-engram-paper-demo-notes.md)
