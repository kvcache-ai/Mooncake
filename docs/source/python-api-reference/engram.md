# Engram Python API

## Overview

Engram provides a conditional memory module for LLMs via N-gram embedding lookup. This API exposes the C++ Engram implementation through Python bindings.

**Note:** Engram requires Mooncake Store for embedding storage and lookup (no local-only mode).

## Quick Start

```python
import numpy as np
from mooncake.store import Engram, EngramConfig, BackboneConfig, MooncakeDistributedStore
from mooncake.mooncake_config import MooncakeConfig

# 1. Setup Mooncake Store
store = MooncakeDistributedStore()
config = MooncakeConfig.load_from_env()
store.setup(
    config.local_hostname,
    config.metadata_server,
    config.global_segment_size,
    config.local_buffer_size,
    config.protocol,
    config.device_name,
    config.master_server_address,
)

# 2. Configure Engram
cfg = EngramConfig()
cfg.engram_vocab_size = [1000, 1000]
cfg.max_ngram_size = 3
cfg.n_embed_per_ngram = 64
cfg.n_head_per_ngram = 4
cfg.layer_ids = [1, 15]
cfg.pad_id = 2
cfg.seed = 0
cfg.kernel_size = 4

bb = BackboneConfig()
bb.hidden_size = 256
bb.hc_mult = 4
bb.vocab_size = 128000
bb.num_layers = 6

# 3. Create Engram
engram = Engram(layer_id=1, config=cfg, backbone_cfg=bb, store=store)

# 4. Populate Store with embeddings
num_heads = (cfg.max_ngram_size - 1) * cfg.n_head_per_ngram  # 8
embed_D = cfg.n_embed_per_ngram // cfg.n_head_per_ngram  # 16

embedding_buffers = []
buffer_sizes = []
for h in range(num_heads):
    vocab_size = cfg.engram_vocab_size[h // cfg.n_head_per_ngram]
    emb_table = np.random.randn(vocab_size, embed_D).astype(np.float32)
    embedding_buffers.append(emb_table)
    buffer_sizes.append(emb_table.nbytes)

engram.populate_store_from_buffers(embedding_buffers, buffer_sizes)

# 5. Forward pass
B, L = 2, 8
hidden_states = np.random.randn(B, L, bb.hc_mult, bb.hidden_size).astype(np.float32)
input_ids = [[1, 2, 3, 4, 5, 6, 7, 8], [9, 10, 11, 12, 13, 14, 15, 16]]

output = engram.forward(hidden_states, input_ids)
print(f"Output shape: {output.shape}")  # (2, 8, 4, 256)

# 6. Cleanup
store.close()
```

## API Reference

### EngramConfig

Configuration for the Engram module.

**Attributes:**

- `tokenizer_name_or_path` (str): Tokenizer name or path (optional)
- `engram_vocab_size` (List[int]): Vocabulary sizes for each N-gram type. Length should be `max_ngram_size - 1`
  - Example: `[1000, 1000]` for 2-gram and 3-gram
- `max_ngram_size` (int): Maximum N-gram size. Engram supports N-grams from 2 to `max_ngram_size`
  - Example: `3` supports 2-gram and 3-gram
- `n_embed_per_ngram` (int): Total embedding dimension per N-gram
  - Example: `64` means each head gets `64 // n_head_per_ngram` dimensions
- `n_head_per_ngram` (int): Number of parallel heads per N-gram type
  - Example: `4` means 4 heads per N-gram type
- `layer_ids` (List[int]): Transformer layer IDs where Engram modules are inserted
  - Example: `[1, 15]` means Engram is used at layers 1 and 15
- `pad_id` (int): Padding token ID
  - Example: `2` or `0`
- `seed` (int): Random seed for multiplier generation
  - Example: `0` for reproducibility
- `kernel_size` (int): ShortConv kernel size
  - Example: `4`

**Example:**

```python
cfg = EngramConfig()
cfg.engram_vocab_size = [1000, 1000]
cfg.max_ngram_size = 3
cfg.n_embed_per_ngram = 64
cfg.n_head_per_ngram = 4
cfg.layer_ids = [1, 15]
cfg.pad_id = 2
cfg.seed = 0
cfg.kernel_size = 4
```

### BackboneConfig

Configuration for the backbone Transformer model.

**Attributes:**

- `hidden_size` (int): Hidden dimension of the transformer
  - Example: `256`, `512`, `1024`
- `hc_mult` (int): Head-channel multiplier (number of parallel channels)
  - Example: `4` means 4 independent hc groups
- `vocab_size` (int): Token vocabulary size (for input_ids range)
  - Example: `128000`
- `num_layers` (int): Total number of transformer layers
  - Example: `6`, `30`

**Example:**

```python
bb = BackboneConfig()
bb.hidden_size = 256
bb.hc_mult = 4
bb.vocab_size = 128000
bb.num_layers = 6
```

### Engram

Main Engram class.

#### Constructor

```python
Engram(
    layer_id: int,
    config: EngramConfig,
    backbone_cfg: BackboneConfig,
    store: MooncakeDistributedStore
)
```

**Parameters:**

- `layer_id` (int): The transformer layer ID where this Engram module is placed
- `config` (EngramConfig): Engram configuration
- `backbone_cfg` (BackboneConfig): Backbone model configuration
- `store` (MooncakeDistributedStore): Mooncake Store instance (required)

**Example:**

```python
engram = Engram(layer_id=1, config=cfg, backbone_cfg=bb, store=store)
```

#### forward()

Execute the forward pass.

```python
output = engram.forward(
    hidden_states: np.ndarray,  # shape=[B, L, hc_mult, D], dtype=float32
    input_ids: List[List[int]]  # shape=[B, L]
) -> np.ndarray  # shape=[B, L, hc_mult, D]
```

**Parameters:**

- `hidden_states`: Input hidden states from the transformer
  - Shape: `[B, L, hc_mult, D]` where:
    - `B`: Batch size
    - `L`: Sequence length
    - `hc_mult`: Head-channel multiplier (from `BackboneConfig`)
    - `D`: Hidden dimension (from `BackboneConfig`)
  - Dtype: `float32`
- `input_ids`: Token IDs for the input sequence
  - Outer list: Batch dimension (length `B`)
  - Inner list: Sequence dimension (length `L`)

**Returns:**

- `output`: Output hidden states with same shape as `hidden_states` `[B, L, hc_mult, D]`

**Example:**

```python
B, L = 2, 8
hidden_states = np.random.randn(B, L, bb.hc_mult, bb.hidden_size).astype(np.float32)
input_ids = [[1, 2, 3, 4, 5, 6, 7, 8], [9, 10, 11, 12, 13, 14, 15, 16]]

output = engram.forward(hidden_states, input_ids)
assert output.shape == (B, L, bb.hc_mult, bb.hidden_size)
```

#### populate_store_from_buffers()

Populate Mooncake Store with embedding tables.

```python
engram.populate_store_from_buffers(
    embedding_buffers: List[np.ndarray],  # Each shape=[vocab_size, embed_D]
    buffer_sizes: List[int]  # Sizes in bytes
)
```

**Parameters:**

- `embedding_buffers`: List of numpy arrays, each representing an embedding table for one head
  - Shape: `[vocab_size, embed_D]` where `embed_D = n_embed_per_ngram // n_head_per_ngram`
  - Number of buffers: `(max_ngram_size - 1) * n_head_per_ngram`
  - Dtype: `float32`
- `buffer_sizes`: List of buffer sizes in bytes (typically `embedding_table.nbytes`)

**Example:**

```python
num_heads = (cfg.max_ngram_size - 1) * cfg.n_head_per_ngram  # 8
embed_D = cfg.n_embed_per_ngram // cfg.n_head_per_ngram  # 16

embedding_buffers = []
buffer_sizes = []
for h in range(num_heads):
    vocab_size = cfg.engram_vocab_size[h // cfg.n_head_per_ngram]
    emb_table = np.random.randn(vocab_size, embed_D).astype(np.float32)
    embedding_buffers.append(emb_table)
    buffer_sizes.append(emb_table.nbytes)

engram.populate_store_from_buffers(embedding_buffers, buffer_sizes)
```

## Storage Format

Embedding tables are stored in Mooncake Store with keys:
```
"engram:l{layer_id}:h{head_idx}"
```

- Each table is a contiguous `[vocab_size, embed_D]` tensor
- Data type: `float32`
- Tables are fetched/uploaded using batch operations for efficiency

## See Also

- [Engram Design Documentation](../design/engram.md) - Architecture and implementation details
- [Mooncake Store API](./mooncake-store.md) - Store setup and configuration
