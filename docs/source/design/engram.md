# Engram: Conditional Memory Module

## Overview

Engram is a **conditional memory module** that implements N-gram embedding lookup for large language models. It provides O(1) knowledge retrieval through scalable lookup tables, complementing Mixture-of-Experts (MoE) architectures by introducing a new axis of sparsity.

**Key Features:**
- ✅ **O(1) Lookup**: Direct hash-based access to N-gram embeddings
- ✅ **Deterministic Addressing**: Supports prefetching and offloading
- ✅ **Distributed Storage**: Integration with Mooncake Store for scalable embedding storage
- ✅ **Zero-Copy Operations**: Efficient memory access using registered buffers
- ✅ **Context-Aware Gating**: Dynamic fusion of embeddings with hidden states

**Paper Reference:** [Conditional Memory via Scalable Lookup: A New Axis of Sparsity for Large Language Models](https://arxiv.org/abs/2601.07372) (arXiv:2601.07372)

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Engram Module                            │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  Input: hidden_states [B, L, hc_mult, D]                   │
│         input_ids [B, L]                                    │
│                                                             │
│  ┌──────────────────────────────────────────────────────┐  │
│  │ 1. N-gram Hash Mapping                               │  │
│  │    - Shift sequences: shift_k(input_ids)             │  │
│  │    - Combine N-grams: XOR(multipliers * tokens)     │  │
│  │    - Hash: hash_id = mix % vocab_size                │  │
│  │    Output: hash_ids [B, L, num_heads]                │  │
│  └──────────────────────────────────────────────────────┘  │
│                          ↓                                  │
│  ┌──────────────────────────────────────────────────────┐  │
│  │ 2. Embedding Lookup                                  │  │
│  │    - Batch fetch: batch_get_buffer(embed_keys)      │  │
│  │    - Direct lookup: embedding_table[hash_ids]       │  │
│  │    Output: embeddings [B, L, num_heads, embed_D]    │  │
│  └──────────────────────────────────────────────────────┘  │
│                          ↓                                  │
│  ┌──────────────────────────────────────────────────────┐  │
│  │ 3. Context-Aware Gating                              │  │
│  │    - Normalize: RMSNorm(embeddings, hidden_states) │  │
│  │    - Compute keys: Linear(embeddings)                │  │
│  │    - Compute gates: sigmoid(sqrt(dot(key, query))) │  │
│  │    - Fuse: gate * Linear(embeddings)                 │  │
│  │    Output: gated_output [B, L, hc_mult, D]          │  │
│  └──────────────────────────────────────────────────────┘  │
│                          ↓                                  │
│  ┌──────────────────────────────────────────────────────┐  │
│  │ 4. ShortConv                                         │  │
│  │    - Normalize: RMSNorm(gated_output)                │  │
│  │    - Convolve: DepthwiseConv1d(kernel, dilation)     │  │
│  │    - Activate: SiLU(conv_output)                      │  │
│  │    Output: conv_output [B, L, hc_mult, D]            │  │
│  └──────────────────────────────────────────────────────┘  │
│                          ↓                                  │
│  ┌──────────────────────────────────────────────────────┐  │
│  │ 5. Residual Connection                               │  │
│  │    output = gated_output + conv_output + hidden_states│ │
│  │    Output: output [B, L, hc_mult, D]                  │  │
│  └──────────────────────────────────────────────────────┘  │
│                                                             │
└─────────────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────────────┐
│              Mooncake Store (Distributed)                    │
│  - Embedding tables stored as keys:                         │
│    "engram:l{layer_id}:h{head_idx}"                         │
│  - Batch operations: batch_get_buffer, batch_put_from       │
│  - Zero-copy: Registered buffers, GPUDirect RDMA            │
└─────────────────────────────────────────────────────────────┘
```

## Core Components

### 1. N-gram Hash Mapping

**Purpose:** Convert token sequences into hash indices for O(1) embedding lookup.

**Process:**

1. **Sequence Shifting**: Create shifted versions of the input sequence
   ```
   Original: [t0, t1, t2, t3, t4]
   Shift k=0: [t0, t1, t2, t3, t4]
   Shift k=1: [pad, t0, t1, t2, t3]
   Shift k=2: [pad, pad, t0, t1, t2]
   ```

2. **N-gram Combination**: For each N-gram size (n=2, 3, ..., max_ngram_size), combine n shifted sequences
   ```
   For 3-gram at position t:
     token[0] = shift_0[t]
     token[1] = shift_1[t]
     token[2] = shift_2[t]
   ```

3. **Hash Computation**: 
   ```cpp
   mix = token[0] * multiplier[0]
   for k in 1..n-1:
       mix = mix XOR (token[k] * multiplier[k])
   hash_id = (mix % vocab_size + vocab_size) % vocab_size
   ```

**Key Design Choices:**
- **Multipliers**: Random odd integers for each position, generated per layer
- **XOR Mixing**: Ensures different token combinations produce different hashes
- **Prime Modulo**: Each head uses a different prime as vocab_size to reduce collisions
- **Multi-Head**: Multiple heads per N-gram capture different aspects

### 2. Multi-Head Embedding Lookup

**Purpose:** Store and retrieve N-gram embeddings efficiently.

**Storage Structure:**
- Each head has an independent embedding table: `[vocab_size, embed_D]`
- Tables are stored in Mooncake Store with keys: `"engram:l{layer_id}:h{head_idx}"`
- Total heads: `(max_ngram_size - 1) * n_head_per_ngram`

**Lookup Process:**
```cpp
// 1. Batch fetch all embedding tables
buffers = store->batch_get_buffer(embed_keys);

// 2. For each position and head, direct lookup
for (b, l, h):
    hash_id = hash_ids[b][l][h]
    embedding = buffers[h][hash_id * embed_D : (hash_id+1) * embed_D]
```

**Performance Optimizations:**
- **Batch Operations**: Fetch all tables in one `batch_get_buffer` call
- **Zero-Copy**: Use registered buffers to avoid CPU-GPU copies
- **Direct Memory Access**: O(1) lookup via array indexing

### 3. Context-Aware Gating

**Purpose:** Dynamically determine how much embedding information to fuse with hidden states.

**Computation Steps:**

1. **Normalization**:
   ```cpp
   normed_embeddings = RMSNorm(embeddings)
   normed_hidden = RMSNorm(hidden_states)  // Per hc group
   ```

2. **Key and Query**:
   ```cpp
   // For each hc group
   key[hc] = RMSNorm(Linear(normed_embeddings, key_proj_weights[hc]))
   query[hc] = normed_hidden[hc]
   ```

3. **Gate Computation**:
   ```cpp
   similarity = dot(key[hc], query[hc]) / sqrt(D)
   g = sqrt(abs(similarity)) * sign(similarity)
   gate[hc] = sigmoid(g)
   ```

4. **Value Fusion**:
   ```cpp
   value = Linear(normed_embeddings, value_proj_weights)
   output[hc] = gate[hc] * value
   ```

**Gating Function Properties:**
- Gate ∈ [0, 1]: Weight for embedding information
- Gate ≈ 1: Heavy use of embeddings (high relevance)
- Gate ≈ 0: Minimal use of embeddings (low relevance)
- Context-dependent: Gate value depends on similarity between embeddings and hidden states

### 4. ShortConv (Short Convolution)

**Purpose:** Local feature extraction and smoothing in the sequence dimension.

**Operation:**
```cpp
// 1D Depthwise Convolution
for each position l:
    sum = 0
    for k in 0..kernel_size-1:
        pos = l - pad + k * dilation
        if pos >= 0 and pos < L:
            sum += input[pos]
    output[l] = sum

// Activation
output = SiLU(output)
```

**Parameters:**
- **kernel_size**: Convolution kernel size (e.g., 4)
- **dilation**: Dilation rate, equals `max_ngram_size` (e.g., 3)
- **padding**: `(kernel_size - 1) * dilation`

**Effects:**
- Captures local dependencies (interactions between adjacent positions)
- Smooths output (reduces noise)
- Enhances expressiveness

### 5. Residual Connection

**Purpose:** Preserve gradient flow and original information.

```cpp
output = gated_output + shortconv_output + hidden_states
```

**Benefits:**
- Prevents gradient vanishing
- Preserves original information even when gates are small
- Stabilizes training

## Mooncake Store Integration

### Storage Format

Embedding tables are stored in Mooncake Store with the following key format:
```
"engram:l{layer_id}:h{head_idx}"
```

**Example:**
- Layer 1, Head 0: `"engram:l1:h0"`
- Layer 1, Head 1: `"engram:l1:h1"`
- Layer 15, Head 0: `"engram:l15:h0"`

**Storage Layout:**
- Each embedding table is stored as a contiguous `[vocab_size, embed_D]` tensor
- Data type: `float32`
- Total size: `vocab_size * embed_D * sizeof(float)` bytes

### Zero-Copy Operations

Engram uses Mooncake Store's zero-copy capabilities:

1. **Buffer Registration**: Embedding tables are registered with `register_buffer()` before upload
2. **Batch Operations**: All tables are fetched/uploaded in a single `batch_get_buffer`/`batch_put_from` call
3. **Direct Memory Access**: Lookups use direct memory access without copying

**Benefits:**
- Reduced CPU-GPU memory copies
- Lower latency for embedding lookups
- Better bandwidth utilization

### Distributed Storage

With Mooncake Store, embedding tables can be:
- **Distributed**: Stored across multiple nodes
- **Offloaded**: Automatically moved to SSD when memory is full
- **Replicated**: Multiple copies for fault tolerance and load balancing
- **Scalable**: Not limited by single-node memory capacity

## Performance Considerations

### Memory Layout

Engram processes 4D tensors with layout `[B, L, hc_mult, D]`:

```
Memory layout (row-major):
[b=0, l=0, hc=0, d=0..D-1] [b=0, l=0, hc=1, d=0..D-1] ... [b=0, l=0, hc=hc_mult-1, d=0..D-1]
[b=0, l=1, hc=0, d=0..D-1] [b=0, l=1, hc=1, d=0..D-1] ... [b=0, l=1, hc=hc_mult-1, d=0..D-1]
...
[b=B-1, l=L-1, hc=hc_mult-1, d=0..D-1]
```

**Access Pattern:**
```cpp
index = (b * L * hc_mult + l * hc_mult + hc) * D + d;
value = data[index];
```

### Optimization Tips

1. **Batch Size**: Larger batch sizes improve throughput (amortize Store overhead)
2. **Sequence Length**: Longer sequences benefit more from Engram (more N-grams)
3. **Embedding Cache**: Frequently accessed tables can be cached in GPU memory
4. **Prefetching**: Hash IDs can be computed ahead of time for prefetching
5. **CUDA Kernels**: Linear, RMSNorm, and Conv1d operations can be accelerated with CUDA kernels

## References

- **Paper**: [Conditional Memory via Scalable Lookup: A New Axis of Sparsity for Large Language Models](https://arxiv.org/abs/2601.07372)
- **GitHub**: [DeepSeek Engram](https://github.com/deepseek-ai/Engram)
- **Mooncake Store**: [Design Documentation](./mooncake-store.md)
