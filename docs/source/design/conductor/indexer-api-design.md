# Mooncake Conductor Indexer API

## Overview

Mooncake Conductor is a KV cache indexer used by routers and gateways to make
cache-aware scheduling decisions. It consumes KV cache events from inference
engines or storage backends, maintains prefix-hit metadata across cache tiers,
and exposes HTTP APIs for service registration and cache-hit queries.

This document incorporates the latest API direction from:

- [RFC #1403: Mooncake KV-Store Indexer API Standardization](https://github.com/kvcache-ai/Mooncake/issues/1403)
- [RFC #1527: KV Events API Standardization](https://github.com/kvcache-ai/Mooncake/issues/1527)

The Conductor can serve multiple model groups in one process. Each query is
scoped by model identity, block size, LoRA identity, tenant isolation, and the
registered instance that can receive traffic.

## Concepts

### Storage tiers

The indexer tracks KV cache availability across three logical tiers:

- **G1, Device Pool**: Device-resident KV blocks, such as GPU, NPU, HBM, or
  other accelerator memory owned by inference engines.
- **G2, Host Pool**: CPU or host DRAM KV blocks, including Mooncake registered
  memory pools.
- **G3, Disk Pool**: SSD, 3FS, DFS, NFS, or other disk-backed KV storage.

The `medium` field identifies the concrete tier or device type. Common values
are `gpu`, `cpu`, and `disk`. Engines may add other values as new media are
supported.

### Identity dimensions

KV cache hits are interpreted under the following dimensions:

| Dimension | Description |
|---|---|
| `model_name` or `model` | Model identifier. KV blocks from different models are incompatible. |
| `block_size` | Number of tokens per KV block. Different block sizes produce different token-to-block mappings. |
| `additional_salt` | Opaque salt used to separate hash namespaces for quantization, model revision, tenant isolation, or other deployment-specific dimensions. |
|`cache_salt`| Ensure cached data blocks are kept separate for different customers|
| `lora_name` | LoRA adapter name. Empty or `null` means the base model. |
| `tenant_id` | Upstream tenant or customer identity. Used for isolation and to keep query output bounded. |
| `instance_id` | Routable API server or engine instance returned by the Indexer API. Routers use this value as the scheduling target. |
| `backend_id` | KV Events identity for the entity that owns the KV blocks. It may be an inference worker, a Mooncake storage daemon, or another cache backend. |
| `medium` | Cache medium where the blocks are present. |
| `dp_rank` | Data-parallel rank that owns or can serve the blocks. |

`instance_id` and `backend_id` intentionally have different meanings.
`instance_id` is the router-facing target in the Indexer API. `backend_id` is
the event-facing cache owner in the KV Events API. In deployments where cache
storage is decoupled from inference workers, `backend_id` can identify a cache
daemon while `instance_id` still identifies the engine endpoint that receives
requests.

## Hashing standard

The standardized event contract recommends **XXH3-64 with seed `S`**.

- **Local block hash**:
  `XXH3(token_bytes_le, S)`, where tokens are little-endian `u32` values
  concatenated for one block.
- **Rolling sequence hash**:
  The first block uses `seq_hash[0] = local_block_hash[0]`. Each subsequent
  block uses:

  ```text
  seq_hash[i] = XXH3(seq_hash[i-1]_le || local_block_hash[i]_le, S)
  ```

  Here `||` means byte concatenation, not a logical OR.

All hashes used by the standardized KV Events API are rolling sequence hashes.
A `seq_hash` identifies the whole prefix up to that block depth, so equal
prefixes produce equal hashes until the first differing block.

If an engine does not follow the standardized hashing scheme, it must provide
`token_ids` in `stored` events so the consumer can recompute the indexer's hash
representation.

## HTTP APIs

### `POST /register`

Registers a KV event publisher and starts consuming events from it.

```json
{
  "endpoint": "tcp://1.1.1.1:5557",
  "replay_endpoint": "tcp://1.1.1.1:5558",
  "type": "vLLM",
  "modelname": "deepseek",
  "lora_name": "sql-adapter",
  "tenant_id": "default",
  "instance_id": "vllm-prefill-node1",
  "block_size": 128,
  "dp_rank": 0,
  "additionalsalt": "w8a8"
}
```

| Field | Required | Description |
|---|---|---|
| `endpoint` | Yes | ZMQ KV event publisher endpoint. |
| `replay_endpoint` | No | ZMQ replay endpoint used to recover missed events. |
| `type` | Yes | Publisher type, such as `vLLM`, `SGLang`, or `Mooncake`. |
| `modelname` | Yes | Model name for this publisher. This is the HTTP API wire name for `model_name`. |
| `lora_name` | No | LoRA adapter name. Empty or omitted means base model. |
| `tenant_id` | No | Tenant identity. Defaults to `default`. |
| `instance_id` | Yes | Router-facing engine or API server instance identity. |
| `block_size` | Yes | KV block size in tokens. |
| `dp_rank` | Yes | Data-parallel rank for this publisher. |
| `additionalsalt` | No | HTTP API wire name for `additional_salt`. Defaults to an empty string. |

Successful response:

```json
{
  "status": "registered successfully",
  "instance_id": "vllm-prefill-node1"
}
```

### `POST /unregister`

Stops consuming events for a registered publisher.

```json
{
  "type": "vLLM",
  "modelname": "deepseek",
  "lora_name": "sql-adapter",
  "tenant_id": "default",
  "instance_id": "vllm-prefill-node1",
  "block_size": 128,
  "dp_rank": 0
}
```

`tenant_id` defaults to `default`. The current implementation removes the
subscription identified by `(instance_id, tenant_id, dp_rank)`.

Successful response:

```json
{
  "status": "unregistered successfully",
  "removed_instances": ["vllm-prefill-node1|default|0"]
}
```

### `POST /query`

Query cache hits by token IDs.

```json
{
  "model": "deepseek",
  "lora_name": "sql-adapter",
  "token_ids": [101, 15, 100, 55, 89],
  "tenant_id": "default",
  "instance_id": "vllm-prefill-node1",
  "block_size": 64,
  "cache_salt": "w8a8"
}
```

| Field | Required | Description |
|---|---|---|
| `model` | Yes | Model name. |
| `lora_name` | No | LoRA adapter name. Empty or omitted means base model. |
| `lora_id` | No | Deprecated compatibility field. Do not use together with `lora_name`. |
| `token_ids` | Yes | Prompt token IDs. Only complete blocks are considered. |
| `tenant_id` | No | Tenant identity. Defaults to `default`. |
| `instance_id` | No | If set, query one instance. If omitted, query all instances registered under the tenant. |
| `block_size` | Yes | KV block size in tokens. |
| `cache_salt` | No | Query-side hash namespace salt. Corresponds to the event `additional_salt` concept. |

Response:

```json
{
  "default": {
    "vllm-prefill-node1": {
      "longest_matched": 256,
      "GPU": 128,
      "DP": {
        "0": 128,
        "1": 256
      },
      "CPU": 256,
      "DISK": 0
    }
  }
}
```

| Field | Description |
|---|---|
| `longest_matched` | Longest continuous prefix hit in tokens across all tracked media and DP ranks for this instance. |
| `GPU`, `CPU`, `DISK` | Matched prefix tokens available on each medium. Names are examples; future media can be added. |
| `DP` | Matched prefix tokens grouped by data-parallel rank. |

### `POST /query_by_hash`

Queries cache hits by precomputed rolling sequence hashes. This API avoids
sending long token lists over the network.

```json
{
  "model": "deepseek",
  "lora_name": "sql-adapter",
  "seq_hashes": [1234567890, 9876543210],
  "tenant_id": "default",
  "instance_id": "vllm-prefill-node1",
  "block_size": 64,
  "cache_salt": "w8a8"
}
```

For compatibility with earlier drafts, clients may call the hash list
`block_hash`, but new clients should use `seq_hashes` to make it explicit that
the values are rolling sequence hashes rather than local block hashes.

The response shape is the same as `/query`. For this API,
`longest_matched = block_size * matched_hash_count`.


## KV Events API

The KV Events API is the wire contract between cache owners and indexers.
Conductor normalizes engine-specific events into this model.

### Event envelope

Every standardized event carries the same envelope:

```json
{
  "event_id": 42,
  "timestamp": 1739145600000,
  "event_type": "stored",
  "model_name": "llama-3.1-8b",
  "block_size": 64,
  "additional_salt": null,
  "lora_name": null,
  "tenant_id": "default",
  "backend_id": "worker-0",
  "medium": "gpu",
  "dp_rank": 0
}
```

| Field | Type | Description |
|---|---|---|
| `event_id` | `u64` | Monotonically increasing sequence number scoped by the event stream dimensions. Authoritative for ordering. |
| `timestamp` | `u64 or null` | Unix epoch milliseconds. Informational only, not used for ordering. |
| `event_type` | `string` | One of `stored`, `removed`, or `cleared`. |
| `model_name` | `string or null` | Model identifier. |
| `block_size` | `u32 or null` | Tokens per block. |
| `additional_salt` | `string or null` | Opaque deployment salt or namespace. |
| `lora_name` | `string or null` | LoRA adapter name, or `null` for the base model. |
| `tenant_id` | `string` | Tenant or customer identity. |
| `backend_id` | `string` | Entity that owns the KV blocks. This can be an engine worker or a decoupled cache daemon. |
| `medium` | `string or null` | Cache medium such as `gpu`, `cpu`, or `disk`. |
| `dp_rank` | `u32 or null` | Data-parallel rank. |

Events must be processed in consecutive `event_id` order within each stream
identified by `(model_name, block_size, additional_salt, lora_name, tenant_id,
backend_id, medium, dp_rank)`.

### `stored`

Published when one or more consecutive blocks are committed to a KV cache.

```json
{
  "event_id": 42,
  "timestamp": 1739145600000,
  "event_type": "stored",
  "model_name": "llama-3.1-8b",
  "block_size": 64,
  "additional_salt": null,
  "lora_name": null,
  "tenant_id": "default",
  "backend_id": "worker-0",
  "medium": "gpu",
  "dp_rank": 0,
  "seq_hashes": [1234567890, 9876543210, 1122334455],
  "base_block_idx": 5,
  "parent_hash": 9999999999,
  "token_ids": null
}
```

| Field | Type | Description |
|---|---|---|
| `seq_hashes` | `u64[]` | Rolling sequence hashes of consecutive stored blocks. |
| `base_block_idx` | `u32 or null` | Zero-based depth of the first block in this event. |
| `parent_hash` | `u64 or null` | Rolling sequence hash at depth `base_block_idx - 1`; `null` at the root. |
| `token_ids` | `u32[] or null` | Tokens across all blocks in this event. Required when the publisher does not use the standardized hash. |

At least one of `base_block_idx` or `parent_hash` must be present so the
consumer can locate the blocks in the sequence.

### `removed`

Published when one or more blocks are evicted.

```json
{
  "event_id": 43,
  "timestamp": 1739145601000,
  "event_type": "removed",
  "model_name": "llama-3.1-8b",
  "block_size": 64,
  "additional_salt": null,
  "lora_name": null,
  "tenant_id": "default",
  "backend_id": "worker-0",
  "medium": "gpu",
  "dp_rank": 0,
  "seq_hashes": [1122334455],
  "base_block_idx": 7
}
```

`seq_hashes` is required. `base_block_idx` is optional but recommended for
collision detection and observability.

### `cleared`

Published when all blocks for the event stream dimensions are purged.

```json
{
  "event_id": 44,
  "timestamp": 1739145602000,
  "event_type": "cleared",
  "model_name": "llama-3.1-8b",
  "block_size": 64,
  "additional_salt": null,
  "lora_name": null,
  "tenant_id": "default",
  "backend_id": "worker-0",
  "medium": "gpu",
  "dp_rank": 0
}
```

No additional payload fields are required.

## Compatibility notes

The current Conductor implementation consumes vLLM ZMQ msgpack batches and
normalizes `BlockStored` and `BlockRemoved` into the internal prefix index.
Registration metadata supplies fields such as `modelname`, `tenant_id`,
`instance_id`, `block_size`, and `additionalsalt` when the engine event does
not carry the full standardized envelope.
