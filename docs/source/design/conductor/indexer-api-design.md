# Mooncake Conductor Indexer

## Introduction
The Mooncake Conductor Indexer is a specialized service designed to efficiently track and report token hit counts across various caching levels for different model instances. It provides a list of APIs that allow users to query token hit statistics based on token ID or chunked token hash, thereby facilitating optimized the performance of LLM inference.The figure below illustrates the architecture of Mooncake KVindexer: ![Mooncake KVindexer](../../image/conductor/architecture.png)

## tiered storage & pools
We drew inspiration from the definition of [KVBM components](https://github.com/ai-dynamo/dynamo/blob/main/docs/kvbm/kvbm_components.md) and divided the KV cache into three levels: G1, G2, and G3. The detailed introduction is as follows:

- **Device Pool(G1)**: Device-resident KV block pool. Allocates mutable device blocks, registers completed blocks (immutable), serves lookups by sequence hash, and is the target for onboarding (Host→Device, Disk→Device).
- **Host Pool(G2)**: Mooncake registered memory KV pool. Receives Device offloads (Device→Host), can onboard to Device (Host→Device), and offloads to Disk. For high-performance, zero-copy data transfers, it utilizes the Mooncake Transfer-Engine.
- **Disk Pool(G3)**: SSD NVMe-backed KV pool. Receives Host offloads (Host→Disk), and provides large space for storing KV. 


## Indexer API

### `POST /query`
 query token hit count.
- **Input**:
    - **Body** (JSON):
        ```json
        {
            "model": "deepseek",
            "lora_name": "xx-adapter",
            "token_ids": [1, 15, 100],
        }
        ```
    - **Parameter Description**:
        - `model`: model name
        - `lora_name`: LoRA adapter name
        - `token_ids`: prompt token id list
- **Output**:
    ```json
    {
        "data": {
            "engine_name": {
                "matched_tokens": 100,
                "G1": {
                    "dp0": 10,
                    "dp1": 20
                },
                "G2": 60,
                "G3": 10
            },
            ... // other engine instance
        }
    }
    ```
    - **Parameter Description**
        - `engine_name`: engine instance name
        - `matched_tokens`: total number of matched token ids
        - `G1`, `G2`, `G3`: token ids hit count for each level
        - `dp0`, `dp1`, .. : token ids hit count for each DP rank

### `POST /query_by_hash`
 query token hit count by chunked_token hash key. Each model service uses its own independent page_size, `matched_tokens = page_size * matched hash_key`
- **Input**:
    - **Body** (JSON):
        ```json
        {
            "model": "deepseek",
            "lora_name": "xx-adapter",
            "block_hash": ["hash_key_by_chunked_tokens"],
        }
        ```
    - **Parameter Description**:
        - `model`: model name
        - `lora_name`: LoRA adapter name
        - `block_hash`: chunk_token hash list
- **Output**:
    ```json
    {
        "data": {
            "engine_name": {
                "matched_tokens": 100,
                "G1": {
                    "dp0": 10,
                    "dp1": 20
                },
                "G2": 60,
                "G3": 10
            },
            ... // other engine instance
        }
    }
    ```
    - **Parameter Description**
        - `engine_name`: engine instance name
        - `matched_tokens`: total number of matched token ids
        - `G1`, `G2`, `G3`: token ids hit count for each level
        - `dp0`, `dp1`, .. : token ids hit count for each DP rank


## Indexer KVEvents Structure
Typically, the device pool is used for loading model weights, with the remaining space registered for KV blocks by the inference service runtime, the host pool and disk pool are managed uniformly by the Mooncake Store. There is a difference in the management unit for KV data between the two: the device pool uses blocks as the smallest unit for KV data, while the host pool and disk pool use Mooncake Store Objects as the smallest unit for KV storage. In practice, users may split a complete KV block into multiple Mooncake Store Objects for maintenance according to parallel strategies such as tensor parallelism (tp) and context parallelism (cp).

Note: The KVEvents defined here indicate event inputs in the indexer, and there may currently be differences from the interfaces in the Mooncake Publisher, which will be unified in the future.

### G1 KVEvents
Publishing events employs the `EventBatch` structure, with each batch containing three types of events:

- `BlockStored`：Adds a single KV block.
- `BlockRemoved`：Removes a single KV block.
- `AllBlocksCleared`：Clears all KV blocks.

```py
EventBatch：
{
    ts: float， # timestamp
    events：list[BlockStored | BlockRemoved | AllBlocksCleared]，
    data_parallel_rank: int | None = None
}


BlockStored:
{
    block_hashes: list[bytes]
    parent_block_hash: bytes | None
    token_ids: list[int]
    block_size: int

    lora_id: int | None
    """Deprecated: use `lora_name` for KV block key hash.
    Retained for backward compatibility.
    """
    
    medium: str | None
    lora_name: str | None
}

BlockRemoved:
{
    block_hashes: list[bytes]
    lora_name: str | None
}

```

### G2/G3 KVEvents
Mooncake Store is a distributed key-value (KV) store. To ensure system consistency, a timestamp is assigned to each KVEvent for maintenance.
Mooncake publishes events using the `EventBatch` structure, with each batch containing three types of events:
- `BlockStoreEvent`：Adds a single Mooncake Store Object.
- `BlockUpdateEvent`：Updates a single Mooncake Store Object.
- `RemoveAllEvent`：Removes all Mooncake Store Objects.

```cpp
EventBatch {
    std::vector<std::variant<BlockStoreEvent, BlockUpdateEvent, RemoveAllEvent>> events,
}

BlockStoreEvent {
    "BlockStoreEvent", // Event type identifier, string type
    float ts,          // timestamp
    std::string mooncake_key,
    std::vector<std::string> addr_list, // Storage location of each replica

    std::byte block_hash,
    std::byte parent_block_hash,
    std::vector<uint32_t> token_id,
    uint32_t block_size,

    std::string model_name,
    std::string lora_name,
    uint32_t lora_id, // Retained for backward compatibility
}

BlockUpdateEvent {
    "BlockUpdateEvent",
    float ts,          // timestamp
    mooncake_key,
    std::vector<std::string> addr_list,
}

RemoveAllEvent {
    "RemoveAllEvent"
}
```
