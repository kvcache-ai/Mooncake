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
            "lora_id": 12, // defined for backward compatibility and should not be used together with `lora_name`
            "token_ids": [1, 15, 100],
        }
        ```
    - **Parameter Description**:
        - `model`: (required, string) model name
        - `lora_name`: (optional, string) The name of the LoRA adapter, default is `None`(indicating no LoRA adapter is used)
        - `lora_id`: (optional, int) The ID of the LoRA adapter. This parameter is defined for backward compatibility and should not be used together with `lora_name`(Only one of them can be specified). Default is `-1`(indicating no LoRA adapter is used)
        - `token_ids`: (required, [int]) prompt token id list
    - **example**:
        ```json
        {
            "model": "deepseek-v3",
            "lora_name": "sql_adapter",
            "token_ids": [101, 15, 100, 55, 89],
        }
        ```
- **Output**:
    ```json
    {
        "data": {
            "engine_name": {
                "longest_matched": 100, // the number of longest prefix matched token among multiple DPs(if there are)
                "GPU": 20,
                "DP": {
                    0: 10,
                    1: 20
                },
                "CPU": 60,
                "DISK": 10
            },
            ... // other engine instance
        }
    }
    ```
    - **Parameter Description**
        - `engine_name`: engine instance name. For example, two service instances are currently started separately by running the `vllm server` command, and they are registered in the indexer with different names(such as vllm-1,vllm-2)
        - `longest_matched`: the number of longest prefix matched token among G1/G2/G3. Indexer will sequentially query the hit status of each token-block according to the prefix order. If it hits, count the situation of this token-block at each level; If it missed, terminate the query (ensuring prefix continuity).
        - `GPU`, `CPU`, `DISK`: token ids hit count for each tiered storage medium. The Indexer will track the storage status of KV-cache across various media. This requires different KV publishers to inform the Indexer of the actual storage medium type via kv-events. The following examples list several common names, such as using GPU or NPU to represent the Device Pool, using CPU to represent the Host Pool, and using DISK to represent the Disk Pool.
        - `DP`: token ids hit count for each DP rank.
    - **example**:

        Assume the input token_ids are [101, 15, 100, 55, 89, 63], the block_size is 2, and the dp2 strategy is enabled. There are three block hashes to match [H1, H2, H3], where H1 hits in GPU (dp0, dp1), CPU, and DISK; H2 hits in GPU (dp0) and CPU; and H3 hits in DISK.
        ```json
        {
            "vllm-1": {
                "longest_matched": 6,
                "GPU": 4,
                "DP": {
                    0: 4,
                    1: 2
                },
                "CPU": 4,
                "DISK": 4
            }
        }
        ```

### `POST /query_by_hash`
 query token hit count by chunked_token hash key. Each model service uses its own independent page_size, `longest_matched = page_size * matched hash_key`
- **Input**:
    - **Body** (JSON):
        ```json
        {
            "model": "deepseek",
            "lora_name": "xx-adapter",
            "lora_id": 12, // defined for backward compatibility and should not be used together with `lora_name`
            "block_hash": ["hash_key_by_chunked_tokens"],
        }
        ```
    - **Parameter Description**:
        - `model`: (required, string) model name
        - `lora_name`: (optional, string) The name of the LoRA adapter, default is `None`(indicating no LoRA adapter is used)
        - `lora_id`: (optional, int) The ID of the LoRA adapter. This parameter is defined for backward compatibility and should not be used together with `lora_name`(Only one of them can be specified). Default is `-1`(indicating no LoRA adapter is used)
        - `block_hash`: (required, [int]) chunk_token hash list
- **Output**:
    ```json
    {
        "data": {
            "engine_name": {
                "longest_matched": 100, // the number of longest prefix matched token among multiple DPs(if there are)
                "GPU": 20,
                "DP": {
                    0: 10,
                    1: 20
                },
                "CPU": 60,
                "DISK": 10
            },
            ... // other engine instance
        }
    }
    ```
    - **Parameter Description**: The output result is same as `/query` api.


## Indexer KVEvents Structure
Typically, the device pool is used for loading model weights, with the remaining space registered for KV blocks by the model inference service runtime, the host pool and disk pool are managed uniformly by the Mooncake Store. There is a difference in the management unit for KV data between the two: the device pool uses blocks as the smallest unit for KV data, while the host pool and disk pool use Mooncake Store Objects as the smallest unit for KV storage. In practice, users may split a complete KV block into multiple Mooncake Store Objects for maintenance according to parallel strategies such as tensor parallelism (tp) and context parallelism (cp).

### G1 KVEvents
[vLLM](https://github.com/vllm-project/vllm/blob/main/vllm/distributed/kv_events.py) and [SGLang](https://github.com/sgl-project/sglang/blob/main/python/sglang/srt/disaggregation/kv_events.py) publishes events using the `EventBatch` structure, with each batch containing three types of events:

- `BlockStored`：Adds a single KV block.
- `BlockRemoved`：Removes a single KV block.
- `AllBlocksCleared`：Clears all KV blocks.

```py
EventBatch：
{
    ts: float， # timestamp
    events：list[BlockStored | BlockRemoved | AllBlocksCleared]，
    data_parallel_rank: int | None = None,  # vLLM use this to indicate dp rank
    attn_dp_rank: int | None = None,        # SGLang use this to indicate dp rank
}


BlockStored:
{
    block_hashes: list[int]
    parent_block_hash: int | None
    token_ids: list[int]
    block_size: int

    lora_id: int | None
    """Deprecated: use `lora_name` for KV block key hash.
    Retained for backward compatibility.
    """
    
    medium: str | None
    """KV cache is categorized by tier. Currently, the following types are supported:
    set "GPU", "NPU" for device pool(G1), 
    set "CPU" for host pool(G2), 
    set "DISK" for disk pool(G3).
    In the future, more medium types can be supported for each tier. For example, "TPU" and "AMD" could be added for device pool.
    """
    lora_name: str | None
}

BlockRemoved:
{
    block_hashes: list[int]
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

    uint64_t block_hash,
    uint64_t parent_block_hash,
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
