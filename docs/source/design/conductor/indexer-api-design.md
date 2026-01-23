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
