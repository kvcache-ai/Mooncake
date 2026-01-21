# Mooncake Conductor Indexer

## Introduction
The Mooncake Conductor Indexer is a specialized service designed to efficiently track and report token hit counts across various caching levels for different model instances. It provides a list of APIs that allow users to query token hit statistics based on token ID or chunked token hash, thereby facilitating optimized the performance of LLM inference.


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
                "L1": {
                    "dp0": 10,
                    "dp1": 20
                },
                "L2": 60,
                "L3": 10
            },
            ... // other engine instance
        }
    }
    ```
    - **Parameter Description**
        - `engine_name`: engine instance name
        - `matched_tokens`: total number of matched token ids
        - `L1`, `L2`, `L3`: token ids hit count for each level
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
        - `medium`: optional，Specify the cache level (L1, L2, L3). By default, all levels are returned.
- **Output**:
    ```json
    {
        "data": {
            "engine_name": {
                "matched_tokens": 100,
                "L1": {
                    "dp0": 10,
                    "dp1": 20
                },
                "L2": 60,
                "L3": 10
            },
            ... // other engine instance
        }
    }
    ```
    - **Parameter Description**
        - `engine_name`: engine instance name
        - `matched_tokens`: total number of matched token ids
        - `L1`, `L2`, `L3`: token ids hit count for each level
        - `dp0`, `dp1`, .. : token ids hit count for each DP rank


### `GET /metrics`
get indexer internal metrics for monitor.
- **Output**:
    ```json
    {
        "data": {
            "query_request": {
                "total": 1000,
                "success": 500,
                "failed": 500
            },
            "kv_cache_number": 10000,
            ...
        }
    }
    ```
    - **Parameter Description**
        - `query_request`: total, success and failed request count
        - `kv_cache_number`: total number of kv cache entries

### `GET /ping`
check if the indexer service is alive.

### `POST /ping`
check if the indexer service is alive.