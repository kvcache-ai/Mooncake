# Guide: vLLM MooncakeStoreConnector

## Overview

This document describes how to deploy vLLM's `MooncakeStoreConnector`. `MooncakeStoreConnector`  is a new vLLM's KV connector that uses `MooncakeDistributedStore` as a shared KV cache pool. It enables:

* **CPU/Disk offloading**: Extend effective KV cache capacity by offloading to CPU memory or SSD via Mooncake's transfer engine.
* **Hash-based prefix caching across instances**: Multiple vLLM instances share cached KV blocks through the store using block-hash deduplication.
* **Flexible deployment**: Works as a single-node KV cache extension (`kv_both`), or in disaggregated prefill-decode setups (`kv_producer` / `kv_consumer`).



## Deployment

### 1. Prerequisites

Before you begin, make sure that:

* [vLLM](https://github.com/vllm-project/vllm) is installed, [Mooncake](https://github.com/kvcache-ai/Mooncake) is installed. Refer to the [vLLM official repository](https://github.com/vllm-project/vllm) and [Mooncake official repository](https://github.com/kvcache-ai/Mooncake) for more installation instructions and building from source.



### 2. Mooncake Master Server

**Start:**

```shell
mooncake_master --port 50063
```

**Configure Mooncake** : Create a JSON configuration file (e.g., `mooncake_config.json`):

```json
{
  "metadata_server": "http://127.0.0.1:8092/metadata",
  "master_server_address": "127.0.0.1:50063",
  "global_segment_size": "0",
  "local_buffer_size": "2147483648",
  "protocol": "rdma",
  "device_name": "",
}
```

**Set environment variable:**

```shell
export MOONCAKE_CONFIG_PATH=/path/to/mooncake_config.json
```



### 3. Usage

**3.1** **Single-Node KV Cache Offloading** (i.e., `kv_both`)

```shell
MOONCAKE_CONFIG_PATH=mooncake_config.json \
vllm serve meta-llama/Llama-3.1-8B-Instruct \
    --kv-transfer-config '{"kv_connector":"MooncakeStoreConnector","kv_role":"kv_both"}'
```

**3.2** **XpYd Disaggregated Prefill-Decode** (i.e., `kv_producer/kv_consumer`)

Prefill Node:

```shell
MOONCAKE_CONFIG_PATH=mooncake_config.json \
VLLM_MOONCAKE_BOOTSTRAP_PORT=50052 \
vllm serve meta-llama/Llama-3.1-8B-Instruct \
    --port 8100 \
    --kv-transfer-config '{
        "kv_connector": "MultiConnector",
        "kv_role": "kv_producer",
        "kv_connector_extra_config": {
            "connectors": [
                {
                    "kv_connector": "MooncakeConnector",
                    "kv_role": "kv_producer"
                },
                {
                    "kv_connector": "MooncakeStoreConnector",
                    "kv_role": "kv_producer"
                }
            ]
        }
    }'
```

Decode Node:

```shell
MOONCAKE_CONFIG_PATH=mooncake_config.json \
VLLM_MOONCAKE_BOOTSTRAP_PORT=50053 \
vllm serve meta-llama/Llama-3.1-8B-Instruct \
    --port 8200 \
    --kv-transfer-config '{
        "kv_connector": "MultiConnector",
        "kv_role": "kv_consumer",
        "kv_connector_extra_config": {
            "connectors": [
                {
                    "kv_connector": "MooncakeConnector",
                    "kv_role": "kv_consumer"
                },
                {
                    "kv_connector": "MooncakeStoreConnector",
                    "kv_role": "kv_consumer"
                }
            ]
        }
    }'
```

Proxy：

```shell
python examples/disaggregated/disaggregated_serving/mooncake_connector/mooncake_connector_proxy.py --prefill http://192.168.0.2:8100 --decode http://192.168.0.3:8200
```



> When running with data parallelism, set a fixed `PYTHONHASHSEED` so that block hashes are consistent across DP ranks:
>
> ```shell
> PYTHONHASHSEED=0 vllm serve ...
> ```
>
> Without this, identical prompts may produce different block hashes on different DP ranks, preventing cross-instance prefix cache hits.
