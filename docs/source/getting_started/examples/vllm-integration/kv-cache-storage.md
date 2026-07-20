# KV Cache Storage & Sharing with MooncakeStore

## Overview

This guide demonstrates how to use `MooncakeStore` / `MooncakeStoreConnector` with vLLM to build a distributed KV cache storage pool. It enables KV cache offloading to CPU/SSD, hash-based prefix caching across multiple vLLM instances, and flexible XpYd disaggregated deployment — where you can dynamically adjust prefill and decode group sizes at runtime.

Compared to Redis-based backends, MooncakeStore achieves significantly lower TTFT (e.g., **~32% improvement** in mean TTFT for 2P2D tp=2 under RDMA). See [benchmark results](../../../performance/vllm-benchmark-results-v1.md) for details.

---

## Choose Your vLLM Backend

| Backend | Connector | vLLM Version | Status | Guide |
|---------|-----------|-------------|--------|-------|
| **vLLM V1** | `MooncakeStoreConnector` | Latest | Recommended | [Jump to V1 guide](#using-vllm-v1-recommended) |
| **vLLM V0** | `MooncakeStore` | ≤ v0.6.4.post1 | Legacy | [Jump to V0 guide](#using-vllm-v0-legacy) |

```{admonition} New Users
:class: tip
If you are starting a new deployment, use the **vLLM V1** backend with `MooncakeStoreConnector`. V0 support is maintained for existing deployments only.
```

Key differences from v0.x to v1:
- **XpYd support and orchestration**: Dynamically change the population of prefill and decode groups
- **More stable and fault-tolerant**: A sudden crash of a single vLLM instance is tolerable; instance-to-instance connections are removed, so each instance works as a vanilla vLLM instance capable of handling requests independently

---

## Using vLLM V1 (Recommended)

This section covers `MooncakeStoreConnector` — the new vLLM KV connector that uses `MooncakeDistributedStore` as a shared KV cache pool. It enables:

- **CPU/Disk offloading**: Extend effective KV cache capacity by offloading to CPU memory or SSD via Mooncake's transfer engine.
- **Hash-based prefix caching across instances**: Multiple vLLM instances share cached KV blocks through the store using block-hash deduplication.
- **Flexible deployment**: Works as a single-node KV cache extension (`kv_both`), or in disaggregated prefill-decode setups (`kv_producer` / `kv_consumer`).

### Deployment

#### 1. Prerequisites

- [vLLM](https://github.com/vllm-project/vllm) is installed
- [Mooncake](https://github.com/kvcache-ai/Mooncake) is installed

Refer to the [vLLM official repository](https://github.com/vllm-project/vllm) and [Mooncake official repository](https://github.com/kvcache-ai/Mooncake) for installation instructions and building from source.

#### 2. Mooncake Master Server

**Start:**

```shell
mooncake_master --port 50063
```

**Configure Mooncake**: Create a JSON configuration file (e.g., `mooncake_config.json`):

```json
{
  "metadata_server": "http://127.0.0.1:8092/metadata",
  "master_server_address": "127.0.0.1:50063",
  "global_segment_size": "0",
  "local_buffer_size": "2147483648",
  "protocol": "rdma",
  "device_name": ""
}
```

**Set environment variable:**

```shell
export MOONCAKE_CONFIG_PATH=/path/to/mooncake_config.json
```

#### 3. Usage

**3.1 Single-Node KV Cache Offloading** (`kv_both`):

```shell
MOONCAKE_CONFIG_PATH=mooncake_config.json \
vllm serve meta-llama/Llama-3.1-8B-Instruct \
    --kv-transfer-config '{"kv_connector":"MooncakeStoreConnector","kv_role":"kv_both"}'
```

**3.2 XpYd Disaggregated Prefill-Decode** (`kv_producer/kv_consumer`):

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

Proxy:

```shell
python examples/disaggregated/disaggregated_serving/mooncake_connector/mooncake_connector_proxy.py \
    --prefill http://192.168.0.2:8100 \
    --decode http://192.168.0.3:8200
```

> When running with data parallelism, set a fixed `PYTHONHASHSEED` so that block hashes are consistent across DP ranks:
>
> ```shell
> PYTHONHASHSEED=0 vllm serve ...
> ```
>
> Without this, identical prompts may produce different block hashes on different DP ranks, preventing cross-instance prefix cache hits.

---

## Using vLLM V0 (Legacy)

```{admonition} Legacy Backend
:class: warning
This section is for vLLM V0 backend with `MooncakeStore`. For new deployments, use the [V1 backend with `MooncakeStoreConnector`](#using-vllm-v1-recommended) above.
```

This integration is based on [PR 10502](https://github.com/vllm-project/vllm/pull/10502) and [PR 12957](https://github.com/vllm-project/vllm/pull/12957) to support KVCache transfer for intra-node and inter-node disaggregated serving.

### Installation

#### Prerequisite

```bash
pip3 install mooncake-transfer-engine
```

```{note}
- If you encounter problems such as missing `lib*.so`, uninstall by `pip3 uninstall mooncake-transfer-engine`, and build manually according to the [instructions](../../build.md).
- For vLLM version ≤ v0.8.4, it requires `mooncake-transfer-engine ≤ 0.3.3.post2`. The interface `mooncake_vllm_adaptor` has been deprecated in the latest release.
```

#### Install vLLM

**1. Clone vLLM:**

```bash
git clone git@github.com:vllm-project/vllm.git
```

**2. Build from source:**

```bash
cd vllm
pip3 install -e .
```

If you encounter problems, refer to the [vLLM official compilation guide](https://docs.vllm.ai/en/latest/getting_started/installation/index.html).

### Configuration

#### Prepare configuration for RDMA

Create a `mooncake.json` file:

```json
{
    "local_hostname": "192.168.0.137",
    "metadata_server": "etcd://192.168.0.137:2379",
    "protocol": "rdma",
    "device_name": "erdma_0",
    "master_server_address": "192.168.0.137:50001"
}
```

- `local_hostname`: The IP address of the current node. All prefill and decode instances on the same node can share this config.
- `metadata_server`: The metadata server. Supports `etcd`, `redis`, and `http` backends.
- `protocol`: `"rdma"` or `"tcp"`.
- `device_name`: Required for RDMA. Multiple NICs separated by commas (`"erdma_0,erdma_1"`).
- `master_server_address`: The IP address and port of the MooncakeStore master daemon.

#### Prepare configuration for TCP

```json
{
    "local_hostname": "192.168.0.137",
    "metadata_server": "etcd://192.168.0.137:2379",
    "protocol": "tcp",
    "device_name": "",
    "master_server_address": "192.168.0.137:50001"
}
```

### Run Example

Change the IP addresses and ports according to your environment. `VLLM_USE_V1=0` is required for vLLM V0 backend.

```bash
# Begin from root of your cloned repo!

# 1. Start the etcd server
etcd --listen-client-urls http://0.0.0.0:2379 --advertise-client-urls http://localhost:2379
# You may need to terminate other etcd processes before running the above command

# 2. Start the mooncake_master server
mooncake_master --port 50001
# If some vllm instances exit unexpectedly, some connection metadata will be
# corrupted since they are not properly cleaned. In that case, we recommend
# you restart the mooncake_master before running another test.

# 3. Run multiple vllm instances
# kv_producer role
MOONCAKE_CONFIG_PATH=./mooncake.json VLLM_USE_V1=0 python3 -m vllm.entrypoints.openai.api_server \
    --model Qwen/Qwen2.5-7B-Instruct-GPTQ-Int4 \
    --port 8100 \
    --max-model-len 10000 \
    --gpu-memory-utilization 0.8 \
    --kv-transfer-config '{"kv_connector":"MooncakeStoreConnector","kv_role":"kv_producer"}'

CUDA_VISIBLE_DEVICES=1 MOONCAKE_CONFIG_PATH=./mooncake.json VLLM_USE_V1=0 python3 -m vllm.entrypoints.openai.api_server \
    --model Qwen/Qwen2.5-7B-Instruct-GPTQ-Int4 \
    --port 8101 \
    --max-model-len 10000 \
    --gpu-memory-utilization 0.8 \
    --kv-transfer-config '{"kv_connector":"MooncakeStoreConnector","kv_role":"kv_producer"}'

CUDA_VISIBLE_DEVICES=2 MOONCAKE_CONFIG_PATH=./mooncake.json VLLM_USE_V1=0 python3 -m vllm.entrypoints.openai.api_server \
    --model Qwen/Qwen2.5-7B-Instruct-GPTQ-Int4 \
    --port 8102 \
    --max-model-len 10000 \
    --gpu-memory-utilization 0.8 \
    --kv-transfer-config '{"kv_connector":"MooncakeStoreConnector","kv_role":"kv_producer"}'

CUDA_VISIBLE_DEVICES=3 MOONCAKE_CONFIG_PATH=./mooncake.json VLLM_USE_V1=0 python3 -m vllm.entrypoints.openai.api_server \
    --model Qwen/Qwen2.5-7B-Instruct-GPTQ-Int4 \
    --port 8103 \
    --max-model-len 10000 \
    --gpu-memory-utilization 0.8 \
    --kv-transfer-config '{"kv_connector":"MooncakeStoreConnector","kv_role":"kv_producer"}'

# kv_consumer role
CUDA_VISIBLE_DEVICES=4 MOONCAKE_CONFIG_PATH=./mooncake.json VLLM_USE_V1=0 python3 -m vllm.entrypoints.openai.api_server \
    --model Qwen/Qwen2.5-7B-Instruct-GPTQ-Int4 \
    --port 8200 \
    --max-model-len 10000 \
    --gpu-memory-utilization 0.8 \
    --kv-transfer-config '{"kv_connector":"MooncakeStoreConnector","kv_role":"kv_consumer"}'

CUDA_VISIBLE_DEVICES=5 MOONCAKE_CONFIG_PATH=./mooncake.json VLLM_USE_V1=0 python3 -m vllm.entrypoints.openai.api_server \
    --model Qwen/Qwen2.5-7B-Instruct-GPTQ-Int4 \
    --port 8201 \
    --max-model-len 10000 \
    --gpu-memory-utilization 0.8 \
    --kv-transfer-config '{"kv_connector":"MooncakeStoreConnector","kv_role":"kv_consumer"}'

CUDA_VISIBLE_DEVICES=6 MOONCAKE_CONFIG_PATH=./mooncake.json VLLM_USE_V1=0 python3 -m vllm.entrypoints.openai.api_server \
    --model Qwen/Qwen2.5-7B-Instruct-GPTQ-Int4 \
    --port 8202 \
    --max-model-len 10000 \
    --gpu-memory-utilization 0.8 \
    --kv-transfer-config '{"kv_connector":"MooncakeStoreConnector","kv_role":"kv_consumer"}'

CUDA_VISIBLE_DEVICES=7 MOONCAKE_CONFIG_PATH=./mooncake.json VLLM_USE_V1=0 python3 -m vllm.entrypoints.openai.api_server \
    --model Qwen/Qwen2.5-7B-Instruct-GPTQ-Int4 \
    --port 8203 \
    --max-model-len 10000 \
    --gpu-memory-utilization 0.8 \
    --kv-transfer-config '{"kv_connector":"MooncakeStoreConnector","kv_role":"kv_consumer"}'
```

**Key parameters:**
- `MOONCAKE_CONFIG_PATH`: Path to the mooncake.json configuration file.
- `VLLM_USE_MODELSCOPE`: Optional. Remove if you have HuggingFace access.
- `VLLM_USE_V1=0`: Required since the disaggregated feature is currently only supported on V0 vLLM. You can also `export` this configuration to the env instead of putting it in front of every command.
- `--model`: The model to use.
- `--port`: The vllm service port on which to listen.
- `--max-model-len`: The maximum length of the model.
- `--tensor-parallel-size` / `-tp`: Supported. All instances should have the same tensor_parallel_size. If running prefill and decode on the same node, set different `CUDA_VISIBLE_DEVICES` (e.g., `CUDA_VISIBLE_DEVICES=0,1` for prefill and `CUDA_VISIBLE_DEVICES=2,3` for decode).
- `--kv-transfer-config`: Set `kv_connector` to `"MooncakeStoreConnector"`, `kv_role` to `"kv_producer"`, `"kv_consumer"`, or `"kv_both"`.
- If some vLLM instances exit unexpectedly, connection metadata may be corrupted. Restart `mooncake_master` before another test.

```bash
# 5. Start the proxy server
cd vllm
python3 examples/online_serving/disagg_examples/disagg_proxy_demo.py \
    --model Qwen/Qwen2.5-7B-Instruct-GPTQ-Int4 \
    --prefill localhost:8100 localhost:8101 \
    --decode localhost:8200 localhost:8201 \
    --port 8000
```

- `--model`: The model and tokenizer used by the proxy server.
- `--port`: The proxy server port on which to listen.
- `--prefill` / `-p`: IP and port of the vllm prefill instances.
- `--decode` / `-d`: IP and port of the vllm decode instances.

#### Dynamic XpYd Adjustment

To dynamically adjust prefill and decode instances at runtime:

```bash
export ADMIN_API_KEY="xxxxxxxx"

# or add it before the command:
ADMIN_API_KEY="xxxxxxxx" python3 vllm/examples/online_serving/disagg_examples/disagg_demo.py \
    --model Qwen/Qwen2.5-7B-Instruct-GPTQ-Int4 \
    --prefill localhost:8100 localhost:8101 \
    --decode localhost:8200 localhost:8201 \
    --port 8000 \
    --scheduling round_robin

# Add instances to groups dynamically
curl -X POST "http://localhost:8000/instances/add" \
  -H "Content-Type: application/json" \
  -H "X-API-Key: $ADMIN_API_KEY" \
  -d '{"type": "prefill", "instance": "localhost:8102"}'

curl -X POST "http://localhost:8000/instances/add" \
  -H "Content-Type: application/json" \
  -H "X-API-Key: $ADMIN_API_KEY" \
  -d '{"type": "prefill", "instance": "localhost:8103"}'

curl -X POST "http://localhost:8000/instances/add" \
  -H "Content-Type: application/json" \
  -H "X-API-Key: $ADMIN_API_KEY" \
  -d '{"type": "decode", "instance": "localhost:8202"}'

curl -X POST "http://localhost:8000/instances/add" \
  -H "Content-Type: application/json" \
  -H "X-API-Key: $ADMIN_API_KEY" \
  -d '{"type": "decode", "instance": "localhost:8203"}'

# Get proxy status
curl localhost:8000/status | jq
```

```{note}
Mooncake team provides this simple round-robin proxy as a demo. In production, you can implement custom global proxy strategies.
```

**Be sure to change the IP address in the commands.**

### Test

```bash
curl -s http://localhost:8000/v1/completions \
  -H "Content-Type: application/json" \
  -d '{
    "model": "Qwen/Qwen2.5-7B-Instruct-GPTQ-Int4",
    "prompt": "San Francisco is a",
    "max_tokens": 1000
  }'
```

- If you are not testing on the proxy server, change `localhost` to the IP address of the proxy server.

---

## Performance

| Scenario | Document |
|----------|----------|
| V1 MooncakeStoreConnector vs Redis | [Benchmark V1](../../../performance/vllm-benchmark-results-v1.md) |
| V0 MooncakeStore vs Redis | [Benchmark V0](../../../performance/vllm-benchmark-results-v0.2.md) |

---

## Troubleshooting

- If you encounter connection issues, check that:
  - All nodes can reach each other over the network
  - Firewall rules allow traffic on the specified ports
  - RDMA devices are properly configured
  - `mooncake_master` is running and reachable
- For missing library errors, rebuild `mooncake-transfer-engine` from source
- If vLLM instances exit unexpectedly, restart `mooncake_master` to clean up corrupted metadata
- Enable debug logging with `VLLM_LOGGING_LEVEL=DEBUG` for detailed diagnostics
