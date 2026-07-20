# Disaggregated Prefill-Decode with MooncakeConnector

## Overview

This guide demonstrates how to use `MooncakeConnector` with vLLM for disaggregated Prefill-Decode (PD) serving. `MooncakeConnector` enables direct cross-node KV cache transfer between prefill and decode instances using RDMA technology, achieving up to **142.25 GB/s** peak bandwidth (71.1% utilization of 8x RoCE).

For more details about Mooncake, please refer to [Mooncake project](https://github.com/kvcache-ai/Mooncake) and [Mooncake documents](https://kvcache-ai.github.io/Mooncake/).

---

## Choose Your vLLM Backend

| Backend | vLLM Version | Status | Guide |
|---------|-------------|--------|-------|
| **vLLM V1** | Latest | Recommended | [Jump to V1 guide](#using-vllm-v1-recommended) |
| **vLLM V0** | ≤ v0.6.4.post1 | Legacy | [Jump to V0 guide](#using-vllm-v0-legacy) |

```{admonition} New Users
:class: tip
If you are starting a new deployment, use the **vLLM V1** backend. V0 support is maintained for existing deployments only.
```

---

## Using vLLM V1 (Recommended)

This section covers `MooncakeConnector` integration with vLLM V1 backend. The integration enables efficient cross-node KV cache transfer via RDMA.

### Installation

#### Prerequisites

Install mooncake-transfer-engine through pip:

```bash
pip install mooncake-transfer-engine
```

```{note}
If you encounter problems such as missing `lib*.so`, uninstall this package by `pip3 uninstall mooncake-transfer-engine`, and build the binaries manually according to the [instructions](../../build.md).
```

#### Install vLLM

Refer to [vLLM official installation guide](https://docs.vllm.ai/en/latest/getting_started/installation.html) for the latest installation instructions.

### Usage

#### Basic Setup (Different Nodes)

**Prefiller Node** (192.168.0.2):

```bash
vllm serve Qwen/Qwen2.5-7B-Instruct \
  --port 8010 \
  --kv-transfer-config '{"kv_connector":"MooncakeConnector","kv_role":"kv_producer"}'
```

**Decoder Node** (192.168.0.3):

```bash
vllm serve Qwen/Qwen2.5-7B-Instruct \
  --port 8020 \
  --kv-transfer-config '{"kv_connector":"MooncakeConnector","kv_role":"kv_consumer"}'
```

**Proxy Server:**

```bash
# In vllm root directory.
python tests/v1/kv_connector/nixl_integration/toy_proxy_server.py \
  --prefiller-host 192.168.0.2 --prefiller-port 8010 \
  --decoder-host 192.168.0.3 --decoder-port 8020
```

> NOTE: The Mooncake Connector currently uses the proxy from nixl_integration. This will be replaced with a self-developed proxy in the future.

Now you can send requests to the proxy server through port 8000.

**Test:**

```bash
curl http://127.0.0.1:8000/v1/chat/completions \
  -H "Content-Type: application/json" \
  -d '{
    "model": "Qwen/Qwen2.5-7B-Instruct",
    "messages": [
      {"role": "user", "content": "Tell me a long story about artificial intelligence."}
    ]
  }'
```

#### Advanced Configuration

**With Tensor Parallelism:**

Prefiller:

```bash
CUDA_VISIBLE_DEVICES=0,1,2,3,4,5,6,7 \
vllm serve Qwen/Qwen2.5-7B-Instruct \
  --port 8010 \
  --tensor-parallel-size 8 \
  --kv-transfer-config '{"kv_connector":"MooncakeConnector","kv_role":"kv_producer"}'
```

Decoder:

```bash
CUDA_VISIBLE_DEVICES=0,1,2,3,4,5,6,7 \
vllm serve Qwen/Qwen2.5-7B-Instruct \
  --port 8020 \
  --tensor-parallel-size 8 \
  --kv-transfer-config '{"kv_connector":"MooncakeConnector","kv_role":"kv_consumer"}'
```

#### Configuration Parameters

- `--kv-transfer-config`: JSON string to configure the KV transfer connector
  - `kv_connector`: Set to `"MooncakeConnector"`
  - `kv_role`: Role of the instance
    - `kv_producer`: For prefiller instances that generate KV caches
    - `kv_consumer`: For decoder instances that consume KV caches
    - `kv_both`: Enables symmetric functionality (experimental)
  - `num_workers`: Thread pool size in each prefiller worker to send kvcache (default 10)

### Environment Variables

- `VLLM_MOONCAKE_BOOTSTRAP_PORT`: Port for Mooncake bootstrap server (default: 8998)
  - Required only for prefiller instances
  - Each vLLM worker needs a unique port on its host
  - For TP/DP deployments, each worker's port is computed as: `base_port + dp_rank * tp_size + tp_rank`
- `VLLM_MOONCAKE_ABORT_REQUEST_TIMEOUT`: Timeout (in seconds) for automatically releasing KV cache (default: 480)
  - Used when a request is aborted to prevent holding resources indefinitely

### Performance

For detailed performance benchmarks and results, see the [vLLM Benchmark](../../../performance/vllm-v1-support-benchmark.md) documentation.

---

## Using vLLM V0 (Legacy)

```{admonition} Legacy Backend
:class: warning
This section is for vLLM V0 backend (≤ v0.6.4.post1). For new deployments, use the [V1 backend](#using-vllm-v1-recommended) above.
```

This integration is based on [PR 10502](https://github.com/vllm-project/vllm/pull/10502) and [PR 10884](https://github.com/vllm-project/vllm/pull/10884). Preview benchmark results are available at [vLLM Benchmark Results V0.2](../../../performance/vllm-benchmark-results-v0.2.md).

### Installation

#### Prerequisite

```bash
pip3 install mooncake-transfer-engine
```

```{note}
- If you encounter problems such as missing `lib*.so`, uninstall this package by `pip3 uninstall mooncake-transfer-engine`, and build the binaries manually according to the [instructions](../../build.md).
- For vLLM version ≤ v0.8.4, it requires `mooncake-transfer-engine ≤ 0.3.3.post2`. In the latest release, the interface `mooncake_vllm_adaptor` has been deprecated.
```

#### Install vLLM

**1. Clone vLLM from official repo:**

```bash
git clone git@github.com:vllm-project/vllm.git
```

**2. Build from source (Include C++ and CUDA code):**

```bash
cd vllm
pip3 uninstall vllm -y
pip3 install -e .
```

```{tip}
If the build fails, try upgrading cmake: `pip3 install cmake --upgrade`.
```

If you encounter any problems, refer to the [vLLM official compilation guide](https://docs.vllm.ai/en/v0.6.4.post1/getting_started/installation.html#install-the-latest-code).

### Configuration

#### Prepare configuration file over RDMA

Create a `mooncake.json` file for both Prefill and Decode instances. Use the identical config file on both sides.

```json
{
  "prefill_url": "192.168.0.137:13003",
  "decode_url": "192.168.0.139:13003",
  "metadata_server": "192.168.0.139:2379",
  "metadata_backend": "etcd",
  "protocol": "rdma",
  "device_name": "erdma_0"
}
```

- `prefill_url`: The IP address and port of the Prefill node (port is used to communicate with metadata server).
- `decode_url`: The IP address and port of the Decode node. If running prefill and decode on the same node, set a different port (at least 50 apart from `prefill_url` port) to avoid conflicts.
- `metadata_server`: The metadata server address. Supports `etcd`, `redis`, and `http` backends. Example: `"etcd://192.168.0.137:2379"`, `"redis://192.168.0.137:6379"`, `"http://192.168.0.137:8080/metadata"`.
- `metadata_backend`: Currently supports `"etcd"`, `"redis"`, and `"http"`. If absent and `metadata_server` has no prefix, defaults to `"etcd"`. This parameter will be deprecated in a future version.
- `protocol`: `"rdma"` or `"tcp"`.
- `device_name`: Required when protocol is `"rdma"`. Multiple NICs can be separated by commas (`"erdma_0,erdma_1"`).

#### Prepare configuration file over TCP

```json
{
  "prefill_url": "192.168.0.137:13003",
  "decode_url": "192.168.0.139:13003",
  "metadata_server": "192.168.0.139:2379",
  "metadata_backend": "etcd",
  "protocol": "tcp",
  "device_name": ""
}
```

### Run Example

Change the IP addresses and ports according to your environment.

```bash
# Begin from root of your cloned repo!

# 1. Start the etcd server
etcd --listen-client-urls http://0.0.0.0:2379 --advertise-client-urls http://localhost:2379

# 2. Run on the prefilling side (producer role)
MOONCAKE_CONFIG_PATH=./mooncake.json VLLM_USE_MODELSCOPE=True python3 -m vllm.entrypoints.openai.api_server \
    --model Qwen/Qwen2.5-7B-Instruct-GPTQ-Int4 \
    --port 8100 \
    --max-model-len 10000 \
    --gpu-memory-utilization 0.8 \
    --kv-transfer-config '{"kv_connector":"MooncakeConnector","kv_role":"kv_producer","kv_rank":0,"kv_parallel_size":2,"kv_buffer_size":2e9}'

# 3. Run on the decoding side (consumer role)
MOONCAKE_CONFIG_PATH=./mooncake.json VLLM_USE_MODELSCOPE=True python3 -m vllm.entrypoints.openai.api_server \
    --model Qwen/Qwen2.5-7B-Instruct-GPTQ-Int4 \
    --port 8200 \
    --max-model-len 10000 \
    --gpu-memory-utilization 0.8 \
    --kv-transfer-config '{"kv_connector":"MooncakeConnector","kv_role":"kv_consumer","kv_rank":1,"kv_parallel_size":2,"kv_buffer_size":2e9}'
```

**Key parameters:**
- `MOONCAKE_CONFIG_PATH`: Path to the mooncake.json configuration file.
- `VLLM_USE_MODELSCOPE`: Optional. Remove if you have HuggingFace access.
- `--kv-transfer-config`: Connector configuration
  - `kv_connector`: `"MooncakeConnector"`
  - `kv_role`: `"kv_producer"` or `"kv_consumer"`
  - `kv_rank`: 0 for producer, 1 for consumer
  - `kv_parallel_size`: Fixed to 2 currently
  - `kv_buffer_size`: KVCache lookup buffer size; increase for longer prompts. If OOM occurs, decrease `--gpu-memory-utilization`.
  - `kv_ip` and `kv_port`: Used to specify the IP address and port of the master node for `"PyNcclConnector"` distributed setup. Not used for `"MooncakeConnector"` currently. Instead, `"MooncakeConnector"` uses a config file to set up the distributed connection.
- `--tensor-parallel-size` / `-tp`: Supported. If running on the same node, set different `CUDA_VISIBLE_DEVICES`.

```{note}
If running prefill and decode on the same node, set a different port for `decode_url`. To avoid port conflicts, ensure the decode port differs by at least 50 from the `prefill_url` port (e.g., `"decode_url": "192.168.0.137:13103"`). If the same URL is set for both, the port of `decode_url` will be automatically incremented by 100.
```

**Proxy Server:**

```bash
python3 proxy_server.py
```

```python
# proxy_server.py
import os
import aiohttp
from quart import Quart, make_response, request

AIOHTTP_TIMEOUT = aiohttp.ClientTimeout(total=6 * 60 * 60)
app = Quart(__name__)

async def forward_request(url, data):
    async with aiohttp.ClientSession(timeout=AIOHTTP_TIMEOUT) as session:
        headers = {"Authorization": f"Bearer {os.environ.get('OPENAI_API_KEY')}"}
        async with session.post(url=url, json=data, headers=headers) as response:
            if response.status == 200:
                async for chunk_bytes in response.content.iter_chunked(1024):
                    yield chunk_bytes

@app.route('/v1/completions', methods=['POST'])
async def handle_request():
    try:
        original_request_data = await request.get_json()
        prefill_request = original_request_data.copy()
        prefill_request['max_tokens'] = 1  # prefill only
        async for _ in forward_request('http://localhost:8100/v1/completions', prefill_request):
            continue
        generator = forward_request('http://192.168.0.139:8200/v1/completions',  # Change IP
                                    original_request_data)
        response = await make_response(generator)
        response.timeout = None
        return response
    except Exception as e:
        import sys, traceback
        exc_info = sys.exc_info()
        print("Error occurred in disagg prefill proxy server")
        print(e)
        print("".join(traceback.format_exception(*exc_info)))

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000)
```

> Be sure to change the IP address in the proxy server code.

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

---

## Troubleshooting

- If you encounter connection issues, check that:
  - All nodes can reach each other over the network
  - Firewall rules allow traffic on the specified ports
  - RDMA devices are properly configured and listed in `device_name`
- For missing library errors, rebuild `mooncake-transfer-engine` from source
- Enable debug logging with `VLLM_LOGGING_LEVEL=DEBUG` for detailed diagnostics
- For production deployments, consider using a more robust proxy solution
