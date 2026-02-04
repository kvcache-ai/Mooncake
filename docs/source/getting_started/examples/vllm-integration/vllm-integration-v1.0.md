# vLLM v1 backend Disaggregated Serving with MooncakeConnector

## Overview

This guide demonstrates how to use the MooncakeConnector with vLLM v1 backend for disaggregated serving in Prefill-Decode separation architecture. The integration enables efficient cross-node KV cache transfer using RDMA technology.

For more details about Mooncake, please refer to [Mooncake project](https://github.com/kvcache-ai/Mooncake) and [Mooncake documents](https://kvcache-ai.github.io/Mooncake/).

## Installation

### Prerequisites

Install mooncake-transfer-engine through pip:

```bash
pip install mooncake-transfer-engine
```

Note: If you encounter problems such as missing `lib*.so`, you should uninstall this package by `pip3 uninstall mooncake-transfer-engine`, and build the binaries manually according to the [instructions](../build.md).

### Install vLLM

Refer to [vLLM official installation guide](https://docs.vllm.ai/en/latest/getting_started/installation.html) for the latest installation instructions.

## Usage

### Basic Setup (Different Nodes)

#### Prefiller Node (192.168.0.2)

```bash
vllm serve Qwen/Qwen2.5-7B-Instruct \
  --port 8010 \
  --kv-transfer-config '{"kv_connector":"MooncakeConnector","kv_role":"kv_producer"}'
```

#### Decoder Node (192.168.0.3)

```bash
vllm serve Qwen/Qwen2.5-7B-Instruct \
  --port 8020 \
  --kv-transfer-config '{"kv_connector":"MooncakeConnector","kv_role":"kv_consumer"}'
```

#### Proxy Server

```bash
# In vllm root directory. 
python tests/v1/kv_connector/nixl_integration/toy_proxy_server.py \
  --prefiller-host 192.168.0.2 --prefiller-port 8010 \
  --decoder-host 192.168.0.3 --decoder-port 8020
```

> NOTE: The Mooncake Connector currently uses the proxy from nixl_integration. This will be replaced with a self-developed proxy in the future.

Now you can send requests to the proxy server through port 8000.

#### Test

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

### Advanced Configuration

#### With Tensor Parallelism

**Prefiller:**

```bash
CUDA_VISIBLE_DEVICES=0,1,2,3,4,5,6,7 \
vllm serve Qwen/Qwen2.5-7B-Instruct \
  --port 8010 \
  --tensor-parallel-size 8 \
  --kv-transfer-config '{"kv_connector":"MooncakeConnector","kv_role":"kv_producer"}'
```

**Decoder:**

```bash
CUDA_VISIBLE_DEVICES=0,1,2,3,4,5,6,7 \
vllm serve Qwen/Qwen2.5-7B-Instruct \
  --port 8020 \
  --tensor-parallel-size 8 \
  --kv-transfer-config '{"kv_connector":"MooncakeConnector","kv_role":"kv_consumer"}'
```

#### Configuration Parameters

- `--kv-transfer-config`: JSON string to configure the KV transfer connector
  - `kv_connector`: Set to "MooncakeConnector"
  - `kv_role`: Role of the instance
    - `kv_producer`: For prefiller instances that generate KV caches
    - `kv_consumer`: For decoder instances that consume KV caches
    - `kv_both`: Enables symmetric functionality (experimental)
    - `num_workers`: Thread pool size in each prefiller worker to send kvcache (default 10)

## Environment Variables

The following environment variables can be used to customize Mooncake behavior:

- `VLLM_MOONCAKE_BOOTSTRAP_PORT`: Port for Mooncake bootstrap server
  - Default: 8998
  - Required only for prefiller instances
  - Each vLLM worker needs a unique port on its host
  - For TP/DP deployments, each worker's port is computed as: `base_port + dp_rank * tp_size + tp_rank`

- `VLLM_MOONCAKE_ABORT_REQUEST_TIMEOUT`: Timeout (in seconds) for automatically releasing KV cache
  - Default: 480
  - Used when a request is aborted to prevent holding resources indefinitely

## Performance

For detailed performance benchmarks and results, see the [vLLM Benchmark](../../performance/vllm-v1-support-benchmark.md) documentation.

## Notes

- Tensor parallelism (TP) is supported for both prefiller and decoder instances
- The proxy server should typically run on the decoder node
- Ensure network connectivity between prefiller and decoder nodes for RDMA transfer
- For production deployments, consider using a more robust proxy solution

## Troubleshooting

- If you encounter connection issues, check that:
  - All nodes can reach each other over the network
  - Firewall rules allow traffic on the specified ports
  - RDMA devices are properly configured
- For missing library errors, rebuild mooncake-transfer-engine from source
- Enable debug logging with `VLLM_LOGGING_LEVEL=DEBUG` for detailed diagnostics
