# vLLM V1 Disaggregated Serving with Mooncake Store and LMCache

## Overview

This guide shows a single-machine 1-prefill/1-decode deployment using vLLM V1,
LMCache's multiprocess server, and Mooncake Store as the LMCache L2 backend.

In this setup, one machine runs Mooncake master, one LMCache MP server, the
disaggregated proxy, the prefiller vLLM instance, and the decoder vLLM
instance. The prefiller and decoder should use different GPUs.

This example uses `"metadata_server":"P2PHANDSHAKE"` for Mooncake transfer
metadata, so the Mooncake HTTP metadata server is not needed. If you switch to
HTTP metadata, remember that Mooncake's HTTP metadata server also defaults to
`8080`, which conflicts with LMCache's HTTP API on a single host.

## Prerequisites

Install Mooncake, vLLM, and LMCache on the machine. The example assumes an RDMA
deployment and uses:

- local host address: `{IP of Machine}`
- RDMA device: `{RDMA device}`
- LMCache checkout path: `/path/to/LMCache`

Replace these values with the local hostname/IP, RDMA device, and LMCache checkout path for your environment.

The `mooncake_store` MP L2 adapter requires LMCache's `lmcache_mooncake` C++
extension. When building LMCache from source, enable Mooncake support, for
example:

```bash
BUILD_MOONCAKE=1 \
MOONCAKE_INCLUDE_DIR=/path/to/mooncake/include \
MOONCAKE_LIB_DIR=/path/to/mooncake/lib \
pip install -e /path/to/LMCache --verbose
```

## Deployment

### 1. Start Mooncake Master

```bash
mooncake_master -v=1 \
  --rpc_port=50051 \
  --metrics_port=9003
```

### 2. Start the LMCache Multiprocess Server

Start one LMCache MP server and configure Mooncake Store as the L2 adapter.

```bash
lmcache server \
  --host 127.0.0.1 \
  --port 5555 \
  --http-host 127.0.0.1 \
  --http-port 8080 \
  --l1-size-gb 32 \
  --eviction-policy LRU \
  --no-l1-use-lazy \
  --l2-adapter '{
    "type": "mooncake_store",
    "local_hostname": "{IP of Machine}",
    "metadata_server": "P2PHANDSHAKE",
    "protocol": "rdma",
    "rdma_devices": "{RDMA device}",
    "global_segment_size": "3221225472",
    "local_buffer_size": "3221225472",
    "master_server_addr": "127.0.0.1:50051"
  }'
```

### 3. Start the Disaggregated Proxy

The proxy receives client requests, sends prefill requests to the prefiller,
sends decode requests to the decoder, and receives LMCache request telemetry
from the prefiller.

```bash
python /path/to/LMCache/examples/disagg_prefill_mp/disagg_proxy_server.py \
  --host 127.0.0.1 \
  --port 8000 \
  --prefiller-host 127.0.0.1 \
  --prefiller-port 8100 \
  --decoder-host 127.0.0.1 \
  --decoder-port 8200 \
  --telemetry-port 5768
```

### 4. Start the vLLM Prefiller

The prefiller reports request telemetry back to the proxy so the proxy knows
when KV cache storage has completed.

```bash
CUDA_VISIBLE_DEVICES=0 \
LMCACHE_REQUEST_TELEMETRY_TYPE=fastapi \
LMCACHE_REQUEST_TELEMETRY_ENDPOINT=http://127.0.0.1:5768/api/v1/telemetry \
vllm serve Qwen/Qwen3-4B \
  --host 127.0.0.1 \
  --port 8100 \
  --gpu-memory-utilization 0.8 \
  --no-enable-log-requests \
  --no-enable-prefix-caching \
  --kv-transfer-config '{
    "kv_connector": "LMCacheMPConnector",
    "kv_role": "kv_both",
    "kv_connector_extra_config": {
      "lmcache.mp.host": "tcp://127.0.0.1",
      "lmcache.mp.port": 5555
    }
  }'
```

### 5. Start the vLLM Decoder

The decoder connects to the same local LMCache MP server. It does not need
request telemetry environment variables; only the prefiller reports the "KV
cache is stored" event back to the proxy.

```bash
CUDA_VISIBLE_DEVICES=1 \
vllm serve Qwen/Qwen3-4B \
  --host 127.0.0.1 \
  --port 8200 \
  --gpu-memory-utilization 0.8 \
  --no-enable-log-requests \
  --no-enable-prefix-caching \
  --kv-transfer-config '{
    "kv_connector": "LMCacheMPConnector",
    "kv_role": "kv_both",
    "kv_connector_extra_config": {
      "lmcache.mp.host": "tcp://127.0.0.1",
      "lmcache.mp.port": 5555
    }
  }'
```

### 6. Send a Test Request

Send traffic to the proxy, not directly to either vLLM instance.

```bash
curl -N http://127.0.0.1:8000/v1/chat/completions \
  -H "Content-Type: application/json" \
  -d '{
    "model": "Qwen/Qwen3-4B",
    "messages": [
      {
        "role": "user",
        "content": "Explain how KV cache reuse helps long-context serving."
      }
    ],
    "max_tokens": 128,
    "temperature": 0.7
  }'
```

## Port and Configuration Checklist

When changing ports away from these defaults, update all dependent settings
together:

- Mooncake master `--rpc_port` must match LMCache `master_server_addr`.
- The vLLM prefiller and decoder should connect to the local LMCache MP server
  via `kv_connector_extra_config.lmcache.mp.host` and
  `kv_connector_extra_config.lmcache.mp.port`.
- Proxy `--prefiller-port` and `--decoder-port` must match the two vLLM
  `--port` values.
- `LMCACHE_REQUEST_TELEMETRY_ENDPOINT` on the prefiller must point to the proxy
  telemetry endpoint.
- If `metadata_server` is changed from `P2PHANDSHAKE` to an HTTP metadata URL,
  enable Mooncake HTTP metadata server and make sure its port does not conflict
  with LMCache `--http-port`.

## Additional Resources

* [Mooncake x LMCache: Unite to Pioneer KVCache-Centric LLM Serving System](../../../getting_started/examples/lmcache-integration.md)
* [LMCache MP `mooncake_store` L2 adapter](https://docs.lmcache.ai/mp/l2_storage.html#mooncake-store-mooncake-store-native-connector)
* [LMCache multiprocess disaggregated prefill example](https://github.com/LMCache/LMCache/tree/dev/examples/disagg_prefill_mp)
