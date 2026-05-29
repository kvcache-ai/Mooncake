# vLLM V1 Disaggregated Serving with Mooncake Store and LMCache

## Overview

This guide shows a two-machine 1-prefill/1-decode deployment using vLLM V1,
LMCache's non-MP `LMCacheConnectorV1`, and Mooncake Store as LMCache's remote
storage backend.

LMCache supports both non-MP mode and MP mode with Mooncake Store. This page
covers the non-MP path, where each vLLM instance loads an LMCache YAML config
through `LMCACHE_CONFIG_FILE` and connects directly to Mooncake Store with
`remote_url: "mooncakestore://..."`. For the LMCache multiprocess server path,
see [vLLM V1 Disaggregated Serving with Mooncake Store and LMCache \[MP\]](vllmv1-lmcache-mp-integration.md).

The examples below use:

- Machine A: Mooncake master and vLLM decoder
- Machine B: vLLM prefiller
- Mooncake master RPC address: `{IP of Machine A}:50051`
- Mooncake HTTP metadata endpoint: `http://{IP of Machine A}:8080/metadata`
- RDMA device: `{RDMA device}`

Replace these placeholders with the IP addresses, hostname, and RDMA device for
your environment.

## Prerequisites

Install Mooncake, vLLM, and LMCache on both machines. For installation details,
refer to the official documentation of each project:

- [Mooncake build guide](../../build.md)
- [LMCache installation](https://docs.lmcache.ai/getting_started/installation.html)
- [vLLM installation](https://docs.vllm.ai/en/latest/getting_started/installation/)

## Deployment

### 1. Start Mooncake Master on Machine A

```bash
mooncake_master -v=1 \
  --rpc_port=50051 \
  --metrics_port=9003 \
  --enable_http_metadata_server=true \
  --http_metadata_server_host=0.0.0.0 \
  --http_metadata_server_port=8080
```

### 2. Configure and Start the vLLM Decoder on Machine A

Modify the vLLM disaggregated prefill launcher to use a Mooncake-backed LMCache
config for the decoder.

The decoder should continue to use `LMCacheConnectorV1` with `kv_role` set to
`kv_consumer`.

```diff
diff --git a/examples/lmcache/disagg_prefill_lmcache_v1/disagg_vllm_launcher.sh b/examples/lmcache/disagg_prefill_lmcache_v1/disagg_vllm_launcher.sh
--- a/examples/lmcache/disagg_prefill_lmcache_v1/disagg_vllm_launcher.sh
+++ b/examples/lmcache/disagg_prefill_lmcache_v1/disagg_vllm_launcher.sh
@@
 elif [[ $1 == "decoder" ]]; then
     # Decoder listens on port 8200
-    decode_config_file=$SCRIPT_DIR/configs/lmcache-decoder-config.yaml
+    decode_config_file=$SCRIPT_DIR/configs/mooncake-decoder-config.yaml
```

Create `configs/mooncake-decoder-config.yaml`:

```yaml
chunk_size: 256
remote_url: "mooncakestore://{IP of Machine A}:50051/"
remote_serde: "naive"
local_cpu: False
max_local_cpu_size: 100

extra_config:
  local_hostname: "{IP of Machine A}"
  metadata_server: "http://{IP of Machine A}:8080/metadata"
  protocol: "rdma"
  device_name: "{RDMA device}"
  master_server_address: "{IP of Machine A}:50051"
  global_segment_size: 32212254720 # 30GB
  local_buffer_size: 1073741824 # 1GB
  transfer_timeout: 1
  save_chunk_meta: False
```

Launch the decoder:

```bash
bash disagg_vllm_launcher.sh decoder Qwen/Qwen2.5-7B-Instruct-GPTQ-Int4
```

### 3. Configure and Start the vLLM Prefiller on Machine B

Modify the launcher to use a Mooncake-backed LMCache config for the prefiller.

The prefiller should continue to use `LMCacheConnectorV1` with `kv_role` set to
`kv_producer`.

```diff
diff --git a/examples/lmcache/disagg_prefill_lmcache_v1/disagg_vllm_launcher.sh b/examples/lmcache/disagg_prefill_lmcache_v1/disagg_vllm_launcher.sh
--- a/examples/lmcache/disagg_prefill_lmcache_v1/disagg_vllm_launcher.sh
+++ b/examples/lmcache/disagg_prefill_lmcache_v1/disagg_vllm_launcher.sh
@@
 if [[ $1 == "prefiller" ]]; then
     # Prefiller listens on port 8100
-    prefill_config_file=$SCRIPT_DIR/configs/lmcache-prefiller-config.yaml
+    prefill_config_file=$SCRIPT_DIR/configs/mooncake-prefiller-config.yaml
```

Create `configs/mooncake-prefiller-config.yaml`:

```yaml
chunk_size: 256
remote_url: "mooncakestore://{IP of Machine A}:50051/"
remote_serde: "naive"
local_cpu: False
max_local_cpu_size: 100

extra_config:
  local_hostname: "{IP of Machine B}"
  metadata_server: "http://{IP of Machine A}:8080/metadata"
  protocol: "rdma"
  device_name: "{RDMA device}"
  master_server_address: "{IP of Machine A}:50051"
  global_segment_size: 32212254720 # 30GB
  local_buffer_size: 1073741824 # 1GB
  transfer_timeout: 1
  save_chunk_meta: False
```

Launch the prefiller:

```bash
bash disagg_vllm_launcher.sh prefiller Qwen/Qwen2.5-7B-Instruct-GPTQ-Int4
```

### 4. Start the Disaggregated Proxy

Use the LMCache [`disagg_proxy_server.py`](https://github.com/LMCache/LMCache/blob/dev/examples/disagg_prefill/disagg_proxy_server.py) to route requests between the prefiller and decoder. According to [LMCache/LMCache#1342](https://github.com/LMCache/LMCache/issues/1342), when using Mooncake Store as the backend, comment out the `wait_decode_kv_ready(...)` call in the proxy before starting it.

```bash
python3 disagg_proxy_server.py \
  --host 0.0.0.0 \
  --port 9000 \
  --prefiller-host {IP of Machine B} \
  --prefiller-port 8100 \
  --decoder-host {IP of Machine A} \
  --decoder-port 8200
```

### 5. Send a Test Request

Send traffic to the proxy, not directly to either vLLM instance.

```bash
curl -N http://{Proxy IP}:9000/v1/chat/completions \
  -H "Content-Type: application/json" \
  -d '{
    "model": "Qwen/Qwen2.5-7B-Instruct-GPTQ-Int4",
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

- Mooncake master `--rpc_port` must match `remote_url` and
  `extra_config.master_server_address`.
- `extra_config.metadata_server` must point to the Mooncake HTTP metadata
  endpoint when HTTP metadata is used.
- Decoder and prefiller `device_name`, `protocol`, `global_segment_size`, and
  `local_buffer_size` should be set for the local hardware and workload.
- Proxy `--prefiller-port` and `--decoder-port` must match the two vLLM
  instance ports.

## Additional Resources

* [Mooncake x LMCache: Unite to Pioneer KVCache-Centric LLM Serving System](../lmcache-integration.md)
* [Using Mooncake in LMCache](https://docs.lmcache.ai/kv_cache/storage_backends/mooncake.html)
* [Using LMCache in vLLM](https://github.com/vllm-project/vllm/tree/main/examples/others/lmcache)
