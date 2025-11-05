# vLLM V1 Disaggregated Serving with Mooncake Store and LMCache

## Overview

The vLLM v1 version has been released with support for PD disaggregation. The detailed design document can be found [here](https://docs.google.com/document/d/1uPGdbEXksKXeN4Q9nUm9hzotqEjQhYmnpAhidLuAsjk). LMCache immediately implemented the corresponding connector to support storage, transmission, and loading of KVCache, enabling collaborative operation with PD nodes. Mooncake, as LMCache's backend storage engine, has undergone extensive optimizations in usability, performance, and stability. This document explains how to deploy a PD disaggregated serving demo using LMCache + Mooncake.

## Deployment

1. First, you need to prepare two GPU-equipped machines, which we will refer to as Machine A and Machine B. Install [vLLM](https://docs.vllm.ai/en/latest/getting_started/quickstart.html), [Mooncake](https://kvcache-ai.github.io/Mooncake/getting_started/build.html) and [LMCache](https://docs.lmcache.ai/getting_started/installation.html) on both Machine A and Machine B. For specific installation instructions, please refer to the official documentation of each repository.

2. Start the Mooncake Master node on Machine A:
```bash
mooncake_master -port 50052 -max_threads 64 -metrics_port 9004 \
  --enable_http_metadata_server=true \
  --http_metadata_server_host=0.0.0.0 \
  --http_metadata_server_port=8080
```

3. Launch the Decoder instance on machine A
- Modify the vllm/examples/others/lmcache/disagg_prefill_lmcache_v1/disagg_vllm_launcher.sh file.
```diff
diff --git a/examples/lmcache/disagg_prefill_lmcache_v1/disagg_vllm_launcher.sh b/examples/lmcache/disagg_prefill_lmcache_v1/disagg_vllm_launcher.sh
index 831ef0bb5..a2ff0744c 100644
--- a/examples/lmcache/disagg_prefill_lmcache_v1/disagg_vllm_launcher.sh
+++ b/examples/lmcache/disagg_prefill_lmcache_v1/disagg_vllm_launcher.sh
 elif [[ $1 == "decoder" ]]; then
     # Decoder listens on port 8200
-    decode_config_file=$SCRIPT_DIR/configs/lmcache-decoder-config.yaml
+    decode_config_file=$SCRIPT_DIR/configs/mooncake-decoder-config.yaml
 
     UCX_TLS=cuda_ipc,cuda_copy,tcp \
         LMCACHE_CONFIG_FILE=$decode_config_file \
         LMCACHE_USE_EXPERIMENTAL=True \
         VLLM_ENABLE_V1_MULTIPROCESSING=1 \
         VLLM_WORKER_MULTIPROC_METHOD=spawn \
         CUDA_VISIBLE_DEVICES=1 \
```
- Add the `mooncake-decoder-config.yaml` file
```yaml
chunk_size: 256
remote_url: "mooncakestore://{IP of Machine A}:50052/"
remote_serde: "naive"
local_cpu: False
max_local_cpu_size: 100

extra_config:
  local_hostname: "{IP of Machine A}"
  metadata_server: "http://{IP of Machine A}:8080/metadata"
  protocol: "rdma"
  device_name: "mlx5_0" # Multiple RDMA devices can be specified as comma-separated list
  master_server_address: "{IP of Machine A}:50052"
  global_segment_size: 32212254720 # 30GB
  local_buffer_size: 1073741824 # 1GB
  transfer_timeout: 1
  save_chunk_meta: False
```

- Launch the Decoder instance using command 
```bash
bash disagg_vllm_launcher.sh decoder Qwen/Qwen2.5-7B-Instruct-GPTQ-Int4
```

4. Launch the Prefiller instance on machine B
- Modify the vllm/examples/others/lmcache/disagg_prefill_lmcache_v1/disagg_vllm_launcher.sh file.
```diff
diff --git a/examples/lmcache/disagg_prefill_lmcache_v1/disagg_vllm_launcher.sh b/examples/lmcache/disagg_prefill_lmcache_v1/disagg_vllm_launcher.sh
index 831ef0bb5..9e5a3f044 100644
--- a/examples/lmcache/disagg_prefill_lmcache_v1/disagg_vllm_launcher.sh
+++ b/examples/lmcache/disagg_prefill_lmcache_v1/disagg_vllm_launcher.sh
@@ -18,12 +18,14 @@ fi
 
 if [[ $1 == "prefiller" ]]; then
     # Prefiller listens on port 8100
-    prefill_config_file=$SCRIPT_DIR/configs/lmcache-prefiller-config.yaml
+    prefill_config_file=$SCRIPT_DIR/configs/mooncake-prefiller-config.yaml
```

- Add the `mooncake-prefiller-config.yaml` file
```yaml
chunk_size: 256
remote_url: "mooncakestore://{IP of Machine A}:50052/"
remote_serde: "naive"
local_cpu: False
max_local_cpu_size: 100

extra_config:
  local_hostname: "{IP of Machine B}"
  metadata_server: "http://{IP of Machine A}:8080/metadata"
  protocol: "rdma"
  device_name: "mlx5_0" # Multiple RDMA devices can be specified as comma-separated list
  master_server_address: "{IP of Machine A}:50052"
  global_segment_size: 32212254720 # 30GB
  local_buffer_size: 1073741824 # 1GB
  transfer_timeout: 1
  save_chunk_meta: False
```

- Launch the Prefiller instance using command 
```bash
bash disagg_vllm_launcher.sh prefiller Qwen/Qwen2.5-7B-Instruct-GPTQ-Int4
```

5. Prepare the router `disagg_proxy_server`

We use the [disagg_proxy_server](https://github.com/LMCache/LMCache/blob/dev/examples/disagg_prefill/disagg_proxy_server.py) provided by LMCache. According to [LMCache/LMCache#1342](https://github.com/LMCache/LMCache/issues/1342), when using Mooncake Store as the backend, you need to comment out `wait_decode_kv_ready(req_id)` in the proxy code.

6. Launch the `disagg_proxy_server` using command

```bash
python3 disagg_proxy_server.py --host localhost --port 9000 --prefiller-host IP_of_Machine_B --prefiller-port 8100 --decoder-host IP_of_Machine_A --decoder-port 8200 
```

7. Now we can send the requests to the `disagg_proxy_server` to test PD disaggregated serving.

## Additional Resources

* [Mooncake x LMCache: Unite to Pioneer KVCache-Centric LLM Serving System](../../../getting_started/examples/lmcache-integration.md)
* [Using Mooncake in LMCache](https://docs.lmcache.ai/kv_cache/storage_backends/mooncake.html)
* [Using LMCache in vLLM](https://github.com/vllm-project/vllm/tree/main/examples/others/lmcache)