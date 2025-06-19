# Mooncake Store x LMCache for vLLM V1

The vLLM v1 version has been released with support for PD separation. The detailed design document can be found here: https://docs.google.com/document/d/1uPGdbEXksKXeN4Q9nUm9hzotqEjQhYmnpAhidLuAsjk. LMCache immediately implemented the corresponding connector to support storage, transmission, and loading of KVCache, enabling collaborative operation with PD nodes. Mooncake, as LMCache's backend storage engine, has undergone extensive optimizations in usability, performance, and stability. This document explains how to deploy a complete vLLM V1 PD separation instance using LMCache + Mooncake.

## Deployment

1. First, you need to prepare two GPU-equipped machines, which we will refer to as Machine A and Machine B. Install vLLM, LMCache, and Mooncake on both Machine A and Machine B. For specific installation instructions, please refer to the official documentation of each repository.

2. Start the Mooncake Master node on Machine A using the following command:
`cd Mooncake/build && ./mooncake-store/src/mooncake_master -v=1 -port=50052 -max_threads 64 -metrics_port 9004`

3. Start etcd on Machine A with the command:
`etcd --listen-client-urls http://0.0.0.0:2379 --advertise-client-urls http://localhost:2379`

4. Launch D endpoint on machine A
- Modify the vllm/examples/others/lmcache/disagg_prefill_lmcache_v1/disagg_vllm_launcher.sh file.
```diff
diff --git a/examples/lmcache/disagg_prefill_lmcache_v1/disagg_vllm_launcher.sh b/examples/lmcache/disagg_prefill_lmcache_v1/disagg_vllm_launcher.sh
index 831ef0bb5..a2ff0744c 100644
--- a/examples/lmcache/disagg_prefill_lmcache_v1/disagg_vllm_launcher.sh
+++ b/examples/lmcache/disagg_prefill_lmcache_v1/disagg_vllm_launcher.sh
@@ -24,6 +24,8 @@ if [[ $1 == "prefiller" ]]; then
         LMCACHE_CONFIG_FILE=$prefill_config_file \
         LMCACHE_USE_EXPERIMENTAL=True \
         VLLM_ENABLE_V1_MULTIPROCESSING=1 \
+       VLLM_USE_MODELSCOPE=True \
+       MOONCAKE_CONFIG_PATH=./mooncake.json \
         VLLM_WORKER_MULTIPROC_METHOD=spawn \
         CUDA_VISIBLE_DEVICES=0 \
         vllm serve $MODEL \
@@ -36,11 +38,13 @@ if [[ $1 == "prefiller" ]]; then
 
 elif [[ $1 == "decoder" ]]; then
     # Decoder listens on port 8200
-    decode_config_file=$SCRIPT_DIR/configs/lmcache-decoder-config.yaml
+    decode_config_file=$SCRIPT_DIR/decode.yaml
 
     UCX_TLS=cuda_ipc,cuda_copy,tcp \
         LMCACHE_CONFIG_FILE=$decode_config_file \
         LMCACHE_USE_EXPERIMENTAL=True \
+       VLLM_USE_MODELSCOPE=True \
+       MOONCAKE_CONFIG_PATH=./mooncake.json \
         VLLM_ENABLE_V1_MULTIPROCESSING=1 \
         VLLM_WORKER_MULTIPROC_METHOD=spawn \
         CUDA_VISIBLE_DEVICES=1 \
```
- Add `decode.yaml` and 'mooncake.json' file
```yaml
chunk_size: 256
local_device: "cpu"
remote_url: "mooncakestore://{IP of Machine A}:50052/"
remote_serde: "naive"
pipelined_backend: False
local_cpu: False
max_local_cpu_size: 100
```
```json
{
        "local_hostname": "{IP of Machine A}",
        "metadata_server": "etcd://{IP of Machine A}:2379",
        "protocol": "rdma",
        "device_name": "erdma_0, erdma_1",
        "global_segment_size": 3355443200,
        "local_buffer_size": 1073741824,
        "master_server_address": "{IP of Machine A}:50052"
}
```
- Launch D endpoint using command `bash disagg_vllm_launcher.sh decoder Qwen/Qwen2.5-7B-Instruct-GPTQ-Int4`

5. Launch P endpoint on machine B
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
+    prefill_config_file=$SCRIPT_DIR/prefill.yaml
 
     UCX_TLS=cuda_ipc,cuda_copy,tcp \
         LMCACHE_CONFIG_FILE=$prefill_config_file \
         LMCACHE_USE_EXPERIMENTAL=True \
         VLLM_ENABLE_V1_MULTIPROCESSING=1 \
+       VLLM_USE_MODELSCOPE=True \
+       MOONCAKE_CONFIG_PATH=./mooncake.json \
         VLLM_WORKER_MULTIPROC_METHOD=spawn \
         CUDA_VISIBLE_DEVICES=0 \
         vllm serve $MODEL \
@@ -42,6 +44,8 @@ elif [[ $1 == "decoder" ]]; then
         LMCACHE_CONFIG_FILE=$decode_config_file \
         LMCACHE_USE_EXPERIMENTAL=True \
         VLLM_ENABLE_V1_MULTIPROCESSING=1 \
+       VLLM_USE_MODELSCOPE=True \
+       MOONCAKE_CONFIG_PATH=./mooncake.json \
         VLLM_WORKER_MULTIPROC_METHOD=spawn \
         CUDA_VISIBLE_DEVICES=1 \
         vllm serve $MODEL \
```

- Add `prefill.yaml` and `mooncake.json` file
```yaml
chunk_size: 256
local_device: "cpu"
remote_url: "mooncakestore://{IP of Machine A}:50052/"
remote_serde: "naive"
pipelined_backend: False
local_cpu: False
max_local_cpu_size: 100
```

```json
{
        "local_hostname": "{IP of Machine B}", 
        "metadata_server": "etcd://{IP of Machine A}:2379",
        "protocol": "rdma",
        "device_name": "erdma_0, erdma_1",
        "global_segment_size": 3355443200,
        "local_buffer_size": 1073741824,
        "master_server_address": "{IP of Machine A}:50052"
}
```

- Launch P endpoint using command `bash disagg_vllm_launcher.sh prefiller Qwen/Qwen2.5-7B-Instruct-GPTQ-Int4`

6. Launch the LoadBalance endpoint using command
```bash
python3 disagg_proxy_server.py --host localhost --port 9000 --prefiller-host IP_of_Machine_B --prefiller-port 8100 --decoder-host IP_of_Machine_A --decoder-port 8200 
```

7. Now we can send the requests to LoadBalance to test PD separation.
