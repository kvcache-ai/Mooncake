# vLLM V0 Disaggregated Serving with MooncakeStore

## Overview
This is the latest version of the MooncakeStore integration doc with the vLLM project based on [PR 10502](https://github.com/vllm-project/vllm/pull/10502) and [PR 12957](https://github.com/vllm-project/vllm/pull/12957) to support KVCache transfer for intra-node and inter-node disaggregated serving scenario. Benchmark results will be released soon.

Main changes from v0.x to v1:
- XpYd support and orchestration
  - dynamic changing the population of prefill group and decode group
- More stable and more fault-tolerant
  - The sudden crash of a single vllm instance is tolerable
  - Since instance-to-instance connections are removed, each instance works as a vanilla vllm instance, which means it can serve the requests that are not from the proxy and finish them normally


**_Please note that this is still an experimental version and will be modified anytime based on feedback from the vLLM community._**
 - **Update(Apr 10, 2025)**: We are working on the vLLM v1 integration now. Stay tuned.
 - **Update(Sep 5, 2025)**: We have released the vLLM v1 integration with Mooncake Store and LMCache. Please refer to [vllmv1-lmcache-integration](vllmv1-lmcache-integration.md) for more details.

## Installation

### Prerequisite

```bash
pip3 install mooncake-transfer-engine
```

Note:
  - If you encounter problems such as missing `lib*.so`, you should uninstall this package by `pip3 uninstall mooncake-transfer-engine`, and build the binaries manually according to the [instructions](build.md).
- For vLLM version <= v0.8.4, it requires mooncake-transfer-engine <= 0.3.3.post2. In the latest release, interface `mooncake_vllm_adaptor` has been deprecated.

### Install the latest version of vLLM
#### 1. Clone vLLM from official repo
```bash
git clone git@github.com:vllm-project/vllm.git
```
#### 2. Build
##### 2.1 Build from source
```bash
cd vllm
pip3 install -e .
```
 - If you encounter any problems that you cannot solve, please refer to the [vLLM official compilation guide](https://docs.vllm.ai/en/latest/getting_started/installation/index.html).

## Configuration
### Prepare configuration file to Run Example over RDMA

- Prepare a _**mooncake.json**_ file for both Prefill and Decode instances

```json
{
    "local_hostname": "192.168.0.137",
    "metadata_server": "etcd://192.168.0.137:2379",
    "protocol": "rdma",
    "device_name": "erdma_0",
    "master_server_address": "192.168.0.137:50001"
}
```
- "local_hostname": The IP address of the current node used to communicate with the metadata server.
  - **_All prefill instances and decode instances can share this config file on the same node._**
- "metadata_server": The metadata server of the mooncake transfer engine. For example,
  - Use `etcd` as backend: `"192.168.0.137:2379"`, `"etcd://192.168.0.137:2379"` or `"etcd://192.168.0.137:2379,192.168.0.138:2379"`
  - Use `redis` as backend: `"redis://192.168.0.137:6379"`
  - Use `http` as backend: `"http://192.168.0.137:8080/metadata"`
- "protocol": The protocol to be used for data transmission. ("rdma/tcp")
- "device_name": The device to be used for data transmission, it is required when "protocol" is set to "rdma". If multiple NIC devices are used, they can be separated by commas such as "erdma_0,erdma_1". Please note that there are no spaces between them.
- "master_server_address": The IP address and the port of the master daemon process of MooncakeStore.
### Prepare configuration file to Run Example over TCP

- Prepare a _**mooncake.json**_ file for both Prefill and Decode instances
```json
{
    "local_hostname": "192.168.0.137",
    "metadata_server": "etcd://192.168.0.137:2379",
    "protocol": "tcp",
    "device_name": "",
    "master_server_address": "192.168.0.137:50001"
}
```

## Run Example
 - Please change the IP addresses and ports in the following guide according to your env.
```bash
# Begin from `root` of your cloned repo!

# 1. Start the etcd server
etcd --listen-client-urls http://0.0.0.0:2379 --advertise-client-urls http://localhost:2379
# You may need to terminate other etcd processes before running the above command

# 2. Start the mooncake_master server
mooncake_master --port 50001
# If some vllm instances exit unexpectedly, some connection metadata will be corrupted since they are not properly cleaned. In that case, we recommend you restart the mooncake_master before running another test.

# 3. Run multiple vllm instances
# kv_producer role
MOONCAKE_CONFIG_PATH=./mooncake.json VLLM_USE_V1=0 python3 -m vllm.entrypoints.openai.api_server --model Qwen/Qwen2.5-7B-Instruct-GPTQ-Int4 --port 8100 --max-model-len 10000 --gpu-memory-utilization 0.8 --kv-transfer-config '{"kv_connector":"MooncakeStoreConnector","kv_role":"kv_producer"}'

CUDA_VISIBLE_DEVICES=1 MOONCAKE_CONFIG_PATH=./mooncake.json VLLM_USE_V1=0 python3 -m vllm.entrypoints.openai.api_server --model Qwen/Qwen2.5-7B-Instruct-GPTQ-Int4 --port 8101 --max-model-len 10000 --gpu-memory-utilization 0.8 --kv-transfer-config '{"kv_connector":"MooncakeStoreConnector","kv_role":"kv_producer"}'

CUDA_VISIBLE_DEVICES=2 MOONCAKE_CONFIG_PATH=./mooncake.json VLLM_USE_V1=0 python3 -m vllm.entrypoints.openai.api_server --model Qwen/Qwen2.5-7B-Instruct-GPTQ-Int4 --port 8102 --max-model-len 10000 --gpu-memory-utilization 0.8 --kv-transfer-config '{"kv_connector":"MooncakeStoreConnector","kv_role":"kv_producer"}'

CUDA_VISIBLE_DEVICES=3 MOONCAKE_CONFIG_PATH=./mooncake.json VLLM_USE_V1=0 python3 -m vllm.entrypoints.openai.api_server --model Qwen/Qwen2.5-7B-Instruct-GPTQ-Int4 --port 8103 --max-model-len 10000 --gpu-memory-utilization 0.8 --kv-transfer-config '{"kv_connector":"MooncakeStoreConnector","kv_role":"kv_producer"}'

# kv_consumer role
CUDA_VISIBLE_DEVICES=4 MOONCAKE_CONFIG_PATH=./mooncake.json VLLM_USE_V1=0 python3 -m vllm.entrypoints.openai.api_server --model Qwen/Qwen2.5-7B-Instruct-GPTQ-Int4 --port 8200 --max-model-len 10000 --gpu-memory-utilization 0.8 --kv-transfer-config '{"kv_connector":"MooncakeStoreConnector","kv_role":"kv_consumer"}'

CUDA_VISIBLE_DEVICES=5 MOONCAKE_CONFIG_PATH=./mooncake.json VLLM_USE_V1=0 python3 -m vllm.entrypoints.openai.api_server --model Qwen/Qwen2.5-7B-Instruct-GPTQ-Int4 --port 8201 --max-model-len 10000 --gpu-memory-utilization 0.8 --kv-transfer-config '{"kv_connector":"MooncakeStoreConnector","kv_role":"kv_consumer"}'

CUDA_VISIBLE_DEVICES=6 MOONCAKE_CONFIG_PATH=./mooncake.json VLLM_USE_V1=0 python3 -m vllm.entrypoints.openai.api_server --model Qwen/Qwen2.5-7B-Instruct-GPTQ-Int4 --port 8202 --max-model-len 10000 --gpu-memory-utilization 0.8 --kv-transfer-config '{"kv_connector":"MooncakeStoreConnector","kv_role":"kv_consumer"}'

CUDA_VISIBLE_DEVICES=7 MOONCAKE_CONFIG_PATH=./mooncake.json VLLM_USE_V1=0 python3 -m vllm.entrypoints.openai.api_server --model Qwen/Qwen2.5-7B-Instruct-GPTQ-Int4 --port 8203 --max-model-len 10000 --gpu-memory-utilization 0.8 --kv-transfer-config '{"kv_connector":"MooncakeStoreConnector","kv_role":"kv_consumer"}'
```

- `MOONCAKE_CONFIG_PATH` is the path to the mooncake.json configuration file.
- `VLLM_USE_MODELSCOPE` is optional, if you have access to huggingface, please remove it.
- `VLLM_USE_V1=0` is required since the disaggregated feature is currently only supported on V0 vLLM.
  - You can also `export` this configuration to the env, instead of putting it in front of every single command.
- The `--model` parameter specifies the model to use.
- The `--port` parameter specifies the vllm service port on which to listen.
- The `--max-model-len` parameter specifies the maximum length of the model.
- Option `--tensor_parallel_size` \ `-tp` is supported. Example: append `-tp 2` to the run command to run vllm with multiple GPUs.
  - Note: All instances should have the same tensor_parallel_size.
  - If you want to run the prefill instance and decode instance on the same node, please set up different `CUDA_VISIBLE_DEVICES`. For example, `CUDA_VISIBLE_DEVICES=0,1` for the prefill instance and `CUDA_VISIBLE_DEVICES=2,3` for the decode instance.

- The `--kv-transfer-config` parameter specifies the connector and its config to be used.
  - Please set up `kv_connector` to `MooncakeStoreConnector`.
  - `kv_role` is the node's role, either 'kv_producer', 'kv_consumer' or 'kv_both'.

```bash
# 4. Start the proxy server
cd vllm
python3 examples/online_serving/disagg_examples/disagg_proxy_demo.py --model Qwen/Qwen2.5-7B-Instruct-GPTQ-Int4 --prefill localhost:8100 localhost:8101  --decode localhost:8200 localhost:8201  --port 8000
```

- The `--model` parameter specifies the model to use, also specifies the tokenizer used by the proxy server.
- The `--port` parameter specifies the vllm service port on which to listen.
- The `--prefill` or `-p` specifies the ip and port of the vllm prefill instances.
- The `--decode` or `-d` specifies the ip and port of the vllm decode instances.

```bash
# If you want to dynamically adjust the instances of p-nodes and d-nodes during runtime, you need to configure this environment variables.
export ADMIN_API_KEY="xxxxxxxx"
# or add it before the command:
ADMIN_API_KEY="xxxxxxxx" python3 vllm/examples/online_serving/disagg_examples/disagg_demo.py --model Qwen/Qwen2.5-7B-Instruct-GPTQ-Int4 --prefill localhost:8100 localhost:8101  --decode localhost:8200 localhost:8201  --port 8000 --scheduling round_robin

# Then use this command to add instances into prefill group or decode group
curl -X POST "http://localhost:8000/instances/add" -H "Content-Type: application/json" -H "X-API-Key: $ADMIN_API_KEY" -d '{"type": "prefill", "instance": "localhost:8102"}'

curl -X POST "http://localhost:8000/instances/add" -H "Content-Type: application/json" -H "X-API-Key: $ADMIN_API_KEY" -d '{"type": "prefill", "instance": "localhost:8103"}'

curl -X POST "http://localhost:8000/instances/add" -H "Content-Type: application/json" -H "X-API-Key: $ADMIN_API_KEY" -d '{"type": "decode", "instance": "localhost:8202"}'

curl -X POST "http://localhost:8000/instances/add" -H "Content-Type: application/json" -H "X-API-Key: $ADMIN_API_KEY" -d '{"type": "decode", "instance": "localhost:8203"}'

# Use this command to get the proxy status
curl localhost:8000/status | jq
```

Mooncake team implements this simple disagg_proxy based on round-robin as a demo. In the production stage, service providers and users can also implement corresponding global proxy strategies according to their needs.

**_Be sure to change the IP address in the commands._**


## Test with openai compatible request
```
curl -s http://localhost:8000/v1/completions -H "Content-Type: application/json" -d '{
  "model": "Qwen/Qwen2.5-7B-Instruct-GPTQ-Int4",
  "prompt": "San Francisco is a",
  "max_tokens": 1000
}'
```
- If you are not testing on the proxy server, please change the `localhost` to the IP address of the proxy server.
