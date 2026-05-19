
# vLLM V1 Disaggregated Serving with MooncakeConductor

## Overview
This is the latest version of the Mooncake Conductor integration doc with the vLLM project to support KVCache-Aware scheduling algorithm.
The conductor can be integrated as a plugin into any proxy to uniformly manage KV events from L1 to L3. We also provide a toy_proxy for those who want to try it out ([proxy](./cacheaware_disaggregated_proxy.py)). Benchmark results will be released soon.

- only vLLM and vLLM-Ascend are supported.


## Installation

The mooncake conductor will be compiled and installed together with the mooncake store. Refer to [Build Guide](https://github.com/kvcache-ai/Mooncake/blob/main/docs/source/getting_started/build.md).

- **WITH_CONDUCTOR must be set to ON in Mooncake/CMakeLists.txt.**

### Install the latest version of vLLM and vLLM-Ascend

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


#### 3. Clone vLLM-Ascend from official repo

```bash
git clone git@github.com:vllm-project/vllm-ascend.git
```

#### 4. Build
##### 4.1 Build from source
```bash
cd vllm-ascend
pip install -e .
```
 - If you encounter any problems that you cannot solve, please refer to the [vLLM-Ascend official compilation guide](https://docs.vllm.ai/projects/ascend/en/latest/).


## Configuration
### Prepare configuration file to Run Example

- Prepare a _**conductor_config.json**_ file for mooncake_conductor. Here is an example:

```json
{
    "kvevent_instance":
    {
        "vllm-1": 
        {
            "ip": "127.0.0.1",
            "port": 5557,
            "type": "vLLM",
            "modelname": "qwen2.5_7B",
            "lora_id": -1
        },
        "mooncake":
        {
            "ip": "127.0.0.1",
            "port": 19997,
            "type": "Mooncake",
            "modelname": "qwen2.5_7B",
            "lora_id": -1
        }
    },
    "http_server_port": 13333
}
```
- `kvevent_instance`: Services capable of reporting KV events.
- `vllm-1/mooncake`: rename of a VLLM instance or Mooncake-master instance.You can modify it according to your own preferences.
  - `ip`: zmq publisher IP.
  - `port`: zmq publisher port.
  - `type`: Mark the type of kv-event publisher. Generally, there are currently only two types: `vLLM` and `Mooncake`.
  - `modelname`: Model name used for match the model.
  - `lora_id`: LoRA Adapter ID.

- `http_server_port`: Conductor http server for querying cache hit rates, default use `13333`.



## Run Example

### 1. Start the mooncake_master server

```sh
# start mooncake_master without kv-event publish
mooncake_master --rpc_port 50051
# start moocake_master with kv-event
mooncake_master -enable_kv_event_publish -kv_event_publisher_endpoint tcp://*:19997 -rpc_port 50051
```
### 2. Run multiple vllm instances
```sh
# kv_producer role
 vllm serve /qwen2.5_7B_instruct/  \
    --enforce-eager \
    --max-model-len 10000 \
    --port 8100 \
    --gpu-memory-utilization 0.8 \
    --served-model-name "qwen2.5_7B" \
    --trust-remote-code \
    --kv-events-config \
    '{
        "publisher": "zmq", 
        "enable_kv_cache_events": true, 
        "endpoint": "tcp://*:5557",
        "topic": "kv-events",
        "replay_endpoint": "tcp://*:5558"
    }' \
    --kv-transfer-config \
    '{
        "kv_connector": "MooncakeConnectorStoreV1",
        "kv_role":"kv_producer",
        "kv_connector_extra_config":{"use_layerwise": false}
    }'
```

```sh
# kv_consumer role
 vllm serve /qwen2.5_7B_instruct/  \
    --enforce-eager \
    --max-model-len 10000 \
    --port 8200 \
    --gpu-memory-utilization 0.8 \
    --served-model-name "qwen2.5_7B" \
    --trust-remote-code \
    --kv-transfer-config \
    '{
        "kv_connector": "MooncakeConnectorStoreV1",
        "kv_role":"kv_consumer",
        "kv_connector_extra_config":{"use_layerwise": false}
    }'

```


### 3. Start the conductor server

```sh
export CONDUCTOR_CONFIG_PATH="./example/conductor_config.json"
mooncake_conductor
```

### 4. Run the proxy in the example

```sh
python cacheaware_disaggregated_proxy.py --prefiller-hosts 127.0.0.1 --prefiller-ports 8100 --decoder-host 127.0.0.1 --decoder-ports 8200 --conductor-address 127.0.0.1:13333
```

## Test with openai compatible request

```sh
curl -s http://localhost:8000/v1/completions -H "Content-Type: application/json" -d '{
  "model": "qwen2.5_7B",
  "prompt": "What are the key architectural differences between vLLM and Mooncake when it comes to handling key-value (KV) cache events, and how can a centralized conductor component be designed in Go to normalize disparate event schemas from these systems, apply consistent metrics collection, and make dynamic scheduling decisions based on real-time KV cache hit rates without relying on Kubernetes-based autoscaling mechanisms?",
  "max_tokens": 1000
}'

```
