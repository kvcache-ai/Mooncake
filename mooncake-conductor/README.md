
# vLLM V1 Disaggregated Serving with MooncakeConductor

## Overview
This is the latest version of the MooncakeConductor integration doc with the vLLM project to support KVCache-Aware scheduling algorithm.
The conductor can be integrated as a plugin into any proxy to uniformly manage KV events from L1 to L3. We also provide a lightweight proxy for those who want to try it out ([proxy](./cacheaware_disaggregated_proxy.py)). Benchmark results will be released soon.

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
### Prepare configuration file to Run Example over RDMA

- Prepare a _**conductor_config.json**_ file for both Prefill and Decode instances

```json
{
    "kvevent_instance":
    {
        "vllm-1": 
        {
            "ip": "127.0.0.1",
            "port": 3337,
            "type": "vLLM",
            "modelname": "qwen2.5",
            "lora_id": -1
        },
        "mooncake":
        {
            "ip": "127.0.0.1",
            "port": 50001,
            "type": "Mooncake",
            "modelname": "qwen2.5",
            "lora_id": -1
        }
    },
    "http_server_port": 13333
}
```
- "kvevent_instance": Services capable of reporting KV events.
  - **_All ports for prefill instances and decode instances must be filled in._**
- "vllm-1": Configuration of a VLLM node. For example,
  - "ip": The IP address of the current node used to communicate with the conductor server.
  - "port": The port of the current node used to communicate with the conductor server.
  - "type": Source of the current node's KV event.
  - "modelname": Model name used for current service-oriented applications.
  - "lora_id": LoRA Adapter ID.

- "http_server_port": The IP port of the conductor used to communicate with the proxy.



## Run Example

### 1. Start the mooncake_master server

```
mooncake_master --port 50001

```
### 2. Run multiple vllm instances
```
# kv_producer role
 vllm serve /qwen2.5_7B_instruct/  \
    --enforce-eager \
    --max-model-len 10000 \
    --port 5800 \
    --gpu-memory-utilization 0.8 \
    --served-model-name "qwen2.5_7B" \
    --trust-remote-code \
    --kv-events-config \
    '{
        "publisher": "zmq", 
        "enable_kv_cache_events": true, 
        "endpoint": "tcp://*:3337",
        "topic": "kv-events",
        "replay_endpoint": "tcp://*:3338"
    }' \
    --kv-transfer-config \
    '{
        "kv_connector": "MooncakeConnectorStoreV1",
        "kv_role":"kv_producer",
        "kv_connector_extra_config":{"use_layerwise": false}
    }'
```

```
# kv_consumer role
 vllm serve /qwen2.5_7B_instruct/  \
    --enforce-eager \
    --max-model-len 10000 \
    --port 5900 \
    --gpu-memory-utilization 0.8 \
    --served-model-name "qwen2.5_7B" \
    --trust-remote-code \
    --kv-events-config \
    '{
        "publisher": "zmq", 
        "enable_kv_cache_events": true, 
        "endpoint": "tcp://*:3337",
        "topic": "kv-events",
        "replay_endpoint": "tcp://*:3338"
    }' \
    --kv-transfer-config \
    '{
        "kv_connector": "MooncakeConnectorStoreV1",
        "kv_role":"kv_consumer",
        "kv_connector_extra_config":{"use_layerwise": false}
    }'

```


### 3. Start the conductor server

```
export CONDUCTOR_CONFIG_PATH="./example/conductor_config.json"
mooncake_conductor --port 55001
```

We implement this simple disagg_proxy based on round-robin as a demo. In the production stage, service providers and users can also implement corresponding global proxy strategies according to their needs.


### 4. Run the proxy in the example

```
python cacheaware_disaggregated_proxy.py --prefiller-hosts 155.55.55.55 --prefiller-ports 5555 --decoder-host 166.66.66.66 --decoder-ports 6666 --conductor-address 133.33.33.33:3333
```


## Test with openai compatible request

```
curl -s http://localhost:8000/v1/completions -H "Content-Type: application/json" -d '{
  "model": "Qwen/Qwen2.5-7B",
  "prompt": "What are the key architectural differences between vLLM and Mooncake when it comes to handling key-value (KV) cache events, and how can a centralized conductor component be designed in Go to normalize disparate event schemas from these systems, apply consistent metrics collection, and make dynamic scheduling decisions based on real-time KV cache hit rates without relying on Kubernetes-based autoscaling mechanisms?",
  "max_tokens": 1000
}'

```
