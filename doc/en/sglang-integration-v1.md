# SGLang Disaggregated Serving with MooncakeTransferEngine

## Overview
This is the latest version of the MooncakeTransferEngine integration doc with the SGLang project based on [PR 4654](https://github.com/sgl-project/sglang/pull/4654) and [PR 4880](https://github.com/sgl-project/sglang/pull/4880) to support KVCache transfer for intra-node and inter-node disaggregated serving scenario.


**_Please note that this is still an experimental version and will be modified anytime based on feedback from the SGLang community._**

## Installation
### Prerequisite
```bash
pip3 install mooncake-transfer-engine
```

Note: If you encounter problems such as missing `lib*.so`, you should uninstall this package by `pip3 uninstall mooncake-transfer-engine`, and build the binaries manually according to the [instructions](build.md).

### Install the latest version of SGLang
#### 1. Clone SGLang from official repo
```bash
git clone git@github.com:sgl-project/sglang.git
```
#### 2. Build
##### 2.1 Build from source
```bash
cd sglang
pip3 install -e "python[all]"
```
 - If you encounter any problems that you cannot solve, please refer to the [SGLang official compilation guide](https://docs.sglang.ai/start/install.html).

## Configuration
### To run prefill instance and decode instance on the same node
Prepare a mooncake.json file:
```json
{
    "local_hostname": "192.168.0.137",
    "metadata_server": "http://192.168.0.137:8998/metadata",
    "protocol": "rdma",
    "device_name": "erdma_0,erdma_1"
}
```
- "local_hostname": The IP address of the current node used to communicate with the metadata server. This param will be deprecated in the future, we want to make the mooncake config as simple as possible.
  - **_All prefill instances and decode instances can share this config file on the same node._**
- "metadata_server": The metadata server of the mooncake transfer engine. For example,
  - Use `http` as backend: `"http://192.168.0.137:8998/metadata"`. Currently, we reuse bootstrap server for metadata server, but it will be deprecated in the future.
  - An external server is OK as well:
    - Use `etcd` as backend: `"192.168.0.137:2379"`, `"etcd://192.168.0.137:2379"` or `"etcd://192.168.0.137:2379,192.168.0.138:2379"`
    - Use `redis` as backend: `"redis://192.168.0.137:6379"`
  - P2P solution is on design now. We want to remove the requirement of the metadata server as well.
- "protocol": The protocol to be used for data transmission. ("rdma/tcp")
- "device_name": The device to be used for data transmission, it is required when "protocol" is set to "rdma". If multiple NIC devices are used, they can be separated by commas such as "erdma_0,erdma_1". Please note that there are no spaces between them.

Prefill: 
```bash
MOONCAKE_CONFIG_PATH=./mooncake.json python -m sglang.launch_server --model-path Qwen/Qwen2.5-7B-Instruct-GPTQ-Int4 --disaggregation-mode prefill --port 30000 --host 192.168.0.137 --tp-size 2
```
Decode:
```bash
MOONCAKE_CONFIG_PATH=./mooncake.json python -m sglang.launch_server --model-path Qwen/Qwen2.5-7B-Instruct-GPTQ-Int4 --disaggregation-mode decode --port 30001 --base-gpu-id 2 --host 192.168.0.137 --tp-size 2
```

Proxy:
```bash
python3 -m sglang.srt.disaggregation.mini_lb --prefill http://192.168.0.137:30000 --decode http://192.168.0.137:30001 --host 0.0.0.0 --port 8000
```

Test:
```bash
curl -X POST http://127.0.0.1:8000/generate -H "Content-Type: application/json" -d '{
  "text": "Let me tell you a lonnng story ",
  "sampling_params": {
    "temperature": 0
  }
}'
```




### To run prefill instance and decode instance on different node
Prepare a mooncake.json file for each node:
```json
{
    "local_hostname": "192.168.0.137",
    "metadata_server": "http://192.168.0.137:8998/metadata",
    "protocol": "rdma",
    "device_name": "erdma_0,erdma_1"
}
```

```json
{
    "local_hostname": "192.168.0.140",
    "metadata_server": "http://192.168.0.137:8998/metadata",
    "protocol": "rdma",
    "device_name": "erdma_0,erdma_1"
}
```

Prefill: 
```bash
MOONCAKE_CONFIG_PATH=./mooncake.json python -m sglang.launch_server --model-path Qwen/Qwen2.5-7B-Instruct-GPTQ-Int4 --disaggregation-mode prefill --port 30000 --host 192.168.0.137 --tp-size 2
```
Decode:
```bash
MOONCAKE_CONFIG_PATH=./mooncake.json python -m sglang.launch_server --model-path Qwen/Qwen2.5-7B-Instruct-GPTQ-Int4 --disaggregation-mode decode --port 30001 --host 192.168.0.140 --tp-size 2
```

Proxy:
```bash
python3 -m sglang.srt.disaggregation.mini_lb --prefill http://192.168.0.137:30000 --decode http://192.168.0.140:30001 --host 0.0.0.0 --port 8000
```

Test:
```bash
curl -X POST http://127.0.0.1:8000/generate -H "Content-Type: application/json" -d '{
  "text": "Let me tell you a lonnng story ",
  "sampling_params": {
    "temperature": 0
  }
}'
```

Note:
 - TP is supported but not required, you can remove `--tp-size 2` if you want.
 - You should start prefill instances first since the metadata server are integrated with prefill node. 
 - XpYd is supported. It is ok to run one prefill and one decode instance on the same node, however, multiple prefill instances or multiple decode instances on the same node are not supported due to port conflict. We will handle it in the next PR if this is a popular request, or you can use docker to avoid port conflict.
   - e.g., `python3 -m sglang.srt.disaggregation.mini_lb --prefill http://192.168.0.137:30000,http://192.168.0.140:30000 --decode http://192.168.0.137:30001,http://192.168.0.140:30001 --host 0.0.0.0 --port 8000`
 - HuggingFace timeout can be addressed by `export SGLANG_USE_MODELSCOPE=true`