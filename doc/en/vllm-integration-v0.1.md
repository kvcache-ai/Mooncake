# vLLM Disaggregated Prefill/Decode Demo

## Overview
Currently, we support mooncake-transfer-engine integration with the vLLM project based on [PR 8498](https://github.com/vllm-project/vllm/pull/8498) (vllm version: v0.6.2) to accelerate KVCache transfer for inter-node disaggregated Prefill/Decode scenario ([Benchmark results](vllm-benchmark-results-v0.1.md)). In the future, we will bypass PR 8498, release a disaggregated KVStore, and fully integrate it with the vLLM Prefix Caching feature to support multi-instance KVCache Sharing.

![vllm-integration-demo](../../image/vllm-integration-demo.gif)

## Installation
### Prerequisite
Please install the Mooncake Transfer Engine according to the [instructions](build.md) first.

### Install an experimental version of vLLM
#### 1. Clone vLLM from an indicated repo
```bash
git clone git@github.com:kvcache-ai/vllm.git
```
#### 2. Build (Choose one of the following options)
##### 2.1 vLLM Python-only build (without compilation)
```bash
cd vllm
git checkout mooncake-integration
pip3 uninstall vllm -y
pip3 install vllm==0.6.2
pip3 uninstall torchvision -y
python3 python_only_dev.py
```
 - **Once you have finished editing or want to install another vLLM wheel, you should exit the development environment using `python3 python_only_dev.py --quit-dev`**

##### 2.2 Build from source (Include C++ and CUDA code)
```bash
cd vllm
git checkout mooncake-integration
pip3 uninstall vllm -y
pip3 install -e .
```
 - **If the build fails, try upgrading the version of cmake through `pip3 install cmake --upgrade`.**
 - If you encounter any problems that you cannot solve, please refer to the [vLLM official compilation guide](https://docs.vllm.ai/en/v0.6.4.post1/getting_started/installation.html#install-the-latest-code).

## Configuration
### Prepare configuration file to Run Example over RDMA

- Prepare a _**mooncake.json**_ file for both Prefill and Decode instances
- **You don't need to change the `prefill_url` and `decode_url` of the config file in the decode side, please use the identical config file.**

```json
{
  "prefill_url": "192.168.0.137:13003",
  "decode_url": "192.168.0.139:13003",
  "metadata_server": "192.168.0.139:2379",
  "protocol": "rdma",
  "device_name": "erdma_0"
}
```
- "prefill_url": The IP address and port of the Prefill node.
  - The port in the URL is used to communicate with etcd server for metadata.
- "decode_url": The IP address and port of the Decode node.
  - The port in the URL is used to communicate with etcd server for metadata.
- "metadata_server": The etcd server of mooncake transfer engine.
- "protocol": The protocol to be used for data transmission. ("rdma/tcp")
- "device_name": The device to be used for data transmission, required when "protocol" is set to "rdma". If multiple NIC devices are used, they can be separated by commas such as "erdma_0,erdma_1". Please note that there are no spaces between them.


### Prepare configuration file to Run Example over TCP

- Prepare a _**mooncake.json**_ file for both Prefill and Decode instances
```json
{
  "prefill_url": "192.168.0.137:13003",
  "decode_url": "192.168.0.139:13003",
  "metadata_server": "192.168.0.139:2379",
  "protocol": "tcp",
  "device_name": ""
}
```


## Run Example
 - Please change the IP addresses and ports in the following guide according to your env.
```bash
# Begin from `root` of your cloned repo!

# 1. Start the etcd server
etcd --listen-client-urls http://0.0.0.0:2379 --advertise-client-urls http://localhost:2379
# You may need to terminate other etcd processes before running the above command

# 2. Run on the prefilling side (producer role)
VLLM_HOST_IP="192.168.0.137" VLLM_PORT="51000" MASTER_ADDR="192.168.0.137" MASTER_PORT="54324" MOONCAKE_CONFIG_PATH=./mooncake.json VLLM_DISTRIBUTED_KV_ROLE=producer VLLM_USE_MODELSCOPE=True python3 -m vllm.entrypoints.openai.api_server --model Qwen/Qwen2.5-7B-Instruct-GPTQ-Int4 --port 8100 --max-model-len 10000 --gpu-memory-utilization 0.95

# 3. Run on the decoding side (consumer role)
VLLM_HOST_IP="192.168.0.137" VLLM_PORT="51000" MASTER_ADDR="192.168.0.137" MASTER_PORT="54324" MOONCAKE_CONFIG_PATH=./mooncake.json VLLM_DISTRIBUTED_KV_ROLE=consumer VLLM_USE_MODELSCOPE=True python3 -m vllm.entrypoints.openai.api_server --model Qwen/Qwen2.5-7B-Instruct-GPTQ-Int4 --port 8200 --max-model-len 10000 --gpu-memory-utilization 0.95
```

- `VLLM_HOST_IP` and `VLLM_PORT` are used for vLLM's internal communication in distributed environments.
- `MASTER_ADDR` and `MASTER_PORT` are used to specify the IP address and port of the master node in a distributed setup.
  - **_Be sure to set up the same `MASTER_ADDR` and same `MASTER_PORT` on each node (either prefill instance IP or decode instance IP is ok)._**
- `MOONCAKE_CONFIG_PATH` is the path to the mooncake.json configuration file.
- `VLLM_DISTRIBUTED_KV_ROLE` is the node's role, either 'producer' or 'consumer'.
- `VLLM_USE_MODELSCOPE` is optional, if you have access to huggingface, please remove it.
- The `--model` parameter specifies the model to use.
- The `--port` parameter specifies the vllm service port on which to listen.
- The `--max-model-len` parameter specifies the maximum length of the model.
- Currently, option `--tensor_parallel_size` \ `-tp` is not supported for inter-node disaggregated scenario due to the `parallel_state` initialization process of `disagg_group` in conflict with vllm's `process_group`. This issue will be addressed in the next patch with the help of (https://github.com/vllm-project/vllm/pull/10072) and (https://github.com/vllm-project/vllm/pull/10275). Stay tuned.
```bash
# 4. Start the proxy server on one node (Let's take the prefill node as an example)
python3 proxy_server.py
```
The implementation of `proxy_server.py`
```python
import os

import aiohttp
from quart import Quart, make_response, request

AIOHTTP_TIMEOUT = aiohttp.ClientTimeout(total=6 * 60 * 60)

app = Quart(__name__)


async def forward_request(url, data):
    async with aiohttp.ClientSession(timeout=AIOHTTP_TIMEOUT) as session:
        headers = {
            "Authorization": f"Bearer {os.environ.get('OPENAI_API_KEY')}"
        }
        async with session.post(url=url, json=data,
                                headers=headers) as response:
            if response.status == 200:
                if True:
                    async for chunk_bytes in response.content.iter_chunked(
                            1024):
                        yield chunk_bytes
                else:
                    content = await response.read()
                    yield content


@app.route('/v1/completions', methods=['POST'])
async def handle_request():
    try:
        original_request_data = await request.get_json()

        prefill_request = original_request_data.copy()
        # change max_tokens = 1 to let it only do prefill
        prefill_request['max_tokens'] = 1

        # finish prefill
        async for _ in forward_request('http://localhost:8100/v1/completions',
                                       prefill_request):
            continue

        # return decode
        generator = forward_request('http://192.168.0.139:8200/v1/completions', # Be sure to change the IP address for your machine
                                    original_request_data)
        response = await make_response(generator)
        response.timeout = None

        return response

    except Exception as e:
        import sys
        import traceback
        exc_info = sys.exc_info()
        print("Error occurred in disagg prefill proxy server")
        print(e)
        print("".join(traceback.format_exception(*exc_info)))


if __name__ == '__main__':
    app.run(host="0.0.0.0",port=8000)
```

**_Be sure to change the IP address in the code._**


## Test with openai compatible request
```
curl -s http://localhost:8000/v1/completions -H "Content-Type: application/json" -d '{
  "model": "Qwen/Qwen2.5-7B-Instruct-GPTQ-Int4",
  "prompt": "San Francisco is a",
  "max_tokens": 1000
}'
```
- If you are not testing on the proxy server, please change the `localhost` to the IP address of the proxy server.
