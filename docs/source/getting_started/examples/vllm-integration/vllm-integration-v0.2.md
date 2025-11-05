# vLLM V0 Disaggregated Serving Demo

## Overview
This is the latest version of mooncake-transfer-engine integration doc with the vLLM project based on [PR 10502](https://github.com/vllm-project/vllm/pull/10502) and [PR 10884](https://github.com/vllm-project/vllm/pull/10884) (vllm version: v0.6.4.post1/main) to accelerate KVCache transfer for inter-node disaggregated serving scenario. We have run some experiments to obtain some [preview benchmark results](../../../performance/vllm-benchmark-results-v0.2.md). More benchmark results will be released in due time.

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
  - For vLLM version <= v0.8.4, you must build from source since the earlier mooncake_vllm_adaptor interface is not contained in the pip wheel and will be deprecated in near future.

### Install the latest version of vLLM
#### 1. Clone vLLM from official repo
```bash
git clone git@github.com:vllm-project/vllm.git
```
#### 2. Build
##### 2.1 Build from source (Include C++ and CUDA code)
```bash
cd vllm
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
  "metadata_backend": "etcd",
  "protocol": "rdma",
  "device_name": "erdma_0"
}
```
- "prefill_url": The IP address and port of the Prefill node.
  - The port in the URL is used to communicate with metadata server.
- "decode_url": The IP address and port of the Decode node.
  - The port in the URL is used to communicate with metadata server.
  - **_If you want to run the prefill instance and decode instance on the same node, please set up a different port for the `decode_url`. To avoid port conflicts, ensure that the port number differs by at least 50 from the port number in `prefill_url`. For example, "decode_url": "192.168.0.137:13103". Please note that if you set up the same URL for both instances, we will automatically add 100 to the port of the `decode_url`._**
- "metadata_server": The metadata server of the mooncake transfer engine. For examples,
  - Use `etcd` as backend: `"192.168.0.137:2379"`, `"etcd://192.168.0.137:2379"` or `"etcd://192.168.0.137:2379,192.168.0.138:2379"`
  - Use `redis` as backend: `"redis://192.168.0.137:6379"`
  - Use `http` as backend: `"http://192.168.0.137:8080/metadata"`
- "metadata_backend": Currently we support "etcd", "redis", and "http" backends. If this parameter is absent and the `metadata_server` is not a complete URL with the backend prefix, the mooncake transfer engine will use "etcd" automatically. Please note that this parameter will be deprecated in the next version, we recommend you provide a complete URL for `metadata_server` with the prefix of the target backend.
- "protocol": The protocol to be used for data transmission. ("rdma/tcp")
- "device_name": The device to be used for data transmission, it is required when "protocol" is set to "rdma". If multiple NIC devices are used, they can be separated by commas such as "erdma_0,erdma_1". Please note that there are no spaces between them.

### Prepare configuration file to Run Example over TCP

- Prepare a _**mooncake.json**_ file for both Prefill and Decode instances
```json
{
  "prefill_url": "192.168.0.137:13003",
  "decode_url": "192.168.0.139:13003",
  "metadata_server": "192.168.0.139:2379",
  "metadata_backend": "etcd",
  "protocol": "tcp",
  "device_name": ""
}
```

Note: we will support auto-detect in the next version when the `protocol` is absent in the config file.

## Run Example
 - Please change the IP addresses and ports in the following guide according to your env.
```bash
# Begin from `root` of your cloned repo!

# 1. Start the etcd server
etcd --listen-client-urls http://0.0.0.0:2379 --advertise-client-urls http://localhost:2379
# You may need to terminate other etcd processes before running the above command

# 2. Run on the prefilling side (producer role)
MOONCAKE_CONFIG_PATH=./mooncake.json VLLM_USE_MODELSCOPE=True python3 -m vllm.entrypoints.openai.api_server --model Qwen/Qwen2.5-7B-Instruct-GPTQ-Int4 --port 8100 --max-model-len 10000 --gpu-memory-utilization 0.8 --kv-transfer-config '{"kv_connector":"MooncakeConnector","kv_role":"kv_producer","kv_rank":0,"kv_parallel_size":2,"kv_buffer_size":2e9}'

# 3. Run on the decoding side (consumer role)
MOONCAKE_CONFIG_PATH=./mooncake.json VLLM_USE_MODELSCOPE=True python3 -m vllm.entrypoints.openai.api_server --model Qwen/Qwen2.5-7B-Instruct-GPTQ-Int4 --port 8200 --max-model-len 10000 --gpu-memory-utilization 0.8 --kv-transfer-config '{"kv_connector":"MooncakeConnector","kv_role":"kv_consumer","kv_rank":1,"kv_parallel_size":2,"kv_buffer_size":2e9}'
```

- `MOONCAKE_CONFIG_PATH` is the path to the mooncake.json configuration file.
- `VLLM_USE_MODELSCOPE` is optional, if you have access to huggingface, please remove it.
- The `--model` parameter specifies the model to use.
- The `--port` parameter specifies the vllm service port on which to listen.
- The `--max-model-len` parameter specifies the maximum length of the model.
- Option `--tensor_parallel_size` \ `-tp` is supported now. Example: append `-tp 2` to the run command to run vllm with multiple GPUs.
  - If you want to run the prefill instance and decode instance on the same node, please set up different `CUDA_VISIBLE_DEVICES`. For example, `CUDA_VISIBLE_DEVICES=0,1` for the prefill instance and `CUDA_VISIBLE_DEVICES=2,3` for the decode instance.

- The `--kv-transfer-config` parameter specifies the connector and its config to be used.
  - Please set up `kv_connector` to `MooncakeConnector`.
  - `kv_role` is the node's role, either 'kv_producer' or 'kv_consumer'.
  - `kv_rank` is the rank of the instance. Currently, `kv_producer`'s rank is 0, `kv_consumer`'s rank is 1.
  - `kv_parallel_size` is fixed to 2 currently.
  - `kv_buffer_size` is the size of the KVCache lookup buffer, if the average `input_len` of the prompt is large, please increase the buffer size. If the OOM still occurs, please decrease the ratio of `--gpu-memory-utilization`.
  - `kv_ip` and `kv_port` are used to specify the IP address and port of the master node for "PyNcclConnector" distributed setup. It is not used for "MooncakeConnector" currently. Instead, "MooncakeConnector" uses a config file to set up the distributed connection. Therefore, you don't need to set these params for "MooncakeConnector" currently.

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
