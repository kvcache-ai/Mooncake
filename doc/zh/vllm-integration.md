# vLLM 分布式预填充/解码分离演示

## 概述
目前，我们基于 [PR 8498](https://github.com/vllm-project/vllm/pull/8498) 支持了 mooncake-transfer-engine 与 vLLM 项目的集成（vllm 版本：v0.6.2），以加速跨节点的分布式预填充/解码分离场景下的 KVCache 传输（[基准测试结果](../en/vllm-benchmark-results.md)）。未来，我们将不依赖 PR 8498 进行开发，发布一个分布式的 KVStore，并将其与 vLLM 的前缀缓存功能进行集成，以支持多实例间的 KVCache 共享。

![vllm-integration-demo](../../image/vllm-integration-demo.gif)

## 安装
### 准备工作
请先参照 [编译指南](build.md) 安装 Mooncake Transfer Engine。

### 安装特定版本vLLM
#### 1. 从指定的仓库克隆 vLLM
```bash
git clone git@github.com:kvcache-ai/vllm.git
```
#### 2. 编译与安装（从如下两种可选方式中选择一种）
##### 2.1 从源码构建 vLLM (仅Python部分)
```bash
cd vllm
git checkout mooncake-integration
pip3 uninstall vllm -y
pip3 install vllm==0.6.2
pip3 uninstall torchvision -y
python3 python_only_dev.py
```
 - **一旦完成测试或想要安装另一个版本的 vLLM，您应该先使用 `python python_only_dev.py --quit-dev` 退出开发环境。**

##### 2.2 从源码编译构建（包括C++及CUDA代码）
```bash
cd vllm
git checkout mooncake-integration
pip3 uninstall vllm -y
pip3 install -e .
```
 - **如果编译失败，请尝试通过如下命令 `pip3 install cmake --upgrade` 升级您的cmake版本。**
 - 如果遇到任何无法解决的问题，请参照[vLLM官方的编译指南](https://docs.vllm.ai/en/v0.6.4.post1/getting_started/installation.html#install-the-latest-code)。

## 配置
### 使用 RDMA 运行示例所需配置文件

- 为预填充和解码实例准备一个 mooncake.json 文件
- **在解码实例侧，你无须更改配置文件里的`prefill_url` 与 `decode_url`，使用完同相同的配置文件即可。**

```json
{
  "prefill_url": "192.168.0.137:13003",
  "decode_url": "192.168.0.139:13003",
  "metadata_server": "192.168.0.139:2379",
  "protocol": "rdma",
  "device_name": "erdma_0"
}
```
- "prefill_url": 预填充节点的 IP 地址和端口。
  - URL 中的端口用于与 etcd 服务器通信以获取元数据。
- "decode_url": 解码节点的 IP 地址和端口。
  - URL 中的端口用于与 etcd 服务器通信以获取元数据。
- "metadata_server": mooncake 传输引擎的 etcd 服务器。
- "protocol": 数据传输协议("rdma/tcp")。
- "device_name": 用于数据传输的设备，当 "protocol" 设置为 "rdma" 时必填。如果使用多个 NIC 设备，它们可以用逗号分隔，如 "erdma_0,erdma_1"。请注意它们之间没有空格。


### 使用 TCP 运行示例所需配置文件

- 为预填充和解码实例准备一个 mooncake.json 文件
```json
{
  "prefill_url": "192.168.0.137:13003",
  "decode_url": "192.168.0.139:13003",
  "metadata_server": "192.168.0.139:2379",
  "protocol": "tcp",
  "device_name": ""
}
```


## 运行示例
 - 请根据您的环境更改以下指南中的 IP 地址和端口。
```bash
# Begin from `root` of your cloned repo!

# 1. 启动 etcd 服务器
etcd --listen-client-urls http://0.0.0.0:2379 --advertise-client-urls http://localhost:2379
# 在运行上述命令之前，您可能需要终止其他 etcd 进程

# 2. 在预填充侧运行（KVCache生产者）
VLLM_HOST_IP="192.168.0.137" VLLM_PORT="51000" MASTER_ADDR="192.168.0.137" MASTER_PORT="54324" MOONCAKE_CONFIG_PATH=./mooncake.json VLLM_DISTRIBUTED_KV_ROLE=producer VLLM_USE_MODELSCOPE=True python3 -m vllm.entrypoints.openai.api_server --model Qwen/Qwen2.5-7B-Instruct-GPTQ-Int4 --port 8100 --max-model-len 10000 --gpu-memory-utilization 0.95

# 3. 在解码侧运行（KVCache消费者）
VLLM_HOST_IP="192.168.0.137" VLLM_PORT="51000" MASTER_ADDR="192.168.0.137" MASTER_PORT="54324" MOONCAKE_CONFIG_PATH=./mooncake.json VLLM_DISTRIBUTED_KV_ROLE=consumer VLLM_USE_MODELSCOPE=True python3 -m vllm.entrypoints.openai.api_server --model Qwen/Qwen2.5-7B-Instruct-GPTQ-Int4 --port 8200 --max-model-len 10000 --gpu-memory-utilization 0.95
```

- `VLLM_HOST_IP` 和 `VLLM_PORT` 用于 vLLM 在分布式环境中的内部通信。
- `MASTER_ADDR` 和 `MASTER_PORT` 用于指定分布式环境中主节点的 IP 地址和端口。
  - **_务必在每个节点上设置相同的 `MASTER_ADDR` 和 `MASTER_PORT` （可以是预填充实例 IP 或解码实例 IP）。_**
- `MOONCAKE_CONFIG_PATH` 是 mooncake.json 配置文件的路径。
- `VLLM_DISTRIBUTED_KV_ROLE` 是节点的角色，可以是 'producer' 或 'consumer'。
- `VLLM_USE_MODELSCOPE` 是可选的，如果您的网络可以访问 huggingface，请去掉这个选项。
- `--model` 参数指定要使用的模型。
- `--port` 参数指定 vllm 服务监听的端口。
- `--max-model-len` 参数指定模型的最大长度。
- 目前，暂不支持通过 `--tensor_parallel_size` \ `-tp` 配置多卡分布式推理，因为 `disagg_group` 的初始化过程与 vllm 的 `process_group` 存在冲突。这个问题将在后续迭代中进行解决，相关PR (https://github.com/vllm-project/vllm/pull/10072) 和 (https://github.com/vllm-project/vllm/pull/10275)也可以有效解决上述问题。敬请期待。
```bash
# 4. 在一个节点上启动代理服务器（我们以预填充节点为例）
python3 proxy_server.py
```
`proxy_server.py` 的实现
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

**_请确保修改代码中的 IP 地址。_**


## 测试
```
curl -s http://localhost:8000/v1/completions -H "Content-Type: application/json" -d '{
  "model": "Qwen/Qwen2.5-7B-Instruct-GPTQ-Int4",
  "prompt": "San Francisco is a",
  "max_tokens": 1000
}'
```
- 如果您不是在代理服务器上进行测试，请将 localhost 更改为代理服务器的 IP 地址。
