# vLLM 分布式预填充与 MooncakeStore 集成

## 概述

这是基于 [PR 10502](https://github.com/vllm-project/vllm/pull/10502) 和 [PR 12957](https://github.com/vllm-project/vllm/pull/12957) 的 MooncakeStore 集成文档的最新版本，支持在节点内部和节点之间进行 KVCache 的传输，适用于分布式预填充/解码场景。基准测试结果将很快发布。

从 v0.x 到 v1 的主要变化：

* XpYd 支持与调度
  * 动态调整预填充组和解码组的规模
* 更加稳定且容错性更强
  * 单个 vllm 实例的突然崩溃是可以容忍的
  * 由于实例之间的连接被移除，每个实例都可以作为普通的 vllm 实例工作，意味着它可以处理非代理请求并正常完成

***请注意，这仍然是一个实验版本，未来可能根据 vLLM 社区的反馈进行修改。***

## 安装

### 前提条件

请首先按照 [安装指南](build.md) 安装 MooncakeStore。

### 安装最新版本的 vLLM 和 Mooncake 包

#### 1. 克隆 vLLM 官方仓库

```bash
git clone git@github.com:vllm-project/vllm.git
```

#### 2. 安装 Mooncake 包

```bash
pip install mooncake-transfer-engine
```

#### 3. 构建

##### 3.1 从源代码构建

```bash
cd vllm
pip3 install -e .
```

* 如果遇到无法解决的问题，请参考 [vLLM 官方编译指南](https://docs.vllm.ai/en/latest/getting_started/installation/index.html)。

## 配置

### 准备配置文件以在 RDMA 上运行的示例

* 为预填充和解码实例准备一个 ***mooncake.json*** 文件

```json
{
    "local_hostname": "192.168.0.137",
    "metadata_server": "etcd://192.168.0.137:2379",
    "protocol": "rdma",
    "device_name": "erdma_0",
    "master_server_address": "192.168.0.137:50001"
}
```

* "local\_hostname": 当前节点的 IP 地址，用于与 etcd 服务器通信。
  * ***所有预填充实例和解码实例可以在同一节点共享此配置文件。***
* "metadata\_server": Mooncake 传输引擎的 etcd 服务器。例如：
  * 使用 `etcd` 作为后端：`"192.168.0.137:2379"`，`"etcd://192.168.0.137:2379"` 或 `"etcd://192.168.0.137:2379,192.168.0.138:2379"`
  * 使用 `redis` 作为后端：`"redis://192.168.0.137:6379"`
  * 使用 `http` 作为后端：`"http://192.168.0.137:8080/metadata"`
* "protocol": 数据传输使用的协议（"rdma/tcp"）。
* "device\_name": 数据传输使用的设备，仅在 "protocol" 设置为 "rdma" 时需要。如果使用多个网卡设备，可以通过逗号分隔，例如 `"erdma_0,erdma_1"`，请注意设备之间没有空格。
* "master\_server\_address": MooncakeStore 的Master进程的 IP 地址和端口。

### 准备配置文件以在 TCP 上运行的示例

* 为预填充和解码实例准备一个 ***mooncake.json*** 文件

```json
{
    "local_hostname": "192.168.0.137",
    "metadata_server": "etcd://192.168.0.137:2379",
    "protocol": "tcp",
    "device_name": "",
    "master_server_address": "192.168.0.137:50001"
}
```

### 创建Master启动脚本

```bash
touch master.py
```

将以下内容复制到 `master.py` 文件中：

```python
import os
import subprocess
import argparse
from importlib.resources import files
from mooncake import *

parser = argparse.ArgumentParser(description="Start Mooncake Master")
parser.add_argument("--port", type=int, help="The port number to use", default=50051)
parser.add_argument("--max_threads", type=int, help="Maximum number of threads to use", default=4)
parser.add_argument("--enable_gc", action="store_true", help="Enable garbage collection", default=False)

args = parser.parse_args()

bin_path = files("mooncake") / "mooncake_master"
print("bin path:", bin_path)

os.chmod(bin_path, 0o755)

command = [bin_path, "--port", str(args.port), "--max_threads", str(args.max_threads)]

if args.enable_gc:
    command.append("--enable_gc")

result = subprocess.run(command)
```

## 运行示例

* 请根据您的环境更改以下命令中的 IP 地址和端口。

```bash
# Begin from `root` of your cloned repo!

# 1. Start the etcd server
etcd --listen-client-urls http://0.0.0.0:2379 --advertise-client-urls http://localhost:2379
# You may need to terminate other etcd processes before running the above command

# 2. Start the mooncake_master server
python master.py --port 50001
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

* `MOONCAKE_CONFIG_PATH` 是 mooncake.json 配置文件的路径。
* `VLLM_USE_MODELSCOPE` 是可选的，如果你有访问 Huggingface 的权限，可以将其去除。
* `VLLM_USE_V1=0` 是必需的，因为目前分布式功能仅支持 V0 vLLM。

  * 你也可以将该配置 `export` 到环境变量中，而无需在每个命令前都加上它。
* `--model` 参数指定使用的模型。
* `--port` 参数指定 vllm 服务监听的端口。
* `--max-model-len` 参数指定模型的最大长度。
* 支持 `--tensor_parallel_size` 或 `-tp` 选项。例如，附加 `-tp 2` 到运行命令中，以便使用多个 GPU 运行 vllm。

  * 注意：所有实例应该使用相同的 `tensor_parallel_size`。
  * 如果你想在同一个节点上运行预填充实例和解码实例，请设置不同的 `CUDA_VISIBLE_DEVICES`。例如，`CUDA_VISIBLE_DEVICES=0,1` 用于预填充实例，`CUDA_VISIBLE_DEVICES=2,3` 用于解码实例。
* `--kv-transfer-config` 参数指定要使用的连接器及其配置。

  * 请设置 `kv_connector` 为 `MooncakeStoreConnector`。
  * `kv_role` 是节点的角色，可以是 'kv\_producer'、'kv\_consumer' 或 'kv\_both'。

```bash
# 4. Start the proxy server
cd vllm
python3 examples/online_serving/disagg_examples/disagg_proxy_demo.py --model Qwen/Qwen2.5-7B-Instruct-GPTQ-Int4 --prefill localhost:8100 localhost:8101  --decode localhost:8200 localhost:8201  --port 8000
```

* `--model` 参数指定使用的模型，也指定代理服务器使用的分词器。
* `--port` 参数指定 vllm 服务的端口。
* `--prefill` 或 `-p` 指定 vllm 预填充实例的 IP 和端口。
* `--decode` 或 `-d` 指定 vllm 解码实例的 IP 和端口。

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

Mooncake 团队实现了一个基于轮询策略的简单 `disagg_proxy` 示例。在生产环境中，服务提供商和用户也可以根据需求实现相应的全局代理策略。

***请务必根据实际情况更改命令中的 IP 地址。***

## 测试 OpenAI 兼容的请求

```bash
curl -s http://localhost:8000/v1/completions -H "Content-Type: application/json" -d '{
  "model": "Qwen/Qwen2.5-7B-Instruct-GPTQ-Int4",
  "prompt": "San Francisco is a",
  "max_tokens": 1000
}'
```

* 如果您没有在代理服务器上进行测试，请将 `localhost` 更改为代理服务器的 IP 地址。
