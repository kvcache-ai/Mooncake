# vLLM 解耦式服务与 MooncakeStore 集成

## 概述
本文档是基于 [PR 10502](https://github.com/vllm-project/vllm/pull/10502) 和 [PR 12957](https://github.com/vllm-project/vllm/pull/12957) 的最新版 MooncakeStore 集成文档，支持节点内及跨节点的 KVCache 传输，用于解耦式服务场景。基准测试结果将很快发布。

v0.x 至 v1 主要变更：
- XpYd 支持与编排
  - 支持动态调整预填充组和解码组的规模
- 更好的稳定性与容错性
  - 单 vLLM 实例崩溃不影响整体服务
  - 由于移除了实例间直连，每个实例都可作为普通的 vLLM 实例独立工作，可正常处理非代理请求

**_请注意当前仍为实验版本，可能根据 vLLM 社区反馈随时调整。_**
- **更新（2025年4月10日）**：我们正在进行 vLLM v1 版本集成工作，敬请期待。

## 安装
### 准备工作
```bash
pip3 install mooncake-transfer-engine
```

注意事项：
  - 如遇缺失 `lib*.so` 等库问题，请先执行 `pip3 uninstall mooncake-transfer-engine` 卸载，然后按照[编译指南](build.md)手动编译。
  - 对于 vLLM <= v0.8.4，请使用mooncake-transfer-engine <= v0.3.3.post2，`mooncake_vllm_adaptor`接口已被废弃。

### 安装最新版 vLLM
#### 1. 克隆官方仓库
```bash
git clone git@github.com:vllm-project/vllm.git
```
#### 2. 编译安装
##### 2.1 源码编译
```bash
cd vllm
pip3 install -e .
```
  - 如遇编译问题，请参考[vLLM 官方编译指南](https://docs.vllm.ai/en/latest/getting_started/installation/index.html)。

## 配置
### 使用 RDMA 运行示例所需配置文件

- 为预填充和解码实例准备一个 _**mooncake.json**_ 文件

```json
{
    "local_hostname": "192.168.0.137",
    "metadata_server": "etcd://192.168.0.137:2379",
    "protocol": "rdma",
    "device_name": "erdma_0",
    "master_server_address": "192.168.0.137:50001"
}
```
- "local_hostname": 当前节点用于连接元数据服务器的 IP 地址
  - **_同一节点上的所有预填充实例和解码实例可共享此配置文件。_**
- "metadata_server": Mooncake 传输引擎的元数据服务器地址，例如：
  - 使用 `etcd` 后端：`"192.168.0.137:2379"`, `"etcd://192.168.0.137:2379"` 或 `"etcd://192.168.0.137:2379,192.168.0.138:2379"`
  - 使用 `redis` 后端：`"redis://192.168.0.137:6379"`
  - 使用 `http` 后端：`"http://192.168.0.137:8080/metadata"`
- "protocol": 数据传输协议("rdma/tcp")。
- "device_name": 用于数据传输的设备，当 "protocol" 设置为 "rdma" 时必填。如果使用多个 NIC 设备，它们可以用逗号分隔，如 "erdma_0,erdma_1"。请注意它们之间没有空格。
- "master_server_address": MooncakeStore 主守护进程的 IP:Port 地址。

### 使用 TCP 运行示例所需配置文件

- 为预填充实例和解码实例准备 _**mooncake.json**_ 配置文件
```json
{
    "local_hostname": "192.168.0.137",
    "metadata_server": "etcd://192.168.0.137:2379",
    "protocol": "tcp",
    "device_name": "",
    "master_server_address": "192.168.0.137:50001"
}
```

## 运行示例
 - 请根据您的环境更改以下指南中的 IP 地址和端口。
```bash
# Begin from `root` of your cloned repo!

# 1. 启动 etcd 服务器
etcd --listen-client-urls http://0.0.0.0:2379 --advertise-client-urls http://localhost:2379
# 在运行上述命令之前，您可能需要终止其他 etcd 进程

# 2. 启动 mooncake_master 服务器
mooncake_master --port 50001
# 若出现 vllm 实例异常退出，由于连接元数据未正确清理。这时，建议在下次测试前重启 mooncake_master

# 3. 启动多个 vLLM 实例
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

- `MOONCAKE_CONFIG_PATH` 指定 mooncake.json 配置文件路径。
- `VLLM_USE_MODELSCOPE` 是可选的，如果您的网络可以访问 huggingface，请去掉这个选项。
- `VLLM_USE_V1=0` 必须设置，因解耦功能当前仅支持 v0 版 vLLM。
  - 您也可通过 `export` 设置环境变量，这样便不用在每行命令前添加。
- `--model` 参数指定要使用的模型。
- `--port` 参数指定 vllm 服务监听的端口。
- `--max-model-len` 参数指定模型的最大长度。
- 支持 `--tensor_parallel_size` \ `-tp` 参数。例如：添加 `-tp 2` 启用多卡并行。
  - 注意：所有实例需使用相同的 tensor_parallel_size。
  - 如果您希望在同节点部署预填充与解码实例，则需设置不同的 `CUDA_VISIBLE_DEVICES`。例如，预填充实例用`CUDA_VISIBLE_DEVICES=0,1`，解码实例用 `CUDA_VISIBLE_DEVICES=2,3`。
- `--kv-transfer-config` 参数指定要使用的连接器及其配置。
  - 请将 `kv_connector` 设为 `MooncakeStoreConnector`。
  - `kv_role` 定义节点角色，可以是 'kv_producer'，'kv_consumer' 或 'kv_both'。

```bash
# 4. 启动代理服务器
cd vllm
python3 examples/online_serving/disagg_examples/disagg_proxy_demo.py --model Qwen/Qwen2.5-7B-Instruct-GPTQ-Int4 --prefill localhost:8100 localhost:8101  --decode localhost:8200 localhost:8201  --port 8000
```

- `--model` 参数指定使用的模型，也指定代理服务器使用的分词器。
- `--port` 参数指定 vllm 服务的端口。
- `--prefill` 或 `-p` 指定 vllm 预填充实例的 IP 和端口。
- `--decode` 或 `-d` 指定 vllm 解码实例的 IP 和端口。

```bash
# 如需运行时动态调整预填充组/解码组实例，需配置管理密钥
export ADMIN_API_KEY="xxxxxxxx"
# 或在命令前添加：
ADMIN_API_KEY="xxxxxxxx" python3 vllm/examples/online_serving/disagg_examples/disagg_demo.py --model Qwen/Qwen2.5-7B-Instruct-GPTQ-Int4 --prefill localhost:8100 localhost:8101  --decode localhost:8200 localhost:8201  --port 8000 --scheduling round_robin

# 然后使用以下命令动态添加实例到对应组
curl -X POST "http://localhost:8000/instances/add" -H "Content-Type: application/json" -H "X-API-Key: $ADMIN_API_KEY" -d '{"type": "prefill", "instance": "localhost:8102"}'

curl -X POST "http://localhost:8000/instances/add" -H "Content-Type: application/json" -H "X-API-Key: $ADMIN_API_KEY" -d '{"type": "prefill", "instance": "localhost:8103"}'

curl -X POST "http://localhost:8000/instances/add" -H "Content-Type: application/json" -H "X-API-Key: $ADMIN_API_KEY" -d '{"type": "decode", "instance": "localhost:8202"}'

curl -X POST "http://localhost:8000/instances/add" -H "Content-Type: application/json" -H "X-API-Key: $ADMIN_API_KEY" -d '{"type": "decode", "instance": "localhost:8203"}'

# 获取代理服务状态
curl localhost:8000/status | jq
```

Mooncake 团队实现的这个简单的 disagg_proxy 示例基于轮询策略。实际生产环境中，服务提供者和用户可根据需求实现全局代理策略。

**_请确保修改命令中的 IP 地址。_**


## 测试 OpenAI 兼容的请求
```
curl -s http://localhost:8000/v1/completions -H "Content-Type: application/json" -d '{
  "model": "Qwen/Qwen2.5-7B-Instruct-GPTQ-Int4",
  "prompt": "San Francisco is a",
  "max_tokens": 1000
}'
```
- 如果您不是在代理服务器上进行测试，请将 localhost 更改为代理服务器的 IP 地址。