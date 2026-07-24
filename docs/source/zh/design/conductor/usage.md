# 运行 Mooncake Conductor

[English](../../../design/conductor/usage.md)

本指南从检出源码开始，一步步运行 Conductor，直到 HTTP 查询返回可用结果。内容包括静态和动态注册事件源、vLLM 与 Mooncake 的稳妥启动顺序，以及准确的清理命令。所有示例都针对当前 C++ 服务，并且只使用当前解析器接受的字段。

## 开始前

先按照[构建指南](../../../getting_started/build.md)安装仓库所需的构建依赖。Conductor 组件还需要 ZeroMQ、C++ 版 MessagePack、OpenSSL、JsonCpp、glog 和 yalantinglibs；CMake 会在配置阶段检查这些依赖。

还需要准备：

- 为每个要注册的 vLLM 数据并行（DP）rank 准备一个键值（KV）事件地址（endpoint）；
- 如果需要共享 CPU 或 Disk 信息，再准备一个 Mooncake Master KV Event 地址；
- 使用 `curl` 执行下文的检查；
- 为所有兼容的 vLLM 进程设置同一个明确的 `PYTHONHASHSEED` 值。

下文中的地址、模型名称和种子都只是示例。开始产生缓存的业务流量前，请把它们替换为同一部署中的真实值。不要省略 `PYTHONHASHSEED`：未设置该环境变量时，vLLM 会生成随机根字节，Conductor 无法根据注册信息复现它们。

## 构建

在仓库根目录执行配置，并且只构建 Conductor 可执行程序：

```bash
cmake -S . -B build -DWITH_CONDUCTOR=ON
cmake --build build --target mooncake_conductor
```

生成的可执行文件是 `build/mooncake-conductor/mooncake_conductor`。用下面的命令确认构建确实生成了该文件：

```bash
test -x build/mooncake-conductor/mooncake_conductor
```

退出状态为 0，说明可执行文件已经可以运行。

## 配置

Conductor 读取两个环境变量：

| 环境变量 | 默认值 | 何时会用到 |
|---|---|---|
| `CONDUCTOR_CONFIG_PATH` | `$HOME/.mooncake/conductor_config.json` | 指定启动时读取的 JSON 文件。文件不存在时，Conductor 不会创建静态订阅，并保留内置 HTTP 端口 `13333`。 |
| `CONDUCTOR_LOG_LEVEL` | `INFO` | 设置 `DEBUG`、`INFO`、`WARN` 或 `ERROR`，不区分大小写。值为空或无效时使用 `INFO`。 |

在可读取的配置文件中，一定要显式设置 `http_server_port`。如果成功读取了配置文件，但其中没有这个字段，当前解析器会把端口设为 `0`，不会继续使用 `13333`。

下面是一份完整的静态配置示例：它为一个引擎注册两个 vLLM rank，并注册一个 Mooncake 共享缓存池。`lora_name` 为空表示基础模型；非空值表示一个低秩适配（Low-Rank Adaptation，LoRA）适配器。

```json
{
  "http_server_port": 13333,
  "kvevent_instance": {
    "vllm-engine-a-rank-0": {
      "endpoint": "tcp://127.0.0.1:5557",
      "replay_endpoint": "tcp://127.0.0.1:5558",
      "type": "vLLM",
      "modelname": "test-model",
      "lora_name": "",
      "tenant_id": "default",
      "instance_id": "engine-a",
      "block_size": 16,
      "dp_rank": 0,
      "cache_group": 0,
      "hash_profile": {
        "strategy": "vllm_v1",
        "algorithm": "sha256_cbor",
        "python_hash_seed": "0",
        "index_projection": "low64_be"
      }
    },
    "vllm-engine-a-rank-1": {
      "endpoint": "tcp://127.0.0.1:5567",
      "replay_endpoint": "tcp://127.0.0.1:5568",
      "type": "vLLM",
      "modelname": "test-model",
      "lora_name": "",
      "tenant_id": "default",
      "instance_id": "engine-a",
      "block_size": 16,
      "dp_rank": 1,
      "cache_group": 0,
      "hash_profile": {
        "strategy": "vllm_v1",
        "algorithm": "sha256_cbor",
        "python_hash_seed": "0",
        "index_projection": "low64_be"
      }
    },
    "mooncake-shared-pool": {
      "endpoint": "tcp://127.0.0.1:6557",
      "replay_endpoint": "",
      "type": "Mooncake",
      "modelname": "test-model",
      "lora_name": "",
      "tenant_id": "default",
      "instance_id": "shared-pool",
      "block_size": 16,
      "dp_rank": 0,
      "cache_group": 0,
      "hash_profile": {
        "strategy": "vllm_v1",
        "algorithm": "sha256_cbor",
        "python_hash_seed": "0",
        "index_projection": "low64_be"
      }
    }
  }
}
```

`python_hash_seed` 必须与每个兼容 vLLM 进程中 `PYTHONHASHSEED` 环境变量的准确文本相同。Conductor 保留这段文本，把它编码为规范 CBOR 文本字符串，再计算 SHA-256 得到根摘要。种子字符串 `"0"` 得到的小写诊断值是 `4e1195df020de59e0d65a33a4279f1183e7ae4e5d980e309f8b55adff2e61c3e`。这个摘要会出现在 `/services` 和 `/global_view` 中；它不是注册输入，也不是通用于所有部署的默认值。

JSON 中必须使用带引号的字符串：`"0"`、`"00"` 和数值 `0` 是不同输入。明确设置的字面量 `random` 受支持，并根据这段准确文本派生根摘要；未设置环境变量时，vLLM 会选择无法注册复现的随机字节，因此不受支持。vLLM 的 `--seed` 控制模型和采样随机性，与前缀缓存的哈希标识无关。请求中的 `cache_salt` 只发送给 `/query`，不是注册字段。准确规则请参阅 {ref}`token 块的哈希计算 <how-token-blocks-become-lookup-values>`。

所有兼容的 vLLM 进程都要使用相同的环境变量文本和规范 CBOR 前缀哈希算法。对于本文的种子零示例：

```bash
PYTHONHASHSEED=0 vllm serve test-model \
  --enable-prefix-caching \
  --prefix-caching-hash-algo sha256_cbor
```

使用绝对配置路径和所需日志级别启动 Conductor：

```bash
export CONDUCTOR_CONFIG_PATH=/absolute/path/to/conductor_config.json
export CONDUCTOR_LOG_LEVEL=INFO
./build/mooncake-conductor/mooncake_conductor
```

启动成功时，日志中会出现 `HTTP server listening port=13333`，以及静态订阅成功和失败的数量。所有静态订阅会同时启动，因此 JSON 中各项的排列顺序不能保证 vLLM 早于 Mooncake 启动。

## 只使用 vLLM 启动

动态注册可以明确控制启动顺序。如果不需要静态订阅，请使用下面这份替代配置启动 Conductor：

```json
{
  "http_server_port": 13333,
  "kvevent_instance": {}
}
```

启动可执行文件后，注册每个 vLLM rank。第一个请求注册 rank `0`：

```bash
curl -sS -X POST http://127.0.0.1:13333/register \
  -H 'Content-Type: application/json' \
  -d '{
    "endpoint": "tcp://127.0.0.1:5557",
    "replay_endpoint": "tcp://127.0.0.1:5558",
    "type": "vLLM",
    "modelname": "test-model",
    "lora_name": "",
    "tenant_id": "default",
    "instance_id": "engine-a",
    "block_size": 16,
    "dp_rank": 0,
    "cache_group": 0,
    "hash_profile": {
      "strategy": "vllm_v1",
      "algorithm": "sha256_cbor",
      "python_hash_seed": "0",
      "index_projection": "low64_be"
    }
  }'
```

用该 rank 自己的事件 endpoint 注册 rank `1`，引擎和哈希设置保持不变：

```bash
curl -sS -X POST http://127.0.0.1:13333/register \
  -H 'Content-Type: application/json' \
  -d '{
    "endpoint": "tcp://127.0.0.1:5567",
    "replay_endpoint": "tcp://127.0.0.1:5568",
    "type": "vLLM",
    "modelname": "test-model",
    "lora_name": "",
    "tenant_id": "default",
    "instance_id": "engine-a",
    "block_size": 16,
    "dp_rank": 1,
    "cache_group": 0,
    "hash_profile": {
      "strategy": "vllm_v1",
      "algorithm": "sha256_cbor",
      "python_hash_seed": "0",
      "index_projection": "low64_be"
    }
  }'
```

每个成功请求都会返回 `registered successfully`。放开会产生缓存的业务流量前，先确认引擎信息：

```bash
curl -sS http://127.0.0.1:13333/global_view
```

对应结果必须显示 `tenant_id` 为 `default`、`model_name` 为 `test-model`、`lora_name` 为空、`block_size` 为 `16`、`python_hash_seed` 为 `"0"`、根摘要为上文的派生值，并且 `"engine-a":[0,1]` 位于 `instances` 下。还要检查 `/services`：其中应包含两个 vLLM 订阅，并显示相同的种子和派生根摘要。如果部署中只有 vLLM，现在可以开始产生缓存的业务流量。

## 添加 Mooncake

添加共享缓存池时，先不要放开会产生缓存的业务流量。按照 [Mooncake 发布端指南](../kv-event/publisher-design.md)启用并启动 Mooncake Master 发布端，然后注册它的实时事件地址：

```bash
curl -sS -X POST http://127.0.0.1:13333/register \
  -H 'Content-Type: application/json' \
  -d '{
    "endpoint": "tcp://127.0.0.1:6557",
    "replay_endpoint": "",
    "type": "Mooncake",
    "modelname": "test-model",
    "lora_name": "",
    "tenant_id": "default",
    "instance_id": "shared-pool",
    "block_size": 16,
    "dp_rank": 0,
    "cache_group": 0,
    "hash_profile": {
      "strategy": "vllm_v1",
      "algorithm": "sha256_cbor",
      "python_hash_seed": "0",
      "index_projection": "low64_be"
    }
  }'
```

配置 Mooncake 发布端，使每个事件携带的租户、模型、LoRA 名称和块大小与 vLLM 的缓存信息一致。`hash_profile` 来自 Mooncake 注册项，并且必须与该事件对应的缓存信息已经绑定的哈希配置相同。注册时也使用同样的缓存信息，这样更容易对照 `/services` 和发布端。Mooncake 的 `instance_id` 和 `dp_rank` 只用于在 `/services` 中标识订阅，以及通过 `/unregister` 注销；它们不会创建查询实例，也不会覆盖事件中的缓存信息。`stored` 事件还必须带有对象 key 和可用的完整连接器哈希；事件检查规则请参阅[订阅兼容指南](../kv-event/subscriber-guide.md)。

放开业务流量前，确认 `/services` 同时包含两个 vLLM rank 和 Mooncake 订阅：

```bash
curl -sS http://127.0.0.1:13333/services
```

如果使用静态配置，则采用另一种稳妥做法：在 `/global_view` 显示预期的 vLLM 缓存信息、引擎、rank、配置种子和派生根摘要，并且 `/services` 显示所有预期订阅和同一份已解析 hash profile 前，保持会产生缓存的业务流量暂停。静态订阅会同时启动，与 JSON 中的排列顺序无关。

这些检查只能说明有限的状态。`/services` 表示 Conductor 接受了注册，并启动了本地订阅客户端；它不能证明远端 ZeroMQ 发布端已经发送事件。`/global_view` 表示 vLLM 缓存信息和 rank 已经注册；它不会恢复 Conductor 连接前已写入 Mooncake 的对象。当前 Mooncake 发布端不会重发此前的状态。

## 检查运行状态

查看当前订阅：

```bash
curl -sS http://127.0.0.1:13333/services
```

对于上面的动态注册示例，`count` 为 `3` 才表示成功。两个 `vLLM` 项的 `InstanceID` 都应为 `engine-a`，`DPRank` 分别为 `0` 和 `1`；`Mooncake` 项的 `InstanceID` 应为 `shared-pool`。三个 `HashProfile` 必须相同，包括 `python_hash_seed` `"0"` 及其派生的 `root_digest`。

查看缓存共享组合和已注册的推理 rank：

```bash
curl -sS http://127.0.0.1:13333/global_view
```

对应结果中出现 `"engine-a":[0,1]`，说明检查成功。Mooncake 不会作为另一个推理实例出现。收到实时事件后，`prefix_count` 可能增加，但仅凭这个计数无法判断每个块来自哪个缓存位置。

## 查询

发送已注册模型的分词器（tokenizer）生成的 token IDs。下面的示例包含一个完整的 16-token 块：

```bash
curl -sS -X POST http://127.0.0.1:13333/query \
  -H 'Content-Type: application/json' \
  -d '{
    "model": "test-model",
    "lora_name": "",
    "tenant_id": "default",
    "block_size": 16,
    "token_ids": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]
  }'
```

成功响应中会有 `instances.engine-a` 对象，其中 DP rank key 为 `"0"` 和 `"1"`。在收到匹配的实时事件前，命中值可能为零。查询只计算完整块。如果事件生产端使用缓存盐值（cache salt）计算请求哈希，请在这次查询中添加取值相同的 `cache_salt` 字符串，不要把它加入注册项。

准确的响应字段请参阅 [HTTP API 参考](./indexer-api-design.md#post-query)；Conductor 如何组合 GPU、CPU 和 Disk 可用性，请参阅{ref}`架构指南 <what-query-fields-mean>`。

## 注销

替换发布端前，先注销准确的服务标识（service key）。这个 key 由 `instance_id`、规范化后的 `tenant_id` 和 `dp_rank` 组成。

删除 Mooncake 订阅：

```bash
curl -sS -X POST http://127.0.0.1:13333/unregister \
  -H 'Content-Type: application/json' \
  -d '{"instance_id":"shared-pool","tenant_id":"default","dp_rank":0}'
```

分别删除每个 vLLM rank：

```bash
curl -sS -X POST http://127.0.0.1:13333/unregister \
  -H 'Content-Type: application/json' \
  -d '{"instance_id":"engine-a","tenant_id":"default","dp_rank":1}'

curl -sS -X POST http://127.0.0.1:13333/unregister \
  -H 'Content-Type: application/json' \
  -d '{"instance_id":"engine-a","tenant_id":"default","dp_rank":0}'
```

每个成功响应都包含 `unregistered successfully` 和准确的已删除 service key，例如 `engine-a|default|0`。检查 `/services`，确认对应订阅已经消失。注销一个 vLLM rank 只删除该 rank 的 GPU 信息；注销 Mooncake 只删除由该 Mooncake endpoint 报告的对象。

## 处理配置问题

| 现象 | 检查 | 处理方式 |
|---|---|---|
| HTTP 请求无法连接，或日志显示端口为 `0` | 确认 `CONDUCTOR_CONFIG_PATH` 指向刚才编辑的文件，并且文件中包含数值类型的 `http_server_port`。 | 设置绝对配置路径，添加明确的非零端口，然后重启 Conductor。 |
| `/services` 中没有某个事件源 | 查看注册响应和 Conductor 日志，检查是否使用了不支持的 type、非零 `cache_group`、无效哈希配置、重复 endpoint 或冲突的 service key。 | type 只使用 `vLLM` 或 `Mooncake`；缓存组使用 `0` 或省略；使用唯一 endpoint 和准确的种子式哈希字段。 |
| `/services` 中有事件源，但命中值一直为零 | 检查发布端是否在 Conductor 连接后发出过事件。`/services` 不能证明事件已经送达。 | 检查发布端状态，配置期间保持业务流量暂停，并在产生新缓存前完成注册。此前的 Mooncake 事件不会重发。 |
| Mooncake 事件没有增加 CPU 或 Disk 可用性 | 对照 `/global_view`，检查 Mooncake 事件携带的租户、模型、LoRA 名称和块大小；比较注册的哈希配置；确认 stored 事件包含对象信息和完整连接器哈希数据。 | 先注册 vLLM，确保事件中的缓存信息和哈希配置一致，并修正 Mooncake 发布端或 key 配置。 |
| `/query` 返回空的 `instances` 对象，或命中长度比预期短 | 对照事件生产端检查 `model`、`tenant_id`、`lora_name`、`block_size`、可选 `instance_id`、token IDs 和 `cache_salt`。同时检查末尾是否是不完整块。 | 查询准确的已注册四字段组合，使用生产端的盐值规则，并发送足够组成完整块的 token。 |
| HTTP 请求返回 `400` | 查看 JSON 中的 `reason`、`field` 和可选 `index`；注册或注销请求的 JSON 格式错误会改为返回纯文本。 | 删除不支持的字段，并按照 [API 字段表](./indexer-api-design.md)修正错误中指出的值。 |
