# Mooncake Conductor HTTP API

[English](../../../design/conductor/indexer-api-design.md)

本参考介绍当前 C++ Conductor 服务实现的五个 HTTP 接口。你可以用这些接口注册
实时事件来源、注销事件来源、查看 Conductor 的内存状态，以及查询可以复用的
缓存前缀。下文中的字段名、可用值、响应字段大小写和错误格式都与当前解析和
序列化代码一致。

## 选择接口

| 方法 | 路径 | 用途 |
|---|---|---|
| `POST` | `/register` | 启动一条 vLLM 或 Mooncake 事件订阅。 |
| `POST` | `/unregister` | 停止一条订阅，并清理该 endpoint 上报的缓存信息。 |
| `POST` | `/query` | 根据提示词的 token ID 查询缓存可用情况。 |
| `GET` | `/global_view` | 查看缓存共享范围和已注册的 vLLM rank。 |
| `GET` | `/services` | 列出正在使用的事件订阅及其完整配置。 |

## 通用请求和响应规则

所有 `POST` 请求体都必须是 JSON 对象。Conductor 会拒绝未知字段。成功响应使用
`application/json`；JSON 对象中各字段的顺序不属于接口约定。

大多数参数检查错误返回状态码 `400` 和一个 `application/json` 对象：

```json
{
  "error": "unsupported request field: root_digest",
  "reason": "unknown_field",
  "field": "root_digest"
}
```

如果错误由某个字段引起，响应会包含 `field`。如果错误来自数组中的某个元素，
响应还会包含 `index`。[错误格式](#理解错误)一节列出了改为返回纯文本的情况。

## `POST /register`

这个接口启动一条事件订阅。每个 vLLM 数据并行（DP）rank 都要使用不同的
endpoint 分别注册。Mooncake 订阅提供共享 CPU 或 Disk 信息，但不会在
`/query` 中增加一行推理实例。

### 请求字段

| 字段 | 必填 | 可接受的值和用途 |
|---|---|---|
| `endpoint` | 是 | 非空的 ZeroMQ 实时事件发布地址。该地址不能已被另一条有效注册使用。 |
| `type` | 是 | 只能是 `vLLM` 或 `Mooncake`。它决定 Conductor 按哪种事件消息格式读取数据。 |
| `modelname` | 是 | 非空的注册模型名。对 vLLM，它决定缓存范围；Mooncake 事件会自行携带模型名，决定实际写入哪个共享缓存范围。 |
| `instance_id` | 是 | 对 vLLM 是非空的推理引擎名称，对 Mooncake 是订阅名称。它是服务键的一部分；Mooncake 的这个值不会成为查询实例。 |
| `block_size` | 是 | 每个块包含的 token 数，必须为正数。对 vLLM，它决定缓存范围；Mooncake 事件会携带实际的块大小。 |
| `dp_rank` | 是 | 从 `0` 到当前平台 `int` 最大值的整数。对 vLLM，它选择 DP rank；它也是每个服务键的一部分。对 Mooncake，它只用于标识订阅。 |
| `hash_profile` | 是 | 包含下表四个哈希字段的对象。 |
| `replay_endpoint` | 否 | 重连后用来请求缺失事件的字符串地址。默认值是 `""`，表示不创建补发连接。 |
| `lora_name` | 否 | 已注册的低秩适配（LoRA）名称，默认值是 `""`。对 vLLM，它决定缓存范围；Mooncake 事件会携带实际的 LoRA 名称。 |
| `tenant_id` | 否 | 已注册的租户名。省略或传入 `""` 都会变成 `"default"`。对 vLLM，它决定缓存范围，也是服务键的一部分；Mooncake 事件会携带实际租户。 |
| `cache_group` | 否 | 整数 `0` 或 `null`。省略也表示不明确指定 group。其他值和数组都会被拒绝。 |

目前只支持下面这一种 `hash_profile`：

| 哈希字段 | 支持的值 |
|---|---|
| `strategy` | `vllm_v1` |
| `algorithm` | `sha256_cbor` |
| `python_hash_seed` | 字符串，内容必须是准确的 `random`，或者数值在 `0..4294967295` 范围内的 ASCII 十进制文本。Conductor 会保留前导零等原始文本。 |
| `index_projection` | `low64_be` |

输入对象必须只包含这四个字段。空值、带正负号、两侧有空白、非字符串、无效
UTF-8 和超出范围的种子都会被拒绝。`"0"` 与 `"00"` 不同；数值类型的 JSON
`0` 无效。注册时提供 `root_digest` 会被视为旧版未知字段并遭到拒绝。

由四个字段确定的同一缓存共享范围必须使用完全相同的已解析哈希配置。对 vLLM，
这些字段来自注册信息；对 Mooncake，这些字段来自每条事件，而哈希配置来自
Mooncake 的注册信息。Conductor 对准确的种子文本进行规范 CBOR 编码，再计算
SHA-256 派生根摘要。这段文本必须与每个兼容 vLLM 进程中的 `PYTHONHASHSEED`
相同。明确设置的字符串 `random` 受支持；未设置该环境变量时，vLLM 会选择
Conductor 无法复现的随机根字节，因此不受支持。vLLM 的 `--seed` 控制模型和
采样随机性，不控制前缀缓存的哈希标识。

关于规范 Concise Binary Object Representation（CBOR）输入顺序、LoRA 和
`cache_salt` 的先后顺序、如何使用完整父摘要逐块计算、如何把最后八个字节按
大端序转成查询值，以及带版本说明的测试样例，请参阅
[token 块的哈希计算说明](./conductor-architecture-design.md)。`cache_salt` 是查询
字段，不是注册字段。Mooncake 事件中的 `additional_salt` 只用于诊断，这个
HTTP 接口不接受该字段。

### 最小请求

```bash
curl -sS -X POST http://127.0.0.1:13333/register \
  -H 'Content-Type: application/json' \
  -d '{
    "endpoint": "tcp://127.0.0.1:5557",
    "type": "vLLM",
    "modelname": "test-model",
    "instance_id": "engine-a",
    "block_size": 16,
    "dp_rank": 0,
    "hash_profile": {
      "strategy": "vllm_v1",
      "algorithm": "sha256_cbor",
      "python_hash_seed": "0",
      "index_projection": "low64_be"
    }
  }'
```

状态码 `200` 返回：

```json
{
  "status": "registered successfully",
  "instance_id": "engine-a"
}
```

完全相同的有效注册再次提交时，Conductor 仍返回相同的成功响应，但不会再启动
一个订阅客户端。如果服务键、endpoint 或哈希配置冲突，Conductor 返回 JSON
格式的 `400`，其中 `reason` 是 `invalid_registration`。本地订阅客户端无法启动
时，Conductor 返回纯文本 `500`。

## `POST /unregister`

这个接口停止一条指定的订阅。它会等待订阅客户端停止，然后再清理该 endpoint
上报的缓存信息。

### 请求字段

| 字段 | 必填 | 可接受的值和用途 |
|---|---|---|
| `instance_id` | 是 | 注册事件来源时使用的非空字符串。 |
| `dp_rank` | 是 | 注册事件来源时使用的非负整数 rank。 |
| `tenant_id` | 否 | 注册事件来源时使用的字符串。省略或传入 `""` 都会变成 `"default"`。 |

不接受其他字段。

### 最小请求

```bash
curl -sS -X POST http://127.0.0.1:13333/unregister \
  -H 'Content-Type: application/json' \
  -d '{"instance_id":"engine-a","dp_rank":0}'
```

状态码 `200` 返回实际删除的服务键：

```json
{
  "status": "unregistered successfully",
  "removed_instances": [
    "engine-a|default|0"
  ]
}
```

服务键不存在时返回纯文本 `404`。如果订阅客户端停止后清理失败，则返回纯文本
`500`。此时 Conductor 会继续占用该服务键和 endpoint，二者都不能重新注册。
请重复发送相同的 `/unregister` 请求，直到清理成功。

## `POST /query`

这个接口使用已注册的哈希配置，对请求中完整的 token 块进行哈希计算。请求中的
租户、模型、LoRA 名称和块大小必须能找到对应的注册信息。请求不能覆盖算法、
策略、`python_hash_seed`、派生根摘要或最后八字节的查询规则。

### 请求字段

| 字段 | 必填 | 可接受的值和用途 |
|---|---|---|
| `model` | 是 | 非空的模型名，必须与注册信息中的 `modelname` 一致。 |
| `block_size` | 是 | 正整数。只有包含足够 token 的完整块会参与哈希计算。 |
| `token_ids` | 是 | 由有符号 32 位整数组成的 JSON 数组。空数组有效，并会为匹配的已注册 rank 返回零命中。 |
| `tenant_id` | 否 | 字符串。省略或传入 `""` 都会变成 `"default"`。 |
| `lora_name` | 否 | 字符串。默认值是 `""`，表示基础模型。 |
| `cache_salt` | 否 | 字符串、`null` 或省略。`null`、`""` 和省略都表示不加 salt；非空值必须与生产方一致。 |
| `instance_id` | 否 | 字符串过滤条件。匹配时只返回指定实例；未知值返回空的 `instances` 对象。 |

请求不能包含覆盖哈希配置的字段。如果找不到对应的缓存共享范围，也会返回状态码
`200` 和空的 `instances` 对象，并且不会创建新状态。

### 最小请求

```bash
curl -sS -X POST http://127.0.0.1:13333/query \
  -H 'Content-Type: application/json' \
  -d '{
    "model": "test-model",
    "block_size": 16,
    "token_ids": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]
  }'
```

如果已经注册 `engine-a` 的 rank `0`，但还没有收到匹配的缓存事件，响应如下：

```json
{
  "instances": {
    "engine-a": {
      "longest_matched": 0,
      "dp": {
        "0": 0
      },
      "gpu": 0,
      "cpu": 0,
      "disk": 0,
      "rank_matches": {
        "0": {
          "gpu": 0,
          "cpu": 0,
          "disk": 0
        }
      }
    }
  }
}
```

### 返回字段

| 字段 | 含义 |
|---|---|
| `instances` | 以选中的已注册 vLLM `instance_id` 为键。Mooncake 订阅不会成为返回结果中的一行。 |
| `longest_matched` | 与实例级 `disk` 相同，即某一个已注册 rank 能实现的最长 GPU 到 CPU 再到 Disk 的有序前缀。 |
| `dp` | 每个 rank 完成 GPU 阶段后的累计边界。rank 键是十进制 JSON 字符串，值仍是整数。 |
| `rank_matches` | 与 `dp` 使用相同 rank 键。每个 rank 对应累计整数 `gpu`、`cpu` 和 `disk` 边界。 |
| `gpu` | 该引擎所有 rank 级 GPU 边界中的最大值。 |
| `cpu` | 每个 rank 从 GPU 继续到共享 CPU 后的累计边界，再取其中最大值。 |
| `disk` | 每个 rank 从 GPU 继续到共享 CPU、再到共享 Disk 后的累计边界，再取其中最大值。 |

Conductor 对每个已注册 rank 先读取连续 GPU 前缀，再让第一个 GPU 未命中的块进入
共享 CPU，随后让第一个 CPU 未命中的块进入共享 Disk。进入较低层后不会回到较高层，
Disk 第一次未命中就结束该 rank 的结果。所有边界只计算查询中的完整块。两个 rank
map 的 key 完全相同，包括零命中 rank，并且每项结果都满足：

```text
dp[rank] == rank_matches[rank].gpu
0 <= rank_matches[rank].gpu
  <= rank_matches[rank].cpu
  <= rank_matches[rank].disk
  <= complete_query_tokens
longest_matched == disk
```

`rank_matches` 是新增字段，原有字段的 JSON 类型保持不变。不过，`cpu`、`disk`
以及部分 `longest_matched` 的值现在表示这条有序累计路径，而不再表示独立前缀或
逐块无序并集。

在当前 HTTP 测试使用的状态中，实例 `1` 的 rank `0` 有 32 个 token 的连续 GPU
缓存，实例 `2` 的 rank `1` 没有 GPU 命中，两者都能看到连续 48 个 token 的
共享 CPU 和 Disk 缓存。准确响应如下：

```json
{
  "instances": {
    "1": {
      "longest_matched": 48,
      "gpu": 32,
      "dp": {
        "0": 32
      },
      "cpu": 48,
      "disk": 48,
      "rank_matches": {
        "0": {
          "gpu": 32,
          "cpu": 48,
          "disk": 48
        }
      }
    },
    "2": {
      "longest_matched": 48,
      "gpu": 0,
      "dp": {
        "1": 0
      },
      "cpu": 48,
      "disk": 48,
      "rank_matches": {
        "1": {
          "gpu": 0,
          "cpu": 48,
          "disk": 48
        }
      }
    }
  }
}
```

再看一个按层续接的例子：rank `0` 的前两个 16-token 块只在 GPU，第三块只在
共享 CPU，第四块只在共享 Disk。该实例返回：

```json
{
  "instances": {
    "engine-a": {
      "longest_matched": 64,
      "gpu": 32,
      "dp": {
        "0": 32
      },
      "cpu": 48,
      "disk": 64,
      "rank_matches": {
        "0": {
          "gpu": 32,
          "cpu": 48,
          "disk": 64
        }
      }
    }
  }
}
```

连续命中和各 rank 的计算规则见[查询字段说明](./conductor-architecture-design.md)。

## `GET /global_view`

这个接口显示当前 Conductor 进程已知的缓存共享范围。请求不需要请求体。

### 最小请求

```bash
curl -sS http://127.0.0.1:13333/global_view
```

注册 `engine-a` 的 rank `0` 和 `1` 后，在收到缓存事件前，单个缓存范围的响应
格式如下：

```json
{
  "context_count": 1,
  "contexts": [
    {
      "model_name": "test-model",
      "lora_name": "",
      "block_size": 16,
      "tenant_id": "default",
      "prefix_count": 0,
      "hash_profile": {
        "strategy": "vllm_v1",
        "algorithm": "sha256_cbor",
        "python_hash_seed": "0",
        "root_digest": "4e1195df020de59e0d65a33a4279f1183e7ae4e5d980e309f8b55adff2e61c3e",
        "index_projection": "low64_be"
      },
      "instances": {
        "engine-a": [
          0,
          1
        ]
      }
    }
  ]
}
```

`context_count` 是 `contexts` 中的条目数。`prefix_count` 统计仍有至少一条 GPU、
CPU 或 Disk 记录的不同查询值；这里的查询值是完整摘要的最后八个字节。
`instances` 把已注册的 vLLM 引擎映射到由数字组成的 rank 数组。Mooncake 订阅
不会加入该映射。这个数组的顺序不固定。已解析的 `hash_profile` 会同时返回准确
的配置种子和由它派生的小写根摘要。

## `GET /services`

这个接口列出正在使用的订阅配置。请求不需要请求体。字段名特意保留下方所示的
大小写，与注册请求不同。

### 最小请求

```bash
curl -sS http://127.0.0.1:13333/services
```

发送上面的最小注册请求后，状态码 `200` 返回：

```json
{
  "count": 1,
  "services": [
    {
      "Endpoint": "tcp://127.0.0.1:5557",
      "ReplayEndpoint": "",
      "Type": "vLLM",
      "ModelName": "test-model",
      "LoraName": "",
      "TenantID": "default",
      "InstanceID": "engine-a",
      "BlockSize": 16,
      "DPRank": 0,
      "CacheGroup": null,
      "HashProfile": {
        "strategy": "vllm_v1",
        "algorithm": "sha256_cbor",
        "python_hash_seed": "0",
        "root_digest": "4e1195df020de59e0d65a33a4279f1183e7ae4e5d980e309f8b55adff2e61c3e",
        "index_projection": "low64_be"
      }
    }
  ]
}
```

`count` 是有效服务键的数量。如果注册请求省略该字段，`CacheGroup` 就是
`null`。服务数组的顺序不固定。出现在这个列表中只说明 Conductor 已完成
本地订阅设置，不代表远端已经发送事件，也不代表已经找回更早的事件。开始产生
缓存的流量前，请对照 `/global_view` 检查 `python_hash_seed` 和派生的
`root_digest`。

## 理解错误

Conductor 会根据错误类型返回 JSON 参数错误或纯文本运行错误：

| 情况 | 状态码 | Content type 和响应体 |
|---|---|---|
| 任意 `POST` 接口的字段检查失败 | `400` | `application/json`，包含 `error`、`reason`，以及适用时的 `field` 或 `index`。 |
| `/query` 的 JSON 格式错误，或请求体不是对象 | `400` | `application/json`，内容为 `{"error":"Invalid JSON object","reason":"invalid_json"}`。 |
| `/register` 或 `/unregister` 的 JSON 格式错误，或请求体不是对象 | `400` | `text/plain; charset=utf-8`，内容为 `Invalid JSON\n`。 |
| `/unregister` 找不到服务键 | `404` | `text/plain; charset=utf-8`，例如 `service not found: engine-a\|default\|0\n`。 |
| 使用 `GET` 请求 `/register`、`/unregister` 或 `/query`；使用 `POST` 请求 `/global_view` 或 `/services` | `405` | `text/plain; charset=utf-8`，内容为 `Method not allowed\n`。 |
| `/register` 无法启动本地订阅客户端 | `500` | `text/plain; charset=utf-8`，开头是 `Failed to subscribe: failed to start ZMQ client:`。 |
| `/unregister` 已停止订阅客户端，但缓存清理失败 | `500` | `text/plain; charset=utf-8`，开头是 `Failed to unregister prefix context:`。 |

例如，`token_ids` 中出现字符串时，会返回精确到数组元素的 JSON 错误：

```json
{
  "error": "token_ids element must be a JSON integer",
  "reason": "invalid_type",
  "field": "token_ids",
  "index": 0
}
```

除 `/services` 外，Conductor 生成的 JSON 响应都以换行符结尾；该紧凑 JSON
响应没有结尾换行。上表中用 `\n` 表示的纯文本错误会带有该结尾换行。
