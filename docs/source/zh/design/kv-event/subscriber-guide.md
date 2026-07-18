# 将 KV Event 来源连接到 Conductor

[English](../../../design/kv-event/subscriber-guide.md)

Conductor 目前可以读取两种键值（KV）事件格式：vLLM 引擎事件和 Mooncake
共享缓存池事件。本页帮助你选择正确的注册来源类型、满足 Conductor 检查的
字段，并准确理解 Store、Remove、Clear 和 unregister 会修改什么。本文也会
介绍哈希转换和漏掉事件时的限制。

## 选择注册来源类型

每个 endpoint 必须且只能注册为下面一种区分大小写的 `type`：

- 推理引擎上报自身 GPU 缓存时使用 `"vLLM"`；
- Mooncake Master 上报共享 CPU 或 Disk 对象时使用 `"Mooncake"`。

Conductor 根据注册时记录的 type 选择 MessagePack 解析方式。即使两种发布端
都使用空 topic，topic 文本也不能选择或覆盖解析方式。接收消息的 endpoint
同时用于标识消息来源、限制清理范围和记录日志。注册字段与示例见
[HTTP API 参考](../conductor/indexer-api-design.md)。

## 对比 vLLM 和 Mooncake

| 问题 | `vLLM` 来源 | `Mooncake` 来源 |
|---|---|---|
| 注册 `type` | 必须是 `vLLM`。 | 必须是 `Mooncake`。 |
| 事件名 | `type` 中的 `BlockStored`、`BlockRemoved`、`AllBlocksCleared`。 | `event_type` 中的 `stored`、`removed`、`cleared`；可以带对应的旧版 `type`。 |
| 批次时间戳 | 以秒为单位的有限 MessagePack 浮点数。事件中不要求单独的时间戳字段。 | 以毫秒为单位的非负 MessagePack 整数。每个事件的 `timestamp` 都必须是同一个整数。 |
| 接受的缓存位置 | `GPU`，不区分大小写。其他值会记录警告并被忽略。 | `CPU` 或 `Disk`，不区分大小写。其他值会记录警告并被忽略。 |
| Tenant、模型、LoRA、块大小 | 可信的注册信息提供这些缓存共享字段。Store 事件中的块大小和低秩适配（LoRA）名称只用于核对。 | Store 事件提供 `tenant_id`、`model_name`、`lora_name` 和 `block_size`。它们必须对应一个已有的 vLLM 缓存共享范围，并使用已注册的 hash profile。 |
| 数据并行 rank | 可信的注册信息提供引擎 rank。批次中的非 `nil` rank 必须与它一致，否则整批都会被拒绝。 | 批次和事件中的 `dp_rank` 只用于排查问题，不会生成 GPU 记录或查询实例。 |
| Group | `group_idx` 可以不存在、为 `nil` 或为 `0`。 | `group_id` 可以是 `nil`、空字符串或十进制字符串 `"0"`。 |
| 对象字段 | 使用 `block_hashes`，没有 Mooncake 对象 key。 | Store 需要 `object_key` 和 `connector_block_hash`；Remove 需要 `object_key`；Clear 不携带这两个字段。 |
| `/query` 的 `instances` | 已注册引擎按 `instance_id` 出现，其中包含已注册的 rank。 | Mooncake 来源不会成为一个 instance。它的 CPU 或 Disk 信息显示在每个兼容的 vLLM 引擎下。 |

决定 CPU 或 Disk 信息能否被某个引擎共用的四个字段是 `tenant_id`、模型、
`lora_name` 和 `block_size`。它们如何影响查询结果，见
[架构页](../conductor/conductor-architecture-design.md#哪些缓存可以共用)。

## 匹配前缀哈希配置

KV Event 不携带 `PYTHONHASHSEED`，也不携带完整的第一个 parent 摘要，因此注册
信息是可信的部署声明。同一兼容缓存范围内的每个 vLLM 和 Mooncake 注册项都必须
提供同一段准确的 `python_hash_seed` 字符串。Conductor 把这段文本编码为规范
CBOR，再计算 SHA-256 派生第一个 parent 使用的 `root_digest`；调用方不注册这个
摘要。

种子必须是字面量 `random`，或者数值在 `0..4294967295` 范围内的 ASCII 十进制
文本。准确文本会影响结果：`"0"` 和 `"00"` 派生不同的根摘要。未设置
`PYTHONHASHSEED` 不受支持，因为此时 vLLM 会选择 Conductor 无法复现的随机根
字节。明确设置的文本 `random` 受支持，并按这段准确字符串计算哈希。vLLM 的
`--seed` 只控制模型和采样随机性，与这个前缀缓存标识无关。

对于种子零配置，所有兼容的 vLLM 进程都使用：

```bash
PYTHONHASHSEED=0 vllm serve test-model \
  --enable-prefix-caching \
  --prefix-caching-hash-algo sha256_cbor
```

注册时把 `python_hash_seed` 设为 `"0"`。Conductor 会通过 `/services` 和
`/global_view` 返回派生的 `root_digest`
`4e1195df020de59e0d65a33a4279f1183e7ae4e5d980e309f8b55adff2e61c3e`，
供部署对照检查。

## 满足 vLLM 事件要求

vLLM payload 必须是
`[timestamp_seconds, [event_maps], data_parallel_rank]`。最后一项可以是
非负整数或 `nil`。每个事件 map 遵循以下规则：

| 事件 | 可识别字段与检查 | 对索引的修改 |
|---|---|---|
| `BlockStored` | 必须包含 `block_hashes`、`parent_block_hash`、`token_ids`、`block_size`、`lora_id`、`medium` 和 `lora_name`。`lora_id` 必须是 `nil`；`block_size` 和 `lora_name` 必须与注册信息一致；`medium` 必须是 GPU。可选的 `group_idx` 必须是 `0` 或 `nil`。 | 把给定哈希加入已注册 endpoint、引擎及其数据并行（DP）rank 的 GPU 信息。 |
| `BlockRemoved` | 必须包含 `block_hashes` 和 `medium`；可选的 `group_idx` 必须是 `0` 或 `nil`。不能带已识别的 stored 专用字段。 | 只删除该已注册 endpoint、引擎和 rank 对这些哈希的 GPU 记录。 |
| `AllBlocksCleared` | 在已识别的 vLLM 字段中只能携带 `type`。 | 删除该已注册 endpoint、引擎和 rank 的全部 GPU 信息，保留其他引擎以及所有共享 CPU/Disk 信息。 |

`block_hashes` 是一个数组，每一项可以是无符号整数或 MessagePack 二进制
字符串。Conductor 会读取 parent hash 和 token ID，但不会用它们替换或重新
计算 `block_hashes`。

`BlockStored` 还识别可选的 `extra_keys`、`kv_cache_spec_kind` 和
`kv_cache_spec_sliding_window`。如果提供 `extra_keys`，它必须是 `nil`，
或者为每个 block hash 提供一项 `nil`/数组。Conductor 只接受
`kv_cache_spec_kind="full_attention"`，并且不接受非 `nil` 的 sliding-window
值。未知 map key 会被忽略；已识别 key 重复或类型错误时，只会使该事件无效。

## 满足 Mooncake 事件要求

Mooncake payload 必须是
`[timestamp_ms, [event_maps], data_parallel_rank]`。最后一项可以是非负整数
或 `nil`。每个事件 map 都必须包含以下通用字段：

| 字段 | 接受的形式 |
|---|---|
| `event_id` | 无符号整数。它会写入日志，但不用于排序或跳过事件。 |
| `timestamp` | 与外层非负 `timestamp_ms` 相同的有符号整数。 |
| `event_type` | `stored`、`removed` 或 `cleared`。 |
| `model_name` | 字符串或 `nil`。Store 最终必须得到非空模型名。 |
| `block_size` | 有符号整数或 `nil`。Store 要求它是正数。 |
| `additional_salt` | 字符串或 `nil`。Conductor 会读取，但不会用于缓存共享字段或哈希查询。 |
| `lora_name` | 字符串或 `nil`；`nil` 表示基础模型。 |
| `tenant_id` | 字符串。Store 和 Clear 要真正修改记录时，需要非空 tenant。 |
| `backend_id` | 字符串；Store、Remove 和 Clear 都要求它非空。 |
| `medium` | 字符串或 `nil`；Store 和 Remove 必须指定 CPU 或 Disk。 |
| `dp_rank` | 非负有符号整数，只用于排查问题。 |

如果带有旧版 `type`，它必须与 `event_type` 一致。每种事件还有以下规则：

| 事件 | 解析时必需的字段 | 真正修改索引还需要什么 |
|---|---|---|
| `stored` | `group_id`、`seq_hashes`、`base_block_idx`、`parent_hash` 和 `token_ids`。后三个字段可以是 `nil`；`seq_hashes` 可以是 `[]`。 | 受支持的 group 和介质、完整的 tenant/model/LoRA/block size、`object_key`，以及可用的完整 `connector_block_hash`。如果有 sequence hash，它必须与完整哈希一致。 |
| `removed` | `group_id`、`seq_hashes` 和 `base_block_idx`；`seq_hashes` 可以是 `[]`。不能包含 stored 专用的 parent 和 token 字段。 | 受支持的 group 和介质，以及 `object_key`。Conductor 会查找 Store 保存的准确对象记录；可选的完整哈希或 sequence hash 必须与该记录一致。 |
| `cleared` | 只包含通用字段。不能带任何已识别的对象、哈希、group、parent、token 或并行拓扑字段。 | 非空的 `backend_id` 和 `tenant_id`。该事件会清除这个来源 endpoint 下匹配的 CPU 和 Disk 对象记录。 |

对象事件还可以包含 `cache_prefix`、`tp_rank`、`head_or_tp_rank`、
`pcp_rank`、`dcp_rank`、`pp_rank` 和 `layer_id`。这些字段的类型见
[发布指南](./publisher-design.md#读取通用事件字段)。如果带有旧版
`block_hashes`，它必须等于 `seq_hashes`；如果 stored 事件带有
`parent_block_hash`，它必须等于 `parent_hash`。

## 转换收到的哈希

Conductor 会把两种来源的哈希转成同一种无符号 64 位查询值。它不会再次计算
事件中的 token 或 parent 字段。

| 收到的哈希 | 转换方式 |
|---|---|
| vLLM 无符号整数 | 它已经是查询值，直接使用。 |
| vLLM MessagePack 二进制字符串 | 至少需要八个字节，然后按大端序读取最后八个字节。 |
| Mooncake `connector_block_hash` | 去掉可选的 `0x`，接受大小写十六进制，要求长度为偶数且至少表示八个字节，将文本统一为小写，然后按大端序读取最后八个解码后的字节。 |

Mooncake Store 的 `seq_hashes` 可以为空，也可以只包含一个值；该值必须等于
从 `connector_block_hash` 得到的查询值。值不同或多于一个时，Conductor 会
拒绝该事件，不修改已保存的对象记录或缓存索引。Remove 如果带有完整哈希或
sequence 值，也必须与此前的 Store 记录一致。

虽然 `/query` 只使用最后八个字节，Conductor 仍会保存完整 Mooncake 哈希和
对象 key。如果两个对象的完整哈希不同，但最后八个字节相同，它们都可以提供
同一个可能的查询命中；删除其中一个不会删除另一个对象的记录。查询端如何生成
哈希，见 [token 块如何变成查找值](../conductor/conductor-architecture-design.md#token-块如何变成查找值)。

## 理解每种事件会修改什么

| 事件 | 准确的修改范围 |
|---|---|
| vLLM `BlockStored` | 在已注册的 tenant/model/LoRA/block size 范围内，为注册来源 endpoint、`instance_id` 和 DP rank 加入 GPU 哈希。 |
| vLLM `BlockRemoved` | 只删除该引擎和 rank 上列出的 GPU 哈希。重复 Remove 不会产生额外影响。 |
| vLLM `AllBlocksCleared` | 删除该引擎和 rank 的全部 GPU 哈希，不会影响其他 rank、引擎或 Mooncake 对象。 |
| Mooncake `stored` | 保存一条由来源 endpoint、`backend_id`、`tenant_id`、`object_key` 和 CPU/Disk 介质共同确定的记录。完全相同的重复事件不会产生额外影响；同一对象和介质上的冲突记录会被拒绝。 |
| Mooncake `removed` | 查找上述准确的对象与介质记录。未知对象不会产生影响，删除时绝不会只按 64 位值搜索。 |
| Mooncake `cleared` | 只删除上报 endpoint、`backend_id` 和 `tenant_id` 下保存的 CPU 和 Disk 记录，保留 GPU 信息以及其他 endpoint、backend 和 tenant。 |

有效批次中的 Mooncake 事件按接收顺序执行。Clear 会删除它之前建立的匹配记录；
同一批次中后续的 Store 仍可重新加入可用性。Conductor 不会按 `event_id` 排序，
也不会因为 ID 重复而自动忽略事件。

## 处理无效消息

Conductor 要求恰好三帧 ZeroMQ（ZMQ）消息：topic、八字节大端序 sequence，
以及一个 MessagePack payload。帧数错误、sequence 帧错误、MessagePack 值
无效、外层不是三项结构、批次时间戳无效、事件不是数组，或者批次 DP 类型错误
时，Conductor 会丢弃整条消息，不处理其中任何事件。vLLM 批次中的非 `nil`
DP rank 如果与注册信息冲突，同样会使整批被拒绝。

外层 payload 有效后，每个 map 会分别解析和执行。格式错误或未通过内容检查的
事件会被跳过，但它前后的有效事件仍按接收顺序生效；不会因为后面的错误撤销
前面已完成的修改。未知字段会被忽略，方便发布端增加新的可选数据；已识别字段
重复或类型错误时，只会拒绝该事件。

修改 topic 不能让某个来源切换到另一种解析方式。即使 payload 的形状看起来像
另一种来源，Conductor 也不会改用另一种方式重试。

## 清理一个来源

调用 `POST /unregister` 时，应使用标识该注册的同一组 `instance_id`、
`tenant_id` 和 `dp_rank`。Conductor 会先停止该订阅并等待其退出，再删除它
贡献的记录，然后才返回成功。旧订阅中已经排队的事件不能在清理完成后重新写入
索引。

对 vLLM 而言，unregister 会删除该 endpoint/引擎/rank 的 GPU 记录和 rank
注册。对 Mooncake 而言，它会删除该注册 endpoint 在所有事件上下文和 backend
中保存的 CPU、Disk 对象记录，同时保留其他 Mooncake endpoint 和全部 vLLM
GPU 记录。Mooncake 注册中的 `instance_id` 和 `dp_rank` 仅用于在 `/services`
中标识服务以及 unregister；它们不会生成查询实例，也不会覆盖事件中的
tenant/model/LoRA/block size。

## 规划序号空缺和重连

Conductor 会记录每个订阅最后收到的传输序号。如果后续序号向前跳跃，Conductor
会记录警告并保留当前缓存记录。这个空缺不会立即触发重发请求，也不会让
Conductor 自动删除可能已经过期的记录。

断开连接后，Conductor 会重新连接实时订阅。只有配置了 `replay_endpoint` 且
已知此前序号时，它才会请求从下一个序号开始的消息。发出请求并收到响应，也不
保证每个漏掉的缓存变化都能恢复。空 replay endpoint 是有效配置，此时只创建
实时订阅。

当前 Mooncake 发布端没有 replay endpoint，也不会发送 Conductor 连接前已经
缓存的对象清单。因此 Mooncake 来源应使用空 `replay_endpoint`；如果缓存流量
更早开始，就不能认为 Conductor 的初始视图是完整的。Conductor 也不会把较旧或
重复的传输序号、重复的 `event_id` 当成重复消息而拒绝，而是会处理收到的事件。

## 检查 Mooncake 到 Conductor 的配置

在允许请求创建缓存对象前，请按以下顺序操作：

1. 使用 `-DENABLE_KV_EVENTS=ON` 构建 Mooncake Store，启用发布端，保持
   `kv_events_emit_object_key=true`，并确认 `GET /kv_events/status` 返回
   `"enabled":true`。
2. 先注册至少一个 `vLLM` 引擎。它的 `tenant_id`、`modelname`、
   `lora_name` 和 `block_size` 必须等于 Mooncake Store 事件将携带的值。
   使用上文明确的 `PYTHONHASHSEED` 和 `--prefix-caching-hash-algo` 设置启动
   每个引擎。确认 `/global_view` 中能看到该引擎和每个预期 rank。
3. 该 vLLM 范围和 `Mooncake` 注册必须使用同一份完整 `hash_profile`：
   `strategy`、`algorithm`、准确的 `python_hash_seed` 和 `index_projection`
   必须完全相同。Conductor 会派生并验证根摘要；任一发布端的事件都不会提供它。
4. 为 Mooncake 发布端配置非空 `backend_id`、可用的模型备用值、正数块大小，
   以及相同的 LoRA 名称。注意，从 `object_key` 解析出的模型会覆盖备用值。
5. 每个对象 key 都应使用可识别的 connector 格式，并包含完整、有效的十六进制
   哈希。Conductor 能接受的 `stored` 必须同时包含 `object_key` 和
   `connector_block_hash`。
6. 不使用 group 或只使用 group zero，并且 Mooncake 来源只上报 CPU 或 Disk
   可用性。Conductor 会忽略其他介质，并拒绝非零 group。
7. 注册 Mooncake endpoint，然后在 `/services` 中检查所有预期的 vLLM 和
   Mooncake 订阅。在 `/services` 和 `/global_view` 中同时核对配置种子和派生
   根摘要。使用静态配置时，各订阅会同时启动；两项检查都通过前，应暂停产生
   缓存的流量。
8. 放开缓存流量，确认 Mooncake 发布端计数增加，再查询一个已知 token 前缀。
   共享 `cpu` 或 `disk` 计数必须出现在兼容的 vLLM instance 下，而不是
   Mooncake 注册名下。

`/services` 只能说明 Conductor 接受了注册，不能证明 ZMQ 发布端已经送达事件，
也不能证明订阅前创建的对象已经被记录。完整的注册和查询命令见
[Conductor 使用指南](../conductor/usage.md)。

## 了解当前完整性限制

Mooncake connector key 可以携带层编号，以及张量并行（TP）、prefill 上下文
并行（PCP）、decode 上下文并行（DCP）和流水线并行（PP）rank。Conductor
会读取这些字段，但目前不会先检查是否已经收到所有必需的层或并行部分，再报告
块可用。它也不会把 `cache_prefix` 或 Mooncake `additional_salt` 作为四个
缓存共享字段之一。

## 维护者源码索引

消息解析实现在 `mooncake-conductor/src/zmq/msg_decoder.cpp` 中；传输序号和
重连处理位于 `mooncake-conductor/src/zmq/zmq_client.cpp`。事件检查和清理位于
`mooncake-conductor/src/kvevent/event_handler.cpp` 和 `event_manager.cpp`。
当前解析和端到端测试数据位于 `mooncake-conductor/tests/msg_decoder_test.cpp`
和 `event_ingest_integration_test.cpp`。
