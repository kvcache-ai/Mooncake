# 发布 Mooncake KV Event

[English](../../../design/kv-event/publisher-design.md)

Mooncake Master 可以向 Conductor 和其他订阅端实时发布键值（KV）缓存变化。
本页介绍如何启用发布端、确认它是否正在发送消息，以及如何理解它实际生成的
MessagePack 字段。本文也会说明影响 Conductor 的对象 key 要求和消息传递限制。

## 启用并检查发布端

在项目根目录中构建启用了 KV Event 的 `mooncake_master`：

```bash
cmake -S . -B build -DENABLE_KV_EVENTS=ON
cmake --build build --target mooncake_master -j
```

启动 Master 时，需要提供可用的后端名称、模型备用值、块大小和 ZeroMQ
（ZMQ）绑定地址。下面的命令使用管理 HTTP 端口 `9003`：

```bash
./build/mooncake-store/src/mooncake_master \
  --enable_kv_events=true \
  --kv_events_bind_endpoint=tcp://0.0.0.0:5557 \
  --kv_events_backend_id=pool-a \
  --kv_events_model_name=Qwen/Qwen2.5-7B-Instruct \
  --kv_events_block_size=16 \
  --kv_events_emit_object_key=true \
  --metrics_port=9003
```

启动成功后，日志中会出现包含以下内容的一行：

```text
kv_events publisher enabled on tcp://0.0.0.0:5557 backend_id=pool-a
```

然后读取发布端状态：

```bash
curl -s http://127.0.0.1:9003/kv_events/status
```

在缓存流量开始前，健康的发布端会返回下面这种结构。如果此前已经执行过缓存
操作，计数也可能已经不是零：

```json
{"enabled":true,"published_batches":0,"published_events":0,"dropped_events":0,"skipped_unparsed_keys":0,"invalid_event_hashes":0}
```

先确认 `enabled` 为 `true`。执行缓存操作后，再确认 `published_events` 以及
通常情况下的 `published_batches` 有所增加。即使没有订阅端，这些计数也会
增加，因此该检查只能证明 Master 已向自己的 ZMQ 发布（PUB）socket 发送
消息，不能证明 Conductor 已经收到消息。

## 了解构建要求

`-DENABLE_KV_EVENTS=ON` 需要 libzmq 头文件和库。如果 CMake 找不到它们，
配置阶段会直接报错。未开启该选项的构建仍保留 Store 公共调用，但使用的是
空实现；运行时参数无法把这个空实现变成真正的发布端，状态会显示
`"enabled":false`。

如果 `kv_events_bind_endpoint` 或 `kv_events_backend_id` 为空，或者创建、
绑定 ZMQ socket 失败，发布端同样会把 `enabled` 设为 `false`。此时应查看
Master 日志。

## 选择 Master 设置

这些设置既可以写进 Master 配置文件，也可以通过命令行参数传入。显式传入的
命令行值会覆盖 `--config_path` 所加载的值。

| 设置 | 默认值 | 何时会用到 |
|---|---:|---|
| `enable_kv_events` | `false` | 必须设为 `true` 才会创建发布端。 |
| `kv_events_bind_endpoint` | 空 | Master 绑定的 ZMQ PUB 地址，例如 `tcp://0.0.0.0:5557`。向 Conductor 注册时应使用 `tcp://master-host:5557` 这类可连接的地址，不能使用 `0.0.0.0`。 |
| `kv_events_backend_id` | 空 | 必须是非空的 Mooncake 后端名称，用于标识上报这些对象的后端。Conductor 会用它限制 Remove 和 Clear 的清理范围。 |
| `kv_events_model_name` | 空 | 无法识别 key 以及 `cleared` 事件所用的备用模型名。从 connector key 解析出的模型名优先。Conductor 要求 `stored` 事件具有非空模型名。 |
| `kv_events_block_size` | `0` | 写入事件的固定 token 数。`0` 会发送为 `nil`；如果 `stored` 的块大小缺失或不是正数，Conductor 会拒绝该事件。 |
| `kv_events_lora_name` | 空 | 固定的低秩适配（LoRA）名称。空值表示基础模型，并发送为 `nil`。 |
| `kv_events_additional_salt` | 空 | 固定字符串，空值发送为 `nil`。Conductor 会读取它，但不会把它用于缓存共享或查询。 |
| `kv_events_dp_rank` | `0` | 写入每个事件和批次的无符号数据并行（DP）值。Conductor 只保留 Mooncake DP 值用于排查问题，不把它当作推理引擎的 rank。 |
| `kv_events_tenant_id` | `default` | 为兼容配置而保留。事件中的 tenant 来自每次 Store 操作；操作中的空 tenant 会变成 `default`。 |
| `kv_events_emit_object_key` | `true` | 在对象事件中加入 `object_key`。与 Conductor 配合时应保持为 `true`，因为 Conductor 需要该 key 才能准确删除对象。 |
| `kv_events_emit_legacy_compat` | `true` | 加入对应的旧字段 `type`、`block_hashes`，以及 stored 事件的 `parent_block_hash`。只要它们与主要字段一致，Conductor 就会接受。 |
| `kv_events_queue_capacity` | `65536` | 发布端队列中最多等待处理的事件数。正数会限制队列长度；`0` 表示不设该限制。参见[规划事件丢失](#规划事件丢失)。 |

`kv_events_queue_capacity` 的命令行帮助目前声称该设置会被忽略，但发布端实现
确实会执行这个限制。上表描述的是当前实际运行行为。

## 读取发布端状态

`GET /kv_events/status` 由 Master 的管理端口提供。该端口通过
`metrics_port` 配置，默认值为 `9003`。响应字段如下：

| 字段 | 含义 |
|---|---|
| `enabled` | 当前 Master 已编译发布功能、完成配置并成功绑定发布地址。 |
| `published_batches` | 三次 ZMQ 帧发送均返回成功的多帧消息数。 |
| `published_events` | 上述成功发送批次中包含的事件数。 |
| `dropped_events` | 等待队列已满时被移除的事件数，加上 ZMQ 发送失败批次中的事件数。 |
| `skipped_unparsed_keys` | 没有序列哈希、并且由于无法发送对象 key 而被跳过的事件，加上 key 中没有可识别块哈希部分但仍被发送的事件。后一种消息包含 `object_key`，却没有 connector hash，因此 Conductor 会拒绝其中的 `stored` 事件。 |
| `invalid_event_hashes` | connector key 已被识别，但其中的块哈希文本无法转成无符号 64 位值。事件仍可能带着对象 key 发出，但不会包含 `connector_block_hash`。 |

计数为零不能证明订阅端已连接。同样，PUB socket 在本地发送成功后仍可能丢失
消息，因此这些计数不能证明 Conductor 已经收到事件。

## 了解三帧 ZMQ 消息

每次发布都是一条恰好包含三帧的 ZMQ 多帧消息：

| 帧 | 准确内容 |
|---:|---|
| 1 | 空 topic 帧。 |
| 2 | 一个按大端序编码的无符号 64 位传输序号。 |
| 3 | 一个 MessagePack payload：`[timestamp_ms, [event_maps], dp_rank]`。 |

发布端最多把 64 个等待事件放进一个 payload。同一 payload 中的所有事件使用
同一个有符号 64 位 Unix 毫秒时间戳。`dp_rank` 是配置的无符号 32 位值。
发布端为每个尝试发送的批次分配一个序号；队列丢弃事件时还会额外保留序号。
每次发布端进程启动后都从 `1` 开始，因此队列丢失会有意留下序号空缺。

payload 中三项的含义如下：

| 项 | MessagePack 形式 | 含义 |
|---:|---|---|
| `timestamp_ms` | 非负整数 | 批次创建时的 Unix 毫秒时间。 |
| `event_maps` | map 数组 | 事件顺序与发布端从队列取出的顺序相同。 |
| `dp_rank` | 无符号整数 | `kv_events_dp_rank` 的副本；Conductor 只将它用于 Mooncake 路径的问题排查。 |

## 读取通用事件字段

每个 `stored`、`removed` 和 `cleared` map 都包含以下主要字段：

| 字段 | MessagePack 形式 | 来源和用途 |
|---|---|---|
| `event_id` | 无符号 64 位整数 | 当前发布端进程中递增的事件编号。进程重启后会重新开始。 |
| `timestamp` | 有符号 64 位整数 | 与外层 `timestamp_ms` 相同的毫秒值。 |
| `event_type` | 字符串 | 只能是 `stored`、`removed` 或 `cleared`。 |
| `model_name` | 字符串或 `nil` | 如果成功解析 connector key，则使用其中的模型；否则使用 `kv_events_model_name`。 |
| `block_size` | 无符号整数或 `nil` | 来自 `kv_events_block_size`；`0` 会变成 `nil`。 |
| `additional_salt` | 字符串或 `nil` | 来自 `kv_events_additional_salt`；空值会变成 `nil`。 |
| `lora_name` | 字符串或 `nil` | 来自 `kv_events_lora_name`；空值会变成 `nil`。 |
| `tenant_id` | 字符串 | Store 操作使用的 tenant；空值会变成 `default`。 |
| `backend_id` | 字符串 | 来自 `kv_events_backend_id`。 |
| `medium` | 字符串或 `nil` | `stored` 和 `removed` 的对象位置；`cleared` 使用 `nil`。 |
| `dp_rank` | 无符号整数 | 来自 `kv_events_dp_rank`。 |

当 `kv_events_emit_legacy_compat=true` 时，每个 map 还会包含 `type`：
`BlockStored`、`BlockRemoved` 或 `AllBlocksCleared`，并与 `event_type`
对应。

`stored` 和 `removed` map 还包含：

| 字段 | MessagePack 形式 | 来源和用途 |
|---|---|---|
| `group_id` | 字符串或 `nil` | 如果 connector 中带有 `group:N`，则使用解析结果；否则使用 Store 对象的 group 字符串。空值会变成 `nil`。 |
| `seq_hashes` | 包含零个或一个无符号整数的数组 | 从对象 key 解析出的低 64 位。无法解析的 key 会生成空数组。 |
| `base_block_idx` | `nil` | connector key 不记录该块在 token 链中的深度。 |
| `object_key` | 字符串，按配置决定是否存在 | 完整 Mooncake key，仅当 `kv_events_emit_object_key=true` 时发送。 |
| `block_hashes` | 数组，按配置决定是否存在 | `seq_hashes` 的旧版副本，仅当兼容字段开启时发送。 |

`stored` map 还包含 `parent_hash` 和 `token_ids`，两者都是 `nil`，因为
当前 connector key 不含这两个值。开启旧版字段时，它还包含
`parent_block_hash=nil`。`removed` map 不包含这些只属于 stored 的字段。
`cleared` map 不包含 group、哈希、对象或并行拓扑字段。

如果成功识别 connector key，发布端还会加入该 key 实际提供的字段：

| 字段 | 何时存在 |
|---|---|
| `connector_block_hash` | 完整块哈希文本是有效十六进制，并成功生成了 `seq_hashes[0]`。 |
| `cache_prefix` | connector 模型名前存在文本。 |
| `tp_rank` | 解析出了 vLLM 张量并行 rank。 |
| `head_or_tp_rank` | 解析出了 vLLM Ascend head 或张量并行 rank。 |
| `pcp_rank` | 解析出了 prefill 上下文并行 rank。 |
| `dcp_rank` | 解析出了 decode 上下文并行 rank。它不是 `dp_rank`。 |
| `pp_rank` | 解析出了流水线并行 rank。 |
| `layer_id` | Ascend 按层 key 中包含层编号。 |

## 理解 `stored`、`removed` 和 `cleared`

这三个事件名表示逻辑上的可用性，不表示物理副本的数量或地址：

| 事件 | Mooncake 发布端表达的含义 | 当前 Conductor 所需字段 |
|---|---|---|
| `stored` | 该对象可从指定的 `cpu` 或 `disk` 介质读取。重复成功的 Put 或 Upsert 提交可能再次上报相同可用性。 | `backend_id`、非空的 `tenant_id` 和 `model_name`、正数 `block_size`、受支持的介质和 group、`object_key`，以及可用的完整 `connector_block_hash`。`lora_name` 可以为空。`seq_hashes` 可以为空；如果有值，则必须与完整哈希一致。 |
| `removed` | 该对象已不能从指定介质读取。它在另一种介质上仍可能可用。 | `backend_id`、受支持的介质和 group，以及 `object_key`。可以不带完整哈希，因为 Conductor 会查找此前 `stored` 保存的对象记录。 |
| `cleared` | 该发布端的 `backend_id` 与事件 `tenant_id` 下的所有对象都已清空。它同时作用于 CPU 和 Disk，并使用 `medium=nil`。 | 非空的 `backend_id` 和 `tenant_id`，且不能带对象字段。 |

开启对象 key 发送后，即使发布端无法完整解析某个 key，也仍可能发送对应的
`stored` 事件。这种事件没有可用的 `connector_block_hash`；当前 Conductor
会记录拒绝日志，不会把它加入共享缓存索引。

## 使用 Conductor 可匹配的 connector key

Mooncake 不会给 Store 客户端调用新增 KV Event 参数，而是从 vLLM connector
生成的 key 中读取信息。可识别的 vLLM 格式如下：

```text
[cache_prefix@]model_name@tp_rank:N@pcpN@dcpN@pp_rank:N@group:N@block_hash_hex
[cache_prefix@]model_name@tp_rank:N@pcpN@dcpN@pp_rank:N@block_hash_hex
```

第二种格式适用于还没有 `group:N` 字段的旧 connector 版本。可识别的 vLLM
Ascend 格式如下：

```text
model_name@pcpN@dcpN@head_or_tp_rank:N@pp_rank:N@block_hash_hex
model_name@pcpN@dcpN@head_or_tp_rank:N@block_hash_hex@layer_id
```

分隔符是没有转义机制的 `@`。在前两种格式中，`tp_rank` 前一个片段会被
当作模型名，更前面的片段会成为 `cache_prefix`。Mooncake Store 不会拒绝
未知或格式错误的 key。发布端仍可将它们作为 `object_key` 发出，但它们无法
提供 Conductor 接受 `stored` 时所需的完整 connector hash。

对 Conductor 而言，`block_hash_hex` 必须是表示至少八个字节的有效十六进制
文本：去掉可选的 `0x` 前缀后，长度至少为 16 个字符，并且字符数必须为
偶数。发布端在 `connector_block_hash` 中保留完整文本，并把最右侧 16 个
十六进制字符转成 `seq_hashes[0]`。Conductor 按大端序读取最后八个字节，
得到 `/query` 使用的 64 位查询值。

完整哈希仍然有用。Conductor 同时保存完整哈希和对象 key，因此两个完整哈希
不同、但最后八个字节相同的对象仍可以分别删除。查询只使用 64 位值，所以只要
其中任一对象存在，就会报告可能命中。可接受的编码和不一致处理方式见
[订阅端哈希规则](./subscriber-guide.md#转换收到的哈希)。

`cache_prefix`、各种 rank 字段和 `layer_id` 用来描述 connector 的 key
空间。发布端无法判断多少层或多少个并行部分才组成一个完整块；当前 Conductor
也不会检查这种完整性。

## 了解 CPU 和 Disk 变化

Master 会把副本信息归纳成两个可用性结果：对象能否从 CPU 读取，以及能否从
Disk 读取。它会按以下规则发出事件：

| Store 操作 | 事件结果 |
|---|---|
| Put 或 Upsert 成功提交 | 针对当前每种可用介质各发送一个 `stored`。 |
| Upsert 期间旧值变得不可读 | 先针对旧可用性发送 `removed`，提交时再发送 `stored`。 |
| Copy、Move、offload、promotion、eviction 或副本清理 | 某种介质首次可用时发送 `stored`；该介质最后一个可用副本消失时发送 `removed`。只要介质仍可用，仅改变副本数量或位置就不会产生可用性变化。 |
| Remove、BatchRemove 或正则删除 | 针对对象此前可用的每种介质各发送一个 `removed`。 |
| RemoveAll | 先逐对象发送 `removed`；仅当 tenant 中没有剩余对象时再发送 `cleared`。 |
| 新 Put 未提交且失败 | 不发送事件。 |

如果一个对象同时可从 CPU 和 Disk 读取，发布端会针对两种介质分别发送事件。

## 规划事件丢失

发布过程是异步的。正数 `kv_events_queue_capacity` 会限制等待队列长度。队列
已满时，发布端会移除最旧的等待事件、增加 `dropped_events`，并保留一个
传输序号，让订阅端能够看到序号空缺。ZMQ 发送失败也会按受影响的事件数增加
`dropped_events`。

发布端只提供 PUB socket：它没有 replay endpoint，无法重发漏掉的事件；
Conductor 连接时，它也不会发送此前已缓存对象的清单。发布端进程只在内存中
保存精简的对象与介质对应关系，不会持久化。后续 Store 变化可以为已有对象
生成新事件，但不能借此还原完整的启动状态。

传输序号跳跃时，Conductor 会警告并保留当前缓存记录；警告本身不会补回漏掉的
变化。在 Conductor 中看到所需注册之前，应暂停产生缓存的流量。完整启动清单
见[订阅指南](./subscriber-guide.md#检查-mooncake-到-conductor-的配置)。

## 维护者源码索引

消息构建和队列位于 `mooncake-store/src/kv_event/kv_event_publisher.cpp`，
connector key 解析位于 `mooncake-store/include/kv_event/key_util.h`。
Master 参数和配置加载位于 `mooncake-store/src/master.cpp`，
`GET /kv_events/status` 位于
`mooncake-store/src/master_admin_service.cpp`。对应测试数据在
`mooncake-store/tests/kv_event_publisher_test.cpp` 中。
