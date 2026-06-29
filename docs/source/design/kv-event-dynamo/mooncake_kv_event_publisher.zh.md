# Mooncake KV Event Publisher

本文介绍 Mooncake KV Event Publisher：使 Mooncake 能够发布 Dynamo 兼容的 KV cache
event，从而让 Dynamo 的 router 基于存储在 Mooncake 中的 block 构建 prefix index。
文档覆盖该特性的两个接口——推理引擎（SGLang）如何把语义信息**输入**到 Mooncake，
以及 Mooncake 如何通过 ZMQ 把事件**输出**出去——并概述内部实现。本文作为后续 PR
的设计/参考说明。

## 1. 概述

Mooncake 是**中心化**的 KV cache 管理者：master 掌握每个逻辑 KV block 的完整状态。
Dynamo 的 router 通过消费 KV event 维护其 prefix index，其参考引擎（SGLang/vLLM）
是**每个 worker 一条 ZMQ 流**。由于 Mooncake 状态集中管理，我们用**一条 ZMQ PUB 流
承载所有 worker 的事件**，并通过每条事件中的 `worker_id` 字段标识 block 的物理归属；
Dynamo 侧由一个 Mooncake 专用 adapter 解析该字段。（详见 `mooncake开发方案.md` §7.3。）

端到端流程：

```text
SGLang（掌握语义）                        Mooncake master（中心化状态）                  Dynamo
  put(key, value, ReplicateConfig{        - 将多个物理对象聚合为                        ZMQ SUB
       group_ids, kv_event_metadata })  →   一个逻辑 block（manifest）              →   + adapter
                                           - 完整时 -> BlockStored                  →   prefix
                                           - 移除时 -> BlockRemoved                      index
                                           - 通过一条 ZMQ PUB 流发布
```

两个关键接口：

- **输入 —— SGLang → Mooncake（§2）：** 写入时携带 `kv_event_metadata`，描述逻辑
  block 的语义（token id、各类 hash、物理布局）。这是 Mooncake 获取其自身无法推导
  的信息的唯一途径。
- **输出 —— Mooncake → Dynamo（§3）：** 一条 ZMQ PUB 流，承载 msgpack 事件。

当写入未携带 `kv_event_metadata` 且未安装 publisher 时，Mooncake 行为完全不变。

## 2. 输入接口：SGLang 如何向 Mooncake 传递语义

Mooncake 存储的是不透明字节，它不知道 token id、sequence hash，也不知道一个逻辑
KV block 由几个物理对象组成。只有引擎知道这些。因此 SGLang 通过扩展 `ReplicateConfig`
把语义信息附加到每次写入上。

### 2.1 逻辑 block 与物理对象

一个逻辑 KV block（例如一个 SGLang prefix page）可能被存为多个物理 Mooncake 对象——
比如一个 MHA page 的 `K` 与 `V` 对象，或若干个 per-head 的 component 对象。模型如下：

- `ReplicateConfig::group_ids` —— 每个物理 key 对应一个 `group_id`，把同一逻辑 block
  的物理对象绑在一起（分组的 key 本就共享元数据路由、合并的 lease 刷新与淘汰行为）。
- `ReplicateConfig::kv_event_metadata` —— group 级别，**非** per-key：每个 `group_id`
  对应一个 `KvBlockEventMetadata`，携带该 block 的语义与期望的物理布局，使 master
  知道逻辑 block 何时被完整存储。

### 2.2 扩展后的 `ReplicateConfig`（`replica.h`）

```cpp
struct ReplicateConfig {
    // ... 既有字段 ...
    std::optional<std::vector<std::string>> group_ids{};
    // 可选的 group 级 KV event 元数据。非 per-key：每个 item 描述一个由 group_id
    // 标识的逻辑 Dynamo KV block。缺省 => 行为不变。
    std::optional<std::vector<KvBlockEventMetadata>> kv_event_metadata{};
};
```

`KvBlockComponentSpec` —— 逻辑 block 的一个物理对象：

| 字段 | 类型 | 含义 |
|------|------|------|
| `object_key` | str | 物理 Mooncake 对象 key。 |
| `component_role` | str | 例如 `"k"`、`"v"`、`"mla_k"`、`"head_3_k"`。 |
| `component_index` | u32 | block 内稳定的排序索引。 |

`KvBlockEventMetadata` —— 一个逻辑 block（一个 `group_id`）：

| 字段 | 类型 | 含义 |
|------|------|------|
| `schema_version` | u32 | 元数据 schema 版本（当前为 `1`）。 |
| `group_id` | str | 逻辑 block id；须与 `group_ids` 中的某项一致。 |
| `block_hash` | u64 | Dynamo 外部 sequence block hash。 |
| `parent_block_hash` | u64? | 父 sequence hash；根 block 不设置。 |
| `token_ids` | `[u32]` | block 内 token id（Dynamo 据此计算 local token hash）。 |
| `block_size` | u32 | 每个 block 的 token 数；须与消费者的 `kv_block_size` 一致。 |
| `dp_rank` | u32? | data-parallel rank（路由命名空间）。 |
| `model_name` | str | 模型名（路由命名空间）。 |
| `lora_name` | str | LoRA adapter 名（路由命名空间）。 |
| `additional_salt` | str | 额外命名空间 salt。 |
| `expected_object_count` | u32 | 完整该 block 需要多少个物理对象。 |
| `expected_components` | `[KvBlockComponentSpec]` | 期望的物理对象（优先；设置后 `expected_object_count` 由其推导）。 |
| `emit_stored_event` | bool | 是否发布 `BlockStored`（默认 true）。 |
| `emit_removed_event` | bool | 是否发布 `BlockRemoved`（默认 true）。 |

完整性判定：提供 `expected_components` 时以其为准，否则用 `expected_object_count`。
对同一 `group_id` 的重复写入，若语义相同（`SameSemanticsAs`）则视为幂等重试接受，
否则视为冲突拒绝。

### 2.3 SGLang 提供 vs. Mooncake 推导

| 信息 | 提供方 |
|------|--------|
| `token_ids`、`block_hash`、`parent_block_hash`、`block_size` | **SGLang**（仅其掌握的语义） |
| `model_name`、`lora_name`、`dp_rank`、`additional_salt` | **SGLang**（路由命名空间） |
| 物理布局（`group_ids`、`expected_components`） | **SGLang** |
| `worker_id`（物理归属） | **Mooncake**（由副本放置推导；见 §4.2） |
| `event_id`、`source`、`tenant_id`、`medium` | **Mooncake** |
| 完整性 / 去重 / stored↔removed 生命周期 | **Mooncake**（manifest） |

### 2.4 Python 绑定（`store_py.cpp`）

`KvBlockComponentSpec`、`KvBlockEventMetadata` 与
`ReplicateConfig.kv_event_metadata` 均已暴露给 Python。示例：一个逻辑 block 存为两个
物理对象（K 与 V），在两者都到位后恰好发布一条 `BlockStored`。

```python
from mooncake.store import (
    ReplicateConfig, KvBlockEventMetadata, KvBlockComponentSpec,
)

key_k, key_v = "blk-42:k", "blk-42:v"

c_k = KvBlockComponentSpec(); c_k.object_key = key_k; c_k.component_role = "k"; c_k.component_index = 0
c_v = KvBlockComponentSpec(); c_v.object_key = key_v; c_v.component_role = "v"; c_v.component_index = 1

meta = KvBlockEventMetadata()
meta.group_id = "blk-42"
meta.block_hash = 0x8f3a_0000_0000_0001
meta.parent_block_hash = 0x8f3a_0000_0000_0000   # 根 block 则不设置
meta.token_ids = [101, 102, 103, 104]
meta.block_size = 64
meta.model_name = "llama-3"
meta.dp_rank = 0
meta.expected_components = [c_k, c_v]              # 或：meta.expected_object_count = 2

config = ReplicateConfig()
config.replica_num = 1
config.group_ids = ["blk-42", "blk-42"]           # 每个物理 key 一项
config.kv_event_metadata = [meta]

store.put(key_k, value_k, config)
store.put(key_v, value_v, config)   # block 此时完整 -> 一条 BlockStored 事件
```

## 3. 输出接口：ZMQ 事件流

### 3.1 传输模型

**设计决策 —— 单条 ZMQ 流 + 每条事件携带 `worker_id`。** Dynamo 设计的 ZMQ 接口是
*一个 worker 连一个 ZMQ*：每个引擎 worker 自己跑一个 PUB socket，Dynamo 通过事件从
哪个 socket 到达来识别来源。但 Mooncake 是中心化的 KV cache 管理——只有 master 掌握
所有 worker 上每个 block 的状态，因此**一个 ZMQ 就足以把所有信息发过去**。我们有意
**保持用一个 ZMQ 流**，转而在发布的 event 中**额外增加 `worker_id` 字段**，用来表示
KV 实际上在哪个物理存储节点；Dynamo 那边之后再写一个 Mooncake 专用的 adapter 来解析
这个 `worker_id`，从而把每条事件归属到正确的 worker。这样既避免了在一个中心化组件上
为每个 worker 各起（并管理）一个 socket，又保留了 Dynamo 的 per-worker 路由语义。
（详见 `mooncake开发方案.md` §7.3。）

- master 持有**一个 PUB socket**；订阅方（Dynamo adapter）用 SUB socket 连接。
- 每次 `Publish()` 发送**一个单事件 batch**。该层不做缓冲/合并；排序与去重由上层
  manifest 逻辑负责。
- 每条事件内的 `worker_id` 告诉消费者该 block 在哪个物理节点。batch 层的 `dp_rank`
  也会被设置（取自事件），但 adapter 应以每条事件的字段为准。

### 3.2 线格式（3 帧）

与 Dynamo 已支持的 SGLang/vLLM ZMQ relay 格式字节兼容
（`dynamo_kv_event_rules.md` 的 "ZMQ Relay Wire Format" 一节）：

| 帧 | 内容 | 说明 |
|----|------|------|
| 1 | `topic` | 与订阅方的 `zmq_topic` 过滤器匹配；默认空字符串（匹配全部）。listener 要求总帧数恰好为 3。 |
| 2 | 8 字节**大端**序列号 | 每个 publisher 单调递增。Dynamo 仅用于 trace/log，**不**作为内部 `event_id`。 |
| 3 | msgpack payload | `[timestamp (f64), [events], dp_rank (i32 或 nil)]`。 |

payload 结构：

```text
[
  timestamp,        # f64，自 epoch 起的秒数（engine batch 时间戳）
  [ <map event> ],  # 原始 KV event 数组（此处长度恒为 1）
  dp_rank           # i32；未知时为 nil
]
```

### 3.3 map event 字段

每条事件是一个 msgpack **map**（Dynamo 按字段名解析，忽略未知字段）。
`BlockStored` 携带全部字段；`BlockRemoved` 仅携带身份字段 + `block_hashes` + `medium`。

公共字段（两种类型都有）：

| 字段 | 类型 | 含义 |
|------|------|------|
| `type` | str | `"BlockStored"` 或 `"BlockRemoved"`。 |
| `block_hashes` | `[u64]` | 外部 sequence block hash（前缀累计）。 |
| `medium` | str | 存储层级，例如 `"EXTERNAL"`。 |
| `event_id` | str | master 分配的唯一 id（`mooncake:<tenant>:<group>:<kind>:<seq>`）。 |
| `source` | str | 恒为 `"mooncake"`。 |
| `tenant_id` | str | Mooncake 租户。 |
| `group_id` | str | 逻辑 KV block group id。 |
| `worker_id` | str | 持有该 block 的物理 worker（见 §4.2）。 |

`BlockStored` 专有字段：`parent_block_hash`（u64 或 nil）、`token_ids`（`[u32]`）、
`block_size`（u32）、`lora_name`（str）、`model_name`（str），以及 `dp_rank`（u32，
仅在事件设置了该值时出现）。

> `source`、`tenant_id`、`group_id`、`worker_id`、`event_id` 是在 Dynamo 基础 schema
> 之上的 Mooncake 扩展字段。原生 Dynamo 解码器会忽略未知字段；负责解析 `worker_id`
> 的是 Mooncake 专用 adapter。

### 3.4 Publisher C++ API（`kv_event_zmq_publisher.h`）

```cpp
struct ZmqKvEventPublisherConfig {
    std::string endpoint{"tcp://0.0.0.0:5557"}; // "tcp://host:*" 自动选空闲端口
    std::string topic{};                         // 帧 1 topic；空表示匹配全部
    bool bind{true};                             // true：bind（master 作服务端）
    int linger_ms{0};                            // 关闭 linger；-1 阻塞，0 丢弃
    int send_hwm{0};                             // 发送高水位；0 表示不限
};

class ZmqKvEventPublisher : public KvEventPublisher {
   public:
    explicit ZmqKvEventPublisher(const ZmqKvEventPublisherConfig& config);
    void Publish(const KvEvent& event) override;
    const std::string& endpoint() const;   // 解析后的端点（含通配端口）
    uint64_t published_count() const;       // 诊断用
};
```

在 master 上安装（任何 `KvEventPublisher` 实现均可；传 `nullptr` 关闭发布）：

```cpp
auto publisher = std::make_shared<ZmqKvEventPublisher>(
    ZmqKvEventPublisherConfig{.endpoint = "tcp://0.0.0.0:5557"});
master_service.SetKvEventPublisher(publisher);
```

## 4. 内部实现

### 4.1 Group manifest 与事件生命周期（`master_service.{h,cpp}`）

- 每个携带 `kv_event_metadata` 的 `group_id` 会得到一个 **manifest**，与其成员存放在
  同一 tenant shard（从而受既有 shard 锁保护）。它跟踪期望的对象 key、当前已到位的
  key，以及 per-worker 的去重记录。
- `PutEnd` 标记某对象完整；当所有期望 component 到位时，master 恰好发布一条
  `BlockStored`（通过 `stored_event_published_workers` 去重）。
- 移除/淘汰会使逻辑 block 变为不完整，并发布一条 `BlockRemoved`（通过
  `removed_event_published_workers` 去重）；当最后一个 group 成员被移除时擦除 manifest。
- 校验会拒绝对某 `group_id` 的冲突性重定义，以及非法元数据（空/重复 group id、缺少
  完整性期望）。

### 4.2 segment→worker_id 注册表（内部）

`worker_id` 必须标识**存储该 block 的物理 worker**，而非用于分配的逻辑 segment 名。
内部注册表按以下优先级解析：

1. **显式覆盖** —— `MasterService::RegisterSegmentWorkerId(segment, id)`（部署配置 /
   控制面），优先级最高。
2. **挂载时端点** —— segment 挂载时自动捕获，取 `Segment::te_endpoint`（节点 ip:port）；
   端点为空时回退到 segment 名。
3. **兜底** —— 注册表中无此 segment 时，用 segment 名本身。

实现要点：

- 两个 map（`segment_worker_explicit_`、`segment_worker_auto_`）由独立的 `shared_mutex`
  保护，从而可在持有 tenant shard 锁时读取。
- `MountSegment` 在挂载成功并释放 segment 锁后，自动注册 `segment.name → te_endpoint`。
- `PutEnd` 流程：`segment = DeriveSegmentNameFromMetadata(...)` →
  `worker_id = ResolveWorkerId(segment)` → 打入事件。（`GetSegmentWorkerId` 暴露映射
  供诊断/测试使用。）
- 注册表为内存态，remount 时重建；**尚未**纳入 HA snapshot（见 §7）。

### 4.3 事件模型与 ZMQ publisher

- `KvEvent` / `KvEventType` 以及 msgpack 编解码函数（`EncodeKvEventMap`、
  `EncodeKvEventBatchPayload`、`DecodeKvEventBatchPayload`）位于 `kv_event.{h,cpp}`，
  由 ZMQ publisher 与测试中的内存 mock 共用。
- `ZmqKvEventPublisher` 采用 **pimpl** 手法，避免 `zmq.hpp` 泄漏进公共头；`Impl` 持有
  `zmq::context_t` 与 PUB `zmq::socket_t`，通过 `ZMQ_LAST_ENDPOINT` 解析具体端点，
  并在 `Publish()` 中构造单事件 batch、前置单调大端序列号，在 mutex 保护下发送 3 帧。
  ZMQ 发送错误仅记录日志并吞掉（发布是 best-effort，绝不能影响数据路径）。

## 5. 构建集成

`mooncake-store/src/CMakeLists.txt` 自动探测 ZeroMQ（cppzmq 头 `zmq.hpp` + `libzmq`，
带 `$CONDA_PREFIX` 提示路径）：

- 找到时：编译 `kv_event_zmq_publisher.cpp`，并把 `MOONCAKE_STORE_WITH_ZMQ`、include
  目录、链接库作为 `mooncake_store` 的 **PUBLIC** 使用要求传播（测试 / 消费方可继承）。
- 未找到时：manifest/编码器仍可构建，仅省略实时 ZMQ publisher。ZMQ 相关代码请用
  `#ifdef MOONCAKE_STORE_WITH_ZMQ` 包裹。

依赖（例如 conda 环境）：`cppzmq` + `zeromq`。

## 6. 测试

`mooncake-store/tests/master_service_test.cpp`：

- manifest 生命周期：创建/完整、跨 group 成员共享、冲突拒绝、group 移除时擦除、
  config 校验。
- 发布：完整时一条 `BlockStored`、重复 `PutEnd` 不重复发布、删除时一条 `BlockRemoved`、
  无 publisher 时为空操作。
- worker_id：`SegmentWorkerRegistryCapturesEndpointAtMount`、
  `KvEventWorkerIdReflectsSegmentEndpoint`、`KvEventWorkerIdExplicitOverrideWins`。
- 传输：`KvEventEncoderRoundTrip`，以及 `ZmqKvEventPublisherRoundTrip`
  （由 `MOONCAKE_STORE_WITH_ZMQ` 保护）——真实 SUB socket 经 TCP 收到 3 帧并解码还原为
  原始 `KvEvent`。

## 7. 局限与后续工作

### 7.1 本次改动的局限

- **HA snapshot**：KV group manifest 与 segment→worker_id 注册表均尚未纳入 master HA
  snapshot。故障切换后注册表可由 remount 重建；但 manifest 去重状态仍需 snapshot 支持
  才能在跨切换时完全正确。
- **批处理**：当前一条 ZMQ 消息一个事件。若吞吐需要，后续可在单 payload 内批量多事件
  （编码器已支持）。

下面两块用于打通端到端链路，但位于各自仓库（SGLang / Dynamo），不在 Mooncake 本次 PR
范围内；在此记录是为了让完整数据流清晰。

### 7.2 SGLang → Mooncake 接入（引擎侧，独立仓库）

输入接口（§2）的 Mooncake 侧已完成，但 SGLang 尚未填充。SGLang 仓库的后续工作：

- 在 KV-cache 写入时，为每个逻辑 block 构造一个 `KvBlockEventMetadata`，连同匹配的
  `group_ids` 一起附加到传给 `store.put` / batch put 的 `ReplicateConfig` 上。
- 将 SGLang block manager 的状态映射到 schema：外部 **sequence** block hash →
  `block_hash` / `parent_block_hash`（前缀累计 hash，而非单 block 的 local hash），
  block 的 `token_ids`，以及 `block_size`（须等于 Dynamo 的 `kv_block_size`）。
- 把每个 block 的物理对象枚举为 `expected_components`（`object_key` +
  `component_role` + `component_index`），例如一个 MHA page 的 K 与 V 对象，或 per-head
  component。为路由命名空间设置 `model_name` / `lora_name` / `dp_rank` /
  `additional_salt`。
- 按部署决定 `emit_stored_event` / `emit_removed_event`，并确保对同一 `group_id` 复用
  （幂等重试）而非重定义。

### 7.3 Dynamo Mooncake adapter（router 侧，独立仓库）

由中心化 publisher 用一条 ZMQ 流承载事件，需要 Dynamo 侧的 Mooncake 专用 adapter
（对应 §3.1 的设计决策）。Dynamo 仓库的后续工作：

- 用 SUB socket 与匹配的 `zmq_topic` 订阅 Mooncake 的单一 PUB 端点，解码 3 帧消息与
  `[timestamp, [events], dp_rank]` payload。
- **解析每条事件的 `worker_id`**，据此把每条 `BlockStored` / `BlockRemoved` 归属到对应
  物理 worker，而不是从 socket/连接推断 worker（默认 one-socket-per-worker 路径所内置的
  假设）。应以每条事件的 `worker_id`（与 `dp_rank`）为准，而非 batch 末尾的字段。
- 将 Mooncake 扩展字段（`source`、`tenant_id`、`group_id`、`event_id`）映射进 Dynamo
  内部的 `RouterEvent` 模型，并把 `medium` → storage tier。遵守 Dynamo 自身的
  `block_size == kv_block_size` 以及重新编号/去重规则。
- 帧 2 的序列号仅作 trace/log；排序依赖 Mooncake 的 `event_id` 与 Dynamo 自身的单调 id。

## 8. 改动 / 新增文件

| 文件 | 改动 |
|------|------|
| `include/replica.h` | `KvBlockComponentSpec`、`KvBlockEventMetadata`、`ReplicateConfig::kv_event_metadata`。 |
| `mooncake-integration/store/store_py.cpp` | 元数据类型 + `kv_event_metadata` 的 Python 绑定。 |
| `include/kv_event.h`、`src/kv_event.cpp` | 事件模型 + msgpack 编解码。 |
| `include/kv_event_zmq_publisher.h`、`src/kv_event_zmq_publisher.cpp` | **新增。** ZMQ PUB 传输（pimpl）。 |
| `include/master_service.h`、`src/master_service.cpp` | group manifest、事件发布/去重、segment→worker_id 注册表、`SetKvEventPublisher` / `Register`/`GetSegmentWorkerId`。 |
| `src/rpc_service.cpp` | RPC 入口处的 `kv_event_metadata` config 级校验。 |
| `src/CMakeLists.txt` | ZeroMQ 探测；条件源文件 + PUBLIC include/lib/`MOONCAKE_STORE_WITH_ZMQ`。 |
| `tests/master_service_test.cpp` | manifest、publisher、worker_id、真实 ZMQ 往返测试。 |
