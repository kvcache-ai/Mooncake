# KV Event

[English](../../../design/kv-event/index.md)

KV Event 用来告诉 Conductor：键值（KV）缓存块当前存放在哪里。在本项目中，
vLLM 上报某个推理引擎及其 rank 持有的 GPU 缓存块，Mooncake 则上报共享
CPU 或 Disk 缓存池中的对象。请根据你要配置的一端选择对应指南。

## 选择事件来源

| 来源 | 上报内容 | 后续文档 |
|---|---|---|
| vLLM | 某个已注册引擎及其数据并行（DP）rank 的 GPU 缓存。 | 查看 [Conductor 订阅指南](./subscriber-guide.md)，了解 Conductor 接受哪些 vLLM 消息字段。 |
| Mooncake Master | 可由兼容的已注册引擎共享的 CPU 或 Disk 对象。 | 查看 [Mooncake 发布指南](./publisher-design.md)，启用发布端并检查其状态。 |

Conductor 根据注册来源的 `type` 选择消息读取方式，而不是根据 ZeroMQ
（ZMQ）的 topic 文本选择。订阅指南详细对比了 `vLLM` 和 `Mooncake`。

## 发布 Mooncake 事件

[Mooncake KV Event 发布指南](./publisher-design.md)介绍构建选项、Master
设置、状态计数、准确的三帧消息格式、connector key 解析以及事件丢失后的
行为。

## 连接 Conductor

[Conductor KV Event 订阅指南](./subscriber-guide.md)说明每种注册来源可以
发送什么、哈希如何转成查询值、哪些事件会更新或清除缓存信息，以及注销时
如何清理。

如需完整的启动、注册和 HTTP 检查步骤，请参阅
[Conductor 使用指南](../conductor/usage.md)。

```{toctree}
:maxdepth: 1
:hidden:

publisher-design
subscriber-guide
```
