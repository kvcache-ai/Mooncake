# Mooncake Conductor

[English](../../../design/conductor/index.md)

Mooncake Conductor 在内存中记录推理引擎和 Mooncake Store 上报的、可以复用的
键值（KV）缓存块。路由器可以在选择推理引擎前查询这些信息。本页汇总了运行
Conductor、理解查询结果和接入客户端所需的文档。

## 选择任务

### 运行 Conductor

按照[使用指南](./usage.md)构建 C++ 服务、配置事件来源、检查注册结果、查询缓存
可用情况，以及注销事件来源。

### 理解查询结果

阅读[架构说明](./conductor-architecture-design.md)，了解 vLLM 的 GPU 事件和
Mooncake 共享的 CPU 或 Disk 事件如何变成每个推理引擎的查询结果。

### 调用 HTTP API

查阅 [HTTP API 参考](./indexer-api-design.md)，了解当前实现的五个接口、可接受
的字段、响应内容和错误格式。

### 集成 Router

阅读 [Router 集成设计](./router-integration-design.md)，了解 Router 如何把
`rank_matches` 中的已注册 rank 与自身可路由 rank 求交集，再结合健康检查和负载
选择目标，以及字段缺失或查询失败时如何继续使用原有调度策略。

### 连接事件来源

先阅读 [KV Event](../kv-event/index.md)，比较 vLLM 和 Mooncake 两种消息处理
方式。该部分还会说明 Mooncake 发送什么，以及 Conductor 接受什么。

```{toctree}
:maxdepth: 1
:hidden:

conductor-architecture-design
usage
indexer-api-design
router-integration-design
```
