# 多租户隔离用户手册

本文档面向运维人员和应用集成人员，说明如何使用 Store-RS 的多租户隔离能力。

当前实现遵循一个核心原则：

- 通过 `mooncake-store-admin` 编写租户级策略
- 由 Store-RS runtime 在启动阶段和请求路径上执行这些策略

Builder、Python、CLI 和环境变量中的本地参数仍然作为兼容性 fallback 保留，但它们已经不是编写租户策略的首选入口。

## 多租户隔离覆盖范围

当前 Store-RS 的多租户隔离主要覆盖以下方面：

- 租户级路由默认值，例如 `route_control` 和 `route_topk`
- 租户级配额默认值，例如 `max_bytes` 和 `max_objects`
- 放置策略默认值，例如副本数和首选存储节点
- QoS 相关默认值，例如公平性和流量整形
- 通过 `tenant`、`domain`、`object_set` 进行租户命名空间选择
- 基于 authoritative metadata 的严格配额准入，而不是基于请求本地快照的 best-effort 检查

当前作用域模型为：

- tenant
- tenant + domain
- tenant + domain + object_set

在当前严格配额的第一阶段实现中，可变的 quota state 仍然存放在 tenant-root 级别的 metadata 中。嵌套选择器目前主要用于 object accounting 查询，以及未来的策略扩展。

## 推荐的控制面模型

推荐按下面的职责划分来使用：

- `mooncake-store-admin` 负责写入租户策略，并执行显式的检查 / 修复命令
- Store-RS runtime 负责执行路由、配额、放置以及请求路径上的隔离逻辑
- `tenant` 仍然是 runtime 启动时做策略查找和 request builder 作用域选择的默认命名空间选择器

这样的设计可以让管理面保持显式，同时避免在 fast path 中临时做控制面策略决策。

## 策略优先级

租户级策略解析优先级如下：

1. 存储在 metadata 中、由 admin 管理的 tenant policy
2. 迁移期间仍然保留支持的 legacy compatibility metadata 读取路径
3. 当 metadata 没有提供对应策略时，退回到 runtime-local builder / Python / standalone client 的 fallback 值

实际使用建议：

- 只要 metadata 中已经存在 tenant policy，就应当把它视为唯一可信的 source of truth
- 本地的 `route_topk`、`route_control`、`namespace_quota`、`execution_fairness`、`bandwidth_shaping` 都应视为 fallback，而不是首选策略编写入口

## 基础运维工作流

### 1. 写入租户策略

通过 admin 写入租户级路由、配额和 QoS 策略：

```bash
mooncake-store-admin \
  --metadata-url redis://127.0.0.1:6380/0 \
  policy set \
  --tenant tenant-a \
  --route-topk 3 \
  --route-control embedded-wrh \
  --max-bytes 1048576 \
  --max-objects 10 \
  --max-remote-batch-items-per-tenant 16 \
  --max-remote-batch-bytes 1048576 \
  --max-remote-batch-burst-items 32 \
  --max-inflight-bytes-per-batch 4194304
```

相关常用命令：

```bash
mooncake-store-admin \
  --metadata-url redis://127.0.0.1:6380/0 \
  policy list

mooncake-store-admin \
  --metadata-url redis://127.0.0.1:6380/0 \
  policy get \
  --tenant tenant-a
```

如果一个部署中存在多个环境或多个 metadata namespace，请通过 `--keyspace <prefix>` 来隔离它们。

## 2. 使用租户身份启动 runtime

即使策略已经存放在 metadata 中，runtime 在启动时仍然应该显式声明自己的 tenant 身份。

Standalone client 示例：

```bash
mooncake-store-client \
  --metadata-url redis://127.0.0.1:6380/0 \
  --stable-id tenant-a-store-1 \
  --tenant tenant-a \
  --state active \
  --storage-bytes 268435456 \
  --scratch-bytes 16777216
```

Rust 示例：

```rust
let client = StoreClientBuilder::new(metadata, "tenant-a-store-1")
    .tenant("tenant-a")
    .state(ClientLifecycleState::Active)
    .local_memory(
        LocalMemoryConfig::new()
            .storage_bytes(256 * 1024 * 1024)
            .scratch_bytes(16 * 1024 * 1024)
            .location("cpu:0"),
    )
    .with_tent(engine)
    .transport_factory(factory)
    .build(now_ms() + 600_000)?;
```

这里需要注意：

- `tenant(...)` 用于选择启动阶段策略查找和 request builder 默认使用的作用域
- request-scoped API 和 per-request replication settings 仍然属于正常的执行期输入
- 即使 route / quota 策略已经交给 metadata 管理，transport 和 memory 配置仍然是 runtime 自己的职责

## QoS / 带宽相关策略说明

当前多租户策略面里，QoS 相关默认值主要包括：

- `execution_fairness.max_remote_batch_items_per_tenant`
- `bandwidth_shaping.max_remote_batch_bytes`
- `bandwidth_shaping.max_remote_batch_burst_items`
- `bandwidth_shaping.max_inflight_bytes_per_batch`

它们的作用可以简单理解为：

- `max_remote_batch_items_per_tenant`：限制一次远端 batch 中单个 tenant 能占用多少 item，避免一个 tenant 把整个 batch 塞满
- `max_remote_batch_bytes`：限制单次远端 batch 的总字节数，避免一次发太大
- `max_remote_batch_burst_items`：限制短时间突发的 batch item 数量
- `max_inflight_bytes_per_batch`：限制单个 batch 在途的字节量，用于带宽整形 / 隔离

需要特别区分两件事：

- strict quota 解决的是“能不能准入、metadata 如何 authoritative 记账”的问题
- fairness / shaping 解决的是“请求路径里怎么切 batch、怎么限制突发和带宽占用”的问题

它们都属于多租户隔离的一部分，但不是同一个机制，也不应混为一谈。

如果你通过 admin 读取 tenant policy，应该能看到这些字段已经进入 metadata，而不是只存在于 runtime-local fallback 配置里。

## 3. 检查 authoritative tenant state

通过 admin 检查严格配额和 object accounting 状态：

```bash
mooncake-store-admin \
  --metadata-url redis://127.0.0.1:6380/0 \
  quota state \
  --tenant tenant-a

mooncake-store-admin \
  --metadata-url redis://127.0.0.1:6380/0 \
  quota object \
  --tenant tenant-a \
  --key object-a

mooncake-store-admin \
  --metadata-url redis://127.0.0.1:6380/0 \
  quota reservations \
  --tenant tenant-a \
  --state pending
```

解释说明：

- `quota state` 和 `quota reservations` 反映的是 authoritative metadata state，而不是本地 best-effort 计数器
- `quota object` 是检查单个逻辑对象 committed accounting record 的最快方式
- 在当前严格配额实现中，quota state 和 reservation 都归并到 tenant-root 级别

## 4. 在需要时执行显式修复

当运维人员怀疑某次中断的写入留下了可见的 pending reservation 时，应该先检查 repair plan：

```bash
mooncake-store-admin \
  --metadata-url redis://127.0.0.1:6380/0 \
  quota reconcile \
  --tenant tenant-a \
  --dry-run
```

修复建议：

- 先使用 `--dry-run`，看清楚 admin 打算 finalize 或 abort 哪些 reservation
- 对于健康完成的运行，通常不应该留下需要 reconcile 的工作
- admin repair 设计上就是显式操作，Store-RS 不会对租户 metadata 做静默猜测式修复

## 严格配额语义

严格配额是多租户隔离模型的一部分，不是一个可有可无的附加选项。

当前行为如下：

- 单对象 `put` 会先在 metadata 中预留配额，然后再执行 allocation / write
- route publication 完成后，quota finalization 才会作为成功的一部分生效
- `remove` 会在 route delete CAS 成为 authoritative 时立即返还 quota
- overwrite 会按照 committed byte delta 计费
- routed `batch_put` 会先为每个条目预留 quota，在每个 route CAS authoritative 之后再 finalize

这避免了旧版 best-effort 模型里“多个并发写在过期快照上同时通过配额检查”的问题。

## Metadata 模型摘要

严格配额依赖以下租户级 metadata 原语：

- `TenantQuotaState`：保存 committed 和 pending usage
- `TenantObjectAccounting`：保存 authoritative committed object size/version
- `TenantQuotaReservation`：用于 reserve / finalize / abort 协调

不同 backend 的实现方式：

- in-memory backend 在单个写锁下完成整个流程
- Redis 使用 backend-atomic Lua reserve/finalize/abort 脚本
- etcd 使用 multi-key compare-and-swap 事务循环

## QoS / 带宽相关运维建议

- 优先把 fairness / shaping 默认值写进 admin policy，而不是散落在每个 runtime 启动命令中
- 如果 metadata 已经提供 tenant-scoped QoS 策略，就把它视为 source of truth，本地 builder / Python fallback 只作为兼容入口
- 当前 strict quota 的 authoritative state 主要体现在 quota / reservation / object accounting 上；QoS / bandwidth shaping 更偏向请求路径执行行为，不会表现为同一套 quota metadata
- `rdma bandwidth isolation` 是当前 e2e 成功行中的一个独立验证项，它更接近“带宽隔离行为是否生效”，而不是 strict quota 记账是否正确

## 运维建议

- 优先使用 `mooncake-store-admin policy ...`，不要把租户 route/quota 设置散落在每个 runtime 启动命令里
- 启动 runtime 时始终显式带上 `tenant`，确保策略查找和 request builder 使用的是正确命名空间
- 把本地 route/resource 参数视为 bootstrap 或 compatibility fallback 即可
- 在 Redis ACL 场景下，如果密码里包含 `@` 之类 URL 保留字符，优先使用 `MC_REDIS_USERNAME` / `MC_REDIS_PASSWORD`
- `cleanup-stale-segments` 用于清理 dead-owner 的 segment metadata，它和 tenant quota reconcile 是两类不同操作

## 验证入口

如果你需要对这个功能做端到端验证，可以从下面这些入口开始：

- `scripts/e2e/run-local-e2e.sh`：覆盖 multi-tenant 行为和 strict tenant quota
- `docs/strict-tenant-quota-e2e-test-guide.md`：解释聚焦 strict quota 的验证路径
- `docs/deployment.md`：描述从策略写入到本地验证的推荐运维流程

## 相关文档

- `docs/deployment.md` — runtime 与 operator 的部署工作流
- `docs/configuration.md` — tenant-scoped 设置的优先级和 fallback 行为
- `docs/multi-tenant-admin-control-plane-design.md` — 控制面设计和 admin 命令模型
- `docs/tenant-quota-consistency-design.md` — strict quota 设计和 metadata 协议
- `docs/architecture.md` — metadata 模型和 runtime 集成细节
- `docs/rust.md` — Rust 客户端使用方式和 tenant-scoped runtime 说明
