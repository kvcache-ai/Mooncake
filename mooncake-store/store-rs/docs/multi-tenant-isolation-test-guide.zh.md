# 多租户隔离测试手册

本文档说明如何验证 Store-RS 的完整多租户隔离功能。

当前功能不只包含一个机制，因此测试也应该分层进行：

- 在主 e2e harness 中验证端到端多租户行为
- 验证 strict quota 的准入、拒绝、返还和复用
- 通过 admin 检查 authoritative tenant metadata
- 在运行被中断时，验证 pending reservation 的 repair 路径

## 测试目标

一套完整的多租户隔离验证，应当证明以下几点：

- tenant policy 会在 tenant-scoped writer 启动前就写入 metadata
- runtime 会从 metadata 解析 tenant-scoped policy，而不是只依赖本地 fallback 参数
- 主 e2e harness 中的多租户数据路径行为正确
- strict quota 准入是 authoritative 的，超限写会被拒绝，同时不会污染 committed state
- delete 会在 authoritative delete 时返还 quota，并使容量能够立即复用
- admin 检查接口能够暴露运维调试和修复所需的 authoritative tenant state

## 主要验证层次

### 1. 主本地 e2e

主命令：

```bash
scripts/e2e/run-local-e2e.sh
```

推荐的快速迭代命令：

```bash
MC_STORE_RS_ENABLE_RDMA=0 \
MC_STORE_RS_BENCH_ITERS=1 \
scripts/e2e/run-local-e2e.sh
```

它对多租户隔离的验证包括：

- 主本地 harness 中的一般 multi-tenant 行为
- Redis-backed runtime path 中的 strict tenant quota
- tenant scoping 场景下的 routed writes 和 multi-replica publication
- expansion、true client shrink、hot-upgrade 等生命周期流程不会破坏 tenant 行为

期望的成功信号必须同时包含：

```text
strict tenant quota
```

以及

```text
multi-tenant
```

并出现在最终的 `e2e ok:` 行中。

### 2. 聚焦 strict tenant quota 的场景

strict quota 场景位于：

- `crates/mooncake-store-e2e/src/main.rs`

相关 helper：

- `put_tenant_quota_policy(...)`
- `verify_strict_tenant_quota(...)`

这个场景使用一个专门的 tenant：

- `tenant-quota-e2e`

它按顺序验证以下内容：

1. tenant quota policy 会在 quota-scoped writer 启动前写入 metadata
2. 一次在配额内的写入会成功
3. metadata quota state 会在 finalize 后成为 committed state
4. 第二次超限写会被拒绝
5. quota state 和 reservation 数量在拒绝后不会漂移
6. delete 会在 authoritative delete 时返还 quota
7. 返还后的 quota 可以立刻复用
8. object accounting 和 reservation 记录会保留下来，供后续检查

## 功能项测试方法详解

这一节按功能拆开说明“要怎么测”“用什么命令测”“看到什么才算通过”。

### 1. 租户策略写入与生效

**测试目标**

验证 tenant policy 确实由 admin 写入 metadata，并且 runtime 启动后会按 tenant 身份解析并使用这份策略，而不是只靠本地 fallback 参数运行。

**怎么测**

先写入一份显式 tenant policy：

```bash
mooncake-store-admin \
  --metadata-url redis://127.0.0.1:6380/0 \
  policy set \
  --tenant tenant-a \
  --route-topk 3 \
  --route-control embedded-wrh \
  --max-bytes 1048576 \
  --max-objects 10
```

然后读取回来确认：

```bash
mooncake-store-admin \
  --metadata-url redis://127.0.0.1:6380/0 \
  policy get \
  --tenant tenant-a
```

之后用同一个 tenant 启动 runtime：

```bash
mooncake-store-client \
  --metadata-url redis://127.0.0.1:6380/0 \
  --stable-id tenant-a-store-1 \
  --tenant tenant-a \
  --state active \
  --storage-bytes 268435456 \
  --scratch-bytes 16777216
```

**通过标准**

- `policy get` 能看到刚写入的 route / quota 配置
- runtime 能正常启动，并以 `tenant-a` 身份加入系统
- 后续 quota / object / reservation 查询都落在同一个 tenant 命名空间下

**失败时重点看什么**

- 是不是 tenant 写错了，导致 runtime 查的是另一个租户作用域
- 是不是没带 `--keyspace`，导致你写入和读取的不是同一个 metadata namespace
- 是不是仍然在依赖本地 fallback 参数，而不是 admin 写入的策略

### 2. 多租户基础隔离行为

**测试目标**

验证主 e2e harness 中已经包含多租户行为，并且 tenant 之间的逻辑路径没有串扰。

**怎么测**

运行主本地 e2e：

```bash
MC_STORE_RS_ENABLE_RDMA=0 \
MC_STORE_RS_BENCH_ITERS=1 \
scripts/e2e/run-local-e2e.sh
```

**通过标准**

- 最终 `e2e ok:` 行中包含 `multi-tenant`
- 运行过程中没有出现 tenant scope 混乱、错误读取别的 tenant 数据、或者 tenant 相关断言失败

**补充解释**

这里的 `multi-tenant` 不是只测“有没有 tenant 字段”，而是验证整个真实运行链路里，tenant scoping 没有被 routed write、replica publish、lifecycle 变化等流程破坏。

### 3. strict quota：策略预置

**测试目标**

验证 strict quota 不是运行时临时拍脑袋判断，而是先由 metadata 中的 tenant policy 决定准入边界。

**怎么测**

运行主 e2e 或聚焦 strict quota 的验证链路后，关注 strict quota 场景的第一步：

- `tenant-quota-e2e` 的 quota policy 会先被写入 metadata
- 测试形状为：
  - `max_bytes = value_size`
  - `max_objects = 1`

这个步骤的代码入口在：

- `put_tenant_quota_policy(...)`

**通过标准**

- quota-scoped writer 启动前，tenant policy 已经存在
- 后续 writer 的行为与这组 quota 参数匹配

### 4. strict quota：准入成功路径

**测试目标**

验证一次在配额范围内的写入能够成功，并且 finalize 后 metadata state 正确。

**怎么测**

运行：

```bash
MC_STORE_RS_ENABLE_RDMA=0 \
MC_STORE_RS_BENCH_ITERS=1 \
scripts/e2e/run-local-e2e.sh
```

然后用 admin 检查：

```bash
mooncake-store-admin \
  --metadata-url redis://127.0.0.1:6380/0 \
  quota state \
  --tenant tenant-quota-e2e

mooncake-store-admin \
  --metadata-url redis://127.0.0.1:6380/0 \
  quota reservations \
  --tenant tenant-quota-e2e
```

**通过标准**

- 第一个对象写入成功
- 另一个 client 可以读回同一个对象
- quota state 满足：
  - `used_bytes == value_size`
  - `used_objects == 1`
  - `pending_reserved_bytes == 0`
  - `pending_reserved_objects == 0`
- reservation 中能看到 finalized 的正向 reservation

**额外建议**

如果想看单个对象是否被正确记账，再执行：

```bash
mooncake-store-admin \
  --metadata-url redis://127.0.0.1:6380/0 \
  quota object \
  --tenant tenant-quota-e2e \
  --key <logical-key>
```

### 5. strict quota：超限拒绝且状态不漂移

**测试目标**

验证第二次超限写会被 authoritative 拒绝，并且拒绝不会把 quota state、reservation state 搞脏。

**怎么测**

仍然使用上面的快速 e2e 命令：

```bash
MC_STORE_RS_ENABLE_RDMA=0 \
MC_STORE_RS_BENCH_ITERS=1 \
scripts/e2e/run-local-e2e.sh
```

strict quota 场景会自动进行第二次同尺寸写入尝试。

**通过标准**

- 第二次写入失败，并且错误语义是 tenant quota conflict
- 失败后 quota state 与第一次成功写入后保持一致
- reservation 数量没有莫名增加
- 最终日志里没有 quota drift、missing accounting、missing reservations 相关错误

**这一步为什么重要**

它证明新的 metadata-authoritative admission path 在拒绝时不会污染 committed state，这正是“严格隔离”区别于 best-effort quota 的关键。

### 6. strict quota：删除返还

**测试目标**

验证 delete 的 quota refund 发生在 authoritative delete 时，而不是等后台 reclaim 很久以后才体现。

**怎么测**

继续使用同一条 e2e：

```bash
MC_STORE_RS_ENABLE_RDMA=0 \
MC_STORE_RS_BENCH_ITERS=1 \
scripts/e2e/run-local-e2e.sh
```

运行后检查：

```bash
mooncake-store-admin \
  --metadata-url redis://127.0.0.1:6380/0 \
  quota state \
  --tenant tenant-quota-e2e

mooncake-store-admin \
  --metadata-url redis://127.0.0.1:6380/0 \
  quota reservations \
  --tenant tenant-quota-e2e
```

**通过标准**

- delete 之后 `used_bytes` 和 `used_objects` 回到 0
- `pending_reserved_*` 保持为 0
- object accounting 中不再保留被删除对象的 active 记录
- reservation 中出现 finalized 的负向 reservation，表示 refund

### 7. strict quota：返还后立即复用

**测试目标**

验证 delete 之后释放出来的 quota 可以马上用于下一次写入，而不是还要等后续异步清理。

**怎么测**

strict quota 场景在 delete 之后会再次写入一个同等大小对象。

运行命令仍然是：

```bash
MC_STORE_RS_ENABLE_RDMA=0 \
MC_STORE_RS_BENCH_ITERS=1 \
scripts/e2e/run-local-e2e.sh
```

**通过标准**

- 删除之后的下一次同尺寸写入立即成功
- 另一个 client 可以读回这个新对象
- 没有出现“理论上已释放，但实际仍无法写入”的现象

### 8. object accounting 可见性

**测试目标**

验证 strict quota 不只是维护一个总量计数器，还能为单个逻辑对象保留 authoritative accounting 记录，便于运维排查。

**怎么测**

在运行结束后，对目标对象执行：

```bash
mooncake-store-admin \
  --metadata-url redis://127.0.0.1:6380/0 \
  quota object \
  --tenant tenant-quota-e2e \
  --key <logical-key>
```

**通过标准**

- 对于当前存在的对象，能看到 committed length 等 accounting 信息
- 对于已经 authoritative delete 的对象，不应再保留错误的 active accounting 状态

### 9. pending reservation repair 路径

**测试目标**

验证当运行异常中断时，运维能够通过 admin inspection / reconcile 看懂并处理残留的 pending reservation。

**怎么测**

如果一次运行异常退出，或者你故意在中途打断测试，再执行：

```bash
mooncake-store-admin \
  --metadata-url redis://127.0.0.1:6380/0 \
  quota reservations \
  --tenant tenant-quota-e2e

mooncake-store-admin \
  --metadata-url redis://127.0.0.1:6380/0 \
  quota reconcile \
  --tenant tenant-quota-e2e \
  --dry-run
```

**通过标准**

- 可以通过 `quota reservations` 看到残留 reservation 的状态
- `quota reconcile --dry-run` 能告诉你哪些项会被 finalize，哪些项会被 abort
- 对于健康完成的运行，这一步通常应显示无额外工作

### 10. routed batch 路径的严格配额语义

**测试目标**

验证严格配额不仅覆盖单对象写，也覆盖 routed `batch_put` 的 all-or-nothing admission 语义。

**怎么测**

这一项主要通过底层测试补充验证，位置在：

- `crates/mooncake-store-client/src/client/tests.rs`

重点关注：

- routed batch all-or-nothing quota admission
- overwrite delta charging
- delete refund behavior

**通过标准**

- batch admission 失败时，不会留下半成功半失败的 quota 污染
- 已获得的 reservation 会在后续 admission 失败时被正确 abort
- finalize 只发生在 route CAS authoritative 之后

### 11. 作用域选择器隔离：tenant / domain / object_set

**测试目标**

验证当前多租户隔离不仅区分 tenant，还能区分更细粒度的 `domain` 和 `object_set`，并确保不同 scope 下的路由、对象可见性和放置结果不会串扰。

**怎么测**

这一项目前主要通过底层测试验证，位置在：

- `crates/mooncake-store-client/src/client/tests.rs`

重点关注带有这些选择器的测试路径：

- `.domain("domain-a")`
- `.object_set("set-a")`
- `NamespaceScope::with_defaults(...)`

可以重点检查：

- 同一个 tenant 下，不同 `domain/object_set` 是否会路由到不同 storage owner
- object key 的 canonical scope 是否正确编码为不同命名空间
- `tenant-a/domain-a/set-a` 与 `tenant-a/domain-b/set-b` 是否保持隔离

**通过标准**

- 不同 scope 下的 route owner 可以不同，且结果稳定
- object lookup / route lookup 落在各自 scope，不出现跨 scope 读到对方对象的情况
- `domain`、`object_set` 不只是标签，而是真正参与 namespace 与 route 身份

### 12. metadata-authored 默认策略：routing / placement / fairness / shaping

**测试目标**

验证 runtime 能从 metadata 中解析 tenant policy 默认值，而不只是依赖本地 builder fallback；这包括 routing、placement、fairness、shaping 等默认策略。

**怎么测**

这一项当前主要通过底层测试验证，重点看：

- `crates/mooncake-store-client/src/client/tests.rs`
- 其中包含 tenant policy 注入后读取 effective defaults 的测试

重点关注这些字段是否从 metadata 生效：

- `default_replica_count`
- `preferred_storage_owners`
- `preferred_segments`
- `execution_fairness.max_remote_batch_items_per_tenant`
- `bandwidth_shaping.max_remote_batch_bytes`
- `bandwidth_shaping.max_remote_batch_burst_items`
- `bandwidth_shaping.max_inflight_bytes_per_batch`

**通过标准**

- client 构建后能读到来自 tenant policy 的 effective defaults
- 没有显式传本地 fallback 时，行为仍然与 metadata policy 一致
- placement / fairness / shaping 行为与 policy 字段对应，而不是退回默认空值

### 13. placement 默认值与偏好：副本数 / preferred owners / preferred segments

**测试目标**

验证 placement 相关默认策略已经进入多租户测试覆盖，包括默认副本数、preferred storage owner、preferred segment 等能力。

**怎么测**

当前主要通过底层测试验证，位置仍然在：

- `crates/mooncake-store-client/src/client/tests.rs`

重点看：

- `preferred_storage_owners([...])`
- `preferred_segments([...])`
- `default_replica_count`

验证思路：

- 给 policy 写入 preferred owner / preferred segment
- 让请求在没有 per-request override 的情况下运行
- 观察 route 结果是否优先命中这些 placement 偏好
- 检查 default replica count 是否影响 route 中 replica 数量

**通过标准**

- route 结果优先命中 preferred owner / segment，或在不可满足时合理回退
- default replica count 会反映到 route replicas 数量中
- placement 行为与 tenant policy 一致，而不是完全由 runtime 局部参数决定

### 14. QoS / fairness：remote batch 切片语义

**测试目标**

验证 `execution_fairness.max_remote_batch_items_per_tenant` 会影响远端 batch 的切片方式，避免单个 tenant 在一次 remote batch 中独占过多 item。

**怎么测**

这一项目前主要通过底层测试验证，位置在：

- `crates/mooncake-store-client/src/client/tests.rs`

重点看这些测试：

- `batch_get_fairness_limits_remote_items_per_tenant`
- `routed_put_fairness_limits_remote_replica_writes_per_batch`

它们的测试形状大致是：

- 构造多个 tenant 的对象请求
- 把 `max_remote_batch_items_per_tenant` 设成一个很小的值（例如 `1`）
- 观察 transport 层提交出去的 batch size 是否被切成更小的片段

**通过标准**

- 功能结果仍然正确，所有对象都能成功读回或写入
- transport 提交记录显示 batch 被按 fairness 约束切片，而不是把同一个 tenant 的 item 一次性全部发出去
- 在策略通过 metadata 注入时，runtime 行为应与 builder fallback 设置一致

### 15. QoS / bandwidth shaping：remote batch 字节上限

**测试目标**

验证 `bandwidth_shaping.max_remote_batch_bytes` 会限制一次 remote batch 的字节数，而不是让请求路径无限聚合。

**怎么测**

这一项目前也主要通过底层测试验证，位置在：

- `crates/mooncake-store-client/src/client/tests.rs`

重点看：

- `batch_get_shaping_caps_remote_batch_bytes`

这个测试会：

- 构造多个远端对象
- 把 `max_remote_batch_bytes` 设成很小的值（例如 `4`）
- 观察 transport 层记录下来的 submitted batch bytes

**通过标准**

- 读请求仍然全部成功
- transport 层的 batch bytes 不会持续超过设定上限
- 请求会被拆成多个更小的 remote batch，而不是一次提交超大字节数

### 16. QoS / bandwidth shaping：在途字节限制

**测试目标**

验证 `max_inflight_bytes_per_batch` 会对请求路径中的在途批量流量施加限制，并成为带宽隔离的一部分基础机制。

**怎么测**

有两层验证方式：

1. 底层测试：
   - `crates/mooncake-store-client/src/client/tests.rs`
   - 重点关注使用 `max_inflight_bytes_per_batch(...)` 的测试
2. 进程级 e2e：
   - `crates/mooncake-store-e2e/src/main.rs`
   - 重点关注 `verify_rdma_bandwidth_isolation(...)`

在 e2e 中，代码会构造两个 routed client：

- `tenant-high`
- `tenant-low`

并给它们设置不同的 `max_inflight_bytes_per_batch`：

- high tenant：更大的 inflight 上限
- low tenant：更小的 inflight 上限

随后并发执行多轮写入，比较两边的吞吐。

**通过标准**

- 两个 tenant 的数据都能正确写入并读回
- 日志里会打印每轮 `rdma isolation round=... ratio=...`
- 最终 `median_ratio` 满足阈值要求，否则会报：
  - `rdma bandwidth isolation ratio below threshold ...`

### 17. route repair / stale authority 修复路径

**测试目标**

验证除了 quota reconcile 之外，系统还具备 route repair / stale authority 修复相关能力，并且这些能力已有底层测试覆盖。

**怎么测**

当前这一项主要通过底层测试验证，位置在：

- `crates/mooncake-store-client/src/client/tests.rs`

重点关注带有 repair route / stale owner / dead owner 修复语义的测试。

验证时重点看：

- dead owner 或 stale authority 出现后，route 查询是否能恢复到 live owner
- repair 发生时不会静默伪造 authoritative state
- route repair 与 quota reconcile 是两类不同问题，不应混在一起解释

**通过标准**

- stale route / dead owner 相关测试能够恢复出正确的 live route
- repair 结果与 authoritative route state 一致
- 文档和测试报告中能把 route repair 与 quota repair 区分开

### 18. quota abort 运维修复路径

**测试目标**

验证在 pending reservation 已经可见、但运维希望显式终止某个 reservation 时，`quota abort` 这条运维路径被纳入测试手册，而不是只写 `reconcile`。

**怎么测**

在检查 reservation 时，除了：

- `quota reservations`
- `quota reconcile --dry-run`

还应该关注：

```bash
mooncake-store-admin \
  --metadata-url redis://127.0.0.1:6380/0 \
  quota abort \
  --tenant tenant-quota-e2e \
  --reservation-id <reservation-id> \
  --dry-run
```

如果运维确认需要显式 abort，再去掉 `--dry-run`。

**通过标准**

- 可以先通过 reservation inspection 找到目标 reservation
- `quota abort --dry-run` 能清楚展示将要 abort 的 reservation
- abort 路径被视为显式运维操作，而不是系统静默自动处理

### 19. RDMA 带宽隔离端到端验证

**测试目标**

验证当前 e2e harness 已经把带宽隔离行为接进真实进程级链路，而不只是停留在底层单测里。

**怎么测**

运行主 e2e 时，不要强制关闭 RDMA，并在支持 RDMA 的机器上执行。

可以关注这些环境变量：

- `MC_STORE_RS_ENABLE_RDMA=1`：显式请求 RDMA
- `MC_STORE_RS_RDMA_ISOLATION_ITERS`
- `MC_STORE_RS_RDMA_ISOLATION_WARMUP_ITERS`
- `MC_STORE_RS_RDMA_ISOLATION_ROUNDS`
- `MC_STORE_RS_RDMA_ISOLATION_MIN_RATIO`

如果宿主机支持 RDMA，成功运行时应在日志和最终成功行中看到：

- `rdma bandwidth isolation`

如果宿主机不支持 RDMA，当前实现会打印类似：

- `skip rdma bandwidth isolation: ...`

这时不应把“跳过”误判为带宽隔离功能失败，而应该把它解释为“当前机器不具备该项验证前提”。

**通过标准**

- 在 RDMA-capable 主机上，`rdma bandwidth isolation` 出现在最终 `e2e ok:` 行中
- 多轮吞吐对比结果达到阈值
- high / low tenant 的对象都能被 reader 正确读回
- 在非 RDMA 主机上，测试可以明确给出 skip 原因，而不是产生含糊的假失败

### 20. QoS：驱逐优先级

**测试目标**

验证不同 `qos_tier` 会影响 reclaim / eviction 的优先级，低优先级对象应先进入回收顺序，高优先级对象应尽量后回收。

**怎么测**

当前这项主要由底层测试覆盖，位置在：

- `crates/mooncake-store-client/src/client/tests.rs:10621`
  - `qos_tier_reclaims_low_priority_before_high_priority()`

相关实现锚点还包括：

- `crates/mooncake-store-client/src/client/runtime_alloc.rs`
  - `reclaim_policy_rank(qos_tier: &str)`
- `crates/mooncake-store-client/src/client/state_store.rs`
  - `take_due_reclaims(...)`

当前 rank 语义大致是：

- `critical` → 更高 rank
- `gold` → 次高 rank
- `default` → 中间 rank
- 其他低优先级 tier（例如 `bronze`）→ 更低 rank

由于 due reclaim 会按 `policy_rank` 升序处理，所以低优先级 tier 会更早被回收。

**通过标准**

- 测试里 `bronze` 会排在最前面
- `default` 在 `bronze` 之后
- `gold` 和 `critical` 更靠后
- 这能证明 QoS tier 已经进入 reclaim 排序逻辑，而不是只作为标签存在

**这项覆盖的边界**

这个测试当前证明的是：

- reclaim 记录里带上了 `qos_tier`
- reclaim rank 会按 qos tier 计算
- due reclaim 出队顺序遵循低优先级先回收的原则

但它还不是一个完整的 black-box e2e 证明。也就是说，它还不能单独证明：

- 在真实容量压力下 background eviction worker 的最终 victim 选择完全符合预期
- CLOCK eviction、route reclaim、对象最终可见性这一整条真实链路都已经被进程级测试覆盖

### 21. QoS / 带宽功能的测试边界

**测试目标**

明确当前多租户文档里，哪些是已经有验证入口的能力，哪些还不应被描述成 strict quota 的一部分。

**怎么测 / 怎么解释**

解释这部分结果时，建议坚持下面的边界：

- strict quota 验证的是 authoritative admission / finalize / refund / repair
- fairness / shaping 验证的是请求路径中的 batch slicing、burst 限制和 inflight 带宽限制
- QoS eviction priority 当前主要由底层 reclaim 排序测试证明
- `rdma bandwidth isolation` 验证的是带宽隔离行为，不等于 quota metadata 记账

也就是说：

- 看到 quota state 正确，并不能自动证明 QoS shaping 正确
- 看到 QoS reclaim 排序单测通过，也不能自动推出真实容量压力下的 black-box eviction 已全部验证
- 看到 RDMA bandwidth isolation 通过，也不能替代 strict quota 的 metadata 检查

**通过标准**

- 测试报告能把 quota、fairness、shaping、QoS eviction priority、RDMA isolation 分开解释
- 不会把尚未由当前 harness 覆盖的行为说成“已经由 strict quota e2e 证明”

## 前置条件

在本地运行验证前，请确保：

- Redis 在配置端口上可用
- workspace 可以正常构建
- 本地环境可以跑标准 e2e harness
- 如果你希望在运行后继续做 metadata 检查，`mooncake-store-admin` 需要能访问同一个 metadata namespace

## 推荐的运维验证流程

### 第一步：运行本地 e2e

```bash
MC_STORE_RS_ENABLE_RDMA=0 \
MC_STORE_RS_BENCH_ITERS=1 \
scripts/e2e/run-local-e2e.sh
```

### 第二步：确认成功行

最终成功行至少要包含这些标记：

- `strict tenant quota`
- `multi-tenant`

如果在 RDMA-capable 环境上运行，还应该关注：

- `rdma bandwidth isolation`

当前一次成功运行大致会是：

```text
e2e ok: single put/get, strict tenant quota, batch put/get, request-level replication policy, true delete reclaim, routed remote write, multi-replica publish, registered-buffer path, overwrite reclaim, multi-tenant, scale-out, elastic expand-shrink, true client shrink, hot-upgrade, rdma bandwidth isolation
```

### 第三步：检查 authoritative tenant metadata

运行结束后，通过 admin 检查 strict quota tenant：

```bash
mooncake-store-admin \
  --metadata-url redis://127.0.0.1:6380/0 \
  quota state \
  --tenant tenant-quota-e2e

mooncake-store-admin \
  --metadata-url redis://127.0.0.1:6380/0 \
  quota reservations \
  --tenant tenant-quota-e2e
```

可选的 object-level 检查：

```bash
mooncake-store-admin \
  --metadata-url redis://127.0.0.1:6380/0 \
  quota object \
  --tenant tenant-quota-e2e \
  --key <logical-key>
```

健康成功后的期望解释：

- `pending_reserved_*` 应该是 `0`
- finalized reservation 应该能解释那次成功写入以及后续 refund 路径
- object accounting 应当和最终 authoritative object visibility 一致

### 第四步：如果运行中断，检查 repair 路径

如果运行崩溃或者中途被打断，并且仍然能看到 pending reservation，应该先检查 repair plan：

```bash
mooncake-store-admin \
  --metadata-url redis://127.0.0.1:6380/0 \
  quota reconcile \
  --tenant tenant-quota-e2e \
  --dry-run
```

期望解释：

- 对于健康完成的运行，通常不应该留下需要 reconcile 的工作
- 应该先使用 `--dry-run`，让运维人员先看到哪些 reservation 会被 finalize 或 abort
- repair 行为是显式的，系统不会静默猜测 tenant metadata 应该如何清理

## QA 检查清单

在把这个功能交给 QA 时，建议他们逐项确认以下内容：

- tenant policy 可以通过 admin 成功写入并读回
- runtime 以指定 tenant 启动后，后续检查都落在正确命名空间中
- `tenant / domain / object_set` 不同 scope 的行为能够正确隔离
- metadata-authored placement / fairness / shaping 默认值会实际影响 runtime 行为
- 主 e2e 脚本能够成功退出
- 最终成功行同时包含 `strict tenant quota` 和 `multi-tenant`
- 如果测试机支持 RDMA，最终成功行还应包含 `rdma bandwidth isolation`
- 没有出现 quota state drift、missing accounting、missing reservations 之类的错误
- 上面的快速验证命令能够稳定通过
- 运行后的 metadata 检查结果符合 tenant-root strict quota 的预期行为
- 在一次健康完成的运行后，`quota reconcile --dry-run` 不应显示异常待处理工作
- 如果做了中断测试，repair plan 的输出与 reservation 实际状态能够对应上

## 底层补充覆盖

e2e 套件是进程级证明，但并不是唯一测试覆盖。

已有的底层测试还覆盖了 strict quota 语义，位置在：

- `crates/mooncake-store-client/src/client/tests.rs`

例如包括：

- 超限写拒绝
- overwrite delta charging
- delete refund 行为
- routed batch all-or-nothing quota admission
- scope selector（tenant/domain/object_set）隔离
- metadata-authored routing / placement / fairness / shaping 默认值解析
- placement 默认值与偏好（replica count / preferred owners / preferred segments）
- route repair / stale authority 修复
- fairness 限制 remote batch item 数
- shaping 限制 remote batch bytes
- inflight bytes shaping
- QoS 驱逐 / reclaim 优先级排序

这些测试和 e2e 是互补关系。主 e2e 的意义在于证明 Redis-backed runtime path 中的真实执行链路已经接入了这套逻辑。

## 需要重点关注的问题

这个领域常见的失败类型包括：

- tenant-scoped writer 启动前，没有先把 tenant policy 写入 metadata
- 在手工运行时，本地 fallback 参数掩盖了缺失的 admin-authored policy
- 中断写入后留下 pending reservation
- reservation/finalize/refund 之间出现 quota state drift
- object accounting 与最终 authoritative object visibility 不一致
- `domain / object_set` 已经传入请求，但 namespace / route 实际上没有隔离开
- metadata-authored placement / fairness / shaping 默认值没有真正进入 runtime，有配置但行为不变
- preferred owner / preferred segment / default replica count 没有反映到 route 结果
- route repair 与 quota repair 被混为一谈，导致误判修复语义
- QoS tier 已经写进对象或 route，但 reclaim / eviction 顺序并没有按优先级变化
- RDMA 测试环境不满足前置条件，却被误判为带宽隔离逻辑失败

## 测试心智模型

解释测试结果时，建议始终使用下面这套心智模型：

- policy 由 admin 编写
- runtime 在请求路径上执行这些 policy
- strict quota state 的 authoritative source 在 metadata 中
- admin 的 inspection 和 repair 命令是面向运维的 source of truth

不要再把 runtime-local quota configuration 单独视为主要验证目标。

## 相关文档

- `docs/multi-tenant-isolation-user-guide.md` — 运维与集成使用手册
- `docs/deployment.md` — 部署流程和 admin 命令示例
- `docs/strict-tenant-quota-e2e-test-guide.md` — 聚焦 strict quota 的 QA handoff 手册
- `docs/tenant-quota-consistency-design.md` — strict quota 协议和 repair 语义
- `docs/multi-tenant-admin-control-plane-design.md` — admin 命令模型和控制面设计
