# P2P Client Redis HA 增量恢复方案

## 目标

v17 目标是直接支持 P2P client 在 Redis HA 模式下的增量恢复，不再把 full
local metadata recovery 作为 Redis HA 的主要路径。

这里的“增量恢复”不是让 client 补写 master 的全局 oplog sequence。全局 oplog
仍然由 master 分配和持久化。client 记录自己产生的 metadata mutations，连接到
新 master 后只 replay 新 master 缺失的部分。

## 第一版范围

第一版只打通 client、master、Redis oplog 的基本闭环：

- client 进程存活，master failover 或重启后可以增量 replay。
- client journal 先放内存，不写 Redis。
- client 重启后本地数据和内存 journal 都丢失，不需要恢复；按 fresh client
  registration 处理。
- 只面向 Redis HA 模式。
- 不保留 Redis HA 路径下的 full recovery fallback。

非目标：

- client 重启后的 mutation journal 恢复。
- client journal 持久化到 Redis 或本地文件。
- 非 Redis HA 模式兼容。

## Mutation ID

每个 client metadata mutation 携带一个 `client_mutation_id`。

建议格式：

```text
client_mutation_id = (timestamp_ms << N) | local_counter
```

要求：

- 在同一个 client 内单调递增。
- 不要求连续。
- 同一毫秒内用 `local_counter` 区分。
- 如果本机时钟回退，使用 `max(now_ms, last_generated_ms)`。
- 如果同一毫秒 counter 耗尽，等待下一毫秒。

不再使用 `client_epoch/client_op_seq` 作为第一版协议字段。

## Client 流程

正常写入：

1. metadata-changing write 生成 `client_mutation_id`。
2. mutation 先进入 client 内存 journal。
3. client 执行本地 metadata 变更。
4. 如果 master 可达，发送带 `client_id/client_mutation_id` 的有序 mutation RPC。
5. master ack 返回 durable cursor 后，client 裁剪 `id <= cursor` 的 journal。

master 不可达：

1. client 进入 degraded。
2. metadata-changing write 仍然可以继续，但必须进入内存 journal。
3. 不再向旧 master 发送 metadata RPC。
4. 连接到新 master 后，从 journal 中 replay 缺失 mutations。

连接到新 master：

1. client 调用 `RegisterClient` 上报当前 endpoint/segments。
2. master 返回该 `client_id` 的 `last_mutation_id`。
3. client 从内存 journal 中按顺序 replay `id > last_mutation_id` 的 mutations。
4. replay 完成后，client 通知 master 清除 syncing 状态。

## Master 流程

master 维护内存 map：

```text
client_id -> last_mutation_id
```

`RegisterClient` 在 P2P Redis HA 模式下需要明确为幂等：

- client 不存在时，保持现有首次注册语义，创建 client metadata，并写
  `REGISTER_CLIENT` oplog。
- client 已存在时，返回成功，更新 endpoint/liveness/syncing 等运行态信息，不再
  写新的 `REGISTER_CLIENT` oplog。
- response 返回 `view_version` 和该 client 当前 `last_mutation_id`。

这保留了旧 `RegisterClient` API，同时把现有测试侧把 `CLIENT_ALREADY_EXISTS`
视为 already restored 的 workaround 收敛到 master 接口语义里。

收到 client mutation：

1. 如果 `client_mutation_id <= last_mutation_id`，认为是重复 replay，直接返回成功。
2. 如果 `client_mutation_id > last_mutation_id`，按请求顺序应用 mutation。
3. 写入 P2P master oplog，oplog entry 携带 `client_id/client_mutation_id`。
4. oplog durable 后推进该 client 的 `last_mutation_id`。
5. 返回最新 durable cursor 给 client。

顺序要求：

- client 对同一个 `client_id` 的 replay 必须串行发送。
- master 对同一个 `client_id` 的 mutation 必须串行处理。
- `client_mutation_id` 不连续，所以 master 不能靠 gap 检测缺失 mutation。
- 正确性依赖 client 按 journal 顺序 replay。

## Redis / Oplog 流程

Redis 中不需要 client journal。client 重启后不恢复本地数据，因此也不需要从
Redis 恢复 client journal。

Redis 侧需要保证 master cursor 可恢复：

- P2P master oplog entry 增加 `client_id` 和 `client_mutation_id`。
- standby replay oplog 时同步维护 `client_id -> last_mutation_id`。
- standby promote 后，可以直接回答 client 的 `last_mutation_id` 查询。
- 如果 standby 通过 snapshot bootstrap，snapshot 也要包含 client cursor map。

这样 client 不需要扫描 master oplog，也不需要把 journal 写到 Redis。新 master 的
点位来自 standby 已经 replay 出来的 cursor map。

## RPC 变化

第一版不新增 reconnect RPC；复用并增强 `RegisterClient` 的 P2P 幂等语义。

第一版仍建议新增 Redis HA 专用 ordered replay RPC，而不是复用
`BatchSyncReplica`。

原因：

- `BatchSyncReplica` 用 add/remove 数组表达最终状态，不保证全局 mutation 顺序。
- degraded 期间同一个 key 的 add/remove 顺序不能丢。
- replay 需要逐条携带 `client_mutation_id` 做幂等。

建议接口：

- `ReplayClientMutations(client_id, mutations)`。
- `FinishClientReplay(client_id, replay_high_watermark)`。

`ReplayClientMutations` response 返回：

- 每条 mutation 的结果。
- master 最新 durable `last_mutation_id`。

## Failure 策略

明确的设计边界：

- client 进程重启后内存 journal 丢失，不做自动补 replay。
- 这种 client 按 fresh registration 处理，不尝试恢复重启前的本地 metadata
  mutations。

必须避免的错误：

- master ack 早于 oplog durable，导致 client trim 后 failover 丢 mutation。
- replay 并发乱序，导致较大的 mutation id 先推进 cursor，较小的 mutation 被误判为重复。
- snapshot bootstrap 不包含 cursor map，导致 promoted master 不知道 client 点位。

## 第一版实现项

第一版需要实现：

- client 内存 mutation journal。
- `client_mutation_id` 生成器。
- Redis HA reconnect 后查询 master cursor。
- ordered replay RPC。
- master 侧 `client_id -> last_mutation_id` cursor map。
- master oplog entry 携带 `client_id/client_mutation_id`。
- standby replay oplog 时恢复 cursor map。
- snapshot bootstrap 同步 cursor map。
- replay 完成后清理 syncing 状态。

建议测试：

- master 切换后只 replay 缺失 mutations。
- degraded 期间同一个 key add/remove 顺序正确。
- 重复 replay 不会重复生效。
- master ack 前 crash，新 master 要求 client 继续 replay。
- snapshot bootstrap 后 promoted master 能返回正确 client cursor。
