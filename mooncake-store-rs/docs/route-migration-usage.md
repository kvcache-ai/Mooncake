# Route Migration 使用手册

最后更新：2026-04-23

本文只说明 `route migration` 这个特性的使用方式：它能做什么、需要哪些前置条件、如何启动服务、如何通过 HTTP 或 CLI 提交任务，以及如何查询任务结果。

## 1. 能力概览

当前显式 route migration 提供两类任务：

- `copy`
  - 显式指定 `source_segment`
  - 显式指定一个或多个 `target_segment`
  - 成功后保留 source replica，并把目标 replica 加入 authoritative route
- `move`
  - 显式指定 `source_segment`
  - 当前只支持一个 `target_segment`
  - 成功后 authoritative route 去掉 source replica，只保留目标 replica

当前约束：

- 迁移粒度是 `tenant/domain/object_set/key`
- durable truth 是 object route，不是 admin task record
- admin task queue 只存在于 `mooncake-store-admin-server` 进程内存里
- `copy` 支持多目标，当前采用 `all-or-nothing`
- `task_executor` 必须显式指定

## 2. 前置条件

使用 route migration 前，需要准备：

- 一个可用的 metadata backend
- 至少一个 source store runtime 和一个 target store runtime
- 一个常驻的 `mooncake-store-admin-server`
- 一个能访问 admin server 的调用方：HTTP client 或 `mooncake-store-admin --admin-url ...`

运行时要求：

- source segment 必须存在于当前 authoritative route 中
- `move` 当前必须且只能有一个 target segment
- `copy` 至少需要一个 target segment
- `task_executor` 必须对应一个当前可用的 runtime stable id

## 3. 对外接口

### 3.1 HTTP API

由常驻的 `mooncake-store-admin-server` 暴露：

- `POST /v1/route-migrations/copy`
- `POST /v1/route-migrations/move`
- `GET /v1/route-migrations`
- `GET /v1/route-migrations/<task_id>`

语义：

- `POST /copy` 提交 `copy` 任务
- `POST /move` 提交 `move` 任务
- `GET list` 查询当前 admin 进程内存里的任务列表
- `GET <task_id>` 查询单个任务状态

### 3.2 CLI

`mooncake-store-admin` 是 admin HTTP 的 operator client，本身不持有任务队列。

当前支持：

- `mooncake-store-admin ... migrate copy`
- `mooncake-store-admin ... migrate move`
- `mooncake-store-admin ... migrate task list`
- `mooncake-store-admin ... migrate task get`

关键约束：

- `migrate` 命令必须显式传 `--admin-url`
- CLI 不会自起一个私有 in-process task queue
- route migration 是异步任务，不是同步 CLI 子过程
- submit body 只接受文档列出的字段；未知字段会返回 `400`

## 4. 启动 admin server

示例：

```bash
cargo run -p mooncake-store-py --bin mooncake-store-admin-server -- \
  --metadata-url redis://127.0.0.1:6380/0 \
  --keyspace mc/store-rs/demo \
  --bind-addr 127.0.0.1:18080
```

说明：

- route migration task queue 只存在于 `mooncake-store-admin-server` 进程内存中
- admin server 重启后，未完成任务不会恢复
- authoritative durable state 仍然是 object route

## 5. 提交任务

### 5.1 HTTP `copy`

```bash
curl -X POST http://127.0.0.1:18080/v1/route-migrations/copy \
  -H 'Content-Type: application/json' \
  -d '{
    "authority": "source-store",
    "tenant": "tenant-a",
    "domain": "domain-a",
    "object_set": "set-a",
    "key": "object-a",
    "source_segment": "source-segment",
    "target_segments": ["target-a", "target-b"],
    "task_executor": "executor-store",
    "max_retries": 5
  }'
```

### 5.2 HTTP `move`

```bash
curl -X POST http://127.0.0.1:18080/v1/route-migrations/move \
  -H 'Content-Type: application/json' \
  -d '{
    "authority": "source-store",
    "tenant": "tenant-a",
    "domain": "domain-a",
    "object_set": "set-a",
    "key": "object-a",
    "source_segment": "source-segment",
    "target_segments": ["target-a"],
    "task_executor": "executor-store",
    "max_retries": 5
  }'
```

### 5.3 CLI `copy`

```bash
mooncake-store-admin \
  --metadata-url redis://127.0.0.1:6380/0 \
  --admin-url http://127.0.0.1:18080 \
  migrate copy \
  --authority source-store \
  --tenant tenant-a \
  --domain domain-a \
  --object-set set-a \
  --key object-a \
  --source-segment source-segment \
  --target-segment target-a \
  --target-segment target-b \
  --task-executor executor-store \
  --max-retries 5
```

### 5.4 CLI `move`

```bash
mooncake-store-admin \
  --metadata-url redis://127.0.0.1:6380/0 \
  --admin-url http://127.0.0.1:18080 \
  migrate move \
  --authority source-store \
  --tenant tenant-a \
  --domain domain-a \
  --object-set set-a \
  --key object-a \
  --source-segment source-segment \
  --target-segment target-a \
  --task-executor executor-store \
  --max-retries 5
```

## 6. 查询任务

HTTP：

```bash
curl http://127.0.0.1:18080/v1/route-migrations
curl http://127.0.0.1:18080/v1/route-migrations/<task_id>
```

CLI：

```bash
mooncake-store-admin \
  --metadata-url redis://127.0.0.1:6380/0 \
  --admin-url http://127.0.0.1:18080 \
  migrate task list

mooncake-store-admin \
  --metadata-url redis://127.0.0.1:6380/0 \
  --admin-url http://127.0.0.1:18080 \
  migrate task get \
  --task-id route-migration-1
```

## 7. 请求字段说明

任务提交请求包含这些字段：

- `authority`
  - route authority stable id
- `tenant`
  - 必填
- `domain`
  - 可选，默认 domain
- `object_set`
  - 可选，默认 object_set
- `key`
  - 逻辑对象 key
- `source_segment`
  - source replica 所在 segment
- `target_segments`
  - target segment 列表
- `task_executor`
  - 执行任务的 runtime stable id
- `max_retries`
  - admin 侧最大重试次数，默认 `5`

约束：

- `copy` 至少需要一个 target
- `move` 当前必须且只能有一个 target
- `task_executor` 不能为空
- 提交请求不再接受 `mode` 字段；`copy` / `move` 语义由 path 决定

## 8. 状态与重试语义

当前 task state：

- `pending`
- `dispatching`
- `running`
- `retry_wait`
- `succeeded`
- `failed`
- `cancelled`

当前 retry 语义：

- admin 活着时，会对 executor 丢失或瞬时 RPC 失败做自动重试
- 默认重试预算 `5`
- 如果 executor 状态丢失，但 authoritative route 已显示任务完成，admin 会直接把任务标记为 `succeeded`
- 如果 admin 重启，内存队列丢失；durable truth 仍然是 route

## 9. 使用建议

建议的调用方式：

- 运维脚本或人工操作：优先使用 `mooncake-store-admin --admin-url ...`
- 程序化控制器：优先直接调 admin HTTP
- 将 route migration 视为独立的 operator / control-plane 能力，不要把它混到正常数据面读写调用里

## 10. 相关文档

- [features.md](./features.md)
- [python.md](./python.md)
- [configuration.md](./configuration.md)
