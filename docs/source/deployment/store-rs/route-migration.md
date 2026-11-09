# Route Migration Guide

Last updated: 2026-04-23

This guide is for operators and control-plane callers that need to submit
explicit key-level route migration tasks in `mooncake-store-rs`.

## 1. Capability Summary

The current implementation exposes two explicit route-migration task types.

- `copy`
  - requires an explicit `source_segment`
  - accepts one or more explicit `target_segments`
  - keeps the source replica and appends the target replica set to the
    authoritative route
- `move`
  - requires an explicit `source_segment`
  - currently accepts exactly one `target_segment`
  - removes the source replica from the authoritative route after the target
    replica is committed

Shared constraints:

- the migration granularity is `tenant/domain/object_set/key`
- the durable truth remains the object route, not the admin task record
- the admin task queue exists only in the memory of `mooncake-store-rs-admin server`
- multi-target `copy` currently uses all-or-nothing semantics
- `task_executor` must be explicitly selected

## 2. Operator Surfaces

The current product surface exposes two operator entry points:

1. admin HTTP API
2. `mooncake-store-rs-admin --admin-url ...` CLI

There is currently no dedicated Python route-migration API.

### 2.1 HTTP API

Endpoint paths, request fields, and retry semantics are documented in the [Store-RS Admin HTTP API reference](../../api-reference/http/store-rs-admin.md).

### 2.2 CLI

`mooncake-store-rs-admin` is an operator client for the admin HTTP surface. It does
not own a private task queue.

Current commands:

- `mooncake-store-rs-admin ... migrate copy`
- `mooncake-store-rs-admin ... migrate move`
- `mooncake-store-rs-admin ... migrate task list`
- `mooncake-store-rs-admin ... migrate task get`

Implementation entry point:

- [mooncake-store-rs-admin.rs](https://github.com/kvcache-ai/Mooncake/blob/main/mooncake-store-rs/crates/mooncake-store-rs-admin/src/bin/mooncake-store-rs-admin.rs)

Key constraints:

- `migrate` commands must pass `--admin-url`
- the CLI does not start an in-process task queue
- route migration is asynchronous; it is not a synchronous CLI subcommand
- submit request bodies accept only the documented fields; legacy `mode` or
  other unknown fields return `400`

## 3. Execution Flow and Responsibility Boundaries

The current end-to-end flow is:

1. the caller submits a task through admin HTTP or through
   `mooncake-store-rs-admin --admin-url ...`
2. `mooncake-store-rs-admin server` stores the task in an in-memory queue
3. admin resolves the live lease for the selected `task_executor`
4. admin submits `SubmitMigrationTask` through control-plane RPC to the chosen
   executor runtime
5. the executor runtime receives the task in `LocalMigrationAdapter`
6. the executor runtime reuses `execute_explicit_route_migration(...)`
7. the executor writes the target replica set and CAS-publishes the new route
8. admin polls `GetMigrationExecutionStatus`; if executor status is lost, admin
   falls back to authoritative route visibility

Key implementation locations:

- admin scheduling and retry:
  - [service.rs](https://github.com/kvcache-ai/Mooncake/blob/main/mooncake-store-rs/crates/mooncake-store-rs-admin/src/admin/service.rs)
- control-plane migration service:
  - [control_plane/mod.rs](https://github.com/kvcache-ai/Mooncake/blob/main/mooncake-store-rs/crates/mooncake-store-client/src/control_plane/mod.rs)
  - [server.rs](https://github.com/kvcache-ai/Mooncake/blob/main/mooncake-store-rs/crates/mooncake-store-client/src/control_plane/server.rs)
  - [client.rs](https://github.com/kvcache-ai/Mooncake/blob/main/mooncake-store-rs/crates/mooncake-store-client/src/control_plane/client.rs)
- executor-side worker:
  - [state_adapters.rs](https://github.com/kvcache-ai/Mooncake/blob/main/mooncake-store-rs/crates/mooncake-store-client/src/client/state_adapters.rs)
- migration execution kernel:
  - [runtime_io.rs](https://github.com/kvcache-ai/Mooncake/blob/main/mooncake-store-rs/crates/mooncake-store-client/src/client/runtime_io.rs)

## 4. What `task_executor` Means

`task_executor` is not a temporary thread and it is not the admin process.

It means:

- which stable runtime is responsible for executing the migration task

Current executor-side implementation:

- each runtime can host a long-lived `LocalMigrationAdapter`
- the adapter owns a long-lived worker thread
- each task is queued to that worker instead of spawning a one-off execution
  thread

The current model is therefore “select a runtime and let its background worker
execute the task”, not “one task equals one thread”.

## 5. Recommended Usage

### 5.1 Operator or Script Usage

Prefer the CLI for manual operations or lightweight scripts:

```bash
mooncake-store-rs-admin \
  --metadata-url redis://127.0.0.1:6380/0 \
  --admin-url http://127.0.0.1:18080 \
  migrate copy \
  --authority source-store \
  --tenant tenant-a \
  --domain domain-a \
  --object-set set-a \
  --key object-a \
  --source-segment source-segment \
  --target-segment target-segment-a \
  --target-segment target-segment-b \
  --task-executor executor-store \
  --max-retries 5
```

Query tasks:

```bash
mooncake-store-rs-admin \
  --metadata-url redis://127.0.0.1:6380/0 \
  --admin-url http://127.0.0.1:18080 \
  migrate task list

mooncake-store-rs-admin \
  --metadata-url redis://127.0.0.1:6380/0 \
  --admin-url http://127.0.0.1:18080 \
  migrate task get \
  --task-id route-migration-1
```

### 5.2 Programmatic Controller Usage

Prefer direct admin HTTP calls from an operator/controller:

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

Query tasks:

```bash
curl http://127.0.0.1:18080/v1/route-migrations
curl http://127.0.0.1:18080/v1/route-migrations/<task_id>
```

## 6. Request Fields

See the [HTTP API request reference](../../api-reference/http/store-rs-admin.md#route-migration-api) for field names and request constraints.

## 7. Failure and Retry Semantics

See the [HTTP API retry reference](../../api-reference/http/store-rs-admin.md#route-migration-api) for completion and failure semantics.

## 8. Scope Boundaries

What this feature is:

- an explicit operator/control-plane migration surface
- a task-based way to pre-place or rebalance selected keys
- a route-authoritative migration mechanism

What this feature is not:

- not a full-segment migration API
- not a durable scheduler that survives admin restart
- not a replacement for the normal `put/get/remove` data plane

## 9. Related Docs

- [Python Guide](../../api-reference/python/store-rs.md)
- [Features](../../getting_started/store-rs-features.md)
- [Configuration Reference](configuration.md)
- [Architecture](../../design/store/store-rs/architecture.md)
