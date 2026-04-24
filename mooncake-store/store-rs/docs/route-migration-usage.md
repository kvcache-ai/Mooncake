# Route Migration Guide

Last updated: 2026-04-23

This guide is for operators and control-plane callers that need to submit
explicit key-level route migration tasks in `mooncake-pro`.

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
- the admin task queue exists only in the memory of `mooncake-store-admin server`
- multi-target `copy` currently uses all-or-nothing semantics
- `task_executor` must be explicitly selected

## 2. Operator Surfaces

The current product surface exposes two operator entry points:

1. admin HTTP API
2. `mooncake-store-admin --admin-url ...` CLI

There is currently no dedicated Python route-migration API.

### 2.1 HTTP API

The long-lived `mooncake-store-admin server` exposes:

- `POST /v1/route-migrations/copy`
- `POST /v1/route-migrations/move`
- `GET /v1/route-migrations`
- `GET /v1/route-migrations/<task_id>`

Implementation entry points:

- [http.rs](../crates/mooncake-store-py/src/admin/http.rs)
- [service.rs](../crates/mooncake-store-py/src/admin/service.rs)

### 2.2 CLI

`mooncake-store-admin` is an operator client for the admin HTTP surface. It does
not own a private task queue.

Current commands:

- `mooncake-store-admin ... migrate copy`
- `mooncake-store-admin ... migrate move`
- `mooncake-store-admin ... migrate task list`
- `mooncake-store-admin ... migrate task get`

Implementation entry point:

- [mooncake-store-admin.rs](../crates/mooncake-store-py/src/bin/mooncake-store-admin.rs)

Key constraints:

- `migrate` commands must pass `--admin-url`
- the CLI does not start an in-process task queue
- route migration is asynchronous; it is not a synchronous CLI subcommand
- submit request bodies accept only the documented fields; legacy `mode` or
  other unknown fields return `400`

## 3. Execution Flow and Responsibility Boundaries

The current end-to-end flow is:

1. the caller submits a task through admin HTTP or through
   `mooncake-store-admin --admin-url ...`
2. `mooncake-store-admin server` stores the task in an in-memory queue
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
  - [service.rs](../crates/mooncake-store-py/src/admin/service.rs)
- control-plane migration service:
  - [control_plane/mod.rs](../crates/mooncake-store-client/src/control_plane/mod.rs)
  - [server.rs](../crates/mooncake-store-client/src/control_plane/server.rs)
  - [client.rs](../crates/mooncake-store-client/src/control_plane/client.rs)
- executor-side worker:
  - [state_adapters.rs](../crates/mooncake-store-client/src/client/state_adapters.rs)
- migration execution kernel:
  - [runtime_io.rs](../crates/mooncake-store-client/src/client/runtime_io.rs)

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
  --target-segment target-segment-a \
  --target-segment target-segment-b \
  --task-executor executor-store \
  --max-retries 5
```

Query tasks:

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

The current request surface uses these key fields:

- `authority`
  - route authority stable id
- `tenant`
  - required
- `domain`
  - optional, defaults to the default domain
- `object_set`
  - optional, defaults to the default object set
- `key`
  - logical object key
- `source_segment`
  - segment that currently hosts the source replica
- `target_segments`
  - explicit target segment list
- `task_executor`
  - stable id of the runtime that executes the task
- `max_retries`
  - admin-side retry budget override; default is `5`

Constraints:

- `copy` requires at least one target
- `move` currently requires exactly one target
- `task_executor` must not be empty
- the admin queue is not persisted; queued tasks are lost if the admin server
  restarts

## 7. Failure and Retry Semantics

Retry is owned by the admin server, not by the CLI.

Current semantics:

- while admin is alive, it automatically retries executor loss and transient RPC
  failures
- the default retry budget is `5`
- if executor status is lost but the authoritative route already shows the task
  is complete, admin marks the task as `succeeded`
- if the admin process restarts, the in-memory queue is lost; the durable truth
  remains the route

Task states currently exposed to callers:

- `pending`
- `dispatching`
- `running`
- `retry_wait`
- `succeeded`
- `failed`
- `cancelled`

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

- [Python Guide](./python.md)
- [Features](./features.md)
- [Configuration Reference](./configuration.md)
- [Architecture](./architecture.md)
