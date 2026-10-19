# Store-RS Admin HTTP API

This reference documents HTTP endpoints served by the standalone
mooncake-store-rs-admin server. Operator workflows and CLI examples are in the
[route migration guide](../../deployment/store-rs/route-migration.md) and
[Store-RS operations](../../deployment/store-rs/index.md).

## Route Migration API

### HTTP endpoints

The long-lived `mooncake-store-rs-admin server` exposes:

- `POST /v1/route-migrations/copy`
- `POST /v1/route-migrations/move`
- `GET /v1/route-migrations`
- `GET /v1/route-migrations/<task_id>`

Implementation entry points:

- [http.rs](https://github.com/kvcache-ai/Mooncake/blob/main/mooncake-store-rs/crates/mooncake-store-rs-admin/src/admin/http.rs)
- [service.rs](https://github.com/kvcache-ai/Mooncake/blob/main/mooncake-store-rs/crates/mooncake-store-rs-admin/src/admin/service.rs)

### Request fields

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

### Retry and completion

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

Reserved but not currently exposed in P1:

- `cancelled`

## Cold-Tier Device Administration

The Admin HTTP API uses `/v1/cold-tier` as its base path. It manages Mooncake cold tier devices, not operating-system block devices or mounts. Operating-system provisioning remains the responsibility of deployment tooling.

Device identity fields:

| Field | Meaning |
|---|---|
| `device_id` | Mooncake-managed logical device identity used by routes and placement |
| `cold_tier_id` | Human-readable cold tier alias, for example `ssd-0` |
| `stable_id` | Storage runtime stable identity |
| `epoch` | Current live runtime incarnation for fencing |
| `kind` | Cold tier kind, for example `ssd` or `nfs` |
| `target` | Runtime-resolved target spec such as a directory or filesystem UUID |
| `root_dir` | Resolved cold root directory |
| `state` | Device lifecycle state |
| `schedulable` | Whether the device accepts new offload placement |

Implemented device operations include create, list, get, register, unregister, disable, enable, drain, blockers query, manual GC, manual free, object cold backing query, pending offload trigger, offload task status/listing, and quarantine reporting.

`register` and `unregister` are Mooncake registry operations. `unregister` without force must not strand cold-only objects on the removed device. Safe forced unregister is allowed only when materialized objects still have hot replicas and can be marked `PendingDelete`.
