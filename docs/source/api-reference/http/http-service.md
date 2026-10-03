# Mooncake Store HTTP Service

The Mooncake Store HTTP Service provides RESTful endpoints for cluster management, monitoring, and data operations. This service is embedded within the `mooncake_master` process and can be enabled alongside the primary RPC services.

## Overview

The HTTP service serves multiple purposes:
- **Metrics & Monitoring**: Prometheus-compatible metrics endpoints
- **Health & HA Inspection**: Service availability, HA role/state, and leader information
- **Cluster Management**: Query and manage distributed storage segments
- **Data Inspection**: Examine stored objects and their replicas
- **Maintenance Operations**: Drain jobs, tenant quota policies, and bulk deletion

The service listens on the master metrics port, configured with `--metrics_port`
(default `9003`) and `--metrics_host` (default `0.0.0.0`). All examples below use
port `9003`.

The Python `mooncake.mooncake_store_service` module also provides a lightweight
Store REST API for data operations and standalone segment mount/unmount
workflows. Unless configured otherwise, it listens on port `8080`. See
[Store REST API Endpoints](#store-rest-api-endpoints).

## Common Behavior

### Service-plane gating

Endpoints that access stored metadata are only served when the master holds the
active service plane. In HA mode, standby masters and masters that are still
starting up reject these requests with HTTP `503`:

```json
{
  "success": false,
  "error_code": -1011,
  "error_message": "service plane is not active"
}
```

The gating does not apply to `/metrics`, `/metrics/summary`, `/health`,
`/version`, `/role`, `/ha_status`, and `/leader`, which remain available in
every HA role.

### Error responses

Most JSON endpoints report failures with a common structure:

```json
{
  "success": false,
  "error_code": -704,
  "error_message": "OBJECT_NOT_FOUND"
}
```

The HTTP status code is derived from the error code:

| HTTP status | Error codes |
|-------------|-------------|
| `400 Bad Request` | `INVALID_PARAMS` |
| `404 Not Found` | `JOB_NOT_FOUND`, `SEGMENT_NOT_FOUND`, `OBJECT_NOT_FOUND`, `TENANT_NOT_REGISTERED` |
| `409 Conflict` | `UNAVAILABLE_IN_CURRENT_MODE`, `UNAVAILABLE_IN_CURRENT_STATUS`, `TENANT_NOT_EMPTY` |
| `500 Internal Server Error` | all other error codes |
| `503 Service Unavailable` | service plane is not active (see above) |

## HTTP Endpoints

### Metrics Endpoints

#### `/metrics`
Prometheus-compatible metrics endpoint providing detailed system metrics in text format. When tenant quota is enabled, per-tenant quota gauges and counters are included as well.

**Method**: `GET`
**Content-Type**: `text/plain; version=0.0.4`
**Response**: Comprehensive metrics including request counts, error rates, latency statistics, and resource utilization

**Example**:
```bash
curl http://localhost:9003/metrics
```

#### `/metrics/summary`
Human-readable metrics summary with key performance indicators.

**Method**: `GET`
**Content-Type**: `text/plain; version=0.0.4`
**Response**: Single-line summary with HA role/state, service readiness, master and HA metric counters, and (in HA mode) the observed leader address and view version

**Example**:
```bash
curl http://localhost:9003/metrics/summary
```

```text
role=primary, state=serving, service_ready=true, master={...}, ha={...}, leader=192.168.1.10:50051, view_version=7
```

### Health & HA Endpoints

These endpoints reflect the local process state and are available in every HA
role, including standby.

#### `/health`
Health check endpoint with role and readiness information.

**Method**: `GET`
**Content-Type**: `application/json; charset=utf-8`
**Response**: JSON object describing process health

**Example**:
```bash
curl http://localhost:9003/health
```

**Response Format**:
```json
{
  "status": "ok",
  "role": "primary",
  "ha_state": "serving",
  "service_ready": true,
  "leader_address": "192.168.1.10:50051",
  "view_version": 7
}
```

**Fields**:
- `status` (string): Always `"ok"` when the HTTP server is up
- `role` (string): HA runtime role of this process (e.g. `primary`, `standby`)
- `ha_state` (string): HA runtime state (e.g. `starting`, `serving`)
- `service_ready` (boolean): Whether the metadata service plane is active on this process
- `leader_address` (string, optional): Observed leader address; only present in HA mode
- `view_version` (integer, optional): Leader view version; only present in HA mode

#### `/version`
Report the master version. Always available, including while the master is in
standby.

**Method**: `GET`
**Content-Type**: `application/json; charset=utf-8`
**Response**: JSON object with:
- `version` (string): Store version used for RPC handshake compatibility
- `display_version` (string): Human-readable release plus short git hash

**Example**:
```bash
curl http://localhost:9003/version
```

```json
{"version":"2.0.0","display_version":"0.3.12.post1 (git: f9e8311f)"}
```

Real clients expose the same `/version` payload on their own client HTTP port
when `enable_client_http_server` is on. See
[Client Metrics Endpoint](../../getting_started/observability.md#client-metrics-endpoint).

#### `/role`
Return the HA runtime role of this process.

**Method**: `GET`
**Content-Type**: `text/plain; charset=utf-8`
**Response**: Role string, e.g. `primary` or `standby`

**Example**:
```bash
curl http://localhost:9003/role
```

#### `/ha_status`
Return the HA runtime state of this process.

**Method**: `GET`
**Content-Type**: `text/plain; charset=utf-8`
**Response**: State string, e.g. `starting` or `serving`

**Example**:
```bash
curl http://localhost:9003/ha_status
```

#### `/leader`
Return the leader currently observed by this process.

**Method**: `GET`
**Content-Type**: `application/json; charset=utf-8`
**Response**: JSON object with leader information

**Example**:
```bash
curl http://localhost:9003/leader
```

**Response Format**:
```json
{
  "present": true,
  "leader_address": "192.168.1.10:50051",
  "view_version": 7
}
```

**Fields**:
- `present` (boolean): Whether a leader view exists (always `false` in non-HA mode)
- `leader_address` (string, optional): Leader address when `present` is `true`
- `view_version` (integer, optional): Leader view version when `present` is `true`

#### `/kv_events/status`
Return publish statistics of the master's KV event publisher (ZeroMQ based event
stream for cache consumers such as vLLM).

**Method**: `GET`
**Content-Type**: `application/json; charset=utf-8`
**Response**: JSON object with publisher statistics

**Example**:
```bash
curl http://localhost:9003/kv_events/status
```

**Response Format**:
```json
{
  "enabled": true,
  "published_batches": 1024,
  "published_events": 65536,
  "dropped_events": 0,
  "skipped_unparsed_keys": 3
}
```

**Fields**:
- `enabled` (boolean): Whether KV event publishing is enabled (`--enable_kv_events`)
- `published_batches` (integer): Total published event batches
- `published_events` (integer): Total published events
- `dropped_events` (integer): Events dropped due to queue pressure
- `skipped_unparsed_keys` (integer): Events skipped because the key could not be parsed

### Data Management Endpoints

#### `/query_key`
Retrieve replica information for a specific key, including memory locations and transport endpoints.

**Method**: `GET`
**Parameters**: `key` (query parameter) - The object key to query
**Content-Type**: `application/json; charset=utf-8`
**Response**: JSON object with success status and replica data array. Only memory replicas are included; use `/batch_query_keys` to inspect disk and NoF replicas.

**Example**:
```bash
curl "http://localhost:9003/query_key?key=my_object"
```

**Success Response** (HTTP 200):
```json
{
  "success": true,
  "data": [
    {
      "size_": 1073741824,
      "buffer_address_": 140732000000000,
      "protocol_": "rdma",
      "transport_endpoint_": "192.168.1.100:12345"
    }
  ]
}
```

**Error Response** (key not found, HTTP 404):
```json
{
  "success": false,
  "error_code": -704,
  "error_message": "OBJECT_NOT_FOUND"
}
```

**Error Response** (service unavailable, HTTP 503):
```json
{
  "success": false,
  "error_code": -1011,
  "error_message": "service plane is not active"
}
```

```{note}
`/query_key` queries the `default` tenant and goes through the regular
read path: it grants a read lease on the object, may trigger promotion, and
updates cache-hit metrics. Use `/batch_query_keys` for a purely read-only
inspection.
```

#### `/batch_query_keys`
Retrieve replica information for multiple keys in a single request, including memory locations and transport endpoints for each key. The endpoint performs a read-only metadata lookup and does not grant leases, trigger promotion, or update cache-hit metrics.

**Method**: `GET`
**Parameters**: `keys` (query parameter) - Comma-separated list of object keys to query (format: key1,key2,key3). Keys are looked up in the `default` tenant.
**Content-Type**: `application/json; charset=utf-8`
**Response**: JSON-formatted mapping of keys to their respective replica descriptors

**Example**:
```bash
curl "http://localhost:9003/batch_query_keys?keys=key1,key2,key3"
```

**Response Format**:
```text
{
  "success": true,
  "data": {
    "key1": {
      "ok": true,
      "values": [
        {
          "size_": 1073741824,
          "buffer_address_": 140732000000000,
          "protocol_": "rdma",
          "transport_endpoint_": "hostname:port"
        }
      ],
      "disk_values": [
        {
          "file_path": "/path/to/object",
          "object_size": 4096
        }
      ],
      "local_disk_values": [
        {
          "client_id": "12345-67890",
          "object_size": 4096,
          "transport_endpoint": "hostname:port"
        }
      ],
      "nof_values": [
        {
          "size_": 1073741824,
          "buffer_address_": 140732000000000,
          "protocol_": "rdma",
          "transport_endpoint_": "hostname:port"
        }
      ]
    },
    "key2": {
      "ok": false,
      "error": "error message"
    }
  }
}
```

The `values` field is always present (empty array when no memory replica exists). The `disk_values`, `local_disk_values`, and `nof_values` fields are optional and only appear when the corresponding replica type is present for the key.

**Error Response** (missing keys parameter, HTTP 400):
```json
{
  "success": false,
  "error": "No keys provided. Use ?keys=key1,key2,..."
}
```

#### `/get_all_keys`
List all keys currently stored in the distributed system.

**Method**: `GET`
**Content-Type**: `text/plain; version=0.0.4`
**Response**: Newline-separated list of all stored keys

**Example**:
```bash
curl http://localhost:9003/get_all_keys
```

### Segment Management Endpoints

#### `/get_all_segments`
List all mounted segments in the cluster.

**Method**: `GET`
**Content-Type**: `text/plain; version=0.0.4`
**Response**: Newline-separated list of segment names

**Example**:
```bash
curl http://localhost:9003/get_all_segments
```

#### `/query_segment`
Query detailed information about a specific segment, including used and available capacity.

**Method**: `GET`
**Parameters**: `segment` (query parameter) - Segment name to query
**Content-Type**: `text/plain; version=0.0.4`
**Response**: Multi-line text with segment details

**Example**:
```bash
curl "http://localhost:9003/query_segment?segment=segment_name"
```

**Response Format**:
```
segment_name
Used(bytes): 1073741824
Capacity(bytes): 4294967296
```

#### `/get_segments_detail`
Get detailed information of all segments in JSON format, including segment metadata, allocator usage, and status.

**Method**: `GET`
**Content-Type**: `application/json; charset=utf-8`
**Response**: JSON object containing an array of segment details

**Example**:
```bash
curl http://localhost:9003/get_segments_detail
```

**Response Format**:
```json
{
  "total_segments": 2,
  "segments": [
    {
      "segment_name": "segment_0",
      "segment_id": "00000000-0000-0000-0000-000000000001",
      "client_id": "00000000-0000-0000-0000-000000000002",
      "base_address": "0x300000000",
      "size_bytes": 17179869184,
      "size_human": "16 GiB",
      "te_endpoint": "192.168.1.1:12345",
      "protocol": "rdma",
      "status": "MOUNTED",
      "allocator_used_bytes": 1073741824,
      "allocator_capacity_bytes": 17179869184,
      "allocator_usage_percent": 6.25
    }
  ]
}
```

**Fields**:
- `total_segments` (integer): Total number of segments in the cluster
- `segments` (array): Array of segment detail objects
  - `segment_name` (string): Name of the segment
  - `segment_id` (string): UUID of the segment
  - `client_id` (string): UUID of the client that owns the segment
  - `base_address` (string): Base memory address in hex
  - `size_bytes` (integer): Segment size in bytes
  - `size_human` (string): Human-readable segment size
  - `te_endpoint` (string): Transport endpoint address
  - `protocol` (string): Transfer protocol (e.g., rdma, tcp)
  - `status` (string): Current segment status
  - `allocator_used_bytes` (integer): Bytes currently allocated
  - `allocator_capacity_bytes` (integer): Total allocator capacity in bytes
  - `allocator_usage_percent` (number): Percentage of allocator capacity used

#### `/api/v1/segments/status`
Query the lifecycle status of a single segment.

**Method**: `GET`
**Parameters**: `segment` (query parameter) - Segment name to query
**Content-Type**: `application/json; charset=utf-8`
**Response**: JSON object with the segment status

**Example**:
```bash
curl "http://localhost:9003/api/v1/segments/status?segment=segment_name"
```

**Response Format**:
```json
{
  "success": true,
  "segment": "segment_name",
  "status": 1,
  "status_name": "OK"
}
```

**Fields**:
- `success` (boolean): Whether the query succeeded
- `segment` (string): Segment name echoed back
- `status` (integer): Numeric segment status
- `status_name` (string): Segment status name. One of `UNDEFINED` (0), `OK` (1), `DRAINING` (2), `DRAINED` (3), `GRACEFULLY_UNMOUNTING` (4), `UNMOUNTING` (5)

#### `PUT /api/v1/segments/status`
Switch a segment between `OK` and `DRAINING` without creating a drain job.
While a segment is `DRAINING`, new allocations skip it, and replicas already on
it stay in place and remain readable. Setting `OK` makes it available for new
allocations again.

**Method**: `PUT`
**Parameters**: `segment` (query parameter) - Segment name
**Request Body**: JSON object with `status` set to `"OK"` or `"DRAINING"`
**Content-Type**: `application/json; charset=utf-8`
**Response**: JSON object with `success`, `segment`, `status`, and
`status_name`

**Status Codes**:
- `200 OK`: The segment is now in the requested status, including when it
  already was
- `400 Bad Request`: Missing `segment`, malformed body, or a `status` other
  than `OK` or `DRAINING`
- `404 Not Found`: No mounted segment has this name
- `409 Conflict`: An unfinished drain job lists this segment as a source, or
  the segment is in a status other than `OK` or `DRAINING`

**Example**:
```bash
curl -X PUT "http://localhost:9003/api/v1/segments/status?segment=segment_0" \
  -H "Content-Type: application/json" \
  -d '{"status": "DRAINING"}'
```

```json
{"success":true,"segment":"segment_0","status":2,"status_name":"DRAINING","error_code":0,"error_message":""}
```

The change is not written to the HA operation log, so it is not guaranteed to
survive a master failover.

### DFS Storage Endpoints

These endpoints manage the shard layout of the descriptor-based DFS (shared
filesystem) storage tier. They return HTTP `409` with
`UNAVAILABLE_IN_CURRENT_MODE` when DFS storage is not enabled on the master.

#### `GET /api/v1/dfs/shard_count`
Query the current number of DFS shards.

**Method**: `GET`
**Content-Type**: `application/json; charset=utf-8`

**Example**:
```bash
curl http://localhost:9003/api/v1/dfs/shard_count
```

**Response Format**:
```json
{
  "success": true,
  "shard_count": 16
}
```

#### `PUT /api/v1/dfs/shard_count`
Expand the DFS storage to a new shard count. Shard counts can only grow.

**Method**: `PUT`
**Content-Type**: `application/json; charset=utf-8`

**Request Body**:
```json
{
  "shard_count": 32
}
```

`shard_count` must be an integer in `[1, INT_MAX]`. Only one expansion can run
at a time; a concurrent request is rejected with HTTP `409`
(`UNAVAILABLE_IN_CURRENT_STATUS`).

**Example**:
```bash
curl -X PUT http://localhost:9003/api/v1/dfs/shard_count \
  -H "Content-Type: application/json" \
  -d '{"shard_count": 32}'
```

**Success Response** (HTTP 200): the resulting shard count in `shard_count`.

### Drain Job Endpoints

Drain jobs migrate all objects away from one or more segments so they can be
unmounted safely. Job states follow `CREATED -> PLANNING -> RUNNING ->
SUCCEEDED | FAILED | CANCELED`.

#### Snapshot recovery

With master snapshots enabled, format `1.1.0` stores DrainJob identity, request,
status, progress, retry budgets and active task associations in the required
`drain_jobs` payload (`[jobs, replication_records]` in MessagePack). Per-object
COPY/MOVE runtime is stored separately for retained draining objects, including
copies started before a drain could schedule a task. The master publishes a snapshot only after every required
payload has uploaded. Cold restart and the existing snapshot-only standby
promotion path restore snapshotted jobs and continue unfinished work.
Running tasks keep their IDs and state; completed work is accounted once.
Clients complete the normal heartbeat/remount handshake. An exact owner and
segment-registration match preserves `DRAINING` or `DRAINED` on remount.
Missing task history is treated as an unrecoverable unit, not replayed blindly.

This is snapshot-only recovery: jobs and progress after the snapshot can be
lost. It does not provide Job OpLog persistence or lossless recovery with an
OpLog suffix. Standby recovery with OpLog following still restores its existing
object view and does not recover DrainJobs. Batch-generated standby snapshots
(`enable_oplog_snapshot`) are also outside this recovery path. The configured snapshot backend
must be reachable by the replacement master; the local-file backend's existing
write/close durability is unchanged (no new power-loss/fsync guarantee).

Legacy `1.0.0` snapshots remain readable. A missing or corrupt job file in a
`1.1.0` snapshot fails that candidate. Cold restart tries older candidates;
the existing standby provider selects the latest snapshot and fails closed
instead of falling back. Invalid job records also reject promotion before
workers or serving start. Older binaries cannot read the new format.

During cold restore and full-payload `1.1.0` promotion, an orphaned `DRAINING`
segment (for example, from a legacy cold-restore snapshot) stays
non-allocatable, with existing readable replicas retained. Recovery logs the
segment and increments `master_orphaned_draining_restore_total`. No target list
is guessed and the segment is not automatically reopened. Interrupted moves
without recoverable runtime state may require operator cleanup; their allocated
target buffers are retained rather than reused while a transfer may still be
writing. After verifying that outstanding transfers have stopped and resolving
any incomplete replicas, an operator can explicitly reopen the segment, then
create a new drain job if needed:

```bash
curl -X PUT "http://localhost:9003/api/v1/segments/status?segment=segment_0" \
  -H "Content-Type: application/json" -d '{"status": "OK"}'
```

Reopening is rejected while a live drain job owns the source or an unfinished
move task still references it. Completed drain jobs remain queryable after
recovery and are not restarted. Legacy standby bootstrap retains its previous
object-view behavior; it does not provide full orphan recovery.

#### `POST /api/v1/drain_jobs`
Create a drain job for the given segments.

**Method**: `POST`
**Content-Type**: `application/json; charset=utf-8`

**Request Body**:
```json
{
  "segments": ["segment_0"],
  "target_segments": ["segment_1"],
  "max_concurrency": 4
}
```

**Fields**:
- `segments` (array of string, required): Segments to drain
- `target_segments` (array of string, optional): Preferred target segments for migrated objects
- `max_concurrency` (integer, optional): Maximum number of concurrently migrating objects. Defaults to `4`

**Example**:
```bash
curl -X POST http://localhost:9003/api/v1/drain_jobs \
  -H "Content-Type: application/json" \
  -d '{"segments": ["segment_0"], "target_segments": ["segment_1"], "max_concurrency": 4}'
```

**Success Response** (HTTP 200):
```json
{
  "success": true,
  "job_id": "00000000-0000-0000-0000-000000000003",
  "status": "CREATED"
}
```

#### `GET /api/v1/drain_jobs/query`
Query the status and progress of a drain job.

**Method**: `GET`
**Parameters**: `job_id` (query parameter) - UUID returned by the create call
**Content-Type**: `application/json; charset=utf-8`

**Example**:
```bash
curl "http://localhost:9003/api/v1/drain_jobs/query?job_id=00000000-0000-0000-0000-000000000003"
```

**Response Format**:
```json
{
  "success": true,
  "job_id": "00000000-0000-0000-0000-000000000003",
  "type": 0,
  "type_name": "DRAIN",
  "status": 2,
  "status_name": "RUNNING",
  "created_at_ms_epoch": 1767225600000,
  "last_updated_at_ms_epoch": 1767225605000,
  "segments": ["segment_0"],
  "succeeded_units": 128,
  "failed_units": 0,
  "blocked_units": 0,
  "active_units": 4,
  "migrated_bytes": 137438953472,
  "message": ""
}
```

**Fields**:
- `type` / `type_name` (integer / string): Job type; currently always `DRAIN` (0)
- `status` / `status_name` (integer / string): Job status. One of `CREATED` (0), `PLANNING` (1), `RUNNING` (2), `SUCCEEDED` (3), `FAILED` (4), `CANCELED` (5)
- `succeeded_units` / `failed_units` / `blocked_units` / `active_units` (integer): Per-object migration counters
- `migrated_bytes` (integer): Total bytes migrated so far
- `message` (string): Additional status or error message

**Error Response** (unknown job, HTTP 404):
```json
{
  "success": false,
  "error_code": -1402,
  "error_message": "JOB_NOT_FOUND"
}
```

#### `POST /api/v1/drain_jobs/cancel`
Cancel a running drain job. Objects already migrated stay migrated; objects not
yet migrated remain on the source segments.

**Method**: `POST`
**Parameters**: `job_id` (query parameter) - UUID returned by the create call
**Content-Type**: `application/json; charset=utf-8`

**Example**:
```bash
curl -X POST "http://localhost:9003/api/v1/drain_jobs/cancel?job_id=00000000-0000-0000-0000-000000000003"
```

**Success Response** (HTTP 200):
```json
{
  "success": true,
  "job_id": "00000000-0000-0000-0000-000000000003",
  "status": "CANCELED"
}
```

### Tenant Quota Endpoints

Tenant quota policies limit how much memory each tenant may cache. These
endpoints require tenant quota to be enabled on the master; otherwise they
return HTTP `409` with `UNAVAILABLE_IN_CURRENT_MODE`.

A quota snapshot has the following structure:

```json
{
  "tenant_id": "tenant-a",
  "requested_quota_bytes": 107374182400,
  "effective_quota_bytes": 85899345920,
  "charged_bytes": 21474836480,
  "admission_closed": false,
  "over_quota": false,
  "has_explicit_policy": true
}
```

**Fields**:
- `tenant_id` (string): Tenant identifier
- `requested_quota_bytes` (integer): Quota configured by the policy
- `effective_quota_bytes` (integer): Quota actually enforced, after fair-share recomputation across tenants
- `charged_bytes` (integer): Bytes currently charged to the tenant
- `admission_closed` (boolean): Whether new allocations are currently rejected for the tenant
- `over_quota` (boolean): Whether the tenant is currently over its effective quota
- `has_explicit_policy` (boolean): Whether an explicit policy exists for the tenant

#### `GET /api/v1/tenant_quotas`
List quota snapshots for all tenants, or query a single tenant.

**Method**: `GET`
**Parameters**: `tenant_id` (query parameter, optional) - When provided, return only the snapshot of this tenant
**Content-Type**: `application/json; charset=utf-8`

**Example** (list all):
```bash
curl http://localhost:9003/api/v1/tenant_quotas
```

**Response Format** (list):
```json
{
  "success": true,
  "data": [
    {
      "tenant_id": "tenant-a",
      "requested_quota_bytes": 107374182400,
      "effective_quota_bytes": 85899345920,
      "charged_bytes": 21474836480,
      "admission_closed": false,
      "over_quota": false,
      "has_explicit_policy": true
    }
  ]
}
```

**Example** (single tenant):
```bash
curl "http://localhost:9003/api/v1/tenant_quotas?tenant_id=tenant-a"
```

**Response Format** (single tenant): same JSON object, with `data` holding one
snapshot instead of an array.

#### `PUT /api/v1/tenant_quotas`
Create or update the quota policy of a tenant.

**Method**: `PUT`
**Parameters**: `tenant_id` (query parameter, required) - Tenant the policy applies to
**Content-Type**: `application/json; charset=utf-8`

**Request Body**:
```json
{
  "requested_quota_bytes": 107374182400
}
```

`requested_quota_bytes` must be in the range `[1, 2^63 - 1]`.

**Example**:
```bash
curl -X PUT "http://localhost:9003/api/v1/tenant_quotas?tenant_id=tenant-a" \
  -H "Content-Type: application/json" \
  -d '{"requested_quota_bytes": 107374182400}'
```

**Success Response** (HTTP 200): the resulting quota snapshot in `data`.

#### `DELETE /api/v1/tenant_quotas`
Delete the quota policy of a tenant.

**Method**: `DELETE`
**Parameters**: `tenant_id` (query parameter, required) - Tenant whose policy is deleted
**Content-Type**: `application/json; charset=utf-8`

**Example**:
```bash
curl -X DELETE "http://localhost:9003/api/v1/tenant_quotas?tenant_id=tenant-a"
```

**Success Response** (HTTP 200): the removed quota snapshot in `data`, or
`null` when the tenant had no explicit policy.

```json
{
  "success": true,
  "data": null
}
```

### Data Deletion Endpoints

#### `POST /api/v1/remove_all`
Remove all stored objects. This is a destructive, cluster-wide operation.

**Method**: `POST`
**Parameters**:
- `force` (query parameter, optional) - Set to `true` or `1` to force removal of objects that still have active leases
- `tenant_id` (query parameter, optional) - Restrict removal to one tenant. When omitted, objects of all tenants are removed and connected clients are notified to drop their local data

**Content-Type**: `application/json; charset=utf-8`

**Example**:
```bash
curl -X POST "http://localhost:9003/api/v1/remove_all?force=true"
```

**Success Response** (HTTP 200):
```json
{
  "success": true,
  "removed_count": 12345
}
```

**Fields**:
- `removed_count` (integer): Number of objects removed

## Store REST API Endpoints

The following endpoints are served by the Python store REST service, which wraps
`MooncakeDistributedStore` with an aiohttp service. The HTTP handlers live in
Python, while mount and unmount operations are delegated to the underlying store
binding. Start the service with:

```bash
python -m mooncake.mooncake_store_service \
  --config /path/to/mooncake_config.json \
  --port 8080
```

If the wheel console scripts are installed, the equivalent command is:

```bash
mc_store_rest_server --config /path/to/mooncake_config.json --port 8080
```

### `/api/mount_shm`
Mount a named shared memory object as one or more Mooncake store segments.
Protocols with a registration-size limit split oversized regions and return
multiple segment ids. Protocols without such a Store-level limit, such as TCP
and RDMA, use a single segment regardless of `max_mr_size`.

**Method**: `POST`
**Content-Type**: `application/json`

**Request Body**:
```json
{
  "name": "mooncake_segment",
  "size": 16777216,
  "offset": 0,
  "protocol": "tcp",
  "location": ""
}
```

**Fields**:
- `name` (string, required): Named shared memory object name. A leading `/` is
  accepted, but path separators are not.
- `size` (integer, required): Number of bytes to mount.
- `offset` (integer, optional): File offset in bytes. Defaults to `0`.
- `protocol` (string, optional): Transfer protocol. Defaults to the service
  configuration protocol.
- `location` (string, optional): Device or locality hint. Defaults to an empty
  string.

**Success Response**:
```json
{
  "status": "success",
  "segment_ids": ["00000000-0000-0000-0000-000000000001"]
}
```

**Example**:
```bash
curl -X POST http://localhost:8080/api/mount_shm \
  -H "Content-Type: application/json" \
  -d '{
        "name": "mooncake_segment",
        "size": 16777216,
        "offset": 0,
        "protocol": "tcp",
        "location": ""
      }'
```

### `/api/unmount_shm`
Unmount one or more segment ids previously returned by `/api/mount_shm`.

**Method**: `POST`
**Content-Type**: `application/json`

**Request Body**:
```json
{
  "segment_ids": ["00000000-0000-0000-0000-000000000001"],
  "grace_period_seconds": 0
}
```

`segment_ids` may also be provided as a single string for one segment.
`grace_period_seconds` is optional and defaults to `0`, which keeps the
existing immediate unmount behavior. When set to a positive value, the master
keeps the segment readable for that grace period while preventing new
allocations, then completes the unmount.

**Success Response**:
```json
{
  "status": "success"
}
```

**Example**:
```bash
curl -X POST http://localhost:8080/api/unmount_shm \
  -H "Content-Type: application/json" \
  -d '{"segment_ids": ["00000000-0000-0000-0000-000000000001"],
       "grace_period_seconds": 30}'
```

### `/api/mount`
Allocate memory inside the store process and mount it as one or more Mooncake
store segments. Protocols with a registration-size limit split oversized
requests and return multiple segment ids. Protocols without such a Store-level
limit, such as TCP and RDMA, use a single segment regardless of `max_mr_size`.
The response includes the actual allocated size after alignment.

**Method**: `POST`
**Content-Type**: `application/json`

**Request Body**:
```json
{
  "size": 16777216,
  "protocol": "tcp",
  "location": ""
}
```

**Fields**:
- `size` (integer, required): Number of bytes requested. Must be positive.
- `protocol` (string, optional): Transfer protocol. Defaults to the service
  configuration protocol.
- `location` (string, optional): Device or locality hint. Defaults to an empty
  string.

**Success Response**:
```json
{
  "status": "success",
  "segment_ids": ["00000000-0000-0000-0000-000000000002"],
  "allocated_size": 16777216
}
```

**Example**:
```bash
curl -X POST http://localhost:8080/api/mount \
  -H "Content-Type: application/json" \
  -d '{"size": 16777216, "protocol": "tcp", "location": ""}'
```

### `/api/unmount`
Unmount one or more segment ids previously returned by `/api/mount` and free
the memory allocated by the store process.

**Method**: `POST`
**Content-Type**: `application/json`

**Request Body**:
```json
{
  "segment_ids": ["00000000-0000-0000-0000-000000000002"],
  "grace_period_seconds": 0
}
```

`segment_ids` may also be provided as a single string for one segment.
`grace_period_seconds` is optional and defaults to `0`, which keeps the
existing immediate unmount-and-free behavior. When set to a positive value, the
master keeps the segment readable for that grace period while preventing new
allocations, then the store releases the local allocated memory after cleanup.

**Success Response**:
```json
{
  "status": "success"
}
```

**Example**:
```bash
curl -X POST http://localhost:8080/api/unmount \
  -H "Content-Type: application/json" \
  -d '{"segment_ids": ["00000000-0000-0000-0000-000000000002"],
       "grace_period_seconds": 30}'
```

### `/api/unmount_local_disk`
Deregister this store's SSD offload tier from the master before the process
goes away. Intended for a shutdown hook.

The master stops naming this store as the owner of the keys it offloaded, so a
reader gets a clean miss instead of a peer that is about to disappear. Without
this, a `LOCAL_DISK` segment leaves the master only when the client expires —
one `client_ttl` after the store stops pinging — and reads that pick up the
stale owner in that window block on the connect retries (see
`MC_RPC_CONNECT_TIMEOUT_MS`) before missing.

The call then holds for `grace_period_seconds` before returning. Unlike a memory
replica, which the NIC serves without help from the store process, a disk
replica is read and pushed by that process, so it has to stay alive for the
reads the master handed out before the deregistration. Offloading is stopped for
good when this is called; the store is expected to exit afterwards.

Returns success and does nothing when SSD offload is not enabled on this store.
Safe to call more than once.

**Method**: `POST`
**Content-Type**: `application/json`

**Request Body**:
```json
{
  "grace_period_seconds": 30
}
```

`grace_period_seconds` is optional and defaults to `0`, which returns as soon as
the master has dropped the segment. Must be a non-negative integer no greater
than 3600 (1 hour); a malformed body or an out-of-range value gets a `400`
without touching the store, so a mistake here (seconds where milliseconds were
meant, say) cannot block a preStop hook for hours.

**Success Response**:
```json
{
  "status": "success"
}
```

**Example** — as a Kubernetes preStop hook, with a
`terminationGracePeriodSeconds` longer than the grace period:
```bash
curl -X POST http://localhost:8080/api/unmount_local_disk \
  -H "Content-Type: application/json" \
  -d '{"grace_period_seconds": 30}'
```
