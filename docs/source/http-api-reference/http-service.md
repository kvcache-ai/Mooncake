# Mooncake Store HTTP Service

The Mooncake Store HTTP Service provides RESTful endpoints for cluster management, monitoring, and data operations. This service is embedded within the `mooncake_master` process and can be enabled alongside the primary RPC services.

## Overview

The HTTP service serves multiple purposes:
- **Metrics & Monitoring**: Prometheus-compatible metrics endpoints
- **Cluster Management**: Query and manage distributed storage segments
- **Data Inspection**: Examine stored objects and their replicas
- **Health Checks**: Service availability and status verification

## HTTP Endpoints

### Metrics Endpoints

#### `/metrics`
Prometheus-compatible metrics endpoint providing detailed system metrics in text format.

**Method**: `GET`
**Content-Type**: `text/plain; version=0.0.4`
**Response**: Comprehensive metrics including request counts, error rates, latency statistics, and resource utilization

**Example**:
```bash
curl http://localhost:8080/metrics
```

#### `/metrics/summary`
Human-readable metrics summary with key performance indicators.

**Method**: `GET`
**Content-Type**: `text/plain; version=0.0.4`
**Response**: Condensed overview of system health and performance metrics

**Example**:
```bash
curl http://localhost:8080/metrics/summary
```

### HA Diagnostics

#### `/ha/status`

`GET /ha/status` returns the local cached HA status as JSON. The endpoint is
read-only and does not probe Redis or peer nodes while serving the request.

Prefer the explicit OpLog field names:

- `oplog_applied_sequence_id`: local metadata apply watermark.
- `oplog_latest_sequence_id`: latest OpLog sequence ID from the most recent
  successful latest read. If the latest read fails, the previous value is
  retained.
- `oplog_latest_read_ok`: whether the latest read in the most recent OpLog
  progress sample succeeded.
- `oplog_trimmed_sequence_id`: OpLog trim watermark.
- `oplog_gc_cursor_sequence_id`: OpLog GC cursor watermark.
- `oplog_apply_lag`: `oplog_latest_sequence_id - oplog_applied_sequence_id`,
  clamped to 0.

Primary async writer fields use the `oplog_writer_` prefix:

- `oplog_writer_running`: whether the local primary async OpLog writer is
  currently running.
- `oplog_writer_pending_writes`: primary writer entries that are still queued,
  retrying, or persisted-but-uncommitted.
- `oplog_writer_last_committed_sequence_id`: writer's last locally confirmed
  committed sequence ID.

The older fields `applied_sequence_id`, `latest_sequence_id`,
`trimmed_sequence_id`, `gc_cursor_sequence_id`, `oplog_lag`, and
`oplog_pending_writes` remain in the response for compatibility.
`master_view_endpoint` and `master_view_updated_at_ms` are clearer aliases for
`master_view_address` and `master_view_timestamp_ms`. `master_view_age_ms` and
`master_view_expires_in_ms` are computed from the cached view timestamp and TTL.

#### `/ha/doctor`

`GET /ha/doctor` returns a local, read-only diagnosis as JSON on the same HTTP
port as `/ha/status`. It does not probe Redis or other nodes while handling the
request.

```json
{"status":"degraded","issues":[{"code":"OPLOG_WRITE_FAILED","message":"seq=123: Redis command failed"}]}
```

- `ok`: the primary is running or a standby is watching, and no active issue is
  recorded.
- `degraded`: an election/renewal, OpLog read/write/commit/apply/reconnect,
  snapshot recovery, registry heartbeat, master view mismatch, or fatal HA
  failure is recorded, or pending OpLogs
  exceed the configured latest stall timeout, or a watching standby stops applying
  known pending OpLogs beyond the catch-up stall timeout.
- `unknown`: HA is disabled, uninitialized, or in a transitional state without a
  recorded failure. This endpoint is not a readiness or cluster health check.

An empty issue list is `[]`. Each issue contains only a stable `code` and a
`message`; successful operations clear the corresponding issue, repeated issues
replace their previous message instead of accumulating, and a new HA round clears
active issues from the preceding round, while retaining the election instability
window described below. OpLog holes, sequence lag, catch-up, optional
snapshot fallback, and historical `last_error` alone are not failures. The
doctor only reports failures observed by the live process and does not retain
history across restarts. It returns HTTP 200 even when `status` is `degraded`
or `unknown`.

`ELECTION_UNSTABLE` reports at least 3 observed leader term changes in the last
300 seconds and makes the result `degraded`. The first valid leader establishes a
baseline; duplicate or older epochs are ignored, while a newer epoch counts once
even when the same node is re-elected. The issue clears when fewer than 3 changes
remain in the sliding window. Disabling HA, changing clusters, or resetting the
process clears history.

`MASTER_VIEW_MISMATCH` means the local process is reporting
`role=primary,state=RUNNING`, but its cached master view does not identify this
same process as the leader. The issue is reported when the cached view is
missing, when `master_view_address` differs from `local_endpoint`, when both
instance IDs are present and differ, or when both epochs are present and differ.
Empty instance IDs are not treated as a mismatch by themselves, so older paths
and non-Redis election backends do not degrade solely because they do not publish
an instance ID.

`OPLOG_LATEST_STALLED` means the Redis writer has pending work and its locally
confirmed committed latest has not advanced for
`oplog_latest_stall_timeout_sec` (default 10 seconds; 0 disables this check).
The timer starts when idle work is admitted, restarts only on confirmed latest
progress while work remains, and stops when work drains, is dropped, or the
writer stops. New entries and retries do not restart it. Idle time,
dropped-only tails, and rewrites at or below the committed latest are excluded.
This diagnoses stalled commits, not slow but continuing progress. The message is
concise, for example `latest=100, pending=3, stalled_ms=12000`.

`OPLOG_CATCHUP_STALLED` means a standby in `WATCHING` has known backlog
(`latest > applied`) without applied progress for
`oplog_catchup_stall_timeout_sec` (default 10 seconds; 0 disables this check).
The timer starts when a valid progress sample first shows backlog, restarts on
applied progress, and clears on catch-up, leaving `WATCHING`, disabling HA, or
starting a new HA round. Latest growth does not restart it; a failed latest read
retains the last successful sample. The message is concise, for example
`applied=100, latest=200, stalled_ms=12000`.

`OPLOG_READ_FAILED` persists across reader reconnects until an actual poll
succeeds (including an empty poll), or the standby stops or is promoted.

### Data Management Endpoints

#### `/query_key`
Retrieve replica information for a specific key, including memory locations and transport endpoints.

**Method**: `GET`
**Parameters**: `key` (query parameter) - The object key to query
**Content-Type**: `text/plain; version=0.0.4`
**Response**: JSON-formatted replica descriptors for memory replicas

**Example**:
```bash
curl "http://localhost:8080/query_key?key=my_object"
```

**Response Format**:
```json
{
  "transport_endpoint_": "hostname:port",
  "buffer_descriptors": [...]
}
```

#### `/batch_query_keys`
Retrieve replica information for multiple keys in a single request, including memory locations and transport endpoints for each key.

**Method**: `GET`
**Parameters**: `keys` (query parameter) - Comma-separated list of object keys to query (format: key1,key2,key3)
**Content-Type**: `application/json; charset=utf-8`
**Response**: JSON-formatted mapping of keys to their respective replica descriptors

**Example**:
```bash
curl "http://localhost:8080/batch_query_keys?keys=key1,key2,key3"
```

**Response Format**:
```json
{
  "success": true,
  "data": {
    "key1": {
      "ok": true,
      "values": [
        {
          "transport_endpoint_": "hostname:port",
          "buffer_descriptor": {...}
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

#### `/get_all_keys`
List all keys currently stored in the distributed system.

**Method**: `GET`
**Content-Type**: `text/plain; version=0.0.4`
**Response**: Newline-separated list of all stored keys

**Example**:
```bash
curl http://localhost:8080/get_all_keys
```

### Segment Management Endpoints

#### `/get_all_segments`
List all mounted segments in the cluster.

**Method**: `GET`
**Content-Type**: `text/plain; version=0.0.4`
**Response**: Newline-separated list of segment names

**Example**:
```bash
curl http://localhost:8080/get_all_segments
```

#### `/query_segment`
Query detailed information about a specific segment, including used and available capacity.

**Method**: `GET`
**Parameters**: `segment` (query parameter) - Segment name to query
**Content-Type**: `text/plain; version=0.0.4`
**Response**: Multi-line text with segment details

**Example**:
```bash
curl "http://localhost:8080/query_segment?segment=segment_name"
```

**Response Format**:
```
segment_name
Used(bytes): 1073741824
Capacity(bytes): 4294967296
```

### Health Check Endpoints

#### `/health`
Basic health check endpoint for service availability verification.

**Method**: `GET`
**Content-Type**: `text/plain; version=0.0.4`
**Response**: `OK` when service is healthy
**Status Codes**: 
- `200 OK`: Service is healthy
- Other: Service may be experiencing issues

**Example**:
```bash
curl http://localhost:8080/health
```

