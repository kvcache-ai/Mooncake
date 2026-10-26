# TENT Metrics System

TENT provides a built-in metrics system based on yalantinglibs, compatible with Prometheus for monitoring data transfer performance and system health.

## Overview

The metrics system supports three metric types:

- **Counter**: Monotonically increasing values (e.g., total bytes transferred, total requests)
- **Gauge**: Current state that can increase or decrease (e.g., in-flight attempts, HTTP server state)
- **Histogram**: Distribution of values with configurable buckets (e.g., latency)

All metrics are thread-safe and designed for high-performance data paths.

## Performance Optimization

The metrics system provides two levels of control for performance optimization:

### Compile-time Disable (Zero Overhead)

By default, metrics are **disabled** at compile time for maximum performance. To enable metrics, build with:

```bash
cmake -DTENT_METRICS_ENABLED=ON ..
```

When disabled at compile time (`TENT_METRICS_ENABLED=OFF`, the default), all metrics macros expand to `((void)0)` and the `recordTaskCompletionMetrics` body is `#if`-gated out, resulting in **zero runtime overhead** on the transfer hot path.

Code that needs to report this build capability without relying on an HTTP
endpoint can query `TentMetrics::isCompiledIn()` in both enabled and disabled
builds.

### Runtime Disable (Minimal Overhead)

When metrics are enabled at compile time, you can still disable them at runtime:

```cpp
// Disable metrics collection at runtime
TentMetrics::setEnabled(false);

// Re-enable metrics collection
TentMetrics::setEnabled(true);

// Check current state
bool enabled = TentMetrics::isEnabled();
```

When disabled at runtime, record functions return immediately after a single atomic load (~1ns overhead).

## Configuration

### Configuration Sources (Priority Order)

1. **Config File** (highest priority)
2. **Environment Variables** (medium priority)
3. **Default Values** (lowest priority)

### Config File Format

TENT metrics configuration is integrated into the main `transfer-engine.json` configuration file:

```json
{
  "local_segment_name": "",
  "metadata_type": "p2p",
  "metadata_servers": "127.0.0.1:2379",
  "log_level": "warning",
  "observability": {
    "slow_request_threshold_us": 0,
    "stuck_request_threshold_us": 0,
    "tracing_enabled": false,
    "trace_sample_rate_ppm": 0,
    "trace_slow_threshold_us": 0
  },
  "metrics": {
    "enabled": true,
    "http_port": 9100,
    "http_host": "0.0.0.0",
    "http_server_threads": 2,
    "report_interval_seconds": 30
  },
  "transports": {
    // ... transport configuration
  }
}
```

**Note**:
- `report_interval_seconds`: Set to 0 to disable periodic logging
- Histogram buckets are fixed at compile time (see `kLatencyBuckets` / `kSizeBuckets` in `tent_metrics.h`) for reproducible observability across deployments.
- Retained trace spans are structured `INFO` logs. Set `log_level` to `info`
  when `observability/tracing_enabled` is true.
- `trace_sample_rate_ppm` is deterministic head sampling in parts per million
  and is capped at `1000000`. Errors, timeouts, cancellations, and requests
  slower than `trace_slow_threshold_us` are retained independently of head
  sampling. A zero slow threshold disables slow-trace retention.
- `stuck_request_threshold_us` emits one correlated `tent.request.stuck`
  warning and increments one counter when a still-pending logical request first
  crosses the threshold. It does not cancel the request. Zero disables the
  event; choose a value above the deployment's expected maximum transfer time.

### Environment Variables

```bash
# Basic settings
TENT_METRICS_ENABLED=true
TENT_METRICS_HTTP_PORT=9100
TENT_METRICS_HTTP_HOST=0.0.0.0
TENT_METRICS_HTTP_SERVER_THREADS=2
TENT_METRICS_REPORT_INTERVAL=30  # Set to 0 to disable periodic logging
```

## Quick Start

### Build with Metrics Enabled

```bash
# Enable metrics at compile time (disabled by default)
cmake -DTENT_METRICS_ENABLED=ON ..
make
```

### Basic Usage

```cpp
#include "tent/metrics/tent_metrics.h"
#include "tent/metrics/config_loader.h"

// Load configuration from transfer-engine.json
auto config = MetricsConfigLoader::loadWithDefaults();

// Initialize TENT metrics system
auto& tent_metrics = TentMetrics::instance();
tent_metrics.initialize(config);

// HTTP server starts automatically
```

### Recording Transfer Metrics

```cpp
// Using convenience macros (recommended)
TENT_RECORD_READ_COMPLETED(RDMA, 1024*1024, 0.025);   // 1MB read in 25ms
TENT_RECORD_WRITE_COMPLETED(RDMA, 512*1024, 0.015);    // 512KB write in 15ms
TENT_RECORD_READ_FAILED(TCP);                           // read failed (no bytes recorded)
TENT_RECORD_WRITE_FAILED(TCP);                          // write failed (no bytes recorded)
TENT_RECORD_TRANSPORT_FAILOVER(RDMA, TCP);              // cross-transport failover event

// Direct API usage
auto& tent_metrics = TentMetrics::instance();
tent_metrics.recordReadCompleted(RDMA, 1024*1024, 0.025);
tent_metrics.recordWriteCompleted(RDMA, 512*1024, 0.015);
tent_metrics.recordReadFailed(TCP);
tent_metrics.recordWriteFailed(TCP);
tent_metrics.recordTransportFailover(RDMA, TCP);

// Deadline feasibility (RFC #2519, observability only):
tent_metrics.recordDeadlineMLU(RDMA, 0.8);        // MLU < 1 met the deadline
tent_metrics.recordDeadlineInfeasible(TCP);        // deadline was in the past at submit

// Causal-chain per-stage latency breakdown (microseconds):
tent_metrics.recordStageLatency(TentMetrics::Stage::QueueWait, RDMA, 12.0);
tent_metrics.recordStageLatency(TentMetrics::Stage::Dispatch, RDMA, 45.0);
tent_metrics.recordStageLatency(TentMetrics::Stage::Transport, RDMA, 130.0);
```

### RAII Latency Measurement

```cpp
// Automatic latency measurement using RAII
{
    TENT_SCOPED_READ_LATENCY(RDMA, 1024 * 1024); // e.g. 1MB
    // ... perform read operation ...
}  // latency automatically recorded when scope exits

{
    TENT_SCOPED_WRITE_LATENCY(RDMA, 512 * 1024); // e.g. 512KB
    // ... perform write operation ...
}
```

## HTTP Server Endpoints

The HTTP server provides multiple endpoints:

- **`/metrics`**: Prometheus format
- **`/metrics/openmetrics`**: OpenMetrics 1.0 format. The latest retained trace
  for each transport/operation attempt series may be attached to the `+Inf`
  latency bucket as an exemplar. Use this endpoint when the collector supports
  exemplars; `/metrics` remains Prometheus 0.0.4 compatible.
- **`/metrics/summary`**: Human-readable summary
- **`/metrics/json`**: JSON format
- **`/health`**: Metrics HTTP process liveness (`200 OK` while the endpoint is serving). It does not report TransferEngine readiness or transport health.
- **`/live`**: Versioned JSON process liveness. This remains `200` while the
  metrics HTTP event loop can serve requests, even when the engine is not
  ready.
- **`/ready`**: Versioned JSON engine readiness. It returns `200` only when a
  bounded readiness provider is registered and all mandatory checks pass;
  otherwise it returns `503` with stable reason identifiers. The provider is
  O(1): it reads atomically maintained aggregate state and never scans live
  batches or tasks.
- **`/diagnostics`**: A versioned, redacted full engine snapshot, always
  returned with `200` so it can be collected during an incident. Unlike
  `/ready`, this endpoint may scan live engine state and is not a probe path.

The three serialization endpoints return HTTP `500` when their response cannot
be serialized. The failed attempt is counted in
`tent_metrics_serialization_failures_total` and becomes visible on the next
successful Prometheus scrape. `/health` remains a liveness-only endpoint.

`/health` is kept for compatibility. New probes should use `/live` for process
liveness and `/ready` for traffic readiness.

The readiness reasons are a closed set:

- `engine_diagnostics_unavailable`
- `engine_unavailable`
- `control_plane_unavailable`
- `no_transport_available`
- `progress_worker_unavailable`
- `progress_worker_stalled`

The progress-worker stall check requires both old in-flight work and a stale
worker progress timestamp. A legitimately long transfer therefore does not
make the engine unready while the worker continues to make progress.

The diagnostics schema is currently version `1`. It contains build identity,
loaded transport types, aggregate batch/queue/window counts, oldest in-flight
age, and progress-worker state. It deliberately omits peer and request
identifiers, segment names, network addresses, memory addresses, keys,
credentials, and payload data. Transport names are bounded enum values rather
than runtime-provided strings.

### Correlated logs and retained traces

Terminal failures, configured slow requests, transport-attempt failures,
failover, and selection failures are emitted as stable `key=value` events.
They carry process-local monotonic `request_id`, `batch_id`, `task_id`, and
`attempt_id` fields plus a 128-bit `trace_id`. They never include payloads,
memory addresses, target offsets, credentials, or peer-provided strings.

When tracing is enabled, TENT emits completed logical-request and physical-
attempt spans as `event=tent.trace.span`. Each record contains `trace_id`,
`span_id`, `parent_span_id`, `span_name`, `status`, `duration_us`, and one of
the bounded retention reasons `head_sampled`, `slow`, `error`, `timeout`, or
`linked_child`. Failed and slow attempts are retained even when the head
sample does not select the request, and retaining a child also retains its
logical parent. This first trace slice is process-local and does not change
the TENT wire protocol; cross-process context propagation can be added later
through a compatible carrier.

The same retained trace ID is published as an OpenMetrics exemplar on
`tent_transport_attempt_latency_us`. Exemplars are deliberately absent from
the legacy `/metrics` response, so existing Prometheus 0.0.4 scrapers keep the
same wire contract.

### Example Responses

**Prometheus Format (`/metrics`)**:
```
# HELP tent_read_bytes_total Total bytes read via TENT
# TYPE tent_read_bytes_total counter
tent_read_bytes_total{transport="rdma"} 1048576
tent_read_bytes_total{transport="tcp"} 524288

# HELP tent_write_bytes_total Total bytes written via TENT
# TYPE tent_write_bytes_total counter
tent_write_bytes_total{transport="rdma"} 524288

# HELP tent_read_requests_total Total read requests via TENT
# TYPE tent_read_requests_total counter
tent_read_requests_total{transport="rdma"} 100
tent_read_requests_total{transport="tcp"} 50

# HELP tent_read_failures_total Total read failures via TENT
# TYPE tent_read_failures_total counter
tent_read_failures_total{transport="tcp"} 2

# HELP tent_transport_failover_total Total cross-transport failover events
# TYPE tent_transport_failover_total counter
tent_transport_failover_total{from="rdma",to="tcp"} 1

# HELP tent_read_latency_us Read latency distribution in microseconds
# TYPE tent_read_latency_us histogram
tent_read_latency_us_bucket{transport="rdma",le="100"} 10
tent_read_latency_us_bucket{transport="rdma",le="500"} 50
...
```

**JSON Format (`/metrics/json`)**:
```json
{
  "tent_read_bytes_total": 1048576,
  "tent_write_bytes_total": 524288,
  "tent_read_requests_total": 100,
  "tent_write_requests_total": 50,
  "tent_read_failures_total": 2,
  "tent_write_failures_total": 1,
  "tent_metrics_runtime_enabled": 1,
  "tent_metrics_http_server_up": 1,
  "tent_metrics_http_start_failures_total": 0,
  "tent_metrics_build_info": {
    "version": "0.3.7",
    "commit": "f751cff"
  },
  "tent_metrics_serialization_attempts_total": {
    "prometheus": 12,
    "json": 1
  },
  "tent_metrics_serialization_failures_total": {
    "prometheus": 0,
    "json": 0
  },
  "tent_metrics_last_success_timestamp_seconds": {
    "prometheus": 1793016000,
    "json": 1793016001
  }
}
```

**Summary Format (`/metrics/summary`)**:
```
Metrics HTTP: up (start failures: 0) | Read: 1.00 MB (100 reqs, 2 fails) | Write: 512.00 KB (50 reqs, 1 fails)
```

**Readiness (`/ready`)**:

```json
{
  "schema_version": 1,
  "ready": true,
  "provider_available": true,
  "readiness_reasons": [],
  "engine": {
    "available": true,
    "control_plane_available": true,
    "loaded_transport_count": 2,
    "loaded_transports": ["rdma", "tcp"]
  },
  "progress_worker": {
    "enabled": true,
    "running": true,
    "last_progress_age_seconds": 0,
    "stall_threshold_seconds": 5
  },
  "runtime_queue": {
    "enabled": false,
    "outstanding_owners": 0,
    "outstanding_bytes": 0,
    "dispatch_inflight_owners": 0,
    "dispatch_inflight_bytes": 0
  },
  "work": {
    "alive_batches": 0,
    "oldest_inflight_age_us": 0
  }
}
```

## Available Metrics

| Metric Name | Type | Labels | Description |
|-------------|------|--------|-------------|
| `tent_read_bytes_total` | Counter | `transport` | Total bytes read via TENT (success only; failures record no bytes) |
| `tent_write_bytes_total` | Counter | `transport` | Total bytes written via TENT (success only) |
| `tent_read_requests_total` | Counter | `transport` | Terminal read requests via TENT (success + failed + timeout + canceled) |
| `tent_write_requests_total` | Counter | `transport` | Terminal write requests via TENT (success + failed + timeout + canceled) |
| `tent_read_failures_total` | Counter | `transport` | Total read failures via TENT |
| `tent_write_failures_total` | Counter | `transport` | Total write failures via TENT |
| `tent_transport_failover_total` | Counter | `from`, `to` | Total cross-transport failover events |
| `tent_transport_attempts_total` | Counter | `transport`, `operation` | Physical transport attempts submitted for execution |
| `tent_transport_attempt_failures_total` | Counter | `transport`, `operation` | Physical transport attempts that terminated unsuccessfully |
| `tent_logical_requests_started_total` | Counter | `operation` | Accepted non-derived logical requests |
| `tent_logical_request_outcomes_total` | Counter | `transport`, `operation`, `outcome` | Terminal logical outcomes (`success`, `failed`, `timeout`, or `canceled`) |
| `tent_transport_attempt_outcomes_total` | Counter | `transport`, `operation`, `outcome` | Terminal physical-attempt outcomes |
| `tent_task_failures_total` | Counter | `transport`, `reason` | Task failures grouped by terminal transport and failure reason |
| `tent_request_stuck_events_total` | Counter | `phase`, `transport`, `operation` | Logical requests that crossed the configured stuck threshold once while still pending |
| `tent_terminal_failures_total` | Counter | `transport`, `phase`, `reason` | Terminal logical task failures grouped by the bounded public failure taxonomy |
| `tent_memory_operations_total` | Counter | `transport`, `operation`, `outcome` | Physical backend memory register/unregister calls |
| `tent_memory_operation_bytes_total` | Counter | `transport`, `operation`, `outcome` | Bytes covered by physical backend memory register/unregister calls |
| `tent_deadline_infeasible_total` | Counter | `transport` | Transfers whose deadline was already in the past at submit time |
| `tent_quarantined_batches_total` | Counter | - | Batches abandoned after repeated reclaim failures and retained until engine teardown |
| `tent_staging_cleanup_timeouts_total` | Counter | - | Staging shutdown drains that reached their configured deadline |
| `tent_rpc_attempts_total` | Counter | `method`, `outcome` | Physical TENT client RPC attempts, including attempts that fail to connect |
| `tent_rpc_retries_total` | Counter | `method` | Retry attempts actually issued by an idempotent RPC call |
| `tent_rdma_slice_events_total` | Counter | `operation`, `event` | RDMA slice lifecycle events from initial worker enqueue through post, completion, timeout, or pre-post failure |
| `tent_rdma_retries_total` | Counter | `operation`, `cause` | RDMA slice retries actually re-enqueued after endpoint, post, or completion failure |
| `tent_rdma_retry_exhausted_total` | Counter | `operation`, `cause` | RDMA slices terminated after reaching the configured retry limit |
| `tent_rdma_wc_errors_total` | Counter | `operation`, `category` | Non-success RDMA work completions split into flush, hardware retry/RNR exhaustion, local protection, remote access/operation, protocol, and other categories |
| `tent_rdma_cq_poll_errors_total` | Counter | - | Completion queue polls that returned an error |
| `tent_rdma_endpoint_events_total` | Counter | `event` | RDMA endpoint construction, ready, retirement and destruction transitions |
| `tent_rdma_qp_transition_failures_total` | Counter | `qp_type`, `transition` | Failed data or notification QP state transitions |
| `tent_rdma_notification_events_total` | Counter | `event` | Notification receive posting/delivery, send posting/completion and channel failures |
| `tent_stream_events_total` | Counter | `transport`, `operation`, `event` | TCP and HP-TCP queue, physical attempt, connection, retry and terminal events |
| `tent_protocol_errors_total` | Counter | `transport`, `kind` | Malformed or inconsistent HP-TCP frames, including header fields, request IDs, and committed lengths |
| `tent_tcp_retransmissions_total` | Counter | `transport`, `role` | Delta of Linux `TCP_INFO.tcpi_total_retrans` sampled from HP-TCP sockets |
| `tent_shm_mapping_events_total` | Counter | `event` | SHM address relocation and backing-object mapping outcomes |
| `tent_shm_copy_events_total` | Counter | `operation`, `outcome` | Synchronous physical SHM copy attempts |
| `tent_segment_events_total` | Counter | `event` | Segment snapshot, registry, remote-cache and generation lifecycle events |
| `tent_transport_selection_events_total` | Counter | `event`, `transport` | Transport candidate, hint, fallback and exhaustion decisions |
| `tent_admission_events_total` | Counter | `owner_kind`, `event` | Runtime admission, rejection, dispatch, deadline-drop and cancellation decisions |
| `tent_inflight_attempts` | Gauge | `transport` | Physical transport attempts submitted but not yet finished |
| `tent_logical_requests_inflight` | Gauge | `operation` | Accepted logical requests without a terminal outcome |
| `tent_registered_buffer_bytes` | Gauge | `transport` | Currently registered local buffer bytes |
| `tent_worker_last_progress_timestamp_seconds` | Gauge | `worker` | Unix timestamp of the last observed engine-progress, staging, or RDMA-worker progress |
| `tent_staging_inflight_tasks` | Gauge | `operation` | Parent staging tasks currently executing in staging workers |
| `tent_staging_inflight_bytes` | Gauge | `operation` | Parent request bytes currently executing in staging workers |
| `tent_staging_cleanup_pending` | Gauge | `kind` | Deferred cleanup groups, child batches, or remote operations still awaiting safe release |
| `tent_rdma_worker_inflight_slices` | Gauge | `operation` | RDMA slices owned by worker queues or completion processing |
| `tent_rdma_posted_inflight_slices` | Gauge | `operation` | RDMA slices reserved for posting or posted and not yet resolved |
| `tent_admission_queue_owners` | Gauge | `owner_kind` | Owners currently accounted by runtime admission control |
| `tent_admission_queue_bytes` | Gauge | `owner_kind` | Bytes currently accounted by runtime admission control |
| `tent_engine_ready` | Gauge | - | `1` when the bounded readiness snapshot has no blocker, otherwise `0` |
| `tent_engine_readiness_reason` | Gauge | `reason` | One-hot bounded readiness blockers; every known reason is always emitted |
| `tent_stream_queue_depth` | Gauge | `transport` | TCP tasks or HP-TCP operations waiting to start |
| `tent_stream_busy_workers` | Gauge | `transport` | TCP workers or HP-TCP lanes currently executing an operation |
| `tent_stream_admitted_tasks` | Gauge | `transport` | HP-TCP tasks currently reserved by admission control |
| `tent_stream_admitted_bytes` | Gauge | `transport` | HP-TCP bytes currently reserved by admission control |
| `tent_stream_connections` | Gauge | `transport`, `role` | Open stream connections split by client and server role |
| `tent_read_latency_us` | Histogram | `transport` | Read latency distribution in microseconds |
| `tent_write_latency_us` | Histogram | `transport` | Write latency distribution in microseconds |
| `tent_read_size_bytes` | Histogram | `transport` | Read request size distribution in bytes |
| `tent_write_size_bytes` | Histogram | `transport` | Write request size distribution in bytes |
| `tent_deadline_mlu_permille` | Histogram | `transport` | Deadline feasibility ratio (MLU x 1000); 1000 = MLU 1.0 (the met/missed boundary) |
| `tent_stage_queue_wait_us` | Histogram | `transport` | Causal chain: queue wait latency in microseconds |
| `tent_stage_dispatch_us` | Histogram | `transport` | Causal chain: dispatch latency in microseconds |
| `tent_stage_transport_us` | Histogram | `transport` | Causal chain: transport execution latency in microseconds |
| `tent_transport_attempt_latency_us` | Histogram | `transport`, `operation` | Observed latency of each physical transport attempt |
| `tent_logical_request_outcome_latency_us` | Histogram | `transport`, `operation`, `outcome` | End-to-end logical request latency, including unsuccessful outcomes |
| `tent_transport_attempt_outcome_latency_us` | Histogram | `transport`, `operation`, `outcome` | Physical attempt latency split by terminal outcome |
| `tent_rpc_attempt_latency_us` | Histogram | `method`, `outcome` | End-to-end latency of each physical client RPC attempt, including connection setup when needed |
| `tent_rdma_completion_latency_us` | Histogram | `operation`, `outcome` | Post-to-CQ latency of physical RDMA slice completions |
| `tent_stream_attempt_latency_us` | Histogram | `transport`, `operation`, `outcome` | End-to-end latency of physical TCP or HP-TCP network attempts |
| `tent_stream_queue_wait_us` | Histogram | `transport`, `operation` | TCP logical-task and HP-TCP physical-slice queue wait before an attempt starts |
| `tent_tcp_rtt_us` | Histogram | `transport`, `role` | Linux TCP_INFO smoothed RTT samples from HP-TCP sockets |
| `tent_tcp_congestion_window_segments` | Histogram | `transport`, `role` | Linux TCP_INFO send congestion-window samples from HP-TCP sockets |
| `tent_shm_copy_latency_us` | Histogram | `operation`, `outcome` | Synchronous physical SHM copy latency |
| `tent_admission_wait_us` | Histogram | `owner_kind` | Runtime admission wait before dispatch |

### Metrics pipeline self-observability

These families describe whether the metrics pipeline itself is usable. They
are emitted with zero values before any transfer traffic is recorded and stay
visible when runtime transfer recording is disabled.

| Metric Name | Type | Labels | Description |
|-------------|------|--------|-------------|
| `tent_metrics_build_info` | Gauge | `version`, `commit` | Build identity; the series value is always `1`. Its presence means metrics were compiled into the serving process. |
| `tent_metrics_runtime_enabled` | Gauge | - | `1` when transfer metric recording is enabled at runtime, otherwise `0` |
| `tent_metrics_http_server_up` | Gauge | - | `1` when the metrics HTTP server successfully bound its configured port |
| `tent_metrics_http_start_failures_total` | Counter | - | Failed metrics HTTP server start attempts since process start |
| `tent_metrics_serialization_attempts_total` | Counter | `format` | Metrics serialization attempts, including the current request |
| `tent_metrics_serialization_failures_total` | Counter | `format` | Metrics serialization attempts that raised an exception |
| `tent_metrics_last_success_timestamp_seconds` | Gauge | `format` | Unix timestamp of the most recent successful serialization; `0` means that format has never succeeded |

**Notes**:
- `*_requests_total` counts terminal logical/merged-transfer outcomes. Its `transport` label is the **final transport**: a request recovered by RDMA→TCP failover is counted once under `tcp`.
- State gauges ignore the runtime sampling switch. Every queue, worker,
  admission, and connection increment is paired with a decrement on success,
  cancellation, timeout, dispatch failure, and shutdown paths.
- A stuck event is a threshold crossing, not a terminal outcome. It is emitted
  at most once per logical request and carries the same `trace_id`,
  `request_id`, `batch_id`, `task_id`, and `attempt_id` contract as failure and
  slow-request logs. The bounded `phase` label distinguishes admission queue
  wait, dispatch, and an active transport attempt; use the correlated log for
  the exact request identity.
- TENT software retries, kernel TCP retransmissions, and RDMA hardware retry
  exhaustion are different signals. `tent_rdma_retries_total` counts software
  re-enqueue, `tent_tcp_retransmissions_total` comes from TCP_INFO, and
  `tent_rdma_wc_errors_total{category=~"retry_exceeded|rnr_retry_exceeded"}`
  reports only the terminal RDMA WC status after hardware retries are
  exhausted. Per-packet RDMA retry counts require NIC/vendor telemetry.
- TCP_INFO is sampled every 256 completed HP-TCP operations and once before a
  connection closes. The classic RPC-backed TCP transport does not expose its
  socket handle here; pair TENT metrics with host TCP retransmission telemetry
  for that transport.
- `tent_logical_requests_started_total - sum(tent_logical_request_outcomes_total)` is the process-local conservation balance. Compare it with `tent_logical_requests_inflight`; a persistent mismatch means a terminal edge was lost or telemetry was disabled during only one side of the interval.
- `tent_transport_attempts_total` and `tent_transport_attempt_outcomes_total` provide the equivalent physical-attempt conservation check. A recovered RDMA→TCP request contributes one failed RDMA attempt and one successful TCP attempt.
- `tent_transport_attempt_failures_total` is a compatibility aggregate of the `failed`, `timeout`, and `canceled` attempt outcomes.
- Attempt latency currently ends when polling or the progress worker observes the terminal status, so it may include completion-observation delay.
- `*_failures_total` does not record bytes; failed transfers transfer no bytes.
- `tent_deadline_infeasible_total` is a dedicated counter (not a histogram sentinel) so infeasible-at-submit cases are distinguishable from genuine high-MLU samples.
- `tent_metrics_last_success_timestamp_seconds{format="..."} == 0` means that format has never completed a successful serialization. Prometheus freshness alerts must select `format="prometheus"` and guard on a value greater than zero; a Prometheus-only deployment is not expected to serialize JSON.
- yalantinglibs omits zero-valued transfer counters/histograms from the Prometheus output, so a transfer metric only appears once it has been observed at least once. The metrics-pipeline self-observability families above are emitted unconditionally.

### Labels

Transfer and attempt metrics carry a `transport` label (the
`tent_transport_failover_total` counter uses `from` and `to` instead) so they
can be sliced by transport without grepping logs. Attempt metrics also carry
an `operation` label. Outcome metrics add a four-valued `outcome` label. Task failures add a closed-set `reason` label and metrics
serialization adds a two-valued `format` label. Terminal failures use the
bounded `phase` and `reason` taxonomy, while memory lifecycle metrics use an
`outcome` label. Label values come from bounded sets; no request IDs,
addresses, segment names, or other unbounded values are accepted.

| Label | Values | Description |
|-------|--------|-------------|
| `transport` | `unspec`, `rdma`, `mnnvl`, `shm`, `nvlink`, `gds`, `io_uring`, `tcp`, `ascend`, `sunrise_link`, `tpu`, `ub`, `mpcomm`, `hp_tcp`, `xpu`, `hylink`, `fabric` | The transport that handled the transfer |
| `operation` | `read`, `write`; `register`, `unregister` | Attempt or memory lifecycle operation, depending on the metric family |
| `phase` | `metadata`, `admission`, `selection`, `connect`, `register`, `submit`, `queue`, `post`, `poll`, `completion`, `retry`, `failover`, `notify`, `cleanup`, `shutdown` | Stable phase where a terminal failure originated |
| `reason` | `submit`, `poll`, `timeout`, `canceled` for `tent_task_failures_total`; `timeout`, `unavailable`, `queue_full`, `resource_exhausted`, `protocol`, `stale_generation`, `wc_error`, `cq_error`, `mr_error`, `peer_closed`, `canceled`, `internal` for `tent_terminal_failures_total` | Bounded failure classification |
| `outcome` | `success`, `failed`, `timeout`, `canceled` for logical/transport outcomes; `success`, `failure` for memory lifecycle metrics; `success`, `connection_error`, `rpc_error` for RPC metrics; `success`, `wc_error`, `flush`, `late` for RDMA completions; `success`, `connection_error`, `io_error`, `timeout`, `peer_closed`, `canceled`, `remote_error` for stream attempts | Bounded operation outcome |
| `worker` | `engine_progress`, `staging`, `rdma` | Worker subsystem reporting forward progress |
| `kind` | `group`, `batch`, `remote_operation` | Deferred staging cleanup resource kind |
| `owner_kind` | `user`, `staging`, `all` | Runtime admission owner class; `all` is the aggregate capacity domain |
| `method` | `get_segment_desc`, `bootstrap_rdma`, `bootstrap_ub`, `send_data`, `recv_data`, `notify`, `probe`, `delegate`, `pin`, `unpin`, `subscribe_segment_update`, `notify_segment_updated`, `unknown` | TENT RPC method; `unknown` safely bounds extension methods |
| `event` | RDMA slice: `enqueued`, `post_attempt`, `path_error`, `post_error`, `completion_success`, `completion_error`, `completion_flush`, `late_completion`, `software_timeout`; endpoint: `constructed`, `construct_failure`, `ready_active`, `ready_passive`, `retiring`, `destroyed`, `destroy_error`; notification: `recv_posted`, `recv_post_error`, `recv_delivered`, `recv_invalid`, `recv_dropped`, `send_posted`, `send_completed`, `send_unavailable`, `send_encode_error`, `send_post_error`, `completion_error`, `cq_poll_error`, `unknown_qp`, `channel_disabled`; stream: `queued`, `attempt_started`, `attempt_success`, `connect_started`, `connect_success`, `connection_error`, `io_error`, `timeout`, `peer_closed`, `canceled`, `remote_error`, `retry_issued`, `retry_exhausted`; selection and admission use the closed decision names documented by their counters | Family-specific bounded lifecycle event or decision |
| `cause` | `endpoint`, `post`, `completion` | RDMA retry or retry-exhaustion source |
| `qp_type` | `data`, `notification` | RDMA queue-pair role; QP numbers are never labels |
| `transition` | `create`, `reset`, `init`, `rtr`, `rts`, `error`, `destroy` | Queue-pair transition that failed |
| `format` | `prometheus`, `json` | Serialization output format |
| `from` | (same set) | Transport that failed before failover |
| `to` | (same set) | Transport that the failover switched to |
| `version` | Build-time constant | TENT build version |
| `commit` | Build-time constant | Git commit used for the build |

Transport label values come from the shared `transportTypeName()` mapping.
`unspec` covers transfers that failed before a transport was selected.

**Cardinality**: the `transport` label has 17 values; the failover
`from`/`to` pair has at most 17x17 = 289 combinations (in practice only a
few pairs ever occur), each attempt metric has at most 17x2 = 34
transport/operation combinations, task failures have at most 17x4 = 68,
terminal failures have at most 17x15x12 = 3060, each memory lifecycle family
has at most 17x2x2 = 68, worker progress has three series, each staging in-flight
family has two operation series, staging cleanup has three kind series, and
each serialization family has exactly two series. Dynamic counters allocate
only label combinations that actually occur. Build identity labels are
constant for the life of the process; a served `tent_metrics_build_info` series
itself confirms that metrics were compiled in. Total series across all metrics
is bounded by these closed sets.

Each RPC family is bounded to 13 methods. Attempts and attempt latency have at
most 13x3 = 39 method/outcome series; retries have at most 13. These are
client-side physical-attempt metrics: an idempotent logical call retried once
records two attempts and one retry. A peer response that carries an
application-level error is still a successful RPC transport attempt; the
caller-facing failure is classified by the owning subsystem metric or log.

RDMA metrics count physical slices, not logical requests. Slice events have at
most 2x9 = 18 operation/event series. Retry and retry-exhausted counters each
have at most 2x3 = 6 operation/cause series, completion latency has at most
2x4 = 8 operation/outcome series, and each RDMA in-flight gauge has two
operation series. `post_attempt` is recorded immediately before the verbs post
call; a rejected work request additionally records `post_error` and is removed
from the posted gauge. `late_completion` is a CQE for a slice already resolved
by timeout, cancellation, or an adjacent queue-pair sweep. A retry is counted
only when the slice is actually re-enqueued; an exhausted attempt increments
only `tent_rdma_retry_exhausted_total`.

Stuck-event labels are bounded to three phases, the `TransportType` enum, and
two operations. Request, batch, peer, address, and attempt identifiers remain
in structured logs and are never Prometheus labels.

Endpoint lifecycle metrics have seven event series. QP transition failures
have at most 2x7 = 14 QP-type/transition series, and notification diagnostics
have fourteen event series. These metrics identify the failing lifecycle phase
without exposing endpoint names, peer addresses, connection generations or QP
numbers. Expected flush completions from an endpoint already retiring are not
counted as notification completion errors.

Stream metrics are emitted only for `tcp` and `hp_tcp`. Classic TCP queue
events represent logical tasks; each control-plane data RPC started by its
retry loop is a physical attempt. HP-TCP queue and attempt events represent
physical slice operations, while `retry_issued` is counted once when a logical
task retry is successfully admitted. Queue wait and attempt latency have at
most 2x2 = 4 transport/operation series before their bounded outcome split;
the event counter has at most 2x2x13 = 52 series and attempt latency has at
most 2x2x7 = 28 series. Peer identity, socket address, lane and request ID are
never metric labels.

`tent_worker_last_progress_timestamp_seconds` advances when the engine progress
worker completes a wake cycle, when a staging worker changes a chunk state or
finishes a parent task, and when an RDMA worker completes a post/poll cycle.
An idle worker is expected to retain its previous timestamp; alert on its age
only when the corresponding in-flight or queue signals show outstanding work.
The JSON endpoint reports the most recent timestamp across workers, while
Prometheus preserves the per-worker labels.

Staging in-flight gauges cover parent tasks after a staging worker dequeues
them and before it hands any undrained resources to deferred cleanup. Work
waiting in a staging shard queue is not included. Deferred cleanup is exposed
separately by `tent_staging_cleanup_pending`, and its `batch` and
`remote_operation` series are decremented only after those resources reach a
confirmed terminal state or ownership moves into bounded shutdown teardown.

### Per-rail load (`getNicLoadStats`)

`TransferEngine::getNicLoadStats()` returns `NicLoadStats` entries with
`device_name`, `inflight_bytes` and `ewma_bandwidth_bps`. RDMA and UB
both implement it: RDMA reports one entry per local NIC, UB reports one entry
per device tracked by its quota manager under the same `ub:<name>:<eid>`
topology name used in logs and metrics. UB reports the highest bandwidth
sample from that device's unpaused rails, or zero when none has a sample.
This snapshot does not filter devices by their current context health.

These are pull-based snapshots, not Prometheus series: UB rail health
(pauses, errors in window, timeouts, endpoint rebuilds) lives in `RailMonitor`
and is read through the transport rather than scraped. See {ref}`TENT Failover
<tent-failover>` for how those counters drive rail selection.

## Integration with TransferEngine

The metrics system is automatically integrated with TransferEngine. When TransferEngine starts, it initializes the metrics system:

```cpp
#include "tent/metrics/tent_metrics.h"
#include "tent/metrics/config_loader.h"

// Load configuration
auto metrics_config = MetricsConfigLoader::loadWithDefaults();
if (metrics_config.enabled) {
    TentMetrics::instance().initialize(metrics_config);
}
```

Metrics are automatically recorded at the TENT layer:

- **Latency tracking**: Start time is recorded when `submitTransfer` is called
- **Metrics recording**: When `getTransferStatus` detects task completion, latency is calculated and metrics are recorded
- **Attempt tracking**: Each concrete `Transport::submitTransferTasks()` call is counted as one attempt. A failed attempt is closed before failover changes the task's current transport, and the replacement attempt gets a fresh attempt timestamp. Synchronous submit failures are also closed as failed attempts. The transport is captured when the attempt starts, so it is attributed correctly even if failover overwrites the task's current transport afterwards. Staging is an orchestration step, not a transport attempt: `ProxyManager` chunks the transfer and issues the real transport submissions, which are the ones counted, so a staged transfer is not double-counted.

This provides two complementary views:

- Logical request latency and outcome, attributed to the final transport for backward compatibility.
- Physical attempt count, failure count, and latency, attributed to the transport that actually executed that attempt.

For a recovered RDMA→TCP request, request metrics record one successful TCP
outcome, while attempt metrics record one failed RDMA attempt and one successful
TCP attempt. The causal-chain stage metrics (`tent_stage_queue_wait_us`,
`tent_stage_dispatch_us`, `tent_stage_transport_us`) are unchanged by this
addition: they remain attributed to the final transport and
`tent_stage_transport_us` still spans the whole request, so existing dashboards
keep their meaning. Use `tent_transport_attempt_latency_us` (labeled by the
transport that actually ran each attempt) to inspect per-attempt latency in a
multi-attempt request.

**Note**: Remember to build with `-DTENT_METRICS_ENABLED=ON` to enable metrics collection.

## Adding New Metrics

To add new metrics to the TENT metrics system, follow these steps:

### Step 1: Declare the Metric

Add the metric member variable in `tent_metrics.h`:

```cpp
// In TentMetrics class private section:

// For a new counter:
ylt::metric::counter_t new_counter_{"tent_new_counter", "Description of the counter"};

// For a new histogram:
ylt::metric::histogram_t new_histogram_{"tent_new_histogram", "Description",
                                        std::vector<double>{/* bucket boundaries */}};
```

### Step 2: Register the Metric

Add the metric pointer to `registerMetrics()` in `tent_metrics.cpp`:

```cpp
void TentMetrics::registerMetrics() {
    counters_ = {
        &read_bytes_total_,
        // ... existing counters ...
        &new_counter_,  // Add new counter here
    };

    histograms_ = {
        {&read_latency_, &kLatencyBuckets},
        // ... existing histograms ...
        {&new_histogram_, &kNewBuckets},  // Add new histogram + its buckets here
    };
}
```

### Step 3: Add Recording Methods (Optional)

If needed, add public methods to record the metric:

```cpp
// In tent_metrics.h:
void recordNewMetric(int64_t value);

// In tent_metrics.cpp:
void TentMetrics::recordNewMetric(int64_t value) {
    if (!initialized_ || !runtime_enabled_.load(std::memory_order_relaxed)) return;
    new_counter_.inc(value);
    // or for histogram:
    // new_histogram_.observe(value);
}
```

### Automatic Serialization

Once registered in `registerMetrics()`, the new metric will be **automatically included** in:
- `/metrics` (Prometheus format)
- `/metrics/json` (JSON format)

No changes to `getPrometheusMetrics()` or `getJsonMetrics()` are required.

## Advanced Configuration

### Histogram Buckets

Histogram bucket boundaries are fixed at compile time (defined as `static inline const std::vector<double>` members in `tent_metrics.h`: `kLatencyBuckets`, `kSizeBuckets`, `kMluPerMilleBuckets`, `kStageBuckets`). They are intentionally not runtime-configurable so that observability is reproducible across deployments. To change buckets, edit the constant in `tent_metrics.h` and rebuild.

### Compatibility

The `metrics/latency_buckets` and `metrics/size_buckets` config keys, and the `TENT_METRICS_LATENCY_BUCKETS` / `TENT_METRICS_SIZE_BUCKETS` environment variables, were removed and are now silently ignored. Histogram buckets are fixed at compile time (see Histogram Buckets above).

**Migration:** remove these keys from your `transfer-engine.json` and unset the environment variables. If custom bucket boundaries are required, edit `kLatencyBuckets` / `kSizeBuckets` in `tent/metrics/tent_metrics.h` and rebuild.

> **Note:** the previous runtime-configurable implementation had a latent bug — the JSON output labeled custom buckets with the compile-time default boundaries, so `/metrics/json` was already mislabeled for custom-bucket deployments. Removing the knob fixes this rather than preserving a buggy code path.

The `metrics/enable_prometheus` and `metrics/enable_json` config keys, and the `TENT_METRICS_ENABLE_PROMETHEUS` / `TENT_METRICS_ENABLE_JSON` environment variables, were removed and are now silently ignored. The `/metrics` and `/metrics/json` HTTP endpoints are now registered unconditionally and cannot be toggled independently — both are read-only and served on the same port, so there was no real scenario where a deployment wanted one disabled.

**Migration:** remove these keys from your `transfer-engine.json` and unset the environment variables. To fully disable the metrics subsystem (no HTTP server, no periodic summary logging, no recording), set `metrics/enabled` to `false` (or `TENT_METRICS_ENABLED=false`). Note that "log-only mode" — where periodic summaries are still logged but `/metrics` is unavailable — only occurs when metrics are enabled but the HTTP port cannot be bound (e.g. port conflict); it is not triggered by `metrics/enabled=false`.

### Validation

```cpp
MetricsConfig config = MetricsConfigLoader::loadWithDefaults();
std::string error_msg;
if (!MetricsConfigLoader::validateConfig(config, &error_msg)) {
    LOG(ERROR) << "Invalid metrics config: " << error_msg;
    return;
}
```

## Prometheus Integration

### Prometheus Configuration

```yaml
# prometheus.yml
scrape_configs:
  - job_name: 'tent-metrics'
    static_configs:
      - targets: ['localhost:9100']
    scrape_interval: 15s
    metrics_path: /metrics
```

### Grafana Queries

```promql
# Transfer throughput (MB/s) — all transports
rate(tent_read_bytes_total[5m]) / 1024 / 1024
rate(tent_write_bytes_total[5m]) / 1024 / 1024

# Transfer throughput (MB/s) — per transport
rate(tent_read_bytes_total{transport="rdma"}[5m]) / 1024 / 1024
rate(tent_read_bytes_total{transport="tcp"}[5m]) / 1024 / 1024

# Request rate
rate(tent_read_requests_total[5m])
rate(tent_write_requests_total[5m])

# Failure rate (all transports)
rate(tent_read_failures_total[5m]) / rate(tent_read_requests_total[5m])

# Failure rate (per transport)
rate(tent_read_failures_total{transport="tcp"}[5m]) / rate(tent_read_requests_total{transport="tcp"}[5m])

# P99 latency (note: latency is in microseconds, convert to seconds for display)
histogram_quantile(0.99, rate(tent_read_latency_us_bucket[5m])) / 1000000
histogram_quantile(0.99, rate(tent_write_latency_us_bucket[5m])) / 1000000

# P99 latency per transport
histogram_quantile(0.99, rate(tent_read_latency_us_bucket{transport="rdma"}[5m])) / 1000000

# Failover rate by transport pair
rate(tent_transport_failover_total{from="rdma",to="tcp"}[5m])

# RDMA physical-attempt failure rate
sum(rate(tent_transport_attempt_failures_total{transport="rdma"}[5m]))
/
sum(rate(tent_transport_attempts_total{transport="rdma"}[5m]))

# P99 latency of RDMA write attempts
histogram_quantile(
  0.99,
  sum by (le) (
    rate(tent_transport_attempt_latency_us_bucket{
      transport="rdma",operation="write"
    }[5m])
  )
) / 1000000

# Transfer metrics recording disabled at runtime
tent_metrics_runtime_enabled == 0

# Any serialization failure in the last five minutes
increase(tent_metrics_serialization_failures_total[5m]) > 0

# Prometheus serialization has succeeded before, but is now stale. With the
# 15-second scrape interval above, 45 seconds allows two missed intervals.
(tent_metrics_last_success_timestamp_seconds{format="prometheus"} > 0)
and
(time() - tent_metrics_last_success_timestamp_seconds{format="prometheus"} > 45)
```
