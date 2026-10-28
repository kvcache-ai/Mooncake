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
- **`/metrics/summary`**: Human-readable summary
- **`/metrics/json`**: JSON format
- **`/health`**: Metrics HTTP process liveness (`200 OK` while the endpoint is serving). It does not report TransferEngine readiness or transport health.

The two serialization endpoints return HTTP `500` when their response cannot
be serialized. The failed attempt is counted in
`tent_metrics_serialization_failures_total` and becomes visible on the next
successful Prometheus scrape. `/health` remains a liveness-only endpoint.

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

## Available Metrics

| Metric Name | Type | Labels | Description |
|-------------|------|--------|-------------|
| `tent_read_bytes_total` | Counter | `transport` | Total bytes read via TENT (success only; failures record no bytes) |
| `tent_write_bytes_total` | Counter | `transport` | Total bytes written via TENT (success only) |
| `tent_read_requests_total` | Counter | `transport` | Total read requests via TENT (success + failure) |
| `tent_write_requests_total` | Counter | `transport` | Total write requests via TENT (success + failure) |
| `tent_read_failures_total` | Counter | `transport` | Total read failures via TENT |
| `tent_write_failures_total` | Counter | `transport` | Total write failures via TENT |
| `tent_transport_failover_total` | Counter | `from`, `to` | Total cross-transport failover events |
| `tent_transport_attempts_total` | Counter | `transport`, `operation` | Physical transport attempts submitted for execution |
| `tent_transport_attempt_failures_total` | Counter | `transport`, `operation` | Physical transport attempts that terminated with `FAILED` |
| `tent_task_failures_total` | Counter | `transport`, `reason` | Task failures grouped by terminal transport and failure reason |
| `tent_deadline_infeasible_total` | Counter | `transport` | Transfers whose deadline was already in the past at submit time |
| `tent_quarantined_batches_total` | Counter | - | Batches abandoned after repeated reclaim failures and retained until engine teardown |
| `tent_inflight_attempts` | Gauge | `transport` | Physical transport attempts submitted but not yet finished |
| `tent_registered_buffer_bytes` | Gauge | `transport` | Currently registered local buffer bytes |
| `tent_read_latency_us` | Histogram | `transport` | Read latency distribution in microseconds |
| `tent_write_latency_us` | Histogram | `transport` | Write latency distribution in microseconds |
| `tent_read_size_bytes` | Histogram | `transport` | Read request size distribution in bytes |
| `tent_write_size_bytes` | Histogram | `transport` | Write request size distribution in bytes |
| `tent_deadline_mlu_permille` | Histogram | `transport` | Deadline feasibility ratio (MLU x 1000); 1000 = MLU 1.0 (the met/missed boundary) |
| `tent_stage_queue_wait_us` | Histogram | `transport` | Causal chain: queue wait latency in microseconds |
| `tent_stage_dispatch_us` | Histogram | `transport` | Causal chain: dispatch latency in microseconds |
| `tent_stage_transport_us` | Histogram | `transport` | Causal chain: transport execution latency in microseconds |
| `tent_transport_attempt_latency_us` | Histogram | `transport`, `operation` | Observed latency of each physical transport attempt |

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
- `tent_transport_attempts_total` and `tent_transport_attempt_failures_total` measure physical transport reliability. A recovered RDMA→TCP request contributes one failed RDMA attempt and one successful TCP attempt.
- Attempt latency currently ends when polling or the progress worker observes the terminal status, so it may include completion-observation delay.
- `*_failures_total` does not record bytes; failed transfers transfer no bytes.
- `tent_deadline_infeasible_total` is a dedicated counter (not a histogram sentinel) so infeasible-at-submit cases are distinguishable from genuine high-MLU samples.
- `tent_metrics_last_success_timestamp_seconds{format="..."} == 0` means that format has never completed a successful serialization. Prometheus freshness alerts must select `format="prometheus"` and guard on a value greater than zero; a Prometheus-only deployment is not expected to serialize JSON.
- yalantinglibs omits zero-valued transfer counters/histograms from the Prometheus output, so a transfer metric only appears once it has been observed at least once. The metrics-pipeline self-observability families above are emitted unconditionally.

### Labels

Transfer and attempt metrics carry a `transport` label (the
`tent_transport_failover_total` counter uses `from` and `to` instead) so they
can be sliced by transport without grepping logs. Attempt metrics also carry
an `operation` label. Task failures add a closed-set `reason` label and metrics
serialization adds a two-valued `format` label. Label values come from bounded
sets; no request IDs, addresses, segment names, or other unbounded values are
accepted.

| Label | Values | Description |
|-------|--------|-------------|
| `transport` | `unspec`, `rdma`, `mnnvl`, `shm`, `nvlink`, `gds`, `io_uring`, `tcp`, `ascend`, `sunrise_link`, `tpu`, `ub`, `mpcomm`, `hp_tcp`, `xpu`, `hylink`, `fabric`, `mlu`, `memfabric` | The transport that handled the transfer |
| `operation` | `read`, `write` | Attempt operation |
| `reason` | `submit`, `poll`, `timeout`, `canceled` | Task failure classification |
| `format` | `prometheus`, `json` | Serialization output format |
| `from` | (same set) | Transport that failed before failover |
| `to` | (same set) | Transport that the failover switched to |
| `version` | Build-time constant | TENT build version |
| `commit` | Build-time constant | Git commit used for the build |

Transport label values come from the shared `transportTypeName()` mapping.
`unspec` covers transfers that failed before a transport was selected.

**Cardinality**: the `transport` label has 19 values; the failover
`from`/`to` pair has at most 19x19 = 361 combinations (in practice only a
few pairs ever occur), each attempt metric has at most 19x2 = 38
transport/operation combinations, task failures have at most 19x4 = 76, and
each serialization family has exactly two series. Build identity labels are
constant for the life of the process; a served `tent_metrics_build_info` series
itself confirms that metrics were compiled in. Total series across all metrics
is bounded by these closed sets.

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
