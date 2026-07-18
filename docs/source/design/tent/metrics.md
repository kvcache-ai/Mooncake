# TENT Metrics System

TENT provides a built-in metrics system based on yalantinglibs, compatible with Prometheus for monitoring data transfer performance and system health.

## Overview

The metrics system supports two metric types:

- **Counter**: Monotonically increasing values (e.g., total bytes transferred, total requests)
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
    "report_interval_seconds": 30,
    "enable_prometheus": true,
    "enable_json": true
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

# Output formats
TENT_METRICS_ENABLE_PROMETHEUS=true
TENT_METRICS_ENABLE_JSON=true
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
TENT_RECORD_READ_COMPLETED(1024*1024, 0.025);   // 1MB read in 25ms
TENT_RECORD_WRITE_COMPLETED(512*1024, 0.015);   // 512KB write in 15ms
TENT_RECORD_READ_FAILED();                       // read failed (no bytes recorded)
TENT_RECORD_WRITE_FAILED();                      // write failed (no bytes recorded)
TENT_RECORD_TRANSPORT_FAILOVER();                // cross-transport failover event

// Direct API usage
auto& tent_metrics = TentMetrics::instance();
tent_metrics.recordReadCompleted(1024*1024, 0.025);
tent_metrics.recordWriteCompleted(512*1024, 0.015);
tent_metrics.recordReadFailed();
tent_metrics.recordWriteFailed();
tent_metrics.recordTransportFailover();

// Deadline feasibility (RFC #2519, observability only):
tent_metrics.recordDeadlineMLU(0.8);        // MLU < 1 met the deadline
tent_metrics.recordDeadlineInfeasible();     // deadline was in the past at submit

// Causal-chain per-stage latency breakdown (microseconds):
tent_metrics.recordStageLatency(TentMetrics::Stage::QueueWait, 12.0);
tent_metrics.recordStageLatency(TentMetrics::Stage::Dispatch, 45.0);
tent_metrics.recordStageLatency(TentMetrics::Stage::Transport, 130.0);
```

### RAII Latency Measurement

```cpp
// Automatic latency measurement using RAII
{
    TENT_SCOPED_READ_LATENCY(1024 * 1024); // e.g. 1MB
    // ... perform read operation ...
}  // latency automatically recorded when scope exits

{
    TENT_SCOPED_WRITE_LATENCY(512 * 1024); // e.g. 512KB
    // ... perform write operation ...
}
```

## HTTP Server Endpoints

The HTTP server provides multiple endpoints:

- **`/metrics`**: Prometheus format
- **`/metrics/summary`**: Human-readable summary
- **`/metrics/json`**: JSON format
- **`/health`**: Health check endpoint

### Example Responses

**Prometheus Format (`/metrics`)**:
```
# HELP tent_read_bytes_total Total bytes read via TENT
# TYPE tent_read_bytes_total counter
tent_read_bytes_total 1048576

# HELP tent_write_bytes_total Total bytes written via TENT
# TYPE tent_write_bytes_total counter
tent_write_bytes_total 524288

# HELP tent_read_requests_total Total read requests via TENT
# TYPE tent_read_requests_total counter
tent_read_requests_total 100

# HELP tent_write_requests_total Total write requests via TENT
# TYPE tent_write_requests_total counter
tent_write_requests_total 50

# HELP tent_read_failures_total Total read failures via TENT
# TYPE tent_read_failures_total counter
tent_read_failures_total 2

# HELP tent_write_failures_total Total write failures via TENT
# TYPE tent_write_failures_total counter
tent_write_failures_total 1

# HELP tent_read_latency_us Read latency distribution in microseconds
# TYPE tent_read_latency_us histogram
tent_read_latency_us_bucket{le="100"} 10
tent_read_latency_us_bucket{le="500"} 50
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
  "tent_write_failures_total": 1
}
```

**Summary Format (`/metrics/summary`)**:
```
Read: 1.00 MB (100 reqs, 2 fails) | Write: 512.00 KB (50 reqs, 1 fails)
```

## Available Metrics

| Metric Name | Type | Description |
|-------------|------|-------------|
| `tent_read_bytes_total` | Counter | Total bytes read via TENT (success only; failures record no bytes) |
| `tent_write_bytes_total` | Counter | Total bytes written via TENT (success only) |
| `tent_read_requests_total` | Counter | Total read requests via TENT (success + failure) |
| `tent_write_requests_total` | Counter | Total write requests via TENT (success + failure) |
| `tent_read_failures_total` | Counter | Total read failures via TENT |
| `tent_write_failures_total` | Counter | Total write failures via TENT |
| `tent_transport_failover_total` | Counter | Total cross-transport failover events |
| `tent_deadline_infeasible_total` | Counter | Transfers whose deadline was already in the past at submit time |
| `tent_read_latency_us` | Histogram | Read latency distribution in microseconds |
| `tent_write_latency_us` | Histogram | Write latency distribution in microseconds |
| `tent_read_size_bytes` | Histogram | Read request size distribution in bytes |
| `tent_write_size_bytes` | Histogram | Write request size distribution in bytes |
| `tent_deadline_mlu_permille` | Histogram | Deadline feasibility ratio (MLU x 1000); 1000 = MLU 1.0 (the met/missed boundary) |
| `tent_stage_queue_wait_us` | Histogram | Causal chain: queue wait latency in microseconds |
| `tent_stage_dispatch_us` | Histogram | Causal chain: dispatch latency in microseconds |
| `tent_stage_transport_us` | Histogram | Causal chain: transport execution latency in microseconds |

**Notes**:
- `*_requests_total` counts both successful and failed requests. To compute the success rate, use `1 - (failures / requests)`.
- `*_failures_total` does not record bytes; failed transfers transfer no bytes.
- `tent_deadline_infeasible_total` is a dedicated counter (not a histogram sentinel) so infeasible-at-submit cases are distinguishable from genuine high-MLU samples.
- yalantinglibs omits zero-valued counters/histograms from the Prometheus output, so a metric only appears once it has been observed at least once.

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

This provides end-to-end latency measurement across all transport types (RDMA, TCP, NVLink, etc.).

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
# Transfer throughput (MB/s)
rate(tent_read_bytes_total[5m]) / 1024 / 1024
rate(tent_write_bytes_total[5m]) / 1024 / 1024

# Request rate
rate(tent_read_requests_total[5m])
rate(tent_write_requests_total[5m])

# Failure rate
rate(tent_read_failures_total[5m]) / rate(tent_read_requests_total[5m])

# P99 latency (note: latency is in microseconds, convert to seconds for display)
histogram_quantile(0.99, rate(tent_read_latency_us_bucket[5m])) / 1000000
histogram_quantile(0.99, rate(tent_write_latency_us_bucket[5m])) / 1000000
```

