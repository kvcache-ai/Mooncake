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

When disabled at compile time (`TENT_METRICS_ENABLED=OFF`, the default), all metrics macros expand to `((void)0)`, resulting in **zero runtime overhead**.

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
    "enable_json": true,
    "latency_buckets": [0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0],
    "size_buckets": [1024, 4096, 16384, 65536, 262144, 1048576, 4194304, 16777216, 67108864, 268435456, 1073741824]
  },
  "transports": {
    // ... transport configuration
  }
}
```

**Note**: 
- `report_interval_seconds`: Set to 0 to disable periodic logging
- `latency_buckets`: Values are in **seconds** (e.g., 0.001 = 1ms). The system internally converts to microseconds for histogram storage.
- `size_buckets`: Values are in bytes

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

# Custom buckets (comma-separated, latency in seconds, size in bytes)
TENT_METRICS_LATENCY_BUCKETS="0.0001,0.0005,0.001,0.005,0.01,0.05,0.1,0.5,1.0"
TENT_METRICS_SIZE_BUCKETS="1024,4096,16384,65536,262144,1048576"
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
TENT_RECORD_READ_FAILED(1024*1024);             // 1MB read failed
TENT_RECORD_WRITE_FAILED(512*1024);             // 512KB write failed

// Direct API usage
auto& tent_metrics = TentMetrics::instance();
tent_metrics.recordReadCompleted(1024*1024, 0.025);
tent_metrics.recordWriteCompleted(512*1024, 0.015);
tent_metrics.recordReadFailed(1024*1024);
tent_metrics.recordWriteFailed(512*1024);
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
| `tent_read_bytes_total` | Counter | Total bytes read via TENT |
| `tent_write_bytes_total` | Counter | Total bytes written via TENT |
| `tent_read_requests_total` | Counter | Total read requests via TENT |
| `tent_write_requests_total` | Counter | Total write requests via TENT |
| `tent_read_failures_total` | Counter | Total read failures via TENT |
| `tent_write_failures_total` | Counter | Total write failures via TENT |
| `tent_read_latency_us` | Histogram | Read latency distribution in microseconds |
| `tent_write_latency_us` | Histogram | Write latency distribution in microseconds |
| `tent_read_size_bytes` | Histogram | Read request size distribution in bytes |
| `tent_write_size_bytes` | Histogram | Write request size distribution in bytes |

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
        &read_latency_,
        // ... existing histograms ...
        &new_histogram_,  // Add new histogram here
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

### Custom Buckets

Define custom histogram buckets for specific use cases:

```cpp
// Latency buckets (in seconds, converted to microseconds internally)
std::vector<double> rdma_latency_buckets = {
    0.000001, 0.000005, 0.00001, 0.00005, 0.0001,  // 1-100μs
    0.0005, 0.001, 0.005, 0.01, 0.05, 0.1          // 0.5-100ms
};

// Size buckets for different data patterns (in bytes)
std::vector<double> message_size_buckets = {
    64, 256, 1024, 4096, 16384, 65536, 262144  // 64B to 256KB
};
```

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

