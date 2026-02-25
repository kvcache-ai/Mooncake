// Copyright 2025 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <atomic>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "tent/common/status.h"
#include "tent/metrics/config_loader.h"

// Compile-time metrics enable/disable switch
// Can be set via CMake option TENT_METRICS_ENABLED or define
// TENT_METRICS_ENABLED=0/1 Default is OFF (0) to match CMake default:
// option(TENT_METRICS_ENABLED ... OFF)
#ifndef TENT_METRICS_ENABLED
#define TENT_METRICS_ENABLED 0
#endif

#if TENT_METRICS_ENABLED
#include <ylt/metric.hpp>
#include <ylt/coro_http/coro_http_server.hpp>
#endif

namespace mooncake::tent {

// Estimated buffer size for Prometheus metrics serialization (pre-allocation
// optimization)
constexpr size_t kPrometheusBufferSize = 4096;

/**
 * @brief TENT metrics system with HTTP server for Prometheus scraping
 *
 * This class provides:
 * - Metrics collection using yalantinglibs
 * - HTTP server for /metrics endpoint (Prometheus format)
 * - Optional periodic logging of metrics summary
 * - Compile-time disable option (TENT_METRICS_ENABLED=0) for zero overhead
 * - Runtime disable option via setEnabled(false) for minimal overhead
 */
class TentMetrics {
   public:
    static TentMetrics& instance();

    // Initialize with configuration and start HTTP server
    Status initialize(const MetricsConfig& config);

    // Cleanup and stop HTTP server
    void shutdown();

    // Runtime enable/disable switch
    // When disabled, record* functions return immediately with minimal overhead
    static void setEnabled(bool enabled) {
        runtime_enabled_.store(enabled, std::memory_order_relaxed);
    }
    static bool isEnabled() {
        return runtime_enabled_.load(std::memory_order_relaxed);
    }

    // Record transfer operations
    void recordReadCompleted(size_t bytes, double latency_seconds = 0.0);
    void recordWriteCompleted(size_t bytes, double latency_seconds = 0.0);
    void recordReadFailed(size_t bytes);
    void recordWriteFailed(size_t bytes);

    // Get metrics for HTTP server
    std::string getPrometheusMetrics();
    std::string getJsonMetrics();
    std::string getSummaryString();

    // Check if initialized
    bool isInitialized() const { return initialized_; }

   private:
    TentMetrics() = default;
    ~TentMetrics();
    TentMetrics(const TentMetrics&) = delete;
    TentMetrics& operator=(const TentMetrics&) = delete;

    // Runtime enable flag (atomic for thread-safe access with minimal overhead)
    static inline std::atomic<bool> runtime_enabled_{true};

    std::atomic<bool> initialized_{false};
    MetricsConfig config_;

#if TENT_METRICS_ENABLED
    // Initialize HTTP server with endpoints
    void initHttpServer();

    // HTTP server for metrics endpoint
    std::unique_ptr<coro_http::coro_http_server> http_server_;

    // Periodic metric reporting thread
    std::thread metric_report_thread_;
    std::atomic<bool> metric_report_running_{false};
    std::mutex metric_report_mutex_;
    std::condition_variable metric_report_cv_;

    // Counters - stored as pointers for unified management
    std::vector<ylt::metric::counter_t*> counters_;
    ylt::metric::counter_t read_bytes_total_{"tent_read_bytes_total",
                                             "Total bytes read via TENT"};
    ylt::metric::counter_t write_bytes_total_{"tent_write_bytes_total",
                                              "Total bytes written via TENT"};
    ylt::metric::counter_t read_requests_total_{"tent_read_requests_total",
                                                "Total read requests via TENT"};
    ylt::metric::counter_t write_requests_total_{
        "tent_write_requests_total", "Total write requests via TENT"};
    ylt::metric::counter_t read_failures_total_{"tent_read_failures_total",
                                                "Total read failures via TENT"};
    ylt::metric::counter_t write_failures_total_{
        "tent_write_failures_total", "Total write failures via TENT"};

    // Histograms - stored as pointers for unified management
    std::vector<ylt::metric::histogram_t*> histograms_;
    // Store bucket boundaries separately since ylt histogram doesn't expose
    // them publicly
    std::vector<std::vector<double>> histogram_boundaries_;

    // Latency histograms use microseconds (us) as unit
    // Default buckets: 100us, 500us, 1ms, 5ms, 10ms, 50ms, 100ms, 500ms, 1s
    static inline const std::vector<double> kLatencyBuckets{
        100, 500, 1000, 5000, 10000, 50000, 100000, 500000, 1000000};
    ylt::metric::histogram_t read_latency_{
        "tent_read_latency_us", "Read latency distribution in microseconds",
        kLatencyBuckets};
    ylt::metric::histogram_t write_latency_{
        "tent_write_latency_us", "Write latency distribution in microseconds",
        kLatencyBuckets};
    // Size histograms for request size distribution (in bytes)
    // Default buckets: 1KB, 4KB, 16KB, 64KB, 256KB, 1MB, 4MB, 16MB, 64MB,
    // 256MB, 1GB
    static inline const std::vector<double> kSizeBuckets{
        1024,    4096,     16384,    65536,     262144,    1048576,
        4194304, 16777216, 67108864, 268435456, 1073741824};
    ylt::metric::histogram_t read_size_{
        "tent_read_size_bytes", "Read request size distribution in bytes",
        kSizeBuckets};
    ylt::metric::histogram_t write_size_{
        "tent_write_size_bytes", "Write request size distribution in bytes",
        kSizeBuckets};

    // Helper to register all metrics to the vectors
    void registerMetrics();
#endif  // TENT_METRICS_ENABLED
};

#if TENT_METRICS_ENABLED

/**
 * @brief RAII helper for automatic latency measurement
 *
 * When runtime metrics are disabled (TentMetrics::isEnabled() == false),
 * this class skips time recording to minimize overhead.
 */
class ScopedLatencyRecorder {
   public:
    enum class OperationType { Read, Write };

    ScopedLatencyRecorder(OperationType type, size_t bytes)
        : type_(type), bytes_(bytes), enabled_(TentMetrics::isEnabled()) {
        // Only record start time if metrics are enabled (avoid clock overhead
        // when disabled)
        if (enabled_) {
            start_ = std::chrono::steady_clock::now();
        }
    }

    ~ScopedLatencyRecorder() {
        if (!enabled_ || failed_)
            return;  // Skip if disabled or already marked as failed
        auto end = std::chrono::steady_clock::now();
        double latency = std::chrono::duration<double>(end - start_).count();
        if (type_ == OperationType::Read) {
            TentMetrics::instance().recordReadCompleted(bytes_, latency);
        } else {
            TentMetrics::instance().recordWriteCompleted(bytes_, latency);
        }
    }

    void markFailed() {
        if (!enabled_) return;  // Skip if disabled
        failed_ = true;
        if (type_ == OperationType::Read) {
            TentMetrics::instance().recordReadFailed(bytes_);
        } else {
            TentMetrics::instance().recordWriteFailed(bytes_);
        }
    }

   private:
    OperationType type_;
    size_t bytes_;
    std::chrono::steady_clock::time_point start_;
    bool enabled_;  // Captured at construction time for consistent behavior
    bool failed_ = false;
};

// Convenience macros for recording metrics (enabled version)
#define TENT_RECORD_READ_COMPLETED(bytes, latency)                         \
    do {                                                                   \
        if (::mooncake::tent::TentMetrics::isEnabled()) {                  \
            ::mooncake::tent::TentMetrics::instance().recordReadCompleted( \
                bytes, latency);                                           \
        }                                                                  \
    } while (0)

#define TENT_RECORD_WRITE_COMPLETED(bytes, latency)                         \
    do {                                                                    \
        if (::mooncake::tent::TentMetrics::isEnabled()) {                   \
            ::mooncake::tent::TentMetrics::instance().recordWriteCompleted( \
                bytes, latency);                                            \
        }                                                                   \
    } while (0)

#define TENT_RECORD_READ_FAILED(bytes)                                         \
    do {                                                                       \
        if (::mooncake::tent::TentMetrics::isEnabled()) {                      \
            ::mooncake::tent::TentMetrics::instance().recordReadFailed(bytes); \
        }                                                                      \
    } while (0)

#define TENT_RECORD_WRITE_FAILED(bytes)                                  \
    do {                                                                 \
        if (::mooncake::tent::TentMetrics::isEnabled()) {                \
            ::mooncake::tent::TentMetrics::instance().recordWriteFailed( \
                bytes);                                                  \
        }                                                                \
    } while (0)

// RAII macro for automatic latency measurement
#define TENT_SCOPED_READ_LATENCY(bytes)                              \
    ::mooncake::tent::ScopedLatencyRecorder _tent_latency_recorder_( \
        ::mooncake::tent::ScopedLatencyRecorder::OperationType::Read, bytes)

#define TENT_SCOPED_WRITE_LATENCY(bytes)                             \
    ::mooncake::tent::ScopedLatencyRecorder _tent_latency_recorder_( \
        ::mooncake::tent::ScopedLatencyRecorder::OperationType::Write, bytes)

#else  // !TENT_METRICS_ENABLED

// No-op stub class for ScopedLatencyRecorder when metrics are disabled
class ScopedLatencyRecorder {
   public:
    enum class OperationType { Read, Write };
    ScopedLatencyRecorder(OperationType, size_t) {}
    void markFailed() {}
};

// Zero-overhead macros when metrics are disabled at compile time
#define TENT_RECORD_READ_COMPLETED(bytes, latency) ((void)0)
#define TENT_RECORD_WRITE_COMPLETED(bytes, latency) ((void)0)
#define TENT_RECORD_READ_FAILED(bytes) ((void)0)
#define TENT_RECORD_WRITE_FAILED(bytes) ((void)0)
#define TENT_SCOPED_READ_LATENCY(bytes) ((void)0)
#define TENT_SCOPED_WRITE_LATENCY(bytes) ((void)0)

#endif  // TENT_METRICS_ENABLED

}  // namespace mooncake::tent
