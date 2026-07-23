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

#include <array>
#include <atomic>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "tent/common/status.h"
#include "tent/common/types.h"
#include "tent/metrics/config_loader.h"

// Compile-time metrics enable/disable switch
// Can be set via CMake option TENT_METRICS_ENABLED or define
// TENT_METRICS_ENABLED=0/1 Default is OFF (0) to match CMake default:
// option(TENT_METRICS_ENABLED ... OFF)
#ifndef TENT_METRICS_ENABLED
#define TENT_METRICS_ENABLED 0
#endif

#if TENT_METRICS_ENABLED
#include <csignal>  // Required before ylt headers: coro_io.hpp uses std::signal/SIGPIPE
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

    // Record transfer operations. The TransportType argument labels the
    // metric with the transport that handled (or attempted) the transfer,
    // so Prometheus queries can break down traffic by transport.
    void recordReadCompleted(TransportType tp, size_t bytes,
                             double latency_seconds = 0.0);
    void recordWriteCompleted(TransportType tp, size_t bytes,
                              double latency_seconds = 0.0);
    void recordReadFailed(TransportType tp);
    void recordWriteFailed(TransportType tp);
    // Failover counter is labeled with both the source and destination
    // transport types so failover flows (e.g. rdma->tcp) are queryable.
    void recordTransportFailover(TransportType from, TransportType to);

    // Record the deadline feasibility ratio (MLU) for a completed transfer
    // that carried a deadline. mlu = actual_transfer_seconds / window_seconds,
    // where window_seconds is (deadline - submit_time). mlu < 1 means the
    // transfer met its deadline; mlu >= 1 means it missed. Observability only.
    void recordDeadlineMLU(TransportType tp, double mlu);

    // Record a transfer whose deadline was already in the past at submit time
    // (infeasible window). Recorded into a dedicated counter so it is
    // distinguishable from genuine MLU samples in the histogram above.
    void recordDeadlineInfeasible(TransportType tp);

    enum class Stage {
        QueueWait,
        Dispatch,
        Transport,
    };

    // Causal chain: record per-stage latency breakdown (microseconds).
    void recordStageLatency(Stage stage, TransportType tp, double latency_us);

    // Get metrics for HTTP server
    std::string getPrometheusMetrics();
    std::string getJsonMetrics();
    std::string getSummaryString();

    // Check if initialized
    bool isInitialized() const { return initialized_; }

    // Port the HTTP metrics server is bound to, or 0 when the endpoint is not
    // running (log-only mode, or metrics disabled at compile time). Backed by
    // an atomic so it is safe to read from other threads while initialize() is
    // still running.
    uint16_t httpPort() const {
        return bound_http_port_.load(std::memory_order_relaxed);
    }

   private:
    TentMetrics() = default;
    ~TentMetrics();
    TentMetrics(const TentMetrics&) = delete;
    TentMetrics& operator=(const TentMetrics&) = delete;

    // Runtime enable flag (atomic for thread-safe access with minimal overhead)
    static inline std::atomic<bool> runtime_enabled_{true};

    std::atomic<bool> initialized_{false};
    MetricsConfig config_;
    // Port the HTTP server actually bound to, 0 until a successful bind. Kept
    // separate from config_.http_port and atomic because httpPort() may be read
    // by other threads while the initializing thread is still binding a port.
    std::atomic<uint16_t> bound_http_port_{0};

#if TENT_METRICS_ENABLED
    // Initialize and start the HTTP server on the configured port. Port
    // assignment is deterministic: co-located ranks are expected to be given
    // distinct ports explicitly (e.g. base_port + local_rank) rather than
    // auto-scanned. Returns an error if the port cannot be bound (the caller
    // then degrades to log-only metrics); on success bound_http_port_ is set.
    Status initHttpServer();

    // Register the /metrics, /metrics/summary, /metrics/json and /health
    // endpoints on the current http_server_ instance.
    void registerHttpHandlers();

    // HTTP server for metrics endpoint
    std::unique_ptr<coro_http::coro_http_server> http_server_;

    // Periodic metric reporting thread
    std::thread metric_report_thread_;
    std::atomic<bool> metric_report_running_{false};
    std::mutex metric_report_mutex_;
    std::condition_variable metric_report_cv_;

    // Counters — stored as base pointers (metric_t*) so that counters with
    // different label arities (N=1 for per-transport, N=2 for failover
    // from→to) share one vector for Prometheus serialize(). The concrete
    // typed members below are used directly for JSON/summary aggregation
    // (which need to iterate label values via copy()).
    std::vector<ylt::metric::metric_t*> counters_;

    // Label name arrays for dynamic metric construction.
    static inline const std::array<std::string, 1> kTransportLabel{"transport"};
    static inline const std::array<std::string, 2> kFailoverLabels{"from",
                                                                   "to"};

    // Per-transport counters (label: transport). Values are int64_t.
    ylt::metric::basic_dynamic_counter<int64_t, 1> read_bytes_total_{
        "tent_read_bytes_total", "Total bytes read via TENT", kTransportLabel};
    ylt::metric::basic_dynamic_counter<int64_t, 1> write_bytes_total_{
        "tent_write_bytes_total", "Total bytes written via TENT",
        kTransportLabel};
    ylt::metric::basic_dynamic_counter<int64_t, 1> read_requests_total_{
        "tent_read_requests_total", "Total read requests via TENT",
        kTransportLabel};
    ylt::metric::basic_dynamic_counter<int64_t, 1> write_requests_total_{
        "tent_write_requests_total", "Total write requests via TENT",
        kTransportLabel};
    ylt::metric::basic_dynamic_counter<int64_t, 1> read_failures_total_{
        "tent_read_failures_total", "Total read failures via TENT",
        kTransportLabel};
    ylt::metric::basic_dynamic_counter<int64_t, 1> write_failures_total_{
        "tent_write_failures_total", "Total write failures via TENT",
        kTransportLabel};
    // Failover counter has two labels (from, to) so failover flows are
    // queryable as e.g. tent_transport_failover_total{from="rdma",to="tcp"}.
    ylt::metric::basic_dynamic_counter<int64_t, 2> failover_total_{
        "tent_transport_failover_total",
        "Total cross-transport failover events", kFailoverLabels};
    ylt::metric::basic_dynamic_counter<int64_t, 1> deadline_infeasible_total_{
        "tent_deadline_infeasible_total",
        "Transfers whose deadline was already in the past at submit",
        kTransportLabel};

    // Pre-constructed label value lookup table, indexed by TransportType enum
    // (see tent/common/types.h:46). Keeps label values in a closed set — no
    // arbitrary strings can reach the metrics. Defined in tent_metrics.cpp.
    static const std::array<std::string, kNumTransportTypes>
        kTransportLabelNames;

    // Bounds-checked lookup of the label string for a TransportType.
    // Returns "unknown" if tp is out of range (defensive — should not happen
    // with the closed-set enum, but guards against memory corruption or a
    // new transport type added without updating the table).
    static const std::string& transportLabel(TransportType tp) {
        if (tp < 0 || tp >= kNumTransportTypes) {
            static const std::string kUnknown = "unknown";
            return kUnknown;
        }
        return kTransportLabelNames[tp];
    }

    // Histograms - paired with their bucket boundaries in a single vector so
    // the two cannot drift out of sync (ylt histogram doesn't expose its
    // boundaries publicly, so we hold them alongside the pointer).
    struct HistogramEntry {
        ylt::metric::basic_dynamic_histogram<int64_t, 1>* h;
        const std::vector<double>* boundaries;
    };
    std::vector<HistogramEntry> histograms_;

    // Latency histograms use microseconds (us) as unit
    // Default buckets: 100us, 500us, 1ms, 5ms, 10ms, 50ms, 100ms, 500ms, 1s
    static inline const std::vector<double> kLatencyBuckets{
        100, 500, 1000, 5000, 10000, 50000, 100000, 500000, 1000000};
    ylt::metric::basic_dynamic_histogram<int64_t, 1> read_latency_{
        "tent_read_latency_us", "Read latency distribution in microseconds",
        kLatencyBuckets, kTransportLabel};
    ylt::metric::basic_dynamic_histogram<int64_t, 1> write_latency_{
        "tent_write_latency_us", "Write latency distribution in microseconds",
        kLatencyBuckets, kTransportLabel};
    // Size histograms for request size distribution (in bytes)
    // Default buckets: 1KB, 4KB, 16KB, 64KB, 256KB, 1MB, 4MB, 16MB, 64MB,
    // 256MB, 1GB
    static inline const std::vector<double> kSizeBuckets{
        1024,    4096,     16384,    65536,     262144,    1048576,
        4194304, 16777216, 67108864, 268435456, 1073741824};
    ylt::metric::basic_dynamic_histogram<int64_t, 1> read_size_{
        "tent_read_size_bytes", "Read request size distribution in bytes",
        kSizeBuckets, kTransportLabel};
    ylt::metric::basic_dynamic_histogram<int64_t, 1> write_size_{
        "tent_write_size_bytes", "Write request size distribution in bytes",
        kSizeBuckets, kTransportLabel};

    // Deadline feasibility ratio (MLU) distribution for transfers that carried
    // a deadline. Stored in per-mille (MLU x 1000) so the histogram can use
    // integer observe() like the others; the 1000 boundary is MLU == 1.0, the
    // feasible/infeasible line (< 1000 met the deadline, >= 1000 missed it).
    static inline const std::vector<double> kMluPerMilleBuckets{
        100, 250, 500, 750, 900, 1000, 1250, 1500, 2000, 5000};
    ylt::metric::basic_dynamic_histogram<int64_t, 1> deadline_mlu_{
        "tent_deadline_mlu_permille",
        "Deadline feasibility ratio (MLU x 1000) distribution",
        kMluPerMilleBuckets, kTransportLabel};

    // Causal chain stage latency histograms (microseconds)
    // Buckets span 10us to 500ms to capture both fast RDMA and slower TCP.
    static inline const std::vector<double> kStageBuckets{
        10, 50, 100, 500, 1000, 5000, 10000, 50000, 100000, 500000};
    ylt::metric::basic_dynamic_histogram<int64_t, 1> stage_queue_wait_{
        "tent_stage_queue_wait_us",
        "Causal chain: queue wait latency in microseconds", kStageBuckets,
        kTransportLabel};
    ylt::metric::basic_dynamic_histogram<int64_t, 1> stage_dispatch_{
        "tent_stage_dispatch_us",
        "Causal chain: dispatch latency in microseconds", kStageBuckets,
        kTransportLabel};
    ylt::metric::basic_dynamic_histogram<int64_t, 1> stage_transport_{
        "tent_stage_transport_us",
        "Causal chain: transport execution latency in microseconds",
        kStageBuckets, kTransportLabel};

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

    ScopedLatencyRecorder(OperationType type, TransportType tp, size_t bytes)
        : type_(type),
          tp_(tp),
          bytes_(bytes),
          enabled_(TentMetrics::isEnabled()) {
        if (enabled_) {
            start_ = std::chrono::steady_clock::now();
        }
    }

    ~ScopedLatencyRecorder() {
        if (!enabled_ || failed_) return;
        auto end = std::chrono::steady_clock::now();
        double latency = std::chrono::duration<double>(end - start_).count();
        if (type_ == OperationType::Read) {
            TentMetrics::instance().recordReadCompleted(tp_, bytes_, latency);
        } else {
            TentMetrics::instance().recordWriteCompleted(tp_, bytes_, latency);
        }
    }

    void markFailed() {
        if (!enabled_) return;
        failed_ = true;
        if (type_ == OperationType::Read) {
            TentMetrics::instance().recordReadFailed(tp_);
        } else {
            TentMetrics::instance().recordWriteFailed(tp_);
        }
    }

   private:
    OperationType type_;
    TransportType tp_;
    size_t bytes_;
    std::chrono::steady_clock::time_point start_;
    bool enabled_;
    bool failed_ = false;
};

// Convenience macros for recording metrics (enabled version).
// The TransportType argument labels each metric so Prometheus queries can
// break down traffic by transport. For failover, from→to labels the flow.
#define TENT_RECORD_READ_COMPLETED(tp, bytes, latency)                     \
    do {                                                                   \
        if (::mooncake::tent::TentMetrics::isEnabled()) {                  \
            ::mooncake::tent::TentMetrics::instance().recordReadCompleted( \
                tp, bytes, latency);                                       \
        }                                                                  \
    } while (0)

#define TENT_RECORD_WRITE_COMPLETED(tp, bytes, latency)                     \
    do {                                                                    \
        if (::mooncake::tent::TentMetrics::isEnabled()) {                   \
            ::mooncake::tent::TentMetrics::instance().recordWriteCompleted( \
                tp, bytes, latency);                                        \
        }                                                                   \
    } while (0)

#define TENT_RECORD_READ_FAILED(tp)                                         \
    do {                                                                    \
        if (::mooncake::tent::TentMetrics::isEnabled()) {                   \
            ::mooncake::tent::TentMetrics::instance().recordReadFailed(tp); \
        }                                                                   \
    } while (0)

#define TENT_RECORD_WRITE_FAILED(tp)                                         \
    do {                                                                     \
        if (::mooncake::tent::TentMetrics::isEnabled()) {                    \
            ::mooncake::tent::TentMetrics::instance().recordWriteFailed(tp); \
        }                                                                    \
    } while (0)

#define TENT_RECORD_TRANSPORT_FAILOVER(from, to)                               \
    do {                                                                       \
        if (::mooncake::tent::TentMetrics::isEnabled()) {                      \
            ::mooncake::tent::TentMetrics::instance().recordTransportFailover( \
                from, to);                                                     \
        }                                                                      \
    } while (0)

#define TENT_SCOPED_READ_LATENCY(tp, bytes)                               \
    ::mooncake::tent::ScopedLatencyRecorder _tent_latency_recorder_(      \
        ::mooncake::tent::ScopedLatencyRecorder::OperationType::Read, tp, \
        bytes)

#define TENT_SCOPED_WRITE_LATENCY(tp, bytes)                               \
    ::mooncake::tent::ScopedLatencyRecorder _tent_latency_recorder_(       \
        ::mooncake::tent::ScopedLatencyRecorder::OperationType::Write, tp, \
        bytes)

#define TENT_RECORD_STAGE_LATENCY(stage, tp, latency_us)                  \
    do {                                                                  \
        if (::mooncake::tent::TentMetrics::isEnabled()) {                 \
            ::mooncake::tent::TentMetrics::instance().recordStageLatency( \
                stage, tp, latency_us);                                   \
        }                                                                 \
    } while (0)

#else  // !TENT_METRICS_ENABLED

// No-op stub class for ScopedLatencyRecorder when metrics are disabled
class ScopedLatencyRecorder {
   public:
    enum class OperationType { Read, Write };
    ScopedLatencyRecorder(OperationType, TransportType, size_t) {}
    void markFailed() {}
};

// Zero-overhead macros when metrics are disabled at compile time
#define TENT_RECORD_READ_COMPLETED(tp, bytes, latency) ((void)0)
#define TENT_RECORD_WRITE_COMPLETED(tp, bytes, latency) ((void)0)
#define TENT_RECORD_READ_FAILED(tp) ((void)0)
#define TENT_RECORD_WRITE_FAILED(tp) ((void)0)
#define TENT_RECORD_TRANSPORT_FAILOVER(from, to) ((void)0)
#define TENT_SCOPED_READ_LATENCY(tp, bytes) ((void)0)
#define TENT_SCOPED_WRITE_LATENCY(tp, bytes) ((void)0)
#define TENT_RECORD_STAGE_LATENCY(stage, tp, latency_us) ((void)0)

#endif  // TENT_METRICS_ENABLED

}  // namespace mooncake::tent
