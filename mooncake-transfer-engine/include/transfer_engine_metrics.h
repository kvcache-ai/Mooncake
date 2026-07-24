// Copyright 2024 KVCache.AI
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

#ifndef TRANSFER_ENGINE_METRICS_H_
#define TRANSFER_ENGINE_METRICS_H_

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

// Standard Prometheus-style metrics for the Classic Transfer Engine.
//
// This mirrors the naming and behaviour of the TENT metrics implementation
// (see mooncake-transfer-engine/tent/.../metrics/tent_metrics.h) as closely as
// practical, while staying independent so that the two implementations can be
// unified in a later change.
//
// The whole system is compiled out when WITH_METRICS is not defined, matching
// the existing Classic TE metrics logging code.

#ifdef WITH_METRICS
#include <csignal>  // Required before ylt headers: coro_io.hpp uses SIGPIPE
#include <ylt/metric/counter.hpp>
#include <ylt/metric/gauge.hpp>
#include <ylt/metric/histogram.hpp>
#endif

#ifdef WITH_METRICS
// Forward-declare the coro_http server so this header stays lightweight; the
// full type is only needed in the .cpp. ylt exposes the server as cinatra with
// a `coro_http` namespace alias (see ylt/coro_http/coro_http_server.hpp).
namespace cinatra {
class coro_http_server;
}  // namespace cinatra
namespace coro_http = cinatra;
#endif

namespace mooncake {

/**
 * @brief Standard metrics for the Classic Transfer Engine.
 *
 * Provides Prometheus-compatible counters, histograms and an inflight gauge
 * for transfer submission / completion / failure, plus an optional HTTP server
 * exposing the same endpoints as TENT (/metrics, /metrics/summary,
 * /metrics/json, /health).
 *
 * Design notes:
 * - Collection is cheap (a handful of atomics) and always active once
 *   initialized; it can be turned off at runtime via setEnabled(false).
 * - The HTTP server is opt-in: it is only started when a port is configured
 *   (env MC_TE_METRIC_HTTP_PORT), so default behaviour is unchanged.
 * - Every transfer must be recorded exactly once. Callers are responsible for
 *   invoking recordSubmitted() once per task at submission and exactly one of
 *   recordCompleted()/recordFailed() once per task when it reaches a terminal
 *   state. The Transfer Engine guards this with the per-task start_time reset.
 */
class TransferEngineMetrics {
   public:
    static TransferEngineMetrics& instance();

    // Configuration for the (optional) metrics HTTP server. Populated from
    // environment variables by loadConfigFromEnv().
    struct Config {
        bool enabled = true;
        // http_port == 0 means "do not start the HTTP server".
        uint16_t http_port = 0;
        std::string http_host = "0.0.0.0";
        uint16_t http_server_threads = 1;
    };

    // Build a Config from environment variables:
    //   MC_TE_METRIC_HTTP_PORT     (uint16, 0/unset disables the HTTP server)
    //   MC_TE_METRIC_HTTP_HOST     (string, default 0.0.0.0)
    //   MC_TE_METRIC_HTTP_THREADS  (uint16, default 1)
    static Config loadConfigFromEnv();

    // Initialize metrics collection and, if a port is configured, start the
    // HTTP server. Safe to call multiple times; only the first call has effect.
    void initialize(const Config& config);

    // Stop the HTTP server and mark the system uninitialized.
    void shutdown();

    bool isInitialized() const {
        return initialized_.load(std::memory_order_acquire);
    }

    // Runtime enable/disable switch. When disabled, record* functions return
    // after a single atomic load.
    static void setEnabled(bool enabled) {
        runtime_enabled_.store(enabled, std::memory_order_relaxed);
    }
    static bool isEnabled() {
        return runtime_enabled_.load(std::memory_order_relaxed);
    }

    // Record a transfer task that has just been submitted. Increments the
    // request counter and the inflight gauge.
    void recordSubmitted();

    // Record a transfer task that completed successfully. Records bytes,
    // request size, latency and decrements the inflight gauge.
    void recordCompleted(size_t bytes, double latency_seconds);

    // Record a transfer task that failed / was canceled / timed out. Increments
    // the failure counter and decrements the inflight gauge.
    void recordFailed();

    // Serialization for the HTTP endpoints.
    std::string getPrometheusMetrics();
    std::string getJsonMetrics();
    std::string getSummaryString();

#ifdef WITH_METRICS
    // Accessors used by the HTTP server and by unit tests.
    double requestsTotal();
    double bytesTotal();
    double failuresTotal();
    int64_t inflightTransfers();
    int64_t latencySampleCount();
    int64_t sizeSampleCount();

    // Reset all metrics to zero. Intended for test isolation only; the metrics
    // singleton outlives individual Transfer Engine instances.
    void resetForTesting();
#endif

   private:
    TransferEngineMetrics();
    ~TransferEngineMetrics();
    TransferEngineMetrics(const TransferEngineMetrics&) = delete;
    TransferEngineMetrics& operator=(const TransferEngineMetrics&) = delete;

    static inline std::atomic<bool> runtime_enabled_{true};
    std::atomic<bool> initialized_{false};

#ifdef WITH_METRICS
    void initHttpServer(const Config& config);

    // ylt histograms keep their _sum in an int64 gauge, which truncates
    // sub-second latencies to zero and suppresses serialization. We therefore
    // track the latency sum ourselves (in seconds) and serialize the latency
    // histogram manually so it is always emitted with a correct _sum.
    void serializeLatencyHistogram(std::string& out);

    Config config_;
    std::unique_ptr<coro_http::coro_http_server> http_server_;

    std::atomic<double> latency_sum_seconds_{0.0};

    // Latency buckets are expressed in seconds so the exported metric follows
    // the Prometheus convention of a *_seconds histogram.
    static inline const std::vector<double> kLatencyBucketsSeconds{
        0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0};
    // Size buckets in bytes: 1KB .. 1GB (matches TENT).
    static inline const std::vector<double> kSizeBuckets{
        1024,    4096,     16384,    65536,     262144,    1048576,
        4194304, 16777216, 67108864, 268435456, 1073741824};

    ylt::metric::counter_t requests_total_{"transfer_requests_total",
                                           "Total transfer requests submitted"};
    ylt::metric::counter_t bytes_total_{"transfer_bytes_total",
                                        "Total bytes transferred"};
    ylt::metric::counter_t failures_total_{"transfer_failures_total",
                                           "Total transfer failures"};
    ylt::metric::gauge_t inflight_transfers_{
        "inflight_transfers", "Number of transfers currently in flight"};
    ylt::metric::histogram_d latency_seconds_{
        "transfer_latency_seconds", "Transfer completion latency in seconds",
        kLatencyBucketsSeconds};
    ylt::metric::histogram_t size_bytes_{
        "transfer_size_bytes", "Transfer request size in bytes", kSizeBuckets};
#endif
};

}  // namespace mooncake

#endif  // TRANSFER_ENGINE_METRICS_H_
