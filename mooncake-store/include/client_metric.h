#pragma once

#include <array>
#include <atomic>
#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <thread>
#include <vector>
#include <ylt/metric/counter.hpp>
#include <ylt/metric/histogram.hpp>
#include "hybrid_metric.h"

namespace mooncake {

// latency bucket is in microsecond
// Tuned for RDMA: fine-grained in <1ms, with ms-scale tail up to 1s
const std::vector<double> kLatencyBucket = {
    // sub-ms to 1ms region
    125, 150, 200, 250, 300, 400, 500, 750, 1000,
    // ms-level tail for batch/occasional spikes
    1500, 2000, 3000, 5000, 7000, 15000, 20000,
    // safeguards for long tails
    50000, 100000, 200000, 500000, 1000000};

struct TransferMetric {
    TransferMetric(std::map<std::string, std::string> labels = {})
        : total_read_bytes("mooncake_transfer_read_bytes", "Total bytes read",
                           labels),
          total_write_bytes("mooncake_transfer_write_bytes",
                            "Total bytes written", labels),
          batch_put_latency_us("mooncake_transfer_batch_put_latency",
                               "Batch Put transfer latency (us)",
                               kLatencyBucket, labels),
          batch_get_latency_us("mooncake_transfer_batch_get_latency",
                               "Batch Get transfer latency (us)",
                               kLatencyBucket, labels),
          get_latency_us("mooncake_transfer_get_latency",
                         "Get transfer latency (us)", kLatencyBucket, labels),
          put_latency_us("mooncake_transfer_put_latency",
                         "Put transfer latency (us)", kLatencyBucket, labels) {}

    ylt::metric::counter_t total_read_bytes;
    ylt::metric::counter_t total_write_bytes;
    ylt::metric::histogram_t batch_put_latency_us;
    ylt::metric::histogram_t batch_get_latency_us;
    ylt::metric::histogram_t get_latency_us;
    ylt::metric::histogram_t put_latency_us;

    void serialize(std::string& str) {
        total_read_bytes.serialize(str);
        total_write_bytes.serialize(str);
        batch_put_latency_us.serialize(str);
        batch_get_latency_us.serialize(str);
        get_latency_us.serialize(str);
        put_latency_us.serialize(str);
    }

    std::string summary_metrics();

   private:
    std::string format_latency_summary(ylt::metric::histogram_t& hist);
};

struct MasterClientMetric {
    std::array<std::string, 1> rpc_names = {"rpc_name"};

    MasterClientMetric(std::map<std::string, std::string> labels = {})
        : rpc_count("mooncake_client_rpc_count",
                    "Total number of RPC calls made by the client", labels,
                    rpc_names),
          rpc_latency("mooncake_client_rpc_latency",
                      "Latency of RPC calls made by the client (in us)",
                      kLatencyBucket, labels, rpc_names) {}

    ylt::metric::hybrid_counter_1t rpc_count;
    ylt::metric::hybrid_histogram_1t rpc_latency;
    void serialize(std::string& str) {
        rpc_count.serialize(str);
        rpc_latency.serialize(str);
    }

    std::string summary_metrics();
};

struct ClientMetric {
    TransferMetric transfer_metric;
    MasterClientMetric master_client_metric;

    /**
     * @brief Creates a ClientMetric instance based on environment variables
     * @return std::unique_ptr<ClientMetric> containing the instance if enabled,
     *         nullptr if disabled
     *
     * Environment variables:
     * - MC_STORE_CLIENT_METRIC: Enable/disable metrics (enabled by default,
     *   set to 0/false to disable)
     * - MC_STORE_CLIENT_METRIC_INTERVAL: Reporting interval in seconds
     *   (default: 0, 0 = collect but don't report)
     */
    static std::unique_ptr<ClientMetric> Create(
        std::map<std::string, std::string> labels = {});

    void serialize(std::string& str);
    std::string summary_metrics();

    uint64_t GetReportingInterval() const { return metrics_interval_seconds_; }

    explicit ClientMetric(uint64_t interval_seconds = 0,
                          std::map<std::string, std::string> labels = {});
    ~ClientMetric();

   private:
    // Metrics reporting thread management
    std::jthread metrics_reporting_thread_;
    std::atomic<bool> should_stop_metrics_thread_{false};
    uint64_t metrics_interval_seconds_{0};

    void StartMetricsReportingThread();
    void StopMetricsReportingThread();
};
};  // namespace mooncake
