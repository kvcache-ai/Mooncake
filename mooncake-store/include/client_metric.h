#pragma once

#include <algorithm>
#include <atomic>
#include <sstream>
#include <thread>
#include <vector>
#include <glog/logging.h>
#include <ylt/metric/counter.hpp>
#include <ylt/metric/histogram.hpp>
#include <ylt/metric/summary.hpp>
#include "utils.h"
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

template <typename BucketValueFn>
std::string format_latency_summary_from_buckets(
    size_t bucket_count, BucketValueFn&& bucket_value,
    const std::string& count_key = "count") {
    int64_t total_count = 0;
    for (size_t i = 0; i < bucket_count; ++i) {
        total_count += bucket_value(i);
    }
    if (bucket_count == 0 || total_count == 0) {
        return "No data";
    }

    std::stringstream ss;
    ss << count_key << "=" << total_count;

    // ceil() so that even a single sample lands in a real bucket instead of
    // matching an empty bucket with target == 0.
    int64_t p95_target = std::max<int64_t>(1, (total_count * 95) / 100);
    int64_t cumulative = 0;
    double p95_bucket = 0;
    for (size_t i = 0; i < bucket_count && i < kLatencyBucket.size(); ++i) {
        cumulative += bucket_value(i);
        if (cumulative >= p95_target) {
            p95_bucket = kLatencyBucket[i];
            break;
        }
    }
    if (p95_bucket > 0) {
        ss << ", p95<" << p95_bucket << "μs";
    }

    // Max bucket: highest bucket boundary that received at least one sample.
    double max_bucket = 0;
    for (size_t i = std::min(bucket_count, kLatencyBucket.size()); i > 0; --i) {
        if (bucket_value(i - 1) > 0) {
            max_bucket = kLatencyBucket[i - 1];
            break;
        }
    }
    if (max_bucket > 0) {
        ss << ", max<" << max_bucket << "μs";
    }
    return ss.str();
}

// Format histogram summary: count, p95, max.
inline std::string format_latency_summary(ylt::metric::histogram_t& hist) {
    auto counts = hist.get_bucket_counts();
    return format_latency_summary_from_buckets(
        counts.size(), [&](size_t i) { return counts[i]->value(); });
}

// Simple stopwatch for measuring elapsed time in microseconds
class Stopwatch {
   public:
    Stopwatch() : start_time_(std::chrono::steady_clock::now()) {}

    int64_t elapsed_us() const {
        auto now = std::chrono::steady_clock::now();
        return std::chrono::duration_cast<std::chrono::microseconds>(
                   now - start_time_)
            .count();
    }

   private:
    std::chrono::steady_clock::time_point start_time_;
};

static inline std::string get_env_or_default(
    const char* env_var, const std::string& default_val = "") {
    const char* val = getenv(env_var);
    return val ? val : default_val;
}

// In production mode, more labels are needed for monitoring and troubleshooting
// Static labels include but are not limited to machine address, cluster name,
// etc. These labels remain constant during the lifetime of the application
const std::string kClusterID = get_env_or_default("MC_STORE_CLUSTER_ID");

// Merge static labels with dynamic labels
const inline std::map<std::string, std::string> merge_labels(
    const std::map<std::string, std::string>& labels) {
    std::map<std::string, std::string> merged_labels;
    if (!kClusterID.empty()) {
        merged_labels["cluster_id"] = kClusterID;
    }
    merged_labels.insert(labels.begin(), labels.end());
    return merged_labels;
}

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
     * @brief Creates a ClientMetric instance (metric collection objects).
     * @return std::unique_ptr<ClientMetric>
     *
     */
    static std::unique_ptr<ClientMetric> Create(
        const std::map<std::string, std::string>& labels = {}) {
        return CreatePtr<ClientMetric>(labels);
    }

    virtual void serialize(std::string& str);
    virtual std::string summary_metrics();

    uint64_t GetReportingInterval() const { return metrics_interval_seconds_; }

    /**
     * @brief Starting the periodic reporting interval after construction.
     */
    void StartMetricReporting(uint64_t interval_seconds);

    explicit ClientMetric(
        uint64_t interval_seconds = 0,
        const std::map<std::string, std::string>& labels = {});
    virtual ~ClientMetric();

   protected:
    /**
     * @brief Template helper for creating metric instances.
     * Used by Create() in base and derived classes.
     */
    template <typename T>
    static std::unique_ptr<T> CreatePtr(
        const std::map<std::string, std::string>& labels) {
        return std::make_unique<T>(0, merge_labels(labels));
    }

   private:
    // Metrics reporting thread management
    std::jthread metrics_reporting_thread_;
    std::atomic<bool> should_stop_metrics_thread_{false};
    uint64_t metrics_interval_seconds_{0};

    void StartMetricsReportingThread();
    void StopMetricsReportingThread();
};
};  // namespace mooncake
