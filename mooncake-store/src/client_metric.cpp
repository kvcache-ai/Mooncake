#include "client_metric.h"
#include "utils.h"

#include <glog/logging.h>
#include <algorithm>
#include <cctype>
#include <chrono>
#include <cstdlib>
#include <thread>
#include <sstream>

namespace mooncake {

namespace {

std::string toLower(const std::string& str) {
    std::string result = str;
    std::transform(result.begin(), result.end(), result.begin(),
                   [](unsigned char c) { return std::tolower(c); });
    return result;
}

bool parseMetricsEnabled() {
    const char* metric_env = std::getenv("MC_STORE_CLIENT_METRIC");
    if (!metric_env) {
        return true;
    }
    std::string value = toLower(metric_env);
    return (value == "1" || value == "true" || value == "yes" ||
            value == "on" || value == "enable");
}

uint64_t parseMetricsInterval() {
    const char* interval_env = std::getenv("MC_STORE_CLIENT_METRIC_INTERVAL");
    if (!interval_env) {
        // Default to disabled
        return 0;
    }

    try {
        uint64_t interval = std::stoull(interval_env);
        if (interval == 0) {
            LOG(INFO) << "Client metrics reporting disabled (interval=0) via "
                         "MC_STORE_CLIENT_METRIC_INTERVAL";
        } else {
            LOG(INFO) << "Client metrics interval set to " << interval
                      << "s via MC_STORE_CLIENT_METRIC_INTERVAL";
        }
        return interval;
    } catch (const std::exception& e) {
        LOG(WARNING) << "Failed to parse MC_STORE_CLIENT_METRIC_INTERVAL: "
                     << interval_env << ", disabling metrics reporting";
        return 0;
    }
}

}  // anonymous namespace

std::string TransferMetric::summary_metrics() {
    std::stringstream ss;
    ss << "=== Transfer Metrics Summary ===\n";

    // Bytes transferred
    auto read_bytes = total_read_bytes.value();
    auto write_bytes = total_write_bytes.value();
    ss << "Total Read: " << byte_size_to_string(read_bytes) << "\n";
    ss << "Total Write: " << byte_size_to_string(write_bytes) << "\n";

    // Latency summaries
    ss << "\n=== Latency Summary (microseconds) ===\n";
    ss << "Get: " << format_latency_summary(get_latency_us) << "\n";
    ss << "Put: " << format_latency_summary(put_latency_us) << "\n";
    ss << "Batch Get: " << format_latency_summary(batch_get_latency_us) << "\n";
    ss << "Batch Put: " << format_latency_summary(batch_put_latency_us) << "\n";

    return ss.str();
}

std::string TransferMetric::format_latency_summary(
    ylt::metric::histogram_t& hist) {
    // Access the internal sum and bucket counts
    auto sum_ptr =
        const_cast<ylt::metric::histogram_t&>(hist).get_bucket_counts();
    if (sum_ptr.empty()) {
        return "No data";
    }

    // Calculate total count from all buckets
    int64_t total_count = 0;
    for (auto& bucket : sum_ptr) {
        total_count += bucket->value();
    }

    if (total_count == 0) {
        return "No data";
    }

    // Get sum from the histogram's internal sum gauge
    // Note: We need to access the private sum_ member, which requires
    // friendship or reflection For now, let's use a simpler approach
    // showing just count
    std::stringstream ss;
    ss << "count=" << total_count;

    // Find P95
    int64_t p95_target = (total_count * 95) / 100;
    int64_t cumulative = 0;
    double p95_bucket = 0;

    for (size_t i = 0; i < sum_ptr.size() && i < kLatencyBucket.size(); i++) {
        cumulative += sum_ptr[i]->value();
        if (cumulative >= p95_target && p95_bucket == 0) {
            p95_bucket = kLatencyBucket[i];
            break;
        }
    }

    if (p95_bucket > 0) {
        ss << ", p95<" << p95_bucket << "μs";
    }

    // Find max bucket (highest bucket with data)
    double max_bucket = 0;
    for (size_t i = sum_ptr.size(); i > 0; i--) {
        size_t idx = i - 1;
        if (idx < kLatencyBucket.size() && sum_ptr[idx]->value() > 0) {
            max_bucket = kLatencyBucket[idx];
            break;
        }
    }

    if (max_bucket > 0) {
        ss << ", max<" << max_bucket << "μs";
    }

    return ss.str();
}

std::string MasterClientMetric::summary_metrics() {
    std::stringstream ss;
    ss << "=== RPC Metrics Summary ===\n";

    // For dynamic metrics, we need to check if there are any labels with
    // data
    if (rpc_count.label_value_count() == 0) {
        ss << "No RPC calls recorded\n";
        return ss.str();
    }

    // Get all available RPC names from the dynamic metrics
    // We'll iterate through all possible RPC names instead of using a fixed
    // list
    std::vector<std::string> all_rpc_names = {"GetReplicaList",
                                              "PutStart",
                                              "PutEnd",
                                              "PutRevoke",
                                              "ExistKey",
                                              "Remove",
                                              "RemoveAll",
                                              "MountSegment",
                                              "UnmountSegment",
                                              "GetFsdir",
                                              "BatchGetReplicaList",
                                              "BatchPutStart",
                                              "BatchPutEnd",
                                              "BatchPutRevoke",
                                              "MountLocalDiskSegment",
                                              "OffloadObjectHeartbeat",
                                              "NotifyOffloadSuccess"};

    bool found_any = false;
    for (const auto& rpc_name : all_rpc_names) {
        std::array<std::string, 1> label_array = {rpc_name};

        // Check if this RPC has any data by trying to access bucket counts
        auto bucket_counts = rpc_latency.get_bucket_counts();
        int64_t total_count = 0;
        for (auto& bucket : bucket_counts) {
            total_count += bucket->value(label_array);
        }

        // Skip RPCs with zero count
        if (total_count == 0) continue;

        found_any = true;
        ss << rpc_name << ": count=" << total_count;

        // Find P95
        int64_t p95_target = (total_count * 95) / 100;
        int64_t cumulative = 0;
        double p95_bucket = 0;

        for (size_t i = 0;
             i < bucket_counts.size() && i < kLatencyBucket.size(); i++) {
            cumulative += bucket_counts[i]->value(label_array);
            if (cumulative >= p95_target && p95_bucket == 0) {
                p95_bucket = kLatencyBucket[i];
                break;
            }
        }

        if (p95_bucket > 0) {
            ss << ", p95<" << p95_bucket << "μs";
        }

        // Find max bucket (highest bucket with data)
        double max_bucket = 0;
        for (size_t i = bucket_counts.size(); i > 0; i--) {
            size_t idx = i - 1;
            if (idx < kLatencyBucket.size() &&
                bucket_counts[idx]->value(label_array) > 0) {
                max_bucket = kLatencyBucket[idx];
                break;
            }
        }

        if (max_bucket > 0) {
            ss << ", max<" << max_bucket << "μs";
        }

        ss << "\n";
    }

    if (!found_any) {
        ss << "No RPC calls recorded\n";
    }

    return ss.str();
}

ClientMetric::ClientMetric(uint64_t interval_seconds,
                           std::map<std::string, std::string> labels)
    : transfer_metric(labels),
      master_client_metric(labels),
      should_stop_metrics_thread_(false),
      metrics_interval_seconds_(interval_seconds) {
    if (metrics_interval_seconds_ > 0) {
        StartMetricsReportingThread();
    }
}

ClientMetric::~ClientMetric() { StopMetricsReportingThread(); }

std::unique_ptr<ClientMetric> ClientMetric::Create(
    std::map<std::string, std::string> labels) {
    if (!parseMetricsEnabled()) {
        LOG(INFO) << "Client metrics disabled (set MC_STORE_CLIENT_METRIC=0 to "
                     "disable)";
        return nullptr;
    }

    uint64_t interval = parseMetricsInterval();

    LOG(INFO) << "Client metrics enabled (default enabled)";

    return std::make_unique<ClientMetric>(interval, labels);
}

void ClientMetric::serialize(std::string& str) {
    transfer_metric.serialize(str);
    master_client_metric.serialize(str);
}

std::string ClientMetric::summary_metrics() {
    std::stringstream ss;
    ss << "Client Metrics Summary\n";
    ss << transfer_metric.summary_metrics();
    ss << "\n";
    ss << master_client_metric.summary_metrics();
    return ss.str();
}

void ClientMetric::StartMetricsReportingThread() {
    should_stop_metrics_thread_ = false;
    metrics_reporting_thread_ = std::jthread([this](
                                                 std::stop_token stop_token) {
        LOG(INFO) << "Client metrics reporting thread started (interval: "
                  << metrics_interval_seconds_ << "s)";

        while (!stop_token.stop_requested() && !should_stop_metrics_thread_) {
            // Sleep for the interval, checking periodically for stop signal
            for (uint64_t i = 0;
                 i < metrics_interval_seconds_ &&
                 !stop_token.stop_requested() && !should_stop_metrics_thread_;
                 ++i) {
                std::this_thread::sleep_for(std::chrono::seconds(1));
            }

            if (stop_token.stop_requested() || should_stop_metrics_thread_) {
                break;  // Exit if stopped during sleep
            }

            // Print metrics summary
            std::string summary = summary_metrics();
            LOG(INFO) << "Client Metrics Report:\n" << summary;
        }
        LOG(INFO) << "Client metrics reporting thread stopped";
    });
}

void ClientMetric::StopMetricsReportingThread() {
    should_stop_metrics_thread_ = true;  // Signal the thread to stop
    if (metrics_reporting_thread_.joinable()) {
        LOG(INFO) << "Waiting for client metrics reporting thread to join...";
        metrics_reporting_thread_.request_stop();
        metrics_reporting_thread_.join();  // Wait for the thread to finish
        LOG(INFO) << "Client metrics reporting thread joined";
    }
}

}  // namespace mooncake
