#include "client_metric.h"

#include <glog/logging.h>
#include <algorithm>
#include <cctype>
#include <chrono>
#include <cstdlib>
#include <thread>

namespace mooncake {

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

std::string MasterClientMetric::summary_metrics() {
    std::stringstream ss;
    ss << "=== RPC Metrics Summary ===\n";

    if (rpc_count.label_value_count() == 0) {
        ss << "No RPC calls recorded\n";
        return ss.str();
    }

    // Dynamically iterate all recorded RPC names from rpc_count.
    // rpc_latency only observes successful calls, so its bucket counts
    // provide both the success count and the success latency distribution.
    auto count_map = rpc_count.copy();
    auto bucket_counts = rpc_latency.get_bucket_counts();
    bool found_any = false;

    for (auto& entry : count_map) {
        const auto& rpc_name = entry->label[0];
        std::array<std::string, 1> label_array = {rpc_name};

        int64_t total_calls = entry->value.load(std::memory_order::relaxed);
        if (total_calls == 0) continue;
        found_any = true;

        auto success_summary = format_latency_summary_from_buckets(
            bucket_counts.size(),
            [&](size_t i) { return bucket_counts[i]->value(label_array); },
            "success");
        if (success_summary == "No data") {
            ss << rpc_name << ": total=" << total_calls << ", success=0\n";
        } else {
            ss << rpc_name << ": total=" << total_calls << ", "
               << success_summary << "\n";
        }
    }

    if (!found_any) {
        ss << "No RPC calls recorded\n";
    }

    return ss.str();
}

ClientMetric::ClientMetric(uint64_t interval_seconds,
                           const std::map<std::string, std::string>& labels)
    : transfer_metric(labels),
      master_client_metric(labels),
      should_stop_metrics_thread_(false),
      metrics_interval_seconds_(interval_seconds) {
    if (metrics_interval_seconds_ > 0) {
        StartMetricsReportingThread();
    }
}

ClientMetric::~ClientMetric() { StopMetricsReportingThread(); }

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

void ClientMetric::StartMetricReporting(uint64_t interval_seconds) {
    StopMetricsReportingThread();
    metrics_interval_seconds_ = interval_seconds;
    if (metrics_interval_seconds_ > 0) {
        StartMetricsReportingThread();
    }
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
