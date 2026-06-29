#include "client_metric.h"

#include <glog/logging.h>
#include <algorithm>
#include <cctype>
#include <chrono>
#include <cstdlib>
#include <thread>

namespace mooncake {

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