#include "client_metric.h"

#include <glog/logging.h>
#include <algorithm>
#include <cctype>
#include <chrono>
#include <cstdlib>
#include <thread>

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

ClientMetric::ClientMetric(uint64_t interval_seconds)
    : should_stop_metrics_thread_(false),
      metrics_interval_seconds_(interval_seconds) {
    if (metrics_interval_seconds_ > 0) {
        StartMetricsReportingThread();
    }
}

ClientMetric::~ClientMetric() { StopMetricsReportingThread(); }

std::unique_ptr<ClientMetric> ClientMetric::Create() {
    if (!parseMetricsEnabled()) {
        LOG(INFO) << "Client metrics disabled (set MC_STORE_CLIENT_METRIC=0 to "
                     "disable)";
        return nullptr;
    }

    uint64_t interval = parseMetricsInterval();

    LOG(INFO) << "Client metrics enabled (default enabled)";

    return std::make_unique<ClientMetric>(interval);
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