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

bool parseBoolEnv(const char* env_name, bool default_value) {
    const char* env_value = std::getenv(env_name);
    if (!env_value) {
        return default_value;
    }

    std::string value = toLower(env_value);
    if (value == "1" || value == "true" || value == "yes" || value == "on" ||
        value == "enable") {
        return true;
    }
    if (value == "0" || value == "false" || value == "no" || value == "off" ||
        value == "disable") {
        return false;
    }

    LOG(WARNING) << "Failed to parse " << env_name << ": " << env_value
                 << ", fallback to default=" << default_value;
    return default_value;
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

ClientMetric::ClientMetric(uint64_t interval_seconds,
                           const std::map<std::string, std::string>& labels,
                           bool bandwidth_reporting_enabled,
                           bool master_rpc_metrics_enabled)
    : transfer_metric(labels),
      master_client_metric(labels),
      transfer_operation_metric(labels),
      ssd_metric(labels),
      should_stop_metrics_thread_(false),
      metrics_interval_seconds_(interval_seconds),
      bandwidth_reporting_enabled_(bandwidth_reporting_enabled),
      master_rpc_metrics_enabled_(master_rpc_metrics_enabled) {
    last_report_snapshot_ = TransferSnapshot{
        static_cast<uint64_t>(transfer_metric.total_read_bytes.value()),
        static_cast<uint64_t>(transfer_metric.total_write_bytes.value()),
        std::chrono::steady_clock::now()};
    if (metrics_interval_seconds_ > 0) {
        StartMetricsReportingThread();
    }
}

ClientMetric::~ClientMetric() { StopMetricsReportingThread(); }

std::unique_ptr<ClientMetric> ClientMetric::Create(
    const std::map<std::string, std::string>& labels,
    bool master_rpc_metrics_enabled) {
    if (!parseMetricsEnabled()) {
        LOG(INFO) << "Client metrics disabled (set MC_STORE_CLIENT_METRIC=0 to "
                     "disable)";
        return nullptr;
    }

    uint64_t interval = parseMetricsInterval();
    bool bandwidth_reporting_enabled =
        parseBoolEnv("MC_STORE_CLIENT_METRIC_BANDWIDTH", true);

    LOG(INFO) << "Client metrics enabled (default enabled)";
    LOG(INFO) << "Client bandwidth summary "
              << (bandwidth_reporting_enabled ? "enabled" : "disabled")
              << " via MC_STORE_CLIENT_METRIC_BANDWIDTH";

    return std::make_unique<ClientMetric>(interval, labels,
                                          bandwidth_reporting_enabled,
                                          master_rpc_metrics_enabled);
}

void ClientMetric::serialize(std::string& str) {
    transfer_metric.serialize(str);
    if (master_rpc_metrics_enabled_) {
        master_client_metric.serialize(str);
    }
    transfer_operation_metric.serialize(str);
    ssd_metric.serialize(str);
}

std::string ClientMetric::summary_metrics() {
    std::stringstream ss;
    ss << "Client Metrics Summary\n";
    ss << transfer_metric.summary_metrics(bandwidth_reporting_enabled_);
    ss << "\n";
    if (master_rpc_metrics_enabled_) {
        ss << master_client_metric.summary_metrics();
        ss << "\n";
    }
    ss << transfer_operation_metric.summary_metrics();
    ss << "\n";
    ss << ssd_metric.summary_metrics();
    return ss.str();
}

std::string ClientMetric::BuildBandwidthReport() {
    if (!bandwidth_reporting_enabled_) {
        return "";
    }

    const auto now = std::chrono::steady_clock::now();
    const uint64_t read_bytes = transfer_metric.total_read_bytes.value();
    const uint64_t write_bytes = transfer_metric.total_write_bytes.value();

    std::lock_guard<std::mutex> lock(snapshot_mutex_);
    if (!last_report_snapshot_.has_value()) {
        last_report_snapshot_ = TransferSnapshot{read_bytes, write_bytes, now};
        return "";
    }

    const auto previous = *last_report_snapshot_;
    last_report_snapshot_ = TransferSnapshot{read_bytes, write_bytes, now};

    const double elapsed_seconds = std::max(
        std::chrono::duration<double>(now - previous.timestamp).count(), 1e-9);
    const uint64_t read_delta = read_bytes >= previous.read_bytes
                                    ? read_bytes - previous.read_bytes
                                    : 0;
    const uint64_t write_delta = write_bytes >= previous.write_bytes
                                     ? write_bytes - previous.write_bytes
                                     : 0;

    std::stringstream ss;
    ss << "=== Interval Throughput Summary ===\n";
    ss << "Read Throughput: "
       << format_metric_rate(read_delta / elapsed_seconds, "B/s") << " ("
       << byte_size_to_string(read_delta) << " over " << std::fixed
       << std::setprecision(2) << elapsed_seconds << "s)\n";
    ss << "Write Throughput: "
       << format_metric_rate(write_delta / elapsed_seconds, "B/s") << " ("
       << byte_size_to_string(write_delta) << " over " << std::fixed
       << std::setprecision(2) << elapsed_seconds << "s)";
    return ss.str();
}

void ClientMetric::StartMetricsReportingThread() {
    should_stop_metrics_thread_ = false;
    metrics_reporting_thread_ =
        std::jthread([this](const std::stop_token& stop_token) {
            LOG(INFO) << "Client metrics reporting thread started (interval: "
                      << metrics_interval_seconds_ << "s)";

            while (!stop_token.stop_requested() &&
                   !should_stop_metrics_thread_) {
                // Sleep for the interval, checking periodically for stop signal
                for (uint64_t i = 0; i < metrics_interval_seconds_ &&
                                     !stop_token.stop_requested() &&
                                     !should_stop_metrics_thread_;
                     ++i) {
                    std::this_thread::sleep_for(std::chrono::seconds(1));
                }

                if (stop_token.stop_requested() ||
                    should_stop_metrics_thread_) {
                    break;  // Exit if stopped during sleep
                }

                // Print metrics summary
                std::string summary = summary_metrics();
                std::string bandwidth_report = BuildBandwidthReport();
                std::string report = "Client Metrics Report:\n" + summary;
                if (!bandwidth_report.empty()) {
                    report += "\n" + bandwidth_report;
                }
                LOG(INFO) << report;
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
