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

#include "tent/metrics/tent_metrics.h"

#include <glog/logging.h>
#include <tent/thirdparty/nlohmann/json.h>
#include <sstream>
#include <iomanip>

namespace mooncake::tent {

TentMetrics& TentMetrics::instance() {
    static TentMetrics instance;
    return instance;
}

TentMetrics::~TentMetrics() { shutdown(); }

#if TENT_METRICS_ENABLED

Status TentMetrics::initialize(const MetricsConfig& config) {
    // Use compare_exchange to prevent race condition during initialization
    bool expected = false;
    if (!initialized_.compare_exchange_strong(expected, true)) {
        return Status::OK();  // Already initialized by another thread
    }

    config_ = config;

    // Set runtime enabled state from config
    runtime_enabled_.store(config_.enabled, std::memory_order_relaxed);

    // Configure histogram buckets if provided (recreate histograms)
    // Note: config latency_buckets are in seconds, convert to microseconds for
    // histogram
    if (!config_.latency_buckets.empty()) {
        // Convert seconds to microseconds for histogram buckets
        std::vector<double> latency_buckets_us;
        latency_buckets_us.reserve(config_.latency_buckets.size());
        for (double bucket_sec : config_.latency_buckets) {
            latency_buckets_us.push_back(bucket_sec *
                                         1000000.0);  // seconds -> microseconds
        }
        read_latency_ = ylt::metric::histogram_t(
            "tent_read_latency_us", "Read latency distribution in microseconds",
            latency_buckets_us);
        write_latency_ = ylt::metric::histogram_t(
            "tent_write_latency_us",
            "Write latency distribution in microseconds", latency_buckets_us);
    }

    // Configure size histogram buckets if provided
    if (!config_.size_buckets.empty()) {
        read_size_ = ylt::metric::histogram_t(
            "tent_read_size_bytes", "Read request size distribution in bytes",
            config_.size_buckets);
        write_size_ = ylt::metric::histogram_t(
            "tent_write_size_bytes", "Write request size distribution in bytes",
            config_.size_buckets);
    }

    // Register all metrics to vectors for unified serialization
    registerMetrics();

    // Initialize and start HTTP server
    initHttpServer();

    // Start periodic metric reporting thread if interval > 0
    if (config_.report_interval_seconds > 0) {
        metric_report_running_ = true;
        metric_report_thread_ = std::thread([this]() {
            while (metric_report_running_) {
                std::string summary = getSummaryString();
                LOG(INFO) << "TENT Metrics: " << summary;

                // Use condition variable for interruptible sleep
                std::unique_lock<std::mutex> lock(metric_report_mutex_);
                metric_report_cv_.wait_for(
                    lock, std::chrono::seconds(config_.report_interval_seconds),
                    [this]() { return !metric_report_running_.load(); });
            }
        });
    }

    LOG(INFO)
        << "TENT metrics initialized successfully, HTTP server listening on "
        << config_.http_host << ":" << config_.http_port
        << ", runtime_enabled=" << (runtime_enabled_.load() ? "true" : "false");
    return Status::OK();
}

void TentMetrics::initHttpServer() {
    using namespace coro_http;

    // Create HTTP server with configurable threads
    http_server_ = std::make_unique<coro_http_server>(
        config_.http_server_threads, config_.http_port);

    // Register /metrics endpoint for Prometheus
    http_server_->set_http_handler<GET>(
        "/metrics", [this](coro_http_request& req, coro_http_response& resp) {
            std::string metrics = getPrometheusMetrics();
            resp.add_header("Content-Type", "text/plain; version=0.0.4");
            resp.set_status_and_content(status_type::ok, std::move(metrics));
        });

    // Register /metrics/summary endpoint for human-readable summary
    http_server_->set_http_handler<GET>(
        "/metrics/summary",
        [this](coro_http_request& req, coro_http_response& resp) {
            std::string summary = getSummaryString();
            resp.add_header("Content-Type", "text/plain");
            resp.set_status_and_content(status_type::ok, std::move(summary));
        });

    // Register /metrics/json endpoint for JSON format
    http_server_->set_http_handler<GET>(
        "/metrics/json",
        [this](coro_http_request& req, coro_http_response& resp) {
            std::string json = getJsonMetrics();
            resp.add_header("Content-Type", "application/json");
            resp.set_status_and_content(status_type::ok, std::move(json));
        });

    // Register /health endpoint for health check
    http_server_->set_http_handler<GET>(
        "/health", [](coro_http_request& req, coro_http_response& resp) {
            resp.add_header("Content-Type", "text/plain");
            resp.set_status_and_content(status_type::ok, "OK");
        });

    // Start the HTTP server asynchronously
    http_server_->async_start();
}

void TentMetrics::shutdown() {
    if (!initialized_) return;

    // Stop metric reporting thread
    metric_report_running_ = false;
    metric_report_cv_.notify_all();  // Wake up the sleeping thread immediately
    if (metric_report_thread_.joinable()) {
        metric_report_thread_.join();
    }

    // Stop HTTP server
    if (http_server_) {
        http_server_->stop();
        http_server_.reset();
    }

    // Clear metric vectors
    counters_.clear();
    histograms_.clear();
    histogram_boundaries_.clear();

    initialized_ = false;
    LOG(INFO) << "TENT metrics shutdown complete";
}

void TentMetrics::registerMetrics() {
    // Pre-allocate vectors to avoid reallocation
    counters_.reserve(6);
    histograms_.reserve(4);
    histogram_boundaries_.reserve(4);

    // Register all counters - add new counters here
    counters_ = {
        &read_bytes_total_,     &write_bytes_total_,   &read_requests_total_,
        &write_requests_total_, &read_failures_total_, &write_failures_total_,
    };

    // Register all histograms - add new histograms here
    // Note: histogram_boundaries_ must match the order of histograms_
    histograms_ = {
        &read_latency_,
        &write_latency_,
        &read_size_,
        &write_size_,
    };
    histogram_boundaries_ = {
        kLatencyBuckets,
        kLatencyBuckets,
        kSizeBuckets,
        kSizeBuckets,
    };
}

void TentMetrics::recordReadCompleted(size_t bytes, double latency_seconds) {
    // Fast path: check runtime switch first
    if (!initialized_ || !runtime_enabled_.load(std::memory_order_relaxed))
        return;

    read_bytes_total_.inc(static_cast<double>(bytes));
    read_requests_total_.inc();
    read_size_.observe(static_cast<int64_t>(bytes));
    if (latency_seconds > 0.0) {
        // Convert seconds to microseconds for histogram (int64_t internally)
        int64_t latency_us = static_cast<int64_t>(latency_seconds * 1000000.0);
        read_latency_.observe(latency_us);
    }
}

void TentMetrics::recordWriteCompleted(size_t bytes, double latency_seconds) {
    // Fast path: check runtime switch first
    if (!initialized_ || !runtime_enabled_.load(std::memory_order_relaxed))
        return;

    write_bytes_total_.inc(static_cast<double>(bytes));
    write_requests_total_.inc();
    write_size_.observe(static_cast<int64_t>(bytes));
    if (latency_seconds > 0.0) {
        // Convert seconds to microseconds for histogram (int64_t internally)
        int64_t latency_us = static_cast<int64_t>(latency_seconds * 1000000.0);
        write_latency_.observe(latency_us);
    }
}

void TentMetrics::recordReadFailed(size_t bytes) {
    // Fast path: check runtime switch first
    if (!initialized_ || !runtime_enabled_.load(std::memory_order_relaxed))
        return;

    read_failures_total_.inc();
    read_requests_total_.inc();  // Count failed requests too
}

void TentMetrics::recordWriteFailed(size_t bytes) {
    // Fast path: check runtime switch first
    if (!initialized_ || !runtime_enabled_.load(std::memory_order_relaxed))
        return;

    write_failures_total_.inc();
    write_requests_total_.inc();  // Count failed requests too
}

std::string TentMetrics::getPrometheusMetrics() {
    if (!initialized_) return "";

    try {
        std::string result;
        // Pre-allocate buffer to avoid reallocation during serialization
        result.reserve(kPrometheusBufferSize);

        // Serialize all counters
        for (auto* counter : counters_) {
            counter->serialize(result);
        }

        // Serialize all histograms
        for (auto* histogram : histograms_) {
            histogram->serialize(result);
        }

        return result;
    } catch (const std::exception& e) {
        LOG(ERROR) << "Failed to serialize Prometheus metrics: " << e.what();
        return "";
    }
}

std::string TentMetrics::getJsonMetrics() {
    if (!initialized_) return "{}";

    try {
        nlohmann::json root;

        // Serialize all counters
        for (auto* counter : counters_) {
            root[counter->str_name()] = counter->value();
        }

        // Serialize all histograms
        for (size_t h = 0; h < histograms_.size(); ++h) {
            auto* histogram = histograms_[h];
            const auto& boundaries = histogram_boundaries_[h];

            auto bucket_counts = histogram->get_bucket_counts();

            // Calculate total count
            int64_t total_count = 0;
            for (auto& bucket : bucket_counts) {
                total_count += bucket->value();
            }

            nlohmann::json hist_obj;
            hist_obj["count"] = total_count;

            nlohmann::json buckets_obj;
            for (size_t i = 0;
                 i < boundaries.size() && i < bucket_counts.size(); ++i) {
                buckets_obj[std::to_string(static_cast<int64_t>(
                    boundaries[i]))] = bucket_counts[i]->value();
            }
            hist_obj["buckets"] = buckets_obj;

            root[histogram->str_name()] = hist_obj;
        }

        return root.dump(2);  // Pretty print with 2-space indent
    } catch (const std::exception& e) {
        LOG(ERROR) << "Failed to serialize JSON metrics: " << e.what();
        return R"({"error": "Failed to serialize metrics"})";
    }
}

std::string TentMetrics::getSummaryString() {
    if (!initialized_) return "Metrics not initialized";

    std::ostringstream oss;
    oss << std::fixed << std::setprecision(2);

    double read_bytes = read_bytes_total_.value();
    double write_bytes = write_bytes_total_.value();
    double read_reqs = read_requests_total_.value();
    double write_reqs = write_requests_total_.value();
    double read_fails = read_failures_total_.value();
    double write_fails = write_failures_total_.value();

    // Format bytes in human-readable form
    auto formatBytes = [](double bytes) -> std::string {
        std::ostringstream s;
        s << std::fixed << std::setprecision(2);
        if (bytes >= 1e12)
            s << bytes / 1e12 << " TB";
        else if (bytes >= 1e9)
            s << bytes / 1e9 << " GB";
        else if (bytes >= 1e6)
            s << bytes / 1e6 << " MB";
        else if (bytes >= 1e3)
            s << bytes / 1e3 << " KB";
        else
            s << bytes << " B";
        return s.str();
    };

    oss << "Read: " << formatBytes(read_bytes) << " ("
        << static_cast<uint64_t>(read_reqs) << " reqs, "
        << static_cast<uint64_t>(read_fails) << " fails) | "
        << "Write: " << formatBytes(write_bytes) << " ("
        << static_cast<uint64_t>(write_reqs) << " reqs, "
        << static_cast<uint64_t>(write_fails) << " fails)";

    return oss.str();
}

#else  // !TENT_METRICS_ENABLED

// Stub implementations when metrics are disabled at compile time
Status TentMetrics::initialize(const MetricsConfig& config) {
    config_ = config;
    initialized_ = true;
    LOG(INFO)
        << "TENT metrics disabled at compile time (TENT_METRICS_ENABLED=0)";
    return Status::OK();
}

void TentMetrics::shutdown() { initialized_ = false; }

void TentMetrics::recordReadCompleted(size_t, double) {}
void TentMetrics::recordWriteCompleted(size_t, double) {}
void TentMetrics::recordReadFailed(size_t) {}
void TentMetrics::recordWriteFailed(size_t) {}

std::string TentMetrics::getPrometheusMetrics() {
    return "# TENT metrics disabled at compile time\n";
}

std::string TentMetrics::getJsonMetrics() {
    return R"({"status": "disabled", "message": "TENT metrics disabled at compile time"})";
}

std::string TentMetrics::getSummaryString() {
    return "TENT metrics disabled at compile time";
}

#endif  // TENT_METRICS_ENABLED

}  // namespace mooncake::tent
