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
    // Validate configuration before touching initialized_. An invalid config
    // (e.g. port 0, zero HTTP threads) would otherwise cause confusing
    // failures inside initHttpServer(); fail fast with a clear error instead.
    // Validating before the compare_exchange avoids a window where
    // initialized_ is set to true and then rolled back on failure.
    std::string error_msg;
    if (!MetricsConfigLoader::validateConfig(config, &error_msg)) {
        LOG(ERROR) << "Invalid TENT metrics config: " << error_msg
                   << "; metrics disabled";
        return Status::InvalidArgument(
            "Invalid TENT metrics config: " + error_msg + LOC_MARK);
    }

    // Use compare_exchange to prevent race condition during initialization
    bool expected = false;
    if (!initialized_.compare_exchange_strong(expected, true)) {
        return Status::OK();  // Already initialized by another thread
    }

    config_ = config;

    // Set runtime enabled state from config
    runtime_enabled_.store(config_.enabled, std::memory_order_relaxed);

    // Register all metrics to vectors for unified serialization
    registerMetrics();

    // Initialize and start HTTP server on the configured port. If the port is
    // busy (e.g. another rank was given the same port), degrade to log-only
    // metrics rather than falsely reporting a listening endpoint.
    Status http_status = initHttpServer();
    const bool http_ok = http_status.ok();
    if (!http_ok) {
        LOG(WARNING) << "TENT metrics HTTP endpoint unavailable on "
                     << config_.http_host << ":" << config_.http_port << " ("
                     << http_status.ToString()
                     << "); continuing with log-only metrics";
    }

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

    if (http_ok) {
        LOG(INFO) << "TENT metrics initialized successfully, HTTP server "
                     "listening on "
                  << config_.http_host << ":"
                  << bound_http_port_.load(std::memory_order_relaxed)
                  << ", runtime_enabled="
                  << (runtime_enabled_.load() ? "true" : "false");
    } else {
        LOG(INFO) << "TENT metrics initialized in log-only mode (HTTP endpoint "
                     "disabled), runtime_enabled="
                  << (runtime_enabled_.load() ? "true" : "false");
    }
    return Status::OK();
}

Status TentMetrics::initHttpServer() {
    using namespace coro_http;

    // Create HTTP server with configurable threads on the configured port.
    // Port assignment is intentionally deterministic: co-located ranks should
    // be given distinct ports explicitly (e.g. base_port + local_rank), not
    // auto-scanned, so a rank's metrics port stays predictable.
    http_server_ = std::make_unique<coro_http_server>(
        config_.http_server_threads, config_.http_port);

    registerHttpHandlers();

    // Start the HTTP server asynchronously. async_start() returns a future that
    // already holds a result ONLY when startup failed (e.g. the port is already
    // in use); on success the future stays pending while the server keeps
    // running. Same idiom as mooncake-store's rpc_service.cpp /
    // real_client.cpp.
    auto ec = http_server_->async_start();
    if (ec.hasResult()) {
        http_server_.reset();
        return Status::RpcServiceError(
            "Failed to start TENT metrics HTTP server" LOC_MARK);
    }

    // Record the bound port (read by httpPort() from other threads, so it must
    // be the atomic, not config_).
    bound_http_port_.store(config_.http_port, std::memory_order_relaxed);
    return Status::OK();
}

void TentMetrics::registerHttpHandlers() {
    using namespace coro_http;

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

    // Reset bound port so httpPort() returns 0 after shutdown, not a stale
    // port from a previous initialization. Without this, a re-initialize
    // that fails to bind would cause httpPort() to report the old port.
    bound_http_port_.store(0, std::memory_order_relaxed);

    initialized_ = false;
    LOG(INFO) << "TENT metrics shutdown complete";
}

void TentMetrics::registerMetrics() {
    // Register all counters as base metric_t* pointers so that counters with
    // different label arities (N=1 per-transport, N=2 failover from→to) share
    // one vector for Prometheus serialize().
    counters_ = {
        &read_bytes_total_,    &write_bytes_total_,
        &read_requests_total_, &write_requests_total_,
        &read_failures_total_, &write_failures_total_,
        &failover_total_,      &deadline_infeasible_total_,
    };

    // Register all histograms - add new histograms here. Each entry pairs the
    // histogram with its compile-time bucket boundaries; the struct keeps the
    // two in sync so getJsonMetrics() cannot mislabel buckets.
    histograms_ = {
        {&read_latency_, &kLatencyBuckets},
        {&write_latency_, &kLatencyBuckets},
        {&read_size_, &kSizeBuckets},
        {&write_size_, &kSizeBuckets},
        {&deadline_mlu_, &kMluPerMilleBuckets},
        {&stage_queue_wait_, &kStageBuckets},
        {&stage_dispatch_, &kStageBuckets},
        {&stage_transport_, &kStageBuckets},
    };
}

void TentMetrics::recordReadCompleted(TransportType tp, size_t bytes,
                                      double latency_seconds) {
    if (!initialized_ || !runtime_enabled_.load(std::memory_order_relaxed))
        return;

    auto label = std::array<std::string, 1>{transportTypeName(tp)};
    read_bytes_total_.inc(label, static_cast<int64_t>(bytes));
    read_requests_total_.inc(label);
    read_size_.observe(label, static_cast<int64_t>(bytes));
    if (latency_seconds > 0.0) {
        int64_t latency_us = static_cast<int64_t>(latency_seconds * 1000000.0);
        read_latency_.observe(label, latency_us);
    }
}

void TentMetrics::recordWriteCompleted(TransportType tp, size_t bytes,
                                       double latency_seconds) {
    if (!initialized_ || !runtime_enabled_.load(std::memory_order_relaxed))
        return;

    auto label = std::array<std::string, 1>{transportTypeName(tp)};
    write_bytes_total_.inc(label, static_cast<int64_t>(bytes));
    write_requests_total_.inc(label);
    write_size_.observe(label, static_cast<int64_t>(bytes));
    if (latency_seconds > 0.0) {
        int64_t latency_us = static_cast<int64_t>(latency_seconds * 1000000.0);
        write_latency_.observe(label, latency_us);
    }
}

void TentMetrics::recordDeadlineMLU(TransportType tp, double mlu) {
    if (!initialized_ || !runtime_enabled_.load(std::memory_order_relaxed))
        return;
    if (mlu < 0.0) return;
    auto label = std::array<std::string, 1>{transportTypeName(tp)};
    deadline_mlu_.observe(label, static_cast<int64_t>(mlu * 1000.0));
}

void TentMetrics::recordDeadlineInfeasible(TransportType tp) {
    if (!initialized_ || !runtime_enabled_.load(std::memory_order_relaxed))
        return;
    deadline_infeasible_total_.inc(
        std::array<std::string, 1>{transportTypeName(tp)});
}

void TentMetrics::recordStageLatency(Stage stage, TransportType tp,
                                     double latency_us) {
    if (!initialized_ || !runtime_enabled_.load(std::memory_order_relaxed))
        return;
    if (latency_us < 0.0) return;
    auto label = std::array<std::string, 1>{transportTypeName(tp)};
    int64_t val = static_cast<int64_t>(latency_us);
    switch (stage) {
        case Stage::QueueWait:
            stage_queue_wait_.observe(label, val);
            break;
        case Stage::Dispatch:
            stage_dispatch_.observe(label, val);
            break;
        case Stage::Transport:
            stage_transport_.observe(label, val);
            break;
    }
}

void TentMetrics::recordReadFailed(TransportType tp) {
    if (!initialized_ || !runtime_enabled_.load(std::memory_order_relaxed))
        return;
    auto label = std::array<std::string, 1>{transportTypeName(tp)};
    read_failures_total_.inc(label);
    read_requests_total_.inc(label);
}

void TentMetrics::recordWriteFailed(TransportType tp) {
    if (!initialized_ || !runtime_enabled_.load(std::memory_order_relaxed))
        return;
    auto label = std::array<std::string, 1>{transportTypeName(tp)};
    write_failures_total_.inc(label);
    write_requests_total_.inc(label);
}

void TentMetrics::recordTransportFailover(TransportType from,
                                          TransportType to) {
    if (!initialized_ || !runtime_enabled_.load(std::memory_order_relaxed))
        return;
    failover_total_.inc(std::array<std::string, 2>{transportTypeName(from),
                                                   transportTypeName(to)});
}

std::string TentMetrics::getPrometheusMetrics() {
    if (!initialized_) return "";

    try {
        std::string result;
        result.reserve(kPrometheusBufferSize);

        // Serialize each metric into a temporary buffer first, then append.
        // ylt's basic_dynamic_histogram::serialize() calls str.clear() when
        // all label combos have sum=0, which would wipe previous output if
        // we serialized directly into `result`. Using a per-metric temporary
        // isolates each serialize() call.
        for (auto* counter : counters_) {
            std::string tmp;
            counter->serialize(tmp);
            result += tmp;
        }

        for (const auto& entry : histograms_) {
            std::string tmp;
            entry.h->serialize(tmp);
            result += tmp;
        }

        return result;
    } catch (const std::exception& e) {
        LOG(ERROR) << "Failed to serialize Prometheus metrics: " << e.what();
        return "";
    }
}

namespace {
// Sum values across all label combos of a dynamic counter. Works with both
// raw pointers (counter members) and shared_ptr (histogram bucket counters).
template <typename CounterPtr>
int64_t sumCounterValues(CounterPtr counter) {
    int64_t total = 0;
    for (auto& e : counter->copy()) {
        total += e->value.load(std::memory_order_relaxed);
    }
    return total;
}
}  // namespace

std::string TentMetrics::getJsonMetrics() {
    if (!initialized_) return "{}";

    try {
        nlohmann::json root;

        // Counters: aggregate (sum) across all transport label values so the
        // JSON endpoint stays a simple flat {name: total} view. Per-transport
        // breakdown is available via the Prometheus endpoint.
        root[read_bytes_total_.str_name()] =
            sumCounterValues(&read_bytes_total_);
        root[write_bytes_total_.str_name()] =
            sumCounterValues(&write_bytes_total_);
        root[read_requests_total_.str_name()] =
            sumCounterValues(&read_requests_total_);
        root[write_requests_total_.str_name()] =
            sumCounterValues(&write_requests_total_);
        root[read_failures_total_.str_name()] =
            sumCounterValues(&read_failures_total_);
        root[write_failures_total_.str_name()] =
            sumCounterValues(&write_failures_total_);
        root[failover_total_.str_name()] = sumCounterValues(&failover_total_);
        root[deadline_infeasible_total_.str_name()] =
            sumCounterValues(&deadline_infeasible_total_);

        // Histograms: sum bucket counts across all transport labels.
        auto serializeHistogram =
            [&](ylt::metric::basic_dynamic_histogram<int64_t, 1>* hist,
                const std::vector<double>& boundaries) {
                auto bucket_counts = hist->get_bucket_counts();
                int64_t total_count = 0;
                nlohmann::json buckets_obj;
                for (size_t i = 0; i < bucket_counts.size(); ++i) {
                    int64_t bucket_total = sumCounterValues(bucket_counts[i]);
                    total_count += bucket_total;
                    if (i < boundaries.size()) {
                        buckets_obj[std::to_string(static_cast<int64_t>(
                            boundaries[i]))] = bucket_total;
                    }
                }
                nlohmann::json hist_obj;
                hist_obj["count"] = total_count;
                hist_obj["buckets"] = buckets_obj;
                root[hist->str_name()] = hist_obj;
            };

        serializeHistogram(&read_latency_, kLatencyBuckets);
        serializeHistogram(&write_latency_, kLatencyBuckets);
        serializeHistogram(&read_size_, kSizeBuckets);
        serializeHistogram(&write_size_, kSizeBuckets);
        serializeHistogram(&deadline_mlu_, kMluPerMilleBuckets);
        serializeHistogram(&stage_queue_wait_, kStageBuckets);
        serializeHistogram(&stage_dispatch_, kStageBuckets);
        serializeHistogram(&stage_transport_, kStageBuckets);

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

    // Aggregate across all transport labels — summary is intentionally a
    // single total line, not per-transport. Per-transport breakdown is via
    // Prometheus.
    double read_bytes = sumCounterValues(&read_bytes_total_);
    double write_bytes = sumCounterValues(&write_bytes_total_);
    double read_reqs = sumCounterValues(&read_requests_total_);
    double write_reqs = sumCounterValues(&write_requests_total_);
    double read_fails = sumCounterValues(&read_failures_total_);
    double write_fails = sumCounterValues(&write_failures_total_);
    double failovers = sumCounterValues(&failover_total_);

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
        << static_cast<uint64_t>(write_fails) << " fails) | "
        << "Failovers: " << static_cast<uint64_t>(failovers);

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

void TentMetrics::recordReadCompleted(TransportType, size_t, double) {}
void TentMetrics::recordWriteCompleted(TransportType, size_t, double) {}
void TentMetrics::recordReadFailed(TransportType) {}
void TentMetrics::recordWriteFailed(TransportType) {}
void TentMetrics::recordTransportFailover(TransportType, TransportType) {}
void TentMetrics::recordDeadlineMLU(TransportType, double) {}
void TentMetrics::recordDeadlineInfeasible(TransportType) {}
void TentMetrics::recordStageLatency(Stage, TransportType, double) {}

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
