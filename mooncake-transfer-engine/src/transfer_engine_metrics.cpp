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

#include "transfer_engine_metrics.h"

#include <glog/logging.h>

#include <cstdlib>
#include <exception>
#include <iomanip>
#include <sstream>

#ifdef WITH_METRICS
#include <ylt/coro_http/coro_http_server.hpp>
#endif

namespace mooncake {

namespace {
// Parse an unsigned 16-bit env var; returns fallback if unset or malformed.
uint16_t getEnvU16(const char* name, uint16_t fallback) {
    const char* value = std::getenv(name);
    if (!value || *value == '\0') return fallback;
    try {
        int parsed = std::stoi(value);
        if (parsed < 0 || parsed > 65535) {
            LOG(WARNING) << "Ignoring out-of-range " << name << "=" << value;
            return fallback;
        }
        return static_cast<uint16_t>(parsed);
    } catch (const std::exception& e) {
        LOG(WARNING) << "Failed to parse " << name << "=" << value
                     << ", using default " << fallback;
        return fallback;
    }
}
}  // namespace

TransferEngineMetrics& TransferEngineMetrics::instance() {
    static TransferEngineMetrics instance;
    return instance;
}

TransferEngineMetrics::Config TransferEngineMetrics::loadConfigFromEnv() {
    Config config;
    config.http_port = getEnvU16("MC_TE_METRIC_HTTP_PORT", 0);
    const char* host = std::getenv("MC_TE_METRIC_HTTP_HOST");
    if (host && *host != '\0') {
        config.http_host = host;
    }
    config.http_server_threads = getEnvU16("MC_TE_METRIC_HTTP_THREADS", 1);
    return config;
}

#ifdef WITH_METRICS

TransferEngineMetrics::TransferEngineMetrics() = default;

TransferEngineMetrics::~TransferEngineMetrics() { shutdown(); }

void TransferEngineMetrics::initialize(const Config& config) {
    bool expected = false;
    if (!initialized_.compare_exchange_strong(expected, true)) {
        return;  // Already initialized.
    }

    config_ = config;
    runtime_enabled_.store(config_.enabled, std::memory_order_relaxed);

    if (config_.http_port != 0) {
        initHttpServer(config_);
    }

    LOG(INFO) << "Transfer Engine metrics initialized (runtime_enabled="
              << (config_.enabled ? "true" : "false")
              << ", http_port=" << config_.http_port << ")";
}

void TransferEngineMetrics::initHttpServer(const Config& config) {
    using namespace coro_http;
    try {
        http_server_ = std::make_unique<coro_http_server>(
            config.http_server_threads, config.http_port, config.http_host);

        http_server_->set_http_handler<GET>(
            "/metrics", [this](coro_http_request&, coro_http_response& resp) {
                resp.add_header("Content-Type", "text/plain; version=0.0.4");
                resp.set_status_and_content(status_type::ok,
                                            getPrometheusMetrics());
            });
        http_server_->set_http_handler<GET>(
            "/metrics/summary",
            [this](coro_http_request&, coro_http_response& resp) {
                resp.add_header("Content-Type", "text/plain");
                resp.set_status_and_content(status_type::ok,
                                            getSummaryString());
            });
        http_server_->set_http_handler<GET>(
            "/metrics/json",
            [this](coro_http_request&, coro_http_response& resp) {
                resp.add_header("Content-Type", "application/json");
                resp.set_status_and_content(status_type::ok, getJsonMetrics());
            });
        http_server_->set_http_handler<GET>(
            "/health", [](coro_http_request&, coro_http_response& resp) {
                resp.add_header("Content-Type", "text/plain");
                resp.set_status_and_content(status_type::ok, "OK");
            });

        http_server_->async_start();
        LOG(INFO) << "Transfer Engine metrics HTTP server listening on "
                  << config.http_host << ":" << config.http_port;
    } catch (const std::exception& e) {
        LOG(ERROR) << "Failed to start Transfer Engine metrics HTTP server: "
                   << e.what();
        http_server_.reset();
    }
}

void TransferEngineMetrics::shutdown() {
    if (!initialized_.exchange(false)) return;
    if (http_server_) {
        http_server_->stop();
        http_server_.reset();
    }
    LOG(INFO) << "Transfer Engine metrics shutdown complete";
}

void TransferEngineMetrics::recordSubmitted() {
    if (!isInitialized() || !isEnabled()) return;
    requests_total_.inc();
    inflight_transfers_.inc();
}

void TransferEngineMetrics::recordCompleted(size_t bytes,
                                            double latency_seconds) {
    if (!isInitialized() || !isEnabled()) return;
    if (bytes > 0) {
        bytes_total_.inc(static_cast<double>(bytes));
    }
    size_bytes_.observe(static_cast<int64_t>(bytes));
    if (latency_seconds > 0.0) {
        latency_seconds_.observe(latency_seconds);
        latency_sum_seconds_.fetch_add(latency_seconds,
                                       std::memory_order_relaxed);
    }
    inflight_transfers_.dec();
}

void TransferEngineMetrics::recordFailed() {
    if (!isInitialized() || !isEnabled()) return;
    failures_total_.inc();
    inflight_transfers_.dec();
}

double TransferEngineMetrics::requestsTotal() {
    return requests_total_.value();
}
double TransferEngineMetrics::bytesTotal() { return bytes_total_.value(); }
double TransferEngineMetrics::failuresTotal() {
    return failures_total_.value();
}
int64_t TransferEngineMetrics::inflightTransfers() {
    return inflight_transfers_.value();
}

int64_t TransferEngineMetrics::latencySampleCount() {
    int64_t total = 0;
    for (auto& bucket : latency_seconds_.get_bucket_counts()) {
        total += bucket->value();
    }
    return total;
}

int64_t TransferEngineMetrics::sizeSampleCount() {
    int64_t total = 0;
    for (auto& bucket : size_bytes_.get_bucket_counts()) {
        total += bucket->value();
    }
    return total;
}

void TransferEngineMetrics::resetForTesting() {
    requests_total_.reset();
    bytes_total_.reset();
    failures_total_.reset();
    // gauge_t derives from counter_t; update() sets an absolute value.
    inflight_transfers_.update(0);
    latency_seconds_ = ylt::metric::histogram_d(
        "transfer_latency_seconds", "Transfer completion latency in seconds",
        kLatencyBucketsSeconds);
    size_bytes_ = ylt::metric::histogram_t(
        "transfer_size_bytes", "Transfer request size in bytes", kSizeBuckets);
    latency_sum_seconds_.store(0.0, std::memory_order_relaxed);
    setEnabled(true);
}

void TransferEngineMetrics::serializeLatencyHistogram(std::string& out) {
    // Manual Prometheus histogram serialization (see latency_sum_seconds_ note
    // in the header): emit cumulative buckets, _sum and _count.
    auto bucket_counts = latency_seconds_.get_bucket_counts();
    int64_t cumulative = 0;
    out.append(
        "# HELP transfer_latency_seconds Transfer completion latency in "
        "seconds\n");
    out.append("# TYPE transfer_latency_seconds histogram\n");
    for (size_t i = 0; i < bucket_counts.size(); ++i) {
        cumulative += bucket_counts[i]->value();
        out.append("transfer_latency_seconds_bucket{le=\"");
        if (i < kLatencyBucketsSeconds.size()) {
            out.append(std::to_string(kLatencyBucketsSeconds[i]));
        } else {
            out.append("+Inf");
        }
        out.append("\"} ").append(std::to_string(cumulative)).append("\n");
    }
    std::ostringstream sum_stream;
    sum_stream << latency_sum_seconds_.load(std::memory_order_relaxed);
    out.append("transfer_latency_seconds_sum ")
        .append(sum_stream.str())
        .append("\n");
    out.append("transfer_latency_seconds_count ")
        .append(std::to_string(cumulative))
        .append("\n");
}

std::string TransferEngineMetrics::getPrometheusMetrics() {
    if (!isInitialized()) return "";
    try {
        std::string result;
        requests_total_.serialize(result);
        bytes_total_.serialize(result);
        failures_total_.serialize(result);
        inflight_transfers_.serialize(result);
        serializeLatencyHistogram(result);
        size_bytes_.serialize(result);
        return result;
    } catch (const std::exception& e) {
        LOG(ERROR) << "Failed to serialize Prometheus metrics: " << e.what();
        return "";
    }
}

std::string TransferEngineMetrics::getJsonMetrics() {
    if (!isInitialized()) return "{}";
    std::ostringstream oss;
    oss << "{"
        << "\"transfer_requests_total\":" << requests_total_.value() << ","
        << "\"transfer_bytes_total\":" << bytes_total_.value() << ","
        << "\"transfer_failures_total\":" << failures_total_.value() << ","
        << "\"inflight_transfers\":" << inflight_transfers_.value() << ","
        << "\"transfer_latency_seconds_count\":" << latencySampleCount() << ","
        << "\"transfer_size_bytes_count\":" << sizeSampleCount() << "}";
    return oss.str();
}

std::string TransferEngineMetrics::getSummaryString() {
    if (!isInitialized()) return "Metrics not initialized";
    std::ostringstream oss;
    oss << "Requests: " << static_cast<uint64_t>(requests_total_.value())
        << " | Bytes: " << static_cast<uint64_t>(bytes_total_.value())
        << " | Failures: " << static_cast<uint64_t>(failures_total_.value())
        << " | Inflight: " << inflight_transfers_.value();
    return oss.str();
}

#else  // !WITH_METRICS

TransferEngineMetrics::TransferEngineMetrics() = default;
TransferEngineMetrics::~TransferEngineMetrics() = default;

void TransferEngineMetrics::initialize(const Config&) {}
void TransferEngineMetrics::shutdown() {}
void TransferEngineMetrics::recordSubmitted() {}
void TransferEngineMetrics::recordCompleted(size_t, double) {}
void TransferEngineMetrics::recordFailed() {}

std::string TransferEngineMetrics::getPrometheusMetrics() {
    return "# Transfer Engine metrics disabled at compile time\n";
}
std::string TransferEngineMetrics::getJsonMetrics() {
    return R"({"status": "disabled"})";
}
std::string TransferEngineMetrics::getSummaryString() {
    return "Transfer Engine metrics disabled at compile time";
}

#endif  // WITH_METRICS

}  // namespace mooncake
