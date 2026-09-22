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

#include "metrics_exporter.h"

#include <glog/logging.h>

#include <charconv>
#include <csignal>  // Required before ylt headers: coro_io.hpp uses SIGPIPE
#include <cstdlib>
#include <cstring>
#include <exception>
#include <sstream>
#include <stdexcept>
#include <utility>

#include <ylt/coro_http/coro_http_server.hpp>

namespace mooncake {
namespace metrics {

namespace {

const char* getEnvValue(const std::string& prefix, const char* suffix) {
    std::string name = prefix + suffix;
    const char* value = std::getenv(name.c_str());
    if (!value || *value == '\0') return nullptr;
    return value;
}

uint16_t getEnvUint16(const std::string& prefix, const char* suffix,
                      uint16_t fallback) {
    const char* value = getEnvValue(prefix, suffix);
    if (!value) return fallback;
    const char* end = value + std::strlen(value);
    uint16_t parsed = 0;
    auto [ptr, ec] = std::from_chars(value, end, parsed);
    if (ec != std::errc() || ptr != end) {
        LOG(WARNING) << "Ignoring invalid " << prefix << suffix << "=" << value
                     << ", using " << fallback;
        return fallback;
    }
    return parsed;
}

}  // namespace

ExporterConfig ExporterConfig::fromEnv(const std::string& prefix) {
    ExporterConfig config;
    if (const char* host = getEnvValue(prefix, "_HTTP_HOST")) {
        config.http_host = host;
    }
    config.http_port = getEnvUint16(prefix, "_HTTP_PORT", config.http_port);
    config.http_server_threads =
        getEnvUint16(prefix, "_HTTP_THREADS", config.http_server_threads);
    if (config.http_server_threads == 0) {
        LOG(WARNING) << "Ignoring " << prefix
                     << "_HTTP_THREADS=0, using 1 thread";
        config.http_server_threads = 1;
    }
    return config;
}

int64_t Histogram::count() {
    int64_t total = 0;
    for (auto& bucket : histogram.get_bucket_counts()) {
        total += bucket->value();
    }
    return total;
}

MetricsExporter::MetricsExporter(std::string name) : name_(std::move(name)) {}

MetricsExporter::~MetricsExporter() { stop(); }

void MetricsExporter::addCounter(ylt::metric::counter_t* counter) {
    if (counter) counters_.push_back(counter);
}

void MetricsExporter::addHistogram(Histogram* histogram) {
    if (histogram) histograms_.push_back(histogram);
}

void MetricsExporter::setSummaryProvider(
    std::function<std::string()> provider) {
    summary_provider_ = std::move(provider);
}

void MetricsExporter::start(const ExporterConfig& config) {
    if (started_.exchange(true)) return;

    if (config.http_port != 0) startHttpServer(config);

    LOG(INFO) << name_ << " metrics started (http="
              << (httpPort()
                      ? config.http_host + ":" + std::to_string(httpPort())
                      : "off")
              << ")";
}

void MetricsExporter::startHttpServer(const ExporterConfig& config) {
    using namespace cinatra;
    try {
        http_server_ = std::make_unique<coro_http_server>(
            config.http_server_threads, config.http_port, config.http_host);

        auto serve = [this](std::string path, std::string content_type,
                            std::function<std::string()> body) {
            http_server_->set_http_handler<GET>(
                std::move(path),
                [content_type = std::move(content_type),
                 body = std::move(body)](coro_http_request&,
                                         coro_http_response& resp) {
                    resp.add_header("Content-Type", content_type);
                    resp.set_status_and_content(status_type::ok, body());
                });
        };
        serve("/metrics", "text/plain; version=0.0.4",
              [this] { return prometheusText(); });
        serve("/metrics/summary", "text/plain",
              [this] { return summaryText(); });
        serve("/metrics/json", "application/json",
              [this] { return jsonText(); });
        serve("/health", "text/plain", [] { return std::string("OK"); });

        // async_start() reports a failed bind through an already-resolved
        // future rather than by throwing.
        if (http_server_->async_start().hasResult()) {
            throw std::runtime_error("bind failed");
        }
        bound_http_port_.store(config.http_port, std::memory_order_relaxed);
    } catch (const std::exception& e) {
        LOG(ERROR) << "Failed to start " << name_ << " metrics HTTP server on "
                   << config.http_host << ":" << config.http_port << ": "
                   << e.what() << ". Metrics are still collected in-process.";
        http_server_.reset();
    }
}

void MetricsExporter::stop() {
    if (http_server_) {
        http_server_->stop();
        http_server_.reset();
    }
    bound_http_port_.store(0, std::memory_order_relaxed);
    started_.store(false, std::memory_order_relaxed);
}

std::string MetricsExporter::prometheusText() {
    try {
        std::string result;
        for (auto* counter : counters_) counter->serialize(result);
        for (auto* entry : histograms_) entry->histogram.serialize(result);
        return result;
    } catch (const std::exception& e) {
        LOG(ERROR) << "Failed to serialize " << name_
                   << " Prometheus metrics: " << e.what();
        return "";
    }
}

std::string MetricsExporter::jsonText() {
    std::ostringstream oss;
    oss << "{";
    const char* separator = "";
    for (auto* counter : counters_) {
        oss << separator << "\"" << counter->str_name()
            << "\":" << counter->value();
        separator = ",";
    }
    for (auto* entry : histograms_) {
        auto bucket_counts = entry->histogram.get_bucket_counts();
        oss << separator << "\"" << entry->histogram.str_name()
            << "\":{\"count\":" << entry->count()
            << ",\"sum\":" << entry->sum.load(std::memory_order_relaxed)
            << ",\"buckets\":{";
        for (size_t i = 0;
             i < entry->boundaries.size() && i < bucket_counts.size(); ++i) {
            if (i > 0) oss << ",";
            oss << "\"" << static_cast<int64_t>(entry->boundaries[i])
                << "\":" << bucket_counts[i]->value();
        }
        oss << "}}";
        separator = ",";
    }
    oss << "}";
    return oss.str();
}

std::string MetricsExporter::summaryText() {
    return summary_provider_ ? summary_provider_() : std::string();
}

}  // namespace metrics
}  // namespace mooncake
