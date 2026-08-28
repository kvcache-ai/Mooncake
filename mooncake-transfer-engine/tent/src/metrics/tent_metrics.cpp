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
#include <iomanip>
#include <sstream>
#include <unordered_set>

namespace mooncake::tent {

TentMetrics& TentMetrics::instance() {
    static TentMetrics instance;
    return instance;
}

TentMetrics::~TentMetrics() { shutdown(); }

#if TENT_METRICS_ENABLED

namespace {
const char* operationName(Request::OpCode operation) {
    return operation == Request::READ ? "read" : "write";
}
}  // namespace

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
    // different label arities (N=1 per-transport, N=2 failover from→to and
    // transport-attempt operation labels) share one vector for Prometheus
    // serialize(). CachedDynamicCounter is-a basic_dynamic_counter, so its
    // ylt serialize() output is unchanged.
    counters_ = {
        &read_bytes_total_,
        &write_bytes_total_,
        &read_requests_total_,
        &write_requests_total_,
        &read_failures_total_,
        &write_failures_total_,
        &failover_total_,
        &transport_attempts_total_,
        &transport_attempt_failures_total_,
        &deadline_infeasible_total_,
        &quarantined_batches_total_,
    };

    // Register the N=1 per-transport histograms for unified serialization.
    // The N=2 transport_attempt_latency_ histogram is serialized separately
    // (it can't share this N=1-typed vector); see getPrometheusMetrics /
    // getJsonMetrics.
    histograms_ = {
        &read_latency_, &write_latency_,    &read_size_,      &write_size_,
        &deadline_mlu_, &stage_queue_wait_, &stage_dispatch_, &stage_transport_,
    };
}

void TentMetrics::recordReadCompleted(TransportType tp, size_t bytes,
                                      double latency_seconds) {
    if (!initialized_ || !runtime_enabled_.load(std::memory_order_relaxed))
        return;

    const size_t slot = transportSlot(tp);
    // The label is only constructed on the first use of this slot (cache
    // miss); the steady state is a relaxed atomic add.
    auto label = [tp] {
        return std::array<std::string, 1>{transportTypeName(tp)};
    };
    auto bytes_val = static_cast<int64_t>(bytes);
    read_bytes_total_.incCached(slot, label, bytes_val);
    read_requests_total_.incCached(slot, label);
    read_size_.observeCached(slot, label, bytes_val);
    if (latency_seconds > 0.0) {
        int64_t latency_us = static_cast<int64_t>(latency_seconds * 1000000.0);
        read_latency_.observeCached(slot, label, latency_us);
    }
}

void TentMetrics::recordWriteCompleted(TransportType tp, size_t bytes,
                                       double latency_seconds) {
    if (!initialized_ || !runtime_enabled_.load(std::memory_order_relaxed))
        return;

    const size_t slot = transportSlot(tp);
    auto label = [tp] {
        return std::array<std::string, 1>{transportTypeName(tp)};
    };
    auto bytes_val = static_cast<int64_t>(bytes);
    write_bytes_total_.incCached(slot, label, bytes_val);
    write_requests_total_.incCached(slot, label);
    write_size_.observeCached(slot, label, bytes_val);
    if (latency_seconds > 0.0) {
        int64_t latency_us = static_cast<int64_t>(latency_seconds * 1000000.0);
        write_latency_.observeCached(slot, label, latency_us);
    }
}

void TentMetrics::recordDeadlineMLU(TransportType tp, double mlu) {
    if (!initialized_ || !runtime_enabled_.load(std::memory_order_relaxed))
        return;
    if (mlu < 0.0) return;
    const size_t slot = transportSlot(tp);
    auto label = [tp] {
        return std::array<std::string, 1>{transportTypeName(tp)};
    };
    auto mlu_permille = static_cast<int64_t>(mlu * 1000.0);
    deadline_mlu_.observeCached(slot, label, mlu_permille);
}

void TentMetrics::recordDeadlineInfeasible(TransportType tp) {
    if (!initialized_ || !runtime_enabled_.load(std::memory_order_relaxed))
        return;
    deadline_infeasible_total_.incCached(transportSlot(tp), [tp] {
        return std::array<std::string, 1>{transportTypeName(tp)};
    });
}

void TentMetrics::recordBatchQuarantined() {
    if (!initialized_ || !runtime_enabled_.load(std::memory_order_relaxed))
        return;
    quarantined_batches_total_.inc();
}

void TentMetrics::recordStageLatency(Stage stage, TransportType tp,
                                     double latency_us) {
    if (!initialized_ || !runtime_enabled_.load(std::memory_order_relaxed))
        return;
    if (latency_us < 0.0) return;
    const size_t slot = transportSlot(tp);
    auto label = [tp] {
        return std::array<std::string, 1>{transportTypeName(tp)};
    };
    int64_t val = static_cast<int64_t>(latency_us);
    switch (stage) {
        case Stage::QueueWait:
            stage_queue_wait_.observeCached(slot, label, val);
            break;
        case Stage::Dispatch:
            stage_dispatch_.observeCached(slot, label, val);
            break;
        case Stage::Transport:
            stage_transport_.observeCached(slot, label, val);
            break;
    }
}

void TentMetrics::recordReadFailed(TransportType tp) {
    if (!initialized_ || !runtime_enabled_.load(std::memory_order_relaxed))
        return;
    const size_t slot = transportSlot(tp);
    auto label = [tp] {
        return std::array<std::string, 1>{transportTypeName(tp)};
    };
    read_failures_total_.incCached(slot, label);
    read_requests_total_.incCached(slot, label);
}

void TentMetrics::recordWriteFailed(TransportType tp) {
    if (!initialized_ || !runtime_enabled_.load(std::memory_order_relaxed))
        return;
    const size_t slot = transportSlot(tp);
    auto label = [tp] {
        return std::array<std::string, 1>{transportTypeName(tp)};
    };
    write_failures_total_.incCached(slot, label);
    write_requests_total_.incCached(slot, label);
}

void TentMetrics::recordTransportFailover(TransportType from,
                                          TransportType to) {
    if (!initialized_ || !runtime_enabled_.load(std::memory_order_relaxed))
        return;
    // Rare event on a large (transport × transport) label domain: plain
    // locked path, no cached cells.
    failover_total_.inc(std::array<std::string, 2>{transportTypeName(from),
                                                   transportTypeName(to)});
}

void TentMetrics::recordTransportAttemptStarted(TransportType tp,
                                                Request::OpCode operation) {
    if (!initialized_ || !runtime_enabled_.load(std::memory_order_relaxed))
        return;
    const size_t slot = attemptSlot(tp, operation);
    transport_attempts_total_.incCached(slot, [tp, operation] {
        return std::array<std::string, 2>{transportTypeName(tp),
                                          operationName(operation)};
    });
}

void TentMetrics::recordTransportAttemptFinished(TransportType tp,
                                                 Request::OpCode operation,
                                                 TransferStatusEnum status,
                                                 double latency_us) {
    if (!initialized_ || !runtime_enabled_.load(std::memory_order_relaxed))
        return;
    const size_t slot = attemptSlot(tp, operation);
    auto label = [tp, operation] {
        return std::array<std::string, 2>{transportTypeName(tp),
                                          operationName(operation)};
    };
    if (status == FAILED) {
        transport_attempt_failures_total_.incCached(slot, label);
    }
    if (latency_us >= 0.0) {
        auto latency_val = static_cast<int64_t>(latency_us);
        transport_attempt_latency_.observeCached(slot, label, latency_val);
    }
}

std::string TentMetrics::getPrometheusMetrics() {
    if (!initialized_) return "";

    try {
        std::string result;
        result.reserve(kPrometheusBufferSize);

        // Counters: ylt's counter_t::serialize() is reliable — no evidence of
        // silent drops in practice. Kept as-is.
        for (auto* counter : counters_) {
            std::string tmp;
            counter->serialize(tmp);
            result += tmp;
        }

        // Histograms: do NOT use ylt's basic_dynamic_histogram::serialize().
        // It silently drops the entire metric (including the # HELP / # TYPE
        // header it already wrote) whenever every label combo has sum_==0 —
        // via `if (value == 0) continue; ... if (value_str.empty())
        // str.clear();`. That condition is reachable in production: e.g.
        // stage_queue_wait_us with sub-microsecond latencies that truncate to
        // 0 under int64_t observation. The JSON endpoint's custom serializer
        // walks get_bucket_counts() directly and is unaffected, which is why
        // /metrics/json reported count=4846 while /metrics omitted the metric
        // entirely. Using the same bucket-walk here closes that drift.
        for (const auto& hist : histograms_) {
            serializeHistogramPrometheus(*hist, result);
        }
        // N=2 transport-attempt latency histogram (labels: transport,
        // operation) goes through the same helper, instantiated for N=2.
        serializeHistogramPrometheus(transport_attempt_latency_, result);

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

template <uint8_t N>
void serializeHistogramToJson(nlohmann::json& root,
                              metrics::CachedDynamicHistogram<N>& hist) {
    auto bucket_counters = hist.bucketCounters();
    const auto& boundaries = hist.boundaries();
    int64_t total_count = 0;
    int64_t total_sum = 0;
    nlohmann::json buckets_obj;
    for (size_t i = 0; i < bucket_counters.size(); ++i) {
        int64_t bucket_total = sumCounterValues(bucket_counters[i]);
        total_count += bucket_total;
        if (i < boundaries.size()) {
            buckets_obj[std::to_string(static_cast<int64_t>(boundaries[i]))] =
                bucket_total;
        }
    }
    // The sum counter is maintained by the histogram itself (one add per
    // observe), matching the Prometheus path.
    for (auto& e : hist.sumCounter()->copy()) {
        total_sum += e->value.load();
    }
    nlohmann::json hist_obj;
    hist_obj["count"] = total_count;
    hist_obj["sum"] = total_sum;
    hist_obj["buckets"] = buckets_obj;
    root[hist.name()] = hist_obj;
}
}  // namespace

template <uint8_t N>
void TentMetrics::serializeHistogramPrometheus(
    metrics::CachedDynamicHistogram<N>& hist, std::string& out) const {
    // Walk the same bucket-counter / sum-counter data the JSON path uses, so
    // the two endpoints cannot drift. Unlike ylt's serialize() this never
    // silently drops a histogram that has observed >=1 sample: ylt clears its
    // output string (taking the # HELP / # TYPE header with it) whenever
    // every label combo has sum_==0, which is reachable in production when
    // sub-microsecond latencies truncate to 0 under int64_t observation.
    //
    // Templated over label arity N: the per-transport histograms are N=1 and
    // the transport-attempt latency histogram is N=2. For N=1 the emitted
    // text is byte-identical to the original single-label implementation.
    auto bucket_counters = hist.bucketCounters();
    if (bucket_counters.empty()) return;

    const auto& boundaries = hist.boundaries();
    const auto& label_names = hist.labelsName();

    // A unique key per label tuple, used to dedup combos and look up the sum.
    auto combo_key = [](const std::array<std::string, N>& lv) {
        std::string k;
        for (uint8_t i = 0; i < N; ++i) {
            k.append(lv[i]);
            k.push_back('\x1f');  // separator that cannot appear in a label
        }
        return k;
    };
    // Render `name0="v0",name1="v1"` for a label tuple (no surrounding braces).
    auto append_labels = [&](std::string& dst,
                             const std::array<std::string, N>& lv) {
        for (uint8_t i = 0; i < N; ++i) {
            if (i) dst.append(",");
            dst.append(label_names[i]).append("=\"").append(lv[i]).append("\"");
        }
    };

    // Build the union of label combos across ALL buckets. A bucket counter
    // only sees a combo after that combo has been observed in that bucket,
    // so no single bucket sees every combo: e.g. queue_wait (sub-us ->
    // bucket[0]) and transport_us (>=100us -> higher buckets) live in
    // disjoint buckets. Unioning across buckets recovers the full set.
    std::vector<const std::array<std::string, N>*> label_combos;
    std::unordered_set<std::string> seen;
    for (auto* bucket_counter : bucket_counters) {
        for (auto& e : bucket_counter->copy()) {
            if (seen.insert(combo_key(e->label)).second) {
                label_combos.push_back(&e->label);
            }
        }
    }
    if (label_combos.empty()) return;

    // Pre-compute per-combo total counts so we can (a) skip totally-empty
    // combos and (b) decide whether to emit the # HELP / # TYPE header at
    // all. A combo with sum==0 but real observations is still emitted.
    std::vector<std::pair<const std::array<std::string, N>*, int64_t>>
        active_combos;
    for (auto* labels_value : label_combos) {
        int64_t total_count = 0;
        for (auto* bucket_counter : bucket_counters) {
            total_count += bucket_counter->value(*labels_value);
        }
        if (total_count > 0) {
            active_combos.emplace_back(labels_value, total_count);
        }
    }
    if (active_combos.empty()) return;

    // Read back the per-combo sum from the histogram's sum counter
    // (maintained alongside each observe() call). copy() returns a vector of
    // {label, value} pairs; build a lookup map for O(1) access per combo.
    std::unordered_map<std::string, int64_t> sum_by_combo;
    for (auto& e : hist.sumCounter()->copy()) {
        sum_by_combo[combo_key(e->label)] = e->value.load();
    }

    const std::string& name = hist.name();
    const std::string help_str{hist.help()};

    // Emit the header once per metric (matches ylt's serialize_head()).
    out.append("# HELP ").append(name).append(" ").append(help_str).append(
        "\n");
    out.append("# TYPE ").append(name).append(" histogram\n");

    for (auto& [labels_value, total_count] : active_combos) {
        int64_t cumulative = 0;
        for (size_t i = 0; i < bucket_counters.size(); ++i) {
            cumulative += bucket_counters[i]->value(*labels_value);
            out.append(name).append("_bucket{");
            append_labels(out, *labels_value);
            out.append(",");
            if (i < boundaries.size()) {
                out.append("le=\"")
                    .append(std::to_string(boundaries[i]))
                    .append("\"} ");
            } else {
                out.append("le=\"+Inf\"} ");
            }
            out.append(std::to_string(cumulative)).append("\n");
        }

        // _sum: read from the sum counter maintained by each observe() call.
        // Falls back to 0 if the combo is not yet in the counter (should not
        // happen for active combos, but defensive).
        int64_t total_sum = 0;
        auto it = sum_by_combo.find(combo_key(*labels_value));
        if (it != sum_by_combo.end()) {
            total_sum = it->second;
        }
        out.append(name).append("_sum{");
        append_labels(out, *labels_value);
        out.append("} ").append(std::to_string(total_sum)).append("\n");

        out.append(name).append("_count{");
        append_labels(out, *labels_value);
        out.append("} ").append(std::to_string(total_count)).append("\n");
    }
}

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
        root[transport_attempts_total_.str_name()] =
            sumCounterValues(&transport_attempts_total_);
        root[transport_attempt_failures_total_.str_name()] =
            sumCounterValues(&transport_attempt_failures_total_);
        root[deadline_infeasible_total_.str_name()] =
            sumCounterValues(&deadline_infeasible_total_);
        root[quarantined_batches_total_.str_name()] =
            static_cast<int64_t>(quarantined_batches_total_.value());

        // Histograms: sum bucket counts across all transport labels. The
        // templated helper also reads back the histogram's sum counter so the
        // JSON endpoint emits "sum" alongside "count" (and stays in sync with
        // the Prometheus endpoint).
        serializeHistogramToJson(root, read_latency_);
        serializeHistogramToJson(root, write_latency_);
        serializeHistogramToJson(root, read_size_);
        serializeHistogramToJson(root, write_size_);
        serializeHistogramToJson(root, deadline_mlu_);
        serializeHistogramToJson(root, stage_queue_wait_);
        serializeHistogramToJson(root, stage_dispatch_);
        serializeHistogramToJson(root, stage_transport_);
        serializeHistogramToJson(root, transport_attempt_latency_);

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
        << "Failovers: " << static_cast<uint64_t>(failovers)
        << " | Quarantined batches: "
        << static_cast<uint64_t>(quarantined_batches_total_.value());

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
void TentMetrics::recordTransportAttemptStarted(TransportType,
                                                Request::OpCode) {}
void TentMetrics::recordTransportAttemptFinished(TransportType, Request::OpCode,
                                                 TransferStatusEnum, double) {}
void TentMetrics::recordDeadlineMLU(TransportType, double) {}
void TentMetrics::recordDeadlineInfeasible(TransportType) {}
void TentMetrics::recordBatchQuarantined() {}
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
