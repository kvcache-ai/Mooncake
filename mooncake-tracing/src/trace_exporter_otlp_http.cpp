#include "trace_exporter.h"

#include <curl/curl.h>
#include <glog/logging.h>

#include <algorithm>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>
#include <unistd.h>

#if __has_include(<jsoncpp/json/json.h>)
#include <jsoncpp/json/json.h>
#else
#include <json/json.h>
#endif

namespace mooncake::tracing {
namespace {
constexpr size_t kMaxRemoteBatchItems = 256;
constexpr size_t kMaxRemoteBatchBytes = 512 * 1024;
constexpr char kSamplingPriorityAttr[] = "sampling.priority";
constexpr char kStructuralSamplingPriority[] = "structural";

std::string BuildSpoolFileName(const TraceConfig& config) {
    std::string file_name = config.service_name;
    if (!config.node_id.empty()) {
        file_name += "-" + config.node_id;
    }
    if (!config.process_role.empty()) {
        file_name += "-" + config.process_role;
    }
    file_name += "-" + std::to_string(::getpid()) + ".spool.jsonl";
    return file_name;
}

std::string ToHex16(uint64_t value) {
    std::ostringstream oss;
    oss << std::hex << std::setw(16) << std::setfill('0') << value;
    return oss.str();
}

std::string NormalizeTraceId(const std::string& trace_id) {
    if (trace_id.size() >= 32) return trace_id.substr(trace_id.size() - 32);
    std::hash<std::string> h;
    return ToHex16(h(trace_id + ":hi")) + ToHex16(h(trace_id + ":lo"));
}

std::string NormalizeSpanId(const std::string& span_id) {
    if (span_id.size() >= 16) return span_id.substr(span_id.size() - 16);
    std::hash<std::string> h;
    return ToHex16(h(span_id));
}

size_t WriteCallback(char* ptr, size_t size, size_t nmemb, void* userdata) {
    auto* out = static_cast<std::string*>(userdata);
    out->append(ptr, size * nmemb);
    return size * nmemb;
}

Json::Value AttrToJson(const std::pair<std::string, std::string>& kv) {
    Json::Value attr;
    attr["key"] = kv.first;
    attr["value"]["stringValue"] = kv.second;
    return attr;
}

bool IsInternalTraceAttr(const std::pair<std::string, std::string>& kv) {
    return kv.first == kSamplingPriorityAttr;
}

Json::Value AttrsToObject(const TraceAttrs& attrs) {
    Json::Value obj(Json::objectValue);
    for (const auto& [key, value] : attrs) {
        if (key == kSamplingPriorityAttr) {
            continue;
        }
        obj[key] = value;
    }
    return obj;
}

bool HasAttrValue(const TraceAttrs& attrs, const std::string& key,
                  const std::string& value) {
    return std::any_of(attrs.begin(), attrs.end(),
                       [&key, &value](const auto& attr) {
                           return attr.first == key && attr.second == value;
                       });
}

bool IsStructuralRecord(const TraceRecord& record) {
    if (record.force_sample) {
        return true;
    }
    return HasAttrValue(record.attrs, kSamplingPriorityAttr,
                        kStructuralSamplingPriority);
}

Json::Value RecordToJson(const TraceRecord& record) {
    Json::Value root;
    root["trace_id"] = record.trace_id;
    root["span_id"] = record.span_id;
    root["parent_span_id"] = record.parent_span_id;
    root["correlation.id"] = record.correlation_id;
    root["service.name"] = record.service_name;
    root["node.id"] = record.node_id;
    root["process.role"] = record.process_role;
    root["span.name"] = record.span_name;
    root["start_time_unix_nano"] = Json::Int64(record.start_time_unix_nano);
    root["end_time_unix_nano"] = Json::Int64(record.end_time_unix_nano);
    root["status"] = record.status;
    root["attrs"] = AttrsToObject(record.attrs);

    Json::Value events(Json::arrayValue);
    for (const auto& event : record.events) {
        Json::Value json_event;
        json_event["name"] = event.name;
        json_event["unix_nano"] = Json::Int64(event.unix_nano);
        json_event["attrs"] = AttrsToObject(event.attrs);
        events.append(json_event);
    }
    root["events"] = events;
    return root;
}

Json::Value RecordToOtlpSpan(const TraceRecord& record) {
    Json::Value span;
    span["traceId"] = NormalizeTraceId(record.trace_id);
    span["spanId"] = NormalizeSpanId(record.span_id);
    if (!record.parent_span_id.empty()) {
        span["parentSpanId"] = NormalizeSpanId(record.parent_span_id);
    }
    span["name"] = record.span_name;
    span["kind"] = 1;
    span["startTimeUnixNano"] = std::to_string(record.start_time_unix_nano);
    span["endTimeUnixNano"] = std::to_string(record.end_time_unix_nano);

    Json::Value attrs(Json::arrayValue);
    if (!record.correlation_id.empty()) {
        attrs.append(AttrToJson({"correlation.id", record.correlation_id}));
    }
    for (const auto& kv : record.attrs) {
        if (IsInternalTraceAttr(kv)) {
            continue;
        }
        attrs.append(AttrToJson(kv));
    }
    span["attributes"] = attrs;

    Json::Value events(Json::arrayValue);
    for (const auto& ev : record.events) {
        Json::Value event;
        event["name"] = ev.name;
        event["timeUnixNano"] = std::to_string(ev.unix_nano);
        Json::Value event_attrs(Json::arrayValue);
        for (const auto& kv : ev.attrs) {
            event_attrs.append(AttrToJson(kv));
        }
        event["attributes"] = event_attrs;
        events.append(event);
    }
    span["events"] = events;
    if (record.status == "ERROR") {
        span["status"]["code"] = 2;
        span["status"]["message"] = "ERROR";
    } else {
        span["status"]["code"] = 1;
    }
    return span;
}

std::string BuildOtlpUrl(const std::string& endpoint, const std::string& path) {
    std::string url = endpoint;
    if (!path.empty()) {
        if (!url.empty() && url.back() == '/' && path.front() == '/') {
            url.pop_back();
        }
        url += path;
    }
    return url;
}

std::string BuildOtlpPayload(const std::vector<TraceRecord>& records) {
    Json::Value root;
    Json::Value resource_spans(Json::arrayValue);
    if (!records.empty()) {
        const auto& first = records.front();
        Json::Value resource_span;
        Json::Value resource_attrs(Json::arrayValue);
        resource_attrs.append(AttrToJson({"service.name", first.service_name}));
        if (!first.node_id.empty()) {
            resource_attrs.append(
                AttrToJson({"service.instance.id", first.node_id}));
        }
        if (!first.process_role.empty()) {
            resource_attrs.append(
                AttrToJson({"mooncake.process_role", first.process_role}));
        }
        resource_span["resource"]["attributes"] = resource_attrs;

        Json::Value scope_spans(Json::arrayValue);
        Json::Value scope_span;
        scope_span["scope"]["name"] = "mooncake-tracing";
        Json::Value spans(Json::arrayValue);
        for (const auto& record : records) {
            spans.append(RecordToOtlpSpan(record));
        }
        scope_span["spans"] = spans;
        scope_spans.append(scope_span);
        resource_span["scopeSpans"] = scope_spans;
        resource_spans.append(resource_span);
    }
    root["resourceSpans"] = resource_spans;

    Json::StreamWriterBuilder builder;
    builder["indentation"] = "";
    return Json::writeString(builder, root);
}

bool IsHighPriorityRecord(const TraceRecord& record, const TraceConfig& config) {
    if (IsStructuralRecord(record)) {
        return true;
    }
    if (record.parent_span_id.empty()) {
        return true;
    }
    if (record.status == "ERROR") {
        return true;
    }
    if (config.sampling_slow_threshold_ms > 0 &&
        record.end_time_unix_nano > record.start_time_unix_nano) {
        const auto duration_ms =
            (record.end_time_unix_nano - record.start_time_unix_nano) / 1000000;
        if (duration_ms >= config.sampling_slow_threshold_ms) {
            return true;
        }
    }
    return false;
}
}  // namespace

OtlpHttpTraceExporter::OtlpHttpTraceExporter(std::string endpoint,
                                             std::string path,
                                             std::string headers,
                                             int timeout_ms)
    : endpoint_(std::move(endpoint)),
      path_(std::move(path)),
      headers_(std::move(headers)),
      timeout_ms_(timeout_ms) {}

OtlpHttpTraceExporter::~OtlpHttpTraceExporter() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (header_list_ != nullptr) {
        curl_slist_free_all(header_list_);
        header_list_ = nullptr;
    }
    if (curl_ != nullptr) {
        curl_easy_cleanup(curl_);
        curl_ = nullptr;
    }
}

void OtlpHttpTraceExporter::Export(const TraceRecord& record) {
    std::string ignored_error;
    (void)TryExport(record, &ignored_error);
}

void OtlpHttpTraceExporter::ExportBatch(const std::vector<TraceRecord>& records) {
    std::string ignored_error;
    (void)TryExportBatch(records, &ignored_error);
}

bool OtlpHttpTraceExporter::TryExport(const TraceRecord& record,
                                      std::string* error_message) {
    return TryExportBatch(std::vector<TraceRecord>{record}, error_message);
}

bool OtlpHttpTraceExporter::EnsureCurlLocked() {
    if (curl_ != nullptr) {
        return true;
    }

    curl_ = curl_easy_init();
    if (curl_ == nullptr) {
        LOG(ERROR) << "OTLP exporter: curl_easy_init failed";
        return false;
    }

    header_list_ = curl_slist_append(header_list_, "Content-Type: application/json");
    if (!headers_.empty()) {
        std::stringstream ss(headers_);
        std::string item;
        while (std::getline(ss, item, ',')) {
            if (!item.empty()) {
                header_list_ = curl_slist_append(header_list_, item.c_str());
            }
        }
    }

    const auto url = BuildOtlpUrl(endpoint_, path_);
    curl_easy_setopt(curl_, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl_, CURLOPT_POST, 1L);
    curl_easy_setopt(curl_, CURLOPT_TIMEOUT_MS, timeout_ms_);
    curl_easy_setopt(curl_, CURLOPT_CONNECTTIMEOUT_MS, timeout_ms_);
    curl_easy_setopt(curl_, CURLOPT_DNS_CACHE_TIMEOUT, 300L);
    curl_easy_setopt(curl_, CURLOPT_TCP_KEEPALIVE, 1L);
    curl_easy_setopt(curl_, CURLOPT_HTTPHEADER, header_list_);
    curl_easy_setopt(curl_, CURLOPT_WRITEFUNCTION, WriteCallback);
    curl_easy_setopt(curl_, CURLOPT_NOSIGNAL, 1L);
    return true;
}

bool OtlpHttpTraceExporter::TryExportBatch(const std::vector<TraceRecord>& records,
                                           std::string* error_message) {
    if (records.empty()) {
        return true;
    }

    std::lock_guard<std::mutex> lock(mutex_);
    if (!EnsureCurlLocked()) {
        if (error_message) {
            *error_message = "curl_easy_init failed";
        }
        return false;
    }

    const std::string payload = BuildOtlpPayload(records);
    std::string response;
    char errbuf[CURL_ERROR_SIZE] = {0};
    curl_easy_setopt(curl_, CURLOPT_POSTFIELDS, payload.c_str());
    curl_easy_setopt(curl_, CURLOPT_POSTFIELDSIZE, payload.size());
    curl_easy_setopt(curl_, CURLOPT_WRITEDATA, &response);
    curl_easy_setopt(curl_, CURLOPT_ERRORBUFFER, errbuf);

    CURLcode rc = curl_easy_perform(curl_);
    long code = 0;
    curl_easy_getinfo(curl_, CURLINFO_RESPONSE_CODE, &code);
    bool ok = rc == CURLE_OK && code >= 200 && code < 300;
    if (rc != CURLE_OK || code < 200 || code >= 300) {
        const auto url = BuildOtlpUrl(endpoint_, path_);
        std::ostringstream oss;
        oss << "OTLP exporter POST failed url=" << url
            << " curl=" << curl_easy_strerror(rc) << " http=" << code
            << " errbuf=" << errbuf << " body=" << response;
        LOG_EVERY_N(ERROR, 100)
            << "OTLP exporter POST failed url=" << url
            << " curl=" << curl_easy_strerror(rc)
            << " http=" << code << " errbuf=" << errbuf
            << " body=" << response << " batch_size=" << records.size();
        if (error_message) {
            *error_message = oss.str();
        }
    }
    return ok;
}

AsyncRemoteTraceExporter::AsyncRemoteTraceExporter(
    TraceConfig config, std::shared_ptr<TraceExporter> fallback_exporter,
    RemoteSendFn send_fn)
    : config_(std::move(config)),
      fallback_exporter_(std::move(fallback_exporter)) {
    if (!fallback_exporter_) {
        fallback_exporter_ = std::make_shared<JsonlTraceExporter>(config_.jsonl_path);
    }
    if (send_fn) {
        remote_send_fns_.push_back(std::move(send_fn));
    } else {
        auto endpoints = config_.remote_endpoints;
        if (endpoints.empty()) {
            endpoints.push_back(config_.otlp_http_endpoint);
        }
        for (const auto& endpoint : endpoints) {
            auto remote = std::make_shared<OtlpHttpTraceExporter>(
                endpoint, config_.otlp_http_path, config_.otlp_headers,
                config_.otlp_timeout_ms);
            remote_send_fns_.push_back(
                [remote](const std::vector<TraceRecord>& records,
                         std::string* error_message) {
                    return remote->TryExportBatch(records, error_message);
                });
        }
    }
    worker_ = std::thread(&AsyncRemoteTraceExporter::WorkerMain, this);
}

AsyncRemoteTraceExporter::~AsyncRemoteTraceExporter() {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        stop_ = true;
    }
    cv_.notify_all();
    if (worker_.joinable()) {
        worker_.join();
    }
}

void AsyncRemoteTraceExporter::Export(const TraceRecord& record) {
    const auto record_bytes = EstimateRecordBytes(record);
    std::lock_guard<std::mutex> lock(mutex_);
    const bool structural = IsStructuralRecord(record);
    const bool queue_full =
        queue_.size() >= config_.exporter_queue_max_items ||
        queued_bytes_ + record_bytes > config_.exporter_queue_max_bytes;
    if (queue_full) {
        stats_.queue_full_count++;

        if (structural) {
            queue_.push_back(record);
            queued_bytes_ += record_bytes;
            stats_.queued_records = queue_.size();
            stats_.queued_bytes = queued_bytes_;
            cv_.notify_one();
            return;
        }

        if (IsHighPriorityRecord(record, config_)) {
            bool evicted = false;
            while ((queue_.size() >= config_.exporter_queue_max_items ||
                    queued_bytes_ + record_bytes >
                        config_.exporter_queue_max_bytes) &&
                   !queue_.empty()) {
                auto it = std::find_if(queue_.begin(), queue_.end(),
                                       [this](const TraceRecord& queued) {
                                           return !IsHighPriorityRecord(
                                               queued, config_);
                                       });
                if (it == queue_.end()) {
                    break;
                }
                queued_bytes_ -=
                    std::min(queued_bytes_, EstimateRecordBytes(*it));
                queue_.erase(it);
                stats_.dropped_records++;
                evicted = true;
            }
            if (evicted && queue_.size() < config_.exporter_queue_max_items &&
                queued_bytes_ + record_bytes <=
                    config_.exporter_queue_max_bytes) {
                queue_.push_back(record);
                queued_bytes_ += record_bytes;
                stats_.queued_records = queue_.size();
                stats_.queued_bytes = queued_bytes_;
                cv_.notify_one();
                return;
            }
        }

        stats_.dropped_records++;
        LOG_EVERY_N(WARNING, 1000)
            << "Tracing queue full, dropping record span=" << record.span_name;
        return;
    }
    queue_.push_back(record);
    queued_bytes_ += record_bytes;
    stats_.queued_records = queue_.size();
    stats_.queued_bytes = queued_bytes_;
    cv_.notify_one();
}

bool AsyncRemoteTraceExporter::FlushForTest(std::chrono::milliseconds timeout) {
    std::unique_lock<std::mutex> lock(mutex_);
    return drained_cv_.wait_for(lock, timeout, [this] {
        return queue_.empty() && !worker_busy_;
    });
}

TraceExporterStats AsyncRemoteTraceExporter::SnapshotStats() const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto snapshot = stats_;
    snapshot.queued_records = queue_.size();
    snapshot.queued_bytes = queued_bytes_;
    return snapshot;
}

size_t AsyncRemoteTraceExporter::EstimateRecordBytes(
    const TraceRecord& record) const {
    size_t total = record.trace_id.size() + record.span_id.size() +
                   record.parent_span_id.size() + record.correlation_id.size() +
                   record.service_name.size() + record.node_id.size() +
                   record.process_role.size() + record.span_name.size() +
                   record.status.size() + sizeof(record);
    for (const auto& attr : record.attrs) {
        total += attr.first.size() + attr.second.size();
    }
    for (const auto& event : record.events) {
        total += event.name.size() + sizeof(event);
        for (const auto& attr : event.attrs) {
            total += attr.first.size() + attr.second.size();
        }
    }
    return total;
}

bool AsyncRemoteTraceExporter::TrySendRemote(const std::vector<TraceRecord>& records,
                                             std::string* error_message) {
    std::ostringstream oss;
    for (size_t i = 0; i < remote_send_fns_.size(); ++i) {
        std::string endpoint_error;
        if (remote_send_fns_[i](records, &endpoint_error)) {
            return true;
        }
        if (!endpoint_error.empty()) {
            if (oss.tellp() > 0) {
                oss << " | ";
            }
            oss << endpoint_error;
        }
    }
    if (error_message) {
        *error_message = oss.str();
    }
    return false;
}

bool AsyncRemoteTraceExporter::TrySpool(const TraceRecord& record,
                                        std::string* error_message) {
    if (config_.exporter_spool_dir.empty()) {
        if (error_message) {
            *error_message = "spool not configured";
        }
        return false;
    }

    std::error_code ec;
    std::filesystem::create_directories(config_.exporter_spool_dir, ec);
    if (ec) {
        if (error_message) {
            *error_message = "create spool dir failed: " + ec.message();
        }
        return false;
    }

    const auto spool_path = std::filesystem::path(config_.exporter_spool_dir) /
                            BuildSpoolFileName(config_);
    std::ofstream out(spool_path, std::ios::app);
    if (!out.is_open()) {
        if (error_message) {
            *error_message = "open spool file failed: " + spool_path.string();
        }
        return false;
    }

    Json::StreamWriterBuilder builder;
    builder["indentation"] = "";
    out << Json::writeString(builder, RecordToJson(record)) << "\n";
    if (!out.good()) {
        if (error_message) {
            *error_message = "write spool file failed: " + spool_path.string();
        }
        return false;
    }
    return true;
}

void AsyncRemoteTraceExporter::WorkerMain() {
    while (true) {
        std::vector<TraceRecord> records;
        {
            std::unique_lock<std::mutex> lock(mutex_);
            cv_.wait(lock, [this] { return stop_ || !queue_.empty(); });
            if (stop_ && queue_.empty()) {
                break;
            }
            worker_busy_ = true;
            size_t batch_bytes = 0;
            while (!queue_.empty() && records.size() < kMaxRemoteBatchItems) {
                const auto next_bytes = EstimateRecordBytes(queue_.front());
                if (!records.empty() &&
                    batch_bytes + next_bytes > kMaxRemoteBatchBytes) {
                    break;
                }
                batch_bytes += next_bytes;
                queued_bytes_ -= std::min(queued_bytes_, next_bytes);
                records.push_back(std::move(queue_.front()));
                queue_.pop_front();
            }
            stats_.queued_records = queue_.size();
            stats_.queued_bytes = queued_bytes_;
        }

        bool exported = false;
        std::string error_message;
        int backoff_ms = std::max(1, config_.exporter_retry_base_ms);
        for (int attempt = 0; attempt <= config_.exporter_retry_max_attempts;
             ++attempt) {
            if (TrySendRemote(records, &error_message)) {
                exported = true;
                break;
            }
            if (attempt == config_.exporter_retry_max_attempts) {
                break;
            }
            {
                std::lock_guard<std::mutex> lock(mutex_);
                stats_.retry_count++;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(backoff_ms));
            backoff_ms =
                std::min(backoff_ms * 2, std::max(backoff_ms, config_.exporter_retry_max_ms));
        }

        if (!exported) {
            size_t spooled_records = 0;
            size_t fallback_records = 0;
            size_t spool_failures = 0;
            std::string spool_error;
            {
                std::lock_guard<std::mutex> lock(mutex_);
                stats_.collector_unreachable_count += records.size();
            }
            LOG_EVERY_N(ERROR, 100)
                << "Tracing remote exporter unavailable, span="
                << (records.empty() ? "" : records.front().span_name)
                << " error=" << error_message
                << " batch_size=" << records.size();

            for (const auto& record : records) {
                bool spooled = false;
                std::string current_spool_error;
                if (!config_.exporter_spool_dir.empty() &&
                    IsHighPriorityRecord(record, config_)) {
                    spooled = TrySpool(record, &current_spool_error);
                    if (spooled) {
                        ++spooled_records;
                    } else if (!current_spool_error.empty()) {
                        ++spool_failures;
                        spool_error = current_spool_error;
                    }
                }

                if (!spooled && fallback_exporter_) {
                    fallback_exporter_->Export(record);
                    ++fallback_records;
                }
            }

            {
                std::lock_guard<std::mutex> lock(mutex_);
                stats_.spooled_records += spooled_records;
                stats_.fallback_count += fallback_records;
                stats_.spool_failures += spool_failures;
            }

            if (!spool_error.empty()) {
                LOG_EVERY_N(ERROR, 100)
                    << "Tracing spool failed, error=" << spool_error;
            }
        }

        {
            std::lock_guard<std::mutex> lock(mutex_);
            worker_busy_ = false;
            if (queue_.empty()) {
                drained_cv_.notify_all();
            }
        }
    }
}

}  // namespace mooncake::tracing
