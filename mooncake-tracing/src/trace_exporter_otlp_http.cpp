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

Json::Value AttrsToObject(const TraceAttrs& attrs) {
    Json::Value obj(Json::objectValue);
    for (const auto& [key, value] : attrs) {
        obj[key] = value;
    }
    return obj;
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
}  // namespace

OtlpHttpTraceExporter::OtlpHttpTraceExporter(std::string endpoint,
                                             std::string path,
                                             std::string headers,
                                             int timeout_ms)
    : endpoint_(std::move(endpoint)),
      path_(std::move(path)),
      headers_(std::move(headers)),
      timeout_ms_(timeout_ms) {}

void OtlpHttpTraceExporter::Export(const TraceRecord& record) {
    std::string ignored_error;
    (void)TryExport(record, &ignored_error);
}

bool OtlpHttpTraceExporter::TryExport(const TraceRecord& record,
                                      std::string* error_message) {
    std::lock_guard<std::mutex> lock(mutex_);
    CURL* curl = curl_easy_init();
    if (!curl) {
        LOG(ERROR) << "OTLP exporter: curl_easy_init failed";
        if (error_message) {
            *error_message = "curl_easy_init failed";
        }
        return false;
    }

    Json::Value root;
    Json::Value resource_spans(Json::arrayValue);
    Json::Value resource_span;
    Json::Value resource_attrs(Json::arrayValue);
    resource_attrs.append(AttrToJson({"service.name", record.service_name}));
    if (!record.node_id.empty()) {
        resource_attrs.append(AttrToJson({"service.instance.id", record.node_id}));
    }
    if (!record.process_role.empty()) {
        resource_attrs.append(AttrToJson({"mooncake.process_role", record.process_role}));
    }
    resource_span["resource"]["attributes"] = resource_attrs;

    Json::Value scope_spans(Json::arrayValue);
    Json::Value scope_span;
    scope_span["scope"]["name"] = "mooncake-tracing";
    Json::Value spans(Json::arrayValue);
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
    for (const auto& kv : record.attrs) attrs.append(AttrToJson(kv));
    span["attributes"] = attrs;

    Json::Value events(Json::arrayValue);
    for (const auto& ev : record.events) {
        Json::Value event;
        event["name"] = ev.name;
        event["timeUnixNano"] = std::to_string(ev.unix_nano);
        Json::Value event_attrs(Json::arrayValue);
        for (const auto& kv : ev.attrs) event_attrs.append(AttrToJson(kv));
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

    spans.append(span);
    scope_span["spans"] = spans;
    scope_spans.append(scope_span);
    resource_span["scopeSpans"] = scope_spans;
    resource_spans.append(resource_span);
    root["resourceSpans"] = resource_spans;

    Json::StreamWriterBuilder builder;
    builder["indentation"] = "";
    const std::string payload = Json::writeString(builder, root);

    std::string url = endpoint_;
    if (!path_.empty()) {
        if (!url.empty() && url.back() == '/' && path_.front() == '/') {
            url.pop_back();
        }
        url += path_;
    }

    struct curl_slist* header_list = nullptr;
    header_list = curl_slist_append(header_list, "Content-Type: application/json");
    if (!headers_.empty()) {
        std::stringstream ss(headers_);
        std::string item;
        while (std::getline(ss, item, ',')) {
            if (!item.empty()) header_list = curl_slist_append(header_list, item.c_str());
        }
    }

    std::string response;
    char errbuf[CURL_ERROR_SIZE] = {0};
    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl, CURLOPT_POST, 1L);
    curl_easy_setopt(curl, CURLOPT_POSTFIELDS, payload.c_str());
    curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, payload.size());
    curl_easy_setopt(curl, CURLOPT_TIMEOUT_MS, timeout_ms_);
    curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT_MS, timeout_ms_);
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, header_list);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, WriteCallback);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &response);
    curl_easy_setopt(curl, CURLOPT_ERRORBUFFER, errbuf);
    curl_easy_setopt(curl, CURLOPT_NOSIGNAL, 1L);

    CURLcode rc = curl_easy_perform(curl);
    long code = 0;
    curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &code);
    bool ok = rc == CURLE_OK && code >= 200 && code < 300;
    if (rc != CURLE_OK || code < 200 || code >= 300) {
        std::ostringstream oss;
        oss << "OTLP exporter POST failed url=" << url
            << " curl=" << curl_easy_strerror(rc) << " http=" << code
            << " errbuf=" << errbuf << " body=" << response;
        LOG(ERROR) << "OTLP exporter POST failed url=" << url
                   << " curl=" << curl_easy_strerror(rc)
                   << " http=" << code << " errbuf=" << errbuf
                   << " body=" << response;
        if (error_message) {
            *error_message = oss.str();
        }
    }

    curl_slist_free_all(header_list);
    curl_easy_cleanup(curl);
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
                [remote](const TraceRecord& record, std::string* error_message) {
                    return remote->TryExport(record, error_message);
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
    if (queue_.size() >= config_.exporter_queue_max_items ||
        queued_bytes_ + record_bytes > config_.exporter_queue_max_bytes) {
        stats_.dropped_records++;
        stats_.queue_full_count++;
        LOG_EVERY_N(WARNING, 100)
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

bool AsyncRemoteTraceExporter::TrySendRemote(const TraceRecord& record,
                                             std::string* error_message) {
    std::ostringstream oss;
    for (size_t i = 0; i < remote_send_fns_.size(); ++i) {
        std::string endpoint_error;
        if (remote_send_fns_[i](record, &endpoint_error)) {
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
        TraceRecord record;
        {
            std::unique_lock<std::mutex> lock(mutex_);
            cv_.wait(lock, [this] { return stop_ || !queue_.empty(); });
            if (stop_ && queue_.empty()) {
                break;
            }
            worker_busy_ = true;
            record = std::move(queue_.front());
            queued_bytes_ -= std::min(queued_bytes_, EstimateRecordBytes(record));
            queue_.pop_front();
            stats_.queued_records = queue_.size();
            stats_.queued_bytes = queued_bytes_;
        }

        bool exported = false;
        std::string error_message;
        int backoff_ms = std::max(1, config_.exporter_retry_base_ms);
        for (int attempt = 0; attempt <= config_.exporter_retry_max_attempts;
             ++attempt) {
            if (TrySendRemote(record, &error_message)) {
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
            bool spooled = false;
            std::string spool_error;
            {
                std::lock_guard<std::mutex> lock(mutex_);
                stats_.collector_unreachable_count++;
            }
            LOG_EVERY_N(ERROR, 100)
                << "Tracing remote exporter unavailable, span="
                << record.span_name << " error=" << error_message;

            if (!config_.exporter_spool_dir.empty()) {
                spooled = TrySpool(record, &spool_error);
                std::lock_guard<std::mutex> lock(mutex_);
                if (spooled) {
                    stats_.spooled_records++;
                } else {
                    stats_.spool_failures++;
                }
            }

            if (!spooled && fallback_exporter_) {
                fallback_exporter_->Export(record);
                std::lock_guard<std::mutex> lock(mutex_);
                stats_.fallback_count++;
            }

            if (!spooled && !spool_error.empty()) {
                LOG_EVERY_N(ERROR, 100)
                    << "Tracing spool failed, span=" << record.span_name
                    << " error=" << spool_error;
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
