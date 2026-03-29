#include "trace_exporter.h"

#include <curl/curl.h>
#include <glog/logging.h>

#include <iomanip>
#include <mutex>
#include <sstream>
#include <string>

#if __has_include(<jsoncpp/json/json.h>)
#include <jsoncpp/json/json.h>
#else
#include <json/json.h>
#endif

namespace mooncake::tracing {
namespace {
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
    std::lock_guard<std::mutex> lock(mutex_);
    CURL* curl = curl_easy_init();
    if (!curl) {
        LOG(ERROR) << "OTLP exporter: curl_easy_init failed";
        return;
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
    if (rc != CURLE_OK || code < 200 || code >= 300) {
        LOG(ERROR) << "OTLP exporter POST failed url=" << url
                   << " curl=" << curl_easy_strerror(rc)
                   << " http=" << code << " errbuf=" << errbuf
                   << " body=" << response;
    }

    curl_slist_free_all(header_list);
    curl_easy_cleanup(curl);
}

}  // namespace mooncake::tracing
