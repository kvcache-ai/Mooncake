#include "trace_exporter.h"

#if __has_include(<jsoncpp/json/json.h>)
#include <jsoncpp/json/json.h>
#else
#include <json/json.h>
#endif

#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <unistd.h>

namespace mooncake::tracing {
namespace {
constexpr char kSamplingPriorityAttr[] = "sampling.priority";

Json::Value ToJson(const TraceAttrs& attrs) {
    Json::Value obj(Json::objectValue);
    for (const auto& [k, v] : attrs) {
        if (k == kSamplingPriorityAttr) {
            continue;
        }
        obj[k] = v;
    }
    return obj;
}
Json::Value ToJson(const TraceRecord& record) {
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
    root["attrs"] = ToJson(record.attrs);
    Json::Value events(Json::arrayValue);
    for (const auto& event : record.events) {
        Json::Value e;
        e["name"] = event.name;
        e["unix_nano"] = Json::Int64(event.unix_nano);
        e["attrs"] = ToJson(event.attrs);
        events.append(e);
    }
    root["events"] = events;
    return root;
}
}  // namespace

void InMemoryTraceExporter::Export(const TraceRecord& record) {
    std::lock_guard<std::mutex> lock(mutex_);
    records_.push_back(record);
}
std::vector<TraceRecord> InMemoryTraceExporter::Snapshot() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return records_;
}

JsonlTraceExporter::JsonlTraceExporter(std::string file_path)
    : file_path_(std::move(file_path)) {
    std::filesystem::create_directories(std::filesystem::path(file_path_).parent_path());
}
void JsonlTraceExporter::Export(const TraceRecord& record) {
    std::lock_guard<std::mutex> lock(mutex_);
    std::ofstream out(file_path_, std::ios::app);
    if (!out.is_open()) return;
    Json::StreamWriterBuilder builder;
    builder["indentation"] = "";
    out << Json::writeString(builder, ToJson(record)) << "\n";
}

std::shared_ptr<TraceExporter> MakeDefaultExporter(
    const std::string& service_name) {
    TraceConfig config = LoadTraceConfig(service_name);
    if (!config.enabled || config.exporter_mode == "off") {
        return nullptr;
    }
    if (config.exporter_mode == "inmemory") {
        return std::make_shared<InMemoryTraceExporter>();
    }
    if (config.exporter_mode == "remote" ||
        config.exporter_mode == "otlp_http" ||
        config.exporter_mode == "otlp") {
        return std::make_shared<AsyncRemoteTraceExporter>(
            config, std::make_shared<JsonlTraceExporter>(config.jsonl_path));
    }
    return std::make_shared<JsonlTraceExporter>(config.jsonl_path);
}

}  // namespace mooncake::tracing
