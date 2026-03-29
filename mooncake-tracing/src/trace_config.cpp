#include "trace_config.h"

#include <algorithm>
#include <cstdlib>
#include <string>
#include <unistd.h>

namespace mooncake::tracing {
namespace {
std::string NormalizeOtlpEndpoint(const char* endpoint) {
    if (!endpoint || std::string(endpoint).empty()) {
        return "http://127.0.0.1:4318";
    }
    return endpoint;
}
}  // namespace

TraceConfig LoadTraceConfig(const std::string& service_name,
                            const std::string& process_role) {
    TraceConfig cfg;
    const char* enabled = std::getenv("MC_TRACING_ENABLED");
    cfg.enabled =
        enabled && std::string(enabled) != "0" && std::string(enabled) != "false";
    const char* mode = std::getenv("MC_TRACING_EXPORTER");
    cfg.exporter_mode = mode ? mode : (cfg.enabled ? "jsonl" : "off");
    const char* path = std::getenv("MC_TRACING_JSONL_PATH");
    cfg.jsonl_path =
        path ? path
             : ("/tmp/mooncake-traces/" + service_name + "-" +
                std::to_string(::getpid()) + ".jsonl");
    cfg.service_name = service_name;
    const char* node = std::getenv("MC_TRACING_NODE_ID");
    cfg.node_id = node ? node : "unknown-node";
    cfg.process_role = process_role;

    const char* otlp_endpoint = std::getenv("OTEL_EXPORTER_OTLP_ENDPOINT");
    const char* otlp_traces_endpoint =
        std::getenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT");
    cfg.otlp_http_endpoint = NormalizeOtlpEndpoint(
        otlp_traces_endpoint ? otlp_traces_endpoint : otlp_endpoint);
    cfg.otlp_http_path = "/v1/traces";
    if (otlp_traces_endpoint &&
        std::string(otlp_traces_endpoint).find("/v1/traces") !=
            std::string::npos) {
        cfg.otlp_http_path.clear();
    }
    const char* otlp_headers = std::getenv("OTEL_EXPORTER_OTLP_HEADERS");
    const char* otlp_traces_headers =
        std::getenv("OTEL_EXPORTER_OTLP_TRACES_HEADERS");
    cfg.otlp_headers =
        otlp_traces_headers ? otlp_traces_headers : (otlp_headers ? otlp_headers : "");
    const char* otlp_timeout = std::getenv("OTEL_EXPORTER_OTLP_TIMEOUT");
    const char* otlp_traces_timeout =
        std::getenv("OTEL_EXPORTER_OTLP_TRACES_TIMEOUT");
    const char* timeout_value =
        otlp_traces_timeout ? otlp_traces_timeout : otlp_timeout;
    if (timeout_value) {
        cfg.otlp_timeout_ms = std::max(1, std::atoi(timeout_value));
    }
    return cfg;
}

}  // namespace mooncake::tracing
