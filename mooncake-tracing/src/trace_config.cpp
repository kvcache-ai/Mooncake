#include "trace_config.h"

#include <algorithm>
#include <cctype>
#include <cstdlib>
#include <sstream>
#include <string>
#include <unistd.h>

namespace mooncake::tracing {
namespace {
std::string BuildLocalTraceFilePath(const std::string& service_name,
                                    const std::string& node_id,
                                    const std::string& suffix) {
    return "/tmp/mooncake-traces/" + service_name + "-" + node_id + "-" +
           std::to_string(::getpid()) + suffix;
}

std::string NormalizeOtlpEndpoint(const char* endpoint) {
    if (!endpoint || std::string(endpoint).empty()) {
        return "http://127.0.0.1:4318";
    }
    return endpoint;
}

std::vector<std::string> SplitCsv(const char* value) {
    std::vector<std::string> result;
    if (!value || !*value) {
        return result;
    }
    std::stringstream ss(value);
    std::string item;
    while (std::getline(ss, item, ',')) {
        if (!item.empty()) {
            result.push_back(item);
        }
    }
    return result;
}

std::string ToLowerCopy(std::string value) {
    std::transform(value.begin(), value.end(), value.begin(),
                   [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
    return value;
}

size_t ParseSizeTEnv(const char* value, size_t default_value) {
    if (!value || !*value) {
        return default_value;
    }
    return static_cast<size_t>(std::strtoull(value, nullptr, 10));
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
    cfg.service_name = service_name;
    const char* node = std::getenv("MC_TRACING_NODE_ID");
    cfg.node_id = node ? node : "unknown-node";
    cfg.process_role = process_role;
    const char* path = std::getenv("MC_TRACING_JSONL_PATH");
    cfg.jsonl_path = path ? path
                          : BuildLocalTraceFilePath(service_name, cfg.node_id,
                                                    ".jsonl");

    const char* otlp_endpoint = std::getenv("OTEL_EXPORTER_OTLP_ENDPOINT");
    const char* otlp_traces_endpoint =
        std::getenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT");
    cfg.otlp_http_endpoint = NormalizeOtlpEndpoint(
        otlp_traces_endpoint ? otlp_traces_endpoint : otlp_endpoint);
    cfg.remote_endpoints.push_back(cfg.otlp_http_endpoint);
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

    const char* remote_endpoints = std::getenv("MC_TRACING_REMOTE_ENDPOINTS");
    auto parsed_endpoints = SplitCsv(remote_endpoints);
    if (!parsed_endpoints.empty()) {
        cfg.remote_endpoints = std::move(parsed_endpoints);
    }

    cfg.exporter_queue_max_items = ParseSizeTEnv(
        std::getenv("MC_TRACING_EXPORTER_QUEUE_MAX_ITEMS"),
        cfg.exporter_queue_max_items);
    cfg.exporter_queue_max_bytes = ParseSizeTEnv(
        std::getenv("MC_TRACING_EXPORTER_QUEUE_MAX_BYTES"),
        cfg.exporter_queue_max_bytes);
    if (const char* retry_base =
            std::getenv("MC_TRACING_EXPORTER_RETRY_BASE_MS")) {
        cfg.exporter_retry_base_ms = std::max(1, std::atoi(retry_base));
    }
    if (const char* retry_max =
            std::getenv("MC_TRACING_EXPORTER_RETRY_MAX_MS")) {
        cfg.exporter_retry_max_ms = std::max(1, std::atoi(retry_max));
    }
    if (const char* retry_attempts =
            std::getenv("MC_TRACING_EXPORTER_RETRY_MAX_ATTEMPTS")) {
        cfg.exporter_retry_max_attempts =
            std::max(0, std::atoi(retry_attempts));
    }
    if (const char* spool_dir =
            std::getenv("MC_TRACING_EXPORTER_SPOOL_DIR")) {
        cfg.exporter_spool_dir = spool_dir;
    }

    if (const char* sampling_mode = std::getenv("MC_TRACING_SAMPLING_MODE")) {
        cfg.sampling_mode = ToLowerCopy(sampling_mode);
    }
    if (const char* sampling_ratio =
            std::getenv("MC_TRACING_SAMPLING_BASE_RATIO")) {
        cfg.sampling_base_ratio = std::clamp(std::atof(sampling_ratio), 0.0, 1.0);
    }
    if (const char* slow_threshold =
            std::getenv("MC_TRACING_SLOW_THRESHOLD_MS")) {
        cfg.sampling_slow_threshold_ms = std::max(0, std::atoi(slow_threshold));
    }
    return cfg;
}

}  // namespace mooncake::tracing
