#pragma once

#include <string>

namespace mooncake::tracing {

struct TraceConfig {
    bool enabled{false};
    std::string exporter_mode{"off"};
    std::string jsonl_path;
    std::string service_name;
    std::string node_id;
    std::string process_role;
    std::string otlp_http_endpoint;
    std::string otlp_http_path{"/v1/traces"};
    std::string otlp_headers;
    int otlp_timeout_ms{3000};
};

TraceConfig LoadTraceConfig(const std::string& service_name,
                            const std::string& process_role = "");

}  // namespace mooncake::tracing
