#pragma once

#include <cstddef>
#include <string>
#include <vector>

namespace mooncake::tracing {

struct TraceConfig {
    bool enabled{false};
    std::string exporter_mode{"off"};
    std::string jsonl_path;
    std::vector<std::string> remote_endpoints;
    std::string service_name;
    std::string node_id;
    std::string process_role;
    std::string otlp_http_endpoint;
    std::string otlp_http_path{"/v1/traces"};
    std::string otlp_headers;
    int otlp_timeout_ms{3000};
    size_t exporter_queue_max_items{1024};
    size_t exporter_queue_max_bytes{4 * 1024 * 1024};
    int exporter_retry_base_ms{100};
    int exporter_retry_max_ms{5000};
    int exporter_retry_max_attempts{3};
    std::string exporter_spool_dir;
    std::string sampling_mode{"debug"};
    double sampling_base_ratio{1.0};
    int sampling_slow_threshold_ms{0};
};

TraceConfig LoadTraceConfig(const std::string& service_name,
                            const std::string& process_role = "");

}  // namespace mooncake::tracing
