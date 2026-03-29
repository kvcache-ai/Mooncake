#pragma once

#include <cstdint>
#include <string>
#include <utility>
#include <vector>

namespace mooncake::tracing {

using TraceAttrs = std::vector<std::pair<std::string, std::string>>;

struct TraceEvent {
    std::string name;
    int64_t unix_nano{0};
    TraceAttrs attrs;
};

struct TraceRecord {
    std::string trace_id;
    std::string span_id;
    std::string parent_span_id;
    std::string correlation_id;
    std::string service_name;
    std::string node_id;
    std::string process_role;
    std::string span_name;
    int64_t start_time_unix_nano{0};
    int64_t end_time_unix_nano{0};
    std::string status{"OK"};
    TraceAttrs attrs;
    std::vector<TraceEvent> events;
};

}  // namespace mooncake::tracing
