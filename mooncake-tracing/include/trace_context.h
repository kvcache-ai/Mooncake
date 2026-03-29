#pragma once

#include <string>

namespace mooncake::tracing {

struct TraceContext {
    std::string trace_id;
    std::string span_id;
    std::string parent_span_id;
    std::string correlation_id;
    bool context_missing{false};

    bool valid() const { return !trace_id.empty() && !span_id.empty(); }
};

struct TraceCarrier {
    std::string trace_id;
    std::string span_id;
    std::string correlation_id;

    bool empty() const {
        return trace_id.empty() && span_id.empty() && correlation_id.empty();
    }
};

TraceCarrier ToCarrier(const TraceContext& ctx);
TraceContext ChildContextFromCarrier(const TraceCarrier& carrier);
TraceContext RootContext(const std::string& correlation_id = "");

}  // namespace mooncake::tracing
