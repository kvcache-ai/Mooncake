#include "trace_context.h"

#include <chrono>
#include <random>
#include <sstream>

namespace mooncake::tracing {
namespace {
std::string RandomHex(size_t len) {
    static thread_local std::mt19937_64 rng(std::random_device{}());
    static constexpr char kHex[] = "0123456789abcdef";
    std::string out;
    out.reserve(len);
    for (size_t i = 0; i < len; ++i) {
        out.push_back(kHex[rng() & 0xF]);
    }
    return out;
}
}  // namespace

TraceCarrier ToCarrier(const TraceContext& ctx) {
    return TraceCarrier{ctx.trace_id, ctx.span_id, ctx.correlation_id};
}

std::string EncodeTraceCarrier(const TraceCarrier& carrier) {
    return carrier.trace_id + "|" + carrier.span_id + "|" +
           carrier.correlation_id;
}

TraceCarrier DecodeTraceCarrier(const std::string& encoded) {
    TraceCarrier carrier;
    const auto first_sep = encoded.find('|');
    const auto second_sep =
        encoded.find('|', first_sep == std::string::npos ? first_sep
                                                         : first_sep + 1);
    if (first_sep == std::string::npos || second_sep == std::string::npos) {
        return carrier;
    }
    carrier.trace_id = encoded.substr(0, first_sep);
    carrier.span_id =
        encoded.substr(first_sep + 1, second_sep - first_sep - 1);
    carrier.correlation_id = encoded.substr(second_sep + 1);
    return carrier;
}

TraceContext ChildContextFromCarrier(const TraceCarrier& carrier) {
    if (carrier.empty()) {
        auto root = RootContext();
        root.context_missing = true;
        return root;
    }
    TraceContext ctx;
    ctx.trace_id = carrier.trace_id.empty() ? RandomHex(32) : carrier.trace_id;
    ctx.parent_span_id = carrier.span_id;
    ctx.span_id = RandomHex(16);
    ctx.correlation_id = carrier.correlation_id.empty() ? RandomHex(16) : carrier.correlation_id;
    return ctx;
}

TraceContext RootContext(const std::string& correlation_id) {
    TraceContext ctx;
    ctx.trace_id = RandomHex(32);
    ctx.span_id = RandomHex(16);
    ctx.parent_span_id.clear();
    ctx.correlation_id = correlation_id.empty() ? RandomHex(16) : correlation_id;
    return ctx;
}

}  // namespace mooncake::tracing
