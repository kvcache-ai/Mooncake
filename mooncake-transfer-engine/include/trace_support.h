#pragma once
#include <string>
#include "trace_context.h"

namespace mooncake::tracing {

inline std::string EncodeCarrierString(const TraceCarrier& carrier) {
    return carrier.trace_id + "|" + carrier.span_id + "|" + carrier.correlation_id;
}

inline TraceCarrier DecodeCarrierString(const std::string& encoded) {
    TraceCarrier carrier;
    auto p1 = encoded.find('|');
    auto p2 = encoded.find('|', p1 == std::string::npos ? p1 : p1 + 1);
    if (p1 == std::string::npos || p2 == std::string::npos) return carrier;
    carrier.trace_id = encoded.substr(0, p1);
    carrier.span_id = encoded.substr(p1 + 1, p2 - p1 - 1);
    carrier.correlation_id = encoded.substr(p2 + 1);
    return carrier;
}

}  // namespace mooncake::tracing
