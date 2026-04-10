#pragma once
#include <string>
#include "trace_context.h"

namespace mooncake::tracing {

inline std::string EncodeCarrierString(const TraceCarrier& carrier) {
    return EncodeTraceCarrier(carrier);
}

inline TraceCarrier DecodeCarrierString(const std::string& encoded) {
    return DecodeTraceCarrier(encoded);
}

}  // namespace mooncake::tracing
