#pragma once

#include <vector>

#include "rpc_types.h"
#include "types.h"

namespace mooncake {

template <typename Service>
auto RegisterNewSegmentsForTest(Service& service,
                                const std::vector<Segment>& segments,
                                const UUID& client_id) {
    UpdateSegmentsRequest request;
    request.client_id = client_id;
    request.request_intent = SegmentUpdateRequestIntent::REGISTER;
    request.segments.reserve(segments.size());
    for (const auto& segment : segments) {
        request.segments.emplace_back(segment, SegmentRegistrationIntent::NEW);
    }
    return service.UpdateSegments(request);
}

template <typename Service>
auto RegisterNewSegmentForTest(Service& service, const Segment& segment,
                               const UUID& client_id) {
    return RegisterNewSegmentsForTest(service, std::vector<Segment>{segment},
                                      client_id);
}

}  // namespace mooncake
