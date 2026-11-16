#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>

#include "conductor/prefixindex/cache_layout.h"
#include "conductor/zmq/event_type.h"

namespace mooncake::conductor::prefixindex {

// Producer-specific layout parsers and event assertions share the common
// CacheLayout representation used by the grouped matcher.

// Shared layout interpretation for vLLM and vLLM-Ascend. The current complete
// deployment declaration uses the connector's ascend-store-v1 wire schema;
// recovery policies remain constrained by the built-in model catalog.
std::string ResolveVllmDeployment(const common::ServiceConfig& service,
                                  std::string_view declaration,
                                  SourceLayout* result);

// Ordinary contexts accept only the default full-prefix cache group.
std::string ValidateVllmGroup(std::optional<int64_t> group);
std::string ValidateVllmStoredAssertions(const zmq::VllmStoredEvent& event,
                                         const common::ServiceConfig& service);

// The caller resolves the group and medium before checking its layout.
std::string ValidateVllmGroupedStoredAssertions(
    const zmq::VllmStoredEvent& event, const common::ServiceConfig& service,
    const CacheLayout& layout, const CacheGroupLayout& group, StorageTier tier);

}  // namespace mooncake::conductor::prefixindex
