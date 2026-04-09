#pragma once

#include <optional>
#include <string>
#include <string_view>

#include <ylt/util/tl/expected.hpp>

#include "ha/ha_types.h"

namespace mooncake {
namespace ha {

bool HasExplicitOpLogBackendConfig(std::string_view oplog_backend_type,
                                   std::string_view oplog_backend_connstring);

std::string ResolveConfiguredHABackendConnstring(
    std::string_view ha_backend_connstring, std::string_view etcd_endpoints);

std::string ResolveConfiguredOpLogBackendType(
    std::string_view ha_backend_type, std::string_view oplog_backend_type);

std::string ResolveConfiguredOpLogBackendConnstring(
    std::string_view ha_backend_type, std::string_view ha_backend_connstring,
    std::string_view oplog_backend_type,
    std::string_view oplog_backend_connstring);

std::optional<HABackendType> ParseConfiguredOpLogBackendType(
    std::string_view ha_backend_type, std::string_view oplog_backend_type);

bool ConfiguredOpLogBackendSupportsFollowing(
    std::string_view ha_backend_type, std::string_view ha_backend_connstring,
    std::string_view oplog_backend_type,
    std::string_view oplog_backend_connstring);

tl::expected<HABackendSpec, ErrorCode> BuildConfiguredOpLogBackendSpec(
    std::string_view ha_backend_type, std::string_view ha_backend_connstring,
    std::string_view oplog_backend_type,
    std::string_view oplog_backend_connstring,
    std::string_view cluster_namespace);

}  // namespace ha
}  // namespace mooncake
