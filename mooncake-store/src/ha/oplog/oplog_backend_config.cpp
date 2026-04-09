#include "ha/oplog/oplog_backend_config.h"

#include "ha/oplog/oplog_store_factory.h"

namespace mooncake {
namespace ha {

namespace {

bool MatchesResolvedHABackend(std::string_view ha_backend_type,
                              std::string_view oplog_backend_type) {
    if (oplog_backend_type.empty() || oplog_backend_type == ha_backend_type) {
        return true;
    }

    const auto parsed_ha = ParseHABackendType(ha_backend_type);
    const auto parsed_oplog = ParseHABackendType(oplog_backend_type);
    return parsed_ha.has_value() && parsed_oplog.has_value() &&
           parsed_ha.value() == parsed_oplog.value();
}

}  // namespace

bool HasExplicitOpLogBackendConfig(std::string_view oplog_backend_type,
                                   std::string_view oplog_backend_connstring) {
    return !oplog_backend_type.empty() || !oplog_backend_connstring.empty();
}

std::string ResolveConfiguredHABackendConnstring(
    std::string_view ha_backend_connstring, std::string_view etcd_endpoints) {
    if (!ha_backend_connstring.empty()) {
        return std::string(ha_backend_connstring);
    }
    return std::string(etcd_endpoints);
}

std::string ResolveConfiguredOpLogBackendType(
    std::string_view ha_backend_type, std::string_view oplog_backend_type) {
    if (!oplog_backend_type.empty()) {
        return std::string(oplog_backend_type);
    }
    return std::string(ha_backend_type);
}

std::string ResolveConfiguredOpLogBackendConnstring(
    std::string_view ha_backend_type, std::string_view ha_backend_connstring,
    std::string_view oplog_backend_type,
    std::string_view oplog_backend_connstring) {
    if (!oplog_backend_connstring.empty()) {
        return std::string(oplog_backend_connstring);
    }
    if (MatchesResolvedHABackend(ha_backend_type, oplog_backend_type)) {
        return std::string(ha_backend_connstring);
    }
    return "";
}

std::optional<HABackendType> ParseConfiguredOpLogBackendType(
    std::string_view ha_backend_type, std::string_view oplog_backend_type) {
    return ParseHABackendType(
        ResolveConfiguredOpLogBackendType(ha_backend_type, oplog_backend_type));
}

bool ConfiguredOpLogBackendSupportsFollowing(
    std::string_view ha_backend_type, std::string_view ha_backend_connstring,
    std::string_view oplog_backend_type,
    std::string_view oplog_backend_connstring) {
    const auto backend_type =
        ParseConfiguredOpLogBackendType(ha_backend_type, oplog_backend_type);
    if (!backend_type.has_value()) {
        return false;
    }

    const auto connstring = ResolveConfiguredOpLogBackendConnstring(
        ha_backend_type, ha_backend_connstring, oplog_backend_type,
        oplog_backend_connstring);
    return SupportsOpLogFollowing(*backend_type) && !connstring.empty();
}

tl::expected<HABackendSpec, ErrorCode> BuildConfiguredOpLogBackendSpec(
    std::string_view ha_backend_type, std::string_view ha_backend_connstring,
    std::string_view oplog_backend_type,
    std::string_view oplog_backend_connstring,
    std::string_view cluster_namespace) {
    const auto backend_type =
        ParseConfiguredOpLogBackendType(ha_backend_type, oplog_backend_type);
    if (!backend_type.has_value()) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    return HABackendSpec{
        .type = backend_type.value(),
        .connstring = ResolveConfiguredOpLogBackendConnstring(
            ha_backend_type, ha_backend_connstring, oplog_backend_type,
            oplog_backend_connstring),
        .cluster_namespace = std::string(cluster_namespace),
    };
}

}  // namespace ha
}  // namespace mooncake
