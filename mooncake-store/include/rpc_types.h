#pragma once

#include "types.h"
#include "replica.h"
#include <ylt/easylog.hpp>

namespace mooncake {

/**
 * @brief Response structure for Ping operation
 */
struct PingResponse {
    ViewVersionId view_version_id;
    ClientStatus client_status;

    PingResponse() = default;
    PingResponse(ViewVersionId view_version, ClientStatus status)
        : view_version_id(view_version), client_status(status) {}

    friend std::ostream& operator<<(std::ostream& os,
                                    const PingResponse& response) noexcept {
        return os << "PingResponse: { view_version_id: "
                  << response.view_version_id
                  << ", client_status: " << response.client_status << " }";
    }
};
YLT_REFL(PingResponse, view_version_id, client_status);

/**
 * @brief Response structure for GetReplicaList operation
 */
struct GetReplicaListResponse {
    std::vector<Replica::Descriptor> replicas;
    uint64_t lease_ttl_ms;

    GetReplicaListResponse() : lease_ttl_ms(0) {}
    GetReplicaListResponse(std::vector<Replica::Descriptor>&& replicas_param,
                           uint64_t lease_ttl_ms_param)
        : replicas(std::move(replicas_param)),
          lease_ttl_ms(lease_ttl_ms_param) {}
};
YLT_REFL(GetReplicaListResponse, replicas, lease_ttl_ms);

/**
 * @brief Response structure for GetStorageConfig operation
 */
struct GetStorageConfigResponse {
    std::string fsdir;
    bool enable_disk_eviction;
    uint64_t quota_bytes;

    GetStorageConfigResponse() : enable_disk_eviction(true), quota_bytes(0) {}
    GetStorageConfigResponse(const std::string& fsdir_param,
                             bool enable_eviction, uint64_t quota)
        : fsdir(fsdir_param),
          enable_disk_eviction(enable_eviction),
          quota_bytes(quota) {}
};
YLT_REFL(GetStorageConfigResponse, fsdir, enable_disk_eviction, quota_bytes);

// usage: export MC_YLT_LOG_LEVEL=info or export MC_YLT_LOG_LEVEL=debug etc.
inline void init_ylt_log_level() {
    const char* env_level = std::getenv("MC_YLT_LOG_LEVEL");
    if (!env_level || !*env_level) {
        // default is WARN
        easylog::set_min_severity(easylog::Severity::WARN);
        return;
    }
    std::string level_str(env_level);
    std::transform(level_str.begin(), level_str.end(), level_str.begin(),
                   [](unsigned char c) { return std::tolower(c); });
    easylog::Severity severity;
    if (level_str == "trace") {
        severity = easylog::Severity::TRACE;
    } else if (level_str == "debug") {
        severity = easylog::Severity::DEBUG;
    } else if (level_str == "info") {
        severity = easylog::Severity::INFO;
    } else if (level_str == "warn" || level_str == "warning") {
        severity = easylog::Severity::WARN;
    } else if (level_str == "error") {
        severity = easylog::Severity::ERROR;
    } else if (level_str == "critical") {
        severity = easylog::Severity::CRITICAL;
    } else {
        // rollback to WARN
        severity = easylog::Severity::WARN;
    }

    easylog::set_min_severity(severity);
}

}  // namespace mooncake