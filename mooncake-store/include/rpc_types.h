#pragma once

#include "types.h"
#include "replica.h"
#include "heartbeat_type.h"

namespace mooncake {

/**
 * @brief P2P specific configuration for read route
 */
struct P2PGetReplicaListConfigExtra {
    // exclude replicas whose segment contains any tag in tag_filters
    std::vector<std::string> tag_filters;
    // filter replicas whose segment priority is lower than priority_limit
    int priority_limit = 0;
};
YLT_REFL(P2PGetReplicaListConfigExtra, tag_filters, priority_limit);

/**
 * @brief Request config for getting replica list
 */
struct GetReplicaListRequestConfig {
    GetReplicaListRequestConfig() = default;
    GetReplicaListRequestConfig(size_t max_c) : max_candidates(max_c) {}

    // 0 means return all viable candidates;
    // otherwise, return at most max_candidates candidates
    static const size_t RETURN_ALL_CANDIDATES = 0;
    size_t max_candidates = RETURN_ALL_CANDIDATES;
    std::optional<P2PGetReplicaListConfigExtra> p2p_config;
};
YLT_REFL(GetReplicaListRequestConfig, max_candidates, p2p_config);

/**
 * @brief Extra info for centralized read route response (Internal use)
 */
struct CentralizedGetReplicaListResponseExtra {
    CentralizedGetReplicaListResponseExtra() = default;
    CentralizedGetReplicaListResponseExtra(uint64_t lease_ttl_ms_param)
        : lease_ttl_ms(lease_ttl_ms_param) {}
    uint64_t lease_ttl_ms = 0;
};
YLT_REFL(CentralizedGetReplicaListResponseExtra, lease_ttl_ms);

/**
 * @brief Response structure for GetReplicaList operation
 */
struct GetReplicaListResponse {
    GetReplicaListResponse() = default;
    GetReplicaListResponse(std::vector<Replica::Descriptor>&& replicas_param,
                           uint64_t lease_ttl_ms_param)
        : replicas(std::move(replicas_param)),
          centralized_extra(lease_ttl_ms_param) {}

    std::vector<Replica::Descriptor> replicas;
    std::optional<CentralizedGetReplicaListResponseExtra> centralized_extra;
};
YLT_REFL(GetReplicaListResponse, replicas, centralized_extra);

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

/**
 * @brief Request structure for Heartbeat operation.
 * Client could set HeartbeatTasks for Master to run
 */
struct HeartbeatRequest {
    UUID client_id;
    std::vector<HeartbeatTask> tasks;
};
YLT_REFL(HeartbeatRequest, client_id, tasks);

/**
 * @brief Response structure for Heartbeat operation.
 * Always returns view_version; client uses it under UNDEFINED status
 * for crash-recovery decisions, other statuses for defensive checks.
 */
struct HeartbeatResponse {
    ClientStatus status;
    ViewVersionId view_version = 0;
    std::vector<HeartbeatTaskResult> task_results;
};
YLT_REFL(HeartbeatResponse, status, view_version, task_results);

/**
 * @brief Request structure for RegisterClient operation.
 * Client calls this on startup to register its UUID and local segments.
 * P2P clients additionally provide ip_address and rpc_port.
 */
struct RegisterClientRequest {
    UUID client_id;
    std::vector<Segment> segments;
    // P2P only: network endpoint info
    std::optional<std::string> ip_address;
    std::optional<uint16_t> rpc_port;
};
YLT_REFL(RegisterClientRequest, client_id, segments, ip_address, rpc_port);

/**
 * @brief Response structure for RegisterClient operation.
 * Returns the master's view_version to client for crash checking.
 */
struct RegisterClientResponse {
    ViewVersionId view_version = 0;
};
YLT_REFL(RegisterClientResponse, view_version);

}  // namespace mooncake