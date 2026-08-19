#pragma once

#include <string>
#include <string_view>
#include <optional>
#include <vector>

#include "replica.h"
#include "types.h"
#include "p2p/p2p_types.h"
#include "p2p/heartbeat_type.h"
#include "task_manager.h"
#include <ylt/reflection/user_reflect_macro.hpp>

namespace mooncake {

// =====================================================================
// Write Route Types (from p2p_rpc_types.h)
// =====================================================================

struct WriteRouteRequestConfig {
    static constexpr size_t RETURN_ALL_CANDIDATES = 0;
    size_t max_candidates = 2;
    ObjectIterateStrategy strategy = ObjectIterateStrategy::CAPACITY_PRIORITY;
    double remote_weight = 0.5;
    double local_write_waterline = 0.5;
    bool top_tier_only = true;
    bool early_return = true;
    std::vector<std::string> tag_filters;
    int priority_limit = 0;

    bool IsValid() const {
        const bool no_local_write = (local_write_waterline <= 0.0);
        const bool no_remote_write = (local_write_waterline >= 1.0);
        const bool no_remote_route = (remote_weight <= 0.0);
        const bool no_local_route = (remote_weight >= 1.0);
        return !(no_local_write && no_remote_route) &&
               !(no_remote_write && no_local_route);
    }
};
YLT_REFL(WriteRouteRequestConfig, max_candidates, strategy, remote_weight,
         local_write_waterline, top_tier_only, early_return, tag_filters,
         priority_limit);

inline std::ostream& operator<<(std::ostream& os,
                                const WriteRouteRequestConfig& config) {
    os << "WriteRouteRequestConfig: { max_candidates: " << config.max_candidates
       << ", strategy: " << config.strategy
       << ", remote_weight: " << config.remote_weight
       << ", local_write_waterline: " << config.local_write_waterline
       << ", top_tier_only: " << (config.top_tier_only ? "true" : "false")
       << ", early_return: " << (config.early_return ? "true" : "false")
       << ", priority_limit: " << config.priority_limit << " }";
    return os;
}

struct WriteRouteRequest {
    std::string_view key;
    UUID client_id;
    size_t size = 0;
    WriteRouteRequestConfig config;
};
YLT_REFL(WriteRouteRequest, key, client_id, size, config);

struct WriteCandidate {
    UUID client_id;
    std::string ip_address;
    uint16_t rpc_port = 0;
    size_t available_capacity = 0;
    double score = 0.0;
};
YLT_REFL(WriteCandidate, client_id, ip_address, rpc_port, available_capacity,
         score);

struct WriteRouteResponse {
    std::vector<WriteCandidate> candidates;
};
YLT_REFL(WriteRouteResponse, candidates);

struct BatchGetWriteRouteRequest {
    UUID client_id;
    std::vector<std::string_view> keys;
    std::vector<size_t> sizes;
    WriteRouteRequestConfig config;
};
YLT_REFL(BatchGetWriteRouteRequest, client_id, keys, sizes, config);

struct BatchGetWriteRouteResponse {
    std::vector<WriteRouteResponse> responses;
    std::vector<ErrorCode> error_codes;
};
YLT_REFL(BatchGetWriteRouteResponse, responses, error_codes);

struct AddReplicaRequest {
    std::string_view key;
    size_t size;
    UUID client_id;
    UUID segment_id;
};
YLT_REFL(AddReplicaRequest, key, size, client_id, segment_id);

struct RemoveReplicaRequest {
    std::string_view key;
    UUID client_id;
    UUID segment_id;
};
YLT_REFL(RemoveReplicaRequest, key, client_id, segment_id);

struct BatchRemoveReplicaRequest {
    std::string_view key;
    UUID client_id;
    std::vector<UUID> segment_ids;
};
YLT_REFL(BatchRemoveReplicaRequest, key, client_id, segment_ids);

struct BatchSyncReplicaRequest {
    UUID client_id;
    std::vector<std::string_view> add_keys;
    std::vector<size_t> add_sizes;
    std::vector<UUID> add_segment_ids;
    std::vector<std::string_view> remove_keys;
    std::vector<UUID> remove_segment_ids;
};
YLT_REFL(BatchSyncReplicaRequest, client_id, add_keys, add_sizes,
         add_segment_ids, remove_keys, remove_segment_ids);

struct BatchSyncReplicaResponse {
    std::vector<ErrorCode> add_results;
    std::vector<ErrorCode> remove_results;
};
YLT_REFL(BatchSyncReplicaResponse, add_results, remove_results);

// =====================================================================
// Read Route Types (P2P-only from rpc_types.h)
// =====================================================================

struct P2PGetReplicaListConfigExtra {
    std::vector<std::string> tag_filters;
    int priority_limit = 0;
};
YLT_REFL(P2PGetReplicaListConfigExtra, tag_filters, priority_limit);

struct GetReplicaListRequestConfig {
    GetReplicaListRequestConfig() = default;
    GetReplicaListRequestConfig(size_t max_c) : max_candidates(max_c) {}

    static const size_t RETURN_ALL_CANDIDATES = 0;
    size_t max_candidates = RETURN_ALL_CANDIDATES;
    std::optional<P2PGetReplicaListConfigExtra> p2p_config;
};
YLT_REFL(GetReplicaListRequestConfig, max_candidates, p2p_config);

typedef GetReplicaListRequestConfig ReadRouteConfig;
typedef P2PGetReplicaListConfigExtra P2PReadRouteConfigExtra;

// =====================================================================
// Heartbeat Types (P2P-only from rpc_types.h)
// =====================================================================

struct HeartbeatRequest {
    UUID client_id;
    std::vector<HeartbeatTask> tasks;
};
YLT_REFL(HeartbeatRequest, client_id, tasks);

struct HeartbeatResponse {
    P2PClientStatus status;
    ViewVersionId view_version = 0;
    std::vector<HeartbeatTaskResult> task_results;
};
YLT_REFL(HeartbeatResponse, status, view_version, task_results);

struct DummyHeartbeatResponse {
    DummyClientStatus status = DummyClientStatus::HEALTH;
    uint64_t mapped_shm_count = 0;
};
YLT_REFL(DummyHeartbeatResponse, status, mapped_shm_count);

// =====================================================================
// Register / Unregister Types (P2P-only from rpc_types.h)
// =====================================================================

struct RegisterClientRequest {
    UUID client_id;
    std::vector<Segment> segments;
    DeploymentMode deployment_mode = DeploymentMode::CENTRALIZATION;

    std::optional<std::string> ip_address;
    std::optional<uint16_t> rpc_port;
};
YLT_REFL(RegisterClientRequest, client_id, segments, deployment_mode,
         ip_address, rpc_port);

struct RegisterClientResponse {
    ViewVersionId view_version = 0;
};
YLT_REFL(RegisterClientResponse, view_version);

struct UnregisterClientRequest {
    UUID client_id;
    DeploymentMode deployment_mode = DeploymentMode::CENTRALIZATION;
};
YLT_REFL(UnregisterClientRequest, client_id, deployment_mode);

struct UnregisterClientResponse {
    ViewVersionId view_version = 0;
};
YLT_REFL(UnregisterClientResponse, view_version);

// =====================================================================
// Query Types (P2P-only from rpc_types.h)
// =====================================================================

struct QueryClientStatusRequest {
    UUID client_id;
};
YLT_REFL(QueryClientStatusRequest, client_id);

struct QueryClientStatusResponse {
    P2PClientStatus status = P2PClientStatus::UNDEFINED;
};
YLT_REFL(QueryClientStatusResponse, status);

}  // namespace mooncake