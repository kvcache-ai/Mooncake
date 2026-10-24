#pragma once

#include <string>
#include <string_view>
#include <vector>

#include "p2p/client/heartbeat_type.h"
#include "p2p/common/p2p_types.h"
#include <ylt/reflection/user_reflect_macro.hpp>

namespace mooncake {

// Request-side string views reference the caller or coro_rpc request buffer.
// RPC handlers must consume them synchronously and must not retain them.

/**
 * @brief Registration data sent by a P2P client to the P2P master.
 */
struct P2PRegisterClientRequest {
    UUID client_id;
    std::vector<P2PSegment> segments;
    std::string ip_address;
    uint16_t rpc_port{0};
};
YLT_REFL(P2PRegisterClientRequest, client_id, segments, ip_address, rpc_port);

struct P2PHeartbeatRequest {
    UUID client_id;
    std::vector<HeartbeatTask> tasks;
    P2PClientServiceState service_state = P2PClientServiceState::INITIALIZING;
};
YLT_REFL(P2PHeartbeatRequest, client_id, tasks, service_state);

struct P2PHeartbeatResponse {
    P2PClientStatus status = P2PClientStatus::UNDEFINED;
    ViewVersionId view_version = 0;
    std::vector<HeartbeatTaskResult> task_results;
};
YLT_REFL(P2PHeartbeatResponse, status, view_version, task_results);

struct P2PMountSegmentRequest {
    UUID client_id;
    P2PSegment segment;
};
YLT_REFL(P2PMountSegmentRequest, client_id, segment);

struct P2PUnmountSegmentRequest {
    UUID client_id;
    UUID segment_id;
};
YLT_REFL(P2PUnmountSegmentRequest, client_id, segment_id);

struct P2PGetReadRouteRequest {
    std::string_view key;
    P2PReadRouteConfig config;
};
YLT_REFL(P2PGetReadRouteRequest, key, config);

struct P2PBatchGetReadRouteRequest {
    std::vector<std::string_view> keys;
    P2PReadRouteConfig config;
};
YLT_REFL(P2PBatchGetReadRouteRequest, keys, config);

struct P2PBatchGetReadRouteResponse {
    std::vector<std::vector<P2PRouteDescriptor>> responses;
    std::vector<ErrorCode> error_codes;
};
YLT_REFL(P2PBatchGetReadRouteResponse, responses, error_codes);

struct P2PGetWriteRouteRequest {
    std::string_view key;
    UUID client_id;
    uint64_t object_size{0};
    P2PWriteRouteConfig config;
};
YLT_REFL(P2PGetWriteRouteRequest, key, client_id, object_size, config);

struct P2PWriteCandidate {
    UUID client_id;
    std::string ip_address;
    uint16_t rpc_port{0};
    size_t available_capacity{0};
    double score{0.0};
};
YLT_REFL(P2PWriteCandidate, client_id, ip_address, rpc_port, available_capacity,
         score);

struct P2PBatchGetWriteRouteRequest {
    UUID client_id;
    std::vector<std::string_view> keys;
    std::vector<uint64_t> object_sizes;
    P2PWriteRouteConfig config;
};
YLT_REFL(P2PBatchGetWriteRouteRequest, client_id, keys, object_sizes, config);

struct P2PBatchGetWriteRouteResponse {
    std::vector<std::vector<P2PWriteCandidate>> responses;
    std::vector<ErrorCode> error_codes;
};
YLT_REFL(P2PBatchGetWriteRouteResponse, responses, error_codes);

struct P2PPublishRouteRequest {
    std::string_view key;
    uint64_t object_size{0};
    UUID client_id;
    UUID segment_id;
};
YLT_REFL(P2PPublishRouteRequest, key, object_size, client_id, segment_id);

struct P2PWithdrawRouteRequest {
    std::string_view key;
    UUID client_id;
    UUID segment_id;
};
YLT_REFL(P2PWithdrawRouteRequest, key, client_id, segment_id);

struct P2PBatchWithdrawRouteRequest {
    std::string_view key;
    UUID client_id;
    std::vector<UUID> segment_ids;
};
YLT_REFL(P2PBatchWithdrawRouteRequest, key, client_id, segment_ids);

struct P2PBatchSyncRoutesRequest {
    UUID client_id;
    std::vector<P2PPublishRouteOperation> publish_operations;
    std::vector<P2PWithdrawRouteOperation> withdraw_operations;
};
YLT_REFL(P2PBatchSyncRoutesRequest, client_id, publish_operations,
         withdraw_operations);

struct P2PBatchSyncRoutesResponse {
    std::vector<ErrorCode> publish_results;
    std::vector<ErrorCode> withdraw_results;
};
YLT_REFL(P2PBatchSyncRoutesResponse, publish_results, withdraw_results);

}  // namespace mooncake
