#ifndef MOONCAKE_PG_CONTROL_PLANE_RPC_H
#define MOONCAKE_PG_CONTROL_PLANE_RPC_H

#include <cstdint>
#include <string>
#include <variant>
#include <vector>

#include <csignal>

#include <ylt/coro_rpc/coro_rpc_server.hpp>

#include "types.h"

namespace mooncake {

// Agent -> Coordinator RPC messages

struct RegisterRequest {
    GlobalRank rank = kInvalidGlobalRank;
    std::string agent_addr;
    std::string te_server_name;
    uint64_t agent_session_epoch = 0;
    uint64_t warmup_recv_addr = 0;  // local warmup recv region for TE handshake
};

// Process-level connection metadata for a remote rank.
// Returned in RegisterResponse so the Agent can feed LinkManager.
struct RankConnectionMetadata {
    GlobalRank rank = kInvalidGlobalRank;
    uint64_t agent_session_epoch = 0;
    std::string agent_addr;
    std::string te_server_name;
    // Warmup region addresses for LinkManager handshake.
    uint64_t warmup_recv_addr = 0;
};

struct RegisterResponse {
    bool success = false;
    std::string error_msg;
    std::vector<uint8_t> all_rank_states;
    std::vector<GroupDescriptor> group_descriptors;
    std::vector<GroupView> current_views;
    std::vector<RankConnectionMetadata> rank_connections;
};

struct HeartbeatRequest {
    GlobalRank rank = kInvalidGlobalRank;
    uint64_t agent_session_epoch = 0;
    std::vector<uint8_t> link_status;
};

struct HeartbeatResponse {
    bool acknowledge = false;
    bool require_reregister = false;
};

struct DeclareGroupRequest {
    GlobalRank rank = kInvalidGlobalRank;
    uint64_t agent_session_epoch = 0;
    GroupDeclaration group;
};

struct DeclareGroupResponse {
    bool success = false;
    GroupView current_view;
    std::string reject_reason;
};

enum class ViewUpdateStatus : uint8_t {
    Rejected = 0,
    Applied = 1,
    AppliedWithDroppedRanks = 2,
};

struct ProposeViewUpdateRequest {
    GroupId group_id = 0;
    GlobalRank source_rank = kInvalidGlobalRank;
    uint64_t agent_session_epoch = 0;
    std::vector<GlobalRank> requested_ranks;  // ranks to activate/deactivate
    bool is_activate = false;
};

struct ProposeViewUpdateResponse {
    ViewUpdateStatus status = ViewUpdateStatus::Rejected;
    uint64_t new_epoch = kInvalidEpoch;
    std::vector<GlobalRank> dropped_ranks;
    std::string reject_reason;
};

// Per-(group_id, rank) endpoint publication unit.
// agent_session_epoch is NOT included here  - the Host fills it in the
// enclosing PublishEndpointRequest before sending the RPC.
struct GroupEndpointPublication {
    GroupId group_id = 0;
    uint64_t endpoint_epoch = kInvalidEpoch;
    GroupEndpointInfo endpoint_info;
};

struct PublishEndpointRequest {
    GlobalRank rank = kInvalidGlobalRank;
    uint64_t agent_session_epoch = 0;
    std::vector<GroupEndpointPublication> endpoints;
};

struct PublishEndpointResponse {
    bool success = false;
    std::string reject_reason;
};

struct TransferObservationReport {
    GroupId group_id = 0;
    GlobalRank reporter_rank = kInvalidGlobalRank;
    uint64_t agent_session_epoch = 0;
    std::vector<uint8_t> attempted_ranks;
    std::vector<uint8_t> failed_ranks;
    std::vector<uint8_t> succeeded_ranks;
};

// Coordinator -> Agent RPC messages

struct PeerJoinedPush {
    GlobalRank rank = kInvalidGlobalRank;
    std::string te_server_name;
    uint64_t warmup_recv_addr = 0;
};

struct RankStateUpdatePush {
    GlobalRank rank = kInvalidGlobalRank;
    uint8_t new_state = 0;
};

struct ViewUpdatePush {
    GroupId group_id = 0;
    GroupDescriptor descriptor;
    GroupView view;
};

struct ViewUpdateAck {
    GlobalRank rank = kInvalidGlobalRank;
    GroupId group_id = 0;
    uint64_t epoch = kInvalidEpoch;
    bool applied = false;
    std::string error_msg;
};

// Effects produced by the Coordinator/Agent state machine

struct EffectTarget {
    enum class Kind { BroadcastOnline, Unicast } kind = Kind::BroadcastOnline;
    GlobalRank rank = kInvalidGlobalRank;
};

// Coordinator effects

template <typename Push>
struct PushEffect {
    EffectTarget target;
    Push push;
};

// Route for ViewUpdate ACKs.  Bootstrap ACKs always go to the bootstrap 2PC
// handler; proposal ACKs additionally go to the proposal 2PC handler and carry
// the propose_id that originated the push.
struct BootstrapAckRoute {};
struct ProposalAckRoute {
    uint64_t propose_id = 0;
};
using ViewUpdateAckRoute = std::variant<BootstrapAckRoute, ProposalAckRoute>;

struct ViewUpdateEffect {
    GroupDescriptor descriptor;
    GroupView view;
    std::vector<GlobalRank> required_acks;
    ViewUpdateAckRoute ack_route = BootstrapAckRoute{};
};

struct ReplyViewUpdateEffect {
    uint64_t propose_id = 0;
    ProposeViewUpdateResponse response;
};

using CoordinatorEffect =
    std::variant<PushEffect<RankStateUpdatePush>, ViewUpdateEffect,
                 ReplyViewUpdateEffect, PushEffect<PeerJoinedPush>>;

// Agent effects

struct SendTransferObservation {
    TransferObservationReport request;
};

struct EnablePeerProbe {
    GlobalRank rank = kInvalidGlobalRank;
    std::string te_server_name;
    uint64_t warmup_recv_addr = 0;
};

struct DisconnectLink {
    GlobalRank peer = kInvalidGlobalRank;
};

struct StopReconnect {
    GlobalRank peer = kInvalidGlobalRank;
};

struct ClearPeerMetadata {
    GlobalRank peer = kInvalidGlobalRank;
};

struct DisconnectAllLinks {};

struct ClearAllPeerMetadata {};

struct PublishRankStateSnapshot {
    std::vector<uint8_t> states;
};

struct ApplyViewToBackend {
    GroupId group_id = 0;
    GroupDescriptor descriptor;
    GroupView view;
};

struct MarkBackendViewStale {
    GroupId group_id = 0;
};

struct NotifyTEUnreachable {
    GlobalRank peer = kInvalidGlobalRank;
};

using AgentEffect =
    std::variant<SendTransferObservation, EnablePeerProbe, DisconnectLink,
                 StopReconnect, ClearPeerMetadata, DisconnectAllLinks,
                 ClearAllPeerMetadata, PublishRankStateSnapshot,
                 ApplyViewToBackend, MarkBackendViewStale, NotifyTEUnreachable>;

// Results produced by the Coordinator/Agent state machine

template <typename Response>
struct CoordinatorApplyResult {
    Response response;
    std::vector<CoordinatorEffect> effects;
};

template <>
struct CoordinatorApplyResult<void> {
    std::vector<CoordinatorEffect> effects;
};

using AgentApplyResult = std::vector<AgentEffect>;

// RPC Service interfaces

class CoordinatorRpcService {
   public:
    virtual ~CoordinatorRpcService() = default;

    virtual void registerAgent(coro_rpc::context<RegisterResponse> ctx,
                               RegisterRequest req) = 0;
    virtual void heartbeat(coro_rpc::context<HeartbeatResponse> ctx,
                           HeartbeatRequest req) = 0;
    virtual void declareGroup(coro_rpc::context<DeclareGroupResponse> ctx,
                              DeclareGroupRequest req) = 0;
    virtual void proposeViewUpdate(
        coro_rpc::context<ProposeViewUpdateResponse> ctx,
        ProposeViewUpdateRequest req) = 0;
    virtual void publishEndpoint(coro_rpc::context<PublishEndpointResponse> ctx,
                                 PublishEndpointRequest req) = 0;
    virtual void reportTransferObservation(TransferObservationReport req) = 0;
};

class AgentRpcService {
   public:
    virtual ~AgentRpcService() = default;

    virtual void onPeerJoined(PeerJoinedPush push) = 0;
    virtual void onRankStateUpdate(RankStateUpdatePush push) = 0;
    virtual void onViewUpdate(coro_rpc::context<ViewUpdateAck> ctx,
                              ViewUpdatePush push) = 0;
};

}  // namespace mooncake

#endif  // MOONCAKE_PG_CONTROL_PLANE_RPC_H
