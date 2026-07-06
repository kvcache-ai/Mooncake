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

struct RegisterAgentRequest {
    GlobalRank rank = kInvalidGlobalRank;
    std::string agent_addr;
    std::string te_server_name;
    uint64_t agent_session_epoch = 0;
    uint64_t warmup_recv_addr = 0;  // local warmup recv region for TE handshake
};

// Process-level connection metadata for a remote rank.
// Returned in RegisterAgentResponse so the Agent can feed LinkManager.
struct RankConnectionMetadata {
    GlobalRank rank = kInvalidGlobalRank;
    std::string agent_addr;
    std::string te_server_name;
    // Warmup region addresses for LinkManager handshake.
    uint64_t warmup_recv_addr = 0;
};

struct RegisterAgentResponse {
    bool success = false;
    std::string reject_reason;
    std::vector<RankState> all_rank_states;
    std::vector<GroupView> groups;
    std::vector<RankConnectionMetadata> rank_connections;
};

struct HeartbeatRequest {
    GlobalRank rank = kInvalidGlobalRank;
    uint64_t agent_session_epoch = 0;
};

struct HeartbeatResponse {
    bool acknowledge = false;
    bool require_reregister = false;
};

struct RegisterGroupRequest {
    GlobalRank rank = kInvalidGlobalRank;
    uint64_t agent_session_epoch = 0;
    GroupView group;
};

struct RegisterGroupResponse {
    bool success = false;
    std::string reject_reason;
};

enum class ViewUpdateStatus : uint8_t {
    Rejected = 0,
    Applied = 1,
    AppliedWithDroppedRanks = 2,
};

struct ProposeViewUpdateRequest {
    GroupId group_id;
    GlobalRank source_rank = kInvalidGlobalRank;
    uint64_t agent_session_epoch = 0;
    std::vector<GlobalRank> requested_ranks;  // ranks to activate/deactivate
    bool is_activation = false;
};

struct ProposeViewUpdateResponse {
    ViewUpdateStatus status = ViewUpdateStatus::Rejected;
    uint64_t new_epoch = 0;
    std::vector<GlobalRank> dropped_ranks;
    std::string reject_reason;
};

// Per-(group_id, rank) endpoint publication unit.
// agent_session_epoch is NOT included here  - the Host fills it in the
// enclosing PublishEndpointRequest before sending the RPC.
struct GroupEndpointPublication {
    GroupId group_id;
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

struct UnregisterGroupRequest {
    GroupId group_id;
    GlobalRank rank = kInvalidGlobalRank;
    uint64_t agent_session_epoch = 0;
};

struct TransferObservationReport {
    GlobalRank reporter_rank = kInvalidGlobalRank;
    uint64_t agent_session_epoch = 0;
    std::vector<uint8_t> attempted_ranks;
    std::vector<uint8_t> failed_ranks_hint;
    std::vector<uint8_t> succeeded_ranks;
};

// Agent -> Coordinator: per-peer link state change, triggered by LinkManager
// LinkUp/LinkDown events.  This is event-driven (not periodic) so it cannot
// overwrite transfer-observation data with a stale snapshot.
struct LinkStateChangeReport {
    GlobalRank reporter_rank = kInvalidGlobalRank;
    GlobalRank peer = kInvalidGlobalRank;
    bool is_up = false;
    uint64_t agent_session_epoch = 0;
};

// Agent -> Coordinator: sync-after-failure RPC.  The caller has observed a
// failure and wants the Coordinator to make a membership decision.  The request
// may piggyback a transfer observation if one was pending locally; otherwise
// the observation was already sent asynchronously.
struct SyncAfterFailureRequest {
    GroupId group_id;
    GlobalRank reporter_rank = kInvalidGlobalRank;
    uint64_t agent_session_epoch = 0;
    uint64_t current_epoch = 0;  // Agent's local GroupView epoch at call time

    // Piggybacked observation.
    std::optional<TransferObservationReport> observation;
};

enum class SyncAfterFailureStatus : uint8_t {
    DecisionApplied =
        0,         // Decision made and applied (ViewUpdate ACKed by caller)
    NoChange = 1,  // No pending decision, epoch matches, no window open
    Rejected = 2,  // Invalid request (stale session, group not found, etc.)
};

struct SyncAfterFailureResponse {
    SyncAfterFailureStatus status = SyncAfterFailureStatus::NoChange;
    uint64_t new_epoch = 0;
    std::string reject_reason;
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
    GroupId group_id;
    GroupView view;
};

struct ViewUpdateAck {
    GlobalRank rank = kInvalidGlobalRank;
    GroupId group_id;
    uint64_t epoch = 0;
    bool applied = false;
    std::string error_msg;
};

// Coordinator effects

// ViewUpdateEffect tells the Host to push a view update to the group members.
// Every online member receives the push via callAsync and ACKs back through
// handleViewUpdateAck.  The State Machine matches ACKs to pending barriers
// (proposal / bootstrap) and sync-after-failure waiters.
struct ViewUpdateEffect {
    GroupView view;
};

struct ReplyProposalEffect {
    uint64_t propose_id = 0;
    ProposeViewUpdateResponse response;
};

struct ReplySyncEffect {
    uint64_t sync_id = 0;
    SyncAfterFailureResponse response;
};

using CoordinatorEffect =
    std::variant<RankStateUpdatePush, ViewUpdateEffect, ReplyProposalEffect,
                 PeerJoinedPush, ReplySyncEffect>;

// Agent effects

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

struct ApplyViewToBackend {
    GroupId group_id;
    GroupView view;
};

struct NotifyTEUnreachable {
    GlobalRank peer = kInvalidGlobalRank;
};

struct RefreshPeerLink {
    GlobalRank peer = kInvalidGlobalRank;
};

struct NotifyGroupReady {
    GroupId group_id;
};

struct NotifyRanksActivated {
    GroupId group_id;
    std::vector<GlobalRank> ranks;
};

using AgentEffect =
    std::variant<EnablePeerProbe, DisconnectLink, StopReconnect,
                 ClearPeerMetadata, DisconnectAllLinks, ClearAllPeerMetadata,
                 ApplyViewToBackend, NotifyTEUnreachable, RefreshPeerLink,
                 NotifyGroupReady, NotifyRanksActivated>;

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

    virtual void registerAgent(coro_rpc::context<RegisterAgentResponse> ctx,
                               RegisterAgentRequest req) = 0;
    virtual void heartbeat(coro_rpc::context<HeartbeatResponse> ctx,
                           HeartbeatRequest req) = 0;
    virtual void registerGroup(coro_rpc::context<RegisterGroupResponse> ctx,
                               RegisterGroupRequest req) = 0;
    virtual void unregisterGroup(UnregisterGroupRequest req) = 0;
    virtual void proposeViewUpdate(
        coro_rpc::context<ProposeViewUpdateResponse> ctx,
        ProposeViewUpdateRequest req) = 0;
    virtual void publishEndpoint(coro_rpc::context<PublishEndpointResponse> ctx,
                                 PublishEndpointRequest req) = 0;
    virtual void reportLinkStateChange(LinkStateChangeReport req) = 0;
    virtual void reportTransferObservation(TransferObservationReport req) = 0;
    virtual void syncAfterFailure(
        coro_rpc::context<SyncAfterFailureResponse> ctx,
        SyncAfterFailureRequest req) = 0;
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
