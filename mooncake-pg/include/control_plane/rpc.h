#ifndef MOONCAKE_PG_CONTROL_PLANE_RPC_H
#define MOONCAKE_PG_CONTROL_PLANE_RPC_H

#include <chrono>
#include <cstdint>
#include <string>
#include <variant>
#include <vector>

#include <csignal>

#include <ylt/coro_rpc/coro_rpc_server.hpp>

#include "control_plane/control_types.h"

namespace mooncake {

inline constexpr auto kProposalAdmissionTimeout = std::chrono::seconds(20);
inline constexpr auto kViewUpdateAckTimeout = std::chrono::seconds(20);

// Agent -> Coordinator RPC messages

struct RegisterAgentRequest {
    GlobalRank rank = kInvalidGlobalRank;
    std::string agent_addr;
    std::string te_server_name;
    uint64_t agent_session_id = 0;
    uint64_t warmup_recv_addr = 0;
};

struct RankConnectionMetadata {
    GlobalRank rank = kInvalidGlobalRank;
    uint64_t rank_epoch = 0;
    std::string agent_addr;
    std::string te_server_name;
    uint64_t warmup_recv_addr = 0;
};

struct RegisterAgentResponse {
    bool success = false;
    std::string reject_reason;
    // The request reached the Coordinator, but this logical registration can
    // no longer be accepted.
    bool require_new_session = false;
    // Coordinator-assigned epoch for the accepted incarnation of the
    // registering rank. Zero means that no incarnation has been accepted.
    uint64_t rank_epoch = 0;
    std::vector<RankState> all_rank_states;
    std::vector<uint64_t> all_rank_epochs;
    std::vector<uint64_t> all_rank_state_versions;
    std::vector<GroupView> groups;
    std::vector<RankConnectionMetadata> rank_connections;
};

struct LinkEventReport {
    GlobalRank reporter_rank = kInvalidGlobalRank;
    uint64_t agent_session_id = 0;
    uint64_t reporter_rank_epoch = 0;
    uint64_t report_id = 0;
    std::vector<LinkEvent::EventType> events;
    // Parallel to events. Each entry identifies the target
    // incarnation against which the observation was made.
    std::vector<uint64_t> target_rank_epochs;
};

struct LinkEventReportAck {
    GlobalRank reporter_rank = kInvalidGlobalRank;
    uint64_t reporter_rank_epoch = 0;
    uint64_t report_id = 0;
};

struct HeartbeatRequest {
    GlobalRank rank = kInvalidGlobalRank;
    uint64_t agent_session_id = 0;
};

struct HeartbeatResponse {
    bool require_new_session = false;
};

struct UnregisterAgentRequest {
    GlobalRank rank = kInvalidGlobalRank;
    uint64_t agent_session_id = 0;
};

struct UnregisterAgentResponse {
    bool success = false;
    std::string reject_reason;
};

struct RegisterGroupRequest {
    GlobalRank rank = kInvalidGlobalRank;
    uint64_t agent_session_id = 0;
    GroupBootstrapId group_bootstrap_id;
    int32_t max_group_size = 0;
    std::vector<GlobalRank> rank_order;
    GroupBootstrapIdResolvePolicy resolve_policy =
        GroupBootstrapIdResolvePolicy::CreateOrAttach;
    bool auto_deactivate = true;
};

struct RegisterGroupResponse {
    bool success = false;
    std::string reject_reason;
    GroupView view;
};

struct ConfirmReadyForActivationRequest {
    GroupId group_id;
    GlobalRank rank = kInvalidGlobalRank;
    uint64_t agent_session_id = 0;
};

struct ConfirmReadyForActivationResponse {
    bool success = false;
    std::string reject_reason;
};

enum class ProposalStatus : uint8_t {
    Rejected = 0,
    Applied = 1,
    AppliedWithDroppedRanks = 2,
};

struct ProposeViewUpdateRequest {
    GroupId group_id;
    GlobalRank source_rank = kInvalidGlobalRank;
    uint64_t agent_session_id = 0;
    std::vector<InGroupRank> requested_ranks;
    bool is_activation = false;
};

struct ProposeViewUpdateResponse {
    ProposalStatus status = ProposalStatus::Rejected;
    uint64_t new_epoch = 0;
    std::vector<InGroupRank> dropped_ranks;
    std::string reject_reason;
};

struct GroupEndpointPublication {
    GroupId group_id;
    GroupEndpointInfo endpoint_info;
};

struct PublishEndpointRequest {
    GlobalRank rank = kInvalidGlobalRank;
    uint64_t agent_session_id = 0;
    std::vector<GroupEndpointPublication> endpoints;
};

struct PublishEndpointResponse {
    bool success = false;
    std::string reject_reason;
};

struct UnregisterGroupRequest {
    GroupId group_id;
    GlobalRank rank = kInvalidGlobalRank;
    uint64_t agent_session_id = 0;
};

struct UnregisterGroupResponse {
    bool success = false;
    std::string reject_reason;
};

struct SyncAfterFailureRequest {
    GroupId group_id;
    GlobalRank reporter_rank = kInvalidGlobalRank;
    uint64_t agent_session_id = 0;
    uint64_t current_epoch = 0;
    // Piggybacked link event report.
    std::optional<LinkEventReport> link_event_report;
};

enum class SyncAfterFailureStatus : uint8_t {
    Reconciled = 0,  // A reconciliation window completed.
    NoPending = 1,   // No reconciliation was pending at request time.
    Rejected = 2,    // Invalid request (stale session, group not found).
};

struct SyncAfterFailureResponse {
    SyncAfterFailureStatus status = SyncAfterFailureStatus::Rejected;
    GroupView view;
    // Piggybacked link event report ack.
    std::optional<LinkEventReportAck> link_event_report_ack;
    std::string reject_reason;
};

// Coordinator -> Agent RPC messages

struct PeerJoinedPush {
    GlobalRank rank = kInvalidGlobalRank;
    uint64_t rank_epoch = 0;
    std::string te_server_name;
    uint64_t warmup_recv_addr = 0;
};

struct RankStatePush {
    GlobalRank rank = kInvalidGlobalRank;
    uint64_t rank_epoch = 0;
    uint64_t rank_state_version = 0;
    RankState new_state = RankState::Offline;
};

struct ViewUpdatePush {
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

struct BroadcastPeerJoined {
    PeerJoinedPush push;
};

struct BroadcastRankState {
    RankStatePush push;
};

struct PushViewUpdate {
    GroupView view;
};

struct ReplyProposal {
    uint64_t propose_id = 0;
    ProposeViewUpdateResponse response;
};

struct ReplySync {
    uint64_t sync_id = 0;
    SyncAfterFailureResponse response;
};

struct ShutdownCoordinatorHost {};

using CoordinatorEffect =
    std::variant<BroadcastRankState, PushViewUpdate, ReplyProposal,
                 BroadcastPeerJoined, ReplySync, ShutdownCoordinatorHost>;

// Agent effects

struct EnablePeerProbe {
    GlobalRank rank = kInvalidGlobalRank;
    uint64_t rank_epoch = 0;
    std::string te_server_name;
    uint64_t warmup_recv_addr = 0;
};

struct DisconnectLink {
    GlobalRank peer = kInvalidGlobalRank;
};

struct RequestLinkHealthCheck {
    GlobalRank peer = kInvalidGlobalRank;
};

struct SendLinkEventReport {
    LinkEventReport report;
};

struct StopReconnect {
    GlobalRank peer = kInvalidGlobalRank;
};

struct DisconnectAllLinks {};

struct ClearAllPeerMetadata {};

struct ApplyViewToCommunicator {
    GroupView view;
    std::vector<RankState> rank_states;
    std::vector<uint64_t> rank_epochs;
    std::vector<bool> activatable;
};

struct ResetPeerState {
    GlobalRank peer = kInvalidGlobalRank;
};

struct RefreshPeerLink {
    GlobalRank peer = kInvalidGlobalRank;
};

struct NotifyLinkRefreshed {
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
    std::variant<EnablePeerProbe, DisconnectLink, RequestLinkHealthCheck,
                 SendLinkEventReport, StopReconnect, DisconnectAllLinks,
                 ClearAllPeerMetadata, ApplyViewToCommunicator, ResetPeerState,
                 RefreshPeerLink, NotifyLinkRefreshed, NotifyGroupReady,
                 NotifyRanksActivated>;

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
    virtual void unregisterAgent(coro_rpc::context<UnregisterAgentResponse> ctx,
                                 UnregisterAgentRequest req) = 0;
    virtual void registerGroup(coro_rpc::context<RegisterGroupResponse> ctx,
                               RegisterGroupRequest req) = 0;
    virtual void unregisterGroup(coro_rpc::context<UnregisterGroupResponse> ctx,
                                 UnregisterGroupRequest req) = 0;
    virtual void confirmReadyForActivation(
        coro_rpc::context<ConfirmReadyForActivationResponse> ctx,
        ConfirmReadyForActivationRequest req) = 0;
    virtual void proposeViewUpdate(
        coro_rpc::context<ProposeViewUpdateResponse> ctx,
        ProposeViewUpdateRequest req) = 0;
    virtual void publishEndpoint(coro_rpc::context<PublishEndpointResponse> ctx,
                                 PublishEndpointRequest req) = 0;
    virtual void reportLinkEvent(coro_rpc::context<LinkEventReportAck> ctx,
                                 LinkEventReport req) = 0;
    virtual void syncAfterFailure(
        coro_rpc::context<SyncAfterFailureResponse> ctx,
        SyncAfterFailureRequest req) = 0;
};

class AgentRpcService {
   public:
    virtual ~AgentRpcService() = default;

    virtual void onPeerJoined(PeerJoinedPush push) = 0;
    virtual void onRankStateUpdate(RankStatePush push) = 0;
    virtual void onViewUpdate(coro_rpc::context<ViewUpdateAck> ctx,
                              ViewUpdatePush push) = 0;
};

}  // namespace mooncake

#endif  // MOONCAKE_PG_CONTROL_PLANE_RPC_H
