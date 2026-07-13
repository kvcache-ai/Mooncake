#ifndef MOONCAKE_PG_COORDINATOR_H
#define MOONCAKE_PG_COORDINATOR_H

#include <chrono>
#include <cstdint>
#include <optional>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <variant>
#include <vector>

#include "rpc.h"

namespace mooncake {

// CoordinatorStateMachine - abstract interface for the control-plane server
// state machine.
class CoordinatorStateMachine {
   public:
    virtual ~CoordinatorStateMachine() = default;

    virtual CoordinatorApplyResult<RegisterAgentResponse> handleRegisterAgent(
        const RegisterAgentRequest& req) = 0;

    virtual CoordinatorApplyResult<HeartbeatResponse> handleHeartbeat(
        const HeartbeatRequest& req) = 0;

    virtual CoordinatorApplyResult<RegisterGroupResponse> handleRegisterGroup(
        const RegisterGroupRequest& req) = 0;

    virtual CoordinatorApplyResult<void> handleUnregisterGroup(
        const UnregisterGroupRequest& req) = 0;

    virtual CoordinatorApplyResult<PublishEndpointResponse>
    handlePublishEndpoint(const PublishEndpointRequest& req) = 0;

    virtual CoordinatorApplyResult<void> handleProposeViewUpdate(
        uint64_t propose_id, const ProposeViewUpdateRequest& req) = 0;

    virtual CoordinatorApplyResult<void> handleTransferObservation(
        const TransferObservationReport& req) = 0;

    virtual CoordinatorApplyResult<void> handleLinkStateChange(
        const LinkStateChangeReport& req) = 0;

    virtual CoordinatorApplyResult<void> handleSyncAfterFailure(
        uint64_t sync_id, const SyncAfterFailureRequest& req) = 0;

    virtual CoordinatorApplyResult<void> handleViewUpdateAck(GroupId group_id,
                                                             GlobalRank rank,
                                                             uint64_t epoch,
                                                             bool applied) = 0;

    virtual CoordinatorApplyResult<void> tick() = 0;
};

// CentralizedCoordinatorStateMachine - single-node implementation of the
// Coordinator state machine.
class CentralizedCoordinatorStateMachine : public CoordinatorStateMachine {
   public:
    explicit CentralizedCoordinatorStateMachine(
        int max_world_size,
        std::chrono::microseconds fault_reconciliation_window =
            std::chrono::microseconds(50000));

    CoordinatorApplyResult<RegisterAgentResponse> handleRegisterAgent(
        const RegisterAgentRequest& req) override;

    CoordinatorApplyResult<HeartbeatResponse> handleHeartbeat(
        const HeartbeatRequest& req) override;

    CoordinatorApplyResult<RegisterGroupResponse> handleRegisterGroup(
        const RegisterGroupRequest& req) override;

    CoordinatorApplyResult<void> handleUnregisterGroup(
        const UnregisterGroupRequest& req) override;

    CoordinatorApplyResult<PublishEndpointResponse> handlePublishEndpoint(
        const PublishEndpointRequest& req) override;

    CoordinatorApplyResult<void> handleProposeViewUpdate(
        uint64_t propose_id, const ProposeViewUpdateRequest& req) override;

    CoordinatorApplyResult<void> handleTransferObservation(
        const TransferObservationReport& req) override;

    CoordinatorApplyResult<void> handleLinkStateChange(
        const LinkStateChangeReport& req) override;

    CoordinatorApplyResult<void> handleSyncAfterFailure(
        uint64_t sync_id, const SyncAfterFailureRequest& req) override;

    CoordinatorApplyResult<void> handleViewUpdateAck(GroupId group_id,
                                                     GlobalRank rank,
                                                     uint64_t epoch,
                                                     bool applied) override;

    CoordinatorApplyResult<void> tick() override;

    RankState getRankState(GlobalRank rank) const {
        if (!rankInRange(rank)) return RankState::Offline;
        return ranks_[rank].state;
    }

    const std::string& getAgentAddr(GlobalRank rank) const {
        return ranks_[rank].agent_addr;
    }

   private:
    int max_world_size_;

    struct RankInfo {
        RankState state = RankState::Offline;
        std::string agent_addr;
        std::string te_server_name;
        uint64_t agent_session_epoch = 0;
        std::chrono::steady_clock::time_point last_heartbeat;
        std::vector<uint8_t> link_status;
        uint64_t warmup_recv_addr = 0;
    };

    // Per-GlobalRank coordinator state.
    std::vector<RankInfo> ranks_;

    std::unordered_map<GroupId, GroupView> group_views_;

    // Coordinator-assigned endpoint epoch counter per GlobalRank.
    // Incremented on every successful publishEndpoint for that rank so the
    // Agent can detect endpoint changes.
    std::vector<uint64_t> endpoint_epochs_;

    struct PendingViewUpdateBarrier {
        GroupId group_id;
        uint64_t epoch = 0;
        std::unordered_set<GlobalRank> waiting_acks;
        std::optional<std::chrono::steady_clock::time_point> deadline;

        struct ProposalCommit {
            uint64_t propose_id = 0;
            ProposeViewUpdateResponse eventual_response;
        };
        struct BootstrapCommit {};

        std::variant<ProposalCommit, BootstrapCommit> commit =
            BootstrapCommit{};
    };
    std::unordered_map<GroupId,
                       std::unordered_map<uint64_t, PendingViewUpdateBarrier>>
        pending_barriers_;

    // Fault reconciliation window.  Transfer observations update link_status
    // immediately, but the coordinator defers the membership decision until
    // the window closes.
    struct FaultReconciliationContext {
        bool active = false;
        std::chrono::steady_clock::time_point deadline;
    };
    FaultReconciliationContext reconciliation_ctx_;
    std::chrono::microseconds fault_reconciliation_window_;

    // Pending sync-after-failure requests.  Keyed by group_id, each entry maps
    // caller_rank -> list of sync_ids waiting for a decision for that group.
    // Resolved ONLY when the caller ACKs the ViewUpdate that carries the
    // decision (handleViewUpdateAck), or when the caller/group is removed and
    // the sync is rejected.
    std::unordered_map<GroupId,
                       std::unordered_map<GlobalRank, std::vector<uint64_t>>>
        pending_syncs_;

    static constexpr auto kProposeTimeout = std::chrono::seconds(2);
    static constexpr auto kHeartbeatTimeout = std::chrono::seconds(5);

    void transitionToOffline(GlobalRank rank,
                             std::vector<CoordinatorEffect>& effects);

    // Recompute the authoritative healthy set (max clique) and update
    // rank-state between Healthy and Synced.  Emits rank-state effects.
    void updateRankStates(std::vector<CoordinatorEffect>& effects);

    // For every auto_deactivate + ready group, mark active ranks that are not
    // in the current healthy set as inactive.  Increments view epoch and emits
    // a ViewUpdate when at least one rank is pruned.
    void applyAutoDeactivate(std::vector<CoordinatorEffect>& effects);

    // Opens a fault reconciliation window if it is not open.
    // An existing window is not extended.
    void tryOpenReconciliationWindow();

    // Apply transfer observation to a reporter's link_status.
    // Returns true when at least one peer was reported as failed.
    bool applyLinkStatusUpdate(RankInfo& reporter,
                               const std::vector<uint8_t>& attempted,
                               const std::vector<uint8_t>& failed);

    bool isMutuallyConnected(GlobalRank a, GlobalRank b) const;

    // Preserve existing healthy ranks that are still mutually connected,
    // then extend with new candidates that have full connectivity to all
    // current healthy members.
    std::vector<GlobalRank> extendHealthySet() const;

    // Bootstrap state machine driver.  Advances groups through:
    //   Bootstrapping -> BootstrapSyncing (when all active ranks are Healthy
    //                    and have published endpoints)
    //   BootstrapSyncing -> Ready (when all active ranks have ACKed)
    //
    // Called after every state-changing operation.
    void checkGroupTransitions(std::vector<CoordinatorEffect>& effects);

    bool processGroupRegistration(GlobalRank joining_rank,
                                  const GroupView& group, bool auto_deactivate,
                                  RegisterGroupResponse& response,
                                  std::vector<CoordinatorEffect>& effects);

    bool canEraseGroup(const GroupView& view) const;
    void eraseGroup(GroupId group_id, std::vector<CoordinatorEffect>& effects);

    bool isActivatableSet(GroupId group_id,
                          const std::vector<GlobalRank>& new_ranks,
                          const GroupView& old_view) const;

    bool isRankActivatable(GroupId group_id, GlobalRank rank,
                           const std::vector<GlobalRank>& future_active) const;

    // Helpers for barrier (proposal, bootstrap, ...) lifecycle.
    void commitBarrier(PendingViewUpdateBarrier barrier,
                       const std::vector<GlobalRank>& dropped,
                       std::vector<CoordinatorEffect>& effects);

    // Compute the ACK set for a ViewUpdate barrier (proposal, bootstrap, ...).
    std::unordered_set<GlobalRank> computeBarrierAckSet(
        const GroupView& old_view, const GroupView& new_view,
        GroupId group_id) const;

    // Reject pending syncs for `group_id` / `group_id, rank`.
    // Emits ReplySyncEffect for each pending sync_id.
    void rejectPendingSyncs(GroupId group_id, GlobalRank rank,
                            const std::string& reason,
                            std::vector<CoordinatorEffect>& effects);
    void rejectPendingSyncs(GroupId group_id, const std::string& reason,
                            std::vector<CoordinatorEffect>& effects);

    // Request validation: rank must be in range, online, and matching session.
    bool hasValidSession(GlobalRank rank, uint64_t session_epoch) const {
        return rankInRange(rank) && ranks_[rank].state != RankState::Offline &&
               ranks_[rank].agent_session_epoch == session_epoch;
    }

    bool rankInRange(GlobalRank rank) const {
        return 0 <= rank && rank < max_world_size_;
    }

    CoordinatorEffect makeRankStateEffect(GlobalRank rank);
};

}  // namespace mooncake

#endif  // MOONCAKE_PG_COORDINATOR_H
