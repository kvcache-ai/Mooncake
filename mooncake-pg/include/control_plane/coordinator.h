#ifndef MOONCAKE_PG_COORDINATOR_H
#define MOONCAKE_PG_COORDINATOR_H

#include <array>
#include <chrono>
#include <cstdint>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "rpc.h"

namespace mooncake {

// CoordinatorStateMachine  - abstract interface for the control-plane server
// state machine.
//
// All methods are pure, synchronous, single-threaded functions.  They mutate
// internal state, return a response (if applicable) and a list of side-effects
// for the Host to execute.  No RPC, no I/O, no threads.
class CoordinatorStateMachine {
   public:
    virtual ~CoordinatorStateMachine() = default;

    virtual CoordinatorApplyResult<RegisterResponse> handleRegister(
        const RegisterRequest& req) = 0;

    virtual CoordinatorApplyResult<HeartbeatResponse> handleHeartbeat(
        const HeartbeatRequest& req) = 0;

    virtual CoordinatorApplyResult<DeclareGroupResponse> handleDeclareGroup(
        const DeclareGroupRequest& req) = 0;

    virtual CoordinatorApplyResult<void> handleProposeViewUpdate(
        uint64_t propose_id, const ProposeViewUpdateRequest& req) = 0;

    virtual CoordinatorApplyResult<void> handleProposalAck(uint64_t propose_id,
                                                           GlobalRank rank,
                                                           uint64_t epoch,
                                                           bool applied) = 0;

    virtual CoordinatorApplyResult<void> handleBootstrapAck(GroupId group_id,
                                                            GlobalRank rank,
                                                            uint64_t epoch) = 0;

    virtual CoordinatorApplyResult<PublishEndpointResponse>
    handlePublishEndpoint(const PublishEndpointRequest& req) = 0;

    virtual CoordinatorApplyResult<void> handleTransferObservation(
        const TransferObservationReport& req) = 0;

    virtual CoordinatorApplyResult<void> checkTimeouts() = 0;
};

// CentralizedCoordinatorStateMachine - single-node implementation of the
// Coordinator state machine.
class CentralizedCoordinatorStateMachine : public CoordinatorStateMachine {
   public:
    explicit CentralizedCoordinatorStateMachine(int max_world_size);

    CoordinatorApplyResult<RegisterResponse> handleRegister(
        const RegisterRequest& req) override;

    CoordinatorApplyResult<HeartbeatResponse> handleHeartbeat(
        const HeartbeatRequest& req) override;

    CoordinatorApplyResult<DeclareGroupResponse> handleDeclareGroup(
        const DeclareGroupRequest& req) override;

    CoordinatorApplyResult<void> handleProposeViewUpdate(
        uint64_t propose_id, const ProposeViewUpdateRequest& req) override;

    CoordinatorApplyResult<void> handleProposalAck(uint64_t propose_id,
                                                   GlobalRank rank,
                                                   uint64_t epoch,
                                                   bool applied) override;

    CoordinatorApplyResult<PublishEndpointResponse> handlePublishEndpoint(
        const PublishEndpointRequest& req) override;

    CoordinatorApplyResult<void> handleTransferObservation(
        const TransferObservationReport& req) override;

    CoordinatorApplyResult<void> checkTimeouts() override;

    CoordinatorApplyResult<void> handleBootstrapAck(GroupId group_id,
                                                    GlobalRank rank,
                                                    uint64_t epoch) override;

    RankState getRankState(GlobalRank rank) const {
        if (!rankInValidRange(rank)) return RankState::OFFLINE;
        return ranks_[rank].state;
    }

    const std::string& getAgentAddr(GlobalRank rank) const {
        return ranks_[rank].agent_addr;
    }

   private:
    int max_world_size_;

    struct RankInfo {
        RankState state = RankState::OFFLINE;
        std::string agent_addr;
        std::string te_server_name;
        uint64_t agent_session_epoch = 0;
        std::chrono::steady_clock::time_point last_heartbeat;
        std::vector<uint8_t> link_status;
        uint64_t warmup_recv_addr = 0;
    };

    std::array<RankInfo, kMaxNumRanks> ranks_;

    std::unordered_map<GroupId, GroupView> group_views_;
    std::unordered_map<GroupId, GroupDescriptor> group_descriptors_;
    std::unordered_map<GroupId, bool> group_auto_deactivate_;

    // Two independent 2PC flows share the same ViewUpdate ACK path:
    //   1. Proposal 2PC (activate/deactivate) - keyed by propose_id
    //   2. Bootstrap 2PC (initial group readiness) - keyed by group_id

    // Proposal 2PC (activate/deactivate).  Maps propose_id -> pending state.
    struct PendingProposal {
        uint64_t propose_id = 0;
        GroupId group_id = 0;
        ProposeViewUpdateResponse eventual_response;
        std::unordered_set<GlobalRank> waiting_acks;
        std::chrono::steady_clock::time_point deadline;
    };
    std::unordered_map<uint64_t, PendingProposal> pending_proposal_acks_;

    // Bootstrap 2PC.  Maps group_id -> set of ranks whose ACK is still
    // pending during the BootstrapSyncing phase.
    std::unordered_map<GroupId, std::unordered_set<GlobalRank>>
        pending_bootstrap_acks_;

    static constexpr auto kProposeTimeout = std::chrono::seconds(2);
    static constexpr auto kHeartbeatTimeout = std::chrono::seconds(5);

    void transitionToOffline(GlobalRank rank,
                             std::vector<CoordinatorEffect>& effects);
    void transitionToSynced(GlobalRank rank,
                            std::vector<CoordinatorEffect>& effects);
    void updateRankHealth(std::vector<CoordinatorEffect>& effects);

    // Bootstrap state machine driver.  Advances groups through:
    //   Bootstrapping -> BootstrapSyncing (when all active ranks are HEALTHY
    //                    and have published endpoints)
    //   BootstrapSyncing -> Ready (when all active ranks have ACKed)
    //
    // Called after every state-changing operation.
    void checkGroupTransitions(std::vector<CoordinatorEffect>& effects);

    bool isMutuallyConnected(GlobalRank a, GlobalRank b) const;
    std::vector<GlobalRank> findHealthySet() const;

    bool rankInValidRange(GlobalRank rank) const;
    bool isRankActivatable(GroupId group_id, GlobalRank rank) const;
    bool isActivatableSet(GroupId group_id,
                          const std::vector<GlobalRank>& new_ranks,
                          const GroupView& old_view) const;

    bool declareGroup(const GroupDeclaration& declaration,
                      DeclareGroupResponse& response);

    std::vector<GlobalRank> computeRequiredViewAcks(const GroupView& old_view,
                                                    const GroupView& new_view,
                                                    GlobalRank proposer) const;

    RegisterResponse buildRegisterResponse(GlobalRank for_rank) const;

    // Effect factory helpers.
    CoordinatorEffect makeRankStateEffect(GlobalRank rank);
    CoordinatorEffect makePeerJoinedEffect(GlobalRank rank);
};

}  // namespace mooncake

#endif  // MOONCAKE_PG_COORDINATOR_H
