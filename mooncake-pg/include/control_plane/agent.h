#ifndef MOONCAKE_PG_AGENT_H
#define MOONCAKE_PG_AGENT_H

#include <atomic>
#include <cstdint>
#include <optional>
#include <unordered_map>
#include <vector>

#include "rpc.h"

namespace mooncake {

// AgentStateMachine - Pure state machine for the control-plane client.
class AgentStateMachine {
   public:
    AgentStateMachine(GlobalRank rank, int max_world_size);

    void registerGroup(const GroupView& group, bool auto_deactivate);
    void unregisterGroup(GroupId group_id);

    AgentApplyResult handlePeerJoined(const PeerJoinedPush& push);
    AgentApplyResult handleRankStateUpdate(const RankStateUpdatePush& push);
    AgentApplyResult handleViewUpdate(const ViewUpdatePush& push);
    AgentApplyResult handleLinkStateChange(GlobalRank peer, bool connected);

    HeartbeatRequest buildHeartbeat() const;

    AgentApplyResult applyRegisterAgentResponse(
        const RegisterAgentResponse& resp);
    AgentApplyResult prepareCleanSlateRegister();

    void setAgentSessionEpoch(uint64_t epoch) {
        agent_session_epoch_.store(epoch, std::memory_order_release);
    }

    std::optional<TransferObservationReport> processTransferObservation(
        const TransferObservationEvent& event);

    // Merge `next` into `acc` for peers where next.attempted_ranks is set.
    // Later observations override earlier ones for the same peer.
    void mergeObservationEvent(TransferObservationEvent& acc,
                               const TransferObservationEvent& next);

    GroupView getGroupView(GroupId group_id) const;

    enum class CoordinatorConnection {
        Connected,
        AgentRegistering,
        Disconnected
    };
    CoordinatorConnection getCoordinatorConnection() const {
        return coordinator_connection_;
    }
    void setCoordinatorConnection(CoordinatorConnection state) {
        coordinator_connection_ = state;
    }

    uint64_t getAgentSessionEpoch() const {
        return agent_session_epoch_.load(std::memory_order_acquire);
    }

    RankState getRankState(GlobalRank rank) const {
        return static_cast<RankState>(
            global_rank_states_[rank].load(std::memory_order_acquire));
    }

    bool isRankActive(GroupId group_id, InGroupRank rank) const;

    // Best-effort local estimate: Healthy && isMember && hasEndpoint.
    bool maybeActivatable(GroupId group_id, InGroupRank rank) const;

   private:
    GlobalRank rank_;
    int max_world_size_;

    RankState rank_state_ = RankState::Offline;
    std::atomic<uint64_t> agent_session_epoch_{0};

    std::unordered_map<GroupId, GroupView> groups_;

    std::vector<std::atomic<uint8_t>> global_rank_states_;
    std::vector<bool> link_connected_;
    std::vector<bool> last_reported_peer_status_;
    std::vector<std::optional<RankConnectionMetadata>> rank_connections_;

    std::unordered_map<GroupId, std::vector<std::atomic<bool>>>
        maybe_activatable_;

    std::unordered_map<GroupId, std::vector<std::atomic<bool>>> group_active_;

    CoordinatorConnection coordinator_connection_ =
        CoordinatorConnection::Disconnected;

    bool rankInRange(GlobalRank rank) const {
        return 0 <= rank && rank < max_world_size_;
    }

    void updateMaybeActivatable(GroupId group_id, InGroupRank rank);
    void updateActive(GroupId group_id, InGroupRank rank);
};

}  // namespace mooncake

#endif  // MOONCAKE_PG_AGENT_H
