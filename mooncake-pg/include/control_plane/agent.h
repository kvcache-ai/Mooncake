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
    AgentApplyResult handleRankStateUpdate(const RankStatePush& push);
    std::pair<AgentApplyResult, bool> applyGroupView(GroupId group_id,
                                                     const GroupView& view);
    std::pair<AgentApplyResult, bool> handleViewUpdate(
        const ViewUpdatePush& push);

    HeartbeatRequest buildHeartbeat() const;

    AgentApplyResult applyRegisterAgentResponse(
        const RegisterAgentResponse& resp);
    AgentApplyResult reset(uint64_t new_session_id);

    AgentApplyResult pushLinkEvent(const LinkEvent& event);
    std::optional<LinkEventReport> getLinkEventReport() const;
    void handleLinkEventReportAck(const LinkEventReportAck& ack);

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

    uint64_t getAgentSessionId() const {
        return agent_session_id_.load(std::memory_order_acquire);
    }

    uint64_t getRankEpoch() const {
        return self_rank_epoch_.load(std::memory_order_acquire);
    }

   private:
    GlobalRank rank_;
    int max_world_size_;

    std::atomic<uint64_t> agent_session_id_{0};
    std::atomic<uint64_t> self_rank_epoch_{0};

    std::unordered_map<GroupId, GroupView> groups_;

    std::vector<RankState> global_rank_states_;
    std::vector<uint64_t> global_rank_epochs_;
    std::vector<uint64_t> global_rank_state_versions_;
    std::vector<std::optional<RankConnectionMetadata>> rank_connections_;

    std::vector<LinkEvent::EventType> observed_link_state_;
    std::vector<uint64_t> observed_target_rank_epochs_;
    uint64_t link_state_version_ = 0;
    uint64_t acked_link_state_version_ = 0;

    CoordinatorConnection coordinator_connection_ =
        CoordinatorConnection::Disconnected;

    bool rankInRange(GlobalRank rank) const {
        return 0 <= rank && rank < max_world_size_;
    }

    void appendApplyViewEffect(const GroupView& view,
                               AgentApplyResult& effects) const;
    void appendApplyViewEffectsForRank(GlobalRank rank,
                                       AgentApplyResult& effects) const;
    void resetRankForNewEpoch(GlobalRank rank, uint64_t rank_epoch,
                              AgentApplyResult& effects);
    bool recordLinkEvent(GlobalRank peer, uint64_t target_rank_epoch,
                         LinkEvent::EventType type);
};

}  // namespace mooncake

#endif  // MOONCAKE_PG_AGENT_H
