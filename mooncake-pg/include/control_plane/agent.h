#ifndef MOONCAKE_PG_AGENT_H
#define MOONCAKE_PG_AGENT_H

#include <array>
#include <cstdint>
#include <optional>
#include <unordered_map>

#include "rpc.h"

namespace mooncake {

// AgentStateMachine - Pure state machine for the control-plane client.
class AgentStateMachine {
   public:
    AgentStateMachine(GlobalRank rank, int max_world_size);

    void registerGroup(const GroupDeclaration& declaration);
    void unregisterGroup(GroupId group_id);

    AgentApplyResult handlePeerJoined(const PeerJoinedPush& push);
    AgentApplyResult handleRankStateUpdate(const RankStateUpdatePush& push);
    AgentApplyResult handleViewUpdate(const ViewUpdatePush& push);
    AgentApplyResult applyGroupView(GroupId group_id, const GroupView& view);
    AgentApplyResult handleLinkStateChanged(GlobalRank peer, bool connected);

    HeartbeatRequest buildHeartbeat() const;

    AgentApplyResult applyRegisterResponse(const RegisterResponse& resp);
    AgentApplyResult prepareCleanSlateRegister();
    AgentApplyResult markOffline();

    void setAgentSessionEpoch(uint64_t epoch) {
        agent_session_epoch_.store(epoch, std::memory_order_release);
    }

    AgentApplyResult processTransferObservation(
        const TransferObservationEvent& event);

    GroupView getGroupView(GroupId group_id) const;
    const GroupDescriptor* getGroupDescriptor(GroupId group_id) const;

    enum class CoordinatorConnection { Connected, Registering, Disconnected };
    CoordinatorConnection getCoordinatorConnection() const {
        return coordinator_connection_;
    }
    void setCoordinatorConnected() {
        coordinator_connection_ = CoordinatorConnection::Connected;
    }
    void setCoordinatorRegistering() {
        coordinator_connection_ = CoordinatorConnection::Registering;
    }
    void setCoordinatorDisconnected() {
        coordinator_connection_ = CoordinatorConnection::Disconnected;
    }

    uint64_t getAgentSessionEpoch() const {
        return agent_session_epoch_.load(std::memory_order_acquire);
    }

   private:
    GlobalRank rank_;
    int max_world_size_;

    RankState rank_state_ = RankState::OFFLINE;
    // Monotonically incremented on (re-)registration.  Written only from
    // the executor thread; read from caller threads (proposeActivate / etc.)
    // via the atomic accessors above.
    std::atomic<uint64_t> agent_session_epoch_{0};

    struct GroupEntry {
        GroupDescriptor descriptor;
        GroupView view;
        bool auto_deactivate = true;
    };
    std::unordered_map<GroupId, GroupEntry> groups_;

    std::array<RankState, kMaxNumRanks> global_rank_states_{};

    // Physical link state from LinkManager events (LinkUp/LinkDown).
    std::array<bool, kMaxNumRanks> link_connected_{};

    // Last peer reachability status reported to the Coordinator.
    // Updated by processTransferObservation when a transfer observation
    // differs from the last reported value (both success and failure
    // transitions trigger an immediate reportTransferObservation RPC).
    std::array<bool, kMaxNumRanks> last_reported_peer_status_{};

    // Snapshot of Coordinator-authoritative rank states, published to
    // LinkManager for worker-facing isRankReady() queries.
    std::array<uint8_t, kMaxNumRanks> rank_state_snapshot_{};

    std::array<std::optional<RankConnectionMetadata>, kMaxNumRanks>
        rank_connections_;

    CoordinatorConnection coordinator_connection_ =
        CoordinatorConnection::Disconnected;

    bool rankInRange(GlobalRank rank) const {
        return rank >= 0 && rank < max_world_size_;
    }

    void syncRankStateSnapshot(AgentApplyResult& effects);
};

}  // namespace mooncake

#endif  // MOONCAKE_PG_AGENT_H
