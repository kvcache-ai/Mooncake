#include "control_plane/agent.h"

#include <glog/logging.h>

namespace mooncake {

// Constructor

AgentStateMachine::AgentStateMachine(GlobalRank rank, int max_world_size)
    : rank_(rank), max_world_size_(max_world_size) {
    CHECK_GT(max_world_size_, 0);
    CHECK_LE(max_world_size_, kMaxNumRanks)
        << "max_world_size " << max_world_size_ << " exceeds kMaxNumRanks ("
        << kMaxNumRanks << ")";
    global_rank_states_.fill(RankState::OFFLINE);
    link_connected_.fill(false);
    last_reported_peer_status_.fill(false);
    rank_state_snapshot_.fill(static_cast<uint8_t>(RankState::OFFLINE));
}

// Group lifecycle

void AgentStateMachine::registerGroup(const GroupDeclaration& declaration) {
    GroupId group_id = declaration.descriptor.group_id;
    GroupEntry entry;
    entry.descriptor = declaration.descriptor;
    entry.auto_deactivate = declaration.auto_deactivate;
    // View is populated later via handleViewUpdate.
    groups_[group_id] = std::move(entry);
}

void AgentStateMachine::unregisterGroup(GroupId group_id) {
    groups_.erase(group_id);
}

GroupView AgentStateMachine::getGroupView(GroupId group_id) const {
    auto it = groups_.find(group_id);
    if (it != groups_.end()) {
        return it->second.view;
    }
    return GroupView{};
}

const GroupDescriptor* AgentStateMachine::getGroupDescriptor(
    GroupId group_id) const {
    auto it = groups_.find(group_id);
    if (it != groups_.end()) {
        return &it->second.descriptor;
    }
    return nullptr;
}

AgentApplyResult AgentStateMachine::handlePeerJoined(
    const PeerJoinedPush& push) {
    AgentApplyResult effects;
    if (!rankInRange(push.rank)) {
        LOG(WARNING) << "AgentStateMachine: handlePeerJoined out-of-range rank "
                     << push.rank;
        return effects;
    }
    if (push.rank == rank_) return effects;

    rank_connections_[push.rank] = RankConnectionMetadata{
        .rank = push.rank,
        .te_server_name = push.te_server_name,
        .warmup_recv_addr = push.warmup_recv_addr,
    };
    // Seed so the first observation of any kind triggers a report.
    last_reported_peer_status_[push.rank] = true;

    effects.push_back(
        EnablePeerProbe{push.rank, push.te_server_name, push.warmup_recv_addr});
    return effects;
}

// handleRankStateUpdate  - Coordinator-authoritative state change

AgentApplyResult AgentStateMachine::handleRankStateUpdate(
    const RankStateUpdatePush& push) {
    AgentApplyResult effects;
    if (!rankInRange(push.rank)) {
        LOG(WARNING) << "AgentStateMachine: handleRankStateUpdate out-of-range "
                     << push.rank;
        return effects;
    }

    global_rank_states_[push.rank] = static_cast<RankState>(push.new_state);

    // Remote OFFLINE: the control plane is dead (process may have exited).
    // Tear down TE link AND stop candidate probe.
    if (push.rank != rank_ &&
        push.new_state == static_cast<uint8_t>(RankState::OFFLINE)) {
        effects.push_back(DisconnectLink{push.rank});
        effects.push_back(StopReconnect{push.rank});
    }

    syncRankStateSnapshot(effects);
    return effects;
}

AgentApplyResult AgentStateMachine::handleViewUpdate(
    const ViewUpdatePush& push) {
    AgentApplyResult effects;
    auto it = groups_.find(push.group_id);
    if (it == groups_.end()) return effects;

    // Update descriptor (rank_order may have grown via activate) and view.
    it->second.descriptor = push.descriptor;
    it->second.view = push.view;

    effects.push_back(
        ApplyViewToBackend{push.group_id, push.descriptor, push.view});
    return effects;
}

// applyGroupView  - apply a GroupView received as an RPC response (e.g.
// declareGroup).  Unlike handleViewUpdate, this does not carry a new
// descriptor; the local descriptor registered with the group is preserved.
AgentApplyResult AgentStateMachine::applyGroupView(GroupId group_id,
                                                   const GroupView& view) {
    AgentApplyResult effects;
    auto it = groups_.find(group_id);
    if (it == groups_.end()) return effects;

    it->second.view = view;
    effects.push_back(
        ApplyViewToBackend{group_id, it->second.descriptor, view});
    return effects;
}

AgentApplyResult AgentStateMachine::handleLinkStateChanged(GlobalRank peer,
                                                           bool connected) {
    AgentApplyResult effects;
    if (!rankInRange(peer)) {
        LOG(WARNING)
            << "AgentStateMachine: handleLinkStateChanged out-of-range "
            << peer;
        return effects;
    }
    link_connected_[peer] = connected;

    if (!connected) {
        effects.push_back(NotifyTEUnreachable{peer});
    } else {
        // Link came up: re-apply current Ready views so backends refresh their
        // cached segment IDs for this peer.  This is required for link recovery
        // (the view stays Ready while the TE link is rebuilt) and covers any
        // edge case where a Ready view was applied before the local TE link
        // finished establishing.
        for (const auto& [group_id, entry] : groups_) {
            if (entry.view.status == GroupStatus::Ready) {
                effects.push_back(
                    ApplyViewToBackend{group_id, entry.descriptor, entry.view});
            }
        }
    }
    return effects;
}

HeartbeatRequest AgentStateMachine::buildHeartbeat() const {
    HeartbeatRequest req;
    req.rank = rank_;
    req.link_status.resize(max_world_size_, 0);
    for (int i = 0; i < max_world_size_; ++i)
        req.link_status[i] = link_connected_[i] ? 1 : 0;
    req.link_status[rank_] = 1;
    return req;
}

AgentApplyResult AgentStateMachine::applyRegisterResponse(
    const RegisterResponse& resp) {
    AgentApplyResult effects;

    if (!resp.success) {
        coordinator_connection_ = CoordinatorConnection::Disconnected;
        return effects;
    }

    // Populate global rank states.
    if (static_cast<int>(resp.all_rank_states.size()) != max_world_size_) {
        LOG(WARNING) << "AgentStateMachine: all_rank_states size mismatch (got "
                     << resp.all_rank_states.size() << ", expected "
                     << max_world_size_ << "); truncating.";
    }
    for (int i = 0; i < max_world_size_ &&
                    i < static_cast<int>(resp.all_rank_states.size());
         ++i) {
        global_rank_states_[i] =
            static_cast<RankState>(resp.all_rank_states[i]);
    }

    // Populate group descriptors.
    for (const auto& desc : resp.group_descriptors) {
        groups_[desc.group_id].descriptor = desc;
    }

    // Populate group views.
    for (const auto& view : resp.current_views) {
        auto& group = groups_[view.group_id];
        group.view = view;
        effects.push_back(
            ApplyViewToBackend{view.group_id, group.descriptor, view});
    }

    // Populate connection metadata and trigger peer probes.
    // Seed so the first observation of any kind triggers a report.
    for (const auto& conn : resp.rank_connections) {
        if (conn.rank == rank_) continue;
        rank_connections_[conn.rank] = conn;
        last_reported_peer_status_[conn.rank] = true;
        effects.push_back(EnablePeerProbe{conn.rank, conn.te_server_name,
                                          conn.warmup_recv_addr});
    }

    coordinator_connection_ = CoordinatorConnection::Connected;
    syncRankStateSnapshot(effects);
    return effects;
}

AgentApplyResult AgentStateMachine::prepareCleanSlateRegister() {
    AgentApplyResult effects;

    rank_state_ = RankState::OFFLINE;
    global_rank_states_.fill(RankState::OFFLINE);
    link_connected_.fill(false);
    last_reported_peer_status_.fill(false);
    rank_state_snapshot_.fill(static_cast<uint8_t>(RankState::OFFLINE));
    for (auto& conn : rank_connections_) conn.reset();

    effects.push_back(DisconnectAllLinks{});
    effects.push_back(ClearAllPeerMetadata{});
    effects.push_back(PublishRankStateSnapshot{
        std::vector<uint8_t>(rank_state_snapshot_.begin(),
                             rank_state_snapshot_.begin() + max_world_size_)});

    return effects;
}

AgentApplyResult AgentStateMachine::markOffline() {
    AgentApplyResult effects;

    rank_state_ = RankState::OFFLINE;
    coordinator_connection_ = CoordinatorConnection::Disconnected;
    global_rank_states_.fill(RankState::OFFLINE);
    link_connected_.fill(false);
    last_reported_peer_status_.fill(false);
    rank_state_snapshot_.fill(static_cast<uint8_t>(RankState::OFFLINE));
    for (auto& conn : rank_connections_) conn.reset();

    effects.push_back(DisconnectAllLinks{});
    effects.push_back(ClearAllPeerMetadata{});
    effects.push_back(PublishRankStateSnapshot{
        std::vector<uint8_t>(rank_state_snapshot_.begin(),
                             rank_state_snapshot_.begin() + max_world_size_)});

    for (auto& [group_id, entry] : groups_) {
        effects.push_back(MarkBackendViewStale{group_id});
    }

    return effects;
}

// processTransferObservation  - report when observation differs from last
//
// When a transfer observation for peer j differs from
// last_reported_peer_status_, include it in an immediate
// reportTransferObservation RPC and update the cache. This covers both failure
// (true->false) and recovery (false->true) transitions. Unchanged observations
// piggyback on the next heartbeat.
AgentApplyResult AgentStateMachine::processTransferObservation(
    const TransferObservationEvent& event) {
    AgentApplyResult effects;

    if (rank_state_ == RankState::OFFLINE) return effects;

    TransferObservationReport req;
    req.group_id = event.group_id;
    req.reporter_rank = rank_;

    bool has_changed = false;

    for (int j = 0; j < max_world_size_; ++j) {
        if (static_cast<size_t>(j) >= event.attempted_ranks.size()) continue;
        if (!event.attempted_ranks[j]) continue;

        bool succeeded =
            static_cast<size_t>(j) < event.succeeded_ranks.size() &&
            event.succeeded_ranks[j];
        bool failed = static_cast<size_t>(j) < event.failed_ranks.size() &&
                      event.failed_ranks[j];

        // Determine current observation: failed takes precedence.
        bool current = succeeded && !failed;

        if (current != last_reported_peer_status_[j]) {
            last_reported_peer_status_[j] = current;
            // Lazy-init the vectors on first change.
            if (!has_changed) {
                req.attempted_ranks.assign(max_world_size_, 0);
                req.failed_ranks.assign(max_world_size_, 0);
                req.succeeded_ranks.assign(max_world_size_, 0);
            }
            req.attempted_ranks[j] = 1;
            req.succeeded_ranks[j] = current ? 1 : 0;
            req.failed_ranks[j] = current ? 0 : 1;
            has_changed = true;
        }
    }

    if (has_changed) {
        effects.push_back(SendTransferObservation{std::move(req)});
    }

    return effects;
}

void AgentStateMachine::syncRankStateSnapshot(AgentApplyResult& effects) {
    bool snapshot_changed = false;
    for (int j = 0; j < max_world_size_; ++j) {
        uint8_t new_val = static_cast<uint8_t>(global_rank_states_[j]);
        if (rank_state_snapshot_[j] != new_val) {
            rank_state_snapshot_[j] = new_val;
            snapshot_changed = true;
        }
    }

    // Update self rank_state_ to track the Coordinator-authoritative state.
    rank_state_ = global_rank_states_[rank_];

    if (snapshot_changed) {
        effects.push_back(PublishRankStateSnapshot{std::vector<uint8_t>(
            rank_state_snapshot_.begin(),
            rank_state_snapshot_.begin() + max_world_size_)});
    }
}

}  // namespace mooncake
