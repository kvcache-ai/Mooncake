#include "control_plane/agent.h"

#include <algorithm>
#include <glog/logging.h>

namespace mooncake {

AgentStateMachine::AgentStateMachine(GlobalRank rank, int max_world_size)
    : rank_(rank), max_world_size_(max_world_size) {
    CHECK_GT(max_world_size_, 0);
    CHECK_LE(max_world_size_, kMaxNumRanks)
        << "max_world_size " << max_world_size_ << " exceeds kMaxNumRanks ("
        << kMaxNumRanks << ")";
    global_rank_states_ = std::vector<RankState>(max_world_size_);
    link_connected_.assign(max_world_size_, false);
    last_reported_peer_status_.assign(max_world_size_, false);
    rank_connections_.resize(max_world_size_);
}

static ApplyViewToBackend makeApplyView(
    const GroupView& view, const std::vector<RankState>& rank_states) {
    std::vector<bool> activatable(view.rank_order.size());
    for (size_t i = 0; i < view.rank_order.size(); ++i) {
        GlobalRank gr = view.rank_order[i];
        bool healthy = rank_states[gr] == RankState::Healthy;
        activatable[i] = healthy && view.members[gr].isMember() &&
                         view.members[gr].hasEndpoint();
    }
    return {view.group_id, view, rank_states, std::move(activatable)};
}

void AgentStateMachine::registerGroup(const GroupView& group,
                                      bool auto_deactivate) {
    GroupView view = group;
    view.auto_deactivate = auto_deactivate;
    groups_[group.group_id] = std::move(view);
}

void AgentStateMachine::unregisterGroup(GroupId group_id) {
    groups_.erase(group_id);
}

GroupView AgentStateMachine::getGroupView(GroupId group_id) const {
    auto it = groups_.find(group_id);
    if (it != groups_.end()) {
        return it->second;
    }
    return GroupView{};
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

    auto old = rank_connections_[push.rank];
    if (old.has_value() && old->te_server_name != push.te_server_name) {
        effects.push_back(DisconnectLink{push.rank});
        effects.push_back(NotifyLinkRefreshed{push.rank});
    }

    rank_connections_[push.rank] = RankConnectionMetadata{
        .rank = push.rank,
        .te_server_name = push.te_server_name,
        .warmup_recv_addr = push.warmup_recv_addr,
    };
    last_reported_peer_status_[push.rank] = false;

    effects.push_back(
        EnablePeerProbe{push.rank, push.te_server_name, push.warmup_recv_addr});
    return effects;
}

AgentApplyResult AgentStateMachine::handleRankStateUpdate(
    const RankStateUpdatePush& push) {
    AgentApplyResult effects;
    if (!rankInRange(push.rank)) {
        LOG(WARNING) << "AgentStateMachine: handleRankStateUpdate out-of-range "
                     << push.rank;
        return effects;
    }

    global_rank_states_[push.rank] = push.new_state;
    if (push.rank == rank_) {
        rank_state_ = push.new_state;
    }

    // Rank state changed: recompute activatable for every group that
    // contains this rank and push updated ApplyViewToBackend effects.
    for (const auto& [group_id, view] : groups_) {
        if (std::find(view.rank_order.begin(), view.rank_order.end(),
                      push.rank) != view.rank_order.end()) {
            effects.push_back(makeApplyView(view, global_rank_states_));
        }
    }

    // Remote Offline: tear down TE link AND stop candidate probe.
    if (push.rank != rank_ && push.new_state == RankState::Offline) {
        effects.push_back(DisconnectLink{push.rank});
        effects.push_back(StopReconnect{push.rank});
        effects.push_back(NotifyLinkRefreshed{push.rank});
    }

    return effects;
}

std::pair<AgentApplyResult, bool> AgentStateMachine::handleViewUpdate(
    const ViewUpdatePush& push) {
    AgentApplyResult effects;
    auto it = groups_.find(push.group_id);
    if (it == groups_.end()) {
        LOG(WARNING) << "[AGENT] handleViewUpdate group=" << push.group_id
                     << " NOT FOUND in groups_ (epoch=" << push.view.epoch
                     << ")";
        return {effects, false};
    }

    const auto& old_view = it->second;

    // Collect peers whose segment caches must be refreshed.
    std::vector<GlobalRank> need_segment_refresh;

    // Detect endpoint updates.
    for (size_t r = 0; r < push.view.members.size(); ++r) {
        if (r == static_cast<size_t>(rank_)) continue;
        if (!push.view.members[r].isMember()) continue;
        uint64_t old_epoch = 0;
        uint64_t new_epoch = 0;
        if (old_view.members[r].hasEndpoint()) {
            old_epoch = old_view.members[r].endpoint->endpoint_epoch;
        }
        if (push.view.members[r].hasEndpoint()) {
            new_epoch = push.view.members[r].endpoint->endpoint_epoch;
        }
        if (new_epoch != 0 && new_epoch != old_epoch) {
            effects.push_back(RefreshPeerLink{static_cast<GlobalRank>(r)});
            need_segment_refresh.push_back(static_cast<GlobalRank>(r));
        }
    }

    // Detect rank activation transitions.
    if (!old_view.members.empty()) {
        std::vector<GlobalRank> newly_activated;
        for (size_t igr = 0; igr < push.view.rank_order.size(); ++igr) {
            GlobalRank gr = push.view.rank_order[igr];
            if (!old_view.members[gr].isActive() &&
                push.view.members[gr].isActive()) {
                newly_activated.push_back(gr);
            }
        }
        if (!newly_activated.empty()) {
            effects.push_back(NotifyRanksActivated{push.group_id,
                                                   std::move(newly_activated)});
        }
    }

    // Detect Ready transition.
    if (old_view.status != GroupStatus::Ready &&
        push.view.status == GroupStatus::Ready) {
        effects.push_back(NotifyGroupReady{push.group_id});
    }

    it->second = push.view;

    effects.push_back(makeApplyView(push.view, global_rank_states_));

    // Must come AFTER ApplyViewToBackend: refreshSegmentID requires
    // latest meta_->rank_order.
    for (auto gr : need_segment_refresh) {
        effects.push_back(NotifyLinkRefreshed{gr});
    }

    return {effects, true};
}

AgentApplyResult AgentStateMachine::handleLinkStateChange(GlobalRank peer,
                                                          bool connected) {
    AgentApplyResult effects;
    if (!rankInRange(peer)) {
        LOG(WARNING) << "AgentStateMachine: handleLinkStateChange out-of-range "
                     << peer;
        return effects;
    }
    link_connected_[peer] = connected;

    if (!connected) {
        effects.push_back(NotifyTEUnreachable{peer});
    } else {
        effects.push_back(NotifyLinkRefreshed{peer});
    }
    return effects;
}

HeartbeatRequest AgentStateMachine::buildHeartbeat() const {
    HeartbeatRequest req;
    req.rank = rank_;
    return req;
}

AgentApplyResult AgentStateMachine::applyRegisterAgentResponse(
    const RegisterAgentResponse& resp) {
    AgentApplyResult effects;

    if (!resp.success) {
        coordinator_connection_ = CoordinatorConnection::Disconnected;
        return effects;
    }

    if (static_cast<int>(resp.all_rank_states.size()) != max_world_size_) {
        LOG(ERROR) << "AgentStateMachine: all_rank_states size mismatch (got "
                   << resp.all_rank_states.size() << ", expected "
                   << max_world_size_ << "); rejecting.";
        return effects;
    }
    global_rank_states_ = resp.all_rank_states;
    rank_state_ = resp.all_rank_states[rank_];

    for (const auto& gv : resp.groups) {
        groups_[gv.group_id] = gv;
        effects.push_back(makeApplyView(gv, global_rank_states_));
    }

    for (const auto& conn : resp.rank_connections) {
        if (conn.rank == rank_) continue;
        rank_connections_[conn.rank] = conn;
        last_reported_peer_status_[conn.rank] = true;
        effects.push_back(EnablePeerProbe{conn.rank, conn.te_server_name,
                                          conn.warmup_recv_addr});
    }

    coordinator_connection_ = CoordinatorConnection::Connected;
    return effects;
}

AgentApplyResult AgentStateMachine::prepareCleanSlateRegister() {
    AgentApplyResult effects;

    rank_state_ = RankState::Offline;
    groups_.clear();
    global_rank_states_ = std::vector<RankState>(max_world_size_);
    std::fill(link_connected_.begin(), link_connected_.end(), false);
    std::fill(last_reported_peer_status_.begin(),
              last_reported_peer_status_.end(), false);
    for (auto& conn : rank_connections_) conn.reset();

    effects.push_back(DisconnectAllLinks{});
    effects.push_back(ClearAllPeerMetadata{});

    return effects;
}

std::optional<TransferObservationReport>
AgentStateMachine::processTransferObservation(
    const TransferObservationEvent& event) {
    if (rank_state_ == RankState::Offline) return std::nullopt;

    if (event.attempted_ranks.size() != static_cast<size_t>(max_world_size_) ||
        event.failed_ranks_hint.size() !=
            static_cast<size_t>(max_world_size_)) {
        LOG(WARNING)
            << "AgentStateMachine: invalid TransferObservationEvent vectors. "
            << "Expected max_world_size=" << max_world_size_ << "; dropping.";
        return std::nullopt;
    }

    TransferObservationReport report;
    report.reporter_rank = rank_;
    report.attempted_ranks.assign(max_world_size_, 0);
    report.failed_ranks_hint.assign(max_world_size_, 0);

    bool has_changed = false;

    for (int peer = 0; peer < max_world_size_; ++peer) {
        if (!event.attempted_ranks[peer]) continue;
        bool current = !event.failed_ranks_hint[peer];
        if (current != last_reported_peer_status_[peer]) {
            last_reported_peer_status_[peer] = current;
            report.attempted_ranks[peer] = 1;
            report.failed_ranks_hint[peer] = current ? 0 : 1;
            has_changed = true;
        }
    }

    if (has_changed) return report;
    return std::nullopt;
}

void AgentStateMachine::mergeObservationEvent(
    TransferObservationEvent& acc, const TransferObservationEvent& next) {
    for (int peer = 0; peer < max_world_size_; ++peer) {
        if (!next.attempted_ranks[peer]) continue;
        acc.attempted_ranks[peer] = 1;
        acc.failed_ranks_hint[peer] = next.failed_ranks_hint[peer];
    }
}

}  // namespace mooncake
