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
    global_rank_epochs_.assign(max_world_size_, 0);
    global_rank_state_versions_.assign(max_world_size_, 0);
    rank_connections_.resize(max_world_size_);
    observed_link_state_.assign(max_world_size_, LinkEvent::EventType::None);
    observed_target_rank_epochs_.assign(max_world_size_, 0);
}

void AgentStateMachine::appendApplyViewEffect(const GroupView& view,
                                              AgentApplyResult& effects) const {
    std::vector<bool> activatable(view.rank_order.size());
    for (size_t i = 0; i < view.rank_order.size(); ++i) {
        GlobalRank gr = view.rank_order[i];
        bool healthy = global_rank_states_[gr] == RankState::Healthy;
        const auto& member = view.members[gr];
        activatable[i] = healthy &&
                         (member.isActive() || member.isAwaitingActivation()) &&
                         member.hasEndpoint();
    }
    effects.push_back(
        ApplyViewToBackend{view.group_id, view, global_rank_states_,
                           global_rank_epochs_, std::move(activatable)});
}

AgentApplyResult AgentStateMachine::registerGroup(const GroupView& group,
                                                  bool auto_deactivate) {
    AgentApplyResult effects;
    auto it = groups_.find(group.group_id);
    if (it != groups_.end()) {
        // RegisterAgent may have installed the Coordinator snapshot before
        // this backend existed. Preserve and replay that authoritative view.
        appendApplyViewEffect(it->second, effects);
        return effects;
    }

    GroupView view = group;
    view.auto_deactivate = auto_deactivate;
    groups_.emplace(group.group_id, std::move(view));
    return effects;
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

void AgentStateMachine::appendApplyViewEffectsForRank(
    GlobalRank rank, AgentApplyResult& effects) const {
    for (const auto& [group_id, view] : groups_) {
        if (std::find(view.rank_order.begin(), view.rank_order.end(), rank) !=
            view.rank_order.end()) {
            appendApplyViewEffect(view, effects);
        }
    }
}

void AgentStateMachine::resetRankForNewEpoch(GlobalRank rank,
                                             uint64_t rank_epoch,
                                             AgentApplyResult& effects) {
    global_rank_epochs_[rank] = rank_epoch;
    global_rank_state_versions_[rank] = 0;
    global_rank_states_[rank] = RankState::Offline;
    rank_connections_[rank].reset();

    if (rank != rank_) {
        effects.push_back(DisconnectLink{rank});
        effects.push_back(StopReconnect{rank});
        effects.push_back(NotifyLinkRefreshed{rank});
    }
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

    if (push.rank_epoch < global_rank_epochs_[push.rank]) return effects;

    const bool new_rank_epoch =
        push.rank_epoch > global_rank_epochs_[push.rank];
    if (new_rank_epoch) {
        resetRankForNewEpoch(push.rank, push.rank_epoch, effects);
        appendApplyViewEffectsForRank(push.rank, effects);
    } else if (global_rank_states_[push.rank] == RankState::Offline) {
        // Offline is terminal within a rank epoch. A delayed PeerJoined for
        // that epoch must not restart the old connection.
        return effects;
    }

    if (rank_connections_[push.rank].has_value()) return effects;

    rank_connections_[push.rank] = RankConnectionMetadata{
        .rank = push.rank,
        .rank_epoch = push.rank_epoch,
        .te_server_name = push.te_server_name,
        .warmup_recv_addr = push.warmup_recv_addr,
    };
    effects.push_back(EnablePeerProbe{push.rank, push.rank_epoch,
                                      push.te_server_name,
                                      push.warmup_recv_addr});
    return effects;
}

AgentApplyResult AgentStateMachine::handleRankStateUpdate(
    const RankStatePush& push) {
    AgentApplyResult effects;
    if (!rankInRange(push.rank)) {
        LOG(WARNING) << "AgentStateMachine: handleRankStateUpdate out-of-range "
                     << push.rank;
        return effects;
    }

    if (push.rank_epoch < global_rank_epochs_[push.rank]) return effects;

    const bool new_rank_epoch =
        push.rank_epoch > global_rank_epochs_[push.rank];
    if (new_rank_epoch) {
        resetRankForNewEpoch(push.rank, push.rank_epoch, effects);
    }

    if (push.rank_state_version <= global_rank_state_versions_[push.rank])
        return effects;

    global_rank_states_[push.rank] = push.new_state;
    global_rank_state_versions_[push.rank] = push.rank_state_version;
    appendApplyViewEffectsForRank(push.rank, effects);

    // Remote Offline: tear down TE link AND stop candidate probe.
    if (push.rank != rank_ && push.new_state == RankState::Offline) {
        rank_connections_[push.rank].reset();
        if (!new_rank_epoch) {
            effects.push_back(DisconnectLink{push.rank});
            effects.push_back(StopReconnect{push.rank});
            effects.push_back(NotifyLinkRefreshed{push.rank});
        }
    }

    return effects;
}

std::pair<AgentApplyResult, bool> AgentStateMachine::applyGroupView(
    GroupId group_id, const GroupView& view) {
    AgentApplyResult effects;
    auto it = groups_.find(group_id);
    if (it == groups_.end()) {
        LOG(WARNING) << "[AGENT] applyGroupView group=" << group_id
                     << " NOT FOUND in groups_ (epoch=" << view.epoch << ")";
        return {effects, false};
    }

    const auto& old_view = it->second;

    // View application is idempotent. A sync response may race with the
    // regular ViewUpdate push for the same decision, or even arrive after a
    // newer view has already been installed.
    if (view.epoch < old_view.epoch) {
        LOG(WARNING) << "[AGENT] Ignored stale GroupView for group=" << group_id
                     << " epoch=" << view.epoch;
        return {effects, false};
    } else if (view.epoch == old_view.epoch) {
        if (view == old_view) return {effects, true};
        LOG(ERROR) << "[AGENT] Ignored conflicting GroupView for group="
                   << group_id << " epoch=" << view.epoch;
        return {effects, false};
    }

    // Collect peers whose segment caches must be refreshed.
    std::vector<GlobalRank> need_segment_refresh;

    // Detect endpoint updates.
    for (size_t r = 0; r < view.members.size(); ++r) {
        if (r == static_cast<size_t>(rank_)) continue;
        if (!view.members[r].isMember()) continue;
        uint64_t old_epoch = 0;
        uint64_t new_epoch = 0;
        if (old_view.members[r].hasEndpoint()) {
            old_epoch = old_view.members[r].endpoint->endpoint_epoch;
        }
        if (view.members[r].hasEndpoint()) {
            new_epoch = view.members[r].endpoint->endpoint_epoch;
        }
        if (new_epoch != 0 && new_epoch != old_epoch) {
            effects.push_back(RefreshPeerLink{static_cast<GlobalRank>(r)});
            need_segment_refresh.push_back(static_cast<GlobalRank>(r));
        }
    }

    // Applying the view must happen-before waking group/rank waiters: callers
    // may submit a collective as soon as waitUntilGroupReady()/joinGroup()
    // returns, and that collective must observe the new data-plane metadata.
    appendApplyViewEffect(view, effects);

    // Detect rank activation transitions.
    if (!old_view.members.empty()) {
        std::vector<GlobalRank> newly_activated;
        for (size_t igr = 0; igr < view.rank_order.size(); ++igr) {
            GlobalRank gr = view.rank_order[igr];
            if (!old_view.members[gr].isActive() &&
                view.members[gr].isActive()) {
                newly_activated.push_back(gr);
            }
        }
        if (!newly_activated.empty()) {
            effects.push_back(
                NotifyRanksActivated{group_id, std::move(newly_activated)});
        }
    }

    // Detect Ready transition.
    if (old_view.status != GroupStatus::Ready &&
        view.status == GroupStatus::Ready) {
        effects.push_back(NotifyGroupReady{group_id});
    }

    it->second = view;

    // Must come AFTER ApplyViewToBackend: refreshSegmentID requires
    // latest meta_->rank_order.
    for (auto gr : need_segment_refresh) {
        effects.push_back(NotifyLinkRefreshed{gr});
    }

    return {effects, true};
}

std::pair<AgentApplyResult, bool> AgentStateMachine::handleViewUpdate(
    const ViewUpdatePush& push) {
    return applyGroupView(push.group_id, push.view);
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

    if (static_cast<int>(resp.all_rank_states.size()) != max_world_size_ ||
        static_cast<int>(resp.all_rank_epochs.size()) != max_world_size_ ||
        static_cast<int>(resp.all_rank_state_versions.size()) !=
            max_world_size_) {
        LOG(ERROR) << "AgentStateMachine: malformed RegisterAgentResponse";
        coordinator_connection_ = CoordinatorConnection::Disconnected;
        return effects;
    }

    self_rank_epoch_.store(resp.rank_epoch, std::memory_order_release);

    // PeerJoined and RankState pushes use independent RPCs and can arrive while
    // the RegisterAgent response is in flight. Merge its snapshot monotonically
    // so it cannot roll a rank back to an older epoch or state version.
    for (int rank = 0; rank < max_world_size_; ++rank) {
        const auto response_epoch = resp.all_rank_epochs[rank];
        const auto response_version = resp.all_rank_state_versions[rank];
        if (response_epoch < global_rank_epochs_[rank] ||
            (response_epoch == global_rank_epochs_[rank] &&
             response_version < global_rank_state_versions_[rank]))
            continue;

        if (response_epoch > global_rank_epochs_[rank]) {
            resetRankForNewEpoch(rank, response_epoch, effects);
        }
        global_rank_states_[rank] = resp.all_rank_states[rank];
        global_rank_epochs_[rank] = response_epoch;
        global_rank_state_versions_[rank] = response_version;
    }

    for (const auto& gv : resp.groups) {
        groups_[gv.group_id] = gv;
        appendApplyViewEffect(gv, effects);
    }

    // The connection list belongs to the same snapshot. Do not install an
    // entry invalidated by above.
    for (const auto& connection : resp.rank_connections) {
        if (connection.rank_epoch != global_rank_epochs_[connection.rank] ||
            global_rank_states_[connection.rank] == RankState::Offline)
            continue;

        rank_connections_[connection.rank] = connection;
        effects.push_back(EnablePeerProbe{
            .rank = connection.rank,
            .rank_epoch = connection.rank_epoch,
            .te_server_name = connection.te_server_name,
            .warmup_recv_addr = connection.warmup_recv_addr,
        });
    }

    coordinator_connection_ = CoordinatorConnection::Connected;
    return effects;
}

AgentApplyResult AgentStateMachine::reset(uint64_t new_session_id) {
    AgentApplyResult effects;

    agent_session_id_.store(new_session_id, std::memory_order_release);
    self_rank_epoch_.store(0, std::memory_order_release);
    std::fill(observed_link_state_.begin(), observed_link_state_.end(),
              LinkEvent::EventType::None);
    std::fill(observed_target_rank_epochs_.begin(),
              observed_target_rank_epochs_.end(), 0);
    link_state_version_ = 0;
    acked_link_state_version_ = 0;
    groups_.clear();
    global_rank_states_ = std::vector<RankState>(max_world_size_);
    std::fill(global_rank_epochs_.begin(), global_rank_epochs_.end(), 0);
    std::fill(global_rank_state_versions_.begin(),
              global_rank_state_versions_.end(), 0);
    for (auto& conn : rank_connections_) conn.reset();

    effects.push_back(DisconnectAllLinks{});
    effects.push_back(ClearAllPeerMetadata{});

    return effects;
}

AgentApplyResult AgentStateMachine::pushLinkEvent(const LinkEvent& event) {
    AgentApplyResult effects;

    if (event.events.size() != static_cast<size_t>(max_world_size_) ||
        event.target_rank_epochs.size() !=
            static_cast<size_t>(max_world_size_)) {
        LOG(WARNING) << "AgentStateMachine: invalid LinkEvent size. "
                     << "Expected max_world_size=" << max_world_size_
                     << "; dropping.";
        return effects;
    }

    bool changed = false;
    for (int peer = 0; peer < max_world_size_; ++peer) {
        auto type = event.events[peer];
        if (type == LinkEvent::EventType::None) continue;
        if (!recordLinkEvent(peer, event.target_rank_epochs[peer], type))
            continue;

        changed = true;
        if (type == LinkEvent::EventType::Failure) {
            effects.push_back(RequestLinkHealthCheck{peer});
        } else if (peer != rank_) {
            effects.push_back(ResetPeerState{peer});
            effects.push_back(NotifyLinkRefreshed{peer});
        }
    }

    if (changed &&
        coordinator_connection_ == CoordinatorConnection::Connected) {
        auto report = getLinkEventReport();
        if (report.has_value()) {
            effects.push_back(SendLinkEventReport{std::move(*report)});
        }
    }

    return effects;
}

std::optional<LinkEventReport> AgentStateMachine::getLinkEventReport() const {
    if (link_state_version_ <= acked_link_state_version_) return std::nullopt;

    LinkEventReport report;
    report.reporter_rank = rank_;
    report.agent_session_id = getAgentSessionId();
    report.reporter_rank_epoch = getRankEpoch();
    report.report_id = link_state_version_;
    report.events = observed_link_state_;
    report.target_rank_epochs = observed_target_rank_epochs_;
    return report;
}

void AgentStateMachine::handleLinkEventReportAck(
    const LinkEventReportAck& ack) {
    if (ack.reporter_rank != rank_ ||
        ack.reporter_rank_epoch != getRankEpoch()) {
        return;
    }
    acked_link_state_version_ =
        std::max(acked_link_state_version_, ack.report_id);
}

bool AgentStateMachine::recordLinkEvent(GlobalRank peer,
                                        uint64_t target_rank_epoch,
                                        LinkEvent::EventType type) {
    if (target_rank_epoch != global_rank_epochs_[peer]) return false;

    if (observed_target_rank_epochs_[peer] == target_rank_epoch &&
        observed_link_state_[peer] == type) {
        return false;
    }

    observed_target_rank_epochs_[peer] = target_rank_epoch;
    observed_link_state_[peer] = type;
    ++link_state_version_;
    return true;
}

}  // namespace mooncake
