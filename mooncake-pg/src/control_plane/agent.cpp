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
    rank_connections_.resize(max_world_size_);
    observed_link_state_.assign(max_world_size_, LinkEvent::EventType::None);
}

static ApplyViewToBackend makeApplyViewEffect(
    const GroupView& view, const std::vector<RankState>& rank_states) {
    std::vector<bool> activatable(view.rank_order.size());
    for (size_t i = 0; i < view.rank_order.size(); ++i) {
        GlobalRank gr = view.rank_order[i];
        bool healthy = rank_states[gr] == RankState::Healthy;
        const auto& member = view.members[gr];
        activatable[i] = healthy &&
                         (member.isActive() || member.isAwaitingActivation()) &&
                         member.hasEndpoint();
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

    effects.push_back(
        EnablePeerProbe{push.rank, push.te_server_name, push.warmup_recv_addr});
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

    global_rank_states_[push.rank] = push.new_state;
    if (push.rank == rank_) {
        rank_state_ = push.new_state;
    }

    // Rank state changed: recompute activatable for every group that
    // contains this rank and push updated ApplyViewToBackend effects.
    for (const auto& [group_id, view] : groups_) {
        if (std::find(view.rank_order.begin(), view.rank_order.end(),
                      push.rank) != view.rank_order.end()) {
            effects.push_back(makeApplyViewEffect(view, global_rank_states_));
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
    effects.push_back(makeApplyViewEffect(view, global_rank_states_));

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
        effects.push_back(makeApplyViewEffect(gv, global_rank_states_));
    }

    for (const auto& conn : resp.rank_connections) {
        if (conn.rank == rank_) continue;
        rank_connections_[conn.rank] = conn;
        effects.push_back(EnablePeerProbe{conn.rank, conn.te_server_name,
                                          conn.warmup_recv_addr});
    }

    coordinator_connection_ = CoordinatorConnection::Connected;
    if (auto report = getLinkEventReport()) {
        effects.push_back(SendLinkEventReport{std::move(*report)});
    }
    return effects;
}

AgentApplyResult AgentStateMachine::reset(uint64_t new_epoch) {
    AgentApplyResult effects;

    agent_session_epoch_.store(new_epoch, std::memory_order_release);
    std::fill(observed_link_state_.begin(), observed_link_state_.end(),
              LinkEvent::EventType::None);
    link_state_version_ = 0;
    acked_link_state_version_ = 0;
    rank_state_ = RankState::Offline;
    groups_.clear();
    global_rank_states_ = std::vector<RankState>(max_world_size_);
    for (auto& conn : rank_connections_) conn.reset();

    effects.push_back(DisconnectAllLinks{});
    effects.push_back(ClearAllPeerMetadata{});

    return effects;
}

AgentApplyResult AgentStateMachine::pushLinkEvent(const LinkEvent& event) {
    AgentApplyResult effects;

    if (event.events.size() != static_cast<size_t>(max_world_size_)) {
        LOG(WARNING) << "AgentStateMachine: invalid LinkEvent size. "
                     << "Expected max_world_size=" << max_world_size_
                     << "; dropping.";
        return effects;
    }

    bool changed = false;
    for (int peer = 0; peer < max_world_size_; ++peer) {
        auto type = event.events[peer];
        if (type == LinkEvent::EventType::None) continue;
        if (!recordLinkEvent(peer, type)) continue;

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
    report.agent_session_epoch = getAgentSessionEpoch();
    report.report_id = link_state_version_;
    report.events = observed_link_state_;
    return report;
}

void AgentStateMachine::handleLinkEventReportAck(
    const LinkEventReportAck& ack) {
    if (ack.rank != rank_ ||
        ack.agent_session_epoch != getAgentSessionEpoch() ||
        ack.report_id > link_state_version_) {
        return;
    }
    acked_link_state_version_ =
        std::max(acked_link_state_version_, ack.report_id);
}

bool AgentStateMachine::recordLinkEvent(GlobalRank peer,
                                        LinkEvent::EventType type) {
    if (observed_link_state_[peer] == type) return false;

    observed_link_state_[peer] = type;
    ++link_state_version_;
    return true;
}

}  // namespace mooncake
