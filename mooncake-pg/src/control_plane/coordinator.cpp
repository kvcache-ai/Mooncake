#include "control_plane/coordinator.h"

#include <algorithm>
#include <cassert>
#include <limits>
#include <set>

#include <glog/logging.h>

#include "pg_utils.h"

namespace mooncake {

CentralizedCoordinatorStateMachine::CentralizedCoordinatorStateMachine(
    int max_world_size, std::chrono::microseconds fault_reconciliation_window)
    : max_world_size_(max_world_size),
      fault_reconciliation_window_(fault_reconciliation_window) {
    CHECK_GT(max_world_size_, 0);
    CHECK_LE(max_world_size_, kMaxNumRanks)
        << "max_world_size " << max_world_size_ << " exceeds kMaxNumRanks ("
        << kMaxNumRanks << ")";
    ranks_.resize(max_world_size_);
    endpoint_epochs_.assign(max_world_size_, 0);
    for (int r = 0; r < max_world_size_; ++r) {
        ranks_[r].link_status.assign(max_world_size_, 0);
        ranks_[r].link_status[r] = 1;
    }
}

CoordinatorApplyResult<RegisterAgentResponse>
CentralizedCoordinatorStateMachine::handleRegisterAgent(
    const RegisterAgentRequest& req) {
    CoordinatorApplyResult<RegisterAgentResponse> result;
    if (!rankInRange(req.rank)) {
        result.response.success = false;
        result.response.reject_reason = "rank out of valid range";
        return result;
    }
    auto& info = ranks_[req.rank];

    // agent_session_id is the idempotency key for a logical registration.
    // Retrying an already-accepted registration must not invalidate link
    // evidence, demote rank state, or rebroadcast lifecycle events.
    const bool same_session = info.agent_session_id == req.agent_session_id;
    if (same_session) {
        if (info.state == RankState::Offline) {
            result.response.success = false;
            result.response.reject_reason =
                "agent session is Offline; start a new registration session";
            result.response.require_new_session = true;
            return result;
        }
        info.last_heartbeat = std::chrono::steady_clock::now();
        populateRegisterAgentResponse(result.response, req.rank);
        return result;
    }

    // A failed / auto-deactivated rank (Synced or Offline) may be replaced
    // immediately. A different logical session may not take ownership from a
    // Healthy rank.
    if (info.state == RankState::Healthy) {
        result.response.success = false;
        result.response.reject_reason =
            "rank already registered and is Healthy; replacement must wait "
            "for the old process to leave the healthy set.";
        return result;
    }

    ++info.rank_epoch;
    info.agent_addr = req.agent_addr;
    info.te_server_name = req.te_server_name;
    info.agent_session_id = req.agent_session_id;
    info.warmup_recv_addr = req.warmup_recv_addr;
    info.last_heartbeat = std::chrono::steady_clock::now();

    // A new rank epoch invalidates both outgoing and incoming observations for
    // the previous incarnation. No old edge is allowed to make the replacement
    // Healthy before fresh, epoch-matched evidence arrives.
    info.link_status.assign(max_world_size_, 0);
    for (auto& peer : ranks_) {
        peer.link_status[req.rank] = 0;
    }
    info.link_status[req.rank] = 1;
    info.last_link_event_report_id = 0;

    for (auto& [group_id, view] : group_views_) {
        auto& member = view.members[req.rank];
        // AwaitingActivation is an uncommitted promise made by the old Agent
        // session, so a new rank epoch cancels it. Active membership is already
        // committed and must only be changed by explicit or automatic
        // deactivation paths, never by registration.
        bool view_changed = false;
        if (member.isAwaitingActivation()) {
            member.status = GroupMemberState::Inactive;
            view_changed = true;
        }

        // Published endpoints belong to one rank epoch. Repeat the Offline
        // reset here for replacements accepted before heartbeat timeout.
        if (member.hasEndpoint()) {
            member.endpoint = std::nullopt;
            view_changed = true;
        }

        if (view_changed) {
            view.epoch++;
            result.effects.push_back(PushViewUpdate{view});
        }
    }

    info.state = RankState::Synced;
    ++info.rank_state_version;

    result.effects.push_back(BroadcastPeerJoined{
        PeerJoinedPush{req.rank, info.rank_epoch, info.te_server_name,
                       info.warmup_recv_addr}});
    result.effects.push_back(makeRankStateEffect(req.rank));

    populateRegisterAgentResponse(result.response, req.rank);
    return result;
}

void CentralizedCoordinatorStateMachine::populateRegisterAgentResponse(
    RegisterAgentResponse& response, GlobalRank rank) const {
    response.success = true;
    response.rank_epoch = ranks_[rank].rank_epoch;
    response.all_rank_states.resize(max_world_size_);
    response.all_rank_epochs.resize(max_world_size_);
    response.all_rank_state_versions.resize(max_world_size_);
    for (int32_t i = 0; i < max_world_size_; ++i) {
        response.all_rank_states[i] = ranks_[i].state;
        response.all_rank_epochs[i] = ranks_[i].rank_epoch;
        response.all_rank_state_versions[i] = ranks_[i].rank_state_version;
    }
    response.groups.reserve(group_views_.size());
    for (const auto& [group_id, view] : group_views_) {
        response.groups.push_back(view);
    }
    response.rank_connections.reserve(max_world_size_);
    for (int32_t i = 0; i < max_world_size_; ++i) {
        if (i == rank || ranks_[i].state == RankState::Offline) continue;
        RankConnectionMetadata connection;
        connection.rank = i;
        connection.rank_epoch = ranks_[i].rank_epoch;
        connection.agent_addr = ranks_[i].agent_addr;
        connection.te_server_name = ranks_[i].te_server_name;
        connection.warmup_recv_addr = ranks_[i].warmup_recv_addr;
        response.rank_connections.push_back(std::move(connection));
    }
}

CoordinatorApplyResult<HeartbeatResponse>
CentralizedCoordinatorStateMachine::handleHeartbeat(
    const HeartbeatRequest& req) {
    CoordinatorApplyResult<HeartbeatResponse> result;
    if (!hasValidSession(req.rank, req.agent_session_id)) {
        result.response.require_new_session = true;
        return result;
    }
    auto& info = ranks_[req.rank];
    info.last_heartbeat = std::chrono::steady_clock::now();
    return result;
}

CoordinatorApplyResult<RegisterGroupResponse>
CentralizedCoordinatorStateMachine::handleRegisterGroup(
    const RegisterGroupRequest& req) {
    CoordinatorApplyResult<RegisterGroupResponse> result;
    if (!hasValidSession(req.rank, req.agent_session_id)) {
        result.response.success = false;
        result.response.reject_reason = "rank out of range or stale session";
        return result;
    }

    bool ok =
        processGroupRegistration(req.rank, req.group, req.group.auto_deactivate,
                                 result.response, result.effects);
    if (!ok) return result;

    result.response.success = true;
    return result;
}

CoordinatorApplyResult<ConfirmReadyForActivationResponse>
CentralizedCoordinatorStateMachine::handleConfirmReadyForActivation(
    const ConfirmReadyForActivationRequest& req) {
    CoordinatorApplyResult<ConfirmReadyForActivationResponse> result;
    if (!hasValidSession(req.rank, req.agent_session_id)) {
        result.response.reject_reason =
            "rank is out of range or has a stale session";
        return result;
    }

    auto group_it = group_views_.find(req.group_id);
    if (group_it == group_views_.end()) {
        result.response.reject_reason = "group not found";
        return result;
    }

    auto& view = group_it->second;
    auto& member = view.members[req.rank];
    if (member.isAwaitingActivation()) {
        result.response.success = true;
        return result;
    }
    if (member.status != GroupMemberState::Inactive) {
        result.response.reject_reason = "rank is not an inactive member";
        return result;
    }

    member.status = GroupMemberState::AwaitingActivation;
    view.epoch++;
    result.effects.push_back(PushViewUpdate{view});
    result.response.success = true;
    return result;
}

CoordinatorApplyResult<void>
CentralizedCoordinatorStateMachine::handleUnregisterGroup(
    const UnregisterGroupRequest& req) {
    CoordinatorApplyResult<void> result;
    if (!hasValidSession(req.rank, req.agent_session_id)) {
        return result;
    }

    auto it = group_views_.find(req.group_id);
    if (it == group_views_.end()) {
        return result;
    }

    auto& view = it->second;
    auto& member = view.members[req.rank];
    if (member.hasLeft()) {
        return result;
    }

    member.status = GroupMemberState::Left;
    member.endpoint = std::nullopt;
    view.epoch++;
    rejectPendingSyncs(req.group_id, req.rank, "rank left the group",
                       result.effects);

    // Don't push a ViewUpdate when other members remain. The departing
    // rank's unregister races with in-flight collectives on survivors:
    // a ViewUpdate that changes activeRanks mid-collective may corrupt
    // the result.
    if (canEraseGroup(view)) {
        eraseGroup(req.group_id, result.effects);
    }
    return result;
}

CoordinatorApplyResult<PublishEndpointResponse>
CentralizedCoordinatorStateMachine::handlePublishEndpoint(
    const PublishEndpointRequest& req) {
    CoordinatorApplyResult<PublishEndpointResponse> result;
    if (!hasValidSession(req.rank, req.agent_session_id)) {
        result.response.success = false;
        result.response.reject_reason = "rank out of range or stale session";
        return result;
    }

    for (const auto& ep : req.endpoints) {
        auto it = group_views_.find(ep.group_id);
        if (it == group_views_.end()) {
            result.response.success = false;
            result.response.reject_reason = "group not found";
            return result;
        }

        auto& view = it->second;
        auto& member = view.members[req.rank];
        member.endpoint = ep.endpoint_info;
        member.endpoint->endpoint_epoch = ++endpoint_epochs_[req.rank];

        if (member.isMember() && view.status == GroupStatus::Ready) {
            view.epoch++;
            result.effects.push_back(PushViewUpdate{view});
        }
    }

    result.response.success = true;
    checkGroupTransitions(result.effects);
    return result;
}

CoordinatorApplyResult<void>
CentralizedCoordinatorStateMachine::handleProposeViewUpdate(
    uint64_t propose_id, const ProposeViewUpdateRequest& req) {
    CoordinatorApplyResult<void> result;
    auto it = group_views_.find(req.group_id);
    if (it == group_views_.end()) {
        result.effects.push_back(ReplyProposal{
            propose_id,
            {ViewUpdateStatus::Rejected, 0, {}, "group not found"}});
        return result;
    }

    GroupView& view = it->second;
    bool changed = false;

    // Reject stale or offline proposer.
    if (!hasValidSession(req.source_rank, req.agent_session_id)) {
        result.effects.push_back(
            ReplyProposal{propose_id,
                          {ViewUpdateStatus::Rejected,
                           view.epoch,
                           {},
                           "source rank is Offline or has a stale session"}});
        return result;
    }

    for (GlobalRank rank : req.requested_ranks) {
        if (!rankInRange(rank)) {
            result.effects.push_back(
                ReplyProposal{propose_id,
                              {ViewUpdateStatus::Rejected,
                               view.epoch,
                               {},
                               "target rank is out of valid range"}});
            return result;
        }
    }

    // Snapshot old view before mutating
    GroupView old_view = view;

    if (req.is_activation) {
        // Check activatable before applying changes.
        // rank_order is managed exclusively in processGroupRegistration.
        for (GlobalRank rank : req.requested_ranks) {
            if (!view.members[rank].isActive()) changed = true;
        }

        if (changed &&
            !isActivatableSet(req.group_id, req.requested_ranks, view)) {
            result.effects.push_back(
                ReplyProposal{propose_id,
                              {ViewUpdateStatus::Rejected,
                               view.epoch,
                               {},
                               "new active set is not activatable"}});
            return result;
        }

        // Apply.
        for (GlobalRank rank : req.requested_ranks) {
            view.members[rank].status = GroupMemberState::Active;
        }
    } else {
        // deactivate
        for (GlobalRank rank : req.requested_ranks) {
            if (view.members[rank].isActive()) {
                view.members[rank].status = GroupMemberState::Inactive;
                view.members[rank].endpoint = std::nullopt;
                changed = true;
            }
        }
    }

    if (!changed) {
        result.effects.push_back(ReplyProposal{
            propose_id, {ViewUpdateStatus::Applied, view.epoch, {}, ""}});
        return result;
    }

    view.epoch++;

    auto required_acks = computeBarrierAckSet(old_view, view);
    pending_barriers_[req.group_id][view.epoch] = PendingViewUpdateBarrier{
        req.group_id, view.epoch, std::move(required_acks),
        std::chrono::steady_clock::now() + kProposeTimeout,
        PendingViewUpdateBarrier::ProposalCommit{
            propose_id, {ViewUpdateStatus::Applied, view.epoch, {}, ""}}};
    result.effects.push_back(PushViewUpdate{view});

    return result;
}

// Update link_status from data-plane evidence. Negative transitions open the
// shared reconciliation window; the healthy-set and membership decision is
// deferred until that window closes. Positive-only transitions are applied
// immediately only when no reconciliation is already in progress.
CoordinatorApplyResult<void>
CentralizedCoordinatorStateMachine::handleLinkEventReport(
    const LinkEventReport& req) {
    CoordinatorApplyResult<void> result;
    processLinkEventReport(req, result.effects);
    return result;
}

void CentralizedCoordinatorStateMachine::tryOpenReconciliationWindow() {
    if (!reconciliation_ctx_.active) {
        reconciliation_ctx_.active = true;
        reconciliation_ctx_.deadline =
            std::chrono::steady_clock::now() + fault_reconciliation_window_;
        reconciliation_ctx_.pending_syncs.clear();
    }
}

void CentralizedCoordinatorStateMachine::tryCloseReconciliationWindow(
    std::vector<CoordinatorEffect>& effects) {
    if (!reconciliation_ctx_.active) return;
    if (std::chrono::steady_clock::now() < reconciliation_ctx_.deadline) {
        return;
    }

    LOG(INFO) << "[COORD] Reconciliation window expired.";
    updateRankStates(effects);
    applyAutoDeactivate(effects);
    checkGroupTransitions(effects);
    resolvePendingSyncs(effects);

    reconciliation_ctx_.active = false;
}

void CentralizedCoordinatorStateMachine::processLinkEventReport(
    const LinkEventReport& report, std::vector<CoordinatorEffect>& effects) {
    if (!hasValidSession(report.reporter_rank, report.agent_session_id)) {
        return;
    }
    const auto& reporter_info = ranks_[report.reporter_rank];
    if (report.reporter_rank_epoch != reporter_info.rank_epoch) return;

    if (report.events.size() != static_cast<size_t>(max_world_size_) ||
        report.target_rank_epochs.size() !=
            static_cast<size_t>(max_world_size_)) {
        LOG(WARNING) << "[COORD] invalid LinkEventReport vectors";
        return;
    }

    effects.push_back(AckLinkEventReport{LinkEventReportAck{
        report.reporter_rank, report.reporter_rank_epoch, report.report_id}});

    auto& reporter = ranks_[report.reporter_rank];
    if (report.report_id <= reporter.last_link_event_report_id) return;
    reporter.last_link_event_report_id = report.report_id;

    bool has_positive = false;
    bool has_negative = false;
    for (int32_t peer = 0; peer < max_world_size_; ++peer) {
        auto type = report.events[peer];
        if (type == LinkEvent::EventType::None) continue;

        const auto& target = ranks_[peer];
        if (target.state == RankState::Offline ||
            report.target_rank_epochs[peer] != target.rank_epoch) {
            continue;
        }

        bool was_up = reporter.link_status[peer] != 0;
        bool is_up = type == LinkEvent::EventType::Success;
        if (was_up == is_up) continue;

        reporter.link_status[peer] = is_up ? 1 : 0;
        if (is_up) {
            has_positive = true;
        } else {
            has_negative = true;
        }
    }

    // Negative evidence opens a reconciliation window. Any positive changes
    // in the same report are applied when the window closes.
    if (has_negative) {
        LOG(INFO) << "[COORD] LinkEventReport has negative -> try opening "
                     "reconciliation window";
        tryOpenReconciliationWindow();
    } else if (has_positive && !reconciliation_ctx_.active) {
        // Positive-only changes do not need reconciliation.
        updateRankStates(effects);
        checkGroupTransitions(effects);
    }
}

// handleSyncAfterFailure - sync-after-failure RPC handler.
CoordinatorApplyResult<void>
CentralizedCoordinatorStateMachine::handleSyncAfterFailure(
    uint64_t sync_id, const SyncAfterFailureRequest& req) {
    CoordinatorApplyResult<void> result;

    if (!hasValidSession(req.reporter_rank, req.agent_session_id)) {
        SyncAfterFailureResponse response;
        response.status = SyncAfterFailureStatus::Rejected;
        response.reject_reason = "rank out of range or stale session";
        result.effects.push_back(ReplySync{sync_id, response});
        return result;
    }
    auto view_it = group_views_.find(req.group_id);
    if (view_it == group_views_.end()) {
        SyncAfterFailureResponse response;
        response.status = SyncAfterFailureStatus::Rejected;
        response.reject_reason = "group not found";
        result.effects.push_back(ReplySync{sync_id, response});
        return result;
    }

    // Apply piggybacked link event report inline.
    if (req.link_event_report.has_value() &&
        req.link_event_report->reporter_rank == req.reporter_rank &&
        req.link_event_report->agent_session_id == req.agent_session_id) {
        processLinkEventReport(*req.link_event_report, result.effects);
    }

    if (reconciliation_ctx_.active) {
        reconciliation_ctx_.pending_syncs[req.group_id][req.reporter_rank]
            .push_back(PendingSync{sync_id, req.agent_session_id});
        return result;
    }

    // The link state report was either already consumed by a completed window,
    // or there is no pending decision. Return the current authoritative view
    // and let AgentHost apply it synchronously before exposing the response.
    auto response =
        makeSyncResponse(SyncAfterFailureStatus::NoPending, req.group_id);
    result.effects.push_back(ReplySync{sync_id, std::move(response)});
    return result;
}

// handleViewUpdateAck - unified ACK handler for all ViewUpdate pushes.
CoordinatorApplyResult<void>
CentralizedCoordinatorStateMachine::handleViewUpdateAck(GroupId group_id,
                                                        GlobalRank rank,
                                                        uint64_t epoch,
                                                        bool applied) {
    CoordinatorApplyResult<void> result;

    if (!applied) return result;

    auto view_it = group_views_.find(group_id);
    if (view_it == group_views_.end()) return result;
    if (epoch != view_it->second.epoch) return result;  // stale ACK

    // Decrement any pending ViewUpdate barrier matching this epoch.
    auto group_it = pending_barriers_.find(group_id);
    if (group_it != pending_barriers_.end()) {
        auto epoch_it = group_it->second.find(epoch);
        if (epoch_it != group_it->second.end()) {
            auto& barrier = epoch_it->second;
            barrier.waiting_acks.erase(rank);
            if (barrier.waiting_acks.empty()) {
                commitBarrier(std::move(barrier), {}, result.effects);
                group_it->second.erase(epoch_it);
            }
        }
    }

    return result;
}

CoordinatorApplyResult<void> CentralizedCoordinatorStateMachine::tick() {
    CoordinatorApplyResult<void> result;
    auto now = std::chrono::steady_clock::now();

    // Heartbeat timeout
    for (int rank = 0; rank < max_world_size_; ++rank) {
        auto& info = ranks_[rank];
        if (info.state == RankState::Offline) continue;
        if (now - info.last_heartbeat > kHeartbeatTimeout) {
            transitionToOffline(rank, "heartbeat timeout", result.effects);
        }
    }

    // Pending ViewUpdate barrier timeouts
    for (auto& [group_id, inner] : pending_barriers_) {
        for (auto it = inner.begin(); it != inner.end();) {
            auto& barrier = it->second;
            if (!barrier.deadline.has_value() || now <= *barrier.deadline) {
                ++it;
                continue;
            }
            // Deadline passed: remaining waiting ranks are treated as dropped.
            std::vector<GlobalRank> dropped(barrier.waiting_acks.begin(),
                                            barrier.waiting_acks.end());
            commitBarrier(std::move(barrier), dropped, result.effects);
            it = inner.erase(it);
        }
    }

    tryCloseReconciliationWindow(result.effects);

    return result;
}

void CentralizedCoordinatorStateMachine::transitionToOffline(
    GlobalRank rank, const char* reason,
    std::vector<CoordinatorEffect>& effects) {
    if (ranks_[rank].state == RankState::Offline) return;

    LOG(INFO) << "[COORD] transitionToOffline rank=" << rank
              << " state=" << static_cast<int>(ranks_[rank].state)
              << " reason=" << reason;
    ranks_[rank].state = RankState::Offline;
    ++ranks_[rank].rank_state_version;
    ranks_[rank].link_status.assign(max_world_size_, 0);

    // Clear this rank's connectivity from all peers.
    for (auto& peer : ranks_) {
        if (static_cast<size_t>(rank) < peer.link_status.size())
            peer.link_status[rank] = 0;
    }

    std::vector<GroupId> pending_groups;
    pending_groups.reserve(reconciliation_ctx_.pending_syncs.size());
    for (const auto& [group_id, _] : reconciliation_ctx_.pending_syncs) {
        pending_groups.push_back(group_id);
    }
    for (const auto& group_id : pending_groups) {
        rejectPendingSyncs(group_id, rank, "rank went offline", effects);
    }

    for (auto& [group_id, view] : group_views_) {
        auto& member = view.members[rank];
        bool view_changed = false;

        // AwaitingActivation must be revoked in every group when the rank goes
        // Offline, independently of that group's auto_deactivate policy. Active
        // membership is handled below and is demoted only when auto_deactivate
        // is enabled for the group.
        if (member.isAwaitingActivation()) {
            member.status = GroupMemberState::Inactive;
            view_changed = true;
        }

        // Endpoint validity is independent of collective membership. Once a
        // rank is Offline, every group must discard its published endpoint and
        // wait for AgentHost to publish it again after re-registration.
        if (member.hasEndpoint()) {
            member.endpoint = std::nullopt;
            view_changed = true;
        }

        if (view.auto_deactivate && member.isActive()) {
            member.status = GroupMemberState::Inactive;
            view_changed = true;
        }

        if (view_changed) {
            view.epoch++;
            effects.push_back(PushViewUpdate{view});
        }
    }

    effects.push_back(makeRankStateEffect(rank));
    updateRankStates(effects);
    applyAutoDeactivate(effects);
    checkGroupTransitions(effects);
}

bool CentralizedCoordinatorStateMachine::isMutuallyConnected(
    GlobalRank a, GlobalRank b) const {
    assert(rankInRange(a) && rankInRange(b));
    if (a == b) return true;
    if (ranks_[a].state == RankState::Offline ||
        ranks_[b].state == RankState::Offline)
        return false;
    return static_cast<size_t>(b) < ranks_[a].link_status.size() &&
           static_cast<size_t>(a) < ranks_[b].link_status.size() &&
           ranks_[a].link_status[b] != 0 && ranks_[b].link_status[a] != 0;
}

std::vector<GlobalRank> CentralizedCoordinatorStateMachine::extendHealthySet()
    const {
    //  Collect current Healthy ranks.
    std::vector<GlobalRank> result;
    for (int i = 0; i < max_world_size_; ++i) {
        if (ranks_[i].state == RankState::Healthy) {
            result.push_back(i);
        }
    }

    // Evict the least-connected rank until the set is a clique.
    // (Focuses strictly on connection density; naturally terminates on
    // singletons).
    while (true) {
        GlobalRank worst = kInvalidGlobalRank;
        int worst_degree = std::numeric_limits<int>::max();

        for (GlobalRank r : result) {
            int degree = 0;
            for (GlobalRank other : result) {
                if (r == other) continue;
                if (isMutuallyConnected(r, other)) ++degree;
            }
            if (degree < worst_degree ||
                (degree == worst_degree &&
                 (worst == kInvalidGlobalRank || r > worst))) {
                worst_degree = degree;
                worst = r;
            }
        }

        int expected = static_cast<int>(result.size()) - 1;
        if (worst_degree >= expected) break;

        result.erase(std::remove(result.begin(), result.end(), worst),
                     result.end());
    }

    // Evict isolated singletons
    if (result.size() == 1) {
        GlobalRank singleton = result[0];
        bool has_connections = false;
        for (int other = 0; other < max_world_size_; ++other) {
            if (other == singleton) continue;
            if (ranks_[other].state == RankState::Offline) continue;
            if (isMutuallyConnected(singleton, other)) {
                has_connections = true;
                break;
            }
        }
        if (!has_connections) {
            result.clear();
        }
    }

    // Extend with new mutually-connected candidates.
    for (int i = 0; i < max_world_size_; ++i) {
        if (ranks_[i].state == RankState::Offline) continue;
        if (std::find(result.begin(), result.end(), i) != result.end())
            continue;
        bool connected_to_all = true;
        for (GlobalRank existing : result) {
            if (!isMutuallyConnected(i, existing)) {
                connected_to_all = false;
                break;
            }
        }
        if (connected_to_all) {
            result.push_back(i);
        }
    }

    return result;
}

void CentralizedCoordinatorStateMachine::updateRankStates(
    std::vector<CoordinatorEffect>& effects) {
    auto healthy_set = extendHealthySet();

    // Update per-rank Healthy / Synced state.
    for (int i = 0; i < max_world_size_; ++i) {
        if (ranks_[i].state == RankState::Offline) continue;

        bool in_healthy = std::find(healthy_set.begin(), healthy_set.end(),
                                    i) != healthy_set.end();

        if (in_healthy && ranks_[i].state != RankState::Healthy) {
            ranks_[i].state = RankState::Healthy;
            ++ranks_[i].rank_state_version;
            effects.push_back(makeRankStateEffect(i));
        } else if (!in_healthy && ranks_[i].state == RankState::Healthy) {
            ranks_[i].state = RankState::Synced;
            ++ranks_[i].rank_state_version;
            effects.push_back(makeRankStateEffect(i));
        }
    }
}

void CentralizedCoordinatorStateMachine::applyAutoDeactivate(
    std::vector<CoordinatorEffect>& effects) {
    auto healthy_set = extendHealthySet();

    // For auto_deactivate groups, remove unhealthy ranks from the active set.
    // However, during bootstrap we do NOT do this: we wait for full mutual
    // connectivity and let waitUntilGroupReady() time out if a peer is truly
    // dead.
    for (auto& [group_id, view] : group_views_) {
        if (!view.auto_deactivate) continue;
        if (view.status != GroupStatus::Ready) continue;
        std::vector<GlobalRank> deactivated_ranks;
        for (int i = 0; i < max_world_size_; ++i) {
            if (!view.members[i].isActive()) continue;
            bool in_healthy = std::find(healthy_set.begin(), healthy_set.end(),
                                        i) != healthy_set.end();
            if (!in_healthy) {
                view.members[i].status = GroupMemberState::Inactive;
                view.members[i].endpoint = std::nullopt;
                deactivated_ranks.push_back(i);
                LOG(INFO) << "[COORD] auto_deactivate group=" << group_id
                          << " rank=" << i;
            }
        }
        if (!deactivated_ranks.empty()) {
            view.epoch++;
            effects.push_back(PushViewUpdate{view});
            LOG(INFO) << "[COORD] auto_deactivate view update group="
                      << group_id << " epoch=" << view.epoch;
        }
    }
}

bool CentralizedCoordinatorStateMachine::isActivatableSet(
    GroupId group_id, const std::vector<GlobalRank>& new_ranks,
    const GroupView& old_view) const {
    // Build the future active set: old active + new ranks.
    std::vector<GlobalRank> future_active;
    for (int i = 0; i < max_world_size_; ++i) {
        if (old_view.members[i].isActive()) {
            future_active.push_back(i);
        }
    }
    for (GlobalRank r : new_ranks) {
        if (!old_view.members[r].isActive()) {
            future_active.push_back(r);
        }
    }

    // Every rank in the future set must be activatable with respect to the
    // full future set.  This guarantees all-to-all mutual connectivity:
    // old <-> old, old <-> new, and new <-> new.
    for (GlobalRank r : future_active) {
        if (!isRankActivatable(group_id, r, future_active)) {
            return false;
        }
    }
    return true;
}

bool CentralizedCoordinatorStateMachine::isRankActivatable(
    GroupId group_id, GlobalRank rank,
    const std::vector<GlobalRank>& future_active) const {
    if (!rankInRange(rank)) {
        return false;
    }
    if (ranks_[rank].state != RankState::Healthy) {
        return false;
    }

    for (GlobalRank other : future_active) {
        if (other == rank) continue;
        if (!isMutuallyConnected(rank, other)) {
            return false;
        }
    }

    auto group = group_views_.find(group_id);
    if (group == group_views_.end()) {
        return false;
    }

    const auto& member = group->second.members[rank];
    return (member.isActive() || member.isAwaitingActivation()) &&
           member.hasEndpoint();
}

void CentralizedCoordinatorStateMachine::checkGroupTransitions(
    std::vector<CoordinatorEffect>& effects) {
    for (auto& [group_id, view] : group_views_) {
        if (view.status == GroupStatus::Bootstrapping) {
            // Collect all active ranks.
            std::vector<GlobalRank> active;
            bool has_any_active = false;
            for (int i = 0; i < max_world_size_; ++i) {
                if (!view.members[i].isActive()) continue;
                has_any_active = true;
                active.push_back(i);
            }

            bool all_ready = true;
            for (GlobalRank r : active) {
                if (!isRankActivatable(group_id, r, active)) {
                    all_ready = false;
                    break;
                }
            }

            if (has_any_active && all_ready) {
                // All active ranks have endpoints and are Healthy.
                // Transition to BootstrapSyncing and initiate a barrier.
                view.status = GroupStatus::BootstrapSyncing;
                view.epoch++;

                auto required_acks = computeBarrierAckSet(view, view);
                pending_barriers_[group_id][view.epoch] =
                    PendingViewUpdateBarrier{
                        group_id, view.epoch, std::move(required_acks),
                        std::nullopt,
                        PendingViewUpdateBarrier::BootstrapCommit{}};

                effects.push_back(PushViewUpdate{view});
            }
        }
        // BootstrapSyncing -> Ready is done in commitBarrier when all required
        // ACKs arrive
    }
}

bool CentralizedCoordinatorStateMachine::processGroupRegistration(
    GlobalRank joining_rank, const GroupView& group, bool auto_deactivate,
    RegisterGroupResponse& response, std::vector<CoordinatorEffect>& effects) {
    GroupId group_id = group.group_id;

    // Validate joining_rank
    if (!rankInRange(joining_rank)) {
        response.success = false;
        response.reject_reason = "joining rank is out of valid range";
        return false;
    }

    // Validate rank_order elements.
    for (GlobalRank r : group.rank_order) {
        if (!rankInRange(r)) {
            response.success = false;
            response.reject_reason = "rank_order contains invalid GlobalRank";
            return false;
        }
    }

    // Validate no duplicates in rank_order.
    {
        std::set<GlobalRank> seen(group.rank_order.begin(),
                                  group.rank_order.end());
        if (seen.size() != group.rank_order.size()) {
            response.success = false;
            response.reject_reason = "rank_order contains duplicate ranks";
            return false;
        }
    }

    // The joining rank must be one of the ranks it declares in rank_order.
    if (std::find(group.rank_order.begin(), group.rank_order.end(),
                  joining_rank) == group.rank_order.end()) {
        response.success = false;
        response.reject_reason = "joining rank not in rank_order";
        return false;
    }

    auto it = group_views_.find(group_id);
    if (it == group_views_.end()) {
        // First declaration -> create group.
        // Founding members are all entries in rank_order.
        GroupView view;
        view.group_id = group_id;
        view.rank_order = group.rank_order;
        view.members.resize(max_world_size_);
        for (GlobalRank r : group.rank_order) {
            view.members[r].status = GroupMemberState::Active;
        }
        view.status = GroupStatus::Bootstrapping;
        group_views_[group_id] = std::move(view);
        group_views_[group_id].auto_deactivate = auto_deactivate;
        response.success = true;
        return true;
    }

    // Group already exists -> validate that the new rank_order is compatible
    // with the existing one.  The first backend to declare the group sets the
    // initial rank_order; later backends must agree on all overlapping
    // positions.
    const auto& existing_order = group_views_[group_id].rank_order;
    const auto& new_order = group.rank_order;

    auto common_len = std::min(existing_order.size(), new_order.size());
    for (int i = 0; i < static_cast<int>(common_len); ++i) {
        if (new_order[i] != existing_order[i]) {
            response.success = false;
            response.reject_reason =
                "rank_order mismatch at position " + std::to_string(i);
            return false;
        }
    }

    // If new_order is longer than existing_order, the extra ranks are
    // not activated here.  They must be activated via a subsequent
    // proposeViewUpdate (activate_rank / recover_ranks) from an existing
    // active member.
    //
    // However, extend the existing rank_order with the new ranks now so that
    // every member's ViewUpdate carries the correct rank_order (local->global
    // mapping).
    bool view_changed = false;
    if (new_order.size() > existing_order.size()) {
        group_views_[group_id].rank_order = new_order;
        view_changed = true;
    }

    auto& view = group_views_[group_id];
    auto& joining_member = view.members[joining_rank];
    if (joining_member.status == GroupMemberState::None) {
        joining_member.status = GroupMemberState::Inactive;
        view_changed = true;
    }

    // A Ready group that receives a registerGroup should push the authoritative
    // Ready view to all members (including the newly-joined inactive rank) so
    // that joining ranks can observe Ready and unblock waitUntilGroupReady().
    if (view.status == GroupStatus::Ready) {
        // A changed payload must never be published under an epoch that agents
        // may already have applied. Repeated registrations with no view change
        // remain idempotent and reuse the current epoch.
        if (view_changed) view.epoch++;
        effects.push_back(PushViewUpdate{view});
    }

    response.success = true;
    return true;
}

// Private: helpers

bool CentralizedCoordinatorStateMachine::canEraseGroup(
    const GroupView& view) const {
    return std::all_of(view.members.begin(), view.members.end(),
                       [](const GroupMember& m) {
                           return m.status == GroupMemberState::None ||
                                  m.status == GroupMemberState::Left;
                       });
}

void CentralizedCoordinatorStateMachine::eraseGroup(
    GroupId group_id, std::vector<CoordinatorEffect>& effects) {
    // Erase any pending ViewUpdate barriers for this group so replies are not
    // sent after the group is gone.
    auto it = pending_barriers_.find(group_id);
    if (it != pending_barriers_.end()) {
        for (auto& [epoch, barrier] : it->second) {
            if (auto* pc =
                    std::get_if<PendingViewUpdateBarrier::ProposalCommit>(
                        &barrier.commit)) {
                effects.push_back(ReplyProposal{pc->propose_id,
                                                {ViewUpdateStatus::Rejected,
                                                 0,
                                                 {},
                                                 "group was destroyed"}});
            }
            // Bootstrap barriers need no reply.
        }
        pending_barriers_.erase(it);
    }

    // Reject any pending sync-after-failure requests: the group is gone.
    rejectPendingSyncs(group_id, "group was destroyed", effects);

    group_views_.erase(group_id);
}

// Effect factories

CoordinatorEffect CentralizedCoordinatorStateMachine::makeRankStateEffect(
    GlobalRank rank) {
    return BroadcastRankState{RankStatePush{rank, ranks_[rank].rank_epoch,
                                            ranks_[rank].rank_state_version,
                                            ranks_[rank].state}};
}

void CentralizedCoordinatorStateMachine::commitBarrier(
    PendingViewUpdateBarrier barrier, const std::vector<GlobalRank>& dropped,
    std::vector<CoordinatorEffect>& effects) {
    std::visit(
        overloaded{
            [&](const PendingViewUpdateBarrier::ProposalCommit& commit) {
                auto response = commit.eventual_response;
                if (!dropped.empty()) {
                    response.status = ViewUpdateStatus::AppliedWithDroppedRanks;
                    response.dropped_ranks = dropped;
                    for (GlobalRank rank : dropped) {
                        transitionToOffline(rank, "ViewUpdate barrier timeout",
                                            effects);
                    }
                }
                effects.push_back(ReplyProposal{commit.propose_id, response});
            },
            [&](const PendingViewUpdateBarrier::BootstrapCommit&) {
                auto it = group_views_.find(barrier.group_id);
                if (it == group_views_.end()) return;
                GroupView& view = it->second;
                view.status = GroupStatus::Ready;
                view.epoch++;
                effects.push_back(PushViewUpdate{view});
            },
        },
        barrier.commit);
}

void CentralizedCoordinatorStateMachine::rejectPendingSyncs(
    GroupId group_id, GlobalRank rank, const std::string& reason,
    std::vector<CoordinatorEffect>& effects) {
    auto& pending_syncs = reconciliation_ctx_.pending_syncs;
    auto group_it = pending_syncs.find(group_id);
    if (group_it == pending_syncs.end()) return;

    auto rank_it = group_it->second.find(rank);
    if (rank_it == group_it->second.end()) return;

    for (const PendingSync& pending : rank_it->second) {
        auto resp =
            makeSyncResponse(SyncAfterFailureStatus::Rejected, group_id);
        resp.reject_reason = reason;
        effects.push_back(ReplySync{pending.sync_id, std::move(resp)});
    }
    group_it->second.erase(rank_it);
    if (group_it->second.empty()) {
        pending_syncs.erase(group_it);
    }
}

// rejectPendingSyncs - reject all pending syncs for a group.
void CentralizedCoordinatorStateMachine::rejectPendingSyncs(
    GroupId group_id, const std::string& reason,
    std::vector<CoordinatorEffect>& effects) {
    auto& pending_syncs = reconciliation_ctx_.pending_syncs;
    auto group_it = pending_syncs.find(group_id);
    if (group_it == pending_syncs.end()) return;

    std::vector<GlobalRank> ranks;
    ranks.reserve(group_it->second.size());
    for (const auto& [rank, _] : group_it->second) {
        ranks.push_back(rank);
    }
    for (GlobalRank rank : ranks) {
        rejectPendingSyncs(group_id, rank, reason, effects);
    }
}

SyncAfterFailureResponse CentralizedCoordinatorStateMachine::makeSyncResponse(
    SyncAfterFailureStatus status, GroupId group_id) const {
    SyncAfterFailureResponse response;
    response.status = status;

    // piggybacked view update
    if (status != SyncAfterFailureStatus::Rejected) {
        auto view_it = group_views_.find(group_id);
        if (view_it != group_views_.end()) {
            response.view = view_it->second;
            response.new_epoch = view_it->second.epoch;
        }
    }
    return response;
}

void CentralizedCoordinatorStateMachine::resolvePendingSyncs(
    std::vector<CoordinatorEffect>& effects) {
    for (auto& [group_id, ranks] : reconciliation_ctx_.pending_syncs) {
        for (auto& [rank, pending_requests] : ranks) {
            for (const PendingSync& pending : pending_requests) {
                auto status = hasValidSession(rank, pending.agent_session_id)
                                  ? SyncAfterFailureStatus::Reconciled
                                  : SyncAfterFailureStatus::Rejected;
                auto response = makeSyncResponse(status, group_id);
                if (status == SyncAfterFailureStatus::Rejected) {
                    response.reject_reason = "stale agent session";
                }
                effects.push_back(
                    ReplySync{pending.sync_id, std::move(response)});
            }
        }
    }
    reconciliation_ctx_.pending_syncs.clear();
}

// computeBarrierAckSet -- ranks that must ACK before a proposal/bootstrap
// barrier can commit.  Includes all online ranks active in either old or new
// view.
std::unordered_set<GlobalRank>
CentralizedCoordinatorStateMachine::computeBarrierAckSet(
    const GroupView& old_view, const GroupView& new_view) const {
    std::unordered_set<GlobalRank> acks;
    for (int i = 0; i < max_world_size_; ++i) {
        if (ranks_[i].state == RankState::Offline) continue;
        if (old_view.members[i].isActive() || new_view.members[i].isActive()) {
            acks.insert(i);
        }
    }
    return acks;
}

}  // namespace mooncake
