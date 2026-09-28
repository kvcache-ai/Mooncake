#include "control_plane/coordinator.h"

#include <algorithm>
#include <iterator>
#include <limits>
#include <set>

#include <glog/logging.h>

#include "error_types.h"
#include "pg_utils.h"

namespace mooncake {

CentralizedCoordinatorStateMachine::CentralizedCoordinatorStateMachine(
    int max_world_size, std::chrono::microseconds fault_reconciliation_window)
    : max_world_size_(max_world_size),
      fault_reconciliation_window_(fault_reconciliation_window) {
    PG_ASSERT(max_world_size_ > 0 && max_world_size_ <= kMaxNumRanks,
              "invalid max_world_size: ", max_world_size_);
    ranks_.resize(max_world_size_);
    endpoint_epochs_.assign(max_world_size_, 0);
    for (int r = 0; r < max_world_size_; ++r) {
        ranks_[r].link_status.assign(max_world_size_, 0);
    }
}

void CentralizedCoordinatorStateMachine::setFaultReconciliationWindow(
    std::chrono::microseconds fault_reconciliation_window) {
    fault_reconciliation_window_ = fault_reconciliation_window;
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

    if (shutdown_requested_) {
        result.response.success = false;
        result.response.reject_reason = "coordinator is shutting down";
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

CoordinatorApplyResult<void>
CentralizedCoordinatorStateMachine::requestShutdown() {
    CoordinatorApplyResult<void> result;
    if (shutdown_requested_) return result;

    shutdown_requested_ = true;
    for (GlobalRank rank = 0; rank < max_world_size_; ++rank) {
        const auto& info = ranks_[rank];
        if (info.state != RankState::Offline) {
            shutdown_pending_ranks_.insert(rank);
        }
    }
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

CoordinatorApplyResult<UnregisterAgentResponse>
CentralizedCoordinatorStateMachine::handleUnregisterAgent(
    const UnregisterAgentRequest& req) {
    CoordinatorApplyResult<UnregisterAgentResponse> result;
    if (!rankInRange(req.rank)) {
        result.response.reject_reason = "rank out of valid range";
        return result;
    }

    auto& info = ranks_[req.rank];
    if (info.agent_session_id != req.agent_session_id) {
        result.response.reject_reason = "stale agent_session_id";
        return result;
    }

    // Agent lifetime is process-scoped and independent from every group. This
    // RPC does not change GroupView; group lifecycle and fault handling remain
    // separate operations.
    if (invalidateAgentSession(req.rank)) {
        result.effects.push_back(makeRankStateEffect(req.rank));
        updateRankStates(result.effects);
    }

    result.response.success = true;
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

    if (req.group_bootstrap_id.empty()) {
        result.response.reject_reason = "group bootstrap id is empty";
        return result;
    }

    if (!validateGroupRegistration(req, result.response)) {
        return result;
    }

    bool new_group = false;
    auto group_id = resolveGroupId(req, result.response, new_group);
    if (!group_id.has_value()) return result;

    processGroupRegistration(req, *group_id, result.effects);

    if (new_group) {
        bindGroupBootstrapId(*group_id, req.group_bootstrap_id);
    }
    result.response.success = true;
    result.response.view = group_views_.at(*group_id);
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

CoordinatorApplyResult<UnregisterGroupResponse>
CentralizedCoordinatorStateMachine::handleUnregisterGroup(
    const UnregisterGroupRequest& req) {
    CoordinatorApplyResult<UnregisterGroupResponse> result;
    if (!hasValidSession(req.rank, req.agent_session_id)) {
        result.response.reject_reason =
            "rank is out of range, Offline, or has a stale session";
        return result;
    }

    auto it = group_views_.find(req.group_id);
    if (it == group_views_.end()) {
        result.response.success = true;
        return result;
    }

    auto& view = it->second;
    auto& member = view.members[req.rank];
    if (member.hasLeft()) {
        result.response.success = true;
        return result;
    }
    if (member.isNone()) {
        result.response.reject_reason = "rank is not registered in the group";
        return result;
    }

    member.status = GroupMemberState::Left;
    member.endpoint = std::nullopt;
    view.epoch++;
    rejectPendingProposals(req.group_id, req.rank, "target rank left the group",
                           result.effects);
    rejectPendingSyncs(req.group_id, req.rank, "rank left the group",
                       result.effects);
    dropRankFromPendingBarriers(req.group_id, req.rank, result.effects);

    // Don't push a ViewUpdate when other members remain. The departing
    // rank's unregister races with in-flight collectives on survivors:
    // a ViewUpdate that changes activeRanks mid-collective may corrupt
    // the result.
    if (canEraseGroup(view)) {
        eraseGroup(req.group_id, result.effects);
    }
    result.response.success = true;
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
    pending_proposals_[req.group_id].push_back(PendingProposal{
        propose_id, req,
        std::chrono::steady_clock::now() + kProposalAdmissionTimeout});
    tryAdmitPendingProposals(req.group_id, result.effects);

    return result;
}

// Update link_status from data-plane evidence. Negative transitions open the
// shared reconciliation window; the healthy-set and membership decision is
// deferred until that window closes. Positive-only transitions are applied
// immediately only when no reconciliation is already in progress.
CoordinatorApplyResult<LinkEventReportAck>
CentralizedCoordinatorStateMachine::handleLinkEventReport(
    const LinkEventReport& req) {
    CoordinatorApplyResult<LinkEventReportAck> result;
    if (auto ack = processLinkEventReport(req, result.effects)) {
        result.response = *ack;
    }
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

std::optional<LinkEventReportAck>
CentralizedCoordinatorStateMachine::processLinkEventReport(
    const LinkEventReport& report, std::vector<CoordinatorEffect>& effects) {
    if (!hasValidSession(report.reporter_rank, report.agent_session_id)) {
        return std::nullopt;
    }
    const auto& reporter_info = ranks_[report.reporter_rank];
    if (report.reporter_rank_epoch != reporter_info.rank_epoch) {
        return std::nullopt;
    }

    if (report.events.size() != static_cast<size_t>(max_world_size_) ||
        report.target_rank_epochs.size() !=
            static_cast<size_t>(max_world_size_)) {
        LOG(WARNING) << "[COORD] invalid LinkEventReport vectors";
        return std::nullopt;
    }

    LinkEventReportAck ack{report.reporter_rank, report.reporter_rank_epoch,
                           report.report_id};

    auto& reporter = ranks_[report.reporter_rank];
    if (report.report_id <= reporter.last_link_event_report_id) return ack;
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
    return ack;
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

    std::optional<LinkEventReportAck> link_event_report_ack;

    // Apply piggybacked link event report inline.
    if (req.link_event_report.has_value() &&
        req.link_event_report->reporter_rank == req.reporter_rank &&
        req.link_event_report->agent_session_id == req.agent_session_id) {
        link_event_report_ack =
            processLinkEventReport(*req.link_event_report, result.effects);
    }

    if (reconciliation_ctx_.active) {
        reconciliation_ctx_.pending_syncs[req.group_id][req.reporter_rank]
            .push_back(PendingSync{sync_id, req.agent_session_id,
                                   std::move(link_event_report_ack)});
        return result;
    }

    // The link state report was either already consumed by a completed window,
    // or there is no pending decision. Return the current authoritative view
    // and let AgentHost apply it synchronously before exposing the response.
    auto response =
        makeSyncResponse(SyncAfterFailureStatus::NoPending, req.group_id);
    response.link_event_report_ack = std::move(link_event_report_ack);
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

    auto group_it = pending_barriers_.find(group_id);
    if (group_it == pending_barriers_.end()) return result;

    auto epoch_it = group_it->second.find(epoch);
    if (epoch_it == group_it->second.end()) return result;

    auto& barrier = epoch_it->second;
    barrier.waiting_acks.erase(rank);
    if (barrier.waiting_acks.empty()) {
        auto completed = std::move(barrier);
        group_it->second.erase(epoch_it);
        if (group_it->second.empty()) pending_barriers_.erase(group_it);
        commitBarrier(std::move(completed), result.effects);
    }

    return result;
}

CoordinatorApplyResult<void> CentralizedCoordinatorStateMachine::tick() {
    CoordinatorApplyResult<void> result;
    if (shutdown_confirmed_) return result;

    auto now = std::chrono::steady_clock::now();

    // Heartbeat timeout
    for (int rank = 0; rank < max_world_size_; ++rank) {
        auto& info = ranks_[rank];
        if (info.state == RankState::Offline) continue;
        if (now - info.last_heartbeat > kHeartbeatTimeout) {
            handleTimedOutAgent(rank, "heartbeat timeout", result.effects);
        }
    }

    // Remove expired barriers first, checkGroupTransitions may create new
    // bootstrap barriers and rehash this map.
    std::vector<PendingViewUpdateBarrier> expired_barriers;
    for (auto group_it = pending_barriers_.begin();
         group_it != pending_barriers_.end();) {
        auto& inner = group_it->second;
        for (auto it = inner.begin(); it != inner.end();) {
            auto& barrier = it->second;
            if (!barrier.deadline.has_value() || now <= *barrier.deadline) {
                ++it;
                continue;
            }
            expired_barriers.push_back(std::move(barrier));
            it = inner.erase(it);
        }
        if (inner.empty()) {
            group_it = pending_barriers_.erase(group_it);
        } else {
            ++group_it;
        }
    }

    for (auto& barrier : expired_barriers) {
        std::vector<GlobalRank> timed_out(barrier.waiting_acks.begin(),
                                          barrier.waiting_acks.end());
        barrier.dropped_ranks.insert(timed_out.begin(), timed_out.end());
        barrier.waiting_acks.clear();

        // Only an ACK timeout invalidates the process-level Agent session. A
        // graceful unregister is also reported as dropped by the barrier, but
        // remains group-scoped (not a timed out agent).
        for (GlobalRank rank : timed_out) {
            handleTimedOutAgent(rank, "ViewUpdate barrier timeout",
                                result.effects);
        }
        commitBarrier(std::move(barrier), result.effects);
    }

    tryCloseReconciliationWindow(result.effects);

    // A proposal may have been waiting for link readiness, rank state, or the
    // preceding membership barrier. Iterate over a snapshot because admission
    // removes empty queues.
    std::vector<GroupId> pending_groups;
    pending_groups.reserve(pending_proposals_.size());
    for (const auto& [group_id, _] : pending_proposals_) {
        pending_groups.push_back(group_id);
    }
    for (const auto& group_id : pending_groups) {
        tryAdmitPendingProposals(group_id, result.effects);
    }

    // ShutdownCoordinatorHost must be the final state-machine effect.
    // All deferred RPCs are resolved before the Host is allowed to stop
    // serving requests.
    tryConfirmShutdown(result.effects);

    return result;
}

bool CentralizedCoordinatorStateMachine::invalidateAgentSession(
    GlobalRank rank) {
    if (ranks_[rank].state == RankState::Offline) return false;

    ranks_[rank].state = RankState::Offline;
    ++ranks_[rank].rank_state_version;
    ranks_[rank].link_status.assign(max_world_size_, 0);

    // Clear this rank's connectivity from all peers.
    for (auto& peer : ranks_) {
        if (static_cast<size_t>(rank) < peer.link_status.size())
            peer.link_status[rank] = 0;
    }

    if (shutdown_requested_) shutdown_pending_ranks_.erase(rank);

    return true;
}

void CentralizedCoordinatorStateMachine::handleTimedOutAgent(
    GlobalRank rank, const char* reason,
    std::vector<CoordinatorEffect>& effects) {
    const auto previous_state = ranks_[rank].state;
    if (!invalidateAgentSession(rank)) return;

    LOG(INFO) << "[COORD] handleTimedOutAgent rank=" << rank
              << " state=" << static_cast<int>(previous_state)
              << " reason=" << reason;

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

void CentralizedCoordinatorStateMachine::tryConfirmShutdown(
    std::vector<CoordinatorEffect>& effects) {
    if (!shutdown_requested_ || shutdown_confirmed_ ||
        !shutdown_pending_ranks_.empty()) {
        return;
    }

    constexpr auto reason = "coordinator shutting down";

    // A shutdown confirmation is terminal. Resolve every deferred response
    // in the state machine first so ShutdownCoordinatorHost is the final
    // effect ever emitted.
    for (auto& group_barriers : pending_barriers_) {
        for (auto& epoch_barrier : group_barriers.second) {
            auto& barrier = epoch_barrier.second;
            auto* commit =
                std::get_if<PendingViewUpdateBarrier::ProposalCommit>(
                    &barrier.commit);
            if (commit == nullptr) continue;
            effects.push_back(ReplyProposal{
                commit->propose_id,
                {ProposalStatus::Rejected, barrier.epoch, {}, reason}});
        }
    }
    pending_barriers_.clear();

    for (const auto& [group_id, proposals] : pending_proposals_) {
        const auto view_it = group_views_.find(group_id);
        const auto epoch =
            view_it == group_views_.end() ? 0 : view_it->second.epoch;
        for (const auto& proposal : proposals) {
            effects.push_back(
                ReplyProposal{proposal.propose_id,
                              {ProposalStatus::Rejected, epoch, {}, reason}});
        }
    }
    pending_proposals_.clear();

    for (const auto& [group_id, ranks] : reconciliation_ctx_.pending_syncs) {
        for (const auto& rank_syncs : ranks) {
            const auto& pending_syncs = rank_syncs.second;
            for (const auto& pending : pending_syncs) {
                auto response = makeSyncResponse(
                    SyncAfterFailureStatus::Rejected, group_id);
                response.link_event_report_ack = pending.link_event_report_ack;
                response.reject_reason = reason;
                effects.push_back(
                    ReplySync{pending.sync_id, std::move(response)});
            }
        }
    }
    reconciliation_ctx_.pending_syncs.clear();
    reconciliation_ctx_.active = false;

    shutdown_confirmed_ = true;
    effects.push_back(ShutdownCoordinatorHost{});
}

bool CentralizedCoordinatorStateMachine::isMutuallyConnected(
    GlobalRank a, GlobalRank b) const {
    PG_ASSERT(rankInRange(a) && rankInRange(b),
              "isMutuallyConnected called with an out-of-range rank");
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
        if (ranks_[i].state == RankState::Healthy &&
            isMutuallyConnected(i, i)) {
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
        // The diagonal is local data-plane readiness. A registered Agent
        // remains Synced until its LinkManager reports the self-link up.
        if (!isMutuallyConnected(i, i)) continue;
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

void CentralizedCoordinatorStateMachine::tryAdmitPendingProposals(
    GroupId group_id, std::vector<CoordinatorEffect>& effects) {
    auto queue_it = pending_proposals_.find(group_id);
    if (queue_it == pending_proposals_.end()) return;

    auto view_it = group_views_.find(group_id);
    if (view_it == group_views_.end()) {
        for (const auto& pending : queue_it->second) {
            effects.push_back(ReplyProposal{
                pending.propose_id,
                {ProposalStatus::Rejected, 0, {}, "group not found"}});
        }
        pending_proposals_.erase(queue_it);
        return;
    }

    GroupView& view = view_it->second;
    auto& queue = queue_it->second;
    const auto now = std::chrono::steady_clock::now();
    auto reply_and_pop = [&](ProposalStatus status, const char* reason) {
        effects.push_back(ReplyProposal{queue.front().propose_id,
                                        {status, view.epoch, {}, reason}});
        queue.pop_front();
    };
    while (!queue.empty()) {
        const auto& pending = queue.front();
        const auto& req = pending.request;

        if (!hasValidSession(req.source_rank, req.agent_session_id)) {
            reply_and_pop(ProposalStatus::Rejected,
                          "source rank is Offline or has a stale session");
            continue;
        }
        if (view.status != GroupStatus::Ready) {
            reply_and_pop(ProposalStatus::Rejected, "group is not ready");
            continue;
        }

        bool has_invalid_target = false;
        bool needs_membership_change = false;

        std::vector<GlobalRank> requested_global_ranks;
        requested_global_ranks.reserve(req.requested_ranks.size());
        for (InGroupRank rank : req.requested_ranks) {
            if (rank < 0 ||
                static_cast<size_t>(rank) >= view.rank_order.size()) {
                has_invalid_target = true;
                break;
            }
            requested_global_ranks.push_back(view.rank_order[rank]);
        }

        if (has_invalid_target) {
            reply_and_pop(ProposalStatus::Rejected,
                          "target in-group rank is out of valid range");
            continue;
        }

        for (GlobalRank rank : requested_global_ranks) {
            switch (view.members[rank].status) {
                case GroupMemberState::None:
                case GroupMemberState::Left:
                    has_invalid_target = true;
                    break;
                case GroupMemberState::Inactive:
                case GroupMemberState::AwaitingActivation:
                    // Inactive is a valid activation target because joinGroup
                    // may advance it to AwaitingActivation while the proposal
                    // is queued. isActivatableSet still requires that
                    // transition before admitting the activation.
                    if (req.is_activation) needs_membership_change = true;
                    break;
                case GroupMemberState::Active:
                    if (!req.is_activation) needs_membership_change = true;
                    break;
            }
        }
        if (has_invalid_target) {
            reply_and_pop(ProposalStatus::Rejected,
                          "target rank is not valid group member");
            continue;
        }

        // A proposal updates group_views_ before its ViewUpdate barrier
        // commits. While that barrier is pending, group_views_ contains the
        // target membership, not a fully acknowledged membership.
        auto barrier_it = pending_barriers_.find(group_id);
        if (barrier_it != pending_barriers_.end() &&
            !barrier_it->second.empty()) {
            return;
        }

        if (!needs_membership_change) {
            reply_and_pop(ProposalStatus::Applied, "");
            continue;
        }
        if (now > pending.deadline) {
            reply_and_pop(ProposalStatus::Rejected,
                          "proposal admission timed out");
            continue;
        }

        if (req.is_activation &&
            !isActivatableSet(group_id, requested_global_ranks, view)) {
            // Keep the FIFO head pending until it times out.
            return;
        }

        GroupView old_view = view;
        if (req.is_activation) {
            for (GlobalRank rank : requested_global_ranks) {
                view.members[rank].status = GroupMemberState::Active;
            }
        } else {
            for (GlobalRank rank : requested_global_ranks) {
                if (!view.members[rank].isActive()) continue;
                view.members[rank].status = GroupMemberState::Inactive;
            }
        }

        const auto propose_id = pending.propose_id;
        queue.pop_front();
        view.epoch++;

        auto required_acks = computeBarrierAckSet(old_view, view);
        pending_barriers_[group_id][view.epoch] = PendingViewUpdateBarrier{
            group_id,
            view.epoch,
            std::move(required_acks),
            {},
            now + kViewUpdateAckTimeout,
            PendingViewUpdateBarrier::ProposalCommit{propose_id}};
        effects.push_back(PushViewUpdate{view});

        if (queue.empty()) pending_proposals_.erase(queue_it);
        return;
    }

    pending_proposals_.erase(queue_it);
}

void CentralizedCoordinatorStateMachine::rejectPendingProposals(
    GroupId group_id, GlobalRank rank, const std::string& reason,
    std::vector<CoordinatorEffect>& effects) {
    auto queue_it = pending_proposals_.find(group_id);
    if (queue_it == pending_proposals_.end()) return;

    const auto view_it = group_views_.find(group_id);
    if (view_it == group_views_.end()) return;
    const auto& rank_order = view_it->second.rank_order;
    const auto epoch = view_it->second.epoch;

    auto& queue = queue_it->second;
    for (auto it = queue.begin(); it != queue.end();) {
        const bool targets_rank = std::any_of(
            it->request.requested_ranks.begin(),
            it->request.requested_ranks.end(), [&](InGroupRank in_group_rank) {
                return in_group_rank >= 0 &&
                       static_cast<size_t>(in_group_rank) < rank_order.size() &&
                       rank_order[in_group_rank] == rank;
            });
        if (!targets_rank) {
            ++it;
            continue;
        }
        effects.push_back(ReplyProposal{
            it->propose_id, {ProposalStatus::Rejected, epoch, {}, reason}});
        it = queue.erase(it);
    }

    if (queue.empty()) pending_proposals_.erase(queue_it);
}

void CentralizedCoordinatorStateMachine::dropRankFromPendingBarriers(
    GroupId group_id, GlobalRank rank,
    std::vector<CoordinatorEffect>& effects) {
    auto group_it = pending_barriers_.find(group_id);
    if (group_it == pending_barriers_.end()) return;

    // Committing a barrier immediately retries the next queued proposal and
    // may insert a new barrier for this group. So first collect completed
    // barriers.
    std::vector<PendingViewUpdateBarrier> completed_barriers;
    auto& barriers = group_it->second;
    for (auto it = barriers.begin(); it != barriers.end();) {
        auto& barrier = it->second;
        if (barrier.waiting_acks.erase(rank) == 0) {
            ++it;
            continue;
        }

        barrier.dropped_ranks.insert(rank);
        if (!barrier.waiting_acks.empty()) {
            ++it;
            continue;
        }

        completed_barriers.push_back(std::move(barrier));
        it = barriers.erase(it);
    }

    if (barriers.empty()) pending_barriers_.erase(group_it);

    for (auto& barrier : completed_barriers) {
        commitBarrier(std::move(barrier), effects);
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
                        group_id,
                        view.epoch,
                        std::move(required_acks),
                        {},
                        std::nullopt,
                        PendingViewUpdateBarrier::BootstrapCommit{}};

                effects.push_back(PushViewUpdate{view});
            }
        }
        // BootstrapSyncing -> Ready is done in commitBarrier when all required
        // ACKs arrive
    }
}

bool CentralizedCoordinatorStateMachine::validateGroupRegistration(
    const RegisterGroupRequest& request,
    RegisterGroupResponse& response) const {
    if (request.max_group_size <= 0 ||
        request.max_group_size > max_world_size_) {
        response.success = false;
        response.reject_reason = "max_group_size is out of valid range";
        return false;
    }

    if (request.rank_order.size() >
        static_cast<size_t>(request.max_group_size)) {
        response.success = false;
        response.reject_reason = "rank_order exceeds max_group_size";
        return false;
    }

    // Validate joining_rank
    if (!rankInRange(request.rank)) {
        response.success = false;
        response.reject_reason = "joining rank is out of valid range";
        return false;
    }

    // Validate rank_order elements.
    for (GlobalRank r : request.rank_order) {
        if (!rankInRange(r)) {
            response.success = false;
            response.reject_reason = "rank_order contains invalid GlobalRank";
            return false;
        }
    }

    // Validate no duplicates in rank_order.
    {
        std::set<GlobalRank> seen(request.rank_order.begin(),
                                  request.rank_order.end());
        if (seen.size() != request.rank_order.size()) {
            response.success = false;
            response.reject_reason = "rank_order contains duplicate ranks";
            return false;
        }
    }

    // The joining rank must be one of the ranks it declares in rank_order.
    if (std::find(request.rank_order.begin(), request.rank_order.end(),
                  request.rank) == request.rank_order.end()) {
        response.success = false;
        response.reject_reason = "joining rank not in rank_order";
        return false;
    }

    return true;
}

static bool isRankOrderPrefix(const std::vector<GlobalRank>& prefix,
                              const std::vector<GlobalRank>& order) {
    return prefix.size() <= order.size() &&
           std::equal(prefix.begin(), prefix.end(), order.begin());
}

std::optional<GroupId> CentralizedCoordinatorStateMachine::resolveGroupId(
    const RegisterGroupRequest& request, RegisterGroupResponse& response,
    bool& new_group) {
    // GroupBootstrapId identifies a PyTorch group_id, not necessarily one
    // runtime group. resolve_policy supplies the choice that cannot be inferred
    // from rank-order relationships alone. Within one bootstrap-id bucket:
    //
    //  * CreateOrAttach never modifies an existing rank order. Without an exact
    //    match, it creates a new runtime group even if rank orders overlap.
    //  * AttachOrExtend never creates a runtime group. Without an exact match,
    //    it must find one unique existing order that is a proper prefix of the
    //    request, has matching capacity, and appends the joining rank.
    //
    // More than one exact match is ambiguous. If there is no exact match, more
    // than one append-compatible match is ambiguous. Callers that need to
    // distinguish them must eventually provide distinct stable
    // GroupBootstrapIds. processGroupRegistration() then applies the resolved
    // registration.
    static constexpr auto GROUP_ID_PREFIX = "mooncake_pg_";
    auto bucket = group_ids_by_bootstrap_id_.find(request.group_bootstrap_id);

    // Exact matching (Attach) is shared by both policies and always takes
    // precedence.
    std::vector<GroupId> exact_groups;
    if (bucket != group_ids_by_bootstrap_id_.end()) {
        for (const auto& group_id : bucket->second) {
            const auto& view = group_views_.at(group_id);
            if (view.max_group_size == request.max_group_size &&
                view.rank_order == request.rank_order) {
                exact_groups.push_back(group_id);
            }
        }
    }

    if (exact_groups.size() > 1) {
        response.reject_reason = "ambiguous exact group matches";
        return std::nullopt;
    }

    if (exact_groups.size() == 1) {
        new_group = false;
        return exact_groups.front();
    }

    switch (request.resolve_policy) {
        case GroupBootstrapIdResolvePolicy::CreateOrAttach:
            new_group = true;
            return GROUP_ID_PREFIX + std::to_string(next_group_id_++);

        case GroupBootstrapIdResolvePolicy::AttachOrExtend: {
            if (bucket == group_ids_by_bootstrap_id_.end()) {
                response.reject_reason = "extension target not found";
                return std::nullopt;
            }

            std::vector<GroupId> extension_groups;
            for (const auto& group_id : bucket->second) {
                const auto& view = group_views_.at(group_id);
                const auto& existing_order = view.rank_order;

                if (view.max_group_size != request.max_group_size) {
                    continue;
                }
                if (existing_order.size() >= request.rank_order.size()) {
                    continue;
                }
                if (!isRankOrderPrefix(existing_order, request.rank_order)) {
                    continue;
                }

                auto appended_begin =
                    request.rank_order.begin() + existing_order.size();
                if (std::find(appended_begin, request.rank_order.end(),
                              request.rank) == request.rank_order.end()) {
                    continue;
                }

                extension_groups.push_back(group_id);
            }

            if (extension_groups.size() > 1) {
                response.reject_reason = "ambiguous extension target";
                return std::nullopt;
            }

            if (extension_groups.empty()) {
                response.reject_reason =
                    "no append-compatible extension target";
                return std::nullopt;
            }

            new_group = false;
            return extension_groups.front();
        }

        default:
            response.reject_reason =
                "invalid group bootstrap id resolve policy";
            return std::nullopt;
    }
}

void CentralizedCoordinatorStateMachine::bindGroupBootstrapId(
    GroupId group_id, GroupBootstrapId group_bootstrap_id) {
    group_ids_by_bootstrap_id_[group_bootstrap_id].push_back(group_id);
    group_bootstrap_ids_.emplace(std::move(group_id),
                                 std::move(group_bootstrap_id));
}

void CentralizedCoordinatorStateMachine::processGroupRegistration(
    const RegisterGroupRequest& request, const GroupId& group_id,
    std::vector<CoordinatorEffect>& effects) {
    auto it = group_views_.find(group_id);
    if (it == group_views_.end()) {
        // First declaration -> create group.
        // Founding members are all entries in rank_order.
        GroupView view;
        view.group_id = group_id;
        view.max_group_size = request.max_group_size;
        view.rank_order = request.rank_order;
        view.members.resize(max_world_size_);
        for (GlobalRank r : request.rank_order) {
            view.members[r].status = GroupMemberState::Active;
        }
        view.status = GroupStatus::Bootstrapping;
        group_views_[group_id] = std::move(view);
        group_views_[group_id].auto_deactivate = request.auto_deactivate;
        return;
    }

    auto& view = it->second;

    // If the request rank order is longer, the extra ranks are
    // not activated here.  They must be activated via a subsequent
    // proposeViewUpdate (activate_rank / recover_ranks) from an existing
    // active member.
    //
    // However, extend the existing rank_order with the new ranks now so that
    // every member's ViewUpdate carries the correct rank_order (local->global
    // mapping).
    bool view_changed = false;
    if (request.rank_order.size() > view.rank_order.size()) {
        view.rank_order = request.rank_order;
        view_changed = true;
    }

    auto& joining_member = view.members[request.rank];
    if (joining_member.status == GroupMemberState::None ||
        joining_member.status == GroupMemberState::Left) {
        joining_member.status = GroupMemberState::Inactive;
        joining_member.endpoint = std::nullopt;
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
                effects.push_back(ReplyProposal{
                    pc->propose_id,
                    {ProposalStatus::Rejected, 0, {}, "group was destroyed"}});
            }
            // Bootstrap barriers need no reply.
        }
        pending_barriers_.erase(it);
    }

    auto proposal_it = pending_proposals_.find(group_id);
    if (proposal_it != pending_proposals_.end()) {
        for (const auto& pending : proposal_it->second) {
            effects.push_back(ReplyProposal{
                pending.propose_id,
                {ProposalStatus::Rejected, 0, {}, "group was destroyed"}});
        }
        pending_proposals_.erase(proposal_it);
    }

    auto sync_group_it = reconciliation_ctx_.pending_syncs.find(group_id);
    if (sync_group_it != reconciliation_ctx_.pending_syncs.end()) {
        for (const auto& rank_syncs : sync_group_it->second) {
            for (const auto& pending : rank_syncs.second) {
                auto response = makeSyncResponse(
                    SyncAfterFailureStatus::Rejected, group_id);
                response.link_event_report_ack = pending.link_event_report_ack;
                response.reject_reason = "group was destroyed";
                effects.push_back(
                    ReplySync{pending.sync_id, std::move(response)});
            }
        }
        reconciliation_ctx_.pending_syncs.erase(sync_group_it);
    }

    auto group_bootstrap_id = group_bootstrap_ids_.at(group_id);
    auto& group_ids = group_ids_by_bootstrap_id_.at(group_bootstrap_id);
    group_ids.erase(std::remove(group_ids.begin(), group_ids.end(), group_id),
                    group_ids.end());
    if (group_ids.empty()) {
        group_ids_by_bootstrap_id_.erase(group_bootstrap_id);
    }
    group_bootstrap_ids_.erase(group_id);
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
    PendingViewUpdateBarrier barrier, std::vector<CoordinatorEffect>& effects) {
    std::visit(
        overloaded{
            [&](const PendingViewUpdateBarrier::ProposalCommit& commit) {
                ProposeViewUpdateResponse response{
                    ProposalStatus::Applied, barrier.epoch, {}, ""};
                if (!barrier.dropped_ranks.empty()) {
                    response.status = ProposalStatus::AppliedWithDroppedRanks;
                    const auto group_it = group_views_.find(barrier.group_id);
                    if (group_it == group_views_.end()) {
                        LOG(ERROR) << "[COORD] cannot map dropped ranks for "
                                   << "missing group " << barrier.group_id;
                        response.status = ProposalStatus::Rejected;
                        response.reject_reason =
                            "group disappeared while committing ViewUpdate";
                    } else {
                        const auto& rank_order = group_it->second.rank_order;
                        response.dropped_ranks.reserve(
                            barrier.dropped_ranks.size());
                        for (GlobalRank rank : barrier.dropped_ranks) {
                            const auto rank_it = std::find(
                                rank_order.begin(), rank_order.end(), rank);
                            if (rank_it == rank_order.end()) {
                                LOG(ERROR)
                                    << "[COORD] dropped GlobalRank " << rank
                                    << " is not in group " << barrier.group_id;
                                response.status = ProposalStatus::Rejected;
                                response.dropped_ranks.clear();
                                response.reject_reason =
                                    "dropped rank is not in the group";
                                break;
                            }
                            response.dropped_ranks.push_back(
                                static_cast<InGroupRank>(std::distance(
                                    rank_order.begin(), rank_it)));
                        }
                        std::sort(response.dropped_ranks.begin(),
                                  response.dropped_ranks.end());
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

    // Barrier completion releases this group's proposal admission lane.
    // Retry immediately on every completion path (ACK, timeout, or graceful
    // unregister) instead of waiting for the next coordinator tick.
    tryAdmitPendingProposals(barrier.group_id, effects);
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
        resp.link_event_report_ack = pending.link_event_report_ack;
        resp.reject_reason = reason;
        effects.push_back(ReplySync{pending.sync_id, std::move(resp)});
    }
    group_it->second.erase(rank_it);
    if (group_it->second.empty()) {
        pending_syncs.erase(group_it);
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
                response.link_event_report_ack = pending.link_event_report_ack;
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
