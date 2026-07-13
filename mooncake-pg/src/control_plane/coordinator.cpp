#include "control_plane/coordinator.h"

#include <algorithm>
#include <limits>
#include <numeric>
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

    // Identity check
    // If the rank is currently Healthy AND the request comes from a different
    // process -> reject.  A failed / auto-deactivated rank (Synced or Offline)
    // may be replaced immediately; waiting for heartbeat timeout is not
    // required because the Coordinator has already removed it from the healthy
    // set.
    bool same_peer = (info.agent_addr == req.agent_addr &&
                      info.te_server_name == req.te_server_name);

    if (info.state == RankState::Healthy && !same_peer) {
        result.response.success = false;
        result.response.reject_reason =
            "rank already registered and is Healthy; replacement must wait "
            "for the old process to leave the healthy set.";
        return result;
    }

    // A new session makes old endpoints invalid
    bool session_changed =
        (info.state != RankState::Offline &&
         info.agent_session_epoch != req.agent_session_epoch);

    info.agent_addr = req.agent_addr;
    info.te_server_name = req.te_server_name;
    info.agent_session_epoch = req.agent_session_epoch;
    info.warmup_recv_addr = req.warmup_recv_addr;
    info.last_heartbeat = std::chrono::steady_clock::now();
    info.link_status.assign(max_world_size_, 0);
    info.link_status[req.rank] = 1;

    if (session_changed) {
        for (auto& [group_id, view] : group_views_) {
            auto& member = view.members[req.rank];
            if (member.agent_session_epoch.has_value() &&
                *member.agent_session_epoch != req.agent_session_epoch) {
                member.agent_session_epoch = std::nullopt;
                member.endpoint = std::nullopt;
            }
        }
    }

    // Broadcast PeerJoinedPush to all online ranks.
    result.effects.push_back(
        PeerJoinedPush{req.rank, info.te_server_name, info.warmup_recv_addr});

    // Transition to Synced.
    info.state = RankState::Synced;
    result.effects.push_back(makeRankStateEffect(req.rank));

    // Build response.
    auto& resp = result.response;
    resp.success = true;
    resp.all_rank_states.resize(max_world_size_);
    for (int32_t i = 0; i < max_world_size_; ++i) {
        resp.all_rank_states[i] = ranks_[i].state;
    }
    for (const auto& [gid, view] : group_views_) {
        resp.groups.push_back(view);
    }
    for (int32_t i = 0; i < max_world_size_; ++i) {
        if (i == req.rank) continue;
        if (ranks_[i].state == RankState::Offline) continue;
        RankConnectionMetadata conn;
        conn.rank = i;
        conn.agent_addr = ranks_[i].agent_addr;
        conn.te_server_name = ranks_[i].te_server_name;
        conn.warmup_recv_addr = ranks_[i].warmup_recv_addr;
        resp.rank_connections.push_back(conn);
    }

    return result;
}

CoordinatorApplyResult<HeartbeatResponse>
CentralizedCoordinatorStateMachine::handleHeartbeat(
    const HeartbeatRequest& req) {
    CoordinatorApplyResult<HeartbeatResponse> result;
    if (!hasValidSession(req.rank, req.agent_session_epoch)) {
        result.response.acknowledge = false;
        result.response.require_reregister = true;
        return result;
    }
    auto& info = ranks_[req.rank];
    info.last_heartbeat = std::chrono::steady_clock::now();

    result.response.acknowledge = true;
    result.response.require_reregister = false;
    return result;
}

CoordinatorApplyResult<RegisterGroupResponse>
CentralizedCoordinatorStateMachine::handleRegisterGroup(
    const RegisterGroupRequest& req) {
    CoordinatorApplyResult<RegisterGroupResponse> result;
    if (!hasValidSession(req.rank, req.agent_session_epoch)) {
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

CoordinatorApplyResult<void>
CentralizedCoordinatorStateMachine::handleUnregisterGroup(
    const UnregisterGroupRequest& req) {
    CoordinatorApplyResult<void> result;
    if (!hasValidSession(req.rank, req.agent_session_epoch)) {
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

    member.status = GroupMemberStatus::Left;
    member.agent_session_epoch = std::nullopt;
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
    if (!hasValidSession(req.rank, req.agent_session_epoch)) {
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
        member.agent_session_epoch = req.agent_session_epoch;
        member.endpoint = ep.endpoint_info;
        member.endpoint->endpoint_epoch = ++endpoint_epochs_[req.rank];

        if (member.isMember() && view.status == GroupStatus::Ready) {
            view.epoch++;
            result.effects.push_back(ViewUpdateEffect{view});
        }
    }

    result.response.success = true;
    checkGroupTransitions(result.effects);
    return result;
}

// transferObservation  - update link_status from data-plane evidence
//
// The link_status is updated immediately. However, the coordinator does
// NOT recompute the authoritative healthy set here.  Instead it opens a
//  fault_reconciliation_window; the membership decision is deferred
// until the window closes.  This gives multiple survivors time to report the
// same failure and prevents a single fast reporter from causing a premature
// auto-deactivation that hides the failure from slower survivors.
CoordinatorApplyResult<void>
CentralizedCoordinatorStateMachine::handleProposeViewUpdate(
    uint64_t propose_id, const ProposeViewUpdateRequest& req) {
    CoordinatorApplyResult<void> result;
    auto it = group_views_.find(req.group_id);
    if (it == group_views_.end()) {
        result.effects.push_back(ReplyProposalEffect{
            propose_id,
            {ViewUpdateStatus::Rejected, 0, {}, "group not found"}});
        return result;
    }

    GroupView& view = it->second;
    GroupView old_view = view;
    bool changed = false;

    // Reject stale or offline proposer.
    if (!hasValidSession(req.source_rank, req.agent_session_epoch)) {
        result.effects.push_back(ReplyProposalEffect{
            propose_id,
            {ViewUpdateStatus::Rejected,
             view.epoch,
             {},
             "source rank is Offline or stale session epoch"}});
        return result;
    }

    for (GlobalRank rank : req.requested_ranks) {
        if (!rankInRange(rank)) {
            result.effects.push_back(
                ReplyProposalEffect{propose_id,
                                    {ViewUpdateStatus::Rejected,
                                     view.epoch,
                                     {},
                                     "target rank is out of valid range"}});
            return result;
        }
    }

    if (req.is_activation) {
        // Check activatable before applying changes.
        // rank_order is managed exclusively in processGroupRegistration.
        for (GlobalRank rank : req.requested_ranks) {
            if (!view.members[rank].isActive()) changed = true;
        }

        if (changed &&
            !isActivatableSet(req.group_id, req.requested_ranks, view)) {
            result.effects.push_back(
                ReplyProposalEffect{propose_id,
                                    {ViewUpdateStatus::Rejected,
                                     view.epoch,
                                     {},
                                     "new active set is not activatable"}});
            return result;
        }

        // Apply.
        for (GlobalRank rank : req.requested_ranks) {
            view.members[rank].status = GroupMemberStatus::Active;
        }
    } else {
        // deactivate
        for (GlobalRank rank : req.requested_ranks) {
            if (view.members[rank].isActive()) {
                view.members[rank].status = GroupMemberStatus::Inactive;
                view.members[rank].agent_session_epoch = std::nullopt;
                view.members[rank].endpoint = std::nullopt;
                changed = true;
            }
        }
    }

    if (!changed) {
        result.effects.push_back(ReplyProposalEffect{
            propose_id, {ViewUpdateStatus::Applied, view.epoch, {}, ""}});
        return result;
    }

    view.epoch++;

    auto required_acks = computeBarrierAckSet(old_view, view, req.group_id);

    // required_acks always includes the proposer (online + active in old
    // view), so the set is never empty in normal operation.
    pending_barriers_[req.group_id][view.epoch] = PendingViewUpdateBarrier{
        req.group_id, view.epoch,
        std::unordered_set<GlobalRank>(required_acks.begin(),
                                       required_acks.end()),
        std::chrono::steady_clock::now() + kProposeTimeout,
        PendingViewUpdateBarrier::ProposalCommit{
            propose_id, {ViewUpdateStatus::Applied, view.epoch, {}, ""}}};
    result.effects.push_back(ViewUpdateEffect{view});

    return result;
}

CoordinatorApplyResult<void>
CentralizedCoordinatorStateMachine::handleTransferObservation(
    const TransferObservationReport& req) {
    CoordinatorApplyResult<void> result;
    if (!hasValidSession(req.reporter_rank, req.agent_session_epoch)) {
        return result;
    }
    auto& reporter = ranks_[req.reporter_rank];

    bool has_negative = applyLinkStatusUpdate(reporter, req.attempted_ranks,
                                              req.failed_ranks_hint);

    if (has_negative) {
        LOG(INFO) << "[COORD] TransferObservation has negative → opening "
                     "reconciliation window";
        tryOpenReconciliationWindow();
    }
    return result;
}

void CentralizedCoordinatorStateMachine::tryOpenReconciliationWindow() {
    if (!reconciliation_ctx_.active) {
        reconciliation_ctx_.active = true;
        reconciliation_ctx_.deadline =
            std::chrono::steady_clock::now() + fault_reconciliation_window_;
    }
}

bool CentralizedCoordinatorStateMachine::applyLinkStatusUpdate(
    RankInfo& reporter, const std::vector<uint8_t>& attempted,
    const std::vector<uint8_t>& failed) {
    bool has_negative = false;
    for (int32_t peer = 0; peer < max_world_size_; ++peer) {
        if (peer >= static_cast<int32_t>(attempted.size()) || !attempted[peer])
            continue;
        // succeeded = attempted && !failed
        if (peer < static_cast<int32_t>(failed.size()) && failed[peer]) {
            reporter.link_status[peer] = 0;
            has_negative = true;
        } else {
            reporter.link_status[peer] = 1;
        }
    }
    return has_negative;
}

CoordinatorApplyResult<void>
CentralizedCoordinatorStateMachine::handleLinkStateChange(
    const LinkStateChangeReport& req) {
    CoordinatorApplyResult<void> result;
    if (!hasValidSession(req.reporter_rank, req.agent_session_epoch))
        return result;
    if (!rankInRange(req.peer)) return result;
    auto& reporter = ranks_[req.reporter_rank];

    reporter.link_status[req.peer] = req.is_up ? 1 : 0;
    reporter.link_status[req.reporter_rank] = 1;  // self is always connected

    updateRankStates(result.effects);
    applyAutoDeactivate(result.effects);
    checkGroupTransitions(result.effects);
    return result;
}

// handleSyncAfterFailure - sync-after-failure RPC handler.
CoordinatorApplyResult<void>
CentralizedCoordinatorStateMachine::handleSyncAfterFailure(
    uint64_t sync_id, const SyncAfterFailureRequest& req) {
    CoordinatorApplyResult<void> result;

    if (!hasValidSession(req.reporter_rank, req.agent_session_epoch)) {
        result.effects.push_back(
            ReplySyncEffect{sync_id,
                            {SyncAfterFailureStatus::Rejected, 0,
                             "rank out of range or stale session"}});
        return result;
    }
    auto& reporter = ranks_[req.reporter_rank];

    auto view_it = group_views_.find(req.group_id);
    if (view_it == group_views_.end()) {
        result.effects.push_back(ReplySyncEffect{
            sync_id, {SyncAfterFailureStatus::Rejected, 0, "group not found"}});
        return result;
    }

    // sync_after_failure is only meaningful for auto_deactivate groups.
    if (!view_it->second.auto_deactivate) {
        result.effects.push_back(ReplySyncEffect{
            sync_id,
            {SyncAfterFailureStatus::Rejected, view_it->second.epoch,
             "group has auto_deactivate=false"}});
        return result;
    }

    // Apply piggybacked observation inline.
    if (req.observation.has_value()) {
        bool has_negative =
            applyLinkStatusUpdate(reporter, req.observation->attempted_ranks,
                                  req.observation->failed_ranks_hint);

        if (has_negative) {
            tryOpenReconciliationWindow();
        }
    }

    uint64_t current_epoch = view_it->second.epoch;
    if (req.current_epoch < current_epoch) {
        // Caller is behind the coordinator.  Defer the reply until the caller
        // ACKs the current ViewUpdate, so get_peer_state() reflects the
        // decision before sync_after_failure() returns.
        pending_syncs_[req.group_id][req.reporter_rank].push_back(sync_id);
        result.effects.push_back(ViewUpdateEffect{view_it->second});
        return result;
    }

    if (reconciliation_ctx_.active) {
        pending_syncs_[req.group_id][req.reporter_rank].push_back(sync_id);
        return result;
    }

    // No window, epoch matches -> no pending decision.
    result.effects.push_back(ReplySyncEffect{
        sync_id, {SyncAfterFailureStatus::NoChange, current_epoch, ""}});
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

    // 1. Resolve sync-after-failure callers waiting on this ACK.
    {
        auto it = pending_syncs_.find(group_id);
        if (it != pending_syncs_.end()) {
            auto rank_it = it->second.find(rank);
            if (rank_it != it->second.end()) {
                SyncAfterFailureResponse resp;
                resp.status = SyncAfterFailureStatus::DecisionApplied;
                resp.new_epoch = view_it->second.epoch;
                for (uint64_t sync_id : rank_it->second) {
                    result.effects.push_back(ReplySyncEffect{sync_id, resp});
                }
                it->second.erase(rank_it);
                if (it->second.empty()) {
                    pending_syncs_.erase(it);
                }
            }
        }
    }

    // 2. Decrement any pending ViewUpdate barrier matching this epoch.
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
            transitionToOffline(rank, result.effects);
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

    // Fault reconciliation window.
    if (reconciliation_ctx_.active && now >= reconciliation_ctx_.deadline) {
        LOG(INFO) << "[COORD] Reconciliation window expired, triggering "
                     "auto_deactivate";
        reconciliation_ctx_.active = false;
        updateRankStates(result.effects);
        applyAutoDeactivate(result.effects);
        checkGroupTransitions(result.effects);
    }

    return result;
}

void CentralizedCoordinatorStateMachine::transitionToOffline(
    GlobalRank rank, std::vector<CoordinatorEffect>& effects) {
    LOG(INFO) << "[COORD] transitionToOffline rank=" << rank
              << " state=" << static_cast<int>(ranks_[rank].state);
    ranks_[rank].state = RankState::Offline;
    ranks_[rank].link_status.assign(max_world_size_, 0);

    // Clear this rank's connectivity from all peers.
    for (auto& peer : ranks_) {
        if (static_cast<size_t>(rank) < peer.link_status.size())
            peer.link_status[rank] = 0;
    }

    // For auto_deactivate=true groups, mark the failed rank as inactive.
    for (auto& [group_id, view] : group_views_) {
        if (!view.auto_deactivate) continue;
        auto& member = view.members[rank];
        if (member.status != GroupMemberStatus::Active) continue;

        member.status = GroupMemberStatus::Inactive;
        member.agent_session_epoch = std::nullopt;
        member.endpoint = std::nullopt;
        view.epoch++;
        rejectPendingSyncs(group_id, rank, "rank went offline", effects);
        effects.push_back(ViewUpdateEffect{view});
    }

    effects.push_back(makeRankStateEffect(rank));
    updateRankStates(effects);
    applyAutoDeactivate(effects);
    checkGroupTransitions(effects);
}

bool CentralizedCoordinatorStateMachine::isMutuallyConnected(
    GlobalRank a, GlobalRank b) const {
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
            effects.push_back(makeRankStateEffect(i));
        } else if (!in_healthy && ranks_[i].state == RankState::Healthy) {
            ranks_[i].state = RankState::Synced;
            effects.push_back(makeRankStateEffect(i));
        }
    }
}

void CentralizedCoordinatorStateMachine::applyAutoDeactivate(
    std::vector<CoordinatorEffect>& effects) {
    auto healthy_set = extendHealthySet();

    // For auto_deactivate groups, remove unhealthy ranks from the active set.
    // However, during bootstrap / BootstrapSyncing we do NOT do this: we wait
    // for full mutual connectivity and let waitUntilGroupReady() time out if a
    // peer is truly dead.
    for (auto& [group_id, view] : group_views_) {
        if (!view.auto_deactivate) continue;
        if (view.status != GroupStatus::Ready) continue;
        std::vector<GlobalRank> deactivated_ranks;
        for (int i = 0; i < max_world_size_; ++i) {
            if (!view.members[i].isActive()) continue;
            bool in_healthy = std::find(healthy_set.begin(), healthy_set.end(),
                                        i) != healthy_set.end();
            if (!in_healthy) {
                view.members[i].status = GroupMemberStatus::Inactive;
                view.members[i].agent_session_epoch = std::nullopt;
                view.members[i].endpoint = std::nullopt;
                deactivated_ranks.push_back(i);
                LOG(INFO) << "[COORD] auto_deactivate group=" << group_id
                          << " rank=" << i;
            }
        }
        if (!deactivated_ranks.empty()) {
            view.epoch++;
            for (GlobalRank deactivated_rank : deactivated_ranks) {
                rejectPendingSyncs(group_id, deactivated_rank,
                                   "rank auto-deactivated", effects);
            }
            effects.push_back(ViewUpdateEffect{view});
            LOG(INFO) << "[COORD] auto_deactivate view update group="
                      << group_id << " epoch=" << view.epoch;
        }
    }
}

bool CentralizedCoordinatorStateMachine::isActivatableSet(
    GroupId group_id, const std::vector<GlobalRank>& new_ranks,
    const GroupView& old_view) const {
    // Build the future active set: old active ∪ new ranks.
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
    bool member_ok = member.isMember();
    bool endpoint_ok = member.hasEndpoint();
    bool session_ok =
        member.agent_session_epoch.has_value() &&
        *member.agent_session_epoch == ranks_[rank].agent_session_epoch;
    return member_ok && endpoint_ok && session_ok;
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

                auto acks_needed = computeBarrierAckSet(view, view, group_id);
                if (acks_needed.empty()) {
                    // No online active rank needs to ACK; become Ready
                    // immediately.
                    view.status = GroupStatus::Ready;
                    view.epoch++;
                    effects.push_back(ViewUpdateEffect{view});
                } else {
                    pending_barriers_[group_id][view.epoch] =
                        PendingViewUpdateBarrier{
                            group_id, view.epoch,
                            std::unordered_set<GlobalRank>(acks_needed.begin(),
                                                           acks_needed.end()),
                            std::nullopt,
                            PendingViewUpdateBarrier::BootstrapCommit{}};

                    effects.push_back(ViewUpdateEffect{view});
                }
            }
        }
        // BootstrapSyncing -> Ready is done in commitBarrier when all required
        // ACKs arrive
    }
}

// Private: processGroupRegistration

bool CentralizedCoordinatorStateMachine::processGroupRegistration(
    GlobalRank joining_rank, const GroupView& group, bool auto_deactivate,
    RegisterGroupResponse& response, std::vector<CoordinatorEffect>& effects) {
    GroupId group_id = group.group_id;

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

    auto it = group_views_.find(group_id);
    if (it == group_views_.end()) {
        // First declaration -> create group.
        // Founding members are all entries in rank_order.
        GroupView view;
        view.group_id = group_id;
        view.rank_order = group.rank_order;
        view.members.resize(max_world_size_);
        for (GlobalRank r : group.rank_order) {
            view.members[r].status = GroupMemberStatus::Active;
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
    // mapping).  getPeerState() relies on rank_order to resolve in-group ranks.
    if (new_order.size() > existing_order.size()) {
        group_views_[group_id].rank_order = new_order;
    }

    // The joining rank needs to be able to receive view updates from the
    // Coordinator, even if it is not yet active.  Promote None to Inactive
    // so that pushViewUpdate() will deliver the authoritative view to it.
    auto& view = group_views_[group_id];
    if (rankInRange(joining_rank) &&
        view.members[joining_rank].status == GroupMemberStatus::None) {
        view.members[joining_rank].status = GroupMemberStatus::Inactive;
    }

    // A Ready group that receives a registerGroup should push the authoritative
    // Ready view to all members (including the newly-joined inactive rank) so
    // that joining ranks can observe Ready and unblock waitUntilGroupReady().
    if (view.status == GroupStatus::Ready) {
        effects.push_back(ViewUpdateEffect{view});
    }

    response.success = true;
    return true;
}

// Private: helpers

bool CentralizedCoordinatorStateMachine::canEraseGroup(
    const GroupView& view) const {
    return std::all_of(view.members.begin(), view.members.end(),
                       [](const GroupMember& m) {
                           return m.status == GroupMemberStatus::None ||
                                  m.status == GroupMemberStatus::Left;
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
                effects.push_back(
                    ReplyProposalEffect{pc->propose_id,
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
    return RankStateUpdatePush{rank, static_cast<uint8_t>(ranks_[rank].state)};
}

void CentralizedCoordinatorStateMachine::commitBarrier(
    PendingViewUpdateBarrier barrier, const std::vector<GlobalRank>& dropped,
    std::vector<CoordinatorEffect>& effects) {
    std::visit(overloaded{
                   [&](const PendingViewUpdateBarrier::ProposalCommit& commit) {
                       auto response = commit.eventual_response;
                       if (!dropped.empty()) {
                           response.status =
                               ViewUpdateStatus::AppliedWithDroppedRanks;
                           response.dropped_ranks = dropped;
                           for (GlobalRank rank : dropped) {
                               transitionToOffline(rank, effects);
                           }
                       }
                       effects.push_back(
                           ReplyProposalEffect{commit.propose_id, response});
                   },
                   [&](const PendingViewUpdateBarrier::BootstrapCommit&) {
                       auto it = group_views_.find(barrier.group_id);
                       if (it == group_views_.end()) return;
                       GroupView& view = it->second;
                       view.status = GroupStatus::Ready;
                       view.epoch++;
                       effects.push_back(ViewUpdateEffect{view});
                   },
               },
               barrier.commit);
}

void CentralizedCoordinatorStateMachine::rejectPendingSyncs(
    GroupId group_id, GlobalRank rank, const std::string& reason,
    std::vector<CoordinatorEffect>& effects) {
    auto group_it = pending_syncs_.find(group_id);
    if (group_it == pending_syncs_.end()) return;

    auto rank_it = group_it->second.find(rank);
    if (rank_it == group_it->second.end()) return;

    SyncAfterFailureResponse resp;
    resp.status = SyncAfterFailureStatus::Rejected;
    resp.reject_reason = reason;
    auto view_it = group_views_.find(group_id);
    if (view_it != group_views_.end()) {
        resp.new_epoch = view_it->second.epoch;
    }

    for (uint64_t sync_id : rank_it->second) {
        effects.push_back(ReplySyncEffect{sync_id, resp});
    }
    group_it->second.erase(rank_it);
    if (group_it->second.empty()) {
        pending_syncs_.erase(group_it);
    }
}

// rejectPendingSyncs - reject all pending syncs for a group.
void CentralizedCoordinatorStateMachine::rejectPendingSyncs(
    GroupId group_id, const std::string& reason,
    std::vector<CoordinatorEffect>& effects) {
    auto group_it = pending_syncs_.find(group_id);
    if (group_it == pending_syncs_.end()) return;

    SyncAfterFailureResponse resp;
    resp.status = SyncAfterFailureStatus::Rejected;
    resp.reject_reason = reason;
    auto view_it = group_views_.find(group_id);
    if (view_it != group_views_.end()) {
        resp.new_epoch = view_it->second.epoch;
    }

    for (auto& [rank, sync_ids] : group_it->second) {
        for (uint64_t sync_id : sync_ids) {
            effects.push_back(ReplySyncEffect{sync_id, resp});
        }
    }
    pending_syncs_.erase(group_it);
}

// computeBarrierAckSet -- ranks that must ACK before a proposal/bootstrap
// barrier can commit.  Includes all online ranks active in either old or new
// view plus ranks with pending sync-after-failure calls.
std::unordered_set<GlobalRank>
CentralizedCoordinatorStateMachine::computeBarrierAckSet(
    const GroupView& old_view, const GroupView& new_view,
    GroupId group_id) const {
    std::unordered_set<GlobalRank> acks;
    for (int i = 0; i < max_world_size_; ++i) {
        if (ranks_[i].state == RankState::Offline) continue;
        if (old_view.members[i].isActive() || new_view.members[i].isActive()) {
            acks.insert(i);
        }
    }
    auto sync_it = pending_syncs_.find(group_id);
    if (sync_it != pending_syncs_.end()) {
        for (const auto& [rank, ids] : sync_it->second) {
            if (rankInRange(rank) && ranks_[rank].state != RankState::Offline) {
                acks.insert(rank);
            }
        }
    }
    return acks;
}

}  // namespace mooncake
