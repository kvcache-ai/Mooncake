// Copyright 2024 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Private implementation fragment included by tcp_transport.cpp from inside
// namespace mooncake. This file contains the bounded connection-lane state
// machine and its test hooks; it is intentionally not a standalone translation
// unit so this review-only split does not change linkage or initialization.

namespace {
constexpr size_t kMaxConcurrentLaneProbes = 1;
// Conservative fixed policy for the first rate-limited implementation. The
// exact cooldown/backoff policy remains subject to maintainer review.
constexpr auto kReconnectRoundCooldown = std::chrono::seconds(1);
constexpr auto kShutdownCancellationWait = std::chrono::seconds(2);

bool shouldLogOccurrence(uint64_t occurrence) {
    return occurrence != 0 && (occurrence & (occurrence - 1)) == 0;
}

void cancelTimerNoThrow(
    const std::shared_ptr<asio::steady_timer>& timer) noexcept {
    if (!timer) return;
    asio::error_code ec;
    timer->cancel(ec);
}

// Tracks only whether executor-posted cancellation actions ran. It is not an
// asynchronous-handler quiescence barrier; stop/join provides that boundary.
struct LaneCancellationPostTracker {
    std::mutex mutex;
    std::condition_variable cv;
    size_t pending = 0;

    void add() {
        std::lock_guard<std::mutex> lock(mutex);
        ++pending;
    }

    void done() {
        {
            std::lock_guard<std::mutex> lock(mutex);
            if (pending != 0) --pending;
        }
        cv.notify_all();
    }

    void waitUntil(std::chrono::steady_clock::time_point deadline) {
        std::unique_lock<std::mutex> lock(mutex);
        cv.wait_until(lock, deadline, [this] { return pending == 0; });
    }
};
}  // namespace

bool TcpTransport::hasUsableLaneLocked(const PeerConnectionGroup& group) {
    for (const auto& lane : group.lanes) {
        if ((lane->state == LaneState::IDLE || lane->state == LaneState::BUSY ||
             lane->state == LaneState::COMPLETING) &&
            lane->socket && lane->socket->is_open()) {
            return true;
        }
    }
    return false;
}

bool TcpTransport::hasDisconnectedLaneLocked(const PeerConnectionGroup& group) {
    return std::any_of(group.lanes.begin(), group.lanes.end(),
                       [](const auto& lane) {
                           return lane->state == LaneState::DISCONNECTED;
                       });
}

bool TcpTransport::hasUntriedDisconnectedLaneLocked(
    const PeerConnectionGroup& group) {
    return std::any_of(
        group.lanes.begin(), group.lanes.end(), [&group](const auto& lane) {
            return lane->state == LaneState::DISCONNECTED &&
                   lane->last_connect_round != group.connect_round;
        });
}

#ifdef MOONCAKE_TCP_TRANSPORT_TEST_HOOKS
size_t TcpTransport::activeSocketCountLocked(const PeerConnectionGroup& group) {
    size_t count = 0;
    for (const auto& lane : group.lanes) {
        if (lane->resolver || lane->socket) ++count;
    }
    return count;
}
#endif

void TcpTransport::beginConnectRoundLocked(PeerConnectionGroup& group) {
    ++group.connect_round;
    if (group.connect_round == 0) group.connect_round = 1;
    group.connect_round_had_success = false;
    group.next_probe_not_before = {};
}

void TcpTransport::enterReconnectCooldownLocked(PeerConnectionGroup& group) {
    group.next_probe_not_before =
        std::chrono::steady_clock::now() + kReconnectRoundCooldown;
}

void TcpTransport::addQueuedBytesLocked(PeerConnectionGroup& group,
                                        uint64_t length) {
    if (group.queued_bytes_saturated) return;
    if (group.queued_bytes > std::numeric_limits<uint64_t>::max() - length) {
        group.queued_bytes = std::numeric_limits<uint64_t>::max();
        group.queued_bytes_saturated = true;
        return;
    }
    group.queued_bytes += length;
}

void TcpTransport::removeQueuedBytesLocked(PeerConnectionGroup& group,
                                           uint64_t length) {
    if (!group.queued_bytes_saturated) {
        group.queued_bytes =
            group.queued_bytes >= length ? group.queued_bytes - length : 0;
        return;
    }

    // Once saturated, UINT64_MAX is only an explicit lower-fidelity marker,
    // not an exact sum. Recompute after removal so exact accounting resumes as
    // soon as the remaining queue fits in uint64_t.
    group.queued_bytes = 0;
    group.queued_bytes_saturated = false;
    for (const auto& item : group.queue) {
        addQueuedBytesLocked(group, item.slice->length);
        if (group.queued_bytes_saturated) break;
    }
}

void TcpTransport::clearQueuedBytesLocked(PeerConnectionGroup& group) {
    group.queued_bytes = 0;
    group.queued_bytes_saturated = false;
}

size_t TcpTransport::expirePendingAdmissionsLocked(
    PeerConnectionGroup& group, std::chrono::steady_clock::time_point now,
    std::deque<TcpWorkItem>& expired) {
    size_t count = 0;
    while (!group.pending_admissions.empty() &&
           group.pending_admissions.front().admission_deadline <= now) {
        expired.emplace_back(std::move(group.pending_admissions.front()));
        group.pending_admissions.pop_front();
        ++count;
    }
    return count;
}

size_t TcpTransport::promotePendingAdmissionsLocked(
    PeerConnectionGroup& group) {
    size_t count = 0;
    while (group.queue.size() < group.queue_capacity &&
           !group.pending_admissions.empty()) {
        group.queue.emplace_back(std::move(group.pending_admissions.front()));
        group.pending_admissions.pop_front();
        addQueuedBytesLocked(group, group.queue.back().slice->length);
        ++count;
    }
    return count;
}

void TcpTransport::refreshAdmissionTimerLocked(
    const std::shared_ptr<PeerConnectionGroup>& group,
    std::deque<TcpWorkItem>& runtime_failed,
    std::shared_ptr<asio::steady_timer>& timer_to_cancel, bool& timer_armed) {
    timer_armed = false;
    if (group->pending_admissions.empty()) {
        if (group->admission_timer) {
            ++group->admission_epoch;
            if (group->admission_epoch == 0) ++group->admission_epoch;
            timer_to_cancel = std::move(group->admission_timer);
        }
        return;
    }

    if (group->admission_timer) return;

    try {
        auto timer = std::make_shared<asio::steady_timer>(group->executor);
        timer->expires_at(group->pending_admissions.front().admission_deadline);
        ++group->admission_epoch;
        if (group->admission_epoch == 0) ++group->admission_epoch;
        const uint64_t admission_epoch = group->admission_epoch;
        group->admission_timer = timer;
        timer->async_wait([group, timer, admission_epoch](asio::error_code ec) {
#ifdef MOONCAKE_TCP_TRANSPORT_TEST_HOOKS
            invokeLaneAdmissionHandlerHook();
#endif
            handleAdmissionTimer(group, timer, admission_epoch, ec);
        });
        timer_armed = true;
        return;
    } catch (...) {
        ++group->admission_epoch;
        if (group->admission_epoch == 0) ++group->admission_epoch;
        if (group->admission_timer)
            timer_to_cancel = std::move(group->admission_timer);
        while (!group->pending_admissions.empty()) {
            runtime_failed.emplace_back(
                std::move(group->pending_admissions.front()));
            group->pending_admissions.pop_front();
        }
        return;
    }
}

void TcpTransport::handleAdmissionTimer(
    const std::shared_ptr<PeerConnectionGroup>& group,
    const std::shared_ptr<asio::steady_timer>& timer, uint64_t admission_epoch,
    asio::error_code ec) {
    std::deque<TcpWorkItem> expired;
    std::deque<TcpWorkItem> runtime_failed;
    std::shared_ptr<asio::steady_timer> timer_to_cancel;
    uint64_t pump_epoch = 0;
    size_t promoted = 0;
    [[maybe_unused]] size_t pending_depth = 0;
    bool timer_armed = false;
    [[maybe_unused]] bool fired = false;
    [[maybe_unused]] bool late = false;
    {
        std::lock_guard<std::mutex> lock(group->mutex);
        if (group->state != GroupState::OPEN ||
            group->admission_epoch != admission_epoch ||
            group->admission_timer != timer) {
            late = true;
        } else {
            group->admission_timer.reset();
            fired = true;
            if (ec) {
                // Cancellation normally increments admission_epoch first and
                // is therefore stale. Keep this guard for other Asio timer
                // errors delivered while the matching timer is still active.
                while (!group->pending_admissions.empty()) {
                    runtime_failed.emplace_back(
                        std::move(group->pending_admissions.front()));
                    group->pending_admissions.pop_front();
                }
            } else {
                expirePendingAdmissionsLocked(
                    *group, std::chrono::steady_clock::now(), expired);
                promoted = promotePendingAdmissionsLocked(*group);
                refreshAdmissionTimerLocked(group, runtime_failed,
                                            timer_to_cancel, timer_armed);
                if (promoted != 0) pump_epoch = requestGroupPumpLocked(*group);
            }
            pending_depth = group->pending_admissions.size();
        }
    }

    cancelTimerNoThrow(timer_to_cancel);
#ifdef MOONCAKE_TCP_TRANSPORT_TEST_HOOKS
    if (late) {
        invokeLaneObserverHook(kLaneAdmissionTimerLate, 0, 0, 0, false);
    } else if (fired) {
        invokeLaneObserverHook(kLaneAdmissionTimerFired, pending_depth, 0, 0,
                               false);
    }
    if (promoted != 0)
        invokeLaneObserverHook(kLaneAdmissionPromoted, pending_depth, promoted,
                               0, false);
    if (timer_armed)
        invokeLaneObserverHook(kLaneAdmissionTimerArmed, pending_depth, 0, 0,
                               false);
#endif
    failWorkItems(std::move(expired), WorkFailureReason::QUEUE_TIMEOUT,
                  group->failure_counters);
    failWorkItems(std::move(runtime_failed),
                  WorkFailureReason::RUNTIME_UNAVAILABLE,
                  group->failure_counters);
    if (pump_epoch != 0) postGroupPump(group, pump_epoch);
}

bool TcpTransport::armRetryTimerLocked(
    const std::shared_ptr<PeerConnectionGroup>& group) {
    if (group->retry_timer || group->state != GroupState::OPEN ||
        group->queue.empty()) {
        return true;
    }

    try {
        auto timer = std::make_shared<asio::steady_timer>(group->executor);
        timer->expires_at(group->next_probe_not_before);
        ++group->retry_epoch;
        if (group->retry_epoch == 0) ++group->retry_epoch;
        const uint64_t retry_epoch = group->retry_epoch;
        group->retry_timer = timer;
        // A handler abandoned by io_context.stop() is harmless: it holds only
        // group/timer shared state and can only request a new connection round.
        timer->async_wait([group, timer, retry_epoch](asio::error_code ec) {
#ifdef MOONCAKE_TCP_TRANSPORT_TEST_HOOKS
            invokeLaneRetryHandlerHook();
#endif
            handleRetryTimer(group, timer, retry_epoch, ec);
        });
        return true;
    } catch (...) {
        group->retry_timer.reset();
        ++group->retry_epoch;
        if (group->retry_epoch == 0) ++group->retry_epoch;
        return false;
    }
}

void TcpTransport::handleRetryTimer(
    const std::shared_ptr<PeerConnectionGroup>& group,
    const std::shared_ptr<asio::steady_timer>& timer, uint64_t retry_epoch,
    asio::error_code ec) {
    uint64_t pump_epoch = 0;
    [[maybe_unused]] bool fired = false;
    [[maybe_unused]] bool late = false;
    {
        std::lock_guard<std::mutex> lock(group->mutex);
        if (group->retry_epoch != retry_epoch || group->retry_timer != timer) {
            late = true;
        } else {
            group->retry_timer.reset();
            if (group->state != GroupState::OPEN) {
                // Shutdown also bumps retry_epoch, so this is normally caught
                // by the identity check above. Retain the state guard for a
                // handler that observes the transition at this boundary.
                late = group->state != GroupState::OPEN;
            } else if (!group->queue.empty() && !ec &&
                       group->probes_in_flight == 0 &&
                       hasDisconnectedLaneLocked(*group) &&
                       !hasUntriedDisconnectedLaneLocked(*group)) {
                beginConnectRoundLocked(*group);
                pump_epoch = requestGroupPumpLocked(*group);
                fired = true;
            } else if (!group->queue.empty() && group->probes_in_flight == 0) {
                pump_epoch = requestGroupPumpLocked(*group);
            }
        }
    }

#ifdef MOONCAKE_TCP_TRANSPORT_TEST_HOOKS
    if (late)
        invokeLaneObserverHook(kLaneRetryLate, 0, 0, 0, false);
    else if (fired)
        invokeLaneObserverHook(kLaneRetryFired, 0, 0, 0, false);
#endif
    if (pump_epoch != 0) postGroupPump(group, pump_epoch);
}

uint64_t TcpTransport::requestGroupPumpLocked(PeerConnectionGroup& group) {
    if (group.state != GroupState::OPEN || group.pump_scheduled ||
        group.queue.empty()) {
        return 0;
    }
    group.pump_scheduled = true;
    ++group.pump_epoch;
    if (group.pump_epoch == 0) ++group.pump_epoch;
    return group.pump_epoch;
}

void TcpTransport::enqueuePooledTransfer(const ConnectionKey& key,
                                         TcpWorkItem work) {
    const auto state = lane_state_;
    std::shared_ptr<PeerConnectionGroup> group;
    std::optional<TcpWorkItem> rejected;
    std::deque<TcpWorkItem> expired;
    std::deque<TcpWorkItem> runtime_failed;
    std::shared_ptr<asio::steady_timer> timer_to_cancel;
    WorkFailureReason rejection_reason = WorkFailureReason::QUEUE_FULL;
    uint64_t pump_epoch = 0;
    [[maybe_unused]] size_t promoted = 0;
    [[maybe_unused]] size_t pending_depth = 0;
    [[maybe_unused]] bool direct_admission = false;
    [[maybe_unused]] bool pending_admission = false;
    [[maybe_unused]] bool hard_rejection = false;
    bool timer_armed = false;
#ifdef MOONCAKE_TCP_TRANSPORT_TEST_HOOKS
    size_t queue_depth = 0;
    uint64_t queued_bytes = 0;
    size_t active_sockets = 0;
#endif

    try {
        std::lock_guard<std::mutex> state_lock(state->mutex);
        if (state->shutting_down) {
            rejected.emplace(std::move(work));
            rejection_reason = WorkFailureReason::SHUTDOWN;
        } else {
            auto runtime = state->runtime.lock();
            if (!runtime) {
                rejected.emplace(std::move(work));
                rejection_reason = WorkFailureReason::RUNTIME_UNAVAILABLE;
            } else {
                auto group_it = state->groups.find(key);
                if (group_it == state->groups.end()) {
                    group = std::make_shared<PeerConnectionGroup>(
                        key, runtime->executor,
                        state->max_queued_transfers_per_peer,
                        state->max_pending_admissions_per_peer,
                        state->admission_timeout, state->failure_counters);
                    group->lanes.reserve(state->lanes_per_peer);
                    for (size_t i = 0; i < state->lanes_per_peer; ++i) {
                        group->lanes.push_back(
                            std::make_shared<ConnectionLane>(i, group));
                    }
                    auto [inserted_it, inserted] =
                        state->groups.emplace(key, group);
                    if (!inserted) group = inserted_it->second;
                } else {
                    group = group_it->second;
                }

                std::lock_guard<std::mutex> group_lock(group->mutex);
                if (group->state != GroupState::OPEN) {
                    rejected.emplace(std::move(work));
                    rejection_reason = WorkFailureReason::SHUTDOWN;
                } else {
                    // Submissions arriving during cooldown remain subject to
                    // the bounded queue and wait until the retry timer expires
                    // and a new round begins, so their added latency is bounded
                    // by the cooldown length.
                    const auto admission_time =
                        std::chrono::steady_clock::now();
                    expirePendingAdmissionsLocked(*group, admission_time,
                                                  expired);
                    promoted = promotePendingAdmissionsLocked(*group);

                    if (group->pending_admissions.empty() &&
                        group->queue.size() < group->queue_capacity) {
                        group->queue.emplace_back(std::move(work));
                        addQueuedBytesLocked(*group,
                                             group->queue.back().slice->length);
                        direct_admission = true;
                    } else if (group->pending_admissions.size() <
                               group->pending_admission_capacity) {
                        work.admission_deadline =
                            admission_time + group->admission_timeout;
                        group->pending_admissions.emplace_back(std::move(work));
                        pending_admission = true;
                    } else {
                        rejected.emplace(std::move(work));
                        hard_rejection = true;
                    }

                    refreshAdmissionTimerLocked(group, runtime_failed,
                                                timer_to_cancel, timer_armed);
                    pump_epoch = requestGroupPumpLocked(*group);
                    pending_depth = group->pending_admissions.size();
#ifdef MOONCAKE_TCP_TRANSPORT_TEST_HOOKS
                    queue_depth = group->queue.size();
                    queued_bytes = group->queued_bytes;
                    active_sockets = activeSocketCountLocked(*group);
#endif
                }
            }
        }
    } catch (const std::exception& e) {
        LOG(ERROR) << "Failed to admit TCP work for " << key.host << ":"
                   << key.port << ". Error: " << e.what();
        if (work.slice) rejected.emplace(std::move(work));
        rejection_reason = WorkFailureReason::RUNTIME_UNAVAILABLE;
    } catch (...) {
        LOG(ERROR) << "Failed to admit TCP work for " << key.host << ":"
                   << key.port << ". Error: unknown exception";
        if (work.slice) rejected.emplace(std::move(work));
        rejection_reason = WorkFailureReason::RUNTIME_UNAVAILABLE;
    }

    cancelTimerNoThrow(timer_to_cancel);
#ifdef MOONCAKE_TCP_TRANSPORT_TEST_HOOKS
    if (rejected) {
        invokeLaneObserverHook(kLaneQueueRejected, queue_depth, queued_bytes,
                               active_sockets, false);
    } else if (direct_admission) {
        invokeLaneObserverHook(kLaneQueueAdmitted, queue_depth, queued_bytes,
                               active_sockets, false);
    }
    if (pending_admission)
        invokeLaneObserverHook(kLaneAdmissionPending, pending_depth, 0, 0,
                               false);
    if (promoted != 0)
        invokeLaneObserverHook(kLaneAdmissionPromoted, pending_depth, promoted,
                               0, false);
    if (timer_armed)
        invokeLaneObserverHook(kLaneAdmissionTimerArmed, pending_depth, 0, 0,
                               false);
    if (hard_rejection)
        invokeLaneObserverHook(kLaneAdmissionHardRejected, pending_depth, 0, 0,
                               false);
#endif

    failWorkItems(std::move(expired), WorkFailureReason::QUEUE_TIMEOUT,
                  state->failure_counters);
    failWorkItems(std::move(runtime_failed),
                  WorkFailureReason::RUNTIME_UNAVAILABLE,
                  state->failure_counters);
    if (rejected)
        failWorkItem(std::move(*rejected), rejection_reason,
                     state->failure_counters);
    else if (pump_epoch != 0)
        postGroupPump(group, pump_epoch);
}

void TcpTransport::postGroupPump(
    const std::shared_ptr<PeerConnectionGroup>& group, uint64_t pump_epoch) {
    auto fail_posted_work = [&](const char* error) {
        std::deque<TcpWorkItem> failed_queue;
        std::deque<TcpWorkItem> failed_pending;
        std::shared_ptr<asio::steady_timer> admission_timer;
        {
            std::lock_guard<std::mutex> lock(group->mutex);
            if (group->pump_scheduled && group->pump_epoch == pump_epoch) {
                group->pump_scheduled = false;
                failed_queue.swap(group->queue);
                clearQueuedBytesLocked(*group);
                failed_pending.swap(group->pending_admissions);
                ++group->admission_epoch;
                if (group->admission_epoch == 0) ++group->admission_epoch;
                admission_timer = std::move(group->admission_timer);
            }
        }
        cancelTimerNoThrow(admission_timer);
        failWorkItems(std::move(failed_queue),
                      WorkFailureReason::RUNTIME_UNAVAILABLE,
                      group->failure_counters);
        failWorkItems(std::move(failed_pending),
                      WorkFailureReason::RUNTIME_UNAVAILABLE,
                      group->failure_counters);
        LOG(ERROR) << "Failed to schedule TCP lane pump for " << group->key.host
                   << ":" << group->key.port
                   << (error && *error ? ". Error: " : "")
                   << (error && *error ? error : "");
    };

    try {
        asio::post(group->executor,
                   [group, pump_epoch] { runGroupPump(group, pump_epoch); });
    } catch (const std::exception& e) {
        fail_posted_work(e.what());
    } catch (...) {
        fail_posted_work(nullptr);
    }
}

void TcpTransport::runGroupPump(
    const std::shared_ptr<PeerConnectionGroup>& group, uint64_t pump_epoch) {
    struct LaneStart {
        std::shared_ptr<ConnectionLane> lane;
        uint64_t epoch;
    };
    std::array<LaneStart, kMaxTcpLanesPerPeer> sessions;
    std::array<LaneStart, kMaxTcpLanesPerPeer> connects;
    size_t session_count = 0;
    size_t connect_count = 0;
    std::deque<TcpWorkItem> failed;
    std::deque<TcpWorkItem> expired;
    std::deque<TcpWorkItem> runtime_failed;
    std::shared_ptr<asio::steady_timer> admission_timer_to_cancel;
    WorkFailureReason failure_reason = WorkFailureReason::CONNECT_FAILED;
    uint64_t followup_pump_epoch = 0;
    [[maybe_unused]] size_t promoted = 0;
    [[maybe_unused]] size_t pending_depth = 0;
    bool queue_detached_after_scheduling = false;
    [[maybe_unused]] bool retry_armed = false;
    [[maybe_unused]] bool cooldown_started = false;
    [[maybe_unused]] bool admission_timer_armed = false;

    {
        std::lock_guard<std::mutex> lock(group->mutex);
        if (!group->pump_scheduled || group->pump_epoch != pump_epoch) return;
        group->pump_scheduled = false;
        if (group->state != GroupState::OPEN) return;

        expirePendingAdmissionsLocked(*group, std::chrono::steady_clock::now(),
                                      expired);
        promoted += promotePendingAdmissionsLocked(*group);

        for (const auto& lane : group->lanes) {
            if (group->queue.empty()) break;
            if (lane->state != LaneState::IDLE) continue;
            if (!lane->socket || !lane->socket->is_open()) {
                lane->socket.reset();
                lane->state = LaneState::DISCONNECTED;
                continue;
            }

            lane->current.emplace(std::move(group->queue.front()));
            const uint64_t length = lane->current->slice->length;
            group->queue.pop_front();
            removeQueuedBytesLocked(*group, length);
            if (group->queue.empty()) clearQueuedBytesLocked(*group);
            promoted += promotePendingAdmissionsLocked(*group);
            lane->state = LaneState::BUSY;
            ++lane->operation_epoch;
            if (lane->operation_epoch == 0) ++lane->operation_epoch;
            sessions[session_count++] = {lane, lane->operation_epoch};
        }

        // Once armed, the retry timer exclusively owns the transition out of
        // cooldown. A delayed pump must not reset round accounting or start a
        // probe before the matching timer handler validates the group.
        bool waiting_for_cooldown = group->retry_timer != nullptr;
        const bool round_exhausted = hasDisconnectedLaneLocked(*group) &&
                                     !hasUntriedDisconnectedLaneLocked(*group);
        if (!waiting_for_cooldown && !group->queue.empty() &&
            group->probes_in_flight == 0 && round_exhausted) {
            const bool cooldown_already_started =
                group->next_probe_not_before !=
                std::chrono::steady_clock::time_point{};
            if (!hasUsableLaneLocked(*group) && !cooldown_already_started) {
                failed.swap(group->queue);
                clearQueuedBytesLocked(*group);
                enterReconnectCooldownLocked(*group);
                failure_reason = WorkFailureReason::CONNECT_FAILED;
                queue_detached_after_scheduling = true;
            } else if (group->next_probe_not_before ==
                       std::chrono::steady_clock::time_point{}) {
                enterReconnectCooldownLocked(*group);
            }
            if (std::chrono::steady_clock::now() <
                group->next_probe_not_before) {
                if (armRetryTimerLocked(group)) {
                    waiting_for_cooldown = true;
                    retry_armed = group->retry_timer != nullptr;
                } else {
                    failed.swap(group->queue);
                    clearQueuedBytesLocked(*group);
                    failure_reason = WorkFailureReason::RUNTIME_UNAVAILABLE;
                    queue_detached_after_scheduling = true;
                }
            } else {
                beginConnectRoundLocked(*group);
            }
        }

        // A lane-local failure may defer only connection probes while healthy
        // siblings continue pulling work above. Reuse the per-group timer so
        // repeated failures cannot create a tight reconnect loop.
        if (!waiting_for_cooldown && failed.empty() && !group->queue.empty() &&
            group->probes_in_flight == 0 &&
            std::chrono::steady_clock::now() < group->next_probe_not_before &&
            armRetryTimerLocked(group)) {
            waiting_for_cooldown = group->retry_timer != nullptr;
            retry_armed = waiting_for_cooldown;
        }

        if (!waiting_for_cooldown && failed.empty()) {
            const size_t probe_limit =
                std::min(group->lanes.size(), kMaxConcurrentLaneProbes);
            while (!group->queue.empty() &&
                   group->probes_in_flight < probe_limit) {
                auto lane_it = std::find_if(
                    group->lanes.begin(), group->lanes.end(),
                    [&group](const auto& lane) {
                        return lane->state == LaneState::DISCONNECTED &&
                               lane->last_connect_round != group->connect_round;
                    });
                if (lane_it == group->lanes.end()) break;

                auto lane = *lane_it;
                lane->state = LaneState::CONNECTING;
                lane->connect_stage = LaneConnectStage::NONE;
                lane->last_connect_round = group->connect_round;
                ++lane->operation_epoch;
                if (lane->operation_epoch == 0) ++lane->operation_epoch;
                ++group->probes_in_flight;
                connects[connect_count++] = {lane, lane->operation_epoch};
            }

            if (!group->queue.empty() && group->probes_in_flight == 0 &&
                hasDisconnectedLaneLocked(*group) &&
                !hasUntriedDisconnectedLaneLocked(*group)) {
                if (!hasUsableLaneLocked(*group)) {
                    failed.swap(group->queue);
                    clearQueuedBytesLocked(*group);
                    enterReconnectCooldownLocked(*group);
                    queue_detached_after_scheduling = true;
                } else {
                    enterReconnectCooldownLocked(*group);
                }
                cooldown_started = true;
            }
        }

        promoted += promotePendingAdmissionsLocked(*group);
        refreshAdmissionTimerLocked(group, runtime_failed,
                                    admission_timer_to_cancel,
                                    admission_timer_armed);
        pending_depth = group->pending_admissions.size();
        if (queue_detached_after_scheduling && !group->queue.empty())
            followup_pump_epoch = requestGroupPumpLocked(*group);
    }

    cancelTimerNoThrow(admission_timer_to_cancel);
#ifdef MOONCAKE_TCP_TRANSPORT_TEST_HOOKS
    if (retry_armed) invokeLaneObserverHook(kLaneRetryArmed, 0, 0, 0, false);
    if (cooldown_started)
        invokeLaneObserverHook(kLaneCooldownStarted, 0, 0, 0, false);
    if (promoted != 0)
        invokeLaneObserverHook(kLaneAdmissionPromoted, pending_depth, promoted,
                               0, false);
    if (admission_timer_armed)
        invokeLaneObserverHook(kLaneAdmissionTimerArmed, pending_depth, 0, 0,
                               false);
#endif
    for (size_t i = 0; i < connect_count; ++i)
        startLaneConnect(group, connects[i].lane, connects[i].epoch);
    for (size_t i = 0; i < session_count; ++i)
        startLaneSession(group, sessions[i].lane, sessions[i].epoch);
    failWorkItems(std::move(failed), failure_reason, group->failure_counters);
    failWorkItems(std::move(expired), WorkFailureReason::QUEUE_TIMEOUT,
                  group->failure_counters);
    failWorkItems(std::move(runtime_failed),
                  WorkFailureReason::RUNTIME_UNAVAILABLE,
                  group->failure_counters);
    if (followup_pump_epoch != 0) postGroupPump(group, followup_pump_epoch);
}

void TcpTransport::startLaneConnect(
    const std::shared_ptr<PeerConnectionGroup>& group,
    const std::shared_ptr<ConnectionLane>& lane, uint64_t epoch) {
    std::string initiation_error;
#ifdef MOONCAKE_TCP_TRANSPORT_TEST_HOOKS
    size_t queue_depth = 0;
    uint64_t queued_bytes = 0;
    size_t active_sockets = 0;
#endif
    {
        std::lock_guard<std::mutex> lock(group->mutex);
        if (group->state != GroupState::OPEN ||
            lane->state != LaneState::CONNECTING ||
            lane->operation_epoch != epoch) {
            return;
        }
#ifdef MOONCAKE_TCP_TRANSPORT_TEST_HOOKS
        if (invokeLaneConnectFailureInjectionHook(lane->lane_id)) {
            initiation_error = "injected lane connect failure";
        } else
#endif
            try {
                lane->resolver =
                    std::make_shared<asio::ip::tcp::resolver>(group->executor);
                lane->socket =
                    std::make_shared<asio::ip::tcp::socket>(group->executor);
                lane->connect_stage = LaneConnectStage::RESOLVING;
                lane->resolver->async_resolve(
                    group->key.host, std::to_string(group->key.port),
                    [group, lane, epoch](
                        asio::error_code ec,
                        asio::ip::tcp::resolver::results_type results) {
#ifdef MOONCAKE_TCP_TRANSPORT_TEST_HOOKS
                        invokeLaneConnectHandlerHook();
#endif
                        handleLaneResolved(group, lane, epoch, ec,
                                           std::move(results));
                    });
#ifdef MOONCAKE_TCP_TRANSPORT_TEST_HOOKS
                queue_depth = group->queue.size();
                queued_bytes = group->queued_bytes;
                active_sockets = activeSocketCountLocked(*group);
#endif
            } catch (const std::exception& e) {
                initiation_error = e.what();
            } catch (...) {
                initiation_error = "unknown exception";
            }
    }

#ifdef MOONCAKE_TCP_TRANSPORT_TEST_HOOKS
    if (initiation_error.empty()) {
        invokeLaneObserverHook(kLaneConnecting, queue_depth, queued_bytes,
                               active_sockets, false);
    }
#endif
    if (!initiation_error.empty())
        handleLaneConnectFailure(group, lane, epoch, initiation_error);
}

void TcpTransport::handleLaneResolved(
    const std::shared_ptr<PeerConnectionGroup>& group,
    const std::shared_ptr<ConnectionLane>& lane, uint64_t epoch,
    asio::error_code ec, asio::ip::tcp::resolver::results_type results) {
    if (ec) {
        handleLaneConnectFailure(group, lane, epoch, ec.message());
        return;
    }

    std::string initiation_error;
    [[maybe_unused]] bool stale = false;
    {
        std::lock_guard<std::mutex> lock(group->mutex);
        if (group->state != GroupState::OPEN ||
            lane->state != LaneState::CONNECTING ||
            lane->connect_stage != LaneConnectStage::RESOLVING ||
            lane->operation_epoch != epoch || !lane->socket) {
            stale = true;
        } else {
            lane->connect_stage = LaneConnectStage::CONNECTING;
            try {
                asio::async_connect(
                    *lane->socket, results,
                    [group, lane, epoch](asio::error_code connect_ec,
                                         const asio::ip::tcp::endpoint&) {
                        handleLaneConnected(group, lane, epoch, connect_ec);
                    });
            } catch (const std::exception& e) {
                initiation_error = e.what();
            } catch (...) {
                initiation_error = "unknown exception";
            }
        }
    }

#ifdef MOONCAKE_TCP_TRANSPORT_TEST_HOOKS
    if (stale) invokeLaneObserverHook(kLaneLateHandler, 0, 0, 0, false);
#endif
    if (!initiation_error.empty())
        handleLaneConnectFailure(group, lane, epoch, initiation_error);
}

void TcpTransport::handleLaneConnected(
    const std::shared_ptr<PeerConnectionGroup>& group,
    const std::shared_ptr<ConnectionLane>& lane, uint64_t epoch,
    asio::error_code ec) {
    if (ec) {
        handleLaneConnectFailure(group, lane, epoch, ec.message());
        return;
    }

    uint64_t pump_epoch = 0;
    [[maybe_unused]] bool stale = false;
    std::string option_error;
    {
        std::lock_guard<std::mutex> lock(group->mutex);
        if (group->state != GroupState::OPEN ||
            lane->state != LaneState::CONNECTING ||
            lane->connect_stage != LaneConnectStage::CONNECTING ||
            lane->operation_epoch != epoch || !lane->socket) {
            stale = true;
        } else {
            asio::error_code option_ec;
            lane->socket->set_option(asio::ip::tcp::no_delay(true), option_ec);
            if (option_ec) {
                option_error = option_ec.message();
            } else {
                if (group->probes_in_flight != 0) --group->probes_in_flight;
                group->connect_round_had_success = true;
                lane->resolver.reset();
                lane->connect_stage = LaneConnectStage::NONE;
                lane->state = LaneState::IDLE;
                pump_epoch = requestGroupPumpLocked(*group);
            }
        }
    }

#ifdef MOONCAKE_TCP_TRANSPORT_TEST_HOOKS
    if (stale) invokeLaneObserverHook(kLaneLateHandler, 0, 0, 0, false);
#endif
    if (!option_error.empty()) {
        handleLaneConnectFailure(group, lane, epoch, option_error);
    } else if (pump_epoch != 0) {
        postGroupPump(group, pump_epoch);
    }
}

void TcpTransport::handleLaneConnectFailure(
    const std::shared_ptr<PeerConnectionGroup>& group,
    const std::shared_ptr<ConnectionLane>& lane, uint64_t epoch,
    const std::string& error) {
    std::shared_ptr<asio::ip::tcp::resolver> resolver;
    std::shared_ptr<asio::ip::tcp::socket> socket;
    std::deque<TcpWorkItem> failed;
    std::deque<TcpWorkItem> expired;
    std::deque<TcpWorkItem> runtime_failed;
    std::shared_ptr<asio::steady_timer> admission_timer_to_cancel;
    uint64_t pump_epoch = 0;
    [[maybe_unused]] size_t promoted = 0;
    [[maybe_unused]] size_t pending_depth = 0;
    bool stale = false;
    [[maybe_unused]] bool cooldown_started = false;
    [[maybe_unused]] bool admission_timer_armed = false;
    uint64_t connect_failure_log_count = 0;
    {
        std::lock_guard<std::mutex> lock(group->mutex);
        if (lane->state != LaneState::CONNECTING ||
            lane->operation_epoch != epoch) {
            stale = true;
        } else {
            connect_failure_log_count = ++group->connect_failure_log_count;
            if (group->probes_in_flight != 0) --group->probes_in_flight;
            resolver = std::move(lane->resolver);
            socket = std::move(lane->socket);
            lane->connect_stage = LaneConnectStage::NONE;
            lane->state = group->state == GroupState::OPEN
                              ? LaneState::DISCONNECTED
                              : LaneState::CLOSING;

            const bool sibling_usable =
                group->state == GroupState::OPEN && hasUsableLaneLocked(*group);

            if (group->state == GroupState::OPEN && !group->queue.empty() &&
                !sibling_usable && group->probes_in_flight == 0 &&
                !hasUntriedDisconnectedLaneLocked(*group)) {
                failed.swap(group->queue);
                clearQueuedBytesLocked(*group);
                enterReconnectCooldownLocked(*group);
                cooldown_started = true;
                expirePendingAdmissionsLocked(
                    *group, std::chrono::steady_clock::now(), expired);
                promoted = promotePendingAdmissionsLocked(*group);
                refreshAdmissionTimerLocked(group, runtime_failed,
                                            admission_timer_to_cancel,
                                            admission_timer_armed);
                pending_depth = group->pending_admissions.size();
                pump_epoch = requestGroupPumpLocked(*group);
            } else {
                pump_epoch = requestGroupPumpLocked(*group);
            }
        }
    }

    if (resolver) {
        try {
            resolver->cancel();
        } catch (...) {
        }
    }
    closeSocketNoThrow(socket);
    cancelTimerNoThrow(admission_timer_to_cancel);
    if (!stale && shouldLogOccurrence(connect_failure_log_count)) {
        LOG(ERROR) << "TCP lane connection to " << group->key.host << ":"
                   << group->key.port << " failed: " << error
                   << " (attempt failure " << connect_failure_log_count << ")";
    }
#ifdef MOONCAKE_TCP_TRANSPORT_TEST_HOOKS
    if (stale) invokeLaneObserverHook(kLaneLateHandler, 0, 0, 0, false);
    if (cooldown_started)
        invokeLaneObserverHook(kLaneCooldownStarted, 0, 0, 0, false);
    if (promoted != 0)
        invokeLaneObserverHook(kLaneAdmissionPromoted, pending_depth, promoted,
                               0, false);
    if (admission_timer_armed)
        invokeLaneObserverHook(kLaneAdmissionTimerArmed, pending_depth, 0, 0,
                               false);
#endif
    failWorkItems(std::move(failed), WorkFailureReason::CONNECT_FAILED,
                  group->failure_counters);
    failWorkItems(std::move(expired), WorkFailureReason::QUEUE_TIMEOUT,
                  group->failure_counters);
    failWorkItems(std::move(runtime_failed),
                  WorkFailureReason::RUNTIME_UNAVAILABLE,
                  group->failure_counters);
    if (pump_epoch != 0) postGroupPump(group, pump_epoch);
}

void TcpTransport::startLaneSession(
    const std::shared_ptr<PeerConnectionGroup>& group,
    const std::shared_ptr<ConnectionLane>& lane, uint64_t epoch) {
    std::string initiation_error;
#ifdef MOONCAKE_TCP_TRANSPORT_TEST_HOOKS
    size_t queue_depth = 0;
    uint64_t queued_bytes = 0;
    size_t active_sockets = 0;
#endif
    {
        std::lock_guard<std::mutex> lock(group->mutex);
        if (group->state != GroupState::OPEN ||
            lane->state != LaneState::BUSY || lane->operation_epoch != epoch ||
            !lane->current) {
            return;
        }
        if (!lane->socket || !lane->socket->is_open()) {
            initiation_error = "lane socket is not open";
        } else {
            // This function runs on the TCP executor. Keep construction and
            // initial Asio initiation under the group lock so shutdown cannot
            // invalidate the checked epoch between validation and initiation.
            // Asio initiating functions do not invoke their completion handler
            // inline, so this cannot call lane terminal completion under the
            // mutex.
            try {
                std::weak_ptr<PeerConnectionGroup> weak_group(group);
                std::weak_ptr<ConnectionLane> weak_lane(lane);
                auto session = std::make_shared<ClientSession>(
                    lane->socket, lane->current->use_v2,
                    [weak_group, weak_lane, epoch](TransferStatusEnum status,
                                                   bool clean) noexcept {
                        auto callback_group = weak_group.lock();
                        auto callback_lane = weak_lane.lock();
                        if (!callback_group || !callback_lane) return;
                        handleLaneTerminal(callback_group, callback_lane, epoch,
                                           status, clean);
                    });
                lane->session = session;
                session->initiate(lane->current->slice->source_addr,
                                  lane->current->slice->tcp.dest_addr,
                                  lane->current->slice->length,
                                  lane->current->slice->opcode);
#ifdef MOONCAKE_TCP_TRANSPORT_TEST_HOOKS
                queue_depth = group->queue.size();
                queued_bytes = group->queued_bytes;
                active_sockets = activeSocketCountLocked(*group);
#endif
            } catch (const std::exception& e) {
                lane->session.reset();
                initiation_error = e.what();
            } catch (...) {
                lane->session.reset();
                initiation_error = "unknown exception";
            }
        }
    }

#ifdef MOONCAKE_TCP_TRANSPORT_TEST_HOOKS
    if (initiation_error.empty()) {
        invokeLaneObserverHook(kLaneBusy, queue_depth, queued_bytes,
                               active_sockets, true);
    }
#endif
    if (!initiation_error.empty()) {
        LOG(ERROR) << "Failed to start TCP lane session for " << group->key.host
                   << ":" << group->key.port << ". Error: " << initiation_error;
        handleLaneTerminal(group, lane, epoch, TransferStatusEnum::FAILED,
                           false);
    }
}

void TcpTransport::handleLaneTerminal(
    const std::shared_ptr<PeerConnectionGroup>& group,
    const std::shared_ptr<ConnectionLane>& lane, uint64_t epoch,
    TransferStatusEnum status, bool connection_clean) noexcept {
    std::optional<TerminalAction> action;
    std::shared_ptr<asio::ip::tcp::socket> socket_to_close;
    bool stale = false;
    {
        std::lock_guard<std::mutex> lock(group->mutex);
        if (lane->operation_epoch != epoch || lane->state != LaneState::BUSY ||
            !lane->current) {
            stale = true;
        } else {
            action.emplace(std::move(*lane->current), status, connection_clean);
            lane->current.reset();
            lane->session.reset();
            lane->state = LaneState::COMPLETING;
            if (!connection_clean || group->state != GroupState::OPEN)
                socket_to_close = std::move(lane->socket);
        }
    }

#ifdef MOONCAKE_TCP_TRANSPORT_TEST_HOOKS
    if (stale) {
        invokeLaneObserverHook(kLaneLateHandler, 0, 0, 0, false);
    }
#endif
    if (stale) return;

    if (status != TransferStatusEnum::COMPLETED) {
        recordWorkFailure(WorkFailureReason::SESSION_FAILED,
                          group->failure_counters);
    }

    // A dirty protocol stream must be closed before terminal Slice status is
    // visible to the caller.
    closeSocketNoThrow(socket_to_close);
    completeTerminalAction(std::move(*action));

    uint64_t pump_epoch = 0;
    std::shared_ptr<asio::ip::tcp::socket> shutdown_socket;
#ifdef MOONCAKE_TCP_TRANSPORT_TEST_HOOKS
    size_t queue_depth = 0;
    uint64_t queued_bytes = 0;
    size_t active_sockets = 0;
#endif
    {
        std::lock_guard<std::mutex> lock(group->mutex);
        if (group->state != GroupState::OPEN ||
            lane->operation_epoch != epoch) {
            lane->state = LaneState::CLOSING;
            shutdown_socket = std::move(lane->socket);
        } else if (connection_clean && lane->socket &&
                   lane->socket->is_open()) {
            lane->state = LaneState::IDLE;
        } else {
            lane->socket.reset();
            lane->state = LaneState::DISCONNECTED;
            // Keep this lane marked as tried in the current round. If another
            // lane is still usable, wait for the group cooldown before a new
            // round can retry disconnected lanes. If this was the last usable
            // lane and the round had previously connected successfully, start
            // a fresh round immediately so queued work is not mistaken for an
            // all-probes-failed round.
            const bool sibling_usable = hasUsableLaneLocked(*group);
            if (sibling_usable) {
                enterReconnectCooldownLocked(*group);
            } else if (!group->queue.empty() && group->probes_in_flight == 0 &&
                       group->connect_round_had_success) {
                beginConnectRoundLocked(*group);
            }
        }
        pump_epoch = requestGroupPumpLocked(*group);
#ifdef MOONCAKE_TCP_TRANSPORT_TEST_HOOKS
        queue_depth = group->queue.size();
        queued_bytes = group->queued_bytes;
        active_sockets = activeSocketCountLocked(*group);
#endif
    }
    closeSocketNoThrow(shutdown_socket);
#ifdef MOONCAKE_TCP_TRANSPORT_TEST_HOOKS
    invokeLaneObserverHook(kLaneTerminal, queue_depth, queued_bytes,
                           active_sockets, false);
#endif
    if (pump_epoch != 0) postGroupPump(group, pump_epoch);
}

void TcpTransport::completeTerminalAction(TerminalAction action) noexcept {
    auto continuation = std::move(action.work.continuation);
    try {
        if (action.status == TransferStatusEnum::COMPLETED)
            action.work.slice->markSuccess();
        else
            action.work.slice->markFailed();
    } catch (const std::exception& e) {
        LOG(ERROR) << "TCP Slice terminal completion threw: " << e.what();
    } catch (...) {
        LOG(ERROR) << "TCP Slice terminal completion threw";
    }

    if (continuation) {
        try {
            continuation();
        } catch (const std::exception& e) {
            LOG(ERROR) << "TCP Slice continuation threw: " << e.what();
        } catch (...) {
            LOG(ERROR) << "TCP Slice continuation threw";
        }
    }
}

uint64_t TcpTransport::recordWorkFailure(
    WorkFailureReason reason,
    const std::shared_ptr<FailureCounters>& counters) noexcept {
    if (!counters) return 0;

    std::atomic<uint64_t>* counter = nullptr;
    switch (reason) {
        case WorkFailureReason::QUEUE_FULL:
            counter = &counters->queue_full;
            break;
        case WorkFailureReason::QUEUE_TIMEOUT:
            counter = &counters->queue_timeout;
            break;
        case WorkFailureReason::RUNTIME_UNAVAILABLE:
            counter = &counters->runtime_unavailable;
            break;
        case WorkFailureReason::CONNECT_FAILED:
            counter = &counters->connect_failed;
            break;
        case WorkFailureReason::SESSION_FAILED:
            counter = &counters->session_failed;
            break;
        case WorkFailureReason::SHUTDOWN:
            counter = &counters->shutdown;
            break;
    }
    if (!counter) return 0;

    const uint64_t occurrence =
        counter->fetch_add(1, std::memory_order_relaxed) + 1;
    if (reason == WorkFailureReason::QUEUE_FULL &&
        shouldLogOccurrence(occurrence)) {
        LOG(WARNING) << "TCP lane queue-full rejection count: " << occurrence;
    }
#ifdef MOONCAKE_TCP_TRANSPORT_TEST_HOOKS
    invokeLaneFailureReasonHook(static_cast<int>(reason));
#endif
    return occurrence;
}

void TcpTransport::failWorkItem(
    TcpWorkItem work, WorkFailureReason reason,
    const std::shared_ptr<FailureCounters>& counters) noexcept {
    recordWorkFailure(reason, counters);
    completeTerminalAction(
        TerminalAction(std::move(work), TransferStatusEnum::FAILED, false));
}

void TcpTransport::failWorkItems(
    std::deque<TcpWorkItem> work, WorkFailureReason reason,
    const std::shared_ptr<FailureCounters>& counters) noexcept {
    while (!work.empty()) {
        auto item = std::move(work.front());
        work.pop_front();
        failWorkItem(std::move(item), reason, counters);
    }
}

void TcpTransport::closeSocketNoThrow(
    const std::shared_ptr<asio::ip::tcp::socket>& socket) noexcept {
    if (!socket) return;
    asio::error_code error;
    socket->cancel(error);
    socket->close(error);
}

void TcpTransport::shutdownConnectionLanes() {
    const auto state = lane_state_;
    std::vector<std::shared_ptr<PeerConnectionGroup>> groups;
    {
        std::lock_guard<std::mutex> state_lock(state->mutex);
        if (state->shutting_down) return;
        state->shutting_down = true;
        groups.reserve(state->groups.size());
        for (const auto& entry : state->groups) groups.push_back(entry.second);
    }

    for (const auto& group : groups) {
        std::deque<TcpWorkItem> accepted_queue;
        std::deque<TcpWorkItem> accepted_pending;
        {
            std::lock_guard<std::mutex> lock(group->mutex);
            group->state = GroupState::CLOSING;
            group->pump_scheduled = false;
            ++group->pump_epoch;
            accepted_queue.swap(group->queue);
            accepted_pending.swap(group->pending_admissions);
            clearQueuedBytesLocked(*group);
            ++group->retry_epoch;
            if (group->retry_epoch == 0) ++group->retry_epoch;
            ++group->admission_epoch;
            if (group->admission_epoch == 0) ++group->admission_epoch;
            for (const auto& lane : group->lanes) {
                ++lane->operation_epoch;
                if (lane->operation_epoch == 0) ++lane->operation_epoch;
                if (lane->state != LaneState::CLOSED)
                    lane->state = LaneState::CLOSING;
            }
        }
        failWorkItems(std::move(accepted_queue), WorkFailureReason::SHUTDOWN,
                      state->failure_counters);
        failWorkItems(std::move(accepted_pending), WorkFailureReason::SHUTDOWN,
                      state->failure_counters);
    }

    auto cancellation_posts = std::make_shared<LaneCancellationPostTracker>();
    if (context_ && running_) {
        for (const auto& group : groups) {
            cancellation_posts->add();
            try {
                asio::post(group->executor, [group, cancellation_posts] {
                    std::vector<std::shared_ptr<ClientSession>> sessions;
                    std::vector<std::shared_ptr<asio::ip::tcp::resolver>>
                        resolvers;
                    std::vector<std::shared_ptr<asio::ip::tcp::socket>> sockets;
                    std::shared_ptr<asio::steady_timer> retry_timer;
                    std::shared_ptr<asio::steady_timer> admission_timer;
                    {
                        std::lock_guard<std::mutex> lock(group->mutex);
                        retry_timer = group->retry_timer;
                        admission_timer = group->admission_timer;
                        for (const auto& lane : group->lanes) {
                            if (lane->session)
                                sessions.push_back(lane->session);
                            if (lane->resolver)
                                resolvers.push_back(lane->resolver);
                            if (lane->socket) sockets.push_back(lane->socket);
                        }
                    }
                    for (const auto& session : sessions)
                        if (session) session->cancel();
                    for (const auto& resolver : resolvers) {
                        if (!resolver) continue;
                        try {
                            resolver->cancel();
                        } catch (...) {
                        }
                    }
                    for (const auto& socket : sockets)
                        closeSocketNoThrow(socket);
                    if (retry_timer) {
                        asio::error_code timer_ec;
                        retry_timer->cancel(timer_ec);
                    }
                    cancelTimerNoThrow(admission_timer);
                    cancellation_posts->done();
                });
            } catch (...) {
                cancellation_posts->done();
            }
        }
        cancellation_posts->waitUntil(std::chrono::steady_clock::now() +
                                      kShutdownCancellationWait);
    }

    running_ = false;
    if (context_) context_->io_context.stop();
    if (thread_.joinable()) thread_.join();

    std::deque<TcpWorkItem> deferred;
    std::vector<std::shared_ptr<ClientSession>> sessions;
    std::vector<std::shared_ptr<asio::ip::tcp::resolver>> resolvers;
    std::vector<std::shared_ptr<asio::ip::tcp::socket>> sockets;
    std::vector<std::shared_ptr<asio::steady_timer>> retry_timers;
    std::vector<std::shared_ptr<asio::steady_timer>> admission_timers;

    for (const auto& group : groups) {
        {
            std::lock_guard<std::mutex> lock(group->mutex);
            if (group->retry_timer)
                retry_timers.push_back(std::move(group->retry_timer));
            if (group->admission_timer)
                admission_timers.push_back(std::move(group->admission_timer));
            for (const auto& lane : group->lanes) {
                if (lane->current) {
                    deferred.emplace_back(std::move(*lane->current));
                    lane->current.reset();
                }
                if (lane->session) sessions.push_back(std::move(lane->session));
                if (lane->resolver)
                    resolvers.push_back(std::move(lane->resolver));
                if (lane->socket) sockets.push_back(std::move(lane->socket));
                lane->connect_stage = LaneConnectStage::NONE;
                lane->state = LaneState::CLOSED;
            }
            group->probes_in_flight = 0;
            group->state = GroupState::CLOSED;
        }
#ifdef MOONCAKE_TCP_TRANSPORT_TEST_HOOKS
        invokeLaneObserverHook(kLaneShutdownClean, 0, 0, 0, false);
#endif
    }

    // No handler is running after join. Reset every Asio-owning field while
    // TcpContext and its execution_context are still alive, then publish
    // terminal failure for work that had been BUSY.
    for (const auto& session : sessions)
        if (session) session->cancel();
    for (const auto& resolver : resolvers) {
        if (!resolver) continue;
        try {
            resolver->cancel();
        } catch (...) {
        }
    }
    for (const auto& socket : sockets) closeSocketNoThrow(socket);
    for (const auto& timer : retry_timers) {
        cancelTimerNoThrow(timer);
    }
    for (const auto& timer : admission_timers) cancelTimerNoThrow(timer);
    sessions.clear();
    resolvers.clear();
    sockets.clear();
    retry_timers.clear();
    admission_timers.clear();

    failWorkItems(std::move(deferred), WorkFailureReason::SHUTDOWN,
                  state->failure_counters);

    const auto& counters = state->failure_counters;
    VLOG(1) << "TCP lane failure totals: queue_full="
            << counters->queue_full.load(std::memory_order_relaxed)
            << ", queue_timeout="
            << counters->queue_timeout.load(std::memory_order_relaxed)
            << ", connect_failed="
            << counters->connect_failed.load(std::memory_order_relaxed)
            << ", runtime_unavailable="
            << counters->runtime_unavailable.load(std::memory_order_relaxed)
            << ", session_failed="
            << counters->session_failed.load(std::memory_order_relaxed)
            << ", shutdown="
            << counters->shutdown.load(std::memory_order_relaxed);

    {
        std::lock_guard<std::mutex> state_lock(state->mutex);
        state->groups.clear();
        state->runtime.reset();
    }
    groups.clear();
    lane_runtime_.reset();
}

bool TcpTransport::validateAddress(uint64_t addr, uint64_t size) const {
    return validateTcpAddress(metadata_, addr, size);
}
