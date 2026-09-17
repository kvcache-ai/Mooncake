// Copyright 2026 KVCache.AI
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

#include "adaptive_congestion_control.h"

#include <algorithm>
#include <limits>
#include <thread>

namespace mooncake::adaptive_congestion_control {
namespace {

Config normalize(Config config) {
    config.min_window_bytes = std::max<uint64_t>(config.min_window_bytes, 1);
    config.max_window_bytes =
        std::max(config.max_window_bytes, config.min_window_bytes);
    config.target_drain_time_ns =
        std::max<uint64_t>(config.target_drain_time_ns, 1);
    config.high_pressure_epochs =
        std::max<uint32_t>(config.high_pressure_epochs, 1);
    config.low_pressure_epochs =
        std::max<uint32_t>(config.low_pressure_epochs, 1);
    config.hard_error_threshold =
        std::max<uint32_t>(config.hard_error_threshold, 1);
    config.cooldown_ns = std::max<uint64_t>(config.cooldown_ns, 1);
    config.probe_window_bytes = std::clamp(
        config.probe_window_bytes, uint64_t{1}, config.min_window_bytes);
    return config;
}

void addSaturating(std::atomic<uint64_t>& target, uint64_t value) {
    if (value == 0) return;
    uint64_t current = target.load(std::memory_order_relaxed);
    while (true) {
        const uint64_t next =
            value > std::numeric_limits<uint64_t>::max() - current
                ? std::numeric_limits<uint64_t>::max()
                : current + value;
        if (target.compare_exchange_weak(current, next,
                                         std::memory_order_relaxed,
                                         std::memory_order_relaxed)) {
            return;
        }
    }
}

struct AcquireResult {
    Decision decision;
    bool reserved;
    uint64_t probe_epoch;
};

uint64_t drainTimeNs(uint64_t backlog_bytes,
                     uint64_t delivery_rate_bytes_per_sec) {
    if (delivery_rate_bytes_per_sec == 0) return 0;
    const long double value =
        static_cast<long double>(backlog_bytes) * 1'000'000'000.0L /
        static_cast<long double>(delivery_rate_bytes_per_sec);
    return value > static_cast<long double>(
                       std::numeric_limits<uint64_t>::max())
               ? std::numeric_limits<uint64_t>::max()
               : static_cast<uint64_t>(value);
}

uint32_t addBounded(uint32_t current, uint64_t value) {
    const uint64_t sum = static_cast<uint64_t>(current) + value;
    return sum > std::numeric_limits<uint32_t>::max()
               ? std::numeric_limits<uint32_t>::max()
               : static_cast<uint32_t>(sum);
}

uint64_t addBounded(uint64_t current, uint64_t value) {
    return value > std::numeric_limits<uint64_t>::max() - current
               ? std::numeric_limits<uint64_t>::max()
               : current + value;
}

uint64_t nextEpoch(uint64_t current) {
    return current == std::numeric_limits<uint64_t>::max() ? 1 : current + 1;
}

}  // namespace

struct CoreAccess {
    static void releaseBytes(DomainState& state, uint64_t bytes) {
        uint64_t current =
            state.inflight_bytes_.load(std::memory_order_relaxed);
        while (true) {
            const uint64_t next = current > bytes ? current - bytes : 0;
            if (state.inflight_bytes_.compare_exchange_weak(
                    current, next, std::memory_order_release,
                    std::memory_order_relaxed)) {
                return;
            }
        }
    }

    static AcquireResult acquireOne(DomainState* state, uint32_t generation,
                                    uint64_t bytes) {
        if (state == nullptr) return {Decision::kAllow, false, false};
        if (state->config_.mode == Mode::kOff) {
            return {Decision::kAllow, false, false};
        }
        if (state->config_.mode == Mode::kObserve) {
            addSaturating(state->inflight_bytes_, bytes);
            if (generation ==
                    state->generation_.load(std::memory_order_acquire) &&
                state->state_.load(std::memory_order_acquire) ==
                    PathState::kProbing) {
                std::lock_guard<std::mutex> lock(state->probe_mutex_);
                if (generation ==
                        state->generation_.load(std::memory_order_acquire) &&
                    state->state_.load(std::memory_order_acquire) ==
                        PathState::kProbing &&
                    !state->probe_draining_ &&
                    state->active_probe_permits_ !=
                        std::numeric_limits<uint64_t>::max()) {
                    ++state->active_probe_permits_;
                    return {
                        Decision::kAllow, true,
                        state->probe_epoch_.load(std::memory_order_relaxed)};
                }
            }
            return {Decision::kAllow, true, 0};
        }
        if (generation != state->generation_.load(std::memory_order_acquire)) {
            return {Decision::kAvoid, false, false};
        }

        const PathState path_state =
            state->state_.load(std::memory_order_acquire);
        if (path_state == PathState::kQuarantined) {
            addSaturating(state->deferred_bytes_, bytes);
            return {Decision::kAvoid, false, false};
        }
        if (path_state == PathState::kProbing) {
            std::lock_guard<std::mutex> lock(state->probe_mutex_);
            if (generation !=
                    state->generation_.load(std::memory_order_acquire) ||
                state->state_.load(std::memory_order_acquire) !=
                    PathState::kProbing) {
                return {Decision::kAvoid, false, false};
            }
            if (state->probe_draining_) {
                addSaturating(state->deferred_bytes_, bytes);
                return {Decision::kDefer, false, false};
            }

            const uint64_t probe_epoch =
                state->probe_epoch_.load(std::memory_order_relaxed);
            const uint64_t limit =
                std::min(state->window_bytes_.load(std::memory_order_relaxed),
                         state->config_.probe_window_bytes);
            uint64_t current =
                state->inflight_bytes_.load(std::memory_order_relaxed);
            while (true) {
                if (current != 0 &&
                    (bytes > limit || current > limit - bytes)) {
                    addSaturating(state->deferred_bytes_, bytes);
                    return {Decision::kDefer, false, false};
                }
                if (state->inflight_bytes_.compare_exchange_weak(
                        current, current + bytes, std::memory_order_acquire,
                        std::memory_order_relaxed)) {
                    if (state->active_probe_permits_ ==
                        std::numeric_limits<uint64_t>::max()) {
                        releaseBytes(*state, bytes);
                        addSaturating(state->deferred_bytes_, bytes);
                        return {Decision::kDefer, false, false};
                    }
                    ++state->active_probe_permits_;
                    return {Decision::kAllow, true, probe_epoch};
                }
            }
        }

        uint64_t limit = state->window_bytes_.load(std::memory_order_relaxed);

        uint64_t current =
            state->inflight_bytes_.load(std::memory_order_relaxed);
        while (true) {
            // A slice is indivisible here. An empty domain may admit one
            // oversized slice; its full reservation blocks further admission.
            if (current != 0 && (bytes > limit || current > limit - bytes)) {
                addSaturating(state->deferred_bytes_, bytes);
                return {Decision::kDefer, false, false};
            }
            if (state->inflight_bytes_.compare_exchange_weak(
                    current, current + bytes, std::memory_order_acquire,
                    std::memory_order_relaxed)) {
                const PathState current_state =
                    state->state_.load(std::memory_order_acquire);
                const bool entered_probe = current_state == PathState::kProbing;
                if (current_state == PathState::kQuarantined || entered_probe) {
                    releaseBytes(*state, bytes);
                    addSaturating(state->deferred_bytes_, bytes);
                    return {Decision::kAvoid, false, 0};
                }
                return {Decision::kAllow, true, 0};
            }
        }
    }

    static void releaseProbePermit(DomainState* state, uint32_t generation,
                                   uint64_t probe_epoch) {
        if (state == nullptr || probe_epoch == 0) return;
        std::lock_guard<std::mutex> lock(state->probe_mutex_);
        if (state->generation_.load(std::memory_order_acquire) != generation ||
            state->probe_epoch_.load(std::memory_order_relaxed) !=
                probe_epoch ||
            state->active_probe_permits_ == 0) {
            return;
        }
        --state->active_probe_permits_;
    }

    static void addSignals(DomainState& state, uint32_t generation,
                           const Signals& signals,
                           uint64_t probe_success_epoch = 0,
                           uint64_t probe_failure_epoch = 0) {
        while (generation != 0) {
            const uint32_t active_generation =
                state.feedback_generation_.load(std::memory_order_acquire);
            if (active_generation == 0) {
                if (state.generation_.load(std::memory_order_acquire) !=
                    generation) {
                    return;
                }
                std::this_thread::yield();
                continue;
            }
            if (active_generation != generation) return;

            state.feedback_writers_.fetch_add(1, std::memory_order_acquire);
            if (state.feedback_generation_.load(std::memory_order_acquire) ==
                generation) {
                break;
            }
            state.feedback_writers_.fetch_sub(1, std::memory_order_release);
        }
        if (generation == 0) return;

        if (signals.delivery_rate_bytes_per_sec != 0) {
            state.backlog_bytes_.store(signals.backlog_bytes,
                                       std::memory_order_relaxed);
            state.delivery_rate_bytes_per_sec_.store(
                signals.delivery_rate_bytes_per_sec, std::memory_order_relaxed);
            state.has_rate_sample_.store(true, std::memory_order_relaxed);
        }
        addSaturating(state.completed_bytes_, signals.completed_bytes);
        addSaturating(state.successful_completions_,
                      signals.successful_completions);
        const PathState path_state =
            state.state_.load(std::memory_order_acquire);
        const uint64_t active_probe_epoch =
            state.probe_epoch_.load(std::memory_order_acquire);
        const bool current_probe_failure =
            probe_failure_epoch != 0 && path_state == PathState::kProbing &&
            probe_failure_epoch == active_probe_epoch;
        if (probe_failure_epoch == 0 || current_probe_failure) {
            addSaturating(state.receiver_pressure_, signals.receiver_pressure);
            addSaturating(state.route_timeouts_, signals.route_timeouts);
            addSaturating(state.hard_errors_, signals.hard_errors);
            addSaturating(state.fatal_failures_, signals.fatal_failures);
        }
        if (probe_success_epoch != 0 && path_state == PathState::kProbing &&
            probe_success_epoch == active_probe_epoch) {
            addSaturating(state.probe_successes_, 1);
        }
        if (current_probe_failure) {
            addSaturating(state.probe_failures_, 1);
        }
        if (signals.poller_stalled) {
            state.poller_stalled_.store(true, std::memory_order_relaxed);
        }
        state.feedback_writers_.fetch_sub(1, std::memory_order_release);
    }

    static void recordOutcome(DomainState* state, uint32_t generation,
                              uint64_t bytes, OutcomeClass outcome,
                              FailureScope scope, bool route,
                              uint64_t probe_epoch) {
        if (state == nullptr || state->config_.mode == Mode::kOff) return;

        Signals signals;
        uint64_t probe_success_epoch = 0;
        uint64_t probe_failure_epoch = 0;
        switch (outcome) {
            case OutcomeClass::kSuccess:
                signals.completed_bytes = bytes;
                signals.successful_completions = 1;
                probe_success_epoch = probe_epoch;
                break;
            case OutcomeClass::kCongestion:
                signals.receiver_pressure = 1;
                break;
            case OutcomeClass::kReceiverPressure:
                // RNR describes one receiver, not pressure on every route
                // sharing the local device.
                if (!route) return;
                signals.receiver_pressure = 1;
                break;
            case OutcomeClass::kRouteTimeout:
                if (route) {
                    signals.route_timeouts = 1;
                    probe_failure_epoch = probe_epoch;
                }
                break;
            case OutcomeClass::kFatal:
                if (route && scope == FailureScope::kCq) {
                    signals.fatal_failures = 1;
                    probe_failure_epoch = probe_epoch;
                } else if (route && (scope == FailureScope::kOperation ||
                                     scope == FailureScope::kQp ||
                                     scope == FailureScope::kRoute)) {
                    signals.hard_errors = 1;
                    probe_failure_epoch = probe_epoch;
                } else if (!route && (scope == FailureScope::kPort ||
                                      scope == FailureScope::kDevice)) {
                    signals.fatal_failures = 1;
                    probe_failure_epoch = probe_epoch;
                }
                break;
            case OutcomeClass::kLocalConfiguration:
            case OutcomeClass::kRemoteMetadata:
            case OutcomeClass::kDerivedFlush:
                return;
        }
        addSignals(*state, generation, signals, probe_success_epoch,
                   probe_failure_epoch);
    }

    static void clearSignals(DomainState& state) {
        state.backlog_bytes_.store(0, std::memory_order_relaxed);
        state.delivery_rate_bytes_per_sec_.store(0, std::memory_order_relaxed);
        state.has_rate_sample_.store(false, std::memory_order_relaxed);
        state.completed_bytes_.store(0, std::memory_order_relaxed);
        state.successful_completions_.store(0, std::memory_order_relaxed);
        state.receiver_pressure_.store(0, std::memory_order_relaxed);
        state.route_timeouts_.store(0, std::memory_order_relaxed);
        state.hard_errors_.store(0, std::memory_order_relaxed);
        state.fatal_failures_.store(0, std::memory_order_relaxed);
        state.probe_successes_.store(0, std::memory_order_relaxed);
        state.probe_failures_.store(0, std::memory_order_relaxed);
        state.poller_stalled_.store(false, std::memory_order_relaxed);
    }

    static bool pauseFeedback(DomainState& state, uint32_t generation) {
        while (generation != 0) {
            uint32_t expected = generation;
            if (state.feedback_generation_.compare_exchange_weak(
                    expected, 0, std::memory_order_acq_rel,
                    std::memory_order_acquire)) {
                while (state.feedback_writers_.load(
                           std::memory_order_acquire) != 0) {
                    std::this_thread::yield();
                }
                return true;
            }
            if (expected != 0 && expected != generation) return false;
            if (state.generation_.load(std::memory_order_acquire) !=
                generation) {
                return false;
            }
            std::this_thread::yield();
        }
        return false;
    }

    static void resumeFeedback(DomainState& state, uint32_t generation) {
        state.feedback_generation_.store(generation, std::memory_order_release);
    }
};

class FeedbackResumeGuard {
   public:
    FeedbackResumeGuard(DomainState& state, uint32_t generation)
        : state_(state), generation_(generation) {}

    ~FeedbackResumeGuard() { CoreAccess::resumeFeedback(state_, generation_); }

    FeedbackResumeGuard(const FeedbackResumeGuard&) = delete;
    FeedbackResumeGuard& operator=(const FeedbackResumeGuard&) = delete;

   private:
    DomainState& state_;
    uint32_t generation_;
};

DomainState::DomainState(Config config, uint32_t generation)
    : config_(normalize(config)),
      window_bytes_(config_.min_window_bytes),
      generation_(generation == 0 ? 1 : generation),
      feedback_generation_(generation == 0 ? 1 : generation) {}

Decision tryAcquire(const PathHandle& path, uint64_t bytes, Permit& permit) {
    bool expected = false;
    if (!permit.active_.compare_exchange_strong(expected, true,
                                                std::memory_order_acq_rel,
                                                std::memory_order_relaxed)) {
        return Decision::kDefer;
    }

    const AcquireResult device =
        CoreAccess::acquireOne(path.device, path.device_generation, bytes);
    if (device.decision != Decision::kAllow) {
        permit.active_.store(false, std::memory_order_release);
        return device.decision;
    }

    const AcquireResult route =
        CoreAccess::acquireOne(path.route, path.route_generation, bytes);
    if (route.decision != Decision::kAllow) {
        if (device.reserved) {
            CoreAccess::releaseProbePermit(path.device, path.device_generation,
                                           device.probe_epoch);
            CoreAccess::releaseBytes(*path.device, bytes);
        }
        permit.active_.store(false, std::memory_order_release);
        return route.decision;
    }

    const bool device_current =
        path.device == nullptr || path.device->config_.mode != Mode::kEnforce ||
        path.device_generation ==
            path.device->generation_.load(std::memory_order_acquire);
    const bool route_current =
        path.route == nullptr || path.route->config_.mode != Mode::kEnforce ||
        path.route_generation ==
            path.route->generation_.load(std::memory_order_acquire);
    if (!device_current || !route_current) {
        if (device.reserved) {
            CoreAccess::releaseProbePermit(path.device, path.device_generation,
                                           device.probe_epoch);
            CoreAccess::releaseBytes(*path.device, bytes);
        }
        if (route.reserved) {
            CoreAccess::releaseProbePermit(path.route, path.route_generation,
                                           route.probe_epoch);
            CoreAccess::releaseBytes(*path.route, bytes);
        }
        permit.active_.store(false, std::memory_order_release);
        return Decision::kAvoid;
    }

    permit.device_ = path.device;
    permit.route_ = path.route;
    permit.bytes_ = bytes;
    permit.device_generation_ = path.device_generation;
    permit.route_generation_ = path.route_generation;
    permit.device_reserved_ = device.reserved;
    permit.route_reserved_ = route.reserved;
    permit.device_probe_epoch_ = device.probe_epoch;
    permit.route_probe_epoch_ = route.probe_epoch;
    return Decision::kAllow;
}

bool complete(Permit& permit, OutcomeClass outcome, FailureScope scope) {
    bool expected = true;
    if (!permit.active_.compare_exchange_strong(expected, false,
                                                std::memory_order_acq_rel,
                                                std::memory_order_relaxed)) {
        return false;
    }

    CoreAccess::recordOutcome(permit.device_, permit.device_generation_,
                              permit.bytes_, outcome, scope, false,
                              permit.device_probe_epoch_);
    CoreAccess::recordOutcome(permit.route_, permit.route_generation_,
                              permit.bytes_, outcome, scope, true,
                              permit.route_probe_epoch_);
    CoreAccess::releaseProbePermit(permit.device_, permit.device_generation_,
                                   permit.device_probe_epoch_);
    CoreAccess::releaseProbePermit(permit.route_, permit.route_generation_,
                                   permit.route_probe_epoch_);
    if (permit.device_reserved_)
        CoreAccess::releaseBytes(*permit.device_, permit.bytes_);
    if (permit.route_reserved_)
        CoreAccess::releaseBytes(*permit.route_, permit.bytes_);
    return true;
}

Permit::Permit(Permit&& other) noexcept { moveFrom(other); }

Permit& Permit::operator=(Permit&& other) noexcept {
    if (this == &other) return *this;
    complete(*this, OutcomeClass::kDerivedFlush, FailureScope::kOperation);
    moveFrom(other);
    return *this;
}

void Permit::moveFrom(Permit& other) noexcept {
    const bool active =
        other.active_.exchange(false, std::memory_order_acq_rel);
    device_ = other.device_;
    route_ = other.route_;
    bytes_ = other.bytes_;
    device_generation_ = other.device_generation_;
    route_generation_ = other.route_generation_;
    device_reserved_ = other.device_reserved_;
    route_reserved_ = other.route_reserved_;
    device_probe_epoch_ = other.device_probe_epoch_;
    route_probe_epoch_ = other.route_probe_epoch_;
    active_.store(active, std::memory_order_release);

    other.device_ = nullptr;
    other.route_ = nullptr;
    other.bytes_ = 0;
    other.device_reserved_ = false;
    other.route_reserved_ = false;
    other.device_probe_epoch_ = 0;
    other.route_probe_epoch_ = 0;
}

void recordSignals(DomainState& state, uint32_t generation,
                   const Signals& signals) {
    CoreAccess::addSignals(state, generation, signals);
}

void controlTick(DomainState& state, uint64_t now_ns) {
    if (state.config_.mode == Mode::kOff) return;

    const uint32_t generation =
        state.generation_.load(std::memory_order_acquire);
    if (!CoreAccess::pauseFeedback(state, generation)) return;
    FeedbackResumeGuard feedback_guard(state, generation);

    const uint64_t backlog =
        state.backlog_bytes_.exchange(0, std::memory_order_relaxed);
    const uint64_t rate = state.delivery_rate_bytes_per_sec_.exchange(
        0, std::memory_order_relaxed);
    const bool has_rate =
        state.has_rate_sample_.exchange(false, std::memory_order_relaxed);
    state.completed_bytes_.exchange(0, std::memory_order_relaxed);
    const uint64_t successes =
        state.successful_completions_.exchange(0, std::memory_order_relaxed);
    const uint64_t receiver_pressure =
        state.receiver_pressure_.exchange(0, std::memory_order_relaxed);
    uint64_t timeouts =
        state.route_timeouts_.exchange(0, std::memory_order_relaxed);
    const uint64_t hard_errors =
        state.hard_errors_.exchange(0, std::memory_order_relaxed);
    const uint64_t fatal_failures =
        state.fatal_failures_.exchange(0, std::memory_order_relaxed);
    const uint64_t probe_successes =
        state.probe_successes_.exchange(0, std::memory_order_relaxed);
    const uint64_t probe_failures =
        state.probe_failures_.exchange(0, std::memory_order_relaxed);
    if (state.poller_stalled_.exchange(false, std::memory_order_relaxed)) {
        timeouts = 0;
    }
    PathState path_state = state.state_.load(std::memory_order_relaxed);
    if (path_state == PathState::kQuarantined) {
        if (now_ns >= state.quarantine_until_ns_) {
            std::lock_guard<std::mutex> lock(state.probe_mutex_);
            state.window_bytes_.store(state.config_.probe_window_bytes,
                                      std::memory_order_relaxed);
            const uint64_t current_epoch =
                state.probe_epoch_.load(std::memory_order_relaxed);
            state.active_probe_permits_ = 0;
            state.probe_draining_ = false;
            state.probe_epoch_.store(nextEpoch(current_epoch),
                                     std::memory_order_release);
            state.state_.store(PathState::kProbing, std::memory_order_release);
        }
        return;
    }

    if (path_state == PathState::kProbing) {
        std::lock_guard<std::mutex> lock(state.probe_mutex_);
        if (probe_failures != 0 || fatal_failures != 0 || hard_errors != 0 ||
            timeouts != 0) {
            state.quarantine_until_ns_ =
                addBounded(now_ns, state.config_.cooldown_ns);
            state.state_.store(PathState::kQuarantined,
                               std::memory_order_release);
            state.probe_draining_ = false;
        } else {
            if (probe_successes != 0) {
                state.probe_draining_ = true;
            }
            if (!state.probe_draining_ || state.active_probe_permits_ != 0) {
                return;
            }
            state.hard_error_streak_ = 0;
            state.high_pressure_streak_ = 0;
            state.low_pressure_streak_ = 0;
            state.window_bytes_.store(state.config_.min_window_bytes,
                                      std::memory_order_relaxed);
            state.state_.store(PathState::kHealthy, std::memory_order_release);
            state.probe_draining_ = false;
        }
        return;
    }

    if (fatal_failures != 0) {
        state.quarantine_until_ns_ =
            addBounded(now_ns, state.config_.cooldown_ns);
        state.high_pressure_streak_ = 0;
        state.low_pressure_streak_ = 0;
        state.state_.store(PathState::kQuarantined, std::memory_order_release);
        return;
    }

    const uint64_t failures = addBounded(hard_errors, timeouts);
    if (failures != 0) {
        state.hard_error_streak_ =
            addBounded(state.hard_error_streak_, failures);
        state.high_pressure_streak_ = 0;
        state.low_pressure_streak_ = 0;
        if (state.hard_error_streak_ >= state.config_.hard_error_threshold) {
            state.quarantine_until_ns_ =
                addBounded(now_ns, state.config_.cooldown_ns);
            state.state_.store(PathState::kQuarantined,
                               std::memory_order_release);
        } else {
            state.state_.store(PathState::kSuspect, std::memory_order_release);
        }
        return;
    }

    if (successes != 0) state.hard_error_streak_ = 0;
    const uint64_t drain_ns = has_rate ? drainTimeNs(backlog, rate) : 0;
    const bool high_pressure =
        receiver_pressure != 0 ||
        (has_rate && drain_ns > state.config_.target_drain_time_ns);
    const bool low_pressure =
        has_rate && drain_ns < state.config_.target_drain_time_ns / 2;

    if (high_pressure) {
        state.low_pressure_streak_ = 0;
        state.high_pressure_streak_ =
            addBounded(state.high_pressure_streak_, 1);
        if (state.high_pressure_streak_ >= state.config_.high_pressure_epochs) {
            const uint64_t current =
                state.window_bytes_.load(std::memory_order_relaxed);
            state.window_bytes_.store(
                std::max(state.config_.min_window_bytes, current / 2),
                std::memory_order_relaxed);
            state.high_pressure_streak_ = 0;
            state.state_.store(PathState::kCongested,
                               std::memory_order_release);
        }
        return;
    }

    state.high_pressure_streak_ = 0;
    if (successes != 0 && low_pressure) {
        state.low_pressure_streak_ = addBounded(state.low_pressure_streak_, 1);
        if (state.low_pressure_streak_ >= state.config_.low_pressure_epochs) {
            const uint64_t current =
                state.window_bytes_.load(std::memory_order_relaxed);
            const uint64_t increment =
                std::max<uint64_t>(1, state.config_.min_window_bytes / 4);
            const uint64_t next =
                current > state.config_.max_window_bytes - increment
                    ? state.config_.max_window_bytes
                    : current + increment;
            state.window_bytes_.store(next, std::memory_order_relaxed);
            state.low_pressure_streak_ = 0;
            state.state_.store(PathState::kHealthy, std::memory_order_release);
        }
    } else {
        state.low_pressure_streak_ = 0;
        if (successes != 0 && path_state == PathState::kSuspect) {
            state.state_.store(PathState::kHealthy, std::memory_order_release);
        }
    }
}

void resetGeneration(DomainState& state, uint32_t generation) {
    const uint32_t next_generation = generation == 0 ? 1 : generation;
    const uint32_t current_generation =
        state.generation_.load(std::memory_order_acquire);
    if (!CoreAccess::pauseFeedback(state, current_generation)) return;

    CoreAccess::clearSignals(state);
    std::lock_guard<std::mutex> lock(state.probe_mutex_);
    state.high_pressure_streak_ = 0;
    state.low_pressure_streak_ = 0;
    state.hard_error_streak_ = 0;
    state.quarantine_until_ns_ = 0;
    state.probe_epoch_.store(0, std::memory_order_relaxed);
    state.active_probe_permits_ = 0;
    state.probe_draining_ = false;
    state.deferred_bytes_.store(0, std::memory_order_relaxed);
    state.window_bytes_.store(state.config_.min_window_bytes,
                              std::memory_order_relaxed);
    state.state_.store(PathState::kHealthy, std::memory_order_relaxed);
    state.generation_.store(next_generation, std::memory_order_release);
    CoreAccess::resumeFeedback(state, next_generation);
}

uint32_t generation(const DomainState& state) {
    return state.generation_.load(std::memory_order_acquire);
}

Snapshot snapshot(const DomainState& state) {
    return {
        state.config_.mode,
        state.state_.load(std::memory_order_acquire),
        state.window_bytes_.load(std::memory_order_relaxed),
        state.inflight_bytes_.load(std::memory_order_relaxed),
        state.deferred_bytes_.load(std::memory_order_relaxed),
        state.generation_.load(std::memory_order_acquire),
    };
}

}  // namespace mooncake::adaptive_congestion_control
