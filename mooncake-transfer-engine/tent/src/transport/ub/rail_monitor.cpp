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

#include "tent/transport/ub/rail_monitor.h"

#include <algorithm>
#include <limits>
#include <unordered_map>

namespace mooncake::tent::ub {

namespace {

double updateEwma(double current, double sample, double alpha) {
    if (current < 0.0) return sample;
    return alpha * sample + (1.0 - alpha) * current;
}

}  // namespace

RailMonitor::RailMonitor(RailMonitorConfig config) {
    if (config.valid()) config_ = config;
}

bool RailMonitor::configure(const RailMonitorConfig& config) {
    if (!config.valid()) return false;
    std::lock_guard<std::mutex> lock(mutex_);
    config_ = config;
    return true;
}

RailMonitorConfig RailMonitor::config() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return config_;
}

bool RailMonitor::registerPath(const UbPostPath& path) {
    if (!path.valid()) return false;
    std::lock_guard<std::mutex> lock(mutex_);
    getOrCreateLocked(path);
    return true;
}

bool RailMonitor::available(const UbPostPath& path, uint64_t now_ns) {
    if (!path.valid()) return false;
    now_ns = normalizedNow(now_ns);
    std::lock_guard<std::mutex> lock(mutex_);
    auto& state = getOrCreateLocked(path);
    now_ns = observeTimeLocked(state, now_ns);
    refreshCooldownLocked(state, now_ns);
    return !state.stats.paused;
}

void RailMonitor::recordSuccess(const UbPostPath& path, uint64_t bytes,
                                uint64_t latency_ns, uint64_t now_ns) {
    if (!path.valid()) return;
    now_ns = normalizedNow(now_ns);
    std::lock_guard<std::mutex> lock(mutex_);
    auto& state = getOrCreateLocked(path);
    const uint64_t observed_ns = observeTimeLocked(state, now_ns);
    refreshCooldownLocked(state, observed_ns);

    ++state.stats.successful_completions;
    if (std::numeric_limits<uint64_t>::max() - state.stats.completed_bytes <
        bytes) {
        state.stats.completed_bytes = std::numeric_limits<uint64_t>::max();
    } else {
        state.stats.completed_bytes += bytes;
    }
    state.stats.last_success_ns = std::max(state.stats.last_success_ns, now_ns);

    // Zero-byte or zero-latency completions are valid completions but are not
    // usable bandwidth observations.
    if (bytes == 0 || latency_ns == 0) return;
    const double bandwidth = static_cast<double>(bytes) * 1'000'000'000.0 /
                             static_cast<double>(latency_ns);
    state.stats.ewma_bandwidth_bytes_per_second =
        updateEwma(state.stats.ewma_bandwidth_bytes_per_second, bandwidth,
                   config_.ewma_alpha);
    state.stats.ewma_latency_ns =
        updateEwma(state.stats.ewma_latency_ns, static_cast<double>(latency_ns),
                   config_.ewma_alpha);
}

void RailMonitor::recordError(const UbPostPath& path, uint64_t now_ns) {
    if (!path.valid()) return;
    now_ns = normalizedNow(now_ns);
    std::lock_guard<std::mutex> lock(mutex_);
    recordFailureLocked(path, now_ns, false);
}

void RailMonitor::recordTimeout(const UbPostPath& path, uint64_t now_ns) {
    if (!path.valid()) return;
    now_ns = normalizedNow(now_ns);
    std::lock_guard<std::mutex> lock(mutex_);
    recordFailureLocked(path, now_ns, true);
}

bool RailMonitor::recordEndpointRebuild(const UbPostPath& path,
                                        uint64_t now_ns) {
    if (!path.valid()) return false;
    now_ns = normalizedNow(now_ns);
    std::lock_guard<std::mutex> lock(mutex_);
    auto& state = getOrCreateLocked(path);
    now_ns = observeTimeLocked(state, now_ns);
    refreshCooldownLocked(state, now_ns);
    if (path.endpoint_generation <= state.recorded_rebuild_generation) {
        return false;
    }
    state.recorded_rebuild_generation = path.endpoint_generation;
    ++state.stats.endpoint_rebuilds;
    return true;
}

RailStats RailMonitor::stats(const UbPostPath& path, uint64_t now_ns) {
    RailStats result;
    result.key = UbRailKey::fromPath(path);
    if (!path.valid()) return result;
    now_ns = normalizedNow(now_ns);
    std::lock_guard<std::mutex> lock(mutex_);
    auto& state = getOrCreateLocked(path);
    now_ns = observeTimeLocked(state, now_ns);
    refreshCooldownLocked(state, now_ns);
    pruneErrorsLocked(state, now_ns);
    state.stats.errors_in_window = static_cast<uint32_t>(std::min<size_t>(
        state.recent_errors.size(), std::numeric_limits<uint32_t>::max()));
    return state.stats;
}

std::vector<RailStats> RailMonitor::allStats(uint64_t now_ns) {
    now_ns = normalizedNow(now_ns);
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<RailStats> result;
    result.reserve(rails_.size());
    for (auto& [key, state] : rails_) {
        const uint64_t observed_ns = observeTimeLocked(state, now_ns);
        refreshCooldownLocked(state, observed_ns);
        pruneErrorsLocked(state, observed_ns);
        state.stats.errors_in_window = static_cast<uint32_t>(std::min<size_t>(
            state.recent_errors.size(), std::numeric_limits<uint32_t>::max()));
        result.push_back(state.stats);
    }
    return result;
}

double RailMonitor::aggregateBandwidth(uint64_t now_ns) {
    now_ns = normalizedNow(now_ns);
    std::lock_guard<std::mutex> lock(mutex_);

    bool has_sample = false;
    std::unordered_map<Topology::NicID, double> best_per_device;
    for (auto& [key, state] : rails_) {
        const uint64_t observed_ns = observeTimeLocked(state, now_ns);
        refreshCooldownLocked(state, observed_ns);
        const double bandwidth = state.stats.ewma_bandwidth_bytes_per_second;
        if (bandwidth < 0.0) continue;
        has_sample = true;
        if (state.stats.paused) continue;
        auto [it, inserted] =
            best_per_device.emplace(key.local_topology_id, bandwidth);
        if (!inserted) it->second = std::max(it->second, bandwidth);
    }

    if (!has_sample) return -1.0;
    double aggregate = 0.0;
    for (const auto& entry : best_per_device) {
        aggregate += entry.second;
    }
    return aggregate;
}

size_t RailMonitor::pathCount() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return rails_.size();
}

uint64_t RailMonitor::normalizedNow(uint64_t now_ns) {
    return now_ns == 0 ? steadyNowNs() : now_ns;
}

uint64_t RailMonitor::deadlineAfter(uint64_t now_ns, uint64_t duration_ns) {
    if (std::numeric_limits<uint64_t>::max() - now_ns < duration_ns) {
        return std::numeric_limits<uint64_t>::max();
    }
    return now_ns + duration_ns;
}

uint64_t RailMonitor::observeTimeLocked(RailState& state, uint64_t event_ns) {
    state.observed_through_ns = std::max(state.observed_through_ns, event_ns);
    return state.observed_through_ns;
}

RailMonitor::RailState& RailMonitor::getOrCreateLocked(const UbPostPath& path) {
    const auto key = UbRailKey::fromPath(path);
    auto [it, inserted] = rails_.try_emplace(key);
    if (inserted) it->second.stats.key = key;
    it->second.stats.latest_endpoint_generation = std::max(
        it->second.stats.latest_endpoint_generation, path.endpoint_generation);
    return it->second;
}

void RailMonitor::insertErrorLocked(RailState& state, uint64_t event_ns) {
    const auto position = std::upper_bound(state.recent_errors.begin(),
                                           state.recent_errors.end(), event_ns);
    state.recent_errors.insert(position, event_ns);
}

void RailMonitor::pruneErrorsLocked(RailState& state, uint64_t now_ns) {
    while (!state.recent_errors.empty()) {
        const uint64_t error_ns = state.recent_errors.front();
        if (error_ns > now_ns || now_ns - error_ns < config_.error_window_ns) {
            break;
        }
        state.recent_errors.pop_front();
    }
}

void RailMonitor::refreshCooldownLocked(RailState& state, uint64_t now_ns) {
    pruneErrorsLocked(state, now_ns);
    if (!state.stats.paused || now_ns < state.stats.cooldown_until_ns) return;
    state.ignore_errors_through_ns =
        std::max(state.ignore_errors_through_ns, state.stats.cooldown_until_ns);
    state.stats.paused = false;
    state.stats.cooldown_until_ns = 0;
    state.recent_errors.clear();
    state.stats.errors_in_window = 0;
    ++state.stats.recoveries;
}

void RailMonitor::recordFailureLocked(const UbPostPath& path, uint64_t now_ns,
                                      bool timeout) {
    auto& state = getOrCreateLocked(path);
    const uint64_t event_ns = now_ns;
    const uint64_t observed_ns = observeTimeLocked(state, event_ns);
    refreshCooldownLocked(state, observed_ns);
    ++state.stats.completion_errors;
    if (timeout) ++state.stats.timeouts;
    state.stats.last_error_ns = std::max(state.stats.last_error_ns, event_ns);

    if (event_ns <= state.ignore_errors_through_ns) return;
    insertErrorLocked(state, event_ns);
    pruneErrorsLocked(state, observed_ns);
    state.stats.errors_in_window = static_cast<uint32_t>(std::min<size_t>(
        state.recent_errors.size(), std::numeric_limits<uint32_t>::max()));

    if (state.recent_errors.size() < config_.error_threshold) return;
    const uint64_t cooldown_until =
        deadlineAfter(state.recent_errors.back(), config_.cooldown_ns);
    if (cooldown_until <= observed_ns) {
        // This entire failure burst and its cooldown are already in the past
        // relative to the watermark. Treat it as a completed health epoch
        // instead of resurrecting an expired pause because an event was late.
        state.ignore_errors_through_ns =
            std::max(state.ignore_errors_through_ns, cooldown_until);
        state.recent_errors.clear();
        state.stats.errors_in_window = 0;
        return;
    }
    if (!state.stats.paused) {
        state.stats.paused = true;
        state.stats.pause_started_ns = state.recent_errors.back();
        ++state.stats.pauses;
    }
    state.stats.cooldown_until_ns =
        std::max(state.stats.cooldown_until_ns, cooldown_until);
}

}  // namespace mooncake::tent::ub
