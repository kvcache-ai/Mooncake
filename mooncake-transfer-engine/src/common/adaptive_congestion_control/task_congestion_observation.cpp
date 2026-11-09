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

#include "task_congestion_observation.h"

#include <utility>

namespace mooncake::adaptive_congestion_control {

TaskCongestionObservation::TaskCongestionObservation(
    size_t submitted_slice_count)
    : slice_count_(submitted_slice_count) {}

bool TaskCongestionObservation::defer(size_t slice_id,
                                      const TaskCongestionEvidence& evidence) {
    return record(slice_id, TaskCongestionState::kCongested, evidence);
}

bool TaskCongestionObservation::avoid(size_t slice_id,
                                      const TaskCongestionEvidence& evidence) {
    return record(slice_id, TaskCongestionState::kLongUnavailable, evidence);
}

bool TaskCongestionObservation::record(size_t slice_id,
                                       TaskCongestionState state,
                                       const TaskCongestionEvidence& evidence) {
    return recordOwned(slice_id, state, evidence, std::nullopt);
}

bool TaskCongestionObservation::defer(size_t slice_id,
                                      TaskCongestionEvidence&& evidence,
                                      size_t submitted_slice_count) {
    return recordOwned(slice_id, TaskCongestionState::kCongested,
                       std::move(evidence), submitted_slice_count);
}

bool TaskCongestionObservation::avoid(size_t slice_id,
                                      TaskCongestionEvidence&& evidence,
                                      size_t submitted_slice_count) {
    return recordOwned(slice_id, TaskCongestionState::kLongUnavailable,
                       std::move(evidence), submitted_slice_count);
}

bool TaskCongestionObservation::recordOwned(
    size_t slice_id, TaskCongestionState state, TaskCongestionEvidence evidence,
    std::optional<size_t> submitted_slice_count) {
    std::lock_guard lock(mutex_);
    if (submitted_slice_count && *submitted_slice_count > slice_count_) {
        if (!slices_.empty()) slices_.resize(*submitted_slice_count);
        slice_count_ = *submitted_slice_count;
    }
    if (slice_id >= slice_count_) {
        invalid_observation_ = true;
        return false;
    }
    if (slices_.empty()) slices_.resize(slice_count_);
    auto& slice = slices_[slice_id];
    if (slice.state == TaskCongestionState::kCongested) --congested_count_;
    if (slice.state == TaskCongestionState::kLongUnavailable)
        --unavailable_count_;
    slice.state = state;
    slice.evidence = std::move(evidence);
    if (state == TaskCongestionState::kCongested) ++congested_count_;
    if (state == TaskCongestionState::kLongUnavailable) ++unavailable_count_;
    slice.observed = true;
    return true;
}

bool TaskCongestionObservation::resolve(size_t slice_id) {
    std::lock_guard lock(mutex_);
    if (slice_id >= slice_count_) {
        invalid_observation_ = true;
        return false;
    }
    if (slices_.empty()) return true;
    auto& slice = slices_[slice_id];
    if (slice.state == TaskCongestionState::kNormal) return true;
    if (slice.state == TaskCongestionState::kCongested) --congested_count_;
    if (slice.state == TaskCongestionState::kLongUnavailable)
        --unavailable_count_;
    slice.state = TaskCongestionState::kNormal;
    // Retain evidence for historical detail after recovery.
    return true;
}

TaskCongestionState TaskCongestionObservation::stateLocked() const {
    if (invalid_observation_) return TaskCongestionState::kUnknown;
    if (unavailable_count_) return TaskCongestionState::kLongUnavailable;
    if (congested_count_) return TaskCongestionState::kCongested;
    return TaskCongestionState::kNormal;
}

TaskCongestionState TaskCongestionObservation::state() const {
    std::lock_guard lock(mutex_);
    return stateLocked();
}

TaskCongestionDetail TaskCongestionObservation::detail() const {
    std::lock_guard lock(mutex_);
    TaskCongestionDetail detail;
    detail.state = stateLocked();
    detail.attempt_kind = TaskCongestionAttemptKind::kRdma;
    detail.attempt_id = {true, 1};

    const SliceRecord* selected = nullptr;
    size_t selected_id = 0;
    bool selected_active = false;
    for (size_t slice_id = 0; slice_id < slices_.size(); ++slice_id) {
        const auto& slice = slices_[slice_id];
        if (!slice.observed) continue;
        const bool active = slice.state != TaskCongestionState::kNormal &&
                            slice.state == detail.state;
        if (selected_active && !active) continue;
        const auto& timestamp = slice.evidence.observed_at_ns;
        if (!selected || (active && !selected_active) ||
            (timestamp.observed &&
             (!selected->evidence.observed_at_ns.observed ||
              timestamp.value >= selected->evidence.observed_at_ns.value)) ||
            (!timestamp.observed &&
             !selected->evidence.observed_at_ns.observed)) {
            selected = &slice;
            selected_id = slice_id;
            selected_active = active;
        }
    }
    if (!selected) return detail;

    detail.reason = selected->evidence.reason;
    detail.slice_id = {true, static_cast<uint64_t>(selected_id)};
    detail.resolved = selected->state == TaskCongestionState::kNormal;
    detail.failure_scope = selected->evidence.failure_scope;
    if (const auto path = selected->evidence.effectivePath())
        detail.affected_path = {true, std::string(*path)};
    detail.observed_at_ns = selected->evidence.observed_at_ns;
    detail.controller_mode = selected->evidence.controller_mode;
    detail.controller_generation = selected->evidence.controller_generation;
    detail.window_bytes = selected->evidence.window_bytes;
    detail.inflight_bytes = selected->evidence.inflight_bytes;
    detail.retry_after_ns = selected->evidence.retry_after_ns;
    return detail;
}

}  // namespace mooncake::adaptive_congestion_control
