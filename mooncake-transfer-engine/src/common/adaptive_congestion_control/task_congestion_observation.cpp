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

namespace mooncake::adaptive_congestion_control {

TaskCongestionObservation::TaskCongestionObservation(
    size_t submitted_slice_count, uint64_t attempt_id)
    : slice_count_(submitted_slice_count), attempt_id_(attempt_id) {}

bool TaskCongestionObservation::currentAttempt(uint64_t attempt_id) const {
    return attempt_id == attempt_id_;
}

bool TaskCongestionObservation::beginAttempt(uint64_t attempt_id,
                                             size_t submitted_slice_count) {
    std::lock_guard lock(mutex_);
    if (attempt_id <= attempt_id_) return false;
    attempt_id_ = attempt_id;
    slice_count_ = submitted_slice_count;
    std::vector<SliceRecord>{}.swap(slices_);
    congested_count_ = 0;
    unavailable_count_ = 0;
    terminal_failure_ = false;
    invalid_observation_ = false;
    if (last_event_) last_event_->resolved = true;
    return true;
}

bool TaskCongestionObservation::extendAttempt(uint64_t attempt_id,
                                              size_t submitted_slice_count) {
    std::lock_guard lock(mutex_);
    if (!currentAttempt(attempt_id) || submitted_slice_count < slice_count_)
        return false;
    if (!slices_.empty()) slices_.resize(submitted_slice_count);
    slice_count_ = submitted_slice_count;
    return true;
}

bool TaskCongestionObservation::defer(size_t slice_id, uint64_t attempt_id,
                                      const TaskCongestionEvidence& evidence) {
    return record(slice_id, attempt_id, TaskCongestionState::kCongested,
                  evidence);
}

bool TaskCongestionObservation::avoid(size_t slice_id, uint64_t attempt_id,
                                      const TaskCongestionEvidence& evidence) {
    return record(slice_id, attempt_id, TaskCongestionState::kLongUnavailable,
                  evidence);
}

bool TaskCongestionObservation::record(size_t slice_id, uint64_t attempt_id,
                                       TaskCongestionState state,
                                       const TaskCongestionEvidence& evidence) {
    std::lock_guard lock(mutex_);
    if (!currentAttempt(attempt_id)) return false;
    if (slice_id >= slice_count_) {
        invalid_observation_ = true;
        return false;
    }
    if (slices_.empty()) slices_.resize(slice_count_);
    auto& slice = slices_[slice_id];
    if (slice.state == state && slice.evidence == evidence) return true;
    if (slice.state == TaskCongestionState::kCongested) --congested_count_;
    if (slice.state == TaskCongestionState::kLongUnavailable)
        --unavailable_count_;
    slice.state = state;
    slice.evidence = evidence;
    if (state == TaskCongestionState::kCongested) ++congested_count_;
    if (state == TaskCongestionState::kLongUnavailable) ++unavailable_count_;
    last_event_ = EventRecord{slice_id, evidence, false};
    return true;
}

bool TaskCongestionObservation::resolve(size_t slice_id, uint64_t attempt_id) {
    std::lock_guard lock(mutex_);
    if (!currentAttempt(attempt_id)) return false;
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
    slice.evidence = TaskCongestionEvidence{};
    if (last_event_ && last_event_->slice_id == slice_id)
        last_event_->resolved = true;
    return true;
}

bool TaskCongestionObservation::terminalFailure(uint64_t attempt_id) {
    std::lock_guard lock(mutex_);
    if (!currentAttempt(attempt_id)) return false;
    terminal_failure_ = true;
    return true;
}

TaskCongestionState TaskCongestionObservation::stateLocked() const {
    if (invalid_observation_) return TaskCongestionState::kUnknown;
    if (unavailable_count_) return TaskCongestionState::kLongUnavailable;
    if (congested_count_) return TaskCongestionState::kCongested;
    if (terminal_failure_) return TaskCongestionState::kUnknown;
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
    detail.attempt_id = {true, attempt_id_};

    std::optional<EventRecord> selected;
    if (detail.state == TaskCongestionState::kCongested ||
        detail.state == TaskCongestionState::kLongUnavailable) {
        for (size_t slice_id = 0; slice_id < slices_.size(); ++slice_id) {
            const auto& slice = slices_[slice_id];
            if (slice.state != detail.state) continue;
            const auto& observed_at = slice.evidence.observed_at_ns;
            if (!selected ||
                (observed_at &&
                 (!selected->evidence.observed_at_ns ||
                  *observed_at >= *selected->evidence.observed_at_ns))) {
                selected = EventRecord{slice_id, slice.evidence, false};
            }
        }
    }
    if (!selected) selected = last_event_;
    if (!selected) return detail;

    if (selected->evidence.reason)
        detail.reason = {true, *selected->evidence.reason};
    detail.slice_id = {true, static_cast<uint64_t>(selected->slice_id)};
    detail.resolved = selected->resolved;
    if (selected->evidence.failure_scope)
        detail.failure_scope = {true, *selected->evidence.failure_scope};
    if (selected->evidence.path)
        detail.affected_path = {true, *selected->evidence.path};
    if (selected->evidence.observed_at_ns)
        detail.observed_at_ns = {true, *selected->evidence.observed_at_ns};
    if (selected->evidence.controller_mode)
        detail.controller_mode = {true, *selected->evidence.controller_mode};
    if (selected->evidence.controller_generation)
        detail.controller_generation = {
            true, *selected->evidence.controller_generation};
    if (selected->evidence.window_bytes)
        detail.window_bytes = {true, *selected->evidence.window_bytes};
    if (selected->evidence.inflight_bytes)
        detail.inflight_bytes = {true, *selected->evidence.inflight_bytes};
    if (selected->evidence.retry_after_ns)
        detail.retry_after_ns = {true, *selected->evidence.retry_after_ns};
    return detail;
}

}  // namespace mooncake::adaptive_congestion_control
