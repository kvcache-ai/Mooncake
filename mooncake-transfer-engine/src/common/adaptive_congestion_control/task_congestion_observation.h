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

#ifndef MOONCAKE_TASK_CONGESTION_OBSERVATION_H
#define MOONCAKE_TASK_CONGESTION_OBSERVATION_H

#include <cstddef>
#include <cstdint>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "task_congestion_status.h"

namespace mooncake::adaptive_congestion_control {

struct TaskCongestionEvidence {
    TaskCongestionObserved<TaskCongestionReason> reason;
    TaskCongestionObserved<TaskCongestionFailureScope> failure_scope;
    std::optional<std::string> path;
    TaskCongestionObserved<uint64_t> observed_at_ns;
    TaskCongestionObserved<TaskCongestionControllerMode> controller_mode;
    TaskCongestionObserved<uint32_t> controller_generation;
    TaskCongestionObserved<uint64_t> window_bytes;
    TaskCongestionObserved<uint64_t> inflight_bytes;
    TaskCongestionObserved<uint64_t> retry_after_ns;

    // Only immutable strings from the process-lifetime NIC path pool qualify.
    const std::string* interned_path = nullptr;

    std::optional<std::string_view> effectivePath() const {
        if (path) return *path;
        if (interned_path) return *interned_path;
        return std::nullopt;
    }
};

// A logical task owns this record until its batch is freed. Slice state is
// allocated on the first anomaly and never exceeds the task's submitted
// slice count. Adapters reject stale controller/endpoint generations before
// reporting evidence; the task retains one RDMA attempt.
class TaskCongestionObservation {
   public:
    explicit TaskCongestionObservation(size_t submitted_slice_count);

    TaskCongestionObservation(const TaskCongestionObservation&) = delete;
    TaskCongestionObservation& operator=(const TaskCongestionObservation&) =
        delete;

    bool defer(size_t slice_id, const TaskCongestionEvidence& evidence);
    bool avoid(size_t slice_id, const TaskCongestionEvidence& evidence);
    bool defer(size_t slice_id, TaskCongestionEvidence&& evidence,
               size_t submitted_slice_count);
    bool avoid(size_t slice_id, TaskCongestionEvidence&& evidence,
               size_t submitted_slice_count);
    bool resolve(size_t slice_id);

    TaskCongestionState state() const;
    TaskCongestionDetail detail() const;

   private:
    struct SliceRecord {
        TaskCongestionState state = TaskCongestionState::kNormal;
        TaskCongestionEvidence evidence;
        bool observed = false;
    };
    bool record(size_t slice_id, TaskCongestionState state,
                const TaskCongestionEvidence& evidence);
    bool recordOwned(size_t slice_id, TaskCongestionState state,
                     TaskCongestionEvidence evidence,
                     std::optional<size_t> submitted_slice_count);
    TaskCongestionState stateLocked() const;

    size_t slice_count_;
    mutable std::mutex mutex_;
    std::vector<SliceRecord> slices_;
    size_t congested_count_ = 0;
    size_t unavailable_count_ = 0;
    bool invalid_observation_ = false;
};

}  // namespace mooncake::adaptive_congestion_control

#endif  // MOONCAKE_TASK_CONGESTION_OBSERVATION_H
