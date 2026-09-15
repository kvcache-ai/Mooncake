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
#include <vector>

#include "task_congestion_status.h"

namespace mooncake::adaptive_congestion_control {

struct TaskCongestionEvidence {
    std::optional<TaskCongestionReason> reason;
    std::optional<TaskCongestionFailureScope> failure_scope;
    std::optional<std::string> path;
    std::optional<uint64_t> observed_at_ns;
    std::optional<TaskCongestionControllerMode> controller_mode;
    std::optional<uint32_t> controller_generation;
    std::optional<uint64_t> window_bytes;
    std::optional<uint64_t> inflight_bytes;
    std::optional<uint64_t> retry_after_ns;

    bool operator==(const TaskCongestionEvidence&) const = default;
};

// A logical task owns this record until its batch is freed. Slice state is
// allocated on the first anomaly and never exceeds this attempt's submitted
// slice count. Adapters reject stale controller/endpoint generations before
// reporting evidence; this class fences stale logical attempts.
class TaskCongestionObservation {
   public:
    TaskCongestionObservation(size_t submitted_slice_count,
                              uint64_t attempt_id);

    TaskCongestionObservation(const TaskCongestionObservation&) = delete;
    TaskCongestionObservation& operator=(const TaskCongestionObservation&) =
        delete;

    bool beginAttempt(uint64_t attempt_id, size_t submitted_slice_count);
    bool extendAttempt(uint64_t attempt_id, size_t submitted_slice_count);
    bool defer(size_t slice_id, uint64_t attempt_id,
               const TaskCongestionEvidence& evidence);
    bool avoid(size_t slice_id, uint64_t attempt_id,
               const TaskCongestionEvidence& evidence);
    bool resolve(size_t slice_id, uint64_t attempt_id);
    bool terminalFailure(uint64_t attempt_id);

    TaskCongestionState state() const;
    TaskCongestionDetail detail() const;

   private:
    struct SliceRecord {
        TaskCongestionState state = TaskCongestionState::kNormal;
        TaskCongestionEvidence evidence;
    };
    struct EventRecord {
        size_t slice_id = 0;
        TaskCongestionEvidence evidence;
        bool resolved = false;
    };

    bool record(size_t slice_id, uint64_t attempt_id, TaskCongestionState state,
                const TaskCongestionEvidence& evidence);
    bool currentAttempt(uint64_t attempt_id) const;
    TaskCongestionState stateLocked() const;

    size_t slice_count_;
    mutable std::mutex mutex_;
    uint64_t attempt_id_;
    std::vector<SliceRecord> slices_;
    size_t congested_count_ = 0;
    size_t unavailable_count_ = 0;
    bool terminal_failure_ = false;
    bool invalid_observation_ = false;
    std::optional<EventRecord> last_event_;
};

}  // namespace mooncake::adaptive_congestion_control

#endif  // MOONCAKE_TASK_CONGESTION_OBSERVATION_H
