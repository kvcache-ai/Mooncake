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

#ifndef MOONCAKE_TASK_CONGESTION_C_PROJECTION_H
#define MOONCAKE_TASK_CONGESTION_C_PROJECTION_H

#include <cstddef>
#include <cstring>

#include "task_congestion_status.h"
#include "task_congestion_status_c.h"

namespace mooncake {

static_assert(static_cast<int>(TaskCongestionState::kNormal) ==
              TASK_CONGESTION_NORMAL);
static_assert(static_cast<int>(TaskCongestionState::kCongested) ==
              TASK_CONGESTION_CONGESTED);
static_assert(static_cast<int>(TaskCongestionState::kLongUnavailable) ==
              TASK_CONGESTION_LONG_UNAVAILABLE);
static_assert(static_cast<int>(TaskCongestionState::kUnknown) ==
              TASK_CONGESTION_UNKNOWN);
static_assert(static_cast<int>(TaskCongestionAttemptKind::kUnknown) ==
              TASK_CONGESTION_ATTEMPT_UNKNOWN);
static_assert(static_cast<int>(TaskCongestionAttemptKind::kRdma) ==
              TASK_CONGESTION_ATTEMPT_RDMA);
static_assert(static_cast<int>(TaskCongestionAttemptKind::kOther) ==
              TASK_CONGESTION_ATTEMPT_OTHER);
static_assert(static_cast<int>(TaskCongestionReason::kUnknown) ==
              TASK_CONGESTION_REASON_UNKNOWN);
static_assert(static_cast<int>(TaskCongestionReason::kByteWindow) ==
              TASK_CONGESTION_REASON_BYTE_WINDOW);
static_assert(static_cast<int>(TaskCongestionReason::kReceiverPressure) ==
              TASK_CONGESTION_REASON_RECEIVER_PRESSURE);
static_assert(static_cast<int>(TaskCongestionReason::kPathQuarantined) ==
              TASK_CONGESTION_REASON_PATH_QUARANTINED);
static_assert(static_cast<int>(TaskCongestionReason::kProbeFailed) ==
              TASK_CONGESTION_REASON_PROBE_FAILED);
static_assert(static_cast<int>(TaskCongestionReason::kNoUsablePath) ==
              TASK_CONGESTION_REASON_NO_USABLE_PATH);
static_assert(static_cast<int>(TaskCongestionReason::kCongestionFeedback) ==
              TASK_CONGESTION_REASON_FEEDBACK);
static_assert(static_cast<int>(TaskCongestionReason::kRouteTimeout) ==
              TASK_CONGESTION_REASON_ROUTE_TIMEOUT);
static_assert(static_cast<int>(TaskCongestionReason::kLocalConfiguration) ==
              TASK_CONGESTION_REASON_LOCAL_CONFIGURATION);
static_assert(static_cast<int>(TaskCongestionReason::kRemoteMetadata) ==
              TASK_CONGESTION_REASON_REMOTE_METADATA);
static_assert(static_cast<int>(TaskCongestionReason::kDerivedFlush) ==
              TASK_CONGESTION_REASON_DERIVED_FLUSH);
static_assert(static_cast<int>(TaskCongestionReason::kFatal) ==
              TASK_CONGESTION_REASON_FATAL);
static_assert(static_cast<int>(TaskCongestionFailureScope::kOperation) ==
              TASK_CONGESTION_SCOPE_OPERATION);
static_assert(static_cast<int>(TaskCongestionFailureScope::kQp) ==
              TASK_CONGESTION_SCOPE_QP);
static_assert(static_cast<int>(TaskCongestionFailureScope::kCq) ==
              TASK_CONGESTION_SCOPE_CQ);
static_assert(static_cast<int>(TaskCongestionFailureScope::kRoute) ==
              TASK_CONGESTION_SCOPE_ROUTE);
static_assert(static_cast<int>(TaskCongestionFailureScope::kPort) ==
              TASK_CONGESTION_SCOPE_PORT);
static_assert(static_cast<int>(TaskCongestionFailureScope::kDevice) ==
              TASK_CONGESTION_SCOPE_DEVICE);
static_assert(static_cast<int>(TaskCongestionControllerMode::kOff) ==
              TASK_CONGESTION_MODE_OFF);
static_assert(static_cast<int>(TaskCongestionControllerMode::kObserve) ==
              TASK_CONGESTION_MODE_OBSERVE);
static_assert(static_cast<int>(TaskCongestionControllerMode::kEnforce) ==
              TASK_CONGESTION_MODE_ENFORCE);

inline int taskCongestionCState(TaskCongestionState state) {
    return static_cast<int>(state);
}

inline void projectTaskCongestionDetail(const TaskCongestionDetail& source,
                                        task_congestion_detail_t& target) {
    target = {};
    target.state = taskCongestionCState(source.state);
    target.attempt_kind = static_cast<int>(source.attempt_kind);
    target.resolved = source.resolved ? 1 : 0;
    if (source.attempt_id.observed) {
        target.observed_fields |= TASK_CONGESTION_HAS_ATTEMPT_ID;
        target.attempt_id = source.attempt_id.value;
    }
    if (source.reason.observed) {
        target.observed_fields |= TASK_CONGESTION_HAS_REASON;
        target.reason = static_cast<int>(source.reason.value);
    }
    if (source.failure_scope.observed) {
        target.observed_fields |= TASK_CONGESTION_HAS_FAILURE_SCOPE;
        target.failure_scope = static_cast<int>(source.failure_scope.value);
    }
    if (source.slice_id.observed) {
        target.observed_fields |= TASK_CONGESTION_HAS_SLICE_ID;
        target.slice_id = source.slice_id.value;
    }
    if (source.affected_path.observed)
        target.observed_fields |= TASK_CONGESTION_HAS_AFFECTED_PATH;
    if (source.observed_at_ns.observed) {
        target.observed_fields |= TASK_CONGESTION_HAS_OBSERVED_AT_NS;
        target.observed_at_ns = source.observed_at_ns.value;
    }
    if (source.controller_mode.observed) {
        target.observed_fields |= TASK_CONGESTION_HAS_CONTROLLER_MODE;
        target.controller_mode = static_cast<int>(source.controller_mode.value);
    }
    if (source.controller_generation.observed) {
        target.observed_fields |= TASK_CONGESTION_HAS_CONTROLLER_GENERATION;
        target.controller_generation = source.controller_generation.value;
    }
    if (source.window_bytes.observed) {
        target.observed_fields |= TASK_CONGESTION_HAS_WINDOW_BYTES;
        target.window_bytes = source.window_bytes.value;
    }
    if (source.inflight_bytes.observed) {
        target.observed_fields |= TASK_CONGESTION_HAS_INFLIGHT_BYTES;
        target.inflight_bytes = source.inflight_bytes.value;
    }
    if (source.retry_after_ns.observed) {
        target.observed_fields |= TASK_CONGESTION_HAS_RETRY_AFTER_NS;
        target.retry_after_ns = source.retry_after_ns.value;
    }
}

// A zero-capacity call only reports the required length. No path is written
// when the caller's buffer is too short.
inline bool copyTaskCongestionPath(const TaskCongestionDetail& source,
                                   char* path_buf, size_t path_capacity,
                                   size_t& required_path_length) {
    required_path_length = source.affected_path.observed
                               ? source.affected_path.value.size() + 1
                               : 0;
    if (!path_capacity) return true;
    if (!path_buf || path_capacity < required_path_length) return false;
    if (required_path_length)
        std::memcpy(path_buf, source.affected_path.value.c_str(),
                    required_path_length);
    return true;
}

}  // namespace mooncake

#endif  // MOONCAKE_TASK_CONGESTION_C_PROJECTION_H
