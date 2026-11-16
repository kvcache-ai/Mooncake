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

#ifndef MOONCAKE_TASK_CONGESTION_STATUS_H
#define MOONCAKE_TASK_CONGESTION_STATUS_H

#include <cstdint>
#include <string>

#include "task_congestion_status_c.h"

namespace mooncake {

enum class TaskCongestionState : uint8_t {
    kNormal = TASK_CONGESTION_NORMAL,
    kCongested = TASK_CONGESTION_CONGESTED,
    kLongUnavailable = TASK_CONGESTION_LONG_UNAVAILABLE,
    kUnknown = TASK_CONGESTION_UNKNOWN,
};

enum class TaskCongestionAttemptKind : uint8_t {
    kUnknown = TASK_CONGESTION_ATTEMPT_UNKNOWN,
    kRdma = TASK_CONGESTION_ATTEMPT_RDMA,
    kOther = TASK_CONGESTION_ATTEMPT_OTHER,
};

enum class TaskCongestionReason : uint8_t {
    kUnknown = TASK_CONGESTION_REASON_UNKNOWN,
    kByteWindow = TASK_CONGESTION_REASON_BYTE_WINDOW,
    kReceiverPressure = TASK_CONGESTION_REASON_RECEIVER_PRESSURE,
    kPathQuarantined = TASK_CONGESTION_REASON_PATH_QUARANTINED,
    kProbeFailed = TASK_CONGESTION_REASON_PROBE_FAILED,
    kNoUsablePath = TASK_CONGESTION_REASON_NO_USABLE_PATH,
    kCongestionFeedback = TASK_CONGESTION_REASON_FEEDBACK,
    kRouteTimeout = TASK_CONGESTION_REASON_ROUTE_TIMEOUT,
    kLocalConfiguration = TASK_CONGESTION_REASON_LOCAL_CONFIGURATION,
    kRemoteMetadata = TASK_CONGESTION_REASON_REMOTE_METADATA,
    kDerivedFlush = TASK_CONGESTION_REASON_DERIVED_FLUSH,
    kFatal = TASK_CONGESTION_REASON_FATAL,
};

enum class TaskCongestionFailureScope : uint8_t {
    kOperation = TASK_CONGESTION_SCOPE_OPERATION,
    kQp = TASK_CONGESTION_SCOPE_QP,
    kCq = TASK_CONGESTION_SCOPE_CQ,
    kRoute = TASK_CONGESTION_SCOPE_ROUTE,
    kPort = TASK_CONGESTION_SCOPE_PORT,
    kDevice = TASK_CONGESTION_SCOPE_DEVICE,
};

enum class TaskCongestionControllerMode : uint8_t {
    kOff = TASK_CONGESTION_MODE_OFF,
    kObserve = TASK_CONGESTION_MODE_OBSERVE,
    kEnforce = TASK_CONGESTION_MODE_ENFORCE,
};

template <typename T>
struct TaskCongestionObserved {
    bool observed = false;
    T value{};
};

struct TaskCongestionDetail {
    TaskCongestionState state = TaskCongestionState::kUnknown;
    TaskCongestionAttemptKind attempt_kind =
        TaskCongestionAttemptKind::kUnknown;
    TaskCongestionObserved<uint64_t> attempt_id;
    TaskCongestionObserved<TaskCongestionReason> reason;
    TaskCongestionObserved<TaskCongestionFailureScope> failure_scope;
    TaskCongestionObserved<uint64_t> slice_id;
    TaskCongestionObserved<std::string> affected_path;
    TaskCongestionObserved<uint64_t> observed_at_ns;
    bool resolved = false;
    TaskCongestionObserved<TaskCongestionControllerMode> controller_mode;
    TaskCongestionObserved<uint32_t> controller_generation;
    TaskCongestionObserved<uint64_t> window_bytes;
    TaskCongestionObserved<uint64_t> inflight_bytes;
    TaskCongestionObserved<uint64_t> retry_after_ns;
};

}  // namespace mooncake

#endif  // MOONCAKE_TASK_CONGESTION_STATUS_H
