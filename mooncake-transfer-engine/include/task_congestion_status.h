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

namespace mooncake {

enum class TaskCongestionState : uint8_t {
    kNormal,
    kCongested,
    kLongUnavailable,
    kUnknown,
};

enum class TaskCongestionAttemptKind : uint8_t { kUnknown, kRdma, kOther };

enum class TaskCongestionReason : uint8_t {
    kUnknown,
    kByteWindow,
    kReceiverPressure,
    kPathQuarantined,
    kProbeFailed,
    kNoUsablePath,
    kCongestionFeedback,
    kRouteTimeout,
    kLocalConfiguration,
    kRemoteMetadata,
    kDerivedFlush,
    kFatal,
};

enum class TaskCongestionFailureScope : uint8_t {
    kOperation,
    kQp,
    kCq,
    kRoute,
    kPort,
    kDevice,
};

enum class TaskCongestionControllerMode : uint8_t {
    kOff,
    kObserve,
    kEnforce,
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
