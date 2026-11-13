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

#ifndef MOONCAKE_TASK_CONGESTION_STATUS_C_H
#define MOONCAKE_TASK_CONGESTION_STATUS_C_H

#include <stdint.h>

// These values are stable across the Classic TE and TENT C APIs.
enum task_congestion_state {
    TASK_CONGESTION_NORMAL = 0,
    TASK_CONGESTION_CONGESTED = 1,
    TASK_CONGESTION_LONG_UNAVAILABLE = 2,
    TASK_CONGESTION_UNKNOWN = 3,
};

enum task_congestion_attempt_kind {
    TASK_CONGESTION_ATTEMPT_UNKNOWN = 0,
    TASK_CONGESTION_ATTEMPT_RDMA = 1,
    TASK_CONGESTION_ATTEMPT_OTHER = 2,
};

enum task_congestion_reason {
    TASK_CONGESTION_REASON_UNKNOWN = 0,
    TASK_CONGESTION_REASON_BYTE_WINDOW = 1,
    TASK_CONGESTION_REASON_RECEIVER_PRESSURE = 2,
    TASK_CONGESTION_REASON_PATH_QUARANTINED = 3,
    TASK_CONGESTION_REASON_PROBE_FAILED = 4,
    TASK_CONGESTION_REASON_NO_USABLE_PATH = 5,
    TASK_CONGESTION_REASON_FEEDBACK = 6,
    TASK_CONGESTION_REASON_ROUTE_TIMEOUT = 7,
    TASK_CONGESTION_REASON_LOCAL_CONFIGURATION = 8,
    TASK_CONGESTION_REASON_REMOTE_METADATA = 9,
    TASK_CONGESTION_REASON_DERIVED_FLUSH = 10,
    TASK_CONGESTION_REASON_FATAL = 11,
};

enum task_congestion_failure_scope {
    TASK_CONGESTION_SCOPE_OPERATION = 0,
    TASK_CONGESTION_SCOPE_QP = 1,
    TASK_CONGESTION_SCOPE_CQ = 2,
    TASK_CONGESTION_SCOPE_ROUTE = 3,
    TASK_CONGESTION_SCOPE_PORT = 4,
    TASK_CONGESTION_SCOPE_DEVICE = 5,
};

enum task_congestion_controller_mode {
    TASK_CONGESTION_MODE_OFF = 0,
    TASK_CONGESTION_MODE_OBSERVE = 1,
    TASK_CONGESTION_MODE_ENFORCE = 2,
};

enum task_congestion_observed_field {
    TASK_CONGESTION_HAS_ATTEMPT_ID = 1u << 0,
    TASK_CONGESTION_HAS_REASON = 1u << 1,
    TASK_CONGESTION_HAS_FAILURE_SCOPE = 1u << 2,
    TASK_CONGESTION_HAS_SLICE_ID = 1u << 3,
    TASK_CONGESTION_HAS_AFFECTED_PATH = 1u << 4,
    TASK_CONGESTION_HAS_OBSERVED_AT_NS = 1u << 5,
    TASK_CONGESTION_HAS_CONTROLLER_MODE = 1u << 6,
    TASK_CONGESTION_HAS_CONTROLLER_GENERATION = 1u << 7,
    TASK_CONGESTION_HAS_WINDOW_BYTES = 1u << 8,
    TASK_CONGESTION_HAS_INFLIGHT_BYTES = 1u << 9,
    TASK_CONGESTION_HAS_RETRY_AFTER_NS = 1u << 10,
};

// Optional fields are meaningful only when their observed bit is set. The
// enum values above are shared with the public C++ types. An absent value is
// not an inferred zero.
struct task_congestion_detail {
    int state;
    int attempt_kind;
    uint32_t observed_fields;
    int resolved;
    uint64_t attempt_id;
    int reason;
    int failure_scope;
    uint64_t slice_id;
    uint64_t observed_at_ns;
    int controller_mode;
    uint32_t controller_generation;
    uint64_t window_bytes;
    uint64_t inflight_bytes;
    uint64_t retry_after_ns;
};

typedef struct task_congestion_detail task_congestion_detail_t;

#endif  // MOONCAKE_TASK_CONGESTION_STATUS_C_H
