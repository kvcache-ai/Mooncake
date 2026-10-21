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

#ifndef MOONCAKE_RDMA_ERROR_CLASSIFIER_H
#define MOONCAKE_RDMA_ERROR_CLASSIFIER_H

#include <infiniband/verbs.h>

#include <cstdint>
#include <optional>

#include "adaptive_congestion_control.h"

namespace mooncake::adaptive_congestion_control {

struct Classification {
    OutcomeClass outcome = OutcomeClass::kFatal;
    FailureScope scope = FailureScope::kOperation;
    bool root_failure = false;
    uint32_t vendor_error = 0;
};

Classification classifyCompletion(ibv_wc_status status,
                                  uint32_t vendor_error = 0);
std::optional<Classification> classifyAsyncEvent(ibv_event_type event);

}  // namespace mooncake::adaptive_congestion_control

#endif  // MOONCAKE_RDMA_ERROR_CLASSIFIER_H
