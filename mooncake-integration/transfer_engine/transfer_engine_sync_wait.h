// Copyright 2024 KVCache.AI
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

#pragma once

#include <chrono>
#include <cstdint>
#include <memory>

#include "common.h"
#include "config.h"
#include "transfer_engine.h"
#include "transport/transport.h"

namespace mooncake {

// Block until the batch signals completion or the deadline passes.
//
// The classic transport publishes BatchDesc::completion_cv when a batch reaches
// a terminal state (transport.h), and Mooncake Store's transfer_task.cpp
// already waits on it. The sync wrappers used to spin on getTransferStatus with
// no sleep or yield, which kept a core busy for every in-flight transfer; this
// parks the caller instead and leaves the timeout and retry policy above
// untouched.
//
// TENT batches do not use the classic BatchDesc layout, so they keep the
// polling behaviour. Mixed and legacy batches whose transports need foreground
// polling to make progress keep it too, and a positive slice_timeout keeps the
// polling path that enforces it. With USE_EVENT_DRIVEN_COMPLETION off nothing
// is published and the call is a no-op for the same reason.
inline void waitForBatchCompletion(
    const std::shared_ptr<TransferEngine>& engine, BatchID batch_id,
    int64_t deadline_nsec) {
#ifdef USE_EVENT_DRIVEN_COMPLETION
    if (engine->isUsingTent()) return;
    auto& batch_desc = Transport::toBatchDesc(batch_id);
    if (!batch_desc.counter_eligible || globalConfig().slice_timeout > 0)
        return;
    if (batch_desc.is_finished.load(std::memory_order_acquire)) return;
    const int64_t remaining = deadline_nsec - getCurrentTimeInNano();
    if (remaining <= 0) return;
    // The same mutex the notifier takes when it updates the predicate, so a
    // notify cannot land between the check above and the wait here.
    std::unique_lock<std::mutex> lock(batch_desc.completion_mutex);
    batch_desc.completion_cv.wait_for(
        lock, std::chrono::nanoseconds(remaining), [&batch_desc] {
            return batch_desc.is_finished.load(std::memory_order_relaxed);
        });
#else
    (void)engine;
    (void)batch_id;
    (void)deadline_nsec;
#endif
}

}  // namespace mooncake
