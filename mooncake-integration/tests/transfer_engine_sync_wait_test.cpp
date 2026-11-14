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

#include "transfer_engine/transfer_engine_sync_wait.h"

#include <chrono>
#include <iostream>
#include <thread>

using namespace mooncake;

namespace {

bool PollingRequiredBatchReturnsImmediately(
    const std::shared_ptr<TransferEngine>& engine) {
    Transport::BatchDesc batch{};
    batch.counter_eligible = false;
    const int64_t start = getCurrentTimeInNano();
    waitForBatchCompletion(engine, reinterpret_cast<BatchID>(&batch),
                           start + 2'000'000'000);
    const int64_t elapsed = getCurrentTimeInNano() - start;
    if (elapsed >= 1'500'000'000) {
        std::cerr << "A polling-required batch waited " << elapsed / 1'000'000
                  << "ms instead of returning to its status poll\n";
        return false;
    }
    return true;
}

bool EventNotificationWakesWaiter(
    const std::shared_ptr<TransferEngine>& engine) {
    Transport::BatchDesc batch{};
    batch.counter_eligible = true;
    std::thread notifier([&batch] {
        std::this_thread::sleep_for(std::chrono::milliseconds(30));
        {
            std::lock_guard<std::mutex> lock(batch.completion_mutex);
            batch.is_finished.store(true, std::memory_order_release);
        }
        batch.completion_cv.notify_all();
    });
    const int64_t start = getCurrentTimeInNano();
    waitForBatchCompletion(engine, reinterpret_cast<BatchID>(&batch),
                           start + 2'000'000'000);
    const int64_t elapsed = getCurrentTimeInNano() - start;
    notifier.join();
    if (!batch.is_finished.load() || elapsed >= 1'000'000'000) {
        std::cerr << "An event-notified batch did not wake promptly ("
                  << elapsed / 1'000'000 << "ms)\n";
        return false;
    }
    return true;
}

bool SliceTimeoutBatchReturnsImmediately(
    const std::shared_ptr<TransferEngine>& engine) {
    Transport::BatchDesc batch{};
    batch.counter_eligible = true;
    const int64_t previous_timeout = globalConfig().slice_timeout;
    globalConfig().slice_timeout = 1;
    const int64_t start = getCurrentTimeInNano();
    waitForBatchCompletion(engine, reinterpret_cast<BatchID>(&batch),
                           start + 2'000'000'000);
    const int64_t elapsed = getCurrentTimeInNano() - start;
    globalConfig().slice_timeout = previous_timeout;
    if (elapsed >= 1'500'000'000) {
        std::cerr << "A batch with slice timeouts waited "
                  << elapsed / 1'000'000
                  << "ms instead of returning to its timeout poll\n";
        return false;
    }
    return true;
}

}  // namespace

int main() {
    auto engine = std::make_shared<TransferEngine>(false);
    const bool polling_ok = PollingRequiredBatchReturnsImmediately(engine);
    const bool notification_ok = EventNotificationWakesWaiter(engine);
    const bool timeout_ok = SliceTimeoutBatchReturnsImmediately(engine);
    return polling_ok && notification_ok && timeout_ok ? 0 : 1;
}
