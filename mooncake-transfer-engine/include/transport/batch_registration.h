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

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <limits>
#include <thread>
#include <vector>

#include "config.h"

namespace mooncake {

inline size_t maxConcurrentRegMr() {
    size_t configured = globalConfig().max_concurrent_reg_mr;
    return configured > 0 ? configured : std::numeric_limits<size_t>::max();
}

// Run every item while bounding thread creation when
// MC_MAX_CONCURRENT_REG_MR is configured. With the default value, this keeps
// the historical one-worker-per-buffer behavior.
template <typename Function>
int runBoundedRegMrBatch(size_t count, Function &&function) {
    if (count == 0) return 0;

    const size_t workers = std::min(count, maxConcurrentRegMr());
    std::atomic<size_t> next{0};
    std::atomic<int> first_error{0};

    auto worker = [&]() {
        for (size_t i = next.fetch_add(1); i < count; i = next.fetch_add(1)) {
            int ret = function(i);
            if (ret) {
                int expected = 0;
                first_error.compare_exchange_strong(expected, ret);
            }
        }
    };

    std::vector<std::thread> threads;
    threads.reserve(workers - 1);
    for (size_t i = 1; i < workers; ++i) threads.emplace_back(worker);
    worker();
    for (auto &thread : threads) thread.join();

    return first_error.load();
}

}  // namespace mooncake
