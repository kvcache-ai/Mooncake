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

#pragma once

#include "bench_runner.h"
#include "tent/common/types.h"

namespace mooncake::tent {

// Called only after the batch is terminal. A canceled aggregate may still
// contain completed tasks; neither their payload nor their latency is lost.
template <typename PollTask>
TransferResult summarizeCanceledTransfer(size_t batch_size, size_t block_size,
                                         double duration_us, PollTask poll) {
    TransferResult result;
    result.duration_us = duration_us;
    for (size_t i = 0; i < batch_size; ++i) {
        TransferStatus status;
        if (!poll(i, status)) return {};
        if (status.s == TransferStatusEnum::COMPLETED) {
            ++result.completed;
            result.transferred_bytes += block_size;
            result.completed_indices.push_back(i);
        } else if (status.s == TransferStatusEnum::CANCELED) {
            ++result.dropped;
        } else {
            return {};  // FAILED/TIMEOUT/INVALID must still abort the run.
        }
    }
    if (result.completed == 0) result.duration_us = kTransferDropped;
    return result;
}

}  // namespace mooncake::tent
