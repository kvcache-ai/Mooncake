// Copyright 2025 KVCache.AI
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

#ifndef TCP_HP_STRIPED_TRANSFER_H_
#define TCP_HP_STRIPED_TRANSFER_H_

#include <atomic>
#include <cstddef>
#include <functional>

#include "tent/common/types.h"

namespace mooncake {
namespace tent {
namespace tcp_hp {

// Coordinates multiple parallel TCP connections (stripes) that together
// complete a single logical transfer. Each stripe transfers a contiguous
// sub-range of the buffer independently.
//
// The transfer is considered COMPLETED only when all stripes succeed.
// If any stripe fails, the overall transfer is marked FAILED immediately.
class StripedTransfer : public std::enable_shared_from_this<StripedTransfer> {
   public:
    StripedTransfer(size_t num_stripes,
                    std::function<void(TransferStatusEnum)> on_complete)
        : num_stripes_(num_stripes), on_complete_(std::move(on_complete)) {}

    // Called by each stripe's HpSession when it finishes.
    void onStripeComplete(TransferStatusEnum status) {
        if (status != TransferStatusEnum::COMPLETED) {
            failed_.fetch_add(1, std::memory_order_relaxed);
        }
        size_t done = completed_.fetch_add(1, std::memory_order_acq_rel) + 1;
        if (done == num_stripes_) {
            bool any_failed =
                failed_.load(std::memory_order_relaxed) > 0;
            if (on_complete_) {
                on_complete_(any_failed ? TransferStatusEnum::FAILED
                                        : TransferStatusEnum::COMPLETED);
            }
        }
    }

   private:
    size_t num_stripes_;
    std::atomic<size_t> completed_{0};
    std::atomic<size_t> failed_{0};
    std::function<void(TransferStatusEnum)> on_complete_;
};

}  // namespace tcp_hp
}  // namespace tent
}  // namespace mooncake

#endif  // TCP_HP_STRIPED_TRANSFER_H_
