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

#ifndef MOONCAKE_EP_DISPATCH_SLOTS_H
#define MOONCAKE_EP_DISPATCH_SLOTS_H

// Device-independent bookkeeping shared by the CUDA and Intel XPU EP buffers;
// plain C++ so both toolchains (and a host-only unit test) build it.

#include <array>
#include <cstdint>
#include <unordered_map>

namespace mooncake {

// Dispatches whose state a buffer keeps at once for their combines.
constexpr int kEpDispatchSlots = 8;

// A dispatch keeps what its combine needs (the top-k snapshot, and on XPU the
// picks it received) in one of kEpDispatchSlots slots, found again from the
// handle by the address of its src_info tensor. Two-batch overlap holds two
// dispatches at once on one buffer. Not thread-safe: callers hold their
// buffer's lock.
class DispatchSlots {
   public:
    // The slot for a new dispatch. Every dispatch takes the next slot, even
    // when its src_info reuses the freed block of an earlier one: keeping
    // that one's slot instead would let a later dispatch take the slot while
    // it is still held. A slot stays its dispatch's for the next
    // kEpDispatchSlots - 1 dispatches.
    int take(uint64_t src_info_ptr) {
        const int slot = next_;
        next_ = (next_ + 1) % kEpDispatchSlots;
        // Forget the slot's previous dispatch, unless its src_info has since
        // been given to a newer one.
        auto prev = slots_.find(owner_[slot]);
        if (prev != slots_.end() && prev->second == slot) slots_.erase(prev);
        owner_[slot] = src_info_ptr;
        slots_[src_info_ptr] = slot;
        return slot;
    }

    // The slot of the dispatch whose handle holds this src_info, or -1 when
    // it is none of the last kEpDispatchSlots dispatches.
    int find(uint64_t src_info_ptr) const {
        auto it = slots_.find(src_info_ptr);
        return it == slots_.end() ? -1 : it->second;
    }

   private:
    std::unordered_map<uint64_t, int> slots_;
    std::array<uint64_t, kEpDispatchSlots> owner_{};
    int next_ = 0;
};

}  // namespace mooncake

#endif  // MOONCAKE_EP_DISPATCH_SLOTS_H
