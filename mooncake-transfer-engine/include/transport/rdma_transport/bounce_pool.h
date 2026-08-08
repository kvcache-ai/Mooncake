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

#ifndef RDMA_BOUNCE_POOL_H_
#define RDMA_BOUNCE_POOL_H_

#include <infiniband/verbs.h>

#include <cstdint>
#include <mutex>
#include <vector>

namespace mooncake {

// TE-managed bounce slot pool used by MsgChannel SEND/RECV.
// Each slot is slot_size bytes (header + payload capacity).
class BouncePool {
   public:
    BouncePool() = default;
    ~BouncePool() { destroy(); }

    BouncePool(const BouncePool &) = delete;
    BouncePool &operator=(const BouncePool &) = delete;

    int construct(ibv_pd *pd, size_t slot_size, size_t slot_count);
    void destroy();

    size_t slotSize() const { return slot_size_; }
    size_t slotCount() const { return slots_.size(); }
    size_t freeCount() const;

    // Acquire a free send slot. Returns -1 if none.
    int acquireSendSlot();
    void releaseSendSlot(int idx);

    char *slotPtr(int idx);
    ibv_mr *slotMr(int idx);
    uint32_t slotLkey(int idx);

    // Recv slots are permanently owned by the channel (posted to RQ).
    char *recvSlotPtr(size_t idx);
    ibv_mr *recvSlotMr(size_t idx);

    // Expand by `extra` slots (send+recv). Returns 0 on success.
    int expand(size_t extra);

   private:
    struct Slot {
        std::vector<char> buf;
        ibv_mr *mr = nullptr;
        bool in_use = false;
    };

    ibv_pd *pd_ = nullptr;
    size_t slot_size_ = 0;
    std::vector<Slot> slots_;       // send slots
    std::vector<Slot> recv_slots_;  // recv slots (same count)
    mutable std::mutex mutex_;
};

}  // namespace mooncake

#endif  // RDMA_BOUNCE_POOL_H_
