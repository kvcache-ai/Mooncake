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

#include "transport/rdma_transport/bounce_pool.h"

#include <glog/logging.h>

#include "error.h"

namespace mooncake {

int BouncePool::construct(ibv_pd *pd, size_t slot_size, size_t slot_count) {
    if (!pd || slot_size < 64 || slot_count == 0) return ERR_INVALID_ARGUMENT;
    std::lock_guard<std::mutex> lock(mutex_);
    if (pd_) return 0;
    pd_ = pd;
    slot_size_ = slot_size;
    slots_.resize(slot_count);
    recv_slots_.resize(slot_count);
    for (size_t i = 0; i < slot_count; ++i) {
        slots_[i].buf.assign(slot_size, 0);
        slots_[i].mr = ibv_reg_mr(pd_, slots_[i].buf.data(), slot_size,
                                  IBV_ACCESS_LOCAL_WRITE);
        if (!slots_[i].mr) {
            PLOG(ERROR) << "BouncePool: failed to register send slot " << i;
            // Fall through to destroyUnlocked via public destroy after unlock.
            pd_ = nullptr;
            break;
        }
        recv_slots_[i].buf.assign(slot_size, 0);
        recv_slots_[i].mr = ibv_reg_mr(pd_, recv_slots_[i].buf.data(),
                                       slot_size, IBV_ACCESS_LOCAL_WRITE);
        if (!recv_slots_[i].mr) {
            PLOG(ERROR) << "BouncePool: failed to register recv slot " << i;
            pd_ = nullptr;
            break;
        }
    }
    if (!pd_) {
        for (auto &s : slots_) {
            if (s.mr) ibv_dereg_mr(s.mr);
            s = Slot{};
        }
        slots_.clear();
        for (auto &s : recv_slots_) {
            if (s.mr) ibv_dereg_mr(s.mr);
            s = Slot{};
        }
        recv_slots_.clear();
        slot_size_ = 0;
        return ERR_MEMORY;
    }
    return 0;
}

void BouncePool::destroy() {
    std::lock_guard<std::mutex> lock(mutex_);
    for (auto &s : slots_) {
        if (s.mr) ibv_dereg_mr(s.mr);
        s.mr = nullptr;
        s.buf.clear();
        s.in_use = false;
    }
    slots_.clear();
    for (auto &s : recv_slots_) {
        if (s.mr) ibv_dereg_mr(s.mr);
        s.mr = nullptr;
        s.buf.clear();
    }
    recv_slots_.clear();
    pd_ = nullptr;
    slot_size_ = 0;
}

size_t BouncePool::freeCount() const {
    std::lock_guard<std::mutex> lock(mutex_);
    size_t n = 0;
    for (const auto &s : slots_)
        if (!s.in_use) ++n;
    return n;
}

int BouncePool::acquireSendSlot() {
    std::lock_guard<std::mutex> lock(mutex_);
    for (size_t i = 0; i < slots_.size(); ++i) {
        if (!slots_[i].in_use) {
            slots_[i].in_use = true;
            return static_cast<int>(i);
        }
    }
    return -1;
}

void BouncePool::releaseSendSlot(int idx) {
    if (idx < 0) return;
    std::lock_guard<std::mutex> lock(mutex_);
    if (static_cast<size_t>(idx) >= slots_.size()) return;
    slots_[idx].in_use = false;
}

char *BouncePool::slotPtr(int idx) {
    if (idx < 0 || static_cast<size_t>(idx) >= slots_.size()) return nullptr;
    return slots_[idx].buf.data();
}

ibv_mr *BouncePool::slotMr(int idx) {
    if (idx < 0 || static_cast<size_t>(idx) >= slots_.size()) return nullptr;
    return slots_[idx].mr;
}

uint32_t BouncePool::slotLkey(int idx) {
    auto *mr = slotMr(idx);
    return mr ? mr->lkey : 0;
}

char *BouncePool::recvSlotPtr(size_t idx) {
    if (idx >= recv_slots_.size()) return nullptr;
    return recv_slots_[idx].buf.data();
}

ibv_mr *BouncePool::recvSlotMr(size_t idx) {
    if (idx >= recv_slots_.size()) return nullptr;
    return recv_slots_[idx].mr;
}

int BouncePool::expand(size_t extra) {
    if (extra == 0) return 0;
    std::lock_guard<std::mutex> lock(mutex_);
    if (!pd_) return ERR_INVALID_ARGUMENT;
    size_t old = slots_.size();
    slots_.resize(old + extra);
    recv_slots_.resize(old + extra);
    for (size_t i = old; i < old + extra; ++i) {
        slots_[i].buf.assign(slot_size_, 0);
        slots_[i].mr = ibv_reg_mr(pd_, slots_[i].buf.data(), slot_size_,
                                  IBV_ACCESS_LOCAL_WRITE);
        if (!slots_[i].mr) return ERR_MEMORY;
        recv_slots_[i].buf.assign(slot_size_, 0);
        recv_slots_[i].mr = ibv_reg_mr(pd_, recv_slots_[i].buf.data(),
                                       slot_size_, IBV_ACCESS_LOCAL_WRITE);
        if (!recv_slots_[i].mr) return ERR_MEMORY;
    }
    return 0;
}

}  // namespace mooncake
