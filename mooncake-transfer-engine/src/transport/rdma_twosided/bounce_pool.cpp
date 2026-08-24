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

#include "transport/rdma_twosided/bounce_pool.h"

#include <glog/logging.h>

namespace mooncake {

void BouncePool::destroyUnlocked() {
    for (auto &s : slots_) {
        if (s.mr) {
            ibv_dereg_mr(s.mr);
            s.mr = nullptr;
        }
    }
    for (auto &s : recv_slots_) {
        if (s.mr) {
            ibv_dereg_mr(s.mr);
            s.mr = nullptr;
        }
    }
    slots_.clear();
    recv_slots_.clear();
    pd_ = nullptr;
    slot_size_ = 0;
    active_count_ = 0;
}

int BouncePool::construct(ibv_pd *pd, size_t slot_size, size_t slot_count) {
    if (!pd || slot_size == 0 || slot_count == 0) return -1;
    std::lock_guard<std::mutex> lock(mutex_);
    if (pd_) return -1;
    pd_ = pd;
    slot_size_ = slot_size;
    slots_.resize(slot_count);
    recv_slots_.resize(slot_count);
    for (size_t i = 0; i < slot_count; ++i) {
        slots_[i].buf.resize(slot_size);
        slots_[i].mr = ibv_reg_mr(pd, slots_[i].buf.data(), slot_size,
                                  IBV_ACCESS_LOCAL_WRITE);
        if (!slots_[i].mr) {
            LOG(ERROR) << "BouncePool: failed to register send slot " << i;
            destroyUnlocked();
            return -1;
        }
        recv_slots_[i].buf.resize(slot_size);
        recv_slots_[i].mr = ibv_reg_mr(pd, recv_slots_[i].buf.data(), slot_size,
                                       IBV_ACCESS_LOCAL_WRITE);
        if (!recv_slots_[i].mr) {
            LOG(ERROR) << "BouncePool: failed to register recv slot " << i;
            destroyUnlocked();
            return -1;
        }
    }
    active_count_ = slot_count;
    return 0;
}

void BouncePool::destroy() {
    std::lock_guard<std::mutex> lock(mutex_);
    destroyUnlocked();
}

size_t BouncePool::slotCount() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return slots_.size();
}

size_t BouncePool::activeCount() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return active_count_;
}

size_t BouncePool::freeSendCount() const {
    std::lock_guard<std::mutex> lock(mutex_);
    size_t n = 0;
    for (size_t i = 0; i < active_count_; ++i) {
        if (!slots_[i].in_use) ++n;
    }
    return n;
}

int BouncePool::acquireSendSlot() {
    std::lock_guard<std::mutex> lock(mutex_);
    for (size_t i = 0; i < active_count_; ++i) {
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
    std::lock_guard<std::mutex> lock(mutex_);
    if (idx < 0 || static_cast<size_t>(idx) >= slots_.size()) return nullptr;
    return slots_[idx].buf.data();
}

ibv_mr *BouncePool::slotMr(int idx) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (idx < 0 || static_cast<size_t>(idx) >= slots_.size()) return nullptr;
    return slots_[idx].mr;
}

uint32_t BouncePool::slotLkey(int idx) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (idx < 0 || static_cast<size_t>(idx) >= slots_.size()) return 0;
    return slots_[idx].mr ? slots_[idx].mr->lkey : 0;
}

char *BouncePool::recvSlotPtr(size_t idx) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (idx >= recv_slots_.size()) return nullptr;
    return recv_slots_[idx].buf.data();
}

ibv_mr *BouncePool::recvSlotMr(size_t idx) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (idx >= recv_slots_.size()) return nullptr;
    return recv_slots_[idx].mr;
}

void BouncePool::markRecvPosted(size_t idx, bool posted) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (idx >= recv_slots_.size()) return;
    recv_slots_[idx].recv_posted = posted;
}

bool BouncePool::recvPosted(size_t idx) const {
    std::lock_guard<std::mutex> lock(mutex_);
    if (idx >= recv_slots_.size()) return false;
    return recv_slots_[idx].recv_posted;
}

int BouncePool::expand(size_t extra) {
    if (extra == 0) return 0;
    std::lock_guard<std::mutex> lock(mutex_);
    if (!pd_) return -1;
    size_t old = slots_.size();
    slots_.resize(old + extra);
    recv_slots_.resize(old + extra);
    for (size_t i = old; i < old + extra; ++i) {
        slots_[i].buf.resize(slot_size_);
        slots_[i].mr = ibv_reg_mr(pd_, slots_[i].buf.data(), slot_size_,
                                  IBV_ACCESS_LOCAL_WRITE);
        if (!slots_[i].mr) {
            LOG(ERROR) << "BouncePool::expand failed at send slot " << i;
            while (slots_.size() > old) {
                auto &s = slots_.back();
                if (s.mr) ibv_dereg_mr(s.mr);
                slots_.pop_back();
            }
            while (recv_slots_.size() > old) {
                auto &s = recv_slots_.back();
                if (s.mr) ibv_dereg_mr(s.mr);
                recv_slots_.pop_back();
            }
            return -1;
        }
        recv_slots_[i].buf.resize(slot_size_);
        recv_slots_[i].mr = ibv_reg_mr(pd_, recv_slots_[i].buf.data(),
                                       slot_size_, IBV_ACCESS_LOCAL_WRITE);
        if (!recv_slots_[i].mr) {
            LOG(ERROR) << "BouncePool::expand failed at recv slot " << i;
            if (slots_[i].mr) {
                ibv_dereg_mr(slots_[i].mr);
                slots_[i].mr = nullptr;
            }
            while (slots_.size() > old) {
                auto &s = slots_.back();
                if (s.mr) ibv_dereg_mr(s.mr);
                slots_.pop_back();
            }
            while (recv_slots_.size() > old) {
                auto &s = recv_slots_.back();
                if (s.mr) ibv_dereg_mr(s.mr);
                recv_slots_.pop_back();
            }
            return -1;
        }
    }
    active_count_ = slots_.size();
    return 0;
}

size_t BouncePool::shrinkToward(size_t target_active, size_t min_active) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (min_active == 0) min_active = 1;
    if (target_active < min_active) target_active = min_active;
    if (active_count_ <= target_active) return active_count_;

    active_count_ = target_active;

    while (slots_.size() > active_count_) {
        size_t i = slots_.size() - 1;
        if (slots_[i].in_use || recv_slots_[i].recv_posted) {
            break;
        }
        if (slots_[i].mr) {
            ibv_dereg_mr(slots_[i].mr);
            slots_[i].mr = nullptr;
        }
        if (recv_slots_[i].mr) {
            ibv_dereg_mr(recv_slots_[i].mr);
            recv_slots_[i].mr = nullptr;
        }
        slots_.pop_back();
        recv_slots_.pop_back();
    }
    return active_count_;
}

}  // namespace mooncake
