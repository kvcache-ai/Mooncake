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

#include "transport_v1/rdma/buffers.h"

#include "transport_v1/rdma/context.h"

namespace mooncake {
namespace v1 {
LocalBuffers::LocalBuffers(std::shared_ptr<RdmaContext> &context)
    : context_(context) {}

LocalBuffers::~LocalBuffers() {}

int LocalBuffers::add(const Transport::BufferEntry &buffer, uint32_t &lkey,
                      uint32_t &rkey) {
    int access = IBV_ACCESS_LOCAL_WRITE;
    if (buffer.visibility == Transport::kGlobalReadWrite) {
        access |= IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ;
    } else if (buffer.visibility == Transport::kGlobalReadOnly) {
        access |= IBV_ACCESS_REMOTE_READ;
    }

    int mem_reg_index =
        context_->registerMemReg(buffer.addr, buffer.length, access);
    if (mem_reg_index < 0) return mem_reg_index;

    auto key = context_->queryMemRegKey(mem_reg_index);
    auto range = AddressRange{buffer.addr, buffer.length};

    int prev_mem_reg_index = -1;

    lock_.lock();
    if (sorted_buffers_.count(range)) {
        prev_mem_reg_index = sorted_buffers_[range].mem_reg_index;
    }
    lkey = key.first;
    rkey = key.second;
    sorted_buffers_[range] = BufferItem{mem_reg_index, lkey, rkey};
    lock_.unlock();

    if (prev_mem_reg_index >= 0)
        return context_->unregisterMemReg(prev_mem_reg_index);

    return 0;
}

int LocalBuffers::remove(const AddressRange &range) {
    lock_.lock();
    if (!sorted_buffers_.count(range)) {
        lock_.unlock();
        return ERR_INVALID_ARGUMENT;
    }
    auto entry = sorted_buffers_[range];
    sorted_buffers_.erase(range);
    lock_.unlock();
    return context_->unregisterMemReg(entry.mem_reg_index);
}

int LocalBuffers::remove(void *addr) {
    int mem_reg_index = 0;
    bool found = false;
    lock_.lock();
    for (auto &entry : sorted_buffers_) {
        auto &range = entry.first;
        if ((uintptr_t)addr >= (uintptr_t)range.addr &&
            (uintptr_t)addr < (uintptr_t)range.addr + range.length) {
            found = true;
            break;
        }
        mem_reg_index++;
    }
    lock_.unlock();
    if (found) return context_->unregisterMemReg(mem_reg_index);
    return 0;
}

int LocalBuffers::query(const AddressRange &target_range,
                        std::vector<Result> &result) {
    result.clear();
    lock_.lockShared();
    for (auto &entry : sorted_buffers_) {
        auto &range = entry.first;
        auto intersect = range.intersect(target_range);
        if (intersect.empty()) continue;
        result.push_back(Result{intersect.addr, intersect.length,
                                entry.second.lkey, entry.second.rkey});
    }
    lock_.unlockShared();
    return 0;
}

PeerBuffers::PeerBuffers() : segment_desc_(nullptr) {}

PeerBuffers::~PeerBuffers() {}

int PeerBuffers::reload(const std::shared_ptr<SegmentDesc> &segment_desc) {
    lock_.lock();
    segment_desc_ = segment_desc;
    lock_.unlock();
    return 0;
}

int PeerBuffers::query(const AddressRange &target_range, int device_id,
                       std::vector<Result> &result) {
    result.clear();
    auto &buffers = std::get<MemorySegmentDesc>(segment_desc_->detail).buffers;
    lock_.lockShared();
    for (auto &entry : buffers) {
        auto range = AddressRange{(void *)entry.addr, entry.length};
        auto intersect = range.intersect(target_range);
        if (intersect.empty()) continue;
        if (device_id < 0 ||
            device_id >= (int)std::min(entry.lkey.size(), entry.rkey.size())) {
            lock_.unlockShared();
            return ERR_INVALID_ARGUMENT;
        }
        result.push_back(Result{intersect.addr, intersect.length,
                                entry.lkey[device_id], entry.rkey[device_id]});
    }
    lock_.unlockShared();
    return 0;
}
}  // namespace v1
}  // namespace mooncake