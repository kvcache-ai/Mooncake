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
LocalBufferSet::LocalBufferSet() {}

LocalBufferSet::~LocalBufferSet() {}

int LocalBufferSet::addBuffer(const Transport::BufferEntry &entry) {
    RWSpinlock::WriteGuard guard(lock_);
    AddressRange range(entry.addr, entry.length);
    auto &buffer_list = buffer_lists_[int(entry.visibility)];
    if (buffer_list.count(range)) {
        buffer_list[range].ref_cnt++;
        return 0;
    }
    auto &item = buffer_list[range];
    item.ref_cnt = 1;
    item.entry = entry;
    return registerMemReg(item);
}

int LocalBufferSet::removeBuffer(const AddressRange &range) {
    RWSpinlock::WriteGuard guard(lock_);
    for (auto vis_level = 0; vis_level < 3; ++vis_level) {
        auto &buffer_list = buffer_lists_[vis_level];
        if (buffer_list.count(range)) {
            auto &item = buffer_list[range];
            item.ref_cnt--;
            if (item.ref_cnt != 0) return 0;
            int ret = unregisterMemReg(item);
            buffer_list.erase(range);
            return ret;
        }
    }
    return ERR_ADDRESS_NOT_REGISTERED;
}

int LocalBufferSet::removeBufferLegacy(void *addr) {
    RWSpinlock::WriteGuard guard(lock_);
    for (auto vis_level = 0; vis_level < 3; ++vis_level) {
        auto &buffer_list = buffer_lists_[vis_level];
        for (auto &item : buffer_list) {
            if (item.first.addr == addr) {
                item.second.ref_cnt--;
                if (item.second.ref_cnt != 0) return 0;
                int ret = unregisterMemReg(item.second);
                buffer_list.erase(item.first);
                return ret;
            }
        }
    }
    return ERR_ADDRESS_NOT_REGISTERED;
}

// TODO handle more complex buffer organization, e.g. the range
// cross multiple buffers.
int LocalBufferSet::findBuffer(const AddressRange &range, RdmaContext *context,
                               Transport::BufferVisibility visibility,
                               std::vector<LocalBufferSet::Result> &result) {
    RWSpinlock::ReadGuard guard(lock_);
    for (auto vis_level = (int)visibility; vis_level < 3; ++vis_level) {
        auto &buffer_list = buffer_lists_[vis_level];
        for (auto &elem : buffer_list) {
            if (elem.first.intersect(range) == range &&
                elem.second.mem_reg_map.count(context)) {
                LocalBufferSet::Result result_entry;
                result_entry.addr = range.addr;
                result_entry.length = range.length;
                auto key =
                    context->queryMemRegKey(elem.second.mem_reg_map[context]);
                result_entry.lkey = key.first;
                result_entry.rkey = key.second;
                result.push_back(result_entry);
                return 0;
            }
        }
    }
    return ERR_ADDRESS_NOT_REGISTERED;
}

int LocalBufferSet::findBufferLegacy(void *addr, RdmaContext *context,
                                     uint32_t &lkey, uint32_t &rkey) {
    RWSpinlock::ReadGuard guard(lock_);
    for (auto vis_level = 0; vis_level < 3; ++vis_level) {
        auto &buffer_list = buffer_lists_[vis_level];
        for (auto &elem : buffer_list) {
            if ((uint64_t)elem.first.addr <= (uint64_t)addr &&
                (uint64_t)addr <
                    (uint64_t)elem.first.addr + elem.first.length &&
                elem.second.mem_reg_map.count(context)) {
                auto key =
                    context->queryMemRegKey(elem.second.mem_reg_map[context]);
                lkey = key.first;
                rkey = key.second;
                return 0;
            }
        }
    }
    return ERR_ADDRESS_NOT_REGISTERED;
}

int LocalBufferSet::addDevice(RdmaContext *context) {
    RWSpinlock::WriteGuard guard(lock_);
    context_list_.insert(context);
    for (auto vis_level = 0; vis_level < 3; ++vis_level) {
        auto &buffer_list = buffer_lists_[vis_level];
        for (auto &item : buffer_list) registerMemReg(item.second);
    }
    return 0;
}

int LocalBufferSet::removeDevice(RdmaContext *context) {
    RWSpinlock::WriteGuard guard(lock_);
    context_list_.erase(context);
    for (auto vis_level = 0; vis_level < 3; ++vis_level) {
        auto &buffer_list = buffer_lists_[vis_level];
        for (auto &item : buffer_list) unregisterMemReg(item.second, context);
    }
    return 0;
}

int LocalBufferSet::registerMemReg(LocalBufferSet::BufferItem &item) {
    auto &entry = item.entry;
    int access = IBV_ACCESS_LOCAL_WRITE;
    if (entry.visibility == Transport::kGlobalReadWrite) {
        access |= IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ;
    } else if (entry.visibility == Transport::kGlobalReadOnly) {
        access |= IBV_ACCESS_REMOTE_READ;
    }
    item.mem_reg_map.clear();
    for (auto context : context_list_) {
        if (item.mem_reg_map.count(context)) continue;
        auto mem_reg =
            context->registerMemReg(entry.addr, entry.length, access);
        if (mem_reg) item.mem_reg_map[context] = mem_reg;
    }
    return 0;
}

int LocalBufferSet::unregisterMemReg(LocalBufferSet::BufferItem &item,
                                     RdmaContext *context_filter) {
    for (auto iter = item.mem_reg_map.begin(); iter != item.mem_reg_map.end();
         iter++) {
        auto &context = iter->first;
        auto &mem_reg = iter->second;
        if (!context_filter || context_filter == context) {
            context->unregisterMemReg(mem_reg);
            iter = item.mem_reg_map.erase(iter);
        }
    }
    return 0;
}

int LocalBufferSet::clear() {
    RWSpinlock::WriteGuard guard(lock_);
    for (auto vis_level = 0; vis_level < 3; ++vis_level) {
        auto &buffer_list = buffer_lists_[vis_level];
        for (auto &item : buffer_list) {
            unregisterMemReg(item.second);
        }
        buffer_list.clear();
    }
    context_list_.clear();
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