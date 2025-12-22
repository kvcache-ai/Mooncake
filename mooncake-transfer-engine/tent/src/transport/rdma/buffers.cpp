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

#include "tent/transport/rdma/buffers.h"
#include "tent/transport/rdma/context.h"

namespace mooncake {
namespace tent {
LocalBufferManager::LocalBufferManager() {}

LocalBufferManager::~LocalBufferManager() { clear(); }

static inline int getAccessFlags(Permission perm) {
    int access = IBV_ACCESS_LOCAL_WRITE;
    if (perm == kGlobalReadWrite) {
        access |= IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ;
    } else if (perm == kGlobalReadOnly) {
        access |= IBV_ACCESS_REMOTE_READ;
    }
    return access;
}

Status LocalBufferManager::addBuffer(BufferDesc& desc,
                                     const MemoryOptions& options) {
    AddressRange range((void*)desc.addr, desc.length);
    BufferEntryForRdma staging;
    auto access = getAccessFlags(options.perm);
    assert(desc.rkey.empty());
    RdmaContext::MemReg mem_reg_list[context_list_.size()] = {0};
    std::vector<std::future<void>> tasks(context_list_.size());
    for (size_t id = 0; id < context_list_.size(); ++id) {
        auto context = context_list_[id];
        auto* mem_reg = &mem_reg_list[id];
        if (!context) continue;
        tasks[id] = std::async([=]() {
            *mem_reg =
                context->registerMemReg((void*)desc.addr, desc.length, access);
        });
    }
    for (auto& task : tasks) task.get();
    for (size_t id = 0; id < context_list_.size(); ++id) {
        if (!context_list_[id]) continue;
        if (!mem_reg_list[id]) {
            return Status::RdmaError(
                "Unable to register buffer of local memory segment" LOC_MARK);
        }
        staging.mem_reg_map[context_list_[id]] = mem_reg_list[id];
        auto keys = context_list_[id]->queryMemRegKey(mem_reg_list[id]);
        desc.lkey.push_back(keys.first);
        desc.rkey.push_back(keys.second);
    }
    staging.options = options;
    RWSpinlock::WriteGuard guard(lock_);
    buffer_list_[range] = staging;
    return Status::OK();
}

Status LocalBufferManager::addBuffer(std::vector<BufferDesc>& desc_list,
                                     const MemoryOptions& options) {
    for (auto& desc : desc_list) {
        CHECK_STATUS(addBuffer(desc, options));
    }
    return Status::OK();
}

Status LocalBufferManager::removeBuffer(BufferDesc& desc) {
    RWSpinlock::WriteGuard guard(lock_);
    AddressRange range((void*)desc.addr, desc.length);
    auto& item = buffer_list_[range];
    for (auto& elem : item.mem_reg_map) {
        elem.first->unregisterMemReg(elem.second);
    }
    desc.rkey.clear();
    buffer_list_.erase(range);
    return Status::OK();
}

Status LocalBufferManager::addDevice(RdmaContext* context) {
    RWSpinlock::WriteGuard guard(lock_);
    assert(topology_ && context);
    int index = topology_->getNicId(context->name());
    if (index < 0) {
        LOG(ERROR) << "Device " << context->name()
                   << " not found in the local segment";
        return Status::DeviceNotFound(
            "Device not found in the local segment" LOC_MARK);
    }

    if (context_list_[index]) {
        LOG(WARNING) << "Device " << context->name()
                     << " already exists in the local segment";
    }
    context_list_[index] = context;
    for (auto& buffer : buffer_list_) {
        auto range = buffer.first;
        auto& options = buffer.second.options;
        auto access = getAccessFlags(options.perm);
        if (buffer.second.mem_reg_map.count(context)) continue;
        auto mem_reg =
            context->registerMemReg(range.addr, range.length, access);
        if (!mem_reg)
            return Status::RdmaError(
                "Device cannot register memory buffer" LOC_MARK);
        buffer.second.mem_reg_map[context] = mem_reg;
    }
    return Status::OK();
}

Status LocalBufferManager::removeDevice(RdmaContext* context, bool do_unreg) {
    RWSpinlock::WriteGuard guard(lock_);
    assert(topology_ && context);
    auto iter = std::find(context_list_.begin(), context_list_.end(), context);
    if (iter == context_list_.end()) return Status::OK();
    for (auto& buffer : buffer_list_) {
        if (!buffer.second.mem_reg_map.count(context)) continue;
        if (do_unreg)
            context->unregisterMemReg(buffer.second.mem_reg_map[context]);
        buffer.second.mem_reg_map.erase(context);
    }
    *iter = nullptr;
    return Status::OK();
}

Status LocalBufferManager::clear() {
    RWSpinlock::WriteGuard guard(lock_);
    for (auto& buffer : buffer_list_) {
        for (auto& elem : buffer.second.mem_reg_map)
            elem.first->unregisterMemReg(elem.second);
    }
    buffer_list_.clear();
    context_list_.clear();
    return Status::OK();
}

}  // namespace tent
}  // namespace mooncake