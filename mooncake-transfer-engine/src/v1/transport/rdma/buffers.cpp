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

#include "v1/transport/rdma/buffers.h"

#include "v1/transport/rdma/context.h"

namespace mooncake {
namespace v1 {
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

Status LocalBufferManager::addBuffer(BufferDesc &desc,
                                     const MemoryOptions &options) {
    RWSpinlock::WriteGuard guard(lock_);
    AddressRange range((void *)desc.addr, desc.length);
    auto access = getAccessFlags(options.perm);
    auto &item = buffer_list_[range];
    assert(desc.rkey.empty());
    for (auto &context : context_list_) {
        if (!context) continue;
        auto mem_reg =
            context->registerMemReg((void *)desc.addr, desc.length, access);
        if (!mem_reg)
            return Status::RdmaError(
                "Failed to register memory region" LOC_MARK);
        item.mem_reg_map[context] = mem_reg;
        auto keys = context->queryMemRegKey(mem_reg);
        desc.lkey.push_back(keys.first);
        desc.rkey.push_back(keys.second);
    }
    item.options = options;
    auto &location = item.options.location;
    if (location == kWildcardLocation) {
        auto entries = getMemoryLocation((void *)desc.addr, desc.length);
        if (!entries.empty()) location = entries[0].location;
    }
    desc.location = location;
    return Status::OK();
}

Status LocalBufferManager::removeBuffer(BufferDesc &desc) {
    RWSpinlock::WriteGuard guard(lock_);
    AddressRange range((void *)desc.addr, desc.length);
    auto &item = buffer_list_[range];
    for (auto &elem : item.mem_reg_map) {
        elem.first->unregisterMemReg(elem.second);
    }
    desc.rkey.clear();
    buffer_list_.erase(range);
    return Status::OK();
}

Status LocalBufferManager::addDevice(RdmaContext *context) {
    RWSpinlock::WriteGuard guard(lock_);
    assert(topology_ && context);
    int index = 0;
    bool found = false;
    for (auto &device : topology_->getDeviceList()) {
        if (device == context->name()) {
            if (context_list_[index])
                return Status::InvalidArgument(
                    "Context existed in local buffer manager" LOC_MARK);
            context_list_[index] = context;
            found = true;
            break;
        } else {
            index++;
        }
    }
    if (!found) return Status::DeviceNotFound("Not matched device" LOC_MARK);
    for (auto &buffer : buffer_list_) {
        auto range = buffer.first;
        auto &options = buffer.second.options;
        auto access = getAccessFlags(options.perm);
        if (buffer.second.mem_reg_map.count(context)) continue;
        auto mem_reg =
            context->registerMemReg(range.addr, range.length, access);
        if (!mem_reg)
            return Status::RdmaError(
                "Failed to register memory region" LOC_MARK);
        buffer.second.mem_reg_map[context] = mem_reg;
    }
    return Status::OK();
}

Status LocalBufferManager::removeDevice(RdmaContext *context, bool do_unreg) {
    RWSpinlock::WriteGuard guard(lock_);
    assert(topology_ && context);
    auto iter = std::find(context_list_.begin(), context_list_.end(), context);
    if (iter == context_list_.end()) return Status::OK();
    for (auto &buffer : buffer_list_) {
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
    for (auto &buffer : buffer_list_) {
        for (auto &elem : buffer.second.mem_reg_map)
            elem.first->unregisterMemReg(elem.second);
    }
    buffer_list_.clear();
    context_list_.clear();
    return Status::OK();
}

Status LocalBufferManager::query(const AddressRange &range,
                                 std::vector<BufferQueryResult> &result,
                                 int retry_count) {
    // TODO Read-friendly lock
    result.clear();
    auto lower = buffer_list_.begin();
    auto upper = buffer_list_.end();
    for (auto it = lower; it != upper; ++it) {
        const auto &buffer = *it;
        auto intersect = buffer.first.intersect(range);
        if (intersect.empty()) continue;
        int device_id;
        auto status = topology_->selectDevice(
            device_id, buffer.second.options.location, retry_count);
        if (!status.ok())
            status = topology_->selectDevice(device_id, kWildcardLocation,
                                             retry_count);
        if (!status.ok()) return status;
        auto context = context_list_[device_id];
        assert(context);
        auto mem_reg_id = buffer.second.mem_reg_map.at(context);
        auto keys = context->queryMemRegKey(mem_reg_id);
        result.push_back(BufferQueryResult{intersect.addr, intersect.length,
                                           keys.first, keys.second, device_id});
    }
    if (result.empty()) {
        return Status::AddressNotRegistered(
            "No matched buffer in given address range" LOC_MARK);
    }
    return Status::OK();
}

const std::string LocalBufferManager::deviceName(int id) {
    RWSpinlock::ReadGuard guard(lock_);
    assert(id >= 0 && id < (int)context_list_.size());
    return context_list_[id] ? context_list_[id]->name() : "unknown";
}

Status queryRemoteSegment(SegmentDesc *desc, const AddressRange &range,
                          std::vector<BufferQueryResult> &result,
                          int retry_count) {
    if (desc->type != SegmentType::Memory)
        return Status::InvalidArgument("Invalid remote segment type");
    auto &detail = std::get<MemorySegmentDesc>(desc->detail);
    auto &topo = detail.topology;
    result.clear();
    for (auto &entry : detail.buffers) {
        auto query_range = AddressRange{(void *)entry.addr, entry.length};
        auto intersect = query_range.intersect(range);
        if (intersect.empty()) continue;
        int device_id;
        auto status = topo.selectDevice(device_id, entry.location, retry_count);
        if (!status.ok())
            status =
                topo.selectDevice(device_id, kWildcardLocation, retry_count);
        if (!status.ok()) return status;
        result.push_back(BufferQueryResult{intersect.addr, intersect.length, 0,
                                           entry.rkey[device_id], device_id});
    }
    if (result.empty()) {
        return Status::AddressNotRegistered(
            "No matched buffer in given address range" LOC_MARK);
    }
    return Status::OK();
}

}  // namespace v1
}  // namespace mooncake