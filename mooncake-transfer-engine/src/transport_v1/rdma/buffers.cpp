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

// Helper function to find the position where a range would be inserted
std::vector<AddressRangeManager::AddressRangeRC>::iterator
AddressRangeManager::findInsertPosition(const AddressRange &range) {
    return lower_bound(addr_list.begin(), addr_list.end(), (char *)range.addr,
                       [](const AddressRangeRC &ar, const char *target) {
                           return static_cast<char *>(ar.addr) < target;
                       });
}

// Helper function to check if two ranges overlap
bool AddressRangeManager::rangesOverlap(const AddressRangeRC &a,
                                        const AddressRange &b) {
    char *a_start = static_cast<char *>(a.addr);
    char *a_end = a_start + a.length;
    char *b_start = static_cast<char *>(b.addr);
    char *b_end = b_start + b.length;
    return !(a_end <= b_start || a_start >= b_end);
}

void AddressRangeManager::add(const AddressRange &range,
                              std::vector<AddressRange> &reg_parts) {
    if (range.length == 0) return;

    char *range_start = static_cast<char *>(range.addr);
    char *range_end = range_start + range.length;

    // Find the position to insert this range
    auto insert_pos = findInsertPosition(range);

    // Check if the range overlaps with any existing ranges
    bool found_overlapping = false;

    // Check ranges before the insert position
    if (insert_pos != addr_list.begin()) {
        auto prev = insert_pos - 1;
        if (rangesOverlap(*prev, range)) {
            found_overlapping = true;
        }
    }

    // Check ranges at and after the insert position
    for (auto it = insert_pos; it != addr_list.end(); ++it) {
        if (!rangesOverlap(*it, range)) {
            break;  // Since the list is sorted, no more overlaps possible
        }
        found_overlapping = true;
    }

    if (!found_overlapping) {
        // No overlap, just add the new range
        addr_list.insert(insert_pos,
                         AddressRangeRC(range.addr, range.length, 1));
        reg_parts.push_back(range);
        return;
    }

    // There are overlapping ranges, need to update the reference count
    // for overlapping parts and split the range if necessary

    // Convert the new range into a list of intervals to process
    std::vector<std::pair<char *, char *>> intervals;
    intervals.emplace_back(range_start, range_end);

    // Iterate through existing ranges and process each interval
    for (auto &existing : addr_list) {
        if (intervals.empty()) break;

        char *existing_start = static_cast<char *>(existing.addr);
        char *existing_end = existing_start + existing.length;

        std::vector<std::pair<char *, char *>> new_intervals;

        for (auto &interval : intervals) {
            char *int_start = interval.first;
            char *int_end = interval.second;

            if (int_end <= existing_start || int_start >= existing_end) {
                // No overlap, keep this interval
                new_intervals.push_back(interval);
            } else {
                // There is overlap with existing range
                // Check if part of the interval is before the existing
                // range
                if (int_start < existing_start) {
                    new_intervals.emplace_back(int_start, existing_start);
                }
                // Check if part of the interval is after the existing range
                if (int_end > existing_end) {
                    new_intervals.emplace_back(existing_end, int_end);
                }
                // Increment the reference count of the overlapping part
                existing.ref_cnt++;
            }
        }

        intervals.swap(new_intervals);
    }

    // Add any remaining non-overlapping parts of the new range
    for (auto &interval : intervals) {
        if (interval.second > interval.first) {
            addr_list.emplace_back(static_cast<void *>(interval.first),
                                   interval.second - interval.first, 1);
            reg_parts.push_back(
                AddressRange{static_cast<void *>(interval.first),
                             size_t(interval.second - interval.first)});
        }
    }

    // Sort the addr_list again after potential insertions
    sort(addr_list.begin(), addr_list.end(),
         [](const AddressRangeRC &a, const AddressRangeRC &b) {
             return static_cast<char *>(a.addr) < static_cast<char *>(b.addr);
         });
}

void AddressRangeManager::remove(const AddressRange &range,
                                 std::vector<AddressRange> &dereg_parts) {
    if (range.length == 0) return;

    dereg_parts.clear();

    char *range_start = static_cast<char *>(range.addr);
    char *range_end = range_start + range.length;

    // Iterate through existing ranges and process each range
    std::vector<AddressRangeRC> new_list;

    for (auto &existing : addr_list) {
        char *existing_start = static_cast<char *>(existing.addr);
        char *existing_end = existing_start + existing.length;

        if (range_end <= existing_start || range_start >= existing_end) {
            // No overlap, keep this range as is
            new_list.push_back(existing);
            continue;
        }

        // There is overlap with this existing range
        if (existing.ref_cnt <= 0) {
            // Invalid reference count, skip
            continue;
        }

        // Decrement the reference count
        existing.ref_cnt--;

        bool fully_removed = false;
        if (existing.ref_cnt <= 0) {
            fully_removed = true;
        }

        // Split the existing range into non-removed parts
        if (existing_start < range_start) {
            new_list.emplace_back(existing.addr, range_start - existing_start,
                                  existing.ref_cnt);
            if (fully_removed) {
                dereg_parts.emplace_back(existing.addr,
                                         range_start - existing_start);
            }
        }
        if (existing_end > range_end) {
            new_list.emplace_back(static_cast<void *>(range_end),
                                  existing_end - range_end, existing.ref_cnt);
            if (fully_removed) {
                dereg_parts.emplace_back(static_cast<void *>(range_end),
                                         existing_end - range_end);
            }
        }

        if (fully_removed && existing_start >= range_start &&
            existing_end <= range_end) {
            dereg_parts.emplace_back(existing.addr, existing.length);
        }
    }

    // Replace the addr_list with the new_list
    addr_list = new_list;

    // Sort the addr_list again after potential modifications
    sort(addr_list.begin(), addr_list.end(),
         [](const AddressRangeRC &a, const AddressRangeRC &b) {
             return static_cast<char *>(a.addr) < static_cast<char *>(b.addr);
         });
}

LocalBufferManager::LocalBufferManager() {}

LocalBufferManager::~LocalBufferManager() { clear(); }

static inline int getAccessFlags(Transport::BufferVisibility visibility) {
    int access = IBV_ACCESS_LOCAL_WRITE;
    if (visibility == Transport::kGlobalReadWrite) {
        access |= IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ;
    } else if (visibility == Transport::kGlobalReadOnly) {
        access |= IBV_ACCESS_REMOTE_READ;
    }
    return access;
}

int LocalBufferManager::addBuffer(const Transport::BufferEntry &buffer_entry) {
    RWSpinlock::WriteGuard guard(lock_);
    AddressRange range(buffer_entry.addr, buffer_entry.length);
    std::vector<AddressRange> reg_parts;
    manager_.add(range, reg_parts);
    auto access = getAccessFlags(buffer_entry.visibility);
    for (auto &to_reg : reg_parts) {
        auto &item = buffer_list_[range];
        for (auto &context : context_list_) {
            if (!context) continue;
            auto mem_reg =
                context->registerMemReg(to_reg.addr, to_reg.length, access);
            if (!mem_reg) return ERR_CONTEXT;
            item.mem_reg_map[context] = mem_reg;
        }
        auto location = buffer_entry.location;
        if (location == kWildcardLocation) {
            auto entries = getMemoryLocation(to_reg.addr, to_reg.length);
            if (!entries.empty()) {
                location = entries[0].location;
            }
        }
        item.entry.addr = to_reg.addr;
        item.entry.length = to_reg.length;
        item.entry.location = buffer_entry.location;
        item.entry.visibility = buffer_entry.visibility;
        item.entry.shm_path = buffer_entry.shm_path;
        item.entry.shm_offset =
            buffer_entry.shm_offset +
            ((uint64_t)to_reg.addr - (uint64_t)buffer_entry.addr);
    }
    return 0;
}

int LocalBufferManager::removeBuffer(const AddressRange &range) {
    RWSpinlock::WriteGuard guard(lock_);
    std::vector<AddressRange> dereg_parts;
    manager_.remove(range, dereg_parts);
    for (auto &to_dereg : dereg_parts) {
        auto &item = buffer_list_[to_dereg];
        for (auto &elem : item.mem_reg_map) {
            elem.first->unregisterMemReg(elem.second);
        }
        buffer_list_.erase(to_dereg);
    }
    return 0;
}

int LocalBufferManager::addDevice(RdmaContext *context) {
    RWSpinlock::WriteGuard guard(lock_);
    assert(topology_ && context);
    int index = 0;
    bool found = false;
    for (auto &device : topology_->getHcaList()) {
        if (device == context->name()) {
            if (context_list_[index]) return ERR_CONTEXT;  // has added
            context_list_[index] = context;
            found = true;
            break;
        } else {
            index++;
        }
    }
    if (!found) return ERR_CONTEXT;  // not matched item
    for (auto &buffer : buffer_list_) {
        auto &to_reg = buffer.second.entry;
        auto access = getAccessFlags(to_reg.visibility);
        if (buffer.second.mem_reg_map.count(context)) continue;
        auto mem_reg =
            context->registerMemReg(to_reg.addr, to_reg.length, access);
        if (!mem_reg) return ERR_CONTEXT;
        buffer.second.mem_reg_map[context] = mem_reg;
    }
    return 0;
}

int LocalBufferManager::removeDevice(RdmaContext *context, bool do_unreg) {
    RWSpinlock::WriteGuard guard(lock_);
    assert(topology_ && context);
    auto iter = std::find(context_list_.begin(), context_list_.end(), context);
    if (iter == context_list_.end()) return 0;
    for (auto &buffer : buffer_list_) {
        if (!buffer.second.mem_reg_map.count(context)) continue;
        if (do_unreg)
            context->unregisterMemReg(buffer.second.mem_reg_map[context]);
        buffer.second.mem_reg_map.erase(context);
    }
    *iter = nullptr;
    return 0;
}

int LocalBufferManager::clear() {
    RWSpinlock::WriteGuard guard(lock_);
    for (auto &buffer : buffer_list_) {
        for (auto &elem : buffer.second.mem_reg_map)
            elem.first->unregisterMemReg(elem.second);
    }
    buffer_list_.clear();
    context_list_.clear();
    return 0;
}

int LocalBufferManager::fillBufferDesc(
    std::shared_ptr<SegmentDesc> &segment_desc) {
    RWSpinlock::ReadGuard guard(lock_);
    auto &detail = std::get<MemorySegmentDesc>(segment_desc->detail);
    detail.buffers.clear();
    for (auto &buffer : buffer_list_) {
        BufferDesc buffer_desc;
        buffer_desc.location = buffer.second.entry.location;
        buffer_desc.addr = (uint64_t)buffer.second.entry.addr;
        buffer_desc.length = buffer.second.entry.length;
        for (auto &device : detail.devices) {
            bool found = false;
            for (auto &elem : buffer.second.mem_reg_map) {
                if (elem.first->name() == device.name) {
                    auto keys = elem.first->queryMemRegKey(elem.second);
                    buffer_desc.lkey.push_back(keys.first);
                    buffer_desc.rkey.push_back(keys.second);
                    found = true;
                }
            }
            if (!found) {
                LOG(WARNING)
                    << "Unregistered memory " << (void *)buffer_desc.addr
                    << "--" << (void *)(buffer_desc.addr + buffer_desc.length)
                    << " for device " << device.name;
                // representing invalid value
                buffer_desc.lkey.push_back(UINT32_MAX);
                buffer_desc.rkey.push_back(UINT32_MAX);
            }
        }
        detail.buffers.push_back(buffer_desc);
    }
    return 0;
}

int LocalBufferManager::query(const AddressRange &range,
                              std::vector<BufferQueryResult> &result,
                              int retry_count) {
    RWSpinlock::ReadGuard guard(lock_);
    result.clear();
    for (auto &buffer : buffer_list_) {
        auto intersect = buffer.first.intersect(range);
        if (intersect.empty()) continue;
        int device_id =
            topology_->selectDevice(buffer.second.entry.location, retry_count);
        if (device_id < 0)
            device_id = topology_->selectDevice(kWildcardLocation, retry_count);
        if (device_id < 0) return ERR_ADDRESS_NOT_REGISTERED;
        auto context = context_list_[device_id];
        if (!context) return ERR_ADDRESS_NOT_REGISTERED;
        auto mem_reg_id = buffer.second.mem_reg_map[context];
        auto keys = context->queryMemRegKey(mem_reg_id);
        result.push_back(BufferQueryResult{intersect.addr, intersect.length,
                                           keys.first, keys.second, device_id});
    }
    return 0;
}

const std::string LocalBufferManager::deviceName(int id) {
    RWSpinlock::ReadGuard guard(lock_);
    assert(id >= 0 && id < (int)context_list_.size());
    return context_list_[id] ? context_list_[id]->name() : "unknown";
}

PeerBuffers::PeerBuffers() : segment_desc_(nullptr) {}

PeerBuffers::~PeerBuffers() {}

int PeerBuffers::reload(const std::shared_ptr<SegmentDesc> &segment_desc) {
    lock_.lock();
    segment_desc_ = segment_desc;
    lock_.unlock();
    return 0;
}

int PeerBuffers::query(const AddressRange &range,
                       std::vector<BufferQueryResult> &result,
                       int retry_count) {
    RWSpinlock::ReadGuard guard(lock_);
    auto &detail = std::get<MemorySegmentDesc>(segment_desc_->detail);
    auto &topo = detail.topology;
    result.clear();
    for (auto &entry : detail.buffers) {
        auto query_range = AddressRange{(void *)entry.addr, entry.length};
        auto intersect = query_range.intersect(range);
        if (intersect.empty()) continue;
        int device_id = topo.selectDevice(entry.location, retry_count);
        if (device_id < 0)
            device_id = topo.selectDevice(kWildcardLocation, retry_count);
        if (device_id < 0) return ERR_ADDRESS_NOT_REGISTERED;
        result.push_back(BufferQueryResult{intersect.addr, intersect.length,
                                           entry.lkey[device_id],
                                           entry.rkey[device_id], device_id});
    }
    return 0;
}

const std::string &PeerBuffers::deviceName(int id) {
    RWSpinlock::ReadGuard guard(lock_);
    auto &detail = std::get<MemorySegmentDesc>(segment_desc_->detail);
    assert(id >= 0 && id < (int)detail.devices.size());
    return detail.devices[id].name;
}
}  // namespace v1
}  // namespace mooncake