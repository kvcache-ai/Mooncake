// Copyright 2024 KVCache.AI
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

#include "v1/metadata/segment.h"

#include <jsoncpp/json/value.h>

#include <cassert>
#include <set>

#include "v1/common.h"
#include "v1/metadata/metadata.h"

namespace mooncake {
namespace v1 {
SegmentManager::SegmentManager(std::unique_ptr<MetadataStore> agent)
    : next_id_(1), store_(std::move(agent)) {
    local_desc_ = std::make_shared<SegmentDesc>();
}

SegmentManager::~SegmentManager() {}

Status SegmentManager::openRemote(SegmentID &handle,
                                  const std::string &segment_name) {
    RWSpinlock::WriteGuard guard(lock_);
    if (name_to_id_map_.count(segment_name)) {
        handle = name_to_id_map_.at(segment_name);
    } else {
        handle = next_id_.fetch_add(1, std::memory_order_relaxed);
        name_to_id_map_[segment_name] = handle;
        id_to_name_map_[handle] = segment_name;
    }
    return Status::OK();
}

Status SegmentManager::closeRemote(SegmentID handle) {
    RWSpinlock::WriteGuard guard(lock_);
    if (!id_to_name_map_.count(handle))
        return Status::InvalidArgument("Invalid segment handle" LOC_MARK);
    auto segment_name = id_to_name_map_[handle];
    name_to_id_map_.erase(segment_name);
    id_to_name_map_.erase(handle);
    id_to_desc_map_.erase(handle);
    return Status::OK();
}

Status SegmentManager::getRemote(SegmentDescRef &desc, SegmentID handle) {
    RWSpinlock::ReadGuard guard(lock_);
    if (!id_to_name_map_.count(handle))
        return Status::InvalidArgument("Invalid segment handle" LOC_MARK);

    if (id_to_desc_map_.count(handle)) {
        desc = id_to_desc_map_[handle];
        return Status::OK();
    }

    auto segment_name = id_to_name_map_[handle];
    auto status = store_->getSegmentDesc(desc, segment_name);
    if (!status.ok()) return status;
    id_to_desc_map_[handle] = desc;
    return Status::OK();
}

Status SegmentManager::getRemote(SegmentDescRef &desc,
                                 const std::string &segment_name) {
    RWSpinlock::ReadGuard guard(lock_);
    if (!name_to_id_map_.count(segment_name))
        return store_->getSegmentDesc(desc, segment_name);

    auto handle = name_to_id_map_.at(segment_name);
    if (id_to_desc_map_.count(handle)) {
        desc = id_to_desc_map_[handle];
        return Status::OK();
    }

    auto status = store_->getSegmentDesc(desc, segment_name);
    if (!status.ok()) return status;
    id_to_desc_map_[handle] = desc;
    return Status::OK();
}

Status SegmentManager::invalidateRemote(SegmentID handle) {
    RWSpinlock::WriteGuard guard(lock_);
    if (!id_to_name_map_.count(handle))
        return Status::InvalidArgument("invalid segment handle" LOC_MARK);
    if (id_to_desc_map_.count(handle)) id_to_desc_map_.erase(handle);
    return Status::OK();
}

Status SegmentManager::synchronizeLocal() {
    return store_->putSegmentDesc(local_desc_);
}

Status SegmentManager::deleteLocal() {
    return store_->deleteSegmentDesc(local_desc_->name);
}

Status SegmentDesc::query(uint64_t base, size_t length,
                          std::vector<BufferDesc> &result) {
    RWSpinlock::ReadGuard guard(lock_);
    auto &detail_cast = std::get<MemorySegmentDesc>(detail);
    result.clear();
    uint64_t current = base;
    for (auto &entry : detail_cast.buffers) {
        uint64_t entry_end = entry.addr + entry.length;
        // check if it is non-overlapped
        if (entry_end <= base) continue;
        if (entry.addr >= base + length) break;
        // first buffer
        if (current < entry.addr)
            return Status::InvalidArgument("Query segment hole existed");
        auto new_current = std::min(base + length, entry_end);
        if (new_current - current > 0) {
            result.push_back(entry);
            current = new_current;
        }
    }
    if (current < base + length)
        return Status::InvalidArgument("Query segment hole existed");
    return Status::OK();
}

Status SegmentDesc::update(
    uint64_t base, size_t length,
    std::function<void(BufferDesc &)> on_update_callback) {
    RWSpinlock::WriteGuard guard(lock_);
    auto &detail_cast = std::get<MemorySegmentDesc>(detail);
    uint64_t current = base;
    std::vector<BufferDesc> to_insert;
    for (auto &entry : detail_cast.buffers) {
        uint64_t entry_end = entry.addr + entry.length;
        // check if it is non-overlapped
        if (entry_end <= base) continue;
        if (entry.addr >= base + length) break;
        // first buffer
        if (current < entry.addr) {
            BufferDesc desc;
            desc.addr = current;
            desc.length = entry.addr - current;
            desc.type = MemBufferType::UNKNOWN;
            on_update_callback(desc);
            to_insert.push_back(desc);
        }
        auto new_current = std::min(base + length, entry_end);
        if (new_current - current > 0) {
            on_update_callback(entry);
            current = new_current;
        }
    }

    if (current < base + length) {
        BufferDesc desc;
        desc.addr = current;
        desc.length = base + length - current;
        desc.type = MemBufferType::UNKNOWN;
        on_update_callback(desc);
        to_insert.push_back(desc);
    }

    if (!to_insert.empty()) {
        detail_cast.buffers.insert(detail_cast.buffers.end(), to_insert.begin(),
                                   to_insert.end());
        std::sort(detail_cast.buffers.begin(), detail_cast.buffers.end(),
                  [](const BufferDesc &lhs, const BufferDesc &rhs) {
                      return lhs.addr < rhs.addr;
                  });
    }
    return Status::OK();
}

}  // namespace v1
}  // namespace mooncake
