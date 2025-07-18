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
#include "v1/utility/memory_location.h"

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
    LOG(INFO) << handle << " " << segment_name;
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
    {
        RWSpinlock::ReadGuard guard(lock_);
        if (!id_to_name_map_.count(handle)) {
            return Status::InvalidArgument("Invalid segment handle" LOC_MARK);
        }
        if (id_to_desc_map_.count(handle)) {
            desc = id_to_desc_map_[handle];
            return Status::OK();
        }
    }

    RWSpinlock::WriteGuard guard(lock_);
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
    {
        RWSpinlock::ReadGuard guard(lock_);
        if (!name_to_id_map_.count(segment_name))
            return store_->getSegmentDesc(desc, segment_name);
        auto handle = name_to_id_map_.at(segment_name);
        if (id_to_desc_map_.count(handle)) {
            desc = id_to_desc_map_[handle];
            return Status::OK();
        }
    }

    RWSpinlock::WriteGuard guard(lock_);
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

bool SegmentManager::isSameMachine(SegmentID handle) {
    if (handle == LOCAL_SEGMENT_ID) return true;
    RWSpinlock::ReadGuard guard(lock_);
    if (id_to_desc_map_.count(handle)) {
        return id_to_desc_map_[handle]->machine_id == local_desc_->machine_id;
    }
    return false;
}

Status SegmentManager::synchronizeLocal() {
    return store_->putSegmentDesc(local_desc_);
}

Status SegmentManager::deleteLocal() {
    return store_->deleteSegmentDesc(local_desc_->name);
}

Status RemoteSegmentCache::get(SegmentDesc *&desc, SegmentID handle) {
    auto current_timestamp = getCurrentTimeInNano();
    if (current_timestamp - last_refresh_timestamp_ > ttl_ms_ * 1000000) {
        id_to_desc_map_.clear();
        last_refresh_timestamp_ = current_timestamp;
    }
    if (!id_to_desc_map_.count(handle)) {
        SegmentDescRef desc_ref;
        auto status = manager_.getRemote(desc_ref, handle);
        if (!status.ok()) return status;
        id_to_desc_map_[handle] = std::move(desc_ref);
    }
    desc = id_to_desc_map_[handle].get();
    return Status::OK();
}

Status LocalSegmentTracker::query(uint64_t base, size_t length,
                                  std::vector<BufferDesc *> &result) {
    auto &detail = std::get<MemorySegmentDesc>(local_desc_->detail);
    assert(length);
    for (auto &buf : detail.buffers) {
        if (buf.addr > base) continue;
        if (buf.addr + buf.length <= base) break;
        result.push_back(&buf);
        if (buf.addr + buf.length >= base + length) {
            return Status::OK();
        } else {
            auto new_base = buf.addr + buf.length;
            auto new_length = base + length - new_base;
            return query(new_base, new_length, result);
        }
    }
    return Status::InvalidArgument("Some buffers are not registered");
}

Status LocalSegmentTracker::add(uint64_t base, size_t length,
                                std::function<Status(BufferDesc &)> callback) {
    auto &detail = std::get<MemorySegmentDesc>(local_desc_->detail);
    for (auto &buf : detail.buffers) {
        if (buf.addr == base && buf.length == length) {
            buf.ref_count++;
            return Status::OK();
        }
    }
    BufferDesc new_desc;
    new_desc.addr = base;
    new_desc.length = length;
    auto entries = getMemoryLocation((void *)base, length);
    if (!entries.empty())
        new_desc.location = entries[0].location;
    else
        new_desc.location = kWildcardLocation;
    new_desc.ref_count = 1;
    auto status = callback(new_desc);
    if (!status.ok()) return status;
    detail.buffers.push_back(new_desc);
    std::sort(detail.buffers.begin(), detail.buffers.end(),
              [](const BufferDesc &lhs, BufferDesc &rhs) -> bool {
                  if (lhs.addr < rhs.addr) return true;
                  if (lhs.addr > rhs.addr) return false;
                  return lhs.length > rhs.length;  // prefer large interval
              });
    return Status::OK();
}

Status LocalSegmentTracker::remove(
    uint64_t base, size_t length,
    std::function<Status(BufferDesc &)> callback) {
    auto &detail = std::get<MemorySegmentDesc>(local_desc_->detail);
    for (auto it = detail.buffers.begin(); it != detail.buffers.end(); ++it) {
        if (it->addr == base && it->length == length) {
            it->ref_count--;
            Status status = Status::OK();
            if (it->ref_count == 0) {
                status = callback(*it);
                detail.buffers.erase(it);
            }
            return status;
        }
    }
    return Status::OK();
}

Status LocalSegmentTracker::update(
    std::function<Status(BufferDesc &)> callback) {
    auto &detail = std::get<MemorySegmentDesc>(local_desc_->detail);
    for (auto &buf : detail.buffers) {
        auto status = callback(buf);
        if (!status.ok()) return status;
    }
    return Status::OK();
}

}  // namespace v1
}  // namespace mooncake
