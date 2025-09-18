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
#include <filesystem>
#include <set>

#include "v1/common/status.h"
#include "v1/memory/location.h"
#include "v1/metadata/metadata.h"
#include "v1/utility/system.h"

namespace mooncake {
namespace v1 {
SegmentManager::SegmentManager(std::unique_ptr<MetadataStore> agent)
    : next_id_(1), version_(0), store_(std::move(agent)) {
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
        LOG(INFO) << "Opened segment #" << handle << ": " << segment_name;
        version_.fetch_add(1, std::memory_order_relaxed);
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
    version_.fetch_add(1, std::memory_order_relaxed);
    return Status::OK();
}

Status SegmentManager::getRemoteCached(SegmentDesc *&desc, SegmentID handle) {
    auto &cache = tl_remote_cache_.get();
    auto current_ts = getCurrentTimeInNano();
    auto current_version = version_.load(std::memory_order_relaxed);
    if (current_ts - cache.last_refresh > ttl_ms_ * 1000000 ||
        cache.version != current_version) {
        cache.id_to_desc_map.clear();
        cache.last_refresh = current_ts;
        cache.version = current_version;
    }
    if (!cache.id_to_desc_map.count(handle)) {
        SegmentDescRef desc_ref;
        auto status = getRemote(desc_ref, handle);
        if (!status.ok()) return status;
        cache.id_to_desc_map[handle] = std::move(desc_ref);
    }
    desc = cache.id_to_desc_map[handle].get();
    assert(desc);
    return Status::OK();
}

Status SegmentManager::getRemote(SegmentDescRef &desc, SegmentID handle) {
    RWSpinlock::WriteGuard guard(lock_);
    if (!id_to_name_map_.count(handle)) {
        return Status::InvalidArgument("Invalid segment handle" LOC_MARK);
    }
    auto segment_name = id_to_name_map_[handle];
    if (segment_name.starts_with(kLocalFileSegmentPrefix)) {
        CHECK_STATUS(makeFileRemote(desc, segment_name));
    } else {
        CHECK_STATUS(store_->getSegmentDesc(desc, segment_name));
    }
    return Status::OK();
}

Status SegmentManager::getRemote(SegmentDescRef &desc,
                                 const std::string &segment_name) {
    return store_->getSegmentDesc(desc, segment_name);
}

Status SegmentManager::invalidateRemote(SegmentID handle) {
    auto &cache = tl_remote_cache_.get();
    if (cache.id_to_desc_map.count(handle)) cache.id_to_desc_map.erase(handle);
    return Status::OK();
}

Status SegmentManager::makeFileRemote(SegmentDescRef &desc,
                                      const std::string &segment_name) {
    std::string path = segment_name.substr(kLocalFileSegmentPrefix.length());
    if (!file_desc_basepath_.empty()) {
        if (file_desc_basepath_.ends_with("/"))
            path = file_desc_basepath_ + path;
        else
            path = file_desc_basepath_ + "/" + path;
    }

    struct stat st;
    if (stat(path.c_str(), &st) || !S_ISREG(st.st_mode))
        return Status::InvalidArgument(std::string("Invalid path: ") + path);

    desc = std::make_shared<SegmentDesc>();
    desc->name = segment_name;
    desc->type = SegmentType::File;
    desc->machine_id = local_desc_->machine_id;
    FileSegmentDesc detail;
    FileBufferDesc buffer;
    buffer.path = path;
    buffer.length = st.st_size;
    buffer.offset = 0;
    detail.buffers.push_back(buffer);
    desc->detail = detail;
    return Status::OK();
}

Status SegmentManager::synchronizeLocal() {
    return store_->putSegmentDesc(local_desc_);
}

Status SegmentManager::deleteLocal() {
    return store_->deleteSegmentDesc(local_desc_->name);
}

Status LocalSegmentTracker::query(uint64_t base, size_t length,
                                  std::vector<BufferDesc *> &result) {
    assert(local_desc_->type == SegmentType::Memory);
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
    assert(local_desc_->type == SegmentType::Memory);
    auto &detail = std::get<MemorySegmentDesc>(local_desc_->detail);
    mutex_.lock();
    for (auto &buf : detail.buffers) {
        if (buf.addr == base && buf.length == length) {
            buf.ref_count++;
            mutex_.unlock();
            return Status::OK();
        }
    }
    mutex_.unlock();
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
    mutex_.lock();
    detail.buffers.push_back(new_desc);
    std::sort(detail.buffers.begin(), detail.buffers.end(),
              [](const BufferDesc &lhs, BufferDesc &rhs) -> bool {
                  if (lhs.addr < rhs.addr) return true;
                  if (lhs.addr > rhs.addr) return false;
                  return lhs.length > rhs.length;  // prefer large interval
              });
    mutex_.unlock();
    return Status::OK();
}

Status LocalSegmentTracker::remove(
    uint64_t base, size_t length,
    std::function<Status(BufferDesc &)> callback) {
    assert(local_desc_->type == SegmentType::Memory);
    auto &detail = std::get<MemorySegmentDesc>(local_desc_->detail);
    mutex_.lock();
    for (auto it = detail.buffers.begin(); it != detail.buffers.end(); ++it) {
        if (it->addr == base && (!length || it->length == length)) {
            it->ref_count--;
            Status status = Status::OK();
            if (it->ref_count == 0) {
                BufferDesc clone = *it;
                detail.buffers.erase(it);
                mutex_.unlock();
                status = callback(clone);
            } else {
                mutex_.unlock();
            }
            return status;
        }
    }
    mutex_.unlock();
    return Status::OK();
}

Status LocalSegmentTracker::forEach(
    std::function<Status(BufferDesc &)> callback) {
    std::lock_guard<std::mutex> lock(mutex_);
    assert(local_desc_->type == SegmentType::Memory);
    auto &detail = std::get<MemorySegmentDesc>(local_desc_->detail);
    for (auto &buf : detail.buffers) {
        auto status = callback(buf);
        if (!status.ok()) return status;
    }
    return Status::OK();
}

}  // namespace v1
}  // namespace mooncake
