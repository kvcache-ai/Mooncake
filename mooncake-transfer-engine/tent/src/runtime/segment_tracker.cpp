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

#include "tent/runtime/segment_tracker.h"

#include <cassert>
#include <filesystem>
#include <set>

#include "tent/common/status.h"
#include "tent/runtime/platform.h"
#include "tent/runtime/control_plane.h"
#include "tent/runtime/segment_registry.h"
#include "tent/common/utils/os.h"

namespace mooncake {
namespace tent {
Status SegmentTracker::query(uint64_t base, size_t length,
                             std::vector<BufferDesc*>& result) {
    assert(local_desc_->type == SegmentType::Memory);
    auto& detail = std::get<MemorySegmentDesc>(local_desc_->detail);
    assert(length);
    for (auto& buf : detail.buffers) {
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

Status SegmentTracker::add(uint64_t base, size_t length,
                           std::function<Status(BufferDesc&)> callback) {
    assert(local_desc_->type == SegmentType::Memory);
    auto& detail = std::get<MemorySegmentDesc>(local_desc_->detail);
    mutex_.lock();
    for (auto& buf : detail.buffers) {
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
    auto entries = Platform::getLoader().getLocation((void*)base, length);
    if (entries.size() == 1)
        new_desc.location = entries[0].location;
    else {
        new_desc.location = entries[0].location;
        for (auto& entry : entries)
            new_desc.regions.push_back(Region{entry.len, entry.location});
    }
    new_desc.ref_count = 1;
    auto status = callback(new_desc);
    if (!status.ok()) return status;
    mutex_.lock();
    detail.buffers.push_back(new_desc);
    std::sort(detail.buffers.begin(), detail.buffers.end(),
              [](const BufferDesc& lhs, BufferDesc& rhs) -> bool {
                  if (lhs.addr < rhs.addr) return true;
                  if (lhs.addr > rhs.addr) return false;
                  return lhs.length > rhs.length;  // prefer large interval
              });
    mutex_.unlock();
    return Status::OK();
}

Status SegmentTracker::addInBatch(
    std::vector<void*> base_list, std::vector<size_t> length_list,
    std::function<Status(std::vector<BufferDesc>&)> callback) {
    assert(base_list.size() == length_list.size());
    std::vector<BufferDesc> new_desc_list;
    for (size_t i = 0; i < base_list.size(); ++i) {
        uint64_t base = (uint64_t)base_list[i];
        size_t length = length_list[i];
        bool found = false;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            assert(local_desc_->type == SegmentType::Memory);
            auto& detail = std::get<MemorySegmentDesc>(local_desc_->detail);
            for (auto& buf : detail.buffers) {
                if (buf.addr == base && buf.length == length) {
                    buf.ref_count++;
                    found = true;
                    break;
                }
            }
        }
        if (found) continue;
        BufferDesc new_desc;
        new_desc.addr = base;
        new_desc.length = length;
        auto entries = Platform::getLoader().getLocation((void*)base, length);
        if (entries.size() == 1)
            new_desc.location = entries[0].location;
        else {
            new_desc.location = entries[0].location;
            for (auto& entry : entries)
                new_desc.regions.push_back(Region{entry.len, entry.location});
        }
        new_desc.ref_count = 1;
        new_desc_list.push_back(new_desc);
    }
    // auto start_ts = getCurrentTimeInNano();
    auto status = callback(new_desc_list);
    // auto end_ts = getCurrentTimeInNano();
    // LOG(INFO) << "Reg time: " << (end_ts - start_ts) / 1000000.0 << " ms";
    if (!status.ok()) return status;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        assert(local_desc_->type == SegmentType::Memory);
        auto& detail = std::get<MemorySegmentDesc>(local_desc_->detail);
        for (auto& new_desc : new_desc_list) {
            detail.buffers.push_back(new_desc);
        }
        std::sort(detail.buffers.begin(), detail.buffers.end(),
                  [](const BufferDesc& lhs, BufferDesc& rhs) -> bool {
                      if (lhs.addr < rhs.addr) return true;
                      if (lhs.addr > rhs.addr) return false;
                      return lhs.length > rhs.length;  // prefer large interval
                  });
    }
    return Status::OK();
}

Status SegmentTracker::remove(uint64_t base, size_t length,
                              std::function<Status(BufferDesc&)> callback) {
    assert(local_desc_->type == SegmentType::Memory);
    auto& detail = std::get<MemorySegmentDesc>(local_desc_->detail);
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

Status SegmentTracker::forEach(std::function<Status(BufferDesc&)> callback) {
    std::lock_guard<std::mutex> lock(mutex_);
    assert(local_desc_->type == SegmentType::Memory);
    auto& detail = std::get<MemorySegmentDesc>(local_desc_->detail);
    for (auto& buf : detail.buffers) {
        auto status = callback(buf);
        if (!status.ok()) return status;
    }
    return Status::OK();
}

}  // namespace tent
}  // namespace mooncake
