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

namespace mooncake {
namespace tent {
namespace {
void sortBuffers(std::vector<BufferDesc>& buffers) {
    std::sort(buffers.begin(), buffers.end(),
              [](const BufferDesc& lhs, const BufferDesc& rhs) -> bool {
                  if (lhs.addr < rhs.addr) return true;
                  if (lhs.addr > rhs.addr) return false;
                  return lhs.length > rhs.length;  // prefer large interval
              });
}

bool containsBuffer(const SegmentDesc& desc, uint64_t base, size_t length) {
    auto& detail = std::get<MemorySegmentDesc>(desc.detail);
    for (auto& buf : detail.buffers) {
        if (buf.addr == base && buf.length == length) return true;
    }
    return false;
}
}  // namespace

Status SegmentTracker::add(uint64_t base, size_t length,
                           std::function<Status(BufferDesc&)> callback) {
    // Read-only pre-scan on the current snapshot: the common miss path pays
    // no clone/publication. A hit is re-verified under the writer mutex; a
    // racing insert of the same range degenerates to today's benign
    // duplicate-registration behavior.
    if (containsBuffer(*manager_.getLocal(), base, length)) {
        bool found = false;
        CHECK_STATUS(manager_.updateLocal([&](SegmentDesc& desc) -> Status {
            assert(desc.type == SegmentType::Memory);
            auto& detail = std::get<MemorySegmentDesc>(desc.detail);
            for (auto& buf : detail.buffers) {
                if (buf.addr == base && buf.length == length) {
                    buf.ref_count++;
                    found = true;
                    break;
                }
            }
            return Status::OK();
        }));
        if (found) return Status::OK();
    }
    BufferDesc new_desc;
    new_desc.addr = base;
    new_desc.length = length;
    auto entries = Platform::getLoader().getLocation((void*)base, length);
    if (entries.size() == 1)
        new_desc.location = entries[0].location;
    else {
        new_desc.location = entries[0].location;
        new_desc.regions = coalesceRegions(entries);
    }
    new_desc.ref_count = 1;
    auto status = callback(new_desc);
    if (!status.ok()) return status;
    return manager_.updateLocal([&](SegmentDesc& desc) -> Status {
        assert(desc.type == SegmentType::Memory);
        auto& detail = std::get<MemorySegmentDesc>(desc.detail);
        detail.buffers.push_back(std::move(new_desc));
        sortBuffers(detail.buffers);
        return Status::OK();
    });
}

Status SegmentTracker::addInBatch(
    std::vector<BufferDesc>& desc_list,
    std::function<Status(std::vector<BufferDesc>&)> callback) {
    std::vector<BufferDesc> new_desc_list;
    // Read-only pre-scan (see add()): skip the ref-count publication when no
    // entry duplicates an already-registered range.
    bool any_dup = false;
    {
        auto snapshot = manager_.getLocal();
        for (auto& entry : desc_list) {
            if (containsBuffer(*snapshot, entry.addr, entry.length)) {
                any_dup = true;
                break;
            }
        }
    }
    if (any_dup) {
        CHECK_STATUS(manager_.updateLocal([&](SegmentDesc& desc) -> Status {
            assert(desc.type == SegmentType::Memory);
            auto& detail = std::get<MemorySegmentDesc>(desc.detail);
            for (auto& entry : desc_list) {
                bool found = false;
                for (auto& buf : detail.buffers) {
                    if (buf.addr == entry.addr && buf.length == entry.length) {
                        buf.ref_count++;
                        found = true;
                        break;
                    }
                }
                if (!found) new_desc_list.push_back(std::move(entry));
            }
            return Status::OK();
        }));
    } else {
        new_desc_list = std::move(desc_list);
    }
    auto status = callback(new_desc_list);
    if (!status.ok()) return status;
    return manager_.updateLocal([&](SegmentDesc& desc) -> Status {
        assert(desc.type == SegmentType::Memory);
        auto& detail = std::get<MemorySegmentDesc>(desc.detail);
        for (auto& new_desc : new_desc_list) {
            detail.buffers.push_back(new_desc);
        }
        sortBuffers(detail.buffers);
        return Status::OK();
    });
}

Status SegmentTracker::remove(uint64_t base, size_t length,
                              std::function<Status(BufferDesc&)> callback) {
    bool removed = false;
    BufferDesc removed_desc;
    CHECK_STATUS(manager_.updateLocal([&](SegmentDesc& desc) -> Status {
        assert(desc.type == SegmentType::Memory);
        auto& detail = std::get<MemorySegmentDesc>(desc.detail);
        for (auto it = detail.buffers.begin(); it != detail.buffers.end();
             ++it) {
            if (it->addr == base && (!length || it->length == length)) {
                it->ref_count--;
                if (it->ref_count == 0) {
                    removed_desc = *it;
                    detail.buffers.erase(it);
                    removed = true;
                }
                break;
            }
        }
        return Status::OK();
    }));
    if (removed) return callback(removed_desc);
    return Status::OK();
}

Status SegmentTracker::forEach(
    std::function<Status(const BufferDesc&)> callback) {
    auto snapshot = manager_.getLocal();
    assert(snapshot->type == SegmentType::Memory);
    for (const auto& buf :
         std::get<MemorySegmentDesc>(snapshot->detail).buffers) {
        auto status = callback(buf);
        if (!status.ok()) return status;
    }
    return Status::OK();
}

}  // namespace tent
}  // namespace mooncake
