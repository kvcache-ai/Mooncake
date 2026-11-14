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

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <filesystem>
#include <set>
#include <unordered_map>
#include <utility>

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

struct BufferRangeHash {
    size_t operator()(const std::pair<uint64_t, size_t>& range) const noexcept {
        return std::hash<uint64_t>()(range.first) ^
               (std::hash<size_t>()(range.second) << 1);
    }
};

// Descriptors with the same range are aliases of one logical registration.
// Keep their reference counts in sync and remove them together.
std::vector<size_t> MatchRange(const std::vector<BufferDesc>& buffers,
                               uint64_t addr, size_t length) {
    std::vector<size_t> matches;
    for (size_t i = 0; i < buffers.size(); ++i) {
        if (buffers[i].addr == addr && buffers[i].length == length) {
            matches.push_back(i);
        }
    }
    return matches;
}

void BumpRange(std::vector<BufferDesc>& buffers,
               const std::vector<size_t>& matches) {
    int logical = 0;
    for (size_t index : matches) {
        logical = std::max(logical, buffers[index].ref_count);
    }
    for (size_t index : matches) {
        buffers[index].ref_count = logical + 1;
    }
}
}  // namespace

Status SegmentTracker::add(uint64_t base, size_t length,
                           std::function<Status(BufferDesc&)> callback) {
    std::lock_guard<std::recursive_mutex> guard(mutex_);
    // Skip the ref-count publication when the current snapshot has no match.
    // The tracker mutex serializes registration and removal across callbacks.
    if (containsBuffer(*manager_.getLocal(), base, length)) {
        bool found = false;
        CHECK_STATUS(manager_.updateLocal([&](SegmentDesc& desc) -> Status {
            assert(desc.type == SegmentType::Memory);
            auto& detail = std::get<MemorySegmentDesc>(desc.detail);
            auto matches = MatchRange(detail.buffers, base, length);
            if (!matches.empty()) {
                BumpRange(detail.buffers, matches);
                found = true;
            }
            return Status::OK();
        }));
        if (found) return Status::OK();
    }
    BufferDesc new_desc;
    new_desc.addr = base;
    new_desc.length = length;
    auto entries = Platform::getLoader().getLocation((void*)base, length);
    if (entries.empty())
        return Status::InternalError("Unable to determine buffer location");
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
    std::function<Status(std::vector<BufferDesc>&)> callback,
    std::function<Status(BufferDesc&)> on_removed) {
    std::lock_guard<std::recursive_mutex> guard(mutex_);
    std::vector<BufferDesc> new_desc_list;
    new_desc_list.reserve(desc_list.size());
    std::unordered_map<std::pair<uint64_t, size_t>, size_t, BufferRangeHash>
        new_desc_indices;
    new_desc_indices.reserve(desc_list.size());
    auto appendNewDescriptor = [&](BufferDesc& entry) {
        auto [it, inserted] = new_desc_indices.emplace(
            std::make_pair(entry.addr, entry.length), new_desc_list.size());
        if (!inserted) {
            new_desc_list[it->second].ref_count++;
            return;
        }
        new_desc_list.push_back(std::move(entry));
    };
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
    // Record added references so a failed callback can roll them back.
    // Publish them before the callback, which may reenter the tracker.
    std::vector<std::pair<uint64_t, uint64_t>> bumped;
    if (any_dup) {
        CHECK_STATUS(manager_.updateLocal([&](SegmentDesc& desc) -> Status {
            assert(desc.type == SegmentType::Memory);
            auto& detail = std::get<MemorySegmentDesc>(desc.detail);
            for (auto& entry : desc_list) {
                auto matches =
                    MatchRange(detail.buffers, entry.addr, entry.length);
                if (!matches.empty()) {
                    // One input, including a repeated input in this batch, is
                    // one logical reference on every alias of the range.
                    BumpRange(detail.buffers, matches);
                    bumped.emplace_back(entry.addr, entry.length);
                } else {
                    appendNewDescriptor(entry);
                }
            }
            return Status::OK();
        }));
    } else {
        for (auto& entry : desc_list) appendNewDescriptor(entry);
    }
    auto status = callback(new_desc_list);
    if (!status.ok()) {
        // Roll back the duplicate ref-counts so a failed registration does
        // not leave buffers pinned forever.
        for (const auto& range : bumped) {
            auto rollback =
                remove(range.first, range.second, [&](BufferDesc& desc) {
                    return on_removed ? on_removed(desc) : Status::OK();
                });
            if (!rollback.ok()) LOG(WARNING) << rollback.ToString();
        }
        return status;
    }
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
    std::lock_guard<std::recursive_mutex> guard(mutex_);
    bool removed = false;
    BufferDesc removed_desc;
    std::vector<BufferDesc> removed_group;
    CHECK_STATUS(manager_.updateLocal([&](SegmentDesc& desc) -> Status {
        assert(desc.type == SegmentType::Memory);
        auto& detail = std::get<MemorySegmentDesc>(desc.detail);
        auto first =
            std::find_if(detail.buffers.begin(), detail.buffers.end(),
                         [&](const auto& buffer) {
                             return buffer.addr == base &&
                                    (!length || buffer.length == length);
                         });
        if (first == detail.buffers.end()) return Status::OK();
        const auto matches = MatchRange(detail.buffers, base, first->length);
        int logical = 0;
        for (size_t index : matches) {
            logical = std::max(logical, detail.buffers[index].ref_count);
        }
        if (logical > 1) {
            for (size_t index : matches) {
                detail.buffers[index].ref_count = logical - 1;
            }
            return Status::OK();
        }
        removed_group.reserve(matches.size());
        for (size_t index : matches) {
            removed_group.push_back(detail.buffers[index]);
        }
        for (auto it = matches.rbegin(); it != matches.rend(); ++it) {
            detail.buffers.erase(detail.buffers.begin() +
                                 static_cast<std::ptrdiff_t>(*it));
        }
        removed_desc = removed_group.front();
        removed_desc.ref_count = 0;
        removed = true;
        return Status::OK();
    }));
    if (!removed) return Status::OK();
    auto status = callback(removed_desc);
    if (!status.ok()) {
        // Successful transport removals may have scrubbed their keys. Keep
        // the remaining registrations and one reference for a safe retry.
        if (removed_group.size() == 1) {
            removed_group.front() = std::move(removed_desc);
        }
        CHECK_STATUS(manager_.updateLocal([&](SegmentDesc& desc) {
            auto& buffers = std::get<MemorySegmentDesc>(desc.detail).buffers;
            for (auto& buffer : removed_group) {
                buffer.ref_count = 1;
                buffers.push_back(std::move(buffer));
            }
            sortBuffers(buffers);
            return Status::OK();
        }));
    }
    return status;
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
