// Copyright 2025 Huawei Technologies Co., Ltd
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

#include "transport/ascend_transport/ascend_direct_transport/shared_host_mapper.h"

#include <glog/logging.h>

#include <sys/mman.h>
#include <unistd.h>

#include "shared_segment/host_memfd.h"

namespace mooncake {
namespace {

const TransferMetadata::BufferDesc *BufferContaining(
    const std::vector<TransferMetadata::BufferDesc> &buffers,
    uint64_t dest_addr, uint64_t length) {
    for (const auto &buffer : buffers) {
        if (dest_addr < buffer.addr) {
            continue;
        }
        const uint64_t offset = dest_addr - buffer.addr;
        if (offset >= buffer.length || buffer.length - offset < length) {
            continue;
        }
        return &buffer;
    }
    return nullptr;
}

}  // namespace

SharedHostMapper::~SharedHostMapper() {
    std::lock_guard<std::mutex> lock(mutex_);
    for (auto &entry : regions_) {
        auto &region = entry.second;
        if (region.owns_mapping && region.local_base != nullptr) {
            (void)munmap(region.local_base, region.length);
        }
    }
    regions_.clear();
}

const SharedHostMapper::ImportedRegion *SharedHostMapper::ResolveHandle(
    const std::string &handle, uint64_t remote_base, uint64_t length) {
    auto it = regions_.find(handle);
    if (it != regions_.end()) {
        return it->second.local_base != nullptr ? &it->second : nullptr;
    }

    // Anything this does not accept was exported on another machine, or is
    // missing a guard that would let it resolve to the wrong memory. Those go
    // out through adxl.
    host_memfd::ParsedHandle parsed;
    if (!host_memfd::ParseLocalHandle(handle, parsed)) {
        regions_.emplace(handle, ImportedRegion{});
        return nullptr;
    }

    ImportedRegion region;
    region.length = length;

    if (parsed.owner_pid == static_cast<int>(getpid())) {
        // Our own allocation. The address is already valid here, so mapping it
        // a second time would only burn address space.
        region.local_base = reinterpret_cast<void *>(remote_base);
        region.owns_mapping = false;
    } else {
        void *window =
            host_memfd::ReserveAlignedWindow(length, host_memfd::Granularity());
        if (window == MAP_FAILED) {
            LOG(WARNING) << "Cannot reserve " << length
                         << " bytes to import shared host segment " << handle
                         << "; falling back to adxl";
            regions_.emplace(handle, ImportedRegion{});
            return nullptr;
        }
        std::string error;
        if (!host_memfd::Import(handle, length, window, error)) {
            // Expected whenever the peer is on another machine or in another
            // PID namespace, so this is not an error for the transfer: the
            // caller simply keeps using adxl.
            (void)munmap(window, length);
            LOG(INFO) << "Cannot import shared host segment " << handle << ": "
                      << error << "; falling back to adxl";
            regions_.emplace(handle, ImportedRegion{});
            return nullptr;
        }
        region.local_base = window;
        region.owns_mapping = true;
        LOG(INFO) << "Imported shared host segment " << handle << " (" << length
                  << " bytes) at " << region.local_base
                  << ", transfers to it become local copies";
    }

    return &regions_.emplace(handle, region).first->second;
}

bool SharedHostMapper::Translate(
    const std::shared_ptr<SegmentDesc> &segment_desc, uint64_t dest_addr,
    uint64_t length, void **local_addr) {
    if (!segment_desc || local_addr == nullptr || length == 0) {
        return false;
    }
    const auto *buffer =
        BufferContaining(segment_desc->buffers, dest_addr, length);
    if (buffer == nullptr || buffer->shm_name.empty()) {
        return false;
    }

    std::lock_guard<std::mutex> lock(mutex_);
    const auto *region =
        ResolveHandle(buffer->shm_name, buffer->addr, buffer->length);
    if (region == nullptr) {
        return false;
    }
    // A handle is cached under the length it was first imported with. A
    // descriptor that now claims a longer buffer under the same handle would
    // translate past the end of the mapping.
    if (buffer->length > region->length) {
        return false;
    }
    *local_addr =
        static_cast<char *>(region->local_base) + (dest_addr - buffer->addr);
    return true;
}

}  // namespace mooncake
