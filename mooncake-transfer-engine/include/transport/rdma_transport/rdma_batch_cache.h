// Copyright 2026 KVCache.AI
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
#pragma once

#include <memory>

#include "buffer_range_index.h"
#include "memory_location.h"
#include "transport/rdma_transport/rdma_transport.h"

namespace mooncake {
// Function-local cache for successful first-attempt selection. The caller
// keeps the selection policy (local/peer and affinity hint) fixed and must not
// use this cache on retries. Multi-device and offset-dependent topology retain
// their original per-request selection. As with the uncached submit path,
// registrations must remain valid while transfers are in flight.
class BatchRdmaDeviceCache {
   public:
    void invalidate() { snapshot_.reset(); }

    template <typename Resolve>
    int select(const std::shared_ptr<TransferMetadata::SegmentDesc> &desc,
               uint64_t addr, size_t length, int &buffer_id, int &device_id,
               Resolve &&resolve) {
        if (desc && desc == snapshot_ &&
            bufferCoversRange(addr_, length_, addr, length)) {
            buffer_id = buffer_id_;
            device_id = device_id_;
            return 0;
        }
        snapshot_.reset();
        buffer_id = -1;
        device_id = -1;
        const int rc = resolve();
        if (rc) return rc;
        if (!desc || buffer_id < 0 || device_id < 0 ||
            static_cast<size_t>(buffer_id) >= desc->buffers.size() ||
            desc->topology.getHcaList().size() != 1 ||
            desc->buffer_range_index.size() != desc->buffers.size() ||
            desc->buffer_range_index.overlaps())
            return rc;
        const auto &buffer = desc->buffers[buffer_id];
        if (buffer.name.starts_with(kSegmentsLocationPrefix)) return rc;
        snapshot_ = desc;
        addr_ = buffer.addr;
        length_ = buffer.length;
        buffer_id_ = buffer_id;
        device_id_ = device_id;
        return rc;
    }

   private:
    std::shared_ptr<TransferMetadata::SegmentDesc> snapshot_;
    uint64_t addr_ = 0, length_ = 0;
    int buffer_id_ = -1, device_id_ = -1;
};
}  // namespace mooncake
