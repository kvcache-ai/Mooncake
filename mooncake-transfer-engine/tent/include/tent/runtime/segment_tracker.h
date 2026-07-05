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

#ifndef SEGMENT_TRACKER_H
#define SEGMENT_TRACKER_H

#include <glog/logging.h>
#include <netdb.h>

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <thread>
#include <unordered_map>
#include <variant>
#include <vector>

#include "tent/runtime/segment.h"
#include "tent/runtime/segment_manager.h"

namespace mooncake {
namespace tent {
// Maintains the buffer list of the local SegmentDesc (ref-counted
// registration / deregistration). All mutations are applied through
// SegmentManager::updateLocal(), so concurrent readers of getLocal() always
// observe consistent snapshots; SegmentTracker itself holds no state besides
// the manager reference.
class SegmentTracker {
   public:
    explicit SegmentTracker(SegmentManager& manager) : manager_(manager) {}

    ~SegmentTracker() {}

    SegmentTracker(const SegmentTracker&) = delete;
    SegmentTracker& operator==(const SegmentTracker&) = delete;

   public:
    Status addInBatch(std::vector<BufferDesc>& desc_list,
                      std::function<Status(std::vector<BufferDesc>&)> callback);

    Status add(uint64_t base, size_t length,
               std::function<Status(BufferDesc&)> callback);

    Status remove(uint64_t base, size_t length,
                  std::function<Status(BufferDesc&)> callback);

    // Iterates over the current snapshot; entries are immutable. Callers
    // needing a mutable copy (e.g. transports scrubbing keys during
    // deregistration) must copy explicitly.
    Status forEach(std::function<Status(const BufferDesc&)> callback);

   private:
    SegmentManager& manager_;
};
}  // namespace tent
}  // namespace mooncake

#endif  // SEGMENT_TRACKER_H
