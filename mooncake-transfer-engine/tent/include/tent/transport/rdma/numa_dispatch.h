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

#ifndef MOONCAKE_TENT_RDMA_NUMA_DISPATCH_H
#define MOONCAKE_TENT_RDMA_NUMA_DISPATCH_H

#include <array>
#include <cstdint>

namespace mooncake::tent {

// One cursor per NIC keeps alternating NICs from aliasing within a NUMA's
// worker subset. NIC IDs share the selector's 64-bit device-mask namespace.
// The caller retains its ordinary cursor for fallback and unknown placement.
class RdmaNumaLaneCursor {
   public:
    RdmaNumaLaneCursor(int num_workers, bool enabled)
        : num_workers_(num_workers), enabled_(enabled) {}

    bool enabled() const { return enabled_; }

    template <class WorkerNode>
    int select(int device, int node, int fallback, WorkerNode worker_node) {
        if (!enabled_ || device < 0 || device >= 64 || node < 0)
            return fallback;
        int count = 0;
        for (int i = 0; i < num_workers_; ++i)
            if (worker_node(i) == node) ++count;
        if (!count) return fallback;

        const uint64_t bit = uint64_t{1} << device;
        if (!(initialized_ & bit)) {
            next_[device] = static_cast<uint32_t>(fallback);
            initialized_ |= bit;
        }
        const auto ordinal = next_[device]++ % static_cast<uint32_t>(count);
        uint32_t current = 0;
        for (int i = 0; i < num_workers_; ++i) {
            if (worker_node(i) == node && current++ == ordinal) return i;
        }
        // Placement may still be getting published during worker startup.
        return fallback;
    }

   private:
    int num_workers_;
    bool enabled_;
    uint64_t initialized_{0};
    std::array<uint32_t, 64> next_{};
};

}  // namespace mooncake::tent

#endif  // MOONCAKE_TENT_RDMA_NUMA_DISPATCH_H
