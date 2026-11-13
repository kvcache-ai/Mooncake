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

#ifndef TENT_SELECTION_STATS_H
#define TENT_SELECTION_STATS_H

#include <cstdint>
#include <string>
#include <vector>

namespace mooncake {
namespace tent {

// Allocation intent for a local device, before any worker fallback remapping.
// Selected bytes are not posted, completed, or wire bytes.
struct DeviceSelectionStats {
    int dev_id;
    // Local device availability, not reachability to a particular peer.
    bool available;
    uint64_t selected_slices;
    uint64_t selected_bytes;
    std::string device_name;
    int numa_node = -1;
};

// Best-effort snapshot: concurrent allocations may be observed at different
// points across fields. These counters do not participate in scheduling.
struct SelectionStats {
    uint64_t allocations = 0;
    uint64_t single_path_allocations = 0;
    uint64_t multi_path_allocations = 0;
    uint64_t probe_allocations = 0;
    std::vector<DeviceSelectionStats> devices;
};

}  // namespace tent
}  // namespace mooncake

#endif  // TENT_SELECTION_STATS_H
