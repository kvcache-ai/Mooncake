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

#ifndef TEBENCH_NUMA_FIRST_TOUCH_H
#define TEBENCH_NUMA_FIRST_TOUCH_H

#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>

#ifdef __linux__
#include <numa.h>
#endif

namespace mooncake {
namespace tent {

struct NumaMapsPlacement {
    int dominant_node = -1;
    int node_count = 0;
};

NumaMapsPlacement parseNumaMapsPlacement(std::string_view maps_line);

#ifdef __linux__
// Saves the calling thread's affinity, runs it on `node`, and restores on
// destruction. Does not change affinity if the original mask cannot be saved.
class CpuAffinityGuard {
   public:
    explicit CpuAffinityGuard(int node);
    ~CpuAffinityGuard();
    CpuAffinityGuard(const CpuAffinityGuard&) = delete;
    CpuAffinityGuard& operator=(const CpuAffinityGuard&) = delete;

    bool pinned() const { return pinned_ok_; }

   private:
    struct bitmask* saved_ = nullptr;
    bool pinned_ok_ = false;
};

void firstTouchPages(void* addr, size_t size);
std::string numaMapsLineFor(void* addr);

// VMA covering `addr` from /proc/self/maps. False if the range is missing.
bool vmaRangeFor(void* addr, uintptr_t* start, uintptr_t* end);
#endif

}  // namespace tent
}  // namespace mooncake

#endif  // TEBENCH_NUMA_FIRST_TOUCH_H
