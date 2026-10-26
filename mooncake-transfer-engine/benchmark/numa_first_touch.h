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
#include <memory>
#include <string>
#include <string_view>

namespace mooncake {
namespace tent {

// Dominant N<id>=<pages> token in a /proc/self/numa_maps line, or -1.
int parseNumaMapsDominantNode(std::string_view maps_line);

// How many N<id>=<pages> tokens have pages > 0. >1 means a mixed VMA.
int parseNumaMapsNodeCount(std::string_view maps_line);

#ifdef __linux__
// Dynamically sized CPU affinity mask. Fixed cpu_set_t is too small when
// CPU IDs exceed CPU_SETSIZE (1024).
class CpuAffinityMask {
   public:
    CpuAffinityMask();
    ~CpuAffinityMask();
    CpuAffinityMask(CpuAffinityMask&&) noexcept;
    CpuAffinityMask& operator=(CpuAffinityMask&&) noexcept;
    CpuAffinityMask(const CpuAffinityMask&) = delete;
    CpuAffinityMask& operator=(const CpuAffinityMask&) = delete;

    bool valid() const;
    bool saveCurrent();
    bool restore() const;
    bool pinToCpu(int cpu) const;
    bool containsCpu(int cpu) const;

   private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

class CpuAffinityGuard {
   public:
    CpuAffinityGuard() { saved_ok_ = mask_.saveCurrent(); }
    ~CpuAffinityGuard() {
        if (saved_ok_) mask_.restore();
    }
    CpuAffinityGuard(const CpuAffinityGuard&) = delete;
    CpuAffinityGuard& operator=(const CpuAffinityGuard&) = delete;

    bool saved() const { return saved_ok_; }
    const CpuAffinityMask& mask() const { return mask_; }

   private:
    CpuAffinityMask mask_;
    bool saved_ok_ = false;
};

void firstTouchPages(void* addr, size_t size);
std::string numaMapsLineFor(void* addr);

// VMA covering `addr` from /proc/self/maps. False if the range is missing.
bool vmaRangeFor(void* addr, uintptr_t* start, uintptr_t* end);

// CPU on `node` that is also in `allowed`. -1 if the cpuset has no overlap.
int pickAllowedCpuOnNode(int node, const CpuAffinityMask& allowed);

// Pin to one allowed CPU of `node` and wait until getcpu is on that node.
bool pinToOneCpuOnNode(int node, const CpuAffinityMask& allowed);
#endif

}  // namespace tent
}  // namespace mooncake

#endif  // TEBENCH_NUMA_FIRST_TOUCH_H
