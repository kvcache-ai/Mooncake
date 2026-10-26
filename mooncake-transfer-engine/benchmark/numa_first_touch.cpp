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

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include "numa_first_touch.h"

#include <algorithm>
#include <cctype>
#include <cstdint>
#include <cstdio>
#include <fstream>

#ifdef __linux__
#include "common.h"

#include <cerrno>
#include <mutex>
#include <numa.h>
#include <pthread.h>
#include <sched.h>
#include <sys/syscall.h>
#include <unistd.h>
#endif

namespace mooncake {
namespace tent {

NumaMapsPlacement parseNumaMapsPlacement(std::string_view maps_line) {
    NumaMapsPlacement out;
    unsigned long best_pages = 0;
    for (size_t i = 0; i < maps_line.size(); ++i) {
        if (maps_line[i] != 'N' || i + 1 >= maps_line.size() ||
            !std::isdigit(static_cast<unsigned char>(maps_line[i + 1]))) {
            continue;
        }
        if (i > 0 &&
            !std::isspace(static_cast<unsigned char>(maps_line[i - 1]))) {
            continue;
        }
        size_t j = i + 1;
        int node = 0;
        while (j < maps_line.size() &&
               std::isdigit(static_cast<unsigned char>(maps_line[j]))) {
            node = node * 10 + (maps_line[j] - '0');
            ++j;
        }
        if (j >= maps_line.size() || maps_line[j] != '=') continue;
        ++j;
        if (j >= maps_line.size() ||
            !std::isdigit(static_cast<unsigned char>(maps_line[j]))) {
            continue;
        }
        unsigned long pages = 0;
        while (j < maps_line.size() &&
               std::isdigit(static_cast<unsigned char>(maps_line[j]))) {
            pages = pages * 10 + (maps_line[j] - '0');
            ++j;
        }
        if (pages == 0) {
            i = j - 1;
            continue;
        }
        ++out.node_count;
        if (out.dominant_node < 0 || pages > best_pages) {
            best_pages = pages;
            out.dominant_node = node;
        }
        i = j - 1;
    }
    return out;
}

#ifdef __linux__
namespace {

class CpuAffinityMask {
   public:
    CpuAffinityMask() = default;
    ~CpuAffinityMask() {
        if (set_) CPU_FREE(set_);
    }
    CpuAffinityMask(const CpuAffinityMask&) = delete;
    CpuAffinityMask& operator=(const CpuAffinityMask&) = delete;

    bool valid() const { return set_ != nullptr; }

    bool saveCurrent() {
        size_t ncpus = 1024;
        for (int attempt = 0; attempt < 8; ++attempt) {
            if (!allocate(ncpus)) return false;
            const int rc = pthread_getaffinity_np(pthread_self(), size_, set_);
            if (rc == 0) return true;
            // pthread_* returns the error number; it does not set errno.
            if (rc != EINVAL) {
                clear();
                return false;
            }
            ncpus *= 2;
        }
        clear();
        return false;
    }

    bool restore() const {
        if (!valid()) return false;
        return pthread_setaffinity_np(pthread_self(), size_, set_) == 0;
    }

    bool pinToCpu(int cpu) const {
        if (cpu < 0) return false;
        const size_t ncpus = std::max(ncpus_ ? ncpus_ : size_t{1024},
                                      static_cast<size_t>(cpu) + 1);
        cpu_set_t* tmp = CPU_ALLOC(ncpus);
        if (!tmp) return false;
        const size_t size = CPU_ALLOC_SIZE(ncpus);
        CPU_ZERO_S(size, tmp);
        CPU_SET_S(cpu, size, tmp);
        const int rc = pthread_setaffinity_np(pthread_self(), size, tmp);
        CPU_FREE(tmp);
        return rc == 0;
    }

    bool containsCpu(int cpu) const {
        if (!valid() || cpu < 0) return false;
        if (static_cast<size_t>(cpu) >= ncpus_) return false;
        return CPU_ISSET_S(cpu, size_, set_);
    }

   private:
    bool allocate(size_t next_ncpus) {
        cpu_set_t* next = CPU_ALLOC(next_ncpus);
        if (!next) return false;
        const size_t next_size = CPU_ALLOC_SIZE(next_ncpus);
        CPU_ZERO_S(next_size, next);
        if (set_) CPU_FREE(set_);
        set_ = next;
        ncpus_ = next_ncpus;
        size_ = next_size;
        return true;
    }

    void clear() {
        if (set_) CPU_FREE(set_);
        set_ = nullptr;
        ncpus_ = 0;
        size_ = 0;
    }

    size_t ncpus_ = 0;
    size_t size_ = 0;
    cpu_set_t* set_ = nullptr;
};

int currentCpuId() {
#ifdef SYS_getcpu
    unsigned cpu = 0;
    if (::syscall(SYS_getcpu, &cpu, nullptr, nullptr) == 0)
        return static_cast<int>(cpu);
#endif
    return -1;
}

int pickAllowedCpuOnNode(int node, const CpuAffinityMask& allowed) {
    if (node < 0 || numa_available() < 0 || !allowed.valid()) return -1;
    struct bitmask* cpu_list = numa_allocate_cpumask();
    if (!cpu_list) return -1;
    {
        std::lock_guard<std::mutex> guard(::mooncake::numaNodeCpuCacheMutex());
        numa_node_to_cpus(node, cpu_list);
    }
    int pick = -1;
    const int nr = numa_num_possible_cpus();
    for (int cpu = 0; cpu < nr; ++cpu) {
        if (!numa_bitmask_isbitset(cpu_list, cpu)) continue;
        if (numa_all_cpus_ptr && !numa_bitmask_isbitset(numa_all_cpus_ptr, cpu))
            continue;
        if (!allowed.containsCpu(cpu)) continue;
        pick = cpu;
        break;
    }
    numa_free_cpumask(cpu_list);
    return pick;
}

bool pinToOneCpuOnNode(int node, const CpuAffinityMask& allowed) {
    const int pick = pickAllowedCpuOnNode(node, allowed);
    if (pick < 0) return false;
    if (!allowed.pinToCpu(pick)) return false;
    for (int i = 0; i < 10000; ++i) {
        const int cpu = currentCpuId();
        if (cpu >= 0 && numa_node_of_cpu(cpu) == node) return true;
        sched_yield();
    }
    const int cpu = currentCpuId();
    return cpu >= 0 && numa_node_of_cpu(cpu) == node;
}

}  // namespace

struct CpuAffinityGuard::Impl {
    CpuAffinityMask mask;
    bool saved_ok = false;
};

CpuAffinityGuard::CpuAffinityGuard(int node) : impl_(std::make_unique<Impl>()) {
    impl_->saved_ok = impl_->mask.saveCurrent();
    if (impl_->saved_ok) pinned_ok_ = pinToOneCpuOnNode(node, impl_->mask);
}

CpuAffinityGuard::~CpuAffinityGuard() {
    if (impl_ && impl_->saved_ok) impl_->mask.restore();
}

void firstTouchPages(void* addr, size_t size) {
    if (!addr || size == 0) return;
    const long page_size = sysconf(_SC_PAGESIZE);
    auto* p = static_cast<char*>(addr);
    if (page_size > 0) {
        for (size_t off = 0; off < size; off += static_cast<size_t>(page_size))
            p[off] = 0;
    }
    p[size - 1] = 0;
}

std::string numaMapsLineFor(void* addr) {
    std::ifstream in("/proc/self/numa_maps");
    if (!in) return "numa_maps unavailable";
    const uintptr_t target = reinterpret_cast<uintptr_t>(addr);
    std::string line, best;
    unsigned long best_start = 0;
    bool found = false;
    while (std::getline(in, line)) {
        unsigned long start = 0;
        if (std::sscanf(line.c_str(), "%lx", &start) != 1) continue;
        if (start <= target && (!found || start >= best_start)) {
            best_start = start;
            best = line;
            found = true;
        }
    }
    return found ? best : "not found in numa_maps";
}

bool vmaRangeFor(void* addr, uintptr_t* start_out, uintptr_t* end_out) {
    if (!addr || !start_out || !end_out) return false;
    std::ifstream in("/proc/self/maps");
    if (!in) return false;
    const uintptr_t target = reinterpret_cast<uintptr_t>(addr);
    std::string line;
    while (std::getline(in, line)) {
        unsigned long start = 0, end = 0;
        if (std::sscanf(line.c_str(), "%lx-%lx", &start, &end) != 2) continue;
        if (target >= start && target < end) {
            *start_out = start;
            *end_out = end;
            return true;
        }
    }
    return false;
}
#endif

}  // namespace tent
}  // namespace mooncake
