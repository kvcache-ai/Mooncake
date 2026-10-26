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
#include <cerrno>
#include <mutex>
#include <sched.h>
#include <pthread.h>
#include <sys/syscall.h>
#include <unistd.h>
#include <numa.h>
#endif

namespace mooncake {
namespace tent {

int parseNumaMapsDominantNode(std::string_view maps_line) {
    int best_node = -1;
    unsigned long best_pages = 0;
    bool found = false;
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
        if (!found || pages > best_pages) {
            best_pages = pages;
            best_node = node;
            found = true;
        }
        i = j - 1;
    }
    return found ? best_node : -1;
}

#ifdef __linux__
struct CpuAffinityMask::Impl {
    size_t ncpus = 0;
    size_t size = 0;
    cpu_set_t* set = nullptr;

    ~Impl() {
        if (set) CPU_FREE(set);
    }

    bool allocate(size_t next_ncpus) {
        cpu_set_t* next = CPU_ALLOC(next_ncpus);
        if (!next) return false;
        const size_t next_size = CPU_ALLOC_SIZE(next_ncpus);
        CPU_ZERO_S(next_size, next);
        if (set) CPU_FREE(set);
        set = next;
        ncpus = next_ncpus;
        size = next_size;
        return true;
    }
};

CpuAffinityMask::CpuAffinityMask() : impl_(std::make_unique<Impl>()) {}
CpuAffinityMask::~CpuAffinityMask() = default;
CpuAffinityMask::CpuAffinityMask(CpuAffinityMask&&) noexcept = default;
CpuAffinityMask& CpuAffinityMask::operator=(CpuAffinityMask&&) noexcept =
    default;

bool CpuAffinityMask::valid() const { return impl_ && impl_->set != nullptr; }

bool CpuAffinityMask::saveCurrent() {
    if (!impl_) return false;
    size_t ncpus = 1024;
    for (int attempt = 0; attempt < 8; ++attempt) {
        if (!impl_->allocate(ncpus)) return false;
        const int rc =
            pthread_getaffinity_np(pthread_self(), impl_->size, impl_->set);
        if (rc == 0) return true;
        if (errno != EINVAL) {
            CPU_FREE(impl_->set);
            impl_->set = nullptr;
            impl_->ncpus = 0;
            impl_->size = 0;
            return false;
        }
        ncpus *= 2;
    }
    CPU_FREE(impl_->set);
    impl_->set = nullptr;
    impl_->ncpus = 0;
    impl_->size = 0;
    return false;
}

bool CpuAffinityMask::restore() const {
    if (!valid()) return false;
    return pthread_setaffinity_np(pthread_self(), impl_->size, impl_->set) == 0;
}

bool CpuAffinityMask::pinToCpu(int cpu) const {
    if (cpu < 0) return false;
    const size_t ncpus =
        std::max(impl_ && impl_->ncpus ? impl_->ncpus : size_t{1024},
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

bool CpuAffinityMask::containsCpu(int cpu) const {
    if (!valid() || cpu < 0) return false;
    if (static_cast<size_t>(cpu) >= impl_->ncpus) return false;
    return CPU_ISSET_S(cpu, impl_->size, impl_->set);
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

static int currentCpuId() {
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
    static std::mutex numa_cpu_cache_mu;
    {
        std::lock_guard<std::mutex> guard(numa_cpu_cache_mu);
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
#endif

}  // namespace tent
}  // namespace mooncake
