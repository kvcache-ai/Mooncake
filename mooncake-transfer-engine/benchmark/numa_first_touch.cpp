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

#include <cctype>
#include <cstdint>
#include <cstdio>
#include <fstream>

#ifdef __linux__
#include "common.h"

#include <mutex>
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
CpuAffinityGuard::CpuAffinityGuard(int node) {
    if (numa_available() < 0 || node < 0) return;
    saved_ = numa_allocate_cpumask();
    if (!saved_) return;
    std::lock_guard<std::mutex> lock(::mooncake::numaNodeCpuCacheMutex());
    if (numa_sched_getaffinity(0, saved_) < 0) {
        numa_free_cpumask(saved_);
        saved_ = nullptr;
        return;
    }
    // numa_run_on_node() uses numa_node_to_cpus(); keep it on the same lock
    // as WorkerPool::bindToSocket().
    pinned_ok_ = numa_run_on_node(node) == 0;
}

CpuAffinityGuard::~CpuAffinityGuard() {
    if (!saved_) return;
    {
        std::lock_guard<std::mutex> lock(::mooncake::numaNodeCpuCacheMutex());
        numa_sched_setaffinity(0, saved_);
    }
    numa_free_cpumask(saved_);
    saved_ = nullptr;
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
