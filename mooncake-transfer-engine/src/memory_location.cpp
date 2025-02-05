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

#include "memory_location.h"

namespace mooncake {

uintptr_t alignPage(uintptr_t address) { return address & ~(pagesize - 1); }

std::string genCpuNodeName(int node) {
    if (node >= 0) return "cpu:" + std::to_string(node);

    // use "*" when failed to get the numa node.
    return "*";
}

const std::vector<MemoryLocationEntry> getMemoryLocation(void *start,
                                                         size_t len) {
    std::vector<MemoryLocationEntry> entries;

    // start and end address may not be page aligned.
    uintptr_t aligned_start = alignPage((uintptr_t)start);
    int n = (uintptr_t(start) - aligned_start + len + pagesize - 1) / pagesize;
    void **pages = (void **)malloc(sizeof(void *) * n);
    int *status = (int *)malloc(sizeof(int) * n);

    for (int i = 0; i < n; i++) {
        pages[i] = (void *)((char *)aligned_start + i * pagesize);
    }

    int rc = numa_move_pages(0, n, pages, nullptr, status, 0);
    if (rc != 0) {
        PLOG(WARNING) << "Failed to get NUMA node, addr: " << start
                      << ", len: " << len;
        entries.push_back({(uint64_t)start, len, "*"});
        return entries;
    }

    int node = status[0];
    uint64_t start_addr = (uint64_t)start;
    uint64_t new_start_addr;
    for (int i = 1; i < n; i++) {
        if (status[i] != node) {
            new_start_addr = alignPage((uint64_t)start) + i * pagesize;
            entries.push_back({start_addr, size_t(new_start_addr - start_addr),
                               genCpuNodeName(node)});
            start_addr = new_start_addr;
            node = status[i];
        }
    }
    entries.push_back(
        {start_addr, (uint64_t)start + len - start_addr, genCpuNodeName(node)});
    return entries;
}

}  // namespace mooncake
