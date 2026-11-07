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

#pragma once

#include <cstddef>
#include <cstdint>
#include <stack>
#include <unordered_set>
#include <vector>

namespace mooncake {
namespace detail {

using ManagedBufferFreeList = std::vector<std::stack<char*>>;

inline void removeSlabFromFreeList(ManagedBufferFreeList& free_list, char* slab,
                                   size_t slab_capacity) {
    const auto begin = reinterpret_cast<uintptr_t>(slab);
    const auto end = begin + slab_capacity;
    for (auto& stack : free_list) {
        std::vector<char*> retained;
        while (!stack.empty()) {
            auto* candidate = stack.top();
            stack.pop();
            const auto address = reinterpret_cast<uintptr_t>(candidate);
            if (address < begin || address >= end)
                retained.push_back(candidate);
        }
        for (auto it = retained.rbegin(); it != retained.rend(); ++it) {
            stack.push(*it);
        }
    }
}

template <typename Unregister, typename Free>
int releaseManagedBuffers(std::vector<char*>& buffer_list,
                          std::unordered_set<char*>& large_buffer_list,
                          ManagedBufferFreeList& free_list,
                          std::unordered_set<uintptr_t>& active_buffers,
                          size_t slab_capacity, Unregister&& unregister,
                          Free&& free) {
    const size_t class_count = free_list.size();
    // The old engine may remain active after a failure. Retire each slab from
    // the allocator as soon as its registration is gone, and retain everything
    // else for a safe retry instead of freeing a live registration.
    for (auto it = buffer_list.begin(); it != buffer_list.end();) {
        const int status = unregister(*it);
        if (status != 0) return status;
        removeSlabFromFreeList(free_list, *it, slab_capacity);
        free(*it);
        it = buffer_list.erase(it);
    }
    for (auto it = large_buffer_list.begin(); it != large_buffer_list.end();) {
        const int status = unregister(*it);
        if (status != 0) return status;
        free(*it);
        it = large_buffer_list.erase(it);
    }
    active_buffers.clear();
    free_list.clear();
    free_list.resize(class_count);
    return 0;
}

}  // namespace detail
}  // namespace mooncake
