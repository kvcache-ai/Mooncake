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

#include "transfer_engine/managed_buffer_release.h"

#include <iostream>
#include <unordered_set>
#include <vector>

namespace {

bool contains(mooncake::detail::ManagedBufferFreeList& free_list,
              char* address) {
    auto entries = free_list;
    for (auto& stack : entries) {
        while (!stack.empty()) {
            if (stack.top() == address) return true;
            stack.pop();
        }
    }
    return false;
}

}  // namespace

int main() {
    using mooncake::detail::ManagedBufferFreeList;
    using mooncake::detail::releaseManagedBuffers;
    std::vector<char> first_storage(4), second_storage(4), large_storage(1);
    auto* first = first_storage.data();
    auto* second = second_storage.data();
    auto* large = large_storage.data();
    std::vector<char*> buffers{first, second};
    std::unordered_set<char*> large_buffers{large};
    ManagedBufferFreeList free_list(1);
    free_list[0].push(first);
    free_list[0].push(second);
    std::unordered_set<uintptr_t> active;
    std::unordered_set<char*> unregistered, freed;
    bool fail_second = true;
    auto unregister = [&](char* buffer) {
        if (buffer == second && fail_second) {
            fail_second = false;
            return -1;
        }
        unregistered.insert(buffer);
        return 0;
    };
    auto free = [&](char* buffer) { freed.insert(buffer); };

    const int failed = releaseManagedBuffers(buffers, large_buffers, free_list,
                                             active, 4, unregister, free);
    int failures = 0;
    const auto expect = [&failures](bool condition, const char* message) {
        if (!condition) {
            std::cerr << message << '\n';
            ++failures;
        }
    };
    expect(failed != 0, "deregistration failure must stop the release");
    expect(unregistered.count(first) == 1, "the first slab should unregister");
    expect(freed.count(first) == 1, "a removed registration can be freed");
    expect(freed.count(second) == 0,
           "a failed deregistration must not free its buffer");
    expect(freed.count(large) == 0,
           "buffers after the failed slab must stay allocated");
    expect(buffers.size() == 1 && buffers.front() == second,
           "the failed slab must remain owned for a retry");
    expect(!contains(free_list, first),
           "a freed slab must leave the allocator free list");
    expect(contains(free_list, second),
           "a registered failed slab must remain allocatable");

    const int resumed = releaseManagedBuffers(buffers, large_buffers, free_list,
                                              active, 4, unregister, free);
    expect(resumed == 0, "a later deregistration success must release");
    expect(freed.count(second) == 1 && freed.count(large) == 1,
           "the retry must release the remaining buffers");
    expect(buffers.empty() && large_buffers.empty(),
           "a complete release must clear ownership");
    expect(free_list.empty() || !contains(free_list, second),
           "a complete release must not retain free slab pointers");
    return failures == 0 ? 0 : 1;
}
