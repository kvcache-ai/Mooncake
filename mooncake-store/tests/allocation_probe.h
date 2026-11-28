#pragma once

#include <cstddef>
#include <cstdint>

namespace mooncake::test {
// Isolate the allocation probe in its own executable and on the calling thread.
struct AllocationProbe {
    static thread_local AllocationProbe* active;
    size_t calls = 0;
    size_t bytes = 0;
    size_t fail_at;
    size_t match_size;

    explicit AllocationProbe(size_t fail_at = SIZE_MAX, size_t match_size = 0)
        : fail_at(fail_at), match_size(match_size) {
        active = this;
    }
    ~AllocationProbe() { active = nullptr; }
};
}  // namespace mooncake::test
