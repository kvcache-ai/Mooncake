#include "thread_pool.h"

#include <atomic>
#include <cstddef>
#include <cstdlib>
#include <exception>
#include <new>

namespace {
// Limit injection to the constructing thread and this test executable.
thread_local int allocations_before_failure = -1;
}  // namespace

void* operator new(std::size_t size) {
    if (allocations_before_failure == 0) {
        allocations_before_failure = -1;
        throw std::bad_alloc();
    }
    if (allocations_before_failure > 0) --allocations_before_failure;
    if (void* ptr = std::malloc(size ? size : 1)) return ptr;
    throw std::bad_alloc();
}

void* operator new[](std::size_t size) { return ::operator new(size); }
void operator delete(void* ptr) noexcept { std::free(ptr); }
void operator delete[](void* ptr) noexcept { std::free(ptr); }
void operator delete(void* ptr, std::size_t) noexcept { std::free(ptr); }
void operator delete[](void* ptr, std::size_t) noexcept { std::free(ptr); }

int main() {
    std::set_terminate([] { std::_Exit(86); });
    int failures = 0;
    // Include failures after one or more workers have started, without
    // depending on a standard library's exact allocation count.
    for (int point = 0; point < 32; ++point) {
        allocations_before_failure = point;
        try {
            mooncake::ThreadPool pool(4);
            allocations_before_failure = -1;
        } catch (const std::bad_alloc&) {
            allocations_before_failure = -1;
            ++failures;
        }
    }
    if (failures == 0) return 1;

    std::atomic<int> completed{0};
    mooncake::ThreadPool pool(2);
    for (int i = 0; i < 16; ++i) pool.enqueue([&] { ++completed; });
    pool.stop();
    return completed == 16 ? 0 : 2;
}
