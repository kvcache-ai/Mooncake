#include "allocation_probe.h"

#include <cstdlib>
#include <new>

using mooncake::test::AllocationProbe;
thread_local AllocationProbe* AllocationProbe::active = nullptr;

void* operator new(size_t size) {
    if (auto* probe = AllocationProbe::active;
        probe && (!probe->match_size || probe->match_size == size)) {
        probe->bytes += size;
        if (probe->calls++ == probe->fail_at) throw std::bad_alloc();
    }
    if (void* ptr = std::malloc(size ? size : 1)) return ptr;
    throw std::bad_alloc();
}
void* operator new(size_t size, const std::nothrow_t&) noexcept {
    try {
        return ::operator new(size);
    } catch (const std::bad_alloc&) {
        return nullptr;
    }
}
void* operator new[](size_t size) { return ::operator new(size); }
void* operator new[](size_t size, const std::nothrow_t& tag) noexcept {
    return ::operator new(size, tag);
}
void operator delete(void* ptr) noexcept { std::free(ptr); }
void operator delete(void* ptr, size_t) noexcept { std::free(ptr); }
void operator delete(void* ptr, const std::nothrow_t&) noexcept {
    std::free(ptr);
}
void operator delete[](void* ptr) noexcept { std::free(ptr); }
void operator delete[](void* ptr, size_t) noexcept { std::free(ptr); }
void operator delete[](void* ptr, const std::nothrow_t&) noexcept {
    std::free(ptr);
}
