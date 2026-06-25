#pragma once

#include <cstddef>
#include <utility>

namespace mooncake {

using PinnedHostBufferDeleter = void (*)(void* addr);

struct PinnedHostBuffer {
    void* addr = nullptr;
    size_t size = 0;
    PinnedHostBufferDeleter deleter = nullptr;

    PinnedHostBuffer() = default;
    PinnedHostBuffer(void* addr, size_t size, PinnedHostBufferDeleter deleter)
        : addr(addr), size(size), deleter(deleter) {}

    PinnedHostBuffer(const PinnedHostBuffer&) = delete;
    PinnedHostBuffer& operator=(const PinnedHostBuffer&) = delete;

    PinnedHostBuffer(PinnedHostBuffer&& other) noexcept {
        *this = std::move(other);
    }
    PinnedHostBuffer& operator=(PinnedHostBuffer&& other) noexcept {
        if (this != &other) {
            reset();
            addr = other.addr;
            size = other.size;
            deleter = other.deleter;
            other.addr = nullptr;
            other.size = 0;
            other.deleter = nullptr;
        }
        return *this;
    }

    ~PinnedHostBuffer() { reset(); }

    void reset() {
        if (addr && deleter) deleter(addr);
        addr = nullptr;
        size = 0;
        deleter = nullptr;
    }
};

}  // namespace mooncake
