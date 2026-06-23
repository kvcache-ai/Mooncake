#pragma once

#include <memory>
#include <mutex>
#include <utility>
#include <vector>

namespace mooncake {
namespace device {

using PinnedHostBufferDeleter = void (*)(void* addr);

struct PinnedHostBuffer {
    void* addr = nullptr;
    size_t size = 0;
    PinnedHostBufferDeleter deleter = nullptr;

    PinnedHostBuffer() = default;
    PinnedHostBuffer(void* addr, size_t size,
                     PinnedHostBufferDeleter deleter)
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

}  // namespace device
}  // namespace mooncake

#include "device/accelerator_registry.h"

namespace mooncake {

/**
 * PinnedBufferPool: Thread-safe pool of reusable pinned host memory buffers.
 *
 * Platform-specific pinned host allocation is delegated to AcceleratorDevice.
 *
 * Pinned memory provides 10x~100x higher D2H bandwidth than pageable memory.
 * Falls back to new char[] if pinned allocation fails.
 *
 * The pool enforces a maximum number of cached buffers (kDefaultMaxPoolSize).
 * When the pool is full, Release() frees the buffer immediately instead of
 * caching it, preventing unbounded pinned memory growth.
 */
class PinnedBufferPool {
   public:
    static constexpr size_t kDefaultMaxPoolSize = 32;

    struct Buffer {
        device::PinnedHostBuffer pinned_host;
        std::unique_ptr<char[]> pageable_host;
        char* data = nullptr;
        size_t capacity = 0;

        Buffer() = default;
        explicit Buffer(device::PinnedHostBuffer pinned_host)
            : pinned_host(std::move(pinned_host)),
              data(static_cast<char*>(this->pinned_host.addr)),
              capacity(this->pinned_host.size) {}

        static Buffer Pageable(size_t size) {
            Buffer buf;
            buf.pageable_host = std::make_unique<char[]>(size);
            buf.data = buf.pageable_host.get();
            buf.capacity = size;
            return buf;
        }

        Buffer(const Buffer&) = delete;
        Buffer& operator=(const Buffer&) = delete;
        Buffer(Buffer&& other) noexcept { *this = std::move(other); }
        Buffer& operator=(Buffer&& other) noexcept {
            if (this != &other) {
                pinned_host = std::move(other.pinned_host);
                pageable_host = std::move(other.pageable_host);
                data = other.data;
                capacity = other.capacity;
                other.data = nullptr;
                other.capacity = 0;
            }
            return *this;
        }
    };

    explicit PinnedBufferPool(size_t max_pool_size = kDefaultMaxPoolSize)
        : max_pool_size_(max_pool_size) {}

    ~PinnedBufferPool() { Clear(); }

    Buffer Acquire(size_t size) {
        {
            std::lock_guard<std::mutex> lk(mutex_);
            for (size_t i = 0; i < pool_.size(); ++i) {
                if (pool_[i].capacity >= size) {
                    Buffer buf = std::move(pool_[i]);
                    // O(1) erase: swap with back then pop
                    pool_[i] = std::move(pool_.back());
                    pool_.pop_back();
                    return buf;
                }
            }
        }
        return AllocNew(size);
    }

    void Release(Buffer buf) {
        std::lock_guard<std::mutex> lk(mutex_);
        if (pool_.size() < max_pool_size_) {
            pool_.push_back(std::move(buf));
        } else {
            // Pool full: free immediately to bound pinned memory usage.
            FreeBuffer(buf);
        }
    }

    void Clear() {
        std::lock_guard<std::mutex> lk(mutex_);
        for (auto& buf : pool_) {
            FreeBuffer(buf);
        }
        pool_.clear();
    }

   private:
    static Buffer AllocNew(size_t size) {
        const auto& registry = device::GetAcceleratorRegistry();
        auto available = registry.AvailableDevices();
        if (available.size() == 1) {
            auto host = available.front()->AllocatePinnedHost(size);
            if (host.addr) return Buffer(std::move(host));
        }
        return Buffer::Pageable(size);
    }

    static void FreeBuffer(Buffer& buf) {
        buf.pinned_host.reset();
        buf.pageable_host.reset();
        buf = {};
    }

    const size_t max_pool_size_;
    std::mutex mutex_;
    std::vector<Buffer> pool_;
};

}  // namespace mooncake
