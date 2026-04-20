#pragma once

#include <mutex>
#include <vector>
#include <cstdlib>
#include "cuda_alike.h"

// Ascend CANN is not covered by cuda_alike.h
#if defined(USE_ASCEND) || defined(USE_ASCEND_DIRECT) || defined(USE_UBSHMEM)
#include <acl/acl_rt.h>
#endif

namespace mooncake {

/**
 * PinnedBufferPool: Thread-safe pool of reusable pinned host memory buffers.
 *
 * Platform pinned alloc APIs:
 *   CUDA / MUSA / MACA : cudaMallocHost   (mapped via cuda_alike.h)
 *   HIP                : hipHostMalloc     (not mapped in hip.h, native API)
 *   Ascend             : aclrtMallocHost
 *   Other              : new char[]        (pageable fallback)
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
        char* data = nullptr;
        size_t capacity = 0;
        bool is_pinned = false;  // Selects correct free API in FreeBuffer
    };

    explicit PinnedBufferPool(size_t max_pool_size = kDefaultMaxPoolSize)
        : max_pool_size_(max_pool_size) {}

    ~PinnedBufferPool() { Clear(); }

    Buffer Acquire(size_t size) {
        {
            std::lock_guard<std::mutex> lk(mutex_);
            for (size_t i = 0; i < pool_.size(); ++i) {
                if (pool_[i].capacity >= size) {
                    Buffer buf = pool_[i];
                    // O(1) erase: swap with back then pop
                    pool_[i] = pool_.back();
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
            pool_.push_back(buf);
        } else {
            // Pool full — free immediately to bound pinned memory usage
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
        Buffer buf;
        buf.capacity = size;
        buf.is_pinned = false;

#if defined(USE_CUDA) || defined(USE_MUSA) || defined(USE_MACA)
        if (cudaMallocHost(reinterpret_cast<void**>(&buf.data), size) ==
            cudaSuccess) {
            buf.is_pinned = true;
        } else {
            buf.data = new char[size];
        }

#elif defined(USE_HIP)
        if (hipHostMalloc(reinterpret_cast<void**>(&buf.data), size, 0) ==
            hipSuccess) {
            buf.is_pinned = true;
        } else {
            buf.data = new char[size];
        }

#elif defined(USE_ASCEND) || defined(USE_ASCEND_DIRECT) || defined(USE_UBSHMEM)
        if (aclrtMallocHost(reinterpret_cast<void**>(&buf.data), size) ==
            ACL_SUCCESS) {
            buf.is_pinned = true;
        } else {
            buf.data = new char[size];
        }

#else
        buf.data = new char[size];
#endif
        return buf;
    }

    static void FreeBuffer(Buffer& buf) {
        if (!buf.data) return;
        if (!buf.is_pinned) {
            delete[] buf.data;
            return;
        }
#if defined(USE_CUDA) || defined(USE_MUSA) || defined(USE_MACA)
        cudaFreeHost(buf.data);
#elif defined(USE_HIP)
        hipHostFree(buf.data);
#elif defined(USE_ASCEND) || defined(USE_ASCEND_DIRECT) || defined(USE_UBSHMEM)
        aclrtFreeHost(buf.data);
#else
        delete[] buf.data;
#endif
    }

    const size_t max_pool_size_;
    std::mutex mutex_;
    std::vector<Buffer> pool_;
};

}  // namespace mooncake
