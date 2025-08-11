#ifndef BUFFER_ALLOCATOR_H
#define BUFFER_ALLOCATOR_H

#include <atomic>
#include <memory>
#include <string>

#include "cachelib_memory_allocator/MemoryAllocator.h"
#include "offset_allocator/offset_allocator.hpp"
#include "types.h"

using facebook::cachelib::MemoryAllocator;
using facebook::cachelib::PoolId;

namespace mooncake {

// forward declare AllocatedBuffer
class AllocatedBuffer;

/**
 * Virtual base class for buffer allocators.
 * Defines the interface that all buffer allocators must implement.
 */
class BufferAllocatorBase {
   public:
    virtual ~BufferAllocatorBase() = default;

    virtual std::unique_ptr<AllocatedBuffer> allocate(size_t size) = 0;
    virtual void deallocate(AllocatedBuffer* handle) = 0;
    virtual size_t capacity() const = 0;
    virtual size_t size() const = 0;
    virtual std::string getSegmentName() const = 0;
};

/**
 * CachelibBufferAllocator manages memory allocation using CacheLib's slab
 * allocation strategy.
 *
 * Important alignment requirements:
 * 1. Base address must be at least 8-byte aligned (CacheLib requirement)
 * 2. Base address should be 4MB aligned since the total size must be a multiple
 * of 4MB
 * 3. Use sufficiently high base addresses (e.g., 0x100000000 for 4GB) to avoid
 * memory conflicts
 *
 * Example usage:
 * ```cpp
 * // Good - properly aligned addresses
 * const size_t base = 0x100000000;  // 4GB aligned
 * const size_t base = 0x200000000;  // 8GB aligned
 *
 * // Bad - will likely crash
 * const size_t base = 0x1234;       // Too low, unaligned
 * const size_t base = 0x100000001;  // Not 4MB aligned
 * ```
 */
class CachelibBufferAllocator
    : public BufferAllocatorBase,
      public std::enable_shared_from_this<CachelibBufferAllocator> {
   public:
    CachelibBufferAllocator(std::string segment_name, size_t base, size_t size);

    ~CachelibBufferAllocator() override;

    std::unique_ptr<AllocatedBuffer> allocate(size_t size) override;

    void deallocate(AllocatedBuffer* handle) override;

    size_t capacity() const override { return total_size_; }
    size_t size() const override { return cur_size_.load(); }
    std::string getSegmentName() const override { return segment_name_; }

   private:
    // metadata
    const std::string segment_name_;
    const size_t base_;
    const size_t total_size_;
    std::atomic_size_t cur_size_;

    // metrics - removed allocated_bytes_ member
    // ylt::metric::gauge_t* allocated_bytes_{nullptr};
    // cachelib
    std::unique_ptr<char[]> header_region_start_;
    size_t header_region_size_;
    std::unique_ptr<facebook::cachelib::MemoryAllocator> memory_allocator_;
    facebook::cachelib::PoolId pool_id_;
};

/**
 * OffsetBufferAllocator manages memory allocation using the OffsetAllocator
 * strategy, which provides efficient memory allocation with bin-based
 * optimization.
 */
class OffsetBufferAllocator
    : public BufferAllocatorBase,
      public std::enable_shared_from_this<OffsetBufferAllocator> {
   public:
    OffsetBufferAllocator(std::string segment_name, size_t base, size_t size);

    ~OffsetBufferAllocator() override;

    std::unique_ptr<AllocatedBuffer> allocate(size_t size) override;

    void deallocate(AllocatedBuffer* handle) override;

    size_t capacity() const override { return total_size_; }
    size_t size() const override { return cur_size_.load(); }
    std::string getSegmentName() const override { return segment_name_; }

   private:
    // metadata
    const std::string segment_name_;
    const size_t base_;
    const size_t total_size_;
    std::atomic_size_t cur_size_;

    // offset allocator implementation
    std::shared_ptr<offset_allocator::OffsetAllocator> offset_allocator_;
};

// The main difference is that it allocates real memory and returns it, while
// BufferAllocator allocates an address
class SimpleAllocator {
   public:
    SimpleAllocator(size_t size);
    ~SimpleAllocator();
    void* allocate(size_t size);
    void deallocate(void* ptr, size_t size);
    void* getBase() const { return base_; }

   private:
    void* base_{nullptr};

    std::unique_ptr<char[]> header_region_start_;
    size_t header_region_size_;

    std::unique_ptr<facebook::cachelib::MemoryAllocator> memory_allocator_;
    facebook::cachelib::PoolId pool_id_;
};

}  // namespace mooncake

#endif  // BUFFER_ALLOCATOR_H
