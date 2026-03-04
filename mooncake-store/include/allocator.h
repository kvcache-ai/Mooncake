#ifndef BUFFER_ALLOCATOR_H
#define BUFFER_ALLOCATOR_H

#include <atomic>
#include <limits>
#include <memory>
#include <string>

#include "cachelib_memory_allocator/MemoryAllocator.h"
#include "offset_allocator/offset_allocator.hpp"
#include "types.h"

using facebook::cachelib::MemoryAllocator;
using facebook::cachelib::PoolId;

namespace mooncake {

// Constant for unknown free space in allocators that don't track it precisely
static constexpr size_t kAllocatorUnknownFreeSpace =
    std::numeric_limits<size_t>::max();

// Forward declarations
class BufferAllocatorBase;

class AllocatedBuffer {
   public:
    friend class CachelibBufferAllocator;
    friend class OffsetBufferAllocator;
    // Forward declaration of the descriptor struct
    struct Descriptor;

    AllocatedBuffer(std::shared_ptr<BufferAllocatorBase> allocator,
                    void* buffer_ptr, std::size_t size, const UUID& segment_id,
                    std::optional<offset_allocator::OffsetAllocationHandle>&&
                        offset_handle = std::nullopt)
        : allocator_(std::move(allocator)),
          buffer_ptr_(buffer_ptr),
          size_(size),
          segment_id_(segment_id),
          offset_handle_(std::move(offset_handle)) {}

    ~AllocatedBuffer();

    AllocatedBuffer(const AllocatedBuffer&) = delete;
    AllocatedBuffer& operator=(const AllocatedBuffer&) = delete;
    AllocatedBuffer(AllocatedBuffer&&) noexcept;
    AllocatedBuffer& operator=(AllocatedBuffer&&) noexcept;

    [[nodiscard]] void* data() const noexcept { return buffer_ptr_; }

    [[nodiscard]] std::size_t size() const noexcept { return this->size_; }

    [[nodiscard]] bool isAllocatorValid() const {
        return !allocator_.expired();
    }

    // Serialize the buffer into a descriptor for transfer
    [[nodiscard]] Descriptor get_descriptor() const;

    [[nodiscard]] std::string getSegmentName() const noexcept;

    [[nodiscard]] UUID getSegmentId() const noexcept { return segment_id_; }

    // Friend declaration for operator<<
    friend std::ostream& operator<<(std::ostream& os,
                                    const AllocatedBuffer& buffer);

    // Represents the serializable state
    struct Descriptor {
        uint64_t size_;
        uintptr_t buffer_address_;
        std::string transport_endpoint_;
        YLT_REFL(Descriptor, size_, buffer_address_, transport_endpoint_);
    };

   private:
    std::weak_ptr<BufferAllocatorBase> allocator_;
    void* buffer_ptr_{nullptr};
    std::size_t size_{0};
    UUID segment_id_;
    // RAII handle for buffer allocated by offset allocator
    std::optional<offset_allocator::OffsetAllocationHandle> offset_handle_{
        std::nullopt};
};

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
    virtual UUID getSegmentId() const = 0;
    virtual std::string getTransportEndpoint() const = 0;

    /**
     * Returns the largest free region available in this allocator.
     * For CacheLib allocators, this returns kAllocatorUnknownFreeSpace as an
     * approximation. For OffsetAllocator, this returns the actual largest free
     * region.
     *
     * Note: This is a best-effort estimate used for filtering. The actual
     * allocation may still fail due to race conditions or fragmentation.
     */
    virtual size_t getLargestFreeRegion() const = 0;
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
    CachelibBufferAllocator(std::string segment_name, size_t base, size_t size,
                            std::string transport_endpoint,
                            const UUID& segment_id);

    ~CachelibBufferAllocator() override;

    std::unique_ptr<AllocatedBuffer> allocate(size_t size) override;

    void deallocate(AllocatedBuffer* handle) override;

    size_t capacity() const override { return total_size_; }
    size_t size() const override { return cur_size_.load(); }
    std::string getSegmentName() const override { return segment_name_; }
    UUID getSegmentId() const override { return segment_id_; }
    std::string getTransportEndpoint() const override {
        return transport_endpoint_;
    }

    /**
     * For CacheLib, return kAllocatorUnknownFreeSpace as we don't have exact
     * free region info. This ensures CacheLib allocators are always considered
     * for allocation.
     */
    size_t getLargestFreeRegion() const override {
        return kAllocatorUnknownFreeSpace;
    }

   private:
    // metadata
    const std::string segment_name_;
    const size_t base_;
    const size_t total_size_;
    std::atomic_size_t cur_size_;
    const std::string transport_endpoint_;
    const UUID segment_id_;

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
    OffsetBufferAllocator(std::string segment_name, size_t base, size_t size,
                          std::string transport_endpoint,
                          const UUID& segment_id);

    ~OffsetBufferAllocator() override;

    std::unique_ptr<AllocatedBuffer> allocate(size_t size) override;

    void deallocate(AllocatedBuffer* handle) override;

    size_t capacity() const override { return total_size_; }
    size_t size() const override { return cur_size_.load(); }
    std::string getSegmentName() const override { return segment_name_; }
    UUID getSegmentId() const override { return segment_id_; }
    std::string getTransportEndpoint() const override {
        return transport_endpoint_;
    }

    /**
     * Returns the actual largest free region from the offset allocator.
     */
    size_t getLargestFreeRegion() const override;

   private:
    // metadata
    const std::string segment_name_;
    const size_t base_;
    const size_t total_size_;
    std::atomic_size_t cur_size_;
    const std::string transport_endpoint_;
    const UUID segment_id_;

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
