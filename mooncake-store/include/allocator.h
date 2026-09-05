#ifndef BUFFER_ALLOCATOR_H
#define BUFFER_ALLOCATOR_H

#include <atomic>
#include <cstdint>
#include <limits>
#include <memory>
#include <string>
#include <vector>

#include <ylt/util/tl/expected.hpp>

#include "cachelib_memory_allocator/MemoryAllocator.h"
#include "offset_allocator/offset_allocator.h"
#include "storage_usage.h"
#include "types.h"

using facebook::cachelib::MemoryAllocator;
using facebook::cachelib::PoolId;

namespace mooncake {

namespace ha {
class AllocatorSnapshotCodec;
}

/**
 * @brief Type of buffer allocator used in the system
 */
enum class ReplicaType {
    MEMORY = 0,      // Memory replica
    DISK = 1,        // Disk replica
    LOCAL_DISK = 2,  // Local disk replica
    NOF_SSD = 3,     // Nvme-oF SSD replica
    ALL = 4,         // All synchronous replicas in put finalize path
    DFS = 100,       // Distributed filesystem page-offset replica
};

struct LiveAllocation {
    uint64_t offset_from_base{0};
    uint64_t requested_size{0};
};

// Constant for unknown free space in allocators that don't track it precisely
static constexpr size_t kAllocatorUnknownFreeSpace =
    std::numeric_limits<size_t>::max();

// Forward declarations
class BufferAllocatorBase;
class Replica;

class AllocatedBuffer {
   public:
    friend class CachelibBufferAllocator;
    friend class OffsetBufferAllocator;
    // Forward declaration of the descriptor struct
    struct Descriptor;

    AllocatedBuffer(std::shared_ptr<BufferAllocatorBase> allocator,
                    void* buffer_ptr, std::size_t size,
                    std::optional<offset_allocator::OffsetAllocationHandle>&&
                        offset_handle = std::nullopt)
        : allocator_(std::move(allocator)),
          buffer_ptr_(buffer_ptr),
          size_(size),
          offset_handle_(std::move(offset_handle)) {}

    AllocatedBuffer(std::shared_ptr<BufferAllocatorBase> allocator,
                    const Descriptor& descriptor);

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

    [[nodiscard]] std::shared_ptr<BufferAllocatorBase> getAllocator() const {
        return allocator_.lock();
    }

    // Serialize the buffer into a descriptor for transfer
    [[nodiscard]] Descriptor get_descriptor() const;

    [[nodiscard]] std::string getSegmentName() const noexcept;

    // Friend declaration for operator<<
    friend std::ostream& operator<<(std::ostream& os,
                                    const AllocatedBuffer& buffer);

    // Represents the serializable state
    struct Descriptor {
        uint64_t size_;
        uintptr_t buffer_address_;
        std::string protocol_;
        std::string transport_endpoint_;
        YLT_REFL(Descriptor, size_, buffer_address_, protocol_,
                 transport_endpoint_);
    };

    void change_to_cxl(std::string client_segment_name);
    void* get_vaddr_from_cxl();

   private:
    bool copyTransferProtocolFrom(const AllocatedBuffer& source);

    std::weak_ptr<BufferAllocatorBase> allocator_;
    std::string segment_name_;
    void* buffer_ptr_{nullptr};
    std::size_t size_{0};
    std::string protocol{"tcp"};
    // RAII handle for buffer allocated by offset allocator
    std::optional<offset_allocator::OffsetAllocationHandle> offset_handle_{
        std::nullopt};

    friend class Serializer<AllocatedBuffer>;
    friend class Replica;
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
    virtual uintptr_t base() const = 0;
    virtual std::string getSegmentName() const = 0;
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

    /**
     * Attach this allocator to a domain usage tracker exactly once, before it
     * is published. Registration is immutable afterward and is released by
     * RAII when the last shared_ptr to the allocator is dropped.
     */
    void AttachUsageTracker(
        const std::shared_ptr<StorageUsageTracker>& usage_tracker);

   protected:
    [[nodiscard]] size_t GetUsageBytes() const noexcept {
        return cur_size_.load(std::memory_order_relaxed);
    }
    void RecordAllocation(size_t bytes) noexcept;
    void RecordDeallocation(size_t bytes) noexcept;
    void SetUsageBytesForRestore(size_t bytes) noexcept {
        cur_size_.store(bytes, std::memory_order_relaxed);
    }

   private:
    std::atomic_size_t cur_size_{0};
    std::unique_ptr<StorageUsageRegistration> usage_registration_;
};

/**
 * A no-op buffer allocator used only for keeping standby promotion metadata
 * alive. It does not actually allocate memory - replicas constructed from
 * this allocator are invalid for actual I/O but preserve endpoint info.
 */
class DummyBufferAllocator final : public BufferAllocatorBase {
   public:
    explicit DummyBufferAllocator(std::string segment_name,
                                  std::string transport_endpoint)
        : segment_name_(std::move(segment_name)),
          transport_endpoint_(std::move(transport_endpoint)) {}

    std::unique_ptr<AllocatedBuffer> allocate(size_t size) override {
        return nullptr;
    }
    void deallocate(AllocatedBuffer* handle) override {}
    size_t capacity() const override { return kAllocatorUnknownFreeSpace; }
    size_t getLargestFreeRegion() const override {
        return kAllocatorUnknownFreeSpace;
    }
    size_t size() const override { return 0; }
    uintptr_t base() const override { return 0; }
    std::string getSegmentName() const override { return segment_name_; }
    std::string getTransportEndpoint() const override {
        return transport_endpoint_;
    }

   private:
    std::string segment_name_;
    std::string transport_endpoint_;
};

/**
 * CachelibBufferAllocator manages memory allocation using CacheLib's slab
 * allocation strategy.
 *
 * The base address and size must both be aligned to CacheLib's slab size.
 *
 * Example usage:
 * ```cpp
 * // Good - properly aligned addresses
 * const size_t base = 0x100000000;  // 4GB aligned
 * const size_t base = 0x200000000;  // 8GB aligned
 *
 * // Bad - Create() returns ErrorCode::INVALID_PARAMS
 * const size_t base = 0x1234;       // Too low, unaligned
 * const size_t base = 0x100000001;  // Not 4MB aligned
 * ```
 */
class CachelibBufferAllocator
    : public BufferAllocatorBase,
      public std::enable_shared_from_this<CachelibBufferAllocator> {
   public:
    static tl::expected<std::shared_ptr<CachelibBufferAllocator>, ErrorCode>
    Create(std::string segment_name, size_t base, size_t size,
           std::string transport_endpoint,
           ReplicaType replica_type = ReplicaType::MEMORY);

    ~CachelibBufferAllocator() override;

    std::unique_ptr<AllocatedBuffer> allocate(size_t size) override;

    void deallocate(AllocatedBuffer* handle) override;

    size_t capacity() const override { return total_size_; }
    size_t size() const override { return GetUsageBytes(); }
    uintptr_t base() const override { return base_; }
    std::string getSegmentName() const override { return segment_name_; }
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
    CachelibBufferAllocator(std::string segment_name, size_t base, size_t size,
                            std::string transport_endpoint,
                            ReplicaType replica_type);

    std::unique_ptr<AllocatedBuffer> adoptImportedBuffer(
        const LiveAllocation& allocation);
    // metadata
    const std::string segment_name_;
    const size_t base_;
    const size_t total_size_;
    const std::string transport_endpoint_;
    const ReplicaType replica_type_;

    // metrics - removed allocated_bytes_ member
    // ylt::metric::gauge_t* allocated_bytes_{nullptr};
    // cachelib
    std::unique_ptr<char[]> header_region_start_;
    size_t header_region_size_;
    std::unique_ptr<facebook::cachelib::MemoryAllocator> memory_allocator_;
    facebook::cachelib::PoolId pool_id_;

    friend struct RestoredCachelibBufferAllocator;
    friend std::optional<struct RestoredCachelibBufferAllocator>
    ImportCachelibBufferAllocator(
        std::string segment_name, size_t base, size_t size,
        std::string transport_endpoint,
        const std::vector<LiveAllocation>& allocations,
        ReplicaType replica_type);
};

struct RestoredCachelibBufferAllocator {
    std::shared_ptr<CachelibBufferAllocator> allocator;
    std::vector<std::unique_ptr<AllocatedBuffer>> buffers;
};

std::optional<RestoredCachelibBufferAllocator> ImportCachelibBufferAllocator(
    std::string segment_name, size_t base, size_t size,
    std::string transport_endpoint,
    const std::vector<LiveAllocation>& allocations,
    ReplicaType replica_type = ReplicaType::MEMORY);

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
                          ReplicaType replica_type = ReplicaType::MEMORY);

    ~OffsetBufferAllocator() override;

    std::unique_ptr<AllocatedBuffer> allocate(size_t size) override;

    void deallocate(AllocatedBuffer* handle) override;

    size_t capacity() const override { return total_size_; }
    size_t size() const override { return GetUsageBytes(); }
    uintptr_t base() const override { return base_; }
    std::string getSegmentName() const override { return segment_name_; }
    std::string getTransportEndpoint() const override {
        return transport_endpoint_;
    }

    /**
     * Returns the actual largest free region from the offset allocator.
     */
    size_t getLargestFreeRegion() const override;

    // Public method to get offset_allocator
    std::shared_ptr<offset_allocator::OffsetAllocator> getOffsetAllocator()
        const {
        return offset_allocator_;
    }

   private:
    void RestoreUsageBytes(size_t bytes) noexcept {
        SetUsageBytesForRestore(bytes);
    }

    // metadata
    const std::string segment_name_;
    const size_t base_;
    const size_t total_size_;
    const std::string transport_endpoint_;
    const ReplicaType replica_type_;

    // offset allocator implementation
    std::shared_ptr<offset_allocator::OffsetAllocator> offset_allocator_;

    friend class Serializer<OffsetBufferAllocator>;
    friend class ha::AllocatorSnapshotCodec;
};

struct RestoredOffsetBufferAllocator {
    std::shared_ptr<OffsetBufferAllocator> allocator;
    std::vector<std::unique_ptr<AllocatedBuffer>> buffers;
};

// Reconstructs an empty OffsetBufferAllocator from final live allocations.
// The returned buffers follow allocation input order. No state is exposed on
// validation or allocation failure.
std::optional<RestoredOffsetBufferAllocator> ImportOffsetBufferAllocator(
    std::string segment_name, size_t base, size_t size,
    std::string transport_endpoint,
    const std::vector<LiveAllocation>& allocations,
    ReplicaType replica_type = ReplicaType::MEMORY);

tl::expected<std::shared_ptr<BufferAllocatorBase>, ErrorCode>
CreateBufferAllocator(BufferAllocatorType allocator_type,
                      std::string segment_name, size_t base, size_t size,
                      std::string transport_endpoint,
                      ReplicaType replica_type = ReplicaType::MEMORY);

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
