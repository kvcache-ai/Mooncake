#pragma once
// (C) Sebastian Aaltonen 2023
// MIT License (see file: LICENSE)

#include <memory>
#include <optional>

#include "mutex.h"

namespace mooncake::offset_allocator {
typedef unsigned char uint8;
typedef unsigned short uint16;
typedef unsigned int uint32;
using NodeIndex = uint32;

// Forward declarations
class OffsetAllocator;
class __Allocator;

static constexpr uint32 NUM_TOP_BINS = 32;
static constexpr uint32 BINS_PER_LEAF = 8;
static constexpr uint32 TOP_BINS_INDEX_SHIFT = 3;
static constexpr uint32 LEAF_BINS_INDEX_MASK = 0x7;
static constexpr uint32 NUM_LEAF_BINS = NUM_TOP_BINS * BINS_PER_LEAF;

struct OffsetAllocation {
    static constexpr uint32 NO_SPACE = 0xffffffff;

    uint32 offset = NO_SPACE;
    NodeIndex metadata = NO_SPACE;  // internal: node index
};

struct OffsetAllocStorageReport {
    uint64_t totalFreeSpace;
    uint64_t largestFreeRegion;
};

struct OffsetAllocStorageReportFull {
    struct Region {
        uint64_t size;
        uint64_t count;
    };

    Region freeRegions[NUM_LEAF_BINS];
};

// RAII Handle class for automatic deallocation
class OffsetAllocationHandle {
   public:
    // Constructor for valid allocation
    OffsetAllocationHandle(std::shared_ptr<OffsetAllocator> allocator,
                     OffsetAllocation allocation, uint64_t base, uint64_t size);

    // Move constructor
    OffsetAllocationHandle(OffsetAllocationHandle&& other) noexcept;

    // Move assignment operator
    OffsetAllocationHandle& operator=(OffsetAllocationHandle&& other) noexcept;

    // Disable copy constructor and copy assignment
    OffsetAllocationHandle(const OffsetAllocationHandle&) = delete;
    OffsetAllocationHandle& operator=(const OffsetAllocationHandle&) = delete;

    // Destructor - automatically deallocates
    ~OffsetAllocationHandle();

    // Check if the allocation handle is valid
    bool isValid() const { return !m_allocator.expired(); }

    // Get offset
    uint64_t address() const { return real_base; }

    void* ptr() const { return reinterpret_cast<void*>(address()); }

    // Get size
    uint64_t size() const { return requested_size; }

   private:
    std::weak_ptr<OffsetAllocator> m_allocator;
    // The offset in m_allocation may not be equal to the real offset.
    OffsetAllocation m_allocation;
    // The real base and requested size of the allocated memory.
    uint64_t real_base;
    uint64_t requested_size;
};

// Wrapper class for __Allocator, it 1) supports thread-safe allocation and
// deallocation, 2) supports creating a buffer or allocating a memory region
// that is larger than the largest bin size (3.75GB). The __allocator class is
// also optimized to round up the allocated size to a bin size. This will
// a) slightly decrease the memory utilization ratio in general cases, b) makes
// no difference when the allocated size is equal to a bin size, c) largely
// improve the memory utilization ratio when the allocated size is mostly
// uniform and not equal to any bin size.
class OffsetAllocator : public std::enable_shared_from_this<OffsetAllocator> {
   public:
    // Factory method to create shared_ptr<OffsetAllocator>
    static std::shared_ptr<OffsetAllocator> create(uint64_t base, size_t size,
                                             uint32 maxAllocs = 128 * 1024);

    // Disable copy constructor and copy assignment
    OffsetAllocator(const OffsetAllocator&) = delete;
    OffsetAllocator& operator=(const OffsetAllocator&) = delete;

    // Disable move constructor and move assignment
    OffsetAllocator(OffsetAllocator&& other) noexcept = delete;
    OffsetAllocator& operator=(OffsetAllocator&& other) noexcept = delete;

    // Destructor
    ~OffsetAllocator() = default;

    // Allocate memory and return a Handle (thread-safe)
    [[nodiscard]]
    std::optional<OffsetAllocationHandle> allocate(size_t size);

    // Get storage report (thread-safe)
    [[nodiscard]]
    OffsetAllocStorageReport storageReport() const;

    // Get full storage report (thread-safe)
    [[nodiscard]]
    OffsetAllocStorageReportFull storageReportFull() const;

   private:
    friend class OffsetAllocationHandle;

    // Internal method for Handle to free allocation (thread-safe)
    void freeAllocation(const OffsetAllocation& allocation);

    std::unique_ptr<__Allocator> m_allocator GUARDED_BY(m_mutex);
    const uint64_t m_base;
    // The real offset and size of the allocated memory need to be multiplied by
    // m_multiplier
    const uint64_t m_multiplier;
    mutable Mutex m_mutex;

    // Private constructor - use create() factory method instead
    OffsetAllocator(uint64_t base, size_t size, uint32 maxAllocs = 128 * 1024);
};

class __Allocator {
    public:
     __Allocator(uint32 size, uint32 maxAllocs = 128 * 1024);
     __Allocator(__Allocator&& other);
     ~__Allocator();
     void reset();
 
     OffsetAllocation allocate(uint32 size);
     void free(OffsetAllocation allocation);
 
     uint32 allocationSize(OffsetAllocation allocation) const;
     OffsetAllocStorageReport storageReport() const;
     OffsetAllocStorageReportFull storageReportFull() const;
 
    private:
     uint32 insertNodeIntoBin(uint32 size, uint32 dataOffset);
     void removeNodeFromBin(uint32 nodeIndex);
 
     struct Node {
         static constexpr NodeIndex unused = 0xffffffff;
 
         uint32 dataOffset = 0;
         uint32 dataSize = 0;
         NodeIndex binListPrev = unused;
         NodeIndex binListNext = unused;
         NodeIndex neighborPrev = unused;
         NodeIndex neighborNext = unused;
         bool used = false;  // TODO: Merge as bit flag
     };
 
     uint32 m_size;
     uint32 m_maxAllocs;
     uint32 m_freeStorage;
 
     uint32 m_usedBinsTop;
     uint8 m_usedBins[NUM_TOP_BINS];
     NodeIndex m_binIndices[NUM_LEAF_BINS];
 
     Node* m_nodes;
     NodeIndex* m_freeNodes;
     uint32 m_freeOffset;
 };

}  // namespace mooncake::offset_allocator