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
class Allocator;
class __Allocator;

static constexpr uint32 NUM_TOP_BINS = 32;
static constexpr uint32 BINS_PER_LEAF = 8;
static constexpr uint32 TOP_BINS_INDEX_SHIFT = 3;
static constexpr uint32 LEAF_BINS_INDEX_MASK = 0x7;
static constexpr uint32 NUM_LEAF_BINS = NUM_TOP_BINS * BINS_PER_LEAF;

struct Allocation {
    static constexpr uint32 NO_SPACE = 0xffffffff;

    uint32 offset = NO_SPACE;
    NodeIndex metadata = NO_SPACE;  // internal: node index
};

struct StorageReport {
    uint32 totalFreeSpace;
    uint32 largestFreeRegion;
};

struct StorageReportFull {
    struct Region {
        uint32 size;
        uint32 count;
    };

    Region freeRegions[NUM_LEAF_BINS];
};

// RAII Handle class for automatic deallocation
class AllocationHandle {
   public:
    // Constructor for valid allocation
    AllocationHandle(std::shared_ptr<Allocator> allocator,
                     Allocation allocation, uint64_t base, uint32_t size);

    // Move constructor
    AllocationHandle(AllocationHandle&& other) noexcept;

    // Move assignment operator
    AllocationHandle& operator=(AllocationHandle&& other) noexcept;

    // Disable copy constructor and copy assignment
    AllocationHandle(const AllocationHandle&) = delete;
    AllocationHandle& operator=(const AllocationHandle&) = delete;

    // Destructor - automatically deallocates
    ~AllocationHandle();

    // Check if the allocation handle is valid
    bool isValid() const { return !m_released && m_allocator; }

    // Get offset
    uint64_t offset() const { return m_base + m_allocation.offset; }

    void* ptr() const { return reinterpret_cast<void*>(offset()); }

    // Get size
    uint32_t size() const { return m_size; }

   private:
    std::shared_ptr<Allocator> m_allocator;
    Allocation m_allocation;
    const uint64_t m_base;
    const uint32_t m_size;
    bool m_released;
};

class __Allocator {
   public:
    __Allocator(uint32 size, uint32 maxAllocs = 128 * 1024);
    __Allocator(__Allocator&& other);
    ~__Allocator();
    void reset();

    Allocation allocate(uint32 size);
    void free(Allocation allocation);

    uint32 allocationSize(Allocation allocation) const;
    StorageReport storageReport() const;
    StorageReportFull storageReportFull() const;

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

// Thread-safe wrapper class for __Allocator
class Allocator : public std::enable_shared_from_this<Allocator> {
   public:
    // Factory method to create shared_ptr<Allocator>
    static std::shared_ptr<Allocator> create(uint64_t base, uint32 size,
                                             uint32 maxAllocs = 128 * 1024);

    // Disable copy constructor and copy assignment
    Allocator(const Allocator&) = delete;
    Allocator& operator=(const Allocator&) = delete;

    // Disable move constructor and move assignment
    Allocator(Allocator&& other) noexcept = delete;
    Allocator& operator=(Allocator&& other) noexcept = delete;

    // Destructor
    ~Allocator() = default;

    // Reset the allocator
    void reset();

    // Allocate memory and return a Handle (thread-safe)
    std::optional<AllocationHandle> allocate(uint32 size);

    // Get allocation size (thread-safe)
    uint32 allocationSize(const Allocation& allocation) const;

    // Get storage report (thread-safe)
    StorageReport storageReport() const;

    // Get full storage report (thread-safe)
    StorageReportFull storageReportFull() const;

   private:
    friend class AllocationHandle;

    // Internal method for Handle to free allocation (thread-safe)
    void freeAllocation(const Allocation& allocation);

    std::shared_ptr<__Allocator> m_allocator GUARDED_BY(m_mutex);
    const uint64_t m_base;
    mutable Mutex m_mutex;

    // Private constructor - use create() factory method instead
    Allocator(uint64_t base, uint32 size, uint32 maxAllocs = 128 * 1024);
};
}  // namespace mooncake::offset_allocator