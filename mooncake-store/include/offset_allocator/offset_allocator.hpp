#pragma once
// (C) Sebastian Aaltonen 2023
// MIT License (see file: LICENSE)

#include <memory>
#include <optional>
#include <vector>
#include <glog/logging.h>

#include "mutex.h"

namespace mooncake::offset_allocator {
typedef unsigned char uint8;
typedef unsigned short uint16;
typedef unsigned int uint32;
using NodeIndex = uint32;

// Forward declarations
class OffsetAllocator;
class __Allocator;
}  // namespace mooncake::offset_allocator

namespace mooncake {
class OffsetBufferAllocator;
void downsizeAllocator(std::shared_ptr<OffsetBufferAllocator> allocator);
}

namespace mooncake::offset_allocator {

static constexpr uint32 NUM_TOP_BINS = 32;
static constexpr uint32 BINS_PER_LEAF = 8;
static constexpr uint32 TOP_BINS_INDEX_SHIFT = 3;
static constexpr uint32 LEAF_BINS_INDEX_MASK = 0x7;
static constexpr uint32 NUM_LEAF_BINS = NUM_TOP_BINS * BINS_PER_LEAF;

struct OffsetAllocation {
    static constexpr uint32 NO_SPACE = 0xffffffff;

   private:
    uint32 offset = NO_SPACE;
    NodeIndex metadata = NO_SPACE;  // internal: node index

   public:
    OffsetAllocation(uint32 offset_param, NodeIndex metadata_param)
        : offset(offset_param), metadata(metadata_param) {}
    // The real offset could be larger than uint32, so we need to cast it to
    // uint64_t
    uint64_t getOffset() const { return static_cast<uint64_t>(offset); }
    bool isNoSpace() const { return offset == NO_SPACE; }

    friend class __Allocator;
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
    // Default constructor: creates an invalid (empty) handle
    OffsetAllocationHandle()
        : m_allocation(OffsetAllocation::NO_SPACE, OffsetAllocation::NO_SPACE),
          real_base(0),
          requested_size(0) {}

    // Constructor for valid allocation
    OffsetAllocationHandle(std::shared_ptr<OffsetAllocator> allocator,
                           OffsetAllocation allocation, uint64_t base,
                           uint64_t size);

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

    friend class OffsetAllocatorTest;  // for unit tests
};

struct OffsetAllocatorMetrics {
    uint64_t allocated_size_;       // Total bytes currently allocated
    uint64_t allocated_num_;        // Number of active allocations
    uint64_t largest_free_region_;  // Size of largest contiguous free region
    uint64_t total_free_space_;     // Total free space available
    const uint64_t capacity;        // Total capacity of the allocator
};

// Stream output operator for OffsetAllocatorMetrics
std::ostream& operator<<(std::ostream& os,
                         const OffsetAllocatorMetrics& metrics);

// Wrapper class for __Allocator, it 1) supports thread-safe allocation and
// deallocation, 2) supports creating a buffer or allocating a memory region
// that is larger than the largest bin size (3.75GB). The __allocator class is
// also optimized to:
// 1) round up the allocated size to a bin size. This will a) slightly decrease
// the memory utilization ratio in general cases, b) makes no difference when
// the allocated size is equal to a bin size, c) largely improve the memory
// utilization ratio when the allocated size is mostly uniform and not equal to
// any bin size.
// 2) dynamically adjust the capacity of the allocator to the allocated size.
// This will a) reduce the memory consumption in general cases, b) auto
// increase the capacity in case there are a lot of small regions to be
// allocated.
class OffsetAllocator : public std::enable_shared_from_this<OffsetAllocator> {
   public:
    // Factory method to create shared_ptr<OffsetAllocator>
    static std::shared_ptr<OffsetAllocator> create(
        uint64_t base, size_t size, uint32 init_capacity = 128 * 1024,
        uint32 max_capacity = (1 << 20));

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

    // Get comprehensive metrics including fragmentation analysis (thread-safe)
    [[nodiscard]]
    OffsetAllocatorMetrics get_metrics() const;

    // Serialize the allocator with serializer.
    template <typename T>
    void serialize_to(T& serializer) const;

    template <typename T>
    static std::shared_ptr<OffsetAllocator> deserialize_from(T& serializer);

   private:
    friend class OffsetAllocationHandle;

    // Internal method for Handle to free allocation (thread-safe)
    void freeAllocation(const OffsetAllocation& allocation, uint64_t size);

    // Internal method to get metrics without locking (caller must hold m_mutex)
    [[nodiscard]]
    OffsetAllocatorMetrics get_metrics_internal() const REQUIRES(m_mutex);

    std::unique_ptr<__Allocator> m_allocator GUARDED_BY(m_mutex);
    uint64_t m_base;
    // The real offset and size of the allocated memory need to be multiplied by
    // m_multiplier
    uint64_t m_multiplier_bits;
    uint64_t m_capacity;
    mutable Mutex m_mutex;

    // Lightweight metrics maintained during allocation/deallocation
    uint64_t m_allocated_size GUARDED_BY(m_mutex) = 0;
    uint64_t m_allocated_num GUARDED_BY(m_mutex) = 0;

    // Private constructor - use create() factory method instead
    OffsetAllocator(uint64_t base, size_t size, uint32 init_capacity,
                    uint32 max_capacity);

    // Private constructor - initialize from serialized data
    template <typename T>
    OffsetAllocator(T& serializer);

    friend class OffsetAllocatorTest;  // for unit tests
    friend void ::mooncake::downsizeAllocator(std::shared_ptr<::mooncake::OffsetBufferAllocator> allocator);
};

class __Allocator {
   public:
    __Allocator(uint32 size, uint32 init_capacity, uint32 max_capacity);
    template <typename T>
    __Allocator(T& serializer) noexcept(false);
    __Allocator(__Allocator&& other);
    ~__Allocator() = default;
    void reset();

    OffsetAllocation allocate(uint32 size);
    void free(OffsetAllocation allocation);

    uint32 allocationSize(OffsetAllocation allocation) const;
    OffsetAllocStorageReport storageReport() const;
    OffsetAllocStorageReportFull storageReportFull() const;

    // Serialize the allocator with serializer.
    template <typename T>
    void serialize_to(T& serializer) const;

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
    uint32 m_current_capacity;
    uint32 m_max_capacity;
    uint32 m_freeStorage;

    uint32 m_usedBinsTop;
    uint8 m_usedBins[NUM_TOP_BINS];
    NodeIndex m_binIndices[NUM_LEAF_BINS];

    std::vector<Node> m_nodes;
    std::vector<NodeIndex> m_freeNodes;
    uint32 m_freeOffset;

    friend class OffsetAllocatorTest;  // for unit tests
    friend void ::mooncake::downsizeAllocator(std::shared_ptr<::mooncake::OffsetBufferAllocator> allocator);
};

// Template method implementations
template <typename T>
void OffsetAllocator::serialize_to(T& serializer) const {
    MutexLocker guard(&m_mutex);

    if (!m_allocator) {
        serializer.set_error("Allocator is not initialized");
        return;
    }

    // Basic member variables
    serializer.write(&m_base, sizeof(m_base));
    serializer.write(&m_multiplier_bits, sizeof(m_multiplier_bits));
    serializer.write(&m_capacity, sizeof(m_capacity));
    serializer.write(&m_allocated_size, sizeof(m_allocated_size));
    serializer.write(&m_allocated_num, sizeof(m_allocated_num));
    // Serialize the allocator
    m_allocator->serialize_to(serializer);
}

template <typename T>
std::shared_ptr<OffsetAllocator> OffsetAllocator::deserialize_from(
    T& serializer) {
    return std::shared_ptr<OffsetAllocator>(new OffsetAllocator(serializer));
}

template <typename T>
OffsetAllocator::OffsetAllocator(T& serializer) {
    // serializer.read() will throw an exception if the buffer is corrupted.
    try {
        serializer.read(&m_base, sizeof(m_base));
        serializer.read(&m_multiplier_bits, sizeof(m_multiplier_bits));
        serializer.read(&m_capacity, sizeof(m_capacity));
        serializer.read(&m_allocated_size, sizeof(m_allocated_size));
        serializer.read(&m_allocated_num, sizeof(m_allocated_num));
        m_allocator = std::make_unique<__Allocator>(serializer);
    } catch (const std::exception& e) {
        LOG(ERROR) << "Deserializing OffsetAllocator failed, error="
                   << e.what();
        throw std::runtime_error("Deserializing OffsetAllocator failed");
    }
}

template <typename T>
void __Allocator::serialize_to(T& serializer) const {
    if (m_nodes.empty() || m_freeNodes.empty()) {
        serializer.set_error("Allocator is not initialized");
        return;
    }

    serializer.write(&m_size, sizeof(m_size));
    serializer.write(&m_current_capacity, sizeof(m_current_capacity));
    serializer.write(&m_max_capacity, sizeof(m_max_capacity));
    serializer.write(&m_freeStorage, sizeof(m_freeStorage));
    serializer.write(&m_usedBinsTop, sizeof(m_usedBinsTop));
    serializer.write(&m_usedBins, sizeof(m_usedBins));
    serializer.write(&m_binIndices, sizeof(m_binIndices));
    serializer.write(&m_freeOffset, sizeof(m_freeOffset));
    serializer.write(m_nodes.data(), m_current_capacity * sizeof(Node));
    serializer.write(m_freeNodes.data(),
                     m_current_capacity * sizeof(NodeIndex));
}

template <typename T>
__Allocator::__Allocator(T& serializer) {
    // serializer.read() will throw an exception if the buffer is corrupted.
    try {
        // Deserialize basic member variables
        serializer.read(&m_size, sizeof(m_size));
        serializer.read(&m_current_capacity, sizeof(m_current_capacity));
        serializer.read(&m_max_capacity, sizeof(m_max_capacity));
        serializer.read(&m_freeStorage, sizeof(m_freeStorage));
        serializer.read(&m_usedBinsTop, sizeof(m_usedBinsTop));
        serializer.read(&m_usedBins, sizeof(m_usedBins));
        serializer.read(&m_binIndices, sizeof(m_binIndices));
        serializer.read(&m_freeOffset, sizeof(m_freeOffset));

        // Allocate memory for nodes and freeNodes
        m_nodes.reserve(m_max_capacity);
        m_freeNodes.reserve(m_max_capacity);

        m_nodes.resize(m_current_capacity);
        m_freeNodes.resize(m_current_capacity);

        // Deserialize the arrays
        serializer.read(m_nodes.data(), m_current_capacity * sizeof(Node));
        serializer.read(m_freeNodes.data(),
                        m_current_capacity * sizeof(NodeIndex));
    } catch (const std::exception& e) {
        LOG(ERROR) << "Deserializing __Allocator failed, error=" << e.what();
        throw std::runtime_error("Deserializing __Allocator failed");
    }
}

}  // namespace mooncake::offset_allocator
