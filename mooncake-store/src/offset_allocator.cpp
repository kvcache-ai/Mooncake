// (C) Sebastian Aaltonen 2023
// MIT License (see file: LICENSE)

#include "offset_allocator/offset_allocator.hpp"

#include <cmath>
#include <iomanip>
#include <iostream>

#include "mutex.h"
#include "utils.h"

#ifdef DEBUG
#include <assert.h>
#define ASSERT(x) assert(x)
// #define DEBUG_VERBOSE
#else
#define ASSERT(x)
#endif

#ifdef DEBUG_VERBOSE
#include <stdio.h>
#endif

#ifdef _MSC_VER
#include <intrin.h>
#endif

#include <cstring>

namespace mooncake::offset_allocator {
inline uint32 lzcnt_nonzero(uint32 v) {
#ifdef _MSC_VER
    unsigned long retVal;
    _BitScanReverse(&retVal, v);
    return 31 - retVal;
#else
    return __builtin_clz(v);
#endif
}

inline uint32 tzcnt_nonzero(uint32 v) {
#ifdef _MSC_VER
    unsigned long retVal;
    _BitScanForward(&retVal, v);
    return retVal;
#else
    return __builtin_ctz(v);
#endif
}

namespace SmallFloat {
static constexpr uint32 MANTISSA_BITS = 3;
static constexpr uint32 MANTISSA_VALUE = 1 << MANTISSA_BITS;
static constexpr uint32 MANTISSA_MASK = MANTISSA_VALUE - 1;
static constexpr uint64_t MAX_BIN_SIZE = 4026531840ull;  // 3.75GB

// Bin sizes follow floating point (exponent + mantissa) distribution (piecewise
// linear log approx) This ensures that for each size class, the average
// overhead percentage stays the same
uint32 uintToFloatRoundUp(uint32 size) {
    uint32 exp = 0;
    uint32 mantissa = 0;

    if (size < MANTISSA_VALUE) {
        // Denorm: 0..(MANTISSA_VALUE-1)
        mantissa = size;
    } else {
        // Normalized: Hidden high bit always 1. Not stored. Just like float.
        uint32 leadingZeros = lzcnt_nonzero(size);
        uint32 highestSetBit = 31 - leadingZeros;

        uint32 mantissaStartBit = highestSetBit - MANTISSA_BITS;
        exp = mantissaStartBit + 1;
        mantissa = (size >> mantissaStartBit) & MANTISSA_MASK;

        uint32 lowBitsMask = (1 << mantissaStartBit) - 1;

        // Round up!
        if ((size & lowBitsMask) != 0) mantissa++;
    }

    return (exp << MANTISSA_BITS) +
           mantissa;  // + allows mantissa->exp overflow for round up
}

uint32 uintToFloatRoundDown(uint32 size) {
    uint32 exp = 0;
    uint32 mantissa = 0;

    if (size < MANTISSA_VALUE) {
        // Denorm: 0..(MANTISSA_VALUE-1)
        mantissa = size;
    } else {
        // Normalized: Hidden high bit always 1. Not stored. Just like float.
        uint32 leadingZeros = lzcnt_nonzero(size);
        uint32 highestSetBit = 31 - leadingZeros;

        uint32 mantissaStartBit = highestSetBit - MANTISSA_BITS;
        exp = mantissaStartBit + 1;
        mantissa = (size >> mantissaStartBit) & MANTISSA_MASK;
    }

    return (exp << MANTISSA_BITS) | mantissa;
}

uint32 floatToUint(uint32 floatValue) {
    uint32 exponent = floatValue >> MANTISSA_BITS;
    uint32 mantissa = floatValue & MANTISSA_MASK;
    if (exponent == 0) {
        // Denorms
        return mantissa;
    } else {
        return (mantissa | MANTISSA_VALUE) << (exponent - 1);
    }
}
}  // namespace SmallFloat

// Utility functions
uint32 findLowestSetBitAfter(uint32 bitMask, uint32 startBitIndex) {
    uint32 maskBeforeStartIndex = (1 << startBitIndex) - 1;
    uint32 maskAfterStartIndex = ~maskBeforeStartIndex;
    uint32 bitsAfter = bitMask & maskAfterStartIndex;
    if (bitsAfter == 0) return OffsetAllocation::NO_SPACE;
    return tzcnt_nonzero(bitsAfter);
}

// __Allocator...
__Allocator::__Allocator(uint32 size, uint32 init_capacity, uint32 max_capacity)
    : m_size(size),
      m_current_capacity(init_capacity),
      m_max_capacity(std::max(init_capacity, max_capacity)) {
    if (sizeof(NodeIndex) == 2) {
        ASSERT(m_max_capacity <= 65536);
    }
    reset();
}

__Allocator::__Allocator(__Allocator&& other)
    : m_size(other.m_size),
      m_current_capacity(other.m_current_capacity),
      m_max_capacity(other.m_max_capacity),
      m_freeStorage(other.m_freeStorage),
      m_usedBinsTop(other.m_usedBinsTop),
      m_nodes(std::move(other.m_nodes)),
      m_freeNodes(std::move(other.m_freeNodes)),
      m_freeOffset(other.m_freeOffset) {
    memcpy(m_usedBins, other.m_usedBins, sizeof(uint8) * NUM_TOP_BINS);
    memcpy(m_binIndices, other.m_binIndices, sizeof(NodeIndex) * NUM_LEAF_BINS);

    other.m_nodes.clear();
    other.m_freeNodes.clear();
    other.m_freeOffset = 0;
    other.m_current_capacity = 0;
    other.m_max_capacity = 0;
    other.m_usedBinsTop = 0;
}

void __Allocator::reset() {
    m_freeStorage = 0;
    m_usedBinsTop = 0;
    m_freeOffset = 0;

    for (uint32 i = 0; i < NUM_TOP_BINS; i++) m_usedBins[i] = 0;

    for (uint32 i = 0; i < NUM_LEAF_BINS; i++) m_binIndices[i] = Node::unused;

    m_nodes.clear();
    m_freeNodes.clear();

    m_nodes.reserve(m_max_capacity);
    m_freeNodes.reserve(m_max_capacity);

    m_nodes.resize(m_current_capacity);
    m_freeNodes.resize(m_current_capacity);

    // Freelist is a stack. Nodes in inverse order so that [0] pops first.
    for (uint32 i = 0; i < m_current_capacity; i++) {
        m_freeNodes[i] = i;
    }

    // Start state: Whole storage as one big node
    // Algorithm will split remainders and push them back as smaller nodes
    insertNodeIntoBin(m_size, 0);
}

OffsetAllocation __Allocator::allocate(uint32 size) {
    // Out of allocations?
    if (m_freeOffset == m_max_capacity) {
        return OffsetAllocation(OffsetAllocation::NO_SPACE,
                                OffsetAllocation::NO_SPACE);
    }
    if (m_freeOffset == m_current_capacity) {
        m_freeNodes.push_back(m_current_capacity);
        m_nodes.emplace_back();
        m_current_capacity++;
    }

    // Round up to bin index to ensure that alloc >= bin
    // Gives us min bin index that fits the size
    uint32 minBinIndex = SmallFloat::uintToFloatRoundUp(size);

    uint32 minTopBinIndex = minBinIndex >> TOP_BINS_INDEX_SHIFT;
    uint32 minLeafBinIndex = minBinIndex & LEAF_BINS_INDEX_MASK;

    uint32 topBinIndex = minTopBinIndex;
    uint32 leafBinIndex = OffsetAllocation::NO_SPACE;

    // If top bin exists, scan its leaf bin. This can fail (NO_SPACE).
    if (m_usedBinsTop & (1 << topBinIndex)) {
        leafBinIndex =
            findLowestSetBitAfter(m_usedBins[topBinIndex], minLeafBinIndex);
    }

    // If we didn't find space in top bin, we search top bin from +1
    if (leafBinIndex == OffsetAllocation::NO_SPACE) {
        topBinIndex = findLowestSetBitAfter(m_usedBinsTop, minTopBinIndex + 1);

        // Out of space?
        if (topBinIndex == OffsetAllocation::NO_SPACE) {
            return OffsetAllocation(OffsetAllocation::NO_SPACE,
                                    OffsetAllocation::NO_SPACE);
        }

        // All leaf bins here fit the alloc, since the top bin was rounded up.
        // Start leaf search from bit 0. NOTE: This search can't fail since at
        // least one leaf bit was set because the top bit was set.
        leafBinIndex = tzcnt_nonzero(m_usedBins[topBinIndex]);
    }

    uint32 binIndex = (topBinIndex << TOP_BINS_INDEX_SHIFT) | leafBinIndex;

    // Pop the top node of the bin. Bin top = node.next.
    uint32 nodeIndex = m_binIndices[binIndex];
    Node& node = m_nodes[nodeIndex];
    uint32 nodeTotalSize = node.dataSize;
    // Modified in Mooncake project: Round up to bin size. Otherwise when this
    // node is freed, if it cannot be merged with neighbors, it will be inserted
    // to a smaller bin.
#ifdef OFFSET_ALLOCATOR_NOT_ROUND_UP
    uint32 roundupSize = size;
#else
    // In default, round up to bin size.
    uint32 roundupSize = SmallFloat::floatToUint(minBinIndex);
#endif
    node.dataSize = roundupSize;
    node.used = true;
    m_binIndices[binIndex] = node.binListNext;
    if (node.binListNext != Node::unused)
        m_nodes[node.binListNext].binListPrev = Node::unused;
    m_freeStorage -= nodeTotalSize;
#ifdef DEBUG_VERBOSE
    printf("Free storage: %u (-%u) (allocate)\n", m_freeStorage, nodeTotalSize);
#endif

    // Bin empty?
    if (m_binIndices[binIndex] == Node::unused) {
        // Remove a leaf bin mask bit
        m_usedBins[topBinIndex] &= ~(1 << leafBinIndex);

        // All leaf bins empty?
        if (m_usedBins[topBinIndex] == 0) {
            // Remove a top bin mask bit
            m_usedBinsTop &= ~(1 << topBinIndex);
        }
    }

    // Push back reminder N elements to a lower bin
    uint32 reminderSize = nodeTotalSize - roundupSize;
    if (reminderSize > 0) {
        uint32 newNodeIndex =
            insertNodeIntoBin(reminderSize, node.dataOffset + roundupSize);

        // Link nodes next to each other so that we can merge them later if both
        // are free And update the old next neighbor to point to the new node
        // (in middle)
        if (node.neighborNext != Node::unused)
            m_nodes[node.neighborNext].neighborPrev = newNodeIndex;
        m_nodes[newNodeIndex].neighborPrev = nodeIndex;
        m_nodes[newNodeIndex].neighborNext = node.neighborNext;
        node.neighborNext = newNodeIndex;
    }

    return OffsetAllocation(node.dataOffset, nodeIndex);
}

void __Allocator::free(OffsetAllocation allocation) {
    ASSERT(allocation.metadata != OffsetAllocation::NO_SPACE);
    if (m_nodes.empty()) return;

    uint32 nodeIndex = allocation.metadata;
    Node& node = m_nodes[nodeIndex];

    // Double delete check
    ASSERT(node.used == true);

    // Merge with neighbors...
    uint32 offset = node.dataOffset;
    uint32 size = node.dataSize;

    if ((node.neighborPrev != Node::unused) &&
        (m_nodes[node.neighborPrev].used == false)) {
        // Previous (contiguous) free node: Change offset to previous node
        // offset. Sum sizes
        Node& prevNode = m_nodes[node.neighborPrev];
        offset = prevNode.dataOffset;
        size += prevNode.dataSize;

        // Remove node from the bin linked list and put it in the freelist
        removeNodeFromBin(node.neighborPrev);

        ASSERT(prevNode.neighborNext == nodeIndex);
        node.neighborPrev = prevNode.neighborPrev;
    }

    if ((node.neighborNext != Node::unused) &&
        (m_nodes[node.neighborNext].used == false)) {
        // Next (contiguous) free node: Offset remains the same. Sum sizes.
        Node& nextNode = m_nodes[node.neighborNext];
        size += nextNode.dataSize;

        // Remove node from the bin linked list and put it in the freelist
        removeNodeFromBin(node.neighborNext);

        ASSERT(nextNode.neighborPrev == nodeIndex);
        node.neighborNext = nextNode.neighborNext;
    }

    uint32 neighborNext = node.neighborNext;
    uint32 neighborPrev = node.neighborPrev;

    // Insert the removed node to freelist
#ifdef DEBUG_VERBOSE
    printf("Putting node %u into freelist[%u] (free)\n", nodeIndex,
           m_freeOffset - 1);
#endif
    m_freeNodes[--m_freeOffset] = nodeIndex;

    // Insert the (combined) free node to bin
    uint32 combinedNodeIndex = insertNodeIntoBin(size, offset);

    // Connect neighbors with the new combined node
    if (neighborNext != Node::unused) {
        m_nodes[combinedNodeIndex].neighborNext = neighborNext;
        m_nodes[neighborNext].neighborPrev = combinedNodeIndex;
    }
    if (neighborPrev != Node::unused) {
        m_nodes[combinedNodeIndex].neighborPrev = neighborPrev;
        m_nodes[neighborPrev].neighborNext = combinedNodeIndex;
    }
}

uint32 __Allocator::insertNodeIntoBin(uint32 size, uint32 dataOffset) {
    // Round down to bin index to ensure that bin >= alloc
    uint32 binIndex = SmallFloat::uintToFloatRoundDown(size);

    uint32 topBinIndex = binIndex >> TOP_BINS_INDEX_SHIFT;
    uint32 leafBinIndex = binIndex & LEAF_BINS_INDEX_MASK;

    // Bin was empty before?
    if (m_binIndices[binIndex] == Node::unused) {
        // Set bin mask bits
        m_usedBins[topBinIndex] |= 1 << leafBinIndex;
        m_usedBinsTop |= 1 << topBinIndex;
    }

    // Take a freelist node and insert on top of the bin linked list (next = old
    // top)
    uint32 topNodeIndex = m_binIndices[binIndex];
    uint32 nodeIndex = m_freeNodes[m_freeOffset++];
#ifdef DEBUG_VERBOSE
    printf("Getting node %u from freelist[%u]\n", nodeIndex, m_freeOffset - 1);
#endif
    m_nodes[nodeIndex] = {.dataOffset = dataOffset,
                          .dataSize = size,
                          .binListNext = topNodeIndex};
    if (topNodeIndex != Node::unused)
        m_nodes[topNodeIndex].binListPrev = nodeIndex;
    m_binIndices[binIndex] = nodeIndex;

    m_freeStorage += size;
#ifdef DEBUG_VERBOSE
    printf("Free storage: %u (+%u) (insertNodeIntoBin)\n", m_freeStorage, size);
#endif

    return nodeIndex;
}

void __Allocator::removeNodeFromBin(uint32 nodeIndex) {
    Node& node = m_nodes[nodeIndex];

    if (node.binListPrev != Node::unused) {
        // Easy case: We have previous node. Just remove this node from the
        // middle of the list.
        m_nodes[node.binListPrev].binListNext = node.binListNext;
        if (node.binListNext != Node::unused)
            m_nodes[node.binListNext].binListPrev = node.binListPrev;
    } else {
        // Hard case: We are the first node in a bin. Find the bin.

        // Round down to bin index to ensure that bin >= alloc
        uint32 binIndex = SmallFloat::uintToFloatRoundDown(node.dataSize);

        uint32 topBinIndex = binIndex >> TOP_BINS_INDEX_SHIFT;
        uint32 leafBinIndex = binIndex & LEAF_BINS_INDEX_MASK;

        m_binIndices[binIndex] = node.binListNext;
        if (node.binListNext != Node::unused)
            m_nodes[node.binListNext].binListPrev = Node::unused;

        // Bin empty?
        if (m_binIndices[binIndex] == Node::unused) {
            // Remove a leaf bin mask bit
            m_usedBins[topBinIndex] &= ~(1 << leafBinIndex);

            // All leaf bins empty?
            if (m_usedBins[topBinIndex] == 0) {
                // Remove a top bin mask bit
                m_usedBinsTop &= ~(1 << topBinIndex);
            }
        }
    }

    // Insert the node to freelist
#ifdef DEBUG_VERBOSE
    printf("Putting node %u into freelist[%u] (removeNodeFromBin)\n", nodeIndex,
           m_freeOffset - 1);
#endif
    m_freeNodes[--m_freeOffset] = nodeIndex;

    m_freeStorage -= node.dataSize;
#ifdef DEBUG_VERBOSE
    printf("Free storage: %u (-%u) (removeNodeFromBin)\n", m_freeStorage,
           node.dataSize);
#endif
}

uint32 __Allocator::allocationSize(OffsetAllocation allocation) const {
    if (allocation.metadata == OffsetAllocation::NO_SPACE) return 0;
    if (m_nodes.empty()) return 0;

    return m_nodes[allocation.metadata].dataSize;
}

OffsetAllocStorageReport __Allocator::storageReport() const {
    uint32 largestFreeRegion = 0;
    uint32 freeStorage = 0;

    // Out of allocations? -> Zero free space
    if (m_freeOffset < m_max_capacity) {
        freeStorage = m_freeStorage;
        if (m_usedBinsTop) {
            uint32 topBinIndex = 31 - lzcnt_nonzero(m_usedBinsTop);
            uint32 leafBinIndex = 31 - lzcnt_nonzero(m_usedBins[topBinIndex]);
            largestFreeRegion = SmallFloat::floatToUint(
                (topBinIndex << TOP_BINS_INDEX_SHIFT) | leafBinIndex);
            ASSERT(freeStorage >= largestFreeRegion);
        }
    }

    return {.totalFreeSpace = freeStorage,
            .largestFreeRegion = largestFreeRegion};
}

OffsetAllocStorageReportFull __Allocator::storageReportFull() const {
    OffsetAllocStorageReportFull report;
    for (uint32 i = 0; i < NUM_LEAF_BINS; i++) {
        uint32 count = 0;
        uint32 nodeIndex = m_binIndices[i];
        while (nodeIndex != Node::unused) {
            nodeIndex = m_nodes[nodeIndex].binListNext;
            count++;
        }
        report.freeRegions[i] = {.size = SmallFloat::floatToUint(i),
                                 .count = count};
    }
    return report;
}

// OffsetAllocationHandle implementation
OffsetAllocationHandle::OffsetAllocationHandle(
    std::shared_ptr<OffsetAllocator> allocator, OffsetAllocation allocation,
    uint64_t base, uint64_t size)
    : m_allocator(std::move(allocator)),
      m_allocation(allocation),
      real_base(base),
      requested_size(size) {}

OffsetAllocationHandle::OffsetAllocationHandle(
    OffsetAllocationHandle&& other) noexcept
    : m_allocator(std::move(other.m_allocator)),
      m_allocation(other.m_allocation),
      real_base(other.real_base),
      requested_size(other.requested_size) {
    other.m_allocation = {OffsetAllocation::NO_SPACE,
                          OffsetAllocation::NO_SPACE};
    other.real_base = 0;
    other.requested_size = 0;
}

OffsetAllocationHandle& OffsetAllocationHandle::operator=(
    OffsetAllocationHandle&& other) noexcept {
    if (this != &other) {
        // Free current allocation if valid{
        auto allocator = m_allocator.lock();
        if (allocator) {
            allocator->freeAllocation(m_allocation, requested_size);
        }

        // Move from other
        m_allocator = std::move(other.m_allocator);
        m_allocation = other.m_allocation;
        real_base = other.real_base;
        requested_size = other.requested_size;

        // Reset other
        other.m_allocation = {OffsetAllocation::NO_SPACE,
                              OffsetAllocation::NO_SPACE};
        other.real_base = 0;
        other.requested_size = 0;
    }
    return *this;
}

OffsetAllocationHandle::~OffsetAllocationHandle() {
    auto allocator = m_allocator.lock();
    if (allocator) {
        allocator->freeAllocation(m_allocation, requested_size);
    }
}

// Helper function to calculate the multiplier
static uint64_t calculateMultiplier(size_t size) {
    uint64_t multiplier_bits = 0;
    for (; SmallFloat::MAX_BIN_SIZE < (size >> multiplier_bits);
         multiplier_bits++) {
    }
    return multiplier_bits;
}

// Thread-safe OffsetAllocator implementation
std::shared_ptr<OffsetAllocator> OffsetAllocator::create(uint64_t base,
                                                         size_t size,
                                                         uint32 init_capacity,
                                                         uint32 max_capacity) {
    // Use a custom deleter to allow private constructor
    return std::shared_ptr<OffsetAllocator>(
        new OffsetAllocator(base, size, init_capacity, max_capacity));
}

OffsetAllocator::OffsetAllocator(uint64_t base, size_t size,
                                 uint32 init_capacity, uint32 max_capacity)
    : m_base(base),
      m_multiplier_bits(calculateMultiplier(size)),
      m_capacity(size) {
    m_allocator = std::make_unique<__Allocator>(size >> m_multiplier_bits,
                                                init_capacity, max_capacity);
}

OffsetAllocator::OffsetAllocator(uint64_t base, size_t size,
                                 uint64_t multiplier_bits,
                                 std::unique_ptr<__Allocator> allocator)
    : m_allocator(std::move(allocator)),
      m_base(base),
      m_multiplier_bits(multiplier_bits),
      m_capacity(size) {}

std::optional<OffsetAllocationHandle> OffsetAllocator::allocate(size_t size) {
    if (size == 0) {
        return std::nullopt;
    }

    MutexLocker guard(&m_mutex);
    if (!m_allocator) {
        return std::nullopt;
    }

    size_t fake_size =
        m_multiplier_bits > 0
            ? ((size + (static_cast<uint64_t>(1) << m_multiplier_bits) - 1u) >>
               m_multiplier_bits)
            : size;

    if (fake_size > SmallFloat::MAX_BIN_SIZE) {
        return std::nullopt;
    }

    OffsetAllocation allocation = m_allocator->allocate(fake_size);
    if (allocation.isNoSpace()) {
        // Log metrics to help understand why allocation failed
        // Note: We're already holding m_mutex, so use internal method
        OffsetAllocatorMetrics metrics = get_metrics_internal();
        VLOG(1) << "OffsetAllocator allocation failed: size=" << size
                << ", fake_size=" << fake_size << ", " << metrics;
        return std::nullopt;
    }

    // Update lightweight metrics
    m_allocated_size += size;
    m_allocated_num++;

    // Use shared_from_this to get a shared_ptr to this OffsetAllocator
    return OffsetAllocationHandle(
        shared_from_this(), allocation,
        m_base + (allocation.getOffset() << m_multiplier_bits), size);
}

OffsetAllocStorageReport OffsetAllocator::storageReport() const {
    MutexLocker guard(&m_mutex);
    if (!m_allocator) {
        return {0, 0};
    }
    OffsetAllocStorageReport report = m_allocator->storageReport();
    return {report.totalFreeSpace << m_multiplier_bits,
            report.largestFreeRegion << m_multiplier_bits};
}

OffsetAllocStorageReportFull OffsetAllocator::storageReportFull() const {
    MutexLocker lock(&m_mutex);
    if (!m_allocator) {
        OffsetAllocStorageReportFull report{};
        return report;
    }
    OffsetAllocStorageReportFull report = m_allocator->storageReportFull();
    for (uint32 i = 0; i < NUM_LEAF_BINS; i++) {
        report.freeRegions[i] = {
            .size = report.freeRegions[i].size << m_multiplier_bits,
            .count = report.freeRegions[i].count};
    }
    return report;
}

OffsetAllocatorMetrics OffsetAllocator::get_metrics_internal() const {
    if (!m_allocator) {
        return {0, 0, 0, 0, m_capacity};
    }

    // Get basic storage report
    OffsetAllocStorageReport basic_report = m_allocator->storageReport();
    return {
        m_allocated_size,  // allocated_size_
        m_allocated_num,   // allocated_num_
        basic_report.largestFreeRegion
            << m_multiplier_bits,  // largest_free_region_
        basic_report.totalFreeSpace << m_multiplier_bits,  // total_free_space_
        m_capacity,                                        // capacity
    };
}

OffsetAllocatorMetrics OffsetAllocator::get_metrics() const {
    MutexLocker guard(&m_mutex);
    return get_metrics_internal();
}

void OffsetAllocator::freeAllocation(const OffsetAllocation& allocation,
                                     uint64_t size) {
    MutexLocker lock(&m_mutex);
    if (m_allocator) {
        m_allocator->free(allocation);
        // Update lightweight metrics
        m_allocated_size -= size;
        m_allocated_num--;
    }
}

// Stream output operator implementation
std::ostream& operator<<(std::ostream& os,
                         const OffsetAllocatorMetrics& metrics) {
    double utilization =
        metrics.capacity > 0
            ? (double)metrics.allocated_size_ / metrics.capacity * 100.0
            : 0.0;
    os << "OffsetAllocatorMetrics{"
       << "allocated=" << mooncake::byte_size_to_string(metrics.allocated_size_)
       << ", allocs=" << metrics.allocated_num_
       << ", capacity=" << mooncake::byte_size_to_string(metrics.capacity)
       << ", utilization=" << std::fixed << std::setprecision(1) << utilization
       << "%"
       << ", free_space="
       << mooncake::byte_size_to_string(metrics.total_free_space_)
       << ", largest_free="
       << mooncake::byte_size_to_string(metrics.largest_free_region_) << "}";
    return os;
}

}  // namespace mooncake::offset_allocator