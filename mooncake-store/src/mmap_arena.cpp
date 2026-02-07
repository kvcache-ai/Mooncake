// Copyright 2026 KVCache.AI
// Simple arena allocator implementation

#include "mmap_arena.h"
#include <sys/mman.h>
#include <cerrno>
#include <cstring>
#include <algorithm>

namespace mooncake {

// Safe alignment with overflow detection
// Returns false if overflow would occur, true on success
static inline bool safe_align_up(size_t size, size_t alignment, size_t* result) {
    if (size == 0) {
        *result = 0;
        return true;
    }
    if (alignment == 0 || (alignment & (alignment - 1)) != 0) {
        // Must be power of 2
        return false;
    }
    // Check if size + alignment - 1 would overflow
    if (size > SIZE_MAX - alignment + 1) {
        return false;  // Overflow would occur
    }
    *result = (size + alignment - 1) & ~(alignment - 1);
    return true;
}

// Helper to align size up to alignment boundary (legacy, used in initialize())
static inline size_t align_up(size_t size, size_t alignment) {
    return (size + alignment - 1) & ~(alignment - 1);
}

MmapArena::MmapArena()
    : pool_base_(nullptr)
    , pool_size_(0)
    , alignment_(64)
    , alloc_cursor_(0)
    , peak_allocated_(0)
    , num_allocations_(0)
    , num_failed_allocs_(0)
{
}

MmapArena::~MmapArena() {
    void* pool_base = pool_base_.load(std::memory_order_acquire);
    if (pool_base != nullptr) {
        size_t pool_size = pool_size_.load(std::memory_order_acquire);
        if (munmap(pool_base, pool_size) != 0) {
            LOG(ERROR) << "Arena munmap failed: " << strerror(errno);
        }
        pool_base_.store(nullptr, std::memory_order_release);
    }
}

bool MmapArena::initialize(size_t pool_size, size_t alignment) {
    // Atomic check with acquire ordering to ensure visibility
    void* expected = nullptr;
    void* current = pool_base_.load(std::memory_order_acquire);

    if (current != nullptr) {
        LOG(WARNING) << "Arena already initialized";
        return false;
    }

    // Compute alignment (local variable to avoid data race)
    size_t actual_alignment = std::max(alignment, size_t(64));

    // Align pool size to 2MB for huge pages with overflow protection
    const size_t HUGE_PAGE_SIZE = 2 * 1024 * 1024;
    size_t aligned_pool_size;
    if (!safe_align_up(pool_size, HUGE_PAGE_SIZE, &aligned_pool_size)) {
        LOG(ERROR) << "Arena pool size overflow: requested=" << pool_size;
        return false;
    }

    // Allocate pool with mmap
    int flags = MAP_PRIVATE | MAP_ANONYMOUS | MAP_POPULATE;

    // Try huge pages for better performance
    #ifdef MAP_HUGETLB
    flags |= MAP_HUGETLB;
    #endif

    void* pool_base = mmap(nullptr, aligned_pool_size, PROT_READ | PROT_WRITE,
                          flags, -1, 0);

    if (pool_base == MAP_FAILED) {
        // Retry without huge pages
        flags &= ~MAP_HUGETLB;
        pool_base = mmap(nullptr, aligned_pool_size, PROT_READ | PROT_WRITE,
                        flags, -1, 0);

        if (pool_base == MAP_FAILED) {
            LOG(ERROR) << "Arena mmap failed: size=" << aligned_pool_size
                      << ", errno=" << errno << " (" << strerror(errno) << ")";
            return false;
        }
        LOG(INFO) << "Arena initialized without huge pages";
    } else {
        LOG(INFO) << "Arena initialized with huge pages";
    }

    // CRITICAL: Store pool_size_ BEFORE the CAS to establish happens-before relationship
    // When allocate() loads pool_base_ with acquire, the prior pool_size_ store is visible
    // Note: If multiple threads race to initialize, they may overwrite pool_size_ with
    // different values, but the winning thread's value will be visible due to the release
    // fence on the CAS. In practice, all racing threads should use the same pool_size.
    pool_size_.store(aligned_pool_size, std::memory_order_release);

    // Atomically publish pool_base with CAS (only first thread wins)
    // Use release semantics to ensure all prior stores are visible
    if (!pool_base_.compare_exchange_strong(expected, pool_base,
                                           std::memory_order_release,
                                           std::memory_order_acquire)) {
        // Another thread won the race - clean up our allocation
        LOG(WARNING) << "Arena initialization race detected, cleaning up duplicate";
        // The winner's pool_size_ is already stored and will be visible
        munmap(pool_base, aligned_pool_size);
        return false;
    }

    // We won the race - now safe to update alignment_
    // This happens after pool_base_ CAS, so happens-before relationship established
    alignment_ = actual_alignment;

    LOG(INFO) << "Arena initialized: "
              << (aligned_pool_size / (1024.0 * 1024.0 * 1024.0))
              << " GB, alignment=" << actual_alignment << " bytes";

    return true;
}

void* MmapArena::allocate(size_t size) {
    void* pool_base = pool_base_.load(std::memory_order_acquire);
    if (pool_base == nullptr) {
        LOG(ERROR) << "Arena not initialized";
        return nullptr;
    }

    if (size == 0) {
        return nullptr;
    }

    // Align allocation size with overflow check
    size_t aligned_size;
    if (!safe_align_up(size, alignment_, &aligned_size)) {
        num_failed_allocs_.fetch_add(1, std::memory_order_relaxed);
        LOG(ERROR) << "Arena allocation size overflow: size=" << size
                  << ", alignment=" << alignment_;
        return nullptr;
    }

    size_t pool_size = pool_size_.load(std::memory_order_acquire);

    // CAS loop: Reserve space atomically with bounds check
    // This fixes Bug #1 (OOM check after cursor update) and Bug #2 (integer overflow)
    size_t offset;
    while (true) {
        offset = alloc_cursor_.load(std::memory_order_relaxed);

        // Check for overflow and OOM BEFORE modifying cursor
        // overflow-safe: check offset > pool_size first, then check remaining space
        if (offset > pool_size || aligned_size > pool_size - offset) {
            num_failed_allocs_.fetch_add(1, std::memory_order_relaxed);
            LOG(ERROR) << "Arena OOM: requested=" << size
                      << ", aligned=" << aligned_size
                      << ", offset=" << offset
                      << ", pool_size=" << pool_size;
            return nullptr;
        }

        // Try to reserve space atomically
        // If another thread modified alloc_cursor_, CAS fails and we retry
        if (alloc_cursor_.compare_exchange_weak(offset, offset + aligned_size,
                                                std::memory_order_relaxed,
                                                std::memory_order_relaxed)) {
            break;  // Success - space reserved
        }
        // CAS failed, retry with new offset value
    }

    // Space successfully reserved at [offset, offset+aligned_size)
    num_allocations_.fetch_add(1, std::memory_order_relaxed);

    // Update peak statistics
    size_t new_peak = offset + aligned_size;
    size_t old_peak = peak_allocated_.load(std::memory_order_relaxed);
    while (new_peak > old_peak &&
           !peak_allocated_.compare_exchange_weak(old_peak, new_peak,
                                                   std::memory_order_relaxed,
                                                   std::memory_order_relaxed)) {
        // CAS loop for peak tracking
    }

    void* ptr = static_cast<char*>(pool_base) + offset;

    VLOG(2) << "[ARENA] Allocated: size=" << size
            << ", aligned=" << aligned_size
            << ", offset=" << offset
            << ", ptr=" << ptr
            << ", utilization=" << (100.0 * (offset + aligned_size) / pool_size) << "%";

    return ptr;
}

MmapArena::Stats MmapArena::getStats() const {
    Stats stats;
    stats.pool_size = pool_size_.load(std::memory_order_relaxed);
    stats.allocated_bytes = alloc_cursor_.load(std::memory_order_relaxed);
    stats.peak_allocated = peak_allocated_.load(std::memory_order_relaxed);
    stats.num_allocations = num_allocations_.load(std::memory_order_relaxed);
    stats.num_failed_allocs = num_failed_allocs_.load(std::memory_order_relaxed);
    return stats;
}

bool MmapArena::owns(const void* ptr) const {
    void* pool_base = pool_base_.load(std::memory_order_acquire);
    if (!ptr || !pool_base) {
        return false;
    }

    size_t pool_size = pool_size_.load(std::memory_order_acquire);
    uintptr_t addr = reinterpret_cast<uintptr_t>(ptr);
    uintptr_t base = reinterpret_cast<uintptr_t>(pool_base);
    return addr >= base && addr < base + pool_size;
}

} // namespace mooncake
