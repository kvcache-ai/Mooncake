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

MmapArena::MmapArena()
    : pool_base_(nullptr)
    , pool_size_(0)
    , alignment_(64)  // Default minimum alignment (cache line)
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
    // Mutex serializes concurrent initialize() calls so that exactly one
    // thread performs the mmap and publishes the pool.  This avoids the
    // metadata-overwrite race that existed with the old CAS approach
    // (losing threads could clobber alignment_/pool_size_ before CAS).
    std::lock_guard<std::mutex> lock(init_mutex_);

    if (pool_size == 0) {
        LOG(ERROR) << "Arena pool size must be > 0";
        return false;
    }

    if (alignment != 0 && (alignment & (alignment - 1)) != 0) {
        LOG(ERROR) << "Arena alignment must be a power of 2, got " << alignment;
        return false;
    }

    if (pool_base_.load(std::memory_order_acquire) != nullptr) {
        LOG(WARNING) << "Arena already initialized";
        return false;
    }

    size_t actual_alignment = std::max(alignment, size_t(64));

    // Align pool size to 2MB for huge pages with overflow protection
    const size_t HUGE_PAGE_SIZE = 2 * 1024 * 1024;
    size_t aligned_pool_size;
    if (!safe_align_up(pool_size, HUGE_PAGE_SIZE, &aligned_pool_size)) {
        LOG(ERROR) << "Arena pool size overflow: requested=" << pool_size;
        return false;
    }

    // Allocate pool with mmap.  Use MAP_POPULATE so that all huge pages
    // are faulted upfront.  Without it, lazy page-faults during GPU DMA
    // (e.g. Mooncake transfer engine ↔ CUDA) cause cudaErrorIllegalAddress
    // on some platforms (observed on H100 80GB).
    int flags = MAP_PRIVATE | MAP_ANONYMOUS | MAP_POPULATE;

    // Try huge pages for better TLB performance
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

    // Store metadata BEFORE publishing pool_base_.
    // The release store on pool_base_ ensures these are visible to any
    // thread that loads pool_base_ with acquire in allocate().
    alignment_.store(actual_alignment, std::memory_order_relaxed);
    pool_size_.store(aligned_pool_size, std::memory_order_relaxed);
    pool_base_.store(pool_base, std::memory_order_release);

    LOG(INFO) << "Arena initialized: "
              << (aligned_pool_size / (1024.0 * 1024.0 * 1024.0))
              << " GB, alignment=" << actual_alignment << " bytes";

    return true;
}

void* MmapArena::allocate(size_t size, size_t alignment) {
    void* pool_base = pool_base_.load(std::memory_order_acquire);
    if (pool_base == nullptr) {
        LOG(ERROR) << "Arena not initialized";
        return nullptr;
    }

    if (size == 0) {
        return nullptr;
    }

    // Effective alignment: max of arena default and caller's request.
    // This honors the caller's alignment contract without weakening
    // the arena's minimum guarantee.
    size_t base_alignment = alignment_.load(std::memory_order_relaxed);
    size_t effective_alignment = std::max(base_alignment, alignment);

    // Align allocation size with overflow check
    size_t aligned_size;
    if (!safe_align_up(size, effective_alignment, &aligned_size)) {
        num_failed_allocs_.fetch_add(1, std::memory_order_relaxed);
        LOG(ERROR) << "Arena allocation size overflow: size=" << size
                  << ", alignment=" << effective_alignment;
        return nullptr;
    }

    size_t pool_size = pool_size_.load(std::memory_order_acquire);

    // CAS loop: Reserve aligned space atomically with bounds check.
    // We align the OFFSET (not just the size) so the returned pointer
    // honours the caller's alignment contract even when the cursor sits
    // at a non-aligned position from a previous smaller-alignment alloc.
    size_t aligned_offset;
    size_t next;
    while (true) {
        size_t raw = alloc_cursor_.load(std::memory_order_relaxed);

        // Align the offset up to effective_alignment
        if (!safe_align_up(raw, effective_alignment, &aligned_offset)) {
            num_failed_allocs_.fetch_add(1, std::memory_order_relaxed);
            LOG(ERROR) << "Arena offset alignment overflow: raw=" << raw
                      << ", alignment=" << effective_alignment;
            return nullptr;
        }

        next = aligned_offset + aligned_size;

        // Check for overflow (next wrapped) and OOM BEFORE modifying cursor
        if (next < aligned_offset || next > pool_size) {
            num_failed_allocs_.fetch_add(1, std::memory_order_relaxed);
            LOG(ERROR) << "Arena OOM: requested=" << size
                      << ", aligned_size=" << aligned_size
                      << ", aligned_offset=" << aligned_offset
                      << ", pool_size=" << pool_size;
            return nullptr;
        }

        // Try to reserve [aligned_offset, next) atomically.
        // CAS from raw (not aligned_offset) — another thread may have
        // bumped the cursor since we loaded it.
        if (alloc_cursor_.compare_exchange_weak(raw, next,
                                                std::memory_order_relaxed,
                                                std::memory_order_relaxed)) {
            break;  // Success - space reserved
        }
        // CAS failed, retry with new raw value
    }

    // Space successfully reserved at [aligned_offset, next)
    num_allocations_.fetch_add(1, std::memory_order_relaxed);

    // Update peak statistics using `next` (the actual end of reservation,
    // including any alignment padding before aligned_offset)
    size_t old_peak = peak_allocated_.load(std::memory_order_relaxed);
    while (next > old_peak &&
           !peak_allocated_.compare_exchange_weak(old_peak, next,
                                                   std::memory_order_relaxed,
                                                   std::memory_order_relaxed)) {
        // CAS loop for peak tracking
    }

    void* ptr = static_cast<char*>(pool_base) + aligned_offset;

    VLOG(2) << "[ARENA] Allocated: size=" << size
            << ", aligned_size=" << aligned_size
            << ", aligned_offset=" << aligned_offset
            << ", ptr=" << ptr
            << ", utilization=" << (100.0 * next / pool_size) << "%";

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
