// Copyright 2026 KVCache.AI
// Simple arena allocator for mmap-based memory allocation
// Used by SGLang HiCache for fast buffer allocation

#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <glog/logging.h>

namespace mooncake {

/**
 * @brief Simple lock-free arena allocator for mmap'd memory
 *
 * Performance: ~50-60ns per allocation (CAS loop)
 * vs ~1000ns for mmap() calls
 *
 * Thread-safe: allocate() is lock-free (CAS), initialize() is mutex-guarded.
 */
class MmapArena {
public:
    struct Stats {
        size_t pool_size;
        size_t allocated_bytes;
        size_t peak_allocated;
        size_t num_allocations;
        size_t num_failed_allocs;
    };

    MmapArena();
    ~MmapArena();

    // Delete copy and move operations (class contains atomics)
    MmapArena(const MmapArena&) = delete;
    MmapArena& operator=(const MmapArena&) = delete;
    MmapArena(MmapArena&&) = delete;
    MmapArena& operator=(MmapArena&&) = delete;

    /**
     * Initialize arena with a large mmap'd pool
     * @param pool_size Total size to pre-allocate (aligned to huge pages)
     * @param alignment Allocation alignment (default 64 bytes)
     * @return true on success
     */
    bool initialize(size_t pool_size, size_t alignment = 64);

    /**
     * Allocate memory from arena
     * @param size Number of bytes to allocate
     * @param alignment Per-call alignment override (0 = use arena default).
     *        Effective alignment is max(arena default, this parameter).
     * @return Pointer to allocated memory, or nullptr if OOM
     */
    void* allocate(size_t size, size_t alignment = 0);

    /**
     * Get current arena statistics
     */
    Stats getStats() const;

    /**
     * Check if arena is initialized (thread-safe)
     */
    bool isInitialized() const {
        return pool_base_.load(std::memory_order_acquire) != nullptr;
    }

    /**
     * Check if pointer was allocated from this arena
     * Used by free_buffer_mmap_memory to determine allocation type
     * @param ptr Pointer to check
     * @return true if ptr is in arena's address range
     */
    bool owns(const void* ptr) const;

    /**
     * Get pool base and size (for debugging/testing)
     */
    void* getPoolBase() const {
        return pool_base_.load(std::memory_order_acquire);
    }

    size_t getPoolSize() const {
        return pool_size_.load(std::memory_order_acquire);
    }

private:
    std::atomic<void*> pool_base_;      // Base address of mmap'd pool (atomic for thread-safety)
    std::atomic<size_t> pool_size_;     // Total pool size (atomic for thread-safety)
    std::atomic<size_t> alignment_;    // Default allocation alignment (atomic for thread-safety)

    std::atomic<size_t> alloc_cursor_;      // Current allocation offset
    std::atomic<size_t> peak_allocated_;    // Peak memory usage
    std::atomic<size_t> num_allocations_;   // Total allocations
    std::atomic<size_t> num_failed_allocs_; // Failed allocations (OOM)

    std::mutex init_mutex_;                // Guards initialize() against concurrent calls
};

} // namespace mooncake
