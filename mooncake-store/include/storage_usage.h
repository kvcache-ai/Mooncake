#pragma once

#include <atomic>
#include <cstddef>

namespace mooncake {

struct StorageUsage {
    size_t used_bytes{0};
    size_t capacity_bytes{0};

    [[nodiscard]] double used_ratio() const noexcept {
        if (capacity_bytes == 0) {
            return 0.0;
        }
        return static_cast<double>(used_bytes) /
               static_cast<double>(capacity_bytes);
    }
};

/**
 * Maintains aggregate usage for one storage domain.
 *
 * Mirrors the old metric-gauge model: allocation updates and reads are plain
 * atomics. Mount/unmount briefly updates used and capacity independently, so
 * readers may observe a best-effort torn pair during topology changes—the same
 * class of race the previous metric-based watermark path accepted.
 */
class StorageUsageTracker {
   public:
    [[nodiscard]] StorageUsage GetUsage() const noexcept {
        return StorageUsage{
            .used_bytes = used_bytes_.load(std::memory_order_relaxed),
            .capacity_bytes = capacity_bytes_.load(std::memory_order_relaxed),
        };
    }

    void AddUsedBytes(size_t bytes) noexcept {
        used_bytes_.fetch_add(bytes, std::memory_order_relaxed);
    }

    void RemoveUsedBytes(size_t bytes) noexcept {
        used_bytes_.fetch_sub(bytes, std::memory_order_relaxed);
    }

    void AttachAllocator(size_t used_bytes, size_t capacity_bytes) noexcept {
        used_bytes_.fetch_add(used_bytes, std::memory_order_relaxed);
        capacity_bytes_.fetch_add(capacity_bytes, std::memory_order_relaxed);
    }

    void DetachAllocator(size_t used_bytes, size_t capacity_bytes) noexcept {
        used_bytes_.fetch_sub(used_bytes, std::memory_order_relaxed);
        capacity_bytes_.fetch_sub(capacity_bytes, std::memory_order_relaxed);
    }

   private:
    std::atomic_size_t used_bytes_{0};
    std::atomic_size_t capacity_bytes_{0};
};

}  // namespace mooncake
