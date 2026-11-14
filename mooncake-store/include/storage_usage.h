#pragma once

#include <atomic>
#include <cstddef>
#include <memory>
#include <utility>

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
 * Requested and rounding bytes are also sampled independently during
 * allocation/deallocation. Watermarks use a best-effort snapshot.
 */
class StorageUsageTracker {
   public:
    [[nodiscard]] StorageUsage GetUsage() const noexcept {
        return StorageUsage{
            .used_bytes = used_bytes_.load(std::memory_order_relaxed),
            .capacity_bytes = capacity_bytes_.load(std::memory_order_relaxed),
        };
    }

    [[nodiscard]] StorageUsage GetEvictionUsage() const noexcept {
        auto usage = GetUsage();
        usage.used_bytes += rounding_bytes_.load(std::memory_order_relaxed);
        return usage;
    }

    void AddUsedBytes(size_t bytes, size_t rounding_bytes) noexcept {
        used_bytes_.fetch_add(bytes, std::memory_order_relaxed);
        if (rounding_bytes != 0) {
            rounding_bytes_.fetch_add(rounding_bytes,
                                      std::memory_order_relaxed);
        }
    }

    void RemoveUsedBytes(size_t bytes, size_t rounding_bytes) noexcept {
        used_bytes_.fetch_sub(bytes, std::memory_order_relaxed);
        if (rounding_bytes != 0) {
            rounding_bytes_.fetch_sub(rounding_bytes,
                                      std::memory_order_relaxed);
        }
    }

    void AttachAllocator(size_t used_bytes, size_t rounding_bytes,
                         size_t capacity_bytes) noexcept {
        AddUsedBytes(used_bytes, rounding_bytes);
        capacity_bytes_.fetch_add(capacity_bytes, std::memory_order_relaxed);
    }

    void DetachAllocator(size_t used_bytes, size_t rounding_bytes,
                         size_t capacity_bytes) noexcept {
        RemoveUsedBytes(used_bytes, rounding_bytes);
        capacity_bytes_.fetch_sub(capacity_bytes, std::memory_order_relaxed);
    }

   private:
    std::atomic_size_t used_bytes_{0};
    std::atomic_size_t capacity_bytes_{0};
    // Bytes reserved by Offset DRAM beyond the requested lengths.
    std::atomic_size_t rounding_bytes_{0};
};

/**
 * RAII registration of one allocator into a StorageUsageTracker.
 *
 * Attach once before the allocator is published. The registration stays
 * immutable for the allocator's lifetime and detaches from the destructor,
 * after in-flight allocate/deallocate calls have dropped their local
 * shared_ptr and the allocator can be destroyed.
 */
class StorageUsageRegistration {
   public:
    StorageUsageRegistration(std::shared_ptr<StorageUsageTracker> tracker,
                             const std::atomic_size_t& used_bytes,
                             const std::atomic_size_t& rounding_bytes,
                             size_t capacity_bytes) noexcept
        : tracker_(std::move(tracker)),
          used_bytes_(used_bytes),
          rounding_bytes_(rounding_bytes),
          capacity_bytes_(capacity_bytes) {
        tracker_->AttachAllocator(
            used_bytes_.load(std::memory_order_relaxed),
            rounding_bytes_.load(std::memory_order_relaxed), capacity_bytes_);
    }

    ~StorageUsageRegistration() {
        tracker_->DetachAllocator(
            used_bytes_.load(std::memory_order_relaxed),
            rounding_bytes_.load(std::memory_order_relaxed), capacity_bytes_);
    }

    StorageUsageRegistration(const StorageUsageRegistration&) = delete;
    StorageUsageRegistration& operator=(const StorageUsageRegistration&) =
        delete;

    void AddUsedBytes(size_t bytes, size_t rounding_bytes) noexcept {
        tracker_->AddUsedBytes(bytes, rounding_bytes);
    }

    void RemoveUsedBytes(size_t bytes, size_t rounding_bytes) noexcept {
        tracker_->RemoveUsedBytes(bytes, rounding_bytes);
    }

   private:
    std::shared_ptr<StorageUsageTracker> tracker_;
    const std::atomic_size_t& used_bytes_;
    const std::atomic_size_t& rounding_bytes_;
    size_t capacity_bytes_;
};

}  // namespace mooncake
