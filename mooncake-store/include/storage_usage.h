#pragma once

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <limits>
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
 * Uses cache-line-padded per-thread stripes for used_bytes so concurrent
 * allocate/deallocate calls across segments do not contend on a single global
 * cache line. Updates use unsigned wrapping arithmetic across stripes so
 * cross-thread add/remove pairs always sum exactly once quiescent, without
 * signed overflow UB. Because GetUsage() reads stripes and capacity_bytes_
 * with relaxed atomics, concurrent readers may observe a best-effort torn
 * snapshot (clamping any transient torn underflow to 0)—matching the same
 * best-effort watermark semantics as the previous metric-based path.
 */
class StorageUsageTracker {
   public:
    static constexpr size_t kNumStripes = 64;
    static_assert((kNumStripes & (kNumStripes - 1)) == 0,
                  "kNumStripes must be a power of two");

    [[nodiscard]] StorageUsage GetUsage() const noexcept {
        uint64_t total_used = 0;
        for (const auto& stripe : used_stripes_) {
            total_used += stripe.value.load(std::memory_order_relaxed);
        }
        // Under concurrent cross-thread add/remove, a relaxed stripe scan can
        // transiently observe a subtraction before its paired addition, which
        // wraps near UINT64_MAX in two's-complement modulo-2^64 arithmetic.
        constexpr uint64_t kUnderflowThreshold =
            std::numeric_limits<uint64_t>::max() >> 1;
        const size_t clamped_used = total_used > kUnderflowThreshold
                                        ? 0
                                        : static_cast<size_t>(total_used);
        return StorageUsage{
            .used_bytes = clamped_used,
            .capacity_bytes = capacity_bytes_.load(std::memory_order_relaxed),
        };
    }

    void AddUsedBytes(size_t bytes) noexcept {
        StripeForCurrentThread().fetch_add(static_cast<uint64_t>(bytes),
                                           std::memory_order_relaxed);
    }

    void RemoveUsedBytes(size_t bytes) noexcept {
        StripeForCurrentThread().fetch_sub(static_cast<uint64_t>(bytes),
                                           std::memory_order_relaxed);
    }

    void AttachAllocator(size_t used_bytes, size_t capacity_bytes) noexcept {
        AddUsedBytes(used_bytes);
        capacity_bytes_.fetch_add(capacity_bytes, std::memory_order_relaxed);
    }

    void DetachAllocator(size_t used_bytes, size_t capacity_bytes) noexcept {
        RemoveUsedBytes(used_bytes);
        capacity_bytes_.fetch_sub(capacity_bytes, std::memory_order_relaxed);
    }

   private:
    struct alignas(64) PaddedCounter {
        std::atomic<uint64_t> value{0};
    };

    std::atomic<uint64_t>& StripeForCurrentThread() noexcept {
        thread_local const size_t idx =
            next_stripe_.fetch_add(1, std::memory_order_relaxed) &
            (kNumStripes - 1);
        return used_stripes_[idx].value;
    }

    static inline std::atomic<size_t> next_stripe_{0};
    std::array<PaddedCounter, kNumStripes> used_stripes_{};
    alignas(64) std::atomic_size_t capacity_bytes_{0};
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
                             std::atomic_size_t& used_bytes,
                             size_t capacity_bytes) noexcept
        : tracker_(std::move(tracker)),
          used_bytes_(used_bytes),
          capacity_bytes_(capacity_bytes) {
        tracker_->AttachAllocator(used_bytes_.load(std::memory_order_relaxed),
                                  capacity_bytes_);
    }

    ~StorageUsageRegistration() {
        tracker_->DetachAllocator(used_bytes_.load(std::memory_order_relaxed),
                                  capacity_bytes_);
    }

    StorageUsageRegistration(const StorageUsageRegistration&) = delete;
    StorageUsageRegistration& operator=(const StorageUsageRegistration&) =
        delete;

    void AddUsedBytes(size_t bytes) noexcept { tracker_->AddUsedBytes(bytes); }

    void RemoveUsedBytes(size_t bytes) noexcept {
        tracker_->RemoveUsedBytes(bytes);
    }

   private:
    std::shared_ptr<StorageUsageTracker> tracker_;
    std::atomic_size_t& used_bytes_;
    size_t capacity_bytes_;
};

}  // namespace mooncake
