#pragma once

#include <cstdint>
#include <mutex>
#include <vector>

namespace mooncake {

class BitmapPageAllocator {
   public:
    BitmapPageAllocator() = default;

    void Init(int64_t page_size, int64_t bucket_size);

    [[nodiscard]] int64_t Allocate(int64_t count = 1);

    void Free(int64_t page_index, int64_t count = 1);

    void MarkAllocated(int64_t page_index, int64_t count = 1);

    [[nodiscard]] bool IsAllocated(int64_t page_index) const;

    [[nodiscard]] int64_t AllocatedCount() const;

    [[nodiscard]] int64_t PageSize() const;

    [[nodiscard]] int64_t NumPages() const;

   private:
    [[nodiscard]] bool IsValidRangeLocked(int64_t page_index,
                                          int64_t count) const;

    [[nodiscard]] bool IsRangeFreeLocked(int64_t page_index,
                                         int64_t count) const;

    void SetRangeLocked(int64_t page_index, int64_t count, bool allocated);

    [[nodiscard]] bool IsAllocatedLocked(int64_t page_index) const;

    [[nodiscard]] static int64_t WordIndex(int64_t page_index);

    [[nodiscard]] static uint64_t BitMask(int64_t page_index);

    int64_t page_size_{0};
    int64_t num_pages_{0};
    std::vector<uint64_t> bitmap_;
    int64_t hint_{0};
    mutable std::mutex mutex_;
};

}  // namespace mooncake
