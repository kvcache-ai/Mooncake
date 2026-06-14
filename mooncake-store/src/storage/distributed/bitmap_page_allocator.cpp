#include "storage/distributed/bitmap_page_allocator.h"

namespace mooncake {

namespace {

constexpr int64_t kBitsPerWord = 64;

}  // namespace

void BitmapPageAllocator::Init(int64_t page_size, int64_t bucket_size) {
    std::lock_guard<std::mutex> lock(mutex_);

    page_size_ = page_size;
    hint_ = 0;

    if (page_size <= 0 || bucket_size <= 0) {
        num_pages_ = 0;
        bitmap_.clear();
        return;
    }

    num_pages_ = bucket_size / page_size;
    const int64_t word_count = (num_pages_ + kBitsPerWord - 1) / kBitsPerWord;
    bitmap_.assign(static_cast<size_t>(word_count), 0);
}

int64_t BitmapPageAllocator::Allocate(int64_t count) {
    std::lock_guard<std::mutex> lock(mutex_);

    if (count <= 0 || count > num_pages_) {
        return -1;
    }

    auto find_in_range = [&](int64_t search_start,
                             int64_t search_end) -> int64_t {
        int64_t i = search_start;
        while (i + count <= search_end) {
            int64_t first_allocated = -1;
            for (int64_t j = 0; j < count; ++j) {
                if (IsAllocatedLocked(i + j)) {
                    first_allocated = i + j;
                    break;
                }
            }
            if (first_allocated == -1) {
                return i;
            }
            i = first_allocated + 1;
        }
        return -1;
    };

    int64_t found = find_in_range(hint_, num_pages_);
    if (found == -1 && hint_ > 0) {
        found = find_in_range(0, hint_ + count - 1);
    }

    if (found != -1) {
        SetRangeLocked(found, count, true);
        hint_ = (found + count) % num_pages_;
        return found;
    }

    return -1;
}

void BitmapPageAllocator::Free(int64_t page_index, int64_t count) {
    std::lock_guard<std::mutex> lock(mutex_);

    if (!IsValidRangeLocked(page_index, count)) {
        return;
    }

    SetRangeLocked(page_index, count, false);
    if (num_pages_ > 0 && page_index < hint_) {
        hint_ = page_index;
    }
}

void BitmapPageAllocator::MarkAllocated(int64_t page_index, int64_t count) {
    std::lock_guard<std::mutex> lock(mutex_);

    if (!IsValidRangeLocked(page_index, count)) {
        return;
    }

    SetRangeLocked(page_index, count, true);
    if (hint_ >= page_index && hint_ < page_index + count) {
        hint_ = (page_index + count) % num_pages_;
    }
}

bool BitmapPageAllocator::IsAllocated(int64_t page_index) const {
    std::lock_guard<std::mutex> lock(mutex_);

    if (page_index < 0 || page_index >= num_pages_) {
        return false;
    }
    return IsAllocatedLocked(page_index);
}

int64_t BitmapPageAllocator::AllocatedCount() const {
    std::lock_guard<std::mutex> lock(mutex_);

    int64_t count = 0;
    for (uint64_t word : bitmap_) {
        count += __builtin_popcountll(word);
    }
    return count;
}

int64_t BitmapPageAllocator::PageSize() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return page_size_;
}

int64_t BitmapPageAllocator::NumPages() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return num_pages_;
}

bool BitmapPageAllocator::IsValidRangeLocked(int64_t page_index,
                                             int64_t count) const {
    return page_index >= 0 && count > 0 && page_index < num_pages_ &&
           count <= num_pages_ - page_index;
}

bool BitmapPageAllocator::IsRangeFreeLocked(int64_t page_index,
                                            int64_t count) const {
    if (!IsValidRangeLocked(page_index, count)) {
        return false;
    }

    for (int64_t i = 0; i < count; ++i) {
        if (IsAllocatedLocked(page_index + i)) {
            return false;
        }
    }
    return true;
}

void BitmapPageAllocator::SetRangeLocked(int64_t page_index, int64_t count,
                                         bool allocated) {
    for (int64_t i = 0; i < count; ++i) {
        const int64_t current_page = page_index + i;
        uint64_t& word = bitmap_[static_cast<size_t>(WordIndex(current_page))];
        const uint64_t mask = BitMask(current_page);
        if (allocated) {
            word |= mask;
        } else {
            word &= ~mask;
        }
    }
}

bool BitmapPageAllocator::IsAllocatedLocked(int64_t page_index) const {
    const uint64_t word = bitmap_[static_cast<size_t>(WordIndex(page_index))];
    return (word & BitMask(page_index)) != 0;
}

int64_t BitmapPageAllocator::WordIndex(int64_t page_index) {
    return page_index / kBitsPerWord;
}

uint64_t BitmapPageAllocator::BitMask(int64_t page_index) {
    return uint64_t{1} << (page_index % kBitsPerWord);
}

}  // namespace mooncake
