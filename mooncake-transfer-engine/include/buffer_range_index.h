// Copyright 2026 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef BUFFER_RANGE_INDEX_H_
#define BUFFER_RANGE_INDEX_H_

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <vector>

namespace mooncake {

// Coverage predicates for BufferDesc-style {addr, length} records. A request
// must sit entirely inside one MR. Length 0 degenerates to point containment,
// so an address on the exclusive end of one buffer belongs to the next one
// only.
inline bool bufferCoversPoint(uint64_t buf_addr, uint64_t buf_len,
                              uint64_t address) {
    return address >= buf_addr && address - buf_addr < buf_len;
}

inline bool bufferCoversRange(uint64_t buf_addr, uint64_t buf_len,
                              uint64_t offset, size_t length) {
    if (length == 0) return bufferCoversPoint(buf_addr, buf_len, offset);
    return offset >= buf_addr && length <= buf_len &&
           offset - buf_addr <= buf_len - length;
}

// Address-sorted snapshot of a SegmentDesc buffer list, so a lookup that
// misses the caller's last-hit cache does not rescan every registered MR.
//
// Lookups are answered for non-overlapping lists only. There a covering
// buffer is unique, so the answer is necessarily the same one a linear scan
// would have matched first. Overlapping lists report overlaps() and are left
// to the plain original-order scan, which keeps first-match semantics true by
// construction instead of by reconstruction.
//
// The snapshot is derived from `buffers`: rebuild it after every mutation of
// that vector, before the descriptor is published.
class BufferRangeIndex {
   public:
    BufferRangeIndex() = default;

    template <typename Buffer>
    explicit BufferRangeIndex(const std::vector<Buffer> &buffers) {
        rebuild(buffers);
    }

    template <typename Buffer>
    void rebuild(const std::vector<Buffer> &buffers) {
        by_start_.clear();
        by_start_.reserve(buffers.size());
        for (size_t i = 0; i < buffers.size(); ++i) {
            by_start_.push_back(
                Entry{buffers[i].addr, buffers[i].length, static_cast<int>(i)});
        }
        std::sort(by_start_.begin(), by_start_.end(),
                  [](const Entry &a, const Entry &b) {
                      if (a.addr != b.addr) return a.addr < b.addr;
                      return a.index < b.index;
                  });
        overlaps_ = false;
        for (size_t i = 1; i < by_start_.size(); ++i) {
            if (by_start_[i].addr < exclusiveEnd(by_start_[i - 1])) {
                overlaps_ = true;
                break;
            }
        }
    }

    bool empty() const { return by_start_.empty(); }
    bool overlaps() const { return overlaps_; }
    size_t size() const { return by_start_.size(); }

    // Position in the original buffer list of the MR covering
    // [addr, addr + length), or -1 when there is none. Overlapping lists
    // always answer -1.
    int findCovering(uint64_t addr, size_t length) const {
        if (overlaps_ || by_start_.empty()) return -1;
        const auto first_after =
            std::upper_bound(by_start_.begin(), by_start_.end(), addr,
                             [](uint64_t value, const Entry &entry) {
                                 return value < entry.addr;
                             });
        if (first_after == by_start_.begin()) return -1;
        const Entry &candidate = *std::prev(first_after);
        return bufferCoversRange(candidate.addr, candidate.length, addr, length)
                   ? candidate.index
                   : -1;
    }

   private:
    struct Entry {
        uint64_t addr = 0;
        uint64_t length = 0;
        int index = 0;
    };

    static uint64_t exclusiveEnd(const Entry &entry) {
        const uint64_t end = entry.addr + entry.length;
        return end < entry.addr ? std::numeric_limits<uint64_t>::max() : end;
    }

    std::vector<Entry> by_start_;
    bool overlaps_ = false;
};

}  // namespace mooncake

#endif  // BUFFER_RANGE_INDEX_H_
