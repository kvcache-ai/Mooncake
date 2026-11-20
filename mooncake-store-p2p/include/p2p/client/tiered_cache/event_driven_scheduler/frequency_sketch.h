#pragma once

#include <algorithm>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <mutex>
#include <vector>

namespace mooncake {

/**
 * @file frequency_sketch.h
 * @brief Count-Min Sketch with 4-bit saturating counters and periodic halving
 *        decay — a TinyLFU frequency estimator.
 *
 * Geometry: table = capacity/4 64-bit words, 16 nibbles/word, depth = 4;
 * counters saturate at 15. The decay sample_size defaults to capacity*10, and a
 * halving reset shifts every nibble right by one once the threshold is hit.
 *
 * Hashing: each of the 4 rows runs an INDEPENDENT splitmix64 finalizer with its
 * own (additive, multiplier_a, multiplier_b) constants. We use 4 fully distinct
 * mixers — rather than the same mixer with adjacent seeds — because adjacent
 * seeds (e.g. key^seed then one mix) leave the rows statistically correlated,
 * which weakens the Count-Min `min` estimate; we only require the 4 rows to be
 * statistically independent.
 *
 * Threading: thread-safe — every public method locks an internal mutex, so
 * callers do not serialize externally.
 */
class FixedDecayPolicy {
   public:
    explicit FixedDecayPolicy(uint32_t sample_size)
        : sample_size_(sample_size) {}

    // min(capacity * 10, UINT32_MAX), with saturating multiply.
    static FixedDecayPolicy FromCapacity(size_t cap) {
        const uint64_t cap64 = static_cast<uint64_t>(cap);
        const uint64_t prod =
            (cap64 > std::numeric_limits<uint64_t>::max() / 10)
                ? std::numeric_limits<uint64_t>::max()
                : cap64 * 10;
        const uint32_t sample = (prod > std::numeric_limits<uint32_t>::max())
                                    ? std::numeric_limits<uint32_t>::max()
                                    : static_cast<uint32_t>(prod);
        return FixedDecayPolicy(sample);
    }

    bool ShouldDecay(uint32_t increments_since_last_reset) const {
        return increments_since_last_reset >= sample_size_;
    }

    uint32_t sample_size() const { return sample_size_; }

   private:
    uint32_t sample_size_;
};

class FrequencySketch {
   public:
    // sample_size == 0 => derive from capacity via FixedDecayPolicy.
    explicit FrequencySketch(size_t capacity, uint32_t sample_size = 0)
        : table_(std::max<size_t>(1, capacity / 4), 0),
          sample_size_(
              sample_size == 0
                  ? FixedDecayPolicy::FromCapacity(capacity).sample_size()
                  : sample_size) {}

    // Bump all 4 rows (saturating at 15). Triggers a halving reset on
    // threshold.
    void Increment(uint64_t key) {
        std::lock_guard<std::mutex> lock(mu_);
        IncrementLocked(key);
    }

    // Fused Increment + Estimate: bump all 4 rows AND return the post-increment
    // Count-Min estimate in a SINGLE pass — one Hash() per row instead of the
    // two passes (8 hashes) that Increment() then Estimate() would do. Used on
    // the hot access path. Semantically identical to Increment then Estimate.
    uint32_t IncrementAndEstimate(uint64_t key) {
        std::lock_guard<std::mutex> lock(mu_);
        if (table_.empty()) {
            return 0;
        }
        bool added = false;
        uint32_t min_count = std::numeric_limits<uint32_t>::max();
        for (int row = 0; row < kDepth; ++row) {
            const uint64_t h = Hash(key, row);
            const size_t cell = static_cast<size_t>(h % table_.size());
            const uint8_t nibble = static_cast<uint8_t>(h & 15);
            if (IncrementAt(cell, nibble)) {
                added = true;
            }
            min_count = std::min<uint32_t>(min_count, CountAt(cell, nibble));
        }
        if (added) {
            ++size_;
            if (sample_size_ != 0 && size_ >= sample_size_) {
                ResetLocked();
                // The halving invalidated the counts read above; recompute.
                return EstimateLocked(key);
            }
        }
        return min_count;
    }

    // Count-Min estimate: min across the 4 rows. Range [0, 15].
    uint32_t Estimate(uint64_t key) const {
        std::lock_guard<std::mutex> lock(mu_);
        return EstimateLocked(key);
    }

    // Halving decay: every nibble >>= 1; size_ is roughly halved minus a
    // correction for the population of odd (lost) low bits.
    void Reset() {
        std::lock_guard<std::mutex> lock(mu_);
        ResetLocked();
    }

    uint32_t size() const {
        std::lock_guard<std::mutex> lock(mu_);
        return size_;
    }

    // Approximate resident memory of the sketch table (for tests / accounting):
    // ~8 * max(1, capacity/4) bytes. table_ size is fixed after construction.
    size_t MemoryBytes() const { return table_.size() * sizeof(uint64_t); }

   private:
    static constexpr int kDepth = 4;
    static constexpr uint64_t kOneMask = 0x1111111111111111ULL;
    static constexpr uint64_t kResetMask = 0x7777777777777777ULL;

    // --- Internal, NON-locking primitives (caller must hold mu_). ---

    void IncrementLocked(uint64_t key) {
        if (table_.empty()) {
            return;
        }
        bool added = false;
        for (int row = 0; row < kDepth; ++row) {
            const uint64_t h = Hash(key, row);
            const size_t cell = static_cast<size_t>(h % table_.size());
            const uint8_t nibble = static_cast<uint8_t>(h & 15);
            if (IncrementAt(cell, nibble)) {
                added = true;
            }
        }
        if (added) {
            ++size_;
            if (sample_size_ != 0 && size_ >= sample_size_) {
                ResetLocked();
            }
        }
    }

    uint32_t EstimateLocked(uint64_t key) const {
        if (table_.empty()) {
            return 0;
        }
        uint32_t min_count = std::numeric_limits<uint32_t>::max();
        for (int row = 0; row < kDepth; ++row) {
            const uint64_t h = Hash(key, row);
            const size_t cell = static_cast<size_t>(h % table_.size());
            const uint8_t nibble = static_cast<uint8_t>(h & 15);
            min_count = std::min<uint32_t>(min_count, CountAt(cell, nibble));
        }
        return min_count;
    }

    void ResetLocked() {
        uint32_t count = 0;
        for (auto& word : table_) {
            count += static_cast<uint32_t>(std::popcount(word & kOneMask));
            word = (word >> 1) & kResetMask;
        }
        const uint32_t half = size_ >> 1;
        const uint32_t dec = count >> 2;
        size_ = half - std::min(half, dec);
    }

    // Returns true if the counter was below 15 and got incremented.
    bool IncrementAt(size_t cell, uint8_t nibble) {
        const unsigned offset = static_cast<unsigned>(nibble) * 4u;
        const uint64_t mask = 0xFULL << offset;
        if ((table_[cell] & mask) != mask) {
            table_[cell] += (1ULL << offset);
            return true;
        }
        return false;
    }

    uint8_t CountAt(size_t cell, uint8_t nibble) const {
        const unsigned offset = static_cast<unsigned>(nibble) * 4u;
        const uint64_t mask = 0xFULL << offset;
        return static_cast<uint8_t>((table_[cell] & mask) >> offset);
    }

    // 4 independent splitmix64 finalizers (see file header). Each row uses a
    // distinct additive increment and two distinct odd multipliers, so no two
    // rows share structure. `cell` and `nibble` both come from the same 64-bit
    // output of a row.
    static uint64_t Hash(uint64_t key, int row) {
        static constexpr uint64_t kInc[kDepth] = {
            0x9E3779B97F4A7C15ULL, 0xD1B54A32D192ED03ULL, 0xCA5A826395121157ULL,
            0x2545F4914F6CDD1DULL};
        static constexpr uint64_t kMulA[kDepth] = {
            0xBF58476D1CE4E5B9ULL, 0xC2B2AE3D27D4EB4FULL, 0xFF51AFD7ED558CCDULL,
            0x9FB21C651E98DF25ULL};
        static constexpr uint64_t kMulB[kDepth] = {
            0x94D049BB133111EBULL, 0x165667B19E3779F9ULL, 0xC4CEB9FE1A85EC53ULL,
            0x9E3779B97F4A7C15ULL};
        uint64_t x = key + kInc[row];
        x = (x ^ (x >> 30)) * kMulA[row];
        x = (x ^ (x >> 27)) * kMulB[row];
        x ^= x >> 31;
        return x;
    }

    mutable std::mutex mu_;
    std::vector<uint64_t> table_;
    uint32_t size_ = 0;
    uint32_t sample_size_;
};

}  // namespace mooncake
