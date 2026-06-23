#pragma once

#include <atomic>
#include <cstdint>
#include <functional>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <vector>

namespace mooncake {

// A high-performance Count-Min Sketch for tracking key access frequency.
// Used by the frequency admission policy to decide whether a key
// should be promoted into the local hot cache.
//
// Optimizations over the original implementation:
// 1. Flat memory layout (single contiguous vector) for better cache locality
// 2. SharedMutex allows concurrent readers (count()) without blocking each other
// 3. Pre-computed row strides avoid repeated multiplications in the hot path
// 4. Batch hash: single base hash with mixing avoids depth separate std::hash calls
class CountMinSketch {
   public:
    explicit CountMinSketch(size_t width = 4096, size_t depth = 4)
        : width_(width > 0 ? width : kDefaultWidth),
          depth_(depth > 0 ? depth : kDefaultDepth),
          table_(width_ * depth_, 0),
          total_increments_(0) {
        // Pre-compute row strides for flat indexing: table_[row * width_ + col]
        row_strides_.reserve(depth_);
        for (size_t i = 0; i < depth_; ++i) {
            row_strides_.push_back(i * width_);
        }
    }

    // Increment the count for |key| and return the estimated min-count.
    // Automatically triggers decay when total_increments exceeds the
    // threshold (width * depth) to prevent counters from saturating.
    uint8_t increment(const std::string &key) {
        std::unique_lock lock(mu_);
        uint8_t min_val = UINT8_MAX;
        // Compute base hash once, derive per-row indices via mixing
        size_t base = std::hash<std::string>{}(key);
        for (size_t i = 0; i < depth_; ++i) {
            size_t idx = hashFromBase(base, i) % width_;
            uint8_t &cell = table_[row_strides_[i] + idx];
            if (cell < UINT8_MAX) {
                ++cell;
            }
            min_val = std::min(min_val, cell);
        }
        if (++total_increments_ >= width_ * depth_) {
            decayLocked();
        }
        return min_val;
    }

    // Return the estimated count for |key| (read-only, concurrent).
    uint8_t count(const std::string &key) const {
        std::shared_lock lock(mu_);
        uint8_t min_val = UINT8_MAX;
        size_t base = std::hash<std::string>{}(key);
        for (size_t i = 0; i < depth_; ++i) {
            size_t idx = hashFromBase(base, i) % width_;
            min_val = std::min(min_val, table_[row_strides_[i] + idx]);
        }
        return min_val;
    }

    // Halve all counters (right-shift by 1). Useful for periodic aging.
    void decay() {
        std::unique_lock lock(mu_);
        decayLocked();
    }

   private:
    static constexpr size_t kDefaultWidth = 4096;
    static constexpr size_t kDefaultDepth = 4;

    // Derive an independent hash for row |seed| from a precomputed base hash.
    // Uses the same mixing constants as the original but avoids re-hashing the key.
    size_t hashFromBase(size_t base, size_t seed) const {
        size_t h = base;
        h ^= seed * 0x9e3779b97f4a7c15ULL + 0x517cc1b727220a95ULL;
        h ^= (h >> 33);
        h *= 0xff51afd7ed558ccdULL;
        h ^= (h >> 33);
        return h;
    }

    void decayLocked() {
        for (auto &cell : table_) {
            cell >>= 1;
        }
        total_increments_ = 0;
    }

    const size_t width_;
    const size_t depth_;
    std::vector<uint8_t> table_;          // flat layout: depth_ rows of width_ cols
    std::vector<size_t> row_strides_;     // precomputed: [0, width_, 2*width_, ...]
    size_t total_increments_;
    mutable std::shared_mutex mu_;        // allows concurrent readers
};

}  // namespace mooncake
