#pragma once

#include <cstdint>
#include <functional>
#include <mutex>
#include <string>
#include <vector>

namespace mooncake {

// A simple Count-Min Sketch for tracking key access frequency.
// Used by the frequency admission policy to decide whether a key
// should be promoted into the local hot cache.
class CountMinSketch {
   public:
    explicit CountMinSketch(size_t width = 4096, size_t depth = 4)
        : width_(width > 0 ? width : kDefaultWidth),
          depth_(depth > 0 ? depth : kDefaultDepth),
          table_(depth_, std::vector<uint8_t>(width_, 0)),
          total_increments_(0) {}

    // Increment the count for |key| and return the estimated min-count.
    // Automatically triggers decay when total_increments exceeds the
    // threshold (width * depth) to prevent counters from saturating.
    uint8_t increment(const std::string &key) {
        std::lock_guard<std::mutex> lock(mu_);
        uint8_t min_val = UINT8_MAX;
        for (size_t i = 0; i < depth_; ++i) {
            size_t idx = hash(key, i) % width_;
            if (table_[i][idx] < UINT8_MAX) {
                ++table_[i][idx];
            }
            min_val = std::min(min_val, table_[i][idx]);
        }
        if (++total_increments_ >= width_ * depth_) {
            decayLocked();
        }
        return min_val;
    }

    // Return the estimated count for |key| (read-only).
    uint8_t count(const std::string &key) const {
        std::lock_guard<std::mutex> lock(mu_);
        uint8_t min_val = UINT8_MAX;
        for (size_t i = 0; i < depth_; ++i) {
            size_t idx = hash(key, i) % width_;
            min_val = std::min(min_val, table_[i][idx]);
        }
        return min_val;
    }

    // Halve all counters (right-shift by 1). Useful for periodic aging.
    void decay() {
        std::lock_guard<std::mutex> lock(mu_);
        decayLocked();
    }

   private:
    static constexpr size_t kDefaultWidth = 4096;
    static constexpr size_t kDefaultDepth = 4;

    size_t hash(const std::string &key, size_t seed) const {
        // Combine std::hash with a per-row seed to get independent hashes.
        size_t h = std::hash<std::string>{}(key);
        h ^= seed * 0x9e3779b97f4a7c15ULL + 0x517cc1b727220a95ULL;
        h ^= (h >> 33);
        h *= 0xff51afd7ed558ccdULL;
        h ^= (h >> 33);
        return h;
    }

    void decayLocked() {
        for (size_t i = 0; i < depth_; ++i) {
            for (size_t j = 0; j < width_; ++j) {
                table_[i][j] >>= 1;
            }
        }
        total_increments_ = 0;
    }

    const size_t width_;
    const size_t depth_;
    std::vector<std::vector<uint8_t>> table_;
    size_t total_increments_;
    mutable std::mutex mu_;
};

}  // namespace mooncake
