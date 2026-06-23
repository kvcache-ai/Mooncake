#pragma once

#include <atomic>
#include <cstdint>
#include <functional>
#include <mutex>
#include <string>
#include <vector>

namespace mooncake {

// A lock-free Count-Min Sketch for tracking key access frequency.
// increment() and count() are completely lock-free using atomic operations.
// decay() uses a mutex for the table sweep but readers are never blocked.
//
// Key optimizations:
// 1. Atomic counters: increment() and count() are lock-free, no mutex acquired
// 2. Flat memory layout: single contiguous vector for cache locality
// 3. Pre-computed row strides: no multiplication in hot path
// 4. Single hash + mixing: avoids depth separate std::hash calls
class CountMinSketch {
   public:
    explicit CountMinSketch(size_t width = 4096, size_t depth = 4)
        : width_(width > 0 ? width : kDefaultWidth),
          depth_(depth > 0 ? depth : kDefaultDepth),
          table_(width_ * depth_),
          total_increments_(0) {
        row_strides_.reserve(depth_);
        for (size_t i = 0; i < depth_; ++i) {
            row_strides_.push_back(i * width_);
        }
    }

    // Lock-free increment. Uses atomic fetch_add with saturation at UINT8_MAX.
    uint8_t increment(const std::string &key) {
        uint8_t min_val = UINT8_MAX;
        size_t base = std::hash<std::string>{}(key);
        for (size_t i = 0; i < depth_; ++i) {
            size_t idx = hashFromBase(base, i) % width_;
            auto &cell = table_[row_strides_[i] + idx];
            uint8_t old_val = cell.load(std::memory_order_relaxed);
            // Saturating increment: only CAS if below max
            while (old_val < UINT8_MAX) {
                if (cell.compare_exchange_weak(old_val, old_val + 1,
                                               std::memory_order_release,
                                               std::memory_order_relaxed)) {
                    old_val++;  // we successfully incremented
                    break;
                }
                // CAS failed: old_val is reloaded, retry
            }
            if (old_val < min_val) min_val = old_val;
        }
        // Decay check: use relaxed counter
        size_t prev = total_increments_.fetch_add(1, std::memory_order_relaxed);
        if (prev + 1 >= width_ * depth_) {
            decay();
        }
        return min_val;
    }

    // Lock-free read. Uses atomic loads with relaxed ordering (Count-Min Sketch
    // is inherently approximate, so strict ordering is unnecessary).
    uint8_t count(const std::string &key) const {
        uint8_t min_val = UINT8_MAX;
        size_t base = std::hash<std::string>{}(key);
        for (size_t i = 0; i < depth_; ++i) {
            size_t idx = hashFromBase(base, i) % width_;
            uint8_t val = table_[row_strides_[i] + idx].load(
                std::memory_order_relaxed);
            if (val < min_val) min_val = val;
        }
        return min_val;
    }

    // Halve all counters. Protected by a mutex since this is a bulk operation
    // that requires consistency across all cells.
    void decay() {
        std::lock_guard<std::mutex> lock(decay_mu_);
        // Only one thread performs decay; others that reached the threshold
        // find total_increments_ already reset and skip.
        if (total_increments_.load(std::memory_order_relaxed) <
            width_ * depth_) {
            return;
        }
        for (auto &cell : table_) {
            cell.store(cell.load(std::memory_order_relaxed) >> 1,
                       std::memory_order_relaxed);
        }
        total_increments_.store(0, std::memory_order_relaxed);
    }

   private:
    static constexpr size_t kDefaultWidth = 4096;
    static constexpr size_t kDefaultDepth = 4;

    size_t hashFromBase(size_t base, size_t seed) const {
        size_t h = base;
        h ^= seed * 0x9e3779b97f4a7c15ULL + 0x517cc1b727220a95ULL;
        h ^= (h >> 33);
        h *= 0xff51afd7ed558ccdULL;
        h ^= (h >> 33);
        return h;
    }

    const size_t width_;
    const size_t depth_;
    std::vector<std::atomic<uint8_t>> table_;  // lock-free flat table
    std::vector<size_t> row_strides_;          // precomputed offsets
    std::atomic<size_t> total_increments_;      // atomic for lock-free check
    std::mutex decay_mu_;                       // protects decay() only
};

}  // namespace mooncake
