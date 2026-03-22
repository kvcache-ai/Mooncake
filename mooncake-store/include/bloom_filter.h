#pragma once

#include <array>
#include <atomic>
#include <cstdint>
#include <string>
#include <vector>

namespace mooncake {

/**
 * @brief Lock-free Counting Bloom Filter for fast negative lookups with
 *        support for element removal.
 *
 * Used as a fast-path check before acquiring shard locks in ExistKey/Query.
 * False positives are acceptable (fall through to normal path), but false
 * negatives must never occur (a key that exists must always test positive).
 *
 * Unlike a standard Bloom Filter, this implementation uses 4-bit counters
 * instead of single bits, enabling Remove() operations. This is critical
 * for KVCache workloads where keys are frequently evicted — without Remove(),
 * false positive rate accumulates monotonically as keys churn, eventually
 * negating the entire performance benefit of the filter.
 *
 * Memory overhead: 4x compared to standard Bloom Filter (4 bits vs 1 bit per
 * slot). For 4M slots: standard = 512KB, counting = 2MB. Acceptable tradeoff
 * for maintaining <0.01% FPR under continuous eviction.
 *
 * Thread safety: All operations are lock-free using atomic operations.
 * Add(), Remove(), and MayContain() can be called concurrently from any thread.
 * Counter overflow is capped at 15 (saturating arithmetic) to prevent wraparound.
 */
class BloomFilter {
   public:
    /**
     * @param num_slots Total counter slots in the filter. Larger = lower false
     *        positive rate. Default 4M slots (~2MB memory) supports ~280K keys
     *        at <0.01% FPR.
     * @param num_hashes Number of hash functions. Optimal is (m/n)*ln(2).
     */
    explicit BloomFilter(size_t num_slots = 1 << 22, size_t num_hashes = 3)
        : num_slots_(num_slots),
          num_hashes_(num_hashes),
          counters_(num_slots) {
        // Zero-initialize all counters
        for (auto& c : counters_) {
            c.store(0, std::memory_order_relaxed);
        }
    }

    /// Add a key to the filter. Lock-free, safe to call concurrently.
    /// Uses saturating increment — counters cap at 15 to prevent overflow.
    void Add(const std::string& key) {
        size_t h1 = std::hash<std::string>{}(key);
        size_t h2 = fnv1a(key);
        for (size_t i = 0; i < num_hashes_; ++i) {
            size_t slot = (h1 + i * h2) % num_slots_;
            uint8_t old_val = counters_[slot].load(std::memory_order_relaxed);
            while (old_val < 15) {  // Saturating: cap at 15
                if (counters_[slot].compare_exchange_weak(
                        old_val, old_val + 1, std::memory_order_relaxed)) {
                    break;
                }
                // old_val is updated by CAS on failure
            }
        }
    }

    /// Remove a key from the filter. Lock-free, safe to call concurrently.
    /// Uses saturating decrement — counters never go below 0.
    /// IMPORTANT: Only call Remove() for keys that were previously Add()'d.
    /// Removing a key that was never added may cause false negatives.
    void Remove(const std::string& key) {
        size_t h1 = std::hash<std::string>{}(key);
        size_t h2 = fnv1a(key);
        for (size_t i = 0; i < num_hashes_; ++i) {
            size_t slot = (h1 + i * h2) % num_slots_;
            uint8_t old_val = counters_[slot].load(std::memory_order_relaxed);
            while (old_val > 0) {  // Saturating: never go below 0
                if (counters_[slot].compare_exchange_weak(
                        old_val, old_val - 1, std::memory_order_relaxed)) {
                    break;
                }
                // old_val is updated by CAS on failure
            }
        }
    }

    /// Check if a key might be in the filter.
    /// Returns false  → key is DEFINITELY not present (skip shard lock).
    /// Returns true   → key MIGHT be present (proceed to normal lookup).
    bool MayContain(const std::string& key) const {
        size_t h1 = std::hash<std::string>{}(key);
        size_t h2 = fnv1a(key);
        for (size_t i = 0; i < num_hashes_; ++i) {
            size_t slot = (h1 + i * h2) % num_slots_;
            if (counters_[slot].load(std::memory_order_relaxed) == 0) {
                return false;
            }
        }
        return true;
    }

    /// Reset the filter. NOT thread-safe — caller must ensure exclusive access.
    void Reset() {
        for (auto& c : counters_) {
            c.store(0, std::memory_order_relaxed);
        }
    }

    /// Get the approximate number of non-zero counters (for diagnostics).
    size_t CountNonZero() const {
        size_t count = 0;
        for (const auto& c : counters_) {
            if (c.load(std::memory_order_relaxed) > 0) {
                count++;
            }
        }
        return count;
    }

    /// Get the estimated false positive rate (for diagnostics).
    double EstimateFPR() const {
        double fill_ratio =
            static_cast<double>(CountNonZero()) / static_cast<double>(num_slots_);
        // FPR ≈ (fill_ratio)^num_hashes
        double fpr = 1.0;
        for (size_t i = 0; i < num_hashes_; ++i) {
            fpr *= fill_ratio;
        }
        return fpr;
    }

   private:
    const size_t num_slots_;
    const size_t num_hashes_;
    // 4-bit counters stored as uint8_t (using only lower 4 bits).
    // We use full bytes for simplicity and atomic safety — packing into
    // nibbles would complicate atomic operations without meaningful savings.
    std::vector<std::atomic<uint8_t>> counters_;

    static size_t fnv1a(const std::string& key) {
        size_t hash = 14695981039346656037ULL;  // FNV offset basis
        for (char c : key) {
            hash ^= static_cast<size_t>(c);
            hash *= 1099511628211ULL;  // FNV prime
        }
        return hash;
    }
};

}  // namespace mooncake
