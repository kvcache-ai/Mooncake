#pragma once

#include <array>
#include <atomic>
#include <cstdint>
#include <string>
#include <vector>

namespace mooncake {

/**
 * @brief Lock-free Bloom filter for fast negative lookups.
 *
 * Used as a fast-path check before acquiring shard locks in ExistKey/Query.
 * False positives are acceptable (fall through to normal path), but false
 * negatives must never occur (a key that exists must always test positive).
 *
 * Thread safety: All operations are lock-free using atomic bit operations.
 * Add() and MayContain() can be called concurrently from any thread.
 * Remove is not supported (use counting bloom filter if needed).
 *
 * When the filter becomes too full (high false positive rate), call Reset()
 * and re-add all existing keys. This is expected to be rare since keys are
 * evicted regularly in KVCache workloads.
 */
class BloomFilter {
   public:
    /**
     * @param num_bits Total bits in the filter. Larger = lower false positive
     *        rate. Default 1M bits (~125KB memory) supports ~70K keys at 1% FPR.
     * @param num_hashes Number of hash functions. Optimal is (m/n)*ln(2).
     */
    explicit BloomFilter(size_t num_bits = 1 << 20, size_t num_hashes = 7)
        : num_bits_(num_bits),
          num_hashes_(num_hashes),
          bits_((num_bits + 63) / 64) {
        // Zero-initialize all atomic words
        for (auto& word : bits_) {
            word.store(0, std::memory_order_relaxed);
        }
    }

    /// Add a key to the filter. Lock-free, safe to call concurrently.
    void Add(const std::string& key) {
        size_t h1 = std::hash<std::string>{}(key);
        size_t h2 = fnv1a(key);
        for (size_t i = 0; i < num_hashes_; ++i) {
            size_t bit = (h1 + i * h2) % num_bits_;
            size_t word_idx = bit / 64;
            size_t bit_idx = bit % 64;
            bits_[word_idx].fetch_or(uint64_t(1) << bit_idx,
                                     std::memory_order_relaxed);
        }
    }

    /// Check if a key might be in the filter.
    /// Returns false  → key is DEFINITELY not present (skip shard lock).
    /// Returns true   → key MIGHT be present (proceed to normal lookup).
    bool MayContain(const std::string& key) const {
        // Compute both base hashes once, then derive k hashes via
        // h(i) = h1 + i*h2 (Kirsch-Mitzenmacher optimization).
        // This reduces per-key cost from O(k * |key|) to O(2 * |key|).
        size_t h1 = std::hash<std::string>{}(key);
        size_t h2 = fnv1a(key);
        for (size_t i = 0; i < num_hashes_; ++i) {
            size_t bit = (h1 + i * h2) % num_bits_;
            size_t word_idx = bit / 64;
            size_t bit_idx = bit % 64;
            if (!(bits_[word_idx].load(std::memory_order_relaxed) &
                  (uint64_t(1) << bit_idx))) {
                return false;
            }
        }
        return true;
    }

    /// Reset the filter. NOT thread-safe — caller must ensure exclusive access.
    void Reset() {
        for (auto& word : bits_) {
            word.store(0, std::memory_order_relaxed);
        }
    }

   private:
    const size_t num_bits_;
    const size_t num_hashes_;
    std::vector<std::atomic<uint64_t>> bits_;

    /// Double hashing: h(i, key) = h1(key) + i * h2(key)
    /// Using two independent hash functions to generate k hash values.
    size_t nthHash(size_t n, const std::string& key) const {
        // Use std::hash for h1, and a FNV-1a variant for h2
        size_t h1 = std::hash<std::string>{}(key);
        size_t h2 = fnv1a(key);
        return h1 + n * h2;
    }

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
