#pragma once

#include <glog/logging.h>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <list>
#include <mutex>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "utils.h"  // StringHash

namespace mooncake {

/**
 * @enum HeatBand
 * @brief Frequency-derived heat band for a key.
 */
enum class HeatBand : uint8_t {
    kCold = 0,
    kWarm = 1,
    kHot = 2,
    kVeryHot = 3,
};

inline constexpr int kNumHeatBands = 4;

/**
 * @struct BandThresholds
 * @brief Frequency cutoffs that map a key's access frequency to a HeatBand.
 *
 * Bands are half-open frequency ranges:
 *   cold     [0, warm)
 *   warm     [warm, hot)
 *   hot      [hot, very_hot)
 *   very-hot [very_hot, inf)
 *
 * Operator-tunable via scheduler config; defaults to warm=3, hot=8,
 * very_hot=15. Must be strictly increasing with warm >= 1 so all four bands are
 * reachable (warm < 1 would leave the cold band empty). Run values through
 * ValidateBandThresholds before use.
 */
struct BandThresholds {
    uint64_t warm = 3;       // freq >= warm  -> at least warm band
    uint64_t hot = 8;        // freq >= hot   -> at least hot band
    uint64_t very_hot = 15;  // freq >= very_hot -> very-hot band
};

inline HeatBand BandOf(uint64_t freq, const BandThresholds& t) {
    if (freq >= t.very_hot) return HeatBand::kVeryHot;
    if (freq >= t.hot) return HeatBand::kHot;
    if (freq >= t.warm) return HeatBand::kWarm;
    return HeatBand::kCold;
}

/**
 * @brief Repair an out-of-range BandThresholds in place, warning on each fix.
 *
 * Enforces warm >= 1 < hot < very_hot by clamping upward (rather than silently
 * mis-banding keys): warm is raised to 1, then hot/very_hot are pushed to at
 * least one above their predecessor. Mirrors the watermark-validation style in
 * the scheduler factory.
 */
inline void ValidateBandThresholds(BandThresholds& t) {
    if (t.warm < 1) {
        LOG(WARNING) << "band_warm_threshold (" << t.warm
                     << ") must be >= 1; clamping to 1";
        t.warm = 1;
    }
    if (t.hot <= t.warm) {
        LOG(WARNING) << "band_hot_threshold (" << t.hot
                     << ") must be > band_warm_threshold (" << t.warm
                     << "); clamping to " << (t.warm + 1);
        t.hot = t.warm + 1;
    }
    if (t.very_hot <= t.hot) {
        LOG(WARNING) << "band_veryhot_threshold (" << t.very_hot
                     << ") must be > band_hot_threshold (" << t.hot
                     << "); clamping to " << (t.hot + 1);
        t.very_hot = t.hot + 1;
    }
}

/**
 * @struct MultiLRUEntry
 * @brief A snapshot row returned by MultiLRU enumeration helpers.
 */
struct MultiLRUEntry {
    std::string key;
    size_t size_bytes = 0;
    HeatBand band = HeatBand::kCold;
};

/**
 * @class MultiLRU
 * @brief Four banded LRU lists (cold/warm/hot/very-hot) over the set of keys
 *        resident in the fast tier.
 *
 * Each band is a doubly-linked recency list (front = MRU, back = LRU). A key's
 * band is its frequency band; re-classification on access moves it between
 * bands while preserving recency within a band. Thread-safe — every public
 * method locks an internal mutex.
 */
class MultiLRU {
   public:
    // Banding thresholds default to BandThresholds{} (3/8/15); pass operator-
    // configured (and validated) thresholds to override.
    explicit MultiLRU(BandThresholds thresholds = {})
        : thresholds_(thresholds) {}

    // Insert (or refresh) a key at the MRU of the band derived from `freq`. If
    // the key is already present, its size is updated and it is moved to that
    // band's MRU position. Takes a raw frequency and computes the band itself,
    // mirroring Touch() (callers no longer hand-roll BandOf).
    void Insert(std::string_view key, size_t size_bytes, uint64_t freq) {
        std::lock_guard<std::mutex> lock(mu_);
        const HeatBand band = BandOf(freq, thresholds_);
        auto it = index_.find(key);
        if (it != index_.end()) {
            it->second->size_bytes = size_bytes;
            MoveToBandFront(it->second, band);
            return;
        }
        auto& list = lists_[static_cast<int>(band)];
        list.push_front(Node{std::string(key), size_bytes, band});
        index_.emplace(list.front().key, list.begin());
    }

    // Recompute the key's band from `freq` and move it to that band's MRU.
    // No-op if the key is not resident. Returns true if the key was present.
    bool Touch(std::string_view key, uint64_t freq) {
        std::lock_guard<std::mutex> lock(mu_);
        auto it = index_.find(key);
        if (it == index_.end()) {
            return false;
        }
        MoveToBandFront(it->second, BandOf(freq, thresholds_));
        return true;
    }

    // Remove a key from whatever band holds it. Returns true if removed.
    bool Remove(std::string_view key) {
        std::lock_guard<std::mutex> lock(mu_);
        auto it = index_.find(key);
        if (it == index_.end()) {
            return false;
        }
        auto node_it = it->second;
        index_.erase(it);
        lists_[static_cast<int>(node_it->band)].erase(node_it);
        return true;
    }

    bool Contains(std::string_view key) const {
        std::lock_guard<std::mutex> lock(mu_);
        return index_.find(key) != index_.end();
    }

    size_t Size() const {
        std::lock_guard<std::mutex> lock(mu_);
        return index_.size();
    }

    // Hottest-first: very-hot MRU->LRU, then hot MRU->LRU, ... until `n` rows
    // are collected. Only as many as exist are returned.
    std::vector<MultiLRUEntry> CollectHot(size_t n) const {
        std::lock_guard<std::mutex> lock(mu_);
        std::vector<MultiLRUEntry> out;
        if (n == 0) return out;
        out.reserve(std::min(n, index_.size()));
        for (int band = kNumHeatBands - 1; band >= 0; --band) {
            for (const auto& node : lists_[band]) {  // front(MRU) -> back(LRU)
                if (out.size() >= n) return out;
                out.push_back(
                    MultiLRUEntry{node.key, node.size_bytes, node.band});
            }
        }
        return out;
    }

    // Coldest-first eviction candidates: cold band LRU->MRU, then warm, ...,
    // up to `max_n`. Within a band, the LRU (back) comes first.
    std::vector<MultiLRUEntry> CollectColdFirst(size_t max_n) const {
        std::lock_guard<std::mutex> lock(mu_);
        std::vector<MultiLRUEntry> out;
        if (max_n == 0) return out;
        out.reserve(std::min(max_n, index_.size()));
        for (int band = 0; band < kNumHeatBands; ++band) {
            const auto& list = lists_[band];
            for (auto rit = list.rbegin(); rit != list.rend(); ++rit) {
                if (out.size() >= max_n) return out;
                out.push_back(
                    MultiLRUEntry{rit->key, rit->size_bytes, rit->band});
            }
        }
        return out;
    }

   private:
    struct Node {
        std::string key;
        size_t size_bytes = 0;
        HeatBand band = HeatBand::kCold;
    };
    using ListIter = std::list<Node>::iterator;

    // Splice `node_it` to the front (MRU) of `target_band`'s list, updating its
    // band tag. std::list::splice preserves the iterator, so `index_` stays
    // valid without re-insertion.
    void MoveToBandFront(ListIter node_it, HeatBand target_band) {
        auto& dst = lists_[static_cast<int>(target_band)];
        auto& src = lists_[static_cast<int>(node_it->band)];
        node_it->band = target_band;
        dst.splice(dst.begin(), src, node_it);
    }

    const BandThresholds thresholds_;  // frequency -> band cutoffs (immutable)
    mutable std::mutex mu_;
    // lists_[band]: front = MRU, back = LRU.
    std::list<Node> lists_[kNumHeatBands];
    std::unordered_map<std::string, ListIter, StringHash, std::equal_to<>>
        index_;
};

}  // namespace mooncake
