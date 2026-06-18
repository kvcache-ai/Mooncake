#pragma once

#include <cstddef>
#include <cstdint>
#include <list>
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

// Band thresholds: 0-1 cold, 2-3 warm, 4-7 hot, 8-15 very-hot.
inline constexpr uint64_t kWarmThreshold = 2;
inline constexpr uint64_t kHotThreshold = 4;
inline constexpr uint64_t kVeryHotThreshold = 8;

inline HeatBand BandOf(uint64_t freq) {
    if (freq >= kVeryHotThreshold) return HeatBand::kVeryHot;
    if (freq >= kHotThreshold) return HeatBand::kHot;
    if (freq >= kWarmThreshold) return HeatBand::kWarm;
    return HeatBand::kCold;
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
 * bands while preserving recency within a band. NOT thread-safe; the owning
 * collector serializes access with its own mutex.
 */
class MultiLRU {
   public:
    // Insert (or refresh) a key into `band` at MRU. If the key is already
    // present, its size is updated and it is moved to `band`'s MRU position.
    void Insert(std::string_view key, size_t size_bytes, HeatBand band) {
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
        auto it = index_.find(key);
        if (it == index_.end()) {
            return false;
        }
        MoveToBandFront(it->second, BandOf(freq));
        return true;
    }

    // Remove a key from whatever band holds it. Returns true if removed.
    bool Remove(std::string_view key) {
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
        return index_.find(key) != index_.end();
    }

    size_t Size() const { return index_.size(); }

    // Hottest-first: very-hot MRU->LRU, then hot MRU->LRU, ... until `n` rows
    // are collected. Only as many as exist are returned.
    std::vector<MultiLRUEntry> CollectHot(size_t n) const {
        std::vector<MultiLRUEntry> out;
        if (n == 0) return out;
        out.reserve(std::min(n, index_.size()));
        for (int band = kNumHeatBands - 1; band >= 0; --band) {
            for (const auto& node : lists_[band]) {  // front(MRU) -> back(LRU)
                if (out.size() >= n) return out;
                out.push_back(MultiLRUEntry{node.key, node.size_bytes,
                                            node.band});
            }
        }
        return out;
    }

    // Coldest-first eviction candidates: cold band LRU->MRU, then warm, ...,
    // up to `max_n`. Within a band, the LRU (back) comes first.
    std::vector<MultiLRUEntry> CollectColdFirst(size_t max_n) const {
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

    // lists_[band]: front = MRU, back = LRU.
    std::list<Node> lists_[kNumHeatBands];
    std::unordered_map<std::string, ListIter, StringHash, std::equal_to<>>
        index_;
};

}  // namespace mooncake
