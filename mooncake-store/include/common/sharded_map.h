#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <type_traits>
#include <unordered_map>
#include <utility>

#include <boost/version.hpp>
#if BOOST_VERSION >= 108200
#include <boost/unordered/unordered_node_map.hpp>
#endif

namespace mooncake {

template <typename Key, typename Value>
using ShardMap =
#if BOOST_VERSION >= 108200
    boost::unordered_node_map<Key, Value>;
#else
    std::unordered_map<Key, Value>;
#endif

// Externally synchronized. Growth rehashes one sub-map, not the entire index.
// Pointers returned by find() must not survive a mutation of this container.
template <typename Key, typename Value, size_t ShardCount = 4096,
          typename Map = ShardMap<Key, Value>>
class ShardedMap {
    static_assert(ShardCount && !(ShardCount & (ShardCount - 1)));
    static_assert(std::is_empty_v<typename Map::hasher>);

   public:
    ShardedMap() = default;
    ShardedMap(const ShardedMap&) = delete;
    ShardedMap& operator=(const ShardedMap&) = delete;

    const Value* find(const Key& key) const {
        const auto& shard = shards_[ShardFor(key)];
        auto it = shard.find(key);
        return it == shard.end() ? nullptr : &it->second;
    }
    bool insert(const Key& key, Value value) {
        const bool inserted =
            shards_[ShardFor(key)].try_emplace(key, std::move(value)).second;
        size_ += inserted;
        return inserted;
    }
    void insert_or_assign(const Key& key, Value value) {
        size_ += shards_[ShardFor(key)]
                     .insert_or_assign(key, std::move(value))
                     .second;
    }
    template <typename Predicate>
    bool erase_if(const Key& key, Predicate predicate) {
        auto& shard = shards_[ShardFor(key)];
        auto it = shard.find(key);
        if (it == shard.end() || !predicate(std::as_const(it->second)))
            return false;
        shard.erase(it);
        --size_;
        return true;
    }
    bool erase(const Key& key) {
        return erase_if(key, [](const auto&) { return true; });
    }
    size_t size() const { return size_; }
    void clear() {
        for (auto& shard : shards_) shard.clear();
        size_ = 0;
    }

   private:
    static size_t ShardFor(const Key& key) {
        // Mix routing separately: sub-map bucket selection may use low bits.
        uint64_t hash = typename Map::hasher{}(key);
        hash ^= hash >> 33;
        hash *= 0xff51afd7ed558ccdULL;
        hash ^= hash >> 33;
        hash *= 0xc4ceb9fe1a85ec53ULL;
        hash ^= hash >> 33;
        return hash & (ShardCount - 1);
    }
    std::array<Map, ShardCount> shards_;
    size_t size_ = 0;
};

}  // namespace mooncake
