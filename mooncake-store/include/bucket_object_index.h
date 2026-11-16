#pragma once
#include <array>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <functional>
#include <list>
#include <string>
#include <type_traits>
#include <utility>
#include "types.h"

namespace mooncake {
namespace test {
struct BucketObjectIndexTestPeer;
}
// Externally synchronized by BucketStorageBackend::mutex_. Grow one segment
// at a time; links point directly to entries owned in insertion order.
template <typename Hash = std::hash<std::string>>
class BucketObjectIndex {
    struct Entry;
    using Entries = std::list<Entry>;
    using Position = typename Entries::iterator;
    struct Entry {
        Entry(const std::string& key, size_t hash,
              StorageObjectMetadata metadata, Position next)
            : key(key), hash(hash), metadata(std::move(metadata)), next(next) {}
        const std::string key;
        const size_t hash;
        StorageObjectMetadata metadata;
        Position next;
    };
    // Fixed power-of-two buckets target one live entry per bucket on average.
    static constexpr size_t kSegmentEntries = 1024;
    struct Segment {
        explicit Segment(Position end) { heads.fill(end); }
        std::array<Position, kSegmentEntries> heads;
    };
    static_assert(std::is_empty_v<Hash> &&
                  std::is_nothrow_default_constructible_v<Hash> &&
                  std::is_nothrow_invocable_v<Hash, const std::string&>);
    static_assert(std::is_nothrow_copy_constructible_v<Position> &&
                  std::is_nothrow_copy_assignable_v<Position>);

   public:
    BucketObjectIndex() { segments_.emplace_back(end_); }
    BucketObjectIndex(const BucketObjectIndex&) = delete;
    BucketObjectIndex& operator=(const BucketObjectIndex&) = delete;
    const StorageObjectMetadata* find(const std::string& key) const {
        auto it = Find(key, Hash{}(key));
        return it == end_ ? nullptr : &it->metadata;
    }
    bool insert(const std::string& key, StorageObjectMetadata metadata) {
        const auto hash = Hash{}(key);
        if (Find(key, hash) != end_) return false;
        if (entries_.size() / kSegmentEntries >= segments_.size()) Split();
        auto& head =
            segments_[SegmentFor(hash)].heads[hash & (kSegmentEntries - 1)];
        head = entries_.emplace(entries_.end(), key, hash, std::move(metadata),
                                head);
        return true;
    }
    void insert_or_assign(const std::string& key,
                          StorageObjectMetadata metadata) {
        auto it = Find(key, Hash{}(key));
        if (it != end_)
            it->metadata = std::move(metadata);
        else
            insert(key, std::move(metadata));
    }
    template <typename Predicate>
    bool erase_if(const std::string& key, Predicate predicate) {
        const auto hash = Hash{}(key);
        auto* link =
            &segments_[SegmentFor(hash)].heads[hash & (kSegmentEntries - 1)];
        while (*link != end_) {
            auto it = *link;
            if (it->hash == hash && it->key == key) {
                if (!predicate(std::as_const(it->metadata))) return false;
                *link = it->next;
                entries_.erase(it);
                return true;
            }
            link = &it->next;
        }
        return false;
    }
    bool erase(const std::string& key) {
        return erase_if(key, [](const auto&) { return true; });
    }
    size_t size() const { return entries_.size(); }
    void clear() {
        segments_.erase(segments_.begin() + 1, segments_.end());
        segments_.front().heads.fill(end_);
        entries_.clear();
        base_ = 1;
        split_ = 0;
    }

   private:
    friend struct test::BucketObjectIndexTestPeer;
    static size_t SegmentHash(size_t value) noexcept {
        // Keep segment routing independent of the inner bucket low bits.
        uint64_t hash = value;
        hash ^= hash >> 33;
        hash *= 0xff51afd7ed558ccdULL;
        hash ^= hash >> 33;
        hash *= 0xc4ceb9fe1a85ec53ULL;
        return hash ^ (hash >> 33);
    }
    size_t SegmentFor(size_t hash) const {
        hash = SegmentHash(hash);
        auto segment = hash & (base_ - 1);
        return segment < split_ ? hash & (2 * base_ - 1) : segment;
    }
    Position Find(const std::string& key, size_t hash) const {
        auto it =
            segments_[SegmentFor(hash)].heads[hash & (kSegmentEntries - 1)];
        while (it != end_ && (it->hash != hash || it->key != key))
            it = it->next;
        return it;
    }
    void Split() {
        auto& source = segments_[split_];
        // Allocate before rewiring links; entry addresses do not change.
        segments_.emplace_back(end_);
        auto& target = segments_.back();
        for (size_t bucket = 0; bucket < kSegmentEntries; ++bucket) {
            auto* link = &source.heads[bucket];
            while (*link != end_) {
                auto it = *link;
                if (SegmentHash(it->hash) & base_) {
                    *link = it->next;
                    it->next = target.heads[bucket];
                    target.heads[bucket] = it;
                } else
                    link = &it->next;
            }
        }
        if (++split_ == base_) {
            base_ *= 2;
            split_ = 0;
        }
    }
    Entries entries_;
    Position end_ = entries_.end();
    std::deque<Segment> segments_;
    size_t base_ = 1;
    size_t split_ = 0;
};
}  // namespace mooncake
