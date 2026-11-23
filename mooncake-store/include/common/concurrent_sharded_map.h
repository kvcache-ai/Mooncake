#pragma once

// High-concurrency shard-locked map keyed by (TenantId, key) with an embedded
// lock-free negative presence filter per shard.
//
// Unlike `ShardedMap` (common/sharded_map.h), which is externally synchronized
// and only splits rehash stalls, `ConcurrentShardedMap` provides internal
// per-shard locking, transparent zero-allocation `string_view` lookups,
// in-place shard-locked updates, and lock-free negative filtering so
// non-participating online I/O paths reject misses with two read-only acquire
// loads (~3ns, zero mutex acquisitions, zero atomic RMWs, zero heap
// allocations).

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/shrink_buckets.h"
#include "tenant_id.h"

namespace mooncake {

struct ScopedObjectKey {
    TenantId tenant_id;
    std::string key;

    friend bool operator==(const ScopedObjectKey&,
                           const ScopedObjectKey&) = default;
};

struct ScopedObjectKeyView {
    const TenantId* tenant_id;
    std::string_view key;
};

struct ScopedObjectKeyHash {
    using is_transparent = void;

    size_t operator()(const ScopedObjectKey& k) const noexcept {
        return HashParts(k.tenant_id, k.key);
    }
    size_t operator()(const ScopedObjectKeyView& k) const noexcept {
        return HashParts(*k.tenant_id, k.key);
    }

    static size_t HashParts(const TenantId& tenant_id,
                            std::string_view key) noexcept {
        return TenantIdHash{}(tenant_id) ^
               (std::hash<std::string_view>{}(key) * 1099511628211ULL);
    }
};

struct ScopedObjectKeyEq {
    using is_transparent = void;

    bool operator()(const ScopedObjectKey& a,
                    const ScopedObjectKey& b) const noexcept {
        return a.tenant_id == b.tenant_id && a.key == b.key;
    }
    bool operator()(const ScopedObjectKey& a,
                    const ScopedObjectKeyView& b) const noexcept {
        return a.tenant_id == *b.tenant_id && a.key == b.key;
    }
    bool operator()(const ScopedObjectKeyView& a,
                    const ScopedObjectKey& b) const noexcept {
        return *a.tenant_id == b.tenant_id && a.key == b.key;
    }
};

// Lock-free negative presence filter embedded inside each sharded stripe.
//
// Thread-safety contract:
//   - InsertLocked / EraseLocked / ClearLocked are single-writer under the
//     shard mutex (no atomic RMW instructions needed).
//   - MayContain(mixed_hash) is lock-free and safe without any lock.
//   - Zero false negatives: InsertLocked publishes bits with release ordering;
//     MayContain reads with acquire ordering.
//   - Counter saturation (at 255) is sticky until ClearLocked (invoked when
//     the shard drains empty or on Clear()), preventing underflow.
class ShardPresenceFilter {
   public:
    // 1024 bits per shard (1024 shards = 1M bits total, ~1 MB footprint):
    // keeps false-positive rate <1% at 50k active tasks and ~10% at 200k.
    static constexpr size_t kNumBits = 1024;
    static constexpr size_t kNumWords = kNumBits / 64;

    static uint64_t MixHash(uint64_t h) noexcept {
        h ^= h >> 33;
        h *= 0xff51afd7ed558ccdULL;
        h ^= h >> 33;
        h *= 0xc4ceb9fe1a85ec53ULL;
        h ^= h >> 33;
        return h;
    }

    static uint64_t MixedHash(const TenantId& tenant_id,
                              std::string_view key) noexcept {
        return MixHash(ScopedObjectKeyHash::HashParts(tenant_id, key));
    }

    bool MayContain(uint64_t mixed_hash) const noexcept {
        const size_t b1 = (mixed_hash >> 16) & (kNumBits - 1);
        const uint64_t w1 = words_[b1 >> 6].load(std::memory_order_acquire);
        if ((w1 & (uint64_t{1} << (b1 & 63))) == 0) {
            return false;
        }
        const size_t b2 = (mixed_hash >> 26) & (kNumBits - 1);
        const uint64_t w2 = words_[b2 >> 6].load(std::memory_order_acquire);
        return (w2 & (uint64_t{1} << (b2 & 63))) != 0;
    }

    void InsertLocked(uint64_t mixed_hash) noexcept {
        SetBitLocked((mixed_hash >> 16) & (kNumBits - 1));
        SetBitLocked((mixed_hash >> 26) & (kNumBits - 1));
    }

    void EraseLocked(uint64_t mixed_hash, bool shard_now_empty) noexcept {
        if (shard_now_empty) {
            ClearLocked();
            return;
        }
        ClearBitLocked((mixed_hash >> 16) & (kNumBits - 1));
        ClearBitLocked((mixed_hash >> 26) & (kNumBits - 1));
    }

    void ClearLocked() noexcept {
        counts_.fill(0);
        for (auto& w : words_) {
            w.store(0, std::memory_order_release);
        }
    }

   private:
    static constexpr uint8_t kSaturated = std::numeric_limits<uint8_t>::max();

    void SetBitLocked(size_t bit) noexcept {
        uint8_t& c = counts_[bit];
        if (c == 0) {
            c = 1;
            const size_t word_idx = bit >> 6;
            const uint64_t mask = uint64_t{1} << (bit & 63);
            const uint64_t cur =
                words_[word_idx].load(std::memory_order_relaxed);
            words_[word_idx].store(cur | mask, std::memory_order_release);
        } else if (c < kSaturated) {
            ++c;
        }
    }

    void ClearBitLocked(size_t bit) noexcept {
        uint8_t& c = counts_[bit];
        if (c == 0 || c == kSaturated) {
            return;
        }
        if (--c == 0) {
            const size_t word_idx = bit >> 6;
            const uint64_t mask = uint64_t{1} << (bit & 63);
            const uint64_t cur =
                words_[word_idx].load(std::memory_order_relaxed);
            words_[word_idx].store(cur & ~mask, std::memory_order_release);
        }
    }

    std::array<std::atomic<uint64_t>, kNumWords> words_{};
    std::array<uint8_t, kNumBits> counts_{};
};

template <typename Value, size_t kShardCount = 1024>
class ConcurrentShardedMap {
    static_assert(kShardCount > 0 && (kShardCount & (kShardCount - 1)) == 0,
                  "kShardCount must be a power of two");
    static_assert(kShardCount <= 1024,
                  "Shard index bits must remain disjoint from filter bits");

   public:
    using Map = std::unordered_map<ScopedObjectKey, Value, ScopedObjectKeyHash,
                                   ScopedObjectKeyEq>;

   private:
    struct alignas(64) Shard {
        std::atomic<size_t> size{0};
        ShardPresenceFilter filter;
        mutable std::mutex mutex;
        Map entries;
    };

   public:
    class ShardView {
       public:
        Value* Find(const TenantId& tenant_id, std::string_view key) {
            auto it = shard_.entries.find(ScopedObjectKeyView{&tenant_id, key});
            return it == shard_.entries.end() ? nullptr : &it->second;
        }

        bool Contains(const TenantId& tenant_id, std::string_view key) const {
            return shard_.entries.contains(
                ScopedObjectKeyView{&tenant_id, key});
        }

        template <typename... Args>
        bool Emplace(const TenantId& tenant_id, std::string_view key,
                     Args&&... args) {
            auto [it, inserted] = shard_.entries.emplace(
                std::piecewise_construct,
                std::forward_as_tuple(tenant_id, std::string(key)),
                std::forward_as_tuple(std::forward<Args>(args)...));
            (void)it;
            if (!inserted) {
                return false;
            }
            shard_.filter.InsertLocked(hash_);
            shard_.size.store(shard_.entries.size(), std::memory_order_relaxed);
            count_.fetch_add(1, std::memory_order_relaxed);
            return true;
        }

        bool InsertOrAssign(const TenantId& tenant_id, std::string_view key,
                            Value value) {
            auto [it, inserted] = shard_.entries.insert_or_assign(
                ScopedObjectKey{tenant_id, std::string(key)}, std::move(value));
            (void)it;
            if (inserted) {
                shard_.filter.InsertLocked(hash_);
                shard_.size.store(shard_.entries.size(),
                                  std::memory_order_relaxed);
                count_.fetch_add(1, std::memory_order_relaxed);
            }
            return inserted;
        }

       private:
        friend class ConcurrentShardedMap;
        ShardView(Shard& shard, uint64_t hash, std::atomic<uint64_t>& count)
            : shard_(shard), hash_(hash), count_(count) {}

        Shard& shard_;
        uint64_t hash_;
        std::atomic<uint64_t>& count_;
    };

    ConcurrentShardedMap() = default;
    ConcurrentShardedMap(const ConcurrentShardedMap&) = delete;
    ConcurrentShardedMap& operator=(const ConcurrentShardedMap&) = delete;

    uint64_t Count() const noexcept {
        return count_.load(std::memory_order_relaxed);
    }
    bool Empty() const noexcept { return Count() == 0; }

    std::atomic<uint64_t>& CountAtomic() noexcept { return count_; }
    const std::atomic<uint64_t>& CountAtomic() const noexcept { return count_; }

    bool Contains(const TenantId& tenant_id, std::string_view key) const {
        if (Empty()) {
            return false;
        }
        const uint64_t h = ShardPresenceFilter::MixedHash(tenant_id, key);
        const Shard& shard = ShardForHash(h);
        if (shard.size.load(std::memory_order_relaxed) == 0 ||
            !shard.filter.MayContain(h)) {
            return false;
        }
        std::lock_guard<std::mutex> lock(shard.mutex);
        return shard.entries.contains(ScopedObjectKeyView{&tenant_id, key});
    }

    std::optional<Value> FindCopy(const TenantId& tenant_id,
                                  std::string_view key) const {
        if (Empty()) {
            return std::nullopt;
        }
        const uint64_t h = ShardPresenceFilter::MixedHash(tenant_id, key);
        const Shard& shard = ShardForHash(h);
        if (shard.size.load(std::memory_order_relaxed) == 0 ||
            !shard.filter.MayContain(h)) {
            return std::nullopt;
        }
        std::lock_guard<std::mutex> lock(shard.mutex);
        auto it = shard.entries.find(ScopedObjectKeyView{&tenant_id, key});
        if (it == shard.entries.end()) {
            return std::nullopt;
        }
        return it->second;
    }

    // Lock-free negative filter fast path before locking the shard. If the key
    // exists, invokes `fn(const Value&)` under the shard lock and returns true.
    template <typename Fn>
    bool VisitExisting(const TenantId& tenant_id, std::string_view key,
                       Fn&& fn) const {
        if (Empty()) {
            return false;
        }
        const uint64_t h = ShardPresenceFilter::MixedHash(tenant_id, key);
        const Shard& shard = ShardForHash(h);
        if (shard.size.load(std::memory_order_relaxed) == 0 ||
            !shard.filter.MayContain(h)) {
            return false;
        }
        std::lock_guard<std::mutex> lock(shard.mutex);
        auto it = shard.entries.find(ScopedObjectKeyView{&tenant_id, key});
        if (it == shard.entries.end()) {
            return false;
        }
        fn(it->second);
        return true;
    }

    std::optional<Value> Erase(const TenantId& tenant_id,
                               std::string_view key) {
        if (Empty()) {
            return std::nullopt;
        }
        const uint64_t h = ShardPresenceFilter::MixedHash(tenant_id, key);
        Shard& shard = ShardForHash(h);
        if (shard.size.load(std::memory_order_relaxed) == 0 ||
            !shard.filter.MayContain(h)) {
            return std::nullopt;
        }
        return EraseInShard(shard, h, tenant_id, key);
    }

    // Erases `(tenant_id, key)` by unconditionally acquiring its shard lock,
    // skipping the lock-free negative filter. Use when the caller has observed
    // an external side-effect published inside `WithShardLocked` before
    // `Emplace` completed (e.g., draining a mailbox entry in flight).
    std::optional<Value> EraseSync(const TenantId& tenant_id,
                                   std::string_view key) {
        const uint64_t h = ShardPresenceFilter::MixedHash(tenant_id, key);
        Shard& shard = ShardForHash(h);
        return EraseInShard(shard, h, tenant_id, key);
    }

    // Locks the shard for `(tenant_id, key)` unconditionally and invokes
    // `fn(ShardView&)`. Use for insert or check-then-insert transitions.
    template <typename Fn>
    decltype(auto) WithShardLocked(const TenantId& tenant_id,
                                   std::string_view key, Fn&& fn) {
        const uint64_t h = ShardPresenceFilter::MixedHash(tenant_id, key);
        Shard& shard = ShardForHash(h);
        std::lock_guard<std::mutex> lock(shard.mutex);
        ShardView view(shard, h, count_);
        return fn(view);
    }

    // Lock-free negative filter fast path before locking the shard. If the key
    // exists, invokes `fn(Value&, bool& erase_entry)`. Setting `erase_entry`
    // to true atomically erases the entry under the same lock. Returns true
    // iff the key was found.
    template <typename Fn>
    bool WithExistingLocked(const TenantId& tenant_id, std::string_view key,
                            Fn&& fn) {
        if (Empty()) {
            return false;
        }
        const uint64_t h = ShardPresenceFilter::MixedHash(tenant_id, key);
        Shard& shard = ShardForHash(h);
        if (shard.size.load(std::memory_order_relaxed) == 0 ||
            !shard.filter.MayContain(h)) {
            return false;
        }
        std::lock_guard<std::mutex> lock(shard.mutex);
        auto it = shard.entries.find(ScopedObjectKeyView{&tenant_id, key});
        if (it == shard.entries.end()) {
            return false;
        }
        bool erase_entry = false;
        fn(it->second, erase_entry);
        if (erase_entry) {
            EraseIteratorLocked(shard, it, h);
        }
        return true;
    }

    // Read-only scan of non-empty shards returning keys matching
    // `pred(const ScopedObjectKey&, const Value&)` without erasing them.
    // Callers that must serialize erasure with an external per-object lock
    // (e.g., MetadataAccessorRW) can collect candidates here and then
    // re-validate and erase each key via `WithExistingLocked`.
    template <typename Pred>
    std::vector<ScopedObjectKey> CollectKeysIf(Pred&& pred) const {
        if (Empty()) {
            return {};
        }
        std::vector<ScopedObjectKey> matches;
        for (const Shard& shard : shards_) {
            if (shard.size.load(std::memory_order_relaxed) == 0) {
                continue;
            }
            std::lock_guard<std::mutex> lock(shard.mutex);
            for (const auto& [k, v] : shard.entries) {
                if (pred(k, v)) {
                    matches.push_back(k);
                }
            }
        }
        return matches;
    }

    // Visits non-empty shards and erases every entry for which
    // `pred(const ScopedObjectKey&, Value&)` returns true. Erased values are
    // moved out and destroyed after releasing each shard lock. Returns the
    // total number of erased entries.
    template <typename Pred>
    size_t EraseIf(Pred&& pred) {
        if (Empty()) {
            return 0;
        }
        size_t total_erased = 0;
        for (Shard& shard : shards_) {
            if (shard.size.load(std::memory_order_relaxed) == 0) {
                continue;
            }
            std::vector<Value> removed;
            {
                std::lock_guard<std::mutex> lock(shard.mutex);
                size_t shard_erased = 0;
                for (auto it = shard.entries.begin();
                     it != shard.entries.end();) {
                    if (pred(it->first, it->second)) {
                        const uint64_t h = ShardPresenceFilter::MixedHash(
                            it->first.tenant_id, it->first.key);
                        removed.push_back(std::move(it->second));
                        it = shard.entries.erase(it);
                        shard.filter.EraseLocked(h, shard.entries.empty());
                        ++shard_erased;
                    } else {
                        ++it;
                    }
                }
                if (shard_erased > 0) {
                    shard.size.store(shard.entries.size(),
                                     std::memory_order_relaxed);
                    count_.fetch_sub(shard_erased, std::memory_order_relaxed);
                    ShrinkBucketsIfSparse(shard.entries);
                    total_erased += shard_erased;
                }
            }
        }
        return total_erased;
    }

   private:
    std::optional<Value> EraseInShard(Shard& shard, uint64_t mixed_hash,
                                      const TenantId& tenant_id,
                                      std::string_view key) {
        std::lock_guard<std::mutex> lock(shard.mutex);
        auto it = shard.entries.find(ScopedObjectKeyView{&tenant_id, key});
        if (it == shard.entries.end()) {
            return std::nullopt;
        }
        std::optional<Value> removed(std::move(it->second));
        EraseIteratorLocked(shard, it, mixed_hash);
        return removed;
    }

    Shard& ShardForHash(uint64_t mixed_hash) noexcept {
        return shards_[mixed_hash & (kShardCount - 1)];
    }
    const Shard& ShardForHash(uint64_t mixed_hash) const noexcept {
        return shards_[mixed_hash & (kShardCount - 1)];
    }

    void EraseIteratorLocked(Shard& shard, typename Map::iterator it,
                             uint64_t mixed_hash) {
        shard.entries.erase(it);
        shard.filter.EraseLocked(mixed_hash, shard.entries.empty());
        shard.size.store(shard.entries.size(), std::memory_order_relaxed);
        count_.fetch_sub(1, std::memory_order_relaxed);
        ShrinkBucketsIfSparse(shard.entries);
    }

    std::atomic<uint64_t> count_{0};
    std::array<Shard, kShardCount> shards_;
};

}  // namespace mooncake
