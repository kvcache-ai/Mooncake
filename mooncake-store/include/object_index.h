#pragma once

#include <array>
#include <cassert>
#include <cstddef>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/transparent_string_hash.h"
#include "object_entry.h"

namespace mooncake {

// The object route for one tenant: a key to a strong ObjectEntry handle.
//
// Identity is the handle: a lookup that hands back the same handle names the
// same publication, and an entry instance stands for exactly one publication,
// so comparing handles is the whole of the identity check.
//
// The route is striped: a key always lands in the same stripe, and each stripe
// is a map under its own lock. A stripe is what bounds the cost of a route that
// grows, because a map that outgrows its buckets rehashes every node it holds
// under its exclusive lock, and that walk is proportional to the nodes. Per-key
// operations still take exactly one lock, since a key's stripe is a function of
// the key alone.
class ObjectIndex {
   public:
    static constexpr size_t kStripeCount = 64;

    // nullptr when the key is absent. The returned handle is strong, so it
    // keeps the entry alive after the stripe lock is released.
    [[nodiscard]] std::shared_ptr<ObjectEntry> Get(std::string_view key) const {
        Stripe& stripe = StripeOf(key);
        std::shared_lock<std::shared_mutex> lock(stripe.lock);
        auto it = stripe.route.find(key);
        return it == stripe.route.end() ? nullptr : it->second;
    }

    // Publish `entry` under its own key. Returns false when the key is
    // already routed, in which case `entry` is left untouched and the caller
    // re-looks-up instead.
    //
    // The route key is the entry's own key, so the slot it lands in and the
    // key every lookup, revalidation and erase uses cannot disagree.
    //
    // An entry is published at most once, which is asserted below: a handle
    // names a publication only while one instance stands for one publication,
    // so publishing the same instance twice is a caller bug rather than a
    // rejected duplicate.
    [[nodiscard]] bool Insert(std::shared_ptr<ObjectEntry> entry) {
        assert(entry != nullptr);
        assert(!entry->IsPublished());
        Stripe& stripe = StripeOf(entry->key());
        std::unique_lock<std::shared_mutex> lock(stripe.lock);
        const auto [it, inserted] =
            stripe.route.try_emplace(entry->key(), std::move(entry));
        if (!inserted) {
            return false;
        }
        it->second->published_.store(true, std::memory_order_relaxed);
        return true;
    }

    // Erase the route slot for `key` only when it still resolves to
    // `expected`, so a teardown that holds an entry cannot drop the
    // replacement a concurrent remove and re-create published under the same
    // key. The handle is an argument rather than a raw pointer because the
    // caller's own handle is what keeps the entry alive across the call; a
    // null one never matches.
    [[nodiscard]] bool EraseIf(std::string_view key,
                               const std::shared_ptr<ObjectEntry>& expected) {
        Stripe& stripe = StripeOf(key);
        std::unique_lock<std::shared_mutex> lock(stripe.lock);
        const auto it = stripe.route.find(key);
        if (it == stripe.route.end() || it->second != expected) {
            return false;
        }
        stripe.route.erase(it);
        return true;
    }

    // True while the route publishes exactly `entry` for `key`.
    [[nodiscard]] bool IsCurrent(
        std::string_view key, const std::shared_ptr<ObjectEntry>& entry) const {
        if (entry == nullptr) {
            return false;
        }
        Stripe& stripe = StripeOf(key);
        std::shared_lock<std::shared_mutex> lock(stripe.lock);
        const auto it = stripe.route.find(key);
        return it != stripe.route.end() && it->second == entry;
    }

    // Runs `fn(stripe)` with the stripe that owns `key` held exclusively, so a
    // caller can read the slot and act on it in one section: the tenant layer
    // drops a key's records and its route slot together, and nothing can
    // replace the publication in between. The key names the stripe, so another
    // key of the same tenant is not affected.
    template <typename Fn>
    decltype(auto) WithExclusiveRoute(std::string_view key, Fn&& fn) {
        Stripe& stripe = StripeOf(key);
        std::unique_lock<std::shared_mutex> lock(stripe.lock);
        return std::forward<Fn>(fn)(stripe.route);
    }

    [[nodiscard]] bool Contains(std::string_view key) const {
        Stripe& stripe = StripeOf(key);
        std::shared_lock<std::shared_mutex> lock(stripe.lock);
        return stripe.route.contains(key);
    }

    [[nodiscard]] size_t ObjectCount() const {
        size_t count = 0;
        for (const Stripe& stripe : stripes_) {
            std::shared_lock<std::shared_mutex> lock(stripe.lock);
            count += stripe.route.size();
        }
        return count;
    }

    // True when no object is currently routed.
    [[nodiscard]] bool Empty() const {
        for (const Stripe& stripe : stripes_) {
            std::shared_lock<std::shared_mutex> lock(stripe.lock);
            if (!stripe.route.empty()) {
                return false;
            }
        }
        return true;
    }

    // Collect strong handles to every object routed, one stripe at a time. A
    // stripe's lock is released before its handles are returned, so a caller
    // can take each entry's own mutex without holding a route lock, and the
    // walk is per stripe rather than one point in time: callers use the result
    // as the keys to resolve again.
    [[nodiscard]] std::vector<std::shared_ptr<ObjectEntry>> SnapshotObjects()
        const {
        // The stripes are counted first so the handles are copied into one
        // allocation: reserving per stripe would reallocate and copy the whole
        // vector once per stripe.
        size_t total = 0;
        for (const Stripe& stripe : stripes_) {
            std::shared_lock<std::shared_mutex> lock(stripe.lock);
            total += stripe.route.size();
        }
        std::vector<std::shared_ptr<ObjectEntry>> entries;
        entries.reserve(total);
        for (const Stripe& stripe : stripes_) {
            std::shared_lock<std::shared_mutex> lock(stripe.lock);
            for (const auto& entry : stripe.route) {
                entries.push_back(entry.second);
            }
        }
        return entries;
    }

   private:
    // One stripe of the route: the strong entry handles of the keys that map to
    // it. Mutating one object's state is finer-grained: each entry guards its
    // own.
    struct Stripe {
        mutable std::shared_mutex lock;
        // Transparent lookup: every accessor takes a view, so a caller that
        // already has one does not build a string to find the entry.
        std::unordered_map<std::string, std::shared_ptr<ObjectEntry>,
                           TransparentStringHash, std::equal_to<>>
            route;
    };

    [[nodiscard]] static size_t StripeIndex(std::string_view key) {
        return TransparentStringHash{}(key) % kStripeCount;
    }

    [[nodiscard]] Stripe& StripeOf(std::string_view key) const {
        return stripes_[StripeIndex(key)];
    }

    mutable std::array<Stripe, kStripeCount> stripes_;
};

}  // namespace mooncake
