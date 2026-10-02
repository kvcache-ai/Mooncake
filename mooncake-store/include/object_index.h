#pragma once

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
// The route is striped: which stripe a key lands in is a function of the key
// alone, so a per-key operation still takes exactly one lock, and a stripe is
// what the route's own cost is charged to. A map that outgrows its buckets
// rehashes every node it holds under its exclusive lock, and that walk is
// proportional to the nodes, so striping charges one key's growth to that
// stripe's keys rather than to every key the tenant holds. A stripe is a lock
// and a map, about 120 bytes, so the count trades a tenant's memory for write
// concurrency and for the length of that walk. Measurements are in
// benchmarks/object_index_contention_bench.cpp.
class ObjectIndex {
   public:
    // 64 stripes: the shipped default. What the sweep settles is a ratio rather
    // than a count. With 32 writer threads on a 256-core host, one stripe per
    // 512 keys still came out ahead of one per 2,048: 256 stripes reached 114M
    // lookups and 9.1M publishes per second against 79M and 6.7M in 64, and one
    // stripe reaches 8.5M and 250k, with the worst single publish into a
    // growing route falling from 20 to 30 ms to 0.8 to 2.6 ms. So 64 stripes
    // are that ratio for a tenant of about thirty thousand objects, and a
    // tenant holding fewer keys cannot use more stripes than it has while one
    // holding far more would want more: the count follows the tenant's size,
    // and this constant stands in for one size. It costs such a tenant
    // about 7.7 kB, against 30.7 kB at 256, which a tenant of a many-tenant
    // service would pay for concurrency it does not have.
    static constexpr size_t kStripeCount = 64;

    // The count is an argument so the measurement can build the same route at
    // another count; the tests take it too, and every other caller takes the
    // default. Nothing reads it from configuration.
    explicit ObjectIndex(size_t stripe_count = kStripeCount)
        : stripes_(stripe_count) {
        assert(stripe_count > 0);
    }

    // nullptr when the key is absent. The returned handle is strong, so it
    // keeps the entry alive after the stripe lock is released.
    [[nodiscard]] std::shared_ptr<ObjectEntry> Get(std::string_view key) const {
        const Stripe& stripe = StripeOf(key);
        std::shared_lock<std::shared_mutex> lock(stripe.mutex);
        const auto it = stripe.route.find(key);
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
        std::unique_lock<std::shared_mutex> lock(stripe.mutex);
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
        std::unique_lock<std::shared_mutex> lock(stripe.mutex);
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
        const Stripe& stripe = StripeOf(key);
        std::shared_lock<std::shared_mutex> lock(stripe.mutex);
        const auto it = stripe.route.find(key);
        return it != stripe.route.end() && it->second == entry;
    }

    // Runs `fn(route)` with the stripe that holds `key` held exclusively. The
    // tenant layer uses this to decide which publication owns a key's records
    // and drop those records in one section, so a publication that replaced the
    // one being torn down cannot have its records dropped in between.
    template <typename Fn>
    decltype(auto) WithExclusiveRoute(std::string_view key, Fn&& fn) {
        Stripe& stripe = StripeOf(key);
        std::unique_lock<std::shared_mutex> lock(stripe.mutex);
        return std::forward<Fn>(fn)(stripe.route);
    }

    [[nodiscard]] bool Contains(std::string_view key) const {
        const Stripe& stripe = StripeOf(key);
        std::shared_lock<std::shared_mutex> lock(stripe.mutex);
        return stripe.route.contains(key);
    }

    // Counted stripe by stripe, so the answer is the route's size at no one
    // instant: a key added to another stripe while this walks is counted.
    [[nodiscard]] size_t ObjectCount() const {
        size_t count = 0;
        for (const Stripe& stripe : stripes_) {
            std::shared_lock<std::shared_mutex> lock(stripe.mutex);
            count += stripe.route.size();
        }
        return count;
    }

    // True when no object is currently routed. Checked stripe by stripe for the
    // same reason.
    [[nodiscard]] bool Empty() const {
        for (const Stripe& stripe : stripes_) {
            std::shared_lock<std::shared_mutex> lock(stripe.mutex);
            if (!stripe.route.empty()) {
                return false;
            }
        }
        return true;
    }

    // Collect strong handles to every object currently routed. Each stripe's
    // lock is released before returning, so a caller can take each entry's own
    // mutex without holding any of them, and the result is a collection rather
    // than the route at one instant.
    //
    // The handles are sized in one pass before any of them is copied: a
    // reservation that grew with each stripe would re-copy every handle
    // collected so far, and it would do that inside that stripe's lock. A route
    // that grows while this runs can still outgrow the reservation, and then
    // one growth happens with the handles already collected.
    [[nodiscard]] std::vector<std::shared_ptr<ObjectEntry>> SnapshotObjects()
        const {
        size_t total = 0;
        for (const Stripe& stripe : stripes_) {
            std::shared_lock<std::shared_mutex> lock(stripe.mutex);
            total += stripe.route.size();
        }

        std::vector<std::shared_ptr<ObjectEntry>> entries;
        entries.reserve(total);
        for (const Stripe& stripe : stripes_) {
            std::shared_lock<std::shared_mutex> lock(stripe.mutex);
            for (const auto& entry : stripe.route) {
                entries.push_back(entry.second);
            }
        }
        return entries;
    }

    // The number of stripes this route was built with.
    [[nodiscard]] size_t StripeCount() const { return stripes_.size(); }

   private:
    // One stripe of the route: the strong entry handles keyed by object key
    // under its own lock. Mutating one object's state is finer-grained: each
    // entry guards its own.
    struct Stripe {
        mutable std::shared_mutex mutex;
        // Transparent lookup: every accessor takes a view, so a caller that
        // already has one does not build a string to find the entry.
        std::unordered_map<std::string, std::shared_ptr<ObjectEntry>,
                           TransparentStringHash, std::equal_to<>>
            route;
    };

    // The same hash the table uses, masked by the count the route was built
    // with, so a key reaches one stripe whether the caller holds a string or a
    // view.
    [[nodiscard]] size_t StripeIndex(std::string_view key) const {
        return TransparentStringHash{}(key) % stripes_.size();
    }

    [[nodiscard]] Stripe& StripeOf(std::string_view key) {
        return stripes_[StripeIndex(key)];
    }
    [[nodiscard]] const Stripe& StripeOf(std::string_view key) const {
        return stripes_[StripeIndex(key)];
    }

    std::vector<Stripe> stripes_;
};

}  // namespace mooncake
