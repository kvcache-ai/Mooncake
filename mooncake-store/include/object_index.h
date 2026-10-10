#pragma once

#include <array>
#include <cassert>
#include <cstddef>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <type_traits>
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
    // Transparent lookup: every accessor takes a view, so a caller that
    // already has one does not build a string to find the entry.
    using RouteMap =
        std::unordered_map<std::string, std::shared_ptr<ObjectEntry>,
                           TransparentStringHash, std::equal_to<>>;

   public:
    static constexpr size_t kStripeCount = 64;

    // nullptr when the key is absent. The returned handle is strong, so it
    // keeps the entry alive after the stripe lock is released.
    [[nodiscard]] std::shared_ptr<ObjectEntry> Get(std::string_view key) const {
        AssertNotInCursor();
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
        AssertNotInCursor();
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
        AssertNotInCursor();
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
        AssertNotInCursor();
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
        AssertNotInCursor();
        Stripe& stripe = StripeOf(key);
        std::unique_lock<std::shared_mutex> lock(stripe.lock);
        return std::forward<Fn>(fn)(stripe.route);
    }

    [[nodiscard]] bool Contains(std::string_view key) const {
        AssertNotInCursor();
        Stripe& stripe = StripeOf(key);
        std::shared_lock<std::shared_mutex> lock(stripe.lock);
        return stripe.route.contains(key);
    }

    [[nodiscard]] size_t ObjectCount() const {
        AssertNotInCursor();
        size_t count = 0;
        for (const Stripe& stripe : stripes_) {
            std::shared_lock<std::shared_mutex> lock(stripe.lock);
            count += stripe.route.size();
        }
        return count;
    }

    // True when no object is currently routed.
    [[nodiscard]] bool Empty() const {
        AssertNotInCursor();
        for (const Stripe& stripe : stripes_) {
            std::shared_lock<std::shared_mutex> lock(stripe.lock);
            if (!stripe.route.empty()) {
                return false;
            }
        }
        return true;
    }

    template <LockMode kMode>
    class Cursor;

    // A cursor over every routed object, each visited under its own read or
    // write lock. The route itself is only ever read: a write cursor may change
    // an object's metadata and state, never which objects are routed. Use it
    // as a range:
    //
    //     for (auto object : index.ReadCursor()) { ... object.metadata() ... }
    //
    // Each object is visited at most once, while the route still publishes
    // it, and the cursor moves one stripe at a time rather than reading one
    // point in time: an object published or dropped meanwhile may or may not
    // be seen.
    //
    // The cursor holds the stripe's lock across the loop body, so no handle is
    // copied, and an entry it visits from there is current without a second
    // lookup. The body therefore must not reach any route, through this index
    // or another (debug builds assert it), nor block on a lock whose holder
    // may be waiting on a route.
    [[nodiscard]] Cursor<LockMode::kRead> ReadCursor() const {
        return Cursor<LockMode::kRead>(*this);
    }
    [[nodiscard]] Cursor<LockMode::kWrite> WriteCursor() {
        return Cursor<LockMode::kWrite>(*this);
    }

    // The single-pass cursor behind `ReadCursor()` and `WriteCursor()`. It
    // owns the locks of the position it stands on, so it is neither copyable
    // nor movable, and leaving the loop early releases them.
    //
    // The lock order is entry, then route, so while it holds a stripe the
    // cursor only tries an entry's lock: an entry busy elsewhere, which may be
    // a teardown waiting on this very stripe, is set aside and visited after
    // the stripes, under a lock it waits for and a route check of its own.
    // Since TearDownObject claims an entry and drops its slot under one hold
    // of the entry, an entry still in the stripe whose lock the cursor
    // obtained has not been torn down.
    template <LockMode kMode>
    class Cursor {
        static constexpr bool kWrite = kMode == LockMode::kWrite;
        using EntryLock =
            std::conditional_t<kWrite, std::unique_lock<std::shared_mutex>,
                               std::shared_lock<std::shared_mutex>>;
        using Index =
            std::conditional_t<kWrite, ObjectIndex, const ObjectIndex>;

       public:
        // The object a position stands on, valid until the cursor advances.
        // It reads like a hold of the same mode; the cursor keeps the lock.
        class Object : public ObjectEntry::LockedAccess<Object, kMode> {
           public:
            // A copy keeps the entry beyond the cursor; acting on it then is
            // an ordinary handle access that checks the route again.
            const std::shared_ptr<ObjectEntry>& handle() const {
                return handle_;
            }

           private:
            friend class Cursor;
            explicit Object(const std::shared_ptr<ObjectEntry>& handle)
                : handle_(handle) {}
            const std::shared_ptr<ObjectEntry>& handle_;
        };

        struct Sentinel {};

        class Iterator {
           public:
            Object operator*() const { return Object(*cursor_->current_); }
            Iterator& operator++() {
                cursor_->Advance();
                return *this;
            }
            bool operator!=(Sentinel) const {
                return cursor_->current_ != nullptr;
            }

           private:
            friend class Cursor;
            explicit Iterator(Cursor* cursor) : cursor_(cursor) {}
            Cursor* cursor_;
        };

        Cursor(const Cursor&) = delete;
        Cursor& operator=(const Cursor&) = delete;
        ~Cursor() {
            entry_lock_ = EntryLock();
            ReleaseStripe();
        }

        Iterator begin() { return Iterator(this); }
        Sentinel end() const { return {}; }

       private:
        friend class ObjectIndex;

        explicit Cursor(Index& index) : index_(index) { Settle(); }

        void Advance() {
            entry_lock_ = EntryLock();
            if (stripe_ < kStripeCount) {
                ++route_it_;
            } else {
                ++deferred_pos_;
            }
            Settle();
        }

        // Moves to the first object at or after the current position whose
        // lock it obtains, or to the end.
        void Settle() NO_THREAD_SAFETY_ANALYSIS {
            for (; stripe_ < kStripeCount; ++stripe_) {
                const Stripe& stripe = index_.stripes_[stripe_];
                if (!stripe_lock_.owns_lock()) {
                    AssertNotInCursor();
                    stripe_lock_ =
                        std::shared_lock<std::shared_mutex>(stripe.lock);
                    ++cursor_stripes_;
                    route_it_ = stripe.route.begin();
                }
                for (; route_it_ != stripe.route.end(); ++route_it_) {
                    const std::shared_ptr<ObjectEntry>& entry =
                        route_it_->second;
                    EntryLock lock(entry->mutex_, std::try_to_lock);
                    if (lock.owns_lock()) {
                        entry_lock_ = std::move(lock);
                        current_ = &entry;
                        return;
                    }
                    deferred_.push_back(entry);
                }
                ReleaseStripe();
            }
            for (; deferred_pos_ < deferred_.size(); ++deferred_pos_) {
                const std::shared_ptr<ObjectEntry>& entry =
                    deferred_[deferred_pos_];
                EntryLock lock(entry->mutex_);
                if (index_.IsCurrent(entry->key(), entry)) {
                    entry_lock_ = std::move(lock);
                    current_ = &entry;
                    return;
                }
            }
            current_ = nullptr;
        }

        void ReleaseStripe() {
            if (stripe_lock_.owns_lock()) {
                stripe_lock_.unlock();
                --cursor_stripes_;
            }
        }

        Index& index_;
        size_t stripe_ = 0;
        std::shared_lock<std::shared_mutex> stripe_lock_;
        typename RouteMap::const_iterator route_it_;
        // Entries that were busy when the cursor passed their stripe.
        std::vector<std::shared_ptr<ObjectEntry>> deferred_;
        size_t deferred_pos_ = 0;
        EntryLock entry_lock_;
        const std::shared_ptr<ObjectEntry>* current_ = nullptr;
    };

   private:
    // One stripe of the route: the strong entry handles of the keys that map to
    // it. Mutating one object's state is finer-grained: each entry guards its
    // own.
    struct Stripe {
        mutable std::shared_mutex lock;
        RouteMap route;
    };

    // How many stripe locks this thread holds for a cursor. A route access
    // made while one is held is the cursor's loop body reaching a route, which
    // can deadlock against a writer that holds an entry and waits on that
    // stripe.
    static inline thread_local int cursor_stripes_ = 0;

    static void AssertNotInCursor() {
        assert(cursor_stripes_ == 0 &&
               "route access from inside an object cursor");
    }

    [[nodiscard]] static size_t StripeIndex(std::string_view key) {
        return TransparentStringHash{}(key) % kStripeCount;
    }

    [[nodiscard]] Stripe& StripeOf(std::string_view key) const {
        return stripes_[StripeIndex(key)];
    }

    mutable std::array<Stripe, kStripeCount> stripes_;
};

}  // namespace mooncake
