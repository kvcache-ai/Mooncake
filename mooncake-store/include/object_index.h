#pragma once

#include <cassert>
#include <cstddef>
#include <cstdint>
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

// The object route for one tenant: a flat map from object key to a strong
// ObjectEntry handle.
//
// A strong handle keeps the entry alive, not current: Get returns it under the
// shared route lock and releases that lock before the caller takes the entry's
// own mutex, so a caller that needs the published instance compares its
// generation under that lock, and a teardown erases with the identity-checked
// EraseIf.
class ObjectIndex {
   public:
    // nullptr when the key is absent. The returned handle is strong, so it
    // keeps the entry alive after the route lock is released.
    [[nodiscard]] std::shared_ptr<ObjectEntry> Get(std::string_view key) const {
        std::shared_lock<std::shared_mutex> lock(route_lock_);
        auto it = route_.find(key);
        return it == route_.end() ? nullptr : it->second;
    }

    // Publish `entry` under its own key. Returns false when the key is
    // already routed, in which case `entry` is left untouched and the caller
    // re-looks-up instead.
    //
    // The route key is the entry's own key, so the slot it lands in and the
    // key every lookup, revalidation and erase uses cannot disagree.
    [[nodiscard]] bool Insert(std::shared_ptr<ObjectEntry> entry) {
        assert(entry != nullptr);
        std::unique_lock<std::shared_mutex> lock(route_lock_);
        const auto [it, inserted] =
            route_.try_emplace(entry->key(), std::move(entry));
        if (!inserted) {
            return false;
        }
        it->second->generation_ = ++generation_counter_;
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
        std::unique_lock<std::shared_mutex> lock(route_lock_);
        const auto it = route_.find(key);
        if (it == route_.end() || it->second != expected) {
            return false;
        }
        route_.erase(it);
        return true;
    }

    [[nodiscard]] bool Contains(std::string_view key) const {
        std::shared_lock<std::shared_mutex> lock(route_lock_);
        return route_.contains(key);
    }

    [[nodiscard]] size_t ObjectCount() const {
        std::shared_lock<std::shared_mutex> lock(route_lock_);
        return route_.size();
    }

    // True when no object is currently routed. The tenant aggregate composes
    // this with the other containers it owns.
    [[nodiscard]] bool Empty() const {
        std::shared_lock<std::shared_mutex> lock(route_lock_);
        return route_.empty();
    }

    // Collect strong handles to every object currently routed. The route lock
    // is released before returning, so a caller can take each entry's own
    // mutex without holding the route lock.
    [[nodiscard]] std::vector<std::shared_ptr<ObjectEntry>> SnapshotObjects()
        const {
        std::shared_lock<std::shared_mutex> lock(route_lock_);
        std::vector<std::shared_ptr<ObjectEntry>> entries;
        entries.reserve(route_.size());
        for (const auto& entry : route_) {
            entries.push_back(entry.second);
        }
        return entries;
    }

   private:
    // Object route: the strong entry handles keyed by object key, guarded by a
    // single shared_mutex. Mutating one object's state is finer-grained: each
    // entry guards its own.
    mutable std::shared_mutex route_lock_;
    // Transparent lookup: every accessor takes a view, so a caller that already
    // has one does not build a string to find the entry.
    std::unordered_map<std::string, std::shared_ptr<ObjectEntry>,
                       TransparentStringHash, std::equal_to<>>
        route_;
    // Monotonic publication counter backing ObjectEntry::generation(). Only
    // read and bumped while route_lock_ is held for writing.
    uint64_t generation_counter_{0};
};

}  // namespace mooncake
