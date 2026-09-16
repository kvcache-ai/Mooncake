#pragma once

#include <algorithm>
#include <cassert>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include <boost/functional/hash.hpp>

#include "common/transparent_string_hash.h"
#include "object_entry.h"
#include "rpc_types.h"

namespace mooncake {

// The object route for one tenant: a flat map from object key to a strong
// ObjectEntry handle, plus the in-flight dynamic-replication lease table.
//
// A strong handle keeps the entry alive, not current. Get returns it under the
// shared route lock and releases that lock before the caller takes the entry's
// own mutex, so a caller revalidates with IsCurrent or erases with the
// identity-checked EraseIf.
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
        // After the slot exists, under the same lock: whatever can see the
        // entry can see its generation.
        it->second->generation_ = ++generation_counter_;
        return true;
    }

    // True when `entry` is still the instance published for `key` (identity
    // plus not torn down), which is how a pinned handle or a deferred eviction
    // candidate revalidates itself against a replacement of the same key.
    // Compares against the caller's pointer, so the caller keeps it alive.
    [[nodiscard]] bool IsCurrent(std::string_view key,
                                 const ObjectEntry* entry) const {
        auto pinned = Get(key);
        if (!pinned || pinned.get() != entry) {
            return false;
        }
        auto lk = entry->LockShared();
        return !entry->is_torn_down;
    }

    // Erase the route slot for `key` only when it still resolves to `expected`,
    // so a teardown that pinned an entry cannot drop the replacement a
    // concurrent remove and re-create published under the same key. `expected`
    // must still be alive; a null one never matches.
    [[nodiscard]] bool EraseIf(std::string_view key,
                               const ObjectEntry* expected) {
        std::unique_lock<std::shared_mutex> lock(route_lock_);
        const auto it = route_.find(key);
        if (it == route_.end() || it->second.get() != expected) {
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

    // True when this route holds no object and the in-flight
    // dynamic-replication lease table is empty. Group membership is tracked
    // beside this route and contributes its own term. Sampled per table rather
    // than atomically, so a true answer can go stale on the next insert.
    [[nodiscard]] bool Empty() const {
        {
            std::shared_lock<std::shared_mutex> leases(leases_lock_);
            if (!dynamic_replication_leases.empty()) {
                return false;
            }
        }
        std::shared_lock<std::shared_mutex> route(route_lock_);
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

    // Test/diagnostic access: runs `fn` against the routed entry's envelope
    // under the per-object lock. The reference must not escape the callback.
    // No-op when the key is not routed.
    template <typename Fn>
    void WithObject(std::string_view key, Fn&& fn) const {
        auto entry = Get(key);
        if (!entry) {
            return;
        }
        entry->WithMetadata(std::forward<Fn>(fn));
    }

    // --- Dynamic-replication lease table (locked accessors) ---
    // In-flight leases are keyed by proposal UUID, not by object key, so they
    // cannot fold into a per-object ObjectEntry.

    // Find a lease by proposal id. Returns a copy so the returned lease stays
    // valid after the lock is released. std::nullopt when absent.
    [[nodiscard]] std::optional<ReplicaActionLease> FindDynamicReplicationLease(
        const UUID& proposal_id) const {
        std::shared_lock<std::shared_mutex> lock(leases_lock_);
        auto it = dynamic_replication_leases.find(proposal_id);
        return it == dynamic_replication_leases.end()
                   ? std::nullopt
                   : std::make_optional<ReplicaActionLease>(it->second);
    }

    // Remove a lease by proposal id. Returns true if one was removed.
    [[nodiscard]] bool RemoveDynamicReplicationLease(const UUID& proposal_id) {
        std::unique_lock<std::shared_mutex> lock(leases_lock_);
        return dynamic_replication_leases.erase(proposal_id) > 0;
    }

    // Record (or overwrite) a lease for a proposal id.
    void PutDynamicReplicationLease(const UUID& proposal_id,
                                    ReplicaActionLease lease) {
        std::unique_lock<std::shared_mutex> lock(leases_lock_);
        dynamic_replication_leases[proposal_id] = std::move(lease);
    }

    // Remove every lease whose object key matches `key`. The table is keyed by
    // proposal, so this scans it.
    void EraseDynamicReplicationLeasesForObject(std::string_view key) {
        std::unique_lock<std::shared_mutex> lock(leases_lock_);
        std::erase_if(dynamic_replication_leases, [key](const auto& entry) {
            return std::string_view(entry.second.key) == key;
        });
    }

    // Remove every lease whose expiry instant has passed.
    void EraseExpiredDynamicReplicationLeases(
        std::chrono::system_clock::time_point now) {
        const int64_t now_ms =
            std::chrono::duration_cast<std::chrono::milliseconds>(
                now.time_since_epoch())
                .count();
        std::unique_lock<std::shared_mutex> lock(leases_lock_);
        std::erase_if(dynamic_replication_leases, [now_ms](const auto& entry) {
            return entry.second.expire_at_ms_epoch < now_ms;
        });
    }

    // Test-only: true when any lease references `key`. Production does not
    // consult the lease table by object key.
    [[nodiscard]] bool HasDynamicReplicationLeaseForKeyForTest(
        std::string_view key) const {
        std::shared_lock<std::shared_mutex> lock(leases_lock_);
        return std::any_of(dynamic_replication_leases.begin(),
                           dynamic_replication_leases.end(),
                           [key](const auto& entry) {
                               return std::string_view(entry.second.key) == key;
                           });
    }

   private:
    // Object route: the strong entry handles keyed by object key, guarded by a
    // single shared_mutex. Per-object mutation is finer-grained
    // (ObjectEntry::mutex).
    mutable std::shared_mutex route_lock_;
    // Transparent lookup: every accessor takes a view, so a caller that already
    // has one does not build a string to find the entry.
    std::unordered_map<std::string, std::shared_ptr<ObjectEntry>,
                       TransparentStringHash, std::equal_to<>>
        route_;
    // Monotonic publication counter backing ObjectEntry::generation(). Only
    // read and bumped while route_lock_ is held for writing.
    uint64_t generation_counter_{0};

    // Guarded by leases_lock_, reached only through the accessors above.
    mutable std::shared_mutex leases_lock_;
    std::unordered_map<UUID, ReplicaActionLease, boost::hash<UUID>>
        dynamic_replication_leases;
};

}  // namespace mooncake
