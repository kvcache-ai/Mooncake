#pragma once

#include <atomic>
#include <chrono>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <boost/functional/hash.hpp>

#include "lease.h"
#include "object_entry.h"
#include "rpc_types.h"
#include "tenant/tenant_id.h"

namespace mooncake::test {
class MasterServiceHATest;
}  // namespace mooncake::test

namespace mooncake {
class MasterService;  // friend: the service owns and operates the store
namespace metadata {

// The per-tenant ObjectIndex: the primary hash map (object key -> strong
// ObjectEntry handle) plus the GroupIndex and the in-flight dynamic-
// replication lease table. The group TTL is single-source (one Lease per
// group, extended on any member's read), so eviction treats a whole group as
// all-or-none by inspecting just the shared lease.
class ObjectIndex {
   public:
    ObjectIndex() = default;
    ~ObjectIndex() = default;

    // The object route is a flat map key -> strong ObjectEntry handle. Get
    // returns the strong handle under the shared route lock (fast), releasing
    // the route before the caller takes the per-object lock; the strong handle
    // keeps the entry alive across that handoff.
    std::shared_ptr<ObjectEntry> Get(const std::string& key) const {
        std::shared_lock<std::shared_mutex> lock(route_lock_);
        auto it = route_.find(key);
        return it == route_.end() ? nullptr : it->second;
    }

    // Insert a NEW entry under this tenant. Returns false if a key already
    // exists (caller re-looks-up instead). The entry's key() must equal `key`.
    // Assigns the entry's route generation (monotonic per index).
    bool Insert(std::string key, std::shared_ptr<ObjectEntry> entry) {
        std::unique_lock<std::shared_mutex> lock(route_lock_);
        // Assign the publication generation before the handle is moved into
        // the route (a failed duplicate insert simply wastes one value).
        entry->generation_ = ++generation_counter_;
        return route_.emplace(std::move(key), std::move(entry)).second;
    }

    // True when `entry` is still the current published instance for `key`
    // (identity + not torn down). Lets a pinned handle or a deferred eviction
    // candidate revalidate itself against replacements of the same key.
    bool IsCurrent(const std::string& key, const ObjectEntry* entry) const {
        auto pinned = Get(key);
        if (!pinned || pinned.get() != entry) {
            return false;
        }
        auto lk = entry->LockShared();
        return !entry->is_torn_down;
    }

    // Erase the route slot for `key` ONLY if it still resolves to `expected`.
    // A teardown that pinned an entry must not blindly erase by key: the
    // per-object lock is released before the route mutation, so a concurrent
    // remove + re-create may already have published a replacement entry under
    // the same key, and erasing by key alone would drop that live object.
    bool EraseIf(const std::string& key, const ObjectEntry* expected) {
        std::unique_lock<std::shared_mutex> lock(route_lock_);
        const auto it = route_.find(key);
        if (it == route_.end() || it->second.get() != expected) {
            return false;
        }
        route_.erase(it);
        return true;
    }

    bool Contains(const std::string& key) const {
        std::shared_lock<std::shared_mutex> lock(route_lock_);
        return route_.find(key) != route_.end();
    }

    size_t ObjectCount() const {
        std::shared_lock<std::shared_mutex> lock(route_lock_);
        return route_.size();
    }

    // True when the tenant holds no object route, no group membership, and no
    // in-flight dynamic-replication lease. Callers hold no locks when invoking.
    // Empty of in-flight dynamic-replication leases and routed objects. Group
    // membership lives in TenantCatalog's GroupIndex.
    bool Empty() const {
        std::shared_lock<std::shared_mutex> ll(leases_lock_);
        if (!dynamic_replication_leases.empty()) {
            return false;
        }
        std::shared_lock<std::shared_mutex> rl(route_lock_);
        return route_.empty();
    }

    // Collect strong handles to every live object under this tenant. The
    // route lock is released before returning, so callers can take each
    // entry's own mutex without holding the route lock — the single primitive
    // every "walk this tenant's objects" loop composes from.
    std::vector<std::shared_ptr<ObjectEntry>> SnapshotObjects() const {
        std::shared_lock<std::shared_mutex> lock(route_lock_);
        std::vector<std::shared_ptr<ObjectEntry>> entries;
        entries.reserve(route_.size());
        for (const auto& [key, entry] : route_) {
            entries.push_back(entry);
        }
        return entries;
    }

    // Callback-scoped test/diagnostic access; production paths get + lock
    // explicitly. Get the entry by key, then run `fn` against its metadata
    // while the per-object `mutex` is held. The metadata reference must not
    // escape the callback. No-op when the key is absent or the entry has no
    // metadata yet.
    template <typename Fn>
    void WithObject(const std::string& key, Fn&& fn) const {
        auto entry = Get(key);
        if (!entry) {
            return;
        }
        entry->WithMetadata(std::forward<Fn>(fn));
    }

    // --- Dynamic-replication lease table (locked accessors) ---
    // In-flight dynamic-replication leases are keyed by proposal UUID (not by
    // object key), so they do not fold into a per-object ObjectEntry. The map
    // is private and reached only through these locked accessors.

    // Find a lease by proposal id. Returns a copy so the returned lease stays
    // valid after the lock is released. std::nullopt when absent.
    std::optional<ReplicaActionLease> FindDynamicReplicationLease(
        const UUID& proposal_id) const {
        std::shared_lock<std::shared_mutex> lock(leases_lock_);
        auto it = dynamic_replication_leases.find(proposal_id);
        return it == dynamic_replication_leases.end()
                   ? std::nullopt
                   : std::make_optional<ReplicaActionLease>(it->second);
    }

    // Remove a lease by proposal id. Returns true if one was removed.
    bool RemoveDynamicReplicationLease(const UUID& proposal_id) {
        std::unique_lock<std::shared_mutex> lock(leases_lock_);
        return dynamic_replication_leases.erase(proposal_id) > 0;
    }

    // Record (or overwrite) a lease for a proposal id.
    void PutDynamicReplicationLease(const UUID& proposal_id,
                                    ReplicaActionLease lease) {
        std::unique_lock<std::shared_mutex> lock(leases_lock_);
        dynamic_replication_leases[proposal_id] = std::move(lease);
    }

    // Remove every lease whose object key matches `key`.
    void EraseDynamicReplicationLeasesForObject(const std::string& key) {
        std::unique_lock<std::shared_mutex> lock(leases_lock_);
        for (auto it = dynamic_replication_leases.begin();
             it != dynamic_replication_leases.end();) {
            if (it->second.key == key) {
                it = dynamic_replication_leases.erase(it);
            } else {
                ++it;
            }
        }
    }

    // Remove every lease whose expiry instant has passed.
    void EraseExpiredDynamicReplicationLeases(
        std::chrono::system_clock::time_point now) {
        const int64_t now_ms =
            std::chrono::duration_cast<std::chrono::milliseconds>(
                now.time_since_epoch())
                .count();
        std::unique_lock<std::shared_mutex> lock(leases_lock_);
        for (auto it = dynamic_replication_leases.begin();
             it != dynamic_replication_leases.end();) {
            if (it->second.expire_at_ms_epoch < now_ms) {
                it = dynamic_replication_leases.erase(it);
            } else {
                ++it;
            }
        }
    }

    // Test-only: true when any lease references `key`. Production does not
    // consult the lease table by object key.
    bool HasDynamicReplicationLeaseForKeyForTest(const std::string& key) const {
        std::shared_lock<std::shared_mutex> lock(leases_lock_);
        for (const auto& [_, lease] : dynamic_replication_leases) {
            if (lease.key == key) {
                return true;
            }
        }
        return false;
    }

   private:
    // Test-only seam: hold the route lock EXCLUSIVELY so concurrent
    // Get/Insert/Erase/Contains block at that boundary. Reached only through
    // TenantCatalog::LockRouteForTesting().
    friend class TenantCatalog;

    std::unique_lock<std::shared_mutex> LockRouteForTesting() const {
        return std::unique_lock<std::shared_mutex>(route_lock_);
    }

    // Object route: the strong entry handles keyed by object key, guarded by a
    // single shared_mutex. Per-object mutation is finer-grained
    // (ObjectEntry::mutex).
    mutable std::shared_mutex route_lock_;
    std::unordered_map<std::string, std::shared_ptr<ObjectEntry>> route_;
    // Monotonic publication counter backing ObjectEntry::generation().
    std::atomic<uint64_t> generation_counter_{0};

    // In-flight dynamic-replication leases (proposal-id keyed). Group
    // membership lives in TenantCatalog's GroupIndex.

    // In-flight dynamic-replication leases, keyed by proposal UUID (not by
    // object key), so they cannot fold into a per-object ObjectEntry. Guarded
    // by leases_lock_ and reached only through the locked accessors above.
    mutable std::shared_mutex leases_lock_;
    std::unordered_map<UUID, ReplicaActionLease, boost::hash<UUID>>
        dynamic_replication_leases;
};

}  // namespace metadata
}  // namespace mooncake
