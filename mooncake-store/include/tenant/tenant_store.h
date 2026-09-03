#pragma once

#include <chrono>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <boost/functional/hash.hpp>

#include "lease.h"
#include "object_entry.h"
#include "rpc_types.h"
#include "tenant/tenant_id.h"

namespace mooncake {
class MasterService;  // friend for the test-only route-lock seam below
namespace tenant {

// A group is not a container of objects: it is a thin membership table plus a
// single shared Lease. The lease is the all-or-none unit consulted by eviction
// (the per-member ObjectMetadata entry points at it); the read path extends
// that shared lease on a member hit without touching this table.
struct GroupState {
    std::unordered_set<std::string> member_keys;
    std::shared_ptr<Lease> lease;

    bool Empty() const { return member_keys.empty(); }
};

// Tenant-scoped container holding that tenant's group membership. MasterService
// owns this; the group TTL is single-source (one Lease per group, extended on
// any member's read), so eviction can treat the whole group as all-or-none by
// inspecting just the shared lease.
class TenantStore {
   public:
    TenantStore() = default;
    ~TenantStore() = default;

    // Return (creating on demand) the single shared Lease for a group_id.
    // Callers wire the returned lease into each member object's own lease slot,
    // so the read path can extend the group TTL without touching this table.
    std::shared_ptr<Lease> LeaseFor(const std::string& group_id) {
        std::unique_lock<std::shared_mutex> lock(groups_lock_);
        auto [it, inserted] = groups_.try_emplace(group_id);
        if (inserted) {
            it->second.lease = std::make_shared<Lease>();
        }
        return it->second.lease;
    }

    bool AddMember(const std::string& group_id, const std::string& member_key) {
        std::unique_lock<std::shared_mutex> lock(groups_lock_);
        auto it = groups_.find(group_id);
        if (it == groups_.end()) {
            return false;  // group must be materialized via LeaseFor first
        }
        return it->second.member_keys.insert(member_key).second;
    }

    bool RemoveMember(const std::string& group_id,
                      const std::string& member_key) {
        std::unique_lock<std::shared_mutex> lock(groups_lock_);
        auto it = groups_.find(group_id);
        if (it == groups_.end()) {
            return false;
        }
        it->second.member_keys.erase(member_key);
        if (it->second.Empty()) {
            groups_.erase(it);
        }
        return true;
    }

    std::vector<std::string> Members(const std::string& group_id) const {
        std::shared_lock<std::shared_mutex> lock(groups_lock_);
        auto it = groups_.find(group_id);
        if (it == groups_.end()) {
            return {};
        }
        return {it->second.member_keys.begin(), it->second.member_keys.end()};
    }

    // The object route is a flat map key -> strong ObjectEntry handle. Read
    // pins the entry under the shared route lock (fast), releases it, then
    // takes the per-object lock; the strong handle keeps the entry alive across
    // that handoff.
    std::shared_ptr<ObjectEntry> Pin(const std::string& key) const {
        std::shared_lock<std::shared_mutex> lock(route_lock_);
        auto it = route_.find(key);
        return it == route_.end() ? nullptr : it->second;
    }

    // Insert a NEW entry under this tenant. Returns false if a key already
    // exists (caller re-pins instead). The entry's key() must equal `key`.
    bool Insert(std::string key, std::shared_ptr<ObjectEntry> entry) {
        std::unique_lock<std::shared_mutex> lock(route_lock_);
        return route_.emplace(std::move(key), std::move(entry)).second;
    }

    bool Erase(const std::string& key) {
        std::unique_lock<std::shared_mutex> lock(route_lock_);
        return route_.erase(key) > 0;
    }

    // Insert an object and, if it has a non-empty group_id, join the group:
    // wire the group's shared Lease into the entry and register it as a member.
    // Returns false if the key already exists. Group membership is wired and
    // checked before the object is published, and rolled back if the publish
    // fails, so a concurrently-pinned entry never observes a non-wired grouped
    // member or a stale membership entry.
    bool InsertObject(std::string key, std::shared_ptr<ObjectEntry> entry) {
        const std::string group_id = entry->group_id();
        if (!group_id.empty()) {
            entry->set_lease(LeaseFor(group_id));
            if (!AddMember(group_id, entry->key())) {
                return false;
            }
        }
        if (!Insert(std::move(key), entry)) {
            if (!group_id.empty()) {
                RemoveMember(group_id, entry->key());
            }
            return false;
        }
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
    bool Empty() const {
        std::shared_lock<std::shared_mutex> gl(groups_lock_);
        std::shared_lock<std::shared_mutex> ll(leases_lock_);
        if (!groups_.empty() || !dynamic_replication_leases.empty()) {
            return false;
        }
        std::shared_lock<std::shared_mutex> rl(route_lock_);
        return route_.empty();
    }

    // Visit every live object under this tenant. Collect the strong handles
    // under the shared route lock, then run the visitor after releasing it, so
    // the visitor never holds the route lock and may freely re-enter route ops.
    // Processing under each ObjectEntry::mutex is the caller's responsibility.
    void VisitObjects(
        const std::function<void(const std::shared_ptr<ObjectEntry>&)>& visitor)
        const {
        std::vector<std::shared_ptr<ObjectEntry>> entries;
        {
            std::shared_lock<std::shared_mutex> lock(route_lock_);
            entries.reserve(route_.size());
            for (const auto& [key, entry] : route_) {
                entries.push_back(entry);
            }
        }
        for (const auto& entry : entries) {
            visitor(entry);
        }
    }

    // Callback-scoped test/diagnostic access; production paths pin + lock
    // explicitly. Pin the entry by key, then run `fn` against its metadata
    // while the per-object `mutex` is held. The metadata reference must not
    // escape the callback. No-op when the key is absent or the entry has no
    // metadata yet.
    template <typename Fn>
    void WithObject(const std::string& key, Fn&& fn) const {
        auto entry = Pin(key);
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

    // Remove every lease that has expired (expire_at_ms_epoch < now_ms).
    void EraseExpiredDynamicReplicationLeases(int64_t now_ms) {
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
    friend class ::mooncake::MasterService;

    // Test-only seam: hold the route lock EXCLUSIVELY so concurrent
    // Pin/Insert/Erase/Contains block at that boundary. Accessed by
    // MasterService (which friends TenantStore) for the snapshot-barrier test
    // hook.
    std::unique_lock<std::shared_mutex> LockRouteForTesting() const {
        return std::unique_lock<std::shared_mutex>(route_lock_);
    }

    // Object route: the strong entry handles keyed by object key, guarded by a
    // single shared_mutex. Per-object mutation is finer-grained
    // (ObjectEntry::mutex).
    mutable std::shared_mutex route_lock_;
    std::unordered_map<std::string, std::shared_ptr<ObjectEntry>> route_;

    mutable std::shared_mutex groups_lock_;
    std::unordered_map<std::string, GroupState> groups_;

    // In-flight dynamic-replication leases, keyed by proposal UUID (not by
    // object key), so they cannot fold into a per-object ObjectEntry. Guarded
    // by leases_lock_ and reached only through the locked accessors above.
    mutable std::shared_mutex leases_lock_;
    std::unordered_map<UUID, ReplicaActionLease, boost::hash<UUID>>
        dynamic_replication_leases;
};

}  // namespace tenant
}  // namespace mooncake
