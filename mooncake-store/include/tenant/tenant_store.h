#pragma once

#include <chrono>
#include <functional>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "lease.h"
#include "object_entry.h"
#include "tenant_id.h"

namespace mooncake {
class MasterService;  // friend for the test-only route-lock seam below
namespace tenant {

// A group is not a container of objects: it is a thin membership table plus a
// single shared Lease, consulted only at eviction (all-or-none). The read path
// never touches this.
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

    bool HasGroup(const std::string& group_id) const {
        std::shared_lock<std::shared_mutex> lock(groups_lock_);
        return groups_.find(group_id) != groups_.end();
    }

    size_t GroupCount() const {
        std::shared_lock<std::shared_mutex> lock(groups_lock_);
        return groups_.size();
    }

    // Empty a previously-empty group: only call with a valid group that has
    // members. Returns true when the group exists and is fully expired.
    bool AllExpired(const std::string& group_id,
                    const std::chrono::system_clock::time_point now) const {
        std::shared_lock<std::shared_mutex> lock(groups_lock_);
        auto it = groups_.find(group_id);
        if (it == groups_.end()) {
            return false;
        }
        return it->second.lease != nullptr && it->second.lease->IsExpired(now);
    }

    // The object route is a flat map key -> strong ObjectEntry handle. Read
    // pins the entry under the shared route lock (fast), releases it, then takes
    // the per-object lock; the strong handle keeps the entry alive across that
    // handoff.
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

    // In-flight dynamic-replication leases are keyed by proposal UUID (not by
    // object key), so they cannot fold into a per-object ObjectEntry. Guard with
    // leases_lock().
    std::unordered_map<UUID, ReplicaActionLease, boost::hash<UUID>>
        dynamic_replication_leases;

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
        const std::function<void(const std::shared_ptr<ObjectEntry>&)>&
            visitor) const {
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

    // Callback-scoped point accessor: pin the entry by key, then run `fn`
    // against its metadata while the per-object `mutex` is held. The metadata
    // reference must not escape the callback. No-op when the key is absent or
    // the entry has no metadata yet.
    template <typename Fn>
    void WithObject(const std::string& key, Fn&& fn) const {
        auto entry = Pin(key);
        if (!entry) {
            return;
        }
        entry->WithMetadata(std::forward<Fn>(fn));
    }

 private:
    friend class ::mooncake::MasterService;

    // Test-only seam: hold the route lock EXCLUSIVELY so concurrent
    // Pin/Insert/Erase/Contains block at that boundary. Accessed by MasterService
    // (which friends TenantStore) for the snapshot-barrier test hook.
    std::unique_lock<std::shared_mutex> LockRouteForTesting() const {
        return std::unique_lock<std::shared_mutex>(route_lock_);
    }

    // Object route: the strong entry handles keyed by object key, guarded by a
    // single shared_mutex. Per-object mutation is finer-grained (ObjectEntry::mutex).
    mutable std::shared_mutex route_lock_;
    std::unordered_map<std::string, std::shared_ptr<ObjectEntry>> route_;

    mutable std::shared_mutex groups_lock_;
    std::unordered_map<std::string, GroupState> groups_;

    // Guards dynamic_replication_leases (public member). Accessed rarely
    // (dynamic-replication admission + expiry sweep), so a dedicated lock.
    mutable std::shared_mutex leases_lock_;
};

}  // namespace tenant
}  // namespace mooncake
