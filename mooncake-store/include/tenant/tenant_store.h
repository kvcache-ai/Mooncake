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
#include "tenant_quota.h"

namespace mooncake {
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

    // ----- Object route: the tenant-local container of its objects -----
    // Objects are routed by their own key (independent of group membership).
    // The route is a flat map key -> strong ObjectEntry handle, sharded across
    // kRouteShards per-shard locks so unrelated keys never contend on one mutex.
    // Read pins the entry under the owning shard's shared lock (fast), releases
    // it, then takes the per-object lock; the strong handle keeps the entry
    // alive across that handoff.
    std::shared_ptr<ObjectEntry> Pin(const std::string& key) const {
        const auto& shard = route_shards_[RouteShardIndex(key)];
        std::shared_lock<std::shared_mutex> lock(shard.route_lock_);
        auto it = shard.route_.find(key);
        return it == shard.route_.end() ? nullptr : it->second;
    }

    // Insert a NEW entry under this tenant. Returns false if a key already
    // exists (caller re-pins instead). The entry's key() must equal `key`.
    bool Insert(std::string key, std::shared_ptr<ObjectEntry> entry) {
        auto& shard = route_shards_[RouteShardIndex(key)];
        std::unique_lock<std::shared_mutex> lock(shard.route_lock_);
        return shard.route_.emplace(std::move(key), std::move(entry)).second;
    }

    bool Erase(const std::string& key) {
        auto& shard = route_shards_[RouteShardIndex(key)];
        std::unique_lock<std::shared_mutex> lock(shard.route_lock_);
        return shard.route_.erase(key) > 0;
    }

    // Insert an object and, if it has a non-empty group_id, join the group:
    // wire the group's shared Lease into the entry and register it as a member.
    // Returns false if the key already exists.
    bool InsertObject(std::string key, std::shared_ptr<ObjectEntry> entry) {
        if (!Insert(std::move(key), entry)) {
            return false;
        }
        if (entry->IsGrouped()) {
            entry->set_lease(LeaseFor(entry->group_id()));
            AddMember(entry->group_id(), entry->key());
        }
        return true;
    }

    bool Contains(const std::string& key) const {
        const auto& shard = route_shards_[RouteShardIndex(key)];
        std::shared_lock<std::shared_mutex> lock(shard.route_lock_);
        return shard.route_.find(key) != shard.route_.end();
    }

    size_t ObjectCount() const {
        size_t count = 0;
        for (const auto& shard : route_shards_) {
            std::shared_lock<std::shared_mutex> lock(shard.route_lock_);
            count += shard.route_.size();
        }
        return count;
    }

    // Test-support: hold the owning shard's route lock EXCLUSIVELY so concurrent
    // Pin/Insert/Erase/Contains on `key` block at that route boundary. Used to
    // deterministically gate a PutStart inside the snapshot barrier.
    std::unique_lock<std::shared_mutex> LockRouteShardForTesting(
        const std::string& key) const {
        const auto& shard = route_shards_[RouteShardIndex(key)];
        return std::unique_lock<std::shared_mutex>(shard.route_lock_);
    }

    // ---- tenant-scoped state that is NOT per-object ----
    TenantQuotaHandle quota_account{nullptr};
    // Count of this tenant's objects with at least one completed LOCAL_DISK
    // replica, used to exclude disk-only objects from the eviction denominator.
    long disk_object_count{0};

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
        for (const auto& shard : route_shards_) {
            std::shared_lock<std::shared_mutex> rl(shard.route_lock_);
            if (!shard.route_.empty()) {
                return false;
            }
        }
        return true;
    }

    // Visit every live object under this tenant. For each shard we collect that
    // shard's strong handles under its shared lock, then run the visitor after
    // releasing it, so the visitor never holds a route shard lock and may
    // freely re-enter route ops. Processing under each ObjectEntry::mutex is
    // the caller's responsibility.
    void VisitObjects(
        const std::function<void(const std::shared_ptr<ObjectEntry>&)>&
            visitor) const {
        for (const auto& shard : route_shards_) {
            std::vector<std::shared_ptr<ObjectEntry>> entries;
            {
                std::shared_lock<std::shared_mutex> lock(shard.route_lock_);
                entries.reserve(shard.route_.size());
                for (const auto& [key, entry] : shard.route_) {
                    entries.push_back(entry);
                }
            }
            for (const auto& entry : entries) {
                visitor(entry);
            }
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
    // One route shard: an owning lock + the objects hashed onto it. Keys route
    // to a shard by hash (RouteShardIndex), so unrelated keys never share a
    // mutex on the read/pin path.
    struct RouteShard {
        mutable std::shared_mutex route_lock_;
        std::unordered_map<std::string, std::shared_ptr<ObjectEntry>> route_;
    };

    static constexpr size_t kRouteShards = 256;

    static size_t RouteShardIndex(const std::string& key) {
        return std::hash<std::string>{}(key) % kRouteShards;
    }

    std::vector<RouteShard> route_shards_ =
        std::vector<RouteShard>(kRouteShards);

    mutable std::shared_mutex groups_lock_;
    std::unordered_map<std::string, GroupState> groups_;

    // Guards dynamic_replication_leases (public member). Accessed rarely
    // (dynamic-replication admission + expiry sweep), so a dedicated lock.
    mutable std::shared_mutex leases_lock_;
};

}  // namespace tenant
}  // namespace mooncake
