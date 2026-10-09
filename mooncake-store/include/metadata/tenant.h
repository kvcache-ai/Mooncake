#pragma once

// Tenant: one tenant's object route and the lifecycle of its groups, plus the
// quota account it was built with. Replica-action leases and promotion
// candidates belong to their own subsystems, which validate what they hold
// against the entry the route publishes before acting on it.
//
// Identity is the entry handle: an entry stands for exactly one publication, so
// `RemoveObject` recognises an object by the handle a caller holds and drops
// its group membership with the slot. Mutating a published object goes through
// `WithPublishedObject`, which re-checks that identity under the entry lock.
//
// Every method synchronizes internally. Lock order: entry lock, route lock,
// then the group table. Only `RemoveObject` holds the route lock across the
// group table, and nothing takes the route lock while holding a group stripe.

#include <algorithm>
#include <cassert>
#include <chrono>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include "group_index.h"
#include "object_index.h"
#include "tenant_quota.h"

namespace mooncake {
namespace metadata {

class Tenant {
   public:
    // `quota_account` is the tenant's account in the quota table, or null when
    // quotas are off. The registry's factory resolves it before building the
    // tenant, and it never changes afterwards: the quota table keeps one stable
    // account per tenant id and a policy recompute updates it in place.
    explicit Tenant(TenantQuotaHandle quota_account = nullptr)
        : quota_account_(quota_account) {}

    // Publishes `entry` on this tenant's route. The route slot and the group
    // lease are wired under the entry's own lock, so a reader that reaches the
    // entry through the route cannot observe a grouped object before its group
    // lease is in place. False when the key is already routed, and then nothing
    // was registered.
    [[nodiscard]] bool InsertObject(std::shared_ptr<ObjectEntry> entry) {
        const std::string group_id = entry->group_id();
        bool inserted = false;
        entry->WithExclusiveAccess(
            [&](ObjectMetadata& metadata, ObjectEntry::State&) {
                inserted = object_index_.Insert(entry);
                if (inserted && !group_id.empty()) {
                    // AddMember returns null only for an empty group_id, which
                    // the guard above already excluded.
                    metadata.lease_ =
                        group_index_.AddMember(group_id, entry->key());
                    assert(metadata.lease_ != nullptr);
                }
            });
        return inserted;
    }

    // Null when the key is absent.
    [[nodiscard]] std::shared_ptr<ObjectEntry> Get(std::string_view key) const {
        return object_index_.Get(key);
    }

    // Removes a torn-down object: its route slot and its group membership,
    // only while the slot still holds `entry`; false, touching nothing,
    // otherwise. Both go under one hold of the route lock, because membership
    // is keyed by the object key: once the slot is released, a newer
    // publication of the same key can register the membership this teardown
    // would drop. Callers hold the entry's own lock, so a publication still
    // wiring its state cannot be torn down half-registered.
    [[nodiscard]] bool RemoveObject(const std::shared_ptr<ObjectEntry>& entry) {
        if (entry == nullptr) {
            return false;
        }
        return object_index_.WithExclusiveRoute([&](auto& route) {
            const auto it = route.find(entry->key());
            if (it == route.end() || it->second != entry) {
                return false;
            }
            route.erase(it);
            UnregisterGroupMember(entry);
            return true;
        });
    }

    // Runs `fn(metadata, state)` on the entry the route currently publishes for
    // `key`, under that entry's own lock, and only once it has re-checked under
    // that lock that the slot still publishes this entry and that the entry is
    // not torn down. False without running `fn` otherwise, so a caller that
    // kept a handle from before resolves the key again instead of acting on it.
    //
    // The callback runs inside the entry's lock, which is not recursive: it
    // must not call back into `WithPublishedObject` for the same key, nor
    // `InsertObject` for the same entry.
    template <typename Fn>
    [[nodiscard]] bool WithPublishedObject(std::string_view key, Fn&& fn) {
        const auto entry = object_index_.Get(key);
        if (entry == nullptr) {
            return false;
        }
        return entry->WithExclusiveAccess(
            [&](ObjectMetadata& metadata, ObjectEntry::State& state) -> bool {
                if (state.is_torn_down ||
                    !object_index_.IsCurrent(key, entry)) {
                    return false;
                }
                std::forward<Fn>(fn)(metadata, state);
                return true;
            });
    }

    [[nodiscard]] bool ContainsObject(std::string_view key) const {
        return object_index_.Contains(key);
    }

    [[nodiscard]] size_t ObjectCount() const {
        return object_index_.ObjectCount();
    }

    // Strong handles to what is routed right now, for a scan that acts on them
    // after it ends: resolve each key again before mutating anything, since an
    // entry can be replaced under the same key in between.
    [[nodiscard]] std::vector<std::shared_ptr<ObjectEntry>> SnapshotObjects()
        const {
        return object_index_.SnapshotObjects();
    }

    // True when the tenant holds no object and no group membership.
    [[nodiscard]] bool Empty() const {
        return object_index_.Empty() && group_index_.Empty();
    }

    // Drops a grouped entry's membership, without touching the route slot;
    // `RemoveObject` calls it as one step of a teardown, and rebuilding
    // membership state calls it on its own. The group and the key
    // come from the entry. Callers must have established that this entry is the
    // one the route publishes, which is what keeps a teardown from dropping the
    // membership a newer publication of the same key registered.
    void UnregisterGroupMember(const std::shared_ptr<ObjectEntry>& entry) {
        const std::string& group_id = entry->group_id();
        if (group_id.empty()) {
            return;
        }
        (void)group_index_.RemoveMember(group_id, entry->key());
    }

    // The member keys of one group, as a snapshot to re-resolve: a group can be
    // dropped and rebuilt while a caller walks the list, so a key in it may
    // resolve to a different object than the one that registered it, or to
    // none. Callers that act on the members, eviction among them, read each key
    // again and check what they find rather than treating the list and the
    // handles as one consistent view.
    [[nodiscard]] std::vector<std::string> GroupMembers(
        std::string_view group_id) const {
        return group_index_.Members(group_id);
    }

    // Rebuilds group membership and the group leases from object metadata, for
    // the snapshot and standby restore paths. The first pass takes the maximum
    // restored deadline per group, so a grouped object is not left on a
    // zero-deadline lease that post-restore cleanup would drop; the second
    // registers membership and points every grouped entry at the group's shared
    // lease. Ungrouped objects keep the lease they were constructed with.
    void RebuildGroupState() {
        std::unordered_map<std::string, std::chrono::system_clock::time_point>
            max_deadline_by_group;
        const auto objects = object_index_.SnapshotObjects();
        for (const auto& entry : objects) {
            entry->WithSharedAccess(
                [&](const ObjectMetadata& metadata, const ObjectEntry::State&) {
                    if (!metadata.IsGrouped()) {
                        return;
                    }
                    const auto deadline = metadata.EvictionDeadline();
                    auto [it, inserted] = max_deadline_by_group.try_emplace(
                        metadata.group_id, deadline);
                    if (!inserted) {
                        it->second = std::max(it->second, deadline);
                    }
                });
        }
        for (const auto& entry : objects) {
            // `group_id` is a const member of the envelope, so it reads without
            // the entry lock, unlike the metadata write below.
            const std::string group_id = entry->group_id();
            if (group_id.empty()) {
                continue;
            }
            auto lease = group_index_.AddMember(group_id, entry->key());
            // A non-empty group_id always yields a lease, as in InsertObject.
            assert(lease != nullptr);
            const auto it = max_deadline_by_group.find(group_id);
            if (it != max_deadline_by_group.end()) {
                lease->ExtendTo(it->second);
            }
            entry->WithExclusiveAccess(
                [&](ObjectMetadata& metadata, ObjectEntry::State&) {
                    metadata.lease_ = std::move(lease);
                });
        }
    }

    // --- Quota account -------------------------------------------------------

    [[nodiscard]] TenantQuotaHandle QuotaAccount() const {
        return quota_account_;
    }

   private:
    // Primary object route: object key -> strong ObjectEntry handle, with the
    // per-object mutation boundary inside the entry.
    ObjectIndex object_index_;

    // Group membership and the one shared Lease per group.
    GroupIndex group_index_;

    // The tenant's quota account, fixed at construction.
    const TenantQuotaHandle quota_account_;
};

}  // namespace metadata
}  // namespace mooncake
