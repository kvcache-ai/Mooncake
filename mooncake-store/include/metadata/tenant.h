#pragma once

// Tenant: one tenant's metadata state — the object route, the group table, the
// in-flight replica-action leases, the promotion-candidate index and the bound
// quota account.
//
// Identity is the entry handle: an entry stands for exactly one publication, so
// `EraseObjectIf` recognises an object by the handle a caller holds. Mutating a
// published object goes through `WithPublishedObject`, which re-checks that
// under the entry lock, and `RemoveObject` drops what a key carries — group
// membership, replica-action leases, the promotion-candidate index entry.
//
// Every method synchronizes internally. Lock order: entry lock, route lock,
// then this tenant's indexes. Only `RemoveObject` holds the route lock across
// those indexes, and nothing takes the route lock while holding one of them.

#include <algorithm>
#include <atomic>
#include <cassert>
#include <chrono>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "dynamic_replication_lease_table.h"
#include "group_index.h"
#include "object_index.h"
#include "tenant_quota.h"

namespace mooncake {
namespace metadata {

class Tenant {
   public:
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

    // Erases the route slot only when it still resolves to `expected`, without
    // touching the records that hang off the key. A publish that failed before
    // registering anything rolls back with this; a teardown that already
    // registered a membership uses `RemoveObject`, which drops those too.
    [[nodiscard]] bool EraseObjectIf(
        const std::shared_ptr<ObjectEntry>& expected) {
        if (expected == nullptr) {
            return false;
        }
        return object_index_.EraseIf(expected->key(), expected);
    }

    // Removes a torn-down object: its route slot and what the key carries
    // (group membership, replica-action leases, the promotion-candidate index
    // entry). Ownership and the drop happen under one hold of the route lock,
    // so a publication that replaces this one cannot have its records dropped
    // in between:
    //
    // - the slot holds `entry`: it owns the records, so they go with the slot;
    // - the slot holds another entry: that publication owns them, so none is
    //   touched;
    // - the slot is empty: no publication owns them, so they go — this undoes
    //   the records a publish registered before it failed.
    //
    // Returns whether the slot was erased, which a false answer does not tell
    // apart from the case that keeps the records. Callers hold the entry's own
    // lock, so a publication still wiring its state cannot be torn down
    // half-registered.
    [[nodiscard]] bool RemoveObject(const std::shared_ptr<ObjectEntry>& entry) {
        if (entry == nullptr) {
            return false;
        }
        return object_index_.WithExclusiveRoute([&](auto& route) {
            const auto it = route.find(entry->key());
            if (it != route.end() && it->second != entry) {
                return false;
            }
            const bool erased = it != route.end();
            if (erased) {
                route.erase(it);
            }
            UnregisterGroupMember(entry);
            lease_table_.EraseForObject(entry->key());
            UnindexPromotionCandidate(entry->key());
            return erased;
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

    // True when the tenant holds no object, no group membership and no lease
    // in flight.
    [[nodiscard]] bool Empty() const {
        return object_index_.Empty() && group_index_.Empty() &&
               lease_table_.Empty();
    }

    // Drops a grouped entry's membership, without touching the route slot or
    // the leases; `RemoveObject` calls it as one step of a teardown, and
    // rebuilding membership state calls it on its own. The group and the key
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

    // --- In-flight replica-action leases ------------------------------------

    // Clears the replica-action state of one object: the pending proposal, the
    // cooldown, and the leases of its key. The caller passes the state it
    // already holds under the entry's lock, so this joins a larger critical
    // section rather than taking the entry lock again. `RemoveObject` is the
    // teardown path; this is the reset path, which leaves the object routed.
    void ResetDynamicReplicationState(
        ObjectEntry::State& state, const std::shared_ptr<ObjectEntry>& entry) {
        assert(entry != nullptr);
        state.dynamic_replication_pending.reset();
        state.dynamic_replication_cooldown = {};
        lease_table_.EraseForObject(entry->key());
    }

    [[nodiscard]] std::optional<ReplicaActionLease> FindDynamicReplicationLease(
        const UUID& proposal_id) const {
        return lease_table_.Find(proposal_id);
    }

    // The entry names the object the proposal belongs to, so a proposal cannot
    // be registered under a key that is not the entry's own.
    void PutDynamicReplicationLease(const std::shared_ptr<ObjectEntry>& entry,
                                    const UUID& proposal_id,
                                    ReplicaActionLease lease) {
        assert(entry != nullptr);
        assert(lease.key == entry->key());
        lease_table_.Put(proposal_id, std::move(lease));
    }

    [[nodiscard]] bool RemoveDynamicReplicationLease(const UUID& proposal_id) {
        return lease_table_.Remove(proposal_id);
    }

    void EraseExpiredDynamicReplicationLeases(
        std::chrono::system_clock::time_point now) {
        lease_table_.EraseExpired(now);
    }

    // --- Promotion candidates -----------------------------------------------
    // Sparse index of the keys whose entry carries a promotion candidate. The
    // entry's own state stays the source of truth; this only lets the retry
    // loop enumerate candidates without walking the whole route.

    // `key` names the object whose candidate this is. A stale caller cannot
    // unindex a newer publication's candidate: `RemoveObject` only reaches the
    // index after it has established that the entry it is unwinding is the one
    // the route publishes.
    void IndexPromotionCandidate(const std::string& key) {
        std::lock_guard<std::mutex> lock(promotion_candidate_keys_mutex_);
        promotion_candidate_keys_.insert(key);
    }

    void UnindexPromotionCandidate(const std::string& key) {
        std::lock_guard<std::mutex> lock(promotion_candidate_keys_mutex_);
        promotion_candidate_keys_.erase(key);
    }

    [[nodiscard]] std::vector<std::string> PromotionCandidateKeys() const {
        std::lock_guard<std::mutex> lock(promotion_candidate_keys_mutex_);
        std::vector<std::string> keys;
        keys.reserve(promotion_candidate_keys_.size());
        for (const auto& candidate : promotion_candidate_keys_) {
            keys.push_back(candidate);
        }
        return keys;
    }

    // --- Quota account -------------------------------------------------------
    // The bound account is read on every object operation and rebound when the
    // quota policy is recomputed, so the handle is atomic: a recompute can run
    // while other threads are charging against the bound account.

    void BindQuotaAccount(TenantQuotaHandle handle) {
        quota_account_.store(handle, std::memory_order_release);
    }

    [[nodiscard]] TenantQuotaHandle BoundQuotaAccount() const {
        return quota_account_.load(std::memory_order_acquire);
    }

   private:
    // Primary object route: object key -> strong ObjectEntry handle, with the
    // per-object mutation boundary inside the entry.
    ObjectIndex object_index_;

    // Group membership and the one shared Lease per group.
    GroupIndex group_index_;

    // The replica-action leases still in flight for this tenant.
    DynamicReplicationLeaseTable lease_table_;

    // The tenant's quota account, rebound when the quota policy is recomputed.
    std::atomic<TenantQuotaHandle> quota_account_{nullptr};

    mutable std::mutex promotion_candidate_keys_mutex_;
    // The keys whose published object carries a promotion candidate.
    std::unordered_set<std::string> promotion_candidate_keys_;
};

}  // namespace metadata
}  // namespace mooncake
