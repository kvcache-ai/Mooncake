#pragma once

// Tenant: one tenant's metadata state — the object route, the group table, the
// in-flight replica-action leases, the promotion-candidate index and the bound
// quota account.
//
// Every method is internally synchronized: each container guards its own
// state, and an object's fields are only touched under that entry's own lock.
// No lock is held across calls.
//
// Lifecycle rule: a handle keeps an entry alive, not current. A caller that
// already holds one keeps working on it, but anything that must act on the
// published instance resolves it through the route again (`Get`) and checks
// identity before mutating the route (`EraseObjectIf`), so a removed tenant or
// a same-key replacement is never touched through a stale handle.

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

    // Erases the route slot only when it still resolves to `expected`, so a
    // teardown that holds a handle cannot drop the replacement a concurrent
    // remove and create published under the same key. The key comes from the
    // handle; a null handle never matches. A publish that failed after taking
    // the slot rolls back with this, because it registered no membership; a
    // teardown that owns the entry uses `RemoveObject` instead.
    [[nodiscard]] bool EraseObjectIf(
        const std::shared_ptr<ObjectEntry>& expected) {
        if (expected == nullptr) {
            return false;
        }
        return object_index_.EraseIf(expected->key(), expected);
    }

    // Removes a torn-down object in one step: its route slot (while it still
    // resolves to `entry`), its group membership, and the replica-action leases
    // still in flight for its key. The teardown path calls this instead of
    // dropping the three by hand, so it cannot leave the membership or a lease
    // behind. Returns whether the route slot was erased; a null handle is a
    // no-op.
    [[nodiscard]] bool RemoveObject(const std::shared_ptr<ObjectEntry>& entry) {
        if (entry == nullptr) {
            return false;
        }
        const bool erased = EraseObjectIf(entry);
        UnregisterGroupMember(entry);
        lease_table_.EraseForObject(entry->key());
        return erased;
    }

    [[nodiscard]] bool ContainsObject(std::string_view key) const {
        return object_index_.Contains(key);
    }

    [[nodiscard]] size_t ObjectCount() const {
        return object_index_.ObjectCount();
    }

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
    // rebuilding membership state calls it on its own. The key and the group
    // come from the entry, so a caller cannot unregister a membership the entry
    // does not have; the member may already be gone, so the removal result is
    // ignored.
    void UnregisterGroupMember(const std::shared_ptr<ObjectEntry>& entry) {
        const std::string& group_id = entry->group_id();
        if (group_id.empty()) {
            return;
        }
        (void)group_index_.RemoveMember(group_id, entry->key());
    }

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
    // cooldown, and the leases still in flight for its key. The caller passes
    // the state it already holds under the entry's lock, so this joins a larger
    // critical section rather than taking the entry lock again. `RemoveObject`
    // is the teardown path; this is the reset path, which leaves the object
    // routed.
    void ResetDynamicReplicationState(ObjectEntry::State& state,
                                      std::string_view key) {
        state.dynamic_replication_pending.reset();
        state.dynamic_replication_cooldown = {};
        lease_table_.EraseForObject(key);
    }

    [[nodiscard]] std::optional<ReplicaActionLease> FindDynamicReplicationLease(
        const UUID& proposal_id) const {
        return lease_table_.Find(proposal_id);
    }

    void PutDynamicReplicationLease(const UUID& proposal_id,
                                    ReplicaActionLease lease) {
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
        return {promotion_candidate_keys_.begin(),
                promotion_candidate_keys_.end()};
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
    std::unordered_set<std::string> promotion_candidate_keys_;
};

}  // namespace metadata
}  // namespace mooncake
