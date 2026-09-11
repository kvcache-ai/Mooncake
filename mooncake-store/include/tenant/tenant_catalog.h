#pragma once

// TenantCatalog: the per-tenant aggregate inside the MetadataCatalog —
// the tenant's ObjectIndex plus quota and eviction-census bookkeeping.

#include <atomic>
#include <chrono>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_set>
#include <vector>

#include "tenant/group_index.h"
#include "tenant/object_index.h"
#include "tenant_quota.h"

namespace mooncake {
namespace metadata {

class TenantCatalog {
   public:
    // Publication transaction: wire the group lease and register membership
    // before publishing, rolling membership back if the publish fails — a
    // concurrently-pinned entry never observes a half-wired grouped member.
    bool InsertObject(std::string key, std::shared_ptr<ObjectEntry> entry) {
        if (key != entry->key()) {
            return false;
        }
        const std::string group_id = entry->group_id();
        if (!group_id.empty()) {
            // One atomic group operation: materialize + register + fetch the
            // paired lease, so membership and the metadata's lease can never
            // disagree across a concurrent destroy/recreate of the group.
            auto lease = group_index.AddMember(group_id, entry->key());
            if (lease == nullptr) {
                return false;
            }
            entry->metadata().SetLease(std::move(lease));
        }
        if (!object_index.Insert(entry->key(), entry)) {
            if (!group_id.empty()) {
                group_index.RemoveMember(group_id, entry->key());
            }
            return false;
        }
        return true;
    }

    // Test/benchmark introspection of the promotion retry queue.
    size_t CountPromotionCandidatesForTesting() const {
        size_t count = 0;
        for (const auto& entry : object_index.SnapshotObjects()) {
            auto lk = entry->LockShared();
            if (entry->promotion_candidate.has_value()) {
                ++count;
            }
        }
        return count;
    }

    void ResetPromotionCandidateBackoffsForTesting() {
        const auto epoch = std::chrono::steady_clock::time_point{};
        for (const auto& entry : object_index.SnapshotObjects()) {
            auto lk = entry->LockUnique();
            if (entry->promotion_candidate.has_value()) {
                entry->promotion_candidate->retry_after = epoch;
            }
        }
    }

    // Sparse index of keys with an active promotion candidate. The entry's
    // promotion_candidate stays the single source of truth; this only lets
    // the retry loop enumerate candidates without scanning the whole route.
    // Membership is maintained by the candidate record/erase chokepoints.
    void IndexPromotionCandidate(const std::string& key) {
        std::lock_guard lock(promotion_candidate_keys_mutex_);
        promotion_candidate_keys_.insert(key);
    }
    void UnindexPromotionCandidate(const std::string& key) {
        std::lock_guard lock(promotion_candidate_keys_mutex_);
        promotion_candidate_keys_.erase(key);
    }
    std::vector<std::string> PromotionCandidateKeys() const {
        std::lock_guard lock(promotion_candidate_keys_mutex_);
        return {promotion_candidate_keys_.begin(),
                promotion_candidate_keys_.end()};
    }
    size_t PromotionCandidateKeyCountForTesting() const {
        std::lock_guard lock(promotion_candidate_keys_mutex_);
        return promotion_candidate_keys_.size();
    }

    // Empty of objects, group membership and (via ObjectIndex) in-flight
    // dynamic-replication leases.
    bool Empty() const { return group_index.Empty() && object_index.Empty(); }

    // Remove a grouped entry's membership (object teardown path).
    void UnregisterGroupMember(const std::string& key,
                               const std::string& group_id) {
        if (group_id.empty()) {
            return;
        }
        group_index.RemoveMember(group_id, key);
    }

    // Member keys of a group (empty when the group is absent).
    std::vector<std::string> GroupMembers(const std::string& group_id) const {
        return group_index.Members(group_id);
    }

    // --- Dynamic-replication lease surface ----------------------------------
    // The lease table is keyed by proposal UUID (not by object key) and lives
    // in ObjectIndex behind its own locks; reached only through these.
    std::optional<ReplicaActionLease> FindDynamicReplicationLease(
        const UUID& proposal_id) const {
        return object_index.FindDynamicReplicationLease(proposal_id);
    }
    bool RemoveDynamicReplicationLease(const UUID& proposal_id) {
        return object_index.RemoveDynamicReplicationLease(proposal_id);
    }
    void PutDynamicReplicationLease(const UUID& proposal_id,
                                    ReplicaActionLease lease) {
        object_index.PutDynamicReplicationLease(proposal_id, std::move(lease));
    }
    void EraseDynamicReplicationLeasesForObject(const std::string& key) {
        object_index.EraseDynamicReplicationLeasesForObject(key);
    }
    void EraseExpiredDynamicReplicationLeases(
        std::chrono::system_clock::time_point now) {
        object_index.EraseExpiredDynamicReplicationLeases(now);
    }
    // Test introspection: true when any lease references `key`.
    bool HasDynamicReplicationLeaseForKeyForTest(const std::string& key) const {
        return object_index.HasDynamicReplicationLeaseForKeyForTest(key);
    }
    // Test introspection: hold the route lock exclusively (HA lock-order
    // gating).
    std::unique_lock<std::shared_mutex> LockRouteForTesting() const {
        return object_index.LockRouteForTesting();
    }

    // --- Quota account binding ----------------------------------------------
    void BindQuotaAccount(TenantQuotaHandle handle) {
        quota_account = std::move(handle);
    }
    TenantQuotaHandle BoundQuotaAccount() const { return quota_account; }

    // Restore path: re-account a restored object carrying >=1 completed
    // LOCAL_DISK replica.
    void AccountRestoredDiskObject() { disk_object_count.fetch_add(1); }

    // Rebuild group membership and shared-lease deadlines from object metadata
    // (snapshot / standby restore). Pass 1 aggregates the maximum restored
    // lease deadline per group so a grouped object is not left with a
    // zero-deadline lease, which post-restore cleanup would drop; pass 2
    // re-wires membership and points every grouped entry at the group's
    // shared Lease. Singletons keep their per-object lease.
    void RebuildGroupState() {
        std::unordered_map<std::string, std::chrono::system_clock::time_point>
            max_deadline_by_group;
        auto objs = object_index.SnapshotObjects();
        for (const auto& entry : objs) {
            auto lk = entry->LockShared();
            if (!entry->metadata().IsGrouped()) {
                continue;
            }
            ObjectMetadata& metadata = entry->metadata();
            const auto deadline = metadata.EvictionDeadline();
            auto [it, inserted] =
                max_deadline_by_group.try_emplace(metadata.group_id, deadline);
            if (!inserted) {
                it->second = std::max(it->second, deadline);
            }
        }
        for (const auto& entry : objs) {
            auto lk = entry->LockUnique();
            if (!entry->metadata().IsGrouped()) {
                continue;
            }
            ObjectMetadata& metadata = entry->metadata();
            auto lease = group_index.AddMember(metadata.group_id, entry->key());
            auto it = max_deadline_by_group.find(metadata.group_id);
            if (lease != nullptr) {
                if (it != max_deadline_by_group.end()) {
                    lease->ExtendTo(it->second);
                }
                metadata.SetLease(std::move(lease));
            }
        }
    }

    // Called after adding a LOCAL_DISK replica. Increments
    // disk_object_count if this is the first completed LOCAL_DISK
    // replica for the object (i.e., exactly 1 completed disk replica now).
    void OnDiskReplicaAdded(const ObjectMetadata& metadata) {
        size_t disk_count = metadata.CountReplicas([](const Replica& r) {
            return r.is_local_disk_replica() && r.is_completed();
        });
        if (disk_count == 1) disk_object_count.fetch_add(1);
    }

    // Called after removing a LOCAL_DISK replica, or when erasing an
    // object that had one. Pass had_completed_disk=true if the object
    // had at least one completed LOCAL_DISK replica before the removal.
    // When the entire object is being erased, call the one-arg overload.
    void OnDiskReplicaRemoved(bool had_completed_disk,
                              const ObjectMetadata& metadata) {
        if (!had_completed_disk) return;
        bool still_has_disk = metadata.HasReplica([](const Replica& r) {
            return r.is_local_disk_replica() && r.is_completed();
        });
        if (!still_has_disk) disk_object_count.fetch_sub(1);
    }

    // Overload for full object erasure — no metadata needed.
    void OnDiskReplicaRemoved(bool had_completed_disk) {
        if (had_completed_disk) disk_object_count.fetch_sub(1);
    }

    // Test introspection: the group's shared Lease (nullptr when absent).
    // Lives here, not on GroupIndex, so the catalog owns its boundary.
    std::shared_ptr<Lease> LeaseForTest(const std::string& group_id) const {
        return group_index.LeaseForTest(group_id);
    }

    std::shared_ptr<ObjectEntry> Get(const std::string& key) const {
        return object_index.Get(key);
    }
    // Only erase when the route still resolves to `expected`; see
    // ObjectIndex::EraseIf.
    bool EraseObjectIf(const std::string& key, const ObjectEntry* expected) {
        return object_index.EraseIf(key, expected);
    }
    bool ContainsObject(const std::string& key) const {
        return object_index.Contains(key);
    }
    size_t ObjectCount() const { return object_index.ObjectCount(); }

    std::vector<std::shared_ptr<ObjectEntry>> SnapshotObjects() const {
        return object_index.SnapshotObjects();
    }

   private:
    // Primary per-tenant object index (key -> strong ObjectEntry handle) with
    // a per-object mutation boundary (ObjectEntry::mutex).
    ObjectIndex object_index;

    // GroupIndex: group_id -> shared Lease + member keys.
    GroupIndex group_index;

    // Count of objects with >=1 completed LOCAL_DISK replica; the eviction
    // base is ObjectCount() - disk_object_count.
    std::atomic<long> disk_object_count{0};

    TenantQuotaHandle quota_account{nullptr};

    mutable std::mutex promotion_candidate_keys_mutex_;
    std::unordered_set<std::string> promotion_candidate_keys_;
};

}  // namespace metadata
}  // namespace mooncake
