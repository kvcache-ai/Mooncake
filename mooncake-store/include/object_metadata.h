#pragma once

// Shared object metadata envelope: identity, replica set, lease and soft-pin
// state for one object. Owned through a pointer by the tenant module
// (mooncake::metadata::ObjectEntry).
//
// NON-copyable / NON-movable and self-locking (holds a SpinLock); always own
// it through a pointer (e.g. unique_ptr<ObjectMetadata>), never by value.

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <functional>
#include <iterator>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "lease.h"
#include "master_metric_manager.h"
#include "object_replica_list.h"
#include "mutex.h"
#include "replica.h"
#include "tenant/tenant_id.h"
#include "tenant_quota_ledger.h"
#include "types.h"

namespace mooncake {

struct ResolvedSoftPinRequest {
    SoftPinAction action{SoftPinAction::PRESERVE};
    uint64_t ttl_ms{0};
};

class ObjectMetadata {
   public:
    struct SoftPinEvaluation {
        bool active{false};
        int metric_delta{0};
        std::optional<std::chrono::system_clock::time_point> removed_deadline;
        std::optional<std::chrono::system_clock::time_point> deadline_to_index;
    };

    struct PendingSoftPinAction {
        SoftPinAction action{SoftPinAction::PRESERVE};
        uint64_t ttl_ms{0};
        std::vector<ReplicaID> eligible_replica_ids;
    };

    // RAII-style metric management
    ~ObjectMetadata() {
        MasterMetricManager::instance().dec_key_count(1);
        if (soft_pin_timeout) {
            MasterMetricManager::instance().dec_soft_pin_key_count(1);
        }
    }

    ObjectMetadata() = delete;

    ObjectMetadata(const UUID& client_id_,
                   const std::chrono::system_clock::time_point put_start_time_,
                   size_t value_length, std::vector<Replica>&& reps,
                   std::optional<std::chrono::system_clock::time_point>
                       committed_soft_pin_timeout = std::nullopt,
                   bool enable_hard_pin = false,
                   ObjectDataType data_type_ = ObjectDataType::UNKNOWN,
                   std::string group_id_ = "", TenantId tenant_id_ = TenantId(),
                   std::string user_key_ = {})
        : client_id(client_id_),
          put_start_time(put_start_time_),
          size(value_length),
          data_type(data_type_),
          group_id(std::move(group_id_)),
          tenant_id(std::move(tenant_id_)),
          user_key(std::move(user_key_)),
          hard_pinned(enable_hard_pin),
          soft_pin_timeout(std::move(committed_soft_pin_timeout)),
          replicas_(std::move(reps)) {
        MasterMetricManager::instance().inc_key_count(1);
        if (soft_pin_timeout) {
            MasterMetricManager::instance().inc_soft_pin_key_count(1);
        }
        MasterMetricManager::instance().observe_value_size(value_length);
    }

    ObjectMetadata(const ObjectMetadata&) = delete;
    ObjectMetadata& operator=(const ObjectMetadata&) = delete;
    ObjectMetadata(ObjectMetadata&&) = delete;
    ObjectMetadata& operator=(ObjectMetadata&&) = delete;

    // The current writer; rewritten when an in-place same-size upsert
    // reuses this object's buffers.
    UUID client_id;
    // Start of the current write; reset on the same in-place update so the
    // discard timeout restarts.
    std::chrono::system_clock::time_point put_start_time;
    const size_t size;
    std::optional<uint64_t> object_checksum;
    const ObjectDataType data_type{ObjectDataType::UNKNOWN};
    const std::string group_id;
    const TenantId tenant_id;
    const std::string user_key;

    const bool hard_pinned{false};  // immutable, set at creation
    bool memory_cache_total_accounted{false};
    bool disk_cache_total_accounted{false};
    TenantQuotaLedger quota_ledger;

    struct DynamicReplicaRecord {
        std::chrono::system_clock::time_point created_at;
        std::string source_segment;
        std::string target_segment;
        std::string target_domain;
        bool complete{false};
    };

    std::unordered_map<ReplicaID, DynamicReplicaRecord> dynamic_replicas;

    void MarkDynamicReplica(ReplicaID replica_id, DynamicReplicaRecord record) {
        dynamic_replicas[replica_id] = std::move(record);
    }

    void MarkDynamicReplicasComplete(
        const std::vector<ReplicaID>& replica_ids) {
        for (const auto& replica_id : replica_ids) {
            auto it = dynamic_replicas.find(replica_id);
            if (it != dynamic_replicas.end()) {
                it->second.complete = true;
            }
        }
    }

    size_t ForgetDynamicReplicas(const std::vector<ReplicaID>& replica_ids) {
        size_t forgotten = 0;
        for (const auto& replica_id : replica_ids) {
            forgotten += dynamic_replicas.erase(replica_id);
        }
        return forgotten;
    }

    size_t DynamicReplicaCount() const { return dynamic_replicas.size(); }

    bool DynamicReplicationRecreateBlocked(
        std::chrono::steady_clock::time_point now) const {
        return now < dynamic_replication_recreate_after;
    }

    void SetDynamicReplicationRecreateAfter(
        std::chrono::steady_clock::time_point deadline) {
        dynamic_replication_recreate_after =
            std::max(dynamic_replication_recreate_after, deadline);
    }

    void AddReplicas(std::vector<Replica>&& replicas) {
        replicas_.AddReplicas(std::move(replicas));
    }

    std::vector<Replica> PopReplicas(
        const std::function<bool(const Replica&)>& pred_fn) {
        return replicas_.PopReplicas(pred_fn);
    }

    std::vector<Replica> PopReplicas() { return replicas_.PopReplicas(); }

    size_t EraseReplicas(const std::function<bool(const Replica&)>& pred_fn) {
        return replicas_.EraseReplicas(pred_fn);
    }

    size_t EraseReplicas() { return replicas_.EraseReplicas(); }

    size_t VisitReplicas(const std::function<bool(const Replica&)>& pred_fn,
                         const std::function<void(Replica&)>& visit_fn) {
        return replicas_.VisitReplicas(pred_fn, visit_fn);
    }

    size_t VisitReplicas(
        const std::function<bool(const Replica&)>& pred_fn,
        const std::function<void(const Replica&)>& visit_fn) const {
        return replicas_.VisitReplicas(pred_fn, visit_fn);
    }

    bool HasReplica(const std::function<bool(const Replica&)>& pred_fn) const {
        return replicas_.HasReplica(pred_fn);
    }

    bool AllReplicas(const std::function<bool(const Replica&)>& pred_fn) const {
        return replicas_.AllReplicas(pred_fn);
    }

    size_t CountReplicas(
        const std::function<bool(const Replica&)>& pred_fn) const {
        return replicas_.CountReplicas(pred_fn);
    }

    size_t CountReplicas() const { return replicas_.CountReplicas(); }

    const std::vector<Replica>& GetAllReplicas() const {
        return replicas_.GetAllReplicas();
    }

    std::optional<ReplicaStatus> HasDiffRepStatus(ReplicaStatus status) const {
        return replicas_.HasDiffRepStatus(status);
    }

    Replica* GetFirstReplica(
        const std::function<bool(const Replica&)>& pred_fn) {
        return replicas_.GetFirstReplica(pred_fn);
    }

    Replica* GetReplicaByID(const ReplicaID& id) {
        return replicas_.GetReplicaByID(id);
    }

    Replica* GetReplicaBySegmentName(const std::string& segment_name) {
        return replicas_.GetReplicaBySegmentName(segment_name);
    }

    // Grant a read lease at now() + ttl. Grouped objects extend the shared
    // group TTL; a zero ttl is a no-op on a live lease (PutEnd cannot
    // expire a live group).
    void GrantReadLease(std::chrono::milliseconds ttl) const {
        SpinLocker locker(&lock);
        lease_->GrantReadLease(ttl);
    }

    // Replace the lease (used to point a grouped object at the group's shared
    // Lease). Takes the same lock as the other lease accessors.
    void SetLease(std::shared_ptr<Lease> lease) const {
        SpinLocker locker(&lock);
        lease_ = std::move(lease);
    }

    // Snapshot of the wired lease; never null (initialized at construction).
    // Takes the same lock as the other lease accessors.
    std::shared_ptr<Lease> lease() const {
        SpinLocker locker(&lock);
        return lease_;
    }

    // Extend the lease deadline (restore path). Locked like the other
    // accessors.
    void ExtendLeaseDeadline(
        std::chrono::system_clock::time_point deadline) const {
        SpinLocker locker(&lock);
        lease_->ExtendTo(deadline);
    }

    bool IsLeaseExpired() const {
        SpinLocker locker(&lock);
        return lease_->IsExpired(std::chrono::system_clock::now());
    }

    // Test/benchmark hook: overwrite the lease deadline outright instead of
    // extending it. Takes `lock` like the other lease accessors.
    void SetLeaseDeadlineForTesting(
        std::chrono::system_clock::time_point deadline) const {
        SpinLocker locker(&lock);
        lease_->SetDeadline(deadline);
    }

    bool IsLeaseExpired(
        const std::chrono::system_clock::time_point& now) const {
        SpinLocker locker(&lock);
        return lease_->IsExpired(now);
    }

    // Lease deadline for the eviction census.
    std::chrono::system_clock::time_point EvictionDeadline() const {
        SpinLocker locker(&lock);
        return lease_->ExpiresAt();
    }

    SoftPinEvaluation EvaluateSoftPin(
        const std::chrono::system_clock::time_point& now) const {
        SpinLocker locker(&lock);
        if (soft_pin_timeout && now >= *soft_pin_timeout) {
            const auto removed_deadline = *soft_pin_timeout;
            soft_pin_timeout.reset();
            return {.active = false,
                    .metric_delta = -1,
                    .removed_deadline = removed_deadline,
                    .deadline_to_index = std::nullopt};
        }
        return {.active = soft_pin_timeout.has_value(),
                .metric_delta = 0,
                .removed_deadline = std::nullopt,
                .deadline_to_index = std::nullopt};
    }

    bool ExpireSoftPinIfDeadlineMatches(
        const std::chrono::system_clock::time_point& expected_deadline,
        const std::chrono::system_clock::time_point& now) const {
        SpinLocker locker(&lock);
        if (!soft_pin_timeout || *soft_pin_timeout != expected_deadline ||
            now < expected_deadline) {
            return false;
        }
        soft_pin_timeout.reset();
        return true;
    }

    static std::chrono::system_clock::time_point ComputeSoftPinDeadline(
        const std::chrono::system_clock::time_point& now, uint64_t ttl_ms) {
        using Milliseconds = std::chrono::milliseconds;
        using MillisecondsRep = Milliseconds::rep;
        const auto max_time = std::chrono::system_clock::time_point::max();
        if (ttl_ms > static_cast<uint64_t>(
                         std::numeric_limits<MillisecondsRep>::max())) {
            return max_time;
        }
        const auto remaining_ms =
            std::chrono::duration_cast<Milliseconds>(max_time - now).count();
        if (remaining_ms < 0 || ttl_ms > static_cast<uint64_t>(remaining_ms)) {
            return max_time;
        }
        const auto ttl = Milliseconds(static_cast<MillisecondsRep>(ttl_ms));
        return now + ttl;
    }

    std::optional<std::chrono::system_clock::time_point>
    GetCommittedSoftPinTimeout() const {
        SpinLocker locker(&lock);
        return soft_pin_timeout;
    }

    // Test hook (eviction DSL and fixtures): overwrite the committed soft-pin
    // deadline directly, bypassing the Begin/Commit state machine and its
    // metric accounting. Takes `lock` like the other accessors.
    void SetCommittedSoftPinTimeoutForTesting(
        std::optional<std::chrono::system_clock::time_point> deadline) const {
        SpinLocker locker(&lock);
        soft_pin_timeout = std::move(deadline);
    }

    void BeginSoftPinAction(const ResolvedSoftPinRequest& request,
                            std::vector<ReplicaID> eligible_replica_ids) {
        pending_soft_pin_action = PendingSoftPinAction{
            request.action, request.ttl_ms, std::move(eligible_replica_ids)};
    }

    bool PendingSoftPinOwnsReplica(ReplicaID replica_id) const {
        if (!pending_soft_pin_action) {
            return false;
        }
        const auto& eligible = pending_soft_pin_action->eligible_replica_ids;
        return std::find(eligible.begin(), eligible.end(), replica_id) !=
               eligible.end();
    }

    void ClearPendingSoftPinAction() { pending_soft_pin_action.reset(); }

    SoftPinEvaluation CommitPendingSoftPin(
        const std::chrono::system_clock::time_point& now) {
        if (!pending_soft_pin_action) {
            return EvaluateSoftPin(now);
        }

        const PendingSoftPinAction pending =
            std::move(*pending_soft_pin_action);
        pending_soft_pin_action.reset();

        SpinLocker locker(&lock);
        int metric_delta = 0;
        std::optional<std::chrono::system_clock::time_point> removed_deadline;
        std::optional<std::chrono::system_clock::time_point> deadline_to_index;
        if (soft_pin_timeout && now >= *soft_pin_timeout) {
            removed_deadline = *soft_pin_timeout;
            soft_pin_timeout.reset();
            --metric_delta;
        }

        switch (pending.action) {
            case SoftPinAction::PRESERVE:
                break;
            case SoftPinAction::ENABLE:
                if (pending.ttl_ms == 0) {
                    if (soft_pin_timeout) {
                        removed_deadline = *soft_pin_timeout;
                        soft_pin_timeout.reset();
                        --metric_delta;
                    }
                } else {
                    if (!soft_pin_timeout) {
                        ++metric_delta;
                    }
                    soft_pin_timeout =
                        ComputeSoftPinDeadline(now, pending.ttl_ms);
                    deadline_to_index = soft_pin_timeout;
                    // Upserting the latest registration supersedes any
                    // expired or previously active deadline.
                    removed_deadline.reset();
                }
                break;
            case SoftPinAction::DISABLE:
                if (soft_pin_timeout) {
                    removed_deadline = *soft_pin_timeout;
                    soft_pin_timeout.reset();
                    --metric_delta;
                }
                break;
        }
        return {.active = soft_pin_timeout.has_value(),
                .metric_delta = metric_delta,
                .removed_deadline = removed_deadline,
                .deadline_to_index = deadline_to_index};
    }

    void ClearPendingSoftPinIfNoViableReplica() {
        if (!pending_soft_pin_action) {
            return;
        }
        const auto& eligible = pending_soft_pin_action->eligible_replica_ids;
        const auto& replicas = replicas_.GetAllReplicas();
        const bool has_viable_replica = std::any_of(
            replicas.begin(), replicas.end(),
            [&eligible](const Replica& replica) {
                const bool belongs_to_write =
                    std::find(eligible.begin(), eligible.end(), replica.id()) !=
                    eligible.end();
                const bool valid_handle = !replica.has_invalid_mem_handle() &&
                                          !replica.has_invalid_nof_handle();
                return belongs_to_write && replica.is_processing() &&
                       valid_handle;
            });
        if (!has_viable_replica) {
            pending_soft_pin_action.reset();
        }
    }

    bool IsHardPinned() const { return hard_pinned; }

    bool IsGrouped() const { return !group_id.empty(); }

    // Valid: >0 size and at least one valid replica.
    bool IsValid() const {
        return size > 0 && HasReplica([](const Replica& replica) {
                   return !replica.is_memory_replica() ||
                          !replica.has_invalid_mem_handle();
               });
    }

    std::vector<std::string> GetReplicaSegmentNames() const {
        return replicas_.GetReplicaSegmentNames();
    }

   private:
    // Guards the enclosed lease/soft-pin state below. A path that touches
    // both this and the owning ObjectEntry takes the entry mutex first and
    // this lock second (never the reverse).
    mutable SpinLock lock;
    // Authoritative lease: ungrouped objects own one; grouped objects share
    // the group's. Never null after construction. Reached only through the
    // lease accessors above.
    mutable std::shared_ptr<Lease> lease_ GUARDED_BY(lock) =
        std::make_shared<Lease>();
    // Committed object soft-pin deadline. Mutated only through the soft-pin
    // accessors above, always under `lock`; kept out of the public API so the
    // GUARDED_BY contract cannot be bypassed.
    mutable std::optional<std::chrono::system_clock::time_point>
        soft_pin_timeout GUARDED_BY(lock);  // committed object soft-pin
                                            // deadline
    // Replica IDs scope a pending soft-pin action to the current write.
    // PutEnd does not carry a generation token, so a stale End from the same
    // client cannot otherwise be distinguished from the current write.
    // Only touched through the Begin/Commit/Clear methods above.
    std::optional<PendingSoftPinAction> pending_soft_pin_action;
    // Dynamic-replication recreate backoff deadline, reachable only through
    // DynamicReplicationRecreateBlocked/SetDynamicReplicationRecreateAfter.
    std::chrono::steady_clock::time_point dynamic_replication_recreate_after{};
    // Use the accessors to visit and modify the replicas.
    ObjectReplicaList replicas_;
};

}  // namespace mooncake
