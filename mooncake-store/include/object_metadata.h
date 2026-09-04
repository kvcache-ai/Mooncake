#pragma once

// Shared object metadata envelope. This is the cache/data envelope that today
// lives nested inside MasterService. It is lifted out so the tenant module
// (mooncake::tenant::ObjectEntry) can hold real metadata without depending on
// MasterService internals.
//
// ObjectMetadata is deliberately NON-copyable / NON-movable and self-locking
// (holds a SpinLock). Callers must own it through a pointer
// (e.g. unique_ptr<ObjectMetadata>), never by value in a resizing container.

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
          soft_pin_timeout(std::move(committed_soft_pin_timeout)),
          hard_pinned(enable_hard_pin),
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

    // Updated by UpsertStart (Case B) to reflect the new writer.
    UUID client_id;
    // Updated by UpsertStart (Case B) to reset the discard timeout.
    std::chrono::system_clock::time_point put_start_time;
    const size_t size;
    std::optional<uint64_t> object_checksum;
    const ObjectDataType data_type{ObjectDataType::UNKNOWN};
    const std::string group_id;
    const TenantId tenant_id;
    const std::string user_key;

    mutable SpinLock lock;
    // Authoritative lease: ungrouped objects own one; grouped objects share
    // the group's. Never null after construction.
    mutable std::shared_ptr<Lease> lease_ GUARDED_BY(lock) =
        std::make_shared<Lease>();
    mutable std::optional<std::chrono::system_clock::time_point>
        soft_pin_timeout GUARDED_BY(lock);  // committed object soft-pin
                                            // deadline
    // Replica IDs scope this action to the current write. PutEnd does not
    // carry a generation token, so a stale End from the same client cannot
    // otherwise be distinguished from the current write.
    std::optional<PendingSoftPinAction> pending_soft_pin_action;
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
    std::chrono::steady_clock::time_point dynamic_replication_recreate_after{};

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
        replicas_.insert(replicas_.end(), std::move_iterator(replicas.begin()),
                         std::move_iterator(replicas.end()));
    }

    std::vector<Replica> PopReplicas(
        const std::function<bool(const Replica&)>& pred_fn) {
        auto partition_point = std::partition(
            replicas_.begin(), replicas_.end(),
            [pred_fn](const Replica& replica) { return !pred_fn(replica); });

        std::vector<Replica> popped_replicas;
        if (partition_point != replicas_.end()) {
            popped_replicas.reserve(
                std::distance(partition_point, replicas_.end()));
            std::move(partition_point, replicas_.end(),
                      std::back_inserter(popped_replicas));
            replicas_.erase(partition_point, replicas_.end());
        }

        return popped_replicas;
    }

    std::vector<Replica> PopReplicas() { return std::move(replicas_); }

    size_t EraseReplicas(const std::function<bool(const Replica&)>& pred_fn) {
        auto erased_replicas = PopReplicas(pred_fn);
        return erased_replicas.size();
    }

    size_t EraseReplicas() {
        auto erased_replicas = PopReplicas();
        return erased_replicas.size();
    }

    size_t VisitReplicas(const std::function<bool(const Replica&)>& pred_fn,
                         const std::function<void(Replica&)>& visit_fn) {
        size_t num_visited = 0;

        for (auto& replica : replicas_) {
            if (pred_fn(replica)) {
                visit_fn(replica);
                num_visited++;
            }
        }

        return num_visited;
    }

    size_t VisitReplicas(
        const std::function<bool(const Replica&)>& pred_fn,
        const std::function<void(const Replica&)>& visit_fn) const {
        size_t num_visited = 0;

        for (auto& replica : replicas_) {
            if (pred_fn(replica)) {
                visit_fn(replica);
                num_visited++;
            }
        }

        return num_visited;
    }

    bool HasReplica(const std::function<bool(const Replica&)>& pred_fn) const {
        return std::any_of(replicas_.begin(), replicas_.end(), pred_fn);
    }

    bool AllReplicas(const std::function<bool(const Replica&)>& pred_fn) const {
        return std::all_of(replicas_.begin(), replicas_.end(), pred_fn);
    }

    size_t CountReplicas(
        const std::function<bool(const Replica&)>& pred_fn) const {
        return std::count_if(replicas_.begin(), replicas_.end(), pred_fn);
    }

    size_t CountReplicas() const { return replicas_.size(); }

    const std::vector<Replica>& GetAllReplicas() const { return replicas_; }

    std::optional<ReplicaStatus> HasDiffRepStatus(ReplicaStatus status) const {
        for (const auto& replica : replicas_) {
            if (replica.status() != status) {
                return replica.status();
            }
        }
        return {};
    }

    Replica* GetFirstReplica(
        const std::function<bool(const Replica&)>& pred_fn) {
        const auto it =
            std::find_if(replicas_.begin(), replicas_.end(), pred_fn);
        return it != replicas_.end() ? &(*it) : nullptr;
    }

    Replica* GetReplicaByID(const ReplicaID& id) {
        return GetFirstReplica(
            [&id](const Replica& replica) { return replica.id() == id; });
    }

    bool EraseReplicaByID(const ReplicaID& id) {
        auto num_erased = EraseReplicas(
            [&id](const Replica& replica) { return replica.id() == id; });
        return num_erased > 0;
    }

    std::size_t EraseReplica(ReplicaType replica_type) {
        return EraseReplicas([replica_type](const Replica& replica) {
            if (replica_type == ReplicaType::ALL) {
                return replica.is_memory_replica() ||
                       replica.is_nof_replica() || replica.is_dfs_replica();
            }
            return replica.type() == replica_type;
        });
    }

    bool HasMemReplica() const {
        return HasReplica(&Replica::fn_is_memory_replica);
    }

    bool HasNoFReplica() const {
        return HasReplica(&Replica::fn_is_nof_replica);
    }

    size_t GetMemReplicaCount() const {
        return CountReplicas(&Replica::fn_is_memory_replica);
    }

    size_t GetNoFReplicaCount() const {
        return CountReplicas(&Replica::fn_is_nof_replica);
    }

    Replica* GetReplicaBySegmentName(const std::string& segment_name) {
        return GetFirstReplica([&segment_name](const Replica& replica) {
            auto names = replica.get_segment_names();
            for (auto& name_opt : names) {
                if (name_opt == segment_name) {
                    return true;
                }
            }
            return false;
        });
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

    bool IsLeaseExpired(std::chrono::system_clock::time_point& now) const {
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
        const bool has_viable_replica = std::any_of(
            replicas_.begin(), replicas_.end(),
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
        std::vector<std::string> segment_names;
        for (const auto& replica : replicas_) {
            const auto& segment_name_options = replica.get_segment_names();
            for (const auto& segment_name_opt : segment_name_options) {
                if (segment_name_opt.has_value()) {
                    segment_names.push_back(segment_name_opt.value());
                }
            }
        }
        return segment_names;
    }

   private:
    // Use the accessors to visit and modify the replicas.
    std::vector<Replica> replicas_;
};

}  // namespace mooncake
