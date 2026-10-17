#pragma once

// ObjectMetadata: the per-object metadata envelope.
//
// Moved out of MasterService so the service and the per-object metadata
// module share one definition. The envelope owns the object identity, the
// authoritative read lease, the soft-pin state and the tenant quota ledger.
// The object's content and the replicas holding it live in its parts.
//
// It has no ownership lock: whoever owns the envelope decides how the
// envelope as a whole is guarded. The `lock` member below is internal and
// narrower — it protects only `lease_` and `soft_pin_timeout`, so a holder of
// the envelope can read either without them tearing.

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <functional>
#include <iterator>
#include <memory>
#include <optional>
#include <ranges>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "lease.h"
#include "master_metric_manager.h"
#include "mutex.h"
#include "object_part.h"
#include "replica.h"
#include "tenant_id.h"
#include "tenant_quota_ledger.h"
#include "types.h"

namespace mooncake {

struct ResolvedSoftPinRequest {
    SoftPinAction action{SoftPinAction::PRESERVE};
    std::chrono::milliseconds ttl{0};
};

struct ObjectMetadata {
    struct SoftPinEvaluation {
        bool active{false};
        int metric_delta{0};
        std::optional<std::chrono::system_clock::time_point> removed_deadline;
        std::optional<std::chrono::system_clock::time_point> deadline_to_index;
    };

    struct PendingSoftPinAction {
        SoftPinAction action{SoftPinAction::PRESERVE};
        std::chrono::milliseconds ttl{0};
        std::unordered_set<ReplicaID> eligible_replica_ids;
    };

    // The most recent write attempt accepted for a part. The record outlives
    // the attempt, which is how a late PutEnd or PutRevoke is told apart from
    // the writer that holds the part now.
    struct PartWriteRecord {
        UUID client_id;
        std::chrono::system_clock::time_point put_start_time;
    };

    // One part and the write that fills it. The slot count is the object's
    // part_count, fixed when the object is created.
    struct PartSlot {
        std::optional<ObjectPart> part;
        std::optional<PartWriteRecord> write;
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
                   std::string user_key_ = {}, uint32_t part_index = 0,
                   uint32_t part_count = 1)
        : data_type(data_type_),
          group_id(std::move(group_id_)),
          tenant_id(std::move(tenant_id_)),
          user_key(std::move(user_key_)),
          soft_pin_timeout(std::move(committed_soft_pin_timeout)),
          hard_pinned(enable_hard_pin) {
        slots_.resize(part_count);
        BeginPart(part_index, value_length, std::move(reps));
        BeginPartWrite(part_index, client_id_, put_start_time_);
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
    // Set once every part held a readable replica. An object that never got
    // there is an artefact of writers that gave up; one that did is cached
    // content that later lost a part, and its parts can be written again.
    bool was_readable{false};
    // Until when a lost part may be written again. Held by the incompleteness
    // sweep, which opens it when it sees a readable object go unreadable.
    std::optional<std::chrono::system_clock::time_point> repair_deadline;
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

    size_t PartCount() const { return slots_.size(); }

    // nullptr when the slot holds no part yet. A pointer taken from a part is
    // invalidated by any add or pop on it.
    const ObjectPart* GetPart(size_t index) const {
        const PartSlot& slot = slots_.at(index);
        return slot.part.has_value() ? &slot.part.value() : nullptr;
    }

    ObjectPart* MutablePart(size_t index) {
        PartSlot& slot = slots_.at(index);
        return slot.part.has_value() ? &slot.part.value() : nullptr;
    }

    // nullptr when no PutStart for this part has arrived yet.
    const PartWriteRecord* GetPartWriteRecord(size_t index) const {
        const PartSlot& slot = slots_.at(index);
        return slot.write.has_value() ? &slot.write.value() : nullptr;
    }

    // True while some part holds a replica a write attempt has not finished
    // with. A part can hold a finished replica and one of another type that is
    // still being written, so the attempt is over only when no replica is
    // PROCESSING.
    bool HasWriteInFlight() const {
        for (const auto& slot : slots_) {
            if (slot.write.has_value() && slot.part.has_value() &&
                slot.part->HasProcessingReplica()) {
                return true;
            }
        }
        return false;
    }

    // The latest PutStart this object received, over its parts: the anchor of
    // the object-level incompleteness timeout.
    std::chrono::system_clock::time_point LastPutStartTime() const {
        std::chrono::system_clock::time_point latest{};
        for (const auto& slot : slots_) {
            if (slot.write.has_value() && slot.write->put_start_time > latest) {
                latest = slot.write->put_start_time;
            }
        }
        return latest;
    }

    // A slot that held a part whose write timed out is empty again by the time
    // its next writer arrives, so the part is built fresh instead of resized.
    void BeginPart(size_t index, size_t length,
                   std::vector<Replica>&& replicas) {
        slots_.at(index).part.emplace(length, std::move(replicas));
    }

    // Opens a write attempt on the part, replacing any record of an earlier
    // one held there.
    void BeginPartWrite(size_t index, const UUID& client_id,
                        std::chrono::system_clock::time_point now) {
        slots_.at(index).write.emplace(PartWriteRecord{client_id, now});
    }

    // The attempt is over: the record goes, the part keeps the replicas it
    // still holds, and a part left with none goes back to empty so a later
    // writer starts fresh instead of inheriting it.
    void ClearPartWrite(size_t index) {
        PartSlot& slot = slots_.at(index);
        slot.write.reset();
        if (slot.part.has_value() && slot.part->replicas().empty()) {
            slot.part.reset();
        }
    }

    // No slot holds a part: the envelope can be dropped.
    bool IsEmpty() const {
        return std::ranges::none_of(
            slots_, [](const PartSlot& slot) { return slot.part.has_value(); });
    }

    // Whether any part still holds a replica: an object with none has no
    // content a repair writer could keep.
    bool HoldsAnyReplica() const {
        for (const auto& slot : slots_) {
            if (slot.part.has_value() && !slot.part->replicas().empty()) {
                return true;
            }
        }
        return false;
    }

    // The object became readable, which it may never be again.
    void MarkReadable() { was_readable = true; }

    bool WasEverReadable() const { return was_readable; }

    // How long a writer has to rewrite a part an object that was readable lost.
    // The sweep opens the window the first time it sees the object unreadable
    // and clears it once the object is readable again, so a repair that never
    // comes leaves the object to be reclaimed whole.
    void OpenRepairWindow(std::chrono::system_clock::time_point deadline) {
        repair_deadline = deadline;
    }

    void CloseRepairWindow() { repair_deadline.reset(); }

    bool HasRepairWindow() const { return repair_deadline.has_value(); }

    bool RepairWindowHeld(std::chrono::system_clock::time_point now) const {
        return repair_deadline.has_value() && now < repair_deadline.value();
    }

    // The length of the object's content: the sum over its parts.
    size_t TotalLength() const {
        size_t total = 0;
        for (const auto& slot : slots_) {
            if (slot.part.has_value()) {
                total += slot.part->length();
            }
        }
        return total;
    }

    // Takes the matching replicas out of every part, for the operations that
    // dispose of the object as a whole. A caller that means one part pops from
    // that part directly.
    std::vector<Replica> PopAllReplicas(
        const std::function<bool(const Replica&)>& pred_fn) {
        std::vector<Replica> popped;
        for (auto& slot : slots_) {
            if (!slot.part.has_value()) {
                continue;
            }
            auto replicas = slot.part->PopReplicas(pred_fn);
            popped.insert(popped.end(),
                          std::make_move_iterator(replicas.begin()),
                          std::make_move_iterator(replicas.end()));
        }
        return popped;
    }

    std::vector<Replica> PopAllReplicas() {
        std::vector<Replica> popped;
        for (auto& slot : slots_) {
            if (!slot.part.has_value()) {
                continue;
            }
            auto replicas = slot.part->PopReplicas();
            popped.insert(popped.end(),
                          std::make_move_iterator(replicas.begin()),
                          std::make_move_iterator(replicas.end()));
        }
        return popped;
    }

    // The queries below answer over every part the object has written; a caller
    // that means one part reads it through GetPart / MutablePart.
    bool HasReplica(const std::function<bool(const Replica&)>& pred_fn) const {
        for (const auto& slot : slots_) {
            if (slot.part.has_value() &&
                std::ranges::any_of(slot.part->replicas(), pred_fn)) {
                return true;
            }
        }
        return false;
    }

    bool AllReplicas(const std::function<bool(const Replica&)>& pred_fn) const {
        for (const auto& slot : slots_) {
            if (slot.part.has_value() &&
                !std::ranges::all_of(slot.part->replicas(), pred_fn)) {
                return false;
            }
        }
        return true;
    }

    size_t CountReplicas(
        const std::function<bool(const Replica&)>& pred_fn) const {
        size_t count = 0;
        for (const auto& slot : slots_) {
            if (slot.part.has_value()) {
                count += static_cast<size_t>(
                    std::ranges::count_if(slot.part->replicas(), pred_fn));
            }
        }
        return count;
    }

    size_t CountReplicas() const {
        size_t count = 0;
        for (const auto& slot : slots_) {
            if (slot.part.has_value()) {
                count += slot.part->replicas().size();
            }
        }
        return count;
    }

    Replica* GetFirstReplica(
        const std::function<bool(const Replica&)>& pred_fn) {
        for (auto& slot : slots_) {
            if (!slot.part.has_value()) {
                continue;
            }
            auto replicas = slot.part->replicas();
            const auto it = std::ranges::find_if(replicas, pred_fn);
            if (it != replicas.end()) {
                return &(*it);
            }
        }
        return nullptr;
    }

    Replica* GetReplicaByID(const ReplicaID& id) {
        return GetFirstReplica(
            [&id](const Replica& replica) { return replica.id() == id; });
    }

    Replica* GetReplicaBySegmentName(const std::string& segment_name) {
        return GetFirstReplica([&segment_name](const Replica& replica) {
            const auto names = replica.get_segment_names();
            return std::ranges::any_of(names,
                                       [&segment_name](const auto& name_opt) {
                                           return name_opt == segment_name;
                                       });
        });
    }

    // Grant a read lease at now() + ttl. Grouped objects extend the shared
    // group TTL; a zero ttl is a no-op on a live lease (PutEnd cannot
    // expire a live group).
    void GrantReadLease(std::chrono::milliseconds ttl) const {
        SpinLocker locker(&lock);
        lease_->GrantReadLease(ttl);
    }

    // Whether the authoritative lease is expired.
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

    // Saturates at the clock's maximum rather than overflowing when the TTL
    // reaches past it. The comparison is done in milliseconds because
    // converting a huge TTL up to the clock's finer period would overflow.
    static std::chrono::system_clock::time_point ComputeSoftPinDeadline(
        const std::chrono::system_clock::time_point& now,
        std::chrono::milliseconds ttl) {
        const auto max_time = std::chrono::system_clock::time_point::max();
        const auto headroom = max_time - now;
        const auto remaining =
            std::chrono::duration_cast<std::chrono::milliseconds>(headroom);
        return ttl > remaining ? max_time : now + ttl;
    }

    std::optional<std::chrono::system_clock::time_point>
    GetCommittedSoftPinTimeout() const {
        SpinLocker locker(&lock);
        return soft_pin_timeout;
    }

    void BeginSoftPinAction(
        const ResolvedSoftPinRequest& request,
        std::unordered_set<ReplicaID> eligible_replica_ids) {
        pending_soft_pin_action = PendingSoftPinAction{
            request.action, request.ttl, std::move(eligible_replica_ids)};
    }

    bool PendingSoftPinOwnsReplica(ReplicaID replica_id) const {
        if (!pending_soft_pin_action) {
            return false;
        }
        return pending_soft_pin_action->eligible_replica_ids.contains(
            replica_id);
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
                if (pending.ttl == std::chrono::milliseconds::zero()) {
                    if (soft_pin_timeout) {
                        removed_deadline = *soft_pin_timeout;
                        soft_pin_timeout.reset();
                        --metric_delta;
                    }
                } else {
                    if (!soft_pin_timeout) {
                        ++metric_delta;
                    }
                    soft_pin_timeout = ComputeSoftPinDeadline(now, pending.ttl);
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
        const bool has_viable_replica =
            HasReplica([&eligible](const Replica& replica) {
                const bool belongs_to_write = eligible.contains(replica.id());
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

    // Readable when every part the object has written holds a replica the
    // caller's rule accepts, and no part is still missing.
    template <typename IsReplicaReadable>
    bool IsReadable(IsReplicaReadable&& is_readable) const {
        if (slots_.empty()) {
            return false;
        }
        for (const auto& slot : slots_) {
            if (!slot.part.has_value() ||
                !slot.part->HasReadableReplica(is_readable)) {
                return false;
            }
        }
        return true;
    }

    // Whether the object still holds content a caller must keep it for: some
    // part has a length and a replica whose handle is still usable. Readability
    // is a different question (MasterService::IsReadable).
    bool IsValid() const {
        return TotalLength() > 0 && HasReplica([](const Replica& replica) {
                   return !replica.is_memory_replica() ||
                          !replica.has_invalid_mem_handle();
               });
    }

   private:
    std::vector<PartSlot> slots_;
};

}  // namespace mooncake
