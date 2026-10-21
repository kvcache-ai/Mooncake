#pragma once

// ObjectMetadata: the per-object metadata envelope.
//
// Moved out of MasterService so the service and the per-object metadata
// module share one definition. The envelope owns the object identity, the
// authoritative read lease, the soft-pin state and the tenant quota ledger.
// The object's content lives in its slots: one slot per part, fixed in number
// when the object is created, each holding that part's ObjectPart and the
// record of the write that produced it.
//
// It has no ownership lock of its own: whoever owns the envelope decides how
// the envelope as a whole is guarded. The read path holds the owning metadata
// shard's shared lock and every write path its exclusive lock, so a part's
// content and its write record need no lock of their own. The `lock` member
// below is internal and narrower — it protects only `lease_` and
// `soft_pin_timeout`.

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <ranges>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <glog/logging.h>
#include <ylt/util/tl/expected.hpp>

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

// The write that produced one part's content: who started it, when, and which
// replicas it handed out. It outlives the write, because a later PutEnd or
// PutRevoke is matched against the attempt current for its part and the
// object-level timeouts are measured from the newest one.
struct WriteAttempt {
    UUID writer;
    std::chrono::system_clock::time_point started;
    std::vector<ReplicaID> replica_ids;
};

// One part of the object. `part` is that part's content, absent until the part
// is first written and again once every replica of it is gone; `attempt` is
// the most recent write of that part, which the next write replaces.
struct PartSlot {
    std::optional<ObjectPart> part;
    std::optional<WriteAttempt> attempt;
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

    // RAII-style metric management
    ~ObjectMetadata() {
        MasterMetricManager::instance().dec_key_count(1);
        if (soft_pin_timeout) {
            MasterMetricManager::instance().dec_soft_pin_key_count(1);
        }
    }

    ObjectMetadata() = delete;

    // The envelope of an object of `part_count` parts. Every part starts empty:
    // content arrives through BeginWrite, which is also what opens the write a
    // later End or Revoke is scoped to.
    ObjectMetadata(
        size_t part_count, TenantId tenant_id_, std::string user_key_,
        std::string group_id_, ObjectDataType data_type_, bool hard_pinned_,
        std::optional<std::chrono::system_clock::time_point> soft_pin_deadline_)
        : data_type(data_type_),
          group_id(std::move(group_id_)),
          tenant_id(std::move(tenant_id_)),
          user_key(std::move(user_key_)),
          soft_pin_timeout(std::move(soft_pin_deadline_)),
          hard_pinned(hard_pinned_),
          part_count_(part_count) {
        if (part_count == 0) {
            LOG(FATAL) << "An object has at least one part, got " << part_count;
        }
        MasterMetricManager::instance().inc_key_count(1);
        if (soft_pin_timeout) {
            MasterMetricManager::instance().inc_soft_pin_key_count(1);
        }
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

    // --- Reading the slots. Caller holds the owning shard's shared lock. ---

    size_t PartCount() const { return part_count_; }

    // That part's content, or null when the part holds none.
    const ObjectPart* GetPart(size_t index) const {
        const PartSlot* slot = FindSlot(index);
        if (slot == nullptr || !slot->part.has_value()) {
            return nullptr;
        }
        return &*slot->part;
    }

    // That part's most recent write, null only when the part has never been
    // written. A pointer rather than a copy: the caller reads it under the same
    // lock, and its replica ids belong to the object.
    const WriteAttempt* GetAttempt(size_t index) const {
        const PartSlot* slot = FindSlot(index);
        if (slot == nullptr || !slot->attempt.has_value()) {
            return nullptr;
        }
        return &*slot->attempt;
    }

    // Every part holds content and every part has a replica a reader can use.
    // Asked for each time it is needed: a part loses its answer the moment the
    // last usable replica of it goes away, whether through eviction, a segment
    // going offline or a handle going stale. A part that holds no content is
    // counted out in one step.
    bool IsReadable() const {
        if (present_parts_ != part_count_) {
            return false;
        }
        for (const auto& [index, slot] : slots_) {
            (void)index;
            if (!std::ranges::any_of(slot.part->replicas(),
                                     &Replica::is_readable)) {
                return false;
            }
        }
        return true;
    }

    // Whether any part still holds a replica whose allocator and segment are
    // both usable. This is the answer to "should this object stay", which used
    // to be called IsValid.
    bool HasAnyReplicaWithValidHandle() const {
        return HasReplica([](const Replica& replica) {
            // A segment that is unavailable right now still counts: the object
            // exists, its replica just cannot be read until the segment comes
            // back.
            return !replica.has_dead_handle();
        });
    }

    // Whether any part still carries an unfinished write: a replica that
    // part's attempt handed out is still PROCESSING.
    bool HasWriteInFlight() const {
        for (const auto& [index, slot] : slots_) {
            (void)index;
            if (!slot.part.has_value() || !slot.attempt.has_value()) {
                continue;
            }
            const auto& attempt = *slot.attempt;
            if (std::ranges::any_of(
                    slot.part->replicas(), [&attempt](const Replica& replica) {
                        return replica.is_processing() &&
                               std::ranges::find(attempt.replica_ids,
                                                 replica.id()) !=
                                   attempt.replica_ids.end();
                    })) {
                return true;
            }
        }
        return false;
    }

    // The newest write of any part: the moment the object last had a writer
    // working on it, which is what the object-level incompleteness timeout is
    // measured from. Empty only for an object no write has opened a part of;
    // an object that arrived from a snapshot counts from the restore, which
    // opens its parts.
    std::optional<std::chrono::system_clock::time_point> GetLastPutStartTime()
        const {
        std::optional<std::chrono::system_clock::time_point> newest;
        for (const auto& [index, slot] : slots_) {
            (void)index;
            if (!slot.attempt.has_value()) {
                continue;
            }
            if (!newest.has_value() || *newest < slot.attempt->started) {
                newest = slot.attempt->started;
            }
        }
        return newest;
    }

    size_t TotalLength() const {
        size_t total = 0;
        for (const auto& [index, slot] : slots_) {
            (void)index;
            if (slot.part.has_value()) {
                total += slot.part->length();
            }
        }
        return total;
    }

    // --- Changing the slots. Caller holds the owning shard's exclusive
    // lock. ---

    // Give part `index` its content and open the write that produced it. The
    // replicas arrive as the allocator handed them out; setting their status
    // is the caller's job. `session_id` is the session this write belongs to,
    // empty when the caller named none.
    tl::expected<void, ErrorCode> BeginWrite(
        size_t index, size_t length, std::vector<Replica>&& replicas,
        const UUID& writer, const std::string& session_id,
        std::chrono::system_clock::time_point now) {
        auto& slot = slots_[index];
        if (index == 0 && part_zero_slot_ == nullptr) {
            part_zero_slot_ = &slot;
        }
        if (slot.part.has_value()) {
            return tl::make_unexpected(ErrorCode::OBJECT_ALREADY_EXISTS);
        }
        // The parts of one object are pieces of one write, so they name one
        // session. It is recorded with the object on the first write that names
        // one, and a later part that names another is a caller mixing two
        // writes in one object. An object of one part is rewritten whole, so
        // the newest write names the session, which is what the object-level
        // record (the stored event, the snapshot) reads.
        if (!session_id.empty()) {
            if (session_id_.empty() || part_count_ == 1) {
                session_id_ = session_id;
            } else if (session_id_ != session_id) {
                LOG(ERROR) << "key=" << user_key << ", part_index=" << index
                           << ", session_id=" << session_id
                           << ", object_session_id=" << session_id_
                           << ", error=session_id_differs_across_parts";
                return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
            }
        }
        std::vector<ReplicaID> replica_ids;
        replica_ids.reserve(replicas.size());
        for (const auto& replica : replicas) {
            replica_ids.push_back(replica.id());
        }
        slot.part.emplace(length, std::move(replicas));
        slot.attempt = WriteAttempt{writer, now, std::move(replica_ids)};
        ++present_parts_;
        return {};
    }

    // The session the object's parts belong to, empty when no write named one.
    const std::string& WriteSessionId() const { return session_id_; }

    // Complete the replicas of part `index` that this part's write handed out
    // and that the caller names. What comes back are the replicas that changed
    // state, so the caller can act on exactly those. A repeated End of replicas
    // that are already COMPLETE succeeds and changes nothing; an End that names
    // nothing of this part's write is refused.
    tl::expected<std::vector<ReplicaID>, ErrorCode> EndWrite(
        size_t index, const std::vector<ReplicaID>& ended_ids) {
        PartSlot* found = FindSlot(index);
        if (found == nullptr || !found->part.has_value() ||
            !found->attempt.has_value()) {
            return tl::make_unexpected(ErrorCode::INVALID_WRITE);
        }
        auto& slot = *found;
        if (ended_ids.empty()) {
            return std::vector<ReplicaID>{};
        }
        bool names_one_of_ours = false;
        std::vector<ReplicaID> completed;
        for (auto& replica : slot.part->replicas()) {
            if (std::ranges::find(ended_ids, replica.id()) == ended_ids.end()) {
                continue;
            }
            if (std::ranges::find(slot.attempt->replica_ids, replica.id()) ==
                slot.attempt->replica_ids.end()) {
                continue;
            }
            names_one_of_ours = true;
            if (replica.is_processing()) {
                replica.mark_complete();
                completed.push_back(replica.id());
            }
        }
        if (!names_one_of_ours) {
            // The End names replicas this part's current write never handed
            // out: either the part was reassigned after the discard timeout or
            // the caller named another part's replicas.
            return tl::make_unexpected(ErrorCode::INVALID_WRITE);
        }
        return completed;
    }

    ObjectPart& MutablePart(size_t index) {
        PartSlot* found = FindSlot(index);
        if (found == nullptr || !found->part.has_value()) {
            LOG(FATAL) << "key=" << user_key << ", part_index=" << index
                       << ", part_count=" << part_count_
                       << ", error=part_holds_no_content";
        }
        return found->part.value();
    }

    // Take replicas out of part `index`. What comes back is out of the object:
    // the caller owns those replicas and decides how to isolate or release
    // them. A part that loses its last replica loses its content with it; the
    // attempt stays in the slot, because it records the write that produced it,
    // and every walk skips a slot whose content is gone.
    std::vector<Replica> PopPartReplicas(
        size_t index, const std::function<bool(const Replica&)>& pred_fn) {
        PartSlot* found = FindSlot(index);
        if (found == nullptr || !found->part.has_value()) {
            return {};
        }
        auto& slot = *found;
        auto popped_replicas = slot.part->PopReplicas(pred_fn);
        if (slot.part->replicas().empty()) {
            slot.part.reset();
            --present_parts_;
        }
        return popped_replicas;
    }

    std::vector<Replica> PopReplicas(
        const std::function<bool(const Replica&)>& pred_fn) {
        std::vector<Replica> popped_replicas;
        for (auto& [index, slot] : slots_) {
            (void)index;
            if (!slot.part.has_value()) {
                continue;
            }
            auto from_part = slot.part->PopReplicas(pred_fn);
            if (slot.part->replicas().empty()) {
                slot.part.reset();
                --present_parts_;
            }
            popped_replicas.insert(popped_replicas.end(),
                                   std::make_move_iterator(from_part.begin()),
                                   std::make_move_iterator(from_part.end()));
        }
        return popped_replicas;
    }

    std::vector<Replica> PopReplicas() {
        return PopReplicas([](const Replica&) { return true; });
    }

    // --- Accessors over the object's replicas, across every part ---
    //
    // A transition that runs on an object as a whole — eviction, expiry, quota,
    // replica cleanup, the read of a whole object — walks every part. The write
    // and read paths that name a part use GetPart and MutablePart instead; the
    // one that adds replicas acts on part 0 and says so.

    // Offload, copy, move and the local-disk registration have no per-part
    // form, so adding replicas to an object of several parts would silently act
    // on part 0 alone. A fatal log rather than an assert keeps the guard in a
    // release build.
    void AddReplicas(std::vector<Replica>&& replicas) {
        if (part_count_ != 1) {
            LOG(FATAL) << "ObjectMetadata::" << __func__
                       << " is only for a single-part object, but key "
                       << user_key << " has " << part_count_ << " parts";
        }
        MutablePart(0).AddReplicas(std::move(replicas));
    }

    // Takes out every replica of every part the predicate selects.
    size_t EraseReplicas(const std::function<bool(const Replica&)>& pred_fn) {
        return PopReplicas(pred_fn).size();
    }

    // visit_fn may modify the replica but must not add or pop replicas.
    size_t VisitReplicas(const std::function<bool(const Replica&)>& pred_fn,
                         const std::function<void(Replica&)>& visit_fn) {
        size_t num_visited = 0;

        for (auto& [index, slot] : slots_) {
            (void)index;
            if (!slot.part.has_value()) {
                continue;
            }
            for (auto& replica : slot.part->replicas()) {
                if (pred_fn(replica)) {
                    visit_fn(replica);
                    num_visited++;
                }
            }
        }

        return num_visited;
    }

    size_t VisitReplicas(
        const std::function<bool(const Replica&)>& pred_fn,
        const std::function<void(const Replica&)>& visit_fn) const {
        size_t num_visited = 0;

        for (const auto& [index, slot] : slots_) {
            (void)index;
            if (!slot.part.has_value()) {
                continue;
            }
            for (const auto& replica : slot.part->replicas()) {
                if (pred_fn(replica)) {
                    visit_fn(replica);
                    num_visited++;
                }
            }
        }

        return num_visited;
    }

    // Every part that holds content, with the index it sits at. A slot whose
    // content is gone keeps the record of the write that produced it, so a
    // caller that wants the parts themselves walks them through this instead of
    // asking for each index in turn: an object sized for 1024 parts may hold
    // one, and every such question would then be 1024 lookups.
    template <typename Fn>
    void ForEachPart(Fn&& fn) const {
        for (const auto& [index, slot] : slots_) {
            if (slot.part.has_value()) {
                fn(static_cast<size_t>(index), *slot.part);
            }
        }
    }

    // Every replica of every part, for a caller that wants all of them rather
    // than the ones a predicate selects: an object of several parts has no
    // single range of replicas to hand out.
    size_t VisitReplicas(
        const std::function<void(const Replica&)>& visit_fn) const {
        return VisitReplicas([](const Replica&) { return true; }, visit_fn);
    }

    // Any part, any replica. The predicates the callers pass in ask domain
    // questions (any COMPLETE replica, every replica complete, how many on a
    // medium), and an object of several parts answers them over all its parts.
    bool HasReplica(const std::function<bool(const Replica&)>& pred_fn) const {
        for (const auto& [index, slot] : slots_) {
            (void)index;
            if (slot.part.has_value() &&
                std::ranges::any_of(slot.part->replicas(), pred_fn)) {
                return true;
            }
        }
        return false;
    }

    // Every part, and every replica of every part. A part that holds no content
    // is counted out first, so an object still being written is not a set of
    // finished replicas, and the removal and drain paths that ask this must not
    // treat one as such.
    bool AllReplicas(const std::function<bool(const Replica&)>& pred_fn) const {
        if (present_parts_ != part_count_) {
            return false;
        }
        for (const auto& [index, slot] : slots_) {
            (void)index;
            if (!std::ranges::all_of(slot.part->replicas(), pred_fn)) {
                return false;
            }
        }
        return true;
    }

    size_t CountReplicas(
        const std::function<bool(const Replica&)>& pred_fn) const {
        size_t count = 0;
        for (const auto& [index, slot] : slots_) {
            (void)index;
            if (!slot.part.has_value()) {
                continue;
            }
            count += static_cast<size_t>(
                std::ranges::count_if(slot.part->replicas(), pred_fn));
        }
        return count;
    }

    size_t CountReplicas() const {
        return CountReplicas([](const Replica&) { return true; });
    }

    Replica* GetFirstReplica(
        const std::function<bool(const Replica&)>& pred_fn) {
        for (auto& [index, slot] : slots_) {
            (void)index;
            if (!slot.part.has_value()) {
                continue;
            }
            for (auto& replica : slot.part->replicas()) {
                if (pred_fn(replica)) {
                    return &replica;
                }
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

    std::vector<std::string> GetReplicaSegmentNames() const {
        std::vector<std::string> segment_names;
        for (const auto& [index, slot] : slots_) {
            (void)index;
            if (!slot.part.has_value()) {
                continue;
            }
            for (const auto& replica : slot.part->replicas()) {
                const auto& segment_name_options = replica.get_segment_names();
                for (const auto& segment_name_opt : segment_name_options) {
                    if (segment_name_opt.has_value()) {
                        segment_names.push_back(segment_name_opt.value());
                    }
                }
            }
        }
        return segment_names;
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

   private:
    // One entry per part that holds content, keyed by its index. A part that
    // holds none has no entry, so an object sized for a layout far larger than
    // what has arrived so far costs only the parts it holds. The attempt of a
    // part whose content is gone stays, which is why the entry stays with it.
    std::unordered_map<uint32_t, PartSlot> slots_;
    const size_t part_count_;
    // Part 0's slot, for the object of one part. Every object written before
    // parts existed is one, and its writes and reads reach part 0 several times
    // each, so they follow this pointer instead of hashing the index. Null
    // until the part is first written; the slot lives in the map above and its
    // address does not move while the object does.
    PartSlot* part_zero_slot_{nullptr};
    // How many parts hold content. A slot outlives the content it held, because
    // the attempt in it records the write that produced that content, so the
    // count is what answers "does every part hold content" without walking.
    size_t present_parts_{0};
    // The session the object's parts belong to, recorded by the first write
    // that named one. One object is one write session, so it belongs to the
    // object rather than to a part, and every later write of it is checked
    // against this one value.
    std::string session_id_;

    // That part's slot, or null when the part has never been written. Part 0 of
    // an object of one part is answered without hashing.
    PartSlot* FindSlot(size_t index) {
        if (index == 0 && part_zero_slot_ != nullptr) {
            return part_zero_slot_;
        }
        const auto found = slots_.find(index);
        return found == slots_.end() ? nullptr : &found->second;
    }
    const PartSlot* FindSlot(size_t index) const {
        if (index == 0 && part_zero_slot_ != nullptr) {
            return part_zero_slot_;
        }
        const auto found = slots_.find(index);
        return found == slots_.end() ? nullptr : &found->second;
    }
};

}  // namespace mooncake
