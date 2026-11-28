#pragma once

// ObjectMetadata: the per-object metadata envelope.
//
// Moved out of MasterService so the service and the per-object metadata
// module share one definition. The envelope carries the object identity, the
// authoritative read lease and the soft-pin state.
// The object's content and the replicas holding it live in the envelope's
// single ObjectPart.
//
// The enclosing slot's reader/writer lock guards record ownership and group
// binding. Read leases are atomic. The spin lock protects soft-pin changes
// that may take place while the enclosing slot is shared.

#include <algorithm>
#include <cassert>
#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <ranges>
#include <span>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/heap_optional.h"
#include "metadata/group_lease.h"
#include "master_metric_manager.h"
#include "mutex.h"
#include "object_part.h"
#include "replica.h"
#include "tenant_id.h"
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

    // RAII-style metric management
    ~ObjectMetadata() {
        MasterMetricManager::instance().dec_key_count(1);
        MasterMetricManager::instance().dec_key_group(key_group, TotalLength());
        if (soft_pin_timeout) {
            MasterMetricManager::instance().dec_soft_pin_key_count(1);
        }
    }

    ObjectMetadata() = delete;

    template <class... Args>
    ObjectMetadata(const UUID& writer, Args&&... args)
        : ObjectMetadata(nullptr, nullptr, writer,
                         std::forward<Args>(args)...) {}

    // A routed publication borrows its map node's immutable key and its
    // namespace's tenant. Both outlive destruction of the record.
    ObjectMetadata(const std::string* slot_key, const TenantId* route_tenant,
                   const UUID& client_id_,
                   const std::chrono::system_clock::time_point put_start_time_,
                   size_t value_length, std::vector<Replica>&& reps,
                   std::optional<std::chrono::system_clock::time_point>
                       committed_soft_pin_timeout = std::nullopt,
                   bool enable_hard_pin = false,
                   ObjectDataType data_type_ = ObjectDataType::UNKNOWN,
                   std::string group_id_ = "",
                   const TenantId& tenant_id_ = TenantId::Default(),
                   const std::string& user_key_ = {},
                   std::string residency_affinity_id_ = "")
        : client_id(client_id_),
          put_start_time(put_start_time_),
          user_key(BindKey(slot_key, user_key_)),
          tenant_id(BindTenant(route_tenant, tenant_id_)),
          key_group(
              MasterMetricManager::instance().classify_key_group(user_key)),
          data_type(data_type_),
          hard_pinned(enable_hard_pin),
          soft_pin_timeout(std::move(committed_soft_pin_timeout)),
          part_(value_length, std::move(reps)) {
        if (slot_key && *slot_key != user_key_) {
            LOG(FATAL) << "Publish under key " << *slot_key
                       << " of an object keyed " << user_key_;
        }
        if (!group_id_.empty() || !residency_affinity_id_.empty()) {
            auto& labels = EnsureLabels();
            labels.unbound_group = std::move(group_id_);
            labels.affinity = std::move(residency_affinity_id_);
        }
        MasterMetricManager::instance().inc_key_count(1);
        MasterMetricManager::instance().inc_key_group(key_group, value_length);
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

   private:
    struct OwnedIdentity {
        std::string key;
        TenantId tenant;
    };
    struct Labels {
        std::string unbound_group;
        std::string affinity;
        std::string session;
        // Standalone envelopes and routes with no associated tenant.
        HeapOptional<OwnedIdentity> owned;
    };
    struct Attributes {
        std::shared_ptr<NamedGroupLease> group_lease;
        HeapOptional<Labels> labels;
    };
    HeapOptional<Attributes> attributes_;
    Attributes& EnsureAttributes() {
        return attributes_ ? *attributes_ : attributes_.emplace();
    }
    Labels& EnsureLabels() {
        auto& labels = EnsureAttributes().labels;
        return labels ? *labels : labels.emplace();
    }
    OwnedIdentity& Owned() {
        auto& owned = EnsureLabels().owned;
        return owned ? *owned : owned.emplace();
    }
    const std::string& BindKey(const std::string* slot_key,
                               const std::string& supplied) {
        if (slot_key) return *slot_key;
        return Owned().key = supplied;
    }
    const TenantId& BindTenant(const TenantId* route_tenant,
                               const TenantId& supplied) {
        if (route_tenant && *route_tenant == supplied) return *route_tenant;
        if (supplied.IsDefault()) return TenantId::Default();
        return Owned().tenant = supplied;
    }
    static const std::string& EmptyString() {
        static const std::string empty;
        return empty;
    }

   public:
    const std::string& user_key;
    const TenantId& tenant_id;
    const uint8_t key_group;
    const ObjectDataType data_type;
    const bool hard_pinned;
    bool memory_cache_total_accounted{false};
    bool disk_cache_total_accounted{false};
    mutable SpinLock lock;

    const std::string& GroupId() const {
        if (attributes_ && attributes_->group_lease)
            return attributes_->group_lease->group_id;
        return attributes_ && attributes_->labels
                   ? attributes_->labels->unbound_group
                   : EmptyString();
    }
    const std::string& ResidencyAffinityId() const {
        return attributes_ && attributes_->labels
                   ? attributes_->labels->affinity
                   : EmptyString();
    }
    const std::string& SessionId() const {
        return attributes_ && attributes_->labels ? attributes_->labels->session
                                                  : EmptyString();
    }
    void SetSessionId(std::string id) {
        if (!id.empty() || (attributes_ && attributes_->labels)) {
            EnsureLabels().session = std::move(id);
        }
    }
    struct PendingSessionTag {
        std::string session_id;
        std::unordered_set<ReplicaID> eligible_replica_ids;
    };
    HeapOptional<PendingSessionTag> pending_session_tag;

    void BeginSessionTag(std::string id,
                         const std::unordered_set<ReplicaID>& replicas) {
        pending_session_tag.reset();
        if (!id.empty()) {
            pending_session_tag = PendingSessionTag{std::move(id), replicas};
        }
    }

    // An offload admitted by PutEnd must be followed by a non-allocating
    // commit of its source replicas. Keep the pending tag until that commit.
    void PrepareSessionTagCommit() {
        if (pending_session_tag) EnsureLabels();
    }

    void CommitSessionTag(ReplicaID replica_id) {
        if (pending_session_tag &&
            pending_session_tag->eligible_replica_ids.contains(replica_id)) {
            EnsureLabels().session = std::move(pending_session_tag->session_id);
            pending_session_tag.reset();
        }
    }

    void ClearSessionTagIfNoViableReplica() {
        if (pending_session_tag && !HasReplica([this](const Replica& replica) {
                return pending_session_tag->eligible_replica_ids.contains(
                           replica.id()) &&
                       replica.is_processing() &&
                       !replica.has_invalid_mem_handle() &&
                       !replica.has_invalid_nof_handle();
            })) {
            pending_session_tag.reset();
        }
    }

    // Group membership changes only under the enclosing object's write lock.
    // Ungrouped reads update the local atomic deadline in-place.
    mutable Lease local_lease_;
    const std::shared_ptr<NamedGroupLease>& GroupLease() const {
        static const std::shared_ptr<NamedGroupLease> none;
        return attributes_ ? attributes_->group_lease : none;
    }
    void BindGroupLease(std::shared_ptr<NamedGroupLease> lease) noexcept {
        assert(attributes_ && lease && lease->group_id == GroupId());
        auto& attrs = *attributes_;
        attrs.group_lease = std::move(lease);
        if (attrs.labels) {
            if (!attrs.labels->owned && attrs.labels->session.empty() &&
                attrs.labels->affinity.empty()) {
                attrs.labels.reset();
            } else {
                std::string{}.swap(attrs.labels->unbound_group);
            }
        }
    }
    Lease& ActiveLease() const {
        return attributes_ && attributes_->group_lease
                   ? *attributes_->group_lease
                   : local_lease_;
    }
    mutable std::optional<std::chrono::system_clock::time_point>
        soft_pin_timeout GUARDED_BY(lock);  // committed object soft-pin
                                            // deadline
    // Replica IDs scope this action to the current write. PutEnd does not
    // carry a generation token, so a stale End from the same client cannot
    // otherwise be distinguished from the current write.
    HeapOptional<PendingSoftPinAction> pending_soft_pin_action;

    struct DynamicReplicaRecord {
        std::chrono::system_clock::time_point created_at;
        std::string source_segment;
        std::string target_segment;
        std::string target_domain;
        bool complete{false};
    };

    // Invalidated by a later Mark, Forget, Set, or Reclaim on this object:
    // those calls may free the state. Use it only under the object lock,
    // before the next mutation.
    const std::unordered_map<ReplicaID, DynamicReplicaRecord>& DynamicReplicas()
        const {
        static const std::unordered_map<ReplicaID, DynamicReplicaRecord> kEmpty;
        return dynamic_replica_state_ ? dynamic_replica_state_->replicas
                                      : kEmpty;
    }

    // True while a replica record or a still-active recreate deadline is
    // stored on the heap.
    bool HasDynamicReplicaState() const {
        return static_cast<bool>(dynamic_replica_state_);
    }

    void MarkDynamicReplica(ReplicaID replica_id, DynamicReplicaRecord record) {
        EnsureDynamicReplicaState().replicas[replica_id] = std::move(record);
    }

    void MarkDynamicReplicasComplete(
        const std::vector<ReplicaID>& replica_ids) {
        if (!dynamic_replica_state_) {
            return;
        }
        auto& replicas = dynamic_replica_state_->replicas;
        for (const auto& replica_id : replica_ids) {
            auto it = replicas.find(replica_id);
            if (it != replicas.end()) {
                it->second.complete = true;
            }
        }
    }

    size_t ForgetDynamicReplicas(const std::vector<ReplicaID>& replica_ids) {
        if (!dynamic_replica_state_) {
            return 0;
        }
        auto& replicas = dynamic_replica_state_->replicas;
        size_t forgotten = 0;
        for (const auto& replica_id : replica_ids) {
            forgotten += replicas.erase(replica_id);
        }
        // erase() leaves the map's buckets behind. Drop them when the map is
        // empty, and drop the deadline too once it has expired.
        ReleaseDynamicReplicaStateIfIdle(std::chrono::steady_clock::now(),
                                         /*discard_buckets=*/true);
        return forgotten;
    }

    size_t DynamicReplicaCount() const {
        return dynamic_replica_state_ ? dynamic_replica_state_->replicas.size()
                                      : 0;
    }

    bool DynamicReplicationRecreateBlocked(
        std::chrono::steady_clock::time_point now) const {
        const auto recreate_after =
            dynamic_replica_state_ ? dynamic_replica_state_->recreate_after
                                   : std::chrono::steady_clock::time_point{};
        return now < recreate_after;
    }

    void SetDynamicReplicationRecreateAfter(
        std::chrono::steady_clock::time_point deadline) {
        if (!dynamic_replica_state_) {
            if (deadline == std::chrono::steady_clock::time_point{}) {
                return;
            }
            dynamic_replica_state_.emplace();
        }
        auto& recreate_after = dynamic_replica_state_->recreate_after;
        recreate_after = std::max(recreate_after, deadline);
        ReleaseDynamicReplicaStateIfIdle(std::chrono::steady_clock::now(),
                                         /*discard_buckets=*/false);
    }

    // Drops an empty state whose recreate deadline has expired. A deadline
    // still in the future is kept. Call under the object's exclusive lock.
    void ReclaimExpiredDynamicReplicaState(
        std::chrono::steady_clock::time_point now) {
        ReleaseDynamicReplicaStateIfIdle(now, /*discard_buckets=*/false);
    }

    // Read under the owner's shared lock, change under its exclusive lock. A
    // replica pointer taken from a part is invalidated by any add or pop on it.
    const ObjectPart& GetPart(size_t index) const {
        if (index != 0) throw std::out_of_range("object part index");
        return part_;
    }

    ObjectPart& MutablePart(size_t index) {
        if (index != 0) throw std::out_of_range("object part index");
        return part_;
    }

    size_t TotalLength() const { return part_.length(); }

    // Temporary: these operate on part 0. The multi-part work replaces them
    // with `parts()` and a flattened `replicas()` view, so each call site says
    // which it means.
    void AddReplicas(std::vector<Replica>&& replicas) {
        MutablePart(0).AddReplicas(std::move(replicas));
    }

    std::vector<Replica> PopReplicas(
        const std::function<bool(const Replica&)>& pred_fn) {
        return MutablePart(0).PopReplicas(pred_fn);
    }

    std::vector<Replica> PopReplicas() { return MutablePart(0).PopReplicas(); }

    size_t EraseReplicas(const std::function<bool(const Replica&)>& pred_fn) {
        return MutablePart(0).PopReplicas(pred_fn).size();
    }

    // visit_fn may modify the replica but must not add or pop replicas.
    size_t VisitReplicas(const std::function<bool(const Replica&)>& pred_fn,
                         const std::function<void(Replica&)>& visit_fn) {
        size_t num_visited = 0;

        for (auto& replica : MutablePart(0).replicas()) {
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

        for (const auto& replica : GetPart(0).replicas()) {
            if (pred_fn(replica)) {
                visit_fn(replica);
                num_visited++;
            }
        }

        return num_visited;
    }

    bool HasReplica(const std::function<bool(const Replica&)>& pred_fn) const {
        return std::ranges::any_of(GetPart(0).replicas(), pred_fn);
    }

    bool AllReplicas(const std::function<bool(const Replica&)>& pred_fn) const {
        return std::ranges::all_of(GetPart(0).replicas(), pred_fn);
    }

    size_t CountReplicas(
        const std::function<bool(const Replica&)>& pred_fn) const {
        return static_cast<size_t>(
            std::ranges::count_if(GetPart(0).replicas(), pred_fn));
    }

    size_t CountReplicas() const { return GetPart(0).replicas().size(); }

    std::span<const Replica> GetAllReplicas() const {
        return GetPart(0).replicas();
    }

    Replica* GetFirstReplica(
        const std::function<bool(const Replica&)>& pred_fn) {
        auto replicas = MutablePart(0).replicas();
        const auto it = std::ranges::find_if(replicas, pred_fn);
        return it != replicas.end() ? &(*it) : nullptr;
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
        ActiveLease().GrantReadLease(ttl);
    }

    // Whether the authoritative lease is expired.
    bool IsLeaseExpired() const {
        return ActiveLease().IsExpired(std::chrono::system_clock::now());
    }

    bool IsLeaseExpired(std::chrono::system_clock::time_point& now) const {
        return ActiveLease().IsExpired(now);
    }

    // Lease deadline eviction ranks the object by.
    std::chrono::system_clock::time_point EvictionDeadline() const {
        return ActiveLease().ExpiresAt();
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

        return ApplySoftPin({pending.action, pending.ttl}, now);
    }

    // Caller holds the object's metadata write lock. Independent commands do
    // not consume or replace a pending write's soft-pin action.
    SoftPinEvaluation ApplySoftPin(
        const ResolvedSoftPinRequest& request,
        const std::chrono::system_clock::time_point& now) {
        SpinLocker locker(&lock);
        int metric_delta = 0;
        std::optional<std::chrono::system_clock::time_point> removed_deadline;
        std::optional<std::chrono::system_clock::time_point> deadline_to_index;
        if (soft_pin_timeout && now >= *soft_pin_timeout) {
            removed_deadline = *soft_pin_timeout;
            soft_pin_timeout.reset();
            --metric_delta;
        }

        switch (request.action) {
            case SoftPinAction::PRESERVE:
                break;
            case SoftPinAction::ENABLE:
                if (request.ttl == std::chrono::milliseconds::zero()) {
                    if (soft_pin_timeout) {
                        removed_deadline = *soft_pin_timeout;
                        soft_pin_timeout.reset();
                        --metric_delta;
                    }
                } else {
                    if (!soft_pin_timeout) {
                        ++metric_delta;
                    }
                    soft_pin_timeout = ComputeSoftPinDeadline(now, request.ttl);
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

    bool IsGrouped() const { return !GroupId().empty(); }

    // Check if the metadata is valid
    // Valid means it has at least one valid replica and size is greater
    // than 0
    bool IsValid() const {
        return GetPart(0).length() > 0 &&
               HasReplica([](const Replica& replica) {
                   return !replica.is_memory_replica() ||
                          !replica.has_invalid_mem_handle();
               });
    }

    std::vector<std::string> GetReplicaSegmentNames() const {
        std::vector<std::string> segment_names;
        for (const auto& replica : GetPart(0).replicas()) {
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
    // Heap node for dynamic replication. Empty while the key has no replica
    // record and no recreate deadline that is still in the future.
    struct DynamicReplicaState {
        std::unordered_map<ReplicaID, DynamicReplicaRecord> replicas;
        std::chrono::steady_clock::time_point recreate_after{};
    };
    HeapOptional<DynamicReplicaState> dynamic_replica_state_;

    DynamicReplicaState& EnsureDynamicReplicaState() {
        if (!dynamic_replica_state_) {
            dynamic_replica_state_.emplace();
        }
        return *dynamic_replica_state_;
    }

    // Map must be empty. An expired or unset deadline frees the node. A
    // deadline still in the future stays; discard_buckets replaces the map
    // so buckets left by erase() are freed.
    void ReleaseDynamicReplicaStateIfIdle(
        std::chrono::steady_clock::time_point now, bool discard_buckets) {
        if (!dynamic_replica_state_ ||
            !dynamic_replica_state_->replicas.empty()) {
            return;
        }
        const auto deadline = dynamic_replica_state_->recreate_after;
        if (now < deadline) {
            if (!discard_buckets) {
                return;
            }
            dynamic_replica_state_.reset();
            dynamic_replica_state_.emplace();
            dynamic_replica_state_->recreate_after = deadline;
            return;
        }
        dynamic_replica_state_.reset();
    }

    // The first part sits in the object itself: every object has one today.
    ObjectPart part_;
};

}  // namespace mooncake
