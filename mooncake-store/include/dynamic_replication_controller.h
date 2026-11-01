#pragma once

// Dynamic MEMORY replication: which hot objects earn one more MEMORY replica,
// where it goes, and the lease a client copies it inside.
//
// The controller owns the heat windows, the admission queue and the thread
// draining it, the per-tenant lease tables, and the index of keys carrying a
// pending task. It owns no object: the pending task and the cooldown of a key
// live on route::ObjectState next to its replication task, so all three are
// guarded by the lock that guards the object's record. The *Locked methods
// apply the rules to a state whose lock the caller already holds.
//
// Lock order: object lock -> lease lock, object lock -> pending-index lock.
// The heat lock and the queue lock are leaves, and the admission thread takes
// neither while it submits.

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <queue>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <ylt/util/tl/expected.hpp>

#include "client_liveness.h"
#include "dynamic_replication_lease_table.h"
#include "metadata/object_state.h"
#include "object_metadata.h"
#include "rpc_types.h"
#include "segment.h"
#include "tenant_id.h"
#include "types.h"

namespace mooncake {

namespace test {
class MasterServiceTestPeer;
}  // namespace test

class DynamicReplicationController {
   public:
    // Where one more MEMORY replica of an object would be copied from and to.
    struct Plan {
        std::string source_segment;
        std::string target_segment;
        std::string target_domain;
        std::shared_ptr<ClientLivenessRecord> source_liveness;
    };

    // Runs on the admission thread once per queued object.
    using SubmitFn =
        std::function<void(const TenantId& tenant_id, const std::string& key)>;

    // `mode` is "off", "observe" or "enforce"; anything else means off.
    DynamicReplicationController(const std::string& mode,
                                 uint32_t heat_window_seconds,
                                 double admission_qps_threshold,
                                 size_t max_memory_replicas);
    ~DynamicReplicationController();

    DynamicReplicationController(const DynamicReplicationController&) = delete;
    DynamicReplicationController& operator=(
        const DynamicReplicationController&) = delete;

    // Starts the admission thread. Stop() joins it and is idempotent; the
    // owner calls it before anything `submit` reaches is torn down.
    void Start(SubmitFn submit);
    void Stop();

    bool Enabled() const { return mode_ != Mode::kOff; }
    bool Enforcing() const { return mode_ == Mode::kEnforce; }

    // Whether an object holding `memory_replicas` readable MEMORY replicas is
    // eligible for one more.
    bool WantsAnotherMemoryReplica(size_t memory_replicas) const;

    // --- Heat admission ----------------------------------------------------

    // Counts one read of the object and, once it turns hot, queues it for a
    // proposal (enforce) or logs that it would have (observe).
    void RecordAccess(const TenantId& tenant_id, const std::string& key);
    // Counts one read; true on the read that makes the object hot.
    bool ObserveAccess(const TenantId& tenant_id, const std::string& key);
    // Whether the object is hot in its current window.
    bool HeatAdmitted(const TenantId& tenant_id, const std::string& key);
    uint32_t AdmissionMinHits() const;

    // --- Proposals and leases ----------------------------------------------

    // The proposal the master raises on its own for a hot object.
    ReplicaActionProposal MakeAutoProposal(const TenantId& tenant_id,
                                           const std::string& key) const;
    // The checks that need no object: mode, shape and deadline.
    tl::expected<void, ErrorCode> ValidateProposal(
        const ReplicaActionProposal& proposal, int64_t now_ms) const;
    // The live lease already issued for this proposal, for an idempotent
    // retry; std::nullopt when there is none, dropping an expired one.
    // INVALID_PARAMS when a live lease exists for a different request.
    tl::expected<std::optional<ReplicaActionLease>, ErrorCode>
    FindReusableLease(const TenantId& tenant_id, const std::string& key,
                      const ReplicaActionProposal& proposal, int64_t now_ms);
    // Picks a source among the readable MEMORY replicas and a target segment
    // with room to spare, preferring another host.
    std::optional<Plan> SelectPlan(
        ScopedSegmentAccess& segment_access, const ObjectMetadata& metadata,
        const std::function<bool(const Replica&)>& is_readable,
        const std::optional<std::string>& preferred_target_segment,
        std::string target_domain) const;

    std::optional<ReplicaActionLease> FindLease(const TenantId& tenant_id,
                                                const UUID& proposal_id) const;
    // Retracts every lease of one object key, so a teardown does not leave a
    // client acting on a key that is gone.
    void EraseLeasesForObject(const TenantId& tenant_id, std::string_view key);
    void EraseExpiredLeases(std::chrono::system_clock::time_point now);

    // The keys whose object was issued a pending task, as a snapshot to
    // resolve again under each object's lock. A key in it may have consumed
    // or dropped its task since, or may name an object that is gone.
    std::vector<std::pair<TenantId, std::string>> PendingKeys() const;
    // Forgets a key of PendingKeys() found carrying no pending task; the caller
    // holds the lock of the key's object, or knows the object is gone.
    void UnindexPendingLocked(const TenantId& tenant_id,
                              const std::string& key);

    // --- Per-object pending state; the caller holds the object lock --------

    // UNAVAILABLE_IN_CURRENT_STATUS while the object is cooling down from its
    // last action; an elapsed cooldown is cleared.
    tl::expected<void, ErrorCode> CheckCooldownLocked(
        route::ObjectState& state) const;
    // Issues the lease for `plan` and records it as the object's pending task,
    // still without a task id.
    ReplicaActionLease IssueLeaseLocked(route::ObjectState& state,
                                        const TenantId& tenant_id,
                                        const std::string& key,
                                        const ReplicaActionProposal& proposal,
                                        const Plan& plan,
                                        uint64_t version_epoch, int64_t now_ms);
    // Binds the submitted task to the pending record, starts the cooldown and
    // files the lease.
    void CommitLeaseLocked(route::ObjectState& state, const TenantId& tenant_id,
                           const std::string& key,
                           const ReplicaActionLease& lease);
    // Whether a task is still pending; an expired one is cleared.
    bool HasPendingLocked(const TenantId& tenant_id, const std::string& key,
                          route::ObjectState& state);
    bool PendingExpiredLocked(const route::ObjectState& state,
                              int64_t now_ms) const;
    // Drops the pending task, the cooldown and the leases of the key.
    void ClearPendingLocked(const TenantId& tenant_id, const std::string& key,
                            route::ObjectState& state);
    // Checks a CopyStart against the pending task. A plain copy passes only
    // when no task is pending; a dynamic copy only for the pending task's own
    // lease, epoch, source and target. An expired or outdated pending task is
    // cleared. Which client owns the source segment is left to the caller.
    tl::expected<void, ErrorCode> ValidateCopyStartLocked(
        route::ObjectState& state, const TenantId& tenant_id,
        const std::string& key, const UUID& lease_id,
        const std::string& source_segment, uint64_t current_version_epoch,
        uint64_t lease_version_epoch,
        const std::vector<std::string>& target_segments);
    // A dynamic copy's hold on the pending task it was admitted for, from a
    // passed CopyStart validation to the registered replication task. Unless
    // released, it drops the pending task, its cooldown and its leases on the
    // way out, so a failed CopyStart does not hold back the next proposal. An
    // inactive guard, for a plain copy, does nothing.
    class PendingCopyGuard {
       public:
        PendingCopyGuard(DynamicReplicationController& controller,
                         const TenantId& tenant_id, const std::string& key,
                         route::ObjectState& state, bool active)
            : controller_(controller),
              tenant_id_(tenant_id),
              key_(key),
              state_(state),
              active_(active) {}
        ~PendingCopyGuard() {
            if (active_) {
                controller_.ClearPendingLocked(tenant_id_, key_, state_);
            }
        }
        PendingCopyGuard(const PendingCopyGuard&) = delete;
        PendingCopyGuard& operator=(const PendingCopyGuard&) = delete;

        // The copy registered its task; the pending state is its to keep.
        void Release() { active_ = false; }

       private:
        DynamicReplicationController& controller_;
        const TenantId& tenant_id_;
        const std::string& key_;
        route::ObjectState& state_;
        bool active_;
    };
    // Consumes the pending task at CopyStart, marking the replica it adds.
    void RegisterCopyStartLocked(
        ObjectMetadata& metadata, route::ObjectState& state,
        const std::string& source_segment, uint64_t version_epoch,
        const std::vector<std::string>& target_segments,
        const std::vector<ReplicaID>& replica_ids) const;
    // Forgets removed dynamic replicas and holds off recreating them.
    size_t RecordReplicaRemoval(
        ObjectMetadata& metadata,
        const std::vector<ReplicaID>& replica_ids) const;

    // The object version a lease is bound to.
    static uint64_t VersionEpoch(const ObjectMetadata& metadata);
    static int64_t NowMs();

   private:
    friend class test::MasterServiceTestPeer;

    enum class Mode { kOff, kObserve, kEnforce };
    struct Window {
        std::chrono::steady_clock::time_point window_start{};
        uint32_t hits{0};
    };
    struct QueuedObject {
        TenantId tenant_id;
        std::string key;
    };

    static constexpr std::chrono::milliseconds kActionCooldown{30000};
    static constexpr std::chrono::milliseconds kLeaseTtl{30000};
    static constexpr std::chrono::milliseconds kRecreateCooldown{60000};
    static constexpr std::chrono::milliseconds kWindowCleanupInterval{1000};
    static constexpr uint64_t kAdmissionThreadSleepMs = 100;
    static constexpr size_t kWindowEntryLimit = 50000;
    static constexpr size_t kWindowCleanupBudget = 256;
    static constexpr size_t kAdmissionQueueLimit = 50000;
    static constexpr size_t kAdmissionBatchSize = 64;
    static constexpr double kTargetHighWatermark = 0.85;

    static uint64_t StableScore(const std::string& key,
                                const std::string& segment);
    void CleanupWindowsLocked(std::chrono::steady_clock::time_point now,
                              std::chrono::seconds window);
    void Enqueue(const TenantId& tenant_id, const std::string& key);
    void AdmissionThreadFunc();

    const Mode mode_;
    const uint32_t heat_window_seconds_;
    const double admission_qps_threshold_;
    const size_t max_memory_replicas_;

    std::mutex heat_mutex_;
    std::unordered_map<std::string, Window> windows_;
    std::deque<std::string> window_order_;
    std::chrono::steady_clock::time_point next_window_cleanup_{};

    std::mutex admission_mutex_;
    std::condition_variable admission_cv_;
    std::queue<QueuedObject> admission_queue_;
    std::unordered_set<std::string> admission_queued_;
    std::atomic<bool> admission_running_{false};
    SubmitFn submit_;
    std::thread admission_thread_;

    // Only tenants with a lease in flight have a table, so a tenant's table
    // goes once its last lease is retracted or expires.
    mutable std::mutex lease_mutex_;
    std::unordered_map<TenantId, DynamicReplicationLeaseTable, TenantIdHash>
        leases_;

    // The expiry sweep resolves these keys instead of walking every object.
    // Only tenants with at least one key have a set.
    mutable std::mutex pending_mutex_;
    std::unordered_map<TenantId, std::unordered_set<std::string>, TenantIdHash>
        pending_keys_;
};

}  // namespace mooncake
