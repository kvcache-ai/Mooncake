#pragma once

// Promotion and prefetch management: upward LOCAL_DISK -> MEMORY admission,
// retry scheduling, and per-object PromotionTask state machine.
//
// Self-contained and thread-safe: PromotionManager owns its frequency sketch
// (CountMinSketch), sharded retry candidate tracker
// (PromotionCandidateTracker), sharded active PromotionTask table (1024
// cache-line-aligned shards with lock-free negative presence filters), and all
// promotion source replica pin/unpin and staged replica cleanup transitions.
// It never acquires MasterService locks itself; callers invoke key-scoped
// methods under their per-object write lock, or supply a lock callback for
// multi-key expiry.

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <ylt/util/tl/expected.hpp>

#include "common/concurrent_sharded_map.h"
#include "config/promotion_config.h"
#include "count_min_sketch.h"
#include "local_ssd/manager.h"
#include "object_metadata.h"
#include "object_runtime_state.h"
#include "promotion_candidate_tracker.h"
#include "replica.h"
#include "tenant_id.h"
#include "types.h"

namespace mooncake {

namespace test {
class MasterServiceTestPeer;
}  // namespace test

class PromotionManager {
   public:
    static constexpr size_t kPromotionRetryBatchSize = 128;
    static constexpr uint32_t kMaxPromotionExecutionFailures = 3;

    struct ReadHitGateResult {
        bool passed{false};
        PromotionQueueResult result{PromotionQueueResult::kDisabled};
        uint8_t freq{0};
    };

    // RAII reservation returned by BeginAlloc(): rolls back the allocation
    // reservation on destruction unless Commit(alloc_id) succeeds.
    class AllocReservation {
       public:
        AllocReservation() = default;
        AllocReservation(AllocReservation&& other) noexcept
            : manager_(std::exchange(other.manager_, nullptr)),
              client_id_(other.client_id_),
              tenant_id_(std::move(other.tenant_id_)),
              key_(std::move(other.key_)) {}
        AllocReservation& operator=(AllocReservation&& other) noexcept {
            if (this != &other) {
                Release();
                manager_ = std::exchange(other.manager_, nullptr);
                client_id_ = other.client_id_;
                tenant_id_ = std::move(other.tenant_id_);
                key_ = std::move(other.key_);
            }
            return *this;
        }
        AllocReservation(const AllocReservation&) = delete;
        AllocReservation& operator=(const AllocReservation&) = delete;
        ~AllocReservation() { Release(); }

        // Binds `alloc_id` on the reserved task and resets `start_time`.
        tl::expected<void, ErrorCode> Commit(ReplicaID alloc_id);

       private:
        friend class PromotionManager;
        AllocReservation(PromotionManager* manager, UUID client_id,
                         TenantId tenant_id, std::string key)
            : manager_(manager),
              client_id_(client_id),
              tenant_id_(std::move(tenant_id)),
              key_(std::move(key)) {}

        void Release() noexcept;

        PromotionManager* manager_{nullptr};
        UUID client_id_{};
        TenantId tenant_id_;
        std::string key_;
    };

    PromotionManager(LocalSsdManager& local_ssd, const PromotionConfig& config,
                     std::chrono::seconds task_timeout);

    PromotionManager(const PromotionManager&) = delete;
    PromotionManager& operator=(const PromotionManager&) = delete;

    // --- Read-hit eligibility & gating -------------------------------------

    bool HasPendingCandidates() const {
        return promotion_on_hit_ && !promotion_candidates_.Empty();
    }

    bool ShouldPromoteOnReadHit(
        std::span<const Replica::Descriptor> readable_replicas) const;

    ReadHitGateResult EvaluateReadHitGate(const TenantId& tenant_id,
                                          std::string_view key,
                                          double memory_used_ratio,
                                          double high_watermark_ratio,
                                          bool force);

    void RecordWatermarkCandidate(const TenantId& tenant_id,
                                  std::string_view key, uint8_t freq);

    // --- Self-contained task admission & prefetch --------------------------

    bool HasTask(const TenantId& tenant_id, std::string_view key) const;

    // Admits `source` (a LOCAL_DISK replica) for promotion under the key write
    // lock, pinning `source` (inc_refcnt) before publishing the task.
    PromotionQueueResult AdmitPromotion(const TenantId& tenant_id,
                                        const std::string& key, Replica& source,
                                        uint8_t freq, bool record_candidate,
                                        bool force);

    // Registers an explicit prefetch task for `source` under the key write
    // lock, verifying ownership and pinning `source` before publishing.
    tl::expected<void, ErrorCode> RegisterPrefetchTask(
        const UUID& client_id, const TenantId& tenant_id,
        const std::string& key, Replica& source);

    tl::expected<std::vector<PromotionTaskItem>, ErrorCode>
    TakePendingPromotions(const UUID& client_id);

    // --- Execution lifecycle: AllocStart / Complete / Fail -----------------

    // Validates task ownership, size, and unstaged state, and atomically
    // reserves allocation until the returned AllocReservation commits or
    // leaves scope.
    tl::expected<AllocReservation, ErrorCode> BeginAlloc(
        const UUID& client_id, const TenantId& tenant_id, std::string_view key,
        uint64_t size);

    tl::expected<ReplicaID, ErrorCode> GetStagedAllocId(
        const UUID& client_id, const TenantId& tenant_id,
        std::string_view key) const;

    // Completes a promotion under the key write lock: clears active task,
    // unpins source replica, updates metrics & cache accounting, and returns
    // whether the task originated from prefetch.
    tl::expected<bool, ErrorCode> CompletePromotion(const UUID& client_id,
                                                    const TenantId& tenant_id,
                                                    std::string_view key,
                                                    bool committed,
                                                    ObjectMetadata& metadata);

    // Fails a promotion under the key write lock: clears active task, unpins
    // source replica, pops any still-processing staged memory replica, and
    // records a retry candidate when eligible.
    tl::expected<void, ErrorCode> FailPromotion(const UUID& client_id,
                                                const TenantId& tenant_id,
                                                std::string_view key,
                                                ObjectMetadata& metadata);

    // --- Candidate retry helpers -------------------------------------------

    std::vector<PromotionCandidateTracker::Key> TakeDueCandidates(
        std::chrono::steady_clock::time_point now,
        size_t limit = kPromotionRetryBatchSize);

    void EraseCandidate(const TenantId& tenant_id, std::string_view key);

    void ReportCandidateOutcome(const TenantId& tenant_id, std::string_view key,
                                PromotionQueueResult result);

    // --- Lifecycle cleanup & expiry ----------------------------------------

    // Cancels an active promotion task if its staged alloc_id is among
    // `removed_replica_ids`, unpinning its source replica in `metadata`.
    bool CancelTaskForRemovedReplicas(
        const TenantId& tenant_id, std::string_view key,
        std::span<const ReplicaID> removed_replica_ids,
        ObjectMetadata& metadata);

    void OnObjectErased(const TenantId& tenant_id, std::string_view key,
                        ObjectMetadata& metadata);

    // Two-phase expiry hidden behind the key-lock callback: scans candidates
    // without holding key locks, then acquires each key lock via
    // `with_locked_metadata` to re-verify expiration, erase the task, unpin
    // the source replica, and pop any still-processing staged replica.
    template <typename LockMetadataFn>
    void DiscardExpiredTasks(const std::chrono::system_clock::time_point& now,
                             LockMetadataFn&& with_locked_metadata) {
        for (const auto& candidate : CollectExpiredTasks(now)) {
            with_locked_metadata(candidate.tenant_id, candidate.key,
                                 [&](ObjectMetadata* metadata) {
                                     TryExpireTaskLocked(candidate.tenant_id,
                                                         candidate.key, now,
                                                         metadata);
                                 });
        }
    }

   private:
    friend class test::MasterServiceTestPeer;

    static void UnpinReplica(ObjectMetadata& metadata, ReplicaID replica_id);
    static void PopStagedReplica(ObjectMetadata& metadata, ReplicaID alloc_id);

    tl::expected<void, ErrorCode> CommitAlloc(const UUID& client_id,
                                              const TenantId& tenant_id,
                                              std::string_view key,
                                              ReplicaID alloc_id);
    void AbortAlloc(const UUID& client_id, const TenantId& tenant_id,
                    std::string_view key);

    std::vector<ScopedObjectKey> CollectExpiredTasks(
        const std::chrono::system_clock::time_point& now) const;

    void TryExpireTaskLocked(const TenantId& tenant_id, std::string_view key,
                             const std::chrono::system_clock::time_point& now,
                             ObjectMetadata* metadata);

    std::optional<PromotionTask> FindTask(const TenantId& tenant_id,
                                          std::string_view key) const;
    std::optional<PromotionCandidate> FindCandidate(const TenantId& tenant_id,
                                                    std::string_view key) const;
    void SeedTaskForTesting(const TenantId& tenant_id, const std::string& key,
                            PromotionTask task);

    LocalSsdManager& local_ssd_;
    const bool promotion_on_hit_;
    const uint32_t promotion_admission_threshold_;
    const uint32_t promotion_queue_limit_;
    const uint32_t promotion_max_per_heartbeat_;
    const std::chrono::seconds task_timeout_;

    ConcurrentShardedMap<PromotionTask> tasks_;
    PromotionCandidateTracker promotion_candidates_;
    std::unique_ptr<CountMinSketch> promotion_sketch_;
};

}  // namespace mooncake
