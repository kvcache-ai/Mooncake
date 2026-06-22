#pragma once

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <thread>

#include <json/value.h>

#include "thread_pool.h"
#include "tiered_cache/event_driven_scheduler/bounded_dedup_queue.h"
#include "tiered_cache/event_driven_scheduler/event_driven_policy.h"
#include "tiered_cache/event_driven_scheduler/tier_roles.h"
#include "tiered_cache/scheduler/client_scheduler_interface.h"
#include "types.h"

namespace mooncake {

class TieredBackend;
class CacheTier;

/**
 * @class EventDrivenClientScheduler
 * @brief Generic, policy-agnostic event-driven scheduler.
 *
 * This class owns only mechanism, never policy:
 *   - lifecycle (Start/Stop), a background evict thread, a small shared thread
 *     pool, and a bounded de-dup movement queue;
 *   - tier-role resolution (fast/slow by priority/tags);
 *   - generic execution of MovementRequests against the data plane.
 *
 * ALL decisions live in the injected EventDrivenPolicy: the On* hooks just feed
 * the policy and enqueue whatever movement it returns. Any EventDrivenPolicy
 * implementation can be plugged in.
 */
class EventDrivenClientScheduler : public IClientScheduler {
   public:
    EventDrivenClientScheduler(TieredBackend* backend, const Json::Value& config,
                               std::unique_ptr<EventDrivenPolicy> policy);
    ~EventDrivenClientScheduler() override;  // Stop() runs first (idempotent)

    void Start() override;
    void Stop() override;
    void RegisterTier(CacheTier* tier) override;

    // The On* hooks only route events: feed the policy, enqueue any returned
    // movement. No business logic lives here.
    void OnAccess(const AccessContext& ctx) override;
    void OnCommit(const CommitContext& ctx) override;
    void OnDelete(const DeleteContext& ctx) override;
    bool OnAllocationFailure(const AllocationFailureContext& ctx) override;
    AccessStats GetHotKeyStats(
        std::optional<size_t> hot_key_num = std::nullopt) const override;

   private:
    void EvictLoop();    // background evict thread
    void ResolveRoles();  // resolve fast/slow from tier topology, Init the policy

    // Execute a single policy-decided movement against the data plane. Returns
    // true if the movement's source-side effect succeeded.
    bool Execute(const MovementRequest& mv);

    bool HasAvailableBytes(UUID tier_id, size_t required_bytes) const;
    void DispatchToPool(std::function<void()> fn);

    TieredBackend* backend_;

    // Declaration order matters for teardown: pool_ is declared LAST so
    // it is destroyed FIRST — draining in-flight movement tasks (which touch
    // move_q_ and, via backend hooks, policy_) before those are destroyed.
    std::unique_ptr<EventDrivenPolicy> policy_;  // the pluggable brain
    BoundedDedupQueue<MovementRequest> move_q_;
    std::unique_ptr<ThreadPool> pool_;

    UUID fast_tier_id_{};
    std::optional<UUID> slow_tier_id_;
    std::atomic<bool> roles_resolved_{false};
    TierRoleConfig role_cfg_;

    std::atomic<bool> running_{false};
    std::atomic<bool> stopped_{false};
    std::thread evict_thread_;
    std::mutex evict_mutex_;
    std::condition_variable evict_cv_;
    bool evict_wakeup_ = false;  // guarded by evict_mutex_

    int loop_interval_ms_ = 1000;
    // Default GetHotKeyStats(nullopt) count; sized to the ~2000-key async
    // metadata-sync batch (overwritten from scheduler.hot_key_num if set).
    size_t hot_key_num_ = 2000;
};

}  // namespace mooncake
