#include "tiered_cache/event_driven_scheduler/event_driven_client_scheduler.h"

#include <glog/logging.h>

#include <algorithm>
#include <chrono>
#include <limits>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "tiered_cache/event_driven_scheduler/json_config_util.h"
#include "tiered_cache/tiered_backend.h"
#include "tiered_cache/tiers/cache_tier.h"

namespace mooncake {

EventDrivenClientScheduler::EventDrivenClientScheduler(
    TieredBackend* backend, const Json::Value& config,
    std::unique_ptr<EventDrivenPolicy> policy)
    : backend_(backend),
      policy_(std::move(policy)),
      move_q_(ed_config::ReadSize(config, "queue_capacity", 1024)),
      pool_(std::make_unique<ThreadPool>(std::max<size_t>(
          1, ed_config::ReadSize(config, "thread_count", 2)))) {
    role_cfg_.mode =
        (ed_config::ReadString(config, "tier_role_mode", "auto") == "manual")
            ? TierRoleMode::kManual
            : TierRoleMode::kAuto;
    role_cfg_.fast_tier_tag =
        ed_config::ReadString(config, "fast_tier_tag", "");
    role_cfg_.slow_tier_tag =
        ed_config::ReadString(config, "slow_tier_tag", "");

    loop_interval_ms_ = ed_config::ReadInt(config, "loop_interval_ms", 1000);
    if (loop_interval_ms_ <= 0) {
        loop_interval_ms_ = 1000;
    }
    hot_key_num_ = ed_config::ReadSize(config, "hot_key_num", 2000);
    LOG(INFO) << "EventDrivenClientScheduler constructed";
}

EventDrivenClientScheduler::~EventDrivenClientScheduler() { Stop(); }

void EventDrivenClientScheduler::Start() {
    bool expected = false;
    if (!running_.compare_exchange_strong(expected, true)) {
        return;  // already started
    }
    ResolveRoles();
    evict_thread_ = std::thread(&EventDrivenClientScheduler::EvictLoop, this);
}

void EventDrivenClientScheduler::Stop() {
    // Stop the loop / reject new work, wake the evict thread, then drain
    // in-flight movement tasks BEFORE the policy/queue they touch can be
    // destroyed, then join the evict thread.
    running_.store(false);
    {
        std::lock_guard<std::mutex> lk(evict_mutex_);
        evict_wakeup_ = true;
    }
    evict_cv_.notify_all();

    if (stopped_.exchange(true)) {
        return;  // teardown already performed (idempotent)
    }
    if (pool_) {
        pool_->stop();
    }
    if (evict_thread_.joinable()) {
        evict_thread_.join();
    }
}

void EventDrivenClientScheduler::RegisterTier(CacheTier* /*tier*/) {
    // Roles are resolved from TierView.priority/tags in Start(); per-tier stats
    // are read on demand from the backend, so nothing is recorded here.
}

void EventDrivenClientScheduler::ResolveRoles() {
    const auto views = backend_->GetTierViews();
    if (views.empty()) {
        LOG(WARNING) << "EventDrivenClientScheduler: no tiers registered";
        roles_resolved_.store(false);
        return;
    }
    const TierRoles roles = ResolveTierRoles(views, role_cfg_);
    fast_tier_id_ = roles.fast;
    slow_tier_id_ = roles.slow;
    policy_->Init(backend_, fast_tier_id_, slow_tier_id_);
    roles_resolved_.store(true);
    LOG(INFO) << "EventDrivenClientScheduler roles resolved: fast="
              << fast_tier_id_.first << "-" << fast_tier_id_.second
              << (slow_tier_id_.has_value() ? ", slow set" : ", slow=none");
}

void EventDrivenClientScheduler::OnAccess(const AccessContext& ctx) {
    // Feed the policy (it updates its own stats) and enqueue any movement it
    // decides. No decision logic lives here.
    std::optional<MovementRequest> mv = policy_->OnAccess(ctx);
    if (!mv.has_value() || !running_.load()) {
        return;
    }
    const std::string key = mv->key;  // copy before moving the request
    if (move_q_.TryPush(key, std::move(*mv))) {
        DispatchToPool([this] {
            if (!running_.load()) {
                return;
            }
            MovementRequest m;
            if (move_q_.TryPop(m)) {
                Execute(m);
            }
        });
    }
}

void EventDrivenClientScheduler::OnCommit(const CommitContext& ctx) {
    policy_->OnCommit(ctx);
}

void EventDrivenClientScheduler::OnDelete(const DeleteContext& ctx) {
    policy_->OnDelete(ctx);
}

bool EventDrivenClientScheduler::OnAllocationFailure(
    const AllocationFailureContext& ctx) {
    // Unified synchronous-reclaim entry point for pressure on the FAST tier.
    // Reached from any allocator path that runs out of fast-tier room:
    //   1. external client writes (Put / PreWrite into DRAM),
    //   2. promotion/onboard (migrating a hot key slow -> fast),
    //   3. internal staging during data movement.
    //
    // Scope (D2-B): the event-driven scheduler force-reclaims ONLY the fast
    // tier. Non-fast (slow) tiers are intentionally not reclaimed here — they
    // own their own eviction inside CacheTier::Allocate (e.g. StorageTier
    // triggers bucket eviction when capacity is exceeded), so a failed offload
    // into a full slow tier has already attempted self-eviction by the time it
    // surfaces. We therefore decline (return false) for non-fast tiers rather
    // than double-managing their capacity.
    if (!running_.load() || !roles_resolved_.load() ||
        ctx.tier_id != fast_tier_id_) {
        return false;
    }
    // Nudge the periodic loop, then synchronously reclaim at least the
    // requested amount using the policy's eviction decision.
    {
        std::lock_guard<std::mutex> lk(evict_mutex_);
        evict_wakeup_ = true;
    }
    evict_cv_.notify_all();

    const auto movements = policy_->DecideEvict(ctx.required_bytes);
    size_t reclaimed = 0;
    for (const auto& mv : movements) {
        if (reclaimed >= ctx.required_bytes || !running_.load()) {
            break;
        }
        if (Execute(mv)) {
            reclaimed += mv.size_bytes;
        }
    }
    return HasAvailableBytes(ctx.tier_id, ctx.required_bytes);
}

AccessStats EventDrivenClientScheduler::GetHotKeyStats(
    std::optional<size_t> hot_key_num) const {
    size_t n = hot_key_num.value_or(hot_key_num_);
    if (n == 0) {
        n = std::numeric_limits<size_t>::max();  // 0 => all (capped internally)
    }
    return policy_->GetHotKeyStats(n);
}

void EventDrivenClientScheduler::EvictLoop() {
    while (running_.load()) {
        {
            std::unique_lock<std::mutex> lk(evict_mutex_);
            evict_cv_.wait_for(
                lk, std::chrono::milliseconds(loop_interval_ms_),
                [this] { return !running_.load() || evict_wakeup_; });
            evict_wakeup_ = false;
        }
        if (!running_.load()) {
            break;
        }
        if (!roles_resolved_.load()) {
            continue;
        }
        const auto movements = policy_->DecideEvict(/*min_reclaim_bytes=*/0);
        for (const auto& mv : movements) {
            if (!running_.load()) {
                return;
            }
            Execute(mv);
        }
    }
}

bool EventDrivenClientScheduler::Execute(const MovementRequest& mv) {
    switch (mv.kind) {
        case MovementRequest::Kind::kReplicate: {
            // Copy source -> dest, retaining the source (e.g. pre-demote). The
            // CAS on the re-read version makes a stale request a no-op.
            // CopyData is strict, so the offload lands on dest (the slow tier)
            // or is skipped — it never bounces back onto a higher-priority
            // tier.
            uint64_t version = 0;
            auto src = backend_->Get(mv.key, mv.source_tier,
                                     /*record_access=*/false, &version);
            if (!src.has_value()) {
                return false;
            }
            auto copy = backend_->CopyData(mv.key, src.value()->loc.data,
                                           mv.dest_tier, version,
                                           /*record_access=*/false);
            if (!copy.has_value()) {
                LOG(ERROR) << "Replicate skipped for " << mv.key << ": "
                        << copy.error();
                return false;
            }
            return true;
        }
        case MovementRequest::Kind::kMigrate: {
            // Copy source -> dest, then delete source. CopyData is strict, so
            // it must land on dest (sync-evicting it if full) and can never
            // fall back to another tier — a successful copy means the new
            // replica is genuinely on dest, so deleting the source cannot lose
            // the only copy. (Strict also makes onboard actually evict-to-fit
            // instead of silently re-landing the copy on the slow tier under
            // fast-tier pressure.)
            uint64_t version = 0;
            auto src = backend_->Get(mv.key, mv.source_tier,
                                     /*record_access=*/false, &version);
            if (!src.has_value()) {
                return false;
            }
            auto copy = backend_->CopyData(mv.key, src.value()->loc.data,
                                           mv.dest_tier, version,
                                           /*record_access=*/false);
            if (!copy.has_value()) {
                LOG(ERROR) << "Migrate copy skipped for " << mv.key << ": "
                        << copy.error();
                return false;
            }
            // Defense-in-depth: confirm residency before dropping the source in
            // case the fresh dest replica was evicted in the meantime.
            if (!backend_->Exist(mv.key, mv.dest_tier)) {
                LOG(ERROR) << "Migrate of " << mv.key
                        << " did not land on dest; keeping source";
                return false;
            }
            auto del = backend_->Delete(mv.key, mv.source_tier);
            return del.has_value();
        }
        case MovementRequest::Kind::kEvict: {
            // Drop the source-tier copy. The backend's Delete fires OnDelete,
            // which lets the policy update its residency bookkeeping.
            auto del = backend_->Delete(mv.key, mv.source_tier);
            return del.has_value();
        }
    }
    return false;
}

bool EventDrivenClientScheduler::HasAvailableBytes(
    UUID tier_id, size_t required_bytes) const {
    for (const auto& view : backend_->GetTierViews()) {
        if (view.id == tier_id) {
            return view.free_space >= required_bytes;
        }
    }
    return false;
}

void EventDrivenClientScheduler::DispatchToPool(std::function<void()> fn) {
    if (!running_.load() || !pool_) {
        return;
    }
    try {
        pool_->enqueue(std::move(fn));
    } catch (const std::runtime_error&) {
        // Pool was stopped between the check and the enqueue; drop the task.
    }
}

}  // namespace mooncake
