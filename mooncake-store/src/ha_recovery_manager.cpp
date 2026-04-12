#include "ha_recovery_manager.h"

#include <glog/logging.h>

#include <thread>
#include <unordered_set>

namespace mooncake {

HARecoveryManager::HARecoveryManager(
    const UUID& client_id, P2PMasterClient& master_client,
    std::optional<DataManager>& data_manager,
    std::unique_ptr<AsyncMetadataNotifier>& notifier,
    ViewVersionId& view_version)
    : client_id_(client_id),
      master_client_(master_client),
      data_manager_(data_manager),
      notifier_(notifier),
      view_version_(view_version) {}

HARecoveryManager::~HARecoveryManager() { Stop(); }

void HARecoveryManager::Stop() {
    std::lock_guard<std::mutex> lk(mutex_);
    if (need_abort_) {
        need_abort_->store(true, std::memory_order_release);
        abort_cv_.notify_all();
    }
    if (recovery_thread_.joinable()) {
        recovery_thread_.join();
    }
    if (state_.load(std::memory_order_relaxed) == HAClientState::SYNCING) {
        TransitionState(HAClientState::DEGRADED, "shutdown");
    }
}

tl::expected<void, ErrorCode> HARecoveryManager::SetSyncCompleted() {
    auto result = master_client_.SetSyncCompleted(client_id_);
    if (!result) {
        LOG(ERROR) << "SetSyncCompleted RPC failed: " << result.error();
    }
    return result;
}

// ============================================================================
// State Machine
// ============================================================================

void HARecoveryManager::TransitionState(HAClientState to,
                                        const std::string& reason) {
    auto from = state_.load(std::memory_order_relaxed);
    LOG(WARNING) << "HA state: " << from << " -> " << to
                 << ", reason=" << reason << ", view_version=" << view_version_;
    state_.store(to, std::memory_order_release);
}

void HARecoveryManager::HandleEvent(HAEvent event) {
    std::lock_guard<std::mutex> lk(mutex_);
    if (event == HAEvent::MASTER_UNREACHABLE &&
        state_.load(std::memory_order_acquire) == HAClientState::DEGRADED) {
        return;
    }

    // Abort + join under mutex_. Safe because recovery thread's exit path
    // uses lock-free CAS (no mutex_ needed), so no deadlock.
    if (need_abort_) {
        need_abort_->store(true, std::memory_order_release);
        abort_cv_.notify_all();
    }
    if (recovery_thread_.joinable()) {
        recovery_thread_.join();
    }

    auto current = state_.load(std::memory_order_relaxed);

    switch (event) {
        case HAEvent::MASTER_UNREACHABLE:
            if (current == HAClientState::FULL ||
                current == HAClientState::SYNCING) {
                TransitionState(HAClientState::DEGRADED,
                                "heartbeat failure threshold exceeded");
                if (notifier_) notifier_->Stop(/*drop_pending=*/true);
            }
            break;

        case HAEvent::MASTER_REACHABLE:
            if (current == HAClientState::DEGRADED) {
                if (notifier_) notifier_->Start();
            }
            if (current != HAClientState::SYNCING) {
                TransitionState(HAClientState::SYNCING,
                                "master connection ready, starting recovery");
            }
            StartRecoveryThread();
            break;
    }
}

// ============================================================================
// Recovery Thread Management
// ============================================================================

void HARecoveryManager::StartRecoveryThread() {
    // Old thread already joined by HandleEvent before acquiring mutex_.
    // Create fresh abort token and start new thread.
    need_abort_ = std::make_shared<std::atomic<bool>>(false);
    auto need_abort = need_abort_;
    recovery_thread_ =
        std::thread([this, need_abort]() { RecoveryPipelineMain(need_abort); });
}

void HARecoveryManager::RecoveryPipelineMain(AbortToken need_abort) {
    LOG(INFO) << "Recovery pipeline started";

    if (!data_manager_.has_value()) {
        LOG(ERROR) << "DataManager not initialized, cannot run recovery";
        return;
    }

    auto aborted = [&]() {
        return need_abort->load(std::memory_order_acquire);
    };

    // Phase 1: Hot key sync — enqueue hot keys first for fastest recovery
    auto hot_stats = data_manager_.value().GetHotKeyStats();
    std::unordered_set<std::string> synced_keys;
    size_t hot_count = 0;

    for (const auto& entry : hot_stats.hot_keys) {
        if (aborted()) return;
        auto tier_ids = data_manager_.value().GetReplicaTierIds(entry.key);
        for (const auto& tier_id : tier_ids) {
            auto handle = data_manager_.value().Get(entry.key, tier_id);
            if (!handle) continue;
            size_t size = handle.value()->loc.data.buffer
                              ? handle.value()->loc.data.buffer->size()
                              : 0;
            if (notifier_ && size > 0) {
                // Hot keys go through normal (high-priority) queue
                notifier_->EnqueueAdd(entry.key, tier_id, size);
                hot_count++;
            }
        }
        synced_keys.insert(entry.key);
    }
    LOG(INFO) << "Recovery Phase 1: enqueued " << hot_count
              << " hot key entries";

    if (aborted()) return;

    // Phase 2+3: Iterate all keys in batches.
    // DRAM entries enqueued before storage entries within each batch.
    auto tier_views = data_manager_.value().GetTierViews();
    std::unordered_set<UUID, boost::hash<UUID>> dram_tiers;
    for (const auto& tv : tier_views) {
        if (tv.type == MemoryType::DRAM) {
            dram_tiers.insert(tv.id);
        }
    }

    size_t dram_count = 0, storage_count = 0;
    bool was_aborted = false;

    data_manager_.value().ForEachKeyBatch(
        [&](std::vector<ReplicaLocation>&& batch) -> bool {
            if (aborted()) {
                was_aborted = true;
                return false;
            }

            std::vector<ReplicaLocation> storage_batch;
            for (auto& e : batch) {
                if (synced_keys.count(e.key)) continue;
                if (dram_tiers.count(e.tier_id)) {
                    if (e.size == 0) continue;
                    if (notifier_) {
                        if (!RecoveryEnqueueAdd(e.key, e.tier_id, e.size,
                                                need_abort)) {
                            was_aborted = true;
                            LOG(WARNING)
                                << "fail to enqueue route recovery list";
                            return false;
                        }
                        dram_count++;
                    }
                } else {
                    storage_batch.push_back(std::move(e));
                }
            }
            for (auto& e : storage_batch) {
                if (e.size == 0) continue;
                if (notifier_) {
                    if (!RecoveryEnqueueAdd(e.key, e.tier_id, e.size,
                                            need_abort)) {
                        was_aborted = true;
                        LOG(WARNING) << "fail to enqueue route recovery list";
                        return false;
                    }
                    storage_count++;
                }
            }
            return true;
        });

    if (was_aborted) {
        LOG(INFO) << "Recovery aborted during key iteration";
        return;
    }

    LOG(INFO) << "Recovery enqueue complete: hot=" << hot_count
              << ", dram=" << dram_count << ", storage=" << storage_count;

    // Wait for recovery queue to drain (all ops sent to Master).
    static constexpr auto kRecoveryDrainTimeout = std::chrono::minutes(10);
    if (notifier_) {
        bool drained = notifier_->WaitForRecoveryDrain(
            [&]() { return aborted(); }, kRecoveryDrainTimeout);
        if (!drained) {
            if (aborted()) {
                LOG(INFO) << "Recovery aborted during drain wait";
                return;
            }
            LOG(WARNING) << "Recovery drain timed out, transitioning to FULL"
                         << " with incomplete route sync";
        }
    }

    // All recovery routes delivered. Notify Master.
    // Retry indefinitely until success or abort — if Master restarts again,
    // HandleEvent(MASTER_UNREACHABLE) will set need_abort and this thread
    // exits.
    while (true) {
        if (aborted()) return;
        auto sync_result = SetSyncCompleted();
        if (sync_result) break;
        LOG(WARNING) << "SetSyncCompleted failed: " << sync_result.error()
                     << ", retrying in 500ms";
        std::unique_lock<std::mutex> lk(abort_mutex_);
        abort_cv_.wait_for(lk, std::chrono::milliseconds(500),
                           [&] { return aborted(); });
    }

    // Transition SYNCING→FULL if not aborted.
    if (!need_abort->load(std::memory_order_acquire)) {
        HAClientState expected = HAClientState::SYNCING;
        if (state_.compare_exchange_strong(expected, HAClientState::FULL,
                                           std::memory_order_acq_rel)) {
            LOG(WARNING) << "HA state: SYNCING -> FULL"
                         << ", reason=recovery complete";
        }
    }
    LOG(INFO) << "Recovery pipeline completed";
}

// return false when abort
bool HARecoveryManager::RecoveryEnqueueAdd(const std::string& key,
                                           const UUID& tier_id, size_t size,
                                           const AbortToken& need_abort) {
    while (true) {
        if (need_abort->load(std::memory_order_acquire)) return false;
        auto r = notifier_->EnqueueRecoveryAdd(key, tier_id, size);
        if (r) return true;
        // Queue full — yield to normal writes then retry
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
}

}  // namespace mooncake
