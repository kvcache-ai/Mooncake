#include "p2p/ha/metadata_recovery_worker.h"

#include <glog/logging.h>

#include <algorithm>
#include <chrono>
#include <exception>
#include <thread>
#include <unordered_set>
#include <utility>

namespace mooncake {

MetadataRecoveryWorker::MetadataRecoveryWorker(DataManager& data_manager,
                                               AsyncMetadataNotifier* notifier,
                                               PublishReplica publish_replica,
                                               RecoveryMode recovery_mode)
    : data_manager_(data_manager),
      notifier_(notifier),
      publish_replica_(std::move(publish_replica)),
      recovery_mode_(recovery_mode) {}

MetadataRecoveryWorker::~MetadataRecoveryWorker() { Stop(); }

void MetadataRecoveryWorker::StopLocked() {
    if (need_abort_) {
        {
            std::lock_guard<std::mutex> lk(retry_mutex_);
            need_abort_->store(true, std::memory_order_release);
        }
        retry_cv_.notify_all();
    }
    if (recovery_thread_.joinable()) {
        recovery_thread_.join();
    }
}

void MetadataRecoveryWorker::Stop() {
    std::lock_guard<std::mutex> lk(mutex_);
    StopLocked();
}

ErrorCode MetadataRecoveryWorker::Start() {
    std::lock_guard<std::mutex> lk(mutex_);
    StopLocked();
    if (recovery_mode_ == RecoveryMode::RegisterOnly) {
        status_.store(Status::COMPLETED, std::memory_order_release);
        return ErrorCode::OK;
    }
    status_.store(Status::RUNNING, std::memory_order_release);
    try {
        need_abort_ = std::make_shared<std::atomic<bool>>(false);
        auto need_abort = need_abort_;
        recovery_thread_ = std::thread([this, need_abort]() {
            try {
                status_.store(RecoveryPipelineMain(need_abort),
                              std::memory_order_release);
            } catch (const std::exception& e) {
                LOG(ERROR) << "Metadata recovery threw: " << e.what();
                status_.store(Status::FAILED, std::memory_order_release);
            } catch (...) {
                LOG(ERROR) << "Metadata recovery threw an unknown exception";
                status_.store(Status::FAILED, std::memory_order_release);
            }
        });
        return ErrorCode::OK;
    } catch (const std::exception& e) {
        LOG(ERROR) << "Failed to start metadata recovery thread: " << e.what();
    } catch (...) {
        LOG(ERROR)
            << "Failed to start metadata recovery thread: unknown exception";
    }
    need_abort_.reset();
    status_.store(Status::FAILED, std::memory_order_release);
    return ErrorCode::INTERNAL_ERROR;
}

MetadataRecoveryWorker::Status MetadataRecoveryWorker::RecoveryPipelineMain(
    AbortToken need_abort) {
    LOG(INFO) << "Recovery pipeline started";

    auto aborted = [&]() {
        return need_abort->load(std::memory_order_acquire);
    };

    // Replay all tracked hot keys before the remaining replicas.
    auto hot_stats = data_manager_.GetHotKeyStats(/*hot_key_num=*/0);
    std::unordered_set<std::string> synced_keys;
    size_t hot_count = 0;

    for (const auto& entry : hot_stats.hot_keys) {
        if (aborted()) {
            return Status::CANCELLED;
        }
        auto size_result = data_manager_.QueryObjectSize(entry.key);
        if (!size_result || size_result.value() == 0) {
            continue;
        }
        size_t size = size_result.value();
        auto tier_ids = data_manager_.GetReplicaTierIds(entry.key);
        for (const auto& tier_id : tier_ids) {
            // Hot keys go through normal (high-priority) queue
            if (!EnqueueWithRetry(entry.key, tier_id, size,
                                  /*is_hot=*/true, need_abort)) {
                return aborted() ? Status::CANCELLED : Status::FAILED;
            }
            hot_count++;
        }
        synced_keys.insert(entry.key);
    }
    LOG(INFO) << "Recovery Phase 1: enqueued " << hot_count
              << " hot key entries";

    if (aborted()) {
        return Status::CANCELLED;
    }

    // Phase 2+3: Iterate all keys in batches.
    // DRAM entries enqueued before storage entries within each batch.
    auto tier_views = data_manager_.GetTierViews();
    std::unordered_set<UUID, boost::hash<UUID>> dram_tiers;
    for (const auto& tv : tier_views) {
        if (tv.type == MemoryType::DRAM) {
            dram_tiers.insert(tv.id);
        }
    }

    size_t dram_count = 0, storage_count = 0;
    bool was_aborted = false;

    data_manager_.ForEachKeyBatch(
        [&](std::vector<ReplicaLocation>&& batch) -> bool {
            if (aborted()) {
                was_aborted = true;
                return false;
            }

            // Pass 1: DRAM entries first for fastest recovery
            for (const auto& e : batch) {
                if (synced_keys.count(e.key)) {
                    continue;
                }
                if (dram_tiers.count(e.tier_id)) {
                    if (e.size == 0) {
                        continue;
                    }
                    if (!EnqueueWithRetry(e.key, e.tier_id, e.size,
                                          /*is_hot=*/false, need_abort)) {
                        was_aborted = true;
                        LOG(WARNING) << "fail to enqueue route recovery list";
                        return false;
                    }
                    dram_count++;
                }
            }
            // Pass 2: Storage entries
            for (const auto& e : batch) {
                if (synced_keys.count(e.key)) {
                    continue;
                }
                if (dram_tiers.count(e.tier_id)) {
                    continue;
                }
                if (e.size == 0) {
                    continue;
                }
                if (!EnqueueWithRetry(e.key, e.tier_id, e.size,
                                      /*is_hot=*/false, need_abort)) {
                    was_aborted = true;
                    LOG(WARNING) << "fail to enqueue route recovery list";
                    return false;
                }
                storage_count++;
            }
            return true;
        });

    if (was_aborted) {
        LOG(INFO) << "Recovery aborted during key iteration";
        return aborted() ? Status::CANCELLED : Status::FAILED;
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
                return Status::CANCELLED;
            }
            LOG(ERROR) << "Recovery drain timed out with incomplete route sync";
            return Status::TIMED_OUT;
        }
    }

    LOG(INFO) << "Recovery pipeline completed";
    return aborted() ? Status::CANCELLED : Status::COMPLETED;
}

bool MetadataRecoveryWorker::EnqueueWithRetry(const std::string& key,
                                              const UUID& tier_id, size_t size,
                                              bool is_hot,
                                              const AbortToken& need_abort) {
    if (!notifier_) {
        auto backoff = std::chrono::milliseconds(100);
        constexpr auto kMaxBackoff = std::chrono::milliseconds(1000);
        while (!need_abort->load(std::memory_order_acquire)) {
            auto result = publish_replica_(key, tier_id, size);
            if (result || result.error() == ErrorCode::REPLICA_ALREADY_EXISTS) {
                return true;
            }
            LOG(ERROR) << "Recovery publication failed, key=" << key
                       << ", error=" << result.error();
            if (result.error() != ErrorCode::RPC_FAIL) {
                return false;
            }
            std::unique_lock<std::mutex> lk(retry_mutex_);
            if (retry_cv_.wait_for(lk, backoff, [&] {
                    return need_abort->load(std::memory_order_acquire);
                })) {
                return false;
            }
            backoff = std::min(backoff * 2, kMaxBackoff);
        }
        return false;
    }
    while (true) {
        if (need_abort->load(std::memory_order_acquire)) {
            return false;
        }
        auto r = is_hot ? notifier_->EnqueueAdd(key, tier_id, size)
                        : notifier_->EnqueueRecoveryAdd(key, tier_id, size);
        if (r) {
            return true;
        }
        // Queue full — yield to normal writes then retry
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
}

}  // namespace mooncake
