#include "promotion_manager.h"

#include <glog/logging.h>

#include <algorithm>
#include <limits>
#include <utility>

#include "master_metric_manager.h"

namespace mooncake {

// --- AllocReservation -------------------------------------------------------

tl::expected<void, ErrorCode> PromotionManager::AllocReservation::Commit(
    ReplicaID alloc_id) {
    if (manager_ == nullptr) {
        return tl::make_unexpected(ErrorCode::REPLICA_IS_NOT_READY);
    }
    auto res = manager_->CommitAlloc(client_id_, tenant_id_, key_, alloc_id);
    if (res) {
        manager_ = nullptr;
    }
    return res;
}

void PromotionManager::AllocReservation::Release() noexcept {
    if (manager_ != nullptr) {
        manager_->AbortAlloc(client_id_, tenant_id_, key_);
        manager_ = nullptr;
    }
}

// --- Static metadata helpers ------------------------------------------------

void PromotionManager::PopStagedReplica(ObjectMetadata& metadata,
                                        ReplicaID alloc_id) {
    if (alloc_id == 0) {
        return;
    }
    (void)metadata.PopReplicas([alloc_id](const Replica& replica) {
        return replica.id() == alloc_id && replica.is_processing();
    });
    metadata.SyncCacheTotalAccounting();
}

// --- PromotionManager -------------------------------------------------------

PromotionManager::PromotionManager(LocalSsdManager& local_ssd,
                                   const PromotionConfig& config,
                                   std::chrono::seconds task_timeout)
    : local_ssd_(local_ssd),
      promotion_on_hit_(config.promotion_on_hit),
      promotion_admission_threshold_(config.promotion_admission_threshold),
      promotion_queue_limit_(config.promotion_queue_limit),
      promotion_max_per_heartbeat_(config.promotion_max_per_heartbeat),
      task_timeout_(task_timeout) {
    config.Validate();
    if (promotion_on_hit_) {
        promotion_sketch_ = std::make_unique<CountMinSketch>();
        LOG(INFO) << "Promotion-on-hit mode enabled: LOCAL_DISK-only Gets "
                     "will queue async promotion to MEMORY (threshold="
                  << promotion_admission_threshold_
                  << ", queue_limit=" << promotion_queue_limit_
                  << ", max_per_heartbeat=" << promotion_max_per_heartbeat_
                  << ")";
    }
}

bool PromotionManager::ShouldPromoteOnReadHit(
    std::span<const Replica::Descriptor> readable_replicas) const {
    if (!promotion_on_hit_) {
        return false;
    }
    const bool any_memory = std::any_of(
        readable_replicas.begin(), readable_replicas.end(),
        [](const auto& descriptor) { return descriptor.is_memory_replica(); });
    if (any_memory) {
        return false;
    }
    return std::any_of(readable_replicas.begin(), readable_replicas.end(),
                       [](const auto& descriptor) {
                           return descriptor.is_local_disk_replica();
                       });
}

PromotionManager::ReadHitGateResult PromotionManager::EvaluateReadHitGate(
    const TenantId& tenant_id, std::string_view key, double memory_used_ratio,
    double high_watermark_ratio, bool force) {
    if (!force && (!promotion_on_hit_ || !promotion_sketch_)) {
        return {.passed = false,
                .result = PromotionQueueResult::kDisabled,
                .freq = 0};
    }

    uint8_t freq = std::numeric_limits<uint8_t>::max();
    if (!force) {
        const auto admission_key = tenant_id.MakeScopedKey(key);
        freq = promotion_sketch_->increment(admission_key);
        if (freq < promotion_admission_threshold_) {
            MasterMetricManager::instance().inc_promotion_rejected_frequency();
            return {.passed = false,
                    .result = PromotionQueueResult::kFrequencyRejected,
                    .freq = freq};
        }
    }

    if (memory_used_ratio >= high_watermark_ratio) {
        MasterMetricManager::instance().inc_promotion_rejected_watermark();
        return {.passed = false,
                .result = PromotionQueueResult::kWatermarkRejected,
                .freq = freq};
    }

    return {
        .passed = true, .result = PromotionQueueResult::kQueued, .freq = freq};
}

void PromotionManager::RecordWatermarkCandidate(const TenantId& tenant_id,
                                                std::string_view key,
                                                uint8_t freq) {
    promotion_candidates_.Record(tenant_id, key, freq,
                                 PromotionCandidateReason::kWatermark,
                                 ErrorCode::OK);
}

// --- Self-contained task admission & prefetch -------------------------------

bool PromotionManager::HasTask(const TenantId& tenant_id,
                               std::string_view key) const {
    return tasks_.Contains(tenant_id, key);
}

PromotionQueueResult PromotionManager::AdmitPromotion(
    const TenantId& tenant_id, const std::string& key, Replica& source,
    uint8_t freq, bool record_candidate, bool force) {
    const auto holder_id = source.get_local_disk_client_id();
    const auto source_session = source.getClientSession();
    if (!holder_id.has_value() || !source_session) {
        promotion_candidates_.Erase(tenant_id, key);
        return PromotionQueueResult::kNoLocalDiskSource;
    }

    const uint64_t object_size =
        source.get_descriptor().get_local_disk_descriptor().object_size;
    const bool source_serving = source_session->IsServing();

    std::optional<UUID> existing_holder_id;
    bool hit_cap = false;
    bool not_serving = false;
    ErrorCode push_error = ErrorCode::OK;

    tasks_.WithShardLocked(tenant_id, key, [&](auto& shard) {
        if (auto* existing = shard.Find(tenant_id, key); existing != nullptr) {
            existing_holder_id = existing->holder_id;
        } else if (tasks_.Count() >= promotion_queue_limit_) {
            hit_cap = true;
        } else if (!source_serving) {
            not_serving = true;
        } else {
            push_error = local_ssd_.EnqueuePromotion(
                *holder_id,
                PromotionTaskItem{.tenant_id = tenant_id.value(),
                                  .key = key,
                                  .size = static_cast<int64_t>(object_size)});
            if (push_error == ErrorCode::OK) {
                const uint32_t execution_failures =
                    promotion_candidates_.Consume(tenant_id, key);
                // Pin via ReplicaPin before making the task visible so any
                // concurrent eviction/move check observes refcnt >= 1.
                shard.Emplace(
                    tenant_id, key,
                    PromotionTask{
                        .source_id = source.id(),
                        .alloc_id = 0,
                        .object_size = object_size,
                        .start_time = std::chrono::system_clock::now(),
                        .holder_id = *holder_id,
                        .execution_failures = execution_failures,
                        .from_prefetch = false,
                        .source_pin = source.Pin()});
            }
        }
    });

    if (existing_holder_id.has_value()) {
        local_ssd_.TouchPromotion(*existing_holder_id, tenant_id, key);
        promotion_candidates_.Erase(tenant_id, key);
        VLOG(1) << "promotion_rejected key=" << key
                << " reason=promotion_in_flight force=" << force;
        return PromotionQueueResult::kAlreadyInFlight;
    }

    if (hit_cap) {
        MasterMetricManager::instance().inc_promotion_rejected_cap();
        if (record_candidate && !force) {
            promotion_candidates_.Record(tenant_id, key, freq,
                                         PromotionCandidateReason::kQueueCap,
                                         ErrorCode::OK);
        }
        return PromotionQueueResult::kQueueCapRejected;
    }

    if (not_serving) {
        VLOG(1) << "promotion_push_failed key=" << key
                << " error=" << ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS;
        if (record_candidate && !force) {
            promotion_candidates_.Record(
                tenant_id, key, freq, PromotionCandidateReason::kPushFailed,
                ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
        }
        return PromotionQueueResult::kPushFailed;
    }

    if (push_error != ErrorCode::OK) {
        VLOG(1) << "promotion_push_failed key=" << key
                << " error=" << push_error;
        if (push_error == ErrorCode::OBJECT_ALREADY_EXISTS) {
            promotion_candidates_.Erase(tenant_id, key);
            return PromotionQueueResult::kAlreadyInFlight;
        }
        if (push_error == ErrorCode::SEGMENT_NOT_FOUND ||
            push_error == ErrorCode::INVALID_PARAMS) {
            promotion_candidates_.Erase(tenant_id, key);
            return PromotionQueueResult::kNoLocalDiskSource;
        }
        if (record_candidate && !force) {
            promotion_candidates_.Record(tenant_id, key, freq,
                                         PromotionCandidateReason::kPushFailed,
                                         push_error);
        }
        return PromotionQueueResult::kPushFailed;
    }

    MasterMetricManager::instance().inc_promotion_in_flight();
    MasterMetricManager::instance().inc_promotion_admitted();
    VLOG(1) << "promotion_queued key=" << key << " size=" << object_size;
    return PromotionQueueResult::kQueued;
}

tl::expected<void, ErrorCode> PromotionManager::RegisterPrefetchTask(
    const UUID& client_id, const TenantId& tenant_id, const std::string& key,
    Replica& source) {
    const auto holder_id = source.get_local_disk_client_id();
    if (!holder_id.has_value() || *holder_id != client_id) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    const uint64_t object_size =
        source.get_descriptor().get_local_disk_descriptor().object_size;

    auto res = tasks_.WithShardLocked(
        tenant_id, key, [&](auto& shard) -> tl::expected<void, ErrorCode> {
            if (shard.Contains(tenant_id, key)) {
                return tl::make_unexpected(ErrorCode::PROMOTION_ALREADY_EXISTS);
            }
            if (tasks_.Count() >= promotion_queue_limit_) {
                return tl::make_unexpected(ErrorCode::KEYS_ULTRA_LIMIT);
            }
            const uint32_t execution_failures =
                promotion_candidates_.Consume(tenant_id, key);
            shard.Emplace(
                tenant_id, key,
                PromotionTask{.source_id = source.id(),
                              .alloc_id = 0,
                              .object_size = object_size,
                              .start_time = std::chrono::system_clock::now(),
                              .holder_id = client_id,
                              .execution_failures = execution_failures,
                              .from_prefetch = true,
                              .source_pin = source.Pin()});
            return {};
        });
    if (!res) {
        return res;
    }

    MasterMetricManager::instance().inc_promotion_in_flight();
    VLOG(1) << "prefetch_task_registered key=" << key
            << " size=" << object_size;
    return {};
}

tl::expected<std::vector<PromotionTaskItem>, ErrorCode>
PromotionManager::TakePendingPromotions(const UUID& client_id) {
    return local_ssd_.TakePromotions(client_id, promotion_max_per_heartbeat_);
}

// --- Execution lifecycle: AllocStart / Complete / Fail ----------------------

tl::expected<PromotionManager::AllocReservation, ErrorCode>
PromotionManager::BeginAlloc(const UUID& client_id, const TenantId& tenant_id,
                             std::string_view key, uint64_t size) {
    ErrorCode err = ErrorCode::REPLICA_IS_NOT_READY;
    tasks_.WithExistingLocked(
        tenant_id, key, [&](PromotionTask& task, bool& /*erase*/) {
            if (task.holder_id != client_id || task.object_size != size) {
                err = ErrorCode::INVALID_PARAMS;
                return;
            }
            if (task.alloc_id != 0 || task.allocating) {
                err = ErrorCode::REPLICA_IS_NOT_READY;
                return;
            }
            task.allocating = true;
            err = ErrorCode::OK;
        });
    if (err != ErrorCode::OK) {
        return tl::make_unexpected(err);
    }
    return AllocReservation(this, client_id, tenant_id, std::string(key));
}

tl::expected<void, ErrorCode> PromotionManager::CommitAlloc(
    const UUID& client_id, const TenantId& tenant_id, std::string_view key,
    ReplicaID alloc_id) {
    ErrorCode err = ErrorCode::REPLICA_IS_NOT_READY;
    tasks_.WithExistingLocked(
        tenant_id, key, [&](PromotionTask& task, bool& /*erase*/) {
            if (task.holder_id != client_id) {
                err = ErrorCode::INVALID_PARAMS;
                return;
            }
            if (!task.allocating || task.alloc_id != 0) {
                err = ErrorCode::REPLICA_IS_NOT_READY;
                return;
            }
            task.alloc_id = alloc_id;
            task.allocating = false;
            task.start_time = std::chrono::system_clock::now();
            err = ErrorCode::OK;
        });
    if (err != ErrorCode::OK) {
        return tl::make_unexpected(err);
    }
    return {};
}

void PromotionManager::AbortAlloc(const UUID& client_id,
                                  const TenantId& tenant_id,
                                  std::string_view key) {
    tasks_.WithExistingLocked(
        tenant_id, key, [&](PromotionTask& task, bool& /*erase*/) {
            if (task.holder_id == client_id && task.alloc_id == 0) {
                task.allocating = false;
            }
        });
}

tl::expected<ReplicaID, ErrorCode> PromotionManager::GetStagedAllocId(
    const UUID& client_id, const TenantId& tenant_id,
    std::string_view key) const {
    tl::expected<ReplicaID, ErrorCode> result =
        tl::make_unexpected(ErrorCode::REPLICA_IS_NOT_READY);
    tasks_.VisitExisting(tenant_id, key, [&](const PromotionTask& task) {
        if (task.alloc_id == 0) {
            result = tl::make_unexpected(ErrorCode::REPLICA_IS_NOT_READY);
            return;
        }
        if (task.holder_id != client_id) {
            result = tl::make_unexpected(ErrorCode::INVALID_PARAMS);
            return;
        }
        result = task.alloc_id;
    });
    return result;
}

tl::expected<bool, ErrorCode> PromotionManager::CompletePromotion(
    const UUID& client_id, const TenantId& tenant_id, std::string_view key,
    bool committed, ObjectMetadata& metadata) {
    ErrorCode err = ErrorCode::REPLICA_IS_NOT_READY;
    bool from_prefetch = false;
    uint64_t completed_bytes = 0;

    tasks_.WithExistingLocked(tenant_id, key,
                              [&](PromotionTask& task, bool& erase_entry) {
                                  if (task.alloc_id == 0) {
                                      err = ErrorCode::REPLICA_IS_NOT_READY;
                                      return;
                                  }
                                  if (task.holder_id != client_id) {
                                      err = ErrorCode::INVALID_PARAMS;
                                      return;
                                  }
                                  from_prefetch = task.from_prefetch;
                                  completed_bytes = task.object_size;
                                  erase_entry = true;
                                  err = ErrorCode::OK;
                              });
    if (err != ErrorCode::OK) {
        return tl::make_unexpected(err);
    }

    MasterMetricManager::instance().dec_promotion_in_flight();
    if (committed) {
        MasterMetricManager::instance().inc_promotion_completed();
        MasterMetricManager::instance().inc_promotion_completed_bytes(
            static_cast<int64_t>(completed_bytes));
        metadata.SyncCacheTotalAccounting();
    } else {
        MasterMetricManager::instance().inc_promotion_cancelled();
    }
    local_ssd_.RemovePromotion(client_id, tenant_id, key);
    return from_prefetch;
}

tl::expected<void, ErrorCode> PromotionManager::FailPromotion(
    const UUID& client_id, const TenantId& tenant_id, std::string_view key,
    ObjectMetadata& metadata) {
    ErrorCode err = ErrorCode::OK;
    bool found = false;
    ReplicaID alloc_id = 0;
    uint32_t prior_failures = 0;
    bool from_prefetch = false;

    tasks_.WithExistingLocked(tenant_id, key,
                              [&](PromotionTask& task, bool& erase_entry) {
                                  if (task.holder_id != client_id) {
                                      err = ErrorCode::INVALID_PARAMS;
                                      return;
                                  }
                                  found = true;
                                  alloc_id = task.alloc_id;
                                  prior_failures = task.execution_failures;
                                  from_prefetch = task.from_prefetch;
                                  erase_entry = true;
                              });
    if (err != ErrorCode::OK) {
        return tl::make_unexpected(err);
    }
    if (!found) {
        return {};
    }

    // Re-record a retry candidate only when a LOCAL_DISK source remains and no
    // non-processing (i.e. already COMPLETE) MEMORY replica exists.
    const bool has_non_staged_memory =
        metadata.HasReplica([](const Replica& replica) {
            return replica.is_memory_replica() && !replica.is_processing();
        });
    const bool can_retry =
        !has_non_staged_memory &&
        metadata.HasReplica(&Replica::fn_is_local_disk_replica);

    PopStagedReplica(metadata, alloc_id);

    MasterMetricManager::instance().dec_promotion_in_flight();
    MasterMetricManager::instance().inc_promotion_failed();

    if (!from_prefetch && can_retry) {
        if (prior_failures >= kMaxPromotionExecutionFailures) {
            LOG(WARNING) << "promotion_execution_gave_up key=" << key
                         << " failures=" << prior_failures;
            MasterMetricManager::instance().inc_promotion_execution_gave_up();
        } else {
            const auto admission_key = tenant_id.MakeScopedKey(key);
            const uint8_t freq =
                promotion_sketch_ ? promotion_sketch_->count(admission_key) : 0;
            promotion_candidates_.Record(
                tenant_id, key, freq,
                PromotionCandidateReason::kExecutionFailed, ErrorCode::OK,
                prior_failures + 1);
        }
    }

    local_ssd_.RemovePromotion(client_id, tenant_id, key);
    return {};
}

// --- Candidate retry helpers ------------------------------------------------

std::vector<PromotionCandidateTracker::Key> PromotionManager::TakeDueCandidates(
    std::chrono::steady_clock::time_point now, size_t limit) {
    if (!promotion_on_hit_) {
        return {};
    }
    return promotion_candidates_.TakeDue(now, limit);
}

void PromotionManager::EraseCandidate(const TenantId& tenant_id,
                                      std::string_view key) {
    promotion_candidates_.Erase(tenant_id, key);
}

void PromotionManager::ReportCandidateOutcome(const TenantId& tenant_id,
                                              std::string_view key,
                                              PromotionQueueResult result) {
    if (result == PromotionQueueResult::kQueued) {
        MasterMetricManager::instance().inc_promotion_candidate_admitted();
    } else if (PromotionCandidateTracker::IsTransient(result)) {
        MasterMetricManager::instance()
            .inc_promotion_candidate_admission_rejected();
        promotion_candidates_.Backoff(tenant_id, key, result);
    } else {
        promotion_candidates_.Erase(tenant_id, key);
    }
}

// --- Lifecycle cleanup & expiry ---------------------------------------------

bool PromotionManager::CancelTaskForRemovedReplicas(
    const TenantId& tenant_id, std::string_view key,
    std::span<const ReplicaID> removed_replica_ids) {
    if (removed_replica_ids.empty()) {
        return false;
    }

    bool cancelled = false;
    UUID holder_id{};
    tasks_.WithExistingLocked(
        tenant_id, key, [&](PromotionTask& task, bool& erase_entry) {
            if (task.alloc_id == 0 ||
                std::find(removed_replica_ids.begin(),
                          removed_replica_ids.end(),
                          task.alloc_id) == removed_replica_ids.end()) {
                return;
            }
            cancelled = true;
            holder_id = task.holder_id;
            erase_entry = true;
        });
    if (!cancelled) {
        return false;
    }

    MasterMetricManager::instance().dec_promotion_in_flight();
    MasterMetricManager::instance().inc_promotion_cancelled();
    local_ssd_.RemovePromotion(holder_id, tenant_id, key);
    return true;
}

void PromotionManager::OnObjectErased(const TenantId& tenant_id,
                                      std::string_view key) {
    if (auto removed = tasks_.Erase(tenant_id, key); removed.has_value()) {
        const UUID holder_id = removed->holder_id;
        removed.reset();
        MasterMetricManager::instance().dec_promotion_in_flight();
        MasterMetricManager::instance().inc_promotion_cancelled();
        local_ssd_.RemovePromotion(holder_id, tenant_id, key);
    }
    promotion_candidates_.Erase(tenant_id, key);
}

std::vector<ScopedObjectKey> PromotionManager::ExpireUnstagedAndCollectStaged(
    const std::chrono::system_clock::time_point& now) {
    std::vector<ScopedObjectKey> staged_expired;
    tasks_.EraseIf([&](const ScopedObjectKey& scoped_key,
                       PromotionTask& task) -> bool {
        if (task.start_time + task_timeout_ > now) {
            return false;
        }
        if (task.alloc_id != 0 || task.allocating) {
            staged_expired.push_back(scoped_key);
            return false;
        }
        LOG(WARNING) << "Promotion task expired for key: " << scoped_key.key;
        MasterMetricManager::instance().dec_promotion_in_flight();
        MasterMetricManager::instance().inc_promotion_expired();
        // Remove pending mailbox entry while holding the shard lock (same lock
        // order as AdmitPromotion: tasks_ shard lock -> local_ssd_ lock).
        local_ssd_.RemovePromotion(task.holder_id, scoped_key.tenant_id,
                                   scoped_key.key);
        return true;
    });
    return staged_expired;
}

void PromotionManager::TryExpireTaskLocked(
    const TenantId& tenant_id, std::string_view key,
    const std::chrono::system_clock::time_point& now,
    ObjectMetadata* metadata) {
    bool expired = false;
    ReplicaID expired_alloc_id = 0;
    UUID holder_id{};
    tasks_.WithExistingLocked(tenant_id, key,
                              [&](PromotionTask& task, bool& erase_entry) {
                                  if (task.start_time + task_timeout_ <= now) {
                                      expired = true;
                                      expired_alloc_id = task.alloc_id;
                                      holder_id = task.holder_id;
                                      erase_entry = true;
                                  }
                              });
    if (expired) {
        LOG(WARNING) << "Promotion task expired for key: " << key;
        MasterMetricManager::instance().dec_promotion_in_flight();
        MasterMetricManager::instance().inc_promotion_expired();
        local_ssd_.RemovePromotion(holder_id, tenant_id, key);
        if (metadata != nullptr) {
            PopStagedReplica(*metadata, expired_alloc_id);
        }
    }
}

// --- Test-only hooks (via MasterServiceTestPeer) ----------------------------

std::optional<PromotionManager::TaskSnapshot> PromotionManager::FindTask(
    const TenantId& tenant_id, std::string_view key) const {
    std::optional<TaskSnapshot> snapshot;
    tasks_.VisitExisting(tenant_id, key, [&](const PromotionTask& task) {
        snapshot = TaskSnapshot{
            .source_id = task.source_id,
            .alloc_id = task.alloc_id,
            .object_size = task.object_size,
            .start_time = task.start_time,
            .holder_id = task.holder_id,
            .execution_failures = task.execution_failures,
            .from_prefetch = task.from_prefetch,
            .allocating = task.allocating,
        };
    });
    return snapshot;
}

std::optional<PromotionCandidate> PromotionManager::FindCandidate(
    const TenantId& tenant_id, std::string_view key) const {
    return promotion_candidates_.Find(tenant_id, key);
}

void PromotionManager::SeedTaskForTesting(const TenantId& tenant_id,
                                          const std::string& key,
                                          PromotionTask task) {
    const bool inserted =
        tasks_.WithShardLocked(tenant_id, key, [&](auto& shard) {
            return shard.InsertOrAssign(tenant_id, key, std::move(task));
        });
    if (inserted) {
        MasterMetricManager::instance().inc_promotion_in_flight();
    }
}

}  // namespace mooncake
