#include "master_service/master_service_test_peer.h"

#include <algorithm>
#include <cassert>
#include <limits>
#include <mutex>

#ifdef USE_NOF
#include "spdk/spdk_wrapper.h"
#endif

namespace mooncake::test {

ErrorCode MasterServiceTestPeer::SetBatchOpLogBackendForTesting(
    std::shared_ptr<HaKvBackend> backend) {
    // Explicit test injection keeps the zero-view fixture API. A configured
    // view still exercises the production fenced path.
    return service_.InitializeBatchOpLogWriter(std::move(backend),
                                               service_.view_version_ > 0);
}

void MasterServiceTestPeer::SetBatchOpLogWriterFactoryForTesting(
    MasterService::BatchOpLogWriterFactory factory) {
    assert(factory);
    assert(!service_.ordered_oplog_writer_);
    service_.batch_oplog_writer_factory_ = std::move(factory);
}

std::string MasterServiceTestPeer::SerializeMetadataForOpLogForTesting(
    MasterService& service, const ObjectMetadata& metadata) {
    return service.SerializeMetadataForOpLog(metadata);
}

void MasterServiceTestPeer::RunBatchEvictForTesting(
    double evict_ratio_target, double evict_ratio_lowerbound) {
    service_.BatchEvict(evict_ratio_target, evict_ratio_lowerbound);
}

void MasterServiceTestPeer::StopEvictionWorkerForTesting() {
    service_.eviction_running_ = false;
    if (service_.eviction_thread_.joinable()) {
        service_.eviction_thread_.join();
    }
}

void MasterServiceTestPeer::SetEvictionCensusHookForTesting(
    std::function<void()> hook) {
    std::lock_guard<std::recursive_mutex> lock(
        service_.eviction_continuation_mutex_);
    service_.eviction_census_hook_for_testing_ = std::move(hook);
}

void MasterServiceTestPeer::PrepareSoftPinSecondPassForTesting(
    size_t shard_idx, std::chrono::system_clock::time_point cutoff) {
    std::lock_guard<std::recursive_mutex> lock(
        service_.eviction_continuation_mutex_);
    MasterService::EvictionContinuation continuation;
    continuation.eviction_base = 1;
    continuation.phase =
        MasterService::EvictionContinuationPhase::kSecondPassSoftPin;
    continuation.second_pass.next_shard = shard_idx;
    continuation.second_pass.scanned_shards = kNumShards - 1;
    continuation.second_pass.cutoff = cutoff;
    continuation.second_pass.remaining_target = 1;
    continuation.second_pass.allow_soft_pinned = true;
    service_.eviction_continuation_ = std::move(continuation);
}

void MasterServiceTestPeer::DisableSoftPinForTesting(const TenantId& tenant_id,
                                                     const std::string& key) {
    MetadataAccessorRW accessor(&service_, ObjectIdentity{tenant_id, key});
    if (!accessor.Exists()) {
        return;
    }

    auto& metadata = accessor.Get();
    const auto soft_pin_deadline = metadata.GetCommittedSoftPinTimeout();
    if (!soft_pin_deadline) {
        return;
    }
    const auto shard_idx = service_.GetMetadataShardIndex(metadata);
    {
        SpinLocker locker(&metadata.lock);
        metadata.soft_pin_timeout.reset();
    }
    service_.soft_pin_deadline_index_.RemoveIfMatches(
        tenant_id.MakeScopedKey(key), shard_idx, *soft_pin_deadline);
    MasterService::ApplySoftPinMetricDelta(-1);
}

uint64_t MasterServiceTestPeer::GetEvictionCensusRunsForTesting() const {
    std::lock_guard<std::recursive_mutex> lock(
        service_.eviction_continuation_mutex_);
    return service_.eviction_census_runs_;
}

bool MasterServiceTestPeer::HasEvictionContinuationForTesting() const {
    std::lock_guard<std::recursive_mutex> lock(
        service_.eviction_continuation_mutex_);
    return service_.eviction_continuation_.has_value();
}

size_t MasterServiceTestPeer::GetEvictionDeadlineCapacityForTesting() const {
    std::lock_guard<std::recursive_mutex> lock(
        service_.eviction_continuation_mutex_);
    if (!service_.eviction_continuation_) return 0;
    return service_.eviction_continuation_->no_soft_pin_deadlines.capacity() +
           service_.eviction_continuation_->soft_pin_deadlines.capacity();
}

uint64_t MasterServiceTestPeer::GetObjectGenerationForTesting(
    const TenantId& tenant_id, const std::string& key) const {
    const size_t shard_idx = service_.getShardIndex(tenant_id, key);
    MasterService::MetadataShardAccessorRO shard(&service_, shard_idx);
    auto tenant_it = shard->tenants.find(tenant_id);
    if (tenant_it == shard->tenants.end()) {
        return 0;
    }
    auto object_it = tenant_it->second.metadata.find(key);
    if (object_it == tenant_it->second.metadata.end()) {
        return 0;
    }
    return object_it->second.generation;
}

size_t MasterServiceTestPeer::GetMetadataShardIndexForTesting(
    const TenantId& tenant_id, const std::string& key) const {
    return service_.getShardIndex(tenant_id, key);
}

std::string MasterServiceTestPeer::GetEvictionContinuationPhaseForTesting()
    const {
    std::lock_guard<std::recursive_mutex> lock(
        service_.eviction_continuation_mutex_);
    if (!service_.eviction_continuation_) {
        return "none";
    }
    switch (service_.eviction_continuation_->phase) {
        case MasterService::EvictionContinuationPhase::kPrimary:
            return "primary";
        case MasterService::EvictionContinuationPhase::kSecondPassNoSoftPin:
            return "second_pass_no_soft_pin";
        case MasterService::EvictionContinuationPhase::kSecondPassSoftPin:
            return "second_pass_soft_pin";
    }
    return "unknown";
}

std::pair<size_t, size_t>
MasterServiceTestPeer::GetEvictionSecondPassPendingCursorForTesting() const {
    std::lock_guard<std::recursive_mutex> lock(
        service_.eviction_continuation_mutex_);
    if (!service_.eviction_continuation_) {
        return {0, 0};
    }
    const auto& second_pass = service_.eviction_continuation_->second_pass;
    return {second_pass.pending_candidates.size(), second_pass.next_pending};
}

std::pair<size_t, size_t>
MasterServiceTestPeer::GetEvictionGroupMemberCursorForTesting() const {
    std::lock_guard<std::recursive_mutex> lock(
        service_.eviction_continuation_mutex_);
    if (!service_.eviction_continuation_ ||
        !service_.eviction_continuation_->group_progress) {
        return {0, 0};
    }
    const auto& progress = *service_.eviction_continuation_->group_progress;
    return {progress.members.size(), progress.next_member};
}

uint64_t MasterServiceTestPeer::GetEvictionContinuationStaleSkipsForTesting()
    const {
    std::lock_guard<std::recursive_mutex> lock(
        service_.eviction_continuation_mutex_);
    return service_.eviction_continuation_stale_skips_;
}

void MasterServiceTestPeer::RunNoFBatchEvictForTesting(
    double evict_ratio_target, double evict_ratio_lowerbound) {
    service_.NoFBatchEvict(evict_ratio_target, evict_ratio_lowerbound);
}

void MasterServiceTestPeer::RunDfsEvictionForTesting() {
    service_.RunDfsEviction();
}

void MasterServiceTestPeer::RunTenantEvictForTesting() {
    service_.EvictTenantsOverWatermark();
}

void MasterServiceTestPeer::SetKvTenantEpochTrackingForTesting(bool enabled) {
    service_.kv_track_tenant_epochs_ = enabled;
}

void MasterServiceTestPeer::SetRemoveAllShardHookForTesting(
    std::function<void(size_t)> hook) {
    service_.kv_remove_all_shard_hook_ = std::move(hook);
}

uint64_t MasterServiceTestPeer::GetKvClearedPublishedForTesting() const {
    return service_.kv_cleared_published_.load(std::memory_order_relaxed);
}

uint64_t MasterServiceTestPeer::GetKvClearedSuppressedForTesting() const {
    return service_.kv_cleared_suppressed_by_epoch_.load(
        std::memory_order_relaxed);
}

void MasterServiceTestPeer::SetNoFProbeFnForTesting(
    MasterService::NoFProbeFn fn) {
#ifdef USE_NOF
    std::lock_guard<std::mutex> lock(service_.nof_probe_fn_mutex_);
    if (fn) {
        service_.nof_probe_fn_ = std::move(fn);
        return;
    }
    service_.nof_probe_fn_ = [](const std::string& te_endpoint,
                                uint32_t timeout_ms,
                                std::string* error_reason) {
        return SpdkWrapper::GetInstance().ProbeNofSegment(
            te_endpoint, timeout_ms, error_reason);
    };
#else
    (void)fn;
#endif
}

size_t MasterServiceTestPeer::GetMountedNoFSegmentCountForTesting() {
    std::vector<MountedNoFSegmentSnapshot> mounted_segments;
    service_.nof_segment_manager_.GetMountedSegmentsSnapshot(mounted_segments);
    return mounted_segments.size();
}

bool MasterServiceTestPeer::IsNoFSegmentMountedForTesting(
    const UUID& segment_id) {
    std::vector<MountedNoFSegmentSnapshot> mounted_segments;
    service_.nof_segment_manager_.GetMountedSegmentsSnapshot(mounted_segments);
    return std::any_of(
        mounted_segments.begin(), mounted_segments.end(),
        [&segment_id](const MountedNoFSegmentSnapshot& snapshot) {
            return snapshot.segment_id == segment_id &&
                   snapshot.status == SegmentStatus::OK;
        });
}

std::optional<uint32_t>
MasterServiceTestPeer::GetNoFHeartbeatFailureCountForTesting(
    const UUID& segment_id) {
    std::lock_guard<std::mutex> lock(service_.nof_heartbeat_mutex_);
    auto it = service_.nof_heartbeat_states_.find(segment_id);
    if (it == service_.nof_heartbeat_states_.end()) {
        return std::nullopt;
    }
    return it->second.consecutive_failures;
}

size_t MasterServiceTestPeer::RunPromotionCandidateRetryForTesting() {
    return service_.RunPromotionCandidateRetry();
}

size_t MasterServiceTestPeer::CountCandidatesForTesting(
    const TenantId& tenant_id) {
    // The count is the tenant's share of the retry index; the candidate state
    // itself lives on each object.
    const auto& tracker = service_.promotion_candidates_;
    std::lock_guard<std::mutex> lock(tracker.mutex_);
    return std::count_if(
        tracker.due_.begin(), tracker.due_.end(),
        [&](const auto& entry) { return entry.first.tenant_id == tenant_id; });
}

void MasterServiceTestPeer::ResetCandidateBackoffsForTesting() {
    const auto epoch = std::chrono::steady_clock::time_point{};
    auto& tracker = service_.promotion_candidates_;
    // The index only names keys, so each key is resolved again under its
    // shard lock and refiled as due now.
    for (const auto& [tenant_id, key] :
         tracker.Due(std::chrono::steady_clock::time_point::max(),
                     std::numeric_limits<size_t>::max())) {
        MasterService::MetadataAccessorRW accessor(
            &service_, MasterService::ObjectIdentity{.tenant_id = tenant_id,
                                                     .user_key = key});
        if (accessor.Exists() && accessor.GetState().promotion_candidate) {
            accessor.GetState().promotion_candidate->retry_after = epoch;
            tracker.File(tenant_id, key, epoch);
        }
    }
}

size_t MasterServiceTestPeer::SoftPinHeapSize() const {
    const auto& index = service_.soft_pin_deadline_index_;
    std::lock_guard lock(index.mutex_);
    return index.heap_.size();
}

size_t MasterServiceTestPeer::SoftPinRegistrationCount() const {
    const auto& index = service_.soft_pin_deadline_index_;
    std::lock_guard lock(index.mutex_);
    return index.registrations_.size();
}

}  // namespace mooncake::test
