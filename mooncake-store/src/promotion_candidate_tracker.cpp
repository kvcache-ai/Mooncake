#include "promotion_candidate_tracker.h"

#include <glog/logging.h>

#include <algorithm>

#include "common/shrink_buckets.h"
#include "master_metric_manager.h"

namespace mooncake {

bool PromotionCandidateTracker::Empty() const {
    return count_.load(std::memory_order_relaxed) == 0;
}

bool PromotionCandidateTracker::IsTransient(PromotionQueueResult result) {
    return result == PromotionQueueResult::kWatermarkRejected ||
           result == PromotionQueueResult::kQueueCapRejected ||
           result == PromotionQueueResult::kPushFailed;
}

std::vector<std::string> PromotionCandidateTracker::Keys(
    const TenantId& tenant_id) const {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto it = keys_.find(tenant_id);
    if (it == keys_.end()) {
        return {};
    }
    return {it->second.begin(), it->second.end()};
}

// --- Per-entry candidate ----------------------------------------------------

void PromotionCandidateTracker::RecordLocked(const TenantId& tenant_id,
                                             const ObjectEntry& entry,
                                             ObjectEntry::State& state,
                                             uint8_t sketch_score,
                                             PromotionCandidateReason reason,
                                             ErrorCode last_error,
                                             uint32_t execution_failures) {
    const auto now = std::chrono::steady_clock::now();
    const std::string& key = entry.key();
    if (state.promotion_candidate.has_value()) {
        // Update existing entry: refresh last_seen, reset
        // retry_after/retry_count. execution_failures is intentionally NOT
        // updated here: a read refresh is new demand signal and may extend
        // the budget, but it must not erase the failure history of this
        // admission chain.
        PromotionCandidate& candidate = *state.promotion_candidate;
        candidate.last_seen = now;
        candidate.last_reason = reason;
        candidate.last_error = last_error;
        if (sketch_score > candidate.sketch_score) {
            candidate.sketch_score = sketch_score;
        }
        candidate.retry_after = now;
        candidate.retry_count = 0;
        return;
    }

    // Reserve a slot in the global candidate limit.
    uint64_t count = count_.load(std::memory_order_relaxed);
    while (count < kLimit) {
        if (count_.compare_exchange_weak(count, count + 1,
                                         std::memory_order_relaxed)) {
            break;
        }
    }
    if (count >= kLimit) {
        VLOG(1) << "promotion_candidate_dropped key=" << key
                << " reason=global_limit";
        MasterMetricManager::instance().inc_promotion_candidate_dropped_limit();
        return;
    }

    state.promotion_candidate =
        PromotionCandidate{.sketch_score = sketch_score,
                           .first_seen = now,
                           .last_seen = now,
                           .retry_after = now,
                           .last_reason = reason,
                           .last_error = last_error,
                           .retry_count = 0,
                           .execution_failures = execution_failures};
    Index(tenant_id, key);
    MasterMetricManager::instance().inc_promotion_candidate_recorded();
    VLOG(1) << "promotion_candidate_recorded key=" << key;
}

void PromotionCandidateTracker::EraseLocked(const TenantId& tenant_id,
                                            const ObjectEntry& entry,
                                            ObjectEntry::State& state) {
    if (!state.promotion_candidate.has_value()) {
        return;
    }
    state.promotion_candidate.reset();
    Unindex(tenant_id, entry.key());
    DecrementCount();
}

uint32_t PromotionCandidateTracker::ConsumeLocked(const TenantId& tenant_id,
                                                  const ObjectEntry& entry,
                                                  ObjectEntry::State& state) {
    if (!state.promotion_candidate.has_value()) {
        return 0;
    }
    const uint32_t execution_failures =
        state.promotion_candidate->execution_failures;
    EraseLocked(tenant_id, entry, state);
    return execution_failures;
}

bool PromotionCandidateTracker::DueLocked(
    const TenantId& tenant_id, const ObjectEntry& entry,
    ObjectEntry::State& state, std::chrono::steady_clock::time_point now) {
    if (!state.promotion_candidate.has_value()) {
        return false;
    }
    const PromotionCandidate& candidate = *state.promotion_candidate;
    if (Stale(candidate, now)) {
        VLOG(1) << "promotion_candidate_expired key=" << entry.key()
                << " retry_count=" << candidate.retry_count;
        // retry_count == 0: the TTL elapsed before the scheduler reached it,
        // so the scan budget was too small. retry_count > 0: it gave up after
        // retries.
        if (candidate.retry_count == 0) {
            MasterMetricManager::instance()
                .inc_promotion_candidate_expired_unevaluated();
        } else {
            MasterMetricManager::instance()
                .inc_promotion_candidate_expired_evaluated();
        }
        EraseLocked(tenant_id, entry, state);
        return false;
    }
    return candidate.retry_after <= now;
}

void PromotionCandidateTracker::BackoffLocked(const TenantId& tenant_id,
                                              const ObjectEntry& entry,
                                              ObjectEntry::State& state,
                                              PromotionQueueResult result) {
    if (!state.promotion_candidate.has_value()) {
        return;
    }
    const auto now = std::chrono::steady_clock::now();
    PromotionCandidate& candidate = *state.promotion_candidate;
    candidate.retry_count++;
    if (result == PromotionQueueResult::kWatermarkRejected) {
        candidate.last_reason = PromotionCandidateReason::kWatermark;
        candidate.last_error = ErrorCode::OK;
    } else if (result == PromotionQueueResult::kQueueCapRejected) {
        candidate.last_reason = PromotionCandidateReason::kQueueCap;
        candidate.last_error = ErrorCode::OK;
    } else {
        candidate.last_reason = PromotionCandidateReason::kPushFailed;
    }

    if (Stale(candidate, now)) {
        VLOG(1) << "promotion_candidate_gave_up key=" << entry.key()
                << " retries=" << candidate.retry_count;
        EraseLocked(tenant_id, entry, state);
        MasterMetricManager::instance()
            .inc_promotion_candidate_expired_evaluated();
    } else {
        candidate.retry_after = now + Backoff(candidate.retry_count);
    }
}

void PromotionCandidateTracker::Reset() {
    std::lock_guard<std::mutex> lock(mutex_);
    keys_.clear();
    count_.store(0, std::memory_order_relaxed);
}

// --- Internals --------------------------------------------------------------

std::chrono::milliseconds PromotionCandidateTracker::Backoff(
    uint32_t retry_count) {
    uint64_t backoff_ms = static_cast<uint64_t>(kInitialBackoff.count());
    for (uint32_t i = 1; i < retry_count; ++i) {
        backoff_ms = std::min<uint64_t>(
            backoff_ms * 2, static_cast<uint64_t>(kMaxBackoff.count()));
    }
    return std::chrono::milliseconds(backoff_ms);
}

bool PromotionCandidateTracker::Stale(
    const PromotionCandidate& candidate,
    std::chrono::steady_clock::time_point now) {
    return now - candidate.last_seen >= kTtl ||
           candidate.retry_count >= kMaxRetries;
}

void PromotionCandidateTracker::Index(const TenantId& tenant_id,
                                      const std::string& key) {
    std::lock_guard<std::mutex> lock(mutex_);
    keys_[tenant_id].insert(key);
}

void PromotionCandidateTracker::Unindex(const TenantId& tenant_id,
                                        const std::string& key) {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto it = keys_.find(tenant_id);
    if (it == keys_.end()) {
        return;
    }
    it->second.erase(key);
    if (it->second.empty()) {
        keys_.erase(it);
        return;
    }
    // erase() never returns bucket memory, so a tenant that lost most of its
    // candidates would keep a high-water bucket array. The shrink is amortized:
    // it rehashes only once the set falls under a quarter of its buckets.
    ShrinkBucketsIfSparse(it->second);
}

void PromotionCandidateTracker::DecrementCount() {
    uint64_t count = count_.load(std::memory_order_relaxed);
    while (count > 0) {
        if (count_.compare_exchange_weak(count, count - 1,
                                         std::memory_order_relaxed)) {
            return;
        }
    }
}

}  // namespace mooncake
