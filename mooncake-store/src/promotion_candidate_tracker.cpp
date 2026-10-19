#include "promotion_candidate_tracker.h"

#include <glog/logging.h>

#include <algorithm>

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

std::vector<PromotionCandidateTracker::Key> PromotionCandidateTracker::Due(
    TimePoint now, size_t limit) const {
    std::vector<Key> keys;
    std::lock_guard<std::mutex> lock(mutex_);
    for (auto it = order_.begin();
         it != order_.end() && it->first <= now && keys.size() < limit; ++it) {
        keys.push_back(*it->second);
    }
    return keys;
}

// --- Per-object candidate ----------------------------------------------------

void PromotionCandidateTracker::RecordLocked(const TenantId& tenant_id,
                                             const std::string& key,
                                             route::ObjectState& state,
                                             uint8_t sketch_score,
                                             PromotionCandidateReason reason,
                                             ErrorCode last_error,
                                             uint32_t execution_failures) {
    const auto now = std::chrono::steady_clock::now();
    if (state.promotion_candidate) {
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
        if (File(tenant_id, key, now)) {
            count_.fetch_add(1, std::memory_order_relaxed);
        }
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

    state.promotion_candidate.emplace(
        PromotionCandidate{.sketch_score = sketch_score,
                           .first_seen = now,
                           .last_seen = now,
                           .retry_after = now,
                           .last_reason = reason,
                           .last_error = last_error,
                           .retry_count = 0,
                           .execution_failures = execution_failures});
    if (!File(tenant_id, key, now)) {
        // A stale entry the object left behind holds a slot of its own.
        DecrementCount();
    }
    MasterMetricManager::instance().inc_promotion_candidate_recorded();
    VLOG(1) << "promotion_candidate_recorded key=" << key;
}

void PromotionCandidateTracker::EraseLocked(const TenantId& tenant_id,
                                            const std::string& key,
                                            route::ObjectState& state) {
    if (!state.promotion_candidate) {
        return;
    }
    state.promotion_candidate.reset();
    if (Unfile(tenant_id, key)) {
        DecrementCount();
    }
}

uint32_t PromotionCandidateTracker::ConsumeLocked(const TenantId& tenant_id,
                                                  const std::string& key,
                                                  route::ObjectState& state) {
    if (!state.promotion_candidate) {
        return 0;
    }
    const uint32_t execution_failures =
        state.promotion_candidate->execution_failures;
    EraseLocked(tenant_id, key, state);
    return execution_failures;
}

bool PromotionCandidateTracker::DueLocked(const TenantId& tenant_id,
                                          const std::string& key,
                                          route::ObjectState& state,
                                          TimePoint now) {
    if (!state.promotion_candidate) {
        return false;
    }
    const PromotionCandidate& candidate = *state.promotion_candidate;
    if (Stale(candidate, now)) {
        VLOG(1) << "promotion_candidate_expired key=" << key
                << " retry_count=" << candidate.retry_count;
        // retry_count == 0: the TTL elapsed before the retry loop reached it,
        // so the per-round batch was too small. retry_count > 0: it gave up
        // after retries.
        if (candidate.retry_count == 0) {
            MasterMetricManager::instance()
                .inc_promotion_candidate_expired_unevaluated();
        } else {
            MasterMetricManager::instance()
                .inc_promotion_candidate_expired_evaluated();
        }
        EraseLocked(tenant_id, key, state);
        return false;
    }
    return candidate.retry_after <= now;
}

void PromotionCandidateTracker::BackoffLocked(const TenantId& tenant_id,
                                              const std::string& key,
                                              route::ObjectState& state,
                                              PromotionQueueResult result) {
    if (!state.promotion_candidate) {
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
        VLOG(1) << "promotion_candidate_gave_up key=" << key
                << " retries=" << candidate.retry_count;
        EraseLocked(tenant_id, key, state);
        MasterMetricManager::instance()
            .inc_promotion_candidate_expired_evaluated();
    } else {
        candidate.retry_after = now + Backoff(candidate.retry_count);
        File(tenant_id, key, candidate.retry_after);
    }
}

void PromotionCandidateTracker::ForgetLocked(const TenantId& tenant_id,
                                             const std::string& key) {
    if (Unfile(tenant_id, key)) {
        DecrementCount();
    }
}

void PromotionCandidateTracker::Reset() {
    std::lock_guard<std::mutex> lock(mutex_);
    order_.clear();
    due_.clear();
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

bool PromotionCandidateTracker::Stale(const PromotionCandidate& candidate,
                                      TimePoint now) {
    return now - candidate.last_seen >= kTtl ||
           candidate.retry_count >= kMaxRetries;
}

bool PromotionCandidateTracker::File(const TenantId& tenant_id,
                                     const std::string& key, TimePoint due) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto [it, inserted] = due_.try_emplace(Key{tenant_id, key}, due);
    if (!inserted) {
        order_.erase({it->second, &it->first});
        it->second = due;
    }
    order_.emplace(due, &it->first);
    return inserted;
}

bool PromotionCandidateTracker::Unfile(const TenantId& tenant_id,
                                       const std::string& key) {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto it = due_.find(Key{tenant_id, key});
    if (it == due_.end()) {
        return false;
    }
    order_.erase({it->second, &it->first});
    due_.erase(it);
    return true;
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
