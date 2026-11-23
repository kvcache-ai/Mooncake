#include "promotion_candidate_tracker.h"

#include <glog/logging.h>

#include <algorithm>
#include <utility>

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

void PromotionCandidateTracker::Record(const TenantId& tenant_id,
                                       std::string_view key,
                                       uint8_t sketch_score,
                                       PromotionCandidateReason reason,
                                       ErrorCode last_error,
                                       uint32_t execution_failures) {
    const auto now = std::chrono::steady_clock::now();
    const uint64_t h = ShardPresenceFilter::MixedHash(tenant_id, key);
    Shard& shard = ShardForHash(h);
    std::lock_guard<std::mutex> lock(shard.mutex);

    auto it = shard.entries.find(ScopedObjectKeyView{&tenant_id, key});
    if (it != shard.entries.end()) {
        // Refresh existing entry: extend last_seen and reset retry budget.
        // execution_failures is intentionally preserved across read refreshes.
        PromotionCandidate& candidate = it->second;
        shard.order.erase({candidate.retry_after, &it->first});
        candidate.last_seen = now;
        candidate.last_reason = reason;
        candidate.last_error = last_error;
        if (sketch_score > candidate.sketch_score) {
            candidate.sketch_score = sketch_score;
        }
        candidate.retry_after = now;
        candidate.retry_count = 0;
        shard.order.emplace(now, &it->first);
        RefreshMinDueLocked(shard);
        return;
    }

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

    auto [inserted_it, inserted] = shard.entries.emplace(
        Key{tenant_id, std::string(key)},
        PromotionCandidate{.sketch_score = sketch_score,
                           .first_seen = now,
                           .last_seen = now,
                           .retry_after = now,
                           .last_reason = reason,
                           .last_error = last_error,
                           .retry_count = 0,
                           .execution_failures = execution_failures});
    (void)inserted;
    shard.order.emplace(now, &inserted_it->first);
    shard.filter.InsertLocked(h);
    shard.size.store(shard.entries.size(), std::memory_order_relaxed);
    RefreshMinDueLocked(shard);

    MasterMetricManager::instance().inc_promotion_candidate_recorded();
    VLOG(1) << "promotion_candidate_recorded key=" << key;
}

void PromotionCandidateTracker::Erase(const TenantId& tenant_id,
                                      std::string_view key) {
    if (Empty()) {
        return;
    }
    const uint64_t h = ShardPresenceFilter::MixedHash(tenant_id, key);
    Shard& shard = ShardForHash(h);
    if (shard.size.load(std::memory_order_relaxed) == 0 ||
        !shard.filter.MayContain(h)) {
        return;
    }
    std::lock_guard<std::mutex> lock(shard.mutex);
    auto it = shard.entries.find(ScopedObjectKeyView{&tenant_id, key});
    if (it != shard.entries.end()) {
        EraseEntryLocked(shard, it, h);
    }
}

uint32_t PromotionCandidateTracker::Consume(const TenantId& tenant_id,
                                            std::string_view key) {
    if (Empty()) {
        return 0;
    }
    const uint64_t h = ShardPresenceFilter::MixedHash(tenant_id, key);
    Shard& shard = ShardForHash(h);
    if (shard.size.load(std::memory_order_relaxed) == 0 ||
        !shard.filter.MayContain(h)) {
        return 0;
    }
    std::lock_guard<std::mutex> lock(shard.mutex);
    auto it = shard.entries.find(ScopedObjectKeyView{&tenant_id, key});
    if (it == shard.entries.end()) {
        return 0;
    }
    const uint32_t execution_failures = it->second.execution_failures;
    EraseEntryLocked(shard, it, h);
    return execution_failures;
}

std::vector<PromotionCandidateTracker::Key> PromotionCandidateTracker::TakeDue(
    TimePoint now, size_t limit) {
    if (limit == 0 || Empty()) {
        return {};
    }

    const int64_t now_ns = ToNs(now);
    std::array<std::pair<int64_t, size_t>, kShardCount> active_shards;
    size_t active_count = 0;
    for (size_t i = 0; i < kShardCount; ++i) {
        if (shards_[i].size.load(std::memory_order_relaxed) == 0) {
            continue;
        }
        const int64_t min_due =
            shards_[i].min_due_ns.load(std::memory_order_relaxed);
        if (min_due <= now_ns) {
            active_shards[active_count++] = {min_due, i};
        }
    }
    if (active_count == 0) {
        return {};
    }
    std::sort(active_shards.begin(),
              active_shards.begin() + static_cast<std::ptrdiff_t>(active_count),
              [](const auto& a, const auto& b) { return a.first < b.first; });

    using DueEntry = std::pair<TimePoint, Key>;
    auto max_due_cmp = [](const DueEntry& a, const DueEntry& b) {
        return a.first < b.first;
    };
    std::vector<DueEntry> heap;
    heap.reserve(limit);
    size_t stale_budget = std::max<size_t>(limit * 4, 256);

    for (size_t idx = 0; idx < active_count; ++idx) {
        const auto [shard_min_due_ns, shard_idx] = active_shards[idx];
        if (heap.size() == limit &&
            ToNs(heap.front().first) <= shard_min_due_ns) {
            break;
        }
        Shard& shard = shards_[shard_idx];
        std::lock_guard<std::mutex> lock(shard.mutex);
        bool erased_any = false;
        auto it = shard.order.begin();
        while (it != shard.order.end() && it->first <= now) {
            if (heap.size() == limit && it->first >= heap.front().first) {
                break;
            }
            const Key* key_ptr = it->second;
            auto map_it = shard.entries.find(*key_ptr);
            if (map_it == shard.entries.end()) {
                it = shard.order.erase(it);
                erased_any = true;
                continue;
            }
            const PromotionCandidate& candidate = map_it->second;
            if (Stale(candidate, now)) {
                if (stale_budget == 0) {
                    break;
                }
                --stale_budget;
                VLOG(1) << "promotion_candidate_expired key=" << key_ptr->key
                        << " retry_count=" << candidate.retry_count;
                if (candidate.retry_count == 0) {
                    MasterMetricManager::instance()
                        .inc_promotion_candidate_expired_unevaluated();
                } else {
                    MasterMetricManager::instance()
                        .inc_promotion_candidate_expired_evaluated();
                }
                const uint64_t h = ShardPresenceFilter::MixedHash(
                    key_ptr->tenant_id, key_ptr->key);
                it = shard.order.erase(it);
                shard.entries.erase(map_it);
                shard.filter.EraseLocked(h, shard.entries.empty());
                DecrementCount();
                erased_any = true;
                continue;
            }
            if (heap.size() < limit) {
                heap.emplace_back(it->first, *key_ptr);
                std::push_heap(heap.begin(), heap.end(), max_due_cmp);
            } else {
                std::pop_heap(heap.begin(), heap.end(), max_due_cmp);
                heap.back() = {it->first, *key_ptr};
                std::push_heap(heap.begin(), heap.end(), max_due_cmp);
            }
            ++it;
        }
        if (erased_any) {
            shard.size.store(shard.entries.size(), std::memory_order_relaxed);
            RefreshMinDueLocked(shard);
            ShrinkBucketsIfSparse(shard.entries);
        }
    }

    std::sort_heap(heap.begin(), heap.end(), max_due_cmp);
    std::vector<Key> keys;
    keys.reserve(heap.size());
    for (auto& [_, k] : heap) {
        keys.push_back(std::move(k));
    }
    return keys;
}

void PromotionCandidateTracker::Backoff(const TenantId& tenant_id,
                                        std::string_view key,
                                        PromotionQueueResult result) {
    if (Empty()) {
        return;
    }
    const uint64_t h = ShardPresenceFilter::MixedHash(tenant_id, key);
    Shard& shard = ShardForHash(h);
    if (shard.size.load(std::memory_order_relaxed) == 0 ||
        !shard.filter.MayContain(h)) {
        return;
    }
    const auto now = std::chrono::steady_clock::now();
    std::lock_guard<std::mutex> lock(shard.mutex);
    auto it = shard.entries.find(ScopedObjectKeyView{&tenant_id, key});
    if (it == shard.entries.end()) {
        return;
    }

    PromotionCandidate& candidate = it->second;
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
        EraseEntryLocked(shard, it, h);
        MasterMetricManager::instance()
            .inc_promotion_candidate_expired_evaluated();
    } else {
        shard.order.erase({candidate.retry_after, &it->first});
        candidate.retry_after = now + BackoffDelay(candidate.retry_count);
        shard.order.emplace(candidate.retry_after, &it->first);
        RefreshMinDueLocked(shard);
    }
}

std::optional<PromotionCandidate> PromotionCandidateTracker::Find(
    const TenantId& tenant_id, std::string_view key) const {
    if (Empty()) {
        return std::nullopt;
    }
    const uint64_t h = ShardPresenceFilter::MixedHash(tenant_id, key);
    const Shard& shard = ShardForHash(h);
    if (shard.size.load(std::memory_order_relaxed) == 0 ||
        !shard.filter.MayContain(h)) {
        return std::nullopt;
    }
    std::lock_guard<std::mutex> lock(shard.mutex);
    auto it = shard.entries.find(ScopedObjectKeyView{&tenant_id, key});
    if (it == shard.entries.end()) {
        return std::nullopt;
    }
    return it->second;
}

size_t PromotionCandidateTracker::CountByTenant(
    const TenantId& tenant_id) const {
    if (Empty()) {
        return 0;
    }
    size_t total = 0;
    for (const Shard& shard : shards_) {
        if (shard.size.load(std::memory_order_relaxed) == 0) {
            continue;
        }
        std::lock_guard<std::mutex> lock(shard.mutex);
        for (const auto& [k, _] : shard.entries) {
            if (k.tenant_id == tenant_id) {
                ++total;
            }
        }
    }
    return total;
}

void PromotionCandidateTracker::RefreshMinDueLocked(Shard& shard) noexcept {
    if (shard.order.empty()) {
        shard.min_due_ns.store(kNoDueNs, std::memory_order_relaxed);
    } else {
        shard.min_due_ns.store(ToNs(shard.order.begin()->first),
                               std::memory_order_relaxed);
    }
}

std::chrono::milliseconds PromotionCandidateTracker::BackoffDelay(
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
    return now >= candidate.last_seen + kTtl ||
           candidate.retry_count >= kMaxRetries;
}

void PromotionCandidateTracker::EraseEntryLocked(Shard& shard,
                                                 EntryMap::iterator it,
                                                 uint64_t mixed_hash) {
    shard.order.erase({it->second.retry_after, &it->first});
    shard.entries.erase(it);
    shard.filter.EraseLocked(mixed_hash, shard.entries.empty());
    shard.size.store(shard.entries.size(), std::memory_order_relaxed);
    RefreshMinDueLocked(shard);
    ShrinkBucketsIfSparse(shard.entries);
    DecrementCount();
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

void PromotionCandidateTracker::ResetBackoffs() {
    const TimePoint epoch{};
    for (Shard& shard : shards_) {
        if (shard.size.load(std::memory_order_relaxed) == 0) {
            continue;
        }
        std::lock_guard<std::mutex> lock(shard.mutex);
        shard.order.clear();
        for (auto& [key, candidate] : shard.entries) {
            candidate.retry_after = epoch;
            shard.order.emplace(epoch, &key);
        }
        RefreshMinDueLocked(shard);
    }
}

}  // namespace mooncake
