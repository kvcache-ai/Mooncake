#include "tiered_cache/event_driven_scheduler/multi_lru_policy.h"

#include <algorithm>
#include <string>

#include "tiered_cache/tiered_backend.h"  // TieredBackend, TierView, DataSource

namespace mooncake {

namespace {

const TierView* FindView(const std::vector<TierView>& views, UUID id) {
    for (const auto& v : views) {
        if (v.id == id) {
            return &v;
        }
    }
    return nullptr;
}

}  // namespace

MultiLRUPolicy::MultiLRUPolicy(const Config& config)
    : config_(config),
      collector_(config.sketch_capacity),
      evict_watermark_(config.evict_watermark) {}

void MultiLRUPolicy::Init(TieredBackend* backend, UUID fast_tier,
                          std::optional<UUID> slow_tier) {
    backend_ = backend;
    fast_tier_ = fast_tier;
    slow_tier_ = slow_tier;
    collector_.SetFastTier(fast_tier);
    initialized_ = true;
}

size_t MultiLRUPolicy::BestEffortSize(std::string_view key, size_t hint) const {
    if (hint != 0) {
        return hint;
    }
    uint64_t version = 0;
    auto handle = backend_->Get(key, fast_tier_, /*record_access=*/false,
                                &version);
    if (handle.has_value() && handle.value() && handle.value()->loc.data.buffer) {
        return handle.value()->loc.data.buffer->size();
    }
    return 0;
}

std::optional<MovementRequest> MultiLRUPolicy::OnAccess(
    const AccessContext& ctx) {
    // Always update the frequency memory / fast-tier band.
    collector_.OnAccess(ctx.key, ctx.served_tier_id);
    if (!initialized_) {
        return std::nullopt;
    }

    const bool from_fast = (ctx.served_tier_id == fast_tier_);
    const bool from_slow =
        slow_tier_.has_value() && ctx.served_tier_id == *slow_tier_;
    if (!from_fast && !from_slow) {
        return std::nullopt;
    }

    // Cheap frequency gate before any backend query (most accesses are cold).
    const uint64_t freq = collector_.GetAccessFrequency(ctx.key);
    if (from_fast && freq <= config_.offload_freq_threshold) {
        return std::nullopt;
    }
    if (from_slow && freq <= config_.onboard_freq_threshold) {
        return std::nullopt;
    }

    const auto views = backend_->GetTierViews();
    const TierView* fast = FindView(views, fast_tier_);
    if (fast == nullptr) {
        return std::nullopt;
    }
    const auto locations = backend_->GetReplicaTierIds(ctx.key);
    const size_t size = BestEffortSize(ctx.key, ctx.size_bytes);

    if (from_fast) {
        if (!slow_tier_.has_value()) {
            return std::nullopt;
        }
        const TierView* slow = FindView(views, *slow_tier_);
        if (slow == nullptr) {
            return std::nullopt;
        }
        if (ShouldOffload(freq, size, locations, *slow, *slow_tier_)) {
            return MovementRequest{MovementRequest::Kind::kReplicate,
                                   std::string(ctx.key), fast_tier_,
                                   *slow_tier_, size};
        }
        return std::nullopt;
    }

    // from_slow: consider promoting into the fast tier.
    if (ShouldOnboard(freq, locations, *fast)) {
        return MovementRequest{MovementRequest::Kind::kMigrate,
                               std::string(ctx.key), *slow_tier_, fast_tier_,
                               size};
    }
    return std::nullopt;
}

void MultiLRUPolicy::OnCommit(const CommitContext& ctx) {
    // Commit enters the fast-tier MultiLRU (collector decides by tier) and
    // contributes to the write-load watermark. NOT counted as a frequency hit.
    collector_.OnCommit(ctx.key, ctx.tier_id, ctx.size_bytes);
    RecordCommitBytes(ctx.tier_id, ctx.size_bytes);
}

void MultiLRUPolicy::OnDelete(const DeleteContext& ctx) {
    collector_.OnDelete(ctx.key, ctx.tier_id);  // tier-aware
}

void MultiLRUPolicy::RecordCommitBytes(UUID tier_id, size_t bytes) {
    std::lock_guard<std::mutex> lock(mutex_);
    committed_bytes_[tier_id] += bytes;
}

double MultiLRUPolicy::evict_watermark() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return evict_watermark_;
}

std::vector<MovementRequest> MultiLRUPolicy::DecideEvict(
    size_t min_reclaim_bytes) {
    std::vector<MovementRequest> out;
    if (!initialized_) {
        return out;
    }
    const auto views = backend_->GetTierViews();
    const TierView* fast = FindView(views, fast_tier_);
    if (fast == nullptr || fast->capacity == 0) {
        return out;
    }
    const size_t capacity = fast->capacity;
    const size_t used = fast->usage;
    const double usage_ratio = Ratio(used, capacity);

    // Float the trigger watermark from write load (snapshot + reset). The seed
    // (config evict_watermark) is the no-load value; load floats it DOWN toward
    // user_floor (reclaim earlier under write pressure). Trigger only.
    double evict_wm = 0.0;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        size_t bytes_since = 0;
        auto cit = committed_bytes_.find(fast_tier_);
        if (cit != committed_bytes_.end()) {
            bytes_since = cit->second;
            cit->second = 0;
        }
        const double load = std::min(1.0, Ratio(bytes_since, capacity));
        const double seed = config_.evict_watermark;
        const double lo = config_.user_floor;
        evict_watermark_ =
            std::min(std::max(seed - load * (seed - lo), lo), seed);
        evict_wm = evict_watermark_;
    }

    // Proportional reclaim target. Denominator is the STATIC (limit - floor)
    // boundary, never the floating watermark.
    size_t target = 0;
    if (usage_ratio > evict_wm) {
        const double denom = config_.limit_watermark - config_.user_floor;
        double rate = 0.0;
        if (denom > 0.0) {
            rate = config_.evict_rate_k * (usage_ratio - config_.user_floor) /
                   denom;
            rate = std::min(std::max(rate, 0.0), 1.0);
        }
        const double over = static_cast<double>(used) -
                            config_.user_floor * static_cast<double>(capacity);
        if (over > 0.0) {
            target = static_cast<size_t>(rate * over);
        }
    }
    // The allocation-failure path requires reclaiming at least this much even
    // if the proportional target is small.
    target = std::max(target, min_reclaim_bytes);
    if (target == 0) {
        return out;
    }

    // Walk coldest-first victims, dropping the fast-tier copy until the target
    // is met. Eviction NEVER migrates a sole copy down to the slow tier: a
    // synchronous copy would slow the evict loop. Demotion to the slow tier is
    // the (asynchronous) offload path's job — eviction only reaps fast-tier
    // copies. Keys with a pre-demoted slow replica survive there; cold sole
    // copies are simply dropped (a future cache miss). This also avoids a
    // per-victim GetReplicaTierIds lookup, keeping the pass fast.
    const auto candidates =
        collector_.CollectEvictionCandidates(config_.candidate_scan_limit);
    size_t planned = 0;
    for (const auto& e : candidates) {
        MovementRequest mv;
        mv.kind = MovementRequest::Kind::kEvict;
        mv.key = e.key;
        mv.source_tier = fast_tier_;
        mv.size_bytes = e.size_bytes;
        out.push_back(std::move(mv));
        planned += e.size_bytes;
        if (planned >= target) {
            break;
        }
    }
    return out;
}

AccessStats MultiLRUPolicy::GetHotKeyStats(size_t hot_key_num) const {
    return collector_.GetHotKeyStats(hot_key_num);
}

bool MultiLRUPolicy::ShouldOffload(uint64_t freq, size_t size_bytes,
                                   const std::vector<UUID>& locations,
                                   const TierView& slow, UUID slow_id) const {
    if (freq <= config_.offload_freq_threshold) {
        return false;
    }
    if (std::find(locations.begin(), locations.end(), slow_id) !=
        locations.end()) {
        return false;  // already on slow tier
    }
    const size_t slow_free =
        slow.capacity > slow.usage ? slow.capacity - slow.usage : 0;
    return slow_free >= size_bytes;
}

bool MultiLRUPolicy::ShouldOnboard(uint64_t freq,
                                   const std::vector<UUID>& locations,
                                   const TierView& fast) const {
    if (freq <= config_.onboard_freq_threshold) {
        return false;
    }
    if (Ratio(fast.usage, fast.capacity) >= config_.onboard_fast_threshold) {
        return false;  // fast tier above the hysteresis ceiling
    }
    if (std::find(locations.begin(), locations.end(), fast_tier_) !=
        locations.end()) {
        return false;  // already on fast tier
    }
    return true;
}

}  // namespace mooncake
