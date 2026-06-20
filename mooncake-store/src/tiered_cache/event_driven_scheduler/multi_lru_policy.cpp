#include "tiered_cache/event_driven_scheduler/multi_lru_policy.h"

#include <glog/logging.h>

#include <algorithm>
#include <chrono>
#include <cmath>
#include <string>
#include <utility>

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

MultiLRUPolicy::MultiLRUPolicy(const Config& config, ClockFn clock)
    : config_(config),
      collector_(config.sketch_capacity),
      clock_(clock ? std::move(clock)
                   : ClockFn{[] { return std::chrono::steady_clock::now(); }}),
      // Start at the low bound: assume high write load at startup so the tier
      // begins with maximum headroom (avoids an immediate alloc-failure fallback
      // before the first periodic pass measures the real load).
      evict_wm_(config.evict_watermark_low) {}

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
        // Topology inconsistency: the resolved fast tier is gone from the views.
        VLOG(2) << "MultiLRU OnAccess: fast tier view missing for " << ctx.key;
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
            VLOG(2) << "MultiLRU OnAccess: slow tier view missing for "
                    << ctx.key;
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

double MultiLRUPolicy::evict_wm() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return evict_wm_;
}

double MultiLRUPolicy::RefreshEvictWatermark(size_t capacity) {
    // Consume the fast tier's committed-byte delta.
    size_t bytes_since = 0;
    if (auto it = committed_bytes_.find(fast_tier_);
        it != committed_bytes_.end()) {
        bytes_since = std::exchange(it->second, 0);
    }

    // Feed it into a time-decayed write-rate integrator (time constant
    // evict_load_window_s). At steady state load_accum_ ~= rate * window, so the
    // load reflects sustained throughput rather than one short pass's bytes and
    // can reach full scale regardless of how fast this loop runs.
    const auto now = clock_();
    if (load_tp_valid_) {
        const double tau = config_.evict_load_window_s;
        const double dt_s =
            std::chrono::duration<double>(now - last_decay_tp_).count();
        const double decay =
            (tau > 0.0 && dt_s > 0.0) ? std::exp(-dt_s / tau) : 0.0;
        load_accum_ = load_accum_ * decay + static_cast<double>(bytes_since);
    } else {
        load_accum_ = static_cast<double>(bytes_since);
    }
    last_decay_tp_ = now;
    load_tp_valid_ = true;

    // Map load in [0,1] onto [high..low]: more load => lower trigger => evict
    // earlier and keep more headroom.
    const double load =
        capacity == 0
            ? 0.0
            : std::min(1.0, load_accum_ / static_cast<double>(capacity));
    const double low = config_.evict_watermark_low;
    const double high = config_.evict_watermark_high;
    evict_wm_ = std::min(std::max(high - load * (high - low), low), high);
    return evict_wm_;
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
        VLOG(2) << "MultiLRU DecideEvict: fast tier view missing or zero "
                   "capacity; nothing to reclaim";
        return out;
    }
    const size_t capacity = fast->capacity;
    const size_t used = fast->usage;
    const double usage_ratio = Ratio(used, capacity);

    // Refresh the floating trigger from write load on the PERIODIC pass only
    // (min_reclaim_bytes == 0); the allocation-failure path reuses the last
    // value read-only so the two paths don't race on the load accumulator.
    double evict_wm = 0.0;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (min_reclaim_bytes == 0) {
            RefreshEvictWatermark(capacity);
        }
        evict_wm = evict_wm_;
    }

    // Proportional reclaim target. Denominator is the STATIC (limit - low)
    // boundary, never the floating watermark; reclaim targets the bytes above
    // the low bound (which doubles as the reclaim floor).
    size_t target = 0;
    if (usage_ratio > evict_wm) {
        const double denom =
            config_.limit_watermark - config_.evict_watermark_low;
        double rate = 0.0;
        if (denom > 0.0) {
            rate = config_.evict_rate_k *
                   (usage_ratio - config_.evict_watermark_low) / denom;
            rate = std::min(std::max(rate, 0.0), 1.0);
        }
        const double over =
            static_cast<double>(used) -
            config_.evict_watermark_low * static_cast<double>(capacity);
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
    //
    // Design assumption: fast-tier data is reconstructible cache (KVCache), so
    // dropping a cold sole copy costs at most a recompute/refetch. Unlike the
    // legacy path (which MIGRATEs a sole copy down before EVICTing), this trades
    // that guarantee for evict-loop speed. Do NOT route non-cache, single-copy-
    // of-record data through this policy expecting durability.
    const auto candidates =
        collector_.CollectEvictionCandidates(config_.candidate_scan_limit);
    size_t planned = 0;
    for (const auto& e : candidates) {
        if (e.size_bytes == 0) {
            // A zero-size victim cannot advance `planned`, so it would never
            // help meet the target and only churns the loop. Commits already
            // reject zero size, so this is defensive.
            continue;
        }
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
    // Backpressure path could not free enough: surface it instead of silently
    // under-reclaiming (the allocation retry may then still fail). The candidate
    // scan is capped at candidate_scan_limit, so a large cold working set can
    // leave a single pass short.
    if (min_reclaim_bytes > 0 && planned < min_reclaim_bytes) {
        LOG_EVERY_N(WARNING, 50)
            << "MultiLRU allocation-failure reclaim fell short: freed "
            << planned << " < required " << min_reclaim_bytes << " (scanned "
            << candidates.size() << " candidates, scan_limit "
            << config_.candidate_scan_limit << ")";
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
