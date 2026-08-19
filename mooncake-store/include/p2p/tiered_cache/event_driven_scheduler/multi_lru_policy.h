#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <mutex>
#include <optional>
#include <unordered_map>
#include <vector>

#include <boost/functional/hash.hpp>

#include "tiered_cache/event_driven_scheduler/event_driven_policy.h"
#include "tiered_cache/event_driven_scheduler/multi_lru_stats_collector.h"
#include "types.h"

namespace mooncake {

class TieredBackend;
struct TierView;  // defined in tiered_backend.h; used by value only in the .cpp

/**
 * @class MultiLRUPolicy
 * @brief Self-contained TinyLFU + 4-band-MultiLRU policy.
 *
 * Owns its statistics (a MultiLRUStatsCollector) and makes every decision
 * internally, exposing only the generic EventDrivenPolicy surface. The
 * scheduler that drives it knows nothing about frequency bands or watermarks.
 *
 * The eviction TRIGGER is a floating watermark bounded by
 * [evict_watermark_low, evict_watermark_high]. It rests at the HIGH bound when
 * there is no write load and floats DOWN toward the LOW bound as write load
 * rises — so a write-heavy fast tier triggers eviction EARLIER and keeps MORE
 * headroom for incoming writes. It is INITIALIZED at the LOW bound: startup
 * pessimistically assumes high write load, so the tier begins with maximum
 * headroom and avoids an immediate synchronous allocation-failure fallback.
 * evict_watermark_low doubles as the reclaim floor: the proportional reclaim
 * RATE uses the STATIC denominator (limit_watermark - evict_watermark_low), and
 * reclaim targets the bytes above evict_watermark_low.
 */
class MultiLRUPolicy : public EventDrivenPolicy {
   public:
    struct Config {
        // Floating eviction-trigger bounds. The trigger rests at `high` with no
        // write load and floats DOWN toward `low` as write load rises (evict
        // earlier / keep more headroom under write pressure). `low` also serves
        // as the reclaim floor and the pessimistic startup value.
        double evict_watermark_low =
            0.70;  // lower bound (max load; floor; init)
        double evict_watermark_high = 0.90;  // upper bound (no write load)
        double limit_watermark = 0.95;  // rate saturates here (full watermark)
        double evict_rate_k = 1.0;      // proportional rate coefficient
        // Write-load response window (seconds): the throughput that fills the
        // fast tier within one window drives the trigger to its low bound. Also
        // the reaction time constant — larger = more sensitive but slower.
        double evict_load_window_s = 2.0;
        uint64_t offload_freq_threshold = 2;
        uint64_t onboard_freq_threshold = 4;
        double onboard_fast_threshold = 0.50;  // < evict_watermark_low (hyst.)
        size_t sketch_capacity = size_t{1} << 16;
        size_t candidate_scan_limit = 4096;  // max victims scanned per pass
        // Frequency cutoffs for the 4-band MultiLRU (warm/hot/very-hot); see
        // BandThresholds. Operator-tunable; defaults to 3/8/15.
        BandThresholds band_thresholds{};
    };

    // Monotonic clock source for the write-load decay; injectable for tests.
    // Defaults to std::chrono::steady_clock::now.
    using ClockFn = std::function<std::chrono::steady_clock::time_point()>;

    explicit MultiLRUPolicy(const Config& config, ClockFn clock = {});

    void Init(TieredBackend* backend, UUID fast_tier,
              std::optional<UUID> slow_tier) override;
    std::optional<MovementRequest> OnAccess(const AccessContext& ctx) override;
    void OnCommit(const CommitContext& ctx) override;
    void OnDelete(const DeleteContext& ctx) override;
    std::vector<MovementRequest> DecideEvict(size_t min_reclaim_bytes) override;
    AccessStats GetHotKeyStats(size_t hot_key_num) const override;

    // Test/introspection: current floating trigger watermark (in
    // [evict_watermark_low, evict_watermark_high]).
    double evict_wm() const;

   private:
    static double Ratio(size_t used, size_t total) {
        return total == 0
                   ? 0.0
                   : static_cast<double>(used) / static_cast<double>(total);
    }

    // Admission checks (operate on already-gathered tier stats).
    bool ShouldOffload(uint64_t freq, size_t size_bytes,
                       const std::vector<UUID>& locations, const TierView& slow,
                       UUID slow_id) const;
    bool ShouldOnboard(uint64_t freq, const std::vector<UUID>& locations,
                       const TierView& fast) const;
    size_t BestEffortSize(std::string_view key, size_t hint) const;

    void RecordCommitBytes(UUID tier_id, size_t bytes);

    // Recompute the floating eviction trigger from accumulated write load,
    // store it in evict_wm_, and return it. Caller must hold mutex_.
    double RefreshEvictWatermark(size_t capacity);

    const Config config_;
    TieredBackend* backend_ = nullptr;
    UUID fast_tier_{};
    std::optional<UUID> slow_tier_;
    bool initialized_ = false;

    MultiLRUStatsCollector collector_;  // the policy owns its statistics

    const ClockFn clock_;  // monotonic clock for write-load decay (immutable)

    mutable std::mutex mutex_;  // guards commit accumulator + load/watermark
    std::unordered_map<UUID, size_t, boost::hash<UUID>> committed_bytes_;
    // Time-decayed committed bytes (a smoothed write-rate proxy) and the last
    // time it was decayed; updated only on the periodic pass.
    double load_accum_ = 0.0;
    std::chrono::steady_clock::time_point last_decay_tp_{};
    bool load_tp_valid_ = false;
    double evict_wm_;  // current floating trigger watermark
};

}  // namespace mooncake
