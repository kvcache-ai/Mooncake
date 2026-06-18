#pragma once

#include <cstddef>
#include <cstdint>
#include <mutex>
#include <optional>
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
 * Doc<->config term mapping: doc low_wm == user_floor,
 * doc high_wm == limit_watermark. The proportional reclaim RATE always uses the
 * STATIC denominator (limit_watermark - user_floor); the floating
 * `evict_watermark` is only the trigger threshold.
 */
class MultiLRUPolicy : public EventDrivenPolicy {
   public:
    struct Config {
        double evict_watermark = 0.90;   // no-load trigger (float start point)
        double user_floor = 0.70;        // doc low_wm
        double limit_watermark = 0.95;   // doc high_wm
        double evict_rate_k = 1.0;       // proportional rate coefficient
        uint64_t offload_freq_threshold = 2;
        uint64_t onboard_freq_threshold = 4;
        double onboard_fast_threshold = 0.50;  // < user_floor (hysteresis)
        size_t sketch_capacity = size_t{1} << 16;
        size_t candidate_scan_limit = 4096;  // max victims scanned per pass
    };

    explicit MultiLRUPolicy(const Config& config);

    void Init(TieredBackend* backend, UUID fast_tier,
              std::optional<UUID> slow_tier) override;
    std::optional<MovementRequest> OnAccess(const AccessContext& ctx) override;
    void OnCommit(const CommitContext& ctx) override;
    void OnDelete(const DeleteContext& ctx) override;
    std::vector<MovementRequest> DecideEvict(size_t min_reclaim_bytes) override;
    AccessStats GetHotKeyStats(size_t hot_key_num) const override;

    // Test/introspection: current floating trigger watermark.
    double evict_watermark() const;

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

    const Config config_;
    TieredBackend* backend_ = nullptr;
    UUID fast_tier_{};
    std::optional<UUID> slow_tier_;
    bool initialized_ = false;

    MultiLRUStatsCollector collector_;  // the policy owns its statistics

    mutable std::mutex mutex_;  // guards commit accumulator + evict_watermark_
    std::unordered_map<UUID, size_t, boost::hash<UUID>> committed_bytes_;
    double evict_watermark_;
};

}  // namespace mooncake
