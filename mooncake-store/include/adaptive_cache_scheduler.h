#pragma once

#include <atomic>
#include <chrono>
#include <cmath>
#include <string>

#include "master_metric_manager.h"

namespace mooncake {

/**
 * @brief Adaptive cache scheduler that dynamically tunes eviction parameters
 *        based on observed workload patterns.
 *
 * Implements Roadmap M3.1 "(Cache Scheduling Interface)" — a feedback-driven
 * controller that reads cache hit metrics and adjusts eviction thresholds to
 * optimize for the current workload.
 *
 * Workload detection:
 *   - PREFIX_HEAVY: High memory hit rate (>70%), skewed access pattern.
 *     → Protect hot objects longer, evict conservatively.
 *   - SCAN_HEAVY: Low memory hit rate (<30%), uniform access.
 *     → Evict aggressively to make room for new data.
 *   - MIXED: Moderate hit rate, typical interactive inference.
 *     → Balanced eviction parameters.
 *
 * Tunable parameters (adjusted every scheduling interval):
 *   - eviction_high_watermark: When to trigger eviction (higher = less eager)
 *   - eviction_ratio: How much to evict per round (higher = more aggressive)
 *   - soft_pin_ttl: How long to protect frequently accessed objects
 *
 * The scheduler uses EWMA (Exponentially Weighted Moving Average) to smooth
 * metric fluctuations and avoid oscillation between modes.
 */
class AdaptiveCacheScheduler {
   public:
    enum class WorkloadMode {
        PREFIX_HEAVY,  // High reuse, protect hot objects
        SCAN_HEAVY,    // Low reuse, fast eviction
        MIXED,         // Balanced
    };

    struct SchedulerConfig {
        // EWMA smoothing factor (0 = no smoothing, 1 = no memory)
        double ewma_alpha = 0.3;

        // Workload classification thresholds
        double prefix_heavy_hit_rate = 0.70;  // Above this → PREFIX_HEAVY
        double scan_heavy_hit_rate = 0.30;    // Below this → SCAN_HEAVY

        // Parameter ranges for each mode
        // PREFIX_HEAVY: conserve memory, protect hot objects
        double prefix_watermark = 0.92;   // Trigger later
        double prefix_evict_ratio = 0.03; // Evict less
        uint64_t prefix_soft_pin_ms = 3600000;  // 1 hour

        // SCAN_HEAVY: fast turnover
        double scan_watermark = 0.80;     // Trigger earlier
        double scan_evict_ratio = 0.10;   // Evict more
        uint64_t scan_soft_pin_ms = 300000;  // 5 minutes

        // MIXED: balanced
        double mixed_watermark = 0.85;
        double mixed_evict_ratio = 0.05;
        uint64_t mixed_soft_pin_ms = 1800000;  // 30 minutes

        // Minimum interval between scheduling decisions
        uint64_t scheduling_interval_ms = 5000;  // 5 seconds
    };

    struct SchedulerOutput {
        double eviction_high_watermark;
        double eviction_ratio;
        uint64_t soft_pin_ttl_ms;
        WorkloadMode mode;
    };

    AdaptiveCacheScheduler()
        : config_(),
          current_mode_(WorkloadMode::MIXED),
          ewma_mem_hit_rate_(0.5),
          ewma_get_rate_(0.0),
          last_total_gets_(0),
          last_mem_hits_(0),
          last_schedule_time_(std::chrono::steady_clock::now()) {}

    explicit AdaptiveCacheScheduler(const SchedulerConfig& config)
        : config_(config),
          current_mode_(WorkloadMode::MIXED),
          ewma_mem_hit_rate_(0.5),
          ewma_get_rate_(0.0),
          last_total_gets_(0),
          last_mem_hits_(0),
          last_schedule_time_(std::chrono::steady_clock::now()) {}

    /**
     * @brief Run one scheduling cycle. Called from EvictionThreadFunc.
     *
     * Reads metrics from MasterMetricManager, updates EWMA estimates,
     * classifies workload, and returns tuned parameters.
     *
     * @return SchedulerOutput with adjusted eviction parameters,
     *         or nullopt if scheduling interval has not elapsed.
     */
    std::optional<SchedulerOutput> Schedule() {
        auto now = std::chrono::steady_clock::now();
        auto elapsed_ms =
            std::chrono::duration_cast<std::chrono::milliseconds>(
                now - last_schedule_time_)
                .count();

        if (elapsed_ms < static_cast<int64_t>(config_.scheduling_interval_ms)) {
            return std::nullopt;  // Too early
        }
        last_schedule_time_ = now;

        // Read current metrics
        auto& metrics = MasterMetricManager::instance();
        int64_t total_gets = metrics.get_total_get_nums();
        int64_t mem_hits = metrics.get_mem_cache_hit_nums();

        // Compute delta since last schedule
        int64_t delta_gets = total_gets - last_total_gets_;
        int64_t delta_hits = mem_hits - last_mem_hits_;
        last_total_gets_ = total_gets;
        last_mem_hits_ = mem_hits;

        // Compute instantaneous hit rate
        double instant_hit_rate = 0.5;  // Default when no data
        if (delta_gets > 0) {
            instant_hit_rate =
                static_cast<double>(delta_hits) / static_cast<double>(delta_gets);
        }

        // EWMA smoothing
        ewma_mem_hit_rate_ = config_.ewma_alpha * instant_hit_rate +
                             (1.0 - config_.ewma_alpha) * ewma_mem_hit_rate_;

        // Compute request rate (gets per second)
        double gets_per_sec = 0.0;
        if (elapsed_ms > 0) {
            gets_per_sec =
                static_cast<double>(delta_gets) * 1000.0 / elapsed_ms;
        }
        ewma_get_rate_ = config_.ewma_alpha * gets_per_sec +
                         (1.0 - config_.ewma_alpha) * ewma_get_rate_;

        // Classify workload
        WorkloadMode new_mode;
        if (ewma_mem_hit_rate_ > config_.prefix_heavy_hit_rate) {
            new_mode = WorkloadMode::PREFIX_HEAVY;
        } else if (ewma_mem_hit_rate_ < config_.scan_heavy_hit_rate) {
            new_mode = WorkloadMode::SCAN_HEAVY;
        } else {
            new_mode = WorkloadMode::MIXED;
        }

        current_mode_ = new_mode;

        // Output parameters based on mode
        SchedulerOutput output;
        output.mode = new_mode;

        switch (new_mode) {
            case WorkloadMode::PREFIX_HEAVY:
                output.eviction_high_watermark = config_.prefix_watermark;
                output.eviction_ratio = config_.prefix_evict_ratio;
                output.soft_pin_ttl_ms = config_.prefix_soft_pin_ms;
                break;
            case WorkloadMode::SCAN_HEAVY:
                output.eviction_high_watermark = config_.scan_watermark;
                output.eviction_ratio = config_.scan_evict_ratio;
                output.soft_pin_ttl_ms = config_.scan_soft_pin_ms;
                break;
            case WorkloadMode::MIXED:
            default:
                output.eviction_high_watermark = config_.mixed_watermark;
                output.eviction_ratio = config_.mixed_evict_ratio;
                output.soft_pin_ttl_ms = config_.mixed_soft_pin_ms;
                break;
        }

        return output;
    }

    WorkloadMode getCurrentMode() const { return current_mode_; }
    double getEwmaHitRate() const { return ewma_mem_hit_rate_; }
    double getEwmaGetRate() const { return ewma_get_rate_; }

    static const char* ModeToString(WorkloadMode mode) {
        switch (mode) {
            case WorkloadMode::PREFIX_HEAVY: return "PREFIX_HEAVY";
            case WorkloadMode::SCAN_HEAVY: return "SCAN_HEAVY";
            case WorkloadMode::MIXED: return "MIXED";
            default: return "UNKNOWN";
        }
    }

   private:
    SchedulerConfig config_;
    WorkloadMode current_mode_;
    double ewma_mem_hit_rate_;
    double ewma_get_rate_;
    int64_t last_total_gets_;
    int64_t last_mem_hits_;
    std::chrono::steady_clock::time_point last_schedule_time_;
};

}  // namespace mooncake
