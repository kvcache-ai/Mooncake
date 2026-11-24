#pragma once

#include <chrono>
#include <cstdint>
#include <map>
#include <mutex>
#include <string>
#include <vector>

#include "hybrid_metric.h"
#include "ylt/metric/counter.hpp"
#include "ylt/metric/gauge.hpp"

namespace mooncake {

/**
 * @brief Allocator counters sampled from jemalloc, in a library-friendly form.
 *
 * The struct deliberately names no jemalloc type: libmooncake_store never links
 * the allocator, so the fields are filled by a collector installed from an
 * executable that does. Page counts are converted to bytes by the collector,
 * which is the side that knows the runtime page size.
 */
/**
 * @brief One jemalloc small size class, as reported by the merged arena view.
 *
 * Occupancy is regs / (slabs * regs_per_slab): a slab is only returned to the
 * OS once every region in it is free, so a class sitting at low occupancy is
 * holding slab_bytes while only using used_bytes.
 */
struct JemallocBinStats {
    uint64_t size = 0;
    uint64_t regs_per_slab = 0;
    uint64_t regs = 0;
    uint64_t slabs = 0;
};

struct JemallocSnapshot {
    // Instantaneous.
    uint64_t allocated = 0;
    uint64_t active = 0;
    uint64_t metadata = 0;
    uint64_t resident = 0;
    uint64_t retained = 0;
    uint64_t mapped = 0;
    uint64_t small_allocated = 0;
    uint64_t large_allocated = 0;
    uint64_t dirty_bytes = 0;
    uint64_t muzzy_bytes = 0;
    uint64_t arenas = 0;
    uint64_t background_threads = 0;
    int64_t opt_dirty_decay_ms = 0;
    int64_t opt_muzzy_decay_ms = 0;

    // Cumulative since process start.
    uint64_t dirty_purged_pages = 0;
    uint64_t muzzy_purged_pages = 0;
    uint64_t dirty_purge_runs = 0;
    uint64_t muzzy_purge_runs = 0;
    uint64_t dirty_madvises = 0;
    uint64_t muzzy_madvises = 0;
    uint64_t small_allocations = 0;
    uint64_t small_deallocations = 0;

    // One entry per small size class, in jemalloc's bin order.
    std::vector<JemallocBinStats> bins;
};

using JemallocStatsCollector = bool (*)(JemallocSnapshot&);

/**
 * @brief Install the process-wide jemalloc stats collector.
 *
 * Called from the main() of a binary that links jemalloc, next to
 * LogAllocatorStatus(). Without it the jemalloc series stay at zero and
 * mooncake_jemalloc_enabled reports 0, which is the correct answer for a
 * process running the system allocator, the Python extension included.
 * Thread-safe; intended to be called once before worker threads start.
 *
 * @param collector Fills a JemallocSnapshot and returns whether it succeeded.
 *                  Passing nullptr uninstalls the current collector.
 */
void SetJemallocStatsCollector(JemallocStatsCollector collector);

// Forward declaration for cross-namespace friend access (test-only).
namespace test {
class AllocatorMetricTest;
}  // namespace test

/**
 * @brief Process memory and allocator metrics, shared by master and client.
 *
 * Process figures come from /proc/self/status and are reported whichever
 * allocator is linked, so the series stay comparable across a glibc/jemalloc
 * switch. They are the ground truth the allocator series are read against:
 * mooncake_jemalloc_resident_bytes is jemalloc's own upper estimate over the
 * extents it maps and can exceed the resident size the kernel reports, so the
 * two are compared for divergence rather than subtracted.
 */
class AllocatorMetric {
   public:
    /**
     * @param labels Static labels applied to every series, matching the
     *               convention of the sibling client metric structs.
     */
    explicit AllocatorMetric(
        const std::map<std::string, std::string>& labels = {});

    /**
     * @brief Re-sample the process and, when installed, the collector.
     *
     * Advancing the jemalloc epoch takes a lock shared with allocating threads,
     * so samples closer together than kMinRefreshInterval reuse the values
     * already published rather than let an unauthenticated /metrics endpoint
     * drive the rate.
     * Thread-safe: concurrent scrapes serialize, and only one of them advances
     * the cumulative counters.
     */
    void Refresh();

    /**
     * @brief Append the metrics in Prometheus text format.
     *
     * Does not sample; call Refresh() first. Named to match the sibling metric
     * structs, whose serialize() chains this one is spliced into.
     */
    void serialize(std::string& str);

    /**
     * @brief Render a short human-readable digest for /metrics/summary.
     */
    std::string summary_metrics();

   private:
    friend class ::mooncake::test::AllocatorMetricTest;

    // A scrape interval is 15s in every deployment, so this only bounds the
    // cost of a hand-driven or looping scraper.
    static constexpr std::chrono::milliseconds kMinRefreshInterval{1000};

    // ylt drops a metric that still holds its untouched zero, so every series
    // is marked once here or it is missing from a fresh process.
    void MarkAllForZeroOutput();

    void RefreshLocked();
    void RefreshProcess();
    void RefreshJemalloc();

    // jemalloc publishes cumulative totals while a ylt counter only accepts
    // increments, so every counter advances by the difference from the previous
    // sample. Guarded by refresh_mutex_.
    void AdvanceCounter(ylt::metric::counter_t& counter, uint64_t total,
                        uint64_t& previous);

    std::mutex refresh_mutex_;
    // Left at the clock epoch so that the first Refresh() is never throttled.
    std::chrono::steady_clock::time_point last_refresh_{};

    ylt::metric::gauge_t process_rss_;
    ylt::metric::gauge_t process_rss_peak_;
    ylt::metric::gauge_t process_rss_anon_;
    ylt::metric::gauge_t process_rss_file_;
    ylt::metric::gauge_t process_rss_shmem_;
    ylt::metric::gauge_t process_vsize_;
    ylt::metric::gauge_t process_swap_;

    ylt::metric::gauge_t jemalloc_enabled_;
    ylt::metric::gauge_t jemalloc_allocated_;
    ylt::metric::gauge_t jemalloc_active_;
    ylt::metric::gauge_t jemalloc_metadata_;
    ylt::metric::gauge_t jemalloc_resident_;
    ylt::metric::gauge_t jemalloc_retained_;
    ylt::metric::gauge_t jemalloc_mapped_;
    ylt::metric::gauge_t jemalloc_small_allocated_;
    ylt::metric::gauge_t jemalloc_large_allocated_;
    ylt::metric::gauge_t jemalloc_dirty_;
    ylt::metric::gauge_t jemalloc_muzzy_;
    ylt::metric::gauge_t jemalloc_arenas_;
    ylt::metric::gauge_t jemalloc_background_threads_;
    ylt::metric::gauge_t jemalloc_opt_dirty_decay_ms_;
    ylt::metric::gauge_t jemalloc_opt_muzzy_decay_ms_;

    ylt::metric::counter_t jemalloc_dirty_purged_pages_;
    ylt::metric::counter_t jemalloc_muzzy_purged_pages_;
    ylt::metric::counter_t jemalloc_dirty_purge_runs_;
    ylt::metric::counter_t jemalloc_muzzy_purge_runs_;
    ylt::metric::counter_t jemalloc_dirty_madvises_;
    ylt::metric::counter_t jemalloc_muzzy_madvises_;
    ylt::metric::counter_t jemalloc_small_allocations_;
    ylt::metric::counter_t jemalloc_small_deallocations_;

    uint64_t prev_dirty_purged_pages_ = 0;
    uint64_t prev_muzzy_purged_pages_ = 0;
    uint64_t prev_dirty_purge_runs_ = 0;
    uint64_t prev_muzzy_purge_runs_ = 0;
    uint64_t prev_dirty_madvises_ = 0;
    uint64_t prev_muzzy_madvises_ = 0;
    uint64_t prev_small_allocations_ = 0;
    uint64_t prev_small_deallocations_ = 0;

    // Unlike the scalar series these carry a label, so they cannot be marked
    // for zero output ahead of the first sample: a labelled metric has no
    // values until one is written.
    ylt::metric::hybrid_gauge_1t jemalloc_bin_regs_;
    ylt::metric::hybrid_gauge_1t jemalloc_bin_slabs_;
    ylt::metric::hybrid_gauge_1t jemalloc_bin_used_bytes_;
    ylt::metric::hybrid_gauge_1t jemalloc_bin_slab_bytes_;
};

}  // namespace mooncake
