#include "allocator_metric.h"

#include <glog/logging.h>

#include <array>
#include <atomic>
#include <cstdio>
#include <fstream>
#include <sstream>
#include <string_view>

namespace mooncake {

namespace {

std::atomic<JemallocStatsCollector> g_jemalloc_collector{nullptr};

constexpr int64_t kKibiBytes = 1024;

// The only label on the per-bin series; its value is the size class in bytes.
const std::array<std::string, 1> kBinSizeLabel = {"size"};

// /proc/self/status reports sizes as "VmRSS:\t  123456 kB". Takes the line by
// reference to a std::string so that the sscanf below sees a terminated buffer.
bool ParseStatusKib(const std::string& line, std::string_view key,
                    int64_t& bytes) {
    if (line.size() <= key.size() || line.compare(0, key.size(), key) != 0) {
        return false;
    }
    long long value = 0;
    if (std::sscanf(line.data() + key.size(), " %lld kB", &value) != 1) {
        return false;
    }
    bytes = static_cast<int64_t>(value) * kKibiBytes;
    return true;
}

}  // namespace

void SetJemallocStatsCollector(JemallocStatsCollector collector) {
    g_jemalloc_collector.store(collector, std::memory_order_release);
}

AllocatorMetric::AllocatorMetric(
    const std::map<std::string, std::string>& labels)
    : process_rss_("mooncake_process_rss_bytes",
                   "Resident set size of the process", labels),
      process_rss_peak_("mooncake_process_rss_peak_bytes",
                        "Peak resident set size since process start", labels),
      process_rss_anon_("mooncake_process_rss_anon_bytes",
                        "Resident anonymous memory of the process", labels),
      process_rss_file_("mooncake_process_rss_file_bytes",
                        "Resident file-backed memory of the process", labels),
      process_rss_shmem_("mooncake_process_rss_shmem_bytes",
                         "Resident shared memory of the process", labels),
      process_vsize_("mooncake_process_vsize_bytes",
                     "Virtual address space size of the process", labels),
      process_swap_("mooncake_process_swap_bytes",
                    "Swapped-out memory of the process", labels),
      jemalloc_enabled_("mooncake_jemalloc_enabled",
                        "1 when jemalloc statistics are being collected",
                        labels),
      jemalloc_allocated_(
          "mooncake_jemalloc_allocated_bytes",
          "Bytes allocated by the application and not yet freed", labels),
      jemalloc_active_("mooncake_jemalloc_active_bytes",
                       "Bytes in pages backing active allocations", labels),
      jemalloc_metadata_("mooncake_jemalloc_metadata_bytes",
                         "Bytes used by allocator metadata", labels),
      jemalloc_resident_("mooncake_jemalloc_resident_bytes",
                         "Bytes in physically resident pages", labels),
      jemalloc_retained_("mooncake_jemalloc_retained_bytes",
                         "Bytes of address space retained but not resident",
                         labels),
      jemalloc_mapped_("mooncake_jemalloc_mapped_bytes",
                       "Bytes in extents mapped by the allocator", labels),
      jemalloc_small_allocated_("mooncake_jemalloc_small_allocated_bytes",
                                "Allocated bytes served by small size classes",
                                labels),
      jemalloc_large_allocated_("mooncake_jemalloc_large_allocated_bytes",
                                "Allocated bytes served by large size classes",
                                labels),
      jemalloc_dirty_("mooncake_jemalloc_dirty_bytes",
                      "Bytes in dirty pages awaiting decay", labels),
      jemalloc_muzzy_("mooncake_jemalloc_muzzy_bytes",
                      "Bytes in muzzy pages awaiting decay", labels),
      jemalloc_arenas_("mooncake_jemalloc_arenas",
                       "Number of arenas the allocator maintains", labels),
      jemalloc_background_threads_("mooncake_jemalloc_background_threads",
                                   "Background decay threads actually running",
                                   labels),
      jemalloc_opt_dirty_decay_ms_(
          "mooncake_jemalloc_opt_dirty_decay_ms",
          "Configured dirty page decay in ms; -1 never purges", labels),
      jemalloc_opt_muzzy_decay_ms_(
          "mooncake_jemalloc_opt_muzzy_decay_ms",
          "Configured muzzy page decay in ms; -1 never purges", labels),
      jemalloc_dirty_purged_pages_("mooncake_jemalloc_dirty_purged_pages_total",
                                   "Dirty pages purged since process start",
                                   labels),
      jemalloc_muzzy_purged_pages_("mooncake_jemalloc_muzzy_purged_pages_total",
                                   "Muzzy pages purged since process start",
                                   labels),
      jemalloc_dirty_purge_runs_("mooncake_jemalloc_dirty_purge_runs_total",
                                 "Dirty decay purge runs since process start",
                                 labels),
      jemalloc_muzzy_purge_runs_("mooncake_jemalloc_muzzy_purge_runs_total",
                                 "Muzzy decay purge runs since process start",
                                 labels),
      jemalloc_dirty_madvises_(
          "mooncake_jemalloc_dirty_madvises_total",
          "madvise calls issued by dirty decay since process start", labels),
      jemalloc_muzzy_madvises_(
          "mooncake_jemalloc_muzzy_madvises_total",
          "madvise calls issued by muzzy decay since process start", labels),
      jemalloc_small_allocations_(
          "mooncake_jemalloc_small_allocations_total",
          "Small allocation requests since process start", labels),
      jemalloc_small_deallocations_(
          "mooncake_jemalloc_small_deallocations_total",
          "Small deallocation requests since process start", labels),
      jemalloc_bin_regs_("mooncake_jemalloc_bin_regs",
                         "Live regions in a small size class", labels,
                         kBinSizeLabel),
      jemalloc_bin_slabs_("mooncake_jemalloc_bin_slabs",
                          "Slabs held by a small size class", labels,
                          kBinSizeLabel),
      jemalloc_bin_used_bytes_("mooncake_jemalloc_bin_used_bytes",
                               "Bytes live in a small size class", labels,
                               kBinSizeLabel),
      jemalloc_bin_slab_bytes_(
          "mooncake_jemalloc_bin_slab_bytes",
          "Bytes the slabs of a small size class hold from the OS", labels,
          kBinSizeLabel) {
    MarkAllForZeroOutput();
}

void AllocatorMetric::MarkAllForZeroOutput() {
    process_rss_.update(0);
    process_rss_peak_.update(0);
    process_rss_anon_.update(0);
    process_rss_file_.update(0);
    process_rss_shmem_.update(0);
    process_vsize_.update(0);
    process_swap_.update(0);

    jemalloc_enabled_.update(0);
    jemalloc_allocated_.update(0);
    jemalloc_active_.update(0);
    jemalloc_metadata_.update(0);
    jemalloc_resident_.update(0);
    jemalloc_retained_.update(0);
    jemalloc_mapped_.update(0);
    jemalloc_small_allocated_.update(0);
    jemalloc_large_allocated_.update(0);
    jemalloc_dirty_.update(0);
    jemalloc_muzzy_.update(0);
    jemalloc_arenas_.update(0);
    jemalloc_background_threads_.update(0);
    jemalloc_opt_dirty_decay_ms_.update(0);
    jemalloc_opt_muzzy_decay_ms_.update(0);

    jemalloc_dirty_purged_pages_.inc(0);
    jemalloc_muzzy_purged_pages_.inc(0);
    jemalloc_dirty_purge_runs_.inc(0);
    jemalloc_muzzy_purge_runs_.inc(0);
    jemalloc_dirty_madvises_.inc(0);
    jemalloc_muzzy_madvises_.inc(0);
    jemalloc_small_allocations_.inc(0);
    jemalloc_small_deallocations_.inc(0);
}

void AllocatorMetric::Refresh() {
    const auto now = std::chrono::steady_clock::now();
    std::lock_guard<std::mutex> guard(refresh_mutex_);
    if (now - last_refresh_ < kMinRefreshInterval) {
        return;
    }
    last_refresh_ = now;
    RefreshLocked();
}

void AllocatorMetric::RefreshLocked() {
    RefreshProcess();
    RefreshJemalloc();
}

void AllocatorMetric::RefreshProcess() {
    std::ifstream status("/proc/self/status");
    if (!status.is_open()) {
        // Only reachable on a kernel without procfs, where every process gauge
        // simply keeps its previous value.
        return;
    }

    std::string line;
    while (std::getline(status, line)) {
        int64_t bytes = 0;
        if (ParseStatusKib(line, "VmRSS:", bytes)) {
            process_rss_.update(bytes);
        } else if (ParseStatusKib(line, "VmHWM:", bytes)) {
            process_rss_peak_.update(bytes);
        } else if (ParseStatusKib(line, "RssAnon:", bytes)) {
            process_rss_anon_.update(bytes);
        } else if (ParseStatusKib(line, "RssFile:", bytes)) {
            process_rss_file_.update(bytes);
        } else if (ParseStatusKib(line, "RssShmem:", bytes)) {
            process_rss_shmem_.update(bytes);
        } else if (ParseStatusKib(line, "VmSize:", bytes)) {
            process_vsize_.update(bytes);
        } else if (ParseStatusKib(line, "VmSwap:", bytes)) {
            process_swap_.update(bytes);
        }
    }
}

void AllocatorMetric::RefreshJemalloc() {
    const JemallocStatsCollector collector =
        g_jemalloc_collector.load(std::memory_order_acquire);
    JemallocSnapshot snapshot;
    if (collector == nullptr || !collector(snapshot)) {
        jemalloc_enabled_.update(0);
        return;
    }

    jemalloc_enabled_.update(1);
    jemalloc_allocated_.update(static_cast<int64_t>(snapshot.allocated));
    jemalloc_active_.update(static_cast<int64_t>(snapshot.active));
    jemalloc_metadata_.update(static_cast<int64_t>(snapshot.metadata));
    jemalloc_resident_.update(static_cast<int64_t>(snapshot.resident));
    jemalloc_retained_.update(static_cast<int64_t>(snapshot.retained));
    jemalloc_mapped_.update(static_cast<int64_t>(snapshot.mapped));
    jemalloc_small_allocated_.update(
        static_cast<int64_t>(snapshot.small_allocated));
    jemalloc_large_allocated_.update(
        static_cast<int64_t>(snapshot.large_allocated));
    jemalloc_dirty_.update(static_cast<int64_t>(snapshot.dirty_bytes));
    jemalloc_muzzy_.update(static_cast<int64_t>(snapshot.muzzy_bytes));
    jemalloc_arenas_.update(static_cast<int64_t>(snapshot.arenas));
    jemalloc_background_threads_.update(
        static_cast<int64_t>(snapshot.background_threads));
    jemalloc_opt_dirty_decay_ms_.update(snapshot.opt_dirty_decay_ms);
    jemalloc_opt_muzzy_decay_ms_.update(snapshot.opt_muzzy_decay_ms);

    AdvanceCounter(jemalloc_dirty_purged_pages_, snapshot.dirty_purged_pages,
                   prev_dirty_purged_pages_);
    AdvanceCounter(jemalloc_muzzy_purged_pages_, snapshot.muzzy_purged_pages,
                   prev_muzzy_purged_pages_);
    AdvanceCounter(jemalloc_dirty_purge_runs_, snapshot.dirty_purge_runs,
                   prev_dirty_purge_runs_);
    AdvanceCounter(jemalloc_muzzy_purge_runs_, snapshot.muzzy_purge_runs,
                   prev_muzzy_purge_runs_);
    AdvanceCounter(jemalloc_dirty_madvises_, snapshot.dirty_madvises,
                   prev_dirty_madvises_);
    AdvanceCounter(jemalloc_muzzy_madvises_, snapshot.muzzy_madvises,
                   prev_muzzy_madvises_);
    AdvanceCounter(jemalloc_small_allocations_, snapshot.small_allocations,
                   prev_small_allocations_);
    AdvanceCounter(jemalloc_small_deallocations_, snapshot.small_deallocations,
                   prev_small_deallocations_);

    for (const auto& bin : snapshot.bins) {
        const std::array<std::string, 1> label = {std::to_string(bin.size)};
        jemalloc_bin_regs_.update(label, static_cast<int64_t>(bin.regs));
        jemalloc_bin_slabs_.update(label, static_cast<int64_t>(bin.slabs));
        jemalloc_bin_used_bytes_.update(
            label, static_cast<int64_t>(bin.regs * bin.size));
        jemalloc_bin_slab_bytes_.update(
            label,
            static_cast<int64_t>(bin.slabs * bin.regs_per_slab * bin.size));
    }
}

void AllocatorMetric::AdvanceCounter(ylt::metric::counter_t& counter,
                                     uint64_t total, uint64_t& previous) {
    if (total < previous) {
        // jemalloc totals only grow within one process, so a regression means
        // the sample is untrustworthy; holding the series keeps it monotonic.
        return;
    }
    counter.inc(static_cast<int64_t>(total - previous));
    previous = total;
}

void AllocatorMetric::serialize(std::string& str) {
    process_rss_.serialize(str);
    process_rss_peak_.serialize(str);
    process_rss_anon_.serialize(str);
    process_rss_file_.serialize(str);
    process_rss_shmem_.serialize(str);
    process_vsize_.serialize(str);
    process_swap_.serialize(str);

    jemalloc_enabled_.serialize(str);
    jemalloc_allocated_.serialize(str);
    jemalloc_active_.serialize(str);
    jemalloc_metadata_.serialize(str);
    jemalloc_resident_.serialize(str);
    jemalloc_retained_.serialize(str);
    jemalloc_mapped_.serialize(str);
    jemalloc_small_allocated_.serialize(str);
    jemalloc_large_allocated_.serialize(str);
    jemalloc_dirty_.serialize(str);
    jemalloc_muzzy_.serialize(str);
    jemalloc_arenas_.serialize(str);
    jemalloc_background_threads_.serialize(str);
    jemalloc_opt_dirty_decay_ms_.serialize(str);
    jemalloc_opt_muzzy_decay_ms_.serialize(str);

    jemalloc_dirty_purged_pages_.serialize(str);
    jemalloc_muzzy_purged_pages_.serialize(str);
    jemalloc_dirty_purge_runs_.serialize(str);
    jemalloc_muzzy_purge_runs_.serialize(str);
    jemalloc_dirty_madvises_.serialize(str);
    jemalloc_muzzy_madvises_.serialize(str);
    jemalloc_small_allocations_.serialize(str);
    jemalloc_small_deallocations_.serialize(str);

    jemalloc_bin_regs_.serialize(str);
    jemalloc_bin_slabs_.serialize(str);
    jemalloc_bin_used_bytes_.serialize(str);
    jemalloc_bin_slab_bytes_.serialize(str);
}

std::string AllocatorMetric::summary_metrics() {
    std::stringstream ss;
    ss << "=== Allocator Metrics Summary ===\n";
    ss << "Process RSS: " << process_rss_.value()
       << " B, peak: " << process_rss_peak_.value() << " B\n";
    if (jemalloc_enabled_.value() == 0) {
        ss << "jemalloc: not linked\n";
        return ss.str();
    }
    ss << "jemalloc allocated: " << jemalloc_allocated_.value()
       << " B, resident: " << jemalloc_resident_.value()
       << " B, retained: " << jemalloc_retained_.value() << " B\n";
    ss << "jemalloc dirty: " << jemalloc_dirty_.value()
       << " B, muzzy: " << jemalloc_muzzy_.value() << " B\n";
    return ss.str();
}

}  // namespace mooncake
