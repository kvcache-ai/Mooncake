#pragma once

#include <glog/logging.h>

#include "allocator_metric.h"

#ifdef STORE_USE_JEMALLOC
#include <jemalloc/jemalloc.h>
#include <sys/types.h>

#include <cstddef>
#include <cstdint>
#include <string>
#endif

namespace mooncake {

#ifdef STORE_USE_JEMALLOC

template <typename T>
inline bool ReadJemallocKnob(const char* name, T& value) {
    size_t size = sizeof(value);
    return mallctl(name, &value, &size, nullptr, 0) == 0;
}

/**
 * @brief Log the jemalloc version and decay tuning once at process startup.
 *
 * The release binaries are stripped, so this line is the only evidence that
 * jemalloc actually replaced the system allocator: mallctl resolves only when
 * jemalloc is linked in, and glibc exposes no equivalent knob.
 * The thread count comes from stats rather than opt.background_thread so that
 * a background thread that failed to spawn is distinguishable from one that
 * was merely requested.
 * Not thread-safe with respect to other allocator tuning; call before spawning
 * worker threads.
 */
inline void LogAllocatorStatus() {
    const char* version = "unknown";
    ReadJemallocKnob("version", version);

    bool background_thread = false;
    ReadJemallocKnob("background_thread", background_thread);

    // stats.* report the snapshot published by the last epoch advance, so a
    // read without this write would return the counters from process start.
    uint64_t epoch = 1;
    mallctl("epoch", nullptr, nullptr, &epoch, sizeof(epoch));

    size_t background_threads = 0;
    ReadJemallocKnob("stats.background_thread.num_threads", background_threads);

    // Decay knobs are signed: -1 means the arena never purges.
    ssize_t dirty_decay_ms = 0;
    ReadJemallocKnob("opt.dirty_decay_ms", dirty_decay_ms);
    ssize_t muzzy_decay_ms = 0;
    ReadJemallocKnob("opt.muzzy_decay_ms", muzzy_decay_ms);

    LOG(INFO) << "Allocator: jemalloc " << version
              << ", background_thread=" << (background_thread ? "on" : "off")
              << ", background_threads=" << background_threads
              << ", dirty_decay_ms=" << dirty_decay_ms
              << ", muzzy_decay_ms=" << muzzy_decay_ms;
}

/**
 * @brief Read one knob of the arena-merged statistics view.
 *
 * jemalloc reserves the arena index MALLCTL_ARENAS_ALL for a merged view, so a
 * single read replaces a loop over arenas.narenas.
 */
template <typename T>
inline bool ReadMergedArenaKnob(const char* suffix, T& value) {
    const std::string name =
        "stats.arenas." + std::to_string(MALLCTL_ARENAS_ALL) + "." + suffix;
    return ReadJemallocKnob(name.c_str(), value);
}

/**
 * @brief Fill a JemallocSnapshot from mallctl, for SetJemallocStatsCollector.
 *
 * Every stats.* knob returns the values published by the last epoch advance, so
 * the epoch is written first. Collection fails as a whole rather than partially
 * when the allocator cannot answer, because a partial snapshot is indis-
 * tinguishable on a dashboard from a healthy one that happens to read zero.
 * Not thread-safe against allocator tuning, and the epoch advance takes a lock
 * shared with allocating threads, so callers must rate-limit.
 *
 * @param snapshot Overwritten field by field on success, including its bin
 *                 vector, so the same instance can be sampled repeatedly. Left
 *                 untouched when the epoch write fails.
 * @return false when the epoch could not be advanced.
 */
inline bool CollectJemallocStats(JemallocSnapshot& snapshot) {
    // A build without --enable-stats still answers the epoch write, so without
    // this check every stats.* read would fail and the whole snapshot would
    // report a plausible-looking zero.
    bool stats_enabled = false;
    if (!ReadJemallocKnob("config.stats", stats_enabled) || !stats_enabled) {
        return false;
    }

    uint64_t epoch = 1;
    if (mallctl("epoch", nullptr, nullptr, &epoch, sizeof(epoch)) != 0) {
        return false;
    }

    // Dirty and muzzy are reported in pages, so a missing page size would turn
    // both into a silent zero that reads as "nothing awaiting decay".
    size_t page_size = 0;
    if (!ReadJemallocKnob("arenas.page", page_size) || page_size == 0) {
        return false;
    }

    // Every size is read into the width jemalloc declares for it; mallctl
    // rejects a mismatched width outright rather than converting.
    auto read_size = [](const char* name, uint64_t& out) {
        size_t value = 0;
        if (ReadJemallocKnob(name, value)) {
            out = value;
        }
    };
    auto read_merged_size = [](const char* suffix, uint64_t& out) {
        size_t value = 0;
        if (ReadMergedArenaKnob(suffix, value)) {
            out = value;
        }
    };
    auto read_merged_u64 = [](const char* suffix, uint64_t& out) {
        uint64_t value = 0;
        if (ReadMergedArenaKnob(suffix, value)) {
            out = value;
        }
    };

    read_size("stats.allocated", snapshot.allocated);
    read_size("stats.active", snapshot.active);
    read_size("stats.metadata", snapshot.metadata);
    read_size("stats.resident", snapshot.resident);
    read_size("stats.retained", snapshot.retained);
    read_size("stats.mapped", snapshot.mapped);
    read_size("stats.background_thread.num_threads",
              snapshot.background_threads);

    read_merged_size("small.allocated", snapshot.small_allocated);
    read_merged_size("large.allocated", snapshot.large_allocated);

    size_t dirty_pages = 0;
    size_t muzzy_pages = 0;
    ReadMergedArenaKnob("pdirty", dirty_pages);
    ReadMergedArenaKnob("pmuzzy", muzzy_pages);
    snapshot.dirty_bytes = uint64_t{dirty_pages} * page_size;
    snapshot.muzzy_bytes = uint64_t{muzzy_pages} * page_size;

    // jemalloc 5 keeps the two decay paths apart and exposes no combined
    // counter, so both halves are reported rather than summed away.
    read_merged_u64("dirty_purged", snapshot.dirty_purged_pages);
    read_merged_u64("muzzy_purged", snapshot.muzzy_purged_pages);
    read_merged_u64("dirty_npurge", snapshot.dirty_purge_runs);
    read_merged_u64("muzzy_npurge", snapshot.muzzy_purge_runs);
    read_merged_u64("dirty_nmadvise", snapshot.dirty_madvises);
    read_merged_u64("muzzy_nmadvise", snapshot.muzzy_madvises);
    read_merged_u64("small.nmalloc", snapshot.small_allocations);
    read_merged_u64("small.ndalloc", snapshot.small_deallocations);

    unsigned arenas = 0;
    if (ReadJemallocKnob("arenas.narenas", arenas)) {
        snapshot.arenas = arenas;
    }

    // Every small size class is reported, drained ones included, so that a
    // class whose slabs are all released keeps its series at zero instead of
    // going stale.
    unsigned nbins = 0;
    snapshot.bins.clear();
    if (ReadJemallocKnob("arenas.nbins", nbins)) {
        snapshot.bins.reserve(nbins);
        for (unsigned i = 0; i < nbins; ++i) {
            const std::string index = std::to_string(i);
            JemallocBinStats bin;
            size_t bin_size = 0;
            uint32_t regs_per_slab = 0;
            if (!ReadJemallocKnob(("arenas.bin." + index + ".size").c_str(),
                                  bin_size) ||
                !ReadJemallocKnob(("arenas.bin." + index + ".nregs").c_str(),
                                  regs_per_slab)) {
                continue;
            }
            bin.size = bin_size;
            bin.regs_per_slab = regs_per_slab;

            size_t regs = 0;
            size_t slabs = 0;
            ReadMergedArenaKnob(("bins." + index + ".curregs").c_str(), regs);
            ReadMergedArenaKnob(("bins." + index + ".curslabs").c_str(), slabs);
            bin.regs = regs;
            bin.slabs = slabs;
            snapshot.bins.push_back(bin);
        }
    }

    // A decay of -1 means the arena never purges, so the signed width has to be
    // preserved all the way into the gauge.
    ssize_t dirty_decay_ms = 0;
    ssize_t muzzy_decay_ms = 0;
    ReadJemallocKnob("opt.dirty_decay_ms", dirty_decay_ms);
    ReadJemallocKnob("opt.muzzy_decay_ms", muzzy_decay_ms);
    snapshot.opt_dirty_decay_ms = dirty_decay_ms;
    snapshot.opt_muzzy_decay_ms = muzzy_decay_ms;

    return true;
}

/**
 * @brief Publish CollectJemallocStats so that /metrics reports the allocator.
 *
 * A no-op in a binary that does not link jemalloc, which keeps the call site in
 * main() free of preprocessor branches.
 */
inline void InstallAllocatorStatsCollector() {
    SetJemallocStatsCollector(&CollectJemallocStats);
}

#else

inline void LogAllocatorStatus() {}

inline void InstallAllocatorStatsCollector() {}

#endif  // STORE_USE_JEMALLOC

}  // namespace mooncake
