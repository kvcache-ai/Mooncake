#pragma once

#include <glog/logging.h>

#ifdef STORE_USE_JEMALLOC
#include <jemalloc/jemalloc.h>
#include <sys/types.h>

#include <cstddef>
#include <cstdint>
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

#else

inline void LogAllocatorStatus() {}

#endif  // STORE_USE_JEMALLOC

}  // namespace mooncake
