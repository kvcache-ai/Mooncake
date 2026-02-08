// Copyright 2025 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef TENT_PREFAULT_H
#define TENT_PREFAULT_H

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <thread>
#include <vector>

#include <errno.h>
#include <sys/mman.h>

#include <glog/logging.h>

namespace mooncake {
namespace tent {

// Runtime capability detection for prefault methods.
// Cached after first detection to avoid repeated syscall failures.
struct PrefaultCapabilities {
    bool madvise_available = false;
    bool mlock_available = false;
    bool detected = false;

    static PrefaultCapabilities& getInstance() {
        static PrefaultCapabilities instance;
        return instance;
    }

    void detect() {
        if (detected) return;

        // Use mmap (not malloc) so the test region is page-aligned and
        // backed by a proper VMA – required by MADV_POPULATE_WRITE.
        const size_t test_size = 4096;
        void* test_mem = mmap(nullptr, test_size, PROT_READ | PROT_WRITE,
                              MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
        if (test_mem == MAP_FAILED) {
            detected = true;
            return;
        }

#ifdef MADV_POPULATE_WRITE
        // Test madvise capability
        int madv_rc = madvise(test_mem, test_size, MADV_POPULATE_WRITE);
        if (madv_rc == 0) {
            madvise_available = true;
        } else {
            // EINVAL: not supported by kernel
            // ENOSYS: not implemented
            // Other errors also mean unavailable
            madvise_available = false;
        }
#endif

        // Test mlock capability
        int mlock_rc = mlock(test_mem, test_size);
        if (mlock_rc == 0) {
            mlock_available = true;
            munlock(test_mem, test_size);
        } else {
            // EPERM: insufficient permissions (common in containers)
            // ENOMEM: exceeded RLIMIT_MEMLOCK
            // Other errors also mean unavailable
            mlock_available = false;
        }

        munmap(test_mem, test_size);
        detected = true;

        // Log detected capabilities once for visibility
        LOG(INFO) << "Prefault capability detection: madvise="
                  << (madvise_available ? "available" : "unavailable")
                  << ", mlock="
                  << (mlock_available ? "available" : "unavailable")
                  << " (touch always available as fallback)";
    }
};

// Prefault helpers are used to reduce page-fault overhead before NUMA probing
// (e.g., numa_move_pages). This is CPU-side prefaulting and is distinct from
// RDMA MR warm-up, which temporarily registers memory to touch/pin pages in
// the RDMA driver path.
struct PrefaultOptions {
    enum class Mode {
        // Auto: try madvise first, then mlock, finally touch as fallback.
        kAuto,
        // madvise(MADV_POPULATE_WRITE) if supported by the kernel.
        kMadvise,
        // mlock/munlock to force pages into memory.
        kMlock,
        // CPU touch (read/write) each page.
        kTouch,
    };

    Mode mode = Mode::kAuto;
    size_t page_size = 4096;
    unsigned max_touch_threads = 8;
    unsigned max_madvise_threads = 8;
    size_t madvise_chunk_bytes = 1ULL << 30;  // 1GB
    int touch_single_thread_threshold_pages = 4096;
};

struct PrefaultResult {
    const char* method = "none";
    unsigned threads = 0;
    size_t chunk_bytes = 0;
    int err = 0;
    long duration_ms = 0;
};

inline PrefaultResult prefaultPages(void** pages, int n,
                                    uintptr_t aligned_start,
                                    const PrefaultOptions& options) {
    PrefaultResult result;
    if (!pages || n <= 0) {
        return result;
    }

    const size_t page_size = options.page_size ? options.page_size : 4096;
    const size_t aligned_len = static_cast<size_t>(n) * page_size;
    const bool allow_fallback = options.mode == PrefaultOptions::Mode::kAuto;

    // Runtime capability detection for Auto mode
    PrefaultCapabilities& caps = PrefaultCapabilities::getInstance();
    if (allow_fallback) {
        caps.detect();
    }

    // Determine which methods to try based on mode and runtime capabilities
    const bool allow_madvise =
        (options.mode == PrefaultOptions::Mode::kMadvise) ||
        (options.mode == PrefaultOptions::Mode::kAuto &&
         caps.madvise_available);
    const bool allow_mlock =
        (options.mode == PrefaultOptions::Mode::kMlock) ||
        (options.mode == PrefaultOptions::Mode::kAuto && caps.mlock_available);
    const bool allow_touch = options.mode == PrefaultOptions::Mode::kAuto ||
                             options.mode == PrefaultOptions::Mode::kTouch;

#ifdef MADV_POPULATE_WRITE
    if (allow_madvise) {
        {
            auto madv_start = std::chrono::steady_clock::now();
            int madv_rc = 0;
            int madv_errno = 0;
            unsigned hwc = std::thread::hardware_concurrency();
            if (hwc == 0) hwc = 1;
            size_t madvise_chunk = options.madvise_chunk_bytes;
            unsigned madvise_threads = static_cast<unsigned>(
                (aligned_len + madvise_chunk - 1) / madvise_chunk);
            if (madvise_threads == 0) madvise_threads = 1;
            unsigned madvise_thread_cap =
                std::min(hwc, options.max_madvise_threads);
            if (madvise_threads > madvise_thread_cap) {
                madvise_threads = madvise_thread_cap;
                madvise_chunk =
                    (aligned_len + madvise_threads - 1) / madvise_threads;
                madvise_chunk =
                    ((madvise_chunk + page_size - 1) / page_size) * page_size;
            }

            if (madvise_threads == 1) {
                madv_rc = madvise(reinterpret_cast<void*>(aligned_start),
                                  aligned_len, MADV_POPULATE_WRITE);
                if (madv_rc != 0) madv_errno = errno;
            } else {
                std::vector<std::thread> madv_threads;
                madv_threads.reserve(madvise_threads);
                std::vector<int> madv_rcs(madvise_threads, 0);
                std::vector<int> madv_errs(madvise_threads, 0);
                for (unsigned t = 0; t < madvise_threads; ++t) {
                    size_t offset = t * madvise_chunk;
                    if (offset >= aligned_len) break;
                    size_t chunk_len =
                        std::min(madvise_chunk, aligned_len - offset);
                    void* chunk_addr =
                        reinterpret_cast<void*>(aligned_start + offset);
                    madv_threads.emplace_back([t, chunk_addr, chunk_len,
                                               &madv_rcs, &madv_errs]() {
                        int rc =
                            madvise(chunk_addr, chunk_len, MADV_POPULATE_WRITE);
                        madv_rcs[t] = rc;
                        if (rc != 0) madv_errs[t] = errno;
                    });
                }
                for (auto& t : madv_threads) t.join();
                for (size_t i = 0; i < madv_rcs.size(); ++i) {
                    if (madv_rcs[i] != 0) {
                        madv_rc = -1;
                        madv_errno = madv_errs[i];
                        break;
                    }
                }
            }

            auto madv_end = std::chrono::steady_clock::now();
            auto madv_ms =
                std::chrono::duration_cast<std::chrono::milliseconds>(
                    madv_end - madv_start)
                    .count();
            if (madv_rc == 0) {
                result.method = "madvise";
                result.threads = madvise_threads;
                result.chunk_bytes = madvise_chunk;
                result.duration_ms = madv_ms;
                return result;
            }

            if (!allow_fallback) {
                result.method = "madvise_failed";
                result.err = madv_errno;
                return result;
            }
        }
    }
#endif

    if (allow_mlock) {
        {
            auto mlock_start = std::chrono::steady_clock::now();
            int mlock_rc =
                mlock(reinterpret_cast<void*>(aligned_start), aligned_len);
            auto mlock_end = std::chrono::steady_clock::now();
            auto mlock_ms =
                std::chrono::duration_cast<std::chrono::milliseconds>(
                    mlock_end - mlock_start)
                    .count();
            if (mlock_rc == 0) {
                munlock(reinterpret_cast<void*>(aligned_start), aligned_len);
                result.method = "mlock";
                result.duration_ms = mlock_ms;
                return result;
            }

            result.err = errno;
            if (!allow_fallback) {
                result.method = "mlock_failed";
                return result;
            }
        }
    }

    if (allow_touch) {
        {
            unsigned hwc = std::thread::hardware_concurrency();
            if (hwc == 0) hwc = 1;
            unsigned num_threads = std::min(hwc, options.max_touch_threads);
            if (n < options.touch_single_thread_threshold_pages)
                num_threads = 1;
            auto touch_start = std::chrono::steady_clock::now();
            std::vector<std::thread> threads;
            threads.reserve(num_threads);
            int block = (n + static_cast<int>(num_threads) - 1) /
                        static_cast<int>(num_threads);
            for (unsigned t = 0; t < num_threads; ++t) {
                int begin = static_cast<int>(t) * block;
                int end = std::min(n, begin + block);
                if (begin >= end) break;
                threads.emplace_back([pages, begin, end]() {
                    for (int i = begin; i < end; ++i) {
                        volatile char* p =
                            reinterpret_cast<volatile char*>(pages[i]);
                        *p = *p;
                    }
                });
            }
            for (auto& thread : threads) thread.join();
            auto touch_end = std::chrono::steady_clock::now();
            auto touch_ms =
                std::chrono::duration_cast<std::chrono::milliseconds>(
                    touch_end - touch_start)
                    .count();
            result.method = "touch";
            result.threads = num_threads;
            result.duration_ms = touch_ms;
        }
    }

    return result;
}

}  // namespace tent
}  // namespace mooncake

#endif  // TENT_PREFAULT_H
