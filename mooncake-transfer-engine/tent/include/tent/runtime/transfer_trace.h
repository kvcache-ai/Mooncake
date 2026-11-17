// Copyright 2026 KVCache.AI
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy at http://www.apache.org/licenses/LICENSE-2.0

#ifndef TENT_TRANSFER_TRACE_H
#define TENT_TRANSFER_TRACE_H

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>

namespace mooncake {
namespace tent {

// One accepted submit interval, retained by its RDMA tasks across retries.
// All offsets use the same monotonic clock; no per-slice clocks when disabled.
struct TransferTrace {
    static bool enabled() {
        static const bool value = [] {
            const char* env = std::getenv("MC_TENT_TRACE_TRANSFER");
            return env && std::strcmp(env, "1") == 0;
        }();
        return value;
    }
    static uint64_t now() {
        return std::chrono::duration_cast<std::chrono::nanoseconds>(
                   std::chrono::steady_clock::now().time_since_epoch())
            .count();
    }
    static void first(std::atomic<uint64_t>& slot, uint64_t ts) {
        auto old = slot.load(std::memory_order_relaxed);
        while ((!old || ts < old) && !slot.compare_exchange_weak(
                                         old, ts, std::memory_order_relaxed)) {
        }
    }
    void posted(uint64_t ts) {
        first(first_post, ts);
        auto old = last_post.load(std::memory_order_relaxed);
        while (ts > old && !last_post.compare_exchange_weak(
                               old, ts, std::memory_order_relaxed)) {
        }
    }
    void finish(uint64_t complete, int status) {
        if (reported) return;  // protected by the runtime batch progress lock
        reported = true;
        const auto enqueue = first_enqueue.load(std::memory_order_relaxed);
        const auto post = first_post.load(std::memory_order_relaxed);
        const auto last = last_post.load(std::memory_order_relaxed);
        auto ms = [](uint64_t end, uint64_t begin) {
            return end && begin && end >= begin
                       ? std::to_string((end - begin) / 1e6)
                       : std::string("null");
        };
        const auto first_post_pct =
            post && complete > start
                ? std::to_string(100.0 * (post - start) / (complete - start))
                : std::string("null");
        std::fprintf(stderr,
                     "TENT_TRACE {\"call\":%llu,\"status\":%d,"
                     "\"runtime_prepare_ms\":%s,\"first_enqueue_ms\":%s,"
                     "\"first_post_ms\":%s,\"enqueue_to_first_post_ms\":%s,"
                     "\"post_window_ms\":%s,\"last_post_to_complete_ms\":%s,"
                     "\"total_api_ms\":%s,\"first_post_api_pct\":%s}\n",
                     static_cast<unsigned long long>(start), status,
                     ms(prepare_end, prepare_start).c_str(),
                     ms(enqueue, start).c_str(), ms(post, start).c_str(),
                     ms(post, enqueue).c_str(), ms(last, post).c_str(),
                     ms(complete, last).c_str(), ms(complete, start).c_str(),
                     first_post_pct.c_str());
    }

    const uint64_t start{now()};
    uint64_t prepare_start{0};
    uint64_t prepare_end{0};
    std::atomic<uint64_t> first_enqueue{0}, first_post{0}, last_post{0};
    bool reported{false};
};

}  // namespace tent
}  // namespace mooncake
#endif  // TENT_TRANSFER_TRACE_H
