// Copyright 2026 KVCache.AI
// Licensed under the Apache License, Version 2.0.
#pragma once

#include <algorithm>
#include <cstdint>
#include <functional>
#include <stdexcept>
#include <string>
#include <vector>
#include "tent/thirdparty/nlohmann/json.h"

namespace mooncake::tent::workload {
using Json = nlohmann::json;
constexpr uint64_t MiB = 1ULL << 20;
struct Event {
    size_t id;
    std::string group;
    uint64_t planned_ns, offset, bytes;
};

inline std::vector<Event> plan(const Json& config) {
    const int slots = config.value("slots", 32);
    if (slots < 1) throw std::invalid_argument("slots must be positive");
    const uint64_t request_bytes = config.value("request_bytes", 64 * MiB);
    if (!request_bytes || request_bytes > 64 * MiB)
        throw std::invalid_argument("request_bytes must be in (0, 64 MiB]");
    if (config.value("block_bytes", 2 * MiB) == 0)
        throw std::invalid_argument("block_bytes must be positive");
    std::vector<Event> events;
    int group_index = 0;
    for (const char* group : {"target", "background"}) {
        const auto& stream = config.at(group);
        int count = stream.value("count", 100);
        int64_t period = stream.value("interval_us", int64_t{5000});
        if (count < 1 || period <= 0)
            throw std::invalid_argument(
                "positive count and interval_us required");
        for (int i = 0; i < count; ++i)
            events.push_back(
                {0, group, uint64_t(i) * period * 1000,
                 uint64_t(group_index * slots + i % slots) * 64 * MiB,
                 request_bytes});
        ++group_index;
    }
    std::stable_sort(events.begin(), events.end(),
                     [](const auto& a, const auto& b) {
                         return a.planned_ns < b.planned_ns;
                     });
    for (size_t i = 0; i < events.size(); ++i) events[i].id = i;
    return events;
}

inline uint64_t memoryBytes(const std::vector<Event>& events) {
    // A separate scratch region is used for warmup, after all request slots.
    uint64_t bytes = 0;
    for (const auto& e : events) bytes = std::max(bytes, e.offset + e.bytes);
    return bytes + 64 * MiB;
}

struct Record {
    explicit Record(const Event& e) : event(e) {}
    Event event;
    int64_t submit_ns = -1, returned_ns = -1, terminal_ns = -1;
    bool done = false, success = false, timed_out = false, unfinished = false;
    uint64_t completed_bytes = 0;
    std::string error;
    Json detail = Json::object();
};
struct Completion {
    bool terminal = false, success = false;
    uint64_t bytes = 0;
    std::string error;
    int64_t visible_ns = -1;
};
// Only the small arrival/recording controller is injectable. No network or
// scheduling model: production callbacks below use a real TransferEngine.
struct Hooks {
    std::function<int64_t()> now;
    std::function<void(int64_t)> wait_until;
    std::function<bool(const Record&)> ready;
    std::function<std::string(Record&)> submit;
    std::function<Completion(Record&)> poll;
    std::function<void(Record&)> cancel;
};
inline std::vector<Record> run(const std::vector<Event>& events,
                               int64_t timeout_ns, int64_t drain_ns,
                               const Hooks& hooks) {
    std::vector<Record> records;
    for (const auto& e : events) records.push_back(Record{e});
    size_t next = 0, done = 0;
    while (done != records.size()) {
        // Submit every due arrival before polling; never gate on completion.
        while (next < records.size() &&
               hooks.now() >= int64_t(records[next].event.planned_ns))
            ++next;
        for (size_t i = 0; i < next; ++i) {
            auto& r = records[i];
            if (r.done || r.submit_ns >= 0) continue;
            if (hooks.now() >= int64_t(r.event.planned_ns) + timeout_ns) {
                r.timed_out = r.done = true;
                r.terminal_ns = hooks.now();
                r.error = "deadline_before_submission";
                ++done;
                continue;
            }
            if (hooks.ready && !hooks.ready(r)) continue;
            r.submit_ns = hooks.now();
            r.error = hooks.submit(r);
            r.returned_ns = hooks.now();
        }
        for (size_t i = 0; i < next; ++i) {
            auto& r = records[i];
            if (r.done || r.submit_ns < 0) continue;
            auto c = hooks.poll(r);
            auto now = hooks.now();
            if (c.terminal) {
                r.terminal_ns = c.visible_ns >= 0 ? c.visible_ns : now;
                r.done = true;
                r.timed_out =
                    r.timed_out ||
                    r.terminal_ns >= int64_t(r.event.planned_ns) + timeout_ns;
                r.success = c.success && r.error.empty() && !r.timed_out;
                r.completed_bytes = c.bytes;
                if (!c.error.empty()) r.error = c.error;
                ++done;
            } else if (now >= int64_t(r.event.planned_ns) + timeout_ns) {
                if (!r.timed_out) {
                    r.timed_out = true;
                    hooks.cancel(r);
                }
                if (now >=
                    int64_t(r.event.planned_ns) + timeout_ns + drain_ns) {
                    r.done = r.unfinished = true;
                    r.error += " unfinished_after_cancel";
                    ++done;
                }
            }
        }
        if (done != records.size()) {
            int64_t wake = hooks.now() + 10000;  // bounded polling visibility
            if (next < records.size() &&
                int64_t(records[next].event.planned_ns) > hooks.now())
                wake = std::min(wake, int64_t(records[next].event.planned_ns));
            hooks.wait_until(wake);
        }
    }
    return records;
}

inline Json recordJson(const Record& r) {
    Json j = {{"id", r.event.id},
              {"group", r.event.group},
              {"planned_ns", r.event.planned_ns},
              {"offset", r.event.offset},
              {"bytes", r.event.bytes},
              {"submit_ns", r.submit_ns},
              {"submit_return_ns", r.returned_ns},
              {"terminal_visible_ns", r.terminal_ns},
              {"success", r.success},
              {"timed_out", r.timed_out},
              {"unfinished", r.unfinished},
              {"completed_bytes", r.completed_bytes},
              {"error", r.error},
              {"detail", r.detail}};
    j["full_wait_ns"] = r.terminal_ns < 0
                            ? Json(nullptr)
                            : Json(r.terminal_ns - int64_t(r.event.planned_ns));
    return j;
}
inline Json summary(const std::vector<Record>& records, int64_t elapsed_ns) {
    Json result;
    for (const auto* group : {"background", "target", "all"}) {
        size_t samples = 0, success = 0, failed = 0, timeouts = 0,
               unfinished = 0;
        uint64_t bytes = 0;
        std::vector<int64_t> waits, terminal_waits;
        for (const auto& r : records) {
            if (std::string(group) != "all" && r.event.group != group) continue;
            ++samples;
            if (r.done && !r.unfinished)
                terminal_waits.push_back(r.terminal_ns -
                                         int64_t(r.event.planned_ns));
            timeouts += r.timed_out;
            unfinished += r.unfinished;
            if (r.success) {
                ++success;
                bytes += r.event.bytes;
                waits.push_back(r.terminal_ns - int64_t(r.event.planned_ns));
            } else if (!r.timed_out && !r.unfinished)
                ++failed;
        }
        std::sort(waits.begin(), waits.end());
        std::sort(terminal_waits.begin(), terminal_waits.end());
        double wait_sum = 0;
        for (auto ns : waits) wait_sum += ns;
        auto percentile = [](const std::vector<int64_t>& values,
                             size_t p) -> Json {
            if (values.empty()) return nullptr;
            return values[(values.size() * p + 99) / 100 - 1] / 1000.0;
        };
        double terminal_sum = 0;
        for (auto ns : terminal_waits) terminal_sum += ns;
        result[group] = {
            {"samples", samples},
            {"all_terminal_samples", terminal_waits.size()},
            {"all_terminal_full_wait_us_mean",
             terminal_waits.empty()
                 ? Json(nullptr)
                 : Json(terminal_sum / terminal_waits.size() / 1000)},
            {"all_terminal_full_wait_us_p50", percentile(terminal_waits, 50)},
            {"all_terminal_full_wait_us_p95", percentile(terminal_waits, 95)},
            {"all_terminal_full_wait_us_p99", percentile(terminal_waits, 99)},
            {"success", success},
            {"failed", failed},
            {"timeouts", timeouts},
            {"unfinished", unfinished},
            {"successful_bytes", bytes},
            {"success_full_wait_us_mean",
             waits.empty() ? Json(nullptr)
                           : Json(wait_sum / waits.size() / 1000)},
            {"success_full_wait_us_p50", percentile(waits, 50)},
            {"success_full_wait_us_p95", percentile(waits, 95)},
            {"success_full_wait_us_p99", percentile(waits, 99)},
            {"successful_GBps",
             elapsed_ns > 0 ? double(bytes) / elapsed_ns : 0.0}};
    }
    result["measurement_ns"] = elapsed_ns;
    return result;
}
}  // namespace mooncake::tent::workload
