// Copyright 2026 KVCache.AI
// Licensed under the Apache License, Version 2.0.
#pragma once

#include <algorithm>
#include <cstdint>
#include <functional>
#include <cstring>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>
#include "tent/thirdparty/nlohmann/json.h"

namespace mooncake::tent::workload {
using Json = nlohmann::json;
constexpr uint64_t MiB = 1ULL << 20;
inline Json parseTrace(const std::string& text, int64_t observed_ns) {
    Json item = {{"observed_steady_ns", observed_ns}, {"raw", text}};
    std::istringstream stream(text);
    std::string token;
    while (stream >> token) {
        auto pos = token.find('=');
        if (pos == std::string::npos) continue;
        auto key = token.substr(0, pos), value = token.substr(pos + 1);
        if (key == "trace_id" || key == "candidates" || key == "probe" ||
            key == "slices" || key == "bytes" || key == "stream_call")
            item[key] = std::stoull(value);
    }
    auto pos = text.find("nic:assigned/inflight/bps=");
    Json rails = Json::array();
    if (pos != std::string::npos) {
        std::istringstream entries(
            text.substr(pos + std::strlen("nic:assigned/inflight/bps=")));
        while (entries >> token) {
            auto colon = token.find(':');
            if (colon == std::string::npos) continue;
            std::string values = token.substr(colon + 1);
            std::replace(values.begin(), values.end(), '/', ' ');
            std::istringstream fields(values);
            uint64_t assigned, inflight;
            double bps;
            if (fields >> assigned >> inflight >> bps)
                rails.push_back({{"name", token.substr(0, colon)},
                                 {"assigned_bytes", assigned},
                                 {"inflight_bytes", inflight},
                                 {"bandwidth_Bps", bps}});
        }
    }
    item["rails"] = rails;
    return item;
}
inline bool validBacklog(const Json& traces, const std::string& busy,
                         uint64_t target_bytes, int64_t background_pending_ns) {
    if (traces.size() != 1) return false;
    const auto& t = traces[0];
    uint64_t busy_bytes = 0, other_bytes = 0, assigned = 0;
    for (const auto& rail : t.at("rails")) {
        assigned += rail.at("assigned_bytes").get<uint64_t>();
        if (rail.at("name") == busy)
            busy_bytes = rail.at("inflight_bytes");
        else
            other_bytes += rail.at("inflight_bytes").get<uint64_t>();
    }
    return t.value("candidates", 0) == 2 && t.value("probe", 1) == 0 &&
           t.at("rails").size() == 2 && assigned == target_bytes &&
           busy_bytes > other_bytes &&
           background_pending_ns >= t.at("observed_ns").get<int64_t>();
}

struct Event {
    size_t id;
    std::string group;
    uint64_t planned_ns, offset, bytes;
    int background = -1;
};

inline std::vector<Event> plan(const Json& config) {
    const auto mode = config.value("mode", std::string("burst"));
    const int count = config.value("count", 8);
    const int burst = config.value("burst_size", 4);
    const int64_t interval = config.value("interval_us", int64_t{5000});
    const int64_t gap = config.value("background_gap_us", int64_t{0});
    const int64_t bg_mib = config.value("background_mib", int64_t{32});
    if ((mode != "burst" && mode != "biased_backlog") || count < 1 ||
        burst < 1 || interval < 0 || gap < 0 || bg_mib < 1 ||
        (mode == "biased_backlog" && gap > interval))
        throw std::invalid_argument("invalid workload plan");
    std::vector<Event> events;
    uint64_t offset = 0;
    auto add = [&](std::string group, uint64_t ns, uint64_t bytes, int bg) {
        events.push_back(
            {events.size(), std::move(group), ns, offset, bytes, bg});
        offset += bytes;
    };
    for (int i = 0; i < count; ++i) {
        if (mode == "burst") {
            add("target", uint64_t(i / burst) * interval * 1000, 64 * MiB, -1);
        } else {
            int bg = events.size();
            add("background", uint64_t(i) * interval * 1000, bg_mib * MiB, -1);
            add("target", (uint64_t(i) * interval + gap) * 1000, 64 * MiB, bg);
        }
    }
    return events;
}

inline uint64_t memoryBytes(const std::vector<Event>& events) {
    // A separate scratch region is used for warmup, after all request slots.
    return events.back().offset + events.back().bytes + 64 * MiB;
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
               hooks.now() >= int64_t(records[next].event.planned_ns)) {
            auto& r = records[next++];
            if (hooks.now() >= int64_t(r.event.planned_ns) + timeout_ns) {
                r.timed_out = r.done = true;
                r.terminal_ns = hooks.now();
                r.error = "deadline_before_submission";
                ++done;
                continue;
            }
            r.submit_ns = hooks.now();
            r.error = hooks.submit(r);
            r.returned_ns = hooks.now();
        }
        for (size_t i = 0; i < next; ++i) {
            auto& r = records[i];
            if (r.done) continue;
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
            if (next < records.size())
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
              {"background_id", r.event.background},
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
    for (const auto* group : {"background", "target"}) {
        size_t samples = 0, success = 0, failed = 0, timeouts = 0,
               unfinished = 0;
        uint64_t bytes = 0;
        std::vector<int64_t> waits;
        for (const auto& r : records) {
            if (r.event.group != group) continue;
            ++samples;
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
        auto percentile = [&](size_t p) -> Json {
            if (waits.empty()) return nullptr;
            return waits[(waits.size() * p + 99) / 100 - 1] / 1000.0;
        };
        result[group] = {{"samples", samples},
                         {"success", success},
                         {"failed", failed},
                         {"timeouts", timeouts},
                         {"unfinished", unfinished},
                         {"successful_bytes", bytes},
                         {"success_full_wait_us_p50", percentile(50)},
                         {"success_full_wait_us_p95", percentile(95)},
                         {"success_full_wait_us_p99", percentile(99)},
                         {"successful_GBps",
                          elapsed_ns > 0 ? double(bytes) / elapsed_ns : 0.0}};
    }
    result["measurement_ns"] = elapsed_ns;
    return result;
}
}  // namespace mooncake::tent::workload
