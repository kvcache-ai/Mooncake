// Copyright 2026 KVCache.AI
// Licensed under the Apache License, Version 2.0.
#include "../benchmark/peer_workload.h"
#include <gtest/gtest.h>

namespace w = mooncake::tent::workload;

static w::Json spec() {
    return {{"slots", 2},
            {"target", {{"count", 3}, {"interval_us", 5000}}},
            {"background", {{"count", 4}, {"interval_us", 2000}}}};
}

TEST(PeerWorkload, CostCaseUsesTwoMiBRequestsInDisjointSlots) {
    auto config = spec();
    config["request_bytes"] = 2 * w::MiB;
    config["block_bytes"] = 64 * 1024;
    const auto events = w::plan(config);
    for (const auto& event : events) {
        EXPECT_EQ(event.bytes, 2 * w::MiB);
    }
    for (const auto& p : events) {
        if (p.group != "target") continue;
        for (const auto& q : events)
            if (q.group == "background")
                EXPECT_LE(p.offset + p.bytes, q.offset);
    }
}

TEST(PeerWorkload, IndependentArrivalsAndDisjointRegions) {
    auto events = w::plan(spec());
    ASSERT_EQ(events.size(), 7u);
    std::vector<uint64_t> times;
    for (const auto& e : events) {
        times.push_back(e.planned_ns / 1000);
        if (e.group == "target")
            EXPECT_LT(e.offset, 128 * w::MiB);
        else
            EXPECT_GE(e.offset, 128 * w::MiB);
    }
    EXPECT_EQ(times,
              (std::vector<uint64_t>{0, 0, 2000, 4000, 5000, 6000, 10000}));
    EXPECT_EQ(w::memoryBytes(events), 320 * w::MiB);
}

TEST(PeerWorkload, SubmitsBothStreamsBeforeFirstCompletion) {
    auto events = w::plan(spec());
    int64_t now = 0;
    int submitted = 0;
    w::Hooks hooks;
    hooks.now = [&] { return now; };
    hooks.wait_until = [&](int64_t t) { now = t; };
    hooks.submit = [&](w::Record&) {
        ++submitted;
        return std::string{};
    };
    hooks.poll = [&](w::Record& r) -> w::Completion {
        if (now < 20'000'000) return {};
        EXPECT_EQ(submitted, 7);
        return {true, true, r.event.bytes, "", now};
    };
    hooks.cancel = [](auto&) { FAIL() << "unexpected timeout"; };
    auto records = w::run(events, 30'000'000, 1'000'000, hooks);
    auto result = w::summary(records, now);
    EXPECT_EQ(result["target"]["success"], 3);
    EXPECT_EQ(result["background"]["success"], 4);
    EXPECT_EQ(result["all"]["success"], 7);
}

TEST(PeerWorkload, FailedSubmissionAndUnfinishedAreNotSuccess) {
    auto s = spec();
    s["target"]["count"] = 1;
    s["background"]["count"] = 1;
    auto events = w::plan(s);
    int64_t now = 0;
    int canceled = 0;
    w::Hooks hooks;
    hooks.now = [&] { return now; };
    hooks.wait_until = [&](int64_t t) { now = t; };
    hooks.submit = [](w::Record& r) {
        return r.event.group == "target" ? "rejected" : "";
    };
    hooks.poll = [](w::Record& r) -> w::Completion {
        if (r.event.group == "target") return {true, false, 0, "rejected"};
        return {};
    };
    hooks.cancel = [&](auto&) { ++canceled; };
    auto result = w::summary(w::run(events, 20000, 20000, hooks), 40000);
    EXPECT_EQ(result["all"]["success"], 0);
    EXPECT_EQ(result["target"]["failed"], 1);
    EXPECT_EQ(result["background"]["unfinished"], 1);
    EXPECT_EQ(canceled, 1);
}

TEST(PeerWorkload, BusyQSlotDoesNotGateDuePRequests) {
    int64_t now = 0;
    w::Hooks hooks;
    hooks.now = [&] { return now; };
    hooks.wait_until = [&](int64_t t) { now = t; };
    hooks.ready = [&](const w::Record& r) {
        return r.event.group == "target" || now >= 20'000'000;
    };
    hooks.submit = [](w::Record&) { return std::string{}; };
    hooks.poll = [](w::Record& r) -> w::Completion {
        return {true, true, r.event.bytes, ""};
    };
    hooks.cancel = [](auto&) { FAIL() << "unexpected timeout"; };
    auto records = w::run(w::plan(spec()), 30'000'000, 1'000'000, hooks);
    for (const auto& r : records) {
        ASSERT_TRUE(r.success);
        if (r.event.group == "target")
            EXPECT_EQ(r.submit_ns, r.event.planned_ns);
        else
            EXPECT_GE(r.submit_ns, 20'000'000);
    }
}
