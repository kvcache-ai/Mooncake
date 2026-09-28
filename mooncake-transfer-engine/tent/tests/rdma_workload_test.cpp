// Copyright 2026 KVCache.AI
// Licensed under the Apache License, Version 2.0.
#include "../benchmark/rdma_workload.h"
#include <gtest/gtest.h>
using namespace mooncake::tent::workload;

TEST(RdmaWorkloadControl, PlanHasDisjointSlotsAndPredeterminedArrivals) {
    auto burst = plan({{"mode", "burst"},
                       {"count", 5},
                       {"burst_size", 2},
                       {"interval_us", 10}});
    EXPECT_EQ(burst[0].planned_ns, 0);
    EXPECT_EQ(burst[1].planned_ns, 0);
    EXPECT_EQ(burst[2].planned_ns, 10000);
    auto biased = plan({{"mode", "biased_backlog"},
                        {"count", 3},
                        {"background_mib", 32},
                        {"background_gap_us", 2}});
    for (size_t i = 0; i < biased.size(); ++i) {
        if (i) {
            EXPECT_EQ(biased[i - 1].offset + biased[i - 1].bytes,
                      biased[i].offset);
        }
        if (i % 2) {
            EXPECT_EQ(biased[i].background, int(i - 1));
            EXPECT_EQ(biased[i].planned_ns - biased[i - 1].planned_ns, 2000);
        }
    }
    EXPECT_EQ(memoryBytes(biased), (3 * (32 + 64) + 64) * MiB);
    EXPECT_THROW(plan({{"count", 0}}), std::invalid_argument);
}

TEST(RdmaWorkloadControl, DueRequestsSubmitBeforeAnyCompletion) {
    auto events = plan({{"count", 4}, {"burst_size", 4}});
    int64_t clock = 0;
    size_t submitted = 0, polls = 0;
    Hooks hooks;
    hooks.now = [&] { return clock; };
    hooks.wait_until = [&](int64_t t) { clock = t; };
    hooks.submit = [&](Record&) {
        ++submitted;
        clock += 100;
        return std::string{};
    };
    hooks.poll = [&](Record& r) {
        EXPECT_EQ(submitted, 4);
        ++polls;
        return Completion{clock >= 10000, true, r.event.bytes, ""};
    };
    hooks.cancel = [](Record&) { FAIL() << "unexpected cancellation"; };
    auto records = run(events, 100000, 10000, hooks);
    EXPECT_GT(polls, 4);
    EXPECT_EQ(records[3].submit_ns, 300);
    EXPECT_EQ(records[0].terminal_ns, 10400);
    EXPECT_EQ(recordJson(records[3])["full_wait_ns"], 10400);
    auto stats = summary(records, 10400);
    EXPECT_EQ(stats["target"]["success"], 4);
}

TEST(RdmaWorkloadControl, FailureTimeoutAndUnfinishedAreNotSuccess) {
    auto events = plan({{"count", 3}, {"burst_size", 3}});
    int64_t clock = 0;
    size_t cancels = 0;
    Hooks hooks;
    hooks.now = [&] { return clock; };
    hooks.wait_until = [&](int64_t t) { clock = t; };
    hooks.submit = [](Record& r) {
        return r.event.id == 0 ? "submit rejected" : "";
    };
    hooks.poll = [&](Record& r) {
        if (r.event.id == 0)
            return Completion{true, false, 0, "submit rejected"};
        if (r.event.id == 1 && clock >= 20000)
            return Completion{true, false, 0, "canceled"};
        return Completion{};
    };
    hooks.cancel = [&](Record&) { ++cancels; };
    auto records = run(events, 10000, 20000, hooks);
    EXPECT_EQ(cancels, 2);
    EXPECT_TRUE(records[1].timed_out);
    EXPECT_EQ(records[1].terminal_ns, 20000);
    EXPECT_TRUE(records[2].unfinished);
    EXPECT_EQ(recordJson(records[2])["full_wait_ns"], nullptr);
    auto stats = summary(records, clock);
    EXPECT_EQ(stats["target"]["success"], 0);
    EXPECT_EQ(stats["target"]["failed"], 1);
    EXPECT_EQ(stats["target"]["timeouts"], 2);
    EXPECT_EQ(stats["target"]["unfinished"], 1);
    EXPECT_EQ(stats["target"]["success_full_wait_us_p99"], nullptr);
}

TEST(RdmaWorkloadControl, LateAdmissionRemainsInFullWait) {
    auto events = plan({{"count", 2}, {"burst_size", 2}});
    int64_t clock = 0;
    Hooks hooks;
    hooks.now = [&] { return clock; };
    hooks.wait_until = [&](int64_t t) { clock = t; };
    hooks.submit = [&](Record&) {
        clock += 20000;
        return std::string{};
    };
    hooks.poll = [](Record& r) {
        return Completion{true, true, r.event.bytes, ""};
    };
    hooks.cancel = [](Record&) {};
    auto records = run(events, 10000, 10000, hooks);
    EXPECT_TRUE(records[1].timed_out);
    EXPECT_EQ(records[1].submit_ns, -1);
    EXPECT_EQ(records[1].error, "deadline_before_submission");
    EXPECT_EQ(recordJson(records[0])["full_wait_ns"], 20000);
    EXPECT_FALSE(records[0].success);
}

TEST(RdmaWorkloadControl,
     TraceAndBacklogEvidenceRejectCompletedOrFilteredCases) {
    // Parser/control fixture only; this is not a real RDMA backlog experiment.
    auto trace = parseTrace(
        "RDMA_BATCH policy=1 probe=0 stream=normal stream_call=1 trace_id=123 "
        "candidates=2 slices=32 bytes=64 nic:assigned/inflight/bps= "
        "mlx5_0:16/32/1.25e+10 mlx5_1:48/0/1.25e+10",
        900);
    EXPECT_EQ(trace["trace_id"], 123);
    ASSERT_EQ(trace["rails"].size(), 2);
    EXPECT_EQ(trace["rails"][0]["inflight_bytes"], 32);
    trace["observed_ns"] = 100;
    auto traces = Json::array({trace});
    EXPECT_TRUE(validBacklog(traces, "mlx5_0", 64, 110));
    EXPECT_FALSE(validBacklog(traces, "mlx5_0", 64, 90));
    EXPECT_FALSE(validBacklog(traces, "mlx5_1", 64, 110));
    traces[0]["candidates"] = 1;
    EXPECT_FALSE(validBacklog(traces, "mlx5_0", 64, 110));
    traces[0]["candidates"] = 2;
    traces[0]["probe"] = 1;
    EXPECT_FALSE(validBacklog(traces, "mlx5_0", 64, 110));
}
