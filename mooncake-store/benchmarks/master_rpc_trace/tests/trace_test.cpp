// Copyright 2026 Alibaba Cloud and its affiliates
// Licensed under the Apache License, Version 2.0.
#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <future>
#include <limits>
#include <sstream>
#include <thread>

#include "trace.h"

namespace mooncake::bench {
namespace {

constexpr auto kHeader =
    R"({"type":"master_rpc_trace","version":1,"time_unit":"us"})";

RpcTrace Parse(const std::string& events) {
    std::istringstream stream(std::string(kHeader) + '\n' + events);
    return ReadTrace(stream);
}

const std::string kStart =
    R"({"id":"s","timestamp_us":0,"client_id":"c","op":"BatchPutStart","keys":["x","y"],"value_sizes":[4096,8192]})";
const std::string kEnd =
    R"({"id":"e","timestamp_us":1000,"client_id":"c","op":"BatchPutEnd","keys":["x","y"],"put_start":"s"})";

TEST(TraceValidation, PreservesBatchAndInfersPutDependency) {
    auto trace = Parse(kStart + '\n' + kEnd);
    EXPECT_EQ(trace.events[0].value_sizes, (std::vector<uint64_t>{4096, 8192}));
    EXPECT_EQ(trace.events[1].dependencies, (std::vector<size_t>{0}));
    EXPECT_EQ(trace.events[1].put_start, 0);
    EXPECT_EQ(trace.events[1].timestamp_us, 1000);
}

TEST(TraceValidation, RejectsInvalidTracesBeforeReplay) {
    const std::vector<std::string> invalid = {
        kStart,  // Unfinished write.
        kEnd,    // Missing start.
        kStart + '\n' + kStart + '\n' + kEnd,
        R"({"id":"r","timestamp_us":-1,"client_id":"c","op":"BatchExistKey","keys":["x"]})",
        R"({"id":"r","timestamp_us":0.1,"client_id":"c","op":"BatchExistKey","keys":["x"]})",
        R"({"id":"r","timestamp_us":1.0,"client_id":"c","op":"BatchExistKey","keys":["x"]})",
        R"({"id":"r","timestamp_us":true,"client_id":"c","op":"BatchExistKey","keys":["x"]})",
        R"({"id":"r","timestamp_us":0,"client_id":"c","op":"BatchExistKey","keys":[]})",
        R"({"id":"r","timestamp_us":0,"client_id":"c","op":"BatchExistKey","keys":["x"],"depends_on":["future"]})",
        R"({"id":"r","timestamp_us":0,"client_id":"c","op":"BatchExistKey","keys":["x"],"timestamp":0})",
        R"({"id":"r","id":"other","timestamp_us":0,"client_id":"c","op":"BatchExistKey","keys":["x"]})",
        kStart + '\n' +
            R"({"id":"e","timestamp_us":1000,"client_id":"wrong","op":"BatchPutEnd","keys":["x","y"],"put_start":"s"})",
        kStart + '\n' +
            R"({"id":"e","timestamp_us":1000,"client_id":"c","op":"BatchPutEnd","keys":["y","x"],"put_start":"s"})",
        kStart + '\n' + kEnd + '\n' +
            R"({"id":"r","timestamp_us":0,"client_id":"c","op":"BatchExistKey","keys":["x"]})",
        kStart + '\n' + kEnd + '\n' +
            R"({"id":"e2","timestamp_us":2000,"client_id":"c","op":"BatchPutRevoke","keys":["x","y"],"put_start":"s"})",
    };
    for (const auto& events : invalid) {
        SCOPED_TRACE(events);
        EXPECT_THROW(Parse(events), std::invalid_argument);
    }
    std::istringstream autobench(R"({"prompt":[1,2],"timestamp":0})");
    EXPECT_THROW(ReadTrace(autobench), std::invalid_argument);
}

TEST(TraceReplay, DependenciesDoNotBlockIndependentRequests) {
    auto trace = Parse(
        kStart + '\n' + kEnd + '\n' +
        R"({"id":"read","timestamp_us":1000,"client_id":"other","op":"BatchExistKey","keys":["z"]})");
    std::promise<void> read_started;
    auto ready = read_started.get_future();
    std::atomic<bool> independent_ran{false};
    auto samples = ReplayTrace(
        trace, 2, 1, [&](const auto& event, const RpcOutcome* start) {
            if (event.id == "s") {
                independent_ran = ready.wait_for(std::chrono::seconds(2)) ==
                                  std::future_status::ready;
                return RpcOutcome{
                    {KeyStatus::OK, KeyStatus::ERROR}, true, "partial write"};
            }
            if (event.id == "read") {
                read_started.set_value();
                return RpcOutcome{{KeyStatus::MISS}, true, {}};
            }
            EXPECT_NE(start, nullptr);
            EXPECT_EQ(start->keys[1], KeyStatus::ERROR);
            return RpcOutcome{{KeyStatus::OK, KeyStatus::SKIPPED}, true, {}};
        });
    EXPECT_TRUE(independent_ran);
    EXPECT_GE(samples[1].start_us, samples[0].finish_us);
    EXPECT_GE(samples[2].start_us, samples[2].scheduled_us);
    auto summary = SummarizeTrace(trace, samples);
    EXPECT_EQ(summary["operations"]["BatchPutEnd"]["issued_keys"].asUInt64(),
              1);
    EXPECT_EQ(summary["operations"]["BatchPutEnd"]["key_status"]["skipped"]
                  .asUInt64(),
              1);
    EXPECT_EQ(
        summary["operations"]["BatchExistKey"]["key_status"]["miss"].asUInt64(),
        1);
}

TEST(TraceReplay, WorkerBacklogAppearsInLagInsteadOfChangingArrivalTimes) {
    auto trace = Parse(
        R"({"id":"a","timestamp_us":0,"client_id":"c","op":"BatchExistKey","keys":["x"]})"
        "\n"
        R"({"id":"b","timestamp_us":1000,"client_id":"c","op":"BatchExistKey","keys":["x"]})");
    auto samples = ReplayTrace(trace, 1, 1, [](const auto& event, const auto*) {
        if (event.id == "a")
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        return RpcOutcome{{KeyStatus::OK}, true, {}};
    });
    EXPECT_EQ(samples[1].scheduled_us, 1000);
    EXPECT_GE(samples[1].start_us - samples[1].scheduled_us, 10000);
    EXPECT_GE(samples[1].start_us, samples[0].finish_us);
}

TEST(TraceReplay, AllFailedWritesDoNotCountSkippedEndsAsRpcCalls) {
    auto trace = Parse(kStart + '\n' + kEnd);
    auto samples =
        ReplayTrace(trace, 2, 10, [](const auto& event, const auto*) {
            if (event.op == "BatchPutStart") {
                return RpcOutcome{
                    {KeyStatus::ERROR, KeyStatus::ERROR}, true, {}};
            }
            return RpcOutcome{
                {KeyStatus::SKIPPED, KeyStatus::SKIPPED}, false, {}};
        });
    const auto end =
        SummarizeTrace(trace, samples)["operations"]["BatchPutEnd"];
    EXPECT_EQ(end["rpc_calls"].asUInt64(), 0);
    EXPECT_TRUE(end["client_call_latency_us"].empty());
    EXPECT_EQ(samples[1].scheduled_us, 100);
}

TEST(TraceReplay, ExceptionsReleaseDependentsAndMarkInvalidResults) {
    auto trace = Parse(kStart + '\n' + kEnd);
    auto samples = ReplayTrace(
        trace, 2, 1,
        [](const auto& event, const RpcOutcome* start) -> RpcOutcome {
            if (event.id == "s")
                throw std::runtime_error("transport exception");
            EXPECT_EQ(start->keys[0], KeyStatus::ERROR);
            return {{KeyStatus::SKIPPED, KeyStatus::SKIPPED}, false, {}};
        });
    EXPECT_EQ(samples[0].outcome.error, "transport exception");
    EXPECT_EQ(samples[1].outcome.keys[0], KeyStatus::SKIPPED);
}

TEST(TraceReplay, InvalidSpeedOrWorkerCountDoesNotExecute) {
    auto trace = Parse(kStart + '\n' + kEnd);
    const TraceExecutor execute = [](const auto&, const auto*) -> RpcOutcome {
        ADD_FAILURE() << "invalid configuration must not execute";
        return {};
    };
    EXPECT_THROW(ReplayTrace(trace, 0, 1, execute), std::invalid_argument);
    EXPECT_THROW(ReplayTrace(trace, 1, 0, execute), std::invalid_argument);
    EXPECT_THROW(
        ReplayTrace(trace, 1, std::numeric_limits<double>::infinity(), execute),
        std::invalid_argument);
}

}  // namespace
}  // namespace mooncake::bench
