// Copyright 2026 KVCache.AI
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

#include "bench_runner.h"

#include <atomic>
#include <chrono>
#include <thread>

#include <gtest/gtest.h>

namespace mooncake {
namespace tent {
namespace {

// Exercises only the worker pool; no transfers or buffers.
class FakeRunner : public BenchRunner {
   public:
    void pinThread(int) override {}
    int runTarget() override { return 0; }
    int startInitiator(int num_threads) override {
        startWorkers(num_threads);
        return 0;
    }
    std::string getSegmentName() const override { return "fake"; }
    std::optional<uint64_t> getLocalBufferBase(int, uint64_t) const override {
        return 0;
    }
    std::optional<uint64_t> getTargetBufferBase(int, uint64_t) const override {
        return 0;
    }
    double runSingleTransfer(uint64_t, uint64_t, uint64_t, uint64_t, uint64_t,
                             OpCode, uint64_t, IntentType) override {
        return running() ? 1.0 : -1.0;
    }

    void failTransfer() { noteTransferFailed(); }
    bool isRunning() const { return running(); }
};

TEST(BenchRunnerTest, RunsEveryTaskAndReusesThePool) {
    FakeRunner runner;
    ASSERT_EQ(runner.startInitiator(4), 0);
    std::atomic<int> calls{0};
    auto task = [&](int) {
        calls.fetch_add(1);
        return 0;
    };
    EXPECT_EQ(runner.runInitiatorTasks(task), 0);
    EXPECT_EQ(calls.load(), 4);
    EXPECT_EQ(runner.runInitiatorTasks(task), 0);
    EXPECT_EQ(calls.load(), 8);
    runner.stopInitiator();
}

TEST(BenchRunnerTest, PassesEachWorkerItsOwnThreadId) {
    FakeRunner runner;
    ASSERT_EQ(runner.startInitiator(8), 0);
    std::atomic<uint32_t> seen{0};
    EXPECT_EQ(runner.runInitiatorTasks([&](int thread_id) {
        seen.fetch_or(1u << thread_id);
        return 0;
    }),
              0);
    EXPECT_EQ(seen.load(), 0xffu);
    runner.stopInitiator();
}

// Regression test for the stack use-after-return: after one worker fails,
// runInitiatorTasks() must still wait for the others, because the task may
// capture the caller's stack (as processBatchSizes() does).
TEST(BenchRunnerTest, WaitsForEveryWorkerAfterAFailure) {
    FakeRunner runner;
    constexpr int kWorkers = 4;
    ASSERT_EQ(runner.startInitiator(kWorkers), 0);
    std::atomic<int> finished_after_failure{0};
    const int rc = runner.runInitiatorTasks([&](int thread_id) {
        if (thread_id == 0) {
            runner.failTransfer();
            return -1;
        }
        // Still busy when the failure is reported.
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
        finished_after_failure.fetch_add(1);
        return 0;
    });
    EXPECT_EQ(rc, -1);
    EXPECT_EQ(finished_after_failure.load(), kWorkers - 1);
    EXPECT_FALSE(runner.isRunning());
    runner.stopInitiator();
}

TEST(BenchRunnerTest, NonZeroTaskResultFailsTheRun) {
    FakeRunner runner;
    ASSERT_EQ(runner.startInitiator(3), 0);
    EXPECT_EQ(runner.runInitiatorTasks(
                  [](int thread_id) { return thread_id == 2 ? 1 : 0; }),
              -1);
    // Only noteTransferFailed() aborts the run; the next run is unaffected.
    EXPECT_TRUE(runner.isRunning());
    EXPECT_EQ(runner.runInitiatorTasks([](int) { return 0; }), 0);
    runner.stopInitiator();
}

TEST(BenchRunnerTest, TransfersStopAfterAFailure) {
    FakeRunner runner;
    ASSERT_EQ(runner.startInitiator(2), 0);
    std::atomic<int> rejected{0};
    const int rc = runner.runInitiatorTasks([&](int thread_id) {
        if (thread_id == 0) runner.failTransfer();
        // Mimics a benchmark loop: keep issuing until a transfer fails.
        auto transfer = [&] {
            return runner.runSingleTransfer(0, 0, 0, 0, 0, READ, 0, {});
        };
        while (transfer() >= 0) {
            std::this_thread::yield();
        }
        rejected.fetch_add(1);
        return -1;
    });
    EXPECT_EQ(rc, -1);
    EXPECT_EQ(rejected.load(), 2);
    runner.stopInitiator();
}

TEST(BenchRunnerTest, RestartsWithADifferentThreadCount) {
    FakeRunner runner;
    std::atomic<int> calls{0};
    auto task = [&](int) {
        calls.fetch_add(1);
        return 0;
    };
    ASSERT_EQ(runner.startInitiator(2), 0);
    EXPECT_EQ(runner.runInitiatorTasks(task), 0);
    runner.stopInitiator();
    ASSERT_EQ(runner.startInitiator(5), 0);
    EXPECT_EQ(runner.runInitiatorTasks(task), 0);
    runner.stopInitiator();
    EXPECT_EQ(calls.load(), 7);
}

}  // namespace
}  // namespace tent
}  // namespace mooncake
