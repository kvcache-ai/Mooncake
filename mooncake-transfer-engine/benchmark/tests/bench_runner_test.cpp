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
#include <future>
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

    // The slot helpers are protected; expose them for the layout tests.
    static std::optional<uint64_t> slot(uint64_t base, uint64_t length,
                                        uint64_t bytes, uint64_t slot_id) {
        return slotAddress(base, length, bytes, slot_id);
    }
    static std::optional<uint64_t> targetSlot(uint64_t base, uint64_t length,
                                              uint64_t bytes,
                                              uint64_t slot_id) {
        return targetSlotAddress(base, length, bytes, slot_id);
    }
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
    // Workers 1..3 stay blocked until `release`, so they are guaranteed to be
    // busy when worker 0 reports the failure, whatever the scheduling order.
    std::promise<void> failed, release;
    std::shared_future<void> released = release.get_future().share();
    std::atomic<int> finished{0};
    auto run = std::async(std::launch::async, [&] {
        return runner.runInitiatorTasks([&](int thread_id) {
            if (thread_id == 0) {
                runner.failTransfer();
                failed.set_value();
                return -1;
            }
            released.wait();
            finished.fetch_add(1);
            return 0;
        });
    });
    failed.get_future().wait();
    // A correct pool cannot return here: three workers are still blocked. The
    // old pool returned as soon as the failure was reported.
    EXPECT_EQ(run.wait_for(std::chrono::milliseconds(500)),
              std::future_status::timeout)
        << "runInitiatorTasks() returned while workers were still running";
    EXPECT_EQ(finished.load(), 0);
    release.set_value();
    EXPECT_EQ(run.get(), -1);
    EXPECT_EQ(finished.load(), kWorkers - 1);
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

TEST(BenchRunnerTest, SlotAddressFitsExactlyAndRejectsOnePastTheEnd) {
    constexpr uint64_t kBase = 0x10000, kLength = 4096, kBytes = 1024;
    EXPECT_EQ(FakeRunner::slot(kBase, kLength, kBytes, 0), kBase);
    EXPECT_EQ(FakeRunner::slot(kBase, kLength, kBytes, 3), kBase + 3072);
    // The last slot ends exactly at base + length, which still fits.
    EXPECT_FALSE(FakeRunner::slot(kBase, kLength, kBytes, 4).has_value());
    // One byte shorter and even the last slot no longer fits.
    EXPECT_FALSE(FakeRunner::slot(kBase, kLength - 1, kBytes, 3).has_value());
    // An empty region is always valid and always starts at base.
    EXPECT_EQ(FakeRunner::slot(kBase, 0, 0, 5), kBase);
    EXPECT_EQ(FakeRunner::slot(kBase, kLength, 0, 5), kBase);
}

TEST(BenchRunnerTest, TargetSlotAddressHonorsOffsetAndRangeSize) {
    constexpr uint64_t kBase = 0x20000, kLength = 8192, kBytes = 1024;
    const size_t saved_offset = XferBenchConfig::target_offset;
    const size_t saved_range = XferBenchConfig::target_range_size;
    XferBenchConfig::target_offset = 0;
    XferBenchConfig::target_range_size = 0;
    EXPECT_EQ(FakeRunner::targetSlot(kBase, kLength, kBytes, 6), kBase + 6144);
    // 8 KB / 1 KB = 8 slots, so slot 7 ends exactly at the end of the buffer.
    EXPECT_EQ(FakeRunner::targetSlot(kBase, kLength, kBytes, 7), kBase + 7168);
    EXPECT_FALSE(FakeRunner::targetSlot(kBase, kLength, kBytes, 8).has_value());

    // --target_offset moves the window: slot 0 starts after the offset.
    XferBenchConfig::target_offset = 1024;
    EXPECT_EQ(FakeRunner::targetSlot(kBase, kLength, kBytes, 0), kBase + 1024);
    EXPECT_EQ(FakeRunner::targetSlot(kBase, kLength, kBytes, 6), kBase + 7168);
    EXPECT_FALSE(FakeRunner::targetSlot(kBase, kLength, kBytes, 7).has_value());

    // --target_range_size limits the usable window on its own.
    XferBenchConfig::target_offset = 0;
    XferBenchConfig::target_range_size = 2048;
    EXPECT_TRUE(FakeRunner::targetSlot(kBase, kLength, kBytes, 1).has_value());
    EXPECT_FALSE(FakeRunner::targetSlot(kBase, kLength, kBytes, 2).has_value());

    XferBenchConfig::target_offset = saved_offset;
    XferBenchConfig::target_range_size = saved_range;
}

TEST(BenchRunnerTest, SweepExitStatusNeedsOneSuccessfulPoint) {
    EXPECT_EQ(sweepExitStatus(false, true), EXIT_SUCCESS);
    EXPECT_EQ(sweepExitStatus(false, false), EXIT_FAILURE);
    EXPECT_EQ(sweepExitStatus(true, true), EXIT_FAILURE);
    EXPECT_EQ(sweepExitStatus(true, false), EXIT_FAILURE);
}

}  // namespace
}  // namespace tent
}  // namespace mooncake
