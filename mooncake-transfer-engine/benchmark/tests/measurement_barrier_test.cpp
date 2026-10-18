// Copyright 2026 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

#include "measurement_barrier.h"

#include <atomic>
#include <chrono>
#include <future>
#include <vector>

#include <gtest/gtest.h>

namespace mooncake {
namespace tent {
namespace {

using namespace std::chrono_literals;

TEST(MeasurementBarrierTest, SnapshotExcludesWarmupAndPrecedesAllMeasurements) {
    constexpr int kThreads = 4;
    MeasurementBarrier barrier(kThreads);
    std::atomic<uint64_t> bytes{0};
    std::atomic<int> snapshots{0};
    uint64_t start_bytes = 0;
    std::promise<void> snapshot_entered;
    auto entered = snapshot_entered.get_future();
    std::promise<void> finish_snapshot;
    auto finish = finish_snapshot.get_future().share();
    std::vector<std::future<bool>> workers;
    for (int i = 0; i < kThreads; ++i) {
        workers.push_back(std::async(std::launch::async, [&] {
            bytes.fetch_add(100);  // Completed warmup traffic.
            if (!barrier.arriveAndWait([&] {
                    snapshots.fetch_add(1);
                    start_bytes = bytes.load();
                    snapshot_entered.set_value();
                    finish.wait();
                    return true;
                }))
                return false;
            const bool snapshot_visible = start_bytes == 100 * kThreads;
            bytes.fetch_add(7);  // Completed measured traffic.
            return snapshot_visible;
        }));
    }
    const auto ready = entered.wait_for(5s);
    EXPECT_EQ(ready, std::future_status::ready);
    if (ready == std::future_status::ready) {
        EXPECT_EQ(bytes.load(), 100u * kThreads);
        for (auto& worker : workers)
            EXPECT_EQ(worker.wait_for(10ms), std::future_status::timeout);
    } else {
        barrier.fail();
    }
    finish_snapshot.set_value();
    for (auto& worker : workers) EXPECT_TRUE(worker.get());
    EXPECT_EQ(snapshots.load(), 1);
    EXPECT_EQ(bytes.load() - start_bytes, 7u * kThreads);
    EXPECT_FALSE(barrier.failed());
}

TEST(MeasurementBarrierTest, SnapshotFailureReleasesAllWorkers) {
    MeasurementBarrier barrier(4);
    std::atomic<int> snapshots{0};
    std::vector<std::future<bool>> workers;
    for (int i = 0; i < 4; ++i) {
        workers.push_back(std::async(std::launch::async, [&] {
            return barrier.arriveAndWait([&] {
                snapshots.fetch_add(1);
                return false;
            });
        }));
    }
    for (auto& worker : workers) {
        EXPECT_EQ(worker.wait_for(5s), std::future_status::ready);
        EXPECT_FALSE(worker.get());
    }
    EXPECT_EQ(snapshots.load(), 1);
    EXPECT_TRUE(barrier.failed());
}

TEST(MeasurementBarrierTest, FailureDuringSnapshotCannotBeOverwritten) {
    MeasurementBarrier barrier(2);
    std::promise<void> snapshot_entered;
    auto entered = snapshot_entered.get_future();
    std::promise<void> finish_snapshot;
    auto finish = finish_snapshot.get_future();
    std::vector<std::future<bool>> workers;
    for (int i = 0; i < 2; ++i) {
        workers.push_back(std::async(std::launch::async, [&] {
            return barrier.arriveAndWait([&] {
                snapshot_entered.set_value();
                finish.wait();
                return true;
            });
        }));
    }
    EXPECT_EQ(entered.wait_for(5s), std::future_status::ready);
    barrier.fail();
    // Successful snapshot completion must preserve the concurrent failure.
    finish_snapshot.set_value();
    for (auto& worker : workers) {
        EXPECT_EQ(worker.wait_for(5s), std::future_status::ready);
        EXPECT_FALSE(worker.get());
    }
    EXPECT_TRUE(barrier.failed());
}

TEST(MeasurementBarrierTest,
     WarmupFailureReleasesWaitersAndRejectsLateArrival) {
    MeasurementBarrier barrier(3);
    std::atomic<int> snapshots{0};
    auto snapshot = [&] {
        snapshots.fetch_add(1);
        return true;
    };
    auto worker = std::async(std::launch::async,
                             [&] { return barrier.arriveAndWait(snapshot); });
    EXPECT_EQ(worker.wait_for(10ms), std::future_status::timeout);
    barrier.fail();  // Another worker's warmup failed instead of arriving.
    EXPECT_EQ(worker.wait_for(5s), std::future_status::ready);
    EXPECT_FALSE(worker.get());
    EXPECT_FALSE(barrier.arriveAndWait(snapshot));
    EXPECT_EQ(snapshots.load(), 0);
    EXPECT_TRUE(barrier.failed());
}

TEST(MeasurementBarrierTest, SingleWorkerRunsSnapshot) {
    MeasurementBarrier barrier(1);
    int snapshots = 0;
    ASSERT_TRUE(barrier.arriveAndWait([&] {
        ++snapshots;
        return true;
    }));
    EXPECT_EQ(snapshots, 1);
}

}  // namespace
}  // namespace tent
}  // namespace mooncake
