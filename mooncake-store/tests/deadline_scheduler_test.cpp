#include "deadline_scheduler.h"

#include <gtest/gtest.h>

#include <chrono>
#include <condition_variable>
#include <mutex>
#include <thread>
#include <vector>

#include "types.h"

namespace mooncake::test {
namespace {

using Scheduler = DeadlineScheduler<UUID>;
using Clock = Scheduler::Clock;

class CallbackRecorder {
   public:
    void Record(const UUID& id) {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            records_.push_back(id);
        }
        cv_.notify_all();
    }

    bool WaitForSize(size_t expected_size, std::chrono::milliseconds timeout) {
        std::unique_lock<std::mutex> lock(mutex_);
        return cv_.wait_for(lock, timeout, [this, expected_size]() {
            return records_.size() >= expected_size;
        });
    }

    std::vector<UUID> Records() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return records_;
    }

    size_t Size() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return records_.size();
    }

   private:
    mutable std::mutex mutex_;
    std::condition_variable cv_;
    std::vector<UUID> records_;
};

UUID MakeUuid(uint64_t value) { return UUID{value, value + 1000}; }

}  // namespace

TEST(DeadlineSchedulerTest, RunsRecordsInDeadlineOrder) {
    CallbackRecorder recorder;
    Scheduler scheduler([&recorder](const UUID& id) { recorder.Record(id); });

    const auto item1 = MakeUuid(1);
    const auto item2 = MakeUuid(2);
    const auto item3 = MakeUuid(3);
    const auto now = Clock::now();

    scheduler.Schedule(item1, now + std::chrono::milliseconds(90));
    scheduler.Schedule(item2, now + std::chrono::milliseconds(30));
    scheduler.Schedule(item3, now + std::chrono::milliseconds(60));

    ASSERT_TRUE(recorder.WaitForSize(3, std::chrono::milliseconds(500)));

    const auto records = recorder.Records();
    ASSERT_EQ(records.size(), 3);
    EXPECT_EQ(records[0], item2);
    EXPECT_EQ(records[1], item3);
    EXPECT_EQ(records[2], item1);
}

TEST(DeadlineSchedulerTest, EarlierRecordPreemptsCurrentWait) {
    CallbackRecorder recorder;
    Scheduler scheduler([&recorder](const UUID& id) { recorder.Record(id); });

    const auto long_item = MakeUuid(10);
    const auto short_item = MakeUuid(20);

    scheduler.Schedule(long_item,
                       Clock::now() + std::chrono::milliseconds(500));
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    scheduler.Schedule(short_item,
                       Clock::now() + std::chrono::milliseconds(40));

    ASSERT_TRUE(recorder.WaitForSize(1, std::chrono::milliseconds(250)));
    const auto records = recorder.Records();
    ASSERT_EQ(records.size(), 1);
    EXPECT_EQ(records[0], short_item);

    scheduler.Stop();
    EXPECT_EQ(recorder.Size(), 1);
}

TEST(DeadlineSchedulerTest, RemoveIfDropsPendingRecords) {
    CallbackRecorder recorder;
    Scheduler scheduler([&recorder](const UUID& id) { recorder.Record(id); });

    const auto removed_item1 = MakeUuid(31);
    const auto kept_item = MakeUuid(41);
    const auto removed_item2 = MakeUuid(32);
    const auto now = Clock::now();

    scheduler.Schedule(removed_item1, now + std::chrono::milliseconds(200));
    scheduler.Schedule(kept_item, now + std::chrono::milliseconds(250));
    scheduler.Schedule(removed_item2, now + std::chrono::milliseconds(300));

    scheduler.RemoveIf([&](const UUID& id) {
        return id == removed_item1 || id == removed_item2;
    });

    ASSERT_TRUE(recorder.WaitForSize(1, std::chrono::milliseconds(600)));
    std::this_thread::sleep_for(std::chrono::milliseconds(150));

    const auto records = recorder.Records();
    ASSERT_EQ(records.size(), 1);
    EXPECT_EQ(records[0], kept_item);
}

TEST(DeadlineSchedulerTest, StopCancelsPendingRecords) {
    CallbackRecorder recorder;
    Scheduler scheduler([&recorder](const UUID& id) { recorder.Record(id); });

    scheduler.Schedule(MakeUuid(50),
                       Clock::now() + std::chrono::milliseconds(200));
    scheduler.Stop();

    std::this_thread::sleep_for(std::chrono::milliseconds(250));
    EXPECT_EQ(recorder.Size(), 0);
}

TEST(DeadlineSchedulerTest, StopOnEmptySchedulerIsNoOp) {
    CallbackRecorder recorder;
    Scheduler scheduler([&recorder](const UUID& id) { recorder.Record(id); });

    scheduler.Stop();
    EXPECT_EQ(recorder.Size(), 0);
}

TEST(DeadlineSchedulerTest, RemoveIfOnEmptySchedulerIsNoOp) {
    CallbackRecorder recorder;
    Scheduler scheduler([&recorder](const UUID& id) { recorder.Record(id); });

    scheduler.RemoveIf([](const UUID&) { return true; });
    EXPECT_EQ(recorder.Size(), 0);
}

TEST(DeadlineSchedulerTest, ContinuesSchedulingAfterQueueDrains) {
    CallbackRecorder recorder;
    Scheduler scheduler([&recorder](const UUID& id) { recorder.Record(id); });

    const auto first_item = MakeUuid(60);
    scheduler.Schedule(first_item,
                       Clock::now() + std::chrono::milliseconds(30));
    ASSERT_TRUE(recorder.WaitForSize(1, std::chrono::milliseconds(500)));

    const auto second_item = MakeUuid(61);
    scheduler.Schedule(second_item,
                       Clock::now() + std::chrono::milliseconds(30));
    ASSERT_TRUE(recorder.WaitForSize(2, std::chrono::milliseconds(500)));

    const auto records = recorder.Records();
    ASSERT_EQ(records.size(), 2);
    EXPECT_EQ(records[0], first_item);
    EXPECT_EQ(records[1], second_item);
}

TEST(DeadlineSchedulerTest, RemoveAllDrainsQueueWithoutRunning) {
    CallbackRecorder recorder;
    Scheduler scheduler([&recorder](const UUID& id) { recorder.Record(id); });

    scheduler.Schedule(MakeUuid(70),
                       Clock::now() + std::chrono::milliseconds(200));
    scheduler.Schedule(MakeUuid(71),
                       Clock::now() + std::chrono::milliseconds(250));
    scheduler.RemoveIf([](const UUID&) { return true; });

    std::this_thread::sleep_for(std::chrono::milliseconds(350));
    EXPECT_EQ(recorder.Size(), 0);
}

TEST(DeadlineSchedulerTest, RunsAlreadyExpiredRecordImmediately) {
    CallbackRecorder recorder;
    Scheduler scheduler([&recorder](const UUID& id) { recorder.Record(id); });

    const auto past_item = MakeUuid(80);
    scheduler.Schedule(past_item, Clock::now() - std::chrono::milliseconds(10));

    ASSERT_TRUE(recorder.WaitForSize(1, std::chrono::milliseconds(500)));
    const auto records = recorder.Records();
    ASSERT_EQ(records.size(), 1);
    EXPECT_EQ(records[0], past_item);
}

TEST(DeadlineSchedulerTest, RunsExpiredBeforePendingRecord) {
    CallbackRecorder recorder;
    Scheduler scheduler([&recorder](const UUID& id) { recorder.Record(id); });

    const auto past_item = MakeUuid(90);
    const auto future_item = MakeUuid(91);
    const auto now = Clock::now();

    scheduler.Schedule(future_item, now + std::chrono::milliseconds(120));
    scheduler.Schedule(past_item, now - std::chrono::milliseconds(10));

    ASSERT_TRUE(recorder.WaitForSize(2, std::chrono::milliseconds(500)));
    const auto records = recorder.Records();
    ASSERT_EQ(records.size(), 2);
    EXPECT_EQ(records[0], past_item);
    EXPECT_EQ(records[1], future_item);
}

}  // namespace mooncake::test
