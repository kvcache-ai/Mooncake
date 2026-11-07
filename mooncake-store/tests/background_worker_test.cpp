#include "background_worker.h"

#include <chrono>
#include <condition_variable>
#include <mutex>

#include <gtest/gtest.h>

namespace mooncake::test {

TEST(BackgroundWorkerTest, CoalescesPendingSchedules) {
    std::mutex mutex;
    std::condition_variable cv;
    size_t callback_count = 0;
    bool release_first_callback = false;

    BackgroundWorker worker([&] {
        std::unique_lock<std::mutex> lock(mutex);
        ++callback_count;
        cv.notify_all();
        if (callback_count == 1) {
            cv.wait(lock, [&] { return release_first_callback; });
        }
    });
    worker.Start();
    worker.Schedule();

    {
        std::unique_lock<std::mutex> lock(mutex);
        ASSERT_TRUE(cv.wait_for(lock, std::chrono::seconds(1),
                                [&] { return callback_count == 1; }));
    }
    for (size_t i = 0; i < 10; ++i) {
        worker.Schedule();
    }
    {
        std::lock_guard<std::mutex> lock(mutex);
        release_first_callback = true;
    }
    cv.notify_all();

    {
        std::unique_lock<std::mutex> lock(mutex);
        ASSERT_TRUE(cv.wait_for(lock, std::chrono::seconds(1),
                                [&] { return callback_count == 2; }));
    }
    worker.Stop();
    EXPECT_EQ(callback_count, 2);
}

TEST(BackgroundWorkerTest, ScheduleIsIgnoredWhileStopped) {
    std::mutex mutex;
    size_t callback_count = 0;
    BackgroundWorker worker([&] {
        std::lock_guard<std::mutex> lock(mutex);
        ++callback_count;
    });

    worker.Schedule();
    worker.Start();
    worker.Stop();
    worker.Schedule();

    std::lock_guard<std::mutex> lock(mutex);
    EXPECT_EQ(callback_count, 0);
}

TEST(BackgroundWorkerTest, ScheduleAfterRunsOnlyOnceTheDelayElapsed) {
    std::mutex mutex;
    std::condition_variable cv;
    size_t callback_count = 0;
    BackgroundWorker worker([&] {
        {
            std::lock_guard<std::mutex> lock(mutex);
            ++callback_count;
        }
        cv.notify_all();
    });
    worker.Start();
    worker.ScheduleAfter(std::chrono::milliseconds(200));

    {
        std::unique_lock<std::mutex> lock(mutex);
        EXPECT_FALSE(cv.wait_for(lock, std::chrono::milliseconds(50),
                                 [&] { return callback_count != 0; }));
    }
    {
        std::unique_lock<std::mutex> lock(mutex);
        ASSERT_TRUE(cv.wait_for(lock, std::chrono::seconds(2),
                                [&] { return callback_count == 1; }));
    }
    worker.Stop();
    EXPECT_EQ(callback_count, 1);
}

TEST(BackgroundWorkerTest, ImmediateScheduleWinsOverAPendingDelay) {
    std::mutex mutex;
    std::condition_variable cv;
    size_t callback_count = 0;
    BackgroundWorker worker([&] {
        {
            std::lock_guard<std::mutex> lock(mutex);
            ++callback_count;
        }
        cv.notify_all();
    });
    worker.Start();
    worker.ScheduleAfter(std::chrono::seconds(30));
    worker.Schedule();

    {
        std::unique_lock<std::mutex> lock(mutex);
        ASSERT_TRUE(cv.wait_for(lock, std::chrono::seconds(1),
                                [&] { return callback_count == 1; }));
    }
    worker.Stop();
    EXPECT_EQ(callback_count, 1);
}

}  // namespace mooncake::test
