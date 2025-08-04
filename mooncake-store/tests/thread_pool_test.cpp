#include <gtest/gtest.h>
#include <glog/logging.h>
#include <atomic>
#include <chrono>
#include <mutex>
#include <condition_variable>
#include "thread_pool.h"

namespace mooncake {

class ThreadPoolTest : public ::testing::Test {
   protected:
    void SetUp() override {
        google::InitGoogleLogging("ThreadPoolTest");
        FLAGS_logtostderr = 1;
    }

    void TearDown() override { google::ShutdownGoogleLogging(); }
};

// Test basic task execution
TEST_F(ThreadPoolTest, BasicTaskExecution) {
    ThreadPool pool(2);
    std::atomic<int> counter(0);
    std::mutex mtx;
    std::condition_variable cv;
    int completed = 0;
    const int total_tasks = 10;

    for (int i = 0; i < total_tasks; ++i) {
        pool.enqueue([&]() {
            counter++;
            std::lock_guard<std::mutex> lock(mtx);
            completed++;
            cv.notify_one();
        });
    }

    std::unique_lock<std::mutex> lock(mtx);
    cv.wait(lock, [&]() { return completed == total_tasks; });

    EXPECT_EQ(counter.load(), total_tasks);
}

// Test parallel execution
TEST_F(ThreadPoolTest, ParallelExecution) {
    const size_t num_threads = 4;
    ThreadPool pool(num_threads);
    std::atomic<int> running_threads(0);
    std::atomic<int> max_concurrent_threads(0);
    std::mutex mtx;
    std::condition_variable cv;
    int completed = 0;
    const int total_tasks = 20;

    for (int i = 0; i < total_tasks; ++i) {
        pool.enqueue([&]() {
            int current = ++running_threads;
            int old_max = max_concurrent_threads.load();
            while (old_max < current &&
                   !max_concurrent_threads.compare_exchange_weak(old_max,
                                                                 current)) {
                // Keep trying to update max
            }

            std::this_thread::sleep_for(std::chrono::milliseconds(10));
            --running_threads;

            std::lock_guard<std::mutex> lock(mtx);
            completed++;
            cv.notify_one();
        });
    }

    std::unique_lock<std::mutex> lock(mtx);
    cv.wait(lock, [&]() { return completed == total_tasks; });

    EXPECT_GE(max_concurrent_threads.load(), 1);
    EXPECT_LE(max_concurrent_threads.load(), num_threads);
}

// Test proper stop
TEST_F(ThreadPoolTest, ProperStop) {
    ThreadPool pool(2);
    std::atomic<int> counter(0);
    std::mutex mtx;
    std::condition_variable cv;
    int completed = 0;
    const int total_tasks = 5;

    for (int i = 0; i < total_tasks; ++i) {
        pool.enqueue([&]() {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
            counter++;
            std::lock_guard<std::mutex> lock(mtx);
            completed++;
            cv.notify_one();
        });
    }

    {
        std::unique_lock<std::mutex> lock(mtx);
        cv.wait(lock, [&]() { return completed == total_tasks; });
    }

    pool.stop();

    EXPECT_EQ(counter.load(), total_tasks);
    EXPECT_THROW({ pool.enqueue([]() {}); }, std::runtime_error);
}

// Test stress with many tasks
TEST_F(ThreadPoolTest, StressTest) {
    const int num_tasks = 1000;
    ThreadPool pool(4);
    std::atomic<int> counter(0);
    std::mutex mtx;
    std::condition_variable cv;
    int completed = 0;

    for (int i = 0; i < num_tasks; ++i) {
        pool.enqueue([&]() {
            counter++;
            std::lock_guard<std::mutex> lock(mtx);
            completed++;
            cv.notify_one();
        });
    }

    std::unique_lock<std::mutex> lock(mtx);
    cv.wait(lock, [&]() { return completed == num_tasks; });

    EXPECT_EQ(counter.load(), num_tasks);
}

}  // namespace mooncake

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}