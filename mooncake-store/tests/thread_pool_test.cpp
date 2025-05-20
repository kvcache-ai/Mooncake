#include <gtest/gtest.h>
#include <atomic>
#include <chrono>
#include <thread>

#include "thread_pool.h"

using namespace mooncake;

class ThreadPoolTest : public ::testing::Test {
   protected:
    void SetUp() override {
    // Initialize thread pool with desired number of threads
    thread_pool_ = std::make_shared<ThreadPool>(5); // Example with 5 threads
    }

    void TearDown() override {
    // Clean up thread pool
    thread_pool_->exit();
    }

    std::shared_ptr<ThreadPool> thread_pool_;
};

TEST_F(ThreadPoolTest, TaskWithReturnValue) {
    std::vector<std::future<int>> results;

    std::string file_name1 = "./test_file1";
    std::string file_name2 = "./test_file2";
    auto make_task = [&](const std::string& file_name) {
        return [file_name]() -> int {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            return 42;
        };
    };

    // Submit task to thread pool
    results.emplace_back(thread_pool_->enqueue(make_task(file_name1)));
    results.emplace_back(thread_pool_->enqueue(make_task(file_name2)));

    // Wait for tasks to complete and check results
    for (size_t i = 0; i < results.size(); i++) {
        int result = results[i].get();
        EXPECT_EQ(result, 42);
    }
}
