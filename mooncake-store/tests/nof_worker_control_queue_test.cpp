#include <glog/logging.h>
#include <gtest/gtest.h>

#include <array>
#include <atomic>
#include <chrono>
#include <cstdlib>
#include <future>
#include <memory>
#include <numeric>
#include <optional>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "transfer_task.h"

namespace mooncake {
namespace {
using namespace std::chrono_literals;
constexpr auto kTimeout = 5s;
using ThreadPair = std::pair<std::thread::id, std::thread::id>;

class NoFWorkerControlQueueTest : public ::testing::Test {
   protected:
    void SetUp() override {
        google::InitGoogleLogging("NoFWorkerControlQueueTest");
        if (const char* value = std::getenv("MC_NOF_WORKERS"))
            original_ = value;
        ASSERT_EQ(setenv("MC_NOF_WORKERS", "2", 1), 0);
    }

    void TearDown() override {
        if (original_)
            EXPECT_EQ(setenv("MC_NOF_WORKERS", original_->c_str(), 1), 0);
        else
            EXPECT_EQ(unsetenv("MC_NOF_WORKERS"), 0);
        google::ShutdownGoogleLogging();
    }

   private:
    std::optional<std::string> original_;
};

TEST_F(NoFWorkerControlQueueTest, RunsOnSelectedWorkerInFifoOrderWithoutIo) {
    constexpr int kTasks = 32;
    std::array<std::vector<int>, 2> sequence;
    std::array<std::vector<std::thread::id>, 2> threads;
    std::array<std::promise<void>, 2> done;
    SpdkNofWorkerPool pool;
    for (int index = 0; index < 2; ++index) {
        for (int task = 0; task < kTasks; ++task) {
            pool.PostControlTask(index, [&, index, task](auto&) {
                sequence[index].push_back(task);
                threads[index].push_back(std::this_thread::get_id());
                if (sequence[index].size() == kTasks) done[index].set_value();
            });
        }
    }
    for (auto& completion : done)
        ASSERT_EQ(completion.get_future().wait_for(kTimeout),
                  std::future_status::ready);

    std::vector<int> expected(kTasks);
    std::iota(expected.begin(), expected.end(), 0);
    for (int index = 0; index < 2; ++index) {
        EXPECT_EQ(sequence[index], expected);
        ASSERT_EQ(threads[index].size(), kTasks);
        EXPECT_NE(threads[index][0], std::this_thread::get_id());
        for (auto thread : threads[index]) EXPECT_EQ(thread, threads[index][0]);
    }
    EXPECT_NE(threads[0][0], threads[1][0]);
}

TEST_F(NoFWorkerControlQueueTest, CallbackCanPostToSameWorker) {
    auto done = std::make_shared<std::promise<ThreadPair>>();
    auto future = done->get_future();
    SpdkNofWorkerPool pool;
    pool.PostControlTask(0, [&pool, done](auto&) {
        auto outer = std::this_thread::get_id();
        pool.PostControlTask(0, [done, outer](auto&) {
            done->set_value({outer, std::this_thread::get_id()});
        });
    });
    ASSERT_EQ(future.wait_for(kTimeout), std::future_status::ready);
    auto [outer, inner] = future.get();
    EXPECT_EQ(outer, inner);
    EXPECT_NE(inner, std::this_thread::get_id());
}

TEST_F(NoFWorkerControlQueueTest, ShutdownDrainsQueuedControlTasks) {
    constexpr int kTasksPerWorker = 32;
    std::atomic<int> completed{0};
    std::array<std::promise<void>, 2> entered;
    auto pool = std::make_unique<SpdkNofWorkerPool>();
    std::promise<void> release;
    auto gate = release.get_future().share();
    for (int index = 0; index < 2; ++index) {
        pool->PostControlTask(index, [&, index, gate](auto&) {
            entered[index].set_value();
            gate.wait();
        });
    }
    for (auto& signal : entered)
        ASSERT_EQ(signal.get_future().wait_for(kTimeout),
                  std::future_status::ready);

    for (int index = 0; index < 2; ++index)
        for (int task = 0; task < kTasksPerWorker; ++task)
            pool->PostControlTask(index, [&](auto&) { ++completed; });
    EXPECT_EQ(completed.load(), 0);

    std::promise<void> stopping;
    auto started = stopping.get_future();
    auto stopped = std::async(std::launch::async,
                              [owned = std::move(pool), &stopping]() mutable {
                                  stopping.set_value();
                                  owned.reset();
                              });
    EXPECT_EQ(started.wait_for(kTimeout), std::future_status::ready);
    EXPECT_EQ(stopped.wait_for(20ms), std::future_status::timeout);
    release.set_value();
    ASSERT_EQ(stopped.wait_for(kTimeout), std::future_status::ready);
    stopped.get();
    EXPECT_EQ(completed.load(), 2 * kTasksPerWorker);
}
}  // namespace
}  // namespace mooncake
