#include "parallel_execute.h"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <stdexcept>
#include <thread>
#include <vector>

namespace mooncake {
namespace {

constexpr size_t kPoolWorkers = 4;

TEST(ParallelExecuteTest, EmptyInputReturnsNoResults) {
    ThreadPool pool(kPoolWorkers);
    std::vector<int> items;

    const auto results =
        ParallelExecute(items, [](const int&) { return ErrorCode::OK; }, pool);

    EXPECT_TRUE(results.empty());
}

TEST(ParallelExecuteTest, ResultsFollowInputOrder) {
    ThreadPool pool(kPoolWorkers);
    // Four distinct codes: a palindromic expectation would still hold if the
    // results came back reversed, so it would not pin the ordering contract.
    const std::vector<ErrorCode> items{
        ErrorCode::INVALID_KEY, ErrorCode::OBJECT_NOT_FOUND,
        ErrorCode::RPC_FAIL, ErrorCode::TRANSFER_FAIL};

    const auto results = ParallelExecute(
        items, [](const ErrorCode& item) { return item; }, pool);

    EXPECT_EQ(results, items);
}

TEST(ParallelExecuteTest, EveryItemIsInFlightAtOnce) {
    ThreadPool pool(kPoolWorkers);
    const std::vector<int> items{0, 1, 2, 3};

    std::mutex mutex;
    std::condition_variable cv;
    size_t arrived = 0;
    std::atomic<bool> all_in_flight{false};

    const auto results = ParallelExecute(
        items,
        [&](const int&) {
            std::unique_lock<std::mutex> lock(mutex);
            ++arrived;
            cv.notify_all();
            if (cv.wait_for(lock, std::chrono::seconds(5),
                            [&] { return arrived == items.size(); })) {
                all_in_flight.store(true);
            }
            return ErrorCode::OK;
        },
        pool);

    EXPECT_TRUE(all_in_flight.load());
    EXPECT_EQ(results.size(), items.size());
}

TEST(ParallelExecuteTest, CallerThreadRunsTheLastItem) {
    ThreadPool pool(kPoolWorkers);
    const std::vector<int> items{0, 1, 2};
    std::vector<std::thread::id> ran_on(items.size());

    ParallelExecute(
        items,
        [&](const int& item) {
            ran_on[item] = std::this_thread::get_id();
            return ErrorCode::OK;
        },
        pool);

    EXPECT_EQ(ran_on.back(), std::this_thread::get_id());
}

TEST(ParallelExecuteTest, SingleItemNeverReachesThePool) {
    ThreadPool pool(kPoolWorkers);
    const std::vector<int> items{0};
    std::thread::id ran_on;

    ParallelExecute(
        items,
        [&](const int&) {
            ran_on = std::this_thread::get_id();
            return ErrorCode::OK;
        },
        pool);

    EXPECT_EQ(ran_on, std::this_thread::get_id());
}

TEST(ParallelExecuteTest, ThrowingTaskBecomesInternalErrorAndSparesTheRest) {
    ThreadPool pool(kPoolWorkers);
    const std::vector<int> items{0, 1, 2, 3};

    const auto results = ParallelExecute(
        items,
        [](const int& item) {
            if (item == 1) {
                throw std::runtime_error("boom");
            }
            if (item == 2) {
                throw 42;
            }
            return ErrorCode::OK;
        },
        pool);

    ASSERT_EQ(results.size(), items.size());
    EXPECT_EQ(results[0], ErrorCode::OK);
    EXPECT_EQ(results[1], ErrorCode::INTERNAL_ERROR);
    EXPECT_EQ(results[2], ErrorCode::INTERNAL_ERROR);
    EXPECT_EQ(results[3], ErrorCode::OK);
}

TEST(ParallelExecuteTest, ExpectedResultsCarryTheErrorAsUnexpected) {
    ThreadPool pool(kPoolWorkers);
    const std::vector<int> items{0, 1};

    const auto results = ParallelExecute(
        items,
        [](const int& item) -> tl::expected<void, ErrorCode> {
            if (item == 0) {
                throw std::runtime_error("boom");
            }
            return {};
        },
        pool);

    ASSERT_EQ(results.size(), items.size());
    ASSERT_FALSE(results[0]);
    EXPECT_EQ(results[0].error(), ErrorCode::INTERNAL_ERROR);
    EXPECT_TRUE(results[1]);
}

TEST(ParallelExecuteTest, StoppedPoolStillRunsEveryItem) {
    ThreadPool pool(kPoolWorkers);
    pool.stop();
    const std::vector<int> items{0, 1, 2, 3};
    std::atomic<size_t> ran{0};

    const auto results = ParallelExecute(
        items,
        [&](const int&) {
            ran.fetch_add(1);
            return ErrorCode::OK;
        },
        pool);

    EXPECT_EQ(ran.load(), items.size());
    EXPECT_EQ(results, std::vector<ErrorCode>(items.size(), ErrorCode::OK));
}

}  // namespace
}  // namespace mooncake
