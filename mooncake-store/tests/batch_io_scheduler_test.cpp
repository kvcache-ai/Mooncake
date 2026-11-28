#include "batch_io_scheduler.h"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <future>
#include <limits>
#include <mutex>
#include <set>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#if defined(__linux__) || defined(__APPLE__)
#include <pthread.h>
#endif

namespace mooncake {

namespace {
using namespace std::chrono_literals;
using IoResult = tl::expected<void, ErrorCode>;

template <typename T>
void ExpectError(const tl::expected<T, ErrorCode>& result, ErrorCode error) {
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(static_cast<int32_t>(result.error()),
              static_cast<int32_t>(error));
}

IoResult Succeed(size_t) { return {}; }

IoResult PrepareAndWait(BatchIoScheduler& scheduler, size_t task_count,
                        uint64_t key,
                        const BatchIoScheduler::Execute& execute) {
    BatchIoScheduler::PendingBatch batch;
    auto result = scheduler.PrepareTasks(batch, task_count, key, execute);
    if (!result) return result;
    return scheduler.SubmitAndWait(batch);
}

#if defined(__linux__) || defined(__APPLE__)
TEST(BatchIoSchedulerTest, NamedWorkersExecuteConcurrently) {
    auto created = BatchIoScheduler::Create(8, {}, 300ms, "test-wio-");
    ASSERT_TRUE(created);
    std::mutex mutex;
    std::condition_variable changed;
    std::set<std::string> names;
    size_t arrived = 0;
    auto execute = [&](size_t) -> IoResult {
        char name[16]{};
        if (pthread_getname_np(pthread_self(), name, sizeof(name)) != 0) {
            return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
        }
        std::unique_lock<std::mutex> lock(mutex);
        names.emplace(name);
        ++arrived;
        changed.notify_all();
        if (!changed.wait_for(lock, 5s, [&] { return arrived == 8; })) {
            return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
        }
        return {};
    };
    BatchIoScheduler::PendingBatch batch;
    // One file per worker: eight single-task groups with distinct keys.
    for (size_t key = 0; key < 8; ++key) {
        ASSERT_TRUE(created.value()->PrepareTasks(batch, 1, key, execute));
    }
    EXPECT_TRUE(created.value()->SubmitAndWait(batch));
    EXPECT_EQ(names.size(), 8);
    for (size_t i = 0; i < 8; ++i) {
        EXPECT_EQ(names.count("test-wio-" + std::to_string(i)), 1);
    }
}
#endif

TEST(BatchIoSchedulerTest, PreparationDefersExecutionUntilSubmission) {
    auto created = BatchIoScheduler::Create(1);
    ASSERT_TRUE(created);
    auto& scheduler = *created.value();
    std::vector<size_t> indices;
    BatchIoScheduler::PendingBatch batch;
    auto execute = [&](size_t index) -> IoResult {
        indices.push_back(index);
        return {};
    };
    ASSERT_TRUE(scheduler.PrepareTasks(batch, 2, 0, execute));
    ASSERT_TRUE(scheduler.PrepareTasks(batch, 3, 0, execute));
    EXPECT_TRUE(indices.empty());
    ASSERT_TRUE(scheduler.SubmitAndWait(batch));
    EXPECT_EQ(indices, (std::vector<size_t>{0, 1, 0, 1, 2}));
    // Submission consumes the batch; repeated submission cannot rerun tasks.
    ASSERT_TRUE(scheduler.SubmitAndWait(batch));
    EXPECT_EQ(indices.size(), 5);
    {
        BatchIoScheduler::PendingBatch abandoned;
        ASSERT_TRUE(scheduler.PrepareTasks(abandoned, 1, 0, execute));
    }
    EXPECT_EQ(indices.size(), 5);
}

TEST(BatchIoSchedulerTest, AllocationFailurePreventsPublishingEntireBatch) {
    struct ThrowOnCopy {
        bool* fail;
        ThrowOnCopy(bool* flag) : fail(flag) {}
        ThrowOnCopy(const ThrowOnCopy& other) : fail(other.fail) {
            if (*fail) throw std::bad_alloc();
        }
        IoResult operator()(size_t) const { return {}; }
    };
    auto created = BatchIoScheduler::Create(1);
    ASSERT_TRUE(created);
    auto& scheduler = *created.value();
    BatchIoScheduler::PendingBatch batch;
    std::atomic<size_t> calls{0};
    ASSERT_TRUE(scheduler.PrepareTasks(batch, 1, 0, [&](size_t) -> IoResult {
        ++calls;
        return {};
    }));
    bool fail = false;
    BatchIoScheduler::Execute execute = ThrowOnCopy(&fail);
    fail = true;
    ExpectError(scheduler.PrepareTasks(batch, 256, 0, std::move(execute)),
                ErrorCode::NO_AVAILABLE_HANDLE);
    // Ignoring the preparation error must not allow later work to be added.
    ExpectError(scheduler.PrepareTasks(batch, 1, 0, Succeed),
                ErrorCode::NO_AVAILABLE_HANDLE);
    ExpectError(scheduler.SubmitAndWait(batch), ErrorCode::NO_AVAILABLE_HANDLE);
    EXPECT_EQ(calls, 0);
    // Submission clears the failed preparation state so the object is reusable.
    EXPECT_TRUE(scheduler.PrepareTasks(batch, 1, 0, Succeed));
    EXPECT_TRUE(scheduler.SubmitAndWait(batch));
}

TEST(BatchIoSchedulerTest, CreateReturnsParameterAndInitializationErrors) {
    ExpectError(BatchIoScheduler::Create(0), ErrorCode::INVALID_PARAMS);
    ExpectError(BatchIoScheduler::Create(1, {}, 0ms),
                ErrorCode::INVALID_PARAMS);
    ExpectError(BatchIoScheduler::Create(1, {}, -1ms),
                ErrorCode::INVALID_PARAMS);
    ExpectError(BatchIoScheduler::Create(std::numeric_limits<size_t>::max()),
                ErrorCode::INTERNAL_ERROR);
}

TEST(BatchIoSchedulerTest, InvalidParametersRejectBeforeExecution) {
    auto created = BatchIoScheduler::Create(1);
    ASSERT_TRUE(created.has_value());
    auto& scheduler = *created.value();
    BatchIoScheduler::PendingBatch invalid_callback;
    ExpectError(scheduler.PrepareTasks(invalid_callback, 1, 0, {}),
                ErrorCode::INVALID_PARAMS);
    ExpectError(scheduler.SubmitAndWait(invalid_callback),
                ErrorCode::INVALID_PARAMS);
    std::atomic<int> calls{0};
    auto execute = [&](size_t) -> IoResult {
        ++calls;
        return {};
    };
    BatchIoScheduler::PendingBatch batch;
    ASSERT_TRUE(scheduler.PrepareTasks(batch, 1, 0, execute));
    ExpectError(scheduler.PrepareTasks(batch, 1, 0, {}),
                ErrorCode::INVALID_PARAMS);
    ExpectError(scheduler.PrepareTasks(batch, 1, 0, execute),
                ErrorCode::INVALID_PARAMS);
    ExpectError(scheduler.SubmitAndWait(batch), ErrorCode::INVALID_PARAMS);
    EXPECT_EQ(calls, 0);
}

TEST(BatchIoSchedulerTest, EmptyBatchDoesNotExecute) {
    auto created = BatchIoScheduler::Create(4);
    ASSERT_TRUE(created.has_value());
    auto& scheduler = *created.value();
    BatchIoScheduler::PendingBatch batch;
    EXPECT_TRUE(scheduler.SubmitAndWait(batch));
    ASSERT_TRUE(scheduler.PrepareTasks(batch, 0, 0, [](size_t) -> IoResult {
        ADD_FAILURE();
        return {};
    }));
    EXPECT_TRUE(scheduler.SubmitAndWait(batch));
}

TEST(BatchIoSchedulerTest, DistinctKeysRunOnDistinctWorkers) {
    auto created = BatchIoScheduler::Create(3);
    ASSERT_TRUE(created.has_value());
    auto& scheduler = *created.value();
    std::mutex mutex;
    std::set<std::thread::id> threads;
    std::atomic<size_t> active{0};
    std::atomic<size_t> peak{0};
    std::atomic<size_t> entered{0};
    std::atomic<size_t> calls{0};
    std::promise<void> release;
    auto gate = release.get_future().share();
    std::vector<std::future<void>> batches;
    for (size_t key = 0; key < 3; ++key) {
        batches.push_back(std::async(
            std::launch::async, [this, &scheduler, &mutex, &threads, &active,
                                 &peak, &entered, &calls, gate, key] {
                BatchIoScheduler::PendingBatch batch;
                ASSERT_TRUE(scheduler.PrepareTasks(
                    batch, 16, key, [&](size_t) -> IoResult {
                        const auto count = ++active;
                        auto old = peak.load();
                        while (old < count &&
                               !peak.compare_exchange_weak(old, count)) {
                        }
                        {
                            std::lock_guard<std::mutex> lock(mutex);
                            threads.insert(std::this_thread::get_id());
                        }
                        ++entered;
                        EXPECT_EQ(gate.wait_for(5s), std::future_status::ready);
                        ++calls;
                        --active;
                        return {};
                    }));
                EXPECT_TRUE(scheduler.SubmitAndWait(batch));
            }));
    }
    const auto deadline = std::chrono::steady_clock::now() + 2s;
    while (entered < 3 && std::chrono::steady_clock::now() < deadline)
        std::this_thread::yield();
    EXPECT_EQ(entered, 3);
    release.set_value();
    for (auto& batch : batches) batch.get();
    EXPECT_EQ(calls, 16 * 3);
    EXPECT_EQ(peak, 3);
    EXPECT_EQ(threads.size(), 3);
    // The same key keeps using the same worker.
    EXPECT_TRUE(PrepareAndWait(scheduler, 1, 0, [&](size_t) -> IoResult {
        EXPECT_EQ(threads.count(std::this_thread::get_id()), 1);
        return {};
    }));
}

TEST(BatchIoSchedulerTest, IndependentPlansRunTogetherWithLocalTaskIndices) {
    auto created = BatchIoScheduler::Create(2);
    ASSERT_TRUE(created.has_value());
    auto& scheduler = *created.value();
    std::vector<size_t> first_visits(1, 0);
    std::vector<size_t> second_visits(2, 0);
    std::promise<void> first_started;
    auto first_started_future = first_started.get_future();
    std::promise<void> second_started;
    auto second_started_future = second_started.get_future();
    BatchIoScheduler::PendingBatch batch;
    ASSERT_TRUE(scheduler.PrepareTasks(
        batch, first_visits.size(), 0, [&](size_t index) -> IoResult {
            EXPECT_EQ(index, 0);
            ++first_visits.at(index);
            first_started.set_value();
            // The second plan must run while this plan is still in flight.
            EXPECT_EQ(second_started_future.wait_for(2s),
                      std::future_status::ready);
            return {};
        }));
    ASSERT_TRUE(scheduler.PrepareTasks(
        batch, second_visits.size(), 1, [&](size_t index) -> IoResult {
            EXPECT_LT(index, second_visits.size());
            ++second_visits.at(index);
            if (index == 0) {
                EXPECT_EQ(first_started_future.wait_for(2s),
                          std::future_status::ready);
                second_started.set_value();
            }
            return {};
        }));
    EXPECT_TRUE(scheduler.SubmitAndWait(batch));
    for (auto visits : first_visits) EXPECT_EQ(visits, 1);
    for (auto visits : second_visits) EXPECT_EQ(visits, 1);
}

TEST(BatchIoSchedulerTest, EachTaskIsProcessedExactlyOnce) {
    auto created = BatchIoScheduler::Create(8);
    ASSERT_TRUE(created.has_value());
    auto& scheduler = *created.value();
    for (size_t count : {1u, 5u, 160u, 161u, 513u}) {
        std::vector<size_t> visits(count, 0);
        BatchIoScheduler::PendingBatch batch;
        ASSERT_TRUE(scheduler.PrepareTasks(batch, count, count,
                                           [&](size_t index) -> IoResult {
                                               EXPECT_LT(index, visits.size());
                                               ++visits.at(index);
                                               return {};
                                           }));
        EXPECT_TRUE(scheduler.SubmitAndWait(batch));
        for (auto visits_for_entry : visits) EXPECT_EQ(visits_for_entry, 1);
    }
}

TEST(BatchIoSchedulerTest, FirstErrorCancelsQueuedWorkOnTheSameWorker) {
    auto created = BatchIoScheduler::Create(1);
    ASSERT_TRUE(created.has_value());
    auto& scheduler = *created.value();
    std::promise<void> entered;
    auto entered_future = entered.get_future().share();
    std::promise<void> release;
    auto release_future = release.get_future().share();
    std::atomic<size_t> unexpected{0};
    auto batch = std::async(std::launch::async, [&] {
        auto result =
            PrepareAndWait(scheduler, 16, 0, [&](size_t index) -> IoResult {
                if (index == 0) {
                    entered.set_value();
                    EXPECT_EQ(release_future.wait_for(5s),
                              std::future_status::ready);
                    return tl::make_unexpected(ErrorCode::FILE_OPEN_FAIL);
                }
                ++unexpected;
                return {};
            });
        // One worker runs this file's tasks in order: the first error cancels
        // every task of the file queued behind it.
        ExpectError(result, ErrorCode::FILE_OPEN_FAIL);
    });
    EXPECT_EQ(entered_future.wait_for(2s), std::future_status::ready);
    EXPECT_EQ(batch.wait_for(50ms), std::future_status::timeout);
    release.set_value();
    batch.get();
    EXPECT_EQ(unexpected, 0);
    // Errors are local to the parent batch; the shared pool remains usable.
    EXPECT_TRUE(
        PrepareAndWait(scheduler, 1, 0, [](size_t) -> IoResult { return {}; }));
}

TEST(BatchIoSchedulerTest, WorkerExceptionReturnsErrorAndPoolRemainsUsable) {
    auto created = BatchIoScheduler::Create(1);
    ASSERT_TRUE(created.has_value());
    auto& scheduler = *created.value();
    std::atomic<size_t> calls{0};
    ExpectError(PrepareAndWait(scheduler, 2, 0,
                               [&](size_t) -> IoResult {
                                   ++calls;
                                   throw std::logic_error("worker failed");
                               }),
                ErrorCode::INTERNAL_ERROR);
    EXPECT_EQ(calls, 1);
    ExpectError(PrepareAndWait(scheduler, 1, 0,
                               [&](size_t) -> IoResult {
                                   ++calls;
                                   throw 42;
                               }),
                ErrorCode::INTERNAL_ERROR);
    EXPECT_EQ(calls, 2);
    EXPECT_TRUE(PrepareAndWait(scheduler, 1, 0, [&](size_t) -> IoResult {
        ++calls;
        return {};
    }));
    EXPECT_EQ(calls, 3);
}

TEST(BatchIoSchedulerTest, ShutdownWaitsForRunningTaskAndRejectsNewWork) {
    auto created = BatchIoScheduler::Create(1);
    ASSERT_TRUE(created.has_value());
    auto& scheduler = *created.value();
    std::promise<void> entered;
    auto entered_future = entered.get_future();
    std::promise<void> release;
    auto gate = release.get_future().share();
    std::atomic<size_t> calls{0};
    auto batch = std::async(std::launch::async, [&] {
        auto result = PrepareAndWait(scheduler, 2, 0, [&](size_t) -> IoResult {
            ++calls;
            entered.set_value();
            EXPECT_EQ(gate.wait_for(5s), std::future_status::ready);
            return {};
        });
        ExpectError(result, ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    });
    EXPECT_EQ(entered_future.wait_for(2s), std::future_status::ready);
    auto shutdown =
        std::async(std::launch::async, [&] { scheduler.Shutdown(); });
    EXPECT_EQ(shutdown.wait_for(30ms), std::future_status::timeout);
    EXPECT_EQ(batch.wait_for(30ms), std::future_status::timeout);
    release.set_value();
    batch.get();
    shutdown.get();
    EXPECT_EQ(calls, 1);
    ExpectError(PrepareAndWait(scheduler, 1, 0,
                               [&](size_t) -> IoResult {
                                   ++calls;
                                   return {};
                               }),
                ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    EXPECT_EQ(calls, 1);
    scheduler.Shutdown();
}

TEST(BatchIoSchedulerTest, FailedGroupDoesNotCancelOtherGroups) {
    auto created = BatchIoScheduler::Create(1);
    ASSERT_TRUE(created);
    auto& scheduler = *created.value();
    BatchIoScheduler::PendingBatch batch;
    size_t bad_calls = 0, good_calls = 0, finished = 0;
    ASSERT_TRUE(scheduler.PrepareTasks(
        batch, 3, 0,
        [&](size_t) -> IoResult {
            ++bad_calls;
            return tl::make_unexpected(ErrorCode::FILE_WRITE_FAIL);
        },
        [&](IoResult result) -> IoResult {
            EXPECT_FALSE(result);
            ++finished;
            return result;
        }));
    ASSERT_TRUE(scheduler.PrepareTasks(
        batch, 3, 1,
        [&](size_t) -> IoResult {
            ++good_calls;
            return {};
        },
        [&](IoResult result) -> IoResult {
            EXPECT_TRUE(result);
            ++finished;
            return result;
        }));
    ExpectError(scheduler.SubmitAndWait(batch), ErrorCode::FILE_WRITE_FAIL);
    EXPECT_EQ(bad_calls, 1);
    EXPECT_EQ(good_calls, 3);
    EXPECT_EQ(finished, 2);
}

TEST(BatchIoSchedulerTest, WaitIncludesIndependentGroupFinalizers) {
    auto created = BatchIoScheduler::Create(2);
    ASSERT_TRUE(created);
    BatchIoScheduler::PendingBatch batch;
    std::promise<void> finalizer_started, release;
    auto released = release.get_future().share();
    ASSERT_TRUE(created.value()->PrepareTasks(batch, 1, 0, Succeed,
                                              [&](IoResult result) -> IoResult {
                                                  finalizer_started.set_value();
                                                  released.wait();
                                                  return result;
                                              }));
    auto submitted = std::async(std::launch::async, [&] {
        return created.value()->SubmitAndWait(batch);
    });
    const auto ready = finalizer_started.get_future().wait_for(2s);
    EXPECT_EQ(ready, std::future_status::ready);
    EXPECT_EQ(submitted.wait_for(20ms), std::future_status::timeout);
    release.set_value();
    EXPECT_TRUE(submitted.get());
}

TEST(BatchIoSchedulerTest, IndependentPlansUseLocalTaskIndices) {
    auto created = BatchIoScheduler::Create(2);
    ASSERT_TRUE(created);
    auto& scheduler = *created.value();
    BatchIoScheduler::PendingBatch batch;
    std::vector<size_t> first_file(3, 0);
    std::vector<size_t> second_file(2, 0);
    std::atomic<size_t> finished{0};
    auto prepare = [&](std::vector<size_t>& visits, size_t key) {
        return scheduler.PrepareTasks(
            batch, visits.size(), key,
            [&visits](size_t index) -> IoResult {
                EXPECT_LT(index, visits.size());
                ++visits.at(index);
                return {};
            },
            [&](IoResult result) -> IoResult {
                EXPECT_TRUE(result);
                ++finished;
                return result;
            });
    };
    ASSERT_TRUE(prepare(first_file, 0));
    ASSERT_TRUE(prepare(second_file, 1));
    EXPECT_EQ(first_file, (std::vector<size_t>{0, 0, 0}));
    EXPECT_EQ(second_file, (std::vector<size_t>{0, 0}));
    EXPECT_EQ(finished.load(), 0);
    ASSERT_TRUE(scheduler.SubmitAndWait(batch));
    EXPECT_EQ(first_file, (std::vector<size_t>{1, 1, 1}));
    EXPECT_EQ(second_file, (std::vector<size_t>{1, 1}));
    EXPECT_EQ(finished.load(), 2);
}

TEST(BatchIoSchedulerTest, EmptyTaskGroupDoesNotRunComplete) {
    auto created = BatchIoScheduler::Create(1);
    ASSERT_TRUE(created);
    BatchIoScheduler::PendingBatch batch;
    size_t finished = 0;
    ASSERT_TRUE(created.value()->PrepareTasks(batch, 0, 0, Succeed,
                                              [&](IoResult result) -> IoResult {
                                                  ++finished;
                                                  return result;
                                              }));
    EXPECT_TRUE(created.value()->SubmitAndWait(batch));
    EXPECT_EQ(finished, 0);
}

TEST(BatchIoSchedulerTest, NotReadyDefersNextTaskButAllowsRunningTaskToFinish) {
    std::atomic<bool> busy{false}, observed_busy{false};
    std::atomic<size_t> calls{0};
    std::promise<void> started, release, finished, backed_off;
    auto released = release.get_future().share();
    auto created = BatchIoScheduler::Create(
        1,
        [&] {
            if (!busy.load()) return true;
            if (!observed_busy.exchange(true)) backed_off.set_value();
            return false;
        },
        5ms);
    ASSERT_TRUE(created);
    BatchIoScheduler::PendingBatch batch;
    ASSERT_TRUE(created.value()->PrepareTasks(batch, 2, 0,
                                              [&](size_t index) -> IoResult {
                                                  ++calls;
                                                  if (index == 0) {
                                                      started.set_value();
                                                      released.wait();
                                                      finished.set_value();
                                                  }
                                                  return {};
                                              }));
    auto submitted = std::async(std::launch::async, [&] {
        return created.value()->SubmitAndWait(batch);
    });
    const auto running = started.get_future().wait_for(2s);
    busy = true;
    release.set_value();
    EXPECT_EQ(running, std::future_status::ready);
    EXPECT_EQ(finished.get_future().wait_for(2s), std::future_status::ready);
    EXPECT_EQ(backed_off.get_future().wait_for(2s), std::future_status::ready);
    EXPECT_EQ(calls.load(), 1);
    EXPECT_EQ(submitted.wait_for(20ms), std::future_status::timeout);
    busy = false;
    EXPECT_TRUE(submitted.get());
    EXPECT_EQ(calls.load(), 2);
}

TEST(BatchIoSchedulerTest, ReadinessBackoffRechecksAndShutdownDrainsGroups) {
    std::atomic<bool> busy{true};
    std::atomic<size_t> checked{0}, called{0}, finished{0};
    auto created = BatchIoScheduler::Create(
        2,
        [&] {
            ++checked;
            return !busy.load();
        },
        5ms);
    ASSERT_TRUE(created);
    BatchIoScheduler::PendingBatch batch;
    ASSERT_TRUE(created.value()->PrepareTasks(
        batch, 3, 0,
        [&](size_t) -> IoResult {
            ++called;
            return {};
        },
        [&](IoResult result) -> IoResult {
            ++finished;
            return result;
        }));
    auto submitted = std::async(std::launch::async, [&] {
        return created.value()->SubmitAndWait(batch);
    });
    const auto deadline = std::chrono::steady_clock::now() + 2s;
    while (checked.load() < 2 && std::chrono::steady_clock::now() < deadline)
        std::this_thread::yield();
    EXPECT_GE(checked.load(), 2);
    EXPECT_EQ(called.load(), 0);
    EXPECT_EQ(submitted.wait_for(20ms), std::future_status::timeout);
    busy = false;
    EXPECT_TRUE(submitted.get());
    EXPECT_EQ(called.load(), 3);
    EXPECT_EQ(finished.load(), 1);

    busy = true;
    BatchIoScheduler::PendingBatch cancelled;
    ASSERT_TRUE(created.value()->PrepareTasks(
        cancelled, 3, 1,
        [&](size_t) -> IoResult {
            ++called;
            return {};
        },
        [&](IoResult result) -> IoResult {
            ++finished;
            return result;
        }));
    auto pending = std::async(std::launch::async, [&] {
        return created.value()->SubmitAndWait(cancelled);
    });
    // Either rejection before publication or cancellation after publication is
    // valid. Neither may execute a task or strand SubmitAndWait.
    created.value()->Shutdown();
    ExpectError(pending.get(), ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    EXPECT_EQ(called.load(), 3);
}

}  // namespace
}  // namespace mooncake
