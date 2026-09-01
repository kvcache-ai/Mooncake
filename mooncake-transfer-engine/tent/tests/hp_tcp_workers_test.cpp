// Copyright 2026 KVCache.AI
#include <gtest/gtest.h>

#include <array>
#include <atomic>
#include <chrono>
#include <future>
#include <stdexcept>
#include <thread>

#include "tent/transport/hp_tcp/hp_tcp_buffer_registry.h"
#include "tent/transport/hp_tcp/hp_tcp_task.h"
#include "tent/transport/hp_tcp/hp_tcp_workers.h"

namespace mooncake::tent {
namespace {
using namespace std::chrono_literals;

template <class Predicate>
bool WaitUntil(Predicate predicate) {
    const auto deadline = std::chrono::steady_clock::now() + 2s;
    while (!predicate() && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(1ms);
    }
    return predicate();
}

Status SubmitToWorker(HighPerformanceTcpWorkers& workers, size_t owner,
                      HighPerformanceTcpWorkers::Task task) {
    std::vector<HighPerformanceTcpWorkers::Command> commands{
        {.worker_id = owner, .run = std::move(task), .cancel = {}}};
    return workers.tryCommitBatch(commands, nullptr, 0, 0, [] {});
}

TEST(HighPerformanceTcpWorkersTest, PreservesAffinity) {
    HighPerformanceTcpWorkers workers({.worker_count = 2});
    ASSERT_TRUE(workers.start().ok());
    std::atomic<size_t> completed{0};
    for (size_t owner : {0u, 1u, 0u, 1u}) {
        ASSERT_TRUE(SubmitToWorker(workers, owner, [&, owner](size_t actual) {
                        EXPECT_EQ(actual, owner);
                        ++completed;
                    }).ok());
    }
    EXPECT_TRUE(WaitUntil([&] { return completed.load() == 4; }));
    EXPECT_TRUE(workers.stop().ok());
}

TEST(HighPerformanceTcpWorkersTest, BatchAdmissionHasNoPartialCommit) {
    HighPerformanceTcpWorkers workers({.worker_count = 1});
    ASSERT_TRUE(workers.start().ok());
    HighPerformanceTcpAdmissionController admission(1, 512);
    ASSERT_TRUE(admission.tryReserve(1, 512).ok());
    bool committed = false;
    std::vector<HighPerformanceTcpWorkers::Command> commands{
        {.worker_id = 0, .run = [](size_t) {}, .cancel = [] {}}};
    EXPECT_TRUE(workers
                    .tryCommitBatch(commands, &admission, 1, 512,
                                    [&] { committed = true; })
                    .IsTooManyRequests());
    EXPECT_FALSE(committed);
    EXPECT_EQ(admission.outstandingTasks(), 1u);
    admission.release(1, 512);
    EXPECT_TRUE(workers.stop().ok());
}

TEST(HighPerformanceTcpAdmissionControllerTest,
     UnderflowFailsClosedWithoutForgingDrain) {
    HighPerformanceTcpAdmissionController admission(2, 1024);
    ASSERT_TRUE(admission.tryReserve(1, 512).ok());

    std::promise<bool> result;
    auto result_future = result.get_future();
    std::thread waiter(
        [&] { result.set_value(admission.waitForZero().IsInternalError()); });

    admission.release(2, 512);

    EXPECT_TRUE(admission.failed());
    EXPECT_EQ(admission.outstandingTasks(), 1u);
    EXPECT_EQ(admission.outstandingBytes(), 512u);
    const auto wait_status = result_future.wait_for(std::chrono::seconds(1));
    if (wait_status != std::future_status::ready) {
        // Ensure a broken implementation cannot strand the test thread.
        admission.release(1, 512);
    }
    EXPECT_EQ(wait_status, std::future_status::ready);
    EXPECT_TRUE(result_future.get());
    waiter.join();
    EXPECT_TRUE(admission.tryReserve(1, 1).IsInternalError());
}

TEST(HighPerformanceTcpWorkersTest,
     FailedWorkerRejectsAdmissionButKeepsTeardownLive) {
    HighPerformanceTcpWorkers workers({.worker_count = 1});
    ASSERT_TRUE(workers.start().ok());

    std::atomic<bool> owner_loop_continued{false};

    asio::post(workers.ioContext(0),
               [] { throw std::runtime_error("test worker failure"); });
    asio::post(workers.ioContext(0), [&] { owner_loop_continued.store(true); });

    ASSERT_TRUE(WaitUntil([&] { return workers.hasFailedWorker(); }));
    EXPECT_TRUE(workers.barrier().ok());
    EXPECT_TRUE(owner_loop_continued.load());
    EXPECT_TRUE(SubmitToWorker(workers, 0, [](size_t) {}).IsInternalError());
    EXPECT_TRUE(workers.stop().IsInternalError());
}

TEST(HighPerformanceTcpTaskTest, CompletionReleasesLeaseAndBudgetOnce) {
    HighPerformanceTcpBufferRegistry registry;
    std::array<uint8_t, 64> memory{};
    uint64_t registration = 0;
    ASSERT_TRUE(registry
                    .add(reinterpret_cast<uint64_t>(memory.data()),
                         memory.size(), kGlobalReadWrite, &registration)
                    .ok());
    HighPerformanceTcpBufferRegistry::Lease lease;
    ASSERT_TRUE(
        registry
            .acquireLocalLease(reinterpret_cast<uint64_t>(memory.data()),
                               memory.size(), &lease)
            .ok());
    HighPerformanceTcpAdmissionController admission(1, memory.size());
    ASSERT_TRUE(admission.tryReserve(1, memory.size()).ok());

    auto task = std::make_shared<HighPerformanceTcpTaskState>(
        memory.size(), 1, [](BatchID) {}, std::move(lease));
    task->activateReservation(&admission);
    EXPECT_TRUE(task->completeOnce(COMPLETED, memory.size()));
    EXPECT_FALSE(task->completeOnce(FAILED, 0));
    EXPECT_EQ(admission.outstandingTasks(), 0u);
    EXPECT_TRUE(
        registry
            .remove(reinterpret_cast<uint64_t>(memory.data()), memory.size())
            .ok());
}

TEST(HighPerformanceTcpWorkersTest, StopIsIdempotentAndNotRestartable) {
    HighPerformanceTcpWorkers workers({.worker_count = 1});
    ASSERT_TRUE(workers.start().ok());
    EXPECT_TRUE(workers.stop().ok());
    EXPECT_TRUE(workers.stop().ok());
    EXPECT_TRUE(workers.start().IsInvalidArgument());
    EXPECT_TRUE(SubmitToWorker(workers, 0, [](size_t) {}).IsInternalError());
}

}  // namespace
}  // namespace mooncake::tent
