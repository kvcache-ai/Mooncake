// Copyright 2026 KVCache.AI
#include <gtest/gtest.h>

#include <array>
#include <atomic>
#include <chrono>
#include <thread>

#include "tent/transport/tcp/high_performance_tcp_buffer_registry.h"
#include "tent/transport/tcp/high_performance_tcp_task.h"
#include "tent/transport/tcp/high_performance_tcp_workers.h"

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

    Request request{};
    request.length = memory.size();
    auto task = std::make_shared<HighPerformanceTcpTaskState>(
        request, 1, [](BatchID) {}, std::move(lease));
    task->activateReservation(&admission, memory.size());
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
