#include <glog/logging.h>
#include <gtest/gtest.h>

#include <chrono>
#include <cstdlib>
#include <future>
#include <latch>
#include <memory>
#include <mutex>
#include <vector>

#include <async_simple/coro/SyncAwait.h>

#include "config.h"
#include "p2p/client/v2/transfer_coordinator.h"
#include "utils.h"

namespace mooncake::v2 {
using namespace std::chrono_literals;

class TransferCoordinatorTest : public ::testing::Test {
   protected:
    static void SetUpTestSuite() {
        static std::once_flag once;
        std::call_once(once, [] {
            google::InitGoogleLogging("TransferCoordinatorTest");
            FLAGS_logtostderr = 1;
        });
    }

    void SetUp() override {
        if (std::getenv("MC_USE_TENT") || std::getenv("MC_USE_TEV1")) {
            GTEST_SKIP() << "These tests control Classic TE batch descriptors";
        }
        engine_ = std::make_shared<TransferEngine>(false);
        ASSERT_EQ(
            engine_->init("P2PHANDSHAKE",
                          "127.0.0.1:" + std::to_string(getFreeTcpPort())),
            0);
        coordinator_ = std::make_unique<TransferCoordinator>(engine_, nullptr);
    }

    void Cancel(BatchID batch) { coordinator_->CancelBatch(batch, 1); }
    void CancelCoro(BatchID batch) {
        async_simple::coro::syncAwait(coordinator_->CancelBatchCoro(batch, 1));
    }
    bool IsDrained(BatchID batch, bool uses_tent) {
        return coordinator_->IsDrained(batch, 1, uses_tent);
    }

    BatchID MakePendingBatch() {
        const auto batch = engine_->allocateBatchID(1);
        auto& task = Transport::toBatchDesc(batch).task_list.emplace_back();
        task.slice_count = 1;
        return batch;
    }

    std::shared_ptr<TransferEngine> engine_;
    std::unique_ptr<TransferCoordinator> coordinator_;
};

TEST_F(TransferCoordinatorTest, CleanupRetainsBuffersBeyondReportingTimeout) {
    const auto sync_batch = MakePendingBatch();
    const auto coro_batch = MakePendingBatch();
    auto& sync_task = Transport::toBatchDesc(sync_batch).task_list.front();
    auto& coro_task = Transport::toBatchDesc(coro_batch).task_list.front();
    auto sync_buffer = std::make_shared<std::vector<char>>(64);
    auto coro_buffer = std::make_shared<std::vector<char>>(64);
    std::weak_ptr<std::vector<char>> sync_lifetime = sync_buffer;
    std::weak_ptr<std::vector<char>> coro_lifetime = coro_buffer;
    std::latch started{2};

    auto sync_done = std::async(std::launch::async,
                                [&, owner = std::move(sync_buffer)]() mutable {
                                    started.count_down();
                                    Cancel(sync_batch);
                                    owner.reset();
                                });
    auto coro_done = std::async(std::launch::async,
                                [&, owner = std::move(coro_buffer)]() mutable {
                                    started.count_down();
                                    CancelCoro(coro_batch);
                                    owner.reset();
                                });
    started.wait();

    EXPECT_EQ(sync_done.wait_for(11s), std::future_status::timeout);
    EXPECT_EQ(coro_done.wait_for(0ms), std::future_status::timeout);
    EXPECT_FALSE(sync_lifetime.expired());
    EXPECT_FALSE(coro_lifetime.expired());

    // Finish through TE's slice counters, as a transport would. Neither batch
    // may have been freed while those counters were still outstanding.
    __atomic_fetch_add(&sync_task.success_slice_count, 1, __ATOMIC_RELAXED);
    __atomic_fetch_add(&coro_task.failed_slice_count, 1, __ATOMIC_RELAXED);
    EXPECT_EQ(sync_done.wait_for(2s), std::future_status::ready);
    EXPECT_EQ(coro_done.wait_for(2s), std::future_status::ready);
    sync_done.get();
    coro_done.get();
    EXPECT_TRUE(sync_lifetime.expired());
    EXPECT_TRUE(coro_lifetime.expired());
}

TEST_F(TransferCoordinatorTest, ClassicTimeoutDoesNotEstablishDrain) {
    const auto batch = MakePendingBatch();
    auto& task = Transport::toBatchDesc(batch).task_list.front();
    const auto old_timeout = globalConfig().slice_timeout;
    globalConfig().slice_timeout = 1;
    Transport::Slice slice{};
    slice.ts = getCurrentTimeInNano() - 2000000000LL;
    task.slice_list.push_back(&slice);

    EXPECT_FALSE(IsDrained(batch, /*uses_tent=*/false));
    EXPECT_FALSE(task.is_finished);
    // The same reported status is terminal for TENT, which resolves slices
    // before reporting TIMEOUT. It must not be treated as terminal for Classic.
    EXPECT_TRUE(IsDrained(batch, /*uses_tent=*/true));

    task.failed_slice_count = 1;
    EXPECT_TRUE(IsDrained(batch, /*uses_tent=*/false));
    task.slice_list.clear();  // The synthetic slice belongs to this stack.
    EXPECT_TRUE(engine_->freeBatchID(batch).ok());
    globalConfig().slice_timeout = old_timeout;
}

}  // namespace mooncake::v2
