// transfer_task_test.cpp
#include "transfer_task.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstring>
#include <future>
#include <functional>
#include <memory>
#include <thread>
#include <vector>

#include "transport/transport.h"
#include "types.h"

namespace mooncake {

// Test fixture for TransferTask tests
// TODO: Currently, this test does not cover TransferSubmitter and
// TransferEngine integration. Will add more tests in the future.
class TransferTaskTest : public ::testing::Test {
   protected:
    void SetUp() override {
        // Initialize glog for logging
        google::InitGoogleLogging("TransferTaskTest");
        FLAGS_logtostderr = 1;  // Output logs to stderr
    }

    void TearDown() override {
        // Cleanup glog
        google::ShutdownGoogleLogging();
    }
};

#ifdef USE_EVENT_DRIVEN_COMPLETION
class TransferEngineOperationStateTestPeer {
   public:
    static TransferFuture MakeFuture(
        BatchID batch_id, size_t batch_size,
        bool requires_periodic_status_polling,
        std::function<Status(BatchID, size_t, TransferStatus&)> status_query,
        std::function<Status(BatchID)> batch_releaser) {
        auto state =
            std::shared_ptr<OperationState>(new TransferEngineOperationState(
                batch_id, batch_size, requires_periodic_status_polling,
                std::move(status_query), std::move(batch_releaser)));
        return TransferFuture(std::move(state));
    }
};

namespace {

class PollDrivenBatch {
   public:
    PollDrivenBatch(size_t slice_count, bool fail_first_slice,
                    int polls_before_completion)
        : polls_before_completion_(polls_before_completion),
          batch_(new Transport::BatchDesc{}) {
        batch_->id = reinterpret_cast<BatchID>(batch_);
        batch_->batch_size = 1;
        batch_->task_list.resize(1);

        auto& task = batch_->task_list.front();
        task.batch_id = batch_->id;
        task.slice_count = slice_count;
        for (size_t index = 0; index < slice_count; ++index) {
            auto* slice = new Transport::Slice{};
            slice->length = 64;
            slice->status = Transport::Slice::POSTED;
            slice->task = &task;
            task.slice_list.push_back(slice);
        }

        if (fail_first_slice) {
            batch_->task_list.front().slice_list.front()->markFailed();
        }
    }

    ~PollDrivenBatch() {
        if (batch_ != nullptr) {
            delete batch_;
        }
    }

    BatchID id() const { return reinterpret_cast<BatchID>(batch_); }

    Status Query(BatchID batch_id, size_t task_id, TransferStatus& status) {
        if (batch_ == nullptr || batch_id != id() || task_id != 0) {
            return Status::InvalidArgument("unexpected fake batch query");
        }

        const int poll = polls_.fetch_add(1, std::memory_order_relaxed) + 1;
        if (poll >= polls_before_completion_) {
            CompletePostedSlices();
        }

        auto& task = batch_->task_list.front();
        const uint64_t success =
            __atomic_load_n(&task.success_slice_count, __ATOMIC_ACQUIRE);
        const uint64_t failed =
            __atomic_load_n(&task.failed_slice_count, __ATOMIC_ACQUIRE);
        status.transferred_bytes =
            __atomic_load_n(&task.transferred_bytes, __ATOMIC_ACQUIRE);
        if (success + failed == task.slice_count) {
            status.s = failed == 0 ? TransferStatusEnum::COMPLETED
                                   : TransferStatusEnum::FAILED;
        } else {
            status.s = TransferStatusEnum::WAITING;
        }
        return Status::OK();
    }

    Status Release(BatchID batch_id) {
        if (batch_ == nullptr || batch_id != id()) {
            return Status::InvalidArgument("unexpected fake batch release");
        }
        if (!__atomic_load_n(&batch_->task_list.front().is_finished,
                             __ATOMIC_ACQUIRE)) {
            return Status::BatchBusy("fake batch still has outstanding work");
        }
        delete batch_;
        batch_ = nullptr;
        released_.store(true, std::memory_order_release);
        return Status::OK();
    }

    void CompletePostedSlices() {
        if (completion_started_.exchange(true, std::memory_order_acq_rel)) {
            return;
        }
        for (auto* slice : batch_->task_list.front().slice_list) {
            if (slice->status == Transport::Slice::POSTED) {
                slice->markSuccess();
            }
        }
    }

    int polls() const { return polls_.load(std::memory_order_relaxed); }
    bool released() const { return released_.load(std::memory_order_acquire); }

   private:
    const int polls_before_completion_;
    Transport::BatchDesc* batch_;
    std::atomic<int> polls_{0};
    std::atomic<bool> completion_started_{false};
    std::atomic<bool> released_{false};
};

struct WaitOutcome {
    bool completed_without_rescue;
    ErrorCode result;
};

WaitOutcome WaitForFutureWithBoundedRescue(TransferFuture& future,
                                           PollDrivenBatch& batch) {
    std::promise<ErrorCode> result_promise;
    auto result_future = result_promise.get_future();
    std::thread waiter([&future, &result_promise] {
        result_promise.set_value(future.wait());
    });

    const bool completed_without_rescue =
        result_future.wait_for(std::chrono::milliseconds(500)) ==
        std::future_status::ready;
    if (!completed_without_rescue) {
        // Keep a regression from making this test wait for the production
        // 60-second deadline. Completing the final fake slice also exercises
        // the real BatchDesc CV notification path and safely releases waiter.
        batch.CompletePostedSlices();
    }

    const bool stopped = result_future.wait_for(std::chrono::seconds(1)) ==
                         std::future_status::ready;
    EXPECT_TRUE(stopped);
    waiter.join();
    return {completed_without_rescue, result_future.get()};
}

}  // namespace

class TransferEngineEventDrivenCompletionTest : public TransferTaskTest {};

TEST_F(TransferEngineEventDrivenCompletionTest,
       FuturePollsPostedSliceWithoutExternalNotification) {
    auto batch = std::make_shared<PollDrivenBatch>(1, false, 5);
    {
        auto future = TransferEngineOperationStateTestPeer::MakeFuture(
            batch->id(), 1, true,
            [batch](BatchID id, size_t task_id, TransferStatus& status) {
                return batch->Query(id, task_id, status);
            },
            [batch](BatchID id) { return batch->Release(id); });

        const auto outcome = WaitForFutureWithBoundedRescue(future, *batch);
        EXPECT_TRUE(outcome.completed_without_rescue);
        EXPECT_EQ(outcome.result, ErrorCode::OK);
        EXPECT_GE(batch->polls(), 5);
    }
    EXPECT_TRUE(batch->released());
}

TEST_F(TransferEngineEventDrivenCompletionTest,
       FutureDrainsPostedSliceAfterPartialSubmissionFailure) {
    auto batch = std::make_shared<PollDrivenBatch>(2, true, 5);
    {
        auto future = TransferEngineOperationStateTestPeer::MakeFuture(
            batch->id(), 1, true,
            [batch](BatchID id, size_t task_id, TransferStatus& status) {
                return batch->Query(id, task_id, status);
            },
            [batch](BatchID id) { return batch->Release(id); });

        const auto outcome = WaitForFutureWithBoundedRescue(future, *batch);
        EXPECT_TRUE(outcome.completed_without_rescue);
        EXPECT_EQ(outcome.result, ErrorCode::TRANSFER_FAIL);
        EXPECT_GE(batch->polls(), 5);
    }
    EXPECT_TRUE(batch->released());
}

TEST_F(TransferEngineEventDrivenCompletionTest,
       NonNvlinkBatchWaitsForNotificationWithoutHighFrequencyPolling) {
    auto batch = std::make_shared<PollDrivenBatch>(1, false, 1000000);
    std::thread completer([batch] {
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        batch->CompletePostedSlices();
    });
    {
        auto future = TransferEngineOperationStateTestPeer::MakeFuture(
            batch->id(), 1, false,
            [batch](BatchID id, size_t task_id, TransferStatus& status) {
                return batch->Query(id, task_id, status);
            },
            [batch](BatchID id) { return batch->Release(id); });

        const auto outcome = WaitForFutureWithBoundedRescue(future, *batch);
        EXPECT_TRUE(outcome.completed_without_rescue);
        EXPECT_EQ(outcome.result, ErrorCode::OK);
        EXPECT_LT(batch->polls(), 10)
            << "a CV-driven non-NVLink batch must not be scanned every 1 ms";
        completer.join();
    }
    EXPECT_TRUE(batch->released());
}
#endif

// Test basic MemcpyOperation functionality
TEST_F(TransferTaskTest, MemcpyOperationBasic) {
    const size_t data_size = 1024;
    std::vector<char> src_data(data_size, 'A');
    std::vector<char> dest_data(data_size, 'B');

    // Create memcpy operation
    MemcpyOperation op(dest_data.data(), src_data.data(), data_size);

    // Verify operation parameters
    EXPECT_EQ(op.dest, dest_data.data());
    EXPECT_EQ(op.src, src_data.data());
    EXPECT_EQ(op.size, data_size);

    // Perform memcpy manually to test
    std::memcpy(op.dest, op.src, op.size);

    // Verify data was copied correctly
    EXPECT_EQ(dest_data, src_data);
    for (size_t i = 0; i < data_size; ++i) {
        EXPECT_EQ(dest_data[i], 'A');
    }
}

// Test MemcpyOperationState functionality
TEST_F(TransferTaskTest, MemcpyOperationState) {
    auto state = std::make_shared<MemcpyOperationState>();

    // Initially not completed
    EXPECT_FALSE(state->is_completed());
    EXPECT_EQ(state->get_strategy(), TransferStrategy::LOCAL_MEMCPY);

    // Set completed with success
    state->set_completed(ErrorCode::OK);
    EXPECT_TRUE(state->is_completed());
    EXPECT_EQ(state->get_result(), ErrorCode::OK);
}

// Test MemcpyWorkerPool basic functionality
TEST_F(TransferTaskTest, MemcpyWorkerPoolBasic) {
    MemcpyWorkerPool pool;

    const size_t data_size = 512;
    std::vector<char> src_data(data_size, 'X');
    std::vector<char> dest_data(data_size, 'Y');

    auto state = std::make_shared<MemcpyOperationState>();

    // Create memcpy operations
    std::vector<MemcpyOperation> operations;
    operations.emplace_back(dest_data.data(), src_data.data(), data_size);

    // Create and submit task
    MemcpyTask task(std::move(operations), state);
    pool.submitTask(std::move(task));

    // Wait for completion
    state->wait_for_completion();

    // Verify completion and result
    EXPECT_TRUE(state->is_completed());
    EXPECT_EQ(state->get_result(), ErrorCode::OK);

    // Verify data was copied correctly
    for (size_t i = 0; i < data_size; ++i) {
        EXPECT_EQ(dest_data[i], 'X');
    }
}

// Test multiple memcpy operations in one task
TEST_F(TransferTaskTest, MemcpyWorkerPoolMultipleOperations) {
    MemcpyWorkerPool pool;

    const size_t num_ops = 3;
    const size_t data_size = 256;

    std::vector<std::vector<char>> src_buffers(num_ops);
    std::vector<std::vector<char>> dest_buffers(num_ops);

    // Initialize source buffers with different patterns
    for (size_t i = 0; i < num_ops; ++i) {
        src_buffers[i].resize(data_size, 'A' + i);
        dest_buffers[i].resize(data_size, 'Z');
    }

    auto state = std::make_shared<MemcpyOperationState>();

    // Create multiple memcpy operations
    std::vector<MemcpyOperation> operations;
    for (size_t i = 0; i < num_ops; ++i) {
        operations.emplace_back(dest_buffers[i].data(), src_buffers[i].data(),
                                data_size);
    }

    // Create and submit task
    MemcpyTask task(std::move(operations), state);
    pool.submitTask(std::move(task));

    // Wait for completion
    state->wait_for_completion();

    // Verify completion and result
    EXPECT_TRUE(state->is_completed());
    EXPECT_EQ(state->get_result(), ErrorCode::OK);

    // Verify all data was copied correctly
    for (size_t i = 0; i < num_ops; ++i) {
        for (size_t j = 0; j < data_size; ++j) {
            EXPECT_EQ(dest_buffers[i][j], 'A' + i);
        }
    }
}

// Test the locality decision used by TransferSubmitter::isLocalTransfer.
// Same-host different-process pairs share an IP but have distinct ports;
// they must NOT be treated as locally addressable, otherwise memcpy in the
// caller process would dereference a virtual address belonging to a peer
// process and segfault.
TEST_F(TransferTaskTest, IsSameProcessEndpoint) {
    // Empty inputs -> not same-process (cannot prove locality).
    EXPECT_FALSE(TransferSubmitter::isSameProcessEndpoint("", ""));
    EXPECT_FALSE(
        TransferSubmitter::isSameProcessEndpoint("", "192.168.1.10:12345"));
    EXPECT_FALSE(
        TransferSubmitter::isSameProcessEndpoint("192.168.1.10:12345", ""));

    // Identical ip:port -> same process.
    EXPECT_TRUE(TransferSubmitter::isSameProcessEndpoint("192.168.1.10:12345",
                                                         "192.168.1.10:12345"));

    // Same host, different port -> different process, NOT local.
    // This is the regression case fixed by this change.
    EXPECT_FALSE(TransferSubmitter::isSameProcessEndpoint(
        "192.168.1.10:12345", "192.168.1.10:12346"));

    // Different hosts -> not local.
    EXPECT_FALSE(TransferSubmitter::isSameProcessEndpoint(
        "192.168.1.10:12345", "192.168.1.11:12345"));

    // Hostname endpoints (non-P2P metadata mode) compare as full strings.
    EXPECT_TRUE(TransferSubmitter::isSameProcessEndpoint("host-a", "host-a"));
    EXPECT_FALSE(TransferSubmitter::isSameProcessEndpoint("host-a", "host-b"));
}

// Test TransferStrategy enum and stream operator
TEST_F(TransferTaskTest, TransferStrategyEnum) {
    // Test enum values
    EXPECT_EQ(static_cast<int>(TransferStrategy::LOCAL_MEMCPY), 0);
    EXPECT_EQ(static_cast<int>(TransferStrategy::TRANSFER_ENGINE), 1);
    EXPECT_EQ(static_cast<int>(TransferStrategy::FILE_READ), 2);
    EXPECT_EQ(static_cast<int>(TransferStrategy::EMPTY), 3);
    EXPECT_EQ(static_cast<int>(TransferStrategy::SPDK_NVMF), 4);

    // Test stream operator
    std::ostringstream oss;
    oss << TransferStrategy::LOCAL_MEMCPY;
    EXPECT_EQ(oss.str(), "LOCAL_MEMCPY");

    oss.str("");
    oss << TransferStrategy::TRANSFER_ENGINE;
    EXPECT_EQ(oss.str(), "TRANSFER_ENGINE");

    oss.str("");
    oss << TransferStrategy::SPDK_NVMF;
    EXPECT_EQ(oss.str(), "SPDK_NVMF");

    oss.str("");
    oss << TransferStrategy::FILE_READ;
    EXPECT_EQ(oss.str(), "FILE_READ");

    oss.str("");
    oss << TransferStrategy::EMPTY;
    EXPECT_EQ(oss.str(), "EMPTY");
}

}  // namespace mooncake

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
