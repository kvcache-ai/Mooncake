#include "transfer_task.h"

#include <gtest/gtest.h>

#include <array>
#include <atomic>
#include <chrono>
#include <cstring>
#include <future>
#include <thread>

#include "tent/common/config.h"
#include "tent/runtime/transfer_engine_impl.h"
#include "tent/runtime/transport.h"
#include "tent/transfer_engine.h"

namespace mooncake {

class TransferEngineImplTestPeer {
   public:
    static void attach(TransferEngine& engine,
                       std::shared_ptr<tent::TransferEngine> tent_engine,
                       std::shared_ptr<tent::Transport> transport) {
        tent_engine->impl_->swapTransportForTest(tent::RDMA, transport);
        engine.impl_tent_ = std::move(tent_engine);
        engine.use_tent_ = true;
    }
};

namespace {
using namespace std::chrono_literals;

// Cancellation deliberately leaves posted work pending. A late copy models
// the source access that must finish before a synchronous Store write returns.
class DelayedTransport : public tent::Transport {
   public:
    struct Batch : SubBatch {
        std::vector<tent::Request> requests;
        size_t size() const override { return requests.size(); }
    };
    DelayedTransport() { caps.dram_to_dram = true; }
    std::atomic<bool> complete{false};
    std::atomic<bool> poll_error{false};
    std::atomic<int> cancellations{0};
    std::atomic<int> copies{0};
    std::atomic<int> frees{0};
    bool cancel_error = false;
    bool fail_first = false;

    tent::Status allocateSubBatch(SubBatchRef& batch, size_t) override {
        batch = new Batch;
        return tent::Status::OK();
    }
    tent::Status freeSubBatch(SubBatchRef& batch) override {
        ++frees;
        delete batch;
        batch = nullptr;
        return tent::Status::OK();
    }
    tent::Status submitTransferTasks(
        SubBatchRef batch,
        const std::vector<tent::Request>& requests) override {
        auto& stored = static_cast<Batch*>(batch)->requests;
        stored.insert(stored.end(), requests.begin(), requests.end());
        return tent::Status::OK();
    }
    tent::Status getTransferStatus(SubBatchRef batch, int id,
                                   tent::TransferStatus& status) override {
        if (poll_error.exchange(false))
            return tent::Status::InternalError("injected poll failure");
        if (fail_first && id == 0) {
            status = {tent::FAILED, 0};
        } else if (!complete.load()) {
            status = {tent::PENDING, 0};
        } else {
            const auto& request = static_cast<Batch*>(batch)->requests.at(id);
            std::memcpy(reinterpret_cast<void*>(request.target_offset),
                        request.source, request.length);
            ++copies;
            status = {tent::COMPLETED, request.length};
        }
        return tent::Status::OK();
    }
    bool supportsCancellation() const override { return true; }
    tent::Status cancelTransferTask(SubBatchRef, int) override {
        ++cancellations;
        return cancel_error
                   ? tent::Status::InternalError("injected cancel failure")
                   : tent::Status::OK();
    }
    tent::Status addMemoryBuffer(tent::BufferDesc& buffer,
                                 const tent::MemoryOptions&) override {
        buffer.transports.push_back(tent::RDMA);
        return tent::Status::OK();
    }
    tent::Status removeMemoryBuffer(tent::BufferDesc&) override {
        return tent::Status::OK();
    }
};

class TentTransferLifetimeTest : public ::testing::Test {
   protected:
    void SetUp() override {
        auto config = std::make_shared<tent::Config>();
        config->set("metadata_type", "p2p");
        config->set("rpc_server_hostname", "127.0.0.1");
        config->set("rpc_server_port", "0");
        config->set("merge_requests", false);
        config->set("enable_auto_failover_on_poll", false);
        for (const auto* name :
             {"tcp", "rdma", "shm", "io_uring", "nvlink", "mnnvl", "gds", "mlu",
              "xpu", "ub", "ascend_direct", "memfabric"})
            config->set(std::string("transports/") + name + "/enable", false);
        backend_ = std::make_shared<tent::TransferEngine>(config);
        ASSERT_TRUE(backend_->available());
        transport_ = std::make_shared<DelayedTransport>();
        TransferEngineImplTestPeer::attach(engine_, backend_, transport_);
        source_.fill('x');
        ASSERT_TRUE(
            backend_->registerLocalMemory(source_.data(), source_.size()).ok());
        ASSERT_TRUE(
            backend_->registerLocalMemory(target_.data(), target_.size()).ok());
    }
    void TearDown() override {
        EXPECT_TRUE(backend_->unregisterLocalMemory(source_.data()).ok());
        EXPECT_TRUE(backend_->unregisterLocalMemory(target_.data()).ok());
    }
    tent::Request request(size_t offset = 0) {
        tent::Request request;
        request.opcode = tent::Request::WRITE;
        request.source = source_.data() + offset;
        request.target_id = LOCAL_SEGMENT_ID;
        request.target_offset =
            reinterpret_cast<uint64_t>(target_.data() + offset);
        request.length = 16;
        request.transport_hint = tent::RDMA;
        return request;
    }
    std::shared_ptr<TransferEngineOperationState> submit(
        size_t count = 1, int64_t timeout = 60000) {
        auto batch = backend_->allocateBatch(count);
        std::vector<tent::Request> requests;
        for (size_t i = 0; i < count; ++i) requests.push_back(request(16 * i));
        EXPECT_TRUE(backend_->submitTransfer(batch, requests).ok());
        return std::make_shared<TransferEngineOperationState>(engine_, batch,
                                                              count, timeout);
    }
    TransferEngine engine_{false};
    std::shared_ptr<tent::TransferEngine> backend_;
    std::shared_ptr<DelayedTransport> transport_;
    std::array<char, 32> source_{}, target_{};
};

TEST_F(TentTransferLifetimeTest,
       TimeoutDrainsAfterAcceptedOrFailedCancellation) {
    for (bool cancel_error : {false, true}) {
        transport_->complete = false;
        transport_->cancel_error = cancel_error;
        transport_->cancellations = 0;
        auto state = submit(1, 1);
        auto result = std::async(std::launch::async, [&] {
            state->wait_for_completion();
            return state->get_result();
        });
        const auto deadline = std::chrono::steady_clock::now() + 2s;
        while (!transport_->cancellations.load() &&
               std::chrono::steady_clock::now() < deadline)
            std::this_thread::yield();
        EXPECT_GT(transport_->cancellations.load(), 0);
        EXPECT_EQ(result.wait_for(20ms), std::future_status::timeout);
        EXPECT_FALSE(state->is_completed());
        transport_->complete = true;
        EXPECT_EQ(result.get(), ErrorCode::TRANSFER_FAIL);
        EXPECT_EQ(std::memcmp(source_.data(), target_.data(), 16), 0);
        state.reset();
    }
    EXPECT_EQ(transport_->frees.load(), 2);
}

TEST_F(TentTransferLifetimeTest, PollFailureDoesNotReleasePendingBuffers) {
    auto state = submit();
    transport_->poll_error = true;
    EXPECT_FALSE(state->is_completed());
    EXPECT_EQ(transport_->cancellations.load(), 1);
    auto result = std::async(std::launch::async, [&] {
        state->wait_for_completion();
        return state->get_result();
    });
    EXPECT_EQ(result.wait_for(20ms), std::future_status::timeout);
    EXPECT_EQ(transport_->frees.load(), 0);
    transport_->complete = true;
    EXPECT_EQ(result.get(), ErrorCode::TRANSFER_FAIL);
    EXPECT_EQ(std::memcmp(source_.data(), target_.data(), 16), 0);
}

TEST_F(TentTransferLifetimeTest, FailedTaskWaitsForRemainingTransfers) {
    transport_->fail_first = true;
    auto state = submit(2);
    EXPECT_FALSE(state->is_completed());
    transport_->complete = true;
    state->wait_for_completion();
    EXPECT_EQ(state->get_result(), ErrorCode::TRANSFER_FAIL);
    EXPECT_EQ(std::memcmp(source_.data() + 16, target_.data() + 16, 16), 0);
}

TEST_F(TentTransferLifetimeTest, DestroyingOperationDrainsPendingTransfers) {
    auto state = submit();
    auto destruction =
        std::async(std::launch::async,
                   [state = std::move(state)]() mutable { state.reset(); });
    EXPECT_EQ(destruction.wait_for(20ms), std::future_status::timeout);
    EXPECT_EQ(transport_->frees.load(), 0);
    transport_->complete = true;
    destruction.get();
    EXPECT_EQ(transport_->copies.load(), 1);
    EXPECT_EQ(transport_->frees.load(), 1);
}

TEST_F(TentTransferLifetimeTest, RouteProbeDoesNotSubmitTransfers) {
    EXPECT_TRUE(backend_->canTransfer(request()));
    auto invalid = request();
    invalid.target_offset += target_.size();
    EXPECT_FALSE(backend_->canTransfer(invalid));
    EXPECT_EQ(transport_->copies.load(), 0);
    EXPECT_EQ(transport_->frees.load(), 0);
}
}  // namespace
}  // namespace mooncake
