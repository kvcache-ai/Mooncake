#include <gtest/gtest.h>

#include <array>
#include <cstdint>
#include <deque>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "nof/nof_runtime.h"
#include "transfer_task.h"

namespace mooncake {
namespace {

// 按接口契约实现的 Mock:回调在 PollCompletion 内同步触发,与 SpdkInitiator
// 的线程契约一致 —— 测试因此是有效的。
class MockInitiator : public NVMeoFInitiator {
   public:
    NofSegmentHandle* OpenSegment(const std::string& ep) override {
        auto it = handles_.find(ep);
        if (it != handles_.end()) {
            return it->second;
        }
        // Opaque handle 是前向声明类型(完整定义只在 spdk_initiator.cpp 内),
        // 接口从不解引用它 —— 用 aligned 存储的稳定地址作为每 endpoint 唯一
        // 的 sentinel 指针(key 用)。绝不 new/delete NofSegmentHandle。
        NofSegmentHandle* h = reinterpret_cast<NofSegmentHandle*>(
            &sentinels_[handles_.size() % sentinels_.size()]);
        handles_[ep] = h;
        return h;
    }

    bool ProbeSegment(const std::string&, uint32_t, std::string*) override {
        return probe_ok_;
    }

    uint32_t GetBlockSize(const NofSegmentHandle*) override {
        return block_size_;
    }

    int SubmitIO(NofSegmentHandle*, void*, uint64_t, uint64_t, NofIOOp,
                 NofIOAdaptor* adaptor) override {
        if (fail_submit_) {
            return -1;
        }
        pending_.push_back(adaptor);
        return 0;
    }

    int64_t PollCompletion(NofSegmentHandle*,
                           uint32_t max_completions) override {
        int64_t n = 0;
        while (!pending_.empty() &&
               (max_completions == 0 || n < max_completions)) {
            auto* adaptor = pending_.front();
            pending_.pop_front();
            NofIOCompletion c;
            c.success = !fail_io_;
            c.sc = 1;
            c.sct = 0;
            if (fail_io_) {
                c.error_string = "mock_io_error";
            }
            adaptor->cb(adaptor->ctx, c);
            ++n;
        }
        return n;
    }

    ErrorCode RegisterMemory(void*, size_t) override { return ErrorCode::OK; }
    ErrorCode UnregisterMemory(void*) override { return ErrorCode::OK; }
    NofCapabilities GetCapabilities() const override { return {}; }

    uint32_t block_size_ = 4096;
    bool fail_submit_ = false;
    bool fail_io_ = false;
    bool probe_ok_ = true;
    std::deque<NofIOAdaptor*> pending_;
    std::map<std::string, NofSegmentHandle*> handles_;
    std::array<uint64_t, 16> sentinels_{};
};

TEST(NofWorkerPoolTest, SubmitAndComplete) {
    auto mock = std::make_shared<MockInitiator>();
    NofWorkerPool pool(mock, 0);
    auto* seg = mock->OpenSegment("trtype:TCP traddr:127.0.0.1 ns:1");
    auto state = std::make_shared<NofOperationState>();
    std::vector<uint8_t> buf(8192);
    NofTask task(seg, buf.data(), /*byte_off=*/0, /*byte_len=*/8192,
                 NofIOOp::kWrite, state);
    pool.submitTask(std::move(task));
    state->wait_for_completion();
    EXPECT_TRUE(state->is_completed());
    EXPECT_EQ(state->get_result(), ErrorCode::OK);
}

TEST(NofWorkerPoolTest, FailurePropagatesErrorDetail) {
    auto mock = std::make_shared<MockInitiator>();
    mock->fail_io_ = true;
    NofWorkerPool pool(mock, 0);
    auto* seg = mock->OpenSegment("trtype:TCP traddr:127.0.0.1 ns:1");
    auto state = std::make_shared<NofOperationState>();
    std::vector<uint8_t> buf(8192);
    NofTask task(seg, buf.data(), 0, 8192, NofIOOp::kWrite, state);
    pool.submitTask(std::move(task));
    state->wait_for_completion();
    EXPECT_TRUE(state->is_completed());
    EXPECT_EQ(state->get_result(), ErrorCode::TRANSFER_FAIL);
    EXPECT_EQ(state->error_detail(), "mock_io_error");
}

TEST(NofWorkerPoolTest, SubmitFailureFailsTask) {
    auto mock = std::make_shared<MockInitiator>();
    mock->fail_submit_ = true;
    NofWorkerPool pool(mock, 0);
    auto* seg = mock->OpenSegment("trtype:TCP traddr:127.0.0.1 ns:1");
    auto state = std::make_shared<NofOperationState>();
    std::vector<uint8_t> buf(8192);
    NofTask task(seg, buf.data(), 0, 8192, NofIOOp::kWrite, state);
    pool.submitTask(std::move(task));
    state->wait_for_completion();
    EXPECT_TRUE(state->is_completed());
    EXPECT_EQ(state->get_result(), ErrorCode::TRANSFER_FAIL);
}

TEST(NofRuntimeTest, NonNofBuildReturnsNullInitiator) {
#ifndef USE_NOF
    auto rt = CreateNofRuntime();
    EXPECT_EQ(rt.initiator, nullptr);
    EXPECT_NE(rt.dma_allocator, nullptr);
#endif
}

}  // namespace
}  // namespace mooncake
