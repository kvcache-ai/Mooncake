#include <gtest/gtest.h>

#include <array>
#include <cstdint>
#include <cstdlib>
#include <deque>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include "nof/nof_runtime.h"
#include "transfer_task.h"

namespace mooncake {
namespace {

// Mock implementing the interface contract: callbacks fire synchronously
// inside PollCompletion, matching SpdkInitiator's threading contract — which
// is what makes the tests valid.
class MockInitiator : public NVMeoFInitiator {
   public:
    NofSegmentHandle* OpenSegment(const std::string& ep) override {
        auto it = handles_.find(ep);
        if (it != handles_.end()) {
            return it->second;
        }
        // The opaque handle is a forward-declared type (its full definition
        // lives only in spdk_initiator.cpp) and the interface never
        // dereferences it — use the stable address of aligned storage as a
        // unique per-endpoint sentinel pointer (map key). Never new/delete
        // NofSegmentHandle.
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

    int SubmitIO(NofSegmentHandle*, void* ptr, uint64_t byte_off,
                 uint64_t byte_len, NofIOOp op,
                 NofIOAdaptor* adaptor) override {
        if (fail_submit_) {
            return -1;
        }
        std::lock_guard<std::mutex> lock(mutex_);
        submitted_.push_back({ptr, byte_off, byte_len, op});
        // fail_ptr_ targets one task's buffer, so a multi-task operation can
        // be made to fail partially without depending on worker scheduling.
        pending_.push_back({adaptor, fail_io_ || ptr == fail_ptr_});
        return 0;
    }

    int64_t PollCompletion(NofSegmentHandle*,
                           uint32_t max_completions) override {
        int64_t n = 0;
        while (true) {
            std::pair<NofIOAdaptor*, bool> entry;
            {
                std::lock_guard<std::mutex> lock(mutex_);
                if (pending_.empty() ||
                    (max_completions != 0 && n >= max_completions)) {
                    break;
                }
                entry = pending_.front();
                pending_.pop_front();
            }
            // The callback runs outside the lock: it settles the operation
            // state, which a waiting test thread is also touching.
            const auto [adaptor, fail] = entry;
            NofIOCompletion c;
            c.success = !fail;
            c.sc = 1;
            c.sct = 0;
            if (fail) {
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

    // One sub-IO as the worker pool handed it to the initiator.
    struct Submission {
        void* ptr;
        uint64_t byte_off;
        uint64_t byte_len;
        NofIOOp op;
    };

    uint32_t block_size_ = 4096;
    bool fail_submit_ = false;
    bool fail_io_ = false;
    bool probe_ok_ = true;
    // When set, only sub-IOs for exactly this buffer address report failure.
    void* fail_ptr_ = nullptr;
    // Worker threads share this mock, so both queues are locked.
    std::mutex mutex_;
    std::vector<Submission> submitted_;
    std::deque<std::pair<NofIOAdaptor*, bool>> pending_;
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

// A multi-run operation settles only after every run reports, and any one
// failure must fail all of it -- a caller that got OK here would reuse a
// buffer whose bytes never reached the target.
TEST(NofWorkerPoolTest, OneFailedTaskFailsTheWholeOperation) {
    auto mock = std::make_shared<MockInitiator>();
    NofWorkerPool pool(mock, 0);
    auto* seg = mock->OpenSegment("trtype:TCP traddr:127.0.0.1 ns:1");
    auto state = std::make_shared<NofOperationState>();
    state->set_pending_tasks(2);
    std::vector<uint8_t> first(8192);
    std::vector<uint8_t> second(8192);
    mock->fail_ptr_ = second.data();
    pool.submitTask(
        NofTask(seg, first.data(), 0, first.size(), NofIOOp::kWrite, state));
    pool.submitTask(NofTask(seg, second.data(), first.size(), second.size(),
                            NofIOOp::kWrite, state));
    state->wait_for_completion();
    EXPECT_TRUE(state->is_completed());
    EXPECT_EQ(state->get_result(), ErrorCode::TRANSFER_FAIL);
    EXPECT_EQ(state->error_detail(), "mock_io_error");
}

// SPDK registers memory in whole 2MB pages and the RDMA memory map hands out
// one rkey per page, so a sub-IO that crosses a page boundary would be
// translated with the first page's rkey and rejected by the target.
TEST(NofWorkerPoolTest, SubIoStaysInsideOneRegisteredPage) {
    if (std::getenv("MC_NOF_SUBMIT_CHUNK_BYTES") != nullptr) {
        GTEST_SKIP() << "expected split sizes assume the default chunk size";
    }
    constexpr size_t kPage = 2ULL << 20;
    constexpr size_t kChunk = 64ULL << 10;

    // Three pages so that a page-aligned address with one chunk either side
    // of it always exists.
    std::vector<uint8_t> buffer(3 * kPage);
    const auto raw = reinterpret_cast<uintptr_t>(buffer.data());
    const uintptr_t boundary = (raw + kChunk + kPage - 1) & ~(kPage - 1);
    ASSERT_GE(boundary - kChunk, raw);
    ASSERT_LE(boundary + kChunk, raw + buffer.size());

    auto mock = std::make_shared<MockInitiator>();
    NofWorkerPool pool(mock, 0);
    auto* seg = mock->OpenSegment("trtype:TCP traddr:127.0.0.1 ns:1");
    auto state = std::make_shared<NofOperationState>();
    auto* ptr = reinterpret_cast<void*>(boundary - kChunk);
    pool.submitTask(NofTask(seg, ptr, 0, 2 * kChunk, NofIOOp::kWrite, state));
    state->wait_for_completion();
    EXPECT_EQ(state->get_result(), ErrorCode::OK);

    // The first DMA stops at the page boundary instead of taking the whole
    // 128 KiB the submit chunk would allow.
    ASSERT_EQ(mock->submitted_.size(), 2u);
    EXPECT_EQ(mock->submitted_[0].ptr, ptr);
    EXPECT_EQ(mock->submitted_[0].byte_len, kChunk);
    EXPECT_EQ(mock->submitted_[1].ptr, reinterpret_cast<void*>(boundary));
    EXPECT_EQ(mock->submitted_[1].byte_off, kChunk);
    EXPECT_EQ(mock->submitted_[1].byte_len, kChunk);

    uint64_t total = 0;
    for (const auto& io : mock->submitted_) {
        const auto addr = reinterpret_cast<uintptr_t>(io.ptr);
        EXPECT_LE((addr & (kPage - 1)) + io.byte_len, kPage);
        total += io.byte_len;
    }
    EXPECT_EQ(total, 2 * kChunk);
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
