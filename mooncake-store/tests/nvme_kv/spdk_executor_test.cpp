#include "nvme_kv/executor_spdk_internal.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <deque>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace mooncake::test {
namespace {

class ScopedEnv {
   public:
    ScopedEnv(const char* name, const char* value) : name_(name) {
        if (const char* original = std::getenv(name)) {
            original_ = original;
        }
        setenv(name, value, 1);
    }

    ~ScopedEnv() {
        if (original_) {
            setenv(name_.c_str(), original_->c_str(), 1);
        } else {
            unsetenv(name_.c_str());
        }
    }

   private:
    std::string name_;
    std::optional<std::string> original_;
};

class FakeKvInitiator : public NVMeoFInitiator {
   public:
    struct Pending {
        NofKvAdaptor* adaptor;
        NofKvCompletion completion;
    };

    NofSegmentHandle* OpenSegment(const std::string&) override {
        return Segment();
    }
    NofSegmentHandle* OpenKvSegment(const std::string&) override {
        return Segment();
    }
    bool ProbeSegment(const std::string&, uint32_t, std::string*) override {
        return true;
    }
    uint32_t GetBlockSize(const NofSegmentHandle*) override { return 4096; }
    int SubmitIO(NofSegmentHandle*, void*, uint64_t, uint64_t, NofIOOp,
                 NofIOAdaptor*) override {
        return -EOPNOTSUPP;
    }
    int64_t PollCompletion(NofSegmentHandle*, uint32_t) override {
        ++poll_count;
        if (poll_error != 0) {
            const int error = poll_error;
            poll_error = 0;
            return error;
        }
        if (defer_completions) {
            return 0;
        }
        if (complete_in_reverse) {
            std::reverse(pending.begin(), pending.end());
        }
        const int64_t completed = static_cast<int64_t>(pending.size());
        while (!pending.empty()) {
            Complete(pending.front());
            pending.pop_front();
        }
        return completed;
    }
    ErrorCode RegisterMemory(void*, size_t) override { return ErrorCode::OK; }
    ErrorCode UnregisterMemory(void*) override { return ErrorCode::OK; }
    NofCapabilities GetCapabilities() const override { return {}; }

    void CloseKvSegment(NofSegmentHandle*) override { ++close_count; }
    void AbortKvSegment(NofSegmentHandle*) override {
        ++abort_count;
        while (!pending.empty()) {
            Pending aborted = pending.front();
            pending.pop_front();
            aborted.completion.status_code_type = 1;
            Complete(aborted);
        }
    }
    bool IsKvNamespace(const NofSegmentHandle*) const override { return true; }

    int SubmitKvStore(NofSegmentHandle*, const void*, uint8_t, const void*,
                      uint32_t, bool, NofKvAdaptor* adaptor) override {
        return Queue(adaptor);
    }
    int SubmitKvRetrieve(NofSegmentHandle*, const void*, uint8_t, void* value,
                         uint32_t value_len, NofKvAdaptor* adaptor) override {
        if (retrieve_value.size() > value_len) {
            return -EINVAL;
        }
        std::copy(retrieve_value.begin(), retrieve_value.end(),
                  static_cast<char*>(value));
        return Queue(adaptor);
    }
    int SubmitKvDelete(NofSegmentHandle*, const void*, uint8_t,
                       NofKvAdaptor* adaptor) override {
        return Queue(adaptor);
    }

    NofSegmentHandle* Segment() {
        return reinterpret_cast<NofSegmentHandle*>(&segment_storage_);
    }

    int Queue(NofKvAdaptor* adaptor) {
        const size_t call = submit_count++;
        if (reject_submit_call == call) {
            return reject_submit_error;
        }
        NofKvCompletion completion;
        if (call < completions.size()) {
            completion = completions[call];
        }
        pending.push_back(Pending{adaptor, completion});
        max_inflight = std::max(max_inflight, pending.size());
        return 0;
    }

    void Complete(const Pending& item) {
        item.adaptor->cb(item.adaptor->ctx, item.completion);
    }

    std::array<std::max_align_t, 1> segment_storage_{};
    std::deque<Pending> pending;
    std::vector<NofKvCompletion> completions;
    std::optional<size_t> reject_submit_call;
    int reject_submit_error = -ENOMEM;
    int poll_error = 0;
    bool complete_in_reverse = false;
    bool defer_completions = false;
    std::string retrieve_value;
    size_t submit_count = 0;
    size_t max_inflight = 0;
    size_t poll_count = 0;
    size_t abort_count = 0;
    size_t close_count = 0;
};

NvmeKvCommandExecutor::PhysicalKey Key(uint8_t seed) {
    NvmeKvCommandExecutor::PhysicalKey key{};
    for (size_t index = 0; index < key.size(); ++index) {
        key[index] = static_cast<uint8_t>(seed + index);
    }
    return key;
}

NvmeKvExecutorResult MakeExecutor(
    const std::shared_ptr<FakeKvInitiator>& initiator,
    uint32_t queue_depth = 2) {
    NvmeKvCommandExecutor::Capabilities capabilities;
    capabilities.effective_max_value_size = 8192;
    capabilities.queue_depth = queue_depth;
    return CreateNvmeKvSpdkExecutorForTesting(
        initiator, std::make_shared<SystemDmaAllocator>(), initiator->Segment(),
        capabilities);
}

TEST(NvmeKvSpdkExecutorTest, BoundsInflightWorkAndHandlesOutOfOrderCompletion) {
    auto initiator = std::make_shared<FakeKvInitiator>();
    initiator->complete_in_reverse = true;
    auto executor_result = MakeExecutor(initiator, 2);
    ASSERT_TRUE(executor_result.has_value());
    auto executor = std::move(executor_result.value());

    const std::array<std::string, 5> values = {"a", "b", "c", "d", "e"};
    std::vector<NvmeKvCommandExecutor::StoreRequest> requests;
    for (size_t index = 0; index < values.size(); ++index) {
        requests.push_back({Key(static_cast<uint8_t>(index)), values[index]});
    }
    executor->StoreBatch(requests);

    for (const auto& request : requests) {
        EXPECT_TRUE(request.result.has_value());
    }
    EXPECT_EQ(2, initiator->max_inflight);
    EXPECT_EQ(3, initiator->poll_count);
}

TEST(NvmeKvSpdkExecutorTest, KeepsPerRequestSubmitAndCompletionErrors) {
    auto initiator = std::make_shared<FakeKvInitiator>();
    initiator->reject_submit_call = 1;
    initiator->completions.resize(3);
    initiator->completions[2].status_code = 0x89;
    auto executor_result = MakeExecutor(initiator, 3);
    ASSERT_TRUE(executor_result.has_value());
    auto executor = std::move(executor_result.value());

    const std::array<std::string, 3> values = {"a", "b", "c"};
    std::vector<NvmeKvCommandExecutor::StoreRequest> requests;
    for (size_t index = 0; index < values.size(); ++index) {
        requests.push_back({Key(static_cast<uint8_t>(index)), values[index]});
    }
    executor->StoreBatch(requests);

    ASSERT_TRUE(requests[0].result.has_value());
    ASSERT_FALSE(requests[1].result.has_value());
    EXPECT_EQ(ErrorCode::BUFFER_OVERFLOW, requests[1].result.error());
    ASSERT_FALSE(requests[2].result.has_value());
    EXPECT_EQ(ErrorCode::OBJECT_ALREADY_EXISTS, requests[2].result.error());
}

TEST(NvmeKvSpdkExecutorTest, AbortsOutstandingCommandsAfterPollFailure) {
    auto initiator = std::make_shared<FakeKvInitiator>();
    initiator->poll_error = -EIO;
    auto executor_result = MakeExecutor(initiator, 3);
    ASSERT_TRUE(executor_result.has_value());
    auto executor = std::move(executor_result.value());

    const std::array<std::string, 3> values = {"a", "b", "c"};
    std::vector<NvmeKvCommandExecutor::StoreRequest> requests;
    for (size_t index = 0; index < values.size(); ++index) {
        requests.push_back({Key(static_cast<uint8_t>(index)), values[index]});
    }
    executor->StoreBatch(requests);

    EXPECT_EQ(1, initiator->abort_count);
    EXPECT_TRUE(initiator->pending.empty());
    for (const auto& request : requests) {
        ASSERT_FALSE(request.result.has_value());
        EXPECT_EQ(ErrorCode::FILE_WRITE_FAIL, request.result.error());
    }
}

TEST(NvmeKvSpdkExecutorTest, AbortsOutstandingCommandsAfterTimeout) {
    ScopedEnv timeout("MOONCAKE_NVME_KV_SPDK_COMPLETION_TIMEOUT_MS", "1");
    auto initiator = std::make_shared<FakeKvInitiator>();
    initiator->defer_completions = true;
    auto executor_result = MakeExecutor(initiator, 3);
    ASSERT_TRUE(executor_result.has_value());
    auto executor = std::move(executor_result.value());

    const std::array<std::string, 3> values = {"a", "b", "c"};
    std::vector<NvmeKvCommandExecutor::StoreRequest> requests;
    for (size_t index = 0; index < values.size(); ++index) {
        requests.push_back({Key(static_cast<uint8_t>(index)), values[index]});
    }
    executor->StoreBatch(requests);

    EXPECT_EQ(1, initiator->abort_count);
    EXPECT_TRUE(initiator->pending.empty());
    for (const auto& request : requests) {
        ASSERT_FALSE(request.result.has_value());
        EXPECT_EQ(ErrorCode::FILE_WRITE_FAIL, request.result.error());
    }
}

TEST(NvmeKvSpdkExecutorTest, RetrievesIntoDmaBackedResult) {
    auto initiator = std::make_shared<FakeKvInitiator>();
    initiator->retrieve_value = "value";
    auto executor_result = MakeExecutor(initiator);
    ASSERT_TRUE(executor_result.has_value());
    auto executor = std::move(executor_result.value());

    auto result = executor->Retrieve(Key(7), initiator->retrieve_value.size());

    ASSERT_TRUE(result.has_value());
    EXPECT_EQ("value", result.value());
    executor.reset();
    EXPECT_EQ(1, initiator->close_count);
}

}  // namespace
}  // namespace mooncake::test
