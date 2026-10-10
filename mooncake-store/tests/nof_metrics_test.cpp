#include <gtest/gtest.h>

#include <array>
#include <cerrno>
#include <cstdlib>
#include <deque>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#include "client_metric.h"
#include "spdk/spdk_wrapper.h"
#include "transfer_task.h"

namespace mooncake {

TEST(NofMetrics, ClientExportAndSummary) {
    ClientMetric metric(0);
    metric.nof_metric.ObserveRead(8192);
    metric.nof_metric.RecordReadErrors();
    std::string text;
    metric.serialize(text);
    EXPECT_NE(text.find("mooncake_nof_read_bytes_total 8192\n"),
              std::string::npos);
    text.clear();
    metric.transfer_metric.serialize(text);
    EXPECT_EQ(text.find("mooncake_nof_"), std::string::npos);
    const auto summary = metric.summary_metrics();
    EXPECT_NE(summary.find("=== NoF Metrics Summary ==="), std::string::npos);
    EXPECT_NE(summary.find("NoF Read: 8.00 KB, ops=1, errors=1"),
              std::string::npos);
}

TEST(NofMetrics, ClientTotalsKeepReadWriteAndErrorsSeparate) {
    NofMetric metric({{"client", "test"}});
    EXPECT_EQ(metric.nof_read_bytes.value(), 0);
    metric.ObserveRead(4096);
    metric.RecordReadErrors();
    metric.ObserveWrite(262144);
    metric.ObserveWrite(524288);
    metric.RecordWriteErrors();
    EXPECT_EQ(metric.nof_read_bytes.value(), 4096);
    EXPECT_EQ(metric.nof_read_ops.value(), 1);
    EXPECT_EQ(metric.nof_read_errors.value(), 1);
    EXPECT_EQ(metric.nof_write_errors.value(), 1);
    EXPECT_EQ(metric.nof_write_bytes.value(), 786432);
    EXPECT_EQ(metric.nof_write_ops.value(), 2);
    std::string text;
    metric.serialize(text);
    EXPECT_NE(text.find("# TYPE mooncake_nof_read_bytes_total counter\n"),
              std::string::npos);
    EXPECT_NE(
        text.find("mooncake_nof_read_bytes_total{client=\"test\"} 4096\n"),
        std::string::npos);
    EXPECT_NE(
        text.find("mooncake_nof_write_bytes_total{client=\"test\"} 786432\n"),
        std::string::npos);
    EXPECT_NE(text.find("mooncake_nof_read_errors_total{client=\"test\"} 1\n"),
              std::string::npos);
    EXPECT_EQ(text.find("operation="), std::string::npos);
    EXPECT_EQ(text.find("reason="), std::string::npos);
    EXPECT_EQ(text.find("segment="), std::string::npos);
}

TEST(NofMetrics, ConcurrentRecordingAndScrapesPreserveTotals) {
    NofMetric metric;
    std::vector<std::jthread> workers;
    for (int i = 0; i < 32; ++i) {
        workers.emplace_back([&] {
            for (int j = 0; j < 10000; ++j) {
                metric.ObserveWrite(262144);
            }
            metric.RecordReadErrors();
        });
    }
    for (int i = 0; i < 100; ++i) {
        std::string text;
        metric.serialize(text);
    }
    for (auto& worker : workers) worker.join();
    EXPECT_EQ(metric.nof_write_ops.value(), 320000);
    EXPECT_EQ(metric.nof_write_bytes.value(), int64_t{320000} * 262144);
    EXPECT_EQ(metric.nof_read_bytes.value(), 0);
    EXPECT_EQ(metric.nof_read_ops.value(), 0);
    EXPECT_EQ(metric.nof_read_errors.value(), 32);
}

namespace {
constexpr uint32_t kBlockSize = 4096;

struct PendingIo {
    spdk_nvme_cmd_cb callback;
    void* context;
    bool fail;
};
}  // namespace

// Replace SPDK at link time; the worker and completion callback stay real.
struct ctrlr_info {};
struct nof_seg_handle {
    int reject_call = 0;
    int fail_call = 0;
    mutable int submit_calls = 0;
    mutable std::deque<PendingIo> pending_io;
};

SpdkWrapper::SpdkWrapper() = default;
SpdkWrapper::~SpdkWrapper() = default;
SpdkWrapper& SpdkWrapper::GetInstance() {
    static SpdkWrapper wrapper;
    return wrapper;
}
nof_seg_handle* SpdkWrapper::OpenNofSegment(const std::string&) {
    ADD_FAILURE() << "Worker tests supply the segment directly";
    return nullptr;
}
uint32_t SpdkWrapper::GetBlockSize(const nof_seg_handle*) { return kBlockSize; }
int SpdkWrapper::SubmitRequest(const nof_seg_handle* segment, void*, uint64_t,
                               uint32_t, int, spdk_nvme_cmd_cb callback,
                               void* context) {
    const int call = ++segment->submit_calls;
    if (call == segment->reject_call) return -ENOMEM;
    segment->pending_io.push_back(
        {callback, context, call == segment->fail_call});
    return 0;
}
int64_t SpdkWrapper::NvmePollProcessCompletion(nof_seg_handle* segment,
                                               uint32_t) {
    int64_t completed = 0;
    while (!segment->pending_io.empty()) {
        // Reverse order also exercises successful completions after a failure.
        const auto io = segment->pending_io.back();
        segment->pending_io.pop_back();
        spdk_nvme_cpl completion{};
        if (io.fail) {
            completion.status.sct = 2;
            completion.status.sc = 128;
        }
        io.callback(io.context, &completion);
        ++completed;
    }
    return completed;
}

namespace test {
class NofWorkerMetricsTest : public ::testing::TestWithParam<int> {
   protected:
    void SetEnv(const char* name, const char* value) {
        const char* old = std::getenv(name);
        old_env_[name] = old ? std::optional<std::string>(old) : std::nullopt;
        setenv(name, value, 1);
    }

    void SetUp() override {
        SetEnv("MC_NOF_WORKERS", "1");
        SetEnv("MC_NOF_SUBMIT_CHUNK_BYTES", "4096");
        SetEnv("MC_NOF_INFLIGHT_BYTES_LIMIT", "16384");
    }

    void TearDown() override {
        for (const auto& [name, value] : old_env_) {
            if (value)
                setenv(name.c_str(), value->c_str(), 1);
            else
                unsetenv(name.c_str());
        }
    }

    std::shared_ptr<SpdkNofOperationState> Submit(SpdkNofWorkerPool& pool,
                                                  uint32_t blocks = 3) {
        auto state = std::make_shared<SpdkNofOperationState>();
        SpdkNofTask task(&segment_, buffer_.data(), 0, blocks, GetParam(),
                         state);
        task.metric = &metric_;
        task.block_size = kBlockSize;
        pool.submitTask(std::move(task));
        return state;
    }

    void ExpectMetrics(int64_t bytes, int64_t ops, int64_t errors) {
        const bool read = GetParam() == kSpdkNofOpRead;
        EXPECT_EQ(metric_.nof_read_bytes.value(), read ? bytes : 0);
        EXPECT_EQ(metric_.nof_read_ops.value(), read ? ops : 0);
        EXPECT_EQ(metric_.nof_read_errors.value(), read ? errors : 0);
        EXPECT_EQ(metric_.nof_write_bytes.value(), read ? 0 : bytes);
        EXPECT_EQ(metric_.nof_write_ops.value(), read ? 0 : ops);
        EXPECT_EQ(metric_.nof_write_errors.value(), read ? 0 : errors);
    }

    nof_seg_handle segment_;
    std::array<char, 5 * kBlockSize> buffer_{};
    NofMetric metric_;
    std::map<std::string, std::optional<std::string>> old_env_;
};

TEST_P(NofWorkerMetricsTest, SuccessfulCompletionsCountEachChunk) {
    SpdkNofWorkerPool pool;
    auto state = Submit(pool);
    state->wait_for_completion();
    EXPECT_EQ(state->get_result(), ErrorCode::OK);
    EXPECT_EQ(segment_.submit_calls, 3);
    ExpectMetrics(3 * kBlockSize, 3, 0);
}

TEST_P(NofWorkerMetricsTest, SubmissionFailureKeepsCompletedChunks) {
    segment_.reject_call = 2;
    SpdkNofWorkerPool pool;
    auto state = Submit(pool);
    state->wait_for_completion();
    EXPECT_EQ(state->get_result(), ErrorCode::TRANSFER_FAIL);
    EXPECT_EQ(segment_.submit_calls, 2);
    ExpectMetrics(kBlockSize, 1, 1);
}

TEST_P(NofWorkerMetricsTest, CompletionFailureKeepsSuccessfulSiblings) {
    segment_.fail_call = 2;
    SpdkNofWorkerPool pool;
    auto state = Submit(pool);
    state->wait_for_completion();
    EXPECT_EQ(state->get_result(), ErrorCode::TRANSFER_FAIL);
    EXPECT_EQ(segment_.submit_calls, 3);
    ExpectMetrics(2 * kBlockSize, 2, 1);
}

INSTANTIATE_TEST_SUITE_P(ReadAndWrite, NofWorkerMetricsTest,
                         ::testing::Values(kSpdkNofOpRead, kSpdkNofOpWrite),
                         [](const ::testing::TestParamInfo<int>& info) {
                             return info.param == kSpdkNofOpRead ? "Read"
                                                                 : "Write";
                         });
}  // namespace test
}  // namespace mooncake
