// transfer_task_test.cpp
#include "transfer_task.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#if __has_include(<jsoncpp/json/json.h>)
#include <jsoncpp/json/json.h>
#else
#include <json/json.h>
#endif

#include "trace_exporter.h"
#include "tracing_facade.h"
#include "types.h"

namespace mooncake {

namespace {

std::vector<std::string> ReadSpanNamesFromJsonl(
    const std::filesystem::path& path) {
    std::ifstream in(path);
    std::vector<std::string> span_names;
    std::string line;

    while (std::getline(in, line)) {
        Json::CharReaderBuilder builder;
        Json::Value root;
        std::string errors;
        std::unique_ptr<Json::CharReader> reader(builder.newCharReader());
        if (!reader->parse(line.data(), line.data() + line.size(), &root,
                           &errors)) {
            continue;
        }
        if (root.isMember("span.name")) {
            span_names.push_back(root["span.name"].asString());
        }
    }
    return span_names;
}

size_t CountSpanName(const std::vector<std::string>& span_names,
                     const std::string& name) {
    return static_cast<size_t>(
        std::count(span_names.begin(), span_names.end(), name));
}

}  // namespace

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

// Test TransferStrategy enum and stream operator
TEST_F(TransferTaskTest, TransferStrategyEnum) {
    // Test enum values
    EXPECT_EQ(static_cast<int>(TransferStrategy::LOCAL_MEMCPY), 0);
    EXPECT_EQ(static_cast<int>(TransferStrategy::TRANSFER_ENGINE), 1);

    // Test stream operator
    std::ostringstream oss;
    oss << TransferStrategy::LOCAL_MEMCPY;
    EXPECT_EQ(oss.str(), "LOCAL_MEMCPY");

    oss.str("");
    oss << TransferStrategy::TRANSFER_ENGINE;
    EXPECT_EQ(oss.str(), "TRANSFER_ENGINE");
}

TEST_F(TransferTaskTest, StartSpanFromCarrierUsesCarrierSpanAsParent) {
    mooncake::tracing::TraceConfig config;
    config.enabled = true;
    config.exporter_mode = "inmemory";
    config.service_name = "test-service";
    config.process_role = "test-role";

    mooncake::tracing::TracingFacade facade(config);
    mooncake::tracing::TraceCarrier carrier{
        .trace_id = "0123456789abcdef0123456789abcdef",
        .span_id = "89abcdef01234567",
        .correlation_id = "corr-1234"};

    auto span = facade.StartSpanFromCarrier("child-span", carrier);
    auto context = span.context();

    EXPECT_EQ(context.trace_id, carrier.trace_id);
    EXPECT_EQ(context.parent_span_id, carrier.span_id);
    EXPECT_EQ(context.correlation_id, carrier.correlation_id);
    EXPECT_NE(context.span_id, carrier.span_id);
}

TEST_F(TransferTaskTest, TransferTraceSessionCreatesStandaloneRootWhenMissingParent) {
    mooncake::tracing::TraceConfig config;
    config.enabled = true;
    config.exporter_mode = "inmemory";
    config.service_name = "test-service";
    config.process_role = "test-role";

    mooncake::tracing::TracingFacade facade(config);
    auto session = TransferTraceSession::Start(facade, nullptr, 4, 4096);

    ASSERT_TRUE(session.parent_context() != nullptr);
    EXPECT_TRUE(session.parent_context()->valid());
    EXPECT_TRUE(session.owns_operation_span());
}

TEST_F(TransferTaskTest, TransferTraceSessionReusesProvidedParentContext) {
    mooncake::tracing::TraceConfig config;
    config.enabled = true;
    config.exporter_mode = "inmemory";
    config.service_name = "test-service";
    config.process_role = "test-role";

    mooncake::tracing::TracingFacade facade(config);
    mooncake::tracing::TraceContext parent_context{
        .trace_id = "trace-parent",
        .span_id = "span-parent",
        .parent_span_id = "",
        .correlation_id = "corr-parent"};

    auto session =
        TransferTraceSession::Start(facade, &parent_context, 2, 1024);

    ASSERT_TRUE(session.parent_context() != nullptr);
    EXPECT_EQ(session.parent_context()->trace_id, parent_context.trace_id);
    EXPECT_EQ(session.parent_context()->span_id, parent_context.span_id);
    EXPECT_FALSE(session.owns_operation_span());
}

TEST_F(TransferTaskTest, TransferTraceSessionReusesSingleWaitCompletionSpan) {
    mooncake::tracing::TraceConfig config;
    config.enabled = true;
    config.exporter_mode = "inmemory";
    config.service_name = "test-service";
    config.process_role = "test-role";

    mooncake::tracing::TracingFacade facade(config);
    auto session = TransferTraceSession::Start(facade, nullptr, 3, 2048);

    auto* first_wait_span = session.EnsureWaitSpan(facade, 77, 3);
    ASSERT_NE(first_wait_span, nullptr);
    ASSERT_TRUE(session.wait_context() != nullptr);
    auto first_context = *session.wait_context();

    auto* second_wait_span = session.EnsureWaitSpan(facade, 77, 3);
    ASSERT_NE(second_wait_span, nullptr);
    ASSERT_TRUE(session.wait_context() != nullptr);
    EXPECT_EQ(session.wait_context()->span_id, first_context.span_id);
    EXPECT_EQ(session.wait_context()->trace_id, first_context.trace_id);

    session.FinishWait(ErrorCode::OK);
    EXPECT_EQ(session.wait_context(), nullptr);
}

TEST_F(TransferTaskTest, TransferTraceSessionEmitsSubmitGapAndCompleteSpans) {
    namespace fs = std::filesystem;

    auto trace_path =
        fs::temp_directory_path() / "transfer-trace-session-spans.jsonl";
    fs::remove(trace_path);

    mooncake::tracing::TraceConfig config;
    config.enabled = true;
    config.exporter_mode = "jsonl";
    config.jsonl_path = trace_path.string();
    config.service_name = "test-service";
    config.process_role = "test-role";

    {
        mooncake::tracing::TracingFacade facade(config);
        auto session = TransferTraceSession::Start(facade, nullptr, 2, 1024);
        session.RecordBatch(55);
        session.StartSubmitGap(facade, 55, 2);
        ASSERT_NE(session.EnsureWaitSpan(facade, 55, 2), nullptr);
        session.FinishWait(ErrorCode::OK);
        session.Finish(facade, ErrorCode::OK);
    }

    auto span_names = ReadSpanNamesFromJsonl(trace_path);
    EXPECT_EQ(CountSpanName(span_names, "mooncake.transfer.operation"), 1u);
    EXPECT_EQ(CountSpanName(span_names, "mooncake.transfer.submit_gap"), 1u);
    EXPECT_EQ(CountSpanName(span_names, "mooncake.transfer.wait_completion"),
              1u);
    EXPECT_EQ(CountSpanName(span_names, "mooncake.transfer.complete"), 1u);

    fs::remove(trace_path);
}

TEST_F(TransferTaskTest, AsyncRemoteExporterDropsWhenQueueIsFull) {
    mooncake::tracing::TraceConfig config;
    config.enabled = true;
    config.exporter_mode = "remote";
    config.service_name = "test-service";
    config.process_role = "test-role";
    config.exporter_queue_max_items = 1;
    config.exporter_queue_max_bytes = 1024;
    config.exporter_retry_base_ms = 1;
    config.exporter_retry_max_ms = 2;
    config.exporter_retry_max_attempts = 1;

    auto fallback = std::make_shared<mooncake::tracing::InMemoryTraceExporter>();
    mooncake::tracing::AsyncRemoteTraceExporter exporter(
        config, fallback,
        [](const std::vector<mooncake::tracing::TraceRecord>&, std::string*) {
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
            return false;
        });

    mooncake::tracing::TraceRecord record;
    record.trace_id = "trace";
    record.span_id = "span";
    record.span_name = "span-name";

    exporter.Export(record);
    exporter.Export(record);
    exporter.Export(record);
    exporter.FlushForTest(std::chrono::milliseconds(200));

    auto stats = exporter.SnapshotStats();
    EXPECT_GE(stats.dropped_records, 1u);
    EXPECT_GE(stats.retry_count, 1u);
    EXPECT_GE(stats.fallback_count, 1u);
}

TEST_F(TransferTaskTest,
       AsyncRemoteExporterPrefersErrorSpanWhenQueueIsFull) {
    mooncake::tracing::TraceConfig config;
    config.enabled = true;
    config.exporter_mode = "remote";
    config.service_name = "priority-service";
    config.process_role = "test-role";
    config.exporter_queue_max_items = 2;
    config.exporter_queue_max_bytes = 2048;
    config.exporter_retry_base_ms = 1;
    config.exporter_retry_max_ms = 2;
    config.exporter_retry_max_attempts = 0;

    auto fallback = std::make_shared<mooncake::tracing::InMemoryTraceExporter>();
    std::atomic<bool> release_remote{false};
    mooncake::tracing::AsyncRemoteTraceExporter exporter(
        config, fallback,
        [&release_remote](const std::vector<mooncake::tracing::TraceRecord>&,
                          std::string*) {
            while (!release_remote.load(std::memory_order_acquire)) {
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            }
            return false;
        });

    mooncake::tracing::TraceRecord low_inflight;
    low_inflight.trace_id = "trace-low-inflight";
    low_inflight.span_id = "span-low-inflight";
    low_inflight.parent_span_id = "parent";
    low_inflight.span_name = "low-inflight";

    mooncake::tracing::TraceRecord low_queue_a;
    low_queue_a.trace_id = "trace-low-a";
    low_queue_a.span_id = "span-low-a";
    low_queue_a.parent_span_id = "parent";
    low_queue_a.span_name = "low-queue-a";

    mooncake::tracing::TraceRecord low_queue_b;
    low_queue_b.trace_id = "trace-low-b";
    low_queue_b.span_id = "span-low-b";
    low_queue_b.parent_span_id = "parent";
    low_queue_b.span_name = "low-queue-b";

    mooncake::tracing::TraceRecord error_record;
    error_record.trace_id = "trace-error";
    error_record.span_id = "span-error";
    error_record.parent_span_id = "parent";
    error_record.span_name = "high-priority-error";
    error_record.status = "ERROR";

    exporter.Export(low_inflight);
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    exporter.Export(low_queue_a);
    exporter.Export(low_queue_b);
    exporter.Export(error_record);

    release_remote.store(true, std::memory_order_release);
    EXPECT_TRUE(exporter.FlushForTest(std::chrono::milliseconds(500)));

    auto exported = fallback->Snapshot();
    bool saw_error_span = false;
    for (const auto& record : exported) {
        if (record.span_name == "high-priority-error") {
            saw_error_span = true;
            break;
        }
    }

    auto stats = exporter.SnapshotStats();
    EXPECT_TRUE(saw_error_span);
    EXPECT_GE(stats.queue_full_count, 1u);
    EXPECT_GE(stats.dropped_records, 1u);
}

TEST_F(TransferTaskTest, TraceSamplerBaseModeKeepsErrorAndSlowSpan) {
    mooncake::tracing::TraceConfig config;
    config.enabled = true;
    config.sampling_mode = "base";
    config.sampling_base_ratio = 0.0;
    config.sampling_slow_threshold_ms = 10;

    mooncake::tracing::TraceSampler sampler(config);

    mooncake::tracing::TraceRecord error_record;
    error_record.status = "ERROR";
    EXPECT_TRUE(sampler.ShouldSample(error_record));

    mooncake::tracing::TraceRecord slow_record;
    slow_record.start_time_unix_nano = 0;
    slow_record.end_time_unix_nano = 20 * 1000 * 1000;
    EXPECT_TRUE(sampler.ShouldSample(slow_record));

    mooncake::tracing::TraceRecord normal_record;
    normal_record.trace_id = "trace-normal";
    normal_record.start_time_unix_nano = 0;
    normal_record.end_time_unix_nano = 1 * 1000 * 1000;
    EXPECT_FALSE(sampler.ShouldSample(normal_record));
}

TEST_F(TransferTaskTest, TraceSamplerDiagKeepsEventfulSpan) {
    mooncake::tracing::TraceConfig config;
    config.enabled = true;
    config.sampling_mode = "diag";
    config.sampling_base_ratio = 0.0;

    mooncake::tracing::TraceSampler sampler(config);

    mooncake::tracing::TraceRecord eventful_record;
    eventful_record.trace_id = "trace-diag";
    eventful_record.events.push_back(
        mooncake::tracing::TraceEvent{"important-event", 1, {}});
    EXPECT_TRUE(sampler.ShouldSample(eventful_record));

    mooncake::tracing::TraceRecord normal_record;
    normal_record.trace_id = "trace-normal";
    EXPECT_FALSE(sampler.ShouldSample(normal_record));
}

TEST_F(TransferTaskTest, AsyncRemoteExporterSpoolsWhenConfiguredAndRemoteFails) {
    namespace fs = std::filesystem;

    mooncake::tracing::TraceConfig config;
    config.enabled = true;
    config.exporter_mode = "remote";
    config.service_name = "spool-service";
    config.process_role = "test-role";
    config.exporter_retry_base_ms = 1;
    config.exporter_retry_max_ms = 2;
    config.exporter_retry_max_attempts = 0;

    auto spool_dir = fs::temp_directory_path() / "mooncake-tracing-spool-test";
    fs::remove_all(spool_dir);
    config.exporter_spool_dir = spool_dir.string();
    config.jsonl_path = (spool_dir / "fallback.jsonl").string();

    mooncake::tracing::AsyncRemoteTraceExporter exporter(
        config, std::make_shared<mooncake::tracing::InMemoryTraceExporter>(),
        [](const std::vector<mooncake::tracing::TraceRecord>&, std::string*) {
            return false;
        });

    mooncake::tracing::TraceRecord record;
    record.trace_id = "trace-spool";
    record.span_id = "span-spool";
    record.span_name = "span-name";

    exporter.Export(record);
    EXPECT_TRUE(exporter.FlushForTest(std::chrono::milliseconds(200)));

    auto stats = exporter.SnapshotStats();
    EXPECT_GE(stats.collector_unreachable_count, 1u);
    EXPECT_GE(stats.spooled_records, 1u);
    EXPECT_EQ(stats.fallback_count, 0u);

    bool found_spool_file = false;
    if (fs::exists(spool_dir)) {
        for (const auto& entry : fs::directory_iterator(spool_dir)) {
            if (entry.is_regular_file()) {
                found_spool_file = true;
                break;
            }
        }
    }
    EXPECT_TRUE(found_spool_file);
    fs::remove_all(spool_dir);
}

TEST_F(TransferTaskTest, AsyncRemoteExporterCountsQueueFullAndCollectorFailure) {
    mooncake::tracing::TraceConfig config;
    config.enabled = true;
    config.exporter_mode = "remote";
    config.service_name = "counter-service";
    config.process_role = "test-role";
    config.exporter_queue_max_items = 1;
    config.exporter_queue_max_bytes = 1024;
    config.exporter_retry_base_ms = 1;
    config.exporter_retry_max_ms = 2;
    config.exporter_retry_max_attempts = 1;

    auto fallback = std::make_shared<mooncake::tracing::InMemoryTraceExporter>();
    mooncake::tracing::AsyncRemoteTraceExporter exporter(
        config, fallback,
        [](const std::vector<mooncake::tracing::TraceRecord>&, std::string*) {
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
            return false;
        });

    mooncake::tracing::TraceRecord record;
    record.trace_id = "trace-counter";
    record.span_id = "span-counter";
    record.span_name = "span-name";

    exporter.Export(record);
    exporter.Export(record);
    exporter.Export(record);
    EXPECT_TRUE(exporter.FlushForTest(std::chrono::milliseconds(200)));

    auto stats = exporter.SnapshotStats();
    EXPECT_GE(stats.dropped_records, 1u);
    EXPECT_GE(stats.queue_full_count, 1u);
    EXPECT_GE(stats.collector_unreachable_count, 1u);
    EXPECT_GE(stats.retry_count, 1u);
    EXPECT_GE(stats.fallback_count, 1u);
}

TEST_F(TransferTaskTest, AsyncRemoteExporterBatchesMultipleRecordsPerSend) {
    mooncake::tracing::TraceConfig config;
    config.enabled = true;
    config.exporter_mode = "remote";
    config.service_name = "batch-service";
    config.process_role = "test-role";
    config.exporter_retry_base_ms = 1;
    config.exporter_retry_max_ms = 2;
    config.exporter_retry_max_attempts = 0;

    std::atomic<int> remote_calls{0};
    std::atomic<size_t> max_batch_size{0};
    std::atomic<bool> release_remote{false};
    mooncake::tracing::AsyncRemoteTraceExporter exporter(
        config, std::make_shared<mooncake::tracing::InMemoryTraceExporter>(),
        [&release_remote,
         &remote_calls,
         &max_batch_size](const std::vector<mooncake::tracing::TraceRecord>& records,
                          std::string*) {
            while (!release_remote.load(std::memory_order_acquire)) {
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            }
            remote_calls.fetch_add(1, std::memory_order_relaxed);
            max_batch_size.store(
                std::max(max_batch_size.load(std::memory_order_relaxed),
                         records.size()),
                std::memory_order_relaxed);
            return true;
        });

    for (int i = 0; i < 8; ++i) {
        mooncake::tracing::TraceRecord record;
        record.trace_id = "trace-batch-" + std::to_string(i);
        record.span_id = "span-batch-" + std::to_string(i);
        record.span_name = "span-batch";
        exporter.Export(record);
    }

    release_remote.store(true, std::memory_order_release);
    EXPECT_TRUE(exporter.FlushForTest(std::chrono::milliseconds(200)));
    EXPECT_LT(remote_calls.load(std::memory_order_relaxed), 8);
    EXPECT_GT(max_batch_size.load(std::memory_order_relaxed), 1u);
}

}  // namespace mooncake

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
