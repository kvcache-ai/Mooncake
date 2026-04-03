// Copyright 2024 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <sys/time.h>

#include <cstdlib>
#include <fstream>
#include <iomanip>
#include <memory>

#include "multi_transport.h"
#include "trace_context.h"
#include "transfer_engine.h"
#include "transfer_engine_impl.h"
#include "transport/transport.h"

using namespace mooncake;

namespace mooncake {

class TransportTest : public ::testing::Test {
   protected:
    void SetUp() override {
        google::InitGoogleLogging("TransportTest");
        FLAGS_logtostderr = 1;
    }

    void TearDown() override { google::ShutdownGoogleLogging(); }
};

static int CreateTempFile() {
    char temp_filename[] = "/tmp/testfileXXXXXX";
    int fd = mkstemp(temp_filename);
    if (fd == -1) {
        return -1;
    }
    unlink(temp_filename);
    return fd;
}

int CreateTempFileWithContent(const char* content) {
    char temp_filename[] = "/tmp/testfileXXXXXX";
    int fd = mkstemp(temp_filename);
    if (fd == -1) {
        return -1;
    }
    unlink(temp_filename);

    ssize_t nbytes = write(fd, content, strlen(content));
    (void)nbytes;
    lseek(fd, 0, SEEK_SET);

    return fd;
}

TEST_F(TransportTest, parseHostNameWithPortTest) {
    std::string local_server_name = "0.0.0.0:1234";
    auto res = parseHostNameWithPort(local_server_name);
    ASSERT_EQ(res.first, "0.0.0.0");
    ASSERT_EQ(res.second, 1234);

    local_server_name = "1.2.3.4:111111";
    res = parseHostNameWithPort(local_server_name);
    ASSERT_EQ(res.first, "1.2.3.4");
    ASSERT_EQ(res.second, 12001);
}

TEST_F(TransportTest, WriteSuccess) {
    int fd = CreateTempFile();
    ASSERT_NE(fd, -1) << "Failed to create temporary file";

    const char* testData = "Hello, World!";
    size_t testDataLen = strlen(testData);

    ssize_t result = writeFully(fd, testData, testDataLen);
    EXPECT_EQ(result, static_cast<ssize_t>(testDataLen));

    char buffer[256] = {0};
    ssize_t nbytes = lseek(fd, 0, SEEK_SET);
    (void)nbytes;
    nbytes = read(fd, buffer, testDataLen);
    (void)nbytes;
    EXPECT_STREQ(buffer, testData);

    close(fd);
}

TEST_F(TransportTest, WriteInvalidFD) {
    const char* testData = "Hello, World!";
    size_t testDataLen = strlen(testData);

    ssize_t result = writeFully(-1, testData, testDataLen);
    ASSERT_EQ(result, -1);
    ASSERT_EQ(errno, EBADF);
}

TEST_F(TransportTest, PartialWrite) {
    int fd = CreateTempFile();
    ASSERT_NE(fd, -1) << "Failed to create temporary file";

    const char* testData = "Hello, World!";
    size_t testDataLen = strlen(testData);

    ssize_t result = writeFully(fd, testData, testDataLen / 2);

    ASSERT_EQ(result, static_cast<ssize_t>(testDataLen / 2));

    char buffer[256] = {0};
    lseek(fd, 0, SEEK_SET);
    ssize_t nbytes = read(fd, buffer, result);
    (void)nbytes;
    ASSERT_EQ(strncmp(buffer, testData, result), 0);
    close(fd);
}

TEST_F(TransportTest, ReadSuccess) {
    const char* testData = "Hello, World!";
    int fd = CreateTempFileWithContent(testData);
    ASSERT_NE(fd, -1) << "Failed to create temporary file";

    char buffer[256] = {0};
    ssize_t bytesRead = readFully(fd, buffer, sizeof(buffer));

    EXPECT_EQ(bytesRead, static_cast<ssize_t>(strlen(testData)));
    EXPECT_STREQ(buffer, testData);

    close(fd);
}

TEST_F(TransportTest, ReadInvalidFD) {
    char buffer[256] = {0};
    ssize_t bytesRead = readFully(-1, buffer, sizeof(buffer));
    EXPECT_EQ(bytesRead, -1);
    EXPECT_EQ(errno, EBADF);
}

TEST_F(TransportTest, PartialRead) {
    const char* testData = "Hello, World!";
    int fd = CreateTempFileWithContent(testData);
    ASSERT_NE(fd, -1) << "Failed to create temporary file";

    char buffer[256] = {0};
    size_t half_len = strlen(testData) / 2;
    ssize_t bytesRead = readFully(fd, buffer, half_len);

    EXPECT_EQ(bytesRead, static_cast<ssize_t>(half_len));
    EXPECT_EQ(strncmp(buffer, testData, half_len), 0);

    close(fd);
}

TEST_F(TransportTest, ReadEmptyFile) {
    int fd = CreateTempFileWithContent("");
    ASSERT_NE(fd, -1) << "Failed to create temporary file";

    char buffer[256] = {0};
    ssize_t bytesRead = readFully(fd, buffer, sizeof(buffer));

    EXPECT_EQ(bytesRead, static_cast<ssize_t>(0));

    close(fd);
}

TEST_F(TransportTest, BatchTraceRegistryDeduplicatesSliceAndTaskTerminal) {
    MultiTransportTraceRegistry registry;
    Transport::BatchID batch_id = 42;

    tracing::TraceContext batch_context{
        .trace_id = "trace-1",
        .span_id = "batch-span",
        .parent_span_id = "",
        .correlation_id = "corr-1"};
    tracing::TraceContext task_context{
        .trace_id = "trace-1",
        .span_id = "task-span",
        .parent_span_id = "batch-span",
        .correlation_id = "corr-1"};

    registry.RegisterBatch(batch_id, batch_context);
    registry.RegisterTask(batch_id, 0, task_context, "tcp", 2, 1024);

    auto task = registry.LookupTask(batch_id, 0);
    ASSERT_TRUE(task.has_value());
    EXPECT_EQ(task->transport_name, "tcp");
    EXPECT_EQ(task->slice_terminal_states.size(), 2u);

    EXPECT_TRUE(registry.MarkSliceQueued(batch_id, 0, 0));
    EXPECT_FALSE(registry.MarkSliceQueued(batch_id, 0, 0));

    EXPECT_TRUE(registry.MarkSliceTerminal(
        batch_id, 0, 0, Transport::Slice::SUCCESS));
    EXPECT_FALSE(registry.MarkSliceTerminal(
        batch_id, 0, 0, Transport::Slice::SUCCESS));

    EXPECT_TRUE(registry.MarkTaskTerminal(batch_id, 0));
    EXPECT_FALSE(registry.MarkTaskTerminal(batch_id, 0));

    EXPECT_TRUE(registry.MarkBatchTerminal(batch_id));
    EXPECT_FALSE(registry.MarkBatchTerminal(batch_id));
}

TEST_F(TransportTest, BatchTraceRegistryTracksTimeoutAsDistinctTerminal) {
    MultiTransportTraceRegistry registry;
    Transport::BatchID batch_id = 7;

    tracing::TraceContext batch_context{
        .trace_id = "trace-2",
        .span_id = "batch-2",
        .parent_span_id = "",
        .correlation_id = "corr-2"};
    tracing::TraceContext task_context{
        .trace_id = "trace-2",
        .span_id = "task-2",
        .parent_span_id = "batch-2",
        .correlation_id = "corr-2"};

    registry.RegisterBatch(batch_id, batch_context);
    registry.RegisterTask(batch_id, 1, task_context, "rdma", 1, 4096);

    EXPECT_TRUE(registry.MarkSliceTerminal(
        batch_id, 1, 0, Transport::Slice::TIMEOUT));
    EXPECT_FALSE(registry.MarkSliceTerminal(
        batch_id, 1, 0, Transport::Slice::FAILED));

    auto task = registry.LookupTask(batch_id, 1);
    ASSERT_TRUE(task.has_value());
    EXPECT_EQ(task->slice_terminal_states[0],
              MultiTransportTraceRegistry::SliceTerminalState::kTimedOut);
}

TEST_F(TransportTest, BatchTraceRegistryKeepsFirstBatchContext) {
    MultiTransportTraceRegistry registry;
    Transport::BatchID batch_id = 99;

    tracing::TraceContext root_context{
        .trace_id = "trace-root",
        .span_id = "root-span",
        .parent_span_id = "",
        .correlation_id = "corr-root"};
    tracing::TraceContext later_context{
        .trace_id = "trace-root",
        .span_id = "later-span",
        .parent_span_id = "root-span",
        .correlation_id = "corr-root"};

    registry.RegisterBatch(batch_id, root_context);
    registry.RegisterBatch(batch_id, later_context);

    auto batch_context = registry.LookupBatchContext(batch_id);
    ASSERT_TRUE(batch_context.has_value());
    EXPECT_EQ(batch_context->span_id, "root-span");
    EXPECT_EQ(batch_context->trace_id, "trace-root");
}

TEST_F(TransportTest, ActiveBatchTraceRegistryKeepsRootAliveUntilFinish) {
    tracing::TracingFacade tracing({.enabled = true,
                                    .exporter_mode = "inmemory",
                                    .service_name = "test-te",
                                    .node_id = "node-a",
                                    .process_role = "unit-test"});
    ActiveBatchTraceRegistry registry;
    Transport::BatchID batch_id = 123;

    auto first = registry.EnsureRoot(tracing, batch_id);
    ASSERT_TRUE(first.context.valid());
    EXPECT_TRUE(first.created);

    auto second = registry.EnsureRoot(tracing, batch_id);
    ASSERT_TRUE(second.context.valid());
    EXPECT_FALSE(second.created);
    EXPECT_EQ(second.context.trace_id, first.context.trace_id);
    EXPECT_EQ(second.context.span_id, first.context.span_id);

    auto data_plane = registry.EnsureDataPlane(tracing, batch_id);
    ASSERT_TRUE(data_plane.context.valid());
    EXPECT_TRUE(data_plane.created);
    EXPECT_EQ(data_plane.context.trace_id, first.context.trace_id);

    auto data_plane_again = registry.EnsureDataPlane(tracing, batch_id);
    ASSERT_TRUE(data_plane_again.context.valid());
    EXPECT_FALSE(data_plane_again.created);
    EXPECT_EQ(data_plane_again.context.span_id, data_plane.context.span_id);

    EXPECT_TRUE(registry.Finish(batch_id, "batch terminal status",
                                {{"status", "COMPLETED"}}, false));
    EXPECT_FALSE(registry.Finish(batch_id, "batch terminal status",
                                 {{"status", "COMPLETED"}}, false));

    auto third = registry.EnsureRoot(tracing, batch_id);
    ASSERT_TRUE(third.context.valid());
    EXPECT_TRUE(third.created);
    EXPECT_NE(third.context.span_id, first.context.span_id);
}

TEST_F(TransportTest, ActiveBatchTraceRegistryUsesProvidedParentContext) {
    tracing::TracingFacade tracing({.enabled = true,
                                    .exporter_mode = "inmemory",
                                    .service_name = "test-te",
                                    .node_id = "node-a",
                                    .process_role = "unit-test"});
    ActiveBatchTraceRegistry registry;
    Transport::BatchID batch_id = 321;

    tracing::TraceContext parent_context{
        .trace_id = "trace-parent",
        .span_id = "parent-span",
        .parent_span_id = "",
        .correlation_id = "corr-parent"};

    auto root = registry.EnsureRoot(tracing, batch_id, &parent_context);
    ASSERT_TRUE(root.context.valid());
    EXPECT_TRUE(root.created);
    EXPECT_EQ(root.context.trace_id, parent_context.trace_id);
    EXPECT_EQ(root.context.parent_span_id, parent_context.span_id);
    EXPECT_EQ(root.context.correlation_id, parent_context.correlation_id);
}

TEST_F(TransportTest,
       GetTransferStatusTreatsObservedSuccessfulSliceAsCompleted) {
    std::string local_server_name = "127.0.0.1:12345";
    MultiTransport multi_transport(nullptr, local_server_name);

    auto* batch_desc = new Transport::BatchDesc();
    batch_desc->id = reinterpret_cast<Transport::BatchID>(batch_desc);
    batch_desc->batch_size = 1;
    batch_desc->task_list.resize(1);

    auto& task = batch_desc->task_list[0];
    task.batch_id = batch_desc->id;
    task.slice_count = 1;
    task.total_bytes = 4096;

    auto* slice = new Transport::Slice();
    slice->task = &task;
    slice->length = 4096;
    slice->status = Transport::Slice::SUCCESS;
    task.slice_list.push_back(slice);

    Transport::TransferStatus status;
    ASSERT_TRUE(
        multi_transport.getTransferStatus(batch_desc->id, 0, status).ok());
    EXPECT_EQ(status.s, Transport::TransferStatusEnum::COMPLETED);
    EXPECT_EQ(status.transferred_bytes, 4096u);
    EXPECT_TRUE(task.is_finished);

    delete batch_desc;
}

TEST_F(TransportTest, SliceTraceLifecycleTracksSuccessAndTimeout) {
    setenv("MC_TRACING_ENABLED", "1", 1);
    setenv("MC_TRACING_EXPORTER", "jsonl", 1);

    tracing::TraceContext parent_context{
        .trace_id = "trace-slice",
        .span_id = "task-span",
        .parent_span_id = "batch-span",
        .correlation_id = "corr-slice"};

    Transport::BatchDesc success_batch;
    success_batch.batch_size = 1;
    Transport::TransferTask success_task;
    success_task.batch_id =
        reinterpret_cast<Transport::BatchID>(&success_batch);
    success_task.slice_count = 1;
    Transport::Slice success_slice;
    success_slice.task = &success_task;
    success_slice.length = 512;
    success_slice.status = Transport::Slice::PENDING;

    success_slice.StartTrace(parent_context, success_task.batch_id, 3, 0, "tcp");
    EXPECT_TRUE(success_slice.has_active_trace_for_test());
    EXPECT_FALSE(success_slice.trace_terminal_recorded_for_test());

    success_slice.markSuccess();
    EXPECT_TRUE(success_slice.trace_terminal_recorded_for_test());
    EXPECT_EQ(success_task.success_slice_count, 1u);
    EXPECT_EQ(success_task.transferred_bytes, 512u);

    Transport::BatchDesc timeout_batch;
    timeout_batch.batch_size = 1;
    Transport::TransferTask timeout_task;
    timeout_task.batch_id =
        reinterpret_cast<Transport::BatchID>(&timeout_batch);
    Transport::Slice timeout_slice;
    timeout_slice.task = &timeout_task;
    timeout_slice.length = 256;
    timeout_slice.status = Transport::Slice::TIMEOUT;

    timeout_slice.StartTrace(parent_context, timeout_task.batch_id, 4, 1,
                             "rdma");
    EXPECT_TRUE(timeout_slice.has_active_trace_for_test());
    EXPECT_TRUE(timeout_slice.trace_terminal_recorded_for_test());

    timeout_slice.markTimeoutForTrace();
    EXPECT_TRUE(timeout_slice.trace_terminal_recorded_for_test());
}
}  // namespace mooncake

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
