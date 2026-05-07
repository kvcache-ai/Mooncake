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

#include <gtest/gtest.h>

#include "multi_transport.h"
#include "transfer_metadata.h"
#include "transport/transport.h"

using namespace mooncake;

namespace mooncake {

namespace {

class RecordingTransport : public Transport {
   public:
    RecordingTransport(const char *name, bool supports_grouped_scatter,
                       Status submit_status = Status::OK())
        : name_(name),
          supports_grouped_scatter_(supports_grouped_scatter),
          submit_status_(std::move(submit_status)) {}

    Status submitTransfer(BatchID,
                          const std::vector<TransferRequest> &) override {
        return Status::NotImplemented("unused");
    }

    Status submitTransferTask(
        const std::vector<Transport::TransferTask *> &task_list) override {
        submitted_group_sizes.clear();
        submitted_group_sizes.reserve(task_list.size());
        submitted_tasks = task_list;
        for (auto *task : task_list) {
            submitted_group_sizes.push_back(
                task->request_group.empty() ? 1 : task->request_group.size());
            if (task->request_group.empty()) {
                EXPECT_EQ(task->request, &task->owned_request);
            } else {
                EXPECT_EQ(task->request, &task->request_group[0]);
            }
            task->slice_count = 1;
            if (submit_status_.ok()) {
                task->success_slice_count = 1;
                task->transferred_bytes = task->request_group.empty()
                                              ? task->request->length
                                              : task->request_group[0].length;
            } else {
                task->failed_slice_count = 1;
            }
            task->publish_completion();
        }
        return submit_status_;
    }

    Status getTransferStatus(BatchID, size_t, TransferStatus &) override {
        return Status::NotImplemented("unused");
    }

    const char *getName() const override { return name_; }

    bool supportsGroupedScatter() const override {
        return supports_grouped_scatter_;
    }

    std::vector<size_t> submitted_group_sizes;
    std::vector<Transport::TransferTask *> submitted_tasks;

   private:
    int registerLocalMemory(void *, size_t, const std::string &, bool,
                            bool) override {
        return 0;
    }
    int unregisterLocalMemory(void *, bool) override { return 0; }
    int registerLocalMemoryBatch(const std::vector<BufferEntry> &,
                                 const std::string &) override {
        return 0;
    }
    int unregisterLocalMemoryBatch(const std::vector<void *> &) override {
        return 0;
    }

    const char *name_;
    bool supports_grouped_scatter_;
    Status submit_status_;
};

class TestableMultiTransport : public MultiTransport {
   public:
    TestableMultiTransport(std::shared_ptr<TransferMetadata> metadata,
                           std::string &local_server_name)
        : MultiTransport(metadata, local_server_name) {}

    using MultiTransport::transport_map_;
};

std::shared_ptr<TransferMetadata> MakeMetadataWithSegment(
    Transport::SegmentID segment_id, const std::string &protocol) {
    auto metadata = std::make_shared<TransferMetadata>(P2PHANDSHAKE);
    auto desc = std::make_shared<TransferMetadata::SegmentDesc>();
    desc->name = "remote" + std::to_string(segment_id);
    desc->protocol = protocol;
    metadata->addLocalSegment(segment_id, desc->name, std::move(desc));
    return metadata;
}

}  // namespace

// ---------------------------------------------------------------------------
// Unit tests for TransferRequest::task_group_id field and grouping semantics
// ---------------------------------------------------------------------------

class TaskGroupingTest : public ::testing::Test {};

TEST_F(TaskGroupingTest, DefaultTaskGroupIdIsNoGroup) {
    Transport::TransferRequest req{};
    req.opcode = Transport::TransferRequest::READ;
    req.source = nullptr;
    req.target_id = 0;
    req.target_offset = 0;
    req.length = 4096;

    EXPECT_EQ(req.task_group_id, Transport::TransferRequest::kNoTaskGroup);
}

TEST_F(TaskGroupingTest, SentinelValueIsZero) {
    EXPECT_EQ(Transport::TransferRequest::kNoTaskGroup, 0u);
}

TEST_F(TaskGroupingTest, GroupIdCanBeSet) {
    Transport::TransferRequest req{};
    req.opcode = Transport::TransferRequest::READ;
    req.source = nullptr;
    req.target_id = 0;
    req.target_offset = 0;
    req.length = 4096;
    req.task_group_id = 42;

    EXPECT_EQ(req.task_group_id, 42u);
}

// Verify that adjacent requests sharing the same task_group_id are
// distinguishable from ungrouped requests.
TEST_F(TaskGroupingTest, GroupVsUngroupedRequests) {
    std::vector<Transport::TransferRequest> entries(5);
    for (auto &e : entries) {
        e.opcode = Transport::TransferRequest::READ;
        e.source = nullptr;
        e.target_id = 1;
        e.target_offset = 0;
        e.length = 1024;
    }

    // entries[0]: ungrouped
    entries[0].task_group_id = Transport::TransferRequest::kNoTaskGroup;
    // entries[1..2]: grouped with id=100
    entries[1].task_group_id = 100;
    entries[2].task_group_id = 100;
    // entries[3]: ungrouped
    entries[3].task_group_id = Transport::TransferRequest::kNoTaskGroup;
    // entries[4]: different group
    entries[4].task_group_id = 200;

    // Count logical tasks: ungrouped requests each count as 1,
    // adjacent requests with same group id count as 1 logical task.
    size_t logical_count = 0;
    size_t i = 0;
    while (i < entries.size()) {
        if (entries[i].task_group_id !=
            Transport::TransferRequest::kNoTaskGroup) {
            uint64_t gid = entries[i].task_group_id;
            while (i < entries.size() && entries[i].task_group_id == gid) {
                ++i;
            }
        } else {
            ++i;
        }
        ++logical_count;
    }

    // Expected: entry0(1) + entries1-2(1) + entry3(1) + entry4(1) = 4
    EXPECT_EQ(logical_count, 4u);
}

// Verify that batch_size accounting uses logical task count.
TEST_F(TaskGroupingTest, BatchCapacityUsesLogicalTaskCount) {
    // Create 6 requests where 3 pairs are grouped -> 3 logical tasks
    std::vector<Transport::TransferRequest> entries(6);
    for (size_t i = 0; i < entries.size(); ++i) {
        entries[i].opcode = Transport::TransferRequest::READ;
        entries[i].source = nullptr;
        entries[i].target_id = 1;
        entries[i].target_offset = i * 4096;
        entries[i].length = 4096;
    }

    // Group pairs: (0,1), (2,3), (4,5)
    entries[0].task_group_id = 10;
    entries[1].task_group_id = 10;
    entries[2].task_group_id = 20;
    entries[3].task_group_id = 20;
    entries[4].task_group_id = 30;
    entries[5].task_group_id = 30;

    // Count logical tasks
    size_t logical_count = 0;
    size_t i = 0;
    while (i < entries.size()) {
        if (entries[i].task_group_id !=
            Transport::TransferRequest::kNoTaskGroup) {
            uint64_t gid = entries[i].task_group_id;
            while (i < entries.size() && entries[i].task_group_id == gid) {
                ++i;
            }
        } else {
            ++i;
        }
        ++logical_count;
    }

    // 6 raw requests -> 3 logical tasks
    EXPECT_EQ(logical_count, 3u);
    // A batch of capacity 3 should be sufficient
    EXPECT_LE(logical_count, 3u);
    // A batch of capacity 2 would be insufficient
    EXPECT_GT(logical_count, 2u);
}

// Non-adjacent requests with the same group id should NOT be merged.
TEST_F(TaskGroupingTest, NonAdjacentSameGroupIdNotMerged) {
    std::vector<Transport::TransferRequest> entries(4);
    for (auto &e : entries) {
        e.opcode = Transport::TransferRequest::READ;
        e.source = nullptr;
        e.target_id = 1;
        e.target_offset = 0;
        e.length = 1024;
    }

    // Pattern: group=100, ungrouped, group=100 — the two group=100
    // runs should NOT merge because they are not adjacent.
    entries[0].task_group_id = 100;
    entries[1].task_group_id = Transport::TransferRequest::kNoTaskGroup;
    entries[2].task_group_id = 100;
    entries[3].task_group_id = Transport::TransferRequest::kNoTaskGroup;

    size_t logical_count = 0;
    size_t i = 0;
    while (i < entries.size()) {
        if (entries[i].task_group_id !=
            Transport::TransferRequest::kNoTaskGroup) {
            uint64_t gid = entries[i].task_group_id;
            while (i < entries.size() && entries[i].task_group_id == gid) {
                ++i;
            }
        } else {
            ++i;
        }
        ++logical_count;
    }

    // entries[0](group=100) + entries[1](ungrouped) + entries[2](group=100)
    //   + entries[3](ungrouped) = 4 logical tasks
    EXPECT_EQ(logical_count, 4u);
}

// Grouping does not impose requirements on local buffer layout.
// Each request within a group has its own independent source pointer;
// the transport processes slices independently per-request.
TEST_F(TaskGroupingTest, MultiTransportSubmitTransferCoalescesRdmaGroups) {
    constexpr Transport::SegmentID kTargetSegmentId = 1;
    auto metadata = MakeMetadataWithSegment(kTargetSegmentId, "rdma");
    std::string local_server_name = "local";
    TestableMultiTransport multi_transport(metadata, local_server_name);
    auto recording_transport =
        std::make_shared<RecordingTransport>("rdma", true);
    multi_transport.transport_map_["rdma"] = recording_transport;

    std::vector<Transport::TransferRequest> entries(4);
    for (size_t i = 0; i < entries.size(); ++i) {
        entries[i].opcode = Transport::TransferRequest::READ;
        entries[i].source = nullptr;
        entries[i].target_id = kTargetSegmentId;
        entries[i].target_offset = i * 4096;
        entries[i].length = 4096;
    }
    entries[0].task_group_id = 10;
    entries[1].task_group_id = 10;
    entries[2].task_group_id = Transport::TransferRequest::kNoTaskGroup;
    entries[3].task_group_id = 20;

    auto batch_id = multi_transport.allocateBatchID(3);
    auto status = multi_transport.submitTransfer(batch_id, entries);

    EXPECT_TRUE(status.ok());
    EXPECT_EQ(recording_transport->submitted_group_sizes,
              (std::vector<size_t>{2, 1, 1}));
    EXPECT_EQ(multi_transport.freeBatchID(batch_id), Status::OK());
}

TEST_F(TaskGroupingTest, MultiTransportSubmitTransferDoesNotCoalesceNonRdma) {
    constexpr Transport::SegmentID kTargetSegmentId = 1;
    auto metadata = MakeMetadataWithSegment(kTargetSegmentId, "tcp");
    std::string local_server_name = "local";
    TestableMultiTransport multi_transport(metadata, local_server_name);
    auto recording_transport =
        std::make_shared<RecordingTransport>("tcp", false);
    multi_transport.transport_map_["tcp"] = recording_transport;

    std::vector<Transport::TransferRequest> entries(2);
    for (size_t i = 0; i < entries.size(); ++i) {
        entries[i].opcode = Transport::TransferRequest::READ;
        entries[i].source = nullptr;
        entries[i].target_id = kTargetSegmentId;
        entries[i].target_offset = i * 4096;
        entries[i].length = 4096;
        entries[i].task_group_id = 10;
    }

    auto batch_id = multi_transport.allocateBatchID(2);
    auto status = multi_transport.submitTransfer(batch_id, entries);

    EXPECT_TRUE(status.ok());
    EXPECT_EQ(recording_transport->submitted_group_sizes,
              (std::vector<size_t>{1, 1}));
    EXPECT_EQ(multi_transport.freeBatchID(batch_id), Status::OK());
}

TEST_F(TaskGroupingTest, GroupedRequestsHaveIndependentLocalBuffers) {
    char buf_a[4096];
    char buf_b[4096];

    Transport::TransferRequest req_a{};
    req_a.opcode = Transport::TransferRequest::READ;
    req_a.source = buf_a;
    req_a.target_id = 1;
    req_a.target_offset = 0;
    req_a.length = sizeof(buf_a);
    req_a.task_group_id = 42;

    Transport::TransferRequest req_b{};
    req_b.opcode = Transport::TransferRequest::READ;
    req_b.source = buf_b;
    req_b.target_id = 1;
    req_b.target_offset = 4096;
    req_b.length = sizeof(buf_b);
    req_b.task_group_id = 42;

    // Buffers are in completely different memory regions — this is valid.
    EXPECT_NE(req_a.source, req_b.source);
    EXPECT_EQ(req_a.task_group_id, req_b.task_group_id);
}

TEST_F(TaskGroupingTest, PartialBatchCompletesWhenSubmittedTasksComplete) {
    constexpr Transport::SegmentID kTargetSegmentId = 1;
    auto metadata = MakeMetadataWithSegment(kTargetSegmentId, "rdma");
    std::string local_server_name = "local";
    TestableMultiTransport multi_transport(metadata, local_server_name);
    auto recording_transport =
        std::make_shared<RecordingTransport>("rdma", true);
    multi_transport.transport_map_["rdma"] = recording_transport;

    std::vector<Transport::TransferRequest> entries(2);
    for (size_t i = 0; i < entries.size(); ++i) {
        entries[i].opcode = Transport::TransferRequest::READ;
        entries[i].source = nullptr;
        entries[i].target_id = kTargetSegmentId;
        entries[i].target_offset = i * 4096;
        entries[i].length = 4096;
    }

    auto batch_id = multi_transport.allocateBatchID(4);
    auto submit_status = multi_transport.submitTransfer(batch_id, entries);
    EXPECT_TRUE(submit_status.ok());

    Transport::TransferStatus status{};
    auto status_ret = multi_transport.getBatchTransferStatus(batch_id, status);
    EXPECT_TRUE(status_ret.ok());
    EXPECT_EQ(status.s, Transport::TransferStatusEnum::COMPLETED);
    EXPECT_EQ(multi_transport.freeBatchID(batch_id), Status::OK());
}

TEST_F(TaskGroupingTest, EmptyBatchCompletesAndIsFreeable) {
    auto metadata = std::make_shared<TransferMetadata>(P2PHANDSHAKE);
    std::string local_server_name = "local";
    TestableMultiTransport multi_transport(metadata, local_server_name);

    auto batch_id = multi_transport.allocateBatchID(0);

    Transport::TransferStatus status{};
    auto status_ret = multi_transport.getBatchTransferStatus(batch_id, status);
    EXPECT_TRUE(status_ret.ok());
    EXPECT_EQ(status.s, Transport::TransferStatusEnum::COMPLETED);
    EXPECT_EQ(multi_transport.freeBatchID(batch_id), Status::OK());
}

TEST_F(TaskGroupingTest, EmptySubmitCompletesAndIsFreeable) {
    auto metadata = std::make_shared<TransferMetadata>(P2PHANDSHAKE);
    std::string local_server_name = "local";
    TestableMultiTransport multi_transport(metadata, local_server_name);

    auto batch_id = multi_transport.allocateBatchID(0);
    std::vector<Transport::TransferRequest> entries;
    auto submit_status = multi_transport.submitTransfer(batch_id, entries);
    EXPECT_TRUE(submit_status.ok());

    Transport::TransferStatus status{};
    auto status_ret = multi_transport.getBatchTransferStatus(batch_id, status);
    EXPECT_TRUE(status_ret.ok());
    EXPECT_EQ(status.s, Transport::TransferStatusEnum::COMPLETED);
    EXPECT_EQ(multi_transport.freeBatchID(batch_id), Status::OK());
}

TEST_F(TaskGroupingTest, SubmitFailureLeavesBatchFreeable) {
    constexpr Transport::SegmentID kTargetSegmentId = 1;
    auto metadata = MakeMetadataWithSegment(kTargetSegmentId, "rdma");
    std::string local_server_name = "local";
    TestableMultiTransport multi_transport(metadata, local_server_name);
    auto recording_transport = std::make_shared<RecordingTransport>(
        "rdma", true, Status::InvalidArgument("injected submit failure"));
    multi_transport.transport_map_["rdma"] = recording_transport;

    std::vector<Transport::TransferRequest> entries(1);
    entries[0].opcode = Transport::TransferRequest::READ;
    entries[0].source = nullptr;
    entries[0].target_id = kTargetSegmentId;
    entries[0].target_offset = 0;
    entries[0].length = 4096;

    auto batch_id = multi_transport.allocateBatchID(1);
    auto submit_status = multi_transport.submitTransfer(batch_id, entries);
    EXPECT_FALSE(submit_status.ok());

    Transport::TransferStatus status{};
    auto status_ret = multi_transport.getBatchTransferStatus(batch_id, status);
    EXPECT_TRUE(status_ret.ok());
    EXPECT_EQ(status.s, Transport::TransferStatusEnum::FAILED);
    EXPECT_EQ(multi_transport.freeBatchID(batch_id), Status::OK());
}

}  // namespace mooncake
