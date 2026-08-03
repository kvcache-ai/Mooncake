// Copyright 2026 KVCache.AI
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

// Regression tests for RdmaTransport::submitTransferTask()'s
// "memory region not registered" (!found_device) error path. A slice that
// already succeeded device selection and was queued into the function-local
// slices_to_post accumulator remains owned by TransferTask::slice_list. It
// must not be deallocated twice, and it must reach a terminal FAILED state if
// a later slice causes submitTransferTask() to return an error.

#include <gtest/gtest.h>

#include <memory>
#include <string>

#include "config.h"
#include "rdma_test_peers.h"
#include "transfer_metadata.h"
#include "transport/rdma_transport/rdma_context.h"
#include "transport/rdma_transport/rdma_transport.h"

using namespace mooncake;

namespace {

using SegmentDesc = TransferMetadata::SegmentDesc;
using BufferDesc = TransferMetadata::BufferDesc;

class SubmitTransferTaskTest : public ::testing::Test {
   protected:
    static constexpr uint64_t kBufferAddr = 0x10000;

    std::shared_ptr<TransferMetadata> metadata_;
    std::unique_ptr<RdmaTransport> transport_;
    std::shared_ptr<RdmaContext> context_;
    uint64_t block_size_ = 0;

    void SetUp() override {
        block_size_ = globalConfig().slice_size;

        metadata_ = std::make_shared<TransferMetadata>(P2PHANDSHAKE);
        transport_ = std::make_unique<RdmaTransport>();
        RdmaTransportTestPeer::bindMetadata(*transport_, metadata_,
                                            "unit-test-server:1234");

        // construct() is never called: no real device is opened. active()
        // defaults to true, which is all submitTransferTask() checks.
        context_ = std::make_shared<RdmaContext>(*transport_, "mlx5_unit_test");
        RdmaTransportTestPeer::addContext(*transport_, context_);

        auto desc = std::make_shared<SegmentDesc>();
        desc->name = "unit-test-server:1234";
        desc->protocol = "rdma";
        BufferDesc buffer;
        buffer.name = "cpu:0";
        buffer.addr = kBufferAddr;
        buffer.length = block_size_;
        buffer.lkey = {1};
        buffer.rkey = {1};
        desc->buffers.push_back(buffer);
        ASSERT_EQ(
            desc->topology.parse(R"({"cpu:0": [["mlx5_unit_test"], []]})"), 0);
        metadata_->addLocalSegment(LOCAL_SEGMENT_ID, desc->name,
                                   std::move(desc));
    }

    void triggerError(Transport::TransferRequest &req,
                      Transport::TransferTask &task) {
        req.opcode = Transport::TransferRequest::WRITE;
        req.source = reinterpret_cast<void *>(kBufferAddr);
        req.length = 2 * block_size_;
        req.target_id = LOCAL_SEGMENT_ID;
        req.target_offset = 0;
        task.request = &req;

        // markFailed() needs a valid BatchDesc in event-driven builds. The
        // direct task is deliberately not inserted into that BatchDesc; the ID
        // is only used by Slice::check_batch_completion().
        task.batch_id = transport_->allocateBatchID(1);
        auto status = transport_->submitTransferTask({&task});
        EXPECT_FALSE(status.ok());
        EXPECT_TRUE(status.IsAddressNotRegistered());
        ASSERT_EQ(task.slice_list.size(), 2u);
        EXPECT_EQ(transport_->freeBatchID(task.batch_id), Status::OK());
    }
};

TEST_F(SubmitTransferTaskTest, NoDuplicateSlice) {
    Transport::Slice *original = nullptr;
    {
        Transport::TransferRequest req;
        Transport::TransferTask task;
        triggerError(req, task);
        original = task.slice_list[0];
    }

    Transport::TransferRequest req;
    Transport::TransferTask task;
    triggerError(req, task);

    EXPECT_EQ(task.slice_list[0], original)
        << "the cache should legitimately reuse the first released slice";
    EXPECT_NE(task.slice_list[0], task.slice_list[1])
        << "two independent slices must never share the same Slice object";
}

TEST_F(SubmitTransferTaskTest, PartialSubmitFailsBatch) {
    auto batch_id = transport_->allocateBatchID(1);
    Transport::TransferRequest request;
    request.opcode = Transport::TransferRequest::WRITE;
    request.source = reinterpret_cast<void *>(kBufferAddr);
    request.length = 2 * block_size_;
    request.target_id = LOCAL_SEGMENT_ID;
    request.target_offset = 0;

    auto submit_status = transport_->submitTransfer(batch_id, {request});
    ASSERT_FALSE(submit_status.ok());
    ASSERT_TRUE(submit_status.IsAddressNotRegistered());

    Transport::TransferStatus transfer_status;
    ASSERT_EQ(transport_->getTransferStatus(batch_id, 0, transfer_status),
              Status::OK());
    EXPECT_EQ(transfer_status.s, Transport::TransferStatusEnum::FAILED);
    EXPECT_EQ(transport_->freeBatchID(batch_id), Status::OK());
}

TEST_F(SubmitTransferTaskTest, PartialSubmitFailsAllTasks) {
    auto batch_id = transport_->allocateBatchID(2);
    Transport::TransferRequest failing_request;
    failing_request.opcode = Transport::TransferRequest::WRITE;
    failing_request.source = reinterpret_cast<void *>(kBufferAddr);
    failing_request.length = 2 * block_size_;
    failing_request.target_id = LOCAL_SEGMENT_ID;
    failing_request.target_offset = 0;

    Transport::TransferRequest unstarted_request;
    unstarted_request.opcode = Transport::TransferRequest::WRITE;
    unstarted_request.source = reinterpret_cast<void *>(kBufferAddr);
    unstarted_request.length = block_size_;
    unstarted_request.target_id = LOCAL_SEGMENT_ID;
    unstarted_request.target_offset = kBufferAddr;

    auto submit_status = transport_->submitTransfer(
        batch_id, {failing_request, unstarted_request});
    ASSERT_FALSE(submit_status.ok());
    ASSERT_TRUE(submit_status.IsAddressNotRegistered());

    std::vector<Transport::TransferStatus> transfer_status;
    ASSERT_EQ(transport_->getTransferStatus(batch_id, transfer_status),
              Status::OK());
    ASSERT_EQ(transfer_status.size(), 2u);
    EXPECT_EQ(transfer_status[0].s, Transport::TransferStatusEnum::FAILED);
    EXPECT_EQ(transfer_status[1].s, Transport::TransferStatusEnum::FAILED);
    EXPECT_EQ(transport_->freeBatchID(batch_id), Status::OK());
}

}  // namespace
