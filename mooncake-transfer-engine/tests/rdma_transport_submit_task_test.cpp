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

#include <algorithm>
#include <functional>
#include <memory>
#include <numeric>
#include <string>
#include <type_traits>

#include "config.h"
#include "multi_transport.h"
#include "rdma_test_peers.h"
#include "transfer_metadata.h"
#include "transport/rdma_transport/rdma_context.h"
#include "transport/rdma_transport/rdma_transport.h"

using namespace mooncake;

#ifdef __linux__
namespace {
std::function<int(const std::vector<Transport::Slice *> &)> submit_probe;
}

extern "C" int
realSubmitPostSend(RdmaContext *, const std::vector<Transport::Slice *> &) asm(
    "__real__ZN8mooncake11RdmaContext14submitPostSendERKSt6vectorIPNS_"
    "9Transport5SliceESaIS4_EE");
extern "C" int
wrapSubmitPostSend(RdmaContext *, const std::vector<Transport::Slice *> &) asm(
    "__wrap__ZN8mooncake11RdmaContext14submitPostSendERKSt6vectorIPNS_"
    "9Transport5SliceESaIS4_EE");
extern "C" int wrapSubmitPostSend(
    RdmaContext *context, const std::vector<Transport::Slice *> &slices) {
    return submit_probe ? submit_probe(slices)
                        : realSubmitPostSend(context, slices);
}
#endif

namespace {

using SegmentDesc = TransferMetadata::SegmentDesc;
using BufferDesc = TransferMetadata::BufferDesc;

class SubmitTransferTaskTest : public ::testing::Test {
   protected:
    static constexpr uint64_t kBufferAddr = 0x10000;

    std::shared_ptr<TransferMetadata> metadata_;
    std::unique_ptr<RdmaTransport> transport_;
    uint64_t block_size_ = 0;
    int local_device_id_ = -1;
    int global_device_id_ = -1;

    uint32_t lkeyForDevice(int device_id) const {
        return static_cast<uint32_t>(100 + device_id);
    }

    void SetUp() override {
        block_size_ = globalConfig().slice_size;

        metadata_ = std::make_shared<TransferMetadata>(P2PHANDSHAKE);
        transport_ = std::make_unique<RdmaTransport>();
        RdmaTransportTestPeer::bindMetadata(*transport_, metadata_,
                                            "unit-test-server:1234");

        auto desc = std::make_shared<SegmentDesc>();
        desc->name = "unit-test-server:1234";
        desc->protocol = "rdma";
        ASSERT_EQ(desc->topology.parse(R"({"cpu:0": [["mlx5_local"], []],)"
                                       R"("cpu:1": [["mlx5_global"], []]})"),
                  0);

        const auto &hca_list = desc->topology.getHcaList();
        auto device_id = [&hca_list](const std::string &name) {
            const auto it = std::find(hca_list.begin(), hca_list.end(), name);
            return it == hca_list.end()
                       ? -1
                       : static_cast<int>(std::distance(hca_list.begin(), it));
        };
        local_device_id_ = device_id("mlx5_local");
        global_device_id_ = device_id("mlx5_global");
        ASSERT_GE(local_device_id_, 0);
        ASSERT_GE(global_device_id_, 0);

        // construct() is never called: no real device is opened. active()
        // defaults to true, which is all submitTransferTask() checks. Contexts
        // and lkeys must retain the topology's global HCA index ordering.
        for (const auto &hca : hca_list) {
            auto context = std::make_shared<RdmaContext>(*transport_, hca);
            RdmaTransportTestPeer::addContext(*transport_, std::move(context));
        }

        BufferDesc buffer;
        buffer.name = "cpu:0";
        buffer.addr = kBufferAddr;
        buffer.length = block_size_;
        for (size_t i = 0; i < hca_list.size(); ++i) {
            buffer.lkey.push_back(lkeyForDevice(static_cast<int>(i)));
            buffer.rkey.push_back(lkeyForDevice(static_cast<int>(i)));
        }
        desc->buffers.push_back(std::move(buffer));
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

    uint32_t selectedLkey(Transport::TransferRequest &req) {
        Transport::TransferTask task;
        triggerError(req, task);
        return task.slice_list.front()->rdma.source_lkey;
    }
};

TEST_F(SubmitTransferTaskTest, NicHintUsesExistingTailPadding) {
    // Mirror the current request layout without nic_hint. The upstream
    // intent_type field grew the baseline from 64 to 72 bytes; nic_hint must
    // fit in its existing tail padding rather than grow every request again.
    struct RequestWithoutNicHint {
        Transport::TransferRequest::OpCode opcode;
        void *source;
        Transport::SegmentID target_id;
        uint64_t target_offset;
        size_t length;
        int advise_retry_cnt;
        int transport_hint;
        int intent_type;
        uint64_t task_group_id;
        int priority;
    };

    EXPECT_EQ(sizeof(Transport::TransferRequest),
              sizeof(RequestWithoutNicHint));
    EXPECT_TRUE(std::is_trivially_copyable_v<Transport::TransferRequest>);
}

TEST_F(SubmitTransferTaskTest, EmptyNicHintPreservesDefaultSelection) {
    Transport::TransferRequest default_request;
    Transport::TransferRequest explicit_empty_request;
    explicit_empty_request.nic_hint = -1;

    EXPECT_EQ(selectedLkey(default_request), lkeyForDevice(local_device_id_));
    EXPECT_EQ(selectedLkey(explicit_empty_request),
              lkeyForDevice(local_device_id_));
}

TEST_F(SubmitTransferTaskTest, NicHintUsesWildcardWhenAbsentFromLocation) {
    Transport::TransferRequest request;
    request.nic_hint = global_device_id_;

    EXPECT_EQ(selectedLkey(request), lkeyForDevice(global_device_id_));
}

TEST_F(SubmitTransferTaskTest, SubmitRetryDropsNicHint) {
    Transport::TransferRequest request;
    request.nic_hint = global_device_id_;
    request.advise_retry_cnt = 1;

    EXPECT_EQ(selectedLkey(request), lkeyForDevice(local_device_id_));
}

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

TEST_F(SubmitTransferTaskTest, GroupedRequestsReportFailurePerRequest) {
    std::vector<Transport::TransferRequest> requests(2);
    for (auto &request : requests) {
        request.opcode = Transport::TransferRequest::WRITE;
        request.source = reinterpret_cast<void *>(kBufferAddr);
        request.length = block_size_;
        request.target_id = LOCAL_SEGMENT_ID;
        request.target_offset = 0;
    }
    requests[1].length = 2 * block_size_;
    std::string server_name = "unit-test-server:1234";
    MultiTransport multi_transport(metadata_, server_name);
    auto batch_id = multi_transport.allocateBatchID(1);
    auto &task = reinterpret_cast<Transport::BatchDesc *>(batch_id)
                     ->task_list.emplace_back();
    task.batch_id = batch_id;
    task.request = requests.data();
    task.request_count = requests.size();
    EXPECT_TRUE(
        transport_->submitTransferTask({&task}).IsAddressNotRegistered());
    Transport::TransferStatus status;
    ASSERT_EQ(transport_->getTransferStatus(batch_id, 0, status), Status::OK());
    EXPECT_EQ(status.s, Transport::TransferStatusEnum::FAILED);
    std::vector<Transport::TransferStatusEnum> request_statuses;
    ASSERT_EQ(multi_transport.getScatterRequestStatuses(batch_id, 0,
                                                        request_statuses),
              Status::OK());
    EXPECT_EQ(request_statuses, std::vector(2, status.s));
    EXPECT_EQ(multi_transport.freeBatchID(batch_id), Status::OK());
}

#ifdef __linux__
class ChunkedSubmitTest : public SubmitTransferTaskTest {
   protected:
    static constexpr size_t kWatermark = 4;
    const size_t saved_max_wr_ = globalConfig().max_wr;
    const size_t saved_num_qp_ = globalConfig().num_qp_per_ep;
    std::vector<Transport::TransferTask *> tasks_;
    std::vector<Transport::Slice *> submitted_;
    std::vector<size_t> chunks_, constructed_;
    size_t cached_ = 0;

    void SetUp() override {
        SubmitTransferTaskTest::SetUp();
        globalConfig().max_wr = 2;
        globalConfig().num_qp_per_ep = 2;
        setBufferLength((2 * kWatermark + 3) * block_size_);
        // Observe the real transport handoff without opening an RDMA device.
        submit_probe = [&](const std::vector<Transport::Slice *> &slices) {
            EXPECT_FALSE(slices.empty());
            chunks_.push_back(slices.size());
            size_t constructed = 0;
            for (auto *task : tasks_) constructed += task->slice_list.size();
            constructed_.push_back(constructed);
            for (auto *slice : slices) {
                auto *task = slice->task.load();
                EXPECT_EQ(task->slice_count, task->slice_list.size());
                EXPECT_EQ(slice->rdma.source_lkey,
                          lkeyForDevice(local_device_id_));
                submitted_.push_back(slice);
                cached_ += slice->from_cache;
                slice->markSuccess();
            }
            return 0;
        };
    }

    void TearDown() override {
        submit_probe = {};
        globalConfig().max_wr = saved_max_wr_;
        globalConfig().num_qp_per_ep = saved_num_qp_;
    }

    void setBufferLength(size_t length) {
        auto desc = metadata_->getSegmentDescByID(LOCAL_SEGMENT_ID);
        desc->buffers[0].length = length;
        desc->rebuildBufferRangeIndex();
    }

    Transport::TransferRequest request(size_t length, uint64_t offset = 0) {
        Transport::TransferRequest req;
        req.opcode = Transport::TransferRequest::WRITE;
        req.source = reinterpret_cast<void *>(kBufferAddr + offset);
        req.length = length;
        req.target_id = LOCAL_SEGMENT_ID;
        req.target_offset = kBufferAddr + 2 * offset;
        return req;
    }

    void bindTasks(const std::vector<Transport::TransferRequest> &requests,
                   std::vector<Transport::TransferTask> &tasks) {
        tasks_.clear();
        submitted_.clear();
        chunks_.clear();
        constructed_.clear();
        cached_ = 0;
        for (size_t i = 0; i < tasks.size(); ++i) {
            tasks[i].request = &requests[i];
            Transport::Slice::unsealTaskSubmission(&tasks[i]);
            tasks_.push_back(&tasks[i]);
        }
    }
};

TEST_F(ChunkedSubmitTest,
       SmallRequestsPublishBoundedChunksIncludingCachedSlices) {
    {
        auto req = request(16384);
        Transport::TransferTask task;
        task.request = &req;
        ASSERT_EQ(transport_->submitTransferTask({&task}), Status::OK());
    }
    for (const auto &expected : {std::vector<size_t>{kWatermark - 1},
                                 {kWatermark},
                                 {kWatermark, 1},
                                 {kWatermark, kWatermark, 1}}) {
        const size_t count =
            std::accumulate(expected.begin(), expected.end(), size_t{0});
        SCOPED_TRACE(count);
        std::vector<Transport::TransferRequest> requests;
        for (size_t i = 0; i < count; ++i)
            requests.push_back(request(16384, i * 16384));
        std::vector<Transport::TransferTask> tasks(count);
        bindTasks(requests, tasks);
        ASSERT_EQ(transport_->submitTransferTask(tasks_), Status::OK());
        EXPECT_EQ(chunks_, expected);
        std::vector<size_t> constructed(expected.size());
        std::partial_sum(expected.begin(), expected.end(), constructed.begin());
        EXPECT_EQ(constructed_, constructed);
        EXPECT_GT(cached_, 0u);
        ASSERT_EQ(submitted_.size(), count);
        for (size_t i = 0; i < count; ++i) {
            EXPECT_EQ(submitted_[i], tasks[i].slice_list.front());
            EXPECT_EQ(submitted_[i]->source_addr, requests[i].source);
            EXPECT_EQ(submitted_[i]->rdma.dest_addr, requests[i].target_offset);
            EXPECT_EQ(submitted_[i]->length, requests[i].length);
            EXPECT_EQ(tasks[i].success_slice_count, 1u);
            EXPECT_EQ(tasks[i].total_bytes, requests[i].length);
        }
    }
}

TEST_F(ChunkedSubmitTest, LargeRequestPublishesChunksAndTailBeforeReturn) {
    const size_t count = 2 * kWatermark + 1;
    std::vector<Transport::TransferRequest> requests{
        request(count * block_size_)};
    std::vector<Transport::TransferTask> tasks(1);
    bindTasks(requests, tasks);
    ASSERT_EQ(transport_->submitTransferTask(tasks_), Status::OK());
    EXPECT_EQ(chunks_, (std::vector<size_t>{kWatermark, kWatermark, 1}));
    EXPECT_EQ(constructed_,
              (std::vector<size_t>{kWatermark, 2 * kWatermark, count}));
    ASSERT_EQ(submitted_.size(), count);
    for (size_t i = 0; i < count; ++i) {
        EXPECT_EQ(submitted_[i], tasks[0].slice_list[i]);
        EXPECT_EQ(reinterpret_cast<uint64_t>(submitted_[i]->source_addr),
                  kBufferAddr + i * block_size_);
        EXPECT_EQ(submitted_[i]->rdma.dest_addr, kBufferAddr + i * block_size_);
        EXPECT_EQ(submitted_[i]->length, block_size_);
    }
    EXPECT_EQ(tasks[0].success_slice_count, count);
    EXPECT_EQ(tasks[0].total_bytes, requests[0].length);
    EXPECT_EQ(tasks[0].transferred_bytes, requests[0].length);
}

TEST_F(ChunkedSubmitTest,
       LaterPreparationFailureLeavesPublishedPrefixSuccessful) {
    setBufferLength((kWatermark + 1) * block_size_);
    std::vector<Transport::TransferRequest> requests{
        request((kWatermark + 2) * block_size_), request(block_size_)};
    std::vector<Transport::TransferTask> tasks(2);
    bindTasks(requests, tasks);
    EXPECT_TRUE(
        transport_->submitTransferTask(tasks_).IsAddressNotRegistered());
    EXPECT_EQ(chunks_, (std::vector<size_t>{kWatermark}));
    EXPECT_EQ(tasks[0].success_slice_count, kWatermark);
    EXPECT_EQ(tasks[0].failed_slice_count, 2u);
    ASSERT_EQ(tasks[0].slice_list.size(), kWatermark + 2);
    for (size_t i = 0; i < tasks[0].slice_list.size(); ++i)
        EXPECT_EQ(tasks[0].slice_list[i]->status,
                  i < kWatermark ? Transport::Slice::SUCCESS
                                 : Transport::Slice::FAILED);
    ASSERT_EQ(tasks[1].slice_list.size(), 1u);
    EXPECT_EQ(tasks[1].failed_slice_count, 1u);
    EXPECT_EQ(tasks[1].slice_list.front()->length, 0u);
}
#endif
}  // namespace
