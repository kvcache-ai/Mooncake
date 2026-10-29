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
#include <chrono>
#include <memory>
#include <string>
#include <thread>
#include <type_traits>

#include "config.h"
#include "multi_transport.h"
#include "rdma_test_peers.h"
#include "transfer_metadata.h"
#include "transport/rdma_transport/endpoint_store.h"
#include "transport/rdma_transport/rdma_context.h"
#include "transport/rdma_transport/rdma_transport.h"
#include "transport/rdma_transport/worker_pool.h"

namespace mooncake {

class WorkerPoolTestPeer {
   public:
    // Match the existing worker/endpoint tests: join consumers before any
    // submission, then inspect the real queue without opening an RDMA device.
    static void stopWorkers(WorkerPool &pool) {
        pool.workers_running_.store(false);
        pool.cond_var_.notify_all();
        for (auto &thread : pool.worker_thread_) thread.join();
    }

    static std::vector<Transport::Slice *> takeQueuedSlices(WorkerPool &pool) {
        std::vector<Transport::Slice *> result;
        for (auto &queues : pool.worker_slice_queue_) {
            for (auto &entry : queues)
                result.insert(result.end(), entry.second.begin(),
                              entry.second.end());
            queues.clear();
        }
        return result;
    }
};

}  // namespace mooncake

using namespace mooncake;

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

TEST_F(SubmitTransferTaskTest,
       ChunkedSubmissionHandlesTailAndPreparationFailure) {
    struct RestoreConfig {
        size_t max_wr = globalConfig().max_wr;
        size_t num_qp = globalConfig().num_qp_per_ep;
        size_t fragment_limit = globalConfig().fragment_limit;
        ~RestoreConfig() {
            globalConfig().max_wr = max_wr;
            globalConfig().num_qp_per_ep = num_qp;
            globalConfig().fragment_limit = fragment_limit;
        }
    } restore_config;
    globalConfig().max_wr = 2;
    globalConfig().num_qp_per_ep = 2;  // Watermark = 4 slices.

    auto &context = *transport_->getContextList()[local_device_id_];
    RdmaContextTestPeer::setTransferWorkerCount(context, 1);
    RdmaContextTestPeer::bindEndpointStore(
        context, std::make_shared<SIEVEEndpointStore>(8));
    auto pool = std::make_shared<WorkerPool>(context);
    WorkerPoolTestPeer::stopWorkers(*pool);
    RdmaContextTestPeer::bindWorkerPool(context, pool);

    struct Case {
        size_t valid_requests;
        size_t published;
        bool fail_preparation;
    };
    const Case cases[] = {{3, 3, false}, {4, 4, false}, {9, 9, false},
                          {3, 0, true},  {4, 4, true},  {5, 4, true},
                          {9, 8, true},  {9, 8, true}};
    const auto original = metadata_->getSegmentDescByID(LOCAL_SEGMENT_ID);
    for (const auto &[count, published, fail] : cases) {
        SCOPED_TRACE(::testing::Message()
                     << "valid=" << count << " fail=" << fail);
        auto desc = std::make_shared<SegmentDesc>(*original);
        for (const auto &hca : desc->topology.getHcaList()) {
            desc->devices.emplace_back();
            desc->devices.back().name = hca;
        }
        desc->buffers[0].length = count * block_size_;
        desc->rebuildBufferRangeIndex();
        metadata_->addLocalSegment(LOCAL_SEGMENT_ID, desc->name,
                                   std::move(desc));

        // An out-of-MR final request stops preparation before the tail flush,
        // making the queued prefix evidence of publication during preparation.
        const size_t request_count = count + (fail ? 1 : 0);
        std::vector<Transport::TransferRequest> requests(request_count);
        for (size_t i = 0; i < request_count; ++i) {
            auto &request = requests[i];
            request.opcode = Transport::TransferRequest::WRITE;
            request.source =
                reinterpret_cast<void *>(kBufferAddr + i * block_size_);
            request.length = block_size_;
            request.target_id = LOCAL_SEGMENT_ID;
            request.target_offset = kBufferAddr + i * block_size_;
        }
        auto batch_id = transport_->allocateBatchID(request_count);
        auto status = transport_->submitTransfer(batch_id, requests);
        if (fail) {
            ASSERT_TRUE(status.IsAddressNotRegistered());
        } else {
            ASSERT_EQ(status, Status::OK());
        }

        auto queued = WorkerPoolTestPeer::takeQueuedSlices(*pool);
        ASSERT_EQ(queued.size(), published);
        auto &batch = Transport::toBatchDesc(batch_id);
        auto &tasks = batch.task_list;
        ASSERT_EQ(tasks.size(), request_count);
        EXPECT_EQ(batch.finished_task_count.load(), request_count - published);
        size_t cached = 0;
        for (size_t i = 0; i < tasks.size(); ++i) {
            ASSERT_EQ(tasks[i].slice_list.size(), 1u);
            EXPECT_NE(tasks[i].completion_state &
                          Transport::Slice::kCompletionSealBit,
                      0u);
            auto *slice = tasks[i].slice_list.front();
            if (i < published) {
                EXPECT_EQ(queued[i], slice);
                ASSERT_EQ(slice->status, Transport::Slice::PENDING);
                EXPECT_FALSE(tasks[i].completion_published);
                cached += slice->from_cache;
                slice->markSuccess();
                EXPECT_EQ(tasks[i].success_slice_count, 1u);
            } else {
                EXPECT_EQ(slice->status, Transport::Slice::FAILED);
            }
            EXPECT_EQ(tasks[i].failed_slice_count, i < published ? 0u : 1u);
            EXPECT_TRUE(tasks[i].completion_published);
            Transport::TransferStatus transfer_status;
            ASSERT_EQ(
                transport_->getTransferStatus(batch_id, i, transfer_status),
                Status::OK());
            EXPECT_EQ(transfer_status.s,
                      i < published ? Transport::COMPLETED : Transport::FAILED);
        }
        EXPECT_EQ(batch.finished_task_count.load(), request_count);
        EXPECT_EQ(batch.has_failure.load(), fail);
#ifdef USE_EVENT_DRIVEN_COMPLETION
        EXPECT_TRUE(batch.is_finished.load());
#endif
        // Earlier iterations return their slices to the cache.
        if (fail && published) {
            EXPECT_GT(cached, 0u);
        }
        EXPECT_EQ(transport_->freeBatchID(batch_id), Status::OK());
    }

#ifdef USE_EVENT_DRIVEN_COMPLETION
    // One task spans a full chunk plus a tail. Invalid remote addresses make
    // the real WorkerPool finish the first chunk before the tail is built.
    globalConfig().fragment_limit = 0;
    auto desc = std::make_shared<SegmentDesc>(*original);
    desc->buffers[0].length = 5 * block_size_;
    desc->rebuildBufferRangeIndex();
    metadata_->addLocalSegment(LOCAL_SEGMENT_ID, desc->name, std::move(desc));
    std::vector<Transport::TransferRequest> requests(1);
    requests[0].opcode = Transport::TransferRequest::WRITE;
    requests[0].source = reinterpret_cast<void *>(kBufferAddr);
    requests[0].length = 5 * block_size_;
    requests[0].target_id = LOCAL_SEGMENT_ID;
    requests[0].target_offset = kBufferAddr + requests[0].length;
    auto batch_id = transport_->allocateBatchID(1);
    auto &batch = Transport::toBatchDesc(batch_id);
    std::unique_lock<std::mutex> lock(batch.completion_mutex);
    Status submit_status;
    // A fresh submit thread also isolates WorkerPool's failed-target cache.
    std::thread submitter([&] {
        submit_status = transport_->submitTransfer(batch_id, requests);
    });
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (batch.finished_task_count.load(std::memory_order_acquire) == 0 &&
           std::chrono::steady_clock::now() < deadline)
        std::this_thread::yield();
    const bool published = batch.finished_task_count.load() == 1;
    // Completion increments the counter before taking completion_mutex. Keep
    // that callback blocked so construction cannot advance past publication.
    const auto slices_at_publication =
        published
            ? __atomic_load_n(&batch.task_list[0].slice_count, __ATOMIC_ACQUIRE)
            : 0;
    lock.unlock();
    submitter.join();

    ASSERT_TRUE(published);
    EXPECT_EQ(slices_at_publication, 5u);  // Without unseal this is 4.
    EXPECT_EQ(submit_status, Status::OK());
    const auto &task = batch.task_list[0];
    EXPECT_EQ(task.slice_count, 5u);
    EXPECT_EQ(task.failed_slice_count, 5u);
    EXPECT_EQ(task.completion_state, Transport::Slice::kCompletionSealBit | 5u);
    EXPECT_TRUE(task.completion_published);
    EXPECT_TRUE(task.is_finished);
    EXPECT_EQ(batch.finished_task_count.load(), 1u);
    EXPECT_TRUE(batch.is_finished.load());
    EXPECT_TRUE(batch.has_failure.load());
    Transport::TransferStatus transfer_status;
    ASSERT_EQ(transport_->getTransferStatus(batch_id, 0, transfer_status),
              Status::OK());
    EXPECT_EQ(transfer_status.s, Transport::FAILED);
    EXPECT_EQ(transport_->freeBatchID(batch_id), Status::OK());
#endif
}

}  // namespace
