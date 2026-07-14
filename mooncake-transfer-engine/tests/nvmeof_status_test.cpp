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

#include <algorithm>
#include <deque>
#include <memory>
#include <utility>
#include <vector>

#include "transport/nvmeof_transport/nvmeof_transport.h"

namespace mooncake {

class CUFileDescPoolTestPeer {
   public:
    static std::shared_ptr<CUFileDescPool> create(
        std::shared_ptr<CUFileBatchAPI> batch_api,
        size_t max_batch_size = 128) {
        return std::shared_ptr<CUFileDescPool>(
            new CUFileDescPool(max_batch_size, std::move(batch_api)));
    }

    static bool cachePolledEvent(std::vector<CUfileIOEvents_t>& events,
                                 const CUfileIOEvents_t& event) {
        return CUFileDescPool::cachePolledEvent(events, event);
    }
};

class NVMeoFTransportTestPeer {
   public:
    static std::unique_ptr<NVMeoFTransport> createWithoutDriver(
        std::shared_ptr<CUFileDescPool> desc_pool =
            std::make_shared<CUFileDescPool>()) {
        return std::unique_ptr<NVMeoFTransport>(
            new NVMeoFTransport(std::move(desc_pool)));
    }

    static Transport::TransferStatus aggregate(
        const std::vector<Transport::TransferStatus>& statuses,
        bool& is_finished) {
        return NVMeoFTransport::aggregateTransferStatus(statuses, is_finished);
    }

    static int addBatchSlice(NVMeoFTransport& transport, void* source_addr,
                             uint64_t file_offset, uint64_t slice_len,
                             uint64_t desc_id,
                             Transport::TransferRequest::OpCode op,
                             CUfileHandle_t file_handle) {
        return transport.addSliceToCUFileBatch(
            source_addr, file_offset, slice_len, desc_id, op, file_handle);
    }
};

class FakeCUFileBatchAPI : public CUFileBatchAPI {
   public:
    CUfileError_t setUp(CUfileBatchHandle_t* batch_handle,
                        unsigned nr) override {
        ++setup_calls;
        *batch_handle = reinterpret_cast<void*>(next_handle++);
        EXPECT_GT(nr, 0);
        return success();
    }

    CUfileError_t submit(CUfileBatchHandle_t batch_handle, unsigned nr,
                         CUfileIOParams_t* params, unsigned flags) override {
        submitted_handle = batch_handle;
        submitted_params = params;
        EXPECT_EQ(flags, 0);
        submitted_counts.push_back(nr);
        submitted_first_cookies.push_back(params[0].cookie);
        return success();
    }

    CUfileError_t getStatus(CUfileBatchHandle_t batch_handle, unsigned min_nr,
                            unsigned* nr, CUfileIOEvents_t* events,
                            struct timespec* timeout) override {
        ++get_status_calls;
        EXPECT_EQ(batch_handle, submitted_handle);
        EXPECT_EQ(min_nr, 0);
        EXPECT_EQ(timeout, nullptr);
        if (get_status_result.err != CU_FILE_SUCCESS) {
            *nr = 0;
            return get_status_result;
        }

        std::vector<CUfileIOEvents_t> completions;
        if (!status_results.empty()) {
            completions = std::move(status_results.front());
            status_results.pop_front();
        }
        const auto count =
            std::min<size_t>(completions.size(), static_cast<size_t>(*nr));
        EXPECT_EQ(count, completions.size());
        std::copy_n(completions.begin(), count, events);
        *nr = count;
        return get_status_result;
    }

    CUfileError_t cancel(CUfileBatchHandle_t batch_handle) override {
        ++cancel_calls;
        EXPECT_EQ(batch_handle, submitted_handle);
        return cancel_result;
    }

    void destroy(CUfileBatchHandle_t batch_handle) override {
        destroyed_handles.push_back(batch_handle);
    }

    static CUfileError_t success() {
        return {.err = CU_FILE_SUCCESS, .cu_err = CUDA_SUCCESS};
    }

    uintptr_t next_handle = 1;
    int setup_calls = 0;
    int get_status_calls = 0;
    int cancel_calls = 0;
    CUfileBatchHandle_t submitted_handle = nullptr;
    CUfileIOParams_t* submitted_params = nullptr;
    CUfileError_t cancel_result = success();
    CUfileError_t get_status_result = success();
    std::vector<unsigned> submitted_counts;
    std::vector<void*> submitted_first_cookies;
    std::vector<CUfileBatchHandle_t> destroyed_handles;
    std::deque<std::vector<CUfileIOEvents_t>> status_results;
};

CUfileIOEvents_t completion(size_t slice_id, CUfileStatus_t status,
                            size_t ret = 0) {
    return {.cookie = reinterpret_cast<void*>(slice_id + 1),
            .status = status,
            .ret = ret};
}

TEST(NVMeoFStatusTest, RejectsUnsupportedMultiTransportSubmission) {
    auto transport = NVMeoFTransportTestPeer::createWithoutDriver();
    Transport::TransferTask task;

    auto status = transport->submitTransferTask({&task});

    EXPECT_TRUE(status.IsNotImplemented());
    EXPECT_EQ(status.message(),
              "NVMeoFTransport does not support MultiTransport batches");
    EXPECT_TRUE(task.is_finished);
}

TEST(NVMeoFStatusTest, RejectsBatchThatExceedsCUFileCapacity) {
    auto batch_api = std::make_shared<FakeCUFileBatchAPI>();
    auto desc_pool = CUFileDescPoolTestPeer::create(batch_api);
    auto transport = NVMeoFTransportTestPeer::createWithoutDriver(desc_pool);

    auto batch_id = transport->allocateBatchID(129);

    EXPECT_EQ(batch_id, static_cast<Transport::BatchID>(ERR_MEMORY));
    EXPECT_EQ(batch_api->setup_calls, 0);
}

TEST(NVMeoFStatusTest, WaitsForEverySliceBeforeReportingTerminalFailure) {
    bool is_finished = true;
    auto status = NVMeoFTransportTestPeer::aggregate(
        {{Transport::FAILED, 0}, {Transport::PENDING, 0}}, is_finished);

    EXPECT_EQ(status.s, Transport::PENDING);
    EXPECT_FALSE(is_finished);
}

TEST(NVMeoFStatusTest, AggregatesCompletedBytes) {
    bool is_finished = false;
    auto status = NVMeoFTransportTestPeer::aggregate(
        {{Transport::COMPLETED, 1024}, {Transport::COMPLETED, 2048}},
        is_finished);

    EXPECT_EQ(status.s, Transport::COMPLETED);
    EXPECT_EQ(status.transferred_bytes, 3072);
    EXPECT_TRUE(is_finished);
}

TEST(NVMeoFStatusTest, UsesDeterministicTerminalFailurePrecedence) {
    bool first_finished = false;
    auto first = NVMeoFTransportTestPeer::aggregate({{Transport::INVALID, 0},
                                                     {Transport::FAILED, 0},
                                                     {Transport::TIMEOUT, 0}},
                                                    first_finished);

    bool second_finished = false;
    auto second = NVMeoFTransportTestPeer::aggregate({{Transport::TIMEOUT, 0},
                                                      {Transport::FAILED, 0},
                                                      {Transport::INVALID, 0}},
                                                     second_finished);

    EXPECT_EQ(first.s, Transport::FAILED);
    EXPECT_EQ(second.s, Transport::FAILED);
    EXPECT_TRUE(first_finished);
    EXPECT_TRUE(second_finished);
}

TEST(NVMeoFStatusTest, CorrelatesPartialCompletionsByCookie) {
    std::vector<CUfileIOEvents_t> cached = {
        {.cookie = reinterpret_cast<void*>(1),
         .status = CUFILE_WAITING,
         .ret = 0},
        {.cookie = reinterpret_cast<void*>(2),
         .status = CUFILE_WAITING,
         .ret = 0}};
    CUfileIOEvents_t second = {.cookie = reinterpret_cast<void*>(2),
                               .status = CUFILE_COMPLETE,
                               .ret = 4096};

    ASSERT_TRUE(CUFileDescPoolTestPeer::cachePolledEvent(cached, second));
    EXPECT_EQ(cached[0].status, CUFILE_WAITING);
    EXPECT_EQ(cached[1].status, CUFILE_COMPLETE);
    EXPECT_EQ(cached[1].ret, 4096);

    CUfileIOEvents_t stale = {.cookie = reinterpret_cast<void*>(2),
                              .status = CUFILE_PENDING,
                              .ret = 0};
    ASSERT_TRUE(CUFileDescPoolTestPeer::cachePolledEvent(cached, stale));
    EXPECT_EQ(cached[1].status, CUFILE_COMPLETE);
    EXPECT_EQ(cached[1].ret, 4096);
}

TEST(NVMeoFStatusTest, RejectsInvalidDescriptorOperations) {
    auto batch_api = std::make_shared<FakeCUFileBatchAPI>();
    auto desc_pool = CUFileDescPoolTestPeer::create(batch_api);
    CUfileIOParams_t params{};
    CUFileBatchSnapshot snapshot;

    EXPECT_EQ(desc_pool->pushParams(-1, params), -1);
    EXPECT_EQ(desc_pool->submitBatch(-1), -1);
    EXPECT_EQ(desc_pool->discardUnsubmittedParams(-1), -1);
    EXPECT_FALSE(desc_pool->isAcceptingSubmissions(-1));
    EXPECT_EQ(desc_pool->pollBatch(-1, snapshot), -1);
    EXPECT_EQ(desc_pool->getSliceNum(-1), -1);
    EXPECT_EQ(desc_pool->freeCUfileDesc(-1), -1);
    EXPECT_EQ(desc_pool->getDesc(-1), nullptr);
}

TEST(NVMeoFStatusTest, DestroysAllocatedHandleWithPool) {
    auto batch_api = std::make_shared<FakeCUFileBatchAPI>();
    {
        auto desc_pool = CUFileDescPoolTestPeer::create(batch_api);
        ASSERT_GE(desc_pool->allocCUfileDesc(1), 0);
    }

    EXPECT_EQ(batch_api->destroyed_handles,
              (std::vector<CUfileBatchHandle_t>{reinterpret_cast<void*>(1)}));
}

TEST(NVMeoFStatusTest, DefersCrossTaskFailureUntilDescriptorIsDrained) {
    auto batch_api = std::make_shared<FakeCUFileBatchAPI>();
    auto desc_pool = CUFileDescPoolTestPeer::create(batch_api);
    auto transport = NVMeoFTransportTestPeer::createWithoutDriver(desc_pool);

    auto batch_id = transport->allocateBatchID(2);
    auto& batch_desc = Transport::toBatchDesc(batch_id);
    auto& nvmeof_desc = *static_cast<NVMeoFBatchDesc*>(batch_desc.context);
    batch_desc.task_list.resize(2);
    nvmeof_desc.task_to_slices = {{0, 1}, {1, 1}};
    nvmeof_desc.transfer_status = {
        {.s = Transport::PENDING, .transferred_bytes = 0},
        {.s = Transport::PENDING, .transferred_bytes = 0}};

    int caller_buffers[2] = {};
    for (size_t i = 0; i < 2; ++i) {
        CUfileIOParams_t params{};
        params.u.batch.devPtr_base = &caller_buffers[i];
        ASSERT_EQ(desc_pool->pushParams(nvmeof_desc.desc_idx_, params), 0);
        batch_desc.task_list[i].slice_count = 1;
    }
    ASSERT_EQ(desc_pool->submitBatch(nvmeof_desc.desc_idx_), 0);

    batch_api->status_results.push_back({completion(0, CUFILE_FAILED)});
    Transport::TransferStatus status{};
    ASSERT_TRUE(transport->getTransferStatus(batch_id, 1, status).ok());

    EXPECT_EQ(batch_api->cancel_calls, 1);
    EXPECT_TRUE(status.s == Transport::WAITING ||
                status.s == Transport::PENDING);
    EXPECT_FALSE(batch_desc.task_list[0].is_finished);
    EXPECT_FALSE(batch_desc.task_list[1].is_finished);

    ASSERT_TRUE(transport->getTransferStatus(batch_id, 1, status).ok());
    EXPECT_EQ(batch_api->cancel_calls, 1);
    EXPECT_TRUE(status.s == Transport::WAITING ||
                status.s == Transport::PENDING);
    EXPECT_TRUE(transport->freeBatchID(batch_id).IsBatchBusy());
    ASSERT_NE(desc_pool->getDesc(nvmeof_desc.desc_idx_), nullptr);
    EXPECT_TRUE(batch_api->destroyed_handles.empty());
    EXPECT_EQ(batch_api->submitted_params[1].u.batch.devPtr_base,
              &caller_buffers[1]);

    batch_api->status_results.push_back({completion(1, CUFILE_CANCELED)});
    ASSERT_TRUE(transport->getTransferStatus(batch_id, 1, status).ok());

    EXPECT_EQ(status.s, Transport::FAILED);
    EXPECT_TRUE(batch_desc.task_list[0].is_finished);
    EXPECT_TRUE(batch_desc.task_list[1].is_finished);

    const int poll_count = batch_api->get_status_calls;
    Transport::TransferStatus initiating_status{};
    ASSERT_TRUE(
        transport->getTransferStatus(batch_id, 0, initiating_status).ok());
    EXPECT_EQ(initiating_status.s, Transport::FAILED);
    EXPECT_EQ(batch_api->get_status_calls, poll_count);

    auto free_status = transport->freeBatchID(batch_id);
    EXPECT_TRUE(free_status.ok());
    EXPECT_EQ(batch_api->destroyed_handles,
              (std::vector<CUfileBatchHandle_t>{batch_api->submitted_handle}));
    if (!free_status.ok()) {
        batch_desc.task_list[0].is_finished = true;
        batch_desc.task_list[1].is_finished = true;
        EXPECT_TRUE(transport->freeBatchID(batch_id).ok());
    }
}

TEST(NVMeoFStatusTest, FinishedTaskPollStillDrivesDescriptorFailureCleanup) {
    auto batch_api = std::make_shared<FakeCUFileBatchAPI>();
    auto desc_pool = CUFileDescPoolTestPeer::create(batch_api);
    auto transport = NVMeoFTransportTestPeer::createWithoutDriver(desc_pool);

    auto batch_id = transport->allocateBatchID(2);
    auto& batch_desc = Transport::toBatchDesc(batch_id);
    auto& nvmeof_desc = *static_cast<NVMeoFBatchDesc*>(batch_desc.context);
    batch_desc.task_list.resize(2);
    nvmeof_desc.task_to_slices = {{0, 1}, {1, 2}};
    nvmeof_desc.transfer_status = {
        {.s = Transport::PENDING, .transferred_bytes = 0},
        {.s = Transport::PENDING, .transferred_bytes = 0}};

    CUfileIOParams_t params{};
    ASSERT_EQ(desc_pool->pushParams(nvmeof_desc.desc_idx_, params), 0);
    ASSERT_EQ(desc_pool->pushParams(nvmeof_desc.desc_idx_, params), 0);
    ASSERT_EQ(desc_pool->pushParams(nvmeof_desc.desc_idx_, params), 0);
    ASSERT_EQ(desc_pool->submitBatch(nvmeof_desc.desc_idx_), 0);

    batch_api->status_results.push_back({completion(0, CUFILE_COMPLETE, 1024)});
    Transport::TransferStatus status{};
    ASSERT_TRUE(transport->getTransferStatus(batch_id, 0, status).ok());
    ASSERT_EQ(status.s, Transport::COMPLETED);
    ASSERT_TRUE(batch_desc.task_list[0].is_finished);
    ASSERT_FALSE(batch_desc.task_list[1].is_finished);

    batch_api->status_results.push_back({completion(1, CUFILE_FAILED)});
    ASSERT_TRUE(transport->getTransferStatus(batch_id, 0, status).ok());

    EXPECT_EQ(status.s, Transport::COMPLETED);
    EXPECT_EQ(status.transferred_bytes, 1024);
    EXPECT_EQ(batch_api->cancel_calls, 1);
    EXPECT_FALSE(batch_desc.task_list[1].is_finished);

    batch_api->status_results.push_back({completion(2, CUFILE_CANCELED)});
    ASSERT_TRUE(transport->getTransferStatus(batch_id, 0, status).ok());
    EXPECT_EQ(status.s, Transport::COMPLETED);
    EXPECT_TRUE(batch_desc.task_list[1].is_finished);

    EXPECT_TRUE(transport->freeBatchID(batch_id).ok());
}

TEST(NVMeoFStatusTest, PreservesSpontaneousCancellationStatus) {
    auto batch_api = std::make_shared<FakeCUFileBatchAPI>();
    auto desc_pool = CUFileDescPoolTestPeer::create(batch_api);
    auto transport = NVMeoFTransportTestPeer::createWithoutDriver(desc_pool);

    auto batch_id = transport->allocateBatchID(1);
    auto& batch_desc = Transport::toBatchDesc(batch_id);
    auto& nvmeof_desc = *static_cast<NVMeoFBatchDesc*>(batch_desc.context);
    batch_desc.task_list.resize(1);
    nvmeof_desc.task_to_slices = {{0, 1}};
    nvmeof_desc.transfer_status = {
        {.s = Transport::PENDING, .transferred_bytes = 0}};

    CUfileIOParams_t params{};
    ASSERT_EQ(desc_pool->pushParams(nvmeof_desc.desc_idx_, params), 0);
    ASSERT_EQ(desc_pool->submitBatch(nvmeof_desc.desc_idx_), 0);

    batch_api->status_results.push_back({completion(0, CUFILE_CANCELED)});
    Transport::TransferStatus status{};
    ASSERT_TRUE(transport->getTransferStatus(batch_id, 0, status).ok());

    EXPECT_EQ(status.s, Transport::CANCELED);
    EXPECT_EQ(batch_api->cancel_calls, 0);
    EXPECT_TRUE(batch_desc.task_list[0].is_finished);
    EXPECT_TRUE(transport->freeBatchID(batch_id).ok());
}

TEST(NVMeoFStatusTest, CancelsAndDrainsRemainingSlicesInOneTask) {
    auto batch_api = std::make_shared<FakeCUFileBatchAPI>();
    auto desc_pool = CUFileDescPoolTestPeer::create(batch_api);
    auto transport = NVMeoFTransportTestPeer::createWithoutDriver(desc_pool);

    auto batch_id = transport->allocateBatchID(1);
    auto& batch_desc = Transport::toBatchDesc(batch_id);
    auto& nvmeof_desc = *static_cast<NVMeoFBatchDesc*>(batch_desc.context);
    batch_desc.task_list.resize(1);
    batch_desc.task_list[0].slice_count = 2;
    nvmeof_desc.task_to_slices = {{0, 2}};
    nvmeof_desc.transfer_status = {
        {.s = Transport::PENDING, .transferred_bytes = 0}};

    CUfileIOParams_t params{};
    ASSERT_EQ(desc_pool->pushParams(nvmeof_desc.desc_idx_, params), 0);
    ASSERT_EQ(desc_pool->pushParams(nvmeof_desc.desc_idx_, params), 0);
    ASSERT_EQ(desc_pool->submitBatch(nvmeof_desc.desc_idx_), 0);

    batch_api->status_results.push_back({completion(0, CUFILE_FAILED)});
    Transport::TransferStatus status{};
    ASSERT_TRUE(transport->getTransferStatus(batch_id, 0, status).ok());
    EXPECT_EQ(status.s, Transport::WAITING);
    EXPECT_EQ(batch_api->cancel_calls, 1);
    EXPECT_FALSE(batch_desc.task_list[0].is_finished);
    EXPECT_TRUE(transport->freeBatchID(batch_id).IsBatchBusy());

    batch_api->status_results.push_back({completion(1, CUFILE_CANCELED)});
    ASSERT_TRUE(transport->getTransferStatus(batch_id, 0, status).ok());
    EXPECT_EQ(status.s, Transport::FAILED);
    EXPECT_TRUE(batch_desc.task_list[0].is_finished);
    EXPECT_TRUE(transport->freeBatchID(batch_id).ok());
}

TEST(NVMeoFStatusTest, CancelFailureStillDrainsAndRetiresHandle) {
    auto batch_api = std::make_shared<FakeCUFileBatchAPI>();
    batch_api->cancel_result = {.err = CU_FILE_INTERNAL_BATCH_CANCEL_ERROR,
                                .cu_err = CUDA_SUCCESS};
    auto desc_pool = CUFileDescPoolTestPeer::create(batch_api);
    auto transport = NVMeoFTransportTestPeer::createWithoutDriver(desc_pool);

    auto batch_id = transport->allocateBatchID(1);
    auto& batch_desc = Transport::toBatchDesc(batch_id);
    auto& nvmeof_desc = *static_cast<NVMeoFBatchDesc*>(batch_desc.context);
    batch_desc.task_list.resize(1);
    batch_desc.task_list[0].slice_count = 2;
    nvmeof_desc.task_to_slices = {{0, 2}};
    nvmeof_desc.transfer_status = {
        {.s = Transport::PENDING, .transferred_bytes = 0}};

    CUfileIOParams_t params{};
    ASSERT_EQ(desc_pool->pushParams(nvmeof_desc.desc_idx_, params), 0);
    ASSERT_EQ(desc_pool->pushParams(nvmeof_desc.desc_idx_, params), 0);
    ASSERT_EQ(desc_pool->submitBatch(nvmeof_desc.desc_idx_), 0);
    const auto failed_handle = batch_api->submitted_handle;

    batch_api->status_results.push_back({completion(0, CUFILE_FAILED)});
    Transport::TransferStatus status{};
    ASSERT_TRUE(transport->getTransferStatus(batch_id, 0, status).ok());
    EXPECT_TRUE(status.s == Transport::WAITING ||
                status.s == Transport::PENDING);
    EXPECT_EQ(batch_api->cancel_calls, 1);
    EXPECT_TRUE(transport->freeBatchID(batch_id).IsBatchBusy());
    EXPECT_TRUE(batch_api->destroyed_handles.empty());

    ASSERT_TRUE(transport->getTransferStatus(batch_id, 0, status).ok());
    EXPECT_EQ(batch_api->cancel_calls, 1);
    EXPECT_TRUE(transport->submitTransfer(batch_id, {}).IsBatchBusy());
    EXPECT_EQ(desc_pool->pushParams(nvmeof_desc.desc_idx_, params), -1);
    EXPECT_EQ(desc_pool->submitBatch(nvmeof_desc.desc_idx_), -1);

    batch_api->status_results.push_back({completion(1, CUFILE_COMPLETE, 1024)});
    ASSERT_TRUE(transport->getTransferStatus(batch_id, 0, status).ok());
    EXPECT_EQ(status.s, Transport::FAILED);
    EXPECT_TRUE(batch_desc.task_list[0].is_finished);
    ASSERT_TRUE(transport->freeBatchID(batch_id).ok());
    ASSERT_EQ(batch_api->destroyed_handles.size(), 1);
    EXPECT_EQ(batch_api->destroyed_handles[0], failed_handle);

    auto next_batch_id = transport->allocateBatchID(1);
    ASSERT_NE(next_batch_id, static_cast<Transport::BatchID>(ERR_MEMORY));
    auto& next_batch_desc = Transport::toBatchDesc(next_batch_id);
    auto& next_nvmeof_desc =
        *static_cast<NVMeoFBatchDesc*>(next_batch_desc.context);
    EXPECT_EQ(batch_api->setup_calls, 2);
    EXPECT_NE(
        desc_pool->getDesc(next_nvmeof_desc.desc_idx_)->batch_handle->handle,
        failed_handle);
    EXPECT_TRUE(transport->freeBatchID(next_batch_id).ok());
}

TEST(NVMeoFStatusTest, ReusesSuccessfulHandleAndSubmitsOnlyNewParams) {
    auto batch_api = std::make_shared<FakeCUFileBatchAPI>();
    auto desc_pool = CUFileDescPoolTestPeer::create(batch_api);

    const int desc_idx = desc_pool->allocCUfileDesc(2);
    ASSERT_GE(desc_idx, 0);
    const auto original_handle =
        desc_pool->getDesc(desc_idx)->batch_handle->handle;
    CUfileIOParams_t params{};
    ASSERT_EQ(desc_pool->pushParams(desc_idx, params), 0);
    ASSERT_EQ(desc_pool->submitBatch(desc_idx), 0);
    ASSERT_EQ(desc_pool->pushParams(desc_idx, params), 0);
    ASSERT_EQ(desc_pool->submitBatch(desc_idx), 0);

    EXPECT_EQ(batch_api->submitted_counts, (std::vector<unsigned>{1, 1}));
    EXPECT_EQ(batch_api->submitted_first_cookies,
              (std::vector<void*>{reinterpret_cast<void*>(1),
                                  reinterpret_cast<void*>(2)}));

    batch_api->status_results.push_back({completion(1, CUFILE_COMPLETE, 2048),
                                         completion(0, CUFILE_COMPLETE, 1024)});
    CUFileBatchSnapshot snapshot;
    ASSERT_EQ(desc_pool->pollBatch(desc_idx, snapshot), 0);
    EXPECT_TRUE(snapshot.all_terminal);
    EXPECT_FALSE(snapshot.failure_seen);
    ASSERT_EQ(desc_pool->freeCUfileDesc(desc_idx), 0);
    EXPECT_TRUE(batch_api->destroyed_handles.empty());

    const int next_desc_idx = desc_pool->allocCUfileDesc(1);
    ASSERT_GE(next_desc_idx, 0);
    EXPECT_EQ(batch_api->setup_calls, 1);
    EXPECT_EQ(desc_pool->getDesc(next_desc_idx)->batch_handle->handle,
              original_handle);
    EXPECT_EQ(desc_pool->freeCUfileDesc(next_desc_idx), 0);
}

TEST(NVMeoFStatusTest, DiscardsOnlyUnsubmittedParams) {
    auto batch_api = std::make_shared<FakeCUFileBatchAPI>();
    auto desc_pool = CUFileDescPoolTestPeer::create(batch_api);

    const int desc_idx = desc_pool->allocCUfileDesc(2);
    ASSERT_GE(desc_idx, 0);
    CUfileIOParams_t params{};
    ASSERT_EQ(desc_pool->pushParams(desc_idx, params), 0);
    ASSERT_EQ(desc_pool->submitBatch(desc_idx), 0);
    ASSERT_EQ(desc_pool->pushParams(desc_idx, params), 0);

    ASSERT_EQ(desc_pool->discardUnsubmittedParams(desc_idx), 0);
    EXPECT_EQ(desc_pool->getSliceNum(desc_idx), 1);

    batch_api->status_results.push_back({completion(0, CUFILE_COMPLETE, 1024)});
    CUFileBatchSnapshot snapshot;
    ASSERT_EQ(desc_pool->pollBatch(desc_idx, snapshot), 0);
    ASSERT_TRUE(snapshot.all_terminal);
    EXPECT_EQ(desc_pool->freeCUfileDesc(desc_idx), 0);
}

TEST(NVMeoFStatusTest, BuildsCUFileBatchParameters) {
    auto batch_api = std::make_shared<FakeCUFileBatchAPI>();
    auto desc_pool = CUFileDescPoolTestPeer::create(batch_api);
    auto transport = NVMeoFTransportTestPeer::createWithoutDriver(desc_pool);
    auto batch_id = transport->allocateBatchID(1);
    auto& batch_desc = Transport::toBatchDesc(batch_id);
    auto& nvmeof_desc = *static_cast<NVMeoFBatchDesc*>(batch_desc.context);
    int caller_buffer = 0;
    auto file_handle = reinterpret_cast<CUfileHandle_t>(0x1234);

    ASSERT_EQ(NVMeoFTransportTestPeer::addBatchSlice(
                  *transport, &caller_buffer, 4096, 8192, nvmeof_desc.desc_idx_,
                  Transport::TransferRequest::READ, file_handle),
              0);

    const auto& params =
        desc_pool->getDesc(nvmeof_desc.desc_idx_)->io_params.front();
    EXPECT_EQ(params.mode, CUFILE_BATCH);
    EXPECT_EQ(params.opcode, CUFILE_READ);
    EXPECT_EQ(params.u.batch.devPtr_base, &caller_buffer);
    EXPECT_EQ(params.u.batch.devPtr_offset, 0);
    EXPECT_EQ(params.u.batch.file_offset, 4096);
    EXPECT_EQ(params.u.batch.size, 8192);
    EXPECT_EQ(params.fh, file_handle);
    EXPECT_EQ(params.cookie, reinterpret_cast<void*>(1));

    EXPECT_TRUE(transport->freeBatchID(batch_id).ok());
}

TEST(NVMeoFStatusTest, PropagatesStatusPollingErrors) {
    auto batch_api = std::make_shared<FakeCUFileBatchAPI>();
    auto desc_pool = CUFileDescPoolTestPeer::create(batch_api);
    auto transport = NVMeoFTransportTestPeer::createWithoutDriver(desc_pool);
    auto batch_id = transport->allocateBatchID(1);
    auto& batch_desc = Transport::toBatchDesc(batch_id);
    auto& nvmeof_desc = *static_cast<NVMeoFBatchDesc*>(batch_desc.context);
    batch_desc.task_list.resize(1);
    nvmeof_desc.task_to_slices = {{0, 1}};
    nvmeof_desc.transfer_status = {
        {.s = Transport::PENDING, .transferred_bytes = 0}};
    CUfileIOParams_t params{};
    ASSERT_EQ(desc_pool->pushParams(nvmeof_desc.desc_idx_, params), 0);
    ASSERT_EQ(desc_pool->submitBatch(nvmeof_desc.desc_idx_), 0);

    batch_api->get_status_result = {
        .err = CU_FILE_INTERNAL_BATCH_GETSTATUS_ERROR, .cu_err = CUDA_SUCCESS};
    Transport::TransferStatus status{};
    EXPECT_TRUE(transport->getTransferStatus(batch_id, 0, status).IsContext());

    batch_api->get_status_result = FakeCUFileBatchAPI::success();
    batch_api->status_results.push_back({completion(0, CUFILE_COMPLETE, 1024)});
    ASSERT_TRUE(transport->getTransferStatus(batch_id, 0, status).ok());
    ASSERT_EQ(status.s, Transport::COMPLETED);
    EXPECT_TRUE(transport->freeBatchID(batch_id).ok());
}

TEST(NVMeoFStatusTest, ValidatesFreePreconditionsAndPoolQuiescence) {
    auto batch_api = std::make_shared<FakeCUFileBatchAPI>();
    auto desc_pool = CUFileDescPoolTestPeer::create(batch_api);
    auto transport = NVMeoFTransportTestPeer::createWithoutDriver(desc_pool);

    EXPECT_TRUE(transport->freeBatchID(0).IsInvalidArgument());

    auto* foreign_batch = new Transport::BatchDesc();
    auto foreign_batch_id = reinterpret_cast<Transport::BatchID>(foreign_batch);
    foreign_batch->context = nullptr;
    EXPECT_TRUE(transport->freeBatchID(foreign_batch_id).IsInvalidArgument());
    delete foreign_batch;

    auto batch_id = transport->allocateBatchID(1);
    auto& batch_desc = Transport::toBatchDesc(batch_id);
    auto& nvmeof_desc = *static_cast<NVMeoFBatchDesc*>(batch_desc.context);
    batch_desc.task_list.resize(1);
    batch_desc.task_list[0].is_finished = true;
    CUfileIOParams_t params{};
    ASSERT_EQ(desc_pool->pushParams(nvmeof_desc.desc_idx_, params), 0);
    ASSERT_EQ(desc_pool->submitBatch(nvmeof_desc.desc_idx_), 0);

    EXPECT_TRUE(transport->freeBatchID(batch_id).IsBatchBusy());

    batch_api->status_results.push_back({completion(0, CUFILE_COMPLETE, 1024)});
    CUFileBatchSnapshot snapshot;
    ASSERT_EQ(desc_pool->pollBatch(nvmeof_desc.desc_idx_, snapshot), 0);
    ASSERT_TRUE(snapshot.all_terminal);
    EXPECT_TRUE(transport->freeBatchID(batch_id).ok());
}

TEST(NVMeoFStatusTest, RetriesStatusPollingWithoutCancelingHealthyBatch) {
    auto batch_api = std::make_shared<FakeCUFileBatchAPI>();
    auto desc_pool = CUFileDescPoolTestPeer::create(batch_api);

    const int desc_idx = desc_pool->allocCUfileDesc(1);
    ASSERT_GE(desc_idx, 0);
    CUfileIOParams_t params{};
    ASSERT_EQ(desc_pool->pushParams(desc_idx, params), 0);
    ASSERT_EQ(desc_pool->submitBatch(desc_idx), 0);

    batch_api->get_status_result = {
        .err = CU_FILE_INTERNAL_BATCH_GETSTATUS_ERROR, .cu_err = CUDA_SUCCESS};
    CUFileBatchSnapshot snapshot;
    EXPECT_EQ(desc_pool->pollBatch(desc_idx, snapshot), -1);
    EXPECT_EQ(batch_api->cancel_calls, 0);
    EXPECT_TRUE(desc_pool->isAcceptingSubmissions(desc_idx));

    batch_api->get_status_result = FakeCUFileBatchAPI::success();
    batch_api->status_results.push_back({completion(0, CUFILE_COMPLETE, 1024)});
    ASSERT_EQ(desc_pool->pollBatch(desc_idx, snapshot), 0);
    EXPECT_TRUE(snapshot.all_terminal);
    EXPECT_FALSE(snapshot.failure_seen);
    EXPECT_EQ(desc_pool->freeCUfileDesc(desc_idx), 0);
    EXPECT_TRUE(batch_api->destroyed_handles.empty());
}

TEST(NVMeoFStatusTest, QuarantinesBatchAfterNonRetryableStatusError) {
    auto batch_api = std::make_shared<FakeCUFileBatchAPI>();
    auto desc_pool = CUFileDescPoolTestPeer::create(batch_api);

    const int desc_idx = desc_pool->allocCUfileDesc(1);
    ASSERT_GE(desc_idx, 0);
    const auto failed_handle =
        desc_pool->getDesc(desc_idx)->batch_handle->handle;
    CUfileIOParams_t params{};
    ASSERT_EQ(desc_pool->pushParams(desc_idx, params), 0);
    ASSERT_EQ(desc_pool->submitBatch(desc_idx), 0);

    batch_api->get_status_result = {.err = CU_FILE_INVALID_VALUE,
                                    .cu_err = CUDA_SUCCESS};
    CUFileBatchSnapshot snapshot;
    EXPECT_EQ(desc_pool->pollBatch(desc_idx, snapshot), -1);
    EXPECT_EQ(batch_api->cancel_calls, 1);
    EXPECT_FALSE(desc_pool->isAcceptingSubmissions(desc_idx));
    EXPECT_TRUE(batch_api->destroyed_handles.empty());

    batch_api->get_status_result = FakeCUFileBatchAPI::success();
    batch_api->status_results.push_back({completion(0, CUFILE_CANCELED)});
    ASSERT_EQ(desc_pool->pollBatch(desc_idx, snapshot), 0);
    EXPECT_TRUE(snapshot.all_terminal);
    EXPECT_TRUE(snapshot.failure_seen);
    EXPECT_EQ(desc_pool->freeCUfileDesc(desc_idx), 0);
    EXPECT_EQ(batch_api->destroyed_handles,
              (std::vector<CUfileBatchHandle_t>{failed_handle}));
}

}  // namespace mooncake
