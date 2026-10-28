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

#include <gtest/gtest.h>

#include <cstdint>
#include <cstring>
#include <memory>
#include <string>
#include <vector>

#include "common/serialization.h"
#include "multi_transport.h"
#include "transfer_metadata.h"
#include "transport/intranode_nvlink_transport/intranode_nvlink_transport.h"

extern "C" {

cudaError_t __wrap_cudaGetDevice(int* device) {
    *device = 0;
    return cudaSuccess;
}

cudaError_t __wrap_cudaSetDevice(int) { return cudaSuccess; }

cudaError_t __wrap_cudaGetDeviceProperties(cudaDeviceProp*, int) {
    return cudaErrorInvalidValue;
}

cudaError_t __wrap_cudaPointerGetAttributes(cudaPointerAttributes* attributes,
                                            const void*) {
    std::memset(attributes, 0, sizeof(*attributes));
    attributes->type = cudaMemoryTypeDevice;
    attributes->device = 0;
    return cudaSuccess;
}

cudaError_t __wrap_cudaStreamCreateWithFlags(cudaStream_t* stream,
                                             unsigned int) {
    *stream = reinterpret_cast<cudaStream_t>(1);
    return cudaSuccess;
}

cudaError_t __wrap_cudaStreamDestroy(cudaStream_t) { return cudaSuccess; }

cudaError_t __wrap_cudaEventCreateWithFlags(cudaEvent_t* event, unsigned int) {
    *event = reinterpret_cast<cudaEvent_t>(1);
    return cudaSuccess;
}

cudaError_t __wrap_cudaEventRecord(cudaEvent_t, cudaStream_t) {
    return cudaSuccess;
}

cudaError_t __wrap_cudaEventSynchronize(cudaEvent_t) { return cudaSuccess; }

cudaError_t __wrap_cudaEventDestroy(cudaEvent_t) { return cudaSuccess; }

cudaError_t __wrap_cudaIpcOpenMemHandle(void**, cudaIpcMemHandle_t,
                                        unsigned int) {
    return cudaErrorPeerAccessUnsupported;
}

}  // extern "C"

namespace mooncake {
namespace {

using TransferRequest = Transport::TransferRequest;
using TransferTask = Transport::TransferTask;

constexpr Transport::SegmentID kRemoteSegment = 42;
constexpr uint64_t kRemoteAddress = 0x800000;
constexpr size_t kRemoteLength = 0x1000;

class IntraNodeNvlinkSubmitAtomicityTest : public ::testing::Test {
   protected:
    void SetUp() override {
        metadata_ = std::make_shared<TransferMetadata>(P2PHANDSHAKE);
        auto remote = std::make_shared<TransferMetadata::SegmentDesc>();
        remote->name = "remote:1234";
        remote->protocol = "nvlink_intra";

        cudaIpcMemHandle_t handle{};
        TransferMetadata::BufferDesc buffer;
        buffer.addr = kRemoteAddress;
        buffer.length = kRemoteLength;
        buffer.shm_name = serializeBinaryData(&handle, sizeof(handle));
        remote->buffers.push_back(std::move(buffer));
        ASSERT_EQ(metadata_->addLocalSegment(kRemoteSegment, remote->name,
                                             std::move(remote)),
                  0);
        transport_.meta() = metadata_;
        batches_ = std::make_unique<MultiTransport>(metadata_, server_name_);
    }

    TransferRequest localRequest() const {
        TransferRequest request;
        request.opcode = TransferRequest::WRITE;
        request.source = reinterpret_cast<void*>(0x100000);
        request.target_id = LOCAL_SEGMENT_ID;
        request.target_offset = 0x200000;
        request.length = 256;
        return request;
    }

    TransferRequest remoteRequest() const {
        auto request = localRequest();
        request.target_id = kRemoteSegment;
        request.target_offset = kRemoteAddress + 64;
        return request;
    }

    void expectRejectedWithoutSlices(Transport::BatchID batch_id,
                                     const std::vector<TransferTask*>& tasks,
                                     const Status& status) {
        EXPECT_FALSE(status.ok());
        EXPECT_NE(status.ToString().find("CUDA IPC"), std::string::npos)
            << status.ToString();
        for (auto* task : tasks) {
            EXPECT_TRUE(task->slice_list.empty());
            EXPECT_EQ(task->slice_count, 0u);
        }
        EXPECT_EQ(batches_->freeBatchID(batch_id), Status::OK());
    }

    std::shared_ptr<TransferMetadata> metadata_;
    IntraNodeNvlinkTransport transport_;
    std::string server_name_{"unit-test:1234"};
    std::unique_ptr<MultiTransport> batches_;
};

TEST_F(IntraNodeNvlinkSubmitAtomicityTest,
       SubmitTransferRejectsWholeRequestGroupBeforePublishingSlices) {
    const std::vector<TransferRequest> requests = {localRequest(),
                                                   remoteRequest()};
    auto batch_id = batches_->allocateBatchID(requests.size());

    auto status = transport_.submitTransfer(batch_id, requests);

    auto& batch = *reinterpret_cast<Transport::BatchDesc*>(batch_id);
    ASSERT_EQ(batch.task_list.size(), requests.size());
    std::vector<TransferTask*> tasks;
    for (auto& task : batch.task_list) tasks.push_back(&task);
    expectRejectedWithoutSlices(batch_id, tasks, status);
}

TEST_F(IntraNodeNvlinkSubmitAtomicityTest,
       SubmitTransferTaskRejectsWholeTaskGroupBeforePublishingSlices) {
    std::vector<TransferRequest> requests = {localRequest(), remoteRequest()};
    auto batch_id = batches_->allocateBatchID(requests.size());
    auto& batch = *reinterpret_cast<Transport::BatchDesc*>(batch_id);
    batch.task_list.resize(requests.size());
    std::vector<TransferTask*> tasks;
    for (size_t i = 0; i < requests.size(); ++i) {
        batch.task_list[i].batch_id = batch_id;
        batch.task_list[i].request = &requests[i];
        tasks.push_back(&batch.task_list[i]);
    }

    auto status = transport_.submitTransferTask(tasks);

    expectRejectedWithoutSlices(batch_id, tasks, status);
}

}  // namespace
}  // namespace mooncake
