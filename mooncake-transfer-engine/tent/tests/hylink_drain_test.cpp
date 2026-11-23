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

#include <algorithm>

#include "tent/transport/hylink/hylink_transport.h"
#include "tent/platform/rocm.h"

namespace {
enum class Failure { None, Create, Record, Query, SecondRecord, Copy, Sync };
Failure failure;
int copies;
int destroyed_streams;
bool fail_get_device;
std::vector<hipStream_t> pending_copies;
uintptr_t next_stream;
bool forbid_new_streams;
int events_created;
}  // namespace

extern "C" {
hipError_t __wrap_hipGetDeviceCount(int* count) {
    *count = 1;
    return hipSuccess;
}
hipError_t __wrap_hipGetDevice(int* device) {
    if (fail_get_device) return hipErrorInvalidValue;
    *device = 0;
    return hipSuccess;
}
hipError_t __wrap_hipSetDevice(int) { return hipSuccess; }
hipError_t __wrap_hipStreamCreate(hipStream_t* stream) {
    if (forbid_new_streams) return hipErrorOutOfMemory;
    *stream = reinterpret_cast<hipStream_t>(++next_stream);
    return hipSuccess;
}
hipError_t __wrap_hipStreamCreateWithFlags(hipStream_t* stream, unsigned) {
    return __wrap_hipStreamCreate(stream);
}
hipError_t __wrap_hipPointerGetAttributes(hipPointerAttribute_t* attr,
                                          const void*) {
    attr->type = hipMemoryTypeDevice;
    attr->device = 0;
    return hipSuccess;
}
hipError_t __wrap_hipStreamSynchronize(hipStream_t) {
    return failure == Failure::Sync ? hipErrorInvalidValue : hipSuccess;
}
hipError_t __wrap_hipStreamDestroy(hipStream_t) {
    ++destroyed_streams;
    return hipSuccess;
}
hipError_t __wrap_hipMemcpyAsync(void*, const void*, size_t, hipMemcpyKind,
                                 hipStream_t stream) {
    if (failure == Failure::Copy) return hipErrorInvalidValue;
    pending_copies.push_back(stream);
    ++copies;
    return hipSuccess;
}
hipError_t __wrap_hipEventCreateWithFlags(hipEvent_t* event, unsigned) {
    // The first event fences the caller stream, the second tracks the copy.
    ++events_created;
    if (events_created > 1 && failure == Failure::Create)
        return hipErrorInvalidValue;
    *event =
        reinterpret_cast<hipEvent_t>(static_cast<uintptr_t>(events_created));
    return hipSuccess;
}
hipError_t __wrap_hipEventRecord(hipEvent_t, hipStream_t stream) {
    if (stream == hipStreamPerThread) return hipSuccess;
    if (failure == Failure::Record ||
        (events_created == 3 && failure == Failure::SecondRecord))
        return hipErrorInvalidValue;
    return hipSuccess;
}
hipError_t __wrap_hipEventSynchronize(hipEvent_t) { return hipSuccess; }
hipError_t __wrap_hipEventDestroy(hipEvent_t) { return hipSuccess; }
hipError_t __wrap_hipEventQuery(hipEvent_t) {
    return failure == Failure::Query ? hipErrorInvalidValue : hipSuccess;
}
hipError_t __wrap_hipStreamQuery(hipStream_t stream) {
    return std::find(pending_copies.begin(), pending_copies.end(), stream) ==
                   pending_copies.end()
               ? hipSuccess
               : hipErrorNotReady;
}
hipError_t __wrap_hipGetLastError() { return hipSuccess; }
const char* __wrap_hipGetErrorString(hipError_t) { return "mock HIP error"; }
}

namespace mooncake::tent {
namespace {

class HylinkDrainTest : public ::testing::Test {
   protected:
    void SetUp() override {
        failure = Failure::Query;
        copies = events_created = 0;
        next_stream = 0x100;
        destroyed_streams = 0;
        fail_get_device = false;
        forbid_new_streams = false;
        pending_copies.clear();
        auto meta = std::make_shared<ControlService>("p2p", "", nullptr);
        ASSERT_TRUE(meta->segmentManager()
                        .updateLocal([&](SegmentDesc& seg) {
                            BufferDesc buf{};
                            buf.addr = reinterpret_cast<uint64_t>(&source);
                            buf.length = sizeof(source);
                            buf.location = "hip:0";
                            std::get<MemorySegmentDesc>(seg.detail)
                                .buffers.push_back(buf);
                            return Status::OK();
                        })
                        .ok());
        std::string name = "local";
        ASSERT_TRUE(transport.install(name, meta, nullptr).ok());
        batch.max_size = 2;
    }
    Request request() {
        Request req{};
        req.opcode = Request::WRITE;
        req.source = &source;
        req.target_id = LOCAL_SEGMENT_ID;
        req.target_offset = reinterpret_cast<uint64_t>(&target);
        req.length = sizeof(source);
        return req;
    }
    HylinkTransport transport;
    HylinkSubBatch batch;
    int source{}, target{};
};

TEST_F(HylinkDrainTest, FailedFileCopyKeepsAcceptedStreamUntilCompletion) {
    failure = Failure::Sync;
    RocmPlatform platform(nullptr);
    Platform::PendingCopy pending;
    ASSERT_FALSE(
        platform.copyWithCompletion(&target, &source, sizeof(source), pending)
            .ok());
    ASSERT_TRUE(pending);
    EXPECT_FALSE(pending->finished());
    pending_copies.clear();
    EXPECT_TRUE(pending->finished());
    pending.reset();
    failure = Failure::Copy;
    ASSERT_FALSE(
        platform.copyWithCompletion(&target, &source, sizeof(source), pending)
            .ok());
    EXPECT_FALSE(pending);
}

class HylinkEventFailureTest : public HylinkDrainTest,
                               public ::testing::WithParamInterface<Failure> {};

TEST_P(HylinkEventFailureTest, AcceptedCopyOutlivesEventFailure) {
    failure = GetParam();
    ASSERT_TRUE(transport.submitTransferTasks(&batch, {request()}).ok());
    EXPECT_EQ(copies, 1);
    TransferStatus status{};
    ASSERT_TRUE(transport.getTransferStatus(&batch, 0, status).ok());
    EXPECT_EQ(status.s, FAILED);
    bool drained = true;
    ASSERT_TRUE(transport.isSubBatchDrained(&batch, drained).ok());
    EXPECT_FALSE(drained);
    pending_copies.clear();
    ASSERT_TRUE(transport.isSubBatchDrained(&batch, drained).ok());
    EXPECT_TRUE(drained);
    ASSERT_TRUE(transport.getTransferStatus(&batch, 0, status).ok());
    EXPECT_EQ(status.s, FAILED);
}

TEST_F(HylinkDrainTest, ReleasesStreamWhenDeviceLookupFailsAtUninstall) {
    failure = Failure::None;
    {
        HylinkSubBatch completed;
        completed.max_size = 1;
        ASSERT_TRUE(
            transport.submitTransferTasks(&completed, {request()}).ok());
        pending_copies.clear();
        bool drained = false;
        ASSERT_TRUE(transport.isSubBatchDrained(&completed, drained).ok());
        ASSERT_TRUE(drained);
    }
    fail_get_device = true;
    EXPECT_TRUE(transport.uninstall().ok());
    EXPECT_EQ(destroyed_streams, 1);
}

TEST_F(HylinkDrainTest, SecondCopyEventFailureKeepsBothTasks) {
    failure = Failure::SecondRecord;
    ASSERT_TRUE(
        transport.submitTransferTasks(&batch, {request(), request()}).ok());
    ASSERT_EQ(copies, 2);
    TransferStatus status{};
    ASSERT_TRUE(transport.getTransferStatus(&batch, 0, status).ok());
    EXPECT_EQ(status.s, COMPLETED);
    ASSERT_TRUE(transport.getTransferStatus(&batch, 1, status).ok());
    EXPECT_EQ(status.s, FAILED);
    bool drained = true;
    ASSERT_TRUE(transport.isSubBatchDrained(&batch, drained).ok());
    EXPECT_FALSE(drained);
    pending_copies.clear();
    ASSERT_TRUE(transport.isSubBatchDrained(&batch, drained).ok());
    EXPECT_TRUE(drained);
}

TEST_P(HylinkEventFailureTest, LaterBatchCannotExtendFailedCopyDrain) {
    failure = GetParam();
    ASSERT_TRUE(transport.submitTransferTasks(&batch, {request()}).ok());
    failure = Failure::Query;
    HylinkSubBatch later;
    later.max_size = 8;
    ASSERT_TRUE(transport
                    .submitTransferTasks(
                        &later, std::vector<Request>(later.max_size, request()))
                    .ok());
    ASSERT_EQ(pending_copies.size(), 1 + later.max_size);
    pending_copies.erase(pending_copies.begin());
    bool drained = false;
    ASSERT_TRUE(transport.isSubBatchDrained(&batch, drained).ok());
    EXPECT_TRUE(drained);
    ASSERT_TRUE(transport.isSubBatchDrained(&later, drained).ok());
    EXPECT_FALSE(drained);
    pending_copies.clear();
    ASSERT_TRUE(transport.isSubBatchDrained(&later, drained).ok());
    EXPECT_TRUE(drained);
}

TEST_F(HylinkDrainTest, CopyRejectionReturnsSubmitErrorAndPreservesPriorTasks) {
    failure = Failure::None;
    ASSERT_TRUE(transport.submitTransferTasks(&batch, {request()}).ok());
    failure = Failure::Copy;
    EXPECT_FALSE(transport.submitTransferTasks(&batch, {request()}).ok());
    ASSERT_EQ(batch.size(), 1u);
    EXPECT_EQ(copies, 1);
    TransferStatus status{};
    ASSERT_TRUE(transport.getTransferStatus(&batch, 0, status).ok());
    EXPECT_EQ(status.s, COMPLETED);
}

TEST_F(HylinkDrainTest, RejectedAppendReturnsOnlyUnusedStreams) {
    batch.max_size = 9;
    ASSERT_TRUE(transport.submitTransferTasks(&batch, {request()}).ok());
    ASSERT_EQ(pending_copies.size(), 1u);
    const auto first = pending_copies.front();
    std::vector<Request> append(8, request());
    append.back().source = nullptr;
    ASSERT_FALSE(transport.submitTransferTasks(&batch, append).ok());
    ASSERT_EQ(batch.size(), 1u);
    EXPECT_EQ(copies, 1);

    forbid_new_streams = true;
    HylinkSubBatch other;
    other.max_size = 1;
    ASSERT_TRUE(transport.submitTransferTasks(&other, {request()}).ok());
    ASSERT_EQ(pending_copies.size(), 2u);
    EXPECT_NE(first, pending_copies.back());
    bool drained = true;
    ASSERT_TRUE(transport.isSubBatchDrained(&batch, drained).ok());
    EXPECT_FALSE(drained);
    pending_copies.erase(pending_copies.begin());
    ASSERT_TRUE(transport.isSubBatchDrained(&batch, drained).ok());
    EXPECT_TRUE(drained);
    TransferStatus status{};
    ASSERT_TRUE(transport.getTransferStatus(&batch, 0, status).ok());
    EXPECT_EQ(status.s, FAILED);
    ASSERT_TRUE(transport.isSubBatchDrained(&other, drained).ok());
    EXPECT_FALSE(drained);
    pending_copies.clear();
    ASSERT_TRUE(transport.isSubBatchDrained(&other, drained).ok());
    EXPECT_TRUE(drained);
}

TEST_F(HylinkDrainTest, RejectsInvalidRequestBeforeFirstCopy) {
    auto invalid = request();
    invalid.source = nullptr;
    EXPECT_FALSE(
        transport.submitTransferTasks(&batch, {request(), invalid}).ok());
    EXPECT_EQ(copies, 0);
    EXPECT_EQ(batch.size(), 0u);
}
INSTANTIATE_TEST_SUITE_P(Event, HylinkEventFailureTest,
                         ::testing::Values(Failure::Create, Failure::Record,
                                           Failure::Query));
}  // namespace
}  // namespace mooncake::tent
