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

#include "tent/transport/hylink/hylink_transport.h"

namespace {
enum class Failure { Create, Record, Query, SecondRecord };
Failure failure;
int copies;
bool stream_ready;
int events_created;
}  // namespace

extern "C" {
hipError_t __wrap_hipGetDeviceCount(int* count) {
    *count = 1;
    return hipSuccess;
}
hipError_t __wrap_hipGetDevice(int* device) {
    *device = 0;
    return hipSuccess;
}
hipError_t __wrap_hipSetDevice(int) { return hipSuccess; }
hipError_t __wrap_hipStreamCreate(hipStream_t* stream) {
    *stream = reinterpret_cast<hipStream_t>(1);
    return hipSuccess;
}
hipError_t __wrap_hipStreamDestroy(hipStream_t) { return hipSuccess; }
hipError_t __wrap_hipMemcpyAsync(void*, const void*, size_t, hipMemcpyKind,
                                 hipStream_t) {
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
hipError_t __wrap_hipEventRecord(hipEvent_t, hipStream_t) {
    if ((events_created > 1 && failure == Failure::Record) ||
        (events_created == 3 && failure == Failure::SecondRecord))
        return hipErrorInvalidValue;
    return hipSuccess;
}
hipError_t __wrap_hipEventSynchronize(hipEvent_t) { return hipSuccess; }
hipError_t __wrap_hipEventDestroy(hipEvent_t) { return hipSuccess; }
hipError_t __wrap_hipEventQuery(hipEvent_t) {
    return failure == Failure::Query ? hipErrorInvalidValue : hipSuccess;
}
hipError_t __wrap_hipStreamQuery(hipStream_t) {
    return stream_ready ? hipSuccess : hipErrorNotReady;
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
        stream_ready = false;
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
    stream_ready = true;
    ASSERT_TRUE(transport.isSubBatchDrained(&batch, drained).ok());
    EXPECT_TRUE(drained);
    ASSERT_TRUE(transport.getTransferStatus(&batch, 0, status).ok());
    EXPECT_EQ(status.s, FAILED);
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
    stream_ready = true;
    ASSERT_TRUE(transport.isSubBatchDrained(&batch, drained).ok());
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
