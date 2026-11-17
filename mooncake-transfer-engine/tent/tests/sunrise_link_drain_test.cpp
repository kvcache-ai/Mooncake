// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0

#include <gtest/gtest.h>
#include <tang_runtime_api.h>

#include <array>
#include <cstring>
#include <limits>
#include <memory>
#include <vector>

#include "tent/transport/sunrise_link/sunrise_link_transport.h"

namespace {

struct Copy {
    void* destination;
    const void* source;
    size_t size;
};
struct MockTang {
    int device = 0;
    int failed_device = -1;
    std::vector<Copy> pending;

    void complete() {
        for (const auto& copy : pending)
            std::memcpy(copy.destination, copy.source, copy.size);
        pending.clear();
    }
} mock;

}  // namespace

extern "C" {
tangError_t __wrap_tangGetDevice(int* device) {
    *device = mock.device;
    return tangSuccess;
}
tangError_t __wrap_tangSetDevice(int device) {
    if (device == mock.failed_device) return tangErrorInvalidValue;
    mock.device = device;
    return tangSuccess;
}
const char* __wrap_tangGetErrorString(tangError_t) { return "injected error"; }
tangError_t __wrap_tangPointerGetAttributes(tangPointerAttributes* attributes,
                                            const void*) {
    *attributes = {};
    attributes->type = tangMemoryTypeDevice;
    attributes->device = mock.device;
    return tangSuccess;
}
tangError_t __wrap_tangStreamCreateWithFlags(tangStream_t* stream, unsigned) {
    *stream = reinterpret_cast<tangStream_t>(1);
    return tangSuccess;
}
tangError_t __wrap_tangStreamDestroy(tangStream_t) { return tangSuccess; }
tangError_t __wrap_tangMemcpyAsync(void* destination, const void* source,
                                   size_t size, tangMemcpyKind, tangStream_t) {
    mock.pending.push_back({destination, source, size});
    return tangSuccess;
}
tangError_t __wrap_tangStreamQuery(tangStream_t) {
    return mock.pending.empty() ? tangSuccess : tangErrorNotReady;
}
tangError_t __wrap_tangStreamSynchronize(tangStream_t) {
    EXPECT_TRUE(mock.pending.empty());
    return tangSuccess;
}
}  // extern "C"

namespace mooncake::tent {

class SunriseLinkTransportTestPeer {
   public:
    static void setup(SunriseLinkTransport& transport, void* first,
                      void* second, size_t size) {
        transport.metadata_ =
            std::make_shared<ControlService>("p2p", "", nullptr);
        transport.async_memcpy_threshold_ = 1;
        transport.registered_memory_[first] = size;
        transport.registered_memory_[second] = size;
        transport.registered_memory_gpu_id_[first] = 0;
        transport.registered_memory_gpu_id_[second] = 1;
    }
};

class SunriseLinkDrainTest : public ::testing::TestWithParam<bool> {
   protected:
    void SetUp() override {
        mock = {};
        source.fill('A');
        batch.max_size = 3;
        batch.task_list.reserve(batch.max_size);
        SunriseLinkTransportTestPeer::setup(transport, source.data(),
                                            source.data() + 16, 16);
    }
    void TearDown() override { mock.complete(); }

    Request request(size_t index) {
        Request result{};
        result.opcode = Request::WRITE;
        result.source = source.data() + index * 16;
        result.target_id = LOCAL_SEGMENT_ID;
        result.target_offset =
            reinterpret_cast<uint64_t>(target.data() + index * 16);
        result.length = 16;
        return result;
    }
    Request rejectedRequest() {
        auto result = request(GetParam() ? 1 : 0);
        if (GetParam()) {
            mock.failed_device = 1;
        } else {
            // No peer is opened for this ID, so real address resolution fails.
            result.target_id = std::numeric_limits<SegmentID>::max();
        }
        return result;
    }
    void checkAcceptedCopy() {
        ASSERT_EQ(batch.size(), 1u);
        ASSERT_EQ(mock.pending.size(), 1u);
        TransferStatus status{};
        ASSERT_TRUE(transport.getTransferStatus(&batch, 0, status).ok());
        EXPECT_EQ(status.s, PENDING);
        bool drained = true;
        ASSERT_TRUE(transport.isSubBatchDrained(&batch, drained).ok());
        EXPECT_FALSE(drained);
        EXPECT_EQ(target[0], 0);

        mock.complete();
        ASSERT_TRUE(transport.isSubBatchDrained(&batch, drained).ok());
        EXPECT_TRUE(drained);
        source.fill('B');
        EXPECT_EQ(target[0], 'A');
        EXPECT_EQ(target[16], 0);
        ASSERT_TRUE(transport.getTransferStatus(&batch, 0, status).ok());
        EXPECT_EQ(status.s, COMPLETED);
        EXPECT_EQ(status.transferred_bytes, 16u);
    }
    SunriseLinkTransport transport;
    SunriseLinkSubBatch batch;
    std::array<char, 32> source{}, target{};
};

TEST_P(SunriseLinkDrainTest, RejectedRequestLeavesNoTask) {
    EXPECT_FALSE(
        transport.submitTransferTasks(&batch, {rejectedRequest()}).ok());
    EXPECT_EQ(batch.size(), 0u);
    EXPECT_TRUE(mock.pending.empty());
    bool drained = false;
    ASSERT_TRUE(transport.isSubBatchDrained(&batch, drained).ok());
    EXPECT_TRUE(drained);
}

TEST_P(SunriseLinkDrainTest, PartialSubmissionKeepsAcceptedCopy) {
    EXPECT_FALSE(transport
                     .submitTransferTasks(
                         &batch, {request(0), rejectedRequest(), request(0)})
                     .ok());
    checkAcceptedCopy();
}

TEST_P(SunriseLinkDrainTest, RejectedAppendKeepsEarlierSubmission) {
    ASSERT_TRUE(transport.submitTransferTasks(&batch, {request(0)}).ok());
    EXPECT_FALSE(
        transport.submitTransferTasks(&batch, {rejectedRequest()}).ok());
    checkAcceptedCopy();
}

INSTANTIATE_TEST_SUITE_P(PreflightFailure, SunriseLinkDrainTest,
                         ::testing::Bool(),
                         [](const ::testing::TestParamInfo<bool>& info) {
                             return info.param ? "DeviceSwitch"
                                               : "RemoteAddress";
                         });

}  // namespace mooncake::tent
