// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0

#include <gtest/gtest.h>

#include <array>
#include <cstring>
#include <memory>
#include <vector>

#include "tent/transport/mnnvl/mnnvl_transport.h"
#include "tent/transport/nvlink/nvlink_transport.h"

namespace {

struct Copy {
    void* dst;
    const void* src;
    size_t size;
};
struct Event {
    size_t copies = 0;
};
struct MockCuda {
    std::vector<Copy> pending;
    size_t accepted = 0, completed = 0, live_events = 0;
    int copy_calls = 0, fail_copy = 0, stream_queries = 0;
    int device = 1;
    bool fail_create = false, fail_record = false, batch_fallback = false;
    cudaError_t stream_error = cudaSuccess, event_error = cudaSuccess;
    cudaError_t get_device_error = cudaSuccess;
    int fail_set_device = -1;

    void complete() {
        for (const auto& copy : pending)
            std::memcpy(copy.dst, copy.src, copy.size);
        completed = accepted;
        pending.clear();
    }
} mock;

cudaStream_t testStream() { return reinterpret_cast<cudaStream_t>(1); }

cudaError_t enqueue(void* dst, const void* src, size_t size) {
    if (++mock.copy_calls == mock.fail_copy) return cudaErrorInvalidValue;
    mock.pending.push_back({dst, src, size});
    ++mock.accepted;
    return cudaSuccess;
}

}  // namespace

extern "C" {
cudaError_t __wrap_cudaGetDevice(int* device) {
    if (mock.get_device_error != cudaSuccess) return mock.get_device_error;
    *device = mock.device;
    return cudaSuccess;
}
cudaError_t __wrap_cudaSetDevice(int device) {
    if (device == mock.fail_set_device) return cudaErrorInvalidDevice;
    mock.device = device;
    return cudaSuccess;
}
const char* __wrap_cudaGetErrorString(cudaError_t) { return "injected error"; }
cudaError_t __wrap_cudaGetLastError() { return cudaSuccess; }
cudaError_t __wrap_cudaMemcpyAsync(void* dst, const void* src, size_t size,
                                   cudaMemcpyKind, cudaStream_t) {
    return enqueue(dst, src, size);
}
#if CUDART_VERSION >= 12080
#if CUDART_VERSION >= 13000
cudaError_t __wrap_cudaMemcpyBatchAsync(const void** dsts, const void** srcs,
                                        size_t* sizes, size_t count,
                                        cudaMemcpyAttributes*, size_t*, size_t,
                                        cudaStream_t) {
#else
cudaError_t __wrap_cudaMemcpyBatchAsync(void** dsts, void** srcs, size_t* sizes,
                                        size_t count, cudaMemcpyAttributes*,
                                        size_t*, size_t, size_t* fail_idx,
                                        cudaStream_t) {
#endif
    if (mock.batch_fallback) return cudaErrorCallRequiresNewerDriver;
    for (size_t i = 0; i < count; ++i) {
        auto result = enqueue(const_cast<void*>(dsts[i]), srcs[i], sizes[i]);
        if (result != cudaSuccess) {
#if CUDART_VERSION < 13000
            *fail_idx = i;
#endif
            return result;
        }
    }
    return cudaSuccess;
}
#endif
cudaError_t __wrap_cudaEventCreateWithFlags(cudaEvent_t* event, unsigned) {
    if (mock.fail_create) return cudaErrorMemoryAllocation;
    *event = reinterpret_cast<cudaEvent_t>(new Event);
    ++mock.live_events;
    return cudaSuccess;
}
cudaError_t __wrap_cudaEventRecord(cudaEvent_t event, cudaStream_t) {
    if (mock.fail_record) return cudaErrorInvalidValue;
    reinterpret_cast<Event*>(event)->copies = mock.accepted;
    return cudaSuccess;
}
cudaError_t __wrap_cudaEventDestroy(cudaEvent_t event) {
    delete reinterpret_cast<Event*>(event);
    --mock.live_events;
    return cudaSuccess;
}
cudaError_t __wrap_cudaEventQuery(cudaEvent_t event) {
    if (mock.event_error != cudaSuccess) return mock.event_error;
    if (!event) return cudaErrorInvalidResourceHandle;
    return reinterpret_cast<Event*>(event)->copies <= mock.completed
               ? cudaSuccess
               : cudaErrorNotReady;
}
cudaError_t __wrap_cudaStreamQuery(cudaStream_t stream) {
    ++mock.stream_queries;
    if (stream != testStream() || mock.device != 1)
        return cudaErrorInvalidResourceHandle;
    if (mock.stream_error != cudaSuccess) return mock.stream_error;
    return mock.pending.empty() ? cudaSuccess : cudaErrorNotReady;
}
}  // extern "C"

namespace mooncake::tent {

class NVLinkTransportTestPeer {
   public:
    using TransportType = NVLinkTransport;
    using Batch = NVLinkSubBatch;
    static void start(TransportType& transport, Batch& batch) {
        transport.async_memcpy_threshold_ = 0;
        std::vector<NVLinkTask*> tasks;
        for (auto& task : batch.task_list) tasks.push_back(&task);
        transport.startTransfer(tasks, &batch);
    }
};

class MnnvlTransportTestPeer {
   public:
    using TransportType = MnnvlTransport;
    using Batch = MnnvlSubBatch;
    static void start(TransportType& transport, Batch& batch) {
        std::vector<MnnvlTask*> tasks;
        for (auto& task : batch.task_list) tasks.push_back(&task);
        transport.startTransfer(tasks, &batch);
    }
};

template <typename Peer>
class CudaTransportDrainTest : public ::testing::Test {
   protected:
    void SetUp() override {
        mock = {};
        batch = std::make_unique<typename Peer::Batch>();
        source.fill('A');
    }
    void TearDown() override {
        mock.complete();
        batch.reset();
        EXPECT_EQ(mock.live_events, 0u);
    }
    void start(size_t count = 1) {
        batch->async_stream = CUDAStreamHandle(testStream(), 1, nullptr);
        batch->stream_device_id = 1;
        batch->task_list.resize(count);
        for (size_t i = 0; i < count; ++i) {
            auto& task = batch->task_list[i];
            task.request.opcode = Request::WRITE;
            task.request.source = source.data() + i * 16;
            task.request.length = 16;
            task.target_addr =
                reinterpret_cast<uint64_t>(target.data() + i * 16);
            task.status_word = PENDING;
        }
        Peer::start(transport, *batch);
    }
    void checkFailedCopyDrain() {
        ASSERT_EQ(mock.pending.size(), 1u);
        for (size_t i = 0; i < batch->size(); ++i) {
            TransferStatus status{};
            ASSERT_TRUE(
                transport.getTransferStatus(batch.get(), i, status).ok());
            EXPECT_EQ(status.s, FAILED);
        }
        mock.device = 7;
        bool drained = true;
        ASSERT_TRUE(transport.isSubBatchDrained(batch.get(), drained).ok());
        EXPECT_FALSE(drained);
        EXPECT_EQ(mock.stream_queries, 1);
        EXPECT_EQ(mock.device, 7);
        EXPECT_EQ(mock.pending.size(), 1u);

        mock.complete();
        ASSERT_TRUE(transport.isSubBatchDrained(batch.get(), drained).ok());
        EXPECT_TRUE(drained);
        EXPECT_EQ(mock.device, 7);
        source.fill('B');
        EXPECT_EQ(target[0], 'A');
        TransferStatus status{};
        ASSERT_TRUE(transport.getTransferStatus(batch.get(), 0, status).ok());
        EXPECT_EQ(status.s, FAILED);
    }
    typename Peer::TransportType transport;
    std::unique_ptr<typename Peer::Batch> batch;
    std::array<char, 32> source{}, target{};
};

using CudaTransports =
    ::testing::Types<NVLinkTransportTestPeer, MnnvlTransportTestPeer>;
TYPED_TEST_SUITE(CudaTransportDrainTest, CudaTransports);

TYPED_TEST(CudaTransportDrainTest,
           PartialSubmissionFailureWaitsForAcceptedCopy) {
    mock.fail_copy = 2;
    this->start(2);
    this->checkFailedCopyDrain();
}

TYPED_TEST(CudaTransportDrainTest, EventCreationFailureWaitsForCopy) {
    mock.fail_create = true;
    this->start();
    this->checkFailedCopyDrain();
}

TYPED_TEST(CudaTransportDrainTest, EventRecordFailureWaitsForCopy) {
    mock.fail_record = true;
    this->start();
    this->checkFailedCopyDrain();
}

TYPED_TEST(CudaTransportDrainTest, EventQueryFailureWaitsForCopy) {
    this->start();
    mock.event_error = cudaErrorInvalidResourceHandle;
    this->checkFailedCopyDrain();
}

TYPED_TEST(CudaTransportDrainTest, StreamQueryErrorDoesNotConfirmDrain) {
    mock.fail_record = true;
    this->start();
    mock.device = 7;
    mock.stream_error = cudaErrorInvalidResourceHandle;
    bool drained = true;
    EXPECT_FALSE(
        this->transport.isSubBatchDrained(this->batch.get(), drained).ok());
    EXPECT_FALSE(drained);
    EXPECT_EQ(mock.device, 7);
    mock.stream_error = cudaSuccess;
    mock.stream_queries = 0;
    this->checkFailedCopyDrain();
}

TYPED_TEST(CudaTransportDrainTest, DeviceErrorsDoNotConfirmDrain) {
    mock.fail_create = true;
    this->start();
    mock.device = 7;
    mock.get_device_error = cudaErrorInvalidDevice;
    bool drained = true;
    EXPECT_FALSE(
        this->transport.isSubBatchDrained(this->batch.get(), drained).ok());
    EXPECT_FALSE(drained);
    mock.get_device_error = cudaSuccess;
    for (int device : {1, 7}) {
        mock.fail_set_device = device;
        drained = true;
        EXPECT_FALSE(
            this->transport.isSubBatchDrained(this->batch.get(), drained).ok());
        EXPECT_FALSE(drained);
    }
    mock.fail_set_device = -1;
    mock.device = 7;
    mock.stream_queries = 0;
    this->checkFailedCopyDrain();
}

TYPED_TEST(CudaTransportDrainTest, SuccessfulCopyMustCompleteBeforeDrain) {
    this->start();
    bool drained = true;
    ASSERT_TRUE(
        this->transport.isSubBatchDrained(this->batch.get(), drained).ok());
    EXPECT_FALSE(drained);
    mock.complete();
    ASSERT_TRUE(
        this->transport.isSubBatchDrained(this->batch.get(), drained).ok());
    EXPECT_TRUE(drained);
    TransferStatus status{};
    ASSERT_TRUE(
        this->transport.getTransferStatus(this->batch.get(), 0, status).ok());
    EXPECT_EQ(status.s, COMPLETED);
    EXPECT_EQ(status.transferred_bytes, 16u);
}

TYPED_TEST(CudaTransportDrainTest, UnusedBatchDoesNotQueryDefaultStream) {
    bool drained = false;
    ASSERT_TRUE(
        this->transport.isSubBatchDrained(this->batch.get(), drained).ok());
    EXPECT_TRUE(drained);
    EXPECT_EQ(mock.stream_queries, 0);
}

using NVLinkDrainTest = CudaTransportDrainTest<NVLinkTransportTestPeer>;
TEST_F(NVLinkDrainTest, FallbackPartialSubmissionWaitsForAcceptedCopy) {
    mock.batch_fallback = true;
    mock.fail_copy = 2;
    start(2);
    checkFailedCopyDrain();
}

}  // namespace mooncake::tent
