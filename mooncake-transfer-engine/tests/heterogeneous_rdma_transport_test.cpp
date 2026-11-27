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

#include <acl/acl.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdlib>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

#define private public
#include "transport/ascend_transport/heterogeneous_rdma_transport.h"
#undef private

namespace mooncake {
namespace {

using namespace std::chrono_literals;

enum class Event { kMemcpyFinished, kRdmaSubmitFinished, kStreamDestroyed };

struct AclMock {
    std::mutex mu;
    std::condition_variable cv;
    int set_device_result{0};
    int create_stream_result{0};
    int synchronize_result{0};
    int memcpy_d2h_result{0};
    int memcpy_d2d_result{0};
    int create_stream_calls{0};
    int destroy_stream_calls{0};
    int rdma_submit_calls{0};
    int rdma_task_submit_calls{0};
    bool block_rdma_submit{false};
    bool rdma_submit_started{false};
    bool allow_rdma_submit{false};
    bool rdma_submit_finished{false};
    bool memcpy_finished{false};
    std::vector<Event> events;
};

AclMock mock;

class MockRdmaTransport : public RdmaTransport {
   public:
    Status submitTransfer(BatchID,
                          const std::vector<TransferRequest> &) override {
        std::unique_lock<std::mutex> lock(mock.mu);
        ++mock.rdma_submit_calls;
        if (mock.block_rdma_submit) {
            mock.rdma_submit_started = true;
            mock.cv.notify_all();
            mock.cv.wait(lock, [&] { return mock.allow_rdma_submit; });
        }
        mock.rdma_submit_finished = true;
        mock.events.push_back(Event::kRdmaSubmitFinished);
        mock.cv.notify_all();
        return Status::OK();
    }

    Status submitTransferTask(
        const std::vector<Transport::TransferTask *> &) override {
        std::lock_guard<std::mutex> lock(mock.mu);
        ++mock.rdma_task_submit_calls;
        return Status::OK();
    }
};

void resetMock() {
    std::lock_guard<std::mutex> lock(mock.mu);
    mock.set_device_result = 0;
    mock.create_stream_result = 0;
    mock.synchronize_result = 0;
    mock.memcpy_d2h_result = 0;
    mock.memcpy_d2d_result = 0;
    mock.create_stream_calls = 0;
    mock.destroy_stream_calls = 0;
    mock.rdma_submit_calls = 0;
    mock.rdma_task_submit_calls = 0;
    mock.block_rdma_submit = false;
    mock.rdma_submit_started = false;
    mock.allow_rdma_submit = false;
    mock.rdma_submit_finished = false;
    mock.memcpy_finished = false;
    mock.events.clear();
}

void initializeInnerTransport(HeterogeneousRdmaTransport &transport) {
    auto metadata = std::make_shared<TransferMetadata>(P2PHANDSHAKE);
    transport.transport_->meta() = metadata;
    transport.transport_ = std::make_unique<MockRdmaTransport>();
    transport.transport_->meta() = std::move(metadata);
}

void runWorkerFailureCase(int set_device_result, int create_stream_result,
                          int memcpy_d2h_result) {
    resetMock();
    mock.set_device_result = set_device_result;
    mock.create_stream_result = create_stream_result;
    mock.memcpy_d2h_result = memcpy_d2h_result;
    HeterogeneousRdmaTransport transport;
    initializeInnerTransport(transport);
    transport.host_addr_ = static_cast<char *>(std::calloc(1, 64));
    transport.running_ = true;
    std::array<char, HUGE_DEVICE_NUM> blocks{};
    for (size_t i = 0; i < HUGE_DEVICE_NUM; ++i) {
        transport.block_queue_.push(blocks.data() + i);
    }
    char source = 0;
    TransferRequest request{};
    request.source = &source;
    request.length = 1;
    Transport::TransferTask task{};
    task.request = &request;
    std::vector<Transport::TransferTask *> tasks{&task};
    Status submit_status;
    std::thread submitter([&] {
        submit_status = transport.aggTransport(
            tasks, std::unique_lock<std::mutex>(transport.memcpy_mutex_));
    });

    bool queued = false;
    for (size_t i = 0; i < 100 && !queued; ++i) {
        {
            std::lock_guard<std::mutex> lock(transport.transfer_queue_mutex_);
            queued = !transport.transfer_queue_.empty();
        }
        if (!queued) std::this_thread::sleep_for(1ms);
    }
    std::thread worker(&HeterogeneousRdmaTransport::transferLoop, &transport);
    worker.join();
    submitter.join();

    EXPECT_TRUE(queued);
    EXPECT_FALSE(submit_status.ok());
    EXPECT_FALSE(transport.running_.load());
    EXPECT_TRUE(transport.transfer_queue_.empty());
    EXPECT_EQ(transport.block_queue_.size(), HUGE_DEVICE_NUM);
}

TEST(HeterogeneousRdmaTransportTest,
     WorkerInitFailureStopsSubmissionsAndDrains) {
    runWorkerFailureCase(1, 0, 0);
    runWorkerFailureCase(0, 1, 0);
}

TEST(HeterogeneousRdmaTransportTest,
     WorkerCopyFailureReleasesBlockAndFailsSubmit) {
    runWorkerFailureCase(0, 0, 1);
}

TEST(HeterogeneousRdmaTransportTest, AggregateCopyFailuresReleaseBlocks) {
    resetMock();
    HeterogeneousRdmaTransport transport;
    initializeInnerTransport(transport);
    transport.host_addr_ = static_cast<char *>(std::calloc(1, 64));
    transport.running_ = true;
    std::array<char, HUGE_DEVICE_NUM> blocks{};
    for (size_t i = 0; i < HUGE_DEVICE_NUM; ++i) {
        transport.block_queue_.push(blocks.data() + i);
    }
    char source = 0;
    TransferRequest request{};
    request.source = &source;
    request.length = 1;
    Transport::TransferTask task{};
    task.request = &request;
    std::vector<Transport::TransferTask *> tasks{&task};

    mock.memcpy_d2d_result = 1;
    EXPECT_FALSE(transport
                     .aggTransport(tasks, std::unique_lock<std::mutex>(
                                              transport.memcpy_mutex_))
                     .ok());
    EXPECT_EQ(transport.block_queue_.size(), HUGE_DEVICE_NUM);

    mock.memcpy_d2d_result = 0;
    mock.synchronize_result = 1;
    EXPECT_FALSE(transport
                     .aggTransport(tasks, std::unique_lock<std::mutex>(
                                              transport.memcpy_mutex_))
                     .ok());
    EXPECT_EQ(transport.block_queue_.size(), HUGE_DEVICE_NUM);
}

TEST(HeterogeneousRdmaTransportTest,
     PublicMultiBlockAggregateSubmissionSucceeds) {
    resetMock();
    HeterogeneousRdmaTransport transport;
    initializeInnerTransport(transport);
    constexpr size_t kTaskCount = 33;
    transport.host_addr_ =
        static_cast<char *>(std::aligned_alloc(64, kTaskCount * 1024 * 1024));
    transport.running_ = true;
    std::array<char, HUGE_DEVICE_NUM> blocks{};
    for (size_t i = 0; i < HUGE_DEVICE_NUM; ++i) {
        transport.block_queue_.push(blocks.data() + i);
    }
    char source = 0;
    std::array<TransferRequest, kTaskCount> requests{};
    std::array<Transport::TransferTask, kTaskCount> task_storage{};
    std::vector<Transport::TransferTask *> tasks;
    for (size_t i = 0; i < kTaskCount; ++i) {
        requests[i].source = &source;
        requests[i].length = 1024 * 1024;
        task_storage[i].request = &requests[i];
        tasks.push_back(&task_storage[i]);
    }

    std::thread worker(&HeterogeneousRdmaTransport::transferLoop, &transport);
    auto status = transport.submitTransferTask(tasks);
    transport.signalShutdown();
    worker.join();

    EXPECT_TRUE(status.ok());
    EXPECT_EQ(mock.rdma_task_submit_calls, 5);
    EXPECT_EQ(transport.block_queue_.size(), HUGE_DEVICE_NUM);
}

TEST(HeterogeneousRdmaTransportTest, DestructorWaitsForAggregateSubmission) {
    resetMock();
    auto transport = std::make_unique<HeterogeneousRdmaTransport>();
    auto *raw = transport.get();
    initializeInnerTransport(*raw);
    raw->host_addr_ = static_cast<char *>(std::calloc(1, 64));
    raw->running_ = true;
    std::array<char, HUGE_DEVICE_NUM> blocks{};
    for (size_t i = 0; i < HUGE_DEVICE_NUM; ++i) {
        raw->block_queue_.push(blocks.data() + i);
    }
    char source = 0;
    TransferRequest request{};
    request.source = &source;
    request.length = 1;
    Transport::TransferTask task{};
    task.request = &request;
    std::vector<Transport::TransferTask *> tasks{&task};
    Status submit_status;
    std::atomic<bool> submit_finished{false};
    std::atomic<bool> destructor_saw_submit_finished{false};
    std::thread submitter([&] {
        HeterogeneousRdmaTransport::SubmissionGuard guard(*raw);
        submit_status = raw->aggTransport(
            tasks, std::unique_lock<std::mutex>(raw->memcpy_mutex_));
        submit_finished.store(true);
    });
    {
        std::unique_lock<std::mutex> lock(raw->transfer_queue_mutex_);
        ASSERT_TRUE(raw->transfer_queue_cv_.wait_for(
            lock, 1s, [&] { return !raw->transfer_queue_.empty(); }));
    }
    std::thread destructor([&] {
        transport.reset();
        destructor_saw_submit_finished.store(submit_finished.load());
    });
    submitter.join();
    destructor.join();

    EXPECT_FALSE(submit_status.ok());
    EXPECT_TRUE(submit_finished.load());
    EXPECT_TRUE(destructor_saw_submit_finished.load());
}

TEST(HeterogeneousRdmaTransportTest, ConcurrentSubmissionsCreateOneCopyStream) {
    resetMock();
    mock.synchronize_result = 1;
    constexpr size_t kSubmitters = 32;
    std::atomic<bool> start{false};
    std::atomic<int> failures{0};

    {
        HeterogeneousRdmaTransport transport;
        initializeInnerTransport(transport);
        transport.running_ = true;
        transport.host_addr_ =
            static_cast<char *>(std::aligned_alloc(64, 4096));
        ASSERT_NE(transport.host_addr_, nullptr);
        TransferRequest request{};
        request.source = reinterpret_cast<void *>(0x3000);
        request.length = 1;
        std::vector<TransferRequest> entries{request};
        std::vector<std::thread> submitters;
        for (size_t i = 0; i < kSubmitters; ++i) {
            submitters.emplace_back([&] {
                while (!start.load(std::memory_order_acquire)) {
                    std::this_thread::yield();
                }
                if (!transport.submitTransfer(0, entries).ok()) {
                    ++failures;
                }
            });
        }
        start.store(true, std::memory_order_release);
        for (auto &submitter : submitters) {
            submitter.join();
        }
        EXPECT_EQ(failures.load(), kSubmitters);
        EXPECT_EQ(mock.create_stream_calls, 1);
        EXPECT_EQ(mock.destroy_stream_calls, 0);
    }
    EXPECT_EQ(mock.destroy_stream_calls, 1);
}

TEST(HeterogeneousRdmaTransportTest, DestructorWaitsForInnerSubmission) {
    resetMock();
    mock.block_rdma_submit = true;
    auto transport = std::make_unique<HeterogeneousRdmaTransport>();
    auto *raw = transport.get();
    initializeInnerTransport(*raw);
    raw->running_ = true;
    raw->host_addr_ = static_cast<char *>(std::aligned_alloc(64, 4096));
    ASSERT_NE(raw->host_addr_, nullptr);
    TransferRequest request{};
    request.source = reinterpret_cast<void *>(0x4000);
    request.length = 1;
    std::vector<TransferRequest> entries{request};
    Status submit_status;
    std::thread submitter(
        [&] { submit_status = raw->submitTransfer(0, entries); });
    {
        std::unique_lock<std::mutex> lock(mock.mu);
        ASSERT_TRUE(mock.cv.wait_for(lock, 1s,
                                     [&] { return mock.rdma_submit_started; }));
    }

    std::atomic<bool> destructor_finished{false};
    std::thread destructor([&] {
        transport.reset();
        destructor_finished.store(true);
    });
    std::this_thread::sleep_for(50ms);
    EXPECT_FALSE(destructor_finished.load());
    EXPECT_EQ(mock.destroy_stream_calls, 0);

    {
        std::lock_guard<std::mutex> lock(mock.mu);
        mock.allow_rdma_submit = true;
        mock.cv.notify_all();
    }
    submitter.join();
    destructor.join();

    EXPECT_TRUE(submit_status.ok());
    EXPECT_TRUE(destructor_finished.load());
    EXPECT_TRUE(mock.memcpy_finished);
    EXPECT_TRUE(mock.rdma_submit_finished);
    EXPECT_EQ(mock.destroy_stream_calls, 1);
    auto destroyed = std::find(mock.events.begin(), mock.events.end(),
                               Event::kStreamDestroyed);
    auto submitted = std::find(mock.events.begin(), mock.events.end(),
                               Event::kRdmaSubmitFinished);
    auto copied = std::find(mock.events.begin(), mock.events.end(),
                            Event::kMemcpyFinished);
    ASSERT_NE(destroyed, mock.events.end());
    ASSERT_NE(submitted, mock.events.end());
    ASSERT_NE(copied, mock.events.end());
    EXPECT_LT(copied, submitted);
    EXPECT_LT(submitted, destroyed);
}

}  // namespace
}  // namespace mooncake

extern "C" int aclrtSetDevice(int) {
    std::lock_guard<std::mutex> lock(mooncake::mock.mu);
    return mooncake::mock.set_device_result;
}

extern "C" int aclrtGetDevice(int *device_id) {
    *device_id = 0;
    return 0;
}

extern "C" int aclrtCreateStream(aclrtStream *stream) {
    std::lock_guard<std::mutex> lock(mooncake::mock.mu);
    ++mooncake::mock.create_stream_calls;
    if (mooncake::mock.create_stream_result == 0) {
        *stream = reinterpret_cast<aclrtStream>(0x1);
    }
    return mooncake::mock.create_stream_result;
}

extern "C" int aclrtDestroyStream(aclrtStream) {
    std::lock_guard<std::mutex> lock(mooncake::mock.mu);
    ++mooncake::mock.destroy_stream_calls;
    mooncake::mock.events.push_back(mooncake::Event::kStreamDestroyed);
    return 0;
}

extern "C" int aclrtSynchronizeStream(aclrtStream) {
    std::lock_guard<std::mutex> lock(mooncake::mock.mu);
    return mooncake::mock.synchronize_result;
}

extern "C" int aclrtMemcpyAsync(void *, size_t, const void *, size_t,
                                aclrtMemcpyKind kind, aclrtStream) {
    std::lock_guard<std::mutex> lock(mooncake::mock.mu);
    const int result = kind == ACL_MEMCPY_DEVICE_TO_HOST
                           ? mooncake::mock.memcpy_d2h_result
                           : mooncake::mock.memcpy_d2d_result;
    if (result == 0) {
        mooncake::mock.memcpy_finished = true;
        mooncake::mock.events.push_back(mooncake::Event::kMemcpyFinished);
    }
    return result;
}

extern "C" int aclrtMemcpy(void *, size_t, const void *, size_t,
                           aclrtMemcpyKind) {
    return 0;
}

extern "C" aclError aclrtPointerGetAttributes(const void *,
                                              aclrtPtrAttributes *attributes) {
    attributes->location.type = ACL_MEM_LOCATION_TYPE_DEVICE;
    return 0;
}

extern "C" int aclrtMalloc(void **address, size_t size, aclrtMemMallocPolicy) {
    *address = std::malloc(size);
    return *address == nullptr;
}

extern "C" int aclrtFree(void *address) {
    std::free(address);
    return 0;
}
