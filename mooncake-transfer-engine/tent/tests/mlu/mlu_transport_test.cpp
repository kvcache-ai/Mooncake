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

#include "tent/transport/mlu/mlu_transport.h"

#include <gtest/gtest.h>
#include <cnrt.h>

#include <cstdint>
#include <chrono>
#include <condition_variable>
#include <cstring>
#include <future>
#include <memory>
#include <mutex>
#include <span>
#include <tuple>
#include <thread>
#include <vector>

#include "tent/common/config.h"
#include "tent/runtime/platform.h"

namespace {
std::atomic<bool> fail_notifier_record{false};
}

extern "C" CNresult __real_cnPlaceNotifier(CNnotifier, CNqueue);
extern "C" CNresult __wrap_cnPlaceNotifier(CNnotifier notifier, CNqueue queue) {
    if (fail_notifier_record.exchange(false)) return CN_ERROR_INVALID_VALUE;
    return __real_cnPlaceNotifier(notifier, queue);
}

namespace mooncake {
namespace tent {
namespace {

constexpr size_t kChunk = 64ul << 10;  // 64 KiB
constexpr size_t kBufSize = 3 * kChunk;

class MluTransportTest : public ::testing::Test {
   protected:
    void SetUp() override {
        auto conf = std::make_shared<Config>();
        loader_ = &Platform::getLoader(conf);
        MemoryOptions options;
        options.location = "mlu:0";
        unsigned int count = 0;
        ASSERT_EQ(cnrtGetDeviceCount(&count), cnrtSuccess);
        if (count == 0) GTEST_SKIP() << "No MLU device visible";
        void *probe = nullptr;
        ASSERT_TRUE(loader_->allocate(&probe, kChunk, options).ok());
        ASSERT_TRUE(loader_->free(probe, kChunk).ok());
        std::string segment = "local";
        ASSERT_TRUE(
            transport_.install(segment, nullptr, nullptr, nullptr).ok());
    }

    void TearDown() override { transport_.uninstall(); }

    void allocDevice(uint8_t **out, size_t size) {
        MemoryOptions options;
        options.location = "mlu:0";
        *out = nullptr;
        void *p = nullptr;
        ASSERT_TRUE(loader_->allocate(&p, size, options).ok());
        ASSERT_NE(p, nullptr);
        *out = static_cast<uint8_t *>(p);
    }

    TransferStatus run(const Request &request) {
        Transport::SubBatchRef batch = nullptr;
        EXPECT_TRUE(transport_.allocateSubBatch(batch, 1).ok());
        EXPECT_TRUE(transport_.submitTransferTasks(batch, {request}).ok());
        auto status = wait(batch);
        EXPECT_TRUE(transport_.freeSubBatch(batch).ok());
        return status;
    }

    TransferStatus wait(Transport::SubBatchRef batch) {
        TransferStatus status{};
        const auto deadline =
            std::chrono::steady_clock::now() + std::chrono::seconds(5);
        do {
            EXPECT_TRUE(transport_.getTransferStatus(batch, 0, status).ok());
            if (status.s != PENDING) return status;
            std::this_thread::yield();
        } while (std::chrono::steady_clock::now() < deadline);
        ADD_FAILURE() << "MLU copy did not complete";
        return status;
    }

    std::shared_ptr<MluCopyQueue> queueFor(const Request &request) {
        Transport::SubBatchRef batch = nullptr;
        EXPECT_TRUE(transport_.allocateSubBatch(batch, 1).ok());
        EXPECT_TRUE(transport_.submitTransferTasks(batch, {request}).ok());
        EXPECT_EQ(wait(batch).s, COMPLETED);
        auto queue = static_cast<MluSubBatch *>(batch)->task_list[0].queue;
        EXPECT_TRUE(transport_.freeSubBatch(batch).ok());
        return queue;
    }

    static Request makeRequest(Request::OpCode op, void *source,
                               uint64_t target, size_t length) {
        Request r;
        r.opcode = op;
        r.source = source;
        r.length = length;
        r.target_id = LOCAL_SEGMENT_ID;
        r.target_offset = target;
        return r;
    }

    Platform *loader_ = nullptr;
    MluTransport transport_;
};

struct RegisteredHost {
    explicit RegisteredHost(MluTransport &transport, size_t size)
        : transport(transport) {
        void *ptr = nullptr;
        auto result = cnrtHostMalloc(&ptr, size);
        if (result != cnrtSuccess) {
            status = Status::InternalError(cnrtGetErrorStr(result));
            return;
        }
        bytes = std::span<uint8_t>(static_cast<uint8_t *>(ptr), size);
        std::fill(bytes.begin(), bytes.end(), 0);
        desc.addr = reinterpret_cast<uint64_t>(bytes.data());
        desc.length = size;
        desc.location = "cpu:0";
        status = transport.addMemoryBuffer(desc, MemoryOptions{});
    }
    ~RegisteredHost() {
        if (bytes.empty()) return;
        EXPECT_TRUE(transport.removeMemoryBuffer(desc).ok());
        EXPECT_EQ(cnrtFreeHost(bytes.data()), cnrtSuccess);
    }
    MluTransport &transport;
    std::span<uint8_t> bytes;
    BufferDesc desc;
    Status status;
};

// Hold a real device queue without adding a transport test hook. The timeout
// lets a broken synchronous implementation fail instead of hanging the suite.
class QueueGate {
   public:
    explicit QueueGate(std::shared_ptr<MluCopyQueue> queue)
        : queue_(std::move(queue)) {
        EXPECT_EQ(cnInvokeHostFunc(queue_->handle, block, this), CN_SUCCESS);
    }
    ~QueueGate() {
        release();
        EXPECT_TRUE(queue_->synchronize().ok());
    }
    void release() {
        std::lock_guard<std::mutex> lock(mutex_);
        released_ = true;
        cv_.notify_all();
    }

   private:
    static void block(void *arg) {
        auto &self = *static_cast<QueueGate *>(arg);
        std::unique_lock<std::mutex> lock(self.mutex_);
        self.cv_.wait_for(lock, std::chrono::seconds(10),
                          [&] { return self.released_; });
    }
    std::shared_ptr<MluCopyQueue> queue_;
    std::mutex mutex_;
    std::condition_variable cv_;
    bool released_ = false;
};

TEST_F(MluTransportTest, LocalStageCopiesFromInteriorDeviceOffset) {
    uint8_t *dev = nullptr;
    ASSERT_NO_FATAL_FAILURE(allocDevice(&dev, kBufSize));
    std::vector<uint8_t> seed(kBufSize);
    for (size_t i = 0; i < kBufSize; ++i) seed[i] = (uint8_t)(i % 251);
    ASSERT_TRUE(loader_->copy(dev, seed.data(), kBufSize).ok());
    std::vector<uint8_t> staging(kChunk, 0);

    auto status = run(makeRequest(Request::WRITE, dev + kChunk,
                                  (uint64_t)staging.data(), kChunk));
    ASSERT_EQ(status.s, TransferStatusEnum::COMPLETED);
    EXPECT_EQ(status.transferred_bytes, kChunk);
    EXPECT_EQ(0, std::memcmp(staging.data(), seed.data() + kChunk, kChunk));

    loader_->free(dev, kBufSize);
}

TEST_F(MluTransportTest, RemoteStageCopiesToInteriorDeviceOffset) {
    uint8_t *dev = nullptr;
    ASSERT_NO_FATAL_FAILURE(allocDevice(&dev, kBufSize));
    std::vector<uint8_t> zero(kBufSize, 0);
    ASSERT_TRUE(loader_->copy(dev, zero.data(), kBufSize).ok());
    std::vector<uint8_t> staging(kChunk);
    for (size_t i = 0; i < kChunk; ++i) staging[i] = (uint8_t)(i % 197);

    auto status = run(makeRequest(Request::WRITE, staging.data(),
                                  (uint64_t)(dev + kChunk), kChunk));
    ASSERT_EQ(status.s, TransferStatusEnum::COMPLETED);

    std::vector<uint8_t> readback(kChunk, 0);
    ASSERT_TRUE(loader_->copy(readback.data(), dev + kChunk, kChunk).ok());
    EXPECT_EQ(0, std::memcmp(readback.data(), staging.data(), kChunk));

    loader_->free(dev, kBufSize);
}

TEST_F(MluTransportTest, ReadStageCopiesHostToDevice) {
    uint8_t *dev = nullptr;
    ASSERT_NO_FATAL_FAILURE(allocDevice(&dev, kBufSize));
    std::vector<uint8_t> staging(kChunk, 0xAB);

    auto status = run(makeRequest(Request::READ, dev + 2 * kChunk,
                                  (uint64_t)staging.data(), kChunk));
    ASSERT_EQ(status.s, TransferStatusEnum::COMPLETED);

    std::vector<uint8_t> readback(kChunk, 0);
    ASSERT_TRUE(loader_->copy(readback.data(), dev + 2 * kChunk, kChunk).ok());
    EXPECT_EQ(0, std::memcmp(readback.data(), staging.data(), kChunk));

    loader_->free(dev, kBufSize);
}

TEST_F(MluTransportTest, FailsWhenNeitherSideIsDeviceMemory) {
    std::vector<uint8_t> host_a(4096, 1), host_b(4096, 2);
    auto status = run(makeRequest(Request::WRITE, host_a.data(),
                                  (uint64_t)host_b.data(), host_a.size()));
    EXPECT_EQ(status.s, TransferStatusEnum::FAILED);
    EXPECT_EQ(status.transferred_bytes, 0u);
    EXPECT_EQ(host_b[0], 2);
}

TEST_F(MluTransportTest, FailsWhenBothSidesAreDeviceMemory) {
    uint8_t *a = nullptr;
    uint8_t *b = nullptr;
    ASSERT_NO_FATAL_FAILURE(allocDevice(&a, kChunk));
    ASSERT_NO_FATAL_FAILURE(allocDevice(&b, kChunk));

    auto status = run(makeRequest(Request::WRITE, a, (uint64_t)b, kChunk));
    EXPECT_EQ(status.s, TransferStatusEnum::FAILED);

    loader_->free(a, kChunk);
    loader_->free(b, kChunk);
}

TEST_F(MluTransportTest, FailsOnNonLocalTarget) {
    uint8_t *dev = nullptr;
    ASSERT_NO_FATAL_FAILURE(allocDevice(&dev, kChunk));
    std::vector<uint8_t> staging(kChunk, 0);

    auto request =
        makeRequest(Request::WRITE, dev, (uint64_t)staging.data(), kChunk);
    request.target_id = LOCAL_SEGMENT_ID + 1;
    auto status = run(request);
    EXPECT_EQ(status.s, TransferStatusEnum::FAILED);

    loader_->free(dev, kChunk);
}

class MluAsyncCopyTest
    : public MluTransportTest,
      public ::testing::WithParamInterface<std::tuple<Request::OpCode, bool>> {
};

TEST_P(MluAsyncCopyTest, RemainsPendingUntilDeviceCopyCompletes) {
    uint8_t *dev = nullptr;
    ASSERT_NO_FATAL_FAILURE(allocDevice(&dev, kBufSize));
    RegisteredHost host(transport_, kBufSize);
    ASSERT_TRUE(host.status.ok()) << host.status.ToString();
    auto [op, device_source] = GetParam();
    auto request =
        makeRequest(op, device_source ? dev + 17 : host.bytes.data() + 17,
                    reinterpret_cast<uint64_t>(
                        device_source ? host.bytes.data() + 17 : dev + 17),
                    kChunk + 1);
    auto queue = queueFor(request);
    ASSERT_NE(queue, nullptr);
    std::vector<uint8_t> seed(kBufSize, 0xAB), zeros(kBufSize, 0);
    const bool to_device = device_source == (op == Request::READ);
    std::fill(host.bytes.begin(), host.bytes.end(), to_device ? 0xAB : 0);
    ASSERT_TRUE(
        loader_->copy(dev, to_device ? zeros.data() : seed.data(), kBufSize)
            .ok());
    QueueGate gate(queue);
    Transport::SubBatchRef batch = nullptr;
    ASSERT_TRUE(transport_.allocateSubBatch(batch, 1).ok());
    const auto start = std::chrono::steady_clock::now();
    ASSERT_TRUE(transport_.submitTransferTasks(batch, {request}).ok());
    EXPECT_LT(std::chrono::steady_clock::now() - start,
              std::chrono::seconds(2));
    TransferStatus status;
    ASSERT_TRUE(transport_.getTransferStatus(batch, 0, status).ok());
    EXPECT_EQ(status.s, PENDING);
    EXPECT_EQ(status.transferred_bytes, 0u);
    gate.release();
    status = wait(batch);
    EXPECT_EQ(status.s, COMPLETED);
    EXPECT_EQ(status.transferred_bytes, request.length);
    ASSERT_TRUE(transport_.freeSubBatch(batch).ok());
    std::vector<uint8_t> actual(kBufSize);
    if (to_device)
        ASSERT_TRUE(loader_->copy(actual.data(), dev, kBufSize).ok());
    else
        actual.assign(host.bytes.begin(), host.bytes.end());
    std::fill(zeros.begin() + 17, zeros.begin() + 17 + request.length, 0xAB);
    EXPECT_EQ(actual, zeros);
    EXPECT_TRUE(loader_->free(dev, kBufSize).ok());
}

INSTANTIATE_TEST_SUITE_P(ReadWrite, MluAsyncCopyTest,
                         ::testing::Combine(::testing::Values(Request::READ,
                                                              Request::WRITE),
                                            ::testing::Bool()));

class MluSmallH2DTest : public MluTransportTest,
                        public ::testing::WithParamInterface<Request::OpCode> {
};

TEST_P(MluSmallH2DTest, CompletesInlineWithAnotherQueuePending) {
    uint8_t *dev = nullptr;
    ASSERT_NO_FATAL_FAILURE(allocDevice(&dev, kBufSize));
    RegisteredHost host(transport_, kBufSize);
    ASSERT_TRUE(host.status.ok()) << host.status.ToString();
    auto queue =
        queueFor(makeRequest(Request::WRITE, host.bytes.data(),
                             reinterpret_cast<uint64_t>(dev), kChunk + 1));
    ASSERT_NE(queue, nullptr);
    QueueGate gate(queue);
    for (size_t length : {1ul, 4096ul, kChunk}) {
        std::vector<uint8_t> zeros(kBufSize, 0);
        ASSERT_TRUE(loader_->copy(dev, zeros.data(), kBufSize).ok());
        std::fill(host.bytes.begin(), host.bytes.end(), 0x6B);
        const auto op = GetParam();
        auto request = makeRequest(
            op, op == Request::READ ? dev + 17 : host.bytes.data() + 17,
            reinterpret_cast<uint64_t>(
                op == Request::READ ? host.bytes.data() + 17 : dev + 17),
            length);
        Transport::SubBatchRef batch = nullptr;
        ASSERT_TRUE(transport_.allocateSubBatch(batch, 1).ok());
        const auto start = std::chrono::steady_clock::now();
        ASSERT_TRUE(transport_.submitTransferTasks(batch, {request}).ok());
        EXPECT_LT(std::chrono::steady_clock::now() - start,
                  std::chrono::seconds(2));
        auto &task = static_cast<MluSubBatch *>(batch)->task_list[0];
        EXPECT_EQ(task.status_word.load(), COMPLETED);
        EXPECT_EQ(task.transferred_bytes.load(), length);
        EXPECT_EQ(task.completion, nullptr);
        EXPECT_EQ(task.queue, nullptr);
        ASSERT_TRUE(transport_.freeSubBatch(batch).ok());
        std::vector<uint8_t> actual(kBufSize);
        ASSERT_TRUE(loader_->copy(actual.data(), dev, kBufSize).ok());
        std::fill(zeros.begin() + 17, zeros.begin() + 17 + length, 0x6B);
        EXPECT_EQ(actual, zeros);
        EXPECT_EQ(cnQueryQueue(queue->handle), CN_ERROR_NOT_READY);
    }
    gate.release();
    EXPECT_TRUE(queue->synchronize().ok());
    EXPECT_TRUE(loader_->free(dev, kBufSize).ok());
}

INSTANTIATE_TEST_SUITE_P(ReadWrite, MluSmallH2DTest,
                         ::testing::Values(Request::READ, Request::WRITE));

TEST_F(MluTransportTest, InlineH2DRejectsDeviceRangeOverflow) {
    uint8_t *dev = nullptr;
    ASSERT_NO_FATAL_FAILURE(allocDevice(&dev, kChunk));
    RegisteredHost host(transport_, kChunk);
    ASSERT_TRUE(host.status.ok()) << host.status.ToString();
    auto status =
        run(makeRequest(Request::WRITE, host.bytes.data(),
                        reinterpret_cast<uint64_t>(dev + kChunk - 1), kChunk));
    EXPECT_EQ(status.s, FAILED);
    EXPECT_EQ(status.transferred_bytes, 0u);
    EXPECT_TRUE(loader_->free(dev, kChunk).ok());
}

class MluAsyncLifecycleTest : public MluTransportTest,
                              public ::testing::WithParamInterface<int> {};

TEST_P(MluAsyncLifecycleTest, DrainsCopiesBeforeReleasingResources) {
    constexpr size_t length = kChunk + 1;
    uint8_t *dev = nullptr;
    ASSERT_NO_FATAL_FAILURE(allocDevice(&dev, length));
    RegisteredHost host(transport_, length);
    ASSERT_TRUE(host.status.ok()) << host.status.ToString();
    auto request = makeRequest(Request::WRITE, host.bytes.data(),
                               reinterpret_cast<uint64_t>(dev), length);
    auto queue = queueFor(request);
    ASSERT_NE(queue, nullptr);
    std::fill(host.bytes.begin(), host.bytes.end(), 0x5C);
    QueueGate gate(queue);
    Transport::SubBatchRef batch = nullptr;
    ASSERT_TRUE(transport_.allocateSubBatch(batch, 1).ok());
    ASSERT_TRUE(transport_.submitTransferTasks(batch, {request}).ok());
    std::promise<void> started;
    auto cleanup = std::async(std::launch::async, [&] {
        started.set_value();
        switch (GetParam()) {
            case 0:
                return transport_.freeSubBatch(batch);
            case 1:
                return transport_.removeMemoryBuffer(host.desc);
            case 2:
                return transport_.quiesce();
            default:
                return transport_.uninstall();
        }
    });
    started.get_future().wait();
    EXPECT_EQ(cleanup.wait_for(std::chrono::milliseconds(50)),
              std::future_status::timeout);
    gate.release();
    EXPECT_TRUE(cleanup.get().ok());
    if (batch) {
        EXPECT_EQ(wait(batch).s, COMPLETED);
        EXPECT_TRUE(transport_.freeSubBatch(batch).ok());
    }
    std::vector<uint8_t> actual(length);
    EXPECT_TRUE(loader_->copy(actual.data(), dev, length).ok());
    EXPECT_EQ(actual,
              std::vector<uint8_t>(host.bytes.begin(), host.bytes.end()));
    EXPECT_TRUE(loader_->free(dev, length).ok());
}

INSTANTIATE_TEST_SUITE_P(FreeUnregisterQuiesceUninstall, MluAsyncLifecycleTest,
                         ::testing::Values(0, 1, 2, 3));

TEST_F(MluTransportTest, NotifierFailureRetainsPendingCopyUntilQueueDrains) {
    constexpr size_t length = kChunk + 1;
    uint8_t *dev = nullptr;
    ASSERT_NO_FATAL_FAILURE(allocDevice(&dev, length));
    RegisteredHost host(transport_, length);
    ASSERT_TRUE(host.status.ok()) << host.status.ToString();
    auto request = makeRequest(Request::WRITE, host.bytes.data(),
                               reinterpret_cast<uint64_t>(dev), length);
    auto queue = queueFor(request);
    ASSERT_NE(queue, nullptr);
    std::fill(host.bytes.begin(), host.bytes.end(), 0xA7);
    QueueGate gate(queue);
    Transport::SubBatchRef batch = nullptr;
    ASSERT_TRUE(transport_.allocateSubBatch(batch, 1).ok());
    fail_notifier_record = true;
    ASSERT_TRUE(transport_.submitTransferTasks(batch, {request}).ok());
    EXPECT_FALSE(fail_notifier_record.load());
    TransferStatus status;
    ASSERT_TRUE(transport_.getTransferStatus(batch, 0, status).ok());
    EXPECT_EQ(status.s, PENDING);
    EXPECT_EQ(status.transferred_bytes, 0u);
    gate.release();
    status = wait(batch);
    EXPECT_EQ(status.s, FAILED);
    EXPECT_EQ(status.transferred_bytes, 0u);
    EXPECT_TRUE(transport_.freeSubBatch(batch).ok());
    std::vector<uint8_t> actual(length);
    ASSERT_TRUE(loader_->copy(actual.data(), dev, length).ok());
    EXPECT_EQ(actual,
              std::vector<uint8_t>(host.bytes.begin(), host.bytes.end()));
    EXPECT_TRUE(loader_->free(dev, length).ok());
}

TEST_F(MluTransportTest, DoesNotUnregisterCallerPinnedMemory) {
    void *host = nullptr;
    ASSERT_EQ(cnrtHostMalloc(&host, kChunk), cnrtSuccess);
    BufferDesc desc;
    desc.addr = reinterpret_cast<uint64_t>(host);
    desc.length = kChunk;
    desc.location = "cpu:0";
    ASSERT_TRUE(transport_.addMemoryBuffer(desc, MemoryOptions{}).ok());
    ASSERT_TRUE(transport_.removeMemoryBuffer(desc).ok());
    cnrtPointerAttributes_t attributes{};
    ASSERT_EQ(cnrtPointerGetAttributes(&attributes, host), cnrtSuccess);
    EXPECT_EQ(attributes.type, cnrtMemTypeHost);
    EXPECT_EQ(cnrtFreeHost(host), cnrtSuccess);
}

TEST_F(MluTransportTest, AllocatesPinnedHostMemory) {
    MemoryOptions options;
    options.location = "cpu:0";
    void *host = nullptr;
    ASSERT_TRUE(transport_.allocateLocalMemory(&host, kChunk, options).ok());
    cnrtPointerAttributes_t attributes{};
    ASSERT_EQ(cnrtPointerGetAttributes(&attributes, host), cnrtSuccess);
    EXPECT_EQ(attributes.type, cnrtMemTypeHost);
    EXPECT_TRUE(transport_.freeLocalMemory(host, kChunk).ok());
}

TEST_F(MluTransportTest, AsyncCopyRejectsDeviceRangeOverflow) {
    uint8_t *dev = nullptr;
    ASSERT_NO_FATAL_FAILURE(allocDevice(&dev, kChunk));
    RegisteredHost host(transport_, kChunk);
    ASSERT_TRUE(host.status.ok()) << host.status.ToString();
    auto status =
        run(makeRequest(Request::WRITE, dev + kChunk - 1,
                        reinterpret_cast<uint64_t>(host.bytes.data()), kChunk));
    EXPECT_EQ(status.s, FAILED);
    EXPECT_EQ(status.transferred_bytes, 0u);
    EXPECT_TRUE(loader_->free(dev, kChunk).ok());
}

TEST_F(MluTransportTest, AsyncCopyPreservesFreshWorkerContext) {
    constexpr size_t length = kChunk + 1;
    uint8_t *dev = nullptr;
    ASSERT_NO_FATAL_FAILURE(allocDevice(&dev, length));
    RegisteredHost host(transport_, length);
    ASSERT_TRUE(host.status.ok()) << host.status.ToString();
    std::fill(host.bytes.begin(), host.bytes.end(), 0x3F);
    std::thread worker([&] {
        ASSERT_EQ(cnCtxSetCurrent(nullptr), CN_SUCCESS);
        EXPECT_EQ(run(makeRequest(Request::WRITE, host.bytes.data(),
                                  reinterpret_cast<uint64_t>(dev), length))
                      .s,
                  COMPLETED);
        CNcontext current = nullptr;
        ASSERT_EQ(cnCtxGetCurrent(&current), CN_SUCCESS);
        EXPECT_EQ(current, nullptr);
    });
    worker.join();
    std::vector<uint8_t> actual(length);
    ASSERT_TRUE(loader_->copy(actual.data(), dev, length).ok());
    EXPECT_EQ(actual,
              std::vector<uint8_t>(host.bytes.begin(), host.bytes.end()));
    EXPECT_TRUE(loader_->free(dev, length).ok());
}

}  // namespace
}  // namespace tent
}  // namespace mooncake
