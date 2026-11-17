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
#include <numa.h>
#include <sched.h>

#include <cstdint>
#include <chrono>
#include <condition_variable>
#include <cstring>
#include <future>
#include <functional>
#include <memory>
#include <mutex>
#include <span>
#include <tuple>
#include <thread>
#include <vector>

#include "tent/common/config.h"
#include "tent/runtime/platform.h"
#include "tent/runtime/transfer_engine_impl.h"
#include "tent/transport/fault_proxy/fault_proxy_transport.h"

namespace {
std::atomic<bool> fail_notifier_record{false};
std::atomic<CNresult> reject_copy{CN_SUCCESS};
std::atomic<CNqueue> fail_queue{nullptr};
std::atomic<bool> fail_query{false};
std::atomic<int> pending_queries{0};
std::atomic<CNqueue> last_recorded_queue{nullptr};
std::atomic<bool> fail_sync{false};
bool force_numa_mismatch = false;
void *forced_pinned_allocation = nullptr;
int forced_numa_node = -1;
int forced_location_queries = 0;
int forced_pinned_frees = 0;
std::function<void()> before_copy, before_record, before_inline,
    after_idle_query;
}  // namespace

extern "C" cnrtRet_t __real_cnrtHostMalloc(void **, size_t);
extern "C" cnrtRet_t __wrap_cnrtHostMalloc(void **ptr, size_t size) {
    auto result = __real_cnrtHostMalloc(ptr, size);
    if (force_numa_mismatch && result == cnrtSuccess)
        forced_pinned_allocation = *ptr;
    return result;
}
extern "C" cnrtRet_t __real_cnrtFreeHost(void *);
extern "C" cnrtRet_t __wrap_cnrtFreeHost(void *ptr) {
    auto result = __real_cnrtFreeHost(ptr);
    if (ptr == forced_pinned_allocation && result == cnrtSuccess) {
        ++forced_pinned_frees;
        forced_pinned_allocation = nullptr;
    }
    return result;
}
extern "C" int __real_numa_move_pages(int, unsigned long, void **, const int *,
                                      int *, int);
extern "C" int __wrap_numa_move_pages(int pid, unsigned long count,
                                      void **pages, const int *nodes,
                                      int *status, int flags) {
    if (force_numa_mismatch && forced_pinned_allocation && count && !nodes &&
        (reinterpret_cast<uintptr_t>(pages[0]) >> 12) ==
            (reinterpret_cast<uintptr_t>(forced_pinned_allocation) >> 12)) {
        std::fill(status, status + count, forced_numa_node);
        ++forced_location_queries;
        return 0;
    }
    return __real_numa_move_pages(pid, count, pages, nodes, status, flags);
}

extern "C" CNresult __real_cnPlaceNotifier(CNnotifier, CNqueue);
extern "C" CNresult __wrap_cnPlaceNotifier(CNnotifier notifier, CNqueue queue) {
    last_recorded_queue = queue;
    if (before_record) before_record();
    if (fail_notifier_record.exchange(false)) return CN_ERROR_INVALID_VALUE;
    return __real_cnPlaceNotifier(notifier, queue);
}

extern "C" CNresult __real_cnMemcpyAsync_V3(CNaddr, CNaddr, cn_uint64_t,
                                            CNqueue);
extern "C" CNresult __wrap_cnMemcpyAsync_V3(CNaddr dst, CNaddr src,
                                            cn_uint64_t len, CNqueue queue) {
    if (before_copy) before_copy();
    auto rejected = reject_copy.exchange(CN_SUCCESS);
    if (rejected != CN_SUCCESS) return rejected;
    return __real_cnMemcpyAsync_V3(dst, src, len, queue);
}
extern "C" CNresult __real_cnMemcpyHtoD(CNaddr, const void *, cn_uint64_t);
extern "C" CNresult __wrap_cnMemcpyHtoD(CNaddr dst, const void *src,
                                        cn_uint64_t len) {
    if (before_inline) before_inline();
    return __real_cnMemcpyHtoD(dst, src, len);
}
extern "C" CNresult __real_cnMemcpyDtoH(void *, CNaddr, cn_uint64_t);
extern "C" CNresult __wrap_cnMemcpyDtoH(void *dst, CNaddr src,
                                        cn_uint64_t len) {
    if (before_inline) before_inline();
    return __real_cnMemcpyDtoH(dst, src, len);
}
extern "C" CNresult __real_cnQueueSync(CNqueue);
extern "C" CNresult __wrap_cnQueueSync(CNqueue queue) {
    if (fail_sync.exchange(false)) return CN_QUEUE_ERROR_INVALID;
    if (queue == fail_queue.load()) return CN_QUEUE_ERROR_INVALID;
    return __real_cnQueueSync(queue);
}
extern "C" CNresult __real_cnQueryNotifier(CNnotifier);
extern "C" CNresult __wrap_cnQueryNotifier(CNnotifier notifier) {
    if (fail_query.load()) return CN_QUEUE_ERROR_INVALID;
    auto result = __real_cnQueryNotifier(notifier);
    if (result == CN_ERROR_NOT_READY) ++pending_queries;
    return result;
}
extern "C" CNresult __real_cnQueryQueue(CNqueue);
extern "C" CNresult __wrap_cnQueryQueue(CNqueue queue) {
    if (queue == fail_queue.load()) return CN_QUEUE_ERROR_INVALID;
    auto result = __real_cnQueryQueue(queue);
    if (result == CN_SUCCESS && after_idle_query) after_idle_query();
    return result;
}

namespace mooncake {
class TransferEngineImplTestPeer {
   public:
    static std::shared_ptr<tent::Transport> tcp(
        tent::TransferEngineImpl &engine) {
        return engine.transport_list_[tent::TCP];
    }
};
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

    void TearDown() override {
        force_numa_mismatch = false;
        forced_pinned_allocation = nullptr;
        forced_location_queries = forced_pinned_frees = 0;
        before_copy = before_record = before_inline = after_idle_query = {};
        fail_query = false;
        fail_sync = false;
        fail_queue = nullptr;
        reject_copy = CN_SUCCESS;
        fail_notifier_record = false;
        transport_.uninstall();
    }

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
    QueueGate(CNqueue handle, CNcontext context)
        : handle_(handle), context_(context) {
        CNcontext previous = nullptr;
        EXPECT_EQ(cnCtxGetCurrent(&previous), CN_SUCCESS);
        EXPECT_EQ(cnCtxSetCurrent(context_), CN_SUCCESS);
        EXPECT_EQ(cnInvokeHostFunc(handle_, block, this), CN_SUCCESS);
        EXPECT_EQ(cnCtxSetCurrent(previous), CN_SUCCESS);
    }
    ~QueueGate() {
        release();
        synchronize();
    }
    void synchronize() {
        if (queue_) {
            EXPECT_TRUE(queue_->synchronize().ok());
        } else {
            CNcontext previous = nullptr;
            EXPECT_EQ(cnCtxGetCurrent(&previous), CN_SUCCESS);
            EXPECT_EQ(cnCtxSetCurrent(context_), CN_SUCCESS);
            EXPECT_EQ(cnQueueSync(handle_), CN_SUCCESS);
            EXPECT_EQ(cnCtxSetCurrent(previous), CN_SUCCESS);
        }
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
    CNqueue handle_ = nullptr;
    CNcontext context_ = nullptr;
    std::mutex mutex_;
    std::condition_variable cv_;
    bool released_ = false;
};

// Delay TCP publication while an earlier MLU task remains pending. A poller
// must not inspect the batch until this submit has published all of its state.
class DelayedTcpAllocation : public FaultProxyTransport {
   public:
    explicit DelayedTcpAllocation(std::shared_ptr<Transport> tcp)
        : FaultProxyTransport(std::move(tcp), {}) {}

    Status allocateSubBatch(SubBatchRef &batch, size_t max_size) override {
        std::this_thread::sleep_for(std::chrono::milliseconds(150));
        return FaultProxyTransport::allocateSubBatch(batch, max_size);
    }

    Status getTransferStatus(SubBatchRef batch, int task_id,
                             TransferStatus &status) override {
        auto result =
            FaultProxyTransport::getTransferStatus(batch, task_id, status);
        if (result.ok() && status.s == COMPLETED) completed = true;
        return result;
    }

    std::atomic<bool> completed{false};
};

class MluNotificationTest : public MluTransportTest {
   protected:
    void checkNotification(bool runtime_queue, bool to_device, bool append_tcp);
};

void MluNotificationTest::checkNotification(bool runtime_queue, bool to_device,
                                            bool append_tcp) {
    auto conf = std::make_shared<Config>();
    conf->set("metadata_type", "p2p");
    conf->set("rpc_server_hostname", "127.0.0.1");
    conf->set("rpc_server_port", "0");
    conf->set("log_level", "warning");
    conf->set("transports/rdma/enable", false);
    conf->set("transports/shm/enable", false);
    conf->set("transports/io_uring/enable", false);
    conf->set("transports/mpcomm/enable", false);
    conf->set("transports/mlu/enable", true);
    conf->set("enable_progress_worker", true);
    conf->set("enable_runtime_queue", runtime_queue);
    conf->set("runtime_queue/progress_fallback_interval_us", 10000UL);
    conf->set("transports/tcp/enable", true);
    conf->set("transports/hp_tcp/enable", false);
    TransferEngineImpl engine(conf);
    ASSERT_TRUE(engine.available());
    std::shared_ptr<DelayedTcpAllocation> tcp;
    if (append_tcp) {
        auto real_tcp = ::mooncake::TransferEngineImplTestPeer::tcp(engine);
        ASSERT_NE(real_tcp, nullptr);
        tcp = std::make_shared<DelayedTcpAllocation>(std::move(real_tcp));
        engine.swapTransportForTest(TCP, tcp);
    }
    uint8_t *dev = nullptr;
    ASSERT_NO_FATAL_FAILURE(allocDevice(&dev, kBufSize));
    RegisteredHost host(transport_, kBufSize);
    ASSERT_TRUE(host.status.ok());
    ASSERT_TRUE(engine.registerLocalMemory(dev, kBufSize).ok());
    ASSERT_TRUE(engine.registerLocalMemory(host.bytes.data(), kBufSize).ok());
    std::fill(host.bytes.begin(), host.bytes.end(), 0x6B);
    ASSERT_TRUE(loader_->copy(dev, host.bytes.data(), kBufSize).ok());
    const auto request =
        makeRequest(Request::WRITE, to_device ? host.bytes.data() : dev,
                    (uint64_t)(to_device ? dev : host.bytes.data()), kBufSize);
    auto warm = engine.allocateBatch(1);
    ASSERT_TRUE(engine.submitTransfer(warm, {request}).ok());
    ASSERT_TRUE(engine.waitTransferCompletion(warm).ok());
    auto queue = last_recorded_queue.load();
    ASSERT_NE(queue, nullptr);
    CNcontext context = nullptr;
    ASSERT_EQ(cnGetMemAttribute(&context, CN_MEM_ATTRIBUTE_CONTEXT,
                                reinterpret_cast<CNaddr>(dev)),
              CN_SUCCESS);
    QueueGate gate(queue, context);
    auto batch = engine.allocateBatch(append_tcp ? 2 : 1);
    Notification notification;
    notification.name = engine.getSegmentName();
    notification.msg = "mlu-background-completion";
    const auto queries_before = pending_queries.load();
    ASSERT_TRUE(engine.submitTransfer(batch, {request}, notification).ok());
    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (pending_queries.load() == queries_before &&
           std::chrono::steady_clock::now() < deadline)
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    EXPECT_GT(pending_queries.load(), queries_before)
        << "worker must first observe the held copy as pending";
    if (append_tcp) {
        auto tcp_request =
            makeRequest(Request::WRITE, host.bytes.data(),
                        (uint64_t)(host.bytes.data() + kChunk), kChunk);
        tcp_request.transport_hint = TCP;
        ASSERT_TRUE(engine.submitTransfer(batch, {tcp_request}).ok());
        // Consume TCP's completion wake while MLU is still pending. Delivery
        // after releasing the DMA gate must come from retained polling.
        deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
        while (!tcp->completed.load() &&
               std::chrono::steady_clock::now() < deadline)
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        EXPECT_TRUE(tcp->completed.load());
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    gate.release();
    gate.synchronize();
    std::vector<Notification> received;
    deadline =
        std::chrono::steady_clock::now() + std::chrono::milliseconds(300);
    do {
        (void)engine.receiveNotification(received);
        if (!received.empty()) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    } while (std::chrono::steady_clock::now() < deadline);
    EXPECT_EQ(received.size(), 1u);
    if (!received.empty()) {
        EXPECT_EQ(received.front().msg, notification.msg);
    }
    // Manual progress is only a cleanup path after the assertion above.
    EXPECT_TRUE(engine.waitTransferCompletion(batch).ok());
    EXPECT_TRUE(engine.unregisterLocalMemory(host.bytes.data(), kBufSize).ok());
    EXPECT_TRUE(engine.unregisterLocalMemory(dev, kBufSize).ok());
    EXPECT_TRUE(loader_->free(dev, kBufSize).ok());
}

class MluProgressTest
    : public MluNotificationTest,
      public ::testing::WithParamInterface<std::tuple<bool, bool>> {};

TEST_P(MluProgressTest, CompletionNotificationWithoutCallerPolling) {
    const auto [runtime_queue, to_device] = GetParam();
    checkNotification(runtime_queue, to_device, false);
}

class MluMixedProgressTest : public MluNotificationTest,
                             public ::testing::WithParamInterface<bool> {};

TEST_P(MluMixedProgressTest, AppendTcpPreservesMluPolling) {
    checkNotification(false, GetParam(), true);
}

INSTANTIATE_TEST_SUITE_P(HostAndDevice, MluMixedProgressTest,
                         ::testing::Bool());
INSTANTIATE_TEST_SUITE_P(DirectAndRuntimeQueue, MluProgressTest,
                         ::testing::Combine(::testing::Bool(),
                                            ::testing::Bool()));

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

TEST_P(MluAsyncCopyTest, QueryErrorPollReturnsBeforeQueueDrains) {
    uint8_t *dev = nullptr;
    ASSERT_NO_FATAL_FAILURE(allocDevice(&dev, kBufSize));
    RegisteredHost host(transport_, kBufSize);
    ASSERT_TRUE(host.status.ok());
    auto [op, device_source] = GetParam();
    auto request = makeRequest(
        op, device_source ? dev : host.bytes.data(),
        (uint64_t)(device_source ? host.bytes.data() : dev), kBufSize);
    auto queue = queueFor(request);
    ASSERT_NE(queue, nullptr);
    QueueGate gate(queue);
    Transport::SubBatchRef batch = nullptr;
    ASSERT_TRUE(transport_.allocateSubBatch(batch, 1).ok());
    ASSERT_TRUE(transport_.submitTransferTasks(batch, {request}).ok());
    fail_query = true;
    TransferStatus status{};
    auto poll = std::async(std::launch::async, [&] {
        return transport_.getTransferStatus(batch, 0, status);
    });
    const auto ready = poll.wait_for(std::chrono::milliseconds(150));
    EXPECT_EQ(ready, std::future_status::ready);
    if (ready == std::future_status::ready) {
        EXPECT_FALSE(poll.get().ok());
        EXPECT_EQ(status.s, PENDING);
        // Further polls query the queue without waiting for its held work.
        for (int i = 0; i < 3; ++i) {
            EXPECT_TRUE(transport_.getTransferStatus(batch, 0, status).ok());
            EXPECT_EQ(status.s, PENDING);
            EXPECT_EQ(status.transferred_bytes, 0u);
        }
        // No later submission may extend the failed task's drain.
        Transport::SubBatchRef rejected = nullptr;
        EXPECT_TRUE(transport_.allocateSubBatch(rejected, 1).ok());
        EXPECT_TRUE(transport_.submitTransferTasks(rejected, {request}).ok());
        EXPECT_TRUE(transport_.getTransferStatus(rejected, 0, status).ok());
        EXPECT_EQ(status.s, FAILED);
        EXPECT_TRUE(transport_.freeSubBatch(rejected).ok());
    }
    // Also release on the buggy implementation, so a failed test cannot hang.
    gate.release();
    if (poll.valid()) (void)poll.get();
    fail_query = false;
    EXPECT_EQ(wait(batch).s, FAILED);
    EXPECT_TRUE(transport_.freeSubBatch(batch).ok());
    EXPECT_EQ(run(request).s, COMPLETED);
    EXPECT_TRUE(loader_->free(dev, kBufSize).ok());
}

TEST_P(MluAsyncCopyTest, FailedQueueRecoversAfterDrain) {
    uint8_t *dev = nullptr;
    ASSERT_NO_FATAL_FAILURE(allocDevice(&dev, kBufSize));
    RegisteredHost host(transport_, kBufSize);
    ASSERT_TRUE(host.status.ok());
    auto [op, device_source] = GetParam();
    const bool to_device = device_source == (op == Request::READ);
    auto request = makeRequest(
        op, device_source ? dev : host.bytes.data(),
        (uint64_t)(device_source ? host.bytes.data() : dev), kBufSize);
    for (bool notifier_failure : {false, true}) {
        // Exercise polling, batch release, and an explicit queue drain that
        // leaves the failed task unpolled while new work is submitted.
        for (int drain = 0; drain < 3; ++drain) {
            SCOPED_TRACE(::testing::Message()
                         << notifier_failure << ":" << drain);
            std::vector<uint8_t> expected(kBufSize, 0x27);
            std::fill(host.bytes.begin(), host.bytes.end(), 0x27);
            ASSERT_TRUE(loader_->copy(dev, expected.data(), kBufSize).ok());
            auto queue = queueFor(request);
            ASSERT_NE(queue, nullptr);
            QueueGate gate(queue);
            Transport::SubBatchRef failed = nullptr;
            ASSERT_TRUE(transport_.allocateSubBatch(failed, 1).ok());
            if (notifier_failure)
                fail_notifier_record = true;
            else
                reject_copy = CN_INVOKE_ERROR_TIMEOUT;
            EXPECT_TRUE(transport_.submitTransferTasks(failed, {request}).ok());
            TransferStatus status{};
            EXPECT_TRUE(transport_.getTransferStatus(failed, 0, status).ok());
            EXPECT_EQ(status.s, PENDING);
            EXPECT_EQ(run(request).s, FAILED);
            gate.release();
            if (drain == 0) {
                EXPECT_EQ(wait(failed).s, FAILED);
            } else if (drain == 1) {
                EXPECT_TRUE(transport_.freeSubBatch(failed).ok());
            } else {
                EXPECT_TRUE(transport_.quiesce().ok());
            }

            std::fill(expected.begin(), expected.end(), 0x6B);
            if (to_device)
                std::fill(host.bytes.begin(), host.bytes.end(), 0x6B);
            else
                EXPECT_TRUE(loader_->copy(dev, expected.data(), kBufSize).ok());
            auto recovered = queueFor(request);
            ASSERT_NE(recovered, nullptr);
            EXPECT_NE(recovered, queue);
            if (to_device) {
                std::vector<uint8_t> actual(kBufSize);
                EXPECT_TRUE(loader_->copy(actual.data(), dev, kBufSize).ok());
                EXPECT_EQ(actual, expected);
            } else {
                EXPECT_TRUE(std::equal(host.bytes.begin(), host.bytes.end(),
                                       expected.begin()));
            }
            QueueGate later_gate(recovered);
            Transport::SubBatchRef later = nullptr;
            EXPECT_TRUE(transport_.allocateSubBatch(later, 1).ok());
            EXPECT_TRUE(transport_.submitTransferTasks(later, {request}).ok());
            if (failed) {
                // New work must not extend the old failed task's drain.
                EXPECT_TRUE(
                    transport_.getTransferStatus(failed, 0, status).ok());
                EXPECT_EQ(status.s, FAILED);
                EXPECT_EQ(status.transferred_bytes, 0u);
                EXPECT_TRUE(transport_.freeSubBatch(failed).ok());
            }
            later_gate.release();
            EXPECT_EQ(wait(later).s, COMPLETED);
            EXPECT_TRUE(transport_.freeSubBatch(later).ok());
        }
    }
    EXPECT_TRUE(loader_->free(dev, kBufSize).ok());
}

TEST_P(MluAsyncCopyTest, PageableCopiesWaitForEarlierQueuedCopy) {
    uint8_t *dev = nullptr;
    ASSERT_NO_FATAL_FAILURE(allocDevice(&dev, kBufSize));
    RegisteredHost host(transport_, kBufSize);
    ASSERT_TRUE(host.status.ok());
    auto [op, device_source] = GetParam();
    const bool to_device = device_source == (op == Request::READ);
    auto request = [&](void *host_ptr, void *device_ptr, size_t length) {
        return makeRequest(op, device_source ? device_ptr : host_ptr,
                           (uint64_t)(device_source ? host_ptr : device_ptr),
                           length);
    };
    std::vector<uint8_t> seed(kBufSize, 0x27);
    ASSERT_TRUE(loader_->copy(dev, seed.data(), kBufSize).ok());
    std::fill(host.bytes.begin(), host.bytes.end(), 0x27);
    auto large = request(host.bytes.data(), dev, kBufSize);
    auto queue = queueFor(large);
    ASSERT_NE(queue, nullptr);
    for (size_t length : {kChunk, kChunk + 1}) {
        SCOPED_TRACE(length);
        std::vector<uint8_t> pageable(length, 0x6B);
        cnrtPointerAttributes_t attributes{};
        ASSERT_EQ(cnrtPointerGetAttributes(&attributes, pageable.data()),
                  cnrtSuccess);
        ASSERT_EQ(attributes.type, cnrtMemTypeUnregistered);
        Transport::SubBatchRef first = nullptr, second = nullptr;
        ASSERT_TRUE(transport_.allocateSubBatch(first, 1).ok());
        ASSERT_TRUE(transport_.allocateSubBatch(second, 1).ok());
        QueueGate gate(queue);
        EXPECT_TRUE(transport_.submitTransferTasks(first, {large}).ok());
        std::promise<void> started;
        std::atomic<int> inline_calls{0};
        before_inline = [&] { ++inline_calls; };
        auto submit = std::async(std::launch::async, [&] {
            started.set_value();
            return transport_.submitTransferTasks(
                second, {request(pageable.data(), dev + 17, length)});
        });
        started.get_future().wait();
        EXPECT_EQ(submit.wait_for(std::chrono::milliseconds(50)),
                  std::future_status::timeout);
        EXPECT_EQ(inline_calls.load(), 0);
        gate.release();
        EXPECT_TRUE(submit.get().ok());
        before_inline = {};
        EXPECT_EQ(inline_calls.load(), 1);
        EXPECT_EQ(wait(second).s, COMPLETED);
        EXPECT_TRUE(transport_.freeSubBatch(first).ok());
        EXPECT_TRUE(transport_.freeSubBatch(second).ok());
        if (to_device) {
            auto expected = seed;
            std::fill(expected.begin() + 17, expected.begin() + 17 + length,
                      0x6B);
            std::vector<uint8_t> actual(kBufSize);
            EXPECT_TRUE(loader_->copy(actual.data(), dev, kBufSize).ok());
            EXPECT_EQ(actual, expected);
        } else {
            EXPECT_EQ(pageable, std::vector<uint8_t>(length, 0x27));
        }
    }
    EXPECT_TRUE(loader_->free(dev, kBufSize).ok());
}

INSTANTIATE_TEST_SUITE_P(ReadWrite, MluAsyncCopyTest,
                         ::testing::Combine(::testing::Values(Request::READ,
                                                              Request::WRITE),
                                            ::testing::Bool()));

class MluSmallH2DTest : public MluTransportTest,
                        public ::testing::WithParamInterface<Request::OpCode> {
};

TEST_P(MluSmallH2DTest, CopiesInlineBeforeQueueCreationAndAfterQueueDrains) {
    uint8_t *dev = nullptr;
    ASSERT_NO_FATAL_FAILURE(allocDevice(&dev, kBufSize));
    RegisteredHost host(transport_, kBufSize);
    ASSERT_TRUE(host.status.ok());
    std::fill(host.bytes.begin(), host.bytes.end(), 0x6B);
    for (bool warm_queue : {false, true}) {
        if (warm_queue) {
            auto queue = queueFor(makeRequest(Request::WRITE, host.bytes.data(),
                                              (uint64_t)dev, kBufSize));
            ASSERT_NE(queue, nullptr);
            ASSERT_EQ(cnQueryQueue(queue->handle), CN_SUCCESS);
        }
        for (size_t length : {1ul, 4096ul, kChunk}) {
            SCOPED_TRACE(::testing::Message() << warm_queue << ":" << length);
            std::vector<uint8_t> expected(kBufSize, 0);
            ASSERT_TRUE(loader_->copy(dev, expected.data(), kBufSize).ok());
            const auto op = GetParam();
            auto request = makeRequest(
                op, op == Request::READ ? dev + 17 : host.bytes.data() + 17,
                (uint64_t)(op == Request::READ ? host.bytes.data() + 17
                                               : dev + 17),
                length);
            Transport::SubBatchRef batch = nullptr;
            ASSERT_TRUE(transport_.allocateSubBatch(batch, 1).ok());
            ASSERT_TRUE(transport_.submitTransferTasks(batch, {request}).ok());
            auto &task = static_cast<MluSubBatch *>(batch)->task_list[0];
            EXPECT_EQ(task.status_word.load(), COMPLETED);
            EXPECT_EQ(task.transferred_bytes.load(), length);
            EXPECT_EQ(task.completion, nullptr);
            EXPECT_EQ(task.queue, nullptr);
            EXPECT_TRUE(transport_.freeSubBatch(batch).ok());
            std::vector<uint8_t> actual(kBufSize);
            ASSERT_TRUE(loader_->copy(actual.data(), dev, kBufSize).ok());
            std::fill(expected.begin() + 17, expected.begin() + 17 + length,
                      0x6B);
            EXPECT_EQ(actual, expected);
        }
    }
    EXPECT_TRUE(loader_->free(dev, kBufSize).ok());
}

TEST_P(MluSmallH2DTest, OrdersOverlappingLargeSmallLargeCopies) {
    uint8_t *dev = nullptr;
    ASSERT_NO_FATAL_FAILURE(allocDevice(&dev, kBufSize));
    RegisteredHost host(transport_, 3 * kBufSize);
    ASSERT_TRUE(host.status.ok());
    std::fill(host.bytes.begin(), host.bytes.begin() + kBufSize, 0x27);
    std::fill(host.bytes.begin() + kBufSize, host.bytes.begin() + 2 * kBufSize,
              0x6B);
    std::fill(host.bytes.begin() + 2 * kBufSize, host.bytes.end(), 0xC4);
    auto make_h2d = [&](uint8_t *source, uint8_t *target, size_t length) {
        return makeRequest(
            GetParam(), GetParam() == Request::READ ? target : source,
            (uint64_t)(GetParam() == Request::READ ? source : target), length);
    };
    auto large = make_h2d(host.bytes.data(), dev, kBufSize);
    auto small = make_h2d(host.bytes.data() + kBufSize, dev + 17, kChunk);
    auto last = make_h2d(host.bytes.data() + 2 * kBufSize, dev + kChunk / 2,
                         kChunk + 1);
    auto queue = queueFor(large);
    ASSERT_NE(queue, nullptr);
    for (bool same_batch : {true, false}) {
        SCOPED_TRACE(same_batch);
        std::vector<Transport::SubBatchRef> batches(same_batch ? 1 : 3,
                                                    nullptr);
        for (auto &batch : batches)
            ASSERT_TRUE(
                transport_.allocateSubBatch(batch, same_batch ? 3 : 1).ok());
        std::vector<uint8_t> expected(kBufSize, 0);
        ASSERT_TRUE(loader_->copy(dev, expected.data(), kBufSize).ok());
        QueueGate gate(queue);
        if (same_batch) {
            EXPECT_TRUE(
                transport_.submitTransferTasks(batches[0], {large, small, last})
                    .ok());
        } else {
            const std::vector<Request> requests{large, small, last};
            for (size_t i = 0; i < requests.size(); ++i)
                EXPECT_TRUE(
                    transport_.submitTransferTasks(batches[i], {requests[i]})
                        .ok());
        }
        for (auto batch : batches) {
            for (auto &task : static_cast<MluSubBatch *>(batch)->task_list) {
                EXPECT_EQ(task.status_word.load(), PENDING);
                EXPECT_EQ(task.queue, queue);
                EXPECT_NE(task.completion, nullptr);
            }
        }
        gate.release();
        // Drain all copies, then check actual overlapping writes, not just
        // the order in which the polling thread observes terminal statuses.
        for (auto &batch : batches)
            EXPECT_TRUE(transport_.freeSubBatch(batch).ok());
        std::fill(expected.begin(), expected.end(), 0x27);
        std::fill(expected.begin() + 17, expected.begin() + 17 + kChunk, 0x6B);
        std::fill(expected.begin() + kChunk / 2,
                  expected.begin() + kChunk / 2 + kChunk + 1, 0xC4);
        std::vector<uint8_t> actual(kBufSize);
        EXPECT_TRUE(loader_->copy(actual.data(), dev, kBufSize).ok());
        EXPECT_EQ(actual, expected);
    }
    EXPECT_TRUE(loader_->free(dev, kBufSize).ok());
}

TEST_P(MluSmallH2DTest, OverlappingWriteAfterCompletionPreservesDependency) {
    uint8_t *dev = nullptr;
    ASSERT_NO_FATAL_FAILURE(allocDevice(&dev, kBufSize));
    RegisteredHost host(transport_, kBufSize);
    ASSERT_TRUE(host.status.ok());
    std::fill(host.bytes.begin(), host.bytes.end(), 0x27);
    EXPECT_EQ(run(makeRequest(Request::WRITE, host.bytes.data(),
                              reinterpret_cast<uint64_t>(dev), kBufSize))
                  .s,
              COMPLETED);
    // Host source memory must still remain unchanged until the preceding
    // copy completes, even though device writes preserve submission order.
    std::fill(host.bytes.begin() + 17, host.bytes.begin() + 17 + kChunk, 0x6B);
    const auto op = GetParam();
    EXPECT_EQ(
        run(makeRequest(
                op, op == Request::READ ? dev + 17 : host.bytes.data() + 17,
                reinterpret_cast<uint64_t>(
                    op == Request::READ ? host.bytes.data() + 17 : dev + 17),
                kChunk))
            .s,
        COMPLETED);
    std::vector<uint8_t> actual(kBufSize);
    ASSERT_TRUE(loader_->copy(actual.data(), dev, kBufSize).ok());
    EXPECT_TRUE(std::equal(actual.begin(), actual.end(), host.bytes.begin()));
    EXPECT_TRUE(loader_->free(dev, kBufSize).ok());
}

INSTANTIATE_TEST_SUITE_P(ReadWrite, MluSmallH2DTest,
                         ::testing::Values(Request::READ, Request::WRITE));

// Hold the first submit before enqueue, notifier placement, an idle result,
// or inline execution (with and without an existing queue). A second submit
// must not enter its copy path until the first has completed submission.
class MluH2DSubmissionOrderTest : public MluTransportTest,
                                  public ::testing::WithParamInterface<int> {};
TEST_P(MluH2DSubmissionOrderTest, ConcurrentSubmitCannotOvertakeFirstCopy) {
    uint8_t *dev = nullptr;
    ASSERT_NO_FATAL_FAILURE(allocDevice(&dev, kBufSize));
    RegisteredHost a(transport_, kBufSize), b(transport_, kBufSize);
    ASSERT_TRUE(a.status.ok());
    ASSERT_TRUE(b.status.ok());
    std::fill(a.bytes.begin(), a.bytes.end(), 0x27);
    std::fill(b.bytes.begin(), b.bytes.end(), 0x6B);
    const bool first_small = GetParam() >= 2;
    auto first = makeRequest(Request::WRITE, a.bytes.data(), (uint64_t)dev,
                             first_small ? kChunk : kBufSize);
    auto second = makeRequest(Request::WRITE, b.bytes.data(), (uint64_t)dev,
                              first_small ? kBufSize : kChunk);
    if (GetParam() == 2 || GetParam() == 4) {
        ASSERT_NE(queueFor(second), nullptr);
    }
    std::vector<uint8_t> expected(kBufSize, 0);
    ASSERT_TRUE(loader_->copy(dev, expected.data(), kBufSize).ok());
    Transport::SubBatchRef first_batch = nullptr, second_batch = nullptr;
    ASSERT_TRUE(transport_.allocateSubBatch(first_batch, 1).ok());
    ASSERT_TRUE(transport_.allocateSubBatch(second_batch, 1).ok());
    std::promise<void> entered, release;
    auto released = release.get_future().share();
    std::atomic<int> calls{0};
    auto pause = [&] {
        if (calls.fetch_add(1) != 0) return;
        entered.set_value();
        EXPECT_EQ(released.wait_for(std::chrono::seconds(5)),
                  std::future_status::ready);
    };
    if (GetParam() == 0) before_copy = pause;
    if (GetParam() == 1) before_record = pause;
    if (GetParam() == 2) after_idle_query = pause;
    if (GetParam() >= 3) before_inline = pause;
    auto submit_a = std::async(std::launch::async, [&] {
        return transport_.submitTransferTasks(first_batch, {first});
    });
    EXPECT_EQ(entered.get_future().wait_for(std::chrono::seconds(3)),
              std::future_status::ready);
    std::promise<void> second_started;
    auto submit_b = std::async(std::launch::async, [&] {
        second_started.set_value();
        return transport_.submitTransferTasks(second_batch, {second});
    });
    second_started.get_future().wait();
    EXPECT_EQ(submit_b.wait_for(std::chrono::milliseconds(50)),
              std::future_status::timeout);
    release.set_value();
    EXPECT_TRUE(submit_a.get().ok());
    EXPECT_TRUE(submit_b.get().ok());
    before_copy = before_record = before_inline = after_idle_query = {};
    EXPECT_EQ(wait(first_batch).s, COMPLETED);
    EXPECT_EQ(wait(second_batch).s, COMPLETED);
    EXPECT_TRUE(transport_.freeSubBatch(first_batch).ok());
    EXPECT_TRUE(transport_.freeSubBatch(second_batch).ok());
    std::fill(expected.begin(), expected.begin() + first.length, 0x27);
    std::fill(expected.begin(), expected.begin() + second.length, 0x6B);
    std::vector<uint8_t> actual(kBufSize);
    EXPECT_TRUE(loader_->copy(actual.data(), dev, kBufSize).ok());
    EXPECT_EQ(actual, expected);
    EXPECT_TRUE(loader_->free(dev, kBufSize).ok());
}
INSTANTIATE_TEST_SUITE_P(EnqueueNotifierIdleColdInlineWarmInline,
                         MluH2DSubmissionOrderTest,
                         ::testing::Values(0, 1, 2, 3, 4));

TEST_F(MluTransportTest, H2DQueueQueryFailureDoesNotPermitInlineCopy) {
    uint8_t *dev = nullptr;
    ASSERT_NO_FATAL_FAILURE(allocDevice(&dev, kBufSize));
    RegisteredHost host(transport_, kBufSize);
    ASSERT_TRUE(host.status.ok());
    std::fill(host.bytes.begin(), host.bytes.end(), 0x27);
    auto queue = queueFor(makeRequest(Request::WRITE, host.bytes.data(),
                                      (uint64_t)dev, kBufSize));
    ASSERT_NE(queue, nullptr);
    std::fill(host.bytes.begin(), host.bytes.end(), 0x6B);
    auto small =
        makeRequest(Request::WRITE, host.bytes.data(), (uint64_t)dev, kChunk);
    std::atomic<int> inline_calls{0};
    before_inline = [&] { ++inline_calls; };
    fail_queue = queue->handle;
    EXPECT_EQ(run(small).s, FAILED);
    fail_queue = nullptr;
    EXPECT_EQ(inline_calls.load(), 0);
    before_inline = {};
    std::vector<uint8_t> actual(kBufSize);
    EXPECT_TRUE(loader_->copy(actual.data(), dev, kBufSize).ok());
    EXPECT_EQ(actual, std::vector<uint8_t>(kBufSize, 0x27));
    // Once the driver confirms the failed queue is idle, inline copies resume.
    EXPECT_EQ(run(small).s, COMPLETED);
    EXPECT_TRUE(loader_->copy(actual.data(), dev, kBufSize).ok());
    EXPECT_TRUE(std::all_of(actual.begin(), actual.begin() + kChunk,
                            [](uint8_t value) { return value == 0x6B; }));
    EXPECT_TRUE(loader_->free(dev, kBufSize).ok());
}

TEST_F(MluTransportTest, PageableH2DDrainFailureDoesNotCopy) {
    uint8_t *dev = nullptr;
    ASSERT_NO_FATAL_FAILURE(allocDevice(&dev, kBufSize));
    RegisteredHost host(transport_, kBufSize);
    ASSERT_TRUE(host.status.ok());
    std::fill(host.bytes.begin(), host.bytes.end(), 0x27);
    auto queue = queueFor(makeRequest(Request::WRITE, host.bytes.data(),
                                      (uint64_t)dev, kBufSize));
    ASSERT_NE(queue, nullptr);
    std::vector<uint8_t> pageable(kChunk, 0x6B);
    QueueGate gate(queue);
    std::atomic<int> inline_calls{0};
    before_inline = [&] { ++inline_calls; };
    fail_sync = true;
    EXPECT_EQ(
        run(makeRequest(Request::WRITE, pageable.data(), (uint64_t)dev, kChunk))
            .s,
        FAILED);
    EXPECT_FALSE(fail_sync.load());
    EXPECT_EQ(inline_calls.load(), 0);
    before_inline = {};
    gate.release();
    EXPECT_TRUE(queue->synchronize().ok());
    std::vector<uint8_t> actual(kBufSize);
    EXPECT_TRUE(loader_->copy(actual.data(), dev, kBufSize).ok());
    EXPECT_EQ(actual, std::vector<uint8_t>(kBufSize, 0x27));
    EXPECT_TRUE(loader_->free(dev, kBufSize).ok());
}

TEST_F(MluTransportTest, SmallH2DDoesNotWaitForD2HQueue) {
    uint8_t *dev = nullptr;
    ASSERT_NO_FATAL_FAILURE(allocDevice(&dev, kBufSize));
    RegisteredHost host(transport_, kBufSize);
    ASSERT_TRUE(host.status.ok());
    auto d2h = queueFor(makeRequest(Request::WRITE, dev,
                                    (uint64_t)host.bytes.data(), kBufSize));
    ASSERT_NE(d2h, nullptr);
    std::fill(host.bytes.begin(), host.bytes.end(), 0x6B);
    QueueGate gate(d2h);
    EXPECT_EQ(run(makeRequest(Request::WRITE, host.bytes.data(), (uint64_t)dev,
                              kChunk))
                  .s,
              COMPLETED);
    EXPECT_EQ(cnQueryQueue(d2h->handle), CN_ERROR_NOT_READY);
    std::vector<uint8_t> actual(kChunk);
    EXPECT_TRUE(loader_->copy(actual.data(), dev, kChunk).ok());
    EXPECT_EQ(actual, std::vector<uint8_t>(kChunk, 0x6B));
    gate.release();
    EXPECT_TRUE(d2h->synchronize().ok());
    EXPECT_TRUE(loader_->free(dev, kBufSize).ok());
}

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
    // A missing marker must not turn the fallback queue drain into an
    // ever-growing wait behind unrelated later submissions.
    EXPECT_EQ(run(request).s, FAILED);
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
    options.location = kWildcardLocation;
    void *host = nullptr;
    ASSERT_TRUE(transport_.allocateLocalMemory(&host, kChunk, options).ok());
    cnrtPointerAttributes_t attributes{};
    ASSERT_EQ(cnrtPointerGetAttributes(&attributes, host), cnrtSuccess);
    EXPECT_EQ(attributes.type, cnrtMemTypeHost);
    EXPECT_TRUE(transport_.freeLocalMemory(host, kChunk).ok());
}

TEST_F(MluTransportTest, PinnedNumaMismatchFallsBackAndReleasesAllocation) {
    const int node = numa_node_of_cpu(sched_getcpu());
    ASSERT_GE(node, 0);
    MemoryOptions options;
    options.location = "cpu:" + std::to_string(node);
    // Keep the real SDK allocation, but report a different placement so the
    // fallback is exercised regardless of where cnrtHostMalloc lands.
    force_numa_mismatch = true;
    forced_numa_node = node + 1;
    void *host = nullptr;
    auto status = transport_.allocateLocalMemory(&host, kChunk, options);
    force_numa_mismatch = false;
    ASSERT_TRUE(status.ok()) << status.ToString();
    EXPECT_EQ(forced_location_queries, 1);
    EXPECT_EQ(forced_pinned_frees, 1);
    cnrtPointerAttributes_t attributes{};
    EXPECT_EQ(cnrtPointerGetAttributes(&attributes, host), cnrtSuccess);
    EXPECT_EQ(attributes.type, cnrtMemTypeUnregistered);
    const auto ranges = loader_->getLocation(host, kChunk);
    EXPECT_FALSE(ranges.empty());
    for (const auto &range : ranges)
        EXPECT_EQ(range.location, options.location);
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

// Park the second task before enqueue, notifier publication, or an inline
// copy. Polling a partially submitted batch must never inspect those fields.
class MluPublicationTest : public MluTransportTest,
                           public ::testing::WithParamInterface<int> {};
TEST_P(MluPublicationTest, PollAndReleaseWaitForSubmissionPublication) {
    const size_t length = GetParam() % 3 == 2 ? kChunk : kChunk + 1;
    uint8_t *dev = nullptr;
    ASSERT_NO_FATAL_FAILURE(allocDevice(&dev, 2 * length));
    RegisteredHost host(transport_, 2 * length);
    ASSERT_TRUE(host.status.ok());
    std::fill(host.bytes.begin(), host.bytes.end(), 0x5A);
    Transport::SubBatchRef batch = nullptr;
    ASSERT_TRUE(transport_.allocateSubBatch(batch, 2).ok());
    std::promise<void> entered, release;
    auto released = release.get_future().share();
    int calls = 0;
    auto gate = [&] {
        if (++calls != 2) return;
        entered.set_value();
        EXPECT_EQ(released.wait_for(std::chrono::seconds(5)),
                  std::future_status::ready);
    };
    if (GetParam() % 3 == 0) before_copy = gate;
    if (GetParam() % 3 == 1) before_record = gate;
    if (GetParam() % 3 == 2) before_inline = gate;
    auto submit = std::async(std::launch::async, [&] {
        return transport_.submitTransferTasks(
            batch, {makeRequest(Request::WRITE, host.bytes.data(),
                                (uint64_t)dev, length),
                    makeRequest(Request::WRITE, host.bytes.data() + length,
                                (uint64_t)(dev + length), length)});
    });
    EXPECT_EQ(entered.get_future().wait_for(std::chrono::seconds(3)),
              std::future_status::ready);
    TransferStatus status{};
    EXPECT_TRUE(transport_.getTransferStatus(batch, 1, status).ok());
    EXPECT_EQ(status.s, PENDING);
    std::promise<void> freeing;
    auto cleanup = std::async(std::launch::async, [&] {
        freeing.set_value();
        return GetParam() < 3 ? transport_.freeSubBatch(batch)
                              : transport_.removeMemoryBuffer(host.desc);
    });
    freeing.get_future().wait();
    EXPECT_EQ(cleanup.wait_for(std::chrono::milliseconds(50)),
              std::future_status::timeout);
    release.set_value();
    EXPECT_TRUE(submit.get().ok());
    EXPECT_TRUE(cleanup.get().ok());
    if (batch) {
        EXPECT_TRUE(transport_.freeSubBatch(batch).ok());
    }
    before_copy = before_record = before_inline = {};
    std::vector<uint8_t> actual(2 * length);
    EXPECT_TRUE(loader_->copy(actual.data(), dev, actual.size()).ok());
    EXPECT_EQ(actual, std::vector<uint8_t>(actual.size(), 0x5A));
    EXPECT_TRUE(loader_->free(dev, actual.size()).ok());
}
INSTANTIATE_TEST_SUITE_P(EnqueueNotifierInline, MluPublicationTest,
                         ::testing::Values(0, 1, 2, 3, 4, 5));

TEST_F(MluTransportTest, RejectedCopyDoesNotWaitForUnrelatedQueueWork) {
    uint8_t *dev = nullptr;
    ASSERT_NO_FATAL_FAILURE(allocDevice(&dev, kBufSize));
    RegisteredHost host(transport_, kBufSize);
    ASSERT_TRUE(host.status.ok());
    auto request = makeRequest(Request::WRITE, host.bytes.data(), (uint64_t)dev,
                               kChunk + 1);
    auto queue = queueFor(request);
    ASSERT_NE(queue, nullptr);
    QueueGate gate(queue);
    Transport::SubBatchRef batch = nullptr;
    ASSERT_TRUE(transport_.allocateSubBatch(batch, 1).ok());
    reject_copy = CN_ERROR_INVALID_VALUE;
    ASSERT_TRUE(transport_.submitTransferTasks(batch, {request}).ok());
    TransferStatus status{};
    EXPECT_TRUE(transport_.getTransferStatus(batch, 0, status).ok());
    EXPECT_EQ(status.s, FAILED);
    EXPECT_FALSE(
        static_cast<MluSubBatch *>(batch)->task_list[0].drain_on_error);
    gate.release();
    EXPECT_TRUE(transport_.freeSubBatch(batch).ok());
    EXPECT_TRUE(loader_->free(dev, kBufSize).ok());
}

TEST_F(MluTransportTest,
       PersistentQueryFailureReportsErrorAndRetainsResources) {
    uint8_t *dev = nullptr;
    ASSERT_NO_FATAL_FAILURE(allocDevice(&dev, kBufSize));
    RegisteredHost host(transport_, kBufSize);
    ASSERT_TRUE(host.status.ok());
    auto request = makeRequest(Request::WRITE, host.bytes.data(), (uint64_t)dev,
                               kChunk + 1);
    auto queue = queueFor(request);
    ASSERT_NE(queue, nullptr);
    QueueGate gate(queue);
    Transport::SubBatchRef batch = nullptr;
    ASSERT_TRUE(transport_.allocateSubBatch(batch, 1).ok());
    ASSERT_TRUE(transport_.submitTransferTasks(batch, {request}).ok());
    fail_queue = queue->handle;
    fail_query = true;
    for (int i = 0; i < 3; ++i) {
        TransferStatus status{};
        EXPECT_FALSE(transport_.getTransferStatus(batch, 0, status).ok());
        EXPECT_EQ(status.s, PENDING);
    }
    EXPECT_FALSE(transport_.freeSubBatch(batch).ok());
    EXPECT_NE(batch, nullptr);
    EXPECT_FALSE(transport_.removeMemoryBuffer(host.desc).ok());
    EXPECT_NE(std::find(host.desc.transports.begin(),
                        host.desc.transports.end(), MLU),
              host.desc.transports.end());
    fail_query = false;
    fail_queue = nullptr;
    gate.release();
    EXPECT_TRUE(transport_.freeSubBatch(batch).ok());
    // The failed queue has drained; subsequent copies can use a fresh queue.
    EXPECT_EQ(run(request).s, COMPLETED);
    EXPECT_TRUE(loader_->free(dev, kBufSize).ok());
}

TEST_F(MluTransportTest, UnregisterDoesNotDrainAnUnrelatedBuffer) {
    uint8_t *dev = nullptr;
    ASSERT_NO_FATAL_FAILURE(allocDevice(&dev, kBufSize));
    RegisteredHost host(transport_, kBufSize), other(transport_, kBufSize);
    ASSERT_TRUE(host.status.ok());
    ASSERT_TRUE(other.status.ok());
    auto request = makeRequest(Request::WRITE, host.bytes.data(), (uint64_t)dev,
                               kChunk + 1);
    auto queue = queueFor(request);
    ASSERT_NE(queue, nullptr);
    QueueGate gate(queue);
    Transport::SubBatchRef batch = nullptr;
    ASSERT_TRUE(transport_.allocateSubBatch(batch, 1).ok());
    ASSERT_TRUE(transport_.submitTransferTasks(batch, {request}).ok());
    auto removed = std::async(std::launch::async, [&] {
        return transport_.removeMemoryBuffer(other.desc);
    });
    EXPECT_EQ(removed.wait_for(std::chrono::milliseconds(100)),
              std::future_status::ready);
    gate.release();
    EXPECT_TRUE(removed.get().ok());
    EXPECT_TRUE(transport_.freeSubBatch(batch).ok());
    EXPECT_TRUE(loader_->free(dev, kBufSize).ok());
}

TEST_F(MluTransportTest,
       QuiesceDrainsHealthyDirectionAfterOtherDirectionFails) {
    uint8_t *dev = nullptr;
    ASSERT_NO_FATAL_FAILURE(allocDevice(&dev, kBufSize));
    RegisteredHost host(transport_, kBufSize);
    ASSERT_TRUE(host.status.ok());
    auto d2h = queueFor(makeRequest(Request::WRITE, dev,
                                    (uint64_t)host.bytes.data(), kChunk + 1));
    auto h2d_request = makeRequest(Request::WRITE, host.bytes.data(),
                                   (uint64_t)dev, kChunk + 1);
    auto h2d = queueFor(h2d_request);
    ASSERT_NE(d2h, nullptr);
    ASSERT_NE(h2d, nullptr);
    QueueGate gate(h2d);
    Transport::SubBatchRef batch = nullptr;
    ASSERT_TRUE(transport_.allocateSubBatch(batch, 1).ok());
    ASSERT_TRUE(transport_.submitTransferTasks(batch, {h2d_request}).ok());
    fail_queue = d2h->handle;
    auto drain =
        std::async(std::launch::async, [&] { return transport_.quiesce(); });
    EXPECT_EQ(drain.wait_for(std::chrono::milliseconds(50)),
              std::future_status::timeout);
    gate.release();
    EXPECT_FALSE(drain.get().ok());
    EXPECT_EQ(cnQueryQueue(h2d->handle), CN_SUCCESS);
    fail_queue = nullptr;
    EXPECT_TRUE(transport_.freeSubBatch(batch).ok());
    EXPECT_TRUE(loader_->free(dev, kBufSize).ok());
}

TEST_F(MluTransportTest, AmbiguousSubmitFailureRecoversOnlyAfterQueueDrains) {
    uint8_t *dev = nullptr;
    ASSERT_NO_FATAL_FAILURE(allocDevice(&dev, kBufSize));
    RegisteredHost host(transport_, kBufSize);
    ASSERT_TRUE(host.status.ok());
    auto request = makeRequest(Request::WRITE, host.bytes.data(), (uint64_t)dev,
                               kChunk + 1);
    auto queue = queueFor(request);
    ASSERT_NE(queue, nullptr);
    QueueGate gate(queue);
    Transport::SubBatchRef batch = nullptr;
    ASSERT_TRUE(transport_.allocateSubBatch(batch, 1).ok());
    reject_copy = CN_INVOKE_ERROR_TIMEOUT;
    ASSERT_TRUE(transport_.submitTransferTasks(batch, {request}).ok());
    TransferStatus status{};
    EXPECT_TRUE(transport_.getTransferStatus(batch, 0, status).ok());
    EXPECT_EQ(status.s, PENDING);
    EXPECT_TRUE(static_cast<MluSubBatch *>(batch)->task_list[0].drain_on_error);
    gate.release();
    EXPECT_EQ(wait(batch).s, FAILED);
    EXPECT_TRUE(transport_.freeSubBatch(batch).ok());
    EXPECT_EQ(run(request).s, COMPLETED);
    request.length = kChunk;
    EXPECT_EQ(run(request).s, COMPLETED);
    EXPECT_TRUE(loader_->free(dev, kBufSize).ok());
}

TEST_F(MluTransportTest, UnregisterWaitsForOwnNotifierNotLaterQueueWork) {
    uint8_t *dev = nullptr;
    ASSERT_NO_FATAL_FAILURE(allocDevice(&dev, 2 * kBufSize));
    RegisteredHost host(transport_, kBufSize), other(transport_, kBufSize);
    ASSERT_TRUE(host.status.ok());
    ASSERT_TRUE(other.status.ok());
    Transport::SubBatchRef batch = nullptr;
    ASSERT_TRUE(transport_.allocateSubBatch(batch, 2).ok());
    auto first = makeRequest(Request::WRITE, host.bytes.data(), (uint64_t)dev,
                             kChunk + 1);
    ASSERT_TRUE(transport_.submitTransferTasks(batch, {first}).ok());
    auto queue = static_cast<MluSubBatch *>(batch)->task_list[0].queue;
    ASSERT_NE(queue, nullptr);
    QueueGate gate(queue);
    auto second = makeRequest(Request::WRITE, other.bytes.data(),
                              (uint64_t)(dev + kBufSize), kChunk + 1);
    ASSERT_TRUE(transport_.submitTransferTasks(batch, {second}).ok());
    auto removed = std::async(std::launch::async, [&] {
        return transport_.removeMemoryBuffer(host.desc);
    });
    EXPECT_EQ(removed.wait_for(std::chrono::milliseconds(200)),
              std::future_status::ready);
    TransferStatus status{};
    EXPECT_TRUE(transport_.getTransferStatus(batch, 1, status).ok());
    EXPECT_EQ(status.s, PENDING);
    gate.release();
    EXPECT_TRUE(removed.get().ok());
    EXPECT_TRUE(transport_.freeSubBatch(batch).ok());
    EXPECT_TRUE(loader_->free(dev, 2 * kBufSize).ok());
}

}  // namespace
}  // namespace tent
}  // namespace mooncake
