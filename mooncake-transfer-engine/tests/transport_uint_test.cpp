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

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <sys/time.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdlib>
#include <fstream>
#include <future>
#include <iomanip>
#include <memory>
#include <mutex>
#include <thread>
#include <utility>

#include "common.h"
#include "config.h"
#include "multi_transport.h"
#include "transfer_engine.h"
#include "transfer_engine_impl.h"
#include "transport/transport.h"

using namespace mooncake;

namespace mooncake {

class TransferEngineImplTestPeer {
   public:
    static void replaceTransports(TransferEngineImpl& engine,
                                  std::shared_ptr<Transport> transport) {
        engine.multi_transports_->transport_map_.clear();
        engine.multi_transports_->transport_map_.emplace("blocking",
                                                         std::move(transport));
    }

    static void replaceTransports(
        TransferEngineImpl& engine,
        const std::vector<std::pair<std::string, std::shared_ptr<Transport>>>&
            transports) {
        engine.multi_transports_->transport_map_.clear();
        for (const auto& [name, transport] : transports) {
            engine.multi_transports_->transport_map_.emplace(name, transport);
        }
    }

    static TransferEngineImpl& implementation(TransferEngine& engine) {
        return *engine.impl_;
    }

    static AutoDiscoverConfig autoDiscoverConfig(
        const TransferEngineImpl& engine) {
        return engine.auto_discover_config_;
    }

    static std::string autoDiscoverTransport(const TransferEngineImpl& engine) {
        return engine.autoDiscoverTransport();
    }

    static void setUseBarex(TransferEngineImpl& engine, bool use_barex) {
        engine.use_barex_ = use_barex;
    }

    static void prepareBusyBatch(MultiTransport& transport,
                                 Transport::BatchID batch_id,
                                 Transport* task_transport) {
        auto& batch = Transport::toBatchDesc(batch_id);
        auto& task = batch.task_list.emplace_back();
        task.batch_id = batch_id;
        task.slice_count = 1;
        task.transport_ = task_transport;
    }

    static Transport::Slice* prepareAgedSliceBatch(
        MultiTransport& transport, Transport::BatchID batch_id,
        int64_t slice_age_ns) {
        auto& batch = Transport::toBatchDesc(batch_id);
        auto& task = batch.task_list.emplace_back();
        task.batch_id = batch_id;
        task.slice_count = 1;

        auto* slice = new Transport::Slice();
        slice->source_addr = nullptr;
        slice->length = 1;
        slice->status = Transport::Slice::PENDING;
        slice->task = &task;
        slice->ts = getCurrentTimeInNano() - slice_age_ns;
        task.slice_list.push_back(slice);
        return slice;
    }

#ifdef USE_EVENT_DRIVEN_COMPLETION
    static void prepareFinishedBatchWithActiveCallback(
        MultiTransport& transport, Transport::BatchID batch_id) {
        auto& batch = Transport::toBatchDesc(batch_id);
        auto& task = batch.task_list.emplace_back();
        task.batch_id = batch_id;
        task.slice_count = 1;
        task.is_finished = true;
        batch.active_completion_callbacks.store(1, std::memory_order_release);
    }

    static void finishActiveCompletionCallback(MultiTransport& transport,
                                               Transport::BatchID batch_id) {
        auto& batch = Transport::toBatchDesc(batch_id);
        batch.active_completion_callbacks.store(0, std::memory_order_release);
    }
#endif

    static bool isCleanupDeferred(MultiTransport& transport,
                                  Transport::BatchID batch_id) {
        std::lock_guard<std::mutex> guard(transport.deferred_cleanup_mutex_);
        return transport.deferred_cleanup_batches_.count(batch_id) != 0;
    }
};

TEST(TransferEngineAutoDiscoverTest, SelectsEfaForEfaProtocol) {
    TransferEngineImpl engine(false);
    engine.setAutoDiscover({.enabled = true, .protocol = "efa"});

    const auto config = TransferEngineImplTestPeer::autoDiscoverConfig(engine);
    EXPECT_TRUE(config.enabled);
    EXPECT_EQ(config.protocol, "efa");
    EXPECT_EQ(TransferEngineImplTestPeer::autoDiscoverTransport(engine), "efa");
}

TEST(TransferEngineAutoDiscoverTest, BarexOverrideTakesPrecedence) {
    TransferEngineImpl engine(false);
    engine.setAutoDiscover({.enabled = true, .protocol = "efa"});
    TransferEngineImplTestPeer::setUseBarex(engine, true);

    EXPECT_EQ(TransferEngineImplTestPeer::autoDiscoverTransport(engine),
              "barex");
}

TEST(TransferEngineAutoDiscoverTest, BoolSetterPreservesDefaultSelection) {
    TransferEngineImpl engine(false);
    engine.setAutoDiscover({.enabled = true, .protocol = "efa"});
    engine.setAutoDiscover(true);

    const auto config = TransferEngineImplTestPeer::autoDiscoverConfig(engine);
    EXPECT_TRUE(config.enabled);
    EXPECT_TRUE(config.protocol.empty());
    EXPECT_EQ(TransferEngineImplTestPeer::autoDiscoverTransport(engine),
              "rdma");
}

class BatchResultTransport : public Transport {
   public:
    explicit BatchResultTransport(int unregister_result = 0)
        : unregister_result_(unregister_result) {}

    int unregisterBatchCalls() const { return unregister_batch_calls_; }
    size_t registeredBufferCount() const { return registered_buffers_.size(); }
    void setRegisterResult(int result) { register_result_ = result; }

    Status submitTransfer(BatchID,
                          const std::vector<TransferRequest>&) override {
        return Status::OK();
    }

    Status getTransferStatus(BatchID, size_t, TransferStatus&) override {
        return Status::OK();
    }

   private:
    int registerLocalMemory(void*, size_t, const std::string&, bool,
                            bool) override {
        return 0;
    }

    int unregisterLocalMemory(void*, bool) override { return 0; }

    int registerLocalMemoryBatch(const std::vector<BufferEntry>& buffer_list,
                                 const std::string&) override {
        if (register_result_) {
            if (!buffer_list.empty()) {
                registered_buffers_.push_back(buffer_list.front().addr);
            }
            return register_result_;
        }
        for (const auto& buffer : buffer_list) {
            registered_buffers_.push_back(buffer.addr);
        }
        return 0;
    }

    int unregisterLocalMemoryBatch(
        const std::vector<void*>& addr_list) override {
        ++unregister_batch_calls_;
        for (void* addr : addr_list) {
            registered_buffers_.erase(
                std::remove(registered_buffers_.begin(),
                            registered_buffers_.end(), addr),
                registered_buffers_.end());
        }
        return unregister_result_;
    }

    const char* getName() const override { return "batch-result"; }

    int register_result_ = 0;
    int unregister_result_;
    int unregister_batch_calls_ = 0;
    std::vector<void*> registered_buffers_;
};

class BlockingRegistrationTransport : public Transport {
   public:
    explicit BlockingRegistrationTransport(int first_registration_result = 0)
        : first_registration_result_(first_registration_result) {}

    void waitForFirstRegistration() {
        std::unique_lock<std::mutex> lock(mutex_);
        cv_.wait(lock, [this] { return first_registration_started_; });
    }

    void releaseFirstRegistration() {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            release_first_registration_ = true;
        }
        cv_.notify_all();
    }

    int registrationCalls() {
        std::lock_guard<std::mutex> lock(mutex_);
        return registration_calls_;
    }

    Status submitTransfer(BatchID,
                          const std::vector<TransferRequest>&) override {
        return Status::OK();
    }

    Status getTransferStatus(BatchID, size_t, TransferStatus&) override {
        return Status::OK();
    }

   private:
    int waitOnFirstRegistration() {
        std::unique_lock<std::mutex> lock(mutex_);
        ++registration_calls_;
        if (registration_calls_ == 1) {
            first_registration_started_ = true;
            cv_.notify_all();
            cv_.wait(lock, [this] { return release_first_registration_; });
            return first_registration_result_;
        }
        return 0;
    }

    int registerLocalMemory(void*, size_t, const std::string&, bool,
                            bool) override {
        return waitOnFirstRegistration();
    }

    int unregisterLocalMemory(void*, bool) override { return 0; }

    int registerLocalMemoryBatch(const std::vector<BufferEntry>&,
                                 const std::string&) override {
        return waitOnFirstRegistration();
    }

    int unregisterLocalMemoryBatch(const std::vector<void*>&) override {
        return 0;
    }

    const char* getName() const override { return "blocking"; }

    std::mutex mutex_;
    std::condition_variable cv_;
    int first_registration_result_;
    int registration_calls_ = 0;
    bool first_registration_started_ = false;
    bool release_first_registration_ = false;
};

class PartialFailureSubmissionTransport : public BatchResultTransport {
   public:
    Status submitTransferTask(
        const std::vector<TransferTask*>& tasks) override {
        for (auto* task : tasks) {
            request_counts.push_back(task->request_count);
            task->slice_count = task->request_count;
            task->success_slice_count = 1;
            task->failed_slice_count = task->request_count - 1;
            for (size_t i = 0; i < task->request_count; ++i) {
                auto* slice = new Slice{};
                slice->length = task->request[i].length;
                slice->status = i == 0 ? Slice::SUCCESS : Slice::FAILED;
                task->slice_list.push_back(slice);
            }
            if (extra_slice_) task->slice_list.push_back(new Slice{});
            task->is_finished = true;
        }
        return Status::InvalidArgument("synthetic submit failure");
    }

    Status getTransferStatus(BatchID, size_t, TransferStatus& status) override {
        status.s = TransferStatusEnum::FAILED;
        return Status::OK();
    }

    bool supportsGroupedScatter() const override { return true; }

    void addExtraSlice() { extra_slice_ = true; }

    std::vector<size_t> request_counts;

   private:
    bool extra_slice_ = false;
};

class ScopedSliceTimeout {
   public:
    explicit ScopedSliceTimeout(int64_t timeout_sec)
        : old_timeout_(globalConfig().slice_timeout) {
        globalConfig().slice_timeout = timeout_sec;
    }

    ~ScopedSliceTimeout() { globalConfig().slice_timeout = old_timeout_; }

   private:
    int64_t old_timeout_;
};

class TimeoutThenFailureTransport : public BatchResultTransport {
   public:
    Status getTransferStatus(BatchID, size_t, TransferStatus& status) override {
        status.s = status_.load(std::memory_order_acquire);
        return Status::OK();
    }

    void fail() {
        status_.store(TransferStatusEnum::FAILED, std::memory_order_release);
    }

   private:
    std::atomic<TransferStatusEnum> status_{TransferStatusEnum::TIMEOUT};
};

class TransportTest : public ::testing::Test {
   protected:
    void SetUp() override {
        google::InitGoogleLogging("TransportTest");
        FLAGS_logtostderr = 1;
    }

    void TearDown() override { google::ShutdownGoogleLogging(); }
};

static int CreateTempFile() {
    char temp_filename[] = "/tmp/testfileXXXXXX";
    int fd = mkstemp(temp_filename);
    if (fd == -1) {
        return -1;
    }
    unlink(temp_filename);
    return fd;
}

int CreateTempFileWithContent(const char* content) {
    char temp_filename[] = "/tmp/testfileXXXXXX";
    int fd = mkstemp(temp_filename);
    if (fd == -1) {
        return -1;
    }
    unlink(temp_filename);

    ssize_t nbytes = write(fd, content, strlen(content));
    (void)nbytes;
    lseek(fd, 0, SEEK_SET);

    return fd;
}

TEST_F(TransportTest, parseHostNameWithPortTest) {
    std::string local_server_name = "0.0.0.0:1234";
    auto res = parseHostNameWithPort(local_server_name);
    ASSERT_EQ(res.first, "0.0.0.0");
    ASSERT_EQ(res.second, 1234);

    local_server_name = "1.2.3.4:111111";
    res = parseHostNameWithPort(local_server_name);
    ASSERT_EQ(res.first, "1.2.3.4");
    ASSERT_EQ(res.second, 12001);
}

TEST_F(TransportTest, RealSliceTimeoutDefersBatchCleanup) {
    ScopedSliceTimeout timeout(/*timeout_sec=*/1);
    std::string server_name = "unit-test-server:1234";
    MultiTransport multi_transport(nullptr, server_name);
    auto batch_id = multi_transport.allocateBatchID(1);
    auto* slice = TransferEngineImplTestPeer::prepareAgedSliceBatch(
        multi_transport, batch_id, /*slice_age_ns=*/2'000'000'000LL);

    Transport::TransferStatus status;
    ASSERT_EQ(multi_transport.getTransferStatus(batch_id, 0, status),
              Status::OK());
    EXPECT_EQ(status.s, Transport::TransferStatusEnum::TIMEOUT);

    Status free_status = multi_transport.freeBatchID(batch_id);
    EXPECT_TRUE(free_status.IsBatchBusy());
    EXPECT_EQ(free_status.message(),
              "BatchID is invalid because cleanup has been deferred");
    EXPECT_TRUE(TransferEngineImplTestPeer::isCleanupDeferred(multi_transport,
                                                              batch_id));

    slice->markFailed();
    constexpr auto kCleanupDeadline = std::chrono::seconds(2);
    const auto deadline = std::chrono::steady_clock::now() + kCleanupDeadline;
    while (TransferEngineImplTestPeer::isCleanupDeferred(multi_transport,
                                                         batch_id) &&
           std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    EXPECT_FALSE(TransferEngineImplTestPeer::isCleanupDeferred(multi_transport,
                                                               batch_id));
}

TEST_F(TransportTest, TimedOutBatchIsReclaimedAfterCallbacksFinish) {
    std::string server_name = "unit-test-server:1234";
    TimeoutThenFailureTransport task_transport;
    MultiTransport multi_transport(nullptr, server_name);
    auto batch_id = multi_transport.allocateBatchID(1);
    TransferEngineImplTestPeer::prepareBusyBatch(multi_transport, batch_id,
                                                 &task_transport);

    Status free_status = multi_transport.freeBatchID(batch_id);
    EXPECT_TRUE(free_status.IsBatchBusy());
    EXPECT_EQ(free_status.message(),
              "BatchID is invalid because cleanup has been deferred");
    EXPECT_TRUE(TransferEngineImplTestPeer::isCleanupDeferred(multi_transport,
                                                              batch_id));

    task_transport.fail();
    constexpr auto kCleanupDeadline = std::chrono::seconds(2);
    const auto deadline = std::chrono::steady_clock::now() + kCleanupDeadline;
    while (TransferEngineImplTestPeer::isCleanupDeferred(multi_transport,
                                                         batch_id) &&
           std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    EXPECT_FALSE(TransferEngineImplTestPeer::isCleanupDeferred(multi_transport,
                                                               batch_id));
}

#ifdef USE_EVENT_DRIVEN_COMPLETION
TEST_F(TransportTest, FinishedBatchWaitsForCompletionCallbackQuiescence) {
    std::string server_name = "unit-test-server:1234";
    MultiTransport multi_transport(nullptr, server_name);
    auto batch_id = multi_transport.allocateBatchID(1);
    TransferEngineImplTestPeer::prepareFinishedBatchWithActiveCallback(
        multi_transport, batch_id);

    Status free_status = multi_transport.freeBatchID(batch_id);
    EXPECT_TRUE(free_status.IsBatchBusy());
    EXPECT_EQ(free_status.message(),
              "BatchID is invalid because cleanup has been deferred");
    EXPECT_TRUE(TransferEngineImplTestPeer::isCleanupDeferred(multi_transport,
                                                              batch_id));

    TransferEngineImplTestPeer::finishActiveCompletionCallback(multi_transport,
                                                               batch_id);
    constexpr auto kCleanupDeadline = std::chrono::seconds(2);
    const auto deadline = std::chrono::steady_clock::now() + kCleanupDeadline;
    while (TransferEngineImplTestPeer::isCleanupDeferred(multi_transport,
                                                         batch_id) &&
           std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    EXPECT_FALSE(TransferEngineImplTestPeer::isCleanupDeferred(multi_transport,
                                                               batch_id));
}
#endif

TEST_F(TransportTest, TransferTaskDestructorRunsSliceCleanup) {
    int cleanup_count = 0;
    {
        Transport::TransferTask task;
        auto* slice = new Transport::Slice();
        slice->source_addr = &cleanup_count;
        slice->cleanup_callback = [](Transport::Slice* released) {
            auto* count = static_cast<int*>(released->source_addr);
            ++*count;
        };
        task.slice_list.push_back(slice);
    }

    EXPECT_EQ(cleanup_count, 1);
}

TEST_F(TransportTest, SliceCleanupRunsOnceBeforeCacheReuse) {
    Transport::ThreadLocalSliceCache cache;
    int cleanup_count = 0;

    Transport::Slice* slice = cache.allocate();
    slice->source_addr = &cleanup_count;
    slice->cleanup_callback = [](Transport::Slice* released) {
        auto* count = static_cast<int*>(released->source_addr);
        ++*count;
    };

    cache.deallocate(slice);
    EXPECT_EQ(cleanup_count, 1);

    Transport::Slice* reused = cache.allocate();
    EXPECT_EQ(reused, slice);
    EXPECT_EQ(reused->cleanup_callback, nullptr);

    // A backend that does not install a callback must not inherit the callback
    // from the previous owner of this cached slice.
    cache.deallocate(reused);
    EXPECT_EQ(cleanup_count, 1);
}

TEST_F(TransportTest, WriteSuccess) {
    int fd = CreateTempFile();
    ASSERT_NE(fd, -1) << "Failed to create temporary file";

    const char* testData = "Hello, World!";
    size_t testDataLen = strlen(testData);

    ssize_t result = writeFully(fd, testData, testDataLen);
    EXPECT_EQ(result, static_cast<ssize_t>(testDataLen));

    char buffer[256] = {0};
    ssize_t nbytes = lseek(fd, 0, SEEK_SET);
    (void)nbytes;
    nbytes = read(fd, buffer, testDataLen);
    (void)nbytes;
    EXPECT_STREQ(buffer, testData);

    close(fd);
}

TEST_F(TransportTest, WriteInvalidFD) {
    const char* testData = "Hello, World!";
    size_t testDataLen = strlen(testData);

    ssize_t result = writeFully(-1, testData, testDataLen);
    ASSERT_EQ(result, -1);
    ASSERT_EQ(errno, EBADF);
}

TEST_F(TransportTest, PartialWrite) {
    int fd = CreateTempFile();
    ASSERT_NE(fd, -1) << "Failed to create temporary file";

    const char* testData = "Hello, World!";
    size_t testDataLen = strlen(testData);

    ssize_t result = writeFully(fd, testData, testDataLen / 2);

    ASSERT_EQ(result, static_cast<ssize_t>(testDataLen / 2));

    char buffer[256] = {0};
    lseek(fd, 0, SEEK_SET);
    ssize_t nbytes = read(fd, buffer, result);
    (void)nbytes;
    ASSERT_EQ(strncmp(buffer, testData, result), 0);
    close(fd);
}

TEST_F(TransportTest, ReadSuccess) {
    const char* testData = "Hello, World!";
    int fd = CreateTempFileWithContent(testData);
    ASSERT_NE(fd, -1) << "Failed to create temporary file";

    char buffer[256] = {0};
    ssize_t bytesRead = readFully(fd, buffer, sizeof(buffer));

    EXPECT_EQ(bytesRead, static_cast<ssize_t>(strlen(testData)));
    EXPECT_STREQ(buffer, testData);

    close(fd);
}

TEST_F(TransportTest, ReadInvalidFD) {
    char buffer[256] = {0};
    ssize_t bytesRead = readFully(-1, buffer, sizeof(buffer));
    EXPECT_EQ(bytesRead, -1);
    EXPECT_EQ(errno, EBADF);
}

TEST_F(TransportTest, PartialRead) {
    const char* testData = "Hello, World!";
    int fd = CreateTempFileWithContent(testData);
    ASSERT_NE(fd, -1) << "Failed to create temporary file";

    char buffer[256] = {0};
    size_t half_len = strlen(testData) / 2;
    ssize_t bytesRead = readFully(fd, buffer, half_len);

    EXPECT_EQ(bytesRead, static_cast<ssize_t>(half_len));
    EXPECT_EQ(strncmp(buffer, testData, half_len), 0);

    close(fd);
}

TEST_F(TransportTest, ReadEmptyFile) {
    int fd = CreateTempFileWithContent("");
    ASSERT_NE(fd, -1) << "Failed to create temporary file";

    char buffer[256] = {0};
    ssize_t bytesRead = readFully(fd, buffer, sizeof(buffer));

    EXPECT_EQ(bytesRead, static_cast<ssize_t>(0));

    close(fd);
}

TEST_F(TransportTest, RegisterLocalMemoryBatchRejectsOverlappingBuffers) {
    TransferEngine engine(false);
    ASSERT_EQ(engine.init(P2PHANDSHAKE, "127.0.0.1:12345"), 0);

    std::array<char, 256> buffer{};
    std::vector<BufferEntry> entries = {
        {buffer.data() + 64, 128},
        {buffer.data(), 128},
    };

    EXPECT_EQ(engine.registerLocalMemoryBatch(entries, "cpu:0"),
              ERR_ADDRESS_OVERLAPPED);
}

TEST_F(TransportTest, RegisterLocalMemoryBatchRejectsZeroLengthBuffer) {
    TransferEngine engine(false);
    ASSERT_EQ(engine.init(P2PHANDSHAKE, "127.0.0.1:12345"), 0);

    std::array<char, 1> buffer{};
    std::vector<BufferEntry> entries = {
        {buffer.data(), 0},
    };

    EXPECT_EQ(engine.registerLocalMemoryBatch(entries, "cpu:0"),
              ERR_INVALID_ARGUMENT);
}

TEST_F(TransportTest, RegisterLocalMemoryBatchAllowsAdjacentBuffers) {
    TransferEngine engine(false);
    ASSERT_EQ(engine.init(P2PHANDSHAKE, "127.0.0.1:12345"), 0);

    std::array<char, 256> buffer{};
    std::vector<BufferEntry> entries = {
        {buffer.data() + 128, 128},
        {buffer.data(), 128},
    };

    EXPECT_EQ(engine.registerLocalMemoryBatch(entries, "cpu:0"), 0);
}

TEST_F(TransportTest, ConcurrentRegisterLocalMemoryRejectsOverlap) {
    TransferEngineImpl engine(false);
    ASSERT_EQ(engine.init(P2PHANDSHAKE, "127.0.0.1:12345"), 0);
    auto transport = std::make_shared<BlockingRegistrationTransport>();
    TransferEngineImplTestPeer::replaceTransports(engine, transport);

    std::array<char, 128> buffer{};
    auto first = std::async(std::launch::async, [&] {
        return engine.registerLocalMemory(buffer.data(), buffer.size(),
                                          "cpu:0");
    });
    transport->waitForFirstRegistration();

    int second =
        engine.registerLocalMemory(buffer.data(), buffer.size(), "cpu:0");
    int registration_calls = transport->registrationCalls();
    transport->releaseFirstRegistration();

    EXPECT_EQ(second, ERR_ADDRESS_OVERLAPPED);
    EXPECT_EQ(registration_calls, 1);
    EXPECT_EQ(first.get(), 0);
    EXPECT_EQ(engine.unregisterLocalMemory(buffer.data()), 0);
}

TEST_F(TransportTest, ConcurrentRegisterLocalMemoryBatchRejectsOverlap) {
    TransferEngineImpl engine(false);
    ASSERT_EQ(engine.init(P2PHANDSHAKE, "127.0.0.1:12345"), 0);
    auto transport = std::make_shared<BlockingRegistrationTransport>();
    TransferEngineImplTestPeer::replaceTransports(engine, transport);

    std::array<char, 128> buffer{};
    std::vector<BufferEntry> entries = {{buffer.data(), buffer.size()}};
    auto first = std::async(std::launch::async, [&] {
        return engine.registerLocalMemoryBatch(entries, "cpu:0");
    });
    transport->waitForFirstRegistration();

    int second = engine.registerLocalMemoryBatch(entries, "cpu:0");
    int registration_calls = transport->registrationCalls();
    transport->releaseFirstRegistration();

    EXPECT_EQ(second, ERR_ADDRESS_OVERLAPPED);
    EXPECT_EQ(registration_calls, 1);
    EXPECT_EQ(first.get(), 0);
    EXPECT_EQ(engine.unregisterLocalMemoryBatch({buffer.data()}), 0);
}

TEST_F(TransportTest, FailedRegistrationReleasesReservedRegion) {
    TransferEngineImpl engine(false);
    ASSERT_EQ(engine.init(P2PHANDSHAKE, "127.0.0.1:12345"), 0);
    auto transport =
        std::make_shared<BlockingRegistrationTransport>(ERR_MEMORY);
    TransferEngineImplTestPeer::replaceTransports(engine, transport);

    std::array<char, 128> buffer{};
    auto first = std::async(std::launch::async, [&] {
        return engine.registerLocalMemory(buffer.data(), buffer.size(),
                                          "cpu:0");
    });
    transport->waitForFirstRegistration();
    transport->releaseFirstRegistration();

    EXPECT_EQ(first.get(), ERR_MEMORY);
    EXPECT_EQ(engine.registerLocalMemory(buffer.data(), buffer.size(), "cpu:0"),
              0);
    EXPECT_EQ(engine.unregisterLocalMemory(buffer.data()), 0);
}

TEST_F(TransportTest, UnregisterLocalMemoryBatchPropagatesTransportError) {
    TransferEngine engine(false);
    ASSERT_EQ(engine.init(P2PHANDSHAKE, "127.0.0.1:12345"), 0);
    ASSERT_NE(engine.installTransport("tcp", nullptr), nullptr);

    std::array<char, 1> buffer{};
    EXPECT_EQ(engine.unregisterLocalMemoryBatch({buffer.data()}),
              ERR_ADDRESS_NOT_REGISTERED);
}

TEST_F(TransportTest, UnregisterLocalMemoryBatchContinuesAcrossTransports) {
    TransferEngineImpl engine(false);
    ASSERT_EQ(engine.init(P2PHANDSHAKE, "127.0.0.1:12345"), 0);
    auto failing = std::make_shared<BatchResultTransport>(ERR_MEMORY);
    auto succeeding = std::make_shared<BatchResultTransport>();
    TransferEngineImplTestPeer::replaceTransports(
        engine, {{"a-failing", failing}, {"b-succeeding", succeeding}});

    std::array<char, 1> buffer{};
    EXPECT_EQ(engine.unregisterLocalMemoryBatch({buffer.data()}), ERR_MEMORY);
    EXPECT_EQ(failing->unregisterBatchCalls(), 1);
    EXPECT_EQ(succeeding->unregisterBatchCalls(), 1);
}

TEST_F(TransportTest, UnregisterLocalMemoryBatchContinuesAfterAddressError) {
    TransferEngineImpl engine(false);
    ASSERT_EQ(engine.init(P2PHANDSHAKE, "127.0.0.1:12345"), 0);
    ASSERT_NE(engine.installTransport("tcp", nullptr), nullptr);

    std::array<char, 2> registered{};
    std::array<char, 1> missing{};
    std::vector<BufferEntry> entries = {
        {registered.data(), 1},
        {registered.data() + 1, 1},
    };
    ASSERT_EQ(engine.registerLocalMemoryBatch(entries, "cpu:0"), 0);

    auto metadata = engine.getMetadata();
    ASSERT_NE(metadata, nullptr);
    auto contains_buffer = [&](void* addr) {
        auto desc = metadata->getSegmentDescByID(LOCAL_SEGMENT_ID);
        if (!desc) return false;
        auto value = reinterpret_cast<uintptr_t>(addr);
        return std::any_of(
            desc->buffers.begin(), desc->buffers.end(),
            [value](const auto& buffer) { return buffer.addr == value; });
    };
    ASSERT_TRUE(contains_buffer(registered.data()));
    ASSERT_TRUE(contains_buffer(registered.data() + 1));

    EXPECT_EQ(engine.unregisterLocalMemoryBatch(
                  {missing.data(), registered.data(), registered.data() + 1}),
              ERR_ADDRESS_NOT_REGISTERED);
    EXPECT_FALSE(contains_buffer(registered.data()));
    EXPECT_FALSE(contains_buffer(registered.data() + 1));
}

TEST_F(TransportTest, RegisterLocalMemoryBatchRollsBackAttemptedTransports) {
    TransferEngineImpl engine(false);
    ASSERT_EQ(engine.init(P2PHANDSHAKE, "127.0.0.1:12345"), 0);
    auto succeeding = std::make_shared<BatchResultTransport>();
    auto failing = std::make_shared<BatchResultTransport>();
    failing->setRegisterResult(ERR_MEMORY);
    TransferEngineImplTestPeer::replaceTransports(
        engine, {{"a-succeeding", succeeding}, {"b-failing", failing}});

    std::array<char, 2> buffer{};
    std::vector<BufferEntry> entries = {
        {buffer.data(), 1},
        {buffer.data() + 1, 1},
    };
    EXPECT_EQ(engine.registerLocalMemoryBatch(entries, "cpu:0"), ERR_MEMORY);
    EXPECT_EQ(succeeding->registeredBufferCount(), 0);
    EXPECT_EQ(failing->registeredBufferCount(), 0);
    EXPECT_EQ(succeeding->unregisterBatchCalls(), 1);
    EXPECT_EQ(failing->unregisterBatchCalls(), 1);

    failing->setRegisterResult(0);
    EXPECT_EQ(engine.registerLocalMemoryBatch(entries, "cpu:0"), 0);
    EXPECT_EQ(succeeding->registeredBufferCount(), entries.size());
    EXPECT_EQ(failing->registeredBufferCount(), entries.size());
    EXPECT_EQ(
        engine.unregisterLocalMemoryBatch({buffer.data(), buffer.data() + 1}),
        0);
}

TEST_F(TransportTest, ScatterSubmitFailurePreservesCompletedFragments) {
    TransferEngine engine(false);
    ASSERT_EQ(engine.init(P2PHANDSHAKE, "127.0.0.1:12345"), 0);
    auto transport = std::make_shared<PartialFailureSubmissionTransport>();
    auto& impl = TransferEngineImplTestPeer::implementation(engine);
    TransferEngineImplTestPeer::replaceTransports(
        impl, {{"partial-failure", transport}});

    constexpr SegmentID kSegmentId = 12;
    auto descriptor = std::make_shared<TransferMetadata::SegmentDesc>();
    descriptor->name = "remote";
    descriptor->protocol = "partial-failure";
    impl.getMetadata()->addLocalSegment(kSegmentId, "remote",
                                        std::move(descriptor));
    std::array<char, 2> buffer{};
    std::array<size_t, 1> offsets{0};
    std::array<size_t, 1> lengths{1};

    auto run = [&] {
        std::vector<bool> fragment_ok;
        TransferEngine::ScatterTransferRange range{
            .opcode = TransferRequest::READ,
            .remote_segment = "remote",
            .remote_base_offset = 0,
            .remote_size = buffer.size(),
            .local_buffer = buffer.data(),
            .local_capacity = 1,
            .local_offsets = offsets,
            .remote_offsets = offsets,
            .lengths = lengths,
            .on_fragment_complete =
                [&](size_t, const Status& status) {
                    fragment_ok.push_back(status.ok());
                },
        };
        auto operation = engine.submitScatter({range, range});
        EXPECT_FALSE(operation.wait().ok());
        return fragment_ok;
    };

    EXPECT_EQ(run(), (std::vector<bool>{true, false}));
    EXPECT_EQ(transport->request_counts, (std::vector<size_t>{2}));
    transport->addExtraSlice();
    EXPECT_EQ(run(), (std::vector<bool>{false, false}));
}

#ifdef USE_EVENT_DRIVEN_COMPLETION
TEST_F(TransportTest, GroupedTaskCompletionWaitsForSubmissionSeal) {
    Transport::BatchDesc batch{};
    batch.id = reinterpret_cast<Transport::BatchID>(&batch);
    batch.batch_size = 1;
    Transport::TransferTask task;
    task.batch_id = batch.id;
    task.request_count = 2;
    task.submission_sealed = false;

    auto complete_slice = [&] {
        __atomic_add_fetch(&task.slice_count, 1, __ATOMIC_ACQ_REL);
        Transport::Slice slice{};
        slice.task = &task;
        slice.length = 1;
        slice.markSuccess();
    };
    complete_slice();
    EXPECT_FALSE(task.is_finished);
    complete_slice();
    EXPECT_FALSE(task.is_finished);

    Transport::Slice::sealTaskSubmission(&task);
    EXPECT_TRUE(task.is_finished);
    EXPECT_TRUE(batch.is_finished.load());
    EXPECT_EQ(batch.finished_task_count.load(), 1);

    Transport::Slice::sealTaskSubmission(&task);
    EXPECT_EQ(batch.finished_task_count.load(), 1);
}
#endif

}  // namespace mooncake

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
