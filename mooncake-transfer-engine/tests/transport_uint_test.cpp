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

#include <array>
#include <condition_variable>
#include <cstdlib>
#include <fstream>
#include <future>
#include <iomanip>
#include <memory>
#include <mutex>
#include <utility>

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
}  // namespace mooncake

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
