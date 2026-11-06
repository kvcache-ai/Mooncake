// Copyright 2026 KVCache.AI
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <gtest/gtest.h>
#include <infiniband/verbs.h>
#include <poll.h>
#include <sys/wait.h>

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#include "config.h"
#include "transfer_engine.h"

using namespace mooncake;

namespace {

bool rdmaDeviceUsable(ibv_device* device) {
    ibv_context* ctx = ibv_open_device(device);
    if (!ctx) return false;
    ibv_device_attr attr{};
    if (ibv_query_device(ctx, &attr) != 0) {
        ibv_close_device(ctx);
        return false;
    }
    bool ok = false;
    for (uint8_t port = 1; port <= attr.phys_port_cnt; ++port) {
        ibv_port_attr port_attr{};
        if (ibv_query_port(ctx, port, &port_attr) != 0) continue;
        if (port_attr.gid_tbl_len > 0 && port_attr.state == IBV_PORT_ACTIVE) {
            ok = true;
            break;
        }
    }
    ibv_close_device(ctx);
    return ok;
}

std::string pickRdmaDevice() {
    const char* override_name = std::getenv("MC_TEST_DEVICE_NAME");
    if (override_name && *override_name) return override_name;
    int num_devices = 0;
    ibv_device** list = ibv_get_device_list(&num_devices);
    if (!list || num_devices == 0) {
        if (list) ibv_free_device_list(list);
        return "";
    }
    std::string name;
    for (int i = 0; i < num_devices; ++i) {
        if (rdmaDeviceUsable(list[i])) {
            name = ibv_get_device_name(list[i]);
            break;
        }
    }
    ibv_free_device_list(list);
    return name;
}

bool sliceTimeoutDisablesEvents() {
    const char* value = std::getenv("MC_SLICE_TIMEOUT");
    if (!value || !*value) return false;
    return std::atoi(value) > 0;
}

bool waitFd(int fd, int timeout_ms) {
    pollfd pfd{};
    pfd.fd = fd;
    pfd.events = POLLIN;
    int rc = ::poll(&pfd, 1, timeout_ms);
    return rc > 0 && (pfd.revents & POLLIN);
}

}  // namespace

class RdmaCompletionFdTest : public ::testing::Test {
   protected:
    void SetUp() override {
        if (sliceTimeoutDisablesEvents()) {
            GTEST_SKIP() << "MC_SLICE_TIMEOUT disables completion notification";
        }
        device_ = pickRdmaDevice();
        if (device_.empty()) GTEST_SKIP() << "no usable RDMA device";
        const char* previous = std::getenv("MC_TEST_DEVICE_NAME");
        saved_device_ = previous ? std::optional<std::string>(previous)
                                 : std::nullopt;
        setenv("MC_TEST_DEVICE_NAME", device_.c_str(), 1);
        previous = std::getenv("MC_TE_FILTERS");
        saved_filters_ = previous ? std::optional<std::string>(previous)
                                  : std::nullopt;
        setenv("MC_TE_FILTERS", device_.c_str(), 1);
        loadGlobalConfig(globalConfig());
        sender_ = makeEngine();
        receiver_ = makeEngine();
        ASSERT_NE(sender_, nullptr);
        ASSERT_NE(receiver_, nullptr);
        receiver_name_ = receiver_->getLocalIpAndPort();
        ASSERT_GE(sender_->completionFd(), 0);
    }

    void TearDown() override {
        sender_.reset();
        receiver_.reset();
        if (saved_device_)
            setenv("MC_TEST_DEVICE_NAME", saved_device_->c_str(), 1);
        else
            unsetenv("MC_TEST_DEVICE_NAME");
        if (saved_filters_)
            setenv("MC_TE_FILTERS", saved_filters_->c_str(), 1);
        else
            unsetenv("MC_TE_FILTERS");
    }

    std::unique_ptr<TransferEngine> makeEngine() {
        auto engine = std::make_unique<TransferEngine>(
            true, std::vector<std::string>{device_});
        if (engine->init("P2PHANDSHAKE", "127.0.0.1:0")) return nullptr;
        return engine;
    }

    SegmentID openReceiver() {
        auto segment = sender_->openSegment(receiver_name_);
        EXPECT_NE(segment, static_cast<SegmentID>(-1));
        return segment;
    }

    BatchID submit(const std::vector<TransferRequest>& requests,
                   size_t capacity, bool notify) {
        BatchID batch = sender_->allocateBatchID(capacity);
        EXPECT_NE(batch, static_cast<BatchID>(0));
        if (notify) {
            EXPECT_TRUE(
                sender_->enableBatchCompletionNotification(batch).ok());
        }
        EXPECT_TRUE(sender_->submitTransfer(batch, requests).ok());
        return batch;
    }

    void release(BatchID batch) {
        Status freed = sender_->freeBatchID(batch);
        if (freed.IsBatchCleanupDeferred())
            sender_->waitUntilDeferredCleanupDone(batch);
        else
            EXPECT_TRUE(freed.ok());
    }

    std::string device_;
    std::string receiver_name_;
    std::optional<std::string> saved_device_;
    std::optional<std::string> saved_filters_;
    std::unique_ptr<TransferEngine> sender_;
    std::unique_ptr<TransferEngine> receiver_;
    std::vector<char> source_;
    std::vector<char> target_;
};

TEST_F(RdmaCompletionFdTest, OptInShortBatchWakesWithoutStatusQuery) {
    constexpr size_t kLen = 4096;
    constexpr int kTasks = 2;
    source_.assign(kTasks * kLen, 0);
    target_.assign(kTasks * kLen, 0);
    for (int i = 0; i < kTasks; ++i)
        std::memset(source_.data() + i * kLen, 'A' + i, kLen);
    ASSERT_EQ(sender_->registerLocalMemory(source_.data(), source_.size()), 0);
    ASSERT_EQ(receiver_->registerLocalMemory(target_.data(), target_.size()),
              0);
    SegmentID segment = openReceiver();
    ASSERT_NE(segment, static_cast<SegmentID>(-1));

    std::vector<TransferRequest> requests(kTasks);
    for (int i = 0; i < kTasks; ++i) {
        requests[i].opcode = TransferRequest::WRITE;
        requests[i].source = source_.data() + i * kLen;
        requests[i].target_id = segment;
        requests[i].target_offset =
            reinterpret_cast<uint64_t>(target_.data() + i * kLen);
        requests[i].length = kLen;
    }
    BatchID batch = submit(requests, /*capacity=*/4, /*notify=*/true);
    ASSERT_TRUE(waitFd(sender_->completionFd(), 10000));
    std::vector<BatchCompletion> done;
    ASSERT_TRUE(sender_->drainCompletions(done).ok());
    ASSERT_EQ(done.size(), 1u);
    EXPECT_EQ(done[0].batch_id, batch);
    EXPECT_EQ(done[0].status.s, TransferStatusEnum::COMPLETED);
    EXPECT_EQ(done[0].status.transferred_bytes, kTasks * kLen);

    TransferStatus status{};
    ASSERT_TRUE(sender_->getBatchTransferStatus(batch, status).ok());
    EXPECT_EQ(status.s, done[0].status.s);
    EXPECT_EQ(status.transferred_bytes, done[0].status.transferred_bytes);
    EXPECT_EQ(0, std::memcmp(source_.data(), target_.data(), source_.size()));
    std::vector<BatchCompletion> again;
    ASSERT_TRUE(sender_->drainCompletions(again).ok());
    EXPECT_TRUE(again.empty());
    release(batch);
}

TEST_F(RdmaCompletionFdTest, UnregisteredRemoteFailsAfterEveryTaskSettles) {
    constexpr size_t kLen = 4096;
    source_.assign(2 * kLen, 'B');
    target_.assign(kLen, 0);
    std::vector<char> unregistered(kLen, 'Z');
    ASSERT_EQ(sender_->registerLocalMemory(source_.data(), source_.size()), 0);
    ASSERT_EQ(receiver_->registerLocalMemory(target_.data(), target_.size()),
              0);
    SegmentID segment = openReceiver();
    ASSERT_NE(segment, static_cast<SegmentID>(-1));

    std::vector<TransferRequest> requests(2);
    for (int i = 0; i < 2; ++i) {
        requests[i].opcode = TransferRequest::WRITE;
        requests[i].source = source_.data() + i * kLen;
        requests[i].target_id = segment;
        requests[i].length = kLen;
    }
    requests[0].target_offset = reinterpret_cast<uint64_t>(target_.data());
    requests[1].target_offset =
        reinterpret_cast<uint64_t>(unregistered.data());
    BatchID batch = submit(requests, /*capacity=*/4, /*notify=*/true);
    ASSERT_TRUE(waitFd(sender_->completionFd(), 10000));
    std::vector<BatchCompletion> done;
    ASSERT_TRUE(sender_->drainCompletions(done).ok());
    ASSERT_EQ(done.size(), 1u);
    EXPECT_EQ(done[0].batch_id, batch);
    EXPECT_EQ(done[0].status.s, TransferStatusEnum::FAILED);
    EXPECT_EQ(done[0].status.transferred_bytes, kLen);

    TransferStatus status{};
    ASSERT_TRUE(sender_->getBatchTransferStatus(batch, status).ok());
    EXPECT_EQ(status.s, TransferStatusEnum::FAILED);
    EXPECT_EQ(status.transferred_bytes, kLen);
    EXPECT_EQ(0, std::memcmp(source_.data(), target_.data(), kLen));
    release(batch);
}

TEST_F(RdmaCompletionFdTest, OneDrainReturnsEveryOptedInBatch) {
    constexpr int kBatches = 3;
    constexpr size_t kLen = 4096;
    source_.assign(kBatches * kLen, 'C');
    target_.assign(kBatches * kLen, 0);
    ASSERT_EQ(sender_->registerLocalMemory(source_.data(), source_.size()), 0);
    ASSERT_EQ(receiver_->registerLocalMemory(target_.data(), target_.size()),
              0);
    SegmentID segment = openReceiver();
    ASSERT_NE(segment, static_cast<SegmentID>(-1));

    std::vector<BatchID> batches;
    for (int i = 0; i < kBatches; ++i) {
        TransferRequest request{};
        request.opcode = TransferRequest::WRITE;
        request.source = source_.data() + i * kLen;
        request.target_id = segment;
        request.target_offset =
            reinterpret_cast<uint64_t>(target_.data() + i * kLen);
        request.length = kLen;
        batches.push_back(submit({request}, /*capacity=*/2, /*notify=*/true));
    }
    // Let every completion land in the queue before the single drain.
    // Same-host 4KB writes finish far inside this window, and nothing
    // here calls getBatchTransferStatus.
    std::this_thread::sleep_for(std::chrono::seconds(2));
    ASSERT_TRUE(waitFd(sender_->completionFd(), 0));
    std::vector<BatchCompletion> done;
    ASSERT_TRUE(sender_->drainCompletions(done).ok());
    ASSERT_EQ(done.size(), static_cast<size_t>(kBatches));
    for (int i = 0; i < kBatches; ++i) {
        bool found = false;
        for (const auto& item : done) {
            if (item.batch_id != batches[i]) continue;
            found = true;
            EXPECT_EQ(item.status.s, TransferStatusEnum::COMPLETED);
            EXPECT_EQ(item.status.transferred_bytes, kLen);
        }
        EXPECT_TRUE(found);
    }
    std::vector<BatchCompletion> again;
    ASSERT_TRUE(sender_->drainCompletions(again).ok());
    EXPECT_TRUE(again.empty());
    EXPECT_EQ(0, std::memcmp(source_.data(), target_.data(), source_.size()));
    for (BatchID batch : batches) release(batch);
}

TEST_F(RdmaCompletionFdTest, NonOptInDoesNotWakeTheFd) {
    constexpr size_t kLen = 4096;
    source_.assign(kLen, 'D');
    target_.assign(kLen, 0);
    ASSERT_EQ(sender_->registerLocalMemory(source_.data(), source_.size()), 0);
    ASSERT_EQ(receiver_->registerLocalMemory(target_.data(), target_.size()),
              0);
    SegmentID segment = openReceiver();
    ASSERT_NE(segment, static_cast<SegmentID>(-1));
    TransferRequest request{};
    request.opcode = TransferRequest::WRITE;
    request.source = source_.data();
    request.target_id = segment;
    request.target_offset = reinterpret_cast<uint64_t>(target_.data());
    request.length = kLen;
    BatchID batch = submit({request}, /*capacity=*/1, /*notify=*/false);

    TransferStatus status{};
    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    do {
        ASSERT_TRUE(sender_->getBatchTransferStatus(batch, status).ok());
        if (status.s == TransferStatusEnum::COMPLETED) break;
        ASSERT_NE(status.s, TransferStatusEnum::FAILED);
        std::this_thread::yield();
    } while (std::chrono::steady_clock::now() < deadline);
    ASSERT_EQ(status.s, TransferStatusEnum::COMPLETED);
    EXPECT_FALSE(waitFd(sender_->completionFd(), 50));
    release(batch);
}

TEST_F(RdmaCompletionFdTest, FreeRightAfterSubmitIsAbsentFromDrain) {
    constexpr size_t kLen = 4096;
    source_.assign(kLen, 'E');
    target_.assign(kLen, 0);
    ASSERT_EQ(sender_->registerLocalMemory(source_.data(), source_.size()), 0);
    ASSERT_EQ(receiver_->registerLocalMemory(target_.data(), target_.size()),
              0);
    SegmentID segment = openReceiver();
    ASSERT_NE(segment, static_cast<SegmentID>(-1));
    TransferRequest request{};
    request.opcode = TransferRequest::WRITE;
    request.source = source_.data();
    request.target_id = segment;
    request.target_offset = reinterpret_cast<uint64_t>(target_.data());
    request.length = kLen;
    BatchID batch = submit({request}, /*capacity=*/1, /*notify=*/true);
    Status freed = sender_->freeBatchID(batch);
    ASSERT_TRUE(freed.ok() || freed.IsBatchCleanupDeferred());
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    std::vector<BatchCompletion> done;
    ASSERT_TRUE(sender_->drainCompletions(done).ok());
    for (const auto& item : done) EXPECT_NE(item.batch_id, batch);
    if (freed.IsBatchCleanupDeferred())
        sender_->waitUntilDeferredCleanupDone(batch);
}

TEST_F(RdmaCompletionFdTest, PythonRoundTrip) {
    const char* previous = std::getenv("PYTHONPATH");
    std::optional<std::string> saved =
        previous ? std::optional<std::string>(previous) : std::nullopt;
    std::string pythonpath = MOONCAKE_ENGINE_DIR;
    if (saved) pythonpath += ":" + *saved;
    setenv("PYTHONPATH", pythonpath.c_str(), 1);

    std::string command =
        std::string("python3 \"") + MOONCAKE_COMPLETION_FD_PY + "\" 2>&1";
    FILE* pipe = popen(command.c_str(), "r");
    ASSERT_NE(pipe, nullptr);
    std::string output;
    char buf[512];
    while (fgets(buf, sizeof(buf), pipe)) output += buf;
    int status = pclose(pipe);
    int rc = WIFEXITED(status) ? WEXITSTATUS(status) : status;

    if (saved)
        setenv("PYTHONPATH", saved->c_str(), 1);
    else
        unsetenv("PYTHONPATH");
    EXPECT_EQ(rc, 0) << output;
}
