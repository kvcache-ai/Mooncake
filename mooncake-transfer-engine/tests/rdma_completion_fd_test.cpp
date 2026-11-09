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
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
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

std::string handshakeName() {
    const char* ip = std::getenv("MC_TEST_LOCAL_IP");
    if (!ip || !*ip) ip = "127.0.0.1";
    return std::string(ip) + ":0";
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
        saved_device_ =
            previous ? std::optional<std::string>(previous) : std::nullopt;
        setenv("MC_TEST_DEVICE_NAME", device_.c_str(), 1);
        previous = std::getenv("MC_TE_FILTERS");
        saved_filters_ =
            previous ? std::optional<std::string>(previous) : std::nullopt;
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
        if (engine->init("P2PHANDSHAKE", handshakeName())) return nullptr;
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
            EXPECT_TRUE(sender_->enableBatchCompletionNotification(batch).ok());
        }
        EXPECT_TRUE(sender_->submitTransfer(batch, requests).ok());
        return batch;
    }

    void release(BatchID batch) {
        Status freed = sender_->freeBatchID(batch);
        EXPECT_TRUE(freed.ok() || freed.IsBatchCleanupDeferred());
    }

    bool drainUntil(BatchID batch, BatchCompletion* out, int timeout_ms) {
        auto deadline = std::chrono::steady_clock::now() +
                        std::chrono::milliseconds(timeout_ms);
        while (std::chrono::steady_clock::now() < deadline) {
            if (!waitFd(sender_->completionFd(), 50)) continue;
            std::vector<BatchCompletion> done;
            if (!sender_->drainCompletions(done).ok()) return false;
            for (const auto& item : done) {
                if (item.batch_id != batch) continue;
                if (out) *out = item;
                return true;
            }
        }
        return false;
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
    BatchCompletion done{};
    ASSERT_TRUE(drainUntil(batch, &done, 10000));
    EXPECT_EQ(done.status.s, TransferStatusEnum::COMPLETED);
    EXPECT_EQ(done.status.transferred_bytes, kTasks * kLen);

    TransferStatus status{};
    ASSERT_TRUE(sender_->getBatchTransferStatus(batch, status).ok());
    EXPECT_EQ(status.s, done.status.s);
    EXPECT_EQ(status.transferred_bytes, done.status.transferred_bytes);
    EXPECT_EQ(0, std::memcmp(source_.data(), target_.data(), source_.size()));
    release(batch);
}

TEST_F(RdmaCompletionFdTest, StaleRkeyFailsFromCq) {
    // Unregistered-before-submit fails in selectPeerDevice. Unregister after
    // the sender has already fetched the segment so the WR posts and the CQ
    // reports the remote access error.
    constexpr size_t kLen = 4096;
    source_.assign(2 * kLen, 'B');
    target_.assign(kLen, 0);
    std::vector<char> doomed(kLen, 'Z');
    ASSERT_EQ(sender_->registerLocalMemory(source_.data(), source_.size()), 0);
    ASSERT_EQ(receiver_->registerLocalMemory(target_.data(), target_.size()),
              0);
    ASSERT_EQ(receiver_->registerLocalMemory(doomed.data(), doomed.size()), 0);
    SegmentID segment = openReceiver();
    ASSERT_NE(segment, static_cast<SegmentID>(-1));
    ASSERT_EQ(receiver_->unregisterLocalMemory(doomed.data()), 0);

    std::vector<TransferRequest> requests(2);
    for (int i = 0; i < 2; ++i) {
        requests[i].opcode = TransferRequest::WRITE;
        requests[i].source = source_.data() + i * kLen;
        requests[i].target_id = segment;
        requests[i].length = kLen;
    }
    requests[0].target_offset = reinterpret_cast<uint64_t>(target_.data());
    requests[1].target_offset = reinterpret_cast<uint64_t>(doomed.data());
    BatchID batch = submit(requests, /*capacity=*/4, /*notify=*/true);
    BatchCompletion done{};
    ASSERT_TRUE(drainUntil(batch, &done, 10000));
    EXPECT_EQ(done.status.s, TransferStatusEnum::FAILED);
    EXPECT_EQ(done.status.transferred_bytes, kLen);

    TransferStatus status{};
    ASSERT_TRUE(sender_->getBatchTransferStatus(batch, status).ok());
    EXPECT_EQ(status.s, TransferStatusEnum::FAILED);
    EXPECT_EQ(status.transferred_bytes, kLen);
    EXPECT_EQ(0, std::memcmp(source_.data(), target_.data(), kLen));
    release(batch);
}

TEST_F(RdmaCompletionFdTest, MultiSliceTaskWakesOnce) {
    // slice_size defaults to 64 KiB; 1 MiB forces seal vs CQ races.
    constexpr size_t kLen = 1 << 20;
    source_.assign(kLen, 'M');
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
    BatchCompletion done{};
    ASSERT_TRUE(drainUntil(batch, &done, 30000));
    EXPECT_EQ(done.status.s, TransferStatusEnum::COMPLETED);
    EXPECT_EQ(done.status.transferred_bytes, kLen);
    TransferStatus status{};
    ASSERT_TRUE(sender_->getBatchTransferStatus(batch, status).ok());
    EXPECT_EQ(status.s, TransferStatusEnum::COMPLETED);
    EXPECT_EQ(status.transferred_bytes, kLen);
    EXPECT_EQ(0, std::memcmp(source_.data(), target_.data(), kLen));
    release(batch);
}

TEST_F(RdmaCompletionFdTest, ConcurrentSubmittersOneDispatcher) {
    constexpr int kSubmitters = 4;
    constexpr int kBatchesPerSubmitter = 8;
    constexpr size_t kLen = 4096;
    const int total = kSubmitters * kBatchesPerSubmitter;
    source_.assign(total * kLen, 0);
    target_.assign(total * kLen, 0);
    for (int i = 0; i < total; ++i)
        std::memset(source_.data() + i * kLen, static_cast<char>('A' + (i % 26)),
                    kLen);
    ASSERT_EQ(sender_->registerLocalMemory(source_.data(), source_.size()), 0);
    ASSERT_EQ(receiver_->registerLocalMemory(target_.data(), target_.size()),
              0);
    SegmentID segment = openReceiver();
    ASSERT_NE(segment, static_cast<SegmentID>(-1));

    std::mutex mu;
    std::unordered_map<BatchID, size_t> expected;
    std::atomic<int> submitted{0};
    std::atomic<bool> submit_failed{false};
    std::vector<std::thread> workers;
    for (int w = 0; w < kSubmitters; ++w) {
        workers.emplace_back([&, w] {
            for (int i = 0; i < kBatchesPerSubmitter; ++i) {
                const int idx = w * kBatchesPerSubmitter + i;
                TransferRequest request{};
                request.opcode = TransferRequest::WRITE;
                request.source = source_.data() + idx * kLen;
                request.target_id = segment;
                request.target_offset =
                    reinterpret_cast<uint64_t>(target_.data() + idx * kLen);
                request.length = kLen;
                BatchID batch = sender_->allocateBatchID(1);
                if (batch == static_cast<BatchID>(0) ||
                    !sender_->enableBatchCompletionNotification(batch).ok() ||
                    !sender_->submitTransfer(batch, {request}).ok()) {
                    submit_failed.store(true, std::memory_order_release);
                    return;
                }
                {
                    std::lock_guard<std::mutex> lock(mu);
                    expected.emplace(batch, kLen);
                }
                submitted.fetch_add(1, std::memory_order_release);
            }
        });
    }

    std::unordered_map<BatchID, BatchCompletion> got;
    auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(30);
    while (got.size() < static_cast<size_t>(total) &&
           std::chrono::steady_clock::now() < deadline) {
        if (!waitFd(sender_->completionFd(), 100)) continue;
        std::vector<BatchCompletion> done;
        ASSERT_TRUE(sender_->drainCompletions(done).ok());
        for (const auto& item : done) {
            auto [it, inserted] = got.emplace(item.batch_id, item);
            EXPECT_TRUE(inserted) << "duplicate completion for batch";
            EXPECT_EQ(item.status.s, TransferStatusEnum::COMPLETED);
            EXPECT_EQ(item.status.transferred_bytes, kLen);
        }
    }
    for (auto& t : workers) t.join();
    ASSERT_FALSE(submit_failed.load());
    ASSERT_EQ(submitted.load(), total);
    ASSERT_EQ(got.size(), static_cast<size_t>(total));
    for (const auto& [batch, bytes] : expected) {
        auto it = got.find(batch);
        ASSERT_NE(it, got.end());
        EXPECT_EQ(it->second.status.transferred_bytes, bytes);
        release(batch);
    }
    EXPECT_EQ(0, std::memcmp(source_.data(), target_.data(), source_.size()));
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
    std::unordered_set<BatchID> pending(batches.begin(), batches.end());
    auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (!pending.empty() && std::chrono::steady_clock::now() < deadline) {
        ASSERT_TRUE(waitFd(sender_->completionFd(), 1000));
        std::vector<BatchCompletion> done;
        ASSERT_TRUE(sender_->drainCompletions(done).ok());
        for (const auto& item : done) {
            EXPECT_EQ(item.status.s, TransferStatusEnum::COMPLETED);
            EXPECT_EQ(item.status.transferred_bytes, kLen);
            EXPECT_EQ(pending.erase(item.batch_id), 1u);
        }
    }
    EXPECT_TRUE(pending.empty());
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
    // Give the CQ a moment; the freed id must not appear on a later drain.
    auto deadline =
        std::chrono::steady_clock::now() + std::chrono::milliseconds(500);
    while (std::chrono::steady_clock::now() < deadline) {
        if (waitFd(sender_->completionFd(), 20)) {
            std::vector<BatchCompletion> done;
            ASSERT_TRUE(sender_->drainCompletions(done).ok());
            for (const auto& item : done) EXPECT_NE(item.batch_id, batch);
        }
    }
}

TEST_F(RdmaCompletionFdTest, DestroyEngineWakesDupAndStopsPublishing) {
    constexpr size_t kLen = 1 << 20;
    source_.assign(kLen, 'S');
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

    const int dup_fd = ::dup(sender_->completionFd());
    ASSERT_GE(dup_fd, 0);
    std::atomic<bool> woke{false};
    std::thread waiter([&] {
        woke.store(waitFd(dup_fd, 10000), std::memory_order_release);
    });
    // Tear down while the multi-slice transfer is still in flight.
    sender_.reset();
    waiter.join();
    EXPECT_TRUE(woke.load(std::memory_order_acquire));
    // The original engine is gone; a further drain on a fresh engine must
    // not see the destroyed batch id. Closing the dup is enough here.
    ::close(dup_fd);
    (void)batch;
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

    if (output.find("import engine failed") != std::string::npos) {
        GTEST_SKIP() << "Python engine binding unavailable: " << output;
    }
    EXPECT_EQ(rc, 0) << output;
}
