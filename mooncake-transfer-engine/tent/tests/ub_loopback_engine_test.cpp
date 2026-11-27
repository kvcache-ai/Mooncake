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

// Runs two TENT engines in one process with only the UB transport enabled and
// the loopback URMA adapter (USE_UB_MOCK=ON, MC_TENT_UB_MOCK=1). It checks
// that engine-level READ/WRITE goes through UbTransport end to end.

#include <gtest/gtest.h>

#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <memory>
#include <thread>
#include <vector>

#include "loopback_urma_adapter.h"
#include "tent/common/config.h"
#include "tent/common/types.h"
#include "tent/transfer_engine.h"

namespace mooncake {
namespace tent {
namespace {

constexpr size_t kBufferLength = 4 * 1024 * 1024;

std::shared_ptr<Config> makeUbOnlyConfig() {
    auto config = std::make_shared<Config>();
    config->set("metadata_type", "p2p");
    config->set("metadata_servers", "P2PHANDSHAKE");
    config->set("transports/tcp/enable", false);
    config->set("transports/rdma/enable", false);
    config->set("transports/shm/enable", false);
    config->set("transports/io_uring/enable", false);
    config->set("transports/ub/enable", true);
    config->set("transports/ub/worker_count", 2);
    config->set("transports/ub/max_endpoints", 64);
    // Small slices so one request is split across many work requests.
    config->set("transports/ub/slice_size", 64 * 1024);
    return config;
}

int waitBatch(TransferEngine& engine, BatchID batch, int timeout_ms = 20000) {
    TransferStatus status;
    for (int i = 0; i < timeout_ms; ++i) {
        auto result = engine.getTransferStatus(batch, status);
        if (!result.ok() || status.s == TransferStatusEnum::FAILED) return -1;
        if (status.s == TransferStatusEnum::COMPLETED) return 1;
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    return 0;
}

uint8_t pattern(size_t i, size_t seed) {
    return static_cast<uint8_t>((i * 7 + seed * 131 + (i >> 12)) & 0xff);
}

class UbLoopbackEngineTest : public ::testing::Test {
   protected:
    void SetUp() override {
        if (!ub::mock::loopbackRequested()) {
            GTEST_SKIP() << ub::mock::kLoopbackEnv << " is not set";
        }
        ub::mock::resetLoopbackStats();

        server_ = std::make_unique<TransferEngine>(makeUbOnlyConfig());
        client_ = std::make_unique<TransferEngine>(makeUbOnlyConfig());
        ASSERT_TRUE(server_->available());
        ASSERT_TRUE(client_->available());

        server_buffer_.assign(kBufferLength, 0);
        client_buffer_.assign(kBufferLength, 0);
        ASSERT_TRUE(
            server_->registerLocalMemory(server_buffer_.data(), kBufferLength)
                .ok());
        ASSERT_TRUE(
            client_->registerLocalMemory(client_buffer_.data(), kBufferLength)
                .ok());
        auto status = client_->openSegment(segment_, server_->getSegmentName());
        ASSERT_TRUE(status.ok()) << status.ToString();
    }

    void TearDown() override {
        if (client_) {
            if (segment_) (void)client_->closeSegment(segment_);
            (void)client_->unregisterLocalMemory(client_buffer_.data(),
                                                 kBufferLength);
        }
        if (server_) {
            (void)server_->unregisterLocalMemory(server_buffer_.data(),
                                                 kBufferLength);
        }
        client_.reset();
        server_.reset();
    }

    Request makeRequest(Request::OpCode op, size_t offset, size_t length,
                        TransportType hint) {
        Request request{};
        request.opcode = op;
        request.source = client_buffer_.data() + offset;
        request.target_id = segment_;
        request.target_offset =
            reinterpret_cast<uint64_t>(server_buffer_.data()) + offset;
        request.length = length;
        request.transport_hint = hint;
        return request;
    }

    int run(const std::vector<Request>& requests) {
        BatchID batch = client_->allocateBatch(requests.size());
        auto status = client_->submitTransfer(batch, requests);
        if (!status.ok()) {
            ADD_FAILURE() << status.ToString();
            (void)client_->freeBatch(batch);
            return -2;
        }
        int done = waitBatch(*client_, batch);
        (void)client_->freeBatch(batch);
        return done;
    }

    std::unique_ptr<TransferEngine> server_;
    std::unique_ptr<TransferEngine> client_;
    std::vector<uint8_t> server_buffer_;
    std::vector<uint8_t> client_buffer_;
    SegmentID segment_ = 0;
};

TEST_F(UbLoopbackEngineTest, WriteThenReadThroughUb) {
    for (size_t i = 0; i < kBufferLength; ++i) {
        client_buffer_[i] = pattern(i, 1);
    }
    ASSERT_EQ(run({makeRequest(Request::WRITE, 0, kBufferLength, UB)}), 1);
    for (size_t i = 0; i < kBufferLength; ++i) {
        ASSERT_EQ(server_buffer_[i], pattern(i, 1)) << "offset " << i;
    }

    for (size_t i = 0; i < kBufferLength; ++i) {
        server_buffer_[i] = pattern(i, 2);
    }
    std::fill(client_buffer_.begin(), client_buffer_.end(), 0);
    ASSERT_EQ(run({makeRequest(Request::READ, 0, kBufferLength, UB)}), 1);
    for (size_t i = 0; i < kBufferLength; ++i) {
        ASSERT_EQ(client_buffer_[i], pattern(i, 2)) << "offset " << i;
    }

    const auto stats = ub::mock::loopbackStats();
    EXPECT_GT(stats.write_requests, 0u);
    EXPECT_GT(stats.read_requests, 0u);
    EXPECT_EQ(stats.bytes, 2 * kBufferLength);
}

// Without a transport hint the engine must still pick UB, since it is the only
// transport installed.
TEST_F(UbLoopbackEngineTest, UnhintedBatchUsesUb) {
    std::vector<Request> requests;
    constexpr size_t kChunk = 256 * 1024 + 17;
    for (size_t offset = 0; offset + kChunk <= kBufferLength;
         offset += kChunk + 3) {
        for (size_t i = 0; i < kChunk; ++i) {
            client_buffer_[offset + i] = pattern(offset + i, 3);
        }
        requests.push_back(makeRequest(Request::WRITE, offset, kChunk, UNSPEC));
    }
    ASSERT_EQ(run(requests), 1);
    for (const auto& request : requests) {
        const size_t offset =
            static_cast<uint8_t*>(request.source) - client_buffer_.data();
        for (size_t i = 0; i < request.length; ++i) {
            ASSERT_EQ(server_buffer_[offset + i], pattern(offset + i, 3))
                << "offset " << offset + i;
        }
    }
    EXPECT_GT(ub::mock::loopbackStats().write_requests, 0u);
}

TEST_F(UbLoopbackEngineTest, OutOfRangeTargetFails) {
    // The target range ends past the server's registered buffer. The engine
    // may reject it at submit time or fail the task; it must not complete.
    Request request = makeRequest(Request::WRITE, 0, 8192, UB);
    request.target_offset += kBufferLength - 4096;
    BatchID batch = client_->allocateBatch(1);
    if (client_->submitTransfer(batch, {request}).ok()) {
        EXPECT_NE(waitBatch(*client_, batch, 5000), 1);
    }
    (void)client_->freeBatch(batch);
    EXPECT_EQ(ub::mock::loopbackStats().bytes, 0u);
}

}  // namespace
}  // namespace tent
}  // namespace mooncake
