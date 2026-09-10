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

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "tent/common/config.h"
#include "tent/rpc/rpc.h"
#include "tent/runtime/control_plane.h"
#include "tent/transport/tcp/tcp_transport.h"

namespace mooncake {
namespace tent {

class TcpTransportTestPeer {
   public:
    static const TcpParams& params(const TcpTransport& transport) {
        return transport.params_;
    }
};

namespace {

std::shared_ptr<ControlService> makeP2PMetadata() {
    return std::make_shared<ControlService>("p2p", "", nullptr);
}

TEST(TcpTransportConfigTest, InstallWithoutConfigKeepsStructDefaults) {
    TcpTransport transport;
    auto metadata = makeP2PMetadata();
    std::string name = "tcp-config-default";
    ASSERT_TRUE(transport.install(name, metadata, nullptr).ok());

    const TcpParams defaults;
    const auto& params = TcpTransportTestPeer::params(transport);
    EXPECT_EQ(params.max_retry_count, defaults.max_retry_count);
    EXPECT_EQ(params.retry_base_delay_ms, defaults.retry_base_delay_ms);
    EXPECT_EQ(params.retry_max_delay_ms, defaults.retry_max_delay_ms);
    EXPECT_EQ(params.max_concurrent_tasks, defaults.max_concurrent_tasks);

    EXPECT_TRUE(transport.uninstall().ok());
}

TEST(TcpTransportConfigTest, InstallReadsConfigKeys) {
    auto conf = std::make_shared<Config>();
    conf->set("transports/tcp/max_retry_count", 5);
    conf->set("transports/tcp/retry_base_delay_ms", 200ULL);
    conf->set("transports/tcp/retry_max_delay_ms", 4000ULL);
    conf->set("transports/tcp/max_concurrent_tasks", 32);

    TcpTransport transport;
    auto metadata = makeP2PMetadata();
    std::string name = "tcp-config-override";
    ASSERT_TRUE(transport.install(name, metadata, nullptr, conf).ok());

    const auto& params = TcpTransportTestPeer::params(transport);
    EXPECT_EQ(params.max_retry_count, 5u);
    EXPECT_EQ(params.retry_base_delay_ms, 200ULL);
    EXPECT_EQ(params.retry_max_delay_ms, 4000ULL);
    EXPECT_EQ(params.max_concurrent_tasks, 32u);

    EXPECT_TRUE(transport.uninstall().ok());
}

TEST(TcpSubBatchTest, PointerStabilityAfterReserve) {
    // allocateSubBatch reserves task_list to max_size so submitTransferTasks
    // can take stable TcpTask* after emplace.
    TcpSubBatch batch;
    batch.max_size = 8;
    batch.task_list.reserve(batch.max_size);

    batch.task_list.emplace_back();
    TcpTask* first = &batch.task_list[0];
    first->target_addr = 42;

    for (int i = 1; i < 8; ++i) {
        batch.task_list.emplace_back();
    }

    EXPECT_EQ(first->target_addr, 42u);
    EXPECT_EQ(&batch.task_list[0], first);
}

TEST(TcpRetryBackoffTest, ExponentialGrowthWithCap) {
    const uint64_t base = 100;
    const uint64_t cap = 2000;
    uint64_t delay = base;

    std::vector<uint64_t> delays;
    for (int attempt = 0; attempt < 6; ++attempt) {
        delays.push_back(delay);
        delay = nextTcpRetryDelay(delay, cap);
    }

    EXPECT_EQ(delays[0], 100u);
    EXPECT_EQ(delays[1], 200u);
    EXPECT_EQ(delays[2], 400u);
    EXPECT_EQ(delays[3], 800u);
    EXPECT_EQ(delays[4], 1600u);
    EXPECT_EQ(delays[5], 2000u);
}

class TcpEndpointRefreshTest : public testing::Test {
   protected:
    void SetUp() override {
        // Reserve A without listening: connect fails without a port-reuse race.
        stale_socket.open(asio::ip::tcp::v4());
        stale_socket.bind({asio::ip::make_address("127.0.0.1"), 0});
        stale_addr = address(stale_socket.local_endpoint().port());
        ASSERT_TRUE(
            data_server
                .registerFunction(
                    SendData,
                    [&](const std::string_view&, std::string& response) {
                        ++data_hits;
                        if (write_fails)
                            response = "remote error after receive";
                    })
                .ok());
        ASSERT_TRUE(data_server
                        .registerFunction(
                            Notify, [&](const std::string_view&,
                                        std::string&) { ++notifications; })
                        .ok());
        uint16_t port = 0;
        ASSERT_TRUE(data_server.start(port).ok());
        data_addr = address(port);
        ASSERT_TRUE(
            metadata_server
                .registerFunction(
                    GetSegmentDesc,
                    [&](const std::string_view&, std::string& response) {
                        const int read = ++metadata_reads;
                        if (refresh_fails && read > 1) return;
                        SegmentDesc desc;
                        desc.name = metadata_addr;
                        desc.type = SegmentType::Memory;
                        desc.machine_id = "tcp-refresh-peer";
                        desc.rpc_server_addr =
                            stale_first && read == 1 ? stale_addr : data_addr;
                        BufferDesc buffer;
                        buffer.addr = 0x1000;
                        buffer.length = sizeof(payload);
                        buffer.location = "cpu";
                        buffer.transports = {TCP};
                        MemorySegmentDesc memory;
                        memory.buffers.push_back(buffer);
                        desc.detail = memory;
                        published_endpoints.push_back(desc.rpc_server_addr);
                        response = json(desc).dump();
                    })
                .ok());
        port = 0;
        ASSERT_TRUE(metadata_server.start(port).ok());
        metadata_addr = address(port);
        ASSERT_TRUE(
            metadata->segmentManager().openRemote(segment, metadata_addr).ok());
        ASSERT_TRUE(transport.allocateSubBatch(batch, 2).ok());
        batch->notify_progress = [&](BatchID) {
            std::lock_guard<std::mutex> lock(completion_mutex);
            ++completed;
            completion_cv.notify_one();
        };
    }

    void TearDown() override {
        // Join callbacks before releasing their captures or task storage.
        EXPECT_TRUE(transport.uninstall().ok());
        if (batch) EXPECT_TRUE(transport.freeSubBatch(batch).ok());
        EXPECT_TRUE(data_server.stop().ok());
        EXPECT_TRUE(metadata_server.stop().ok());
    }

    void install(size_t retries = 1) {
        auto conf = std::make_shared<Config>();
        conf->set("transports/tcp/max_concurrent_tasks", 1);
        conf->set("transports/tcp/max_retry_count", retries);
        conf->set("transports/tcp/retry_base_delay_ms", 0ULL);
        std::string name = "tcp-refresh-client";
        ASSERT_TRUE(transport.install(name, metadata, nullptr, conf).ok());
    }

    void transfer(TransferStatusEnum expected, bool unknown_write = false) {
        Request request{};
        request.opcode = Request::WRITE;
        request.source = payload;
        request.length = sizeof(payload);
        request.target_offset = 0x1000;
        request.target_id = segment;
        const size_t task_id = batch->size();
        ASSERT_TRUE(transport.submitTransferTasks(batch, {request}).ok());
        {
            std::unique_lock<std::mutex> lock(completion_mutex);
            ASSERT_TRUE(completion_cv.wait_for(
                lock, std::chrono::seconds(3),
                [&] { return completed == task_id + 1; }));
        }
        TransferStatus status{PENDING, 0};
        const auto result = transport.getTransferStatus(batch, task_id, status);
        EXPECT_EQ(status.s, expected);
        if (unknown_write) {
            EXPECT_TRUE(result.IsRpcServiceError()) << result.ToString();
        } else {
            EXPECT_TRUE(result.ok()) << result.ToString();
        }
    }

    static std::string address(uint16_t port) {
        return "127.0.0.1:" + std::to_string(port);
    }

    asio::io_context io;
    asio::ip::tcp::socket stale_socket{io};
    CoroRpcAgent data_server, metadata_server;
    std::string stale_addr, data_addr, metadata_addr;
    std::atomic<int> data_hits{0}, metadata_reads{0}, notifications{0};
    bool stale_first{true}, refresh_fails{false}, write_fails{false};
    std::vector<std::string> published_endpoints;
    char payload[8] = "refresh";
    std::mutex completion_mutex;
    std::condition_variable completion_cv;
    size_t completed{0};
    std::shared_ptr<ControlService> metadata = makeP2PMetadata();
    TcpTransport transport;
    Transport::SubBatchRef batch{nullptr};
    SegmentID segment{0};
};

TEST_F(TcpEndpointRefreshTest, PreSendFailureUsesNewEndpoint) {
    install();
    transfer(COMPLETED);
    EXPECT_EQ(published_endpoints,
              (std::vector<std::string>{stale_addr, data_addr}));
    EXPECT_EQ(data_hits, 1);
}

TEST_F(TcpEndpointRefreshTest, BudgetZeroInvalidatesSameWorkerCache) {
    install(0);
    transfer(FAILED);
    EXPECT_EQ(metadata_reads, 1);
    EXPECT_EQ(data_hits, 0);
    // The same worker must fetch B for the next request, despite no retry.
    transfer(COMPLETED);
    EXPECT_EQ(metadata_reads, 2);
    EXPECT_EQ(data_hits, 1);
}

TEST_F(TcpEndpointRefreshTest, RefreshFailureStopsRetry) {
    refresh_fails = true;
    install();
    transfer(FAILED);
    EXPECT_EQ(metadata_reads, 2);
    EXPECT_EQ(data_hits, 0);
}

TEST_F(TcpEndpointRefreshTest, DispatchedWriteIsNotRetried) {
    stale_first = false;
    write_fails = true;
    install(2);
    transfer(FAILED, true);
    EXPECT_EQ(metadata_reads, 1);
    EXPECT_EQ(data_hits, 1);
}

TEST_F(TcpEndpointRefreshTest, SuccessfulWritesKeepCachedEndpoint) {
    stale_first = false;
    install();
    transfer(COMPLETED);
    transfer(COMPLETED);
    EXPECT_EQ(metadata_reads, 1);
    EXPECT_EQ(data_hits, 2);
}

TEST_F(TcpEndpointRefreshTest, NotificationStillRefreshesConnectionFailure) {
    install();
    EXPECT_TRUE(transport.sendNotification(segment, {"test", "refresh"}).ok());
    EXPECT_EQ(metadata_reads, 2);
    EXPECT_EQ(notifications, 1);
}

}  // namespace
}  // namespace tent
}  // namespace mooncake
