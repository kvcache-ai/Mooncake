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

#include <array>
#include <atomic>
#include <chrono>
#include <future>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "tent/common/config.h"
#include "tent/runtime/control_plane.h"
#include "tent/runtime/transport_selector.h"
#include "tent/transport/tcp/tcp_transport.h"

namespace mooncake {
namespace tent {

class TcpTransportTestPeer {
   public:
    static const TcpParams& params(const TcpTransport& transport) {
        return transport.params_;
    }

    static void startTransfer(TcpTransport& transport, TcpTask* task) {
        transport.startTransfer(task);
    }
};

namespace {

std::shared_ptr<ControlService> makeP2PMetadata() {
    return std::make_shared<ControlService>("p2p", "", nullptr);
}

std::string uniqueLoopbackAddress(uint16_t port) {
    static std::atomic<uint32_t> sequence{2};
    const uint32_t value = sequence.fetch_add(1, std::memory_order_relaxed);
    return "127." + std::to_string((value >> 16) & 0xff) + "." +
           std::to_string((value >> 8) & 0xff) + "." +
           std::to_string(value & 0xff) + ":" + std::to_string(port);
}

struct BlockFirstRpc {
    std::atomic<size_t> calls{0};
    std::promise<void> first_entered;
    std::promise<void> release_first;
    std::shared_future<void> first_released{release_first.get_future().share()};
    std::promise<void> first_finished;
};

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
    EXPECT_EQ(params.data_rpc_timeout_ms, defaults.data_rpc_timeout_ms);
    EXPECT_EQ(params.max_concurrent_tasks, defaults.max_concurrent_tasks);

    EXPECT_TRUE(transport.uninstall().ok());
}

TEST(TcpTransportConfigTest, InstallReadsConfigKeys) {
    auto conf = std::make_shared<Config>();
    conf->set("transports/tcp/max_retry_count", 5);
    conf->set("transports/tcp/retry_base_delay_ms", 200ULL);
    conf->set("transports/tcp/retry_max_delay_ms", 4000ULL);
    conf->set("transports/tcp/data_rpc_timeout_ms", 1500LL);
    conf->set("transports/tcp/max_concurrent_tasks", 32);

    TcpTransport transport;
    auto metadata = makeP2PMetadata();
    std::string name = "tcp-config-override";
    ASSERT_TRUE(transport.install(name, metadata, nullptr, conf).ok());

    const auto& params = TcpTransportTestPeer::params(transport);
    EXPECT_EQ(params.max_retry_count, 5u);
    EXPECT_EQ(params.retry_base_delay_ms, 200ULL);
    EXPECT_EQ(params.retry_max_delay_ms, 4000ULL);
    EXPECT_EQ(params.data_rpc_timeout_ms, 1500);
    EXPECT_EQ(params.max_concurrent_tasks, 32u);

    EXPECT_TRUE(transport.uninstall().ok());
}

TEST(TcpTransportConfigTest, RejectsZeroWorkersAndAllowsRetry) {
    auto conf = std::make_shared<Config>();
    conf->set("transports/tcp/max_concurrent_tasks", 0);

    TcpTransport transport;
    auto metadata = makeP2PMetadata();
    std::string name = "tcp-config-zero-workers";
    auto status = transport.install(name, metadata, nullptr, conf);
    ASSERT_TRUE(status.IsInvalidArgument()) << status.ToString();
    EXPECT_NE(status.ToString().find("transports/tcp/max_concurrent_tasks"),
              std::string::npos);

    conf->set("transports/tcp/max_concurrent_tasks", 1);
    ASSERT_TRUE(transport.install(name, metadata, nullptr, conf).ok());
    EXPECT_EQ(TcpTransportTestPeer::params(transport).max_concurrent_tasks, 1u);
    EXPECT_TRUE(transport.uninstall().ok());
}

TEST(TcpTransportConfigTest, RejectsInvalidDataRpcTimeout) {
    for (int64_t timeout_ms : {0LL, -2LL}) {
        auto conf = std::make_shared<Config>();
        conf->set("transports/tcp/data_rpc_timeout_ms", timeout_ms);

        TcpTransport transport;
        auto metadata = makeP2PMetadata();
        std::string name = "tcp-config-invalid-timeout";
        auto status = transport.install(name, metadata, nullptr, conf);

        EXPECT_TRUE(status.IsInvalidArgument())
            << "timeout_ms=" << timeout_ms << ": " << status.ToString();
    }
}

TEST(TcpTransportConfigTest, AllowsDisabledDataRpcTimeout) {
    auto conf = std::make_shared<Config>();
    conf->set("transports/tcp/data_rpc_timeout_ms", -1LL);

    TcpTransport transport;
    auto metadata = makeP2PMetadata();
    std::string name = "tcp-config-disabled-timeout";
    ASSERT_TRUE(transport.install(name, metadata, nullptr, conf).ok());
    EXPECT_EQ(TcpTransportTestPeer::params(transport).data_rpc_timeout_ms, -1);
    EXPECT_TRUE(transport.uninstall().ok());
}

TEST(TcpTransportConfigTest, DataRpcTimeoutBoundsSlowSendAndRecv) {
    CoroRpcAgent server;
    const auto slow_handler = [](const std::string_view&, std::string&) {
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
    };
    ASSERT_TRUE(server.registerFunction(SendData, slow_handler).ok());
    ASSERT_TRUE(server.registerFunction(RecvData, slow_handler).ok());

    uint16_t port = 0;
    ASSERT_TRUE(server.start(port, false, 2).ok());

    std::array<char, 4> payload{};
    const std::string address = uniqueLoopbackAddress(port);
    for (auto call : {ControlClient::sendDataWithTimeout,
                      ControlClient::recvDataWithTimeout}) {
        auto status = call(address, 0, payload.data(), payload.size(),
                           std::chrono::milliseconds(100));

        EXPECT_TRUE(status.IsTimeout()) << status.ToString();
        EXPECT_FALSE(status.IsRpcServiceError()) << status.ToString();
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    EXPECT_TRUE(server.stop().ok());
}

TEST(TcpTransportConfigTest, TimedOutSendDropsClientBeforeLateReply) {
    auto state = std::make_shared<BlockFirstRpc>();
    CoroRpcAgent server;
    ASSERT_TRUE(
        server
            .registerFunction(SendData,
                              [state](const std::string_view&, std::string&) {
                                  if (state->calls.fetch_add(
                                          1, std::memory_order_relaxed) == 0) {
                                      state->first_entered.set_value();
                                      state->first_released.wait();
                                      state->first_finished.set_value();
                                  }
                              })
            .ok());
    uint16_t port = 0;
    ASSERT_TRUE(server.start(port, false, 2).ok());

    const std::string address = uniqueLoopbackAddress(port);
    char source = 0;
    auto entered = state->first_entered.get_future();
    auto finished = state->first_finished.get_future();
    auto first = std::async(std::launch::async, [&] {
        return ControlClient::sendDataWithTimeout(
            address, 0, &source, sizeof(source),
            std::chrono::milliseconds(100));
    });

    if (entered.wait_for(std::chrono::seconds(1)) !=
        std::future_status::ready) {
        state->release_first.set_value();
        first.wait();
        EXPECT_TRUE(server.stop().ok());
        FAIL() << "first SendData handler was not entered";
        return;
    }
    if (first.wait_for(std::chrono::seconds(1)) != std::future_status::ready) {
        state->release_first.set_value();
        first.wait();
        EXPECT_TRUE(server.stop().ok());
        FAIL() << "first SendData call did not time out";
        return;
    }
    auto first_status = first.get();
    EXPECT_TRUE(first_status.IsTimeout()) << first_status.ToString();

    auto second_status = ControlClient::sendDataWithTimeout(
        address, 0, &source, sizeof(source), std::chrono::seconds(1));
    EXPECT_TRUE(second_status.ok()) << second_status.ToString();

    state->release_first.set_value();
    EXPECT_EQ(finished.wait_for(std::chrono::seconds(1)),
              std::future_status::ready);

    auto third_status = ControlClient::sendDataWithTimeout(
        address, 0, &source, sizeof(source), std::chrono::seconds(1));
    EXPECT_TRUE(third_status.ok()) << third_status.ToString();
    EXPECT_EQ(state->calls.load(std::memory_order_relaxed), 3u);
    EXPECT_TRUE(server.stop().ok());
}

TEST(TcpTransportConfigTest, TimedOutRecvDropsClientBeforeLateReply) {
    auto state = std::make_shared<BlockFirstRpc>();
    CoroRpcAgent server;
    ASSERT_TRUE(
        server
            .registerFunction(
                RecvData,
                [state](const std::string_view&, std::string& response) {
                    const size_t call =
                        state->calls.fetch_add(1, std::memory_order_relaxed);
                    if (call == 0) {
                        state->first_entered.set_value();
                        state->first_released.wait();
                        state->first_finished.set_value();
                    }
                    response = call == 0 ? "OLD0" : call == 1 ? "NEW1" : "NEW2";
                })
            .ok());
    uint16_t port = 0;
    ASSERT_TRUE(server.start(port, false, 2).ok());

    const std::string address = uniqueLoopbackAddress(port);
    std::array<char, 4> target{};
    auto entered = state->first_entered.get_future();
    auto finished = state->first_finished.get_future();
    auto first = std::async(std::launch::async, [&] {
        return ControlClient::recvDataWithTimeout(
            address, 0, target.data(), target.size(),
            std::chrono::milliseconds(100));
    });

    if (entered.wait_for(std::chrono::seconds(1)) !=
        std::future_status::ready) {
        state->release_first.set_value();
        first.wait();
        EXPECT_TRUE(server.stop().ok());
        FAIL() << "first RecvData handler was not entered";
        return;
    }
    if (first.wait_for(std::chrono::seconds(1)) != std::future_status::ready) {
        state->release_first.set_value();
        first.wait();
        EXPECT_TRUE(server.stop().ok());
        FAIL() << "first RecvData call did not time out";
        return;
    }
    auto first_status = first.get();
    EXPECT_TRUE(first_status.IsTimeout()) << first_status.ToString();

    auto second_status = ControlClient::recvDataWithTimeout(
        address, 0, target.data(), target.size(), std::chrono::seconds(1));
    EXPECT_TRUE(second_status.ok()) << second_status.ToString();
    EXPECT_EQ(target, (std::array<char, 4>{'N', 'E', 'W', '1'}));

    state->release_first.set_value();
    EXPECT_EQ(finished.wait_for(std::chrono::seconds(1)),
              std::future_status::ready);

    target.fill(0);
    auto third_status = ControlClient::recvDataWithTimeout(
        address, 0, target.data(), target.size(), std::chrono::seconds(1));
    EXPECT_TRUE(third_status.ok()) << third_status.ToString();
    EXPECT_EQ(target, (std::array<char, 4>{'N', 'E', 'W', '2'}));
    EXPECT_EQ(state->calls.load(std::memory_order_relaxed), 3u);
    EXPECT_TRUE(server.stop().ok());
}

TEST(TcpTransportConfigTest, TimeoutTextRemainsRpcServiceError) {
    CoroRpcAgent server;
    ASSERT_TRUE(
        server
            .registerFunction(
                SendData, [](const std::string_view&,
                             std::string& response) { response = "time out"; })
            .ok());
    uint16_t port = 0;
    ASSERT_TRUE(server.start(port).ok());

    char source = 0;
    auto status = ControlClient::sendDataWithTimeout(
        uniqueLoopbackAddress(port), 0, &source, sizeof(source),
        std::chrono::milliseconds(500));

    EXPECT_TRUE(status.IsRpcServiceError()) << status.ToString();
    EXPECT_FALSE(status.IsTimeout()) << status.ToString();
    EXPECT_NE(status.ToString().find("time out"), std::string::npos);
    EXPECT_TRUE(server.stop().ok());
}

TEST(TcpTransportConfigTest, DisabledDataRpcTimeoutAllowsSlowPeer) {
    CoroRpcAgent server;
    ASSERT_TRUE(
        server
            .registerFunction(SendData,
                              [](const std::string_view&, std::string&) {
                                  std::this_thread::sleep_for(
                                      std::chrono::milliseconds(200));
                              })
            .ok());
    uint16_t port = 0;
    ASSERT_TRUE(server.start(port).ok());

    char source = 0;
    const auto started_at = std::chrono::steady_clock::now();
    auto status = ControlClient::sendDataWithTimeout(
        uniqueLoopbackAddress(port), 0, &source, sizeof(source),
        std::chrono::milliseconds(-1));
    const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - started_at);

    EXPECT_TRUE(status.ok()) << status.ToString();
    EXPECT_GE(elapsed.count(), 200);
    EXPECT_TRUE(server.stop().ok());
}

TEST(TcpTransportConfigTest, ExpiredBudgetBeforeRequestIsSafeToRetry) {
    std::atomic<size_t> calls{0};
    CoroRpcAgent server;
    ASSERT_TRUE(
        server
            .registerFunction(SendData,
                              [&calls](const std::string_view&, std::string&) {
                                  calls.fetch_add(1, std::memory_order_relaxed);
                              })
            .ok());
    uint16_t port = 0;
    ASSERT_TRUE(server.start(port).ok());

    const std::string address = uniqueLoopbackAddress(port);
    char source = 0;
    ASSERT_TRUE(
        ControlClient::sendData(address, 0, &source, sizeof(source)).ok());

    auto expired = ControlClient::sendDataWithTimeout(
        address, 0, &source, sizeof(source), std::chrono::milliseconds(0));
    EXPECT_TRUE(expired.IsRpcConnectionError()) << expired.ToString();
    EXPECT_FALSE(expired.IsTimeout()) << expired.ToString();
    EXPECT_EQ(calls.load(std::memory_order_relaxed), 1u);

    auto retry = ControlClient::sendData(address, 0, &source, sizeof(source));
    EXPECT_TRUE(retry.ok()) << retry.ToString();
    EXPECT_EQ(calls.load(std::memory_order_relaxed), 2u);
    EXPECT_TRUE(server.stop().ok());
}

TEST(TcpTransportConfigTest, TimeoutPublishesTransferTimeout) {
    std::atomic<size_t> calls{0};
    CoroRpcAgent server;
    ASSERT_TRUE(
        server
            .registerFunction(SendData,
                              [&calls](const std::string_view&, std::string&) {
                                  calls.fetch_add(1, std::memory_order_relaxed);
                                  std::this_thread::sleep_for(
                                      std::chrono::milliseconds(500));
                              })
            .ok());
    uint16_t port = 0;
    ASSERT_TRUE(server.start(port).ok());

    std::array<char, 4> source{};
    std::array<char, 4> target{};
    auto metadata = makeP2PMetadata();
    ASSERT_TRUE(metadata->segmentManager()
                    .updateLocal([&](SegmentDesc& desc) {
                        desc.type = SegmentType::Memory;
                        desc.rpc_server_addr = uniqueLoopbackAddress(port);
                        BufferDesc buffer;
                        buffer.addr = reinterpret_cast<uint64_t>(target.data());
                        buffer.length = target.size();
                        std::get<MemorySegmentDesc>(desc.detail)
                            .buffers.push_back(buffer);
                        return Status::OK();
                    })
                    .ok());

    auto conf = std::make_shared<Config>();
    conf->set("transports/tcp/data_rpc_timeout_ms", 100LL);
    conf->set("transports/tcp/max_retry_count", 3);
    TcpTransport transport;
    std::string name = "tcp-timeout-client";
    ASSERT_TRUE(transport.install(name, metadata, nullptr, conf).ok());

    TcpTask task;
    task.request.opcode = Request::WRITE;
    task.request.source = source.data();
    task.request.target_id = LOCAL_SEGMENT_ID;
    task.request.target_offset = reinterpret_cast<uint64_t>(target.data());
    task.request.length = target.size();
    TcpTransportTestPeer::startTransfer(transport, &task);

    EXPECT_EQ(task.status_word.load(), TransferStatusEnum::TIMEOUT);
    EXPECT_EQ(task.transferred_bytes.load(), 0u);
    EXPECT_TRUE(task.non_replayable_failure);
    EXPECT_EQ(calls.load(std::memory_order_relaxed), 1u);

    EXPECT_TRUE(transport.uninstall().ok());
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    EXPECT_TRUE(server.stop().ok());
}

TEST(TcpTransportConfigTest, ReadTimeoutRetriesOnFreshClient) {
    auto state = std::make_shared<BlockFirstRpc>();
    CoroRpcAgent server;
    ASSERT_TRUE(
        server
            .registerFunction(
                RecvData,
                [state](const std::string_view&, std::string& response) {
                    if (state->calls.fetch_add(1, std::memory_order_relaxed) ==
                        0) {
                        state->first_entered.set_value();
                        state->first_released.wait();
                        state->first_finished.set_value();
                    }
                    response = "DATA";
                })
            .ok());
    uint16_t port = 0;
    ASSERT_TRUE(server.start(port, false, 2).ok());

    std::array<char, 4> source{};
    std::array<char, 4> remote{};
    auto metadata = makeP2PMetadata();
    ASSERT_TRUE(metadata->segmentManager()
                    .updateLocal([&](SegmentDesc& desc) {
                        desc.type = SegmentType::Memory;
                        desc.rpc_server_addr = uniqueLoopbackAddress(port);
                        BufferDesc buffer;
                        buffer.addr = reinterpret_cast<uint64_t>(remote.data());
                        buffer.length = remote.size();
                        std::get<MemorySegmentDesc>(desc.detail)
                            .buffers.push_back(buffer);
                        return Status::OK();
                    })
                    .ok());

    auto conf = std::make_shared<Config>();
    conf->set("transports/tcp/data_rpc_timeout_ms", 100LL);
    conf->set("transports/tcp/max_retry_count", 1);
    conf->set("transports/tcp/retry_base_delay_ms", 1ULL);
    TcpTransport transport;
    std::string name = "tcp-read-timeout-retry";
    ASSERT_TRUE(transport.install(name, metadata, nullptr, conf).ok());

    TcpTask task;
    task.request.opcode = Request::READ;
    task.request.source = source.data();
    task.request.target_id = LOCAL_SEGMENT_ID;
    task.request.target_offset = reinterpret_cast<uint64_t>(remote.data());
    task.request.length = remote.size();

    auto entered = state->first_entered.get_future();
    auto finished = state->first_finished.get_future();
    auto transfer = std::async(std::launch::async, [&] {
        TcpTransportTestPeer::startTransfer(transport, &task);
    });
    if (entered.wait_for(std::chrono::seconds(1)) !=
        std::future_status::ready) {
        state->release_first.set_value();
        transfer.wait();
        EXPECT_TRUE(transport.uninstall().ok());
        EXPECT_TRUE(server.stop().ok());
        FAIL() << "first READ handler was not entered";
        return;
    }
    if (transfer.wait_for(std::chrono::seconds(2)) !=
        std::future_status::ready) {
        state->release_first.set_value();
        transfer.wait();
        EXPECT_TRUE(transport.uninstall().ok());
        EXPECT_TRUE(server.stop().ok());
        FAIL() << "READ did not recover on its configured retry";
        return;
    }
    transfer.get();

    EXPECT_EQ(task.status_word.load(), TransferStatusEnum::COMPLETED);
    EXPECT_EQ(task.transferred_bytes.load(), source.size());
    EXPECT_EQ(source, (std::array<char, 4>{'D', 'A', 'T', 'A'}));
    EXPECT_EQ(state->calls.load(std::memory_order_relaxed), 2u);

    state->release_first.set_value();
    EXPECT_EQ(finished.wait_for(std::chrono::seconds(1)),
              std::future_status::ready);
    EXPECT_TRUE(transport.uninstall().ok());
    EXPECT_TRUE(server.stop().ok());
}

TEST(TcpTransportCapabilityTest, CpuPlatformCanSelectPeerGpu) {
    if (Platform::getLoader().type() != "cpu") {
        GTEST_SKIP() << "Requires a CPU-only build";
    }

    auto conf = std::make_shared<Config>();
    conf->set("transports/tcp/max_concurrent_tasks", 1);
    auto transport = std::make_shared<TcpTransport>();
    auto metadata = makeP2PMetadata();
    std::string name = "tcp-cpu-peer-gpu";
    ASSERT_TRUE(transport->install(name, metadata, nullptr, conf).ok());

    TransportSelector selector(conf);
    std::array<std::shared_ptr<Transport>, kSupportedTransportTypes>
        transports{};
    transports[TCP] = transport;
    const std::vector<TransportType> tcp_buffer{TCP};
    const std::vector<TransportType> no_tcp_buffer{};

    struct Case {
        const char* name;
        MemoryType local;
        MemoryType peer;
        bool peer_exports_tcp;
        TransportType expected;
    };
    const Case cases[] = {
        {"host_peer_host", MTYPE_CPU, MTYPE_CPU, true, TCP},
        {"host_peer_gpu", MTYPE_CPU, MTYPE_CUDA, true, TCP},
        {"host_peer_gpu_without_tcp", MTYPE_CPU, MTYPE_CUDA, false, UNSPEC},
        {"local_gpu_peer_host", MTYPE_CUDA, MTYPE_CPU, true, UNSPEC},
        {"local_gpu_peer_gpu", MTYPE_CUDA, MTYPE_CUDA, true, UNSPEC},
    };
    // These are selection metadata, not device allocations or GPU transfers.
    // READ and WRITE keep the same local/peer roles in SelectionContext.
    for (const auto& test : cases) {
        SCOPED_TRACE(test.name);
        SelectionContext context{};
        context.segment_type = SegmentType::Memory;
        context.local_memory_type = test.local;
        context.remote_memory_type = test.peer;
        context.buffer_transports =
            test.peer_exports_tcp ? &tcp_buffer : &no_tcp_buffer;
        context.transfer_size = 1;
        EXPECT_EQ(selector.select(context, transports, 0, TCP).transport,
                  test.expected);
    }

    EXPECT_TRUE(transport->uninstall().ok());
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

}  // namespace
}  // namespace tent
}  // namespace mooncake
