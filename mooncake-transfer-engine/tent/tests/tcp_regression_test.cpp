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

#include <algorithm>
#include <array>
#include <atomic>
#include <cstdlib>
#include <future>
#include <limits>
#include <new>
#include <endian.h>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "tent/common/config.h"
#include "tent/runtime/control_plane.h"
#include "tent/runtime/transfer_engine_impl.h"
#include "tent/transport/tcp/tcp_transport.h"
#include "rpc_fault_peer.h"

namespace {
thread_local size_t fail_allocation_size = 0;
std::atomic<int> injected_failures{0};
}  // namespace

// Inject the worker's SendData allocation. Also prevent a regressed size guard
// from allocating a multi-GiB payload or reading beyond the small test buffer.
[[gnu::noinline]] void* operator new(size_t size) {
    if ((size && size == fail_allocation_size) ||
        size > mooncake::tent::kTcpMaxReadBytes) {
        fail_allocation_size = 0;
        ++injected_failures;
        throw std::bad_alloc();
    }
    if (void* ptr = std::malloc(size ? size : 1)) return ptr;
    throw std::bad_alloc();
}
[[gnu::noinline]] void operator delete(void* ptr) noexcept { std::free(ptr); }
[[gnu::noinline]] void operator delete(void* ptr, size_t) noexcept {
    std::free(ptr);
}
// Pair the nothrow form too: GoogleTest uses it for temporary buffers under
// ASan.
[[gnu::noinline]] void* operator new(size_t size,
                                     const std::nothrow_t&) noexcept {
    try {
        return ::operator new(size);
    } catch (const std::bad_alloc&) {
        return nullptr;
    }
}
[[gnu::noinline]] void operator delete(void* ptr,
                                       const std::nothrow_t&) noexcept {
    ::operator delete(ptr);
}

namespace mooncake::tent {
class TcpTransportTestPeer {
   public:
    static void arm(TcpTransport& tcp, size_t size) {
        tcp.thread_pool_->enqueue([size] { fail_allocation_size = size; })
            .get();
    }
    static void drain(TcpTransport& tcp) {
        tcp.thread_pool_->enqueue([] {}).get();
    }
    static void retries(TcpTransport& tcp, size_t count) {
        tcp.params_.max_retry_count = count;
    }
};

namespace {
using namespace std::chrono_literals;

class ControlledTcp : public TcpTransport {
   public:
    bool pause = false;
    TcpSubBatch* last_batch = nullptr;
    std::promise<void> entered, resume;
    std::shared_future<void> resumed = resume.get_future().share();
    std::weak_ptr<int> callback_lifetime;

    Status submitTransferTasks(SubBatchRef batch,
                               const std::vector<Request>& requests) override {
        last_batch = static_cast<TcpSubBatch*>(batch);
        auto notify = batch->notify_progress;
        if (pause) {
            auto marker = std::make_shared<int>(0);
            callback_lifetime = marker;
            batch->notify_progress = [this, marker, notify](BatchID id) {
                // Stack copies keep the test itself safe on the old version.
                auto wait = resumed;
                auto callback = notify;
                entered.set_value();
                wait.wait();
                if (callback) callback(id);
            };
        }
        auto status = TcpTransport::submitTransferTasks(batch, requests);
        batch->notify_progress = std::move(notify);
        return status;
    }
};

class TcpRegression : public testing::Test {
   protected:
    std::array<char, 8192> data{};
    std::unique_ptr<TransferEngineImpl> engine;
    std::shared_ptr<ControlledTcp> tcp;
    std::shared_ptr<ControlService> metadata;

    void SetUp() override { startEngine(); }

    void startEngine(const std::string& port = "0") {
        auto config = std::make_shared<Config>();
        config->set("metadata_type", "p2p");
        config->set("metadata_servers", "");
        config->set("rpc_server_hostname", "127.0.0.1");
        config->set("rpc_server_port", port);
        config->set("rpc_server_threads", 2);
        config->set("enable_progress_worker", true);
        config->set("enable_auto_failover_on_poll", false);
        config->set("transports/tcp/enable", true);
        config->set("transports/rdma/enable", false);
        config->set("transports/shm/enable", false);
        config->set("transports/tcp/max_concurrent_tasks", 1);
        config->set("transports/tcp/max_retry_count", 0);
        engine = std::make_unique<TransferEngineImpl>(config);
        ASSERT_TRUE(engine->available());
        tcp = std::make_shared<ControlledTcp>();
        metadata = std::make_shared<ControlService>("p2p", "", nullptr);
        auto name = engine->getSegmentName();
        ASSERT_TRUE(tcp->install(name, metadata, nullptr, config).ok());
        engine->swapTransportForTest(TCP, tcp);
        ASSERT_TRUE(engine->registerLocalMemory(data.data(), data.size()).ok());
        ASSERT_TRUE(metadata->segmentManager()
                        .updateLocal([&](SegmentDesc& desc) {
                            desc.type = SegmentType::Memory;
                            desc.rpc_server_addr = name;
                            BufferDesc buffer;
                            buffer.addr =
                                reinterpret_cast<uint64_t>(data.data());
                            buffer.length = data.size();
                            std::get<MemorySegmentDesc>(desc.detail)
                                .buffers.push_back(buffer);
                            return Status::OK();
                        })
                        .ok());
    }

    void TearDown() override {
        if (tcp) {
            tcp->resume.set_value();
            EXPECT_TRUE(tcp->uninstall().ok());
        }
        if (engine && engine->available()) {
            EXPECT_TRUE(
                engine->unregisterLocalMemory(data.data(), data.size()).ok());
        }
    }

    Request writeRequest(size_t length = 16) {
        Request request{};
        request.opcode = Request::WRITE;
        request.source = data.data();
        request.target_id = LOCAL_SEGMENT_ID;
        request.target_offset = reinterpret_cast<uint64_t>(data.data() + 4096);
        request.length = length;
        request.transport_hint = TCP;
        return request;
    }

    void checkImmediateFree(TransferStatusEnum expected) {
        tcp->pause = true;
        auto batch = engine->allocateBatch(1);
        ASSERT_TRUE(engine->submitTransfer(batch, {writeRequest()}).ok());
        auto entered = tcp->entered.get_future();
        ASSERT_EQ(entered.wait_for(5s), std::future_status::ready);
        TransferStatus status{};
        EXPECT_TRUE(engine->getTransferStatus(batch, status).ok());
        EXPECT_EQ(status.s, expected);
        EXPECT_TRUE(engine->freeBatch(batch).ok());
        EXPECT_EQ(engine->aliveBatchCountForTest(), 0u);
        EXPECT_FALSE(tcp->callback_lifetime.expired());
        // TearDown releases and joins the worker before destroying the engine.
    }
};

TEST_F(TcpRegression, SuccessfulTerminalAllowsImmediateFreeBatch) {
    checkImmediateFree(COMPLETED);
}

TEST_F(TcpRegression, FailedTerminalAllowsImmediateFreeBatch) {
    ASSERT_TRUE(metadata->segmentManager()
                    .updateLocal([](SegmentDesc& desc) {
                        desc.rpc_server_addr.clear();
                        return Status::OK();
                    })
                    .ok());
    checkImmediateFree(FAILED);
}

TEST_F(TcpRegression, WorkerThrowFailsBatchAndNextRequestCompletes) {
    std::fill_n(data.data(), 4096, 'W');
    injected_failures = 0;
    // libstdc++ reserve(n) allocates n + one terminating byte.
    TcpTransportTestPeer::arm(*tcp, sizeof(XferDataDesc) + 4096 + 1);
    for (auto expected : {FAILED, COMPLETED}) {
        auto batch = engine->allocateBatch(1);
        ASSERT_TRUE(engine->submitTransfer(batch, {writeRequest(4096)}).ok());
        // A FIFO fence on the same single worker also bounds the old PENDING
        // bug.
        TcpTransportTestPeer::drain(*tcp);
        TransferStatus status{};
        ASSERT_TRUE(engine->getTransferStatus(batch, status).ok());
        EXPECT_EQ(status.s, expected);
        EXPECT_EQ(status.transferred_bytes, expected == FAILED ? 0u : 4096u);
        EXPECT_EQ(injected_failures.load(), 1);
        EXPECT_EQ(data[4096], expected == FAILED ? '\0' : 'W');
        if (status.s == PENDING) {
            // Failed-test cleanup only, after the worker has returned.
            tcp->last_batch->task_list[0].status_word.store(FAILED);
        }
        EXPECT_TRUE(engine->freeBatch(batch).ok());
        EXPECT_EQ(engine->aliveBatchCountForTest(), 0u);
    }
}

TEST_F(TcpRegression, ServerChecksEveryRegistrationPermission) {
    ASSERT_TRUE(engine->unregisterLocalMemory(data.data(), data.size()).ok());
    for (auto permission :
         {kLocalReadWrite, kGlobalReadOnly, kGlobalReadWrite}) {
        ASSERT_TRUE(
            engine->registerLocalMemory(data.data(), data.size(), permission)
                .ok());
        for (auto opcode : {Request::READ, Request::WRITE}) {
            SCOPED_TRACE(testing::Message() << permission << "/" << opcode);
            data.fill('R');
            std::array<char, 8192> local;
            local.fill('L');
            const bool allowed =
                permission == kGlobalReadWrite ||
                (permission == kGlobalReadOnly && opcode == Request::READ);
            auto call = opcode == Request::READ ? ControlClient::recvData
                                                : ControlClient::sendData;
            // Direct RPC bypasses client-side registration/routing policy.
            auto status = call(engine->getSegmentName(),
                               reinterpret_cast<uint64_t>(data.data()),
                               local.data(), local.size());
            EXPECT_EQ(status.ok(), allowed) << status.ToString();
            if (!allowed) {
                EXPECT_NE(status.ToString().find("permission denied"),
                          std::string::npos);
            }
            const char remote_value =
                allowed && opcode == Request::WRITE ? 'L' : 'R';
            const char local_value =
                allowed && opcode == Request::READ ? 'R' : 'L';
            EXPECT_TRUE(std::all_of(data.begin(), data.end(),
                                    [=](char c) { return c == remote_value; }));
            EXPECT_TRUE(std::all_of(local.begin(), local.end(),
                                    [=](char c) { return c == local_value; }));
        }
        ASSERT_TRUE(
            engine->unregisterLocalMemory(data.data(), data.size()).ok());
    }
    ASSERT_TRUE(engine->registerLocalMemory(data.data(), data.size()).ok());
}

TEST_F(TcpRegression, PayloadCapsRejectBeforeAllocationOrDataRpc) {
    CoroRpcAgent receiver;
    std::atomic<int> calls{0};
    for (auto id : {SendData, RecvData}) {
        ASSERT_TRUE(
            receiver.registerFunction(id, [&](const auto&, auto&) { ++calls; })
                .ok());
    }
    uint16_t port = 0;
    ASSERT_TRUE(receiver.start(port).ok());
    auto address = "127.0.0.1:" + std::to_string(port);
    ASSERT_TRUE(ControlClient::sendData(address, 0, data.data(), 1).ok());
    ASSERT_EQ(calls.load(), 1);
    calls = 0;
    injected_failures = 0;
    for (auto opcode : {Request::READ, Request::WRITE}) {
        for (size_t length : {tcpMaxTransferBytes(opcode) + 1,
                              std::numeric_limits<size_t>::max()}) {
            auto call = opcode == Request::READ ? ControlClient::recvData
                                                : ControlClient::sendData;
            EXPECT_NO_THROW({
                EXPECT_TRUE(call(address,
                                 reinterpret_cast<uint64_t>(data.data()),
                                 data.data(), length)
                                .IsInvalidArgument());
            });
        }
    }
    EXPECT_EQ(injected_failures.load(), 0);
    EXPECT_EQ(calls.load(), 0);
    EXPECT_EQ(kTcpMaxWriteBytes + sizeof(XferDataDesc),
              std::numeric_limits<uint32_t>::max());
    EXPECT_TRUE(receiver.stop().ok());
}

TEST_F(TcpRegression, MalformedWriteLengthCannotOverflowOrModifyTarget) {
    data.fill('T');
    const auto original = data;
    CoroRpcAgent rpc;
    for (size_t length : {size_t{1}, kTcpMaxWriteBytes + 1,
                          std::numeric_limits<size_t>::max()}) {
        XferDataDesc header{htole64(reinterpret_cast<uint64_t>(data.data())),
                            htole64(length)};
        std::string request(reinterpret_cast<char*>(&header), sizeof(header));
        std::string response;
        ASSERT_TRUE(
            rpc.call(engine->getSegmentName(), SendData, request, response)
                .ok());
        EXPECT_EQ(response, "SendData failed: invalid request size");
        EXPECT_EQ(data, original);
    }
}

TEST_F(TcpRegression, FirstDataRpcAfterSamePortRestartSucceeds) {
    std::array<char, 16> local;
    for (auto opcode : {Request::WRITE, Request::READ}) {
        SCOPED_TRACE(opcode);
        const auto address = engine->getSegmentName();
        local.fill('W');
        ASSERT_TRUE(ControlClient::sendData(
                        address, reinterpret_cast<uint64_t>(data.data()),
                        local.data(), local.size())
                        .ok());

        ASSERT_TRUE(tcp->uninstall().ok());
        ASSERT_TRUE(
            engine->unregisterLocalMemory(data.data(), data.size()).ok());
        engine.reset();
        tcp.reset();
        metadata.reset();
        // As in rpc_reconnect_test, allow the FIN to reach the idle socket.
        std::this_thread::sleep_for(100ms);
        data.fill('N');
        startEngine(address.substr(address.rfind(':') + 1));
        ASSERT_EQ(engine->getSegmentName(), address) << "port drifted";

        auto call = opcode == Request::WRITE ? ControlClient::sendData
                                             : ControlClient::recvData;
        auto status = call(address, reinterpret_cast<uint64_t>(data.data()),
                           local.data(), local.size());
        ASSERT_TRUE(status.ok()) << status.ToString();
        EXPECT_TRUE(std::all_of(local.begin(), local.end(), [&](char c) {
            return c == (opcode == Request::WRITE ? 'W' : 'N');
        }));
        EXPECT_TRUE(
            std::all_of(data.begin(), data.begin() + local.size(), [&](char c) {
                return c == (opcode == Request::WRITE ? 'W' : 'N');
            }));
        EXPECT_TRUE(std::all_of(data.begin() + local.size(), data.end(),
                                [](char c) { return c == 'N'; }));
    }
}

TEST_F(TcpRegression, LostOrTruncatedDataReplyIsUnknownAndNeverReplayed) {
    for (auto fault :
         {RpcFaultPeer::Fault::Eof, RpcFaultPeer::Fault::Reset,
          RpcFaultPeer::Fault::ShortHeader, RpcFaultPeer::Fault::ShortBody}) {
        SCOPED_TRACE(static_cast<int>(fault));
        for (auto opcode : {Request::WRITE, Request::READ}) {
            SCOPED_TRACE(opcode);
            RpcFaultPeer peer(fault);
            ASSERT_TRUE(peer.available());
            ASSERT_TRUE(
                ControlClient::sendData(peer.address(), 0, data.data(), 1)
                    .ok());
            auto call = opcode == Request::WRITE ? ControlClient::sendData
                                                 : ControlClient::recvData;
            auto status = call(peer.address(), 0, data.data(), 1);
            EXPECT_TRUE(status.IsRpcUnknownEffect()) << status.ToString();
            EXPECT_FALSE(status.IsRpcServiceError());
            EXPECT_EQ(peer.requests.load(), 2);
            EXPECT_EQ(peer.connections.load(), 1);
            // The uncertain call retired its socket. A new explicit request
            // reconnects, rather than returning the broken stream to the pool.
            ASSERT_TRUE(
                ControlClient::sendData(peer.address(), 0, data.data(), 1)
                    .ok());
            EXPECT_EQ(peer.requests.load(), 3);
            EXPECT_EQ(peer.connections.load(), 2);
        }
    }
}

TEST_F(TcpRegression, UnknownEffectSuppressesTcpRetryAndEngineFailover) {
    TcpTransportTestPeer::retries(*tcp, 3);
    Config config;
    config.set("enable_auto_failover_on_poll", true);
    config.set("max_failover_attempts", 3);
    ASSERT_TRUE(engine
                    ->publishRuntimeConfigForTest(
                        buildTentConfigBundle(config, 1).runtime)
                    .ok());
    for (auto opcode : {Request::READ, Request::WRITE}) {
        SCOPED_TRACE(opcode);
        RpcFaultPeer peer(RpcFaultPeer::Fault::Eof);
        ASSERT_TRUE(peer.available());
        ASSERT_TRUE(metadata->segmentManager()
                        .updateLocal([&](SegmentDesc& desc) {
                            desc.rpc_server_addr = peer.address();
                            return Status::OK();
                        })
                        .ok());
        for (int attempt = 0; attempt < 2; ++attempt) {
            // Warm the production worker's thread-local connection pool.
            auto request = writeRequest();
            if (attempt == 1) request.opcode = opcode;
            auto batch = engine->allocateBatch(1);
            ASSERT_TRUE(engine->submitTransfer(batch, {request}).ok());
            TcpTransportTestPeer::drain(*tcp);
            TransferStatus status{};
            ASSERT_TRUE(engine->getTransferStatus(batch, status).ok());
            EXPECT_EQ(status.s, attempt == 0 ? COMPLETED : FAILED);
            if (attempt == 1) {
                auto error = tcp->getTransferStatus(tcp->last_batch, 0, status);
                EXPECT_TRUE(error.IsRpcUnknownEffect()) << error.ToString();
                EXPECT_EQ(status.transferred_bytes, 0u);
                EXPECT_TRUE(engine->progressBatch(batch, status).ok());
                EXPECT_EQ(status.s, FAILED);
            }
            EXPECT_TRUE(engine->freeBatch(batch).ok());
        }
        EXPECT_EQ(peer.requests.load(), 2);
        EXPECT_EQ(peer.connections.load(), 1);
    }
}

TEST(DataRpcDeadline, ExpiredBeforeDispatchSendsNothing) {
    RpcFaultPeer peer(RpcFaultPeer::Fault::Eof);
    ASSERT_TRUE(peer.available());
    CoroRpcAgent client;
    std::string response = "unchanged";
    auto status = client.callOwned(peer.address(), SendData, "unused", response,
                                   std::chrono::steady_clock::now());
    EXPECT_TRUE(status.IsRpcConnectionError()) << status.ToString();
    EXPECT_FALSE(status.IsRpcUnknownEffect());
    EXPECT_EQ(response, "unchanged");
    EXPECT_EQ(peer.requests.load(), 0);
    EXPECT_EQ(peer.connections.load(), 0);
}

TEST(DataRpcDeadline, TimeoutAfterDispatchIsUnknownAndNeverReplayed) {
    RpcFaultPeer peer(RpcFaultPeer::Fault::Timeout);
    ASSERT_TRUE(peer.available());
    CoroRpcAgent client;
    std::string response;
    ASSERT_TRUE(
        client.callOwned(peer.address(), SendData, "warm", response).ok());
    response = "unchanged";
    const auto start = std::chrono::steady_clock::now();
    auto status = client.callOwned(peer.address(), SendData, "once", response,
                                   start + 50ms);
    EXPECT_TRUE(status.IsRpcUnknownEffect()) << status.ToString();
    EXPECT_FALSE(status.IsRpcServiceError());
    EXPECT_LT(std::chrono::steady_clock::now() - start, 1s);
    EXPECT_EQ(response, "unchanged");
    EXPECT_EQ(peer.requests.load(), 2);
    EXPECT_EQ(peer.connections.load(), 1);
}

TEST(DataRpcReply, AnsweredHandlerErrorIsDistinctAndNeverReplayed) {
    CoroRpcAgent server, client;
    std::atomic<int> side_effects{0};
    ASSERT_TRUE(server
                    .registerFunction(SendData,
                                      [&](const auto&, auto&) {
                                          ++side_effects;
                                          throw std::runtime_error(
                                              "after the WRITE");
                                      })
                    .ok());
    uint16_t port = 0;
    ASSERT_TRUE(server.start(port).ok());
    std::string response;
    auto status = client.callOwned("127.0.0.1:" + std::to_string(port),
                                   SendData, "once", response);
    EXPECT_EQ(status.code(), Status::Code::kRpcServiceError);
    EXPECT_FALSE(status.IsRpcUnknownEffect());
    EXPECT_EQ(side_effects.load(), 1);
}

}  // namespace
}  // namespace mooncake::tent
