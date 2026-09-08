// Copyright 2026 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <memory>
#include <random>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include "tent/runtime/control_plane.h"
#include "tent/runtime/platform.h"
#include "tent/transfer_engine.h"
#include "tent/transport/hp_tcp/hp_tcp_protocol.h"
#include "tent/transport/hp_tcp/hp_tcp_transport.h"

namespace mooncake::tent {

class HighPerformanceTcpTransportTestPeer {
   public:
    static void failWorker(HighPerformanceTcpTransport& transport) {
        asio::post(transport.workers_->ioContext(0), [] {
            throw std::runtime_error("injected HP TCP worker failure");
        });
    }

    static bool hasFailedWorker(const HighPerformanceTcpTransport& transport) {
        return transport.workers_->hasFailedWorker();
    }

    static Status barrier(HighPerformanceTcpTransport& transport) {
        return transport.workers_->barrier();
    }
};

namespace {

template <class Predicate>
bool WaitUntil(Predicate predicate,
               std::chrono::milliseconds timeout = std::chrono::seconds(2)) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (!predicate() && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::yield();
    }
    return predicate();
}

std::shared_ptr<ControlService> MakeLocalMetadata() {
    // Standalone transports also use the process-wide platform loader.
    // Initialize it with a config before the full-engine cases probe topology.
    (void)Platform::getLoader(std::make_shared<Config>());
    auto metadata = std::make_shared<ControlService>("p2p", "", nullptr);
    EXPECT_TRUE(metadata->segmentManager()
                    .updateLocal([](SegmentDesc& segment) -> Status {
                        segment.name = "hp_transport_test";
                        segment.machine_id = "hp_transport_test_machine";
                        segment.rpc_server_addr = "127.0.0.1:40000";
                        segment.type = SegmentType::Memory;
                        std::get<MemorySegmentDesc>(segment.detail) =
                            MemorySegmentDesc{};
                        return Status::OK();
                    })
                    .ok());
    return metadata;
}

HighPerformanceTcpParams MakeParams() {
    HighPerformanceTcpParams params;
    params.bind_address = "127.0.0.1";
    params.advertise_address = "127.0.0.1";
    params.port = 0;
    params.worker_count = 2;
    params.connections_per_peer = 2;
    params.max_outstanding_tasks = 16;
    params.max_outstanding_bytes = 1 << 20;
    params.max_transfer_bytes = 1 << 20;
    params.connect_timeout_ms = 1000;
    params.progress_timeout_ms = 1000;
    return params;
}

bool ContainsTransport(const BufferDesc& desc, TransportType type) {
    return std::find(desc.transports.begin(), desc.transports.end(), type) !=
           desc.transports.end();
}

TEST(HighPerformanceTcpTransportTest,
     WorkerFailureDoesNotHideCommittedTerminalStatus) {
    auto metadata = MakeLocalMetadata();
    HighPerformanceTcpTransport transport(MakeParams());
    std::string segment_name = "hp_transport_test";
    ASSERT_TRUE(
        transport.install(segment_name, metadata, nullptr, nullptr).ok());

    Transport::SubBatchRef batch = nullptr;
    ASSERT_TRUE(transport.allocateSubBatch(batch, 1).ok());
    auto* hp_batch = dynamic_cast<HighPerformanceTcpSubBatch*>(batch);
    ASSERT_NE(hp_batch, nullptr);

    Request request{};
    request.length = 37;
    auto task = std::make_shared<HighPerformanceTcpTaskState>(
        request.length, 0, [](BatchID) {},
        HighPerformanceTcpBufferRegistry::Lease{});
    ASSERT_TRUE(task->completeOnce(COMPLETED, request.length));
    hp_batch->tasks.push_back(std::move(task));

    HighPerformanceTcpTransportTestPeer::failWorker(transport);
    ASSERT_TRUE(WaitUntil([&] {
        return HighPerformanceTcpTransportTestPeer::hasFailedWorker(transport);
    }));

    TransferStatus status{FAILED, 0};
    const Status result = transport.getTransferStatus(batch, 0, status);
    EXPECT_TRUE(result.ok()) << result.ToString();
    EXPECT_EQ(status.s, COMPLETED);
    EXPECT_EQ(status.transferred_bytes, request.length);

    EXPECT_TRUE(transport.freeSubBatch(batch).ok());
    EXPECT_TRUE(transport.quiesce().IsInternalError());
    EXPECT_TRUE(transport.uninstall().ok());
}

TEST(HighPerformanceTcpTransportTest, UnknownWriteOutcomeIsPermanent) {
    HighPerformanceTcpTransport transport(MakeParams());
    Transport::SubBatchRef batch = nullptr;
    ASSERT_TRUE(transport.allocateSubBatch(batch, 1).ok());
    auto* hp_batch = dynamic_cast<HighPerformanceTcpSubBatch*>(batch);
    ASSERT_NE(hp_batch, nullptr);

    auto uncertain_write = std::make_shared<HighPerformanceTcpTaskState>(
        0, 0, [](BatchID) {}, HighPerformanceTcpBufferRegistry::Lease{});
    ASSERT_TRUE(uncertain_write->completeOnce(
        FAILED, 0, HighPerformanceTcpStatus::kInternalError));
    hp_batch->tasks.push_back(std::move(uncertain_write));

    TransferStatus status{};
    const Status unsafe_replay = transport.getTransferStatus(batch, 0, status);
    EXPECT_TRUE(unsafe_replay.IsInvalidEntry()) << unsafe_replay.ToString();
    EXPECT_EQ(status.s, FAILED);
    EXPECT_TRUE(transport.freeSubBatch(batch).ok());
}

TEST(HighPerformanceTcpTransportTest,
     PublishesEndpointAndSeparatesLocalOnlyCapabilities) {
    auto metadata = MakeLocalMetadata();
    auto params = MakeParams();
    params.bind_address.clear();
    params.rail_addresses = {"127.0.0.1", "127.0.0.2"};
    HighPerformanceTcpTransport transport(std::move(params));
    std::string segment_name = "hp_transport_test";
    ASSERT_TRUE(
        transport.install(segment_name, metadata, nullptr, nullptr).ok());

    const SegmentDescRef local = metadata->segmentManager().getLocal();
    const auto attr_it = local->getMemory().transport_attrs.find(
        static_cast<int>(TransportType::HP_TCP));
    ASSERT_NE(attr_it, local->getMemory().transport_attrs.end());
    HighPerformanceTcpEndpointAttr endpoint;
    ASSERT_TRUE(
        DecodeHighPerformanceTcpEndpointAttr(attr_it->second, &endpoint).ok());
    ASSERT_EQ(endpoint.endpoints.size(), 2U);
    EXPECT_EQ(endpoint.endpoints[0].host, "127.0.0.1");
    EXPECT_EQ(endpoint.endpoints[1].host, "127.0.0.2");
    EXPECT_NE(endpoint.endpoints[0].port, 0);
    EXPECT_EQ(endpoint.endpoints[0].port, endpoint.endpoints[1].port);

    std::array<uint8_t, 64> local_only_storage{};
    BufferDesc local_only;
    local_only.addr = reinterpret_cast<uint64_t>(local_only_storage.data());
    local_only.length = local_only_storage.size();
    local_only.location = "cpu:0";
    MemoryOptions local_options;
    local_options.perm = kLocalReadWrite;
    ASSERT_TRUE(transport.addMemoryBuffer(local_only, local_options).ok());
    EXPECT_TRUE(transport.tracksLocalBuffer(local_only));
    EXPECT_FALSE(ContainsTransport(local_only, TransportType::HP_TCP));
    EXPECT_EQ(local_only.transport_attrs.count(TransportType::HP_TCP), 0U);

    std::array<uint8_t, 64> global_storage{};
    BufferDesc global;
    global.addr = reinterpret_cast<uint64_t>(global_storage.data());
    global.length = global_storage.size();
    global.location = "cpu:0";
    MemoryOptions global_options;
    global_options.perm = kGlobalReadWrite;
    ASSERT_TRUE(transport.addMemoryBuffer(global, global_options).ok());
    EXPECT_TRUE(ContainsTransport(global, TransportType::HP_TCP));
    const auto buffer_attr = global.transport_attrs.find(TransportType::HP_TCP);
    ASSERT_NE(buffer_attr, global.transport_attrs.end());
    HighPerformanceTcpBufferAttr decoded_buffer;
    ASSERT_TRUE(
        DecodeHighPerformanceTcpBufferAttr(buffer_attr->second, &decoded_buffer)
            .ok());
    EXPECT_NE(decoded_buffer.registration_id, 0U);
    EXPECT_EQ(decoded_buffer.permission, "global_read_write");

    ASSERT_TRUE(transport.quiesce().ok());
    ASSERT_TRUE(transport.removeMemoryBuffer(local_only).ok());
    ASSERT_TRUE(transport.removeMemoryBuffer(global).ok());
    ASSERT_TRUE(transport.uninstall().ok());
}

TEST(HighPerformanceTcpTransportTest, RejectsMismatchedSingleRailListener) {
    auto params = MakeParams();
    params.rail_addresses = {"127.0.0.2"};
    HighPerformanceTcpTransport transport(std::move(params));
    std::string segment_name = "hp_transport_test";
    EXPECT_TRUE(
        transport.install(segment_name, MakeLocalMetadata(), nullptr, nullptr)
            .IsInvalidArgument());
}

Status PublishBuffers(const std::shared_ptr<ControlService>& metadata,
                      const std::vector<BufferDesc>& buffers) {
    return metadata->segmentManager().updateLocal(
        [&](SegmentDesc& segment) -> Status {
            std::get<MemorySegmentDesc>(segment.detail).buffers = buffers;
            return Status::OK();
        });
}

Status WaitForTransportResult(HighPerformanceTcpTransport& transport,
                              Transport::SubBatchRef batch,
                              TransferStatus& transfer_status) {
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(2);
    Status result = Status::OK();
    while (std::chrono::steady_clock::now() < deadline) {
        result = transport.getTransferStatus(batch, 0, transfer_status);
        if (transfer_status.s != PENDING) return result;
        std::this_thread::yield();
    }
    return Status::InternalError("HP TCP test transfer did not finish");
}

// Test-only relay: observe the wire request and forward it to the real server.
// Pumped by the test thread; no transport worker waits for another callback.
class RailRelay {
   public:
    struct Observation {
        std::string source;
        std::string destination;
        HighPerformanceTcpRequestFrame request;
    };

    RailRelay(const std::string& address, uint16_t server_port)
        : acceptor_(io_, {asio::ip::make_address(address), 0}),
          backend_(asio::ip::make_address(address), server_port) {
        asio::co_spawn(io_, accept(), [this](std::exception_ptr error) {
            if (error) error_ = error;
        });
    }

    uint16_t port() const { return acceptor_.local_endpoint().port(); }
    void poll() {
        io_.poll();
        if (error_) std::rethrow_exception(error_);
    }
    size_t connections = 0;
    std::vector<Observation> observations;

   private:
    asio::awaitable<void> accept() {
        for (;;) {
            auto socket = co_await acceptor_.async_accept(asio::use_awaitable);
            ++connections;
            asio::co_spawn(io_, forward(std::move(socket)),
                           [this](std::exception_ptr error) {
                               if (error) error_ = error;
                           });
        }
    }

    asio::awaitable<void> forward(asio::ip::tcp::socket socket) {
        asio::ip::tcp::socket server(io_);
        co_await server.async_connect(backend_, asio::use_awaitable);
        for (;;) {
            std::array<uint8_t, kHighPerformanceTcpRequestSize> header{};
            co_await asio::async_read(socket, asio::buffer(header),
                                      asio::use_awaitable);
            HighPerformanceTcpRequestFrame request;
            if (!DecodeHighPerformanceTcpRequest(header.data(), header.size(),
                                                 &request)
                     .ok()) {
                throw std::runtime_error("invalid relayed HP TCP request");
            }
            co_await asio::async_write(server, asio::buffer(header),
                                       asio::use_awaitable);
            std::vector<uint8_t> payload(request.length);
            if (request.opcode == HighPerformanceTcpOpcode::kWrite) {
                co_await asio::async_read(socket, asio::buffer(payload),
                                          asio::use_awaitable);
                co_await asio::async_write(server, asio::buffer(payload),
                                           asio::use_awaitable);
            }
            std::array<uint8_t, kHighPerformanceTcpResponseSize> response{};
            co_await asio::async_read(server, asio::buffer(response),
                                      asio::use_awaitable);
            co_await asio::async_write(socket, asio::buffer(response),
                                       asio::use_awaitable);
            if (request.opcode == HighPerformanceTcpOpcode::kRead) {
                co_await asio::async_read(server, asio::buffer(payload),
                                          asio::use_awaitable);
                co_await asio::async_write(socket, asio::buffer(payload),
                                           asio::use_awaitable);
            }
            observations.push_back(
                {socket.remote_endpoint().address().to_string(),
                 socket.local_endpoint().address().to_string(), request});
        }
    }

    asio::io_context io_;
    asio::ip::tcp::acceptor acceptor_;
    asio::ip::tcp::endpoint backend_;
    std::exception_ptr error_;
};

void CheckRailPayloads(bool multi_rail) {
    constexpr size_t kLength = (4ULL << 20) + 97;
    constexpr size_t kOffset = 37;
    std::vector<uint8_t> remote(kLength);
    auto params = MakeParams();
    params.bind_address.clear();
    params.rail_addresses =
        multi_rail ? std::vector<std::string>{"127.0.0.1", "127.0.0.2"}
                   : std::vector<std::string>{"127.0.0.1"};
    params.max_outstanding_bytes = 8ULL << 20;
    params.max_transfer_bytes = 8ULL << 20;

    auto server_metadata = MakeLocalMetadata();
    uint16_t rpc_port = 0;
    ASSERT_TRUE(server_metadata->start(rpc_port).ok());
    const std::string server_name = "127.0.0.1:" + std::to_string(rpc_port);
    ASSERT_TRUE(server_metadata->segmentManager()
                    .updateLocal([&](SegmentDesc& segment) -> Status {
                        segment.name = server_name;
                        segment.rpc_server_addr = server_name;
                        return Status::OK();
                    })
                    .ok());

    HighPerformanceTcpTransport server(params);
    std::string installed_server_name = server_name;
    ASSERT_TRUE(
        server.install(installed_server_name, server_metadata, nullptr, nullptr)
            .ok());
    std::mt19937 data(3689);
    for (size_t i = 0; i < remote.size(); ++i) {
        remote[i] = static_cast<uint8_t>(data());
    }
    auto expected = remote;
    BufferDesc remote_desc;
    remote_desc.addr = reinterpret_cast<uint64_t>(remote.data());
    remote_desc.length = remote.size();
    remote_desc.location = "cpu:0";
    MemoryOptions remote_options;
    remote_options.type = HP_TCP;
    remote_options.perm = kGlobalReadWrite;
    ASSERT_TRUE(server.addMemoryBuffer(remote_desc, remote_options).ok());
    ASSERT_TRUE(PublishBuffers(server_metadata, {remote_desc}).ok());

    HighPerformanceTcpEndpointAttr endpoint;
    ASSERT_TRUE(DecodeHighPerformanceTcpEndpointAttr(
                    server_metadata->segmentManager()
                        .getLocal()
                        ->getMemory()
                        .transport_attrs.at(static_cast<int>(HP_TCP)),
                    &endpoint)
                    .ok());
    std::vector<std::unique_ptr<RailRelay>> relays;
    for (auto& rail : endpoint.endpoints) {
        relays.push_back(std::make_unique<RailRelay>(rail.host, rail.port));
        rail.port = relays.back()->port();
    }
    ASSERT_TRUE(server_metadata->segmentManager()
                    .updateLocal([&](SegmentDesc& segment) {
                        return EncodeHighPerformanceTcpEndpointAttr(
                            endpoint,
                            &std::get<MemorySegmentDesc>(segment.detail)
                                 .transport_attrs[static_cast<int>(HP_TCP)]);
                    })
                    .ok());

    auto config = std::make_shared<Config>();
    config->set("metadata_type", "p2p");
    config->set("transports/tcp/enable", false);
    config->set("transports/rdma/enable", false);
    config->set("transports/shm/enable", false);
    config->set("transports/hp_tcp/enable", true);
    config->set("transports/hp_tcp/bind_address", "");
    config->set("transports/hp_tcp/rail_addresses", params.rail_addresses);
    config->set("transports/hp_tcp/connections_per_peer", 2);
    // A stalled peer must not block the healthy peer on this same worker.
    config->set("transports/hp_tcp/worker_count", 1);
    config->set("transports/hp_tcp/progress_timeout_ms", 5000);
    std::vector<uint8_t> local(kLength, 0);
    std::vector<uint8_t> stalled_buffer(kLength);
    TransferEngine client(config);
    ASSERT_TRUE(client.available());
    SegmentID target = 0;
    ASSERT_TRUE(client.openSegment(target, server_name).ok());

    ASSERT_TRUE(client.registerLocalMemory(local.data(), local.size()).ok());
    // The first two small READs create both lanes; later cases reuse them.
    for (auto opcode : {Request::READ, Request::WRITE}) {
        const auto lengths =
            opcode == Request::READ
                ? std::vector<size_t>{4096, (2ULL << 20) - 1, 2ULL << 20,
                                      (4ULL << 20) + 3}
                : std::vector<size_t>{4096, (4ULL << 20) + 3};
        for (size_t length : lengths) {
            SCOPED_TRACE(::testing::Message()
                         << multi_rail << ":" << opcode << ":" << length);
            std::fill(local.begin(), local.end(), 0xa5);
            if (opcode == Request::WRITE) {
                for (size_t i = kOffset; i < kOffset + length; ++i)
                    expected[i] ^= 0xff;
                std::copy_n(expected.data() + kOffset, length,
                            local.data() + kOffset);
            }
            for (auto& relay : relays) relay->observations.clear();
            Request request{};
            request.opcode = opcode;
            request.source = local.data() + kOffset;
            request.target_id = target;
            request.target_offset = remote_desc.addr + kOffset;
            request.length = length;
            request.transport_hint = HP_TCP;
            const auto batch = client.allocateBatch(1);
            ASSERT_TRUE(client.submitTransfer(batch, {request}).ok());
            TransferStatus status{PENDING, 0};
            ASSERT_TRUE(WaitUntil([&] {
                for (auto& relay : relays) relay->poll();
                EXPECT_TRUE(client.getTransferStatus(batch, status).ok());
                return status.s != PENDING;
            }));
            ASSERT_EQ(status.s, COMPLETED);
            EXPECT_EQ(status.transferred_bytes, length);
            const size_t count =
                multi_rail && opcode == Request::READ && length >= (2ULL << 20)
                    ? 2
                    : 1;
            ASSERT_TRUE(WaitUntil([&] {
                size_t observed = 0;
                for (auto& relay : relays) {
                    relay->poll();
                    observed += relay->observations.size();
                }
                return observed == count;
            }));
            uint64_t cursor = request.target_offset;
            for (size_t rail = 0; rail < relays.size(); ++rail) {
                if (count == 2) {
                    ASSERT_EQ(relays[rail]->observations.size(), 1U);
                }
                for (const auto& seen : relays[rail]->observations) {
                    EXPECT_EQ(seen.source, params.rail_addresses[rail]);
                    EXPECT_EQ(seen.destination, params.rail_addresses[rail]);
                    EXPECT_EQ(seen.request.opcode,
                              opcode == Request::READ
                                  ? HighPerformanceTcpOpcode::kRead
                                  : HighPerformanceTcpOpcode::kWrite);
                    EXPECT_EQ(seen.request.remote_addr, cursor);
                    EXPECT_EQ(seen.request.length,
                              length / count + (rail < length % count ? 1 : 0));
                    cursor += seen.request.length;
                }
            }
            EXPECT_EQ(cursor, request.target_offset + length);
            EXPECT_EQ(std::memcmp(local.data() + kOffset,
                                  expected.data() + kOffset, length),
                      0);
            if (opcode == Request::WRITE) {
                // After the ACK, synchronize with the server workers before
                // inspecting their memory from this test thread.
                ASSERT_TRUE(
                    HighPerformanceTcpTransportTestPeer::barrier(server).ok());
                EXPECT_EQ(remote, expected);
            }
            EXPECT_TRUE(std::all_of(local.begin(), local.begin() + kOffset,
                                    [](uint8_t b) { return b == 0xa5; }));
            EXPECT_TRUE(std::all_of(local.begin() + kOffset + length,
                                    local.end(),
                                    [](uint8_t b) { return b == 0xa5; }));
            ASSERT_TRUE(client.freeBatch(batch).ok());
            size_t connections = 0;
            for (auto& relay : relays) connections += relay->connections;
            EXPECT_EQ(connections,
                      opcode == Request::READ && length == 4096 ? 1U : 2U);
        }
    }
    if (multi_rail) {
        // A second peer sends one payload byte per slice, then stalls. The
        // original peer must still complete in this same engine and worker
        // pool.
        asio::io_context io;
        asio::ip::tcp::acceptor stalled(io, {asio::ip::tcp::v4(), 0});
        stalled.non_blocking(true);
        auto stalled_metadata = MakeLocalMetadata();
        uint16_t stalled_rpc_port = 0;
        ASSERT_TRUE(stalled_metadata->start(stalled_rpc_port).ok());
        const auto stalled_name =
            "127.0.0.1:" + std::to_string(stalled_rpc_port);
        auto stalled_endpoint = endpoint;
        for (auto& rail : stalled_endpoint.endpoints)
            rail.port = stalled.local_endpoint().port();
        ASSERT_TRUE(
            stalled_metadata->segmentManager()
                .updateLocal([&](SegmentDesc& segment) {
                    segment = *server_metadata->segmentManager().getLocal();
                    segment.name = segment.rpc_server_addr = stalled_name;
                    return EncodeHighPerformanceTcpEndpointAttr(
                        stalled_endpoint,
                        &std::get<MemorySegmentDesc>(segment.detail)
                             .transport_attrs[static_cast<int>(HP_TCP)]);
                })
                .ok());
        SegmentID stalled_target;
        ASSERT_TRUE(client.openSegment(stalled_target, stalled_name).ok());
        ASSERT_TRUE(
            client.registerLocalMemory(stalled_buffer.data(), kLength).ok());
        Request request{};
        request.opcode = Request::READ;
        request.source = stalled_buffer.data();
        request.target_id = stalled_target;
        request.target_offset = remote_desc.addr;
        request.length = (4ULL << 20) + 3;
        request.transport_hint = HP_TCP;
        auto slow = client.allocateBatch(1);
        ASSERT_TRUE(client.submitTransfer(slow, {request}).ok());
        std::array<asio::ip::tcp::socket, 2> sockets{asio::ip::tcp::socket(io),
                                                     asio::ip::tcp::socket(io)};
        for (auto& socket : sockets) {
            ASSERT_TRUE(WaitUntil([&] {
                std::error_code error;
                if (!socket.is_open()) stalled.accept(socket, error);
                return socket.is_open();
            }));
            std::array<uint8_t, kHighPerformanceTcpRequestSize> header{};
            asio::read(socket, asio::buffer(header));
            HighPerformanceTcpRequestFrame frame;
            ASSERT_TRUE(DecodeHighPerformanceTcpRequest(header.data(),
                                                        header.size(), &frame)
                            .ok());
            const auto response = EncodeHighPerformanceTcpResponse(
                {HighPerformanceTcpStatus::kOk, frame.request_id,
                 frame.length});
            asio::write(socket, asio::buffer(response));
            asio::write(socket, asio::buffer(remote.data(), 1));
        }
        request.source = local.data();
        request.target_id = target;
        request.length = 4096;
        auto healthy = client.allocateBatch(1);
        ASSERT_TRUE(client.submitTransfer(healthy, {request}).ok());
        TransferStatus status{PENDING, 0};
        ASSERT_TRUE(WaitUntil([&] {
            for (auto& relay : relays) relay->poll();
            EXPECT_TRUE(client.getTransferStatus(healthy, status).ok());
            return status.s != PENDING;
        }));
        EXPECT_EQ(status.s, COMPLETED);
        EXPECT_EQ(std::memcmp(local.data(), expected.data(), 4096), 0);
        ASSERT_TRUE(client.freeBatch(healthy).ok());
        ASSERT_TRUE(client.getTransferStatus(slow, status).ok());
        EXPECT_EQ(status.s, PENDING);
        ASSERT_TRUE(WaitUntil(
            [&] {
                EXPECT_TRUE(client.getTransferStatus(slow, status).ok());
                return status.s != PENDING;
            },
            std::chrono::seconds(6)));
        EXPECT_EQ(status.s, TIMEOUT);
        ASSERT_TRUE(client.freeBatch(slow).ok());
        ASSERT_TRUE(
            client.unregisterLocalMemory(stalled_buffer.data(), kLength).ok());
    }
    ASSERT_TRUE(client.unregisterLocalMemory(local.data(), local.size()).ok());
    ASSERT_TRUE(server.removeMemoryBuffer(remote_desc).ok());
    ASSERT_TRUE(server.quiesce().ok());
    ASSERT_TRUE(server.uninstall().ok());
}

TEST(HighPerformanceTcpTransportTest, RailPayloadsAndConnectionReuse) {
    CheckRailPayloads(true);
}

TEST(HighPerformanceTcpTransportTest, SingleRailPayloadsAndConnectionReuse) {
    CheckRailPayloads(false);
}

TEST(HighPerformanceTcpTransportTest,
     SlicedStaleRegistrationCancelsAndRetries) {
    constexpr size_t kLength = 4ULL << 20;
    std::vector<uint8_t> remote_storage(kLength, 0x5a);
    std::vector<uint8_t> local_storage(kLength);
    auto params = MakeParams();
    params.bind_address.clear();
    params.rail_addresses = {"127.0.0.1", "127.0.0.2"};
    params.max_outstanding_bytes = 8ULL << 20;
    params.max_transfer_bytes = 8ULL << 20;
    params.worker_count = 1;
    params.progress_timeout_ms = 5000;
    asio::io_context peer_io;
    asio::ip::tcp::acceptor peer(peer_io,
                                 {asio::ip::make_address("127.0.0.1"), 0});
    peer.non_blocking(true);

    auto server_metadata = MakeLocalMetadata();
    uint16_t rpc_port = 0;
    ASSERT_TRUE(server_metadata->start(rpc_port).ok());
    ASSERT_NE(rpc_port, 0);
    const std::string server_name = "127.0.0.1:" + std::to_string(rpc_port);
    ASSERT_TRUE(server_metadata->segmentManager()
                    .updateLocal([&](SegmentDesc& segment) -> Status {
                        segment.name = server_name;
                        segment.rpc_server_addr = server_name;
                        return Status::OK();
                    })
                    .ok());

    HighPerformanceTcpTransport server(params);
    std::string installed_server_name = server_name;
    ASSERT_TRUE(
        server.install(installed_server_name, server_metadata, nullptr, nullptr)
            .ok());
    BufferDesc registration_a;
    registration_a.addr = reinterpret_cast<uint64_t>(remote_storage.data());
    registration_a.length = remote_storage.size();
    registration_a.location = "cpu:0";
    MemoryOptions remote_options;
    remote_options.type = HP_TCP;
    remote_options.perm = kGlobalReadWrite;
    ASSERT_TRUE(server.addMemoryBuffer(registration_a, remote_options).ok());
    HighPerformanceTcpBufferAttr attr_a;
    ASSERT_TRUE(
        DecodeHighPerformanceTcpBufferAttr(
            registration_a.transport_attrs.at(TransportType::HP_TCP), &attr_a)
            .ok());
    ASSERT_TRUE(PublishBuffers(server_metadata, {registration_a}).ok());

    auto client_metadata = MakeLocalMetadata();
    ASSERT_TRUE(client_metadata->segmentManager()
                    .updateLocal([](SegmentDesc& segment) -> Status {
                        // Deliberately omit a callback address. The remote
                        // cache stays on A until this test invalidates it.
                        segment.rpc_server_addr.clear();
                        return Status::OK();
                    })
                    .ok());
    HighPerformanceTcpTransport client(params);
    std::string client_name = "hp_transport_client";
    ASSERT_TRUE(
        client.install(client_name, client_metadata, nullptr, nullptr).ok());
    SegmentID target = 0;
    ASSERT_TRUE(
        client_metadata->segmentManager().openRemote(target, server_name).ok());
    SegmentDescRef cached_a;
    ASSERT_TRUE(client_metadata->segmentManager()
                    .getRemoteCached(cached_a, target)
                    .ok());
    const BufferDesc* cached_buffer_a =
        cached_a->findBuffer(registration_a.addr, registration_a.length);
    ASSERT_NE(cached_buffer_a, nullptr);
    auto& cached_memory = std::get<MemorySegmentDesc>(cached_a->detail);
    auto& encoded_endpoint = cached_memory.transport_attrs.at(
        static_cast<int>(TransportType::HP_TCP));
    HighPerformanceTcpEndpointAttr mixed_endpoint;
    ASSERT_TRUE(
        DecodeHighPerformanceTcpEndpointAttr(encoded_endpoint, &mixed_endpoint)
            .ok());
    ASSERT_EQ(mixed_endpoint.endpoints.size(), 2U);
    for (auto& endpoint : mixed_endpoint.endpoints) {
        endpoint = {"127.0.0.1", peer.local_endpoint().port()};
    }
    ASSERT_TRUE(
        EncodeHighPerformanceTcpEndpointAttr(mixed_endpoint, &encoded_endpoint)
            .ok());

    ASSERT_TRUE(server.removeMemoryBuffer(registration_a).ok());
    BufferDesc registration_b;
    registration_b.addr = reinterpret_cast<uint64_t>(remote_storage.data());
    registration_b.length = remote_storage.size();
    registration_b.location = "cpu:0";
    ASSERT_TRUE(server.addMemoryBuffer(registration_b, remote_options).ok());
    HighPerformanceTcpBufferAttr attr_b;
    ASSERT_TRUE(
        DecodeHighPerformanceTcpBufferAttr(
            registration_b.transport_attrs.at(TransportType::HP_TCP), &attr_b)
            .ok());
    ASSERT_NE(attr_a.registration_id, attr_b.registration_id);
    ASSERT_TRUE(PublishBuffers(server_metadata, {registration_b}).ok());

    BufferDesc local;
    local.addr = reinterpret_cast<uint64_t>(local_storage.data());
    local.length = local_storage.size();
    local.location = "cpu:0";
    MemoryOptions local_options;
    local_options.type = HP_TCP;
    local_options.perm = kLocalReadWrite;
    ASSERT_TRUE(client.addMemoryBuffer(local, local_options).ok());

    Transport::SubBatchRef batch = nullptr;
    ASSERT_TRUE(client.allocateSubBatch(batch, 1).ok());
    Request request{};
    request.opcode = Request::READ;
    request.source = local_storage.data();
    request.target_id = target;
    request.target_offset = registration_b.addr;
    request.length = registration_b.length;
    request.transport_hint = HP_TCP;

    const Status submitted = client.submitTransferTasks(batch, {request});
    ASSERT_TRUE(submitted.ok()) << submitted.ToString();
    std::error_code error;
    const auto accept = [&](asio::ip::tcp::socket& socket) {
        return WaitUntil([&] {
            if (socket.is_open()) return true;
            peer.accept(socket, error);
            return !error;
        });
    };
    asio::ip::tcp::socket stale_socket(peer_io);
    asio::ip::tcp::socket stalled_socket(peer_io);
    ASSERT_TRUE(accept(stale_socket));
    ASSERT_TRUE(accept(stalled_socket));

    std::array<uint8_t, kHighPerformanceTcpRequestSize> stale_request{};
    asio::read(stale_socket, asio::buffer(stale_request), error);
    ASSERT_FALSE(error);
    std::array<uint8_t, kHighPerformanceTcpRequestSize> stalled_request{};
    asio::read(stalled_socket, asio::buffer(stalled_request), error);
    ASSERT_FALSE(error);

    HighPerformanceTcpRequestFrame frame;
    ASSERT_TRUE(DecodeHighPerformanceTcpRequest(stale_request.data(),
                                                stale_request.size(), &frame)
                    .ok());
    const auto response = EncodeHighPerformanceTcpResponse(
        {HighPerformanceTcpStatus::kStaleRegistration, frame.request_id, 0});
    asio::write(stale_socket, asio::buffer(response), error);
    ASSERT_FALSE(error);

    TransferStatus transfer_status{PENDING, 0};
    Status first_result =
        WaitForTransportResult(client, batch, transfer_status);
    if (transfer_status.s == PENDING) {
        (void)client.quiesce();
    }
    uint8_t unexpected = 0;
    (void)stalled_socket.read_some(asio::buffer(&unexpected, 1), error);
    EXPECT_TRUE(error);
    ASSERT_EQ(transfer_status.s, FAILED);
    EXPECT_TRUE(first_result.IsNeedsRefreshCache()) << first_result.ToString();

    ASSERT_TRUE(
        client_metadata->segmentManager().invalidateRemote(target).ok());
    ASSERT_TRUE(client.retryTransferTask(batch, 0, request).ok());

    Status retry_result =
        WaitForTransportResult(client, batch, transfer_status);
    EXPECT_TRUE(retry_result.ok()) << retry_result.ToString();
    EXPECT_EQ(transfer_status.s, COMPLETED);
    EXPECT_EQ(transfer_status.transferred_bytes, request.length);

    SegmentDescRef refreshed;
    ASSERT_TRUE(client_metadata->segmentManager()
                    .getRemoteCached(refreshed, target)
                    .ok());
    const BufferDesc* refreshed_buffer =
        refreshed->findBuffer(registration_b.addr, registration_b.length);
    ASSERT_NE(refreshed_buffer, nullptr);
    HighPerformanceTcpBufferAttr refreshed_attr;
    ASSERT_TRUE(DecodeHighPerformanceTcpBufferAttr(
                    refreshed_buffer->transport_attrs.at(TransportType::HP_TCP),
                    &refreshed_attr)
                    .ok());
    EXPECT_EQ(refreshed_attr.registration_id, attr_b.registration_id);

    ASSERT_TRUE(client.freeSubBatch(batch).ok());
    ASSERT_TRUE(client.removeMemoryBuffer(local).ok());
    ASSERT_TRUE(server.removeMemoryBuffer(registration_b).ok());
    ASSERT_TRUE(client.quiesce().ok());
    ASSERT_TRUE(server.quiesce().ok());
    ASSERT_TRUE(client.uninstall().ok());
    ASSERT_TRUE(server.uninstall().ok());
}

}  // namespace
}  // namespace mooncake::tent
