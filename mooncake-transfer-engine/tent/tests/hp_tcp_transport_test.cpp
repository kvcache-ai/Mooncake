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
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include "tent/runtime/control_plane.h"
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

    static uint64_t connectionsCreated(
        const HighPerformanceTcpTransport& transport) {
        return transport.client_->connectionsCreatedForTest();
    }
};

namespace {

template <class Predicate>
bool WaitUntil(Predicate predicate) {
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (!predicate() && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::yield();
    }
    return predicate();
}

std::shared_ptr<ControlService> MakeLocalMetadata() {
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

TEST(HighPerformanceTcpTransportTest, SlicesSingleLargeReadAcrossTwoRails) {
    constexpr size_t kLength = 4ULL << 20;
    auto params = MakeParams();
    params.bind_address.clear();
    params.rail_addresses = {"127.0.0.1", "127.0.0.2"};
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
    std::vector<uint8_t> remote(kLength);
    for (size_t i = 0; i < remote.size(); ++i) {
        remote[i] = static_cast<uint8_t>((i * 17) & 0xff);
    }
    BufferDesc remote_desc;
    remote_desc.addr = reinterpret_cast<uint64_t>(remote.data());
    remote_desc.length = remote.size();
    remote_desc.location = "cpu:0";
    MemoryOptions remote_options;
    remote_options.type = HP_TCP;
    remote_options.perm = kGlobalReadWrite;
    ASSERT_TRUE(server.addMemoryBuffer(remote_desc, remote_options).ok());
    ASSERT_TRUE(PublishBuffers(server_metadata, {remote_desc}).ok());

    auto client_metadata = MakeLocalMetadata();
    HighPerformanceTcpTransport client(params);
    std::string client_name = "hp_transport_client";
    ASSERT_TRUE(
        client.install(client_name, client_metadata, nullptr, nullptr).ok());
    SegmentID target = 0;
    ASSERT_TRUE(
        client_metadata->segmentManager().openRemote(target, server_name).ok());

    std::vector<uint8_t> local(kLength, 0);
    BufferDesc local_desc;
    local_desc.addr = reinterpret_cast<uint64_t>(local.data());
    local_desc.length = local.size();
    local_desc.location = "cpu:0";
    MemoryOptions local_options;
    local_options.type = HP_TCP;
    local_options.perm = kLocalReadWrite;
    ASSERT_TRUE(client.addMemoryBuffer(local_desc, local_options).ok());

    Transport::SubBatchRef batch = nullptr;
    ASSERT_TRUE(client.allocateSubBatch(batch, 1).ok());
    Request request{};
    request.opcode = Request::READ;
    request.source = local.data();
    request.target_id = target;
    request.target_offset = remote_desc.addr;
    request.length = remote_desc.length;
    request.transport_hint = HP_TCP;
    ASSERT_TRUE(client.submitTransferTasks(batch, {request}).ok());

    TransferStatus transfer_status;
    const Status result =
        WaitForTransportResult(client, batch, transfer_status);
    EXPECT_TRUE(result.ok()) << result.ToString();
    EXPECT_EQ(transfer_status.s, COMPLETED);
    EXPECT_EQ(transfer_status.transferred_bytes, kLength);
    EXPECT_EQ(std::memcmp(local.data(), remote.data(), kLength), 0);
    EXPECT_EQ(HighPerformanceTcpTransportTestPeer::connectionsCreated(client),
              2);

    ASSERT_TRUE(client.freeSubBatch(batch).ok());
    ASSERT_TRUE(client.removeMemoryBuffer(local_desc).ok());
    ASSERT_TRUE(server.removeMemoryBuffer(remote_desc).ok());
    ASSERT_TRUE(client.quiesce().ok());
    ASSERT_TRUE(server.quiesce().ok());
    ASSERT_TRUE(client.uninstall().ok());
    ASSERT_TRUE(server.uninstall().ok());
}

TEST(HighPerformanceTcpTransportTest,
     StaleRegistrationRefreshRetriesSameTransportWithFreshMetadata) {
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

    HighPerformanceTcpTransport server(MakeParams());
    std::string installed_server_name = server_name;
    ASSERT_TRUE(
        server.install(installed_server_name, server_metadata, nullptr, nullptr)
            .ok());
    std::array<uint8_t, 64> remote_storage{};
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
    HighPerformanceTcpTransport client(MakeParams());
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

    std::array<uint8_t, 64> local_storage{};
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
    ASSERT_TRUE(client.submitTransferTasks(batch, {request}).ok());

    TransferStatus transfer_status;
    Status first_result =
        WaitForTransportResult(client, batch, transfer_status);
    EXPECT_EQ(transfer_status.s, FAILED);
    EXPECT_TRUE(first_result.IsNeedsRefreshCache()) << first_result.ToString();

    int metadata_refresh_retry_count = 0;
    ASSERT_LT(metadata_refresh_retry_count, 1);
    ++metadata_refresh_retry_count;
    ASSERT_TRUE(
        client_metadata->segmentManager().invalidateRemote(target).ok());
    ASSERT_TRUE(client.retryTransferTask(batch, 0, request).ok());

    Status retry_result =
        WaitForTransportResult(client, batch, transfer_status);
    EXPECT_TRUE(retry_result.ok()) << retry_result.ToString();
    EXPECT_EQ(transfer_status.s, COMPLETED);
    EXPECT_EQ(transfer_status.transferred_bytes, request.length);
    EXPECT_EQ(metadata_refresh_retry_count, 1);

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
