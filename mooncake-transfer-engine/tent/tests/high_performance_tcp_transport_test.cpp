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
#include <cstdint>
#include <memory>
#include <string>

#include "tent/runtime/control_plane.h"
#include "tent/transport/tcp/high_performance_tcp_protocol.h"
#include "tent/transport/tcp/high_performance_tcp_transport.h"

namespace mooncake::tent {
namespace {

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
    params.queue_capacity_per_worker = 8;
    params.connections_per_peer = 2;
    params.max_outstanding_tasks = 16;
    params.max_outstanding_bytes = 1 << 20;
    params.max_transfer_bytes = 1 << 20;
    params.chunk_size = 16 << 10;
    params.connect_timeout_ms = 1000;
    params.progress_timeout_ms = 1000;
    return params;
}

bool ContainsTransport(const BufferDesc& desc, TransportType type) {
    return std::find(desc.transports.begin(), desc.transports.end(), type) !=
           desc.transports.end();
}

TEST(HighPerformanceTcpTransportTest,
     PublishesEndpointAndSeparatesLocalOnlyCapabilities) {
    auto metadata = MakeLocalMetadata();
    HighPerformanceTcpTransport transport(MakeParams());
    std::string segment_name = "hp_transport_test";
    ASSERT_TRUE(
        transport.install(segment_name, metadata, nullptr, nullptr).ok());

    const SegmentDescRef local = metadata->segmentManager().getLocal();
    const auto attr_it = local->getMemory().transport_attrs.find(
        static_cast<int>(TransportType::TCP));
    ASSERT_NE(attr_it, local->getMemory().transport_attrs.end());
    HighPerformanceTcpEndpointAttr endpoint;
    ASSERT_TRUE(
        DecodeHighPerformanceTcpEndpointAttr(attr_it->second, &endpoint).ok());
    ASSERT_EQ(endpoint.endpoints.size(), 1U);
    EXPECT_EQ(endpoint.endpoints[0].host, "127.0.0.1");
    EXPECT_NE(endpoint.endpoints[0].port, 0);

    std::array<uint8_t, 64> local_only_storage{};
    BufferDesc local_only;
    local_only.addr = reinterpret_cast<uint64_t>(local_only_storage.data());
    local_only.length = local_only_storage.size();
    local_only.location = "cpu:0";
    MemoryOptions local_options;
    local_options.perm = kLocalReadWrite;
    ASSERT_TRUE(transport.addMemoryBuffer(local_only, local_options).ok());
    EXPECT_TRUE(transport.tracksLocalBuffer(local_only));
    EXPECT_FALSE(ContainsTransport(local_only, TransportType::TCP));
    EXPECT_EQ(local_only.transport_attrs.count(TransportType::TCP), 0U);

    std::array<uint8_t, 64> global_storage{};
    BufferDesc global;
    global.addr = reinterpret_cast<uint64_t>(global_storage.data());
    global.length = global_storage.size();
    global.location = "cpu:0";
    MemoryOptions global_options;
    global_options.perm = kGlobalReadWrite;
    ASSERT_TRUE(transport.addMemoryBuffer(global, global_options).ok());
    EXPECT_TRUE(ContainsTransport(global, TransportType::TCP));
    const auto buffer_attr = global.transport_attrs.find(TransportType::TCP);
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

TEST(HighPerformanceTcpTransportTest, CanReinstallAfterCompleteTeardown) {
    auto metadata = MakeLocalMetadata();
    HighPerformanceTcpTransport transport(MakeParams());
    std::string segment_name = "hp_transport_test";

    ASSERT_TRUE(
        transport.install(segment_name, metadata, nullptr, nullptr).ok());
    ASSERT_TRUE(transport.quiesce().ok());
    ASSERT_TRUE(transport.uninstall().ok());

    ASSERT_TRUE(
        transport.install(segment_name, metadata, nullptr, nullptr).ok());
    ASSERT_TRUE(transport.quiesce().ok());
    ASSERT_TRUE(transport.uninstall().ok());
}

TEST(HighPerformanceTcpTransportTest, ExplicitBindFailureIsReturned) {
    auto metadata = MakeLocalMetadata();
    auto params = MakeParams();
    params.bind_address = "203.0.113.1";  // TEST-NET-3, not a local interface.
    HighPerformanceTcpTransport transport(params);
    std::string segment_name = "hp_transport_test";

    const Status status =
        transport.install(segment_name, metadata, nullptr, nullptr);
    EXPECT_FALSE(status.ok());
    EXPECT_TRUE(transport.uninstall().ok());
}

}  // namespace
}  // namespace mooncake::tent
