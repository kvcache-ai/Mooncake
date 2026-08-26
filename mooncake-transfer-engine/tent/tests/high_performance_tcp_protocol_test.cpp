// Copyright 2026 KVCache.AI
#include <gtest/gtest.h>
#include <array>
#include <chrono>
#include <thread>
#include "tent/transport/tcp/high_performance_tcp_buffer_registry.h"
#include "tent/transport/tcp/high_performance_tcp_protocol.h"
namespace mooncake::tent {
TEST(HighPerformanceTcpProtocolTest, UsesNetworkEndianFixedFrames) {
    HighPerformanceTcpRequestFrame in{HighPerformanceTcpOpcode::kWrite,
                                      0x0102030405060708ULL, 9, 10, 11};
    auto bytes = EncodeHighPerformanceTcpRequest(in);
    EXPECT_EQ(bytes[0], 0x4d);
    EXPECT_EQ(bytes[1], 0x43);
    EXPECT_EQ(bytes[8], 1);
    EXPECT_EQ(bytes[15], 8);
    HighPerformanceTcpRequestFrame out;
    ASSERT_TRUE(
        DecodeHighPerformanceTcpRequest(bytes.data(), bytes.size(), &out).ok());
    EXPECT_EQ(out.request_id, in.request_id);
    EXPECT_EQ(out.opcode, in.opcode);
    bytes[7] = 1;
    EXPECT_TRUE(
        DecodeHighPerformanceTcpRequest(bytes.data(), bytes.size(), &out)
            .IsInvalidArgument());
}
TEST(HighPerformanceTcpRegistryTest, EnforcesPermissionsAndUnregisterLease) {
    HighPerformanceTcpBufferRegistry registry;
    std::array<char, 32> data{};
    uint64_t id = 0;
    ASSERT_TRUE(registry
                    .add(reinterpret_cast<uint64_t>(data.data()), data.size(),
                         kGlobalReadOnly, &id)
                    .ok());
    HighPerformanceTcpBufferRegistry::Lease lease;
    EXPECT_TRUE(registry
                    .acquireRemoteLease(reinterpret_cast<uint64_t>(data.data()),
                                        4, id, HighPerformanceTcpOpcode::kRead,
                                        &lease)
                    .ok());
    HighPerformanceTcpBufferRegistry::Lease denied;
    EXPECT_TRUE(registry
                    .acquireRemoteLease(reinterpret_cast<uint64_t>(data.data()),
                                        4, id, HighPerformanceTcpOpcode::kWrite,
                                        &denied)
                    .IsAddressNotRegistered());
    std::atomic<bool> removed{false};
    std::thread remover([&] {
        EXPECT_TRUE(
            registry
                .remove(reinterpret_cast<uint64_t>(data.data()), data.size())
                .ok());
        removed = true;
    });
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    EXPECT_FALSE(removed);
    lease = HighPerformanceTcpBufferRegistry::Lease{};
    remover.join();
    EXPECT_TRUE(removed);
}
}  // namespace mooncake::tent
