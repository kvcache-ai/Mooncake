// Copyright 2026 KVCache.AI
#include <gtest/gtest.h>

#include <array>
#include <atomic>
#include <thread>

#include "tent/transport/tcp/high_performance_tcp_buffer_registry.h"

namespace mooncake::tent {
namespace {

TEST(HighPerformanceTcpBufferRegistryTest, EnforcesPermissionAndRegistration) {
    HighPerformanceTcpBufferRegistry registry;
    std::array<uint8_t, 64> data{};
    const uint64_t base = reinterpret_cast<uint64_t>(data.data());
    uint64_t id = 0;
    ASSERT_TRUE(registry.add(base, data.size(), kGlobalReadOnly, &id).ok());

    HighPerformanceTcpBufferRegistry::Lease lease;
    HighPerformanceTcpStatus failure;
    ASSERT_TRUE(registry
                    .acquireRemoteLease(base, data.size(), id,
                                        HighPerformanceTcpOpcode::kRead, &lease,
                                        &failure)
                    .ok());
    lease.reset();
    EXPECT_FALSE(registry
                     .acquireRemoteLease(base, data.size(), id,
                                         HighPerformanceTcpOpcode::kWrite,
                                         &lease, &failure)
                     .ok());
    EXPECT_EQ(failure, HighPerformanceTcpStatus::kPermissionDenied);
    EXPECT_FALSE(registry
                     .acquireRemoteLease(base, data.size(), id + 1,
                                         HighPerformanceTcpOpcode::kRead,
                                         &lease, &failure)
                     .ok());
    EXPECT_EQ(failure, HighPerformanceTcpStatus::kStaleRegistration);
}

TEST(HighPerformanceTcpBufferRegistryTest,
     RejectsRegistrationFromPreviousRegistryIncarnation) {
    std::array<uint8_t, 64> data{};
    const uint64_t base = reinterpret_cast<uint64_t>(data.data());

    uint64_t stale_id = 0;
    {
        HighPerformanceTcpBufferRegistry previous;
        ASSERT_TRUE(
            previous.add(base, data.size(), kGlobalReadWrite, &stale_id).ok());
    }

    HighPerformanceTcpBufferRegistry current;
    uint64_t current_id = 0;
    ASSERT_TRUE(
        current.add(base, data.size(), kGlobalReadWrite, &current_id).ok());
    ASSERT_NE(stale_id, current_id);

    HighPerformanceTcpBufferRegistry::Lease lease;
    HighPerformanceTcpStatus failure;
    EXPECT_FALSE(current
                     .acquireRemoteLease(base, data.size(), stale_id,
                                         HighPerformanceTcpOpcode::kRead,
                                         &lease, &failure)
                     .ok());
    EXPECT_EQ(failure, HighPerformanceTcpStatus::kStaleRegistration);
    EXPECT_TRUE(current
                    .acquireRemoteLease(base, data.size(), current_id,
                                        HighPerformanceTcpOpcode::kRead, &lease,
                                        &failure)
                    .ok());
}

TEST(HighPerformanceTcpBufferRegistryTest, UnregisterWaitsForActiveLease) {
    HighPerformanceTcpBufferRegistry registry;
    std::array<uint8_t, 64> data{};
    const uint64_t base = reinterpret_cast<uint64_t>(data.data());
    uint64_t id = 0;
    ASSERT_TRUE(registry.add(base, data.size(), kGlobalReadWrite, &id).ok());
    HighPerformanceTcpBufferRegistry::Lease lease;
    ASSERT_TRUE(registry.acquireLocalLease(base, data.size(), &lease).ok());

    std::atomic<bool> done{false};
    std::thread remover([&] {
        EXPECT_TRUE(registry.remove(base, data.size()).ok());
        done = true;
    });
    while (registry.tracks(base, data.size())) std::this_thread::yield();
    EXPECT_FALSE(done.load());
    lease.reset();
    remover.join();
    EXPECT_TRUE(done.load());
}

TEST(HighPerformanceTcpBufferRegistryTest, CloseRejectsNewWork) {
    HighPerformanceTcpBufferRegistry registry;
    std::array<uint8_t, 8> data{};
    uint64_t id = 0;
    ASSERT_TRUE(registry
                    .add(reinterpret_cast<uint64_t>(data.data()), data.size(),
                         kGlobalReadWrite, &id)
                    .ok());
    registry.close();
    EXPECT_TRUE(
        registry.add(0x1000, 8, kGlobalReadWrite, nullptr).IsTooManyRequests());
    EXPECT_FALSE(registry.reopen().ok());
}

}  // namespace
}  // namespace mooncake::tent
