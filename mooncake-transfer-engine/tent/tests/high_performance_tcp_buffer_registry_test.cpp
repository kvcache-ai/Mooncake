// Copyright 2026 KVCache.AI
#include <gtest/gtest.h>

#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <limits>
#include <thread>

#include "tent/transport/tcp/high_performance_tcp_buffer_registry.h"

namespace mooncake::tent {
namespace {
using namespace std::chrono_literals;

TEST(HighPerformanceTcpBufferRegistryTest, LocalLeaseIgnoresRemotePermission) {
    HighPerformanceTcpBufferRegistry registry;
    std::array<uint8_t, 64> data{};
    uint64_t registration = 0;
    ASSERT_TRUE(registry
                    .add(reinterpret_cast<uint64_t>(data.data()), data.size(),
                         kLocalReadWrite, &registration)
                    .ok());
    EXPECT_NE(registration, 0u);

    HighPerformanceTcpBufferRegistry::Lease local;
    EXPECT_TRUE(registry
                    .acquireLocalLease(
                        reinterpret_cast<uint64_t>(data.data()) + 8, 16, &local)
                    .ok());
    EXPECT_TRUE(local);

    HighPerformanceTcpBufferRegistry::Lease remote;
    HighPerformanceTcpBufferRegistry::AcquireFailure failure;
    EXPECT_FALSE(
        registry
            .acquireRemoteLease(reinterpret_cast<uint64_t>(data.data()), 8,
                                registration, HighPerformanceTcpOpcode::kRead,
                                &remote, &failure)
            .ok());
    EXPECT_EQ(
        failure,
        HighPerformanceTcpBufferRegistry::AcquireFailure::kPermissionDenied);
}

TEST(HighPerformanceTcpBufferRegistryTest,
     EnforcesReadOnlyAndStaleRegistration) {
    HighPerformanceTcpBufferRegistry registry;
    std::array<uint8_t, 64> data{};
    uint64_t registration = 0;
    ASSERT_TRUE(registry
                    .add(reinterpret_cast<uint64_t>(data.data()), data.size(),
                         kGlobalReadOnly, &registration)
                    .ok());

    HighPerformanceTcpBufferRegistry::Lease lease;
    HighPerformanceTcpBufferRegistry::AcquireFailure failure;
    EXPECT_TRUE(registry
                    .acquireRemoteLease(reinterpret_cast<uint64_t>(data.data()),
                                        data.size(), registration,
                                        HighPerformanceTcpOpcode::kRead, &lease,
                                        &failure)
                    .ok());
    lease.reset();

    EXPECT_FALSE(registry
                     .acquireRemoteLease(
                         reinterpret_cast<uint64_t>(data.data()), data.size(),
                         registration, HighPerformanceTcpOpcode::kWrite, &lease,
                         &failure)
                     .ok());
    EXPECT_EQ(
        failure,
        HighPerformanceTcpBufferRegistry::AcquireFailure::kPermissionDenied);

    EXPECT_FALSE(registry
                     .acquireRemoteLease(
                         reinterpret_cast<uint64_t>(data.data()), data.size(),
                         registration + 1, HighPerformanceTcpOpcode::kRead,
                         &lease, &failure)
                     .ok());
    EXPECT_EQ(
        failure,
        HighPerformanceTcpBufferRegistry::AcquireFailure::kStaleRegistration);
}

TEST(HighPerformanceTcpBufferRegistryTest, RejectsOverlapAndOverflowRanges) {
    HighPerformanceTcpBufferRegistry registry;
    uint64_t id = 0;
    ASSERT_TRUE(registry.add(0x1000, 0x100, kGlobalReadWrite, &id).ok());
    EXPECT_FALSE(registry.add(0x1080, 0x100, kGlobalReadWrite, nullptr).ok());
    EXPECT_FALSE(registry.add(0x0f80, 0x100, kGlobalReadWrite, nullptr).ok());
    EXPECT_TRUE(registry.add(0x1100, 0x100, kGlobalReadWrite, nullptr).ok());
    EXPECT_FALSE(registry
                     .add(std::numeric_limits<uint64_t>::max() - 7, 16,
                          kGlobalReadWrite, nullptr)
                     .ok());

    HighPerformanceTcpBufferRegistry::Lease lease;
    EXPECT_FALSE(registry.acquireLocalLease(0x10f0, 0x20, &lease).ok());
}

TEST(HighPerformanceTcpBufferRegistryTest, UnregisterHidesThenWaitsForLease) {
    HighPerformanceTcpBufferRegistry registry;
    std::array<uint8_t, 64> data{};
    const uint64_t base = reinterpret_cast<uint64_t>(data.data());
    uint64_t id = 0;
    ASSERT_TRUE(registry.add(base, data.size(), kGlobalReadWrite, &id).ok());

    HighPerformanceTcpBufferRegistry::Lease lease;
    ASSERT_TRUE(registry.acquireLocalLease(base, data.size(), &lease).ok());
    std::atomic<bool> started{false};
    std::atomic<bool> done{false};
    std::thread remover([&] {
        started.store(true, std::memory_order_release);
        EXPECT_TRUE(registry.remove(base, data.size()).ok());
        done.store(true, std::memory_order_release);
    });
    while (!started.load(std::memory_order_acquire)) std::this_thread::yield();
    for (int i = 0; i < 100 && registry.tracks(base, data.size()); ++i) {
        std::this_thread::sleep_for(1ms);
    }
    EXPECT_FALSE(registry.tracks(base, data.size()));
    EXPECT_FALSE(done.load(std::memory_order_acquire));

    HighPerformanceTcpBufferRegistry::Lease late;
    EXPECT_FALSE(registry.acquireLocalLease(base, 1, &late).ok());
    lease.reset();
    remover.join();
    EXPECT_TRUE(done.load(std::memory_order_acquire));
}

TEST(HighPerformanceTcpBufferRegistryTest,
     CloseRejectsNewWorkAndReopenRequiresEmptyRegistry) {
    HighPerformanceTcpBufferRegistry registry;
    std::array<uint8_t, 32> data{};
    const uint64_t base = reinterpret_cast<uint64_t>(data.data());
    uint64_t id = 0;
    ASSERT_TRUE(registry.add(base, data.size(), kGlobalReadWrite, &id).ok());
    registry.close();
    EXPECT_TRUE(registry.closing());
    EXPECT_FALSE(registry.add(base + 128, 32, kGlobalReadWrite, nullptr).ok());

    HighPerformanceTcpBufferRegistry::Lease lease;
    HighPerformanceTcpBufferRegistry::AcquireFailure failure;
    EXPECT_FALSE(registry
                     .acquireRemoteLease(base, 1, id,
                                         HighPerformanceTcpOpcode::kRead,
                                         &lease, &failure)
                     .ok());
    EXPECT_EQ(failure,
              HighPerformanceTcpBufferRegistry::AcquireFailure::kShuttingDown);
    EXPECT_FALSE(registry.reopen().ok());
    EXPECT_TRUE(registry.remove(base, data.size()).ok());
    EXPECT_TRUE(registry.reopen().ok());
    EXPECT_FALSE(registry.closing());
}

}  // namespace
}  // namespace mooncake::tent
