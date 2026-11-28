#include <gtest/gtest.h>

#include <atomic>
#include <memory>
#include <thread>
#include <utility>
#include <vector>

#include "allocator.h"
#include "client_liveness.h"
#include "replica.h"
#include "types.h"

namespace mooncake {
namespace {

ClientSessionSharedPtr MakeActiveSession(const UUID& client_id) {
    return std::make_shared<const ClientLivenessRecord>(
        client_id, ClientLivenessRecord::Clock::now());
}

Replica MakeMemoryReplica(const std::shared_ptr<OffsetBufferAllocator>& alloc,
                          size_t size = 256) {
    auto buf = alloc->allocate(size);
    EXPECT_NE(buf, nullptr);
    return Replica(std::move(buf), ReplicaStatus::COMPLETE);
}

TEST(ReplicaPinTest, LifecycleMovesOutlivesReplicaAndConcurrentStress) {
    const UUID client_id = generate_uuid();
    auto session = MakeActiveSession(client_id);
    auto alloc = std::make_shared<OffsetBufferAllocator>(
        "seg_pin", 0x100000, 64 * 1024 * 1024, "tcp://127.0.0.1:9100", "tcp");

    // Multiple pins, move semantics on both ReplicaPin and Replica.
    {
        Replica r1 = MakeMemoryReplica(alloc, 256);
        EXPECT_EQ(r1.get_refcnt(), 0u);
        EXPECT_FALSE(r1.is_busy());

        ReplicaPin p1 = r1.Pin();
        ReplicaPin p2 = r1.Pin();
        EXPECT_TRUE(p1.Active());
        EXPECT_EQ(p1.replica_id(), r1.id());
        EXPECT_EQ(r1.get_refcnt(), 2u);
        EXPECT_TRUE(r1.is_busy());

        // Move ReplicaPin: count must remain 2.
        ReplicaPin p1_moved(std::move(p1));
        EXPECT_FALSE(p1.Active());
        EXPECT_TRUE(p1_moved.Active());
        EXPECT_EQ(r1.get_refcnt(), 2u);

        // Move-assign over an active pin releases the overwritten pin.
        p2 = std::move(p1_moved);
        EXPECT_FALSE(p1_moved.Active());
        EXPECT_EQ(r1.get_refcnt(), 1u);

        // Move Replica while pinned: pin stays attached to moved-to Replica.
        Replica r2(std::move(r1));
        EXPECT_EQ(r1.get_refcnt(), 0u);
        EXPECT_EQ(r2.get_refcnt(), 1u);

        Replica r3 = MakeMemoryReplica(alloc, 256);
        r3 = std::move(r2);
        EXPECT_EQ(r2.get_refcnt(), 0u);
        EXPECT_EQ(r3.get_refcnt(), 1u);

        p2.Reset();
        EXPECT_EQ(r3.get_refcnt(), 0u);
        EXPECT_FALSE(r3.is_busy());
        // Repeated Reset is a safe no-op.
        p2.Reset();
        EXPECT_EQ(r3.get_refcnt(), 0u);
    }

    // ReplicaPin safely outlives its owning Replica (e.g. object torn down
    // before an async task releases its handle).
    {
        ReplicaPin dangling_pin;
        {
            Replica disk(client_id, 128, "tcp://127.0.0.1:9100",
                         ReplicaStatus::COMPLETE, session);
            dangling_pin = disk.Pin();
            EXPECT_EQ(disk.get_refcnt(), 1u);
        }
        EXPECT_TRUE(dangling_pin.Active());
        dangling_pin.Reset();
        EXPECT_FALSE(dangling_pin.Active());
    }

    // Concurrent pin/unpin from multiple threads converges cleanly to 0.
    {
        Replica shared = MakeMemoryReplica(alloc, 256);
        constexpr int kThreads = 8;
        constexpr int kIters = 1000;
        std::atomic<bool> go{false};
        std::vector<std::thread> threads;
        threads.reserve(kThreads);
        for (int t = 0; t < kThreads; ++t) {
            threads.emplace_back([&]() {
                while (!go.load(std::memory_order_acquire)) {
                }
                for (int i = 0; i < kIters; ++i) {
                    ReplicaPin pin = shared.Pin();
                    EXPECT_GE(shared.get_refcnt(), 1u);
                    if (i % 3 == 0) {
                        pin.Reset();
                    }
                }
            });
        }
        go.store(true, std::memory_order_release);
        for (auto& th : threads) {
            th.join();
        }
        EXPECT_EQ(shared.get_refcnt(), 0u);
        EXPECT_FALSE(shared.is_busy());
    }
}

}  // namespace
}  // namespace mooncake
