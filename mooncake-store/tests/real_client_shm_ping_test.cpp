// Test ping health from SHM registration state without a transfer engine.
// Missing registrations report DISCONNECTION so Dummy can re-register.

#include <gtest/gtest.h>

#include <chrono>
#include <future>
#include <memory>
#include <thread>

#include "real_client_test_helper.h"

namespace mooncake {

namespace {

void AddClient(RealClient& rc, const UUID& id) {
    SharedMutexLocker lock(&RealClientTestHelper::ShmMutex(rc));
    RealClientTestHelper::ShmContexts(rc)[id] =
        std::make_shared<RealClientTestHelper::ShmContext>();
}

void RemoveClient(RealClient& rc, const UUID& id) {
    SharedMutexLocker lock(&RealClientTestHelper::ShmMutex(rc));
    RealClientTestHelper::ShmContexts(rc).erase(id);
}

}  // namespace

// A client absent from the map (never registered, or dropped) gets
// DISCONNECTION; a mapped client gets HEALTH.
TEST(RealClientShmPingTest, PingStatusFollowsMapPresence) {
    RealClient rc;
    UUID id{1, 2};

    EXPECT_EQ(rc.ping(id).value().status, DummyClientStatus::DISCONNECTION);

    AddClient(rc, id);
    EXPECT_EQ(rc.ping(id).value().status, DummyClientStatus::HEALTH);

    // Erase the context directly to simulate a missing map entry.
    RemoveClient(rc, id);
    EXPECT_EQ(rc.ping(id).value().status, DummyClientStatus::DISCONNECTION);

    // Reinsert an empty context to restore the map entry.
    AddClient(rc, id);
    EXPECT_EQ(rc.ping(id).value().status, DummyClientStatus::HEALTH);
}

// ping() reports the number of segments RealClient has mapped for the client,
// so the dummy can detect a partial loss (present but under-mapped) and
// re-register. mapped_shm_count is 0 when absent and tracks
// mapped_shms_.size().
TEST(RealClientShmPingTest, PingReportsMappedShmCount) {
    RealClient rc;
    UUID id{5, 6};

    // Absent -> DISCONNECTION, count 0.
    auto absent = rc.ping(id);
    EXPECT_EQ(absent.value().status, DummyClientStatus::DISCONNECTION);
    EXPECT_EQ(absent.value().mapped_shm_count, 0u);

    // Present but no segments mapped yet -> HEALTH, count 0.
    AddClient(rc, id);
    auto empty = rc.ping(id);
    EXPECT_EQ(empty.value().status, DummyClientStatus::HEALTH);
    EXPECT_EQ(empty.value().mapped_shm_count, 0u);

    // Two mapped segments -> count 2 (buffers left null; ping only reads size,
    // and rc's teardown skips unmapping when client_service_ is unset).
    {
        SharedMutexLocker lock(&RealClientTestHelper::ShmMutex(rc));
        auto& ctx = RealClientTestHelper::ShmContexts(rc)[id];
        SharedMutexLocker ctx_lock(&ctx->mutex_);
        ctx->mapped_shms_.push_back(RealClientTestHelper::MappedShm{});
        ctx->mapped_shms_.push_back(RealClientTestHelper::MappedShm{});
    }
    auto mapped = rc.ping(id);
    EXPECT_EQ(mapped.value().status, DummyClientStatus::HEALTH);
    EXPECT_EQ(mapped.value().mapped_shm_count, 2u);
}

// Mapped and unmapped clients are reported independently.
TEST(RealClientShmPingTest, PingStatusIsPerClient) {
    RealClient rc;
    UUID mapped{1, 2};
    UUID absent{3, 4};

    AddClient(rc, mapped);
    EXPECT_EQ(rc.ping(mapped).value().status, DummyClientStatus::HEALTH);
    EXPECT_EQ(rc.ping(absent).value().status, DummyClientStatus::DISCONNECTION);
}

// ping() must finish while another reader holds the shared map lock.
// Release the reader before checking or collecting the async result,
// including when the wait times out.
TEST(RealClientShmPingTest, PingNotBlockedByConcurrentReader) {
    RealClient rc;
    UUID id{7, 8};

    // Keep the future outside the lock scope so unwinding unlocks first.
    std::future<tl::expected<DummyHeartbeatResponse, ErrorCode>> pending;
    std::future_status status;
    {
        SharedMutexLocker reader(&RealClientTestHelper::ShmMutex(rc),
                                 shared_lock);
        pending = std::async(std::launch::async, [&] { return rc.ping(id); });
        status = pending.wait_for(std::chrono::seconds(5));
    }

    EXPECT_EQ(status, std::future_status::ready);
    auto result = pending.get();
    EXPECT_TRUE(result.has_value());
}

// Concurrent map/ping smoke test; TSan checks races on the executed paths.
// Each thread owns its UUID, so its expected ping status is deterministic.
TEST(RealClientShmPingTest, PingConcurrentWithMapMutation) {
    RealClient rc;
    constexpr int kThreads = 8;
    constexpr int kIters = 2000;

    std::vector<std::thread> threads;
    for (int t = 0; t < kThreads; ++t) {
        threads.emplace_back([&rc, t] {
            UUID id{static_cast<uint64_t>(t), 0};
            for (int i = 0; i < kIters; ++i) {
                if (i & 1) {
                    AddClient(rc, id);
                } else {
                    RemoveClient(rc, id);
                }
                auto r = rc.ping(id);
                ASSERT_TRUE(r.has_value());
                EXPECT_EQ(r.value().status,
                          (i & 1) ? DummyClientStatus::HEALTH
                                  : DummyClientStatus::DISCONNECTION);
            }
        });
    }
    for (auto& th : threads) th.join();
    SUCCEED();
}

}  // namespace mooncake
