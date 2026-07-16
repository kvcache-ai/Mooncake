// Unit tests for the RealClient dummy-ping / DISCONNECTION signal.
//
// ping() reports HEALTH iff the client is present in shm_contexts_; a client
// whose segments were dropped (monitor expiry / real-client restart) is absent
// and gets DISCONNECTION, prompting the dummy to re-register. These tests drive
// that logic WITHOUT any RDMA/IPC/transfer-engine setup: RealClient's
// constructor is lightweight and ping() only touches the lock-free ping queue
// and shm_contexts_. The full round trip (monitor expiry -> unmap -> ping
// DISCONNECTION -> dummy re-register) needs a live transfer engine and is
// covered by the e2e harness.

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <memory>
#include <thread>

#include "real_client.h"

namespace mooncake {

namespace {

void AddClient(RealClient& rc, const UUID& id) {
    SharedMutexLocker lock(&rc.dummy_client_mutex_);
    rc.shm_contexts_[id] = std::make_shared<RealClient::ShmContext>();
}

void RemoveClient(RealClient& rc, const UUID& id) {
    SharedMutexLocker lock(&rc.dummy_client_mutex_);
    rc.shm_contexts_.erase(id);
}

}  // namespace

// A client absent from the map (never registered, or dropped) gets
// DISCONNECTION; a mapped client gets HEALTH.
TEST(RealClientRemountTest, PingStatusFollowsMapPresence) {
    RealClient rc;
    UUID id{1, 2};

    EXPECT_EQ(rc.ping(id).value().status, DummyClientStatus::DISCONNECTION);

    AddClient(rc, id);
    EXPECT_EQ(rc.ping(id).value().status, DummyClientStatus::HEALTH);

    // Simulate the monitor dropping the client's segments.
    RemoveClient(rc, id);
    EXPECT_EQ(rc.ping(id).value().status, DummyClientStatus::DISCONNECTION);

    // Re-registration makes it healthy again.
    AddClient(rc, id);
    EXPECT_EQ(rc.ping(id).value().status, DummyClientStatus::HEALTH);
}

// ping() reports the number of segments RealClient has mapped for the client,
// so the dummy can detect a partial loss (present but under-mapped) and
// re-register. mapped_shm_count is 0 when absent and tracks mapped_shms.size().
TEST(RealClientRemountTest, PingReportsMappedShmCount) {
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
        SharedMutexLocker lock(&rc.dummy_client_mutex_);
        auto& ctx = rc.shm_contexts_[id];
        SharedMutexLocker ctx_lock(&ctx->mutex);
        ctx->mapped_shms.push_back(RealClient::MappedShm{});
        ctx->mapped_shms.push_back(RealClient::MappedShm{});
    }
    auto mapped = rc.ping(id);
    EXPECT_EQ(mapped.value().status, DummyClientStatus::HEALTH);
    EXPECT_EQ(mapped.value().mapped_shm_count, 2u);
}

// Mapped and unmapped clients are reported independently.
TEST(RealClientRemountTest, PingStatusIsPerClient) {
    RealClient rc;
    UUID mapped{1, 2};
    UUID absent{3, 4};

    AddClient(rc, mapped);
    EXPECT_EQ(rc.ping(mapped).value().status, DummyClientStatus::HEALTH);
    EXPECT_EQ(rc.ping(absent).value().status, DummyClientStatus::DISCONNECTION);
}

// ping() must take dummy_client_mutex_ in SHARED mode, so a concurrent reader
// (another ping, or a put/get doing find_shm_context) never blocks a heartbeat.
// If ping mistakenly took the lock exclusively, this call would block behind
// the reader below and the test would time out.
TEST(RealClientRemountTest, PingNotBlockedByConcurrentReader) {
    RealClient rc;
    UUID id{7, 8};

    std::atomic<bool> locked{false};
    std::atomic<bool> release{false};
    std::thread reader([&] {
        SharedMutexLocker lock(&rc.dummy_client_mutex_, shared_lock);
        locked.store(true);
        while (!release.load()) {
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
        }
    });

    while (!locked.load()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }

    auto start = std::chrono::steady_clock::now();
    auto r = rc.ping(id);
    auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                          std::chrono::steady_clock::now() - start)
                          .count();

    EXPECT_TRUE(r.has_value());
    EXPECT_LT(elapsed_ms, 200);

    release.store(true);
    reader.join();
}

// Concurrent add/remove/ping across many threads must be race-free (run under
// TSan). shm_contexts_ is guarded by dummy_client_mutex_; ping reads it under a
// shared lock while add/remove take it exclusively.
TEST(RealClientRemountTest, PingConcurrentWithMapMutation) {
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
            }
        });
    }
    for (auto& th : threads) th.join();
    SUCCEED();
}

}  // namespace mooncake
