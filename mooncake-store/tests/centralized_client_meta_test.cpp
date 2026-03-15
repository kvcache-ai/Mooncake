#include <gtest/gtest.h>
#include <thread>
#include <atomic>
#include <random>
#include "centralized_client_meta.h"

namespace mooncake {

// Helper class to expose protected members for testing
class TestableClientMeta : public CentralizedClientMeta {
   public:
    using CentralizedClientMeta::CentralizedClientMeta;

    void SetHealthStatus(ClientStatus status) {
        SharedMutexLocker lock(&client_mutex_);
        health_state_.status = status;
    }

    ClientStatus GetHealthStatus() const {
        SharedMutexLocker lock(&client_mutex_, shared_lock);
        return health_state_.status;
    }
};

class ClientMetaTest : public ::testing::Test {
   protected:
    void SetUp() override {
        // Initialize logging
        google::InitGoogleLogging("ClientMetaTest");
        FLAGS_logtostderr = 1;
    }

    void TearDown() override { google::ShutdownGoogleLogging(); }

    std::shared_ptr<TestableClientMeta> CreateClientMeta() {
        UUID client_id = {12345, 67890};
        return std::make_shared<TestableClientMeta>(
            client_id, BufferAllocatorType::OFFSET);
    }

    Segment MakeSegment() {
        Segment segment;
        segment.id = {1, 1};
        segment.name = "test_segment";
        segment.size = 1024 * 1024;
        segment.extra =
            CentralizedSegmentExtraData{.base = 0x100000000, .te_endpoint = ""};
        return segment;
    }
};

TEST_F(ClientMetaTest, MountSegmentCheckHealth) {
    auto meta = CreateClientMeta();
    auto segment = MakeSegment();

    // Case 1: Health (Default) -> OK
    ASSERT_EQ(meta->GetHealthStatus(), ClientStatus::HEALTH);
    auto res = meta->MountSegment(segment);
    EXPECT_TRUE(res.has_value());

    // Case 2: DISCONNECTION -> Fail
    meta->SetHealthStatus(ClientStatus::DISCONNECTION);
    segment.id = {2, 2};  // New segment
    res = meta->MountSegment(segment);
    EXPECT_FALSE(res.has_value());
    EXPECT_EQ(res.error(), ErrorCode::CLIENT_UNHEALTHY);

    // Case 3: CRASHED -> Fail
    meta->SetHealthStatus(ClientStatus::CRASHED);
    segment.id = {3, 3};
    res = meta->MountSegment(segment);
    EXPECT_FALSE(res.has_value());
    EXPECT_EQ(res.error(), ErrorCode::CLIENT_UNHEALTHY);
}

TEST_F(ClientMetaTest, UnmountSegmentCheckHealth) {
    auto meta = CreateClientMeta();
    auto segment = MakeSegment();

    // Mount first (while HEALTH)
    ASSERT_TRUE(meta->MountSegment(segment).has_value());

    // Case 1: Health -> OK
    auto res = meta->UnmountSegment(segment.id);
    EXPECT_TRUE(res.has_value());

    // Remount for next test
    ASSERT_TRUE(meta->MountSegment(segment).has_value());

    // Case 2: DISCONNECTION -> Fail
    meta->SetHealthStatus(ClientStatus::DISCONNECTION);
    res = meta->UnmountSegment(segment.id);
    EXPECT_FALSE(res.has_value());
    EXPECT_EQ(res.error(), ErrorCode::CLIENT_UNHEALTHY);
}

TEST_F(ClientMetaTest, QueryIpCheckHealth) {
    auto meta = CreateClientMeta();

    // Case 1: Health -> OK
    auto res = meta->QueryIp({12345, 67890});
    EXPECT_TRUE(res.has_value());

    // Case 2: CRASHED -> Fail
    meta->SetHealthStatus(ClientStatus::CRASHED);
    res = meta->QueryIp({12345, 67890});
    EXPECT_FALSE(res.has_value());
    EXPECT_EQ(res.error(), ErrorCode::CLIENT_UNHEALTHY);
}

TEST_F(ClientMetaTest, MountLocalDiskSegmentCheckHealth) {
    auto meta = CreateClientMeta();

    // Case 1: Health -> OK
    auto res = meta->MountLocalDiskSegment(true);
    EXPECT_TRUE(res.has_value());

    // Case 2: DISCONNECTION -> Fail
    meta->SetHealthStatus(ClientStatus::DISCONNECTION);
    res = meta->MountLocalDiskSegment(true);
    EXPECT_FALSE(res.has_value());
    EXPECT_EQ(res.error(), ErrorCode::CLIENT_UNHEALTHY);
}

TEST_F(ClientMetaTest, PushOffloadingQueueCheckHealth) {
    auto meta = CreateClientMeta();

    // Mount local disk segment first
    ASSERT_TRUE(meta->MountLocalDiskSegment(true).has_value());

    // Mount a segment for checking existence
    auto segment = MakeSegment();
    ASSERT_TRUE(meta->MountSegment(segment).has_value());

    // Case 1: Health -> OK
    auto res = meta->PushOffloadingQueue("key1", 100, segment.name);
    EXPECT_TRUE(res.has_value());

    // Case 2: CRASHED -> Fail (CLIENT_UNHEALTHY)
    meta->SetHealthStatus(ClientStatus::CRASHED);
    res = meta->PushOffloadingQueue("key1", 100, "seg1");
    EXPECT_FALSE(res.has_value());
    EXPECT_EQ(res.error(), ErrorCode::CLIENT_UNHEALTHY);
}

TEST_F(ClientMetaTest, OffloadObjectHeartbeatFull) {
    auto meta = CreateClientMeta();

    // Case 1: No local disk segment -> Should return error (or empty if handled
    // by wrapper) The implementation returns SEGMENT_NOT_FOUND, check wrapper
    // behavior
    auto res = meta->OffloadObjectHeartbeat(true);
    EXPECT_FALSE(res.has_value());

    // Case 2: With local disk but no segments mounted
    ASSERT_TRUE(meta->MountLocalDiskSegment(true).has_value());
    res = meta->OffloadObjectHeartbeat(true);
    ASSERT_TRUE(res.has_value());
    EXPECT_TRUE(res->empty());

    // Case 3: Push data and verify heartbeat
    auto segment = MakeSegment();
    ASSERT_TRUE(meta->MountSegment(segment).has_value());

    std::string key1 = "obj_1";
    int64_t size1 = 1024;
    std::string key2 = "obj_2";
    int64_t size2 = 2048;

    ASSERT_TRUE(
        meta->PushOffloadingQueue(key1, size1, segment.name).has_value());
    ASSERT_TRUE(
        meta->PushOffloadingQueue(key2, size2, segment.name).has_value());

    res = meta->OffloadObjectHeartbeat(true);
    ASSERT_TRUE(res.has_value());
    EXPECT_EQ(res->size(), 2);
    EXPECT_EQ((*res)[key1], size1);
    EXPECT_EQ((*res)[key2], size2);

    // After heartbeat, internal map is moved, next heartbeat should be empty
    res = meta->OffloadObjectHeartbeat(true);
    ASSERT_TRUE(res.has_value());
    EXPECT_TRUE(res->empty());
}

// Concurrency Test
TEST_F(ClientMetaTest, ConcurrentHealthChangeAndMount) {
    auto meta = CreateClientMeta();
    std::atomic<bool> stop{false};
    std::atomic<int> success_count{0};
    std::atomic<int> fail_unhealthy_count{0};
    std::atomic<int> op_count{0};

    // Thread A: Toggles Health Status
    std::thread state_thread([&]() {
        std::mt19937 rng(std::random_device{}());
        std::uniform_int_distribution<int> dist(
            0, 2);  // 0: HEALTH, 1: DISC, 2: CRASHED
        while (!stop) {
            int state = dist(rng);
            ClientStatus s = ClientStatus::HEALTH;
            if (state == 1) {
                s = ClientStatus::DISCONNECTION;
            } else if (state == 2) {
                s = ClientStatus::CRASHED;
            }

            meta->SetHealthStatus(s);
        }
    });

    // Thread B: Attempts to Mount Segments
    std::thread mount_thread([&]() {
        int i = 0;
        while (!stop) {
            i++;
            Segment segment;
            segment.id = {100, (uint64_t)i};
            segment.name = "seg_" + std::to_string(i);
            segment.size = 1024 * 1024;
            segment.extra = CentralizedSegmentExtraData{.base = 0x200000000,
                                                        .te_endpoint = ""};

            auto res = meta->MountSegment(segment);
            if (res.has_value()) {
                success_count.fetch_add(1);
            } else if (res.error() == ErrorCode::CLIENT_UNHEALTHY) {
                fail_unhealthy_count.fetch_add(1);
            } else {
                LOG(ERROR) << "MountSegment failed with error: " << res.error();
            }
            op_count.fetch_add(1);
        }
    });

    // Run for 1 second
    std::this_thread::sleep_for(std::chrono::seconds(1));
    stop = true;
    state_thread.join();
    mount_thread.join();

    EXPECT_EQ(success_count + fail_unhealthy_count, op_count);
}

TEST_F(ClientMetaTest, GetSegments) {
    auto meta = CreateClientMeta();
    auto s1 = MakeSegment();
    auto s2 = MakeSegment();
    s2.id = {2, 2};
    s2.name = "seg2";
    ASSERT_TRUE(meta->MountSegment(s1).has_value());
    ASSERT_TRUE(meta->MountSegment(s2).has_value());
    auto res = meta->GetSegments();
    ASSERT_TRUE(res.has_value());
    EXPECT_EQ(res->size(), 2);

    bool found_s1 = false;
    bool found_s2 = false;
    for (const auto& s : *res) {
        if (s.id == s1.id) {
            EXPECT_EQ(s.name, s1.name);
            EXPECT_EQ(s.size, s1.size);
            found_s1 = true;
        } else if (s.id == s2.id) {
            EXPECT_EQ(s.name, s2.name);
            EXPECT_EQ(s.size, s2.size);
            found_s2 = true;
        }
    }
    EXPECT_TRUE(found_s1);
    EXPECT_TRUE(found_s2);
}

TEST_F(ClientMetaTest, QuerySegmentsByName) {
    auto meta = CreateClientMeta();
    auto s1 = MakeSegment();
    s1.name = "test_seg";
    ASSERT_TRUE(meta->MountSegment(s1).has_value());
    auto res = meta->QuerySegments("test_seg");
    ASSERT_TRUE(res.has_value());

    // QuerySegments returns pair<size, capacity>
    EXPECT_EQ(res->first, 0);
    EXPECT_EQ(res->second, s1.size);
}

TEST_F(ClientMetaTest, QuerySegmentById) {
    auto meta = CreateClientMeta();
    auto s1 = MakeSegment();
    ASSERT_TRUE(meta->MountSegment(s1).has_value());
    auto res = meta->QuerySegment(s1.id);
    ASSERT_TRUE(res.has_value());
    EXPECT_EQ((*res)->id, s1.id);
    EXPECT_EQ((*res)->name, s1.name);
    EXPECT_EQ((*res)->size, s1.size);
}

TEST_F(ClientMetaTest, SegmentRemovalCallback) {
    auto meta = CreateClientMeta();
    auto s1 = MakeSegment();
    ASSERT_TRUE(meta->MountSegment(s1).has_value());
    bool called = false;
    meta->SetSegmentRemovalCallback([&](const UUID& id) {
        if (id == s1.id) called = true;
    });
    ASSERT_TRUE(meta->UnmountSegment(s1.id).has_value());
    EXPECT_TRUE(called);
}

TEST_F(ClientMetaTest, HealthStateMachine) {
    auto meta = CreateClientMeta();
    constexpr int64_t kDisconnectTimeoutSec = 1;
    constexpr int64_t kCrashTimeoutSec = 2;
    ClientMeta::SetTimeouts(kDisconnectTimeoutSec, kCrashTimeoutSec);
    EXPECT_TRUE(meta->is_health());

    // Wait for disconnect: more than kDisconnectTimeoutSec
    std::this_thread::sleep_for(
        std::chrono::milliseconds(kDisconnectTimeoutSec * 1000 + 100));
    auto res = meta->CheckHealth();
    EXPECT_EQ(res.first, ClientStatus::HEALTH);
    EXPECT_EQ(res.second, ClientStatus::DISCONNECTION);
    EXPECT_FALSE(meta->is_health());

    // Recover by heartbeat
    res = meta->Heartbeat();
    EXPECT_EQ(res.first, ClientStatus::DISCONNECTION);
    EXPECT_EQ(res.second, ClientStatus::HEALTH);
    EXPECT_TRUE(meta->is_health());

    // Wait for crash: more than kCrashTimeoutSec
    std::this_thread::sleep_for(
        std::chrono::milliseconds(kCrashTimeoutSec * 1000 + 100));
    res = meta->CheckHealth();
    EXPECT_EQ(res.second, ClientStatus::CRASHED);
    EXPECT_FALSE(meta->is_health());

    // Once CRASHED, heartbeat cannot recover
    res = meta->Heartbeat();
    EXPECT_EQ(res.second, ClientStatus::CRASHED);
}

}  // namespace mooncake
