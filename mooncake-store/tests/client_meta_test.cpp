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

// Concurrency Test
TEST_F(ClientMetaTest, ConcurrentHealthChangeAndMount) {
    auto meta = CreateClientMeta();
    std::atomic<bool> stop{false};
    std::atomic<int> success_count{0};
    std::atomic<int> fail_unhealthy_count{0};

    // Thread A: Toggles Health Status
    std::thread state_thread([&]() {
        std::mt19937 rng(std::random_device{}());
        std::uniform_int_distribution<int> dist(
            0, 2);  // 0: HEALTH, 1: DISC, 2: CRASHED
        while (!stop) {
            int state = dist(rng);
            ClientStatus s = ClientStatus::HEALTH;
            if (state == 1) s = ClientStatus::DISCONNECTION;
            if (state == 2) s = ClientStatus::CRASHED;

            meta->SetHealthStatus(s);
            std::this_thread::sleep_for(std::chrono::microseconds(10));
        }
    });

    // Thread B: Attempts to Mount Segments
    std::thread mount_thread([&]() {
        int i = 0;
        while (!stop) {
            Segment segment;
            segment.id = {100, (uint64_t)++i};
            segment.name = "seg_" + std::to_string(i);
            segment.size = 1024 * 1024;
            segment.extra = CentralizedSegmentExtraData{.base = 0x200000000,
                                                        .te_endpoint = ""};

            auto res = meta->MountSegment(segment);
            if (res.has_value()) {
                success_count++;
            } else if (res.error() == ErrorCode::CLIENT_UNHEALTHY) {
                fail_unhealthy_count++;
            } else {
                // Should not happen ideally, or other errors
            }
            std::this_thread::sleep_for(std::chrono::microseconds(10));
        }
    });

    // Run for 1 second
    std::this_thread::sleep_for(std::chrono::seconds(1));
    stop = true;
    state_thread.join();
    mount_thread.join();

    LOG(INFO) << "Concurrent Test Stats: Success=" << success_count
              << ", UnhealthyFail=" << fail_unhealthy_count;

    EXPECT_GT(success_count + fail_unhealthy_count, 0);
}

}  // namespace mooncake
