#include "master_service.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <memory>
#include <thread>

namespace mooncake::test {

namespace {

constexpr size_t kDefaultNoFSegmentBase = 0x500000000;
constexpr size_t kDefaultNoFSegmentSize = 1024 * 1024 * 16;

NoFSegment MakeNoFSegment(std::string name, std::string endpoint,
                          size_t base = kDefaultNoFSegmentBase,
                          size_t size = kDefaultNoFSegmentSize) {
    NoFSegment segment;
    segment.id = generate_uuid();
    segment.name = std::move(name);
    segment.base = base;
    segment.size = size;
    segment.te_endpoint = std::move(endpoint);
    return segment;
}

bool WaitForCondition(std::chrono::milliseconds timeout,
                      std::chrono::milliseconds interval,
                      const std::function<bool()>& condition) {
    auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        if (condition()) {
            return true;
        }
        std::this_thread::sleep_for(interval);
    }
    return condition();
}

}  // namespace

class NoFHeartbeatTest : public ::testing::Test {
   protected:
    void SetUp() override {
        google::InitGoogleLogging("NoFHeartbeatTest");
        FLAGS_logtostderr = true;
    }

    void TearDown() override { google::ShutdownGoogleLogging(); }

    std::unique_ptr<MasterService> CreateService(int64_t heartbeat_interval_sec,
                                                 uint32_t probe_timeout_ms,
                                                 uint32_t failure_threshold,
                                                 int64_t client_ttl_sec = 10) {
        auto config =
            MasterServiceConfig::builder()
                .set_memory_allocator(BufferAllocatorType::OFFSET)
                .set_client_live_ttl_sec(client_ttl_sec)
                .set_nof_heartbeat_interval_sec(heartbeat_interval_sec)
                .set_nof_heartbeat_probe_timeout_ms(probe_timeout_ms)
                .set_nof_heartbeat_failures_threshold(failure_threshold)
                .build();
        return std::make_unique<MasterService>(config);
    }
};

TEST_F(NoFHeartbeatTest, HealthyNoFSegmentDoesNotUnmount) {
    auto service = CreateService(/*heartbeat_interval_sec=*/1,
                                 /*probe_timeout_ms=*/50,
                                 /*failure_threshold=*/3);
    std::atomic<int> probe_calls{0};
    service->SetNoFProbeFnForTesting(
        [&probe_calls](const std::string&, uint32_t, std::string*) {
            probe_calls.fetch_add(1, std::memory_order_relaxed);
            return true;
        });

    UUID client_id = generate_uuid();
    NoFSegment segment = MakeNoFSegment("nof_seg_ok", "nof_ok");
    ASSERT_TRUE(service->MountNoFSegment(segment, client_id).has_value());

    ASSERT_TRUE(WaitForCondition(std::chrono::milliseconds(2500),
                                 std::chrono::milliseconds(50),
                                 [&]() { return probe_calls.load() >= 1; }));
    EXPECT_TRUE(service->IsNoFSegmentMountedForTesting(segment.id));
    EXPECT_EQ(service->GetMountedNoFSegmentCountForTesting(), 1u);
    auto failure_count =
        service->GetNoFHeartbeatFailureCountForTesting(segment.id);
    ASSERT_TRUE(failure_count.has_value());
    EXPECT_EQ(*failure_count, 0u);
}

TEST_F(NoFHeartbeatTest, NewlyMountedNoFSegmentHasInitialGracePeriod) {
    auto service = CreateService(/*heartbeat_interval_sec=*/2,
                                 /*probe_timeout_ms=*/50,
                                 /*failure_threshold=*/1);
    std::atomic<int> probe_calls{0};
    service->SetNoFProbeFnForTesting(
        [&probe_calls](const std::string&, uint32_t, std::string* reason) {
            probe_calls.fetch_add(1, std::memory_order_relaxed);
            if (reason) {
                *reason = "submit_fail";
            }
            return false;
        });

    UUID client_id = generate_uuid();
    NoFSegment segment = MakeNoFSegment("nof_seg_grace", "nof_grace");
    ASSERT_TRUE(service->MountNoFSegment(segment, client_id).has_value());

    std::this_thread::sleep_for(std::chrono::milliseconds(800));

    EXPECT_EQ(probe_calls.load(), 0);
    EXPECT_TRUE(service->IsNoFSegmentMountedForTesting(segment.id));
    auto failure_count =
        service->GetNoFHeartbeatFailureCountForTesting(segment.id);
    ASSERT_TRUE(failure_count.has_value());
    EXPECT_EQ(*failure_count, 0u);
}

TEST_F(NoFHeartbeatTest, FailedNoFSegmentUnmountsAfterThreshold) {
    auto service = CreateService(/*heartbeat_interval_sec=*/1,
                                 /*probe_timeout_ms=*/50,
                                 /*failure_threshold=*/3);
    std::atomic<int> probe_calls{0};
    service->SetNoFProbeFnForTesting(
        [&probe_calls](const std::string&, uint32_t, std::string* reason) {
            probe_calls.fetch_add(1, std::memory_order_relaxed);
            if (reason) {
                *reason = "submit_fail";
            }
            return false;
        });

    UUID client_id = generate_uuid();
    NoFSegment segment = MakeNoFSegment("nof_seg_fail", "nof_fail");
    ASSERT_TRUE(service->MountNoFSegment(segment, client_id).has_value());

    ASSERT_TRUE(WaitForCondition(
        std::chrono::milliseconds(5000), std::chrono::milliseconds(50),
        [&]() { return !service->IsNoFSegmentMountedForTesting(segment.id); }));
    EXPECT_GE(probe_calls.load(), 3);
    EXPECT_EQ(service->GetMountedNoFSegmentCountForTesting(), 0u);
    EXPECT_FALSE(
        service->GetNoFHeartbeatFailureCountForTesting(segment.id).has_value());
}

TEST_F(NoFHeartbeatTest, FailureCountResetsAfterRecovery) {
    auto service = CreateService(/*heartbeat_interval_sec=*/1,
                                 /*probe_timeout_ms=*/50,
                                 /*failure_threshold=*/3);
    std::atomic<int> probe_calls{0};
    service->SetNoFProbeFnForTesting(
        [&probe_calls](const std::string&, uint32_t, std::string* reason) {
            int current = probe_calls.fetch_add(1, std::memory_order_relaxed);
            if (current < 2) {
                if (reason) {
                    *reason = "submit_fail";
                }
                return false;
            }
            return true;
        });

    UUID client_id = generate_uuid();
    NoFSegment segment = MakeNoFSegment("nof_seg_recover", "nof_recover");
    ASSERT_TRUE(service->MountNoFSegment(segment, client_id).has_value());

    ASSERT_TRUE(WaitForCondition(
        std::chrono::milliseconds(5000), std::chrono::milliseconds(50), [&]() {
            auto failure_count =
                service->GetNoFHeartbeatFailureCountForTesting(segment.id);
            return probe_calls.load() >= 4 && failure_count.has_value() &&
                   *failure_count == 0;
        }));
    EXPECT_TRUE(service->IsNoFSegmentMountedForTesting(segment.id));
}

TEST_F(NoFHeartbeatTest, OnlyFailedSegmentIsUnmounted) {
    auto service = CreateService(/*heartbeat_interval_sec=*/1,
                                 /*probe_timeout_ms=*/50,
                                 /*failure_threshold=*/2);
    std::atomic<int> good_probe_calls{0};
    std::atomic<int> bad_probe_calls{0};
    service->SetNoFProbeFnForTesting(
        [&good_probe_calls, &bad_probe_calls](const std::string& endpoint,
                                              uint32_t, std::string* reason) {
            if (endpoint == "nof_good") {
                good_probe_calls.fetch_add(1, std::memory_order_relaxed);
                return true;
            }
            bad_probe_calls.fetch_add(1, std::memory_order_relaxed);
            if (reason) {
                *reason = "submit_fail";
            }
            return false;
        });

    UUID client_id = generate_uuid();
    NoFSegment good_segment = MakeNoFSegment("nof_seg_good", "nof_good");
    NoFSegment bad_segment =
        MakeNoFSegment("nof_seg_bad", "nof_bad",
                       kDefaultNoFSegmentBase + kDefaultNoFSegmentSize,
                       kDefaultNoFSegmentSize);
    ASSERT_TRUE(service->MountNoFSegment(good_segment, client_id).has_value());
    ASSERT_TRUE(service->MountNoFSegment(bad_segment, client_id).has_value());

    ASSERT_TRUE(WaitForCondition(
        std::chrono::milliseconds(5000), std::chrono::milliseconds(50), [&]() {
            return service->GetMountedNoFSegmentCountForTesting() == 1u;
        }));
    EXPECT_TRUE(service->IsNoFSegmentMountedForTesting(good_segment.id));
    EXPECT_FALSE(service->IsNoFSegmentMountedForTesting(bad_segment.id));
    EXPECT_GE(good_probe_calls.load(), 1);
    EXPECT_GE(bad_probe_calls.load(), 2);
}

TEST_F(NoFHeartbeatTest, ClientExpiryDoesNotUnmountNoFSegment) {
    auto service = CreateService(/*heartbeat_interval_sec=*/5,
                                 /*probe_timeout_ms=*/50,
                                 /*failure_threshold=*/1,
                                 /*client_ttl_sec=*/1);
    service->SetNoFProbeFnForTesting(
        [](const std::string&, uint32_t, std::string* reason) {
            if (reason) {
                *reason = "submit_fail";
            }
            return false;
        });

    UUID client_id = generate_uuid();
    NoFSegment segment =
        MakeNoFSegment("nof_seg_ignore_client_ttl", "nof_ignore_client_ttl");
    ASSERT_TRUE(service->MountNoFSegment(segment, client_id).has_value());

    std::this_thread::sleep_for(std::chrono::milliseconds(1800));

    EXPECT_TRUE(service->IsNoFSegmentMountedForTesting(segment.id));
    EXPECT_EQ(service->GetMountedNoFSegmentCountForTesting(), 1u);
    auto failure_count =
        service->GetNoFHeartbeatFailureCountForTesting(segment.id);
    ASSERT_TRUE(failure_count.has_value());
    EXPECT_EQ(*failure_count, 0u);
}

}  // namespace mooncake::test
