#include "master_service.h"
#include "master_service/master_service_test_peer.h"

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
    MasterServiceTestPeer(*service).SetNoFProbeFnForTesting(
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
    EXPECT_TRUE(MasterServiceTestPeer(*service).IsNoFSegmentMountedForTesting(
        segment.id));
    EXPECT_EQ(
        MasterServiceTestPeer(*service).GetMountedNoFSegmentCountForTesting(),
        1u);
    auto failure_count =
        MasterServiceTestPeer(*service).GetNoFHeartbeatFailureCountForTesting(
            segment.id);
    ASSERT_TRUE(failure_count.has_value());
    EXPECT_EQ(*failure_count, 0u);
}

TEST_F(NoFHeartbeatTest, NewlyMountedNoFSegmentHasInitialGracePeriod) {
    auto service = CreateService(/*heartbeat_interval_sec=*/2,
                                 /*probe_timeout_ms=*/50,
                                 /*failure_threshold=*/1);
    std::atomic<int> probe_calls{0};
    MasterServiceTestPeer(*service).SetNoFProbeFnForTesting(
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
    EXPECT_TRUE(MasterServiceTestPeer(*service).IsNoFSegmentMountedForTesting(
        segment.id));
    auto failure_count =
        MasterServiceTestPeer(*service).GetNoFHeartbeatFailureCountForTesting(
            segment.id);
    ASSERT_TRUE(failure_count.has_value());
    EXPECT_EQ(*failure_count, 0u);
}

TEST_F(NoFHeartbeatTest, FailedNoFSegmentUnmountsAfterThreshold) {
    auto service = CreateService(/*heartbeat_interval_sec=*/1,
                                 /*probe_timeout_ms=*/50,
                                 /*failure_threshold=*/3);
    std::atomic<int> probe_calls{0};
    MasterServiceTestPeer(*service).SetNoFProbeFnForTesting(
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
        std::chrono::milliseconds(5000), std::chrono::milliseconds(50), [&]() {
            return !MasterServiceTestPeer(*service)
                        .IsNoFSegmentMountedForTesting(segment.id);
        }));
    EXPECT_GE(probe_calls.load(), 3);
    EXPECT_EQ(
        MasterServiceTestPeer(*service).GetMountedNoFSegmentCountForTesting(),
        0u);
    EXPECT_FALSE(MasterServiceTestPeer(*service)
                     .GetNoFHeartbeatFailureCountForTesting(segment.id)
                     .has_value());
}

TEST_F(NoFHeartbeatTest, FailureCountResetsAfterRecovery) {
    auto service = CreateService(/*heartbeat_interval_sec=*/1,
                                 /*probe_timeout_ms=*/50,
                                 /*failure_threshold=*/3);
    std::atomic<int> probe_calls{0};
    MasterServiceTestPeer(*service).SetNoFProbeFnForTesting(
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
                MasterServiceTestPeer(*service)
                    .GetNoFHeartbeatFailureCountForTesting(segment.id);
            return probe_calls.load() >= 4 && failure_count.has_value() &&
                   *failure_count == 0;
        }));
    EXPECT_TRUE(MasterServiceTestPeer(*service).IsNoFSegmentMountedForTesting(
        segment.id));
}

TEST_F(NoFHeartbeatTest, OnlyFailedSegmentIsUnmounted) {
    auto service = CreateService(/*heartbeat_interval_sec=*/1,
                                 /*probe_timeout_ms=*/50,
                                 /*failure_threshold=*/2);
    std::atomic<int> good_probe_calls{0};
    std::atomic<int> bad_probe_calls{0};
    MasterServiceTestPeer(*service).SetNoFProbeFnForTesting(
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
            return MasterServiceTestPeer(*service)
                       .GetMountedNoFSegmentCountForTesting() == 1u;
        }));
    EXPECT_TRUE(MasterServiceTestPeer(*service).IsNoFSegmentMountedForTesting(
        good_segment.id));
    EXPECT_FALSE(MasterServiceTestPeer(*service).IsNoFSegmentMountedForTesting(
        bad_segment.id));
    EXPECT_GE(good_probe_calls.load(), 1);
    EXPECT_GE(bad_probe_calls.load(), 2);
}

TEST_F(NoFHeartbeatTest, ClientExpiryDoesNotUnmountNoFSegment) {
    auto service = CreateService(/*heartbeat_interval_sec=*/5,
                                 /*probe_timeout_ms=*/50,
                                 /*failure_threshold=*/1,
                                 /*client_ttl_sec=*/1);
    MasterServiceTestPeer(*service).SetNoFProbeFnForTesting(
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

    EXPECT_TRUE(MasterServiceTestPeer(*service).IsNoFSegmentMountedForTesting(
        segment.id));
    EXPECT_EQ(
        MasterServiceTestPeer(*service).GetMountedNoFSegmentCountForTesting(),
        1u);
    auto failure_count =
        MasterServiceTestPeer(*service).GetNoFHeartbeatFailureCountForTesting(
            segment.id);
    ASSERT_TRUE(failure_count.has_value());
    EXPECT_EQ(*failure_count, 0u);
}

// ---------------------------------------------------------------------------
// Storage device inventory: the master reports every mounted NoF segment as a
// cold-tier storage device, with health derived from heartbeat probe
// accounting. These tests pin the mapping between probe outcomes and the
// health state operators see through the admin API.
// ---------------------------------------------------------------------------

TEST_F(NoFHeartbeatTest, ListStorageDevicesReportsHealthyNoFSegment) {
    auto service = CreateService(/*heartbeat_interval_sec=*/1,
                                 /*probe_timeout_ms=*/50,
                                 /*failure_threshold=*/3);
    std::atomic<int> probe_calls{0};
    MasterServiceTestPeer(*service).SetNoFProbeFnForTesting(
        [&probe_calls](const std::string&, uint32_t, std::string*) {
            probe_calls.fetch_add(1, std::memory_order_relaxed);
            return true;
        });

    UUID client_id = generate_uuid();
    NoFSegment segment = MakeNoFSegment("nof_seg_device", "nof_device");
    ASSERT_TRUE(service->MountNoFSegment(segment, client_id).has_value());

    ASSERT_TRUE(WaitForCondition(std::chrono::milliseconds(2500),
                                 std::chrono::milliseconds(50),
                                 [&]() { return probe_calls.load() >= 1; }));

    // The inventory is eventually consistent with the probe thread, so wait for
    // the first successful probe to be reflected rather than asserting once.
    std::vector<StorageDeviceMetadata> devices;
    ASSERT_TRUE(WaitForCondition(
        std::chrono::milliseconds(2500), std::chrono::milliseconds(50), [&]() {
            auto result = service->ListStorageDevices();
            if (!result.has_value() || result->size() != 1) {
                return false;
            }
            devices = *result;
            return devices[0].health == StorageDeviceHealth::HEALTHY &&
                   devices[0].last_success_unix_ms > 0;
        }));

    ASSERT_EQ(devices.size(), 1u);
    const auto& device = devices[0];
    EXPECT_EQ(device.device_id, segment.id);
    EXPECT_EQ(device.name, "nof_seg_device");
    EXPECT_EQ(device.endpoint, "nof_device");
    EXPECT_EQ(device.owner_client_id, client_id);
    EXPECT_EQ(device.health, StorageDeviceHealth::HEALTHY);
    EXPECT_TRUE(device.schedulable);
    EXPECT_EQ(device.consecutive_failures, 0u);
    EXPECT_TRUE(device.last_error.empty());
    EXPECT_GT(device.capacity_bytes, 0);
    EXPECT_GE(device.used_bytes, 0);
}

TEST_F(NoFHeartbeatTest, StorageDevicesReportDegradedBeforeHeartbeatUnmount) {
    // Health thresholds are derived from failure_threshold, so 6 yields
    // degraded_failures=3 and failed_failures=6. With a 1s probe interval the
    // device is reported DEGRADED after roughly 3s while the alive-timeout
    // unmount only fires at roughly 6s. That window is the point of the state
    // machine: today a flapping disk is invisible until it disappears.
    auto service = CreateService(/*heartbeat_interval_sec=*/1,
                                 /*probe_timeout_ms=*/50,
                                 /*failure_threshold=*/6);
    MasterServiceTestPeer(*service).SetNoFProbeFnForTesting(
        [](const std::string&, uint32_t, std::string* reason) {
            if (reason) {
                *reason = "completion_timeout";
            }
            return false;
        });

    UUID client_id = generate_uuid();
    NoFSegment segment = MakeNoFSegment("nof_seg_sick", "nof_sick");
    ASSERT_TRUE(service->MountNoFSegment(segment, client_id).has_value());

    auto health_of = [&](StorageDeviceHealth* out, uint32_t* failures,
                         bool* schedulable) -> bool {
        auto result = service->ListStorageDevices();
        if (!result.has_value() || result->size() != 1) {
            std::cout << "DBG size="
                      << (result.has_value() ? (int)result->size() : -1)
                      << std::endl;
            return false;
        }
        *out = (*result)[0].health;
        *failures = (*result)[0].consecutive_failures;
        *schedulable = (*result)[0].schedulable;
        std::cout << "DBG health=" << StorageDeviceHealthToString(*out)
                  << " failures=" << *failures << std::endl;
        return true;
    };

    StorageDeviceHealth health = StorageDeviceHealth::UNKNOWN;
    uint32_t failures = 0;
    bool schedulable = false;
    ASSERT_TRUE(WaitForCondition(
        std::chrono::seconds(10), std::chrono::milliseconds(50), [&]() {
            return health_of(&health, &failures, &schedulable) &&
                   health == StorageDeviceHealth::DEGRADED;
        }));
    EXPECT_GE(failures, 3u);
    // DEGRADED is a warning rather than a verdict: the device keeps serving and
    // stays eligible for allocation until the master actually gives up on it.
    EXPECT_TRUE(schedulable);
    EXPECT_TRUE(MasterServiceTestPeer(*service).IsNoFSegmentMountedForTesting(
        segment.id));

    // Reporting health must not weaken the existing terminal behaviour: the
    // alive timeout still unmounts a device that never recovers.
    EXPECT_TRUE(WaitForCondition(
        std::chrono::seconds(20), std::chrono::milliseconds(100), [&]() {
            return !MasterServiceTestPeer(*service)
                        .IsNoFSegmentMountedForTesting(segment.id);
        }));
}

TEST_F(NoFHeartbeatTest, MaintenancePlanFlagsFailingDeviceForRecovery) {
    auto service = CreateService(/*heartbeat_interval_sec=*/1,
                                 /*probe_timeout_ms=*/50,
                                 /*failure_threshold=*/6);
    MasterServiceTestPeer(*service).SetNoFProbeFnForTesting(
        [](const std::string&, uint32_t, std::string* reason) {
            if (reason) {
                *reason = "submit_fail";
            }
            return false;
        });

    UUID client_id = generate_uuid();
    NoFSegment segment = MakeNoFSegment("nof_seg_plan", "nof_plan");
    ASSERT_TRUE(service->MountNoFSegment(segment, client_id).has_value());

    StorageDeviceMaintenancePlan plan;
    ASSERT_TRUE(WaitForCondition(
        std::chrono::seconds(10), std::chrono::milliseconds(50), [&]() {
            auto result = service->GetStorageDeviceMaintenancePlan();
            if (!result.has_value()) return false;
            plan = *result;
            return !plan.recovery_candidates.empty();
        }));

    ASSERT_EQ(plan.recovery_candidates.size(), 1u);
    EXPECT_EQ(plan.recovery_candidates[0].device_id, segment.id);
    EXPECT_EQ(plan.recovery_candidates[0].name, "nof_seg_plan");
    EXPECT_EQ(plan.recovery_candidates[0].reason, "probe_degraded");
    // A freshly mounted segment is nowhere near the GC high watermark.
    EXPECT_TRUE(plan.gc_candidates.empty());
}

TEST_F(NoFHeartbeatTest, RequestStorageDeviceProbeTriggersImmediateProbe) {
    // A long interval means the scheduled probe would not run during the test,
    // so any probe observed after the request came from the request itself.
    auto service = CreateService(/*heartbeat_interval_sec=*/600,
                                 /*probe_timeout_ms=*/50,
                                 /*failure_threshold=*/3);
    std::atomic<int> probe_calls{0};
    MasterServiceTestPeer(*service).SetNoFProbeFnForTesting(
        [&probe_calls](const std::string&, uint32_t, std::string*) {
            probe_calls.fetch_add(1, std::memory_order_relaxed);
            return true;
        });

    UUID client_id = generate_uuid();
    NoFSegment segment = MakeNoFSegment("nof_seg_probe", "nof_probe");
    ASSERT_TRUE(service->MountNoFSegment(segment, client_id).has_value());

    // Wait until the heartbeat thread has registered the device.
    ASSERT_TRUE(WaitForCondition(
        std::chrono::seconds(5), std::chrono::milliseconds(50), [&]() {
            auto result = service->ListStorageDevices();
            return result.has_value() && result->size() == 1;
        }));
    EXPECT_EQ(probe_calls.load(), 0);

    ASSERT_TRUE(service->RequestStorageDeviceProbe(segment.id).has_value());
    EXPECT_TRUE(WaitForCondition(std::chrono::seconds(5),
                                 std::chrono::milliseconds(50),
                                 [&]() { return probe_calls.load() >= 1; }));

    // Unknown devices are rejected rather than silently accepted.
    const auto missing = service->RequestStorageDeviceProbe(generate_uuid());
    ASSERT_FALSE(missing.has_value());
    EXPECT_EQ(missing.error(), ErrorCode::SEGMENT_NOT_FOUND);
}

}  // namespace mooncake::test
