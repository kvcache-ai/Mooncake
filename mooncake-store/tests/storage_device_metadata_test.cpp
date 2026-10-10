#include "storage_device_metadata.h"

#include <gtest/gtest.h>

#include <vector>

namespace mooncake {
namespace {

using Health = StorageDeviceHealth;

StorageDeviceProbeState Probed(uint32_t consecutive_failures) {
    StorageDeviceProbeState state;
    state.ever_probed = true;
    state.consecutive_failures = consecutive_failures;
    return state;
}

const StorageDeviceHealthPolicy kPolicy{/*degraded_failures=*/2,
                                        /*failed_failures=*/4};

TEST(StorageDeviceHealthNameTest, RoundTripsEveryDeclaredState) {
    const std::vector<Health> all{Health::UNKNOWN, Health::HEALTHY,
                                  Health::DEGRADED, Health::FAILED,
                                  Health::UNMOUNTING};
    for (const auto health : all) {
        const auto name = StorageDeviceHealthToString(health);
        ASSERT_FALSE(name.empty());
        Health parsed = Health::UNKNOWN;
        ASSERT_TRUE(StorageDeviceHealthFromString(name, parsed)) << name;
        EXPECT_EQ(parsed, health);
    }
}

TEST(StorageDeviceHealthNameTest, ParsingIsCaseInsensitiveAndRejectsJunk) {
    Health parsed = Health::UNKNOWN;
    EXPECT_TRUE(StorageDeviceHealthFromString("degraded", parsed));
    EXPECT_EQ(parsed, Health::DEGRADED);
    EXPECT_TRUE(StorageDeviceHealthFromString("FAILED", parsed));
    EXPECT_EQ(parsed, Health::FAILED);
    EXPECT_FALSE(StorageDeviceHealthFromString("", parsed));
    EXPECT_FALSE(StorageDeviceHealthFromString("NOPE", parsed));
    EXPECT_FALSE(StorageDeviceHealthFromString("DISABLED", parsed));
}

TEST(DeriveStorageDeviceHealthTest, UnprobedDeviceIsUnknown) {
    StorageDeviceProbeState state;
    EXPECT_EQ(DeriveStorageDeviceHealth(state, kPolicy), Health::UNKNOWN);
}

TEST(DeriveStorageDeviceHealthTest, SuccessClearsBackToHealthy) {
    EXPECT_EQ(DeriveStorageDeviceHealth(Probed(0), kPolicy), Health::HEALTHY);
    // A single failure is below the degraded threshold of 2.
    EXPECT_EQ(DeriveStorageDeviceHealth(Probed(1), kPolicy), Health::HEALTHY);
}

TEST(DeriveStorageDeviceHealthTest, EscalatesDegradedThenFailed) {
    EXPECT_EQ(DeriveStorageDeviceHealth(Probed(2), kPolicy), Health::DEGRADED);
    EXPECT_EQ(DeriveStorageDeviceHealth(Probed(3), kPolicy), Health::DEGRADED);
    EXPECT_EQ(DeriveStorageDeviceHealth(Probed(4), kPolicy), Health::FAILED);
    EXPECT_EQ(DeriveStorageDeviceHealth(Probed(99), kPolicy), Health::FAILED);
}

TEST(DeriveStorageDeviceHealthTest, UnmountingWinsOverProbeAccounting) {
    StorageDeviceProbeState state = Probed(9);
    state.unmounting = true;
    EXPECT_EQ(DeriveStorageDeviceHealth(state, kPolicy), Health::UNMOUNTING);
}

TEST(DeriveStorageDeviceHealthTest, DegradedThresholdIsClampedToFailed) {
    // A misconfigured policy with degraded above failed must still reach
    // FAILED, and must never report DEGRADED past the failed threshold.
    const StorageDeviceHealthPolicy inverted{/*degraded_failures=*/9,
                                             /*failed_failures=*/3};
    EXPECT_EQ(DeriveStorageDeviceHealth(Probed(2), inverted), Health::HEALTHY);
    EXPECT_EQ(DeriveStorageDeviceHealth(Probed(3), inverted), Health::FAILED);
    EXPECT_EQ(DeriveStorageDeviceHealth(Probed(9), inverted), Health::FAILED);
}

TEST(DeriveStorageDeviceHealthTest, ZeroThresholdsAreTreatedAsOne) {
    const StorageDeviceHealthPolicy zero{/*degraded_failures=*/0,
                                         /*failed_failures=*/0};
    EXPECT_EQ(DeriveStorageDeviceHealth(Probed(0), zero), Health::HEALTHY);
    EXPECT_EQ(DeriveStorageDeviceHealth(Probed(1), zero), Health::FAILED);
}

StorageDeviceMetadata MakeDevice(const std::string& name, Health health,
                                 int64_t used, int64_t capacity) {
    StorageDeviceMetadata device;
    device.device_id = UUID{static_cast<uint64_t>(name.size()), 0};
    device.name = name;
    device.health = health;
    device.schedulable = health == Health::HEALTHY || health == Health::UNKNOWN;
    device.used_bytes = used;
    device.capacity_bytes = capacity;
    return device;
}

TEST(StorageDeviceMaintenanceTest, FlagsFailingDevicesForRecovery) {
    EXPECT_EQ(
        StorageDeviceRecoveryReason(MakeDevice("a", Health::FAILED, 0, 1)),
        "probe_failed");
    EXPECT_EQ(
        StorageDeviceRecoveryReason(MakeDevice("a", Health::DEGRADED, 0, 1)),
        "probe_degraded");
    EXPECT_FALSE(
        StorageDeviceRecoveryReason(MakeDevice("a", Health::HEALTHY, 0, 1))
            .has_value());
    EXPECT_FALSE(
        StorageDeviceRecoveryReason(MakeDevice("a", Health::UNKNOWN, 0, 1))
            .has_value());
    // Unmounting devices are handled by GC, not recovery.
    EXPECT_FALSE(
        StorageDeviceRecoveryReason(MakeDevice("a", Health::UNMOUNTING, 0, 1))
            .has_value());
}

TEST(StorageDeviceMaintenanceTest, GcUsesWatermarkAndLifecycle) {
    EXPECT_EQ(StorageDeviceGcReason(MakeDevice("a", Health::UNMOUNTING, 0, 100),
                                    0.95),
              "unmounting");
    EXPECT_EQ(
        StorageDeviceGcReason(MakeDevice("a", Health::HEALTHY, 95, 100), 0.95),
        "high_usage");
    EXPECT_FALSE(
        StorageDeviceGcReason(MakeDevice("a", Health::HEALTHY, 94, 100), 0.95)
            .has_value());
    // Unknown usage must not be reported as full.
    EXPECT_FALSE(
        StorageDeviceGcReason(MakeDevice("a", Health::HEALTHY, -1, 100), 0.95)
            .has_value());
    EXPECT_FALSE(
        StorageDeviceGcReason(MakeDevice("a", Health::HEALTHY, 100, 0), 0.95)
            .has_value());
    // An out-of-range watermark disables the usage rule but keeps lifecycle.
    EXPECT_FALSE(
        StorageDeviceGcReason(MakeDevice("a", Health::HEALTHY, 100, 100), 0.0)
            .has_value());
    EXPECT_EQ(
        StorageDeviceGcReason(MakeDevice("a", Health::UNMOUNTING, 0, 100), 0.0),
        "unmounting");
}

TEST(StorageDeviceMaintenanceTest, PlanSeparatesRecoveryFromGc) {
    const std::vector<StorageDeviceMetadata> devices{
        MakeDevice("healthy", Health::HEALTHY, 10, 100),
        MakeDevice("failed", Health::FAILED, 10, 100),
        MakeDevice("degraded", Health::DEGRADED, 10, 100),
        MakeDevice("leaving", Health::UNMOUNTING, 10, 100),
        MakeDevice("full", Health::HEALTHY, 99, 100),
    };

    const auto plan = BuildStorageDeviceMaintenancePlan(devices, 0.95);

    ASSERT_EQ(plan.recovery_candidates.size(), 2u);
    EXPECT_EQ(plan.recovery_candidates[0].name, "failed");
    EXPECT_EQ(plan.recovery_candidates[0].reason, "probe_failed");
    EXPECT_EQ(plan.recovery_candidates[1].name, "degraded");
    EXPECT_EQ(plan.recovery_candidates[1].reason, "probe_degraded");

    ASSERT_EQ(plan.gc_candidates.size(), 2u);
    EXPECT_EQ(plan.gc_candidates[0].name, "leaving");
    EXPECT_EQ(plan.gc_candidates[0].reason, "unmounting");
    EXPECT_EQ(plan.gc_candidates[1].name, "full");
    EXPECT_EQ(plan.gc_candidates[1].reason, "high_usage");
}

TEST(StorageDeviceMaintenanceTest, EmptyInventoryYieldsEmptyPlan) {
    const auto plan = BuildStorageDeviceMaintenancePlan({}, 0.95);
    EXPECT_TRUE(plan.recovery_candidates.empty());
    EXPECT_TRUE(plan.gc_candidates.empty());
}

}  // namespace
}  // namespace mooncake
