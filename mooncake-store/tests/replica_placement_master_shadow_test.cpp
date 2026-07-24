// Copyright 2026 Mooncake Authors

#include "master_service.h"

#include "master_metric_manager.h"

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <unistd.h>
#include <gtest/gtest.h>

namespace mooncake::test {
namespace {

size_t Tier(ReplicaPlacementTier tier) { return static_cast<size_t>(tier); }

size_t Observation(ReplicaTemperature temperature,
                   ReplicaPlacementShadowSignalStatus status) {
    return static_cast<size_t>(temperature) *
               kReplicaPlacementShadowSignalStatusCount +
           static_cast<size_t>(status);
}

size_t Intent(ReplicaTemperature temperature, ReplicaPlacementTier tier) {
    return static_cast<size_t>(temperature) * kReplicaPlacementTierCount +
           Tier(tier);
}

uint64_t TotalObservations(
    const ReplicaPlacementShadowCountersSnapshot& counters) {
    uint64_t total = 0;
    for (uint64_t count : counters.observations) total += count;
    return total;
}

ReplicaPlacementShadowConfig ShadowConfig() {
    ReplicaPlacementShadowConfig config;
    config.policy.targets[static_cast<size_t>(ReplicaTemperature::COLD)] = {
        ReplicaTierTarget{1, true}, ReplicaTierTarget{0, false},
        ReplicaTierTarget{0, false}, ReplicaTierTarget{0, false}};
    config.policy.targets[static_cast<size_t>(ReplicaTemperature::WARM)] = {
        ReplicaTierTarget{1, true}, ReplicaTierTarget{1, false},
        ReplicaTierTarget{0, false}, ReplicaTierTarget{0, false}};
    config.policy.targets[static_cast<size_t>(ReplicaTemperature::HOT)] = {
        ReplicaTierTarget{2, true}, ReplicaTierTarget{1, false},
        ReplicaTierTarget{0, false}, ReplicaTierTarget{0, false}};
    config.policy.min_complete_replicas = 1;
    config.policy.max_total_replicas = 4;
    config.warm_threshold = 2;
    config.hot_threshold = 3;
    config.signal_ttl = std::chrono::seconds(30);
    config.sketch_width = 4096;
    config.sketch_depth = 4;
    return config;
}

MasterReplicaPlacementShadowConfig ExplicitSignalShadowConfig() {
    MasterReplicaPlacementShadowConfig config;
    config.evaluator = ShadowConfig();
    config.auto_collect_master_signals = false;
    return config;
}

ReplicaPlacementSignalSnapshot AvailableSnapshot(uint64_t generation) {
    ReplicaPlacementSignalSnapshot snapshot;
    snapshot.generation = generation;
    snapshot.ready = true;
    snapshot.observed_at = std::chrono::steady_clock::now();
    for (auto& tier : snapshot.tiers) {
        tier.allocation_state = ReplicaTierSignalState::AVAILABLE;
        tier.health_state = ReplicaTierSignalState::AVAILABLE;
    }
    return snapshot;
}

class ReplicaPlacementMasterShadowTest : public ::testing::Test {
   protected:
    static constexpr size_t kSegmentBase = 0x530000000;
    static constexpr size_t kSegmentSize = 16 * 1024 * 1024;

    UUID PrepareMemorySegment(MasterService& service, const std::string& name) {
        Segment segment;
        segment.id = generate_uuid();
        segment.name = name;
        segment.base = kSegmentBase;
        segment.size = kSegmentSize;
        segment.te_endpoint = name;
        UUID client_id = generate_uuid();
        EXPECT_TRUE(service.MountSegment(segment, client_id).has_value());
        return client_id;
    }

    void PutObject(MasterService& service, const UUID& client_id,
                   const std::string& key) {
        ReplicateConfig config;
        config.replica_num = 1;
        ASSERT_TRUE(
            service.PutStart(client_id, key, TenantId::Default(), 1024, config)
                .has_value());
        ASSERT_TRUE(service
                        .PutEnd(client_id, key, TenantId::Default(),
                                ReplicaType::MEMORY)
                        .has_value());
    }
};

TEST_F(ReplicaPlacementMasterShadowTest, DefaultOffHasNoObserverState) {
    MasterService service;

    EXPECT_FALSE(service.ReplicaPlacementShadowEnabled());
    EXPECT_EQ(
        service.PublishReplicaPlacementSignalSnapshot(AvailableSnapshot(1)),
        ReplicaPlacementSignalPublishStatus::NOT_ENABLED);
    EXPECT_EQ(service.ReplicaPlacementShadowSignalGeneration(), 0);
    EXPECT_FALSE(service.GetReplicaPlacementShadowCounters().has_value());
}

TEST_F(ReplicaPlacementMasterShadowTest,
       MissingSnapshotRecordsDegradedObservationWithoutActuation) {
    MasterServiceConfig config;
    config.replica_placement_shadow_config = ExplicitSignalShadowConfig();
    MasterService service(config);
    const UUID client_id = PrepareMemorySegment(service, "shadow_missing");
    PutObject(service, client_id, "key");

    ASSERT_TRUE(service.GetReplicaList("key", TenantId::Default()).has_value());

    const auto counters = service.GetReplicaPlacementShadowCounters();
    ASSERT_TRUE(counters.has_value());
    EXPECT_EQ(TotalObservations(*counters), 1);
    EXPECT_EQ(counters->observations[Observation(
                  ReplicaTemperature::COLD,
                  ReplicaPlacementShadowSignalStatus::MISSING_SNAPSHOT)],
              1);
    service.RemoveAll();
}

TEST_F(ReplicaPlacementMasterShadowTest,
       UnavailableAndStaleExternalSignalsRemainNonActuating) {
    MasterServiceConfig config;
    config.replica_placement_shadow_config = ExplicitSignalShadowConfig();
    MasterService service(config);
    ReplicaPlacementSignalSnapshot unavailable;
    unavailable.generation = 1;
    unavailable.observed_at = std::chrono::steady_clock::now();
    ASSERT_EQ(
        service.PublishReplicaPlacementSignalSnapshot(std::move(unavailable)),
        ReplicaPlacementSignalPublishStatus::PUBLISHED);
    const UUID client_id =
        PrepareMemorySegment(service, "shadow_external_faults");
    PutObject(service, client_id, "key");

    ASSERT_TRUE(service.GetReplicaList("key", TenantId::Default()).has_value());
    auto stale = AvailableSnapshot(2);
    stale.observed_at =
        std::chrono::steady_clock::now() - std::chrono::seconds(31);
    ASSERT_EQ(service.PublishReplicaPlacementSignalSnapshot(std::move(stale)),
              ReplicaPlacementSignalPublishStatus::PUBLISHED);
    ASSERT_TRUE(service.GetReplicaList("key", TenantId::Default()).has_value());

    const auto counters = service.GetReplicaPlacementShadowCounters();
    ASSERT_TRUE(counters.has_value());
    EXPECT_EQ(counters->observations[Observation(
                  ReplicaTemperature::COLD,
                  ReplicaPlacementShadowSignalStatus::SOURCE_UNAVAILABLE)],
              1);
    EXPECT_EQ(counters->observations[Observation(
                  ReplicaTemperature::WARM,
                  ReplicaPlacementShadowSignalStatus::STALE_SNAPSHOT)],
              1);
    uint64_t intents = 0;
    for (uint64_t count : counters->add_intents) intents += count;
    for (uint64_t count : counters->remove_intents) intents += count;
    EXPECT_EQ(intents, 0);
    service.RemoveAll();
}

TEST_F(ReplicaPlacementMasterShadowTest,
       SingleAndBatchGetDriveShadowButAdminGetDoesNot) {
    MasterServiceConfig config;
    config.replica_placement_shadow_config = ExplicitSignalShadowConfig();
    MasterService service(config);
    ASSERT_EQ(
        service.PublishReplicaPlacementSignalSnapshot(AvailableSnapshot(1)),
        ReplicaPlacementSignalPublishStatus::PUBLISHED);
    const UUID client_id = PrepareMemorySegment(service, "shadow_reads");
    PutObject(service, client_id, "hot");
    PutObject(service, client_id, "batch");

    ASSERT_TRUE(service.GetReplicaList("hot", TenantId::Default()).has_value());
    ASSERT_TRUE(service.GetReplicaList("hot", TenantId::Default()).has_value());
    ASSERT_TRUE(service.GetReplicaList("hot", TenantId::Default()).has_value());
    const auto batch =
        service.BatchGetReplicaList({"batch"}, TenantId::Default());
    ASSERT_EQ(batch.size(), 1);
    ASSERT_TRUE(batch[0].has_value());

    auto counters = service.GetReplicaPlacementShadowCounters();
    ASSERT_TRUE(counters.has_value());
    EXPECT_EQ(TotalObservations(*counters), 4);
    EXPECT_EQ(counters->add_intents[Intent(ReplicaTemperature::WARM,
                                           ReplicaPlacementTier::LOCAL_DISK)],
              1);
    EXPECT_EQ(counters->add_intents[Intent(ReplicaTemperature::HOT,
                                           ReplicaPlacementTier::MEMORY)],
              1);

    ASSERT_TRUE(
        service.GetReplicaListForAdmin("hot", TenantId::Default()).has_value());
    const auto after_admin = service.GetReplicaPlacementShadowCounters();
    ASSERT_TRUE(after_admin.has_value());
    EXPECT_EQ(TotalObservations(*after_admin), 4);
    service.RemoveAll();
}

TEST_F(ReplicaPlacementMasterShadowTest,
       ConcurrentGetsPreserveExactObservationCount) {
    MasterServiceConfig config;
    config.replica_placement_shadow_config = ExplicitSignalShadowConfig();
    MasterService service(config);
    ASSERT_EQ(
        service.PublishReplicaPlacementSignalSnapshot(AvailableSnapshot(1)),
        ReplicaPlacementSignalPublishStatus::PUBLISHED);
    const UUID client_id = PrepareMemorySegment(service, "shadow_concurrent");
    PutObject(service, client_id, "key");

    constexpr size_t kThreadCount = 8;
    constexpr size_t kIterations = 500;
    std::atomic<uint64_t> failures{0};
    std::vector<std::thread> threads;
    threads.reserve(kThreadCount);
    for (size_t thread = 0; thread < kThreadCount; ++thread) {
        threads.emplace_back([&]() {
            for (size_t i = 0; i < kIterations; ++i) {
                if (!service.GetReplicaList("key", TenantId::Default())
                         .has_value()) {
                    failures.fetch_add(1, std::memory_order_relaxed);
                }
            }
        });
    }
    for (auto& thread : threads) thread.join();

    const auto counters = service.GetReplicaPlacementShadowCounters();
    ASSERT_TRUE(counters.has_value());
    EXPECT_EQ(failures.load(std::memory_order_relaxed), 0);
    EXPECT_EQ(TotalObservations(*counters), kThreadCount * kIterations);
    service.RemoveAll();
}

TEST_F(ReplicaPlacementMasterShadowTest,
       PrometheusMetricsUseOnlyFixedCardinalityLabels) {
    MasterServiceConfig config;
    config.replica_placement_shadow_config = ExplicitSignalShadowConfig();
    MasterService service(config);
    ASSERT_EQ(
        service.PublishReplicaPlacementSignalSnapshot(AvailableSnapshot(1)),
        ReplicaPlacementSignalPublishStatus::PUBLISHED);
    const UUID client_id = PrepareMemorySegment(service, "shadow_metrics");
    const std::string secret_key = "must_not_appear_object_key_7f3a";
    PutObject(service, client_id, secret_key);

    ASSERT_TRUE(
        service.GetReplicaList(secret_key, TenantId::Default()).has_value());

    ReplicaPlacementShadowResult synthetic;
    synthetic.temperature = ReplicaTemperature::HOT;
    synthetic.signal_status = ReplicaPlacementShadowSignalStatus::READY;
    synthetic.plan.adjustments[Tier(ReplicaPlacementTier::LOCAL_DISK)].add = 2;
    synthetic.plan.adjustments[Tier(ReplicaPlacementTier::MEMORY)].remove = 1;
    synthetic.plan.degraded_reasons[0] =
        ReplicaPlacementDegradedReason::REQUIRED_TIER_UNHEALTHY;
    synthetic.plan.degraded_reason_count = 1;
    MasterMetricManager::instance().observe_replica_placement_shadow(synthetic);

    const std::string metrics =
        MasterMetricManager::instance().serialize_metrics();
    EXPECT_NE(
        metrics.find("master_replica_placement_shadow_observations_total"),
        std::string::npos);
    EXPECT_NE(metrics.find("temperature=\"hot\",status=\"ready\""),
              std::string::npos);
    EXPECT_NE(metrics.find("temperature=\"hot\",tier=\"local_disk\""),
              std::string::npos);
    EXPECT_NE(metrics.find("temperature=\"hot\",tier=\"memory\""),
              std::string::npos);
    EXPECT_NE(metrics.find("reason=\"required_tier_unhealthy\""),
              std::string::npos);
    EXPECT_EQ(metrics.find(secret_key), std::string::npos);
    EXPECT_EQ(metrics.find("tenant"), std::string::npos)
        << "replica placement SHADOW metric labels must not contain tenants";
    EXPECT_EQ(metrics.find("endpoint"), std::string::npos)
        << "replica placement SHADOW metric labels must not contain endpoints";
    service.RemoveAll();
}

TEST_F(ReplicaPlacementMasterShadowTest,
       AutoCollectorPublishesRealMountedMemorySignalsWithBoundedRefresh) {
    MasterReplicaPlacementShadowConfig shadow;
    shadow.evaluator = ShadowConfig();
    shadow.evaluator.policy
        .targets[static_cast<size_t>(ReplicaTemperature::COLD)]
                [Tier(ReplicaPlacementTier::MEMORY)]
        .desired = 2;
    shadow.auto_collect_master_signals = true;
    shadow.signal_refresh_interval = std::chrono::seconds(1);
    MasterServiceConfig config;
    config.replica_placement_shadow_config = shadow;
    MasterService service(config);
    const UUID client_id = PrepareMemorySegment(service, "shadow_collector");
    PutObject(service, client_id, "key");

    ASSERT_TRUE(service.GetReplicaList("key", TenantId::Default()).has_value());
    EXPECT_EQ(service.ReplicaPlacementShadowSignalGeneration(), 1);
    ASSERT_TRUE(service.GetReplicaList("key", TenantId::Default()).has_value());
    EXPECT_EQ(service.ReplicaPlacementShadowSignalGeneration(), 1)
        << "refresh interval must prevent a signal scan on every Get";

    const auto counters = service.GetReplicaPlacementShadowCounters();
    ASSERT_TRUE(counters.has_value());
    EXPECT_EQ(TotalObservations(*counters), 2);
    EXPECT_EQ(counters->observations[Observation(
                  ReplicaTemperature::COLD,
                  ReplicaPlacementShadowSignalStatus::READY)],
              1);
    EXPECT_EQ(counters->observations[Observation(
                  ReplicaTemperature::WARM,
                  ReplicaPlacementShadowSignalStatus::READY)],
              1);
    EXPECT_EQ(counters->add_intents[Intent(ReplicaTemperature::COLD,
                                           ReplicaPlacementTier::MEMORY)],
              1);
    service.RemoveAll();
}

TEST_F(ReplicaPlacementMasterShadowTest,
       AutoCollectorUsesReportedLocalDiskCapacity) {
    MasterReplicaPlacementShadowConfig shadow;
    shadow.evaluator = ShadowConfig();
    shadow.evaluator.policy.targets[static_cast<size_t>(
        ReplicaTemperature::COLD)][Tier(ReplicaPlacementTier::LOCAL_DISK)] =
        ReplicaTierTarget{1, true};
    MasterServiceConfig config;
    config.enable_offload = true;
    config.replica_placement_shadow_config = shadow;
    MasterService service(config);
    const UUID client_id = PrepareMemorySegment(service, "shadow_local_disk");
    ASSERT_TRUE(service.MountLocalDiskSegment(client_id, true).has_value());
    ASSERT_TRUE(service.ReportSsdCapacity(client_id, 1024 * 1024).has_value());
    PutObject(service, client_id, "key");

    ASSERT_TRUE(service.GetReplicaList("key", TenantId::Default()).has_value());
    const auto counters = service.GetReplicaPlacementShadowCounters();
    ASSERT_TRUE(counters.has_value());
    EXPECT_EQ(counters->add_intents[Intent(ReplicaTemperature::COLD,
                                           ReplicaPlacementTier::LOCAL_DISK)],
              1);
    service.RemoveAll();
}

TEST_F(ReplicaPlacementMasterShadowTest,
       AutoCollectorKeepsUnreportedLocalDiskCapacityUnknown) {
    MasterReplicaPlacementShadowConfig shadow;
    shadow.evaluator = ShadowConfig();
    shadow.evaluator.policy.targets[static_cast<size_t>(
        ReplicaTemperature::COLD)][Tier(ReplicaPlacementTier::LOCAL_DISK)] =
        ReplicaTierTarget{1, true};
    MasterServiceConfig config;
    config.enable_offload = true;
    config.replica_placement_shadow_config = shadow;
    MasterService service(config);
    const UUID client_id =
        PrepareMemorySegment(service, "shadow_local_unknown");
    ASSERT_TRUE(service.MountLocalDiskSegment(client_id, true).has_value());
    PutObject(service, client_id, "key");

    ASSERT_TRUE(service.GetReplicaList("key", TenantId::Default()).has_value());
    const auto counters = service.GetReplicaPlacementShadowCounters();
    ASSERT_TRUE(counters.has_value());
    EXPECT_EQ(counters->add_intents[Intent(ReplicaTemperature::COLD,
                                           ReplicaPlacementTier::LOCAL_DISK)],
              0);
    EXPECT_EQ(
        counters->degraded[static_cast<size_t>(
            ReplicaPlacementDegradedReason::REQUIRED_TIER_SIGNAL_UNKNOWN)],
        1);
    service.RemoveAll();
}

TEST_F(ReplicaPlacementMasterShadowTest,
       AutoCollectorRejectsKnownFullLocalDisk) {
    MasterReplicaPlacementShadowConfig shadow;
    shadow.evaluator = ShadowConfig();
    shadow.evaluator.policy.targets[static_cast<size_t>(
        ReplicaTemperature::COLD)][Tier(ReplicaPlacementTier::LOCAL_DISK)] =
        ReplicaTierTarget{2, true};
    MasterServiceConfig config;
    config.enable_offload = true;
    config.replica_placement_shadow_config = shadow;
    MasterService service(config);
    const UUID client_id = PrepareMemorySegment(service, "shadow_local_full");
    ASSERT_TRUE(service.MountLocalDiskSegment(client_id, true).has_value());
    constexpr int64_t kCapacity = 1024;
    ASSERT_TRUE(service.ReportSsdCapacity(client_id, kCapacity).has_value());
    PutObject(service, client_id, "key");
    StorageObjectMetadata metadata;
    metadata.data_size = kCapacity;
    metadata.transport_endpoint = "shadow_local_full";
    OffloadTaskItem task{
        .tenant_id = "default", .key = "key", .size = kCapacity};
    ASSERT_TRUE(service.NotifyOffloadSuccess(client_id, {task}, {metadata})
                    .has_value());

    ASSERT_TRUE(service.GetReplicaList("key", TenantId::Default()).has_value());
    const auto counters = service.GetReplicaPlacementShadowCounters();
    ASSERT_TRUE(counters.has_value());
    EXPECT_EQ(counters->add_intents[Intent(ReplicaTemperature::COLD,
                                           ReplicaPlacementTier::LOCAL_DISK)],
              0);
    EXPECT_EQ(counters->degraded[static_cast<size_t>(
                  ReplicaPlacementDegradedReason::REQUIRED_TIER_UNAVAILABLE)],
              1);
    service.RemoveAll();
}

TEST_F(ReplicaPlacementMasterShadowTest,
       AutoCollectorProbesConfiguredRemoteStoreDirectory) {
    const auto root = std::filesystem::temp_directory_path() /
                      ("mooncake_shadow_remote_" +
                       std::to_string(static_cast<uint64_t>(::getpid())));
    const std::string cluster_id = "cluster";
    ASSERT_TRUE(std::filesystem::create_directories(root / cluster_id));

    MasterReplicaPlacementShadowConfig shadow;
    shadow.evaluator = ShadowConfig();
    shadow.evaluator.policy.targets[static_cast<size_t>(
        ReplicaTemperature::COLD)][Tier(ReplicaPlacementTier::REMOTE_STORE)] =
        ReplicaTierTarget{2, true};
    MasterServiceConfig config;
    config.root_fs_dir = root.string();
    config.cluster_id = cluster_id;
    config.replica_placement_shadow_config = shadow;
    {
        MasterService service(config);
        const UUID client_id =
            PrepareMemorySegment(service, "shadow_remote_store");
        PutObject(service, client_id, "key");

        ASSERT_TRUE(
            service.GetReplicaList("key", TenantId::Default()).has_value());
        const auto counters = service.GetReplicaPlacementShadowCounters();
        ASSERT_TRUE(counters.has_value());
        EXPECT_EQ(
            counters->add_intents[Intent(ReplicaTemperature::COLD,
                                         ReplicaPlacementTier::REMOTE_STORE)],
            1);
        service.RemoveAll();
    }
    std::error_code error;
    std::filesystem::remove_all(root, error);
    EXPECT_FALSE(error);
}

TEST_F(ReplicaPlacementMasterShadowTest,
       AutoCollectorRejectsMissingRemoteStoreDirectory) {
    const auto root = std::filesystem::temp_directory_path() /
                      ("mooncake_shadow_missing_remote_" +
                       std::to_string(static_cast<uint64_t>(::getpid())));
    std::error_code cleanup_error;
    std::filesystem::remove_all(root, cleanup_error);
    ASSERT_FALSE(std::filesystem::exists(root));

    MasterReplicaPlacementShadowConfig shadow;
    shadow.evaluator = ShadowConfig();
    shadow.evaluator.policy.targets[static_cast<size_t>(
        ReplicaTemperature::COLD)][Tier(ReplicaPlacementTier::REMOTE_STORE)] =
        ReplicaTierTarget{2, true};
    MasterServiceConfig config;
    config.root_fs_dir = root.string();
    config.cluster_id = "missing";
    config.replica_placement_shadow_config = shadow;
    MasterService service(config);
    const UUID client_id = PrepareMemorySegment(service, "shadow_remote_down");
    PutObject(service, client_id, "key");

    ASSERT_TRUE(service.GetReplicaList("key", TenantId::Default()).has_value());
    const auto counters = service.GetReplicaPlacementShadowCounters();
    ASSERT_TRUE(counters.has_value());
    EXPECT_EQ(counters->add_intents[Intent(ReplicaTemperature::COLD,
                                           ReplicaPlacementTier::REMOTE_STORE)],
              0);
    EXPECT_EQ(counters->degraded[static_cast<size_t>(
                  ReplicaPlacementDegradedReason::REQUIRED_TIER_UNHEALTHY)],
              1);
    service.RemoveAll();
}

TEST(ReplicaPlacementMasterShadowConfigTest,
     AutoCollectorRefreshCannotOutliveSignalTtl) {
    MasterReplicaPlacementShadowConfig shadow;
    shadow.evaluator = ShadowConfig();
    shadow.evaluator.signal_ttl = std::chrono::milliseconds(10);
    shadow.signal_refresh_interval = std::chrono::milliseconds(11);
    MasterServiceConfig config;
    config.replica_placement_shadow_config = shadow;

    EXPECT_THROW((void)MasterService(config), std::invalid_argument);
}

TEST(ReplicaPlacementMasterShadowConfigTest,
     WrappedConfigPropagatesExplicitOptInOnly) {
    WrappedMasterServiceConfig wrapped;
    wrapped.default_kv_lease_ttl = DEFAULT_DEFAULT_KV_LEASE_TTL;
    MasterServiceConfig disabled(wrapped);
    EXPECT_FALSE(disabled.replica_placement_shadow_config.has_value());

    wrapped.replica_placement_shadow_config = ExplicitSignalShadowConfig();
    MasterServiceConfig enabled(wrapped);
    ASSERT_TRUE(enabled.replica_placement_shadow_config.has_value());
    EXPECT_EQ(enabled.replica_placement_shadow_config->evaluator.warm_threshold,
              2);
    EXPECT_EQ(enabled.replica_placement_shadow_config->evaluator.hot_threshold,
              3);
}

}  // namespace
}  // namespace mooncake::test
