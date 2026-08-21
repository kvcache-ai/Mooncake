#include "master_service.h"

#include <atomic>
#include <chrono>
#include <filesystem>
#include <future>
#include <fstream>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <string>
#include <thread>
#include <unordered_set>
#include <vector>

#include <gtest/gtest.h>
#include <unistd.h>

#include "allocation_strategy.h"
#include "master_metric_manager.h"
#include "tenant_quota_policy_store.h"
#include "types.h"

namespace mooncake::test {

class BlockingTenantQuotaPolicyStore final : public TenantQuotaPolicyStore {
   public:
    explicit BlockingTenantQuotaPolicyStore(TenantQuotaPolicySnapshot snapshot)
        : snapshot_(std::move(snapshot)),
          allow_save_(allow_save_promise_.get_future()) {}

    std::future<void> SaveStarted() {
        return save_started_promise_.get_future();
    }

    void AllowSave() { allow_save_promise_.set_value(); }

    tl::expected<TenantQuotaPolicySnapshot, std::string> Load() override {
        return snapshot_;
    }

    tl::expected<void, std::string> Save(
        const TenantQuotaPolicySnapshot& snapshot) override {
        snapshot_ = snapshot;
        save_started_promise_.set_value();
        allow_save_.wait();
        return {};
    }

   private:
    TenantQuotaPolicySnapshot snapshot_;
    std::promise<void> save_started_promise_;
    std::promise<void> allow_save_promise_;
    std::future<void> allow_save_;
};

#ifdef USE_NOF
class BlockingAllocationStrategy final : public AllocationStrategy {
   public:
    BlockingAllocationStrategy()
        : allow_allocation_(allow_allocation_promise_.get_future()) {}

    std::future<void> AllocationStarted() {
        return allocation_started_promise_.get_future();
    }

    void AllowAllocation() { allow_allocation_promise_.set_value(); }

    tl::expected<std::vector<Replica>, ErrorCode> Allocate(
        const AllocatorManager& allocator_manager, const size_t slice_length,
        const size_t replica_num,
        const std::vector<std::string>& preferred_segments,
        const std::set<std::string>& excluded_segments,
        const ReplicaType replica_type) override {
        BlockOnce();
        return delegate_.Allocate(allocator_manager, slice_length, replica_num,
                                  preferred_segments, excluded_segments,
                                  replica_type);
    }

    tl::expected<Replica, ErrorCode> AllocateFrom(
        const AllocatorManager& allocator_manager, const size_t slice_length,
        const std::string& segment_name) override {
        return delegate_.AllocateFrom(allocator_manager, slice_length,
                                      segment_name);
    }

   private:
    void BlockOnce() {
        bool expected = true;
        if (block_next_allocation_.compare_exchange_strong(expected, false)) {
            allocation_started_promise_.set_value();
            allow_allocation_.wait();
        }
    }

    RandomAllocationStrategy delegate_;
    std::atomic<bool> block_next_allocation_{true};
    std::promise<void> allocation_started_promise_;
    std::promise<void> allow_allocation_promise_;
    std::future<void> allow_allocation_;
};
#endif

class MasterServiceTenantQuotaTest : public ::testing::Test {
   protected:
    static constexpr size_t kSegmentBase = 0x500000000;

    std::string WritePolicyFile(
        const std::map<TenantId, uint64_t>& tenant_quotas) {
        TenantQuotaPolicySnapshot snapshot;
        for (const auto& [tenant_id, quota] : tenant_quotas) {
            snapshot.tenant_quotas.emplace(tenant_id.value(), quota);
        }
        auto path =
            std::filesystem::temp_directory_path() /
            ("mooncake_tenant_quota_test_" + std::to_string(::getpid()) + "_" +
             std::to_string(next_policy_file_++) + ".yaml");
        std::ofstream out(path);
        out << FormatTenantQuotaPolicyYaml(snapshot);
        out.close();
        policy_files_.push_back(path.string());
        return path.string();
    }

    MasterServiceConfig MakeConfig(
        const std::map<TenantId, uint64_t>& tenant_quotas,
        bool enable_multi_tenants = true) {
        auto builder = MasterServiceConfig::builder().set_enable_multi_tenants(
            enable_multi_tenants);
        if (enable_multi_tenants) {
            builder.set_tenant_quota_connector_type("file")
                .set_tenant_quota_connector_uri(WritePolicyFile(tenant_quotas));
        }
        return builder.build();
    }

    UUID MountSegment(MasterService& service, size_t size = 4096,
                      std::string name = "quota_segment") {
        Segment segment;
        segment.id = generate_uuid();
        segment.name = std::move(name);
        segment.base = kSegmentBase + next_segment_offset_;
        segment.size = size;
        segment.te_endpoint = segment.name;
        next_segment_offset_ += size + 4096;

        UUID client_id = generate_uuid();
        auto result = service.MountSegment(segment, client_id);
        EXPECT_TRUE(result.has_value()) << toString(result.error());
        return client_id;
    }

#ifdef USE_NOF
    UUID MountNoFSegment(MasterService& service, size_t size = 4096,
                         std::string name = "quota_nof_segment") {
        NoFSegment segment;
        segment.id = generate_uuid();
        segment.name = std::move(name);
        segment.base = kSegmentBase + next_segment_offset_;
        segment.size = size;
        segment.te_endpoint = segment.name;
        next_segment_offset_ += size + 4096;

        UUID client_id = generate_uuid();
        auto result = service.MountNoFSegment(segment, client_id);
        EXPECT_TRUE(result.has_value()) << toString(result.error());
        return client_id;
    }
#endif

    ReplicateConfig MemoryConfig() {
        ReplicateConfig config;
        config.replica_num = 1;
        return config;
    }

    void PutComplete(MasterService& service, const UUID& client_id,
                     const std::string& key, const TenantId& tenant_id,
                     uint64_t size) {
        auto start =
            service.PutStart(client_id, key, tenant_id, size, MemoryConfig());
        ASSERT_TRUE(start.has_value()) << toString(start.error());
        auto end =
            service.PutEnd(client_id, key, tenant_id, ReplicaType::MEMORY);
        ASSERT_TRUE(end.has_value()) << toString(end.error());
    }

    TenantQuotaSnapshot Snapshot(MasterService& service,
                                 const TenantId& tenant_id) {
        auto snapshot = service.GetTenantQuotaSnapshot(tenant_id);
        EXPECT_TRUE(snapshot.has_value());
        return *snapshot;
    }

    void ReloadTenantQuotaPolicyFromStore(MasterService& service) {
        service.LoadTenantQuotaPoliciesFromStoreOrThrow();
        service.RebuildTenantQuotaUsageFromMetadata();
    }

    void ReplaceTenantQuotaPolicyStore(
        MasterService& service, std::unique_ptr<TenantQuotaPolicyStore> store) {
        service.tenant_quota_policy_store_ = std::move(store);
    }

    int64_t LocalDiskUsedBytes(MasterService& service, const UUID& client_id) {
        auto usage = service.local_ssd_manager_.GetUsage(client_id);
        EXPECT_TRUE(usage.has_value());
        if (!usage) {
            return -1;
        }
        return usage->used_bytes;
    }

    void ClearInvalidHandlesForTesting(
        MasterService& service,
        const std::unordered_set<UUID, boost::hash<UUID>>& alive_clients) {
        service.ClearInvalidHandles(alive_clients);
    }

#ifdef USE_NOF
    void ReplaceAllocationStrategy(
        MasterService& service, std::shared_ptr<AllocationStrategy> strategy) {
        service.allocation_strategy_ = std::move(strategy);
    }
#endif

    tl::expected<void, ErrorCode> ChargeTenantQuotaForTest(
        MasterService& service, const TenantId& tenant_id, uint64_t bytes) {
        return service.ChargeTenantQuota(
            service.tenant_quota_table_.GetOrCreateTenantHandle(tenant_id),
            bytes);
    }

    TenantQuotaHandle GetOrCreateTenantStateHandleForTest(
        MasterService& service, size_t shard_idx, const TenantId& tenant_id) {
        MasterService::MetadataShardAccessorRW shard(&service, shard_idx);
        auto& tenant_state =
            service.GetOrCreateTenantState(shard.get(), tenant_id);
        return service.GetBoundTenantQuotaHandle(tenant_state);
    }

    tl::expected<void, ErrorCode> ChargeBoundTenantQuotaForTest(
        MasterService& service, TenantQuotaHandle account, uint64_t bytes) {
        return service.ChargeTenantQuota(account, bytes);
    }

    void ReleaseBoundTenantQuotaForTest(MasterService& service,
                                        TenantQuotaHandle account,
                                        uint64_t bytes) {
        service.ReleaseTenantQuota(account, bytes);
    }

    void DiscardExpiredProcessingForTest(MasterService& service,
                                         const TenantId& tenant_id,
                                         const std::string& key) {
        const size_t shard_idx = service.getMetadataShardIndex(tenant_id, key);
        MasterService::MetadataShardAccessorRW shard(&service, shard_idx);
        service.DiscardExpiredProcessingReplicas(
            shard, std::chrono::system_clock::time_point::max());
    }

    void FinalizeExpiredProcessingForTest(MasterService& service,
                                          const TenantId& tenant_id,
                                          const std::string& key) {
        OpLogEntry entry;
        entry.tenant_id = tenant_id.value();
        entry.object_key = key;
        service.FinalizeExpiredProcessingReplicasAfterDurable(
            entry, std::chrono::system_clock::now());
    }

    void FinalizeRemovedMemoryReplicasForTest(MasterService& service,
                                              const TenantId& tenant_id,
                                              const std::string& key) {
        std::vector<ReplicaID> removed_ids;
        {
            MasterService::MetadataAccessorRW accessor(
                &service, MasterService::ObjectIdentity{tenant_id, key});
            ASSERT_TRUE(accessor.Exists());
            accessor.Get().VisitReplicas(
                &Replica::fn_is_memory_replica,
                [&removed_ids](Replica& replica) {
                    removed_ids.push_back(replica.id());
                    replica.mark_removed();
                });
        }
        ASSERT_FALSE(removed_ids.empty());

        OpLogEntry entry;
        entry.tenant_id = tenant_id.value();
        entry.object_key = key;
        service.FinalizeRemovedReplicasAfterDurable(
            entry, removed_ids, MasterService::QuotaEraseMode::kFull);
    }

    void AddCompletedDiskReplica(MasterService& service, const UUID& client_id,
                                 const std::string& key,
                                 const TenantId& tenant_id, uint64_t size) {
        Replica disk_replica(client_id, size, "disk-endpoint",
                             ReplicaStatus::COMPLETE);
        auto result =
            service.AddReplica(client_id, key, tenant_id, disk_replica);
        ASSERT_TRUE(result.has_value()) << toString(result.error());
    }

    void ExpectDiskOnlyObjectAndChargedBytes(MasterService& service,
                                             const TenantId& tenant_id,
                                             const std::string& key,
                                             uint64_t charged_bytes) {
        EXPECT_EQ(Snapshot(service, tenant_id).charged_bytes, charged_bytes);
        auto replicas = service.GetReplicaList(key, tenant_id);
        ASSERT_TRUE(replicas.has_value()) << toString(replicas.error());
        ASSERT_EQ(replicas->replicas.size(), 1);
        EXPECT_TRUE(replicas->replicas.front().is_local_disk_replica());
    }

    std::unique_lock<std::shared_mutex> LockSnapshotForTest(
        MasterService& service) {
        return std::unique_lock<std::shared_mutex>(service.snapshot_mutex_);
    }

    std::unique_lock<std::mutex> LockTenantQuotaRecomputeForTest(
        MasterService& service) {
        return std::unique_lock<std::mutex>(
            service.tenant_quota_recompute_mutex_);
    }

    std::unique_lock<std::mutex> LockTenantQuotaPolicyForTest(
        MasterService& service) {
        return std::unique_lock<std::mutex>(service.tenant_quota_policy_mutex_);
    }

    ErrorCode MountSegmentWithoutQuotaRecomputeForTest(MasterService& service,
                                                       size_t size,
                                                       std::string name) {
        Segment segment;
        segment.id = generate_uuid();
        segment.name = std::move(name);
        segment.base = kSegmentBase + next_segment_offset_;
        segment.size = size;
        segment.te_endpoint = segment.name;
        next_segment_offset_ += size + 4096;

        auto segment_access = service.segment_manager_.getSegmentAccess();
        return segment_access.MountSegment(segment, generate_uuid());
    }

    void RecomputeTenantEffectiveQuotasForTest(MasterService& service) {
        service.RecomputeTenantEffectiveQuotas();
    }

    bool WaitForTenantQuotaPolicyMutexContention(MasterService& service) {
        for (int i = 0; i < 500; ++i) {
            if (!service.tenant_quota_policy_mutex_.try_lock()) {
                return true;
            }
            service.tenant_quota_policy_mutex_.unlock();
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        return false;
    }

    void TearDown() override {
        for (const auto& path : policy_files_) {
            std::error_code ec;
            std::filesystem::remove(path, ec);
        }
    }

    size_t next_segment_offset_ = 0;
    size_t next_policy_file_ = 0;
    std::vector<std::string> policy_files_;
};

TEST_F(MasterServiceTenantQuotaTest,
       SingleTenantModeCollapsesTenantsAndDisablesQuota) {
    MasterService service(MakeConfig({}, /*enable_multi_tenants=*/false));
    UUID client_id = MountSegment(service, /*size=*/1024);

    PutComplete(service, client_id, "shared-key", TenantId("tenant-a"), 800);

    EXPECT_TRUE(service.ExistKey("shared-key", TenantId("tenant-b")).value());
    auto duplicate = service.PutStart(client_id, "shared-key",
                                      TenantId("tenant-b"), 1, MemoryConfig());
    ASSERT_FALSE(duplicate.has_value());
    EXPECT_EQ(duplicate.error(), ErrorCode::OBJECT_ALREADY_EXISTS);
    EXPECT_TRUE(service
                    .Remove("shared-key", TenantId("tenant-b"),
                            /*force=*/true)
                    .has_value());
    EXPECT_FALSE(
        service.GetTenantQuotaSnapshot(TenantId("tenant-a")).has_value());
}

TEST_F(MasterServiceTenantQuotaTest,
       MultiTenantModeRejectsUnregisteredAndImplicitDefaultWrites) {
    MasterService service(MakeConfig({{TenantId("tenant-a"), 1000}}));
    UUID client_id = MountSegment(service);

    auto missing = service.PutStart(client_id, "missing", TenantId("tenant-b"),
                                    10, MemoryConfig());
    ASSERT_FALSE(missing.has_value());
    EXPECT_EQ(missing.error(), ErrorCode::TENANT_NOT_REGISTERED);

    auto implicit_default = service.PutStart(
        client_id, "default-key", TenantId::Default(), 10, MemoryConfig());
    ASSERT_FALSE(implicit_default.has_value());
    EXPECT_EQ(implicit_default.error(), ErrorCode::TENANT_NOT_REGISTERED);

    const std::string control_tenant("tenant\0bad", 10);
    EXPECT_FALSE(TenantId(control_tenant).IsValid());

    auto register_default =
        service.UpsertTenantQuotaPolicy(TenantId::Default(), 100);
    ASSERT_TRUE(register_default.has_value())
        << toString(register_default.error());
    PutComplete(service, client_id, "registered-default", TenantId::Default(),
                10);

    PutComplete(service, client_id, "ok", TenantId("tenant-a"), 10);
}

TEST_F(MasterServiceTenantQuotaTest,
       SameTenantStatesAcrossMetadataShardsShareBoundHandle) {
    const TenantId tenant_id("tenant-a");
    MasterService service(MakeConfig({{tenant_id, 1000}}));
    MountSegment(service);

    auto* first_handle =
        GetOrCreateTenantStateHandleForTest(service, 0, tenant_id);
    auto* second_handle =
        GetOrCreateTenantStateHandleForTest(service, 1, tenant_id);

    ASSERT_NE(first_handle, nullptr);
    EXPECT_EQ(first_handle, second_handle);

    auto charge = ChargeBoundTenantQuotaForTest(service, first_handle, 128);
    ASSERT_TRUE(charge.has_value()) << toString(charge.error());
    EXPECT_EQ(Snapshot(service, tenant_id).charged_bytes, 128);

    ReleaseBoundTenantQuotaForTest(service, second_handle, 128);
    EXPECT_EQ(Snapshot(service, tenant_id).charged_bytes, 0);
}

TEST_F(MasterServiceTenantQuotaTest,
       ChargeRejectsMissingHandleWhenQuotaIsEnabled) {
    const TenantId tenant_id("tenant-a");
    MasterService service(MakeConfig({{tenant_id, 1000}}));
    MountSegment(service);

    auto charge = ChargeBoundTenantQuotaForTest(service, nullptr, 1);
    ASSERT_FALSE(charge.has_value());
    EXPECT_EQ(charge.error(), ErrorCode::INTERNAL_ERROR);
    EXPECT_EQ(Snapshot(service, tenant_id).charged_bytes, 0);
}

TEST_F(MasterServiceTenantQuotaTest,
       MultiTenantModeRejectsUnregisteredOffloadSuccess) {
    MasterService service(MakeConfig({{TenantId("tenant-a"), 1000}}));
    UUID client_id = MountSegment(service);

    StorageObjectMetadata metadata;
    metadata.data_size = 128;
    metadata.transport_endpoint = "disk-endpoint";
    std::vector<OffloadTaskItem> tasks{
        OffloadTaskItem{.tenant_id = "tenant-b", .key = "ghost", .size = 128}};

    auto result = service.NotifyOffloadSuccess(client_id, tasks, {metadata});

    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::TENANT_NOT_REGISTERED);
    auto missing = service.ExistKey("ghost", TenantId("tenant-b"));
    ASSERT_TRUE(missing.has_value()) << toString(missing.error());
    EXPECT_FALSE(missing.value());
}

TEST_F(MasterServiceTenantQuotaTest,
       MultiTenantModeAllowsRegisteredOffloadSuccess) {
    MasterService service(MakeConfig({{TenantId("tenant-a"), 1000}}));
    UUID client_id = MountSegment(service);

    StorageObjectMetadata metadata;
    metadata.data_size = 128;
    metadata.transport_endpoint = "disk-endpoint";
    std::vector<OffloadTaskItem> tasks{
        OffloadTaskItem{.tenant_id = "tenant-a", .key = "cold", .size = 128}};

    auto result = service.NotifyOffloadSuccess(client_id, tasks, {metadata});

    ASSERT_TRUE(result.has_value()) << toString(result.error());
    auto exists = service.ExistKey("cold", TenantId("tenant-a"));
    ASSERT_TRUE(exists.has_value()) << toString(exists.error());
    EXPECT_TRUE(exists.value());
    EXPECT_EQ(Snapshot(service, TenantId("tenant-a")).charged_bytes, 0);
}

TEST_F(MasterServiceTenantQuotaTest,
       ConnectorPolicyReloadKeepsLocalDiskOnlyOrphanAccessible) {
    const std::string initial_policy = WritePolicyFile(
        {{TenantId("tenant-a"), 1000}, {TenantId("tenant-b"), 1000}});
    auto config = MasterServiceConfig::builder()
                      .set_enable_multi_tenants(true)
                      .set_tenant_quota_connector_type("file")
                      .set_tenant_quota_connector_uri(initial_policy)
                      .build();
    MasterService service(config);
    UUID client_id = MountSegment(service);

    StorageObjectMetadata metadata;
    metadata.data_size = 128;
    metadata.transport_endpoint = "disk-endpoint";
    std::vector<OffloadTaskItem> tasks{
        OffloadTaskItem{.tenant_id = "tenant-b", .key = "cold", .size = 128}};
    ASSERT_TRUE(
        service.NotifyOffloadSuccess(client_id, tasks, {metadata}).has_value());

    {
        std::ofstream out(initial_policy);
        TenantQuotaPolicySnapshot replacement;
        replacement.tenant_quotas = {{"tenant-a", 1000}};
        out << FormatTenantQuotaPolicyYaml(replacement);
    }
    ReloadTenantQuotaPolicyFromStore(service);

    EXPECT_FALSE(
        service.GetTenantQuotaSnapshot(TenantId("tenant-b")).has_value());

    EXPECT_TRUE(service.Remove("cold", TenantId("tenant-b"), /*force=*/true)
                    .has_value());
    EXPECT_FALSE(
        service.GetTenantQuotaSnapshot(TenantId("tenant-b")).has_value());
}

TEST_F(MasterServiceTenantQuotaTest,
       NotifyOffloadSuccessCompletesExistingOrphanObject) {
    const std::string initial_policy = WritePolicyFile(
        {{TenantId("tenant-a"), 1000}, {TenantId("tenant-b"), 1000}});
    auto config = MasterServiceConfig::builder()
                      .set_enable_multi_tenants(true)
                      .set_enable_offload(true)
                      .set_tenant_quota_connector_type("file")
                      .set_tenant_quota_connector_uri(initial_policy)
                      .build();
    MasterService service(config);
    UUID client_id = MountSegment(service);
    ASSERT_TRUE(service.MountLocalDiskSegment(client_id, true).has_value());
    PutComplete(service, client_id, "warming", TenantId("tenant-b"), 128);

    {
        std::ofstream out(initial_policy);
        TenantQuotaPolicySnapshot replacement;
        replacement.tenant_quotas = {{"tenant-a", 1000}};
        out << FormatTenantQuotaPolicyYaml(replacement);
    }
    ReloadTenantQuotaPolicyFromStore(service);
    auto orphan = Snapshot(service, TenantId("tenant-b"));
    EXPECT_FALSE(orphan.has_explicit_policy);
    EXPECT_TRUE(orphan.admission_closed);
    EXPECT_EQ(orphan.charged_bytes, 128);

    StorageObjectMetadata metadata;
    metadata.data_size = 128;
    metadata.transport_endpoint = "disk-endpoint";
    std::vector<OffloadTaskItem> tasks{OffloadTaskItem{
        .tenant_id = "tenant-b", .key = "warming", .size = 128}};

    auto result = service.NotifyOffloadSuccess(client_id, tasks, {metadata});

    ASSERT_TRUE(result.has_value()) << toString(result.error());
    auto replicas = service.GetReplicaList("warming", TenantId("tenant-b"));
    ASSERT_TRUE(replicas.has_value()) << toString(replicas.error());
    EXPECT_TRUE(std::any_of(replicas->replicas.begin(),
                            replicas->replicas.end(),
                            [](const Replica::Descriptor& replica) {
                                return replica.is_local_disk_replica();
                            }));
}

TEST_F(MasterServiceTenantQuotaTest,
       NotifyOffloadSuccessRejectsOrphanObjectWithoutOffloadTask) {
    const std::string initial_policy = WritePolicyFile(
        {{TenantId("tenant-a"), 1000}, {TenantId("tenant-b"), 1000}});
    auto config = MasterServiceConfig::builder()
                      .set_enable_multi_tenants(true)
                      .set_tenant_quota_connector_type("file")
                      .set_tenant_quota_connector_uri(initial_policy)
                      .build();
    MasterService service(config);
    UUID client_id = MountSegment(service);
    PutComplete(service, client_id, "warming", TenantId("tenant-b"), 128);

    {
        std::ofstream out(initial_policy);
        TenantQuotaPolicySnapshot replacement;
        replacement.tenant_quotas = {{"tenant-a", 1000}};
        out << FormatTenantQuotaPolicyYaml(replacement);
    }
    ReloadTenantQuotaPolicyFromStore(service);
    EXPECT_FALSE(Snapshot(service, TenantId("tenant-b")).has_explicit_policy);

    StorageObjectMetadata metadata;
    metadata.data_size = 128;
    metadata.transport_endpoint = "disk-endpoint";
    std::vector<OffloadTaskItem> tasks{OffloadTaskItem{
        .tenant_id = "tenant-b", .key = "warming", .size = 128}};

    auto result = service.NotifyOffloadSuccess(client_id, tasks, {metadata});

    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::TENANT_NOT_REGISTERED);
}

TEST_F(MasterServiceTenantQuotaTest,
       NotifyOffloadSuccessDoesNotCountAddReplicaUpdateAsNewDiskUsage) {
    const std::string policy = WritePolicyFile({{TenantId("tenant-a"), 1000}});
    auto config = MasterServiceConfig::builder()
                      .set_enable_multi_tenants(true)
                      .set_enable_offload(true)
                      .set_tenant_quota_connector_type("file")
                      .set_tenant_quota_connector_uri(policy)
                      .build();
    MasterService service(config);
    UUID client_a = MountSegment(service, 4096, "quota_segment_a");
    UUID client_b = MountSegment(service, 4096, "quota_segment_b");
    ASSERT_TRUE(service.MountLocalDiskSegment(client_a, true).has_value());
    ASSERT_TRUE(service.MountLocalDiskSegment(client_b, true).has_value());

    StorageObjectMetadata first_metadata{};
    first_metadata.data_size = 128;
    first_metadata.transport_endpoint = "disk-endpoint-a";
    std::vector<OffloadTaskItem> tasks{
        OffloadTaskItem{.tenant_id = "tenant-a", .key = "cold", .size = 128}};
    ASSERT_TRUE(service.NotifyOffloadSuccess(client_a, tasks, {first_metadata})
                    .has_value());
    EXPECT_EQ(LocalDiskUsedBytes(service, client_a), 128);
    EXPECT_EQ(LocalDiskUsedBytes(service, client_b), 0);

    StorageObjectMetadata second_metadata{};
    second_metadata.data_size = 128;
    second_metadata.transport_endpoint = "disk-endpoint-b";
    auto result =
        service.NotifyOffloadSuccess(client_b, tasks, {second_metadata});

    ASSERT_TRUE(result.has_value()) << toString(result.error());
    EXPECT_EQ(LocalDiskUsedBytes(service, client_a), 128);
    EXPECT_EQ(LocalDiskUsedBytes(service, client_b), 0);

    auto replicas = service.GetReplicaList("cold", TenantId("tenant-a"));
    ASSERT_TRUE(replicas.has_value()) << toString(replicas.error());
    ASSERT_EQ(replicas->replicas.size(), 1u);
    EXPECT_EQ(replicas->replicas.front().get_local_disk_descriptor().client_id,
              client_a);

    StorageObjectMetadata empty_endpoint_metadata{};
    empty_endpoint_metadata.data_size = 128;
    auto empty_endpoint_result = service.NotifyOffloadSuccess(
        client_b, tasks, {empty_endpoint_metadata});
    ASSERT_TRUE(empty_endpoint_result.has_value())
        << toString(empty_endpoint_result.error());
    EXPECT_EQ(LocalDiskUsedBytes(service, client_a), 128);
    EXPECT_EQ(LocalDiskUsedBytes(service, client_b), 0);
}

TEST_F(MasterServiceTenantQuotaTest,
       NotifyOffloadSuccessTransfersDiskReplicaAfterClientRestart) {
    const std::string policy = WritePolicyFile({{TenantId("tenant-a"), 1000}});
    auto config = MasterServiceConfig::builder()
                      .set_enable_multi_tenants(true)
                      .set_enable_offload(true)
                      .set_tenant_quota_connector_type("file")
                      .set_tenant_quota_connector_uri(policy)
                      .build();
    MasterService service(config);
    UUID old_client = MountSegment(service, 4096, "restart_segment_old");
    UUID restarted_client =
        MountSegment(service, 4096, "restart_segment_restarted");
    ASSERT_TRUE(service.MountLocalDiskSegment(old_client, true).has_value());
    ASSERT_TRUE(
        service.MountLocalDiskSegment(restarted_client, true).has_value());

    const int64_t initial_allocated_file_size =
        MasterMetricManager::instance().get_allocated_file_size();

    std::vector<OffloadTaskItem> tasks{
        OffloadTaskItem{.tenant_id = "tenant-a", .key = "cold", .size = 128}};
    StorageObjectMetadata metadata{};
    metadata.data_size = 128;
    metadata.transport_endpoint = "stable-disk-endpoint";
    ASSERT_TRUE(service.NotifyOffloadSuccess(old_client, tasks, {metadata})
                    .has_value());
    EXPECT_EQ(LocalDiskUsedBytes(service, old_client), 128);
    EXPECT_EQ(MasterMetricManager::instance().get_allocated_file_size(),
              initial_allocated_file_size + 128);

    StorageObjectMetadata inconsistent_metadata{};
    inconsistent_metadata.data_size = 256;
    inconsistent_metadata.transport_endpoint = "stable-disk-endpoint";
    auto inconsistent_result = service.NotifyOffloadSuccess(
        restarted_client, tasks, {inconsistent_metadata});
    ASSERT_FALSE(inconsistent_result.has_value());
    EXPECT_EQ(inconsistent_result.error(), ErrorCode::INVALID_PARAMS);
    EXPECT_EQ(LocalDiskUsedBytes(service, old_client), 128);
    EXPECT_EQ(LocalDiskUsedBytes(service, restarted_client), 0);
    EXPECT_EQ(MasterMetricManager::instance().get_allocated_file_size(),
              initial_allocated_file_size + 128);

    ASSERT_TRUE(
        service.NotifyOffloadSuccess(restarted_client, tasks, {metadata})
            .has_value());
    EXPECT_EQ(LocalDiskUsedBytes(service, old_client), 0);
    EXPECT_EQ(LocalDiskUsedBytes(service, restarted_client), 128);
    EXPECT_EQ(MasterMetricManager::instance().get_allocated_file_size(),
              initial_allocated_file_size + 128);

    auto replicas = service.GetReplicaList("cold", TenantId("tenant-a"));
    ASSERT_TRUE(replicas.has_value()) << toString(replicas.error());
    ASSERT_EQ(replicas->replicas.size(), 1u);
    const auto& local_disk =
        replicas->replicas.front().get_local_disk_descriptor();
    EXPECT_EQ(local_disk.client_id, restarted_client);
    EXPECT_EQ(local_disk.transport_endpoint, "stable-disk-endpoint");

    ClearInvalidHandlesForTesting(service, {restarted_client});
    auto after_old_client_expiry =
        service.GetReplicaList("cold", TenantId("tenant-a"));
    ASSERT_TRUE(after_old_client_expiry.has_value())
        << toString(after_old_client_expiry.error());
    ASSERT_EQ(after_old_client_expiry->replicas.size(), 1u);
    EXPECT_EQ(after_old_client_expiry->replicas.front()
                  .get_local_disk_descriptor()
                  .client_id,
              restarted_client);
}

TEST_F(MasterServiceTenantQuotaTest,
       NotifyOffloadSuccessRejectsRestartTransferWithoutMountedDiskSegment) {
    const std::string policy = WritePolicyFile({{TenantId("tenant-a"), 1000}});
    auto config = MasterServiceConfig::builder()
                      .set_enable_multi_tenants(true)
                      .set_enable_offload(true)
                      .set_tenant_quota_connector_type("file")
                      .set_tenant_quota_connector_uri(policy)
                      .build();
    MasterService service(config);
    UUID old_client = MountSegment(service, 4096, "restart_segment_old");
    UUID restarted_client =
        MountSegment(service, 4096, "restart_segment_restarted");
    ASSERT_TRUE(service.MountLocalDiskSegment(old_client, true).has_value());

    std::vector<OffloadTaskItem> tasks{
        OffloadTaskItem{.tenant_id = "tenant-a", .key = "cold", .size = 128}};
    StorageObjectMetadata metadata{};
    metadata.data_size = 128;
    metadata.transport_endpoint = "stable-disk-endpoint";
    ASSERT_TRUE(service.NotifyOffloadSuccess(old_client, tasks, {metadata})
                    .has_value());

    auto transfer =
        service.NotifyOffloadSuccess(restarted_client, tasks, {metadata});
    ASSERT_FALSE(transfer.has_value());
    EXPECT_EQ(transfer.error(), ErrorCode::SEGMENT_NOT_FOUND);
    EXPECT_EQ(LocalDiskUsedBytes(service, old_client), 128);

    auto replicas = service.GetReplicaList("cold", TenantId("tenant-a"));
    ASSERT_TRUE(replicas.has_value()) << toString(replicas.error());
    ASSERT_EQ(replicas->replicas.size(), 1u);
    EXPECT_EQ(replicas->replicas.front().get_local_disk_descriptor().client_id,
              old_client);
}

TEST_F(MasterServiceTenantQuotaTest,
       RegisteredTenantQuotaAdmissionDoesNotCreateImplicitTenants) {
    MasterService service(MakeConfig({{TenantId("tenant-a"), 100}}));
    UUID client_id = MountSegment(service);

    auto hard_pinned = MemoryConfig();
    hard_pinned.with_hard_pin = true;
    auto first = service.PutStart(client_id, "key-a", TenantId("tenant-a"), 80,
                                  hard_pinned);
    ASSERT_TRUE(first.has_value()) << toString(first.error());
    EXPECT_EQ(Snapshot(service, TenantId("tenant-a")).charged_bytes, 80);
    ASSERT_TRUE(service
                    .PutEnd(client_id, "key-a", TenantId("tenant-a"),
                            ReplicaType::MEMORY)
                    .has_value());

    auto over = service.PutStart(client_id, "key-b", TenantId("tenant-a"), 30,
                                 MemoryConfig());

    ASSERT_FALSE(over.has_value());
    EXPECT_EQ(over.error(), ErrorCode::TENANT_QUOTA_EXCEEDED);
    EXPECT_EQ(Snapshot(service, TenantId("tenant-a")).charged_bytes, 80);
    EXPECT_FALSE(
        service.GetTenantQuotaSnapshot(TenantId("tenant-b")).has_value());
}

TEST_F(MasterServiceTenantQuotaTest, PutRevokeRefundsStartCharge) {
    MasterService service(MakeConfig({{TenantId("tenant-a"), 100}}));
    UUID client_id = MountSegment(service);

    auto start = service.PutStart(client_id, "key", TenantId("tenant-a"), 100,
                                  MemoryConfig());
    ASSERT_TRUE(start.has_value()) << toString(start.error());
    EXPECT_EQ(Snapshot(service, TenantId("tenant-a")).charged_bytes, 100);

    auto over = service.PutStart(client_id, "other", TenantId("tenant-a"), 1,
                                 MemoryConfig());
    ASSERT_FALSE(over.has_value());
    EXPECT_EQ(over.error(), ErrorCode::TENANT_QUOTA_EXCEEDED);

    ASSERT_TRUE(service
                    .PutRevoke(client_id, "key", TenantId("tenant-a"),
                               ReplicaType::MEMORY)
                    .has_value());
    EXPECT_EQ(Snapshot(service, TenantId("tenant-a")).charged_bytes, 0);
}

TEST_F(MasterServiceTenantQuotaTest,
       SizeChangingUpsertTransfersAndReleasesReplacementCharge) {
    const TenantId tenant_id("tenant-a");
    MasterService service(MakeConfig({{tenant_id, 1000}}));
    UUID client_id = MountSegment(service);
    PutComplete(service, client_id, "key", tenant_id, 100);

    auto upsert =
        service.UpsertStart(client_id, "key", tenant_id, 200, MemoryConfig());
    ASSERT_TRUE(upsert.has_value()) << toString(upsert.error());
    EXPECT_EQ(Snapshot(service, tenant_id).charged_bytes, 300);

    auto end =
        service.UpsertEnd(client_id, "key", tenant_id, ReplicaType::MEMORY);
    ASSERT_TRUE(end.has_value()) << toString(end.error());
    EXPECT_EQ(Snapshot(service, tenant_id).charged_bytes, 200);
}

TEST_F(MasterServiceTenantQuotaTest,
       SizeChangingUpsertFromDiskOnlyObjectChargesNewReplica) {
    const TenantId tenant_id("tenant-a");
    MasterService service(MakeConfig({{tenant_id, 1000}}));
    UUID client_id = MountSegment(service);

    StorageObjectMetadata metadata;
    metadata.data_size = 100;
    metadata.transport_endpoint = "disk-endpoint";
    std::vector<OffloadTaskItem> tasks{OffloadTaskItem{
        .tenant_id = tenant_id.value(), .key = "key", .size = 100}};
    ASSERT_TRUE(
        service.NotifyOffloadSuccess(client_id, tasks, {metadata}).has_value());
    ASSERT_EQ(Snapshot(service, tenant_id).charged_bytes, 0);

    auto upsert =
        service.UpsertStart(client_id, "key", tenant_id, 200, MemoryConfig());
    ASSERT_TRUE(upsert.has_value()) << toString(upsert.error());
    EXPECT_EQ(Snapshot(service, tenant_id).charged_bytes, 200);

    auto end =
        service.UpsertEnd(client_id, "key", tenant_id, ReplicaType::MEMORY);
    ASSERT_TRUE(end.has_value()) << toString(end.error());
    EXPECT_EQ(Snapshot(service, tenant_id).charged_bytes, 200);
}

TEST_F(MasterServiceTenantQuotaTest,
       SizeChangingUpsertRevokeReleasesNewAndReplacementCharge) {
    const TenantId tenant_id("tenant-a");
    MasterService service(MakeConfig({{tenant_id, 1000}}));
    UUID client_id = MountSegment(service);
    PutComplete(service, client_id, "key", tenant_id, 100);

    auto upsert =
        service.UpsertStart(client_id, "key", tenant_id, 200, MemoryConfig());
    ASSERT_TRUE(upsert.has_value()) << toString(upsert.error());
    EXPECT_EQ(Snapshot(service, tenant_id).charged_bytes, 300);

    auto revoke =
        service.UpsertRevoke(client_id, "key", tenant_id, ReplicaType::MEMORY);
    ASSERT_TRUE(revoke.has_value()) << toString(revoke.error());
    EXPECT_EQ(Snapshot(service, tenant_id).charged_bytes, 0);
}

TEST_F(MasterServiceTenantQuotaTest,
       PartialProcessingExpirySettlesPendingCharge) {
    const TenantId tenant_id("tenant-a");
    MasterService service(MakeConfig({{tenant_id, 1000}}));
    UUID client_id = MountSegment(service);

    auto start =
        service.PutStart(client_id, "key", tenant_id, 100, MemoryConfig());
    ASSERT_TRUE(start.has_value()) << toString(start.error());
    AddCompletedDiskReplica(service, client_id, "key", tenant_id, 100);
    EXPECT_EQ(Snapshot(service, tenant_id).charged_bytes, 100);

    DiscardExpiredProcessingForTest(service, tenant_id, "key");

    ExpectDiskOnlyObjectAndChargedBytes(service, tenant_id, "key", 0);
}

TEST_F(MasterServiceTenantQuotaTest,
       DurablePartialProcessingExpirySettlesPendingCharge) {
    const TenantId tenant_id("tenant-a");
    MasterService service(MakeConfig({{tenant_id, 1000}}));
    UUID client_id = MountSegment(service);

    auto start =
        service.PutStart(client_id, "key", tenant_id, 100, MemoryConfig());
    ASSERT_TRUE(start.has_value()) << toString(start.error());
    AddCompletedDiskReplica(service, client_id, "key", tenant_id, 100);
    EXPECT_EQ(Snapshot(service, tenant_id).charged_bytes, 100);

    FinalizeExpiredProcessingForTest(service, tenant_id, "key");

    ExpectDiskOnlyObjectAndChargedBytes(service, tenant_id, "key", 0);
}

TEST_F(MasterServiceTenantQuotaTest,
       PartialSizeChangingUpsertRevokeReleasesReplacementCharge) {
    const TenantId tenant_id("tenant-a");
    MasterService service(MakeConfig({{tenant_id, 1000}}));
    UUID client_id = MountSegment(service);
    PutComplete(service, client_id, "key", tenant_id, 100);

    auto upsert =
        service.UpsertStart(client_id, "key", tenant_id, 200, MemoryConfig());
    ASSERT_TRUE(upsert.has_value()) << toString(upsert.error());
    AddCompletedDiskReplica(service, client_id, "key", tenant_id, 200);
    EXPECT_EQ(Snapshot(service, tenant_id).charged_bytes, 300);

    auto revoke =
        service.UpsertRevoke(client_id, "key", tenant_id, ReplicaType::MEMORY);

    ASSERT_TRUE(revoke.has_value()) << toString(revoke.error());
    ExpectDiskOnlyObjectAndChargedBytes(service, tenant_id, "key", 0);
}

TEST_F(MasterServiceTenantQuotaTest,
       DurablePartialUpsertRevokeReleasesReplacementCharge) {
    const TenantId tenant_id("tenant-a");
    MasterService service(MakeConfig({{tenant_id, 1000}}));
    UUID client_id = MountSegment(service);
    PutComplete(service, client_id, "key", tenant_id, 100);

    auto upsert =
        service.UpsertStart(client_id, "key", tenant_id, 200, MemoryConfig());
    ASSERT_TRUE(upsert.has_value()) << toString(upsert.error());
    AddCompletedDiskReplica(service, client_id, "key", tenant_id, 200);
    EXPECT_EQ(Snapshot(service, tenant_id).charged_bytes, 300);

    FinalizeRemovedMemoryReplicasForTest(service, tenant_id, "key");

    ExpectDiskOnlyObjectAndChargedBytes(service, tenant_id, "key", 0);
}

TEST_F(MasterServiceTenantQuotaTest, CopyStartRequiresQuotaForNewReplica) {
    MasterService service(MakeConfig({{TenantId("tenant-a"), 150}}));
    UUID client_id = MountSegment(service, /*size=*/1024, "segment-a");
    MountSegment(service, /*size=*/1024, "segment-b");

    ReplicateConfig config = MemoryConfig();
    config.preferred_segment = "segment-a";
    auto put_start =
        service.PutStart(client_id, "key", TenantId("tenant-a"), 100, config);
    ASSERT_TRUE(put_start.has_value()) << toString(put_start.error());
    ASSERT_TRUE(
        service
            .PutEnd(client_id, "key", TenantId("tenant-a"), ReplicaType::MEMORY)
            .has_value());

    auto copy = service.CopyStart(client_id, "key", TenantId("tenant-a"),
                                  "segment-a", {"segment-b"});

    ASSERT_FALSE(copy.has_value());
    EXPECT_EQ(copy.error(), ErrorCode::TENANT_QUOTA_EXCEEDED);
    auto snapshot = Snapshot(service, TenantId("tenant-a"));
    EXPECT_EQ(snapshot.charged_bytes, 100);
}

TEST_F(MasterServiceTenantQuotaTest, CopyEndRetainsAdditionalReplicaCharge) {
    MasterService service(MakeConfig({{TenantId("tenant-a"), 300}}));
    UUID client_id = MountSegment(service, /*size=*/1024, "segment-a");
    MountSegment(service, /*size=*/1024, "segment-b");

    ReplicateConfig config = MemoryConfig();
    config.preferred_segment = "segment-a";
    auto put_start =
        service.PutStart(client_id, "key", TenantId("tenant-a"), 100, config);
    ASSERT_TRUE(put_start.has_value()) << toString(put_start.error());
    ASSERT_TRUE(
        service
            .PutEnd(client_id, "key", TenantId("tenant-a"), ReplicaType::MEMORY)
            .has_value());

    auto copy = service.CopyStart(client_id, "key", TenantId("tenant-a"),
                                  "segment-a", {"segment-b"});
    ASSERT_TRUE(copy.has_value()) << toString(copy.error());
    auto in_flight = Snapshot(service, TenantId("tenant-a"));
    EXPECT_EQ(in_flight.charged_bytes, 200);

    ASSERT_TRUE(
        service.CopyEnd(client_id, "key", TenantId("tenant-a")).has_value());
    auto completed = Snapshot(service, TenantId("tenant-a"));
    EXPECT_EQ(completed.charged_bytes, 200);
}

TEST_F(MasterServiceTenantQuotaTest, CopyRevokeRefundsStartCharge) {
    MasterService service(MakeConfig({{TenantId("tenant-a"), 300}}));
    UUID client_id = MountSegment(service, /*size=*/1024, "segment-a");
    MountSegment(service, /*size=*/1024, "segment-b");

    ReplicateConfig config = MemoryConfig();
    config.preferred_segment = "segment-a";
    auto put_start =
        service.PutStart(client_id, "key", TenantId("tenant-a"), 100, config);
    ASSERT_TRUE(put_start.has_value()) << toString(put_start.error());
    ASSERT_TRUE(
        service
            .PutEnd(client_id, "key", TenantId("tenant-a"), ReplicaType::MEMORY)
            .has_value());

    ASSERT_TRUE(service
                    .CopyStart(client_id, "key", TenantId("tenant-a"),
                               "segment-a", {"segment-b"})
                    .has_value());
    EXPECT_EQ(Snapshot(service, TenantId("tenant-a")).charged_bytes, 200);

    ASSERT_TRUE(
        service.CopyRevoke(client_id, "key", TenantId("tenant-a")).has_value());
    EXPECT_EQ(Snapshot(service, TenantId("tenant-a")).charged_bytes, 100);
}

TEST_F(MasterServiceTenantQuotaTest,
       MoveStartRequiresQuotaForTemporaryReplica) {
    MasterService service(MakeConfig({{TenantId("tenant-a"), 150}}));
    UUID client_id = MountSegment(service, /*size=*/1024, "segment-a");
    MountSegment(service, /*size=*/1024, "segment-b");

    ReplicateConfig config = MemoryConfig();
    config.preferred_segment = "segment-a";
    auto put_start =
        service.PutStart(client_id, "key", TenantId("tenant-a"), 100, config);
    ASSERT_TRUE(put_start.has_value()) << toString(put_start.error());
    ASSERT_TRUE(
        service
            .PutEnd(client_id, "key", TenantId("tenant-a"), ReplicaType::MEMORY)
            .has_value());

    auto move = service.MoveStart(client_id, "key", TenantId("tenant-a"),
                                  "segment-a", "segment-b");

    ASSERT_FALSE(move.has_value());
    EXPECT_EQ(move.error(), ErrorCode::TENANT_QUOTA_EXCEEDED);
    auto snapshot = Snapshot(service, TenantId("tenant-a"));
    EXPECT_EQ(snapshot.charged_bytes, 100);
}

TEST_F(MasterServiceTenantQuotaTest, MoveEndSettlesToFinalReplicaCharge) {
    MasterService service(MakeConfig({{TenantId("tenant-a"), 300}}));
    UUID client_id = MountSegment(service, /*size=*/1024, "segment-a");
    MountSegment(service, /*size=*/1024, "segment-b");

    ReplicateConfig config = MemoryConfig();
    config.preferred_segment = "segment-a";
    ASSERT_TRUE(
        service.PutStart(client_id, "key", TenantId("tenant-a"), 100, config)
            .has_value());
    ASSERT_TRUE(
        service
            .PutEnd(client_id, "key", TenantId("tenant-a"), ReplicaType::MEMORY)
            .has_value());

    ASSERT_TRUE(service
                    .MoveStart(client_id, "key", TenantId("tenant-a"),
                               "segment-a", "segment-b")
                    .has_value());
    EXPECT_EQ(Snapshot(service, TenantId("tenant-a")).charged_bytes, 200);

    ASSERT_TRUE(
        service.MoveEnd(client_id, "key", TenantId("tenant-a")).has_value());
    EXPECT_EQ(Snapshot(service, TenantId("tenant-a")).charged_bytes, 100);
}

TEST_F(MasterServiceTenantQuotaTest, DeletePolicyRequiresTenantWithoutObjects) {
    MasterService service(MakeConfig({{TenantId("tenant-a"), 1000}}));
    UUID client_id = MountSegment(service);
    PutComplete(service, client_id, "key", TenantId("tenant-a"), 100);

    auto delete_non_empty =
        service.DeleteTenantQuotaPolicy(TenantId("tenant-a"));
    ASSERT_FALSE(delete_non_empty.has_value());
    EXPECT_EQ(delete_non_empty.error(), ErrorCode::TENANT_NOT_EMPTY);

    auto upsert = service.UpsertTenantQuotaPolicy(TenantId("tenant-b"), 100);
    ASSERT_TRUE(upsert.has_value()) << toString(upsert.error());
    auto delete_empty = service.DeleteTenantQuotaPolicy(TenantId("tenant-b"));
    ASSERT_TRUE(delete_empty.has_value()) << toString(delete_empty.error());
    EXPECT_FALSE(delete_empty.value().has_value());
}

TEST_F(MasterServiceTenantQuotaTest,
       DeletePolicyBlocksValidatedChargesBeforeConnectorSave) {
    MasterService service(MakeConfig({{TenantId("tenant-a"), 1000}}));
    MountSegment(service);

    TenantQuotaPolicySnapshot current_policy;
    current_policy.tenant_quotas = {{"tenant-a", 1000}};
    auto blocking_store =
        std::make_unique<BlockingTenantQuotaPolicyStore>(current_policy);
    auto* blocking_store_ptr = blocking_store.get();
    auto save_started = blocking_store_ptr->SaveStarted();
    ReplaceTenantQuotaPolicyStore(service, std::move(blocking_store));

    using DeleteResult =
        tl::expected<std::optional<TenantQuotaSnapshot>, ErrorCode>;
    std::optional<DeleteResult> delete_result;
    std::thread delete_thread([&] {
        delete_result.emplace(
            service.DeleteTenantQuotaPolicy(TenantId("tenant-a")));
    });

    if (save_started.wait_for(std::chrono::seconds(5)) !=
        std::future_status::ready) {
        blocking_store_ptr->AllowSave();
        delete_thread.join();
        FAIL() << "timed out waiting for connector save";
    }

    auto charge = ChargeTenantQuotaForTest(service, TenantId("tenant-a"), 1);
    EXPECT_FALSE(charge.has_value());
    EXPECT_EQ(charge.error(), ErrorCode::TENANT_NOT_REGISTERED);

    auto zero_byte_charge =
        ChargeTenantQuotaForTest(service, TenantId("tenant-a"), 0);
    EXPECT_FALSE(zero_byte_charge.has_value());
    EXPECT_EQ(zero_byte_charge.error(), ErrorCode::TENANT_NOT_REGISTERED);

    blocking_store_ptr->AllowSave();
    delete_thread.join();

    ASSERT_TRUE(delete_result.has_value());
    ASSERT_TRUE(delete_result->has_value()) << toString(delete_result->error());
    EXPECT_FALSE(delete_result->value().has_value());
    EXPECT_FALSE(
        service.GetTenantQuotaSnapshot(TenantId("tenant-a")).has_value());
}

TEST_F(MasterServiceTenantQuotaTest,
       DeletePolicyWaitsForInFlightAddReplicaBeforeEmptyCheck) {
    MasterService service(MakeConfig({{TenantId("tenant-a"), 1000}}));
    UUID client_id = MountSegment(service);

    TenantQuotaPolicySnapshot current_policy;
    current_policy.tenant_quotas = {{"tenant-a", 1000}};
    auto blocking_store =
        std::make_unique<BlockingTenantQuotaPolicyStore>(current_policy);
    auto* blocking_store_ptr = blocking_store.get();
    auto save_started = blocking_store_ptr->SaveStarted();
    ReplaceTenantQuotaPolicyStore(service, std::move(blocking_store));

    auto snapshot_lock = LockSnapshotForTest(service);
    std::optional<tl::expected<bool, ErrorCode>> add_result;
    std::thread add_thread([&] {
        Replica replica(client_id, 128, "disk-endpoint",
                        ReplicaStatus::COMPLETE);
        add_result.emplace(service.AddReplica(client_id, "cold",
                                              TenantId("tenant-a"), replica));
    });

    if (!WaitForTenantQuotaPolicyMutexContention(service)) {
        snapshot_lock.unlock();
        add_thread.join();
        FAIL() << "timed out waiting for AddReplica to enter tenant policy "
                  "critical section";
    }

    using DeleteResult =
        tl::expected<std::optional<TenantQuotaSnapshot>, ErrorCode>;
    std::optional<DeleteResult> delete_result;
    std::thread delete_thread([&] {
        delete_result.emplace(
            service.DeleteTenantQuotaPolicy(TenantId("tenant-a")));
    });

    const auto premature_save =
        save_started.wait_for(std::chrono::milliseconds(200));
    if (premature_save == std::future_status::ready) {
        blocking_store_ptr->AllowSave();
    }
    snapshot_lock.unlock();
    add_thread.join();
    delete_thread.join();

    ASSERT_EQ(premature_save, std::future_status::timeout)
        << "tenant deletion reached connector save before in-flight "
           "AddReplica completed";
    ASSERT_TRUE(add_result.has_value());
    ASSERT_TRUE(add_result->has_value()) << toString(add_result->error());
    ASSERT_TRUE(delete_result.has_value());
    ASSERT_FALSE(delete_result->has_value());
    EXPECT_EQ(delete_result->error(), ErrorCode::TENANT_NOT_EMPTY);
    auto exists = service.ExistKey("cold", TenantId("tenant-a"));
    ASSERT_TRUE(exists.has_value()) << toString(exists.error());
    EXPECT_TRUE(exists.value());
}

#ifdef USE_NOF
TEST_F(MasterServiceTenantQuotaTest,
       DeletePolicySeesZeroChargePutStartMetadataCreateWithoutPolicyLock) {
    MasterService service(MakeConfig({{TenantId("tenant-a"), 1000}}));
    UUID client_id = MountNoFSegment(service);

    auto blocking_strategy = std::make_shared<BlockingAllocationStrategy>();
    auto* blocking_strategy_ptr = blocking_strategy.get();
    auto allocation_started = blocking_strategy_ptr->AllocationStarted();
    ReplaceAllocationStrategy(service, std::move(blocking_strategy));

    ReplicateConfig config;
    config.replica_num = 0;
    config.nof_replica_num = 1;

    std::optional<tl::expected<std::vector<Replica::Descriptor>, ErrorCode>>
        put_result;
    auto policy_lock = LockTenantQuotaPolicyForTest(service);
    std::thread put_thread([&] {
        put_result.emplace(service.PutStart(client_id, "nof-key",
                                            TenantId("tenant-a"), 128, config));
    });

    if (allocation_started.wait_for(std::chrono::seconds(5)) !=
        std::future_status::ready) {
        policy_lock.unlock();
        blocking_strategy_ptr->AllowAllocation();
        put_thread.join();
        FAIL() << "zero-charge PutStart waited for tenant quota policy mutex";
    }
    policy_lock.unlock();

    using DeleteResult =
        tl::expected<std::optional<TenantQuotaSnapshot>, ErrorCode>;
    std::promise<DeleteResult> delete_promise;
    auto delete_future = delete_promise.get_future();
    std::thread delete_thread([&] {
        delete_promise.set_value(
            service.DeleteTenantQuotaPolicy(TenantId("tenant-a")));
    });

    EXPECT_EQ(delete_future.wait_for(std::chrono::milliseconds(200)),
              std::future_status::timeout)
        << "tenant deletion passed the metadata scan while zero-charge "
           "PutStart still held the target metadata shard";

    blocking_strategy_ptr->AllowAllocation();
    put_thread.join();
    delete_thread.join();

    ASSERT_TRUE(put_result.has_value());
    ASSERT_TRUE(put_result->has_value()) << toString(put_result->error());
    auto delete_result = delete_future.get();
    ASSERT_FALSE(delete_result.has_value());
    EXPECT_EQ(delete_result.error(), ErrorCode::TENANT_NOT_EMPTY);

    auto snapshot = Snapshot(service, TenantId("tenant-a"));
    EXPECT_EQ(snapshot.charged_bytes, 0);
}
#endif

TEST_F(MasterServiceTenantQuotaTest,
       EffectiveQuotaUsesOnlyExplicitPolicyAndScalesProportionally) {
    MasterService service(
        MakeConfig({{TenantId("tenant-a"), 200}, {TenantId("tenant-b"), 400}}));
    MountSegment(service, /*size=*/300);

    EXPECT_EQ(Snapshot(service, TenantId("tenant-a")).effective_quota_bytes,
              100);
    EXPECT_EQ(Snapshot(service, TenantId("tenant-b")).effective_quota_bytes,
              200);
}

TEST_F(MasterServiceTenantQuotaTest,
       CapacityIsSampledInsideQuotaRecomputeCoordination) {
    MasterService service(MakeConfig({{TenantId("tenant-a"), 1000}}));
    auto recompute_lock = LockTenantQuotaRecomputeForTest(service);
    ASSERT_EQ(MountSegmentWithoutQuotaRecomputeForTest(service, /*size=*/100,
                                                       "capacity-a"),
              ErrorCode::OK);

    std::promise<void> recompute_started;
    auto recompute_started_future = recompute_started.get_future();
    auto recompute = std::async(std::launch::async, [&] {
        recompute_started.set_value();
        RecomputeTenantEffectiveQuotasForTest(service);
    });
    recompute_started_future.wait();

    EXPECT_EQ(recompute.wait_for(std::chrono::milliseconds(100)),
              std::future_status::timeout);
    const auto second_mount_result = MountSegmentWithoutQuotaRecomputeForTest(
        service, /*size=*/50, "capacity-b");
    recompute_lock.unlock();

    ASSERT_EQ(second_mount_result, ErrorCode::OK);
    ASSERT_EQ(recompute.wait_for(std::chrono::seconds(5)),
              std::future_status::ready);
    recompute.get();
    EXPECT_EQ(Snapshot(service, TenantId("tenant-a")).effective_quota_bytes,
              150);
}

TEST_F(MasterServiceTenantQuotaTest,
       ConnectorPolicyReloadCreatesOrphanStateAndAllowsCleanup) {
    const std::string initial_policy = WritePolicyFile(
        {{TenantId("tenant-a"), 1000}, {TenantId("tenant-b"), 1000}});
    auto config = MasterServiceConfig::builder()
                      .set_enable_multi_tenants(true)
                      .set_tenant_quota_connector_type("file")
                      .set_tenant_quota_connector_uri(initial_policy)
                      .build();
    MasterService service(config);
    UUID client_id = MountSegment(service);
    PutComplete(service, client_id, "orphan-key", TenantId("tenant-b"), 100);

    {
        std::ofstream out(initial_policy);
        TenantQuotaPolicySnapshot replacement;
        replacement.tenant_quotas = {{"tenant-a", 1000}};
        out << FormatTenantQuotaPolicyYaml(replacement);
    }
    ReloadTenantQuotaPolicyFromStore(service);

    auto orphan = Snapshot(service, TenantId("tenant-b"));
    EXPECT_FALSE(orphan.has_explicit_policy);
    EXPECT_EQ(orphan.requested_quota_bytes, 0);
    EXPECT_EQ(orphan.effective_quota_bytes, 0);
    EXPECT_TRUE(orphan.over_quota);

    EXPECT_TRUE(
        service.GetReplicaList("orphan-key", TenantId("tenant-b")).has_value());
    auto write = service.PutStart(client_id, "new-key", TenantId("tenant-b"), 1,
                                  MemoryConfig());
    ASSERT_FALSE(write.has_value());
    EXPECT_EQ(write.error(), ErrorCode::TENANT_NOT_REGISTERED);

    EXPECT_TRUE(service
                    .Remove("orphan-key", TenantId("tenant-b"),
                            /*force=*/true)
                    .has_value());
}

}  // namespace mooncake::test
