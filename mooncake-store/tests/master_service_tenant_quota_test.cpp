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
#include <vector>

#include <gtest/gtest.h>
#include <unistd.h>

#include "allocation_strategy.h"
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

    tl::expected<std::vector<Replica>, ErrorCode> Allocate(
        const AllocatorManager& allocator_manager, const size_t slice_length,
        const size_t replica_num,
        const std::vector<std::string>& preferred_segments,
        const std::set<std::string>& excluded_segments,
        const ReplicaType replica_type,
        const SsdMetricsProvider* ssd_provider) override {
        (void)ssd_provider;
        return Allocate(allocator_manager, slice_length, replica_num,
                        preferred_segments, excluded_segments, replica_type);
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
        const std::map<std::string, uint64_t>& tenant_quotas) {
        TenantQuotaPolicySnapshot snapshot;
        snapshot.tenant_quotas = tenant_quotas;
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
        const std::map<std::string, uint64_t>& tenant_quotas,
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
                     const std::string& key, const std::string& tenant_id,
                     uint64_t size) {
        auto start =
            service.PutStart(client_id, key, tenant_id, size, MemoryConfig());
        ASSERT_TRUE(start.has_value()) << toString(start.error());
        auto end =
            service.PutEnd(client_id, key, tenant_id, ReplicaType::MEMORY);
        ASSERT_TRUE(end.has_value()) << toString(end.error());
    }

    TenantQuotaSnapshot Snapshot(MasterService& service,
                                 const std::string& tenant_id) {
        auto snapshot = service.GetTenantQuotaSnapshotForTesting(tenant_id);
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
        auto access = service.segment_manager_.getLocalDiskSegmentAccess();
        auto& segments = access.getClientLocalDiskSegment();
        auto it = segments.find(client_id);
        EXPECT_TRUE(it != segments.end());
        if (it == segments.end()) {
            return -1;
        }
        return it->second->ssd_used_bytes.load(std::memory_order_relaxed);
    }

#ifdef USE_NOF
    void ReplaceAllocationStrategy(
        MasterService& service, std::shared_ptr<AllocationStrategy> strategy) {
        service.allocation_strategy_ = std::move(strategy);
    }
#endif

    tl::expected<void, ErrorCode> ReserveTenantQuotaForTest(
        MasterService& service, const std::string& tenant_id, uint64_t bytes) {
        return service.ReserveTenantQuota(tenant_id, bytes);
    }

    std::unique_lock<std::shared_mutex> LockSnapshotForTest(
        MasterService& service) {
        return std::unique_lock<std::shared_mutex>(service.snapshot_mutex_);
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

    PutComplete(service, client_id, "shared-key", "tenant-a", 800);

    EXPECT_TRUE(service.ExistKey("shared-key", "tenant-b").value());
    auto duplicate = service.PutStart(client_id, "shared-key", "tenant-b", 1,
                                      MemoryConfig());
    ASSERT_FALSE(duplicate.has_value());
    EXPECT_EQ(duplicate.error(), ErrorCode::OBJECT_ALREADY_EXISTS);
    EXPECT_TRUE(service
                    .Remove("shared-key", "tenant-b",
                            /*force=*/true)
                    .has_value());
    EXPECT_FALSE(
        service.GetTenantQuotaSnapshotForTesting("tenant-a").has_value());
}

TEST_F(MasterServiceTenantQuotaTest,
       MultiTenantModeRejectsEmptyUnregisteredAndImplicitDefaultWrites) {
    MasterService service(MakeConfig({{"tenant-a", 1000}}));
    UUID client_id = MountSegment(service);

    auto empty = service.PutStart(client_id, "empty", "", 10, MemoryConfig());
    ASSERT_FALSE(empty.has_value());
    EXPECT_EQ(empty.error(), ErrorCode::TENANT_NOT_REGISTERED);

    auto missing =
        service.PutStart(client_id, "missing", "tenant-b", 10, MemoryConfig());
    ASSERT_FALSE(missing.has_value());
    EXPECT_EQ(missing.error(), ErrorCode::TENANT_NOT_REGISTERED);

    auto implicit_default = service.PutStart(client_id, "default-key",
                                             "default", 10, MemoryConfig());
    ASSERT_FALSE(implicit_default.has_value());
    EXPECT_EQ(implicit_default.error(), ErrorCode::TENANT_NOT_REGISTERED);

    const std::string control_tenant("tenant\0bad", 10);
    auto control = service.PutStart(client_id, "control-key", control_tenant,
                                    10, MemoryConfig());
    ASSERT_FALSE(control.has_value());
    EXPECT_EQ(control.error(), ErrorCode::TENANT_NOT_REGISTERED);

    auto control_policy = service.UpsertTenantQuotaPolicy(control_tenant, 100);
    ASSERT_FALSE(control_policy.has_value());
    EXPECT_EQ(control_policy.error(), ErrorCode::INVALID_PARAMS);

    auto register_default = service.UpsertTenantQuotaPolicy("default", 100);
    ASSERT_TRUE(register_default.has_value())
        << toString(register_default.error());
    PutComplete(service, client_id, "registered-default", "default", 10);

    PutComplete(service, client_id, "ok", "tenant-a", 10);
}

TEST_F(MasterServiceTenantQuotaTest,
       MultiTenantModeRejectsUnregisteredOffloadSuccess) {
    MasterService service(MakeConfig({{"tenant-a", 1000}}));
    UUID client_id = MountSegment(service);

    StorageObjectMetadata metadata;
    metadata.data_size = 128;
    metadata.transport_endpoint = "disk-endpoint";
    std::vector<OffloadTaskItem> tasks{
        OffloadTaskItem{.tenant_id = "tenant-b", .key = "ghost", .size = 128}};

    auto result = service.NotifyOffloadSuccess(client_id, tasks, {metadata});

    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::TENANT_NOT_REGISTERED);
    auto missing = service.ExistKey("ghost", "tenant-b");
    ASSERT_TRUE(missing.has_value()) << toString(missing.error());
    EXPECT_FALSE(missing.value());
}

TEST_F(MasterServiceTenantQuotaTest,
       MultiTenantModeAllowsRegisteredOffloadSuccess) {
    MasterService service(MakeConfig({{"tenant-a", 1000}}));
    UUID client_id = MountSegment(service);

    StorageObjectMetadata metadata;
    metadata.data_size = 128;
    metadata.transport_endpoint = "disk-endpoint";
    std::vector<OffloadTaskItem> tasks{
        OffloadTaskItem{.tenant_id = "tenant-a", .key = "cold", .size = 128}};

    auto result = service.NotifyOffloadSuccess(client_id, tasks, {metadata});

    ASSERT_TRUE(result.has_value()) << toString(result.error());
    auto exists = service.ExistKey("cold", "tenant-a");
    ASSERT_TRUE(exists.has_value()) << toString(exists.error());
    EXPECT_TRUE(exists.value());
    EXPECT_EQ(Snapshot(service, "tenant-a").used_bytes, 0);
}

TEST_F(MasterServiceTenantQuotaTest,
       ConnectorPolicyReloadKeepsLocalDiskOnlyOrphanVisible) {
    const std::string initial_policy =
        WritePolicyFile({{"tenant-a", 1000}, {"tenant-b", 1000}});
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

    auto orphan = Snapshot(service, "tenant-b");
    EXPECT_FALSE(orphan.has_explicit_policy);
    EXPECT_EQ(orphan.used_bytes, 0);
    EXPECT_EQ(orphan.committed_count, 0);
    EXPECT_EQ(orphan.metadata_object_count, 1);
    EXPECT_TRUE(orphan.over_quota);

    EXPECT_TRUE(service.Remove("cold", "tenant-b", /*force=*/true).has_value());
    EXPECT_FALSE(
        service.GetTenantQuotaSnapshotForTesting("tenant-b").has_value());
}

TEST_F(MasterServiceTenantQuotaTest,
       NotifyOffloadSuccessCompletesExistingOrphanObject) {
    const std::string initial_policy =
        WritePolicyFile({{"tenant-a", 1000}, {"tenant-b", 1000}});
    auto config = MasterServiceConfig::builder()
                      .set_enable_multi_tenants(true)
                      .set_enable_offload(true)
                      .set_tenant_quota_connector_type("file")
                      .set_tenant_quota_connector_uri(initial_policy)
                      .build();
    MasterService service(config);
    UUID client_id = MountSegment(service);
    ASSERT_TRUE(service.MountLocalDiskSegment(client_id, true).has_value());
    PutComplete(service, client_id, "warming", "tenant-b", 128);

    {
        std::ofstream out(initial_policy);
        TenantQuotaPolicySnapshot replacement;
        replacement.tenant_quotas = {{"tenant-a", 1000}};
        out << FormatTenantQuotaPolicyYaml(replacement);
    }
    ReloadTenantQuotaPolicyFromStore(service);
    EXPECT_FALSE(Snapshot(service, "tenant-b").has_explicit_policy);

    StorageObjectMetadata metadata;
    metadata.data_size = 128;
    metadata.transport_endpoint = "disk-endpoint";
    std::vector<OffloadTaskItem> tasks{OffloadTaskItem{
        .tenant_id = "tenant-b", .key = "warming", .size = 128}};

    auto result = service.NotifyOffloadSuccess(client_id, tasks, {metadata});

    ASSERT_TRUE(result.has_value()) << toString(result.error());
    auto replicas = service.GetReplicaList("warming", "tenant-b");
    ASSERT_TRUE(replicas.has_value()) << toString(replicas.error());
    EXPECT_TRUE(std::any_of(replicas->replicas.begin(),
                            replicas->replicas.end(),
                            [](const Replica::Descriptor& replica) {
                                return replica.is_local_disk_replica();
                            }));
}

TEST_F(MasterServiceTenantQuotaTest,
       NotifyOffloadSuccessRejectsOrphanObjectWithoutOffloadTask) {
    const std::string initial_policy =
        WritePolicyFile({{"tenant-a", 1000}, {"tenant-b", 1000}});
    auto config = MasterServiceConfig::builder()
                      .set_enable_multi_tenants(true)
                      .set_tenant_quota_connector_type("file")
                      .set_tenant_quota_connector_uri(initial_policy)
                      .build();
    MasterService service(config);
    UUID client_id = MountSegment(service);
    PutComplete(service, client_id, "warming", "tenant-b", 128);

    {
        std::ofstream out(initial_policy);
        TenantQuotaPolicySnapshot replacement;
        replacement.tenant_quotas = {{"tenant-a", 1000}};
        out << FormatTenantQuotaPolicyYaml(replacement);
    }
    ReloadTenantQuotaPolicyFromStore(service);
    EXPECT_FALSE(Snapshot(service, "tenant-b").has_explicit_policy);

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
    const std::string policy = WritePolicyFile({{"tenant-a", 1000}});
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

    StorageObjectMetadata first_metadata;
    first_metadata.data_size = 128;
    first_metadata.transport_endpoint = "disk-endpoint-a";
    std::vector<OffloadTaskItem> tasks{
        OffloadTaskItem{.tenant_id = "tenant-a", .key = "cold", .size = 128}};
    ASSERT_TRUE(service.NotifyOffloadSuccess(client_a, tasks, {first_metadata})
                    .has_value());
    EXPECT_EQ(LocalDiskUsedBytes(service, client_a), 128);
    EXPECT_EQ(LocalDiskUsedBytes(service, client_b), 0);

    StorageObjectMetadata second_metadata;
    second_metadata.data_size = 128;
    second_metadata.transport_endpoint = "disk-endpoint-b";
    auto result =
        service.NotifyOffloadSuccess(client_b, tasks, {second_metadata});

    ASSERT_TRUE(result.has_value()) << toString(result.error());
    EXPECT_EQ(LocalDiskUsedBytes(service, client_a), 128);
    EXPECT_EQ(LocalDiskUsedBytes(service, client_b), 0);
}

TEST_F(MasterServiceTenantQuotaTest,
       RegisteredTenantQuotaAdmissionDoesNotCreateImplicitTenants) {
    MasterService service(MakeConfig({{"tenant-a", 100}}));
    UUID client_id = MountSegment(service);

    auto hard_pinned = MemoryConfig();
    hard_pinned.with_hard_pin = true;
    auto first =
        service.PutStart(client_id, "key-a", "tenant-a", 80, hard_pinned);
    ASSERT_TRUE(first.has_value()) << toString(first.error());
    ASSERT_TRUE(
        service.PutEnd(client_id, "key-a", "tenant-a", ReplicaType::MEMORY)
            .has_value());

    auto over =
        service.PutStart(client_id, "key-b", "tenant-a", 30, MemoryConfig());

    ASSERT_FALSE(over.has_value());
    EXPECT_EQ(over.error(), ErrorCode::TENANT_QUOTA_EXCEEDED);
    EXPECT_EQ(Snapshot(service, "tenant-a").used_bytes, 80);
    EXPECT_FALSE(
        service.GetTenantQuotaSnapshotForTesting("tenant-b").has_value());
}

TEST_F(MasterServiceTenantQuotaTest, CopyStartRequiresQuotaForNewReplica) {
    MasterService service(MakeConfig({{"tenant-a", 150}}));
    UUID client_id = MountSegment(service, /*size=*/1024, "segment-a");
    MountSegment(service, /*size=*/1024, "segment-b");

    ReplicateConfig config = MemoryConfig();
    config.preferred_segment = "segment-a";
    auto put_start =
        service.PutStart(client_id, "key", "tenant-a", 100, config);
    ASSERT_TRUE(put_start.has_value()) << toString(put_start.error());
    ASSERT_TRUE(
        service.PutEnd(client_id, "key", "tenant-a", ReplicaType::MEMORY)
            .has_value());

    auto copy = service.CopyStart(client_id, "key", "tenant-a", "segment-a",
                                  {"segment-b"});

    ASSERT_FALSE(copy.has_value());
    EXPECT_EQ(copy.error(), ErrorCode::TENANT_QUOTA_EXCEEDED);
    auto snapshot = Snapshot(service, "tenant-a");
    EXPECT_EQ(snapshot.used_bytes, 100);
    EXPECT_EQ(snapshot.reserved_bytes, 0);
    EXPECT_EQ(snapshot.committed_count, 1);
}

TEST_F(MasterServiceTenantQuotaTest,
       CopyEndCommitsAdditionalReplicaWithoutExtraObjectCount) {
    MasterService service(MakeConfig({{"tenant-a", 300}}));
    UUID client_id = MountSegment(service, /*size=*/1024, "segment-a");
    MountSegment(service, /*size=*/1024, "segment-b");

    ReplicateConfig config = MemoryConfig();
    config.preferred_segment = "segment-a";
    auto put_start =
        service.PutStart(client_id, "key", "tenant-a", 100, config);
    ASSERT_TRUE(put_start.has_value()) << toString(put_start.error());
    ASSERT_TRUE(
        service.PutEnd(client_id, "key", "tenant-a", ReplicaType::MEMORY)
            .has_value());

    auto copy = service.CopyStart(client_id, "key", "tenant-a", "segment-a",
                                  {"segment-b"});
    ASSERT_TRUE(copy.has_value()) << toString(copy.error());
    auto in_flight = Snapshot(service, "tenant-a");
    EXPECT_EQ(in_flight.used_bytes, 100);
    EXPECT_EQ(in_flight.reserved_bytes, 100);

    ASSERT_TRUE(service.CopyEnd(client_id, "key", "tenant-a").has_value());
    auto completed = Snapshot(service, "tenant-a");
    EXPECT_EQ(completed.used_bytes, 200);
    EXPECT_EQ(completed.reserved_bytes, 0);
    EXPECT_EQ(completed.committed_count, 1);
    EXPECT_EQ(completed.metadata_object_count, 1);
}

TEST_F(MasterServiceTenantQuotaTest,
       MoveStartRequiresQuotaForTemporaryReplica) {
    MasterService service(MakeConfig({{"tenant-a", 150}}));
    UUID client_id = MountSegment(service, /*size=*/1024, "segment-a");
    MountSegment(service, /*size=*/1024, "segment-b");

    ReplicateConfig config = MemoryConfig();
    config.preferred_segment = "segment-a";
    auto put_start =
        service.PutStart(client_id, "key", "tenant-a", 100, config);
    ASSERT_TRUE(put_start.has_value()) << toString(put_start.error());
    ASSERT_TRUE(
        service.PutEnd(client_id, "key", "tenant-a", ReplicaType::MEMORY)
            .has_value());

    auto move = service.MoveStart(client_id, "key", "tenant-a", "segment-a",
                                  "segment-b");

    ASSERT_FALSE(move.has_value());
    EXPECT_EQ(move.error(), ErrorCode::TENANT_QUOTA_EXCEEDED);
    auto snapshot = Snapshot(service, "tenant-a");
    EXPECT_EQ(snapshot.used_bytes, 100);
    EXPECT_EQ(snapshot.reserved_bytes, 0);
}

TEST_F(MasterServiceTenantQuotaTest, AdminDeleteRequiresEmptyTenant) {
    MasterService service(MakeConfig({{"tenant-a", 1000}}));
    UUID client_id = MountSegment(service);
    PutComplete(service, client_id, "key", "tenant-a", 100);

    auto empty_upsert = service.UpsertTenantQuotaPolicy("", 100);
    ASSERT_FALSE(empty_upsert.has_value());
    EXPECT_EQ(empty_upsert.error(), ErrorCode::INVALID_PARAMS);

    auto empty_delete = service.DeleteTenantQuotaPolicy("");
    ASSERT_FALSE(empty_delete.has_value());
    EXPECT_EQ(empty_delete.error(), ErrorCode::INVALID_PARAMS);

    auto delete_non_empty = service.DeleteTenantQuotaPolicy("tenant-a");
    ASSERT_FALSE(delete_non_empty.has_value());
    EXPECT_EQ(delete_non_empty.error(), ErrorCode::TENANT_NOT_EMPTY);

    auto upsert = service.UpsertTenantQuotaPolicy("tenant-b", 100);
    ASSERT_TRUE(upsert.has_value()) << toString(upsert.error());
    auto delete_empty = service.DeleteTenantQuotaPolicy("tenant-b");
    ASSERT_TRUE(delete_empty.has_value()) << toString(delete_empty.error());
    EXPECT_FALSE(delete_empty.value().has_value());
}

TEST_F(MasterServiceTenantQuotaTest,
       DeletePolicyBlocksValidatedReservationsBeforeConnectorSave) {
    MasterService service(MakeConfig({{"tenant-a", 1000}}));
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
        delete_result.emplace(service.DeleteTenantQuotaPolicy("tenant-a"));
    });

    if (save_started.wait_for(std::chrono::seconds(5)) !=
        std::future_status::ready) {
        blocking_store_ptr->AllowSave();
        delete_thread.join();
        FAIL() << "timed out waiting for connector save";
    }

    auto reserve = ReserveTenantQuotaForTest(service, "tenant-a", 1);
    EXPECT_FALSE(reserve.has_value());
    EXPECT_EQ(reserve.error(), ErrorCode::TENANT_NOT_REGISTERED);

    auto zero_byte_reserve = ReserveTenantQuotaForTest(service, "tenant-a", 0);
    EXPECT_FALSE(zero_byte_reserve.has_value());
    EXPECT_EQ(zero_byte_reserve.error(), ErrorCode::TENANT_NOT_REGISTERED);

    blocking_store_ptr->AllowSave();
    delete_thread.join();

    ASSERT_TRUE(delete_result.has_value());
    ASSERT_TRUE(delete_result->has_value()) << toString(delete_result->error());
    EXPECT_FALSE(delete_result->value().has_value());
    EXPECT_FALSE(
        service.GetTenantQuotaSnapshotForTesting("tenant-a").has_value());
}

TEST_F(MasterServiceTenantQuotaTest,
       DeletePolicyWaitsForInFlightAddReplicaBeforeEmptyCheck) {
    MasterService service(MakeConfig({{"tenant-a", 1000}}));
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
        add_result.emplace(
            service.AddReplica(client_id, "cold", "tenant-a", replica));
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
        delete_result.emplace(service.DeleteTenantQuotaPolicy("tenant-a"));
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
    auto exists = service.ExistKey("cold", "tenant-a");
    ASSERT_TRUE(exists.has_value()) << toString(exists.error());
    EXPECT_TRUE(exists.value());
}

#ifdef USE_NOF
TEST_F(MasterServiceTenantQuotaTest,
       DeletePolicyWaitsForZeroChargePutStartMetadataCreate) {
    MasterService service(MakeConfig({{"tenant-a", 1000}}));
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
    std::thread put_thread([&] {
        put_result.emplace(
            service.PutStart(client_id, "nof-key", "tenant-a", 128, config));
    });

    if (allocation_started.wait_for(std::chrono::seconds(5)) !=
        std::future_status::ready) {
        blocking_strategy_ptr->AllowAllocation();
        put_thread.join();
        FAIL() << "timed out waiting for PutStart allocation";
    }

    using DeleteResult =
        tl::expected<std::optional<TenantQuotaSnapshot>, ErrorCode>;
    std::optional<DeleteResult> delete_result;
    std::thread delete_thread([&] {
        delete_result.emplace(service.DeleteTenantQuotaPolicy("tenant-a"));
    });

    ASSERT_TRUE(WaitForTenantQuotaPolicyMutexContention(service))
        << "DeleteTenantQuotaPolicy did not wait for zero-charge PutStart";

    blocking_strategy_ptr->AllowAllocation();
    put_thread.join();
    delete_thread.join();

    ASSERT_TRUE(put_result.has_value());
    ASSERT_TRUE(put_result->has_value()) << toString(put_result->error());
    ASSERT_TRUE(delete_result.has_value());
    ASSERT_FALSE(delete_result->has_value());
    EXPECT_EQ(delete_result->error(), ErrorCode::TENANT_NOT_EMPTY);

    auto snapshot = Snapshot(service, "tenant-a");
    EXPECT_EQ(snapshot.used_bytes, 0);
    EXPECT_EQ(snapshot.reserved_bytes, 0);
    EXPECT_EQ(snapshot.metadata_object_count, 1);
}
#endif

TEST_F(MasterServiceTenantQuotaTest,
       EffectiveQuotaUsesOnlyExplicitPolicyAndScalesProportionally) {
    MasterService service(MakeConfig({{"tenant-a", 200}, {"tenant-b", 400}}));
    MountSegment(service, /*size=*/300);

    EXPECT_EQ(Snapshot(service, "tenant-a").effective_quota_bytes, 100);
    EXPECT_EQ(Snapshot(service, "tenant-b").effective_quota_bytes, 200);
}

TEST_F(MasterServiceTenantQuotaTest,
       ConnectorPolicyReloadCreatesOrphanStateAndAllowsCleanup) {
    const std::string initial_policy =
        WritePolicyFile({{"tenant-a", 1000}, {"tenant-b", 1000}});
    auto config = MasterServiceConfig::builder()
                      .set_enable_multi_tenants(true)
                      .set_tenant_quota_connector_type("file")
                      .set_tenant_quota_connector_uri(initial_policy)
                      .build();
    MasterService service(config);
    UUID client_id = MountSegment(service);
    PutComplete(service, client_id, "orphan-key", "tenant-b", 100);

    {
        std::ofstream out(initial_policy);
        TenantQuotaPolicySnapshot replacement;
        replacement.tenant_quotas = {{"tenant-a", 1000}};
        out << FormatTenantQuotaPolicyYaml(replacement);
    }
    ReloadTenantQuotaPolicyFromStore(service);

    auto orphan = Snapshot(service, "tenant-b");
    EXPECT_FALSE(orphan.has_explicit_policy);
    EXPECT_EQ(orphan.requested_quota_bytes, 0);
    EXPECT_EQ(orphan.effective_quota_bytes, 0);
    EXPECT_TRUE(orphan.over_quota);

    EXPECT_TRUE(service.GetReplicaList("orphan-key", "tenant-b").has_value());
    auto write =
        service.PutStart(client_id, "new-key", "tenant-b", 1, MemoryConfig());
    ASSERT_FALSE(write.has_value());
    EXPECT_EQ(write.error(), ErrorCode::TENANT_NOT_REGISTERED);

    EXPECT_TRUE(service
                    .Remove("orphan-key", "tenant-b",
                            /*force=*/true)
                    .has_value());
}

}  // namespace mooncake::test
