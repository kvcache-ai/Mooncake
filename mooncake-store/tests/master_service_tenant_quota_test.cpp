#include "master_service.h"

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
    std::optional<tl::expected<void, ErrorCode>> add_result;
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
