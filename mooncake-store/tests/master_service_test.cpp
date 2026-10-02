#include "ha/snapshot/snapshot_constants.h"
#include "master_service.h"
#include "master_service/master_service_test_peer.h"
#include "rpc_service.h"

#include <glog/logging.h>
#include <gtest/gtest.h>
#include <ylt/struct_json/json_reader.h>

#include <algorithm>
#include <atomic>
#include <barrier>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <functional>
#include <map>
#include <memory>
#include <limits>
#include <optional>
#include <string>
#include <thread>
#include <utility>
#include <vector>
#include <unordered_set>

#include <unistd.h>

#include "common/zstd_util.h"
#include "serialize/serializer.h"
#include "tenant_quota_policy_store.h"
#include "types.h"
#include "master_service_test_fixture.h"

namespace mooncake::test {

class ScopedEnvVar {
   public:
    explicit ScopedEnvVar(const char* name) : name_(name) {
        Capture();
        ::unsetenv(name_.c_str());
    }

    ScopedEnvVar(const char* name, const char* value) : name_(name) {
        Capture();
        ::setenv(name_.c_str(), value, 1);
    }

    ~ScopedEnvVar() {
        if (previous_value_.has_value()) {
            ::setenv(name_.c_str(), previous_value_->c_str(), 1);
        } else {
            ::unsetenv(name_.c_str());
        }
    }

   private:
    void Capture() {
        const char* value = ::getenv(name_.c_str());
        if (value != nullptr) {
            previous_value_ = value;
        }
    }

    std::string name_;
    std::optional<std::string> previous_value_;
};

TEST(TenantScopedStorageKeyTest, RoundTripsAndParsesLegacyKeys) {
    const auto scoped =
        TenantId("tenant:with:colon").MakeScopedKey("path/key:with:colon");
    EXPECT_NE(scoped.find('\0'), std::string::npos);

    auto [tenant_id, key] = TenantId::ParseScopedKey(scoped);
    EXPECT_EQ(tenant_id.value(), "tenant:with:colon");
    EXPECT_EQ(key, "path/key:with:colon");

    auto [default_tenant, default_key] = TenantId::ParseScopedKey("raw_key");
    EXPECT_EQ(default_tenant.value(), TenantId::kDefaultValue);
    EXPECT_EQ(default_key, "raw_key");

    std::string legacy = "legacy_tenant";
    legacy.push_back('\0');
    legacy.append("legacy_key");
    auto [legacy_tenant, legacy_key] = TenantId::ParseScopedKey(legacy);
    EXPECT_EQ(legacy_tenant.value(), "legacy_tenant");
    EXPECT_EQ(legacy_key, "legacy_key");
}

std::string GenerateKeyForSegment(const UUID& client_id,
                                  const std::unique_ptr<MasterService>& service,
                                  const std::string& segment_name) {
    static std::atomic<uint64_t> counter(0);

    while (true) {
        std::string key = "key_" + std::to_string(counter.fetch_add(1));
        std::vector<Replica::Descriptor> replica_list;

        // Check if the key already exists.
        auto exist_result = service->ExistKey(key, TenantId::Default());
        if (exist_result.has_value() && exist_result.value()) {
            continue;  // Retry if the key already exists
        }

        // Attempt to put the key.
        auto put_result = service->PutStart(client_id, key, TenantId::Default(),
                                            {1024}, {.replica_num = 1});
        if (put_result.has_value()) {
            replica_list = std::move(put_result.value());
        }
        ErrorCode code =
            put_result.has_value() ? ErrorCode::OK : put_result.error();

        if (code == ErrorCode::OBJECT_ALREADY_EXISTS) {
            continue;  // Retry if the key already exists
        }
        if (code != ErrorCode::OK) {
            throw std::runtime_error("PutStart failed with code: " +
                                     std::to_string(static_cast<int>(code)));
        }
        auto put_end_result = service->PutEnd(
            client_id, key, TenantId::Default(), ReplicaType::MEMORY);
        if (!put_end_result.has_value()) {
            throw std::runtime_error("PutEnd failed");
        }
        if (replica_list[0]
                .get_memory_descriptor()
                .buffer_descriptor.transport_endpoint_ == segment_name) {
            return key;
        }
        // Clean up failed attempt
        auto remove_result = service->Remove(key, TenantId::Default());
        if (!remove_result.has_value()) {
            // Ignore cleanup failure
        }
    }
}

TEST_F(MasterServiceTest, MountUnmountSegmentWithCachelibAllocator) {
    // Create a MasterService instance for testing.
    auto service_config =
        MasterServiceConfig::builder()
            .set_memory_allocator(BufferAllocatorType::CACHELIB)
            .build();
    std::unique_ptr<MasterService> service_(new MasterService(service_config));
    auto segment = MakeSegment();
    UUID client_id = generate_uuid();
    const auto original_base = segment.base;
    const auto original_size = segment.size;

    // Test invalid parameters.
    // Invalid buffer address (0).
    segment.base = 0;
    segment.size = original_size;
    auto mount_result1 = service_->MountSegment(segment, client_id);
    EXPECT_FALSE(mount_result1.has_value());
    EXPECT_EQ(ErrorCode::INVALID_PARAMS, mount_result1.error());

    // Invalid segment size (0).
    segment.base = original_base;
    segment.size = 0;
    auto mount_result2 = service_->MountSegment(segment, client_id);
    EXPECT_FALSE(mount_result2.has_value());
    EXPECT_EQ(ErrorCode::INVALID_PARAMS, mount_result2.error());

    // Base is not aligned
    segment.base = original_base + 1;
    segment.size = original_size;
    auto mount_result3 = service_->MountSegment(segment, client_id);
    EXPECT_FALSE(mount_result3.has_value());
    EXPECT_EQ(ErrorCode::INVALID_PARAMS, mount_result3.error());

    // Size is not aligned
    segment.base = original_base;
    segment.size = original_size + 1;
    auto mount_result4 = service_->MountSegment(segment, client_id);
    EXPECT_FALSE(mount_result4.has_value());
    EXPECT_EQ(ErrorCode::INVALID_PARAMS, mount_result4.error());

    // Test normal mount operation.
    segment.base = original_base;
    segment.size = original_size;
    auto mount_result5 = service_->MountSegment(segment, client_id);
    EXPECT_TRUE(mount_result5.has_value());

    // Test mounting the same segment again (idempotent request should succeed).
    auto mount_result6 = service_->MountSegment(segment, client_id);
    EXPECT_TRUE(mount_result6.has_value());

    // Test unmounting the segment.
    auto unmount_result1 = service_->UnmountSegment(segment.id, client_id);
    EXPECT_TRUE(unmount_result1.has_value());

    // Test unmounting the same segment again (idempotent request should
    // succeed).
    auto unmount_result2 = service_->UnmountSegment(segment.id, client_id);
    EXPECT_TRUE(unmount_result2.has_value());

    // Test unmounting a non-existent segment (idempotent request should
    // succeed).
    UUID non_existent_id = generate_uuid();
    auto unmount_result3 = service_->UnmountSegment(non_existent_id, client_id);
    EXPECT_TRUE(unmount_result3.has_value());

    // Test remounting after unmount.
    auto mount_result7 = service_->MountSegment(segment, client_id);
    EXPECT_TRUE(mount_result7.has_value());
    auto unmount_result4 = service_->UnmountSegment(segment.id, client_id);
    EXPECT_TRUE(unmount_result4.has_value());
}

TEST_F(MasterServiceTest, MountUnmountSegmentWithOffsetAllocator) {
    // Create a MasterService instance for testing.
    auto service_config = MasterServiceConfig::builder()
                              .set_memory_allocator(BufferAllocatorType::OFFSET)
                              .build();
    std::unique_ptr<MasterService> service_(new MasterService(service_config));
    auto segment = MakeSegment();
    UUID client_id = generate_uuid();
    const auto original_base = segment.base;
    const auto original_size = segment.size;

    // Test invalid parameters.
    // Invalid buffer address (0).
    segment.base = 0;
    segment.size = original_size;
    auto mount_result1 = service_->MountSegment(segment, client_id);
    EXPECT_FALSE(mount_result1.has_value());
    EXPECT_EQ(ErrorCode::INVALID_PARAMS, mount_result1.error());

    // Invalid segment size (0).
    segment.base = original_base;
    segment.size = 0;
    auto mount_result2 = service_->MountSegment(segment, client_id);
    EXPECT_FALSE(mount_result2.has_value());
    EXPECT_EQ(ErrorCode::INVALID_PARAMS, mount_result2.error());

    // Test normal mount operation.
    segment.base = original_base;
    segment.size = original_size;
    auto mount_result5 = service_->MountSegment(segment, client_id);
    EXPECT_TRUE(mount_result5.has_value());

    // Test mounting the same segment again (idempotent request should succeed).
    auto mount_result6 = service_->MountSegment(segment, client_id);
    EXPECT_TRUE(mount_result6.has_value());

    // Test unmounting the segment.
    auto unmount_result1 = service_->UnmountSegment(segment.id, client_id);
    EXPECT_TRUE(unmount_result1.has_value());

    // Test unmounting the same segment again (idempotent request should
    // succeed).
    auto unmount_result2 = service_->UnmountSegment(segment.id, client_id);
    EXPECT_TRUE(unmount_result2.has_value());

    // Test unmounting a non-existent segment (idempotent request should
    // succeed).
    UUID non_existent_id = generate_uuid();
    auto unmount_result3 = service_->UnmountSegment(non_existent_id, client_id);
    EXPECT_TRUE(unmount_result3.has_value());

    // Test remounting after unmount.
    auto mount_result7 = service_->MountSegment(segment, client_id);
    EXPECT_TRUE(mount_result7.has_value());
    auto unmount_result4 = service_->UnmountSegment(segment.id, client_id);
    EXPECT_TRUE(unmount_result4.has_value());
}

TEST_F(MasterServiceTest, SoftPinZeroTtlSkipsSoftPinRegistration) {
    auto service_config = MasterServiceConfig::builder()
                              .set_default_kv_soft_pin_ttl(50)
                              .set_max_kv_soft_pin_ttl(100)
                              .build();
    std::unique_ptr<MasterService> service(new MasterService(service_config));
    [[maybe_unused]] const auto context = PrepareSimpleSegment(*service);
    const UUID client_id = generate_uuid();

    ReplicateConfig config;
    const int64_t baseline =
        MasterMetricManager::instance().get_soft_pin_key_count();
    config.soft_pin_action = SoftPinAction::ENABLE;
    config.soft_pin_ttl_ms = 0;
    ASSERT_TRUE(
        service
            ->PutStart(client_id, "zero_ttl", TenantId::Default(), 1024, config)
            .has_value());
    ASSERT_TRUE(service
                    ->PutEnd(client_id, "zero_ttl", TenantId::Default(),
                             ReplicaType::MEMORY)
                    .has_value());
    EXPECT_FALSE(GetSoftPinDeadline(*service, "zero_ttl").has_value());
    EXPECT_EQ(MasterMetricManager::instance().get_soft_pin_key_count(),
              baseline);
}

TEST_F(MasterServiceTest, SoftPinMasterConfigRejectsDefaultAboveMaximum) {
    auto invalid_config = MasterServiceConfig::builder()
                              .set_default_kv_soft_pin_ttl(101)
                              .set_max_kv_soft_pin_ttl(100)
                              .build();
    EXPECT_THROW(MasterService service(invalid_config), std::invalid_argument);
}

TEST_F(MasterServiceTest, SoftPinDeadlineCalculationSaturatesAtMaximum) {
    using Clock = std::chrono::system_clock;

    const auto normal_now = Clock::time_point(std::chrono::seconds(10));
    EXPECT_EQ(ComputeSoftPinDeadlineForTest(normal_now,
                                            std::chrono::milliseconds(25)),
              normal_now + std::chrono::milliseconds(25));
    EXPECT_EQ(ComputeSoftPinDeadlineForTest(normal_now,
                                            std::chrono::milliseconds::max()),
              Clock::time_point::max());

    const auto near_max =
        Clock::time_point::max() - std::chrono::milliseconds(5);
    EXPECT_EQ(
        ComputeSoftPinDeadlineForTest(near_max, std::chrono::milliseconds(10)),
        Clock::time_point::max());
}

#ifdef USE_NOF
TEST_F(MasterServiceTest, PutEndAllCompletesMemoryAndNoFReplicas) {
    std::unique_ptr<MasterService> service_(new MasterService());
    [[maybe_unused]] const auto mem_context = PrepareSimpleSegment(*service_);
    NoFSegment nof_segment = MakeNoFSegment();
    const UUID client_id = generate_uuid();
    ASSERT_TRUE(service_->MountNoFSegment(nof_segment, client_id).has_value());

    ReplicateConfig config;
    config.replica_num = 1;
    config.nof_replica_num = 1;
    auto put_start_result = service_->PutStart(
        client_id, "test_key_all", TenantId::Default(), 1024, config);
    ASSERT_TRUE(put_start_result.has_value());

    auto put_end_result = service_->PutEnd(
        client_id, "test_key_all", TenantId::Default(), ReplicaType::ALL);
    ASSERT_TRUE(put_end_result.has_value());

    auto get_replica_result =
        service_->GetReplicaList("test_key_all", TenantId::Default());
    ASSERT_TRUE(get_replica_result.has_value());

    bool has_complete_memory = false;
    bool has_complete_nof = false;
    for (const auto& replica : get_replica_result->replicas) {
        if (replica.is_memory_replica() &&
            replica.status == ReplicaStatus::COMPLETE) {
            has_complete_memory = true;
        }
        if (replica.is_nof_replica() &&
            replica.status == ReplicaStatus::COMPLETE) {
            has_complete_nof = true;
        }
    }
    EXPECT_TRUE(has_complete_memory);
    EXPECT_TRUE(has_complete_nof);
}

TEST_F(MasterServiceTest, PutEndMemoryDoesNotCompleteNoFReplica) {
    std::unique_ptr<MasterService> service_(new MasterService());
    [[maybe_unused]] const auto mem_context = PrepareSimpleSegment(*service_);
    NoFSegment nof_segment =
        MakeNoFSegment("test_nof_segment_2", "test_nof_segment_endpoint_2");
    const UUID client_id = generate_uuid();
    ASSERT_TRUE(service_->MountNoFSegment(nof_segment, client_id).has_value());

    ReplicateConfig config;
    config.replica_num = 1;
    config.nof_replica_num = 1;
    auto put_start_result = service_->PutStart(
        client_id, "test_key_split", TenantId::Default(), 1024, config);
    ASSERT_TRUE(put_start_result.has_value());

    auto put_end_result = service_->PutEnd(
        client_id, "test_key_split", TenantId::Default(), ReplicaType::MEMORY);
    ASSERT_TRUE(put_end_result.has_value());

    auto get_replica_result =
        service_->GetReplicaList("test_key_split", TenantId::Default());
    ASSERT_TRUE(get_replica_result.has_value());
    ASSERT_EQ(get_replica_result->replicas.size(), 1u);
    EXPECT_TRUE(get_replica_result->replicas[0].is_memory_replica());
    EXPECT_EQ(get_replica_result->replicas[0].status, ReplicaStatus::COMPLETE);

    auto put_revoke_result = service_->PutRevoke(
        client_id, "test_key_split", TenantId::Default(), ReplicaType::NOF_SSD);
    ASSERT_TRUE(put_revoke_result.has_value());

    auto final_replica_result =
        service_->GetReplicaList("test_key_split", TenantId::Default());
    ASSERT_TRUE(final_replica_result.has_value());
    ASSERT_EQ(final_replica_result->replicas.size(), 1u);
    EXPECT_TRUE(final_replica_result->replicas[0].is_memory_replica());
    EXPECT_EQ(final_replica_result->replicas[0].status,
              ReplicaStatus::COMPLETE);
}

TEST_F(MasterServiceTest, PartialRevokePreservesPendingSoftPin) {
    std::unique_ptr<MasterService> service(new MasterService());
    [[maybe_unused]] const auto mem_context = PrepareSimpleSegment(*service);
    NoFSegment nof_segment =
        MakeNoFSegment("soft_pin_nof", "soft_pin_nof_endpoint");
    const UUID client_id = generate_uuid();
    ASSERT_TRUE(service->MountNoFSegment(nof_segment, client_id).has_value());

    const int64_t baseline =
        MasterMetricManager::instance().get_soft_pin_key_count();
    ReplicateConfig config;
    config.replica_num = 1;
    config.nof_replica_num = 1;
    config.soft_pin_action = SoftPinAction::ENABLE;
    ASSERT_TRUE(service
                    ->PutStart(client_id, "partial_revoke_soft_pin",
                               TenantId::Default(), 1024, config)
                    .has_value());
    ASSERT_TRUE(service
                    ->PutRevoke(client_id, "partial_revoke_soft_pin",
                                TenantId::Default(), ReplicaType::MEMORY)
                    .has_value());
    EXPECT_EQ(MasterMetricManager::instance().get_soft_pin_key_count(),
              baseline);

    ASSERT_TRUE(service
                    ->PutEnd(client_id, "partial_revoke_soft_pin",
                             TenantId::Default(), ReplicaType::NOF_SSD)
                    .has_value());
    EXPECT_EQ(MasterMetricManager::instance().get_soft_pin_key_count(),
              baseline + 1);
    EXPECT_TRUE(
        GetSoftPinDeadline(*service, "partial_revoke_soft_pin").has_value());
}

TEST_F(MasterServiceTest, LaterReplicaEndDoesNotRefreshSoftPin) {
    std::unique_ptr<MasterService> service(new MasterService());
    [[maybe_unused]] const auto mem_context = PrepareSimpleSegment(*service);
    NoFSegment nof_segment =
        MakeNoFSegment("soft_pin_later_end", "soft_pin_later_end_endpoint");
    const UUID client_id = generate_uuid();
    ASSERT_TRUE(service->MountNoFSegment(nof_segment, client_id).has_value());

    ReplicateConfig config;
    config.replica_num = 1;
    config.nof_replica_num = 1;
    config.soft_pin_action = SoftPinAction::ENABLE;
    ASSERT_TRUE(service
                    ->PutStart(client_id, "later_end_soft_pin",
                               TenantId::Default(), 1024, config)
                    .has_value());
    ASSERT_TRUE(service
                    ->PutEnd(client_id, "later_end_soft_pin",
                             TenantId::Default(), ReplicaType::MEMORY)
                    .has_value());
    const auto first_deadline =
        GetSoftPinDeadline(*service, "later_end_soft_pin");
    ASSERT_TRUE(first_deadline.has_value());

    ASSERT_TRUE(service
                    ->PutEnd(client_id, "later_end_soft_pin",
                             TenantId::Default(), ReplicaType::NOF_SSD)
                    .has_value());
    EXPECT_EQ(GetSoftPinDeadline(*service, "later_end_soft_pin"),
              first_deadline);
}

TEST_F(MasterServiceTest, PutStartOnePlusOneAllowsSingleAllocatedReplica) {
    std::unique_ptr<MasterService> service_(new MasterService());
    [[maybe_unused]] const auto mem_context = PrepareSimpleSegment(*service_);
    const UUID client_id = generate_uuid();

    ReplicateConfig config;
    config.replica_num = 1;
    config.nof_replica_num = 1;
    auto put_start_result = service_->PutStart(
        client_id, "test_key_one_plus_one", TenantId::Default(), 1024, config);
    ASSERT_TRUE(put_start_result.has_value());
    ASSERT_EQ(put_start_result->size(), 1u);
    EXPECT_TRUE(put_start_result->front().is_memory_replica());
}
#endif

TEST_F(MasterServiceTest, DfsPutEndAllAndUpsertTopologyAreAtomic) {
    const auto dfs_root = (std::filesystem::temp_directory_path() /
                           ("master_dfs_sync_" + std::to_string(::getpid())))
                              .string();
    std::filesystem::create_directories(dfs_root);
    ScopedEnvVar enable_dfs("MOONCAKE_ENABLE_DFS", "1");
    ScopedEnvVar fs_adapter("MOONCAKE_DFS_FS_ADAPTER", "posix");
    ScopedEnvVar root_dir("MOONCAKE_DFS_ROOT_DIR", dfs_root.c_str());
    ScopedEnvVar shard_count("MOONCAKE_DFS_SHARD_COUNT", "1");
    ScopedEnvVar shard_capacity("MOONCAKE_DFS_SHARD_CAPACITY", "1048576");
    ScopedEnvVar alignment("MOONCAKE_DFS_ALIGNMENT", "4096");
    ScopedEnvVar eviction("MOONCAKE_DFS_EVICTION_ENABLED", "0");
    ScopedEnvVar deferred_free("MOONCAKE_DFS_DEFERRED_FREE_SECONDS", "0");
    ScopedEnvVar single_tenant("MOONCAKE_DFS_SINGLE_TENANT", "true");

    {
        MasterService service;
        const auto context = PrepareSimpleSegment(service);
        ReplicateConfig config;
        config.replica_num = 1;
        config.dfs_replica_num = 1;

        auto start = service.PutStart(context.client_id, "dfs_atomic",
                                      TenantId::Default(), 4096, config);
        ASSERT_TRUE(start.has_value());
        ASSERT_EQ(start->size(), 2);
        ASSERT_TRUE(service
                        .PutEnd(context.client_id, "dfs_atomic",
                                TenantId::Default(), ReplicaType::ALL)
                        .has_value());

        auto query = service.GetReplicaList("dfs_atomic", TenantId::Default());
        ASSERT_TRUE(query.has_value());
        ASSERT_EQ(query->replicas.size(), 2);
        for (const auto& replica : query->replicas) {
            EXPECT_EQ(replica.status, ReplicaStatus::COMPLETE);
        }

        ReplicateConfig mismatched_config;
        auto leased_upsert = service.UpsertStart(
            context.client_id, "dfs_atomic", TenantId::Default(), 4096, config);
        ASSERT_FALSE(leased_upsert.has_value());
        EXPECT_EQ(leased_upsert.error(), ErrorCode::OBJECT_HAS_LEASE);

        mismatched_config.replica_num = 1;
        auto upsert =
            service.UpsertStart(context.client_id, "dfs_atomic",
                                TenantId::Default(), 4096, mismatched_config);
        ASSERT_FALSE(upsert.has_value());
        EXPECT_EQ(upsert.error(), ErrorCode::INVALID_PARAMS);

        query = service.GetReplicaList("dfs_atomic", TenantId::Default());
        ASSERT_TRUE(query.has_value());
        ASSERT_EQ(query->replicas.size(), 2);
        for (const auto& replica : query->replicas) {
            EXPECT_EQ(replica.status, ReplicaStatus::COMPLETE);
        }

        auto revoke_start = service.PutStart(context.client_id, "dfs_revoke",
                                             TenantId::Default(), 4096, config);
        ASSERT_TRUE(revoke_start.has_value());
        ASSERT_TRUE(service
                        .PutRevoke(context.client_id, "dfs_revoke",
                                   TenantId::Default(), ReplicaType::ALL)
                        .has_value());
        auto revoked =
            service.GetReplicaList("dfs_revoke", TenantId::Default());
        ASSERT_FALSE(revoked.has_value());
        EXPECT_EQ(revoked.error(), ErrorCode::OBJECT_NOT_FOUND);
    }

    {
        MasterService service(MakeStrictTenantConfig({"default"}));
        const auto context = PrepareSimpleSegment(service);
        ReplicateConfig dfs_config;
        dfs_config.replica_num = 1;
        dfs_config.dfs_replica_num = 1;

        auto failed = service.PutStart(context.client_id, "dfs_quota_failure",
                                       TenantId::Default(),
                                       kStrictTenantQuotaBytes, dfs_config);
        ASSERT_FALSE(failed.has_value());
        EXPECT_EQ(failed.error(), ErrorCode::NO_AVAILABLE_HANDLE);

        ReplicateConfig memory_config;
        memory_config.replica_num = 1;
        auto retry = service.PutStart(
            context.client_id, "quota_after_dfs_failure", TenantId::Default(),
            kStrictTenantQuotaBytes, memory_config);
        ASSERT_TRUE(retry.has_value()) << toString(retry.error());
        ASSERT_TRUE(service
                        .PutRevoke(context.client_id, "quota_after_dfs_failure",
                                   TenantId::Default(), ReplicaType::ALL)
                        .has_value());
    }
    std::error_code ec;
    std::filesystem::remove_all(dfs_root, ec);
}

TEST_F(MasterServiceTest, DfsBucketMemoryAllocationFailurePreservesBuckets) {
    const auto dfs_root =
        (std::filesystem::temp_directory_path() /
         ("master_dfs_bucket_memory_failure_" + std::to_string(::getpid())))
            .string();
    std::filesystem::create_directories(dfs_root);
    ScopedEnvVar enable_dfs("MOONCAKE_ENABLE_DFS", "1");
    ScopedEnvVar fs_adapter("MOONCAKE_DFS_FS_ADAPTER", "posix");
    ScopedEnvVar root_dir("MOONCAKE_DFS_ROOT_DIR", dfs_root.c_str());
    ScopedEnvVar allocator("MOONCAKE_DFS_ALLOCATOR", "bucket");
    ScopedEnvVar bucket_capacity("MOONCAKE_DFS_BUCKET_CAPACITY", "8192");
    ScopedEnvVar max_bucket_count("MOONCAKE_DFS_MAX_BUCKET_COUNT", "4");
    ScopedEnvVar alignment("MOONCAKE_DFS_ALIGNMENT", "4096");
    ScopedEnvVar eviction("MOONCAKE_DFS_EVICTION_ENABLED", "1");
    ScopedEnvVar high_watermark("MOONCAKE_DFS_EVICTION_HIGH_WATERMARK", "1.0");
    ScopedEnvVar low_watermark("MOONCAKE_DFS_EVICTION_LOW_WATERMARK", "0.9");
    ScopedEnvVar deferred_free("MOONCAKE_DFS_DEFERRED_FREE_SECONDS", "0");
    ScopedEnvVar single_tenant("MOONCAKE_DFS_SINGLE_TENANT", "true");

    {
        MasterService service;
        const auto context = PrepareSimpleSegment(service, "small_memory",
                                                  kDefaultSegmentBase, 8192);
        ReplicateConfig config;
        config.replica_num = 1;
        config.dfs_replica_num = 1;

        for (const char* key : {"memory_full_a", "memory_full_b"}) {
            auto start = service.PutStart(context.client_id, key,
                                          TenantId::Default(), 4096, config);
            ASSERT_TRUE(start.has_value()) << key << ": " << start.error();
            ASSERT_TRUE(service
                            .PutEnd(context.client_id, key, TenantId::Default(),
                                    ReplicaType::ALL)
                            .has_value());
        }

        ASSERT_TRUE(
            service.UnmountSegment(context.segment_id, context.client_id)
                .has_value());

        auto failed = service.PutStart(context.client_id, "memory_full_c",
                                       TenantId::Default(), 4096, config);
        ASSERT_FALSE(failed.has_value());
        EXPECT_EQ(failed.error(), ErrorCode::NO_AVAILABLE_HANDLE);

        for (const char* key : {"memory_full_a", "memory_full_b"}) {
            auto query = service.GetReplicaList(key, TenantId::Default());
            ASSERT_TRUE(query.has_value()) << key;
            ASSERT_EQ(query->replicas.size(), 1u);
            EXPECT_TRUE(query->replicas.front().is_dfs_replica());
        }
    }

    std::error_code ec;
    std::filesystem::remove_all(dfs_root, ec);
}

TEST_F(MasterServiceTest, DfsBucketUpsertRestoresEvictedReplica) {
    const auto dfs_root =
        (std::filesystem::temp_directory_path() /
         ("master_dfs_bucket_upsert_restore_" + std::to_string(::getpid())))
            .string();
    std::filesystem::create_directories(dfs_root);
    ScopedEnvVar enable_dfs("MOONCAKE_ENABLE_DFS", "1");
    ScopedEnvVar fs_adapter("MOONCAKE_DFS_FS_ADAPTER", "posix");
    ScopedEnvVar root_dir("MOONCAKE_DFS_ROOT_DIR", dfs_root.c_str());
    ScopedEnvVar allocator("MOONCAKE_DFS_ALLOCATOR", "bucket");
    ScopedEnvVar bucket_capacity("MOONCAKE_DFS_BUCKET_CAPACITY", "8192");
    ScopedEnvVar max_bucket_count("MOONCAKE_DFS_MAX_BUCKET_COUNT", "2");
    ScopedEnvVar alignment("MOONCAKE_DFS_ALIGNMENT", "4096");
    ScopedEnvVar eviction("MOONCAKE_DFS_EVICTION_ENABLED", "1");
    ScopedEnvVar high_watermark("MOONCAKE_DFS_EVICTION_HIGH_WATERMARK", "0.7");
    ScopedEnvVar low_watermark("MOONCAKE_DFS_EVICTION_LOW_WATERMARK", "0.5");
    ScopedEnvVar eviction_interval("MOONCAKE_DFS_EVICTION_CHECK_INTERVAL",
                                   "60");
    ScopedEnvVar deferred_free("MOONCAKE_DFS_DEFERRED_FREE_SECONDS", "0");
    ScopedEnvVar single_tenant("MOONCAKE_DFS_SINGLE_TENANT", "true");

    {
        MasterService service;
        const auto context = PrepareSimpleSegment(service);
        ReplicateConfig config;
        config.replica_num = 1;
        config.dfs_replica_num = 1;

        std::optional<uint64_t> old_memory_address;
        std::optional<int> old_bucket_id;
        for (const std::string key :
             {"upsert_restore", "bucket_0_peer", "bucket_1_peer"}) {
            auto start = service.PutStart(context.client_id, key,
                                          TenantId::Default(), 4096, config);
            ASSERT_TRUE(start.has_value()) << key << ": " << start.error();
            if (key == "upsert_restore") {
                for (const auto& descriptor : *start) {
                    if (descriptor.is_memory_replica()) {
                        old_memory_address =
                            descriptor.get_memory_descriptor()
                                .buffer_descriptor.buffer_address_;
                    } else if (descriptor.is_dfs_replica()) {
                        old_bucket_id =
                            descriptor.get_dfs_descriptor().shard_idx;
                    }
                }
            }
            ASSERT_TRUE(service
                            .PutEnd(context.client_id, key, TenantId::Default(),
                                    ReplicaType::ALL)
                            .has_value());
        }
        ASSERT_TRUE(old_memory_address.has_value());
        ASSERT_TRUE(old_bucket_id.has_value());

        MasterServiceTestPeer(service).RunDfsEvictionForTesting();
        auto after_eviction =
            service.GetReplicaList("upsert_restore", TenantId::Default());
        ASSERT_TRUE(after_eviction.has_value());
        ASSERT_EQ(after_eviction->replicas.size(), 1u);
        EXPECT_TRUE(after_eviction->replicas.front().is_memory_replica());

        auto mismatched_config = config;
        mismatched_config.replica_num = 2;
        auto mismatched =
            service.UpsertStart(context.client_id, "upsert_restore",
                                TenantId::Default(), 4096, mismatched_config);
        ASSERT_FALSE(mismatched.has_value());
        EXPECT_EQ(mismatched.error(), ErrorCode::INVALID_PARAMS);

        auto restored = service.UpsertStart(context.client_id, "upsert_restore",
                                            TenantId::Default(), 4096, config);
        ASSERT_TRUE(restored.has_value()) << restored.error();
        ASSERT_EQ(restored->size(), 2u);
        bool restored_memory = false;
        bool restored_dfs = false;
        for (const auto& descriptor : *restored) {
            if (descriptor.is_memory_replica()) {
                restored_memory = true;
                EXPECT_NE(descriptor.get_memory_descriptor()
                              .buffer_descriptor.buffer_address_,
                          *old_memory_address);
            } else if (descriptor.is_dfs_replica()) {
                restored_dfs = true;
                EXPECT_NE(descriptor.get_dfs_descriptor().shard_idx,
                          *old_bucket_id);
            }
        }
        EXPECT_TRUE(restored_memory);
        EXPECT_TRUE(restored_dfs);
        ASSERT_TRUE(service
                        .UpsertEnd(context.client_id, "upsert_restore",
                                   TenantId::Default(), ReplicaType::ALL)
                        .has_value());

        auto complete =
            service.GetReplicaList("upsert_restore", TenantId::Default());
        ASSERT_TRUE(complete.has_value());
        EXPECT_EQ(complete->replicas.size(), 2u);
        EXPECT_EQ(
            std::count_if(complete->replicas.begin(), complete->replicas.end(),
                          [](const Replica::Descriptor& descriptor) {
                              return descriptor.is_dfs_replica();
                          }),
            1);
    }

    std::error_code ec;
    std::filesystem::remove_all(dfs_root, ec);
}

TEST_F(MasterServiceTest,
       DfsBucketConcurrentPutAndUpsertShareSingleRecoveryBucket) {
    const auto dfs_root =
        (std::filesystem::temp_directory_path() /
         ("master_dfs_bucket_single_flight_" + std::to_string(::getpid())))
            .string();
    std::filesystem::create_directories(dfs_root);
    ScopedEnvVar enable_dfs("MOONCAKE_ENABLE_DFS", "1");
    ScopedEnvVar fs_adapter("MOONCAKE_DFS_FS_ADAPTER", "posix");
    ScopedEnvVar root_dir("MOONCAKE_DFS_ROOT_DIR", dfs_root.c_str());
    ScopedEnvVar allocator("MOONCAKE_DFS_ALLOCATOR", "bucket");
    ScopedEnvVar bucket_capacity("MOONCAKE_DFS_BUCKET_CAPACITY", "32768");
    ScopedEnvVar max_bucket_count("MOONCAKE_DFS_MAX_BUCKET_COUNT", "4");
    ScopedEnvVar alignment("MOONCAKE_DFS_ALIGNMENT", "4096");
    ScopedEnvVar eviction("MOONCAKE_DFS_EVICTION_ENABLED", "1");
    ScopedEnvVar high_watermark("MOONCAKE_DFS_EVICTION_HIGH_WATERMARK", "1.0");
    ScopedEnvVar low_watermark("MOONCAKE_DFS_EVICTION_LOW_WATERMARK", "0.9");
    ScopedEnvVar eviction_interval("MOONCAKE_DFS_EVICTION_CHECK_INTERVAL",
                                   "60");
    ScopedEnvVar deferred_free("MOONCAKE_DFS_DEFERRED_FREE_SECONDS", "0");
    ScopedEnvVar single_tenant("MOONCAKE_DFS_SINGLE_TENANT", "true");

    {
        MasterService service;
        const auto context = PrepareSimpleSegment(service);
        ReplicateConfig config;
        config.replica_num = 1;
        config.dfs_replica_num = 1;

        constexpr size_t kObjectSize = 4096;
        constexpr size_t kConcurrentWrites = 8;
        constexpr size_t kOldKeyCount = 32;
        std::vector<std::string> old_keys;
        std::vector<std::string> new_keys;
        for (size_t i = 0; old_keys.size() < kOldKeyCount ||
                           new_keys.size() < kConcurrentWrites;
             ++i) {
            const std::string key = "single_flight_key_" + std::to_string(i);
            if (old_keys.size() < kOldKeyCount) {
                old_keys.push_back(key);
            } else {
                new_keys.push_back(key);
            }
        }

        for (const auto& key : old_keys) {
            auto start =
                service.PutStart(context.client_id, key, TenantId::Default(),
                                 kObjectSize, config);
            ASSERT_TRUE(start.has_value()) << key << ": " << start.error();
            ASSERT_TRUE(service
                            .PutEnd(context.client_id, key, TenantId::Default(),
                                    ReplicaType::ALL)
                            .has_value());
        }

        // Hold one seeded object's entry open: the bucket that holds it cannot
        // be validated while this lock is held, so the writers below meet a
        // full allocator the way a slow eviction validation leaves it.
        const auto blocked_entry = MasterServiceTestPeer::FindObject(
            service, MasterServiceTestPeer::ObjectIdentity{TenantId::Default(),
                                                           old_keys.front()});
        ASSERT_NE(blocked_entry, nullptr);
        std::atomic<bool> release_blocked_entry{false};
        std::thread blocker([&] {
            blocked_entry->WithExclusiveAccess([&](ObjectMetadata&,
                                                   ObjectEntry::State&) {
                while (!release_blocked_entry.load()) {
                    std::this_thread::sleep_for(std::chrono::milliseconds(1));
                }
            });
        });
        std::barrier start_barrier(kConcurrentWrites + 1);
        std::vector<int> errors(kConcurrentWrites,
                                static_cast<int>(ErrorCode::INTERNAL_ERROR));
        std::vector<int> bucket_ids(kConcurrentWrites, -1);
        std::vector<std::thread> writers;
        writers.reserve(kConcurrentWrites);
        for (size_t i = 0; i < kConcurrentWrites; ++i) {
            writers.emplace_back([&, i] {
                start_barrier.arrive_and_wait();
                auto result =
                    i % 2 == 0
                        ? service.PutStart(context.client_id, new_keys[i],
                                           TenantId::Default(), kObjectSize,
                                           config)
                        : service.UpsertStart(context.client_id, new_keys[i],
                                              TenantId::Default(), kObjectSize,
                                              config);
                if (!result) {
                    errors[i] = static_cast<int>(result.error());
                    return;
                }
                errors[i] = static_cast<int>(ErrorCode::OK);
                for (const auto& descriptor : *result) {
                    if (descriptor.is_dfs_replica()) {
                        bucket_ids[i] =
                            descriptor.get_dfs_descriptor().shard_idx;
                    }
                }
            });
        }

        start_barrier.arrive_and_wait();
        // Keep eviction validation blocked long enough for all writers to
        // encounter the full allocator. Without single-flight recovery they
        // freeze different LRU buckets while waiting on this entry.
        std::this_thread::sleep_for(std::chrono::milliseconds(250));
        release_blocked_entry.store(true);
        blocker.join();
        for (auto& writer : writers) writer.join();

        for (size_t i = 0; i < kConcurrentWrites; ++i) {
            EXPECT_EQ(errors[i], static_cast<int>(ErrorCode::OK))
                << new_keys[i];
            EXPECT_GE(bucket_ids[i], 0) << new_keys[i];
            EXPECT_EQ(bucket_ids[i], bucket_ids.front()) << new_keys[i];
        }

        size_t old_dfs_replicas = 0;
        for (const auto& key : old_keys) {
            auto query = service.GetReplicaList(key, TenantId::Default());
            ASSERT_TRUE(query.has_value()) << key;
            old_dfs_replicas +=
                std::count_if(query->replicas.begin(), query->replicas.end(),
                              [](const Replica::Descriptor& descriptor) {
                                  return descriptor.is_dfs_replica();
                              });
        }
        EXPECT_EQ(old_dfs_replicas, kOldKeyCount - kConcurrentWrites);
    }

    std::error_code ec;
    std::filesystem::remove_all(dfs_root, ec);
}

TEST_F(MasterServiceTest, LeasedUpsertAllocationFailurePreservesObject) {
    MasterServiceConfig service_config;
    service_config.memory_allocator = BufferAllocatorType::OFFSET;
    service_config.default_kv_lease_ttl = 10 * 1000;
    MasterService service(service_config);

    constexpr size_t kSegmentSize = 1024 * 1024;
    const auto context = PrepareSimpleSegment(
        service, "leased_upsert_segment", kDefaultSegmentBase, kSegmentSize);
    ReplicateConfig config;
    config.replica_num = 1;

    const std::string key = "leased_upsert_allocation_failure";
    auto initial = service.PutStart(context.client_id, key, TenantId::Default(),
                                    kSegmentSize, config);
    ASSERT_TRUE(initial.has_value()) << toString(initial.error());
    ASSERT_TRUE(service
                    .PutEnd(context.client_id, key, TenantId::Default(),
                            ReplicaType::MEMORY)
                    .has_value());

    // Retain a read lease so UpsertStart must allocate a replacement instead
    // of reusing the old buffer.
    auto snapshot = service.GetReplicaList(key, TenantId::Default());
    ASSERT_TRUE(snapshot.has_value());

    auto failed = service.UpsertStart(
        context.client_id, key, TenantId::Default(), kSegmentSize, config);
    ASSERT_FALSE(failed.has_value());
    EXPECT_EQ(failed.error(), ErrorCode::NO_AVAILABLE_HANDLE);

    auto still_readable = service.GetReplicaList(key, TenantId::Default());
    ASSERT_TRUE(still_readable.has_value());
    ASSERT_EQ(still_readable->replicas.size(), snapshot->replicas.size());
    EXPECT_EQ(still_readable->replicas[0]
                  .get_memory_descriptor()
                  .buffer_descriptor.buffer_address_,
              snapshot->replicas[0]
                  .get_memory_descriptor()
                  .buffer_descriptor.buffer_address_);
}

// DFS replicas live in their own variant branch, so is_disk_replica() does not
// match them. KV subscribers still expect one logical tier per storage class,
// which is what these assertions pin down.
TEST_F(MasterServiceTest, DfsReplicaNormalizesToDiskMediumForKvEvents) {
    const auto dfs_root =
        (std::filesystem::temp_directory_path() /
         ("master_dfs_kv_media_" + std::to_string(::getpid())))
            .string();
    std::filesystem::create_directories(dfs_root);
    ScopedEnvVar enable_dfs("MOONCAKE_ENABLE_DFS", "1");
    ScopedEnvVar fs_adapter("MOONCAKE_DFS_FS_ADAPTER", "posix");
    ScopedEnvVar root_dir("MOONCAKE_DFS_ROOT_DIR", dfs_root.c_str());
    ScopedEnvVar shard_count("MOONCAKE_DFS_SHARD_COUNT", "1");
    ScopedEnvVar shard_capacity("MOONCAKE_DFS_SHARD_CAPACITY", "1048576");
    ScopedEnvVar alignment("MOONCAKE_DFS_ALIGNMENT", "4096");
    ScopedEnvVar eviction("MOONCAKE_DFS_EVICTION_ENABLED", "0");
    ScopedEnvVar deferred_free("MOONCAKE_DFS_DEFERRED_FREE_SECONDS", "0");
    ScopedEnvVar single_tenant("MOONCAKE_DFS_SINGLE_TENANT", "true");

    {
        MasterService service;
        const auto context = PrepareSimpleSegment(service);
        ReplicateConfig config;
        config.replica_num = 1;
        config.dfs_replica_num = 1;

        auto start = service.PutStart(context.client_id, "dfs_kv_media",
                                      TenantId::Default(), 4096, config);
        ASSERT_TRUE(start.has_value());
        ASSERT_EQ(start->size(), 2);
        ASSERT_TRUE(service
                        .PutEnd(context.client_id, "dfs_kv_media",
                                TenantId::Default(), ReplicaType::ALL)
                        .has_value());

        // Memory + DFS collapse to exactly the two logical tiers, sorted the
        // same way the publisher sorts them.
        auto media = KvMediaForKey(service, "dfs_kv_media");
        ASSERT_TRUE(media.has_value());
        EXPECT_EQ(std::vector<std::string>({"cpu", "disk"}), *media);

        auto removal_media = KvRemovalMediaForKey(service, "dfs_kv_media");
        ASSERT_TRUE(removal_media.has_value());
        EXPECT_EQ(std::vector<std::string>({"cpu", "disk"}), *removal_media);
    }

    std::error_code ec;
    std::filesystem::remove_all(dfs_root, ec);
}

// A removal path can run while a replica is still PROCESSING. That medium was
// never announced as available, but the removal snapshot must still name it:
// otherwise a tier that a subscriber may have seen is never retracted. The
// contrast with KvMediaForKey, which counts only completed replicas, is the
// whole point of having two helpers.
TEST_F(MasterServiceTest, ProcessingReplicaMediumAppearsInRemovalSnapshot) {
    MasterService service;
    const auto context = PrepareSimpleSegment(service);
    ReplicateConfig config;
    config.replica_num = 1;

    // PutStart without PutEnd leaves the replica PROCESSING.
    auto start = service.PutStart(context.client_id, "processing_kv_media",
                                  TenantId::Default(), 4096, config);
    ASSERT_TRUE(start.has_value());

    auto media = KvMediaForKey(service, "processing_kv_media");
    ASSERT_TRUE(media.has_value());
    EXPECT_TRUE(media->empty()) << "a PROCESSING replica is not yet available "
                                   "and must not be announced";

    auto removal_media = KvRemovalMediaForKey(service, "processing_kv_media");
    ASSERT_TRUE(removal_media.has_value());
    EXPECT_EQ(std::vector<std::string>({"cpu"}), *removal_media);
}

// A RemoveAll whose oplog slot reservation fails must leave the object in
// place. That is the state in which announcing `cleared` would be a lie, so the
// skip has to be recorded rather than treated as a successful wipe.
TEST_F(MasterServiceTest, RemoveAllKeepsObjectWhenOpLogReservationFails) {
    // enable_oplog_ needs HA + oplog + the etcd backend, but the ordered oplog
    // writer is only installed by a separate init that needs a live etcd. So
    // this config reaches ReserveBatchOpLogSlot with a null writer, which is
    // exactly the reservation-failure branch.
    auto config = MasterServiceConfig::builder()
                      .set_enable_ha(true)
                      .set_enable_oplog(true)
                      .set_ha_backend_type("etcd")
                      .build();
    auto service = std::make_unique<MasterService>(config);
    const auto context = PrepareSimpleSegment(*service);

    ReplicateConfig replicate_config;
    replicate_config.replica_num = 1;
    ASSERT_TRUE(service
                    ->PutStart(context.client_id, "reservation_fail_key",
                               TenantId::Default(), 4096, replicate_config)
                    .has_value());
    ASSERT_TRUE(service
                    ->PutEnd(context.client_id, "reservation_fail_key",
                             TenantId::Default(), ReplicaType::ALL)
                    .has_value());

    // force=true clears the lease gate, so the reservation is the only thing
    // left that can stop the removal.
    EXPECT_EQ(0, service->RemoveAll(TenantId::Default(), true));

    auto exists =
        service->ExistKey("reservation_fail_key", TenantId::Default());
    ASSERT_TRUE(exists.has_value());
    EXPECT_TRUE(exists.value())
        << "the object survived, so the tenant was never emptied";
}

// RemoveAll releases one tenant route at a time, so a commit can land in a
// tenant the scan already finished. Publishing `cleared` then orders it after
// that commit's `stored`, telling subscribers to drop a live object. The hook
// parks the scan there so the commit is pinned into that window.
TEST_F(MasterServiceTest, ConcurrentCommitDuringScanSuppressesClear) {
    MasterService service;
    MasterServiceTestPeer(service).SetKvTenantEpochTrackingForTesting(true);
    const auto context = PrepareSimpleSegment(service);
    ReplicateConfig config;
    config.replica_num = 1;

    ASSERT_TRUE(service
                    .PutStart(context.client_id, "victim_key",
                              TenantId::Default(), 1024, config)
                    .has_value());
    ASSERT_TRUE(service
                    .PutEnd(context.client_id, "victim_key",
                            TenantId::Default(), ReplicaType::ALL)
                    .has_value());

    bool committed = false;
    MasterServiceTestPeer(service).SetRemoveAllTenantHookForTesting(
        [&](size_t) {
            // Commit exactly once, immediately after the scan releases the
            // tenant route the new key belongs to, so the scan can never
            // observe it.
            if (committed) {
                return;
            }
            committed = true;
            ASSERT_TRUE(service
                            .PutStart(context.client_id, "racer_key",
                                      TenantId::Default(), 1024, config)
                            .has_value());
            ASSERT_TRUE(service
                            .PutEnd(context.client_id, "racer_key",
                                    TenantId::Default(), ReplicaType::ALL)
                            .has_value());
        });

    service.RemoveAll(true);
    MasterServiceTestPeer(service).SetRemoveAllTenantHookForTesting(nullptr);

    ASSERT_TRUE(committed) << "the hook never fired, so nothing was raced";
    auto exists = service.ExistKey("racer_key", TenantId::Default());
    ASSERT_TRUE(exists.has_value());
    EXPECT_TRUE(exists.value()) << "the raced commit must still be live";

    EXPECT_EQ(0u,
              MasterServiceTestPeer(service).GetKvClearedPublishedForTesting())
        << "a clear here would retract racer_key, which was just announced";
    EXPECT_EQ(
        1u, MasterServiceTestPeer(service).GetKvClearedSuppressedForTesting());
}

// The mirror image: with no concurrent commit the epoch is unchanged, so the
// clear must still go out. Without this the fix could pass by never publishing.
TEST_F(MasterServiceTest, UncontendedScanStillPublishesClear) {
    MasterService service;
    MasterServiceTestPeer(service).SetKvTenantEpochTrackingForTesting(true);
    const auto context = PrepareSimpleSegment(service);
    ReplicateConfig config;
    config.replica_num = 1;

    ASSERT_TRUE(service
                    .PutStart(context.client_id, "lonely_key",
                              TenantId::Default(), 1024, config)
                    .has_value());
    ASSERT_TRUE(service
                    .PutEnd(context.client_id, "lonely_key",
                            TenantId::Default(), ReplicaType::ALL)
                    .has_value());

    service.RemoveAll(true);

    EXPECT_EQ(1u,
              MasterServiceTestPeer(service).GetKvClearedPublishedForTesting());
    EXPECT_EQ(
        0u, MasterServiceTestPeer(service).GetKvClearedSuppressedForTesting());
}

// The tenant-scoped overload reads the epoch before its scan instead of at
// first sight of an object, so it needs its own coverage of the same ordering
// rule.
TEST_F(MasterServiceTest, TenantScopedRemoveAllSuppressesClearOnRace) {
    MasterService service;
    MasterServiceTestPeer(service).SetKvTenantEpochTrackingForTesting(true);
    const auto context = PrepareSimpleSegment(service);
    ReplicateConfig config;
    config.replica_num = 1;

    ASSERT_TRUE(service
                    .PutStart(context.client_id, "scoped_victim",
                              TenantId::Default(), 1024, config)
                    .has_value());
    ASSERT_TRUE(service
                    .PutEnd(context.client_id, "scoped_victim",
                            TenantId::Default(), ReplicaType::ALL)
                    .has_value());

    bool committed = false;
    MasterServiceTestPeer(service).SetRemoveAllTenantHookForTesting(
        [&](size_t) {
            // The tenant-scoped overload releases the one route it scanned, so
            // this fire is the last point at which a commit into that tenant
            // can still escape the scan.
            if (committed) {
                return;
            }
            committed = true;
            ASSERT_TRUE(service
                            .PutStart(context.client_id, "scoped_racer",
                                      TenantId::Default(), 1024, config)
                            .has_value());
            ASSERT_TRUE(service
                            .PutEnd(context.client_id, "scoped_racer",
                                    TenantId::Default(), ReplicaType::ALL)
                            .has_value());
        });

    service.RemoveAll(TenantId::Default(), true);
    MasterServiceTestPeer(service).SetRemoveAllTenantHookForTesting(nullptr);

    ASSERT_TRUE(committed) << "the hook never fired, so nothing was raced";
    EXPECT_EQ(0u,
              MasterServiceTestPeer(service).GetKvClearedPublishedForTesting());
    EXPECT_EQ(
        1u, MasterServiceTestPeer(service).GetKvClearedSuppressedForTesting());
}

TEST_F(MasterServiceTest, StandbySnapshotRestorePreservesTenantScopedKeys) {
    const TenantId tenant_a("tenant_restore_a");
    const TenantId tenant_b("tenant_restore_b");
    MasterService service(
        MakeStrictTenantConfig({tenant_a.value(), tenant_b.value()}));
    const std::string key = "shared_restore_key";

    Replica replica(generate_uuid(), 128, "local://standby",
                    ReplicaStatus::COMPLETE);
    StandbyObjectMetadata metadata;
    metadata.client_id = generate_uuid();
    metadata.size = 128;
    metadata.replicas.push_back(replica.get_descriptor());

    ASSERT_TRUE(
        service
            .RestoreFromStandbySnapshot({{tenant_a.value(), key, metadata}},
                                        /*initial_oplog_sequence_id=*/0, {})
            .has_value());

    EXPECT_TRUE(service.ExistKey(key, tenant_a).value_or(false));
    EXPECT_FALSE(service.ExistKey(key, tenant_b).value_or(true));
    EXPECT_FALSE(service.ExistKey(key, TenantId::Default()).value_or(true));
}

// A snapshot reload replaces every publication, so the replica-action records
// the service keeps beside them, the promotion-candidate index and the
// dynamic-replication leases, have to go with them: a key published again
// after the reload must inherit none of them.
TEST_F(MasterServiceTest, SnapshotReloadDropsReplicaActionState) {
    const TenantId tenant("tenant_reload");
    const std::string key = "reloaded_key";
    const uint64_t slice_length = 1024;
    const MasterServiceTestPeer::ObjectIdentity identity{tenant, key};

    // promotion_on_hit binds only with the offload machinery on, and a zero
    // pool watermark sends every admission down the watermark-rejection
    // branch that records the retry candidate.
    MasterServiceConfig config = MakeStrictTenantConfig({tenant.value()});
    config.enable_offload = true;
    config.promotion_on_hit = true;
    config.promotion_admission_threshold = 1;
    config.eviction_high_watermark_ratio = 0.0;
    MasterService service(config);
    const auto segment = PrepareSimpleSegment(service);
    MasterServiceTestPeer peer(service);
    // The eviction pass retries promotion candidates, and its retry loop
    // unindexes a candidate whose object still carries a memory replica.
    MasterServiceTestPeer::EvictionRunning(service) = false;
    if (MasterServiceTestPeer::EvictionThread(service).joinable()) {
        MasterServiceTestPeer::EvictionThread(service).join();
    }

    ReplicateConfig put_config;
    put_config.replica_num = 1;
    PutCompletedObject(service, segment.client_id, key, tenant, put_config,
                       slice_length);

    auto publication = MasterServiceTestPeer::FindObject(service, identity);
    ASSERT_NE(publication, nullptr);
    ASSERT_EQ(peer.TryPushPromotionQueue(identity, /*record_candidate=*/true),
              MasterServiceTestPeer::PromotionQueueResult::kWatermarkRejected);

    const UUID proposal_id = generate_uuid();
    ReplicaActionLease lease;
    lease.proposal_id = proposal_id;
    lease.lease_id = proposal_id;
    lease.tenant_id = tenant.value();
    lease.key = key;
    // An hour out: no sweep can retract it during the test.
    lease.expire_at_ms_epoch =
        MasterServiceTestPeer::DynamicReplicationNowMs() + 3600000;
    peer.PutDynamicReplicationLeaseForTesting(tenant, publication, proposal_id,
                                              lease);

    // Both records name the key rather than the publication, so they are the
    // service's own state and not the entry's.
    const auto candidates =
        MasterServiceTestPeer::PromotionCandidateKeys(service, tenant);
    ASSERT_FALSE(candidates.empty());
    ASSERT_TRUE(MasterServiceTestPeer::FindDynamicReplicationLease(
                    service, tenant, proposal_id)
                    .has_value());
    ASSERT_TRUE(MasterServiceTestPeer::HasReplicaActionState(service, tenant));
    EXPECT_NE(MasterServiceTestPeer::PromotionCandidateCount(service).load(
                  std::memory_order_relaxed),
              0u);
    // The retry cursor and the in-flight counter move during a sweep, so a
    // reset that dropped only the candidate index would leave them behind.
    MasterServiceTestPeer::PromotionRetryCursor(service).store(7);
    MasterServiceTestPeer::PromotionInFlight(service).store(3);

    MasterServiceTestPeer::MetadataSerializer serializer(&service);
    serializer.Reset();

    EXPECT_EQ(MasterServiceTestPeer::Tenants(service).Lookup(tenant), nullptr);
    EXPECT_EQ(MasterServiceTestPeer::FindObject(service, identity), nullptr);
    EXPECT_TRUE(
        MasterServiceTestPeer::PromotionCandidateKeys(service, tenant).empty());
    EXPECT_EQ(MasterServiceTestPeer::PromotionCandidateCount(service).load(
                  std::memory_order_relaxed),
              0u);
    EXPECT_EQ(MasterServiceTestPeer::PromotionRetryCursor(service).load(
                  std::memory_order_relaxed),
              0u);
    EXPECT_EQ(MasterServiceTestPeer::PromotionInFlight(service).load(
                  std::memory_order_relaxed),
              0u);
    EXPECT_FALSE(MasterServiceTestPeer::FindDynamicReplicationLease(
                     service, tenant, proposal_id)
                     .has_value());
    EXPECT_FALSE(MasterServiceTestPeer::HasReplicaActionState(service, tenant))
        << "the record held both halves, so the reset drops it with them";

    // The same tenant and key published again inherit nothing.
    PutCompletedObject(service, segment.client_id, key, tenant, put_config,
                       slice_length);
    auto republished = MasterServiceTestPeer::FindObject(service, identity);
    ASSERT_NE(republished, nullptr);
    EXPECT_NE(republished, publication);
    EXPECT_TRUE(
        MasterServiceTestPeer::PromotionCandidateKeys(service, tenant).empty());
    EXPECT_FALSE(MasterServiceTestPeer::FindDynamicReplicationLease(
                     service, tenant, proposal_id)
                     .has_value());
    // The new publication carries no ledger state of its own beyond the
    // object just put.
    const auto committed_quota = republished->WithSharedAccess(
        [](const ObjectMetadata& metadata, const ObjectEntry::State&) {
            return metadata.quota_ledger.CommittedBytes();
        });
    EXPECT_EQ(committed_quota, slice_length);
}

namespace {

// The tenants the registry holds: one per tenant id a publish path created.
size_t RegisteredTenantCount(MasterService& service) {
    size_t count = 0;
    MasterServiceTestPeer::Tenants(service).Visit(
        [&](const TenantId&, const std::shared_ptr<metadata::Tenant>&) {
            ++count;
        });
    return count;
}

}  // namespace

// A request that publishes nothing must not register a tenant: a client naming
// a tenant id this service never stored into would otherwise grow the registry
// with an empty tenant per request.
// A PutStart that fails before it stores anything leaves the registry as it
// was: the tenant is created when the object is about to be published, not when
// a request arrives, so a rejected write cannot leave an empty tenant behind.
TEST_F(MasterServiceTest, FailedPutStartLeavesNoTenantBehind) {
    const TenantId filling("tenant_put_fills_pool");
    const TenantId failing("tenant_put_never_publishes");
    MasterService service(
        MakeStrictTenantConfig({filling.value(), failing.value()}));
    constexpr size_t kSegmentSize = 8 * 1024 * 1024;
    constexpr size_t kObjectSize = 4 * 1024 * 1024;
    const auto context = PrepareSimpleSegment(
        service, "put_failure_segment", kDefaultSegmentBase, kSegmentSize);
    const size_t tenants_before = RegisteredTenantCount(service);

    ReplicateConfig config;
    config.replica_num = 1;
    PutCompletedObject(service, context.client_id, "fill_a", filling, config,
                       kObjectSize);
    ASSERT_EQ(RegisteredTenantCount(service), tenants_before + 1);

    // `failing` has a quota of its own and an empty route, and the request
    // exceeds that quota: the write is refused before anything is published.
    auto failed = service.PutStart(context.client_id, "no_room", failing,
                                   kObjectSize + 1, config);
    ASSERT_FALSE(failed.has_value());
    EXPECT_EQ(ErrorCode::TENANT_QUOTA_EXCEEDED, failed.error());
    EXPECT_EQ(RegisteredTenantCount(service), tenants_before + 1)
        << "a PutStart that never publishes must not register its tenant";
    EXPECT_EQ(MasterServiceTestPeer::Tenants(service).Lookup(failing), nullptr);
    EXPECT_FALSE(service.GetReplicaList("no_room", failing).has_value());

    // The same tenant is registered by a write that does publish.
    PutCompletedObject(service, context.client_id, "stored", failing, config,
                       kObjectSize);
    EXPECT_EQ(RegisteredTenantCount(service), tenants_before + 2);
    EXPECT_NE(MasterServiceTestPeer::Tenants(service).Lookup(failing), nullptr);
}

TEST_F(MasterServiceTest, MissDoesNotRegisterTenant) {
    const TenantId tenant("tenant_miss_scope");
    auto service = std::make_unique<MasterService>(
        MakeStrictTenantConfig({tenant.value()}));
    [[maybe_unused]] const auto context = PrepareSimpleSegment(*service);
    const size_t tenants_before = RegisteredTenantCount(*service);

    // A remove for an unregistered tenant and for a key the registered tenant
    // does not publish.
    EXPECT_FALSE(service->Remove("missing_key", TenantId("tenant_never_used"))
                     .has_value());
    EXPECT_FALSE(service->Remove("missing_key", tenant).has_value());
    EXPECT_FALSE(service
                     ->PutRevoke(generate_uuid(), "missing_key", tenant,
                                 ReplicaType::MEMORY)
                     .has_value());
    EXPECT_EQ(service->GetReplicaList("missing_key", tenant).has_value(),
              false);
    EXPECT_EQ(RegisteredTenantCount(*service), tenants_before);

    // Publishing under a configured tenant registers it, and only it.
    PutCompletedObject(*service, context.client_id, "published_key", tenant,
                       ReplicateConfig{.replica_num = 1}, 1024);
    EXPECT_EQ(RegisteredTenantCount(*service), tenants_before + 1);
    EXPECT_EQ(MasterServiceTestPeer::FindObject(
                  *service,
                  MasterServiceTestPeer::ObjectIdentity{
                      tenant, "published_key"}) != nullptr,
              true);
}

// A payload's entry keys are the shard slots a master that predates the tenant
// model reads them as, and that master rejects a key at or above its own shard
// count. A state with more tenants than slots therefore spreads them over the
// slots instead of numbering past the last one, and every object still carries
// its own tenant id, so the decode restores all of them.
TEST_F(MasterServiceTest, SnapshotPacksMoreTenantsThanShardSlots) {
    const size_t tenant_count = ha::kSnapshotShardSlots + 1;
    std::vector<std::string> tenant_ids;
    tenant_ids.reserve(tenant_count);
    const auto key_of = [](size_t index) {
        return "slot_key_" + std::to_string(index);
    };
    for (size_t i = 0; i < tenant_count; ++i) {
        tenant_ids.push_back("slot_tenant_" + std::to_string(i));
    }

    MasterService service(MakeStrictTenantConfig(tenant_ids));
    const auto context = PrepareSimpleSegment(service);
    ReplicateConfig put_config;
    put_config.replica_num = 1;
    for (size_t i = 0; i < tenant_count; ++i) {
        PutCompletedObject(service, context.client_id, key_of(i),
                           TenantId(tenant_ids[i]), put_config);
    }

    MasterServiceTestPeer::MetadataSerializer serializer(&service);
    auto payload = serializer.Serialize();
    ASSERT_TRUE(payload.has_value());

    auto handle = msgpack::unpack(
        reinterpret_cast<const char*>(payload->data()), payload->size());
    const msgpack::object& root = handle.get();
    const msgpack::object* shards = nullptr;
    for (uint32_t i = 0; i < root.via.map.size; ++i) {
        const auto& key = root.via.map.ptr[i].key;
        if (key.type == msgpack::type::STR &&
            std::string(key.via.str.ptr, key.via.str.size) == "shards") {
            shards = &root.via.map.ptr[i].val;
        }
    }
    ASSERT_NE(shards, nullptr);
    ASSERT_EQ(ha::kSnapshotShardSlots, shards->via.map.size);
    for (uint32_t i = 0; i < shards->via.map.size; ++i) {
        EXPECT_LT(shards->via.map.ptr[i].key.as<uint32_t>(),
                  ha::kSnapshotShardSlots)
            << "an entry key at or above the slot count is one an older "
               "master rejects";
    }

    // The decode replaces the state this service holds, and its segments stay
    // mounted, so the memory replicas the payload names resolve.
    MasterServiceTestPeer::MetadataSerializer reader(&service);
    ASSERT_TRUE(reader.Deserialize(*payload).has_value());
    for (const size_t index : {size_t{0}, tenant_count / 2, tenant_count - 1}) {
        EXPECT_TRUE(service.ExistKey(key_of(index), TenantId(tenant_ids[index]))
                        .value_or(false))
            << "tenant " << tenant_ids[index] << " lost its object";
    }
}

// Decoding a snapshot replaces the metadata the routes hold: a payload that
// carries no objects leaves no object behind, so a key the payload does not
// carry cannot survive from the state the service was in.
TEST_F(MasterServiceTest, SnapshotDecodeReplacesPublishedMetadata) {
    const TenantId tenant("tenant_snapshot_scope");
    const std::string key = "snapshot_key";

    MasterService service(MakeStrictTenantConfig({tenant.value()}));
    const auto context = PrepareSimpleSegment(service);
    PutCompletedObject(service, context.client_id, key, tenant,
                       ReplicateConfig{.replica_num = 1}, 1024);
    ASSERT_NE(MasterServiceTestPeer::FindObject(
                  service, MasterServiceTestPeer::ObjectIdentity{tenant, key}),
              nullptr);
    ASSERT_EQ(RegisteredTenantCount(service), 1u);

    // The payload of a service that holds no object, which for a snapshot of
    // this one would mean the tenant's objects are gone.
    std::vector<uint8_t> empty_payload;
    {
        MasterService empty(MakeStrictTenantConfig({tenant.value()}));
        MasterServiceTestPeer::MetadataSerializer serializer(&empty);
        auto encoded = serializer.Serialize();
        ASSERT_TRUE(encoded.has_value());
        empty_payload = std::move(*encoded);
    }

    MasterServiceTestPeer::MetadataSerializer reader(&service);
    ASSERT_TRUE(reader.Deserialize(empty_payload).has_value());

    EXPECT_EQ(MasterServiceTestPeer::FindObject(
                  service, MasterServiceTestPeer::ObjectIdentity{tenant, key}),
              nullptr)
        << "a key the payload does not carry must not survive the decode";
    EXPECT_EQ(RegisteredTenantCount(service), 0u);
}

namespace {

// One object of a metadata payload: the tenant and key it routes, the group it
// belongs to, and the lease deadline the payload records for it.
struct SnapshotPayloadObject {
    std::string tenant_id;
    std::string key;
    std::string group_id;
    uint64_t lease_deadline_ms;
    uint64_t replica_id;
    uint64_t object_size{1024};
};

// A metadata payload shaped the way MetadataSerializer::Serialize writes one:
// one compressed shard per list of objects, each object routed by the tenant id
// in its own item, with an empty discarded-replicas list and a replica id base.
// Building it by hand pins the deadline every item carries, which is what the
// decode takes each group's shared lease from.
std::vector<uint8_t> BuildSnapshotMetadataPayload(
    const std::vector<std::vector<SnapshotPayloadObject>>& shards,
    const UUID& client_id) {
    constexpr uint64_t kPutStartTimeMs = 1700000000000ULL;
    constexpr uint64_t kReplicaNextId = 1000000ULL;

    msgpack::sbuffer root_buffer;
    MsgpackPacker root_packer(&root_buffer);
    root_packer.pack_map(3);

    root_packer.pack(std::string("shards"));
    root_packer.pack_map(shards.size());
    for (size_t shard_index = 0; shard_index < shards.size(); ++shard_index) {
        msgpack::sbuffer shard_buffer;
        MsgpackPacker shard_packer(&shard_buffer);
        shard_packer.pack_map(1);
        shard_packer.pack(std::string("metadata"));
        shard_packer.pack_array(shards[shard_index].size());
        for (const auto& object : shards[shard_index]) {
            shard_packer.pack_array(3);
            shard_packer.pack(object.tenant_id);
            shard_packer.pack(object.key);
            // The layout SerializeMetadata writes: the seven leading fields,
            // the replica count, the data type, one replica per count, then
            // hard_pinned and group_id.
            shard_packer.pack_array(11);
            shard_packer.pack(UuidToString(client_id));
            shard_packer.pack(kPutStartTimeMs);
            shard_packer.pack(object.object_size);
            shard_packer.pack(object.lease_deadline_ms);
            shard_packer.pack(false);
            shard_packer.pack(uint64_t{0});
            shard_packer.pack(uint32_t{1});
            shard_packer.pack(static_cast<uint8_t>(ObjectDataType::TENSOR));
            // One DISK replica, as Serializer<Replica> packs one.
            shard_packer.pack_array(4);
            shard_packer.pack(object.replica_id);
            shard_packer.pack(static_cast<int16_t>(ReplicaStatus::COMPLETE));
            shard_packer.pack(static_cast<int8_t>(ReplicaType::DISK));
            shard_packer.pack_array(2);
            shard_packer.pack(std::string("/tmp/mooncake_decode_replica.data"));
            shard_packer.pack(object.object_size);
            shard_packer.pack(false);
            shard_packer.pack(object.group_id);
        }
        const auto compressed =
            zstd_compress(reinterpret_cast<const uint8_t*>(shard_buffer.data()),
                          shard_buffer.size(), 3);
        root_packer.pack(std::to_string(shard_index));
        root_packer.pack_bin(compressed.size());
        root_packer.pack_bin_body(
            reinterpret_cast<const char*>(compressed.data()),
            compressed.size());
    }

    root_packer.pack(std::string("discarded_replicas"));
    root_packer.pack_array(0);
    root_packer.pack(std::string("replica_next_id"));
    root_packer.pack(kReplicaNextId);

    const auto* data = reinterpret_cast<const uint8_t*>(root_buffer.data());
    return std::vector<uint8_t>(data, data + root_buffer.size());
}

}  // namespace

// Decoding a snapshot wires what the payload carries: each object's group
// membership, one shared group lease per tenant raised to the latest deadline
// among that group's members, a same-named group in another tenant kept apart,
// and no replica-action state inherited from before the decode. A payload whose
// deadlines the test pins shows which of them the group ends on.
TEST_F(MasterServiceTest, SnapshotDecodeWiresGroupsAndDropsActionState) {
    const TenantId tenant_a("tenant_decode_groups_a");
    const TenantId tenant_b("tenant_decode_groups_b");
    const std::string group_id = "decode-shared-group";
    const std::string staged_key = "decode_staged_key";
    const std::string restored_key_a1 = "restored_group_a1";
    const std::string restored_key_a2 = "restored_group_a2";
    const std::string restored_key_b = "restored_group_b1";
    const uint64_t base_deadline_ms = 4200000000000ULL;
    // The member decoded first carries the latest deadline of its group, so a
    // group lease that ends on the last member the decode saw, or that a later
    // member of the same group lowers, fails the assertions below.
    const uint64_t group_a_latest_deadline_ms = base_deadline_ms + 60000;
    const uint64_t group_a_earlier_deadline_ms = base_deadline_ms;
    const uint64_t group_b_deadline_ms = base_deadline_ms + 30000;

    MasterServiceConfig config =
        MakeStrictTenantConfig({tenant_a.value(), tenant_b.value()});
    // Promotion on hit with the pool held over its watermark: offload is what
    // gives promotion its LOCAL_DISK replicas, and the watermark gate is what
    // records the candidate this test stages, which the decode has to clear
    // along with the lease beside it.
    config.enable_offload = true;
    config.promotion_on_hit = true;
    config.promotion_admission_threshold = 1;
    config.eviction_high_watermark_ratio = 0.0;
    MasterService service(config);
    // Pool eviction is held over its watermark, so the periodic worker would
    // evict what this test stages and its retry loop would unindex the staged
    // candidate; the replica cleanup worker moves replicas. The serializer runs
    // against the state those would move.
    MasterServiceTestPeer::EvictionRunning(service) = false;
    if (MasterServiceTestPeer::EvictionThread(service).joinable()) {
        MasterServiceTestPeer::EvictionThread(service).join();
    }
    MasterServiceTestPeer::ReplicaCleanupWorker(service).Stop();
    MasterServiceTestPeer peer(service);
    const auto context = PrepareSimpleSegment(service);

    // One publication of this service, so the decode below can be shown to
    // replace what the routes held.
    ReplicateConfig put_config;
    put_config.replica_num = 1;
    PutCompletedObject(service, context.client_id, staged_key, tenant_a,
                       put_config);

    const auto route_of = [&](const TenantId& tenant, const std::string& key) {
        return MasterServiceTestPeer::FindObject(
            service, MasterServiceTestPeer::ObjectIdentity{tenant, key});
    };
    // The lease a member holds: an entry's metadata borrows the lease of the
    // group it belongs to, so two members of one group report one lease and two
    // groups report their own.
    const auto lease_of = [&](const TenantId& tenant, const std::string& key) {
        auto lease = peer.WithStoredObjectForRead(
            tenant, key,
            [](const std::shared_ptr<ObjectEntry>&,
               const ObjectMetadata& metadata,
               const ObjectEntry::State&) { return metadata.lease_; });
        return lease.has_value() ? *lease : nullptr;
    };
    const auto deadline_ms_of = [](const std::shared_ptr<Lease>& lease) {
        return static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::milliseconds>(
                lease->ExpiresAt().time_since_epoch())
                .count());
    };

    // Replica-action state staged on the staged publication: a promotion
    // candidate the retry loop would keep retrying, and a dynamic-replication
    // lease a client could still act on. A decode has to drop both.
    const MasterServiceTestPeer::ObjectIdentity staged{tenant_a, staged_key};
    const auto staged_entry =
        MasterServiceTestPeer::FindObject(service, staged);
    ASSERT_NE(staged_entry, nullptr);
    ASSERT_EQ(peer.TryPushPromotionQueue(staged, /*record_candidate=*/true),
              MasterServiceTestPeer::PromotionQueueResult::kWatermarkRejected);
    const UUID proposal_id = generate_uuid();
    ReplicaActionLease lease;
    lease.proposal_id = proposal_id;
    lease.lease_id = proposal_id;
    lease.tenant_id = tenant_a.value();
    lease.key = staged_key;
    lease.expire_at_ms_epoch =
        MasterServiceTestPeer::DynamicReplicationNowMs() + 3600000;
    peer.PutDynamicReplicationLeaseForTesting(tenant_a, staged_entry,
                                              proposal_id, lease);
    ASSERT_EQ(peer.CountCandidatesForTesting(tenant_a), 1u);
    ASSERT_TRUE(MasterServiceTestPeer::FindDynamicReplicationLease(
                    service, tenant_a, proposal_id)
                    .has_value());

    // A payload whose deadlines are pinned: the member that comes first carries
    // the latest deadline of its group. Its objects carry keys the service does
    // not hold, so the decode replaces the staged publication with them, and
    // they are DISK-backed, so decoding them is metadata alone rather than a
    // second claim on a segment allocation.
    const auto payload = BuildSnapshotMetadataPayload(
        {{{tenant_a.value(), restored_key_a1, group_id,
           group_a_latest_deadline_ms, 7001},
          {tenant_a.value(), restored_key_a2, group_id,
           group_a_earlier_deadline_ms, 7002}},
         {{tenant_b.value(), restored_key_b, group_id, group_b_deadline_ms,
           7003}}},
        context.client_id);
    {
        MasterServiceTestPeer::MetadataSerializer serializer(&service);
        ASSERT_TRUE(serializer.Deserialize(payload).has_value());
    }

    // The staged publication is gone, and so are the records that named it.
    EXPECT_EQ(route_of(tenant_a, staged_key), nullptr);
    EXPECT_EQ(peer.CountCandidatesForTesting(tenant_a), 0u);
    EXPECT_FALSE(MasterServiceTestPeer::FindDynamicReplicationLease(
                     service, tenant_a, proposal_id)
                     .has_value());

    // Membership is wired per tenant from the payload, and every member of one
    // group borrows that group's single lease, which ends at the latest
    // deadline among its members. A group with the same id in another tenant is
    // another group, with a lease of its own and the deadline its own payload
    // carries.
    auto decoded_members_a =
        GetGroupMemberKeysForTest(service, group_id, tenant_a.value());
    std::sort(decoded_members_a.begin(), decoded_members_a.end());
    EXPECT_EQ(decoded_members_a,
              (std::vector<std::string>{restored_key_a1, restored_key_a2}));
    EXPECT_EQ(GetGroupMemberKeysForTest(service, group_id, tenant_b.value()),
              (std::vector<std::string>{restored_key_b}));
    const auto decoded_lease_a1 = lease_of(tenant_a, restored_key_a1);
    const auto decoded_lease_a2 = lease_of(tenant_a, restored_key_a2);
    ASSERT_NE(decoded_lease_a1, nullptr);
    ASSERT_NE(decoded_lease_a2, nullptr);
    EXPECT_EQ(decoded_lease_a1.get(), decoded_lease_a2.get())
        << "the members of one group share one lease";
    EXPECT_EQ(deadline_ms_of(decoded_lease_a1), group_a_latest_deadline_ms)
        << "the group lease ends at the latest deadline among its members";
    const auto decoded_lease_b = lease_of(tenant_b, restored_key_b);
    ASSERT_NE(decoded_lease_b, nullptr);
    EXPECT_NE(decoded_lease_b.get(), decoded_lease_a1.get())
        << "the same group id in another tenant is another group";
    EXPECT_EQ(deadline_ms_of(decoded_lease_b), group_b_deadline_ms)
        << "another tenant's group keeps the deadline its own payload carries";
}

TEST_F(MasterServiceTest, GetAllKeysListsOnlyRequestedTenant) {
    const TenantId tenant_a("tenant_get_all_keys_a");
    auto service_ = std::make_unique<MasterService>(MakeStrictTenantConfig(
        {std::string(TenantId::kDefaultValue), tenant_a.value()}));
    [[maybe_unused]] const auto context = PrepareSimpleSegment(*service_);
    const UUID client_id = generate_uuid();

    const std::string shared_key = "shared_listing_key";
    const std::string default_only_key = "default_listing_key";
    const std::string tenant_only_key = "tenant_listing_key";

    ReplicateConfig config;
    config.replica_num = 1;
    ASSERT_TRUE(
        service_
            ->PutStart(client_id, shared_key, TenantId::Default(), 1024, config)
            .has_value());
    ASSERT_TRUE(service_
                    ->PutEnd(client_id, shared_key, TenantId::Default(),
                             ReplicaType::MEMORY)
                    .has_value());
    ASSERT_TRUE(service_
                    ->PutStart(client_id, default_only_key, TenantId::Default(),
                               1024, config)
                    .has_value());
    ASSERT_TRUE(service_
                    ->PutEnd(client_id, default_only_key, TenantId::Default(),
                             ReplicaType::MEMORY)
                    .has_value());
    ASSERT_TRUE(
        service_->PutStart(client_id, shared_key, tenant_a, 1024, config)
            .has_value());
    ASSERT_TRUE(
        service_->PutEnd(client_id, shared_key, tenant_a, ReplicaType::MEMORY)
            .has_value());
    ASSERT_TRUE(
        service_->PutStart(client_id, tenant_only_key, tenant_a, 1024, config)
            .has_value());
    ASSERT_TRUE(
        service_
            ->PutEnd(client_id, tenant_only_key, tenant_a, ReplicaType::MEMORY)
            .has_value());

    auto default_keys = service_->GetAllKeys(TenantId::Default());
    ASSERT_TRUE(default_keys.has_value());
    EXPECT_NE(std::find(default_keys->begin(), default_keys->end(), shared_key),
              default_keys->end());
    EXPECT_NE(
        std::find(default_keys->begin(), default_keys->end(), default_only_key),
        default_keys->end());
    EXPECT_EQ(
        std::find(default_keys->begin(), default_keys->end(), tenant_only_key),
        default_keys->end());

    auto tenant_keys = service_->GetAllKeys(tenant_a);
    ASSERT_TRUE(tenant_keys.has_value());
    EXPECT_NE(std::find(tenant_keys->begin(), tenant_keys->end(), shared_key),
              tenant_keys->end());
    EXPECT_NE(
        std::find(tenant_keys->begin(), tenant_keys->end(), tenant_only_key),
        tenant_keys->end());
    EXPECT_EQ(
        std::find(tenant_keys->begin(), tenant_keys->end(), default_only_key),
        tenant_keys->end());
}

TEST_F(MasterServiceTest, TenantScopedPutsAndRemovesUpdateGlobalKeyCount) {
    const std::string key = "shared_user_key";
    const TenantId tenant_a("tenant_key_count_a");
    const TenantId tenant_b("tenant_key_count_b");
    auto service_ = std::make_unique<MasterService>(
        MakeStrictTenantConfig({tenant_a.value(), tenant_b.value()}));
    [[maybe_unused]] const auto context = PrepareSimpleSegment(*service_);
    const UUID client_id = generate_uuid();

    ReplicateConfig config;
    config.replica_num = 1;

    EXPECT_EQ(service_->GetKeyCount(), 0u);
    ASSERT_TRUE(
        service_->PutStart(client_id, key, tenant_a, 1024, config).has_value());
    ASSERT_TRUE(service_->PutEnd(client_id, key, tenant_a, ReplicaType::MEMORY)
                    .has_value());
    ASSERT_TRUE(
        service_->PutStart(client_id, key, tenant_b, 2048, config).has_value());
    ASSERT_TRUE(service_->PutEnd(client_id, key, tenant_b, ReplicaType::MEMORY)
                    .has_value());
    EXPECT_EQ(service_->GetKeyCount(), 2u);

    ASSERT_TRUE(service_->Remove(key, tenant_a, /*force=*/true).has_value());
    EXPECT_TRUE(service_->GetReplicaList(key, tenant_b).has_value());
    EXPECT_EQ(service_->GetKeyCount(), 1u);

    ASSERT_TRUE(service_->Remove(key, tenant_b, /*force=*/true).has_value());
    EXPECT_EQ(service_->GetKeyCount(), 0u);
}

TEST_F(MasterServiceTest, MasterConfigParsesLocalFirstStrategy) {
    MasterConfig config{};
    config.allocation_strategy = "local_first";

    WrappedMasterServiceConfig wrapped_config(config, 0);
    MasterServiceConfig service_config(wrapped_config);
    EXPECT_EQ(service_config.allocation_strategy_type,
              AllocationStrategyType::LOCAL_FIRST);
}

TEST_F(MasterServiceTest, ProtectCopyMoveSourceFromEviction) {
    const uint64_t kv_lease_ttl = 100;
    const uint64_t client_live_ttl = 600;
    auto service_config = MasterServiceConfig::builder()
                              .set_default_kv_lease_ttl(kv_lease_ttl)
                              .set_client_live_ttl_sec(client_live_ttl)
                              .build();
    std::unique_ptr<MasterService> service_(new MasterService(service_config));

    // Mount 2 segments (segment_1, segment_2) with PrepareSimpleSegment, each
    // 16 MB
    constexpr size_t kBaseAddr = 0x100000000;
    constexpr size_t kSegmentSize = 16 * 1024 * 1024;  // 16 MB
    [[maybe_unused]] const auto context1 =
        PrepareSimpleSegment(*service_, "segment_1", kBaseAddr, kSegmentSize);
    [[maybe_unused]] const auto context2 =
        PrepareSimpleSegment(*service_, "segment_2", kBaseAddr, kSegmentSize);

    const UUID client_id = context1.client_id;

    const std::string copy_key = "copy_key";
    const std::string move_key = "move_key";
    uint64_t slice_length = 1024 * 1024;
    ReplicateConfig config;
    config.replica_num = 1;
    config.preferred_segment = "segment_1";

    // Put two objects for move and copy tests.
    auto put_start_result = service_->PutStart(
        client_id, copy_key, TenantId::Default(), slice_length, config);
    ASSERT_TRUE(put_start_result.has_value());
    auto put_end_result = service_->PutEnd(
        client_id, copy_key, TenantId::Default(), ReplicaType::MEMORY);
    ASSERT_TRUE(put_end_result.has_value());

    put_start_result = service_->PutStart(
        client_id, move_key, TenantId::Default(), slice_length, config);
    ASSERT_TRUE(put_start_result.has_value());
    put_end_result = service_->PutEnd(client_id, move_key, TenantId::Default(),
                                      ReplicaType::MEMORY);
    ASSERT_TRUE(put_end_result.has_value());

    // Start copy and move operations.
    auto copy_start_result = service_->CopyStart(
        client_id, copy_key, TenantId::Default(), "segment_1", {"segment_2"});
    ASSERT_TRUE(copy_start_result.has_value());

    auto move_start_result = service_->MoveStart(
        client_id, move_key, TenantId::Default(), "segment_1", "segment_2");
    ASSERT_TRUE(move_start_result.has_value());

    // Put more objects to trigger eviction. Do not prefer any segments.
    config.preferred_segment = "";
    for (size_t i = 0; i < 128 * (kSegmentSize * 2 / slice_length); ++i) {
        std::string key = "test_key_" + std::to_string(i);
        auto put_start_result = service_->PutStart(
            client_id, key, TenantId::Default(), slice_length, config);
        if (put_start_result.has_value()) {
            auto put_end_result = service_->PutEnd(
                client_id, key, TenantId::Default(), ReplicaType::MEMORY);
            ASSERT_TRUE(put_end_result.has_value());
        } else {
            // wait for eviction to work
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
    }

    // Wait all objects lease expiring and then remove them.
    std::this_thread::sleep_for(std::chrono::milliseconds(kv_lease_ttl * 2));
    auto remove_all_result = service_->RemoveAll();
    ASSERT_TRUE(remove_all_result > 0);

    // Try end copy and move operations, should success.
    auto copy_end_result =
        service_->CopyEnd(client_id, copy_key, TenantId::Default());
    EXPECT_TRUE(copy_end_result.has_value());

    auto move_end_result =
        service_->MoveEnd(client_id, move_key, TenantId::Default());
    EXPECT_TRUE(move_end_result.has_value());
}

TEST_F(MasterServiceTest, DiscardTimeoutCopyMove) {
    const uint64_t kv_lease_ttl = 100;
    const uint64_t client_live_ttl = 600;
    const uint64_t put_discard_timeout = 1;
    const uint64_t put_release_timeout = 2;
    auto service_config =
        MasterServiceConfig::builder()
            .set_default_kv_lease_ttl(kv_lease_ttl)
            .set_client_live_ttl_sec(client_live_ttl)
            .set_put_start_discard_timeout_sec(put_discard_timeout)
            .set_put_start_release_timeout_sec(put_release_timeout)
            .build();
    std::unique_ptr<MasterService> service_(new MasterService(service_config));

    // Mount 2 segments (segment_1, segment_2) with PrepareSimpleSegment, each
    // 16 MB
    constexpr size_t kBaseAddr = 0x100000000;
    constexpr size_t kSegmentSize = 16 * 1024 * 1024;  // 16 MB
    [[maybe_unused]] const auto context1 =
        PrepareSimpleSegment(*service_, "segment_1", kBaseAddr, kSegmentSize);
    [[maybe_unused]] const auto context2 =
        PrepareSimpleSegment(*service_, "segment_2", kBaseAddr, kSegmentSize);

    const UUID client_id = context1.client_id;

    const std::string copy_key = "copy_key";
    const std::string move_key = "move_key";
    uint64_t slice_length = 1024 * 1024;
    ReplicateConfig config;
    config.replica_num = 1;
    config.preferred_segment = "segment_1";

    // Put two objects for move and copy tests.
    auto put_start_result = service_->PutStart(
        client_id, copy_key, TenantId::Default(), slice_length, config);
    ASSERT_TRUE(put_start_result.has_value());
    auto put_end_result = service_->PutEnd(
        client_id, copy_key, TenantId::Default(), ReplicaType::MEMORY);
    ASSERT_TRUE(put_end_result.has_value());

    put_start_result = service_->PutStart(
        client_id, move_key, TenantId::Default(), slice_length, config);
    ASSERT_TRUE(put_start_result.has_value());
    put_end_result = service_->PutEnd(client_id, move_key, TenantId::Default(),
                                      ReplicaType::MEMORY);
    ASSERT_TRUE(put_end_result.has_value());

    // Start copy and move operations.
    auto copy_start_result = service_->CopyStart(
        client_id, copy_key, TenantId::Default(), "segment_1", {"segment_2"});
    ASSERT_TRUE(copy_start_result.has_value());

    auto move_start_result = service_->MoveStart(
        client_id, move_key, TenantId::Default(), "segment_1", "segment_2");
    ASSERT_TRUE(move_start_result.has_value());

    // Wait for the operations timeout.
    std::this_thread::sleep_for(std::chrono::seconds(put_release_timeout));

    // Put more objects to trigger eviction. Do not prefer any segments.
    config.preferred_segment = "";
    for (size_t i = 0; i < 128 * (kSegmentSize * 2 / slice_length); ++i) {
        std::string key = "test_key_" + std::to_string(i);
        auto put_start_result = service_->PutStart(
            client_id, key, TenantId::Default(), slice_length, config);
        if (put_start_result.has_value()) {
            auto put_end_result = service_->PutEnd(
                client_id, key, TenantId::Default(), ReplicaType::MEMORY);
            ASSERT_TRUE(put_end_result.has_value());
        } else {
            // wait for eviction to work
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
    }

    // Try end copy and move operations, should fail because the objects are
    // evicted.
    auto copy_end_result =
        service_->CopyEnd(client_id, copy_key, TenantId::Default());
    EXPECT_FALSE(copy_end_result.has_value());
    EXPECT_EQ(copy_end_result.error(), ErrorCode::OBJECT_NOT_FOUND);

    auto move_end_result =
        service_->MoveEnd(client_id, move_key, TenantId::Default());
    EXPECT_FALSE(move_end_result.has_value());
    EXPECT_EQ(move_end_result.error(), ErrorCode::OBJECT_NOT_FOUND);
}

TEST_F(MasterServiceTest, CleanupStaleHandlesTest) {
    std::unique_ptr<MasterService> service_(new MasterService());

    // Mount a segment for testing
    constexpr size_t buffer = 0x300000000;
    constexpr size_t size = 1024 * 1024 * 16;  // 16MB
    auto segment = MakeSegment("test_segment", buffer, size);
    UUID client_id = generate_uuid();

    // Mount the segment
    auto mount_result = service_->MountSegment(segment, client_id);
    ASSERT_TRUE(mount_result.has_value());

    // Create an object that will be stored in the segment
    std::string key = "segment_object";
    uint64_t slice_length = 1024 * 1024;  // One 1MB slice
    ReplicateConfig config;
    config.replica_num = 1;  // One replica

    // Create the object
    auto put_start_result = service_->PutStart(
        client_id, key, TenantId::Default(), slice_length, config);
    ASSERT_TRUE(put_start_result.has_value());
    auto put_end_result = service_->PutEnd(client_id, key, TenantId::Default(),
                                           ReplicaType::MEMORY);
    ASSERT_TRUE(put_end_result.has_value());

    // Verify object exists
    auto get_result = service_->GetReplicaList(key, TenantId::Default());
    ASSERT_TRUE(get_result.has_value());
    auto retrieved_replicas = get_result.value().replicas;
    ASSERT_EQ(1, retrieved_replicas.size());

    // Unmount the segment
    auto unmount_result1 = service_->UnmountSegment(segment.id, client_id);
    ASSERT_TRUE(unmount_result1.has_value());

    // Try to get the object - it should be automatically removed since the
    // replica is invalid
    auto get_result2 = service_->GetReplicaList(key, TenantId::Default());
    EXPECT_FALSE(get_result2.has_value());
    EXPECT_EQ(ErrorCode::OBJECT_NOT_FOUND, get_result2.error());

    // Mount the segment again
    mount_result = service_->MountSegment(segment, client_id);
    ASSERT_TRUE(mount_result.has_value());

    // Create another object
    std::string key2 = "another_segment_object";
    auto put_start_result2 = service_->PutStart(
        client_id, key2, TenantId::Default(), slice_length, config);
    ASSERT_TRUE(put_start_result2.has_value());
    auto put_end_result2 = service_->PutEnd(
        client_id, key2, TenantId::Default(), ReplicaType::MEMORY);
    ASSERT_TRUE(put_end_result2.has_value());

    // Verify we can get it
    auto get_result3 = service_->GetReplicaList(key2, TenantId::Default());
    ASSERT_TRUE(get_result3.has_value());

    // Unmount the segment
    auto unmount_result2 = service_->UnmountSegment(segment.id, client_id);
    ASSERT_TRUE(unmount_result2.has_value());

    // Try to remove the object that should already be cleaned up
    auto remove_result = service_->Remove(key2, TenantId::Default());
    EXPECT_FALSE(remove_result.has_value());
    EXPECT_EQ(ErrorCode::OBJECT_NOT_FOUND, remove_result.error());
}

TEST_F(MasterServiceTest, UnmountSegmentHidesReplicasBeforeAsyncCleanup) {
    std::unique_ptr<MasterService> service_(new MasterService());

    // Mount two segments for testing
    constexpr size_t buffer1 = 0x300000000;
    constexpr size_t buffer2 = 0x400000000;
    constexpr size_t size = 1024 * 1024 * 16;

    auto segment1 = MakeSegment("segment1", buffer1, size);
    auto segment2 = MakeSegment("segment2", buffer2, size);
    UUID client_id = generate_uuid();
    auto mount_result1 = service_->MountSegment(segment1, client_id);
    ASSERT_TRUE(mount_result1.has_value());
    auto mount_result2 = service_->MountSegment(segment2, client_id);
    ASSERT_TRUE(mount_result2.has_value());

    // Create two objects in the two segments
    std::string key1 =
        GenerateKeyForSegment(client_id, service_, segment1.name);
    std::string key2 =
        GenerateKeyForSegment(client_id, service_, segment2.name);
    uint64_t slice_length = 1024;
    ReplicateConfig config;
    config.replica_num = 1;

    PauseReplicaCleanup(*service_);

    // Unmount segment1. The allocator becomes unavailable synchronously while
    // physical metadata cleanup runs on the background worker.
    auto unmount_result1 = service_->UnmountSegment(segment1.id, client_id);
    ASSERT_TRUE(unmount_result1.has_value());

    // Query paths must not expose the unavailable replica while its physical
    // metadata is still waiting for background cleanup.
    ASSERT_EQ(2u, service_->GetKeyCount());
    auto get_result1 = service_->GetReplicaList(key1, TenantId::Default());
    ASSERT_FALSE(get_result1.has_value());
    EXPECT_EQ(ErrorCode::OBJECT_NOT_FOUND, get_result1.error());

    auto exists1 = service_->ExistKey(key1, TenantId::Default());
    ASSERT_TRUE(exists1.has_value());
    EXPECT_FALSE(*exists1);
    auto exists2 = service_->ExistKey(key2, TenantId::Default());
    ASSERT_TRUE(exists2.has_value());
    EXPECT_TRUE(*exists2);

    auto batch_exists =
        service_->BatchExistKey({key1, key2}, TenantId::Default());
    ASSERT_EQ(2u, batch_exists.size());
    ASSERT_TRUE(batch_exists[0].has_value());
    EXPECT_FALSE(*batch_exists[0]);
    ASSERT_TRUE(batch_exists[1].has_value());
    EXPECT_TRUE(*batch_exists[1]);

    auto all_keys = service_->GetAllKeys(TenantId::Default());
    ASSERT_TRUE(all_keys.has_value());
    EXPECT_EQ(all_keys->end(),
              std::find(all_keys->begin(), all_keys->end(), key1));
    EXPECT_NE(all_keys->end(),
              std::find(all_keys->begin(), all_keys->end(), key2));

    // Verify objects in segment2 is still there
    auto get_result2 = service_->GetReplicaList(key2, TenantId::Default());
    ASSERT_TRUE(get_result2.has_value());

    // The worker eventually removes the old physical metadata, after which
    // the same key can be inserted again.
    ResumeReplicaCleanup(*service_);
    for (size_t i = 0; i < 100 && service_->GetKeyCount() != 1; ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    ASSERT_EQ(1u, service_->GetKeyCount());

    // Verify put key1 will put into segment2 rather than segment1
    auto put_start_result = service_->PutStart(
        client_id, key1, TenantId::Default(), slice_length, config);
    ASSERT_TRUE(put_start_result.has_value());
    replica_list = put_start_result.value();
    auto put_end_result = service_->PutEnd(client_id, key1, TenantId::Default(),
                                           ReplicaType::MEMORY);
    ASSERT_TRUE(put_end_result.has_value());
    auto get_result3 = service_->GetReplicaList(key1, TenantId::Default());
    ASSERT_TRUE(get_result3.has_value());
    auto retrieved = get_result3.value();
    ASSERT_EQ(replica_list[0]
                  .get_memory_descriptor()
                  .buffer_descriptor.transport_endpoint_,
              segment2.name);
    EXPECT_EQ(2u, service_->GetKeyCount());
}

// A mass client expiry marks handles stale all over the metadata table, and the
// sweep takes one object's lock at a time, so the unlinking of one visit must
// not hide the objects behind it: every object whose only memory segment was
// unmounted is erased, and every object on a live segment stays readable.
TEST_F(MasterServiceTest, ClearInvalidHandlesSweepsUnmountedSegments) {
    auto service = std::make_unique<MasterService>();
    PauseReplicaCleanup(*service);

    constexpr size_t kSegmentSize = 1024 * 1024 * 128;
    const std::string stale_segment_name = "sweep_stale_segment";
    const std::string live_segment_name = "sweep_live_segment";
    const auto stale_segment = PrepareSimpleSegment(
        *service, stale_segment_name, 0x300000000, kSegmentSize);
    const auto live_segment = PrepareSimpleSegment(*service, live_segment_name,
                                                   0x400000000, kSegmentSize);

    // Enough keys per segment that the walk covers many objects.
    constexpr size_t kKeysPerSegment = 100;

    std::vector<std::string> stale_keys;
    std::vector<std::string> live_keys;
    for (size_t i = 0; i < 2 * kKeysPerSegment; ++i) {
        const std::string key = "sweep_key_" + std::to_string(i);

        const bool on_stale_segment = stale_keys.size() < kKeysPerSegment;
        const UUID& client_id =
            on_stale_segment ? stale_segment.client_id : live_segment.client_id;
        const std::string& segment_name =
            on_stale_segment ? stale_segment_name : live_segment_name;
        ReplicateConfig config;
        config.replica_num = 1;
        config.preferred_segments = {segment_name};

        auto put_start = service->PutStart(client_id, key, TenantId::Default(),
                                           1024, config);
        ASSERT_TRUE(put_start.has_value()) << "key=" << key;
        ASSERT_EQ(1u, put_start->size());
        ASSERT_EQ(segment_name, (*put_start)[0]
                                    .get_memory_descriptor()
                                    .buffer_descriptor.transport_endpoint_);
        ASSERT_TRUE(service
                        ->PutEnd(client_id, key, TenantId::Default(),
                                 ReplicaType::MEMORY)
                        .has_value());
        (on_stale_segment ? stale_keys : live_keys).push_back(key);
    }

    ASSERT_TRUE(
        service
            ->UnmountSegment(stale_segment.segment_id, stale_segment.client_id)
            .has_value());
    ClearInvalidHandlesForTest(*service);

    // GetKeyCount counts physical metadata, so it distinguishes "swept" from
    // "merely hidden from the read paths by the unmount".
    EXPECT_EQ(live_keys.size(), service->GetKeyCount());
    for (const auto& key : stale_keys) {
        auto get_result = service->GetReplicaList(key, TenantId::Default());
        ASSERT_FALSE(get_result.has_value()) << "key=" << key;
        EXPECT_EQ(ErrorCode::OBJECT_NOT_FOUND, get_result.error())
            << "key=" << key;
    }
    for (const auto& key : live_keys) {
        auto get_result = service->GetReplicaList(key, TenantId::Default());
        ASSERT_TRUE(get_result.has_value()) << "key=" << key << " was swept";
        ASSERT_EQ(1u, get_result->replicas.size()) << "key=" << key;
        EXPECT_TRUE(get_result->replicas[0].is_memory_replica())
            << "key=" << key;
    }
}

TEST_F(MasterServiceTest, UnmountSegmentKeepsSynchronousCleanupInHaMode) {
    auto config = MasterServiceConfig::builder().set_enable_ha(true).build();
    auto service = std::make_unique<MasterService>(config);

    auto segment = MakeSegment("ha_sync_segment");
    UUID client_id = generate_uuid();
    ASSERT_TRUE(service->MountSegment(segment, client_id).has_value());
    const auto key = GenerateKeyForSegment(client_id, service, segment.name);
    auto exists = service->ExistKey(key, TenantId::Default());
    ASSERT_TRUE(exists.has_value());
    ASSERT_TRUE(exists.value());

    ASSERT_TRUE(service->UnmountSegment(segment.id, client_id).has_value());
    EXPECT_EQ(0u, service->GetKeyCount());
}

TEST_F(MasterServiceTest, CopyInProgressDoesNotKeepUnmountedSourceVisible) {
    auto service = std::make_unique<MasterService>();
    const auto source =
        PrepareSimpleSegment(*service, "copy_source", kDefaultSegmentBase);
    PrepareSimpleSegment(*service, "copy_target",
                         kDefaultSegmentBase + kDefaultSegmentSize);

    const UUID client_id = source.client_id;
    ReplicateConfig config;
    config.replica_num = 1;
    config.preferred_segment = "copy_source";
    PutCompletedObject(*service, client_id, "copy_key", config);
    ASSERT_TRUE(service
                    ->CopyStart(client_id, "copy_key", TenantId::Default(),
                                "copy_source", {"copy_target"})
                    .has_value());

    PauseReplicaCleanup(*service);
    ASSERT_TRUE(service->UnmountSegment(source.segment_id, source.client_id)
                    .has_value());

    ExpectKeyHiddenFromReadApis(*service, "copy_key");

    ASSERT_TRUE(service->CopyRevoke(client_id, "copy_key", TenantId::Default())
                    .has_value());
    ResumeReplicaCleanup(*service);
}

TEST_F(MasterServiceTest, MoveInProgressDoesNotKeepUnmountedSourceVisible) {
    auto service = std::make_unique<MasterService>();
    const auto source =
        PrepareSimpleSegment(*service, "move_source", kDefaultSegmentBase);
    PrepareSimpleSegment(*service, "move_target",
                         kDefaultSegmentBase + kDefaultSegmentSize);

    const UUID client_id = source.client_id;
    ReplicateConfig config;
    config.replica_num = 1;
    config.preferred_segment = "move_source";
    PutCompletedObject(*service, client_id, "move_key", config);
    ASSERT_TRUE(service
                    ->MoveStart(client_id, "move_key", TenantId::Default(),
                                "move_source", "move_target")
                    .has_value());

    PauseReplicaCleanup(*service);
    ASSERT_TRUE(service->UnmountSegment(source.segment_id, source.client_id)
                    .has_value());

    ExpectKeyHiddenFromReadApis(*service, "move_key");

    ASSERT_TRUE(service->MoveRevoke(client_id, "move_key", TenantId::Default())
                    .has_value());
    ResumeReplicaCleanup(*service);
}

TEST_F(MasterServiceTest, MoveEndReleasesSourceRefcountWhenTargetGone) {
    // The client-visible flow (MoveEnd fails with REPLICA_IS_GONE, a
    // subsequent UpsertStart succeeds, MoveRevoke reports no task) is covered
    // by the MoveEndWithVanishedTargetReleasesSource scenario. This test keeps
    // the private invariant: the source refcount must be released before the
    // move task is erased.
    auto service = std::make_unique<MasterService>();
    const auto source =
        PrepareSimpleSegment(*service, "refcnt_source", kDefaultSegmentBase);
    const auto target = PrepareSimpleSegment(
        *service, "refcnt_target", kDefaultSegmentBase + kDefaultSegmentSize);

    const UUID client_id = source.client_id;
    ReplicateConfig config;
    config.replica_num = 1;
    config.preferred_segment = "refcnt_source";
    PutCompletedObject(*service, client_id, "refcnt_key", config);

    ASSERT_TRUE(service
                    ->MoveStart(client_id, "refcnt_key", TenantId::Default(),
                                "refcnt_source", "refcnt_target")
                    .has_value());
    ASSERT_TRUE(service->UnmountSegment(target.segment_id, target.client_id)
                    .has_value());

    auto move_end =
        service->MoveEnd(client_id, "refcnt_key", TenantId::Default());
    ASSERT_FALSE(move_end.has_value());
    EXPECT_EQ(ErrorCode::REPLICA_IS_GONE, move_end.error());

    const auto source_refcnt =
        GetReplicaRefcntBySegmentName(*service, "refcnt_key", "refcnt_source");
    ASSERT_TRUE(source_refcnt.has_value());
    EXPECT_EQ(0, source_refcnt.value());
}

TEST_F(MasterServiceTest, PutStartPartialAllocationIsObservable) {
    std::unique_ptr<MasterService> service_(new MasterService());

    // Mount two segments only
    constexpr size_t buffer1 = 0x300000000;
    constexpr size_t buffer2 = 0x400000000;
    constexpr size_t segment_size = 1024 * 1024 * 64;  // 64MB

    auto segment1 = MakeSegment("segment1", buffer1, segment_size);
    auto segment2 = MakeSegment("segment2", buffer2, segment_size);
    UUID client_id = generate_uuid();
    ASSERT_TRUE(service_->MountSegment(segment1, client_id).has_value());
    ASSERT_TRUE(service_->MountSegment(segment2, client_id).has_value());

    auto& metrics = MasterMetricManager::instance();
    const int64_t partial_before = metrics.get_put_start_partial_allocations();

    // Request more replicas than available segments: best-effort keeps the
    // put successful but the degradation must be recorded.
    ReplicateConfig config;
    config.replica_num = 3;
    auto put_start_result = service_->PutStart(
        client_id, "partial_alloc_key", TenantId::Default(), 1024, config);
    ASSERT_TRUE(put_start_result.has_value());
    ASSERT_EQ(2u, put_start_result->size());
    ASSERT_EQ(metrics.get_put_start_partial_allocations(), partial_before + 1);

    // A fully satisfied allocation must not be counted as partial.
    ReplicateConfig full_config;
    full_config.replica_num = 2;
    auto full_result = service_->PutStart(
        client_id, "full_alloc_key", TenantId::Default(), 1024, full_config);
    ASSERT_TRUE(full_result.has_value());
    ASSERT_EQ(2u, full_result->size());
    ASSERT_EQ(metrics.get_put_start_partial_allocations(), partial_before + 1);
}

TEST_F(MasterServiceTest, UnmountSegmentPerformance) {
    std::unique_ptr<MasterService> service_(new MasterService());
    constexpr size_t kBufferAddress = 0x300000000;
    constexpr size_t kSegmentSize = 1024 * 1024 * 256;  // 256MB
    std::string segment_name = "perf_test_segment";
    auto segment = MakeSegment(segment_name, kBufferAddress, kSegmentSize);
    UUID client_id = generate_uuid();

    // Mount a segment for testing
    auto mount_result = service_->MountSegment(segment, client_id);
    ASSERT_TRUE(mount_result.has_value());

    // Create 10000 keys for testing
    constexpr int kNumKeys = 1000;
    std::vector<std::string> keys;
    keys.reserve(kNumKeys);

    auto start = std::chrono::steady_clock::now();

    // Create `kNumKeys` keys
    for (int i = 0; i < kNumKeys; ++i) {
        std::string key =
            GenerateKeyForSegment(client_id, service_, segment_name);
        keys.push_back(key);
    }

    auto create_end = std::chrono::steady_clock::now();

    // Execute unmount operation and record operation time
    auto unmount_start = std::chrono::steady_clock::now();
    auto unmount_result = service_->UnmountSegment(segment.id, client_id);
    EXPECT_TRUE(unmount_result.has_value());
    auto unmount_end = std::chrono::steady_clock::now();

    auto unmount_duration =
        std::chrono::duration_cast<std::chrono::milliseconds>(unmount_end -
                                                              unmount_start);

    // Unmount operation should be very fast, so we set 1s limit
    EXPECT_LE(unmount_duration.count(), 1000)
        << "Unmount operation took " << unmount_duration.count()
        << "ms which exceeds 1 second limit";

    // Verify all keys are gone
    for (const auto& key : keys) {
        auto get_result = service_->GetReplicaList(key, TenantId::Default());
        EXPECT_FALSE(get_result.has_value());
        EXPECT_EQ(ErrorCode::OBJECT_NOT_FOUND, get_result.error());
    }

    // Output performance report
    auto total_create_duration =
        std::chrono::duration_cast<std::chrono::milliseconds>(create_end -
                                                              start);
    std::cout << "\nPerformance Metrics:\n"
              << "Keys created: " << kNumKeys << "\n"
              << "Creation time: " << total_create_duration.count() << "ms\n"
              << "Unmount time: " << unmount_duration.count() << "ms\n";
}

TEST_F(MasterServiceTest, RemoveSoftPinObject) {
    const uint64_t kv_lease_ttl = 200;
    // set a large soft_pin_ttl so the granted soft pin will not quickly expire
    const uint64_t kv_soft_pin_ttl = 10000;
    const bool allow_evict_soft_pinned_objects = true;
    auto service_config = MasterServiceConfig::builder()
                              .set_default_kv_lease_ttl(kv_lease_ttl)
                              .set_default_kv_soft_pin_ttl(kv_soft_pin_ttl)
                              .set_allow_evict_soft_pinned_objects(
                                  allow_evict_soft_pinned_objects)
                              .build();
    std::unique_ptr<MasterService> service_(new MasterService(service_config));
    const UUID client_id = generate_uuid();
    // Mount segment and put an object
    constexpr size_t buffer = 0x300000000;
    constexpr size_t size = 1024 * 1024 * 16;
    [[maybe_unused]] const auto context =
        PrepareSimpleSegment(*service_, "test_segment", buffer, size);

    std::string key = "test_key";
    uint64_t slice_length = 1024;
    ReplicateConfig config;
    config.replica_num = 1;
    config.soft_pin_action = SoftPinAction::ENABLE;

    // Verify soft pin does not block remove
    ASSERT_TRUE(service_
                    ->PutStart(client_id, key, TenantId::Default(),
                               slice_length, config)
                    .has_value());
    ASSERT_TRUE(
        service_
            ->PutEnd(client_id, key, TenantId::Default(), ReplicaType::MEMORY)
            .has_value());
    EXPECT_EQ(SoftPinRegistrationCount(*service_), 1u);
    EXPECT_TRUE(service_->Remove(key, TenantId::Default()).has_value());
    EXPECT_EQ(SoftPinRegistrationCount(*service_), 0u);

    // Verify soft pin does not block RemoveAll
    ASSERT_TRUE(service_
                    ->PutStart(client_id, key, TenantId::Default(),
                               slice_length, config)
                    .has_value());
    ASSERT_TRUE(
        service_
            ->PutEnd(client_id, key, TenantId::Default(), ReplicaType::MEMORY)
            .has_value());
    EXPECT_EQ(SoftPinRegistrationCount(*service_), 1u);
    EXPECT_EQ(1, service_->RemoveAll());
    EXPECT_EQ(SoftPinRegistrationCount(*service_), 0u);
}

TEST_F(MasterServiceTest, SoftPinActionsCommitOnFirstReadableUpsert) {
    auto service_config = MasterServiceConfig::builder()
                              .set_default_kv_soft_pin_ttl(10000)
                              .build();
    std::unique_ptr<MasterService> service(new MasterService(service_config));
    [[maybe_unused]] const auto context = PrepareSimpleSegment(*service);
    const UUID client_id = generate_uuid();
    const int64_t baseline =
        MasterMetricManager::instance().get_soft_pin_key_count();

    ReplicateConfig enable;
    enable.soft_pin_action = SoftPinAction::ENABLE;
    enable.soft_pin_ttl_ms = 5000;
    ASSERT_TRUE(service
                    ->PutStart(client_id, "action_key", TenantId::Default(),
                               1024, enable)
                    .has_value());
    EXPECT_EQ(MasterMetricManager::instance().get_soft_pin_key_count(),
              baseline);
    const auto before_first_completion = std::chrono::system_clock::now();
    ASSERT_TRUE(service
                    ->PutEnd(client_id, "action_key", TenantId::Default(),
                             ReplicaType::MEMORY)
                    .has_value());
    const auto initial_deadline = GetSoftPinDeadline(*service, "action_key");
    ASSERT_TRUE(initial_deadline.has_value());
    EXPECT_GT(*initial_deadline,
              before_first_completion + std::chrono::seconds(4));
    EXPECT_LT(*initial_deadline,
              before_first_completion + std::chrono::seconds(9));
    EXPECT_EQ(MasterMetricManager::instance().get_soft_pin_key_count(),
              baseline + 1);

    ReplicateConfig preserve;
    ASSERT_TRUE(service
                    ->UpsertStart(client_id, "action_key", TenantId::Default(),
                                  1024, preserve)
                    .has_value());
    EXPECT_EQ(GetSoftPinDeadline(*service, "action_key"), initial_deadline);
    ASSERT_TRUE(service
                    ->UpsertEnd(client_id, "action_key", TenantId::Default(),
                                ReplicaType::MEMORY)
                    .has_value());
    EXPECT_EQ(GetSoftPinDeadline(*service, "action_key"), initial_deadline);

    ReplicateConfig disable;
    disable.soft_pin_action = SoftPinAction::DISABLE;
    ASSERT_TRUE(service
                    ->UpsertStart(client_id, "action_key", TenantId::Default(),
                                  2048, disable)
                    .has_value());
    EXPECT_TRUE(GetSoftPinDeadline(*service, "action_key").has_value());
    EXPECT_EQ(MasterMetricManager::instance().get_soft_pin_key_count(),
              baseline + 1);
    ASSERT_TRUE(service
                    ->UpsertEnd(client_id, "action_key", TenantId::Default(),
                                ReplicaType::MEMORY)
                    .has_value());
    EXPECT_FALSE(GetSoftPinDeadline(*service, "action_key").has_value());
    EXPECT_EQ(MasterMetricManager::instance().get_soft_pin_key_count(),
              baseline);

    ReplicateConfig enable_again;
    enable_again.soft_pin_action = SoftPinAction::ENABLE;
    enable_again.soft_pin_ttl_ms = 3000;
    ASSERT_TRUE(service
                    ->UpsertStart(client_id, "action_key", TenantId::Default(),
                                  2048, enable_again)
                    .has_value());
    EXPECT_FALSE(GetSoftPinDeadline(*service, "action_key").has_value());
    const auto before_enable_again = std::chrono::system_clock::now();
    ASSERT_TRUE(service
                    ->UpsertEnd(client_id, "action_key", TenantId::Default(),
                                ReplicaType::MEMORY)
                    .has_value());
    const auto enabled_again_deadline =
        GetSoftPinDeadline(*service, "action_key");
    ASSERT_TRUE(enabled_again_deadline.has_value());
    EXPECT_GT(*enabled_again_deadline,
              before_enable_again + std::chrono::seconds(2));
    EXPECT_LT(*enabled_again_deadline,
              before_enable_again + std::chrono::seconds(5));
    EXPECT_EQ(MasterMetricManager::instance().get_soft_pin_key_count(),
              baseline + 1);
}

TEST_F(MasterServiceTest, SoftPinDeadlineIndexExpiresOnlyDueEntries) {
    std::unique_ptr<MasterService> service(new MasterService());
    [[maybe_unused]] const auto context = PrepareSimpleSegment(*service);
    const UUID client_id = generate_uuid();
    const int64_t baseline =
        MasterMetricManager::instance().get_soft_pin_key_count();

    ReplicateConfig config;
    config.soft_pin_action = SoftPinAction::ENABLE;
    PutCompletedObject(*service, client_id, "deadline_key", config);

    ReplicateConfig grouped_config = config;
    grouped_config.group_ids =
        std::vector<std::string>{UnrelatedGroupId("grouped_deadline_key")};
    PutCompletedObject(*service, client_id, "grouped_deadline_key",
                       grouped_config);

    const auto first_deadline =
        std::chrono::system_clock::now() + std::chrono::hours(1);
    const auto second_deadline = first_deadline + std::chrono::seconds(1);
    SetSoftPinDeadlineForTest(*service, "deadline_key", first_deadline);
    SetSoftPinDeadlineForTest(*service, "grouped_deadline_key",
                              second_deadline);

    EXPECT_EQ(SoftPinRegistrationCount(*service), 2u);
    CleanupExpiredSoftPinsAt(*service, first_deadline);
    EXPECT_FALSE(GetSoftPinDeadline(*service, "deadline_key").has_value());
    EXPECT_EQ(GetSoftPinDeadline(*service, "grouped_deadline_key"),
              second_deadline);
    EXPECT_EQ(SoftPinRegistrationCount(*service), 1u);
    EXPECT_EQ(MasterMetricManager::instance().get_soft_pin_key_count(),
              baseline + 1);

    CleanupExpiredSoftPinsAt(*service, second_deadline);
    EXPECT_FALSE(
        GetSoftPinDeadline(*service, "grouped_deadline_key").has_value());
    EXPECT_EQ(SoftPinRegistrationCount(*service), 0u);
    EXPECT_EQ(MasterMetricManager::instance().get_soft_pin_key_count(),
              baseline);
}

TEST_F(MasterServiceTest, SoftPinTtlUpdateInvalidatesOldHeapEntry) {
    auto service_config = MasterServiceConfig::builder()
                              .set_default_kv_soft_pin_ttl(5000)
                              .build();
    std::unique_ptr<MasterService> service(new MasterService(service_config));
    [[maybe_unused]] const auto context = PrepareSimpleSegment(*service);
    const UUID client_id = generate_uuid();
    const int64_t baseline =
        MasterMetricManager::instance().get_soft_pin_key_count();

    ReplicateConfig enable;
    enable.soft_pin_action = SoftPinAction::ENABLE;
    PutCompletedObject(*service, client_id, "ttl_update_key", enable);
    const auto first_deadline = GetSoftPinDeadline(*service, "ttl_update_key");
    ASSERT_TRUE(first_deadline.has_value());

    enable.soft_pin_ttl_ms = 20000;
    ASSERT_TRUE(service
                    ->UpsertStart(client_id, "ttl_update_key",
                                  TenantId::Default(), 1024, enable)
                    .has_value());
    ASSERT_TRUE(service
                    ->UpsertEnd(client_id, "ttl_update_key",
                                TenantId::Default(), ReplicaType::MEMORY)
                    .has_value());
    const auto updated_deadline =
        GetSoftPinDeadline(*service, "ttl_update_key");
    ASSERT_TRUE(updated_deadline.has_value());
    EXPECT_GT(*updated_deadline, *first_deadline);
    EXPECT_EQ(SoftPinRegistrationCount(*service), 1u);
    EXPECT_GE(SoftPinDeadlineHeapSize(*service), 2u);
    EXPECT_EQ(MasterMetricManager::instance().get_soft_pin_key_count(),
              baseline + 1);

    CleanupExpiredSoftPinsAt(*service, *first_deadline);
    EXPECT_EQ(GetSoftPinDeadline(*service, "ttl_update_key"), updated_deadline);
    EXPECT_EQ(MasterMetricManager::instance().get_soft_pin_key_count(),
              baseline + 1);

    CleanupExpiredSoftPinsAt(*service, *updated_deadline);
    EXPECT_FALSE(GetSoftPinDeadline(*service, "ttl_update_key").has_value());
    EXPECT_EQ(MasterMetricManager::instance().get_soft_pin_key_count(),
              baseline);
}

TEST_F(MasterServiceTest,
       SizeChangingUpsertIndexesInheritedDeadlineBeforeCompletion) {
    std::unique_ptr<MasterService> service(new MasterService());
    [[maybe_unused]] const auto context = PrepareSimpleSegment(*service);
    const UUID client_id = generate_uuid();
    const int64_t baseline =
        MasterMetricManager::instance().get_soft_pin_key_count();

    ReplicateConfig enable;
    enable.soft_pin_action = SoftPinAction::ENABLE;
    PutCompletedObject(*service, client_id, "resize_pending", enable);
    const auto inherited_deadline =
        std::chrono::system_clock::now() + std::chrono::hours(1);
    SetSoftPinDeadlineForTest(*service, "resize_pending", inherited_deadline);

    ReplicateConfig preserve;
    ASSERT_TRUE(service
                    ->UpsertStart(client_id, "resize_pending",
                                  TenantId::Default(), 2048, preserve)
                    .has_value());
    EXPECT_EQ(GetSoftPinDeadline(*service, "resize_pending"),
              inherited_deadline);
    EXPECT_EQ(SoftPinRegistrationCount(*service), 1u);

    CleanupExpiredSoftPinsAt(*service, inherited_deadline);
    EXPECT_FALSE(GetSoftPinDeadline(*service, "resize_pending").has_value());
    EXPECT_EQ(MasterMetricManager::instance().get_soft_pin_key_count(),
              baseline);

    ASSERT_TRUE(service
                    ->UpsertEnd(client_id, "resize_pending",
                                TenantId::Default(), ReplicaType::MEMORY)
                    .has_value());
    EXPECT_FALSE(GetSoftPinDeadline(*service, "resize_pending").has_value());
    EXPECT_EQ(SoftPinRegistrationCount(*service), 0u);
}

TEST_F(MasterServiceTest, SoftPinDeadlineHeapCompactsRepeatedUpdates) {
    MasterService service;
    const auto base = std::chrono::system_clock::now();
    constexpr size_t kUpdates = 5000;
    for (size_t i = 0; i < kUpdates; ++i) {
        UpsertSoftPinDeadlineIndexForTest(
            service, "compaction_key", base + std::chrono::milliseconds(i + 1));
    }

    EXPECT_EQ(SoftPinRegistrationCount(service), 1u);
    EXPECT_LE(SoftPinDeadlineHeapSize(service), 4096u);
    EXPECT_EQ(PopExpiredSoftPinDeadlinesForTest(
                  service, base + std::chrono::milliseconds(kUpdates - 1)),
              0u);
    EXPECT_EQ(SoftPinRegistrationCount(service), 1u);
    EXPECT_EQ(PopExpiredSoftPinDeadlinesForTest(
                  service, base + std::chrono::milliseconds(kUpdates)),
              1u);
}

TEST_F(MasterServiceTest,
       ExpiredSoftPinIsNotCarriedAcrossUpsertMetadataReplacement) {
    std::unique_ptr<MasterService> service(new MasterService());
    [[maybe_unused]] const auto context = PrepareSimpleSegment(*service);
    const UUID client_a = generate_uuid();
    const UUID client_b = generate_uuid();
    const int64_t baseline =
        MasterMetricManager::instance().get_soft_pin_key_count();

    ReplicateConfig enable;
    enable.soft_pin_action = SoftPinAction::ENABLE;
    enable.soft_pin_ttl_ms = 10000;
    ReplicateConfig preserve;

    ASSERT_TRUE(
        service
            ->PutStart(client_a, "preempted", TenantId::Default(), 1024, enable)
            .has_value());
    ASSERT_TRUE(service
                    ->PutEnd(client_a, "preempted", TenantId::Default(),
                             ReplicaType::MEMORY)
                    .has_value());
    SetSoftPinDeadlineForTest(
        *service, "preempted",
        std::chrono::system_clock::now() - std::chrono::seconds(1));
    ASSERT_TRUE(service
                    ->UpsertStart(client_a, "preempted", TenantId::Default(),
                                  1024, preserve)
                    .has_value());
    ASSERT_TRUE(service
                    ->UpsertStart(client_b, "preempted", TenantId::Default(),
                                  1024, preserve)
                    .has_value());
    EXPECT_FALSE(GetSoftPinDeadline(*service, "preempted").has_value());
    EXPECT_EQ(MasterMetricManager::instance().get_soft_pin_key_count(),
              baseline);

    ASSERT_TRUE(
        service
            ->PutStart(client_a, "resized", TenantId::Default(), 1024, enable)
            .has_value());
    ASSERT_TRUE(service
                    ->PutEnd(client_a, "resized", TenantId::Default(),
                             ReplicaType::MEMORY)
                    .has_value());
    SetSoftPinDeadlineForTest(
        *service, "resized",
        std::chrono::system_clock::now() - std::chrono::seconds(1));
    ASSERT_TRUE(service
                    ->UpsertStart(client_a, "resized", TenantId::Default(),
                                  2048, preserve)
                    .has_value());
    EXPECT_FALSE(GetSoftPinDeadline(*service, "resized").has_value());
    EXPECT_EQ(MasterMetricManager::instance().get_soft_pin_key_count(),
              baseline);
}

TEST_F(MasterServiceTest, RepeatedPutEndDoesNotRefreshSoftPin) {
    std::unique_ptr<MasterService> service(new MasterService());
    [[maybe_unused]] const auto context = PrepareSimpleSegment(*service);
    const UUID client_id = generate_uuid();

    ReplicateConfig config;
    config.soft_pin_action = SoftPinAction::ENABLE;
    config.soft_pin_ttl_ms = 5000;
    ASSERT_TRUE(service
                    ->PutStart(client_id, "repeat_end", TenantId::Default(),
                               1024, config)
                    .has_value());
    ASSERT_TRUE(service
                    ->PutEnd(client_id, "repeat_end", TenantId::Default(),
                             ReplicaType::MEMORY)
                    .has_value());
    const auto first_deadline = GetSoftPinDeadline(*service, "repeat_end");
    ASSERT_TRUE(first_deadline.has_value());

    ASSERT_TRUE(service
                    ->PutEnd(client_id, "repeat_end", TenantId::Default(),
                             ReplicaType::MEMORY)
                    .has_value());
    EXPECT_EQ(GetSoftPinDeadline(*service, "repeat_end"), first_deadline);
}

TEST_F(MasterServiceTest, SoftPinExpiresAndGetDoesNotReactivate) {
    const uint64_t kv_lease_ttl = 200;
    const uint64_t kv_soft_pin_ttl = 20;
    auto service_config = MasterServiceConfig::builder()
                              .set_default_kv_lease_ttl(kv_lease_ttl)
                              .set_default_kv_soft_pin_ttl(kv_soft_pin_ttl)
                              .build();
    std::unique_ptr<MasterService> service_(new MasterService(service_config));
    const UUID client_id = generate_uuid();

    constexpr size_t buffer = 0x300000000;
    constexpr size_t segment_size = 1024 * 1024 * 16;
    constexpr size_t value_size = 1024;
    [[maybe_unused]] const auto context =
        PrepareSimpleSegment(*service_, "test_segment", buffer, segment_size);

    const int64_t baseline =
        MasterMetricManager::instance().get_soft_pin_key_count();
    ReplicateConfig config;
    config.soft_pin_action = SoftPinAction::ENABLE;
    ASSERT_TRUE(service_
                    ->PutStart(client_id, "pin_key", TenantId::Default(),
                               value_size, config)
                    .has_value());
    EXPECT_EQ(MasterMetricManager::instance().get_soft_pin_key_count(),
              baseline);
    ASSERT_TRUE(service_
                    ->PutEnd(client_id, "pin_key", TenantId::Default(),
                             ReplicaType::MEMORY)
                    .has_value());
    EXPECT_EQ(MasterMetricManager::instance().get_soft_pin_key_count(),
              baseline + 1);

    const auto deadline = GetSoftPinDeadline(*service_, "pin_key");
    ASSERT_TRUE(deadline.has_value());
    CleanupExpiredSoftPinsAt(*service_, *deadline);
    ASSERT_TRUE(
        service_->GetReplicaList("pin_key", TenantId::Default()).has_value());
    EXPECT_EQ(MasterMetricManager::instance().get_soft_pin_key_count(),
              baseline);

    ASSERT_TRUE(service_->ExistKey("pin_key", TenantId::Default()).value());
    EXPECT_EQ(MasterMetricManager::instance().get_soft_pin_key_count(),
              baseline);
    service_->RemoveAll();
}

TEST_F(MasterServiceTest, ExistKeyLeasesPinSegmentButProbeKeyDoesNot) {
    // Set a long lease TTL so leases granted by ExistKey will not expire
    // during the test.
    const uint64_t kv_lease_ttl = 2000;
    auto service_config = MasterServiceConfig::builder()
                              .set_default_kv_lease_ttl(kv_lease_ttl)
                              .build();
    constexpr size_t kSegmentSize = 4 * 1024 * 1024;
    constexpr size_t kObjectSize = 2 * 1024 * 1024;

    // ExistKey grants a read lease on every hit, so a scan-heavy client can
    // pin the entire segment: eviction cannot reclaim the probed objects and
    // new allocations fail.
    {
        std::unique_ptr<MasterService> service_(
            new MasterService(service_config));
        [[maybe_unused]] const auto context =
            PrepareSimpleSegment(*service_, "exist_lease_segment",
                                 kDefaultSegmentBase, kSegmentSize);
        const UUID client_id = generate_uuid();

        ReplicateConfig config;
        config.replica_num = 1;
        for (const auto& key : {"exist_key_a", "exist_key_b"}) {
            PutCompletedObject(*service_, client_id, key, config, kObjectSize);
            auto exists = service_->ExistKey(key, TenantId::Default());
            ASSERT_TRUE(exists.has_value());
            ASSERT_TRUE(exists.value());
        }

        ReplicateConfig trigger_config;
        trigger_config.replica_num = 1;
        auto trigger_result = service_->PutStart(
            client_id, "trigger_exist_eviction", TenantId::Default(),
            kObjectSize, trigger_config);
        ASSERT_FALSE(trigger_result.has_value());
        EXPECT_EQ(ErrorCode::NO_AVAILABLE_HANDLE, trigger_result.error());
    }

    // ProbeKey shares the lookup path but grants no lease, so the probed
    // objects stay evictable and the allocation eventually succeeds by
    // evicting them.
    {
        std::unique_ptr<MasterService> service_(
            new MasterService(service_config));
        [[maybe_unused]] const auto context = PrepareSimpleSegment(
            *service_, "probe_segment", kDefaultSegmentBase, kSegmentSize);
        const UUID client_id = generate_uuid();

        ReplicateConfig config;
        config.replica_num = 1;
        for (const auto& key : {"probe_key_a", "probe_key_b"}) {
            PutCompletedObject(*service_, client_id, key, config, kObjectSize);
            auto probed = service_->ProbeKey(key, TenantId::Default());
            ASSERT_TRUE(probed.has_value());
            ASSERT_TRUE(probed.value());
        }

        // A missing key reports false.
        auto missing =
            service_->ProbeKey("probe_missing_key", TenantId::Default());
        ASSERT_TRUE(missing.has_value());
        EXPECT_FALSE(missing.value());

        ReplicateConfig trigger_config;
        trigger_config.replica_num = 1;
        bool allocated = false;
        for (int i = 0; i < 40 && !allocated; ++i) {
            auto trigger_result = service_->PutStart(
                client_id, "trigger_probe_eviction_" + std::to_string(i),
                TenantId::Default(), kObjectSize, trigger_config);
            allocated = trigger_result.has_value();
            if (!allocated) {
                std::this_thread::sleep_for(std::chrono::milliseconds(50));
            }
        }
        EXPECT_TRUE(allocated);
    }
}

TEST_F(MasterServiceTest, BatchProbeKeyReportsPointInTimeExistence) {
    std::unique_ptr<MasterService> service_(new MasterService());

    constexpr size_t buffer = 0x300000000;
    constexpr size_t size = 1024 * 1024 * 16;
    auto segment = MakeSegment("probe_batch_segment", buffer, size);
    UUID client_id = generate_uuid();
    ASSERT_TRUE(service_->MountSegment(segment, client_id).has_value());

    const std::string existing_key = "probe_batch_existing_key";
    ReplicateConfig config;
    config.replica_num = 1;
    PutCompletedObject(*service_, client_id, existing_key, config);

    const std::string missing_key = "probe_batch_missing_key";
    auto results = service_->BatchProbeKey(
        {existing_key, missing_key, existing_key}, TenantId::Default());
    ASSERT_EQ(3u, results.size());
    ASSERT_TRUE(results[0].has_value());
    EXPECT_TRUE(*results[0]);
    ASSERT_TRUE(results[1].has_value());
    EXPECT_FALSE(*results[1]);
    ASSERT_TRUE(results[2].has_value());
    EXPECT_TRUE(*results[2]);
}

TEST_F(MasterServiceTest, WrappedBatchExistKeyUsesTenantAwareBatchPath) {
    const TenantId tenant_id("wrapped_batch_exist_tenant");
    auto service_config = MakeStrictWrappedConfig(
        {std::string(TenantId::kDefaultValue), tenant_id.value()});
    WrappedMasterService service_(service_config);

    Segment segment = MakeSegment("wrapped_batch_exist_segment");
    const UUID client_id = generate_uuid();
    ASSERT_TRUE(service_.MountSegment(segment, client_id).has_value());

    ReplicateConfig config;
    config.replica_num = 1;
    const std::string tenant_key_a = "wrapped_batch_tenant_a";
    const std::string tenant_key_b = "wrapped_batch_tenant_b";
    const std::string default_only_key = "wrapped_batch_default_only";
    const std::string missing_key = "wrapped_batch_missing";

    std::vector<std::string> tenant_keys = {tenant_key_a, tenant_key_b};
    std::vector<uint64_t> tenant_sizes = {1024, 2048};
    auto tenant_put_start = service_.BatchPutStart(
        client_id, tenant_keys, tenant_sizes, config, tenant_id.value());
    ASSERT_EQ(tenant_put_start.size(), tenant_keys.size());
    for (const auto& result : tenant_put_start) {
        ASSERT_TRUE(result.has_value()) << toString(result.error());
    }
    auto tenant_put_end =
        service_.BatchPutEnd(client_id, MakeObjectMetas(tenant_keys),
                             ReplicaType::MEMORY, tenant_id.value());
    ASSERT_EQ(tenant_put_end.size(), tenant_keys.size());
    for (const auto& result : tenant_put_end) {
        ASSERT_TRUE(result.has_value()) << toString(result.error());
    }

    auto default_put_start =
        service_.PutStart(client_id, default_only_key, 1024, config);
    ASSERT_TRUE(default_put_start.has_value());
    ASSERT_TRUE(service_
                    .PutEnd(client_id,
                            ObjectMeta{default_only_key, std::nullopt},
                            ReplicaType::MEMORY)
                    .has_value());

    auto& metrics = MasterMetricManager::instance();
    const auto base_requests = metrics.get_batch_exist_key_requests();
    const auto base_items = metrics.get_batch_exist_key_items();
    const auto base_failures = metrics.get_batch_exist_key_failures();
    const auto base_partial = metrics.get_batch_exist_key_partial_successes();
    const auto base_failed_items = metrics.get_batch_exist_key_failed_items();

    std::vector<std::string> lookup_keys = {tenant_key_a, default_only_key,
                                            missing_key, tenant_key_b};
    auto resp = service_.BatchExistKey(lookup_keys, tenant_id.value());
    ASSERT_EQ(resp.size(), lookup_keys.size());
    EXPECT_TRUE(resp[0].value());
    EXPECT_FALSE(resp[1].value());
    EXPECT_FALSE(resp[2].value());
    EXPECT_TRUE(resp[3].value());

    EXPECT_EQ(base_requests + 1, metrics.get_batch_exist_key_requests());
    EXPECT_EQ(base_items + lookup_keys.size(),
              metrics.get_batch_exist_key_items());
    EXPECT_EQ(base_failures, metrics.get_batch_exist_key_failures());
    EXPECT_EQ(base_partial, metrics.get_batch_exist_key_partial_successes());
    EXPECT_EQ(base_failed_items, metrics.get_batch_exist_key_failed_items());
}

TEST_F(MasterServiceTest, WrappedWriteBoundaryRejectsInvalidTenantIds) {
    WrappedMasterService service(
        MakeStrictWrappedConfig({"registered-tenant"}));
    ReplicateConfig config;
    config.replica_num = 1;
    const UUID client_id = generate_uuid();

    auto empty =
        service.PutStart(client_id, "empty-tenant-key", 1024, config, "");
    ASSERT_FALSE(empty.has_value());
    EXPECT_EQ(empty.error(), ErrorCode::TENANT_NOT_REGISTERED);

    const std::string control_tenant("tenant\0bad", 10);
    auto invalid = service.PutStart(client_id, "invalid-tenant-key", 1024,
                                    config, control_tenant);
    ASSERT_FALSE(invalid.has_value());
    EXPECT_EQ(invalid.error(), ErrorCode::TENANT_NOT_REGISTERED);
}

TEST_F(MasterServiceTest, WrappedRequestBoundaryRejectsInvalidTenantIds) {
    WrappedMasterService service(
        MakeStrictWrappedConfig({std::string(TenantId::kDefaultValue)}));

    auto invalid = service.GetReplicaList("missing-key", "_invalid-tenant");
    ASSERT_FALSE(invalid.has_value());
    EXPECT_EQ(invalid.error(), ErrorCode::INVALID_PARAMS);

    const std::string control_tenant("tenant\0bad", 10);
    auto batch =
        service.BatchGetReplicaList({"key-a", "key-b"}, control_tenant);
    ASSERT_EQ(batch.size(), 2u);
    for (const auto& result : batch) {
        ASSERT_FALSE(result.has_value());
        EXPECT_EQ(result.error(), ErrorCode::INVALID_PARAMS);
    }

    std::vector<OffloadTaskItem> tasks = {
        {.tenant_id = "_invalid-tenant", .key = "key", .size = 1}};
    std::vector<StorageObjectMetadata> metadatas = {
        {.bucket_id = 0,
         .offset = 0,
         .key_size = 3,
         .data_size = 1,
         .transport_endpoint = "segment"}};
    auto offload =
        service.NotifyOffloadSuccess(generate_uuid(), tasks, metadatas);
    ASSERT_FALSE(offload.has_value());
    EXPECT_EQ(offload.error(), ErrorCode::INVALID_PARAMS);

    // RemoveAll has a legacy scalar return type and cannot carry ErrorCode.
    EXPECT_EQ(service.RemoveAll(false, "_invalid-tenant"), 0);
}

TEST_F(MasterServiceTest, WrappedRequestBoundaryPreservesTenantNormalization) {
    WrappedMasterService multi_tenant_service(
        MakeStrictWrappedConfig({std::string(TenantId::kDefaultValue)}));
    auto empty = multi_tenant_service.ExistKey("missing-key", "");
    ASSERT_TRUE(empty.has_value());
    EXPECT_FALSE(empty.value());

    WrappedMasterServiceConfig single_tenant_config;
    single_tenant_config.default_kv_lease_ttl = 100;
    single_tenant_config.enable_metric_reporting = false;
    single_tenant_config.enable_multi_tenants = false;
    WrappedMasterService single_tenant_service(single_tenant_config);
    auto invalid =
        single_tenant_service.ExistKey("missing-key", "_invalid-tenant");
    ASSERT_TRUE(invalid.has_value());
    EXPECT_FALSE(invalid.value());
}

TEST_F(MasterServiceTest, PutStartExpiringTest) {
    // Reset storage space metrics.
    MasterMetricManager::instance().reset_allocated_mem_size();
    MasterMetricManager::instance().reset_total_mem_capacity();

    MasterServiceConfig master_config;
    master_config.put_start_discard_timeout_sec = 3;
    master_config.put_start_release_timeout_sec = 5;
    std::unique_ptr<MasterService> service_(new MasterService(master_config));

    constexpr size_t kReplicaCnt = 3;
    constexpr size_t kBaseAddr = 0x300000000;
    constexpr size_t kSegmentSize = 1024 * 1024 * 16;  // 16MB

    // Mount 3 segments.
    std::vector<MountedSegmentContext> contexts;
    contexts.reserve(kReplicaCnt);
    for (size_t i = 0; i < kReplicaCnt; ++i) {
        auto context = PrepareSimpleSegment(
            *service_, "segment_" + std::to_string(i),
            kBaseAddr + static_cast<size_t>(i) * kSegmentSize, kSegmentSize);
        contexts.push_back(context);
    }

    // The client_id used to put objects.
    auto client_id = generate_uuid();
    std::string key_1 = "test_key_1", key_2 = "test_key_2";
    uint64_t value_length = 6 * 1024 * 1024;  // 6MB
    uint64_t slice_length = value_length;
    ReplicateConfig config;
    config.replica_num = kReplicaCnt;

    // Put key_1, should success.
    auto put_start_result = service_->PutStart(
        client_id, key_1, TenantId::Default(), slice_length, config);
    EXPECT_TRUE(put_start_result.has_value());
    replica_list = put_start_result.value();
    EXPECT_EQ(replica_list.size(), kReplicaCnt);
    for (size_t i = 0; i < kReplicaCnt; i++) {
        EXPECT_EQ(ReplicaStatus::PROCESSING, replica_list[i].status);
    }

    // Put key_1 again, should fail because the key exists.
    put_start_result = service_->PutStart(client_id, key_1, TenantId::Default(),
                                          slice_length, config);
    EXPECT_FALSE(put_start_result.has_value());
    EXPECT_EQ(put_start_result.error(), ErrorCode::OBJECT_ALREADY_EXISTS);

    // Wait for a while until the put-start expired.
    for (size_t i = 0; i <= master_config.put_start_discard_timeout_sec; i++) {
        for (auto& context : contexts) {
            auto result = service_->Ping(context.client_id);
            EXPECT_TRUE(result.has_value());
        }
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }

    // Put key_1 again, should success because the old one has expired and will
    // be discarded by this put.
    put_start_result = service_->PutStart(client_id, key_1, TenantId::Default(),
                                          slice_length, config);
    EXPECT_TRUE(put_start_result.has_value());
    replica_list = put_start_result.value();
    EXPECT_EQ(replica_list.size(), kReplicaCnt);
    for (size_t i = 0; i < kReplicaCnt; i++) {
        EXPECT_EQ(ReplicaStatus::PROCESSING, replica_list[i].status);
    }

    // Complete key_1.
    auto put_end_result = service_->PutEnd(
        client_id, key_1, TenantId::Default(), ReplicaType::MEMORY);
    EXPECT_TRUE(put_end_result.has_value());

    // Protect key_1 from eviction.
    auto get_result = service_->GetReplicaList(key_1, TenantId::Default());
    EXPECT_TRUE(get_result.has_value());

    // Put key_2, should fail because the key_1 occupied 12MB (6MB processing,
    // 6MB discarded but not yet released) on each segment.
    put_start_result = service_->PutStart(client_id, key_2, TenantId::Default(),
                                          slice_length, config);
    EXPECT_FALSE(put_start_result.has_value());
    EXPECT_EQ(put_start_result.error(), ErrorCode::NO_AVAILABLE_HANDLE);

    // Wait for a while until the discarded replicas are released.
    for (size_t i = 0; i <= master_config.put_start_release_timeout_sec -
                                master_config.put_start_discard_timeout_sec;
         i++) {
        for (auto& context : contexts) {
            auto result = service_->Ping(context.client_id);
            EXPECT_TRUE(result.has_value());
        }
        // Protect key_1 from eviction.
        auto get_result = service_->GetReplicaList(key_1, TenantId::Default());
        EXPECT_TRUE(get_result.has_value());
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }

    // Put key_2 again, should success because the discarded replica has been
    // released.
    put_start_result = service_->PutStart(client_id, key_2, TenantId::Default(),
                                          slice_length, config);
    EXPECT_TRUE(put_start_result.has_value());
    replica_list = put_start_result.value();
    EXPECT_EQ(replica_list.size(), kReplicaCnt);
    for (size_t i = 0; i < kReplicaCnt; i++) {
        EXPECT_EQ(ReplicaStatus::PROCESSING, replica_list[i].status);
    }

    // Wait for a while until key_2 can be discarded and released.
    for (size_t i = 0; i <= master_config.put_start_release_timeout_sec; i++) {
        for (auto& context : contexts) {
            auto result = service_->Ping(context.client_id);
            EXPECT_TRUE(result.has_value());
        }
        // Protect key_1 from eviction.
        auto get_result = service_->GetReplicaList(key_1, TenantId::Default());
        EXPECT_TRUE(get_result.has_value());
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }

    // Put key_2 again, should fail because eviction has not been triggered. And
    // this PutStart should trigger the eviction. Only BatchEvict moves the
    // eviction attempt counter, so take the baseline before the trigger: the
    // eviction thread polls every 10 ms, and sampling after the failing
    // PutStart could race a completed BatchEvict and wait for a second one
    // that never comes.
    const int64_t eviction_attempts_before =
        MasterMetricManager::instance().get_mem_eviction_attempts();
    put_start_result = service_->PutStart(client_id, key_2, TenantId::Default(),
                                          slice_length, config);
    EXPECT_FALSE(put_start_result.has_value());
    EXPECT_EQ(put_start_result.error(), ErrorCode::NO_AVAILABLE_HANDLE);

    // The failed PutStart above sets need_mem_eviction_, and the eviction
    // thread answers with an asynchronous BatchEvict. Polling PutStart for up
    // to put_start_release_timeout_sec cannot tell that path apart from the
    // periodic DiscardExpiredProcessingReplicas fallback, which releases the
    // same replicas on the same 5 s scale and would pass the test without
    // exercising the immediate eviction, and the periodic path never touches
    // the attempt counter.
    WaitUntil([&] {
        return MasterMetricManager::instance().get_mem_eviction_attempts() >
               eviction_attempts_before;
    });
    put_start_result = service_->PutStart(client_id, key_2, TenantId::Default(),
                                          slice_length, config);
    ASSERT_TRUE(put_start_result.has_value())
        << toString(put_start_result.error());
    replica_list = put_start_result.value();
    EXPECT_EQ(replica_list.size(), kReplicaCnt);
    for (size_t i = 0; i < kReplicaCnt; i++) {
        EXPECT_EQ(ReplicaStatus::PROCESSING, replica_list[i].status);
    }

    // Complete key_2.
    put_end_result = service_->PutEnd(client_id, key_2, TenantId::Default(),
                                      ReplicaType::MEMORY);
    EXPECT_TRUE(put_end_result.has_value());
}

TEST_F(MasterServiceTest, TenantTasksCarryTenantInPayload) {
    const TenantId tenant_id("tenant_for_async_task");
    auto service = std::make_unique<MasterService>(
        MakeStrictTenantConfig({tenant_id.value()}));
    const auto ctx0 = PrepareSimpleSegment(*service, "segment_0", 0x300000000,
                                           kDefaultSegmentSize);
    [[maybe_unused]] const auto ctx1 = PrepareSimpleSegment(
        *service, "segment_1", 0x400000000, kDefaultSegmentSize);

    const UUID put_client_id = generate_uuid();
    const std::string key = "tenant_task_key";

    ReplicateConfig config;
    config.replica_num = 1;
    config.preferred_segment = "segment_0";

    ASSERT_TRUE(service
                    ->PutStart(put_client_id, key, tenant_id,
                               /*slice_length=*/1024, config)
                    .has_value());
    ASSERT_TRUE(
        service->PutEnd(put_client_id, key, tenant_id, ReplicaType::MEMORY)
            .has_value());

    auto copy_task_id = service->CreateCopyTask(key, tenant_id, {"segment_1"});
    ASSERT_TRUE(copy_task_id.has_value());
    auto move_task_id =
        service->CreateMoveTask(key, tenant_id, "segment_0", "segment_1");
    ASSERT_TRUE(move_task_id.has_value());

    auto fetched = service->FetchTasks(ctx0.client_id, /*batch_size=*/16);
    ASSERT_TRUE(fetched.has_value());
    ASSERT_EQ(fetched->size(), 2u);

    bool saw_copy = false;
    bool saw_move = false;
    for (const auto& assignment : *fetched) {
        if (assignment.id == copy_task_id.value()) {
            ReplicaCopyPayload payload;
            struct_json::from_json(payload, assignment.payload);
            EXPECT_EQ(payload.tenant_id, tenant_id.value());
            EXPECT_EQ(payload.key, key);
            saw_copy = true;
        } else if (assignment.id == move_task_id.value()) {
            ReplicaMovePayload payload;
            struct_json::from_json(payload, assignment.payload);
            EXPECT_EQ(payload.tenant_id, tenant_id.value());
            EXPECT_EQ(payload.key, key);
            saw_move = true;
        }
    }
    EXPECT_TRUE(saw_copy);
    EXPECT_TRUE(saw_move);
}

TEST_F(MasterServiceTest, LegacyTaskPayloadDefaultsTenant) {
    ReplicaCopyPayload copy_payload;
    struct_json::from_json(
        copy_payload,
        R"({"key":"legacy_copy_key","source":"segment_0","targets":["segment_1"]})");
    EXPECT_EQ(copy_payload.tenant_id, TenantId::kDefaultValue);
    EXPECT_EQ(copy_payload.key, "legacy_copy_key");
    EXPECT_EQ(copy_payload.source, "segment_0");
    ASSERT_EQ(copy_payload.targets.size(), 1u);
    EXPECT_EQ(copy_payload.targets[0], "segment_1");

    ReplicaMovePayload move_payload;
    struct_json::from_json(
        move_payload,
        R"({"key":"legacy_move_key","source":"segment_0","target":"segment_1"})");
    EXPECT_EQ(move_payload.tenant_id, TenantId::kDefaultValue);
    EXPECT_EQ(move_payload.key, "legacy_move_key");
    EXPECT_EQ(move_payload.source, "segment_0");
    EXPECT_EQ(move_payload.target, "segment_1");
}

TEST_F(MasterServiceTest,
       CreateDrainJobMarksSegmentDrainingAndSkipsAllocation) {
    auto service_config =
        MasterServiceConfig::builder().set_default_kv_lease_ttl(0).build();
    auto service_ = std::make_unique<MasterService>(service_config);

    const auto ctx0 = PrepareSimpleSegment(*service_, "segment_0", 0x300000000,
                                           kDefaultSegmentSize);
    [[maybe_unused]] const auto ctx1 = PrepareSimpleSegment(
        *service_, "segment_1", 0x400000000, kDefaultSegmentSize);

    CreateDrainJobRequest request;
    request.segments = {"segment_0"};
    request.target_segments = {"segment_1"};
    request.max_concurrency = 1;

    auto job_id = service_->CreateDrainJob(request);
    ASSERT_TRUE(job_id.has_value());

    auto segment_status = service_->QuerySegmentStatus("segment_0");
    ASSERT_TRUE(segment_status.has_value());
    EXPECT_EQ(segment_status.value(), SegmentStatus::DRAINING);

    ReplicateConfig config;
    config.replica_num = 1;
    config.preferred_segment = "segment_0";

    auto put_result =
        service_->PutStart(ctx0.client_id, "drain_skip_allocation_key",
                           TenantId::Default(), 1024, config);
    ASSERT_TRUE(put_result.has_value());
    ASSERT_EQ(put_result->size(), 1u);
    EXPECT_EQ(put_result->front()
                  .get_memory_descriptor()
                  .buffer_descriptor.transport_endpoint_,
              "segment_1");
    ASSERT_TRUE(service_
                    ->PutEnd(ctx0.client_id, "drain_skip_allocation_key",
                             TenantId::Default(), ReplicaType::MEMORY)
                    .has_value());
}

TEST_F(MasterServiceTest, DrainJobSchedulesMoveTaskAndConvergesToDrained) {
    auto service_config =
        MasterServiceConfig::builder().set_default_kv_lease_ttl(0).build();
    auto service_ = std::make_unique<MasterService>(service_config);

    const auto ctx0 = PrepareSimpleSegment(*service_, "segment_0", 0x300000000,
                                           kDefaultSegmentSize);
    [[maybe_unused]] const auto ctx1 = PrepareSimpleSegment(
        *service_, "segment_1", 0x400000000, kDefaultSegmentSize);

    const UUID put_client_id = generate_uuid();
    const std::string key =
        PutObjectOnSegment(*service_, put_client_id, "segment_0");

    CreateDrainJobRequest request;
    request.segments = {"segment_0"};
    request.target_segments = {"segment_1"};
    request.max_concurrency = 1;

    auto job_id = service_->CreateDrainJob(request);
    ASSERT_TRUE(job_id.has_value());

    WaitUntil(
        [&] { return ExecutePendingMoveTasks(*service_, ctx0.client_id); });

    WaitUntil([&] {
        auto query = service_->QueryDrainJob(job_id.value());
        return query.has_value() && query->status == JobStatus::SUCCEEDED;
    });

    auto query = service_->QueryDrainJob(job_id.value());
    ASSERT_TRUE(query.has_value());
    EXPECT_EQ(query->status, JobStatus::SUCCEEDED);
    EXPECT_EQ(query->active_units, 0u);
    EXPECT_GE(query->succeeded_units, 1u);

    auto segment_status = service_->QuerySegmentStatus("segment_0");
    ASSERT_TRUE(segment_status.has_value());
    EXPECT_EQ(segment_status.value(), SegmentStatus::DRAINED);

    auto replicas = service_->GetReplicaList(key, TenantId::Default());
    ASSERT_TRUE(replicas.has_value());
    std::unordered_set<std::string> segment_names;
    for (const auto& replica : replicas->replicas) {
        segment_names.insert(replica.get_memory_descriptor()
                                 .buffer_descriptor.transport_endpoint_);
    }
    EXPECT_TRUE(segment_names.contains("segment_1"));
    EXPECT_FALSE(segment_names.contains("segment_0"));
}

TEST_F(MasterServiceTest, CancelDrainJobRestoresSegmentStatus) {
    auto service_ = std::make_unique<MasterService>();

    [[maybe_unused]] const auto ctx0 = PrepareSimpleSegment(
        *service_, "segment_0", 0x300000000, kDefaultSegmentSize);
    [[maybe_unused]] const auto ctx1 = PrepareSimpleSegment(
        *service_, "segment_1", 0x400000000, kDefaultSegmentSize);

    CreateDrainJobRequest request;
    request.segments = {"segment_0"};
    request.target_segments = {"segment_1"};
    request.max_concurrency = 1;

    auto job_id = service_->CreateDrainJob(request);
    ASSERT_TRUE(job_id.has_value());

    auto draining_status = service_->QuerySegmentStatus("segment_0");
    ASSERT_TRUE(draining_status.has_value());
    EXPECT_EQ(draining_status.value(), SegmentStatus::DRAINING);

    auto cancel_result = service_->CancelDrainJob(job_id.value());
    ASSERT_TRUE(cancel_result.has_value());

    auto job = service_->QueryDrainJob(job_id.value());
    ASSERT_TRUE(job.has_value());
    EXPECT_EQ(job->status, JobStatus::CANCELED);

    auto restored_status = service_->QuerySegmentStatus("segment_0");
    ASSERT_TRUE(restored_status.has_value());
    EXPECT_EQ(restored_status.value(), SegmentStatus::OK);
}

TEST_F(MasterServiceTest, CancelDrainJobRejectsActiveMoveTasks) {
    auto service_config =
        MasterServiceConfig::builder().set_default_kv_lease_ttl(0).build();
    auto service_ = std::make_unique<MasterService>(service_config);

    const auto ctx0 = PrepareSimpleSegment(*service_, "segment_0", 0x300000000,
                                           kDefaultSegmentSize);
    [[maybe_unused]] const auto ctx1 = PrepareSimpleSegment(
        *service_, "segment_1", 0x400000000, kDefaultSegmentSize);

    const UUID put_client_id = generate_uuid();
    PutObjectOnSegment(*service_, put_client_id, "segment_0");

    CreateDrainJobRequest request;
    request.segments = {"segment_0"};
    request.target_segments = {"segment_1"};
    request.max_concurrency = 1;

    auto job_id = service_->CreateDrainJob(request);
    ASSERT_TRUE(job_id.has_value());

    WaitUntil([&] {
        auto fetched = service_->FetchTasks(ctx0.client_id, /*batch_size=*/16);
        return fetched.has_value() && !fetched->empty();
    });

    auto cancel_result = service_->CancelDrainJob(job_id.value());
    ASSERT_FALSE(cancel_result.has_value());
    EXPECT_EQ(cancel_result.error(), ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
}

TEST_F(MasterServiceTest, DrainJobFailsAfterRetryBudgetExhausted) {
    auto service_config =
        MasterServiceConfig::builder().set_default_kv_lease_ttl(0).build();
    auto service_ = std::make_unique<MasterService>(service_config);

    const auto ctx0 = PrepareSimpleSegment(*service_, "segment_0", 0x300000000,
                                           kDefaultSegmentSize);
    [[maybe_unused]] const auto ctx1 = PrepareSimpleSegment(
        *service_, "segment_1", 0x400000000, kDefaultSegmentSize);

    const UUID put_client_id = generate_uuid();
    PutObjectOnSegment(*service_, put_client_id, "segment_0");

    CreateDrainJobRequest request;
    request.segments = {"segment_0"};
    request.target_segments = {"segment_1"};
    request.max_concurrency = 1;

    auto job_id = service_->CreateDrainJob(request);
    ASSERT_TRUE(job_id.has_value());

    for (int attempt = 0; attempt < 3; ++attempt) {
        WaitUntil(
            [&] { return FailPendingMoveTasks(*service_, ctx0.client_id); });
    }

    WaitUntil([&] {
        auto query = service_->QueryDrainJob(job_id.value());
        return query.has_value() && query->status == JobStatus::FAILED;
    });

    auto query = service_->QueryDrainJob(job_id.value());
    ASSERT_TRUE(query.has_value());
    EXPECT_EQ(query->status, JobStatus::FAILED);
    EXPECT_EQ(query->active_units, 0u);
    EXPECT_GE(query->failed_units, 3u);

    auto segment_status = service_->QuerySegmentStatus("segment_0");
    ASSERT_TRUE(segment_status.has_value());
    EXPECT_EQ(segment_status.value(), SegmentStatus::OK);
}

TEST_F(MasterServiceTest, SetSegmentStatusStopsAndRestoresAllocation) {
    auto service_config =
        MasterServiceConfig::builder().set_default_kv_lease_ttl(0).build();
    auto service_ = std::make_unique<MasterService>(service_config);

    const auto ctx0 = PrepareSimpleSegment(*service_, "segment_0", 0x300000000,
                                           kDefaultSegmentSize);
    [[maybe_unused]] const auto ctx1 = PrepareSimpleSegment(
        *service_, "segment_1", 0x400000000, kDefaultSegmentSize);

    auto replica_endpoints = [&](const std::string& key) {
        std::unordered_set<std::string> endpoints;
        auto replicas = service_->GetReplicaList(key, TenantId::Default());
        EXPECT_TRUE(replicas.has_value());
        if (replicas.has_value()) {
            for (const auto& replica : replicas->replicas) {
                endpoints.insert(replica.get_memory_descriptor()
                                     .buffer_descriptor.transport_endpoint_);
            }
        }
        return endpoints;
    };
    auto put_preferring_segment_0 = [&] {
        return replica_endpoints(
            PutObjectOnSegment(*service_, ctx0.client_id, "segment_0"));
    };

    const std::string existing_key =
        PutObjectOnSegment(*service_, ctx0.client_id, "segment_0");

    ASSERT_TRUE(service_->SetSegmentStatus("segment_0", SegmentStatus::DRAINING)
                    .has_value());
    ASSERT_TRUE(service_->SetSegmentStatus("segment_0", SegmentStatus::DRAINING)
                    .has_value());
    auto status = service_->QuerySegmentStatus("segment_0");
    ASSERT_TRUE(status.has_value());
    EXPECT_EQ(status.value(), SegmentStatus::DRAINING);

    EXPECT_EQ(put_preferring_segment_0(),
              std::unordered_set<std::string>{"segment_1"});
    EXPECT_EQ(replica_endpoints(existing_key),
              std::unordered_set<std::string>{"segment_0"});

    ASSERT_TRUE(
        service_->SetSegmentStatus("segment_0", SegmentStatus::OK).has_value());
    status = service_->QuerySegmentStatus("segment_0");
    ASSERT_TRUE(status.has_value());
    EXPECT_EQ(status.value(), SegmentStatus::OK);

    EXPECT_EQ(put_preferring_segment_0(),
              std::unordered_set<std::string>{"segment_0"});
}

TEST_F(MasterServiceTest, SetSegmentStatusRejectsOtherTargetStatuses) {
    auto service_ = std::make_unique<MasterService>();
    [[maybe_unused]] const auto ctx0 = PrepareSimpleSegment(
        *service_, "segment_0", 0x300000000, kDefaultSegmentSize);

    for (auto target :
         {SegmentStatus::UNDEFINED, SegmentStatus::DRAINED,
          SegmentStatus::GRACEFULLY_UNMOUNTING, SegmentStatus::UNMOUNTING}) {
        auto result = service_->SetSegmentStatus("segment_0", target);
        ASSERT_FALSE(result.has_value());
        EXPECT_EQ(result.error(), ErrorCode::INVALID_PARAMS);
    }

    auto status = service_->QuerySegmentStatus("segment_0");
    ASSERT_TRUE(status.has_value());
    EXPECT_EQ(status.value(), SegmentStatus::OK);
}

TEST_F(MasterServiceTest, SetSegmentStatusRejectsUnknownSegment) {
    auto service_ = std::make_unique<MasterService>();

    auto result =
        service_->SetSegmentStatus("no_such_segment", SegmentStatus::DRAINING);
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::SEGMENT_NOT_FOUND);
}

TEST_F(MasterServiceTest, SetSegmentStatusRejectsSegmentUnderDrainJob) {
    auto service_config =
        MasterServiceConfig::builder().set_default_kv_lease_ttl(0).build();
    auto service_ = std::make_unique<MasterService>(service_config);

    const auto ctx0 = PrepareSimpleSegment(*service_, "segment_0", 0x300000000,
                                           kDefaultSegmentSize);
    [[maybe_unused]] const auto ctx1 = PrepareSimpleSegment(
        *service_, "segment_1", 0x400000000, kDefaultSegmentSize);

    // The object keeps the job unfinished because no one runs its move task.
    PutObjectOnSegment(*service_, ctx0.client_id, "segment_0");

    CreateDrainJobRequest request;
    request.segments = {"segment_0"};
    request.target_segments = {"segment_1"};
    request.max_concurrency = 1;
    auto job_id = service_->CreateDrainJob(request);
    ASSERT_TRUE(job_id.has_value());

    for (auto target : {SegmentStatus::OK, SegmentStatus::DRAINING}) {
        auto result = service_->SetSegmentStatus("segment_0", target);
        ASSERT_FALSE(result.has_value());
        EXPECT_EQ(result.error(), ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    }

    auto status = service_->QuerySegmentStatus("segment_0");
    ASSERT_TRUE(status.has_value());
    EXPECT_EQ(status.value(), SegmentStatus::DRAINING);
}

// ===================== Client Offboarding Tests =====================

TEST_F(MasterServiceTest, ClientOffboardingRetryPolicy) {
    EXPECT_EQ(ClientOffboardingRetryDelayForTest(1), std::chrono::seconds(1));
    EXPECT_EQ(ClientOffboardingRetryDelayForTest(2), std::chrono::seconds(2));
    EXPECT_EQ(ClientOffboardingRetryDelayForTest(3), std::chrono::seconds(4));
    EXPECT_EQ(ClientOffboardingRetryDelayForTest(4), std::chrono::seconds(8));
    EXPECT_EQ(ClientOffboardingRetryDelayForTest(5), std::chrono::seconds(16));
    EXPECT_EQ(ClientOffboardingRetryDelayForTest(6), std::chrono::seconds(30));
    EXPECT_EQ(ClientOffboardingRetryDelayForTest(100),
              std::chrono::seconds(30));
    EXPECT_FALSE(ClientOffboardingShouldAlertForTest(9));
    EXPECT_TRUE(ClientOffboardingShouldAlertForTest(10));
    EXPECT_TRUE(ClientOffboardingShouldAlertForTest(11));
}

TEST_F(MasterServiceTest, ReMountDoesNotRecoverSuspectedClient) {
    MasterService service;
    auto segment = MakeSegment("suspected_remount_segment");
    const UUID client_id = generate_uuid();
    ASSERT_TRUE(service.MountSegment(segment, client_id).has_value());

    const auto liveness = FindClientLivenessForTest(service, client_id);
    ASSERT_TRUE(liveness);
    ASSERT_EQ(
        liveness->Evaluate(ClientLivenessRecord::Clock::now(),
                           std::chrono::seconds::zero(), std::chrono::hours(1)),
        ClientLivenessTransition::BECAME_SUSPECTED);
    MasterMetricManager::instance().client_liveness_became_suspected();

    ASSERT_TRUE(service.ReMountSegment({segment}, client_id).has_value());
    EXPECT_EQ(liveness->state(), ClientLivenessState::SUSPECTED);
    EXPECT_EQ(service.Ping(client_id)->client_status, ClientStatus::OK);
    EXPECT_EQ(liveness->state(), ClientLivenessState::ACTIVE);
}

TEST_F(MasterServiceTest,
       InPlaceUpsertRejectsSuspectedTargetWithoutChangingMetadata) {
    MasterService service;
    auto segment = MakeSegment("suspected_upsert_segment");
    const UUID client_id = generate_uuid();
    ASSERT_TRUE(service.MountSegment(segment, client_id).has_value());

    const auto key = PutObjectOnSegment(service, client_id, segment.name);
    ReplicateConfig config;
    config.replica_num = 1;
    config.preferred_segment = segment.name;

    const auto liveness = FindClientLivenessForTest(service, client_id);
    ASSERT_TRUE(liveness);
    ASSERT_EQ(
        liveness->Evaluate(ClientLivenessRecord::Clock::now(),
                           std::chrono::seconds::zero(), std::chrono::hours(1)),
        ClientLivenessTransition::BECAME_SUSPECTED);
    MasterMetricManager::instance().client_liveness_became_suspected();

    auto upsert =
        service.UpsertStart(client_id, key, TenantId::Default(), 1024, config);
    ASSERT_FALSE(upsert.has_value());
    EXPECT_EQ(upsert.error(), ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);

    ASSERT_TRUE(service.Ping(client_id).has_value());
    auto get = service.GetReplicaList(key, TenantId::Default());
    ASSERT_TRUE(get.has_value()) << toString(get.error());
    ASSERT_EQ(get->replicas.size(), 1u);
    EXPECT_EQ(get->replicas.front().status, ReplicaStatus::COMPLETE);
}

TEST_F(MasterServiceTest,
       ClientOffboardingProcessesRealSegmentAndMetadataResiduals) {
    MasterService service;
    auto segment = MakeSegment("offboarding_segment");
    const UUID client_id = generate_uuid();
    ASSERT_TRUE(service.MountSegment(segment, client_id).has_value());

    const std::string key =
        PutObjectOnSegment(service, client_id, segment.name);
    const auto liveness = FindClientLivenessForTest(service, client_id);
    ASSERT_TRUE(liveness);

    ClientOffboardingJob job;
    job.client_id = client_id;
    job.liveness = liveness;
    job.pending_prepare_segments.push_back(
        {.segment_id = segment.id,
         .segment_name = segment.name,
         .transport_endpoint = segment.te_endpoint});

    ASSERT_TRUE(ProcessClientOffboardingForTest(service, job));
    EXPECT_TRUE(job.pending_prepare_segments.empty());
    EXPECT_TRUE(job.prepared_segments.empty());
    EXPECT_TRUE(job.metadata_cleanup_accepted);
    EXPECT_TRUE(job.local_ssd_unregistered);
    EXPECT_FALSE(FindClientLivenessForTest(service, client_id));
    EXPECT_FALSE(service.QuerySegmentStatusById(segment.id).has_value());

    auto exists = service.ExistKey(key, TenantId::Default());
    ASSERT_TRUE(exists.has_value());
    EXPECT_FALSE(*exists);
}

TEST_F(MasterServiceTest,
       ClientOffboardingKeepsPreparedResidualWithoutRepreparingIt) {
    MasterService service;
    const UUID client_id = generate_uuid();
    auto prepared_segment = MakeSegment("offboarding_prepared_segment");
    auto blocked_segment =
        MakeSegment("offboarding_blocked_segment", /*base=*/0x400000000);
    ASSERT_TRUE(service.MountSegment(prepared_segment, client_id).has_value());
    ASSERT_TRUE(service.MountSegment(blocked_segment, client_id).has_value());
    size_t blocked_metrics_dec_capacity = 0;
    ASSERT_EQ(ErrorCode::OK,
              PrepareUnmountSegmentForTest(service, blocked_segment.id,
                                           blocked_metrics_dec_capacity));

    ClientOffboardingJob job;
    job.client_id = client_id;
    job.liveness = FindClientLivenessForTest(service, client_id);
    ASSERT_TRUE(job.liveness);
    job.pending_prepare_segments = {
        {.segment_id = prepared_segment.id,
         .segment_name = prepared_segment.name,
         .transport_endpoint = prepared_segment.te_endpoint},
        {.segment_id = blocked_segment.id,
         .segment_name = blocked_segment.name,
         .transport_endpoint = blocked_segment.te_endpoint}};

    ASSERT_FALSE(ProcessClientOffboardingForTest(service, job));
    ASSERT_EQ(job.prepared_segments.size(), 1u);
    ASSERT_EQ(job.pending_prepare_segments.size(), 1u);
    EXPECT_EQ(job.prepared_segments.front().segment_id, prepared_segment.id);
    EXPECT_EQ(job.pending_prepare_segments.front().segment_id,
              blocked_segment.id);
    const auto retained_capacity =
        job.prepared_segments.front().metrics_dec_capacity;

    ASSERT_FALSE(ProcessClientOffboardingForTest(service, job));
    ASSERT_EQ(job.prepared_segments.size(), 1u);
    ASSERT_EQ(job.pending_prepare_segments.size(), 1u);
    EXPECT_EQ(job.prepared_segments.front().segment_id, prepared_segment.id);
    EXPECT_EQ(job.prepared_segments.front().metrics_dec_capacity,
              retained_capacity);

    ASSERT_EQ(ErrorCode::OK, CommitUnmountSegmentForTest(
                                 service, blocked_segment.id, client_id,
                                 blocked_metrics_dec_capacity));
    ASSERT_TRUE(ProcessClientOffboardingForTest(service, job));
    EXPECT_TRUE(job.prepared_segments.empty());
    EXPECT_TRUE(job.pending_prepare_segments.empty());
    EXPECT_FALSE(FindClientLivenessForTest(service, client_id));
}

}  // namespace mooncake::test

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
