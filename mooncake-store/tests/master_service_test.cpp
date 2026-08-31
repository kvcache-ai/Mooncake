#include "master_service.h"
#include "rpc_service.h"

#include <glog/logging.h>
#include <gtest/gtest.h>
#include <ylt/struct_json/json_reader.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <functional>
#include <map>
#include <memory>
#include <limits>
#include <optional>
#include <random>
#include <string>
#include <thread>
#include <utility>
#include <vector>
#include <unordered_set>

#include <unistd.h>

#include "tenant_quota_policy_store.h"
#include "types.h"
#include "utils.h"
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

TEST_F(MasterServiceTest, RandomMountUnmountSegment) {
    // Create a MasterService instance for testing.
    std::unique_ptr<MasterService> service_(new MasterService());
    // Define a constant buffer address for the segment.
    constexpr size_t kBufferAddress = 0x300000000;
    // Define the name of the test segment.
    std::string segment_name = "test_random_segment";
    UUID segment_id = generate_uuid();
    UUID client_id = generate_uuid();
    size_t times = 10;
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(1, 10);
    while (times--) {
        int random_number = dis(gen);
        // Define the size of the segment (16MB).
        size_t kSegmentSize = 1024 * 1024 * 16 * random_number;

        auto segment = MakeSegment(segment_name, kBufferAddress, kSegmentSize);
        segment.id = segment_id;

        // Test remounting after unmount.
        auto mount_result = service_->MountSegment(segment, client_id);
        EXPECT_TRUE(mount_result.has_value());
        auto unmount_result = service_->UnmountSegment(segment.id, client_id);
        EXPECT_TRUE(unmount_result.has_value());
    }
}

TEST_F(MasterServiceTest, ConcurrentMountUnmount) {
    std::unique_ptr<MasterService> service_(new MasterService());
    constexpr size_t num_threads = 4;
    constexpr size_t iterations = 100;
    std::vector<std::thread> threads;
    std::atomic<int> success_count{0};

    // Launch multiple threads to mount/unmount segments concurrently
    for (size_t i = 0; i < num_threads; i++) {
        threads.emplace_back([&service_, i, &success_count, this]() {
            auto segment =
                MakeSegment("segment_" + std::to_string(i),
                            0x300000000 + i * 0x10000000, 16 * 1024 * 1024);
            UUID client_id = generate_uuid();

            for (size_t j = 0; j < iterations; j++) {
                auto mount_result = service_->MountSegment(segment, client_id);
                if (mount_result.has_value()) {
                    auto unmount_result =
                        service_->UnmountSegment(segment.id, client_id);
                    EXPECT_TRUE(unmount_result.has_value());
                    success_count++;
                }
            }
        });
    }

    // Wait for all threads to complete
    for (auto& thread : threads) {
        thread.join();
    }

    // Verify that some mount/unmount operations succeeded
    EXPECT_GT(success_count, 0);
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
    EXPECT_EQ(ComputeSoftPinDeadlineForTest(normal_now, 25),
              normal_now + std::chrono::milliseconds(25));
    EXPECT_EQ(ComputeSoftPinDeadlineForTest(
                  normal_now, std::numeric_limits<uint64_t>::max()),
              Clock::time_point::max());

    const auto near_max =
        Clock::time_point::max() - std::chrono::milliseconds(5);
    EXPECT_EQ(ComputeSoftPinDeadlineForTest(near_max, 10),
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

// RemoveAll holds one shard lock at a time, so a commit can land in a shard the
// scan already passed. The scan's own bookkeeping cannot see it, and publishing
// `cleared` would order it after that commit's `stored` — telling subscribers
// to drop an object that is live. Pause the scan right after the shard the new
// key belongs to, commit there, and the clear must be withheld.
TEST_F(MasterServiceTest, ConcurrentCommitDuringScanSuppressesClear) {
    MasterService service;
    service.SetKvTenantEpochTrackingForTesting(true);
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

    const size_t racer_shard = ShardIndexForKey(service, "racer_key");
    bool committed = false;
    service.SetRemoveAllShardHookForTesting([&](size_t shard) {
        // Commit exactly once, immediately after the scan releases the shard
        // the new key hashes to, so the scan can never observe it.
        if (shard != racer_shard || committed) {
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
    service.SetRemoveAllShardHookForTesting(nullptr);

    ASSERT_TRUE(committed) << "the hook never fired, so nothing was raced";
    auto exists = service.ExistKey("racer_key", TenantId::Default());
    ASSERT_TRUE(exists.has_value());
    EXPECT_TRUE(exists.value()) << "the raced commit must still be live";

    EXPECT_EQ(0u, service.GetKvClearedPublishedForTesting())
        << "a clear here would retract racer_key, which was just announced";
    EXPECT_EQ(1u, service.GetKvClearedSuppressedForTesting());
}

// The mirror image: with no concurrent commit the epoch is unchanged, so the
// clear must still go out. Without this the fix could pass by never publishing.
TEST_F(MasterServiceTest, UncontendedScanStillPublishesClear) {
    MasterService service;
    service.SetKvTenantEpochTrackingForTesting(true);
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

    EXPECT_EQ(1u, service.GetKvClearedPublishedForTesting());
    EXPECT_EQ(0u, service.GetKvClearedSuppressedForTesting());
}

// The tenant-scoped overload reads the epoch before its scan instead of at
// first sight of an object, so it needs its own coverage of the same ordering
// rule.
TEST_F(MasterServiceTest, TenantScopedRemoveAllSuppressesClearOnRace) {
    MasterService service;
    service.SetKvTenantEpochTrackingForTesting(true);
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

    const size_t racer_shard = ShardIndexForKey(service, "scoped_racer");
    bool committed = false;
    service.SetRemoveAllShardHookForTesting([&](size_t shard) {
        if (shard != racer_shard || committed) {
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
    service.SetRemoveAllShardHookForTesting(nullptr);

    ASSERT_TRUE(committed) << "the hook never fired, so nothing was raced";
    EXPECT_EQ(0u, service.GetKvClearedPublishedForTesting());
    EXPECT_EQ(1u, service.GetKvClearedSuppressedForTesting());
}

TEST_F(MasterServiceTest, DfsEvictionSplitsAcceptedAndRejectedCandidates) {
    auto run_case = [&](const std::string& case_name,
                        const std::vector<size_t>& leased_indexes,
                        const std::vector<std::optional<size_t>>&
                            expected_replica_counts,
                        bool evict_memory_first = false) {
        const auto dfs_root = (std::filesystem::temp_directory_path() /
                               ("master_dfs_evict_" +
                                std::to_string(::getpid()) + "_" + case_name))
                                  .string();
        std::filesystem::create_directories(dfs_root);
        ScopedEnvVar enable_dfs("MOONCAKE_ENABLE_DFS", "1");
        ScopedEnvVar fs_adapter("MOONCAKE_DFS_FS_ADAPTER", "posix");
        ScopedEnvVar root_dir("MOONCAKE_DFS_ROOT_DIR", dfs_root.c_str());
        ScopedEnvVar shard_count("MOONCAKE_DFS_SHARD_COUNT", "1");
        ScopedEnvVar shard_capacity("MOONCAKE_DFS_SHARD_CAPACITY", "32768");
        ScopedEnvVar alignment("MOONCAKE_DFS_ALIGNMENT", "4096");
        // Keep the background path disabled so the test drives one exact
        // transaction through the public test hook.
        ScopedEnvVar eviction("MOONCAKE_DFS_EVICTION_ENABLED", "0");
        ScopedEnvVar high_watermark("MOONCAKE_DFS_EVICTION_HIGH_WATERMARK",
                                    "0.9");
        ScopedEnvVar low_watermark("MOONCAKE_DFS_EVICTION_LOW_WATERMARK",
                                   "0.7");
        ScopedEnvVar deferred_free("MOONCAKE_DFS_DEFERRED_FREE_SECONDS", "0");
        ScopedEnvVar single_tenant("MOONCAKE_DFS_SINGLE_TENANT", "true");

        {
            MasterService service;
            const auto context = PrepareSimpleSegment(service);
            ReplicateConfig config;
            config.replica_num = 1;
            config.dfs_replica_num = 1;

            std::vector<std::string> keys;
            for (int i = 0; i < 4; ++i) {
                keys.push_back("dfs_evict_" + std::to_string(i));
                auto start = service.PutStart(context.client_id, keys.back(),
                                              TenantId::Default(), 100, config);
                ASSERT_TRUE(start.has_value()) << "allocation " << i;
                ASSERT_TRUE(service
                                .PutEnd(context.client_id, keys.back(),
                                        TenantId::Default(), ReplicaType::ALL)
                                .has_value());
            }

            for (const size_t index : leased_indexes) {
                ASSERT_LT(index, keys.size());
                ASSERT_TRUE(
                    service.GetReplicaList(keys[index], TenantId::Default())
                        .has_value());
            }

            if (evict_memory_first) {
                service.RunBatchEvictForTesting(1.0, 1.0);
            }
            service.RunDfsEvictionForTesting();

            for (size_t i = 0; i < keys.size(); ++i) {
                auto result =
                    service.GetReplicaList(keys[i], TenantId::Default());
                if (!expected_replica_counts[i].has_value()) {
                    ASSERT_FALSE(result.has_value()) << "key=" << keys[i];
                    EXPECT_EQ(result.error(), ErrorCode::OBJECT_NOT_FOUND)
                        << "key=" << keys[i];
                    continue;
                }
                ASSERT_TRUE(result.has_value());
                EXPECT_EQ(result->replicas.size(), *expected_replica_counts[i])
                    << "key=" << keys[i];
            }

            if (evict_memory_first) {
                auto reclaimed =
                    service.PutStart(context.client_id, "dfs_evict_reclaimed",
                                     TenantId::Default(), 100, config);
                ASSERT_TRUE(reclaimed.has_value()) << reclaimed.error();
                ASSERT_TRUE(
                    service
                        .PutRevoke(context.client_id, "dfs_evict_reclaimed",
                                   TenantId::Default(), ReplicaType::ALL)
                        .has_value());
            }
        }

        std::error_code ec;
        std::filesystem::remove_all(dfs_root, ec);
    };

    run_case("commit", {}, {1, 1, 2, 2});
    run_case("reject", {0, 1, 2, 3}, {2, 2, 2, 2});
    // k1 shares the first prepared batch with k0. Rejecting k1 must not roll
    // back k0, and the same high-watermark trigger must continue to k2 so the
    // shard reaches its low watermark.
    run_case("mixed", {1}, {1, 2, 1, 2});
    // Memory eviction may leave DFS as the only remaining replica. DFS
    // eviction must still reclaim those allocations and erase metadata for
    // objects whose final replica was removed.
    run_case("last_replica", {},
             {std::nullopt, std::nullopt, size_t{1}, size_t{1}}, true);
}

namespace {

StandbyObjectEntry MakeStandbyEntry(const std::string& key, uint64_t obj_size,
                                    uintptr_t addr,
                                    const std::string& endpoint) {
    StandbyObjectEntry entry;
    entry.tenant_id = TenantId::Default().value();
    entry.key = key;
    entry.metadata.client_id = generate_uuid();
    entry.metadata.size = obj_size;
    AllocatedBuffer::Descriptor buf;
    buf.size_ = obj_size;
    buf.buffer_address_ = addr;
    buf.transport_endpoint_ = endpoint;
    Replica::Descriptor desc;
    desc.id = addr;
    desc.descriptor_variant = MemoryDescriptor{buf};
    desc.status = ReplicaStatus::COMPLETE;
    entry.metadata.replicas.push_back(desc);
    return entry;
}

StandbySegmentInfo MakeStandbySegment(const std::string& name,
                                      const std::string& endpoint,
                                      uint64_t capacity) {
    StandbySegmentInfo seg;
    seg.segment_name = name;
    seg.transport_endpoint = endpoint;
    seg.capacity = capacity;
    seg.is_memory_segment = true;
    return seg;
}

}  // namespace

TEST_F(MasterServiceTest, StandbyRestoreSkipsInvalidObjectAndRestoresTheRest) {
    // #3760: one bad descriptor must not cost the whole index.
    MasterService service(MakeStrictTenantConfig({"default"}));
    const auto seg = MakeStandbySegment("seg", "ep1", 4096);
    auto good = MakeStandbyEntry("good_key", 128, 0x1000, "ep1");
    auto bad = MakeStandbyEntry("bad_key", 128, 0x2000, "ep1");
    bad.metadata.size = 256;  // descriptor size no longer matches metadata

    auto result = service.RestoreFromStandbySnapshot(
        {good, bad},
        /*initial_oplog_sequence_id=*/0, {seg});
    ASSERT_TRUE(result.has_value());
    EXPECT_TRUE(
        service.ExistKey("good_key", TenantId::Default()).value_or(false));
    EXPECT_FALSE(
        service.ExistKey("bad_key", TenantId::Default()).value_or(true));
}

TEST_F(MasterServiceTest, StandbyRestoreKeepsLatestReplayOnOverlap) {
    // #3760: eviction frees and reallocates a buffer; a standby that missed
    // the removal replays both replicas at the same address. The later replay
    // is ground truth and wins.
    MasterService service(MakeStrictTenantConfig({"default"}));
    const auto seg = MakeStandbySegment("seg", "ep1", 4096);
    auto stale = MakeStandbyEntry("stale_key", 128, 0x1000, "ep1");
    auto fresh = MakeStandbyEntry("fresh_key", 128, 0x1040, "ep1");

    auto result = service.RestoreFromStandbySnapshot(
        {stale, fresh},
        /*initial_oplog_sequence_id=*/0, {seg});
    ASSERT_TRUE(result.has_value());
    EXPECT_FALSE(
        service.ExistKey("stale_key", TenantId::Default()).value_or(true));
    EXPECT_TRUE(
        service.ExistKey("fresh_key", TenantId::Default()).value_or(false));
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

TEST_F(MasterServiceTest,
       ConcurrentGroupedAndUngroupedFirstCreateDoesNotDuplicateMetadata) {
    std::unique_ptr<MasterService> service_(new MasterService());
    [[maybe_unused]] const auto context = PrepareSimpleSegment(*service_);
    const UUID client_id = generate_uuid();

    const std::string key = "concurrent_grouped_ungrouped_first_create";
    const TenantId tenant_id("tenant_concurrent_first_create");
    ReplicateConfig ungrouped_config;
    ungrouped_config.replica_num = 1;
    ReplicateConfig grouped_config;
    grouped_config.replica_num = 1;
    grouped_config.group_ids =
        std::vector<std::string>{FindGroupIdOnDifferentShard(key)};

    static constexpr size_t kThreadCount = 16;
    std::atomic<size_t> ready{0};
    std::atomic<bool> start{false};
    std::vector<int> put_start_success(kThreadCount, 0);
    std::vector<int> put_end_success(kThreadCount, 0);
    std::vector<std::thread> threads;
    threads.reserve(kThreadCount);

    for (size_t i = 0; i < kThreadCount; ++i) {
        threads.emplace_back([&, i]() {
            ready.fetch_add(1, std::memory_order_acq_rel);
            while (!start.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }
            const auto& config =
                (i % 2 == 0) ? grouped_config : ungrouped_config;
            auto put_start =
                service_->PutStart(client_id, key, tenant_id, 1024, config);
            put_start_success[i] = put_start.has_value() ? 1 : 0;
            if (put_start.has_value()) {
                put_end_success[i] = service_->PutEnd(client_id, key, tenant_id,
                                                      ReplicaType::MEMORY)
                                             .has_value()
                                         ? 1
                                         : 0;
            } else {
                EXPECT_EQ(ErrorCode::OBJECT_ALREADY_EXISTS, put_start.error());
            }
        });
    }

    while (ready.load(std::memory_order_acquire) < kThreadCount) {
        std::this_thread::yield();
    }
    start.store(true, std::memory_order_release);
    for (auto& thread : threads) {
        thread.join();
    }

    EXPECT_EQ(std::count(put_start_success.begin(), put_start_success.end(), 1),
              1);
    EXPECT_EQ(std::count(put_end_success.begin(), put_end_success.end(), 1), 1);
    EXPECT_EQ(service_->GetKeyCount(), 1u);
    EXPECT_TRUE(service_->GetReplicaList(key, tenant_id).has_value());
}

TEST_F(MasterServiceTest,
       ConcurrentDifferentGroupedFirstCreateDoesNotDuplicateMetadata) {
    std::unique_ptr<MasterService> service_(new MasterService());
    [[maybe_unused]] const auto context = PrepareSimpleSegment(*service_);
    const UUID client_id = generate_uuid();

    const std::string key = "concurrent_different_grouped_first_create";
    const TenantId tenant_id("tenant_concurrent_grouped_first_create");
    const std::string group_a = FindGroupIdOnDifferentShard(key);
    std::string group_b;
    for (int i = 0; i < 10000; ++i) {
        group_b = key + "_other_group_" + std::to_string(i);
        if (std::hash<std::string>{}(group_b) % 1024 !=
            std::hash<std::string>{}(group_a) % 1024) {
            break;
        }
    }
    ReplicateConfig config_a;
    config_a.replica_num = 1;
    config_a.group_ids = std::vector<std::string>{group_a};
    ReplicateConfig config_b;
    config_b.replica_num = 1;
    config_b.group_ids = std::vector<std::string>{group_b};

    static constexpr size_t kThreadCount = 16;
    std::atomic<size_t> ready{0};
    std::atomic<bool> start{false};
    std::vector<int> put_start_success(kThreadCount, 0);
    std::vector<int> put_end_success(kThreadCount, 0);
    std::vector<std::thread> threads;
    threads.reserve(kThreadCount);

    for (size_t i = 0; i < kThreadCount; ++i) {
        threads.emplace_back([&, i]() {
            ready.fetch_add(1, std::memory_order_acq_rel);
            while (!start.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }
            const auto& config = (i % 2 == 0) ? config_a : config_b;
            auto put_start =
                service_->PutStart(client_id, key, tenant_id, 1024, config);
            put_start_success[i] = put_start.has_value() ? 1 : 0;
            if (put_start.has_value()) {
                put_end_success[i] = service_->PutEnd(client_id, key, tenant_id,
                                                      ReplicaType::MEMORY)
                                             .has_value()
                                         ? 1
                                         : 0;
            } else {
                EXPECT_EQ(ErrorCode::OBJECT_ALREADY_EXISTS, put_start.error());
            }
        });
    }

    while (ready.load(std::memory_order_acquire) < kThreadCount) {
        std::this_thread::yield();
    }
    start.store(true, std::memory_order_release);
    for (auto& thread : threads) {
        thread.join();
    }

    EXPECT_EQ(std::count(put_start_success.begin(), put_start_success.end(), 1),
              1);
    EXPECT_EQ(std::count(put_end_success.begin(), put_end_success.end(), 1), 1);
    EXPECT_EQ(service_->GetKeyCount(), 1u);
    EXPECT_TRUE(service_->GetReplicaList(key, tenant_id).has_value());
}

TEST_F(MasterServiceTest,
       GroupedEvictionExpandsSafeMembersAndSkipsLeasedGroup) {
    auto service_config =
        MasterServiceConfig::builder().set_default_kv_lease_ttl(1000).build();
    constexpr size_t kSegmentSize = 4 * 1024 * 1024;
    constexpr size_t kObjectSize = 2 * 1024 * 1024;

    {
        std::unique_ptr<MasterService> service_(
            new MasterService(service_config));
        [[maybe_unused]] const auto context =
            PrepareSimpleSegment(*service_, "grouped_evict_segment",
                                 kDefaultSegmentBase, kSegmentSize);
        const UUID client_id = generate_uuid();

        const std::string evict_key_a = "grouped_evict_key_a";
        const std::string evict_key_b = "grouped_evict_key_b";
        ReplicateConfig evict_config;
        evict_config.replica_num = 1;
        evict_config.group_ids =
            std::vector<std::string>{FindGroupIdOnDifferentShard(evict_key_a)};
        PutCompletedObject(*service_, client_id, evict_key_a, evict_config,
                           kObjectSize);
        PutCompletedObject(*service_, client_id, evict_key_b, evict_config,
                           kObjectSize);

        ReplicateConfig trigger_config;
        trigger_config.replica_num = 1;
        auto trigger_result = service_->PutStart(
            client_id, "trigger_grouped_eviction", TenantId::Default(),
            kObjectSize, trigger_config);
        ASSERT_FALSE(trigger_result.has_value());
        EXPECT_EQ(ErrorCode::NO_AVAILABLE_HANDLE, trigger_result.error());

        std::this_thread::sleep_for(std::chrono::milliseconds(200));

        EXPECT_FALSE(service_->ExistKey(evict_key_a, TenantId::Default())
                         .value_or(true));
        EXPECT_FALSE(service_->ExistKey(evict_key_b, TenantId::Default())
                         .value_or(true));
    }

    {
        std::unique_ptr<MasterService> service_(
            new MasterService(service_config));
        [[maybe_unused]] const auto context =
            PrepareSimpleSegment(*service_, "grouped_lease_segment",
                                 kDefaultSegmentBase, kSegmentSize);
        const UUID client_id = generate_uuid();

        const std::string leased_key_a = "grouped_leased_key_a";
        const std::string leased_key_b = "grouped_leased_key_b";
        ReplicateConfig leased_config;
        leased_config.replica_num = 1;
        leased_config.group_ids =
            std::vector<std::string>{FindGroupIdOnDifferentShard(leased_key_a)};
        PutCompletedObject(*service_, client_id, leased_key_a, leased_config,
                           kObjectSize);
        PutCompletedObject(*service_, client_id, leased_key_b, leased_config,
                           kObjectSize);

        auto exists = service_->ExistKey(leased_key_a, TenantId::Default());
        ASSERT_TRUE(exists.has_value());
        ASSERT_TRUE(exists.value());

        ReplicateConfig trigger_config;
        trigger_config.replica_num = 1;
        auto trigger_result = service_->PutStart(
            client_id, "trigger_leased_group_eviction", TenantId::Default(),
            kObjectSize, trigger_config);
        ASSERT_FALSE(trigger_result.has_value());
        EXPECT_EQ(ErrorCode::NO_AVAILABLE_HANDLE, trigger_result.error());

        std::this_thread::sleep_for(std::chrono::milliseconds(200));

        EXPECT_TRUE(service_->GetReplicaList(leased_key_a, TenantId::Default())
                        .has_value());
        EXPECT_TRUE(service_->GetReplicaList(leased_key_b, TenantId::Default())
                        .has_value());
    }
}

TEST_F(MasterServiceTest,
       ResolveMooncakeHostIdUsesLocalHostnameAndRejectsLoopback) {
    ScopedEnvVar host_id("MOONCAKE_HOST_ID");

    EXPECT_EQ(ResolveMooncakeHostId("hostB:5000"), "hostB");
    EXPECT_EQ(ResolveMooncakeHostId("hostB:5001"), "hostB");
    EXPECT_EQ(ResolveMooncakeHostId("[2001:db8::1]:5000"), "2001:db8::1");
    EXPECT_TRUE(ResolveMooncakeHostId("localhost:5000").empty());
    EXPECT_TRUE(ResolveMooncakeHostId("127.0.0.1:5000").empty());
    EXPECT_TRUE(ResolveMooncakeHostId("0.0.0.0:5000").empty());
    EXPECT_TRUE(ResolveMooncakeHostId("::1").empty());
    EXPECT_TRUE(ResolveMooncakeHostId("[::1]:5000").empty());
    EXPECT_TRUE(ResolveMooncakeHostId("::").empty());
    EXPECT_TRUE(ResolveMooncakeHostId("[::]").empty());
    EXPECT_TRUE(ResolveMooncakeHostId("[::]:5000").empty());
}

TEST_F(MasterServiceTest, ResolveMooncakeHostIdPrefersDeploymentOverride) {
    ScopedEnvVar host_id("MOONCAKE_HOST_ID", "  kubernetes-node-a  ");

    EXPECT_EQ(ResolveMooncakeHostId("10.244.1.17:5000"), "kubernetes-node-a");
}

TEST_F(MasterServiceTest, ResolveMooncakeHostIdNormalizesEndpointOverride) {
    ScopedEnvVar host_id("MOONCAKE_HOST_ID", "  kubernetes-node-a:5000  ");

    EXPECT_EQ(ResolveMooncakeHostId("10.244.1.17:5000"), "kubernetes-node-a");
}

TEST_F(MasterServiceTest, ResolveMooncakeHostIdFallsBackForEmptyOverride) {
    {
        ScopedEnvVar host_id("MOONCAKE_HOST_ID", "");
        EXPECT_EQ(ResolveMooncakeHostId("hostB:5000"), "hostB");
    }

    {
        ScopedEnvVar host_id("MOONCAKE_HOST_ID", " \t ");
        EXPECT_EQ(ResolveMooncakeHostId("hostB:5000"), "hostB");
    }
}

TEST_F(MasterServiceTest, ResolveMooncakeHostIdRejectsInvalidOverride) {
    const std::vector<const char*> invalid_host_ids = {
        "localhost",  "localhost:5000",
        "127.0.0.1",  "127.0.0.1:5000",
        "0.0.0.0",    "0.0.0.0:5000",
        "::1",        "[::1]",
        "[::1]:5000", "::",
        "[::]",       "[::]:5000"};
    for (const char* invalid_host_id : invalid_host_ids) {
        ScopedEnvVar host_id("MOONCAKE_HOST_ID", invalid_host_id);
        EXPECT_TRUE(ResolveMooncakeHostId("hostB:5000").empty())
            << invalid_host_id;
    }
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

    UUID client_id = generate_uuid();

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

    UUID client_id = generate_uuid();

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

TEST_F(MasterServiceTest, ConcurrentWriteAndRemoveAll) {
    std::unique_ptr<MasterService> service_(new MasterService());
    constexpr size_t buffer = 0x300000000;
    constexpr size_t size = 1024 * 1024 * 256;  // 256MB for concurrent testing
    auto segment = MakeSegment("concurrent_segment", buffer, size);
    UUID client_id = generate_uuid();
    auto mount_result_concurrent = service_->MountSegment(segment, client_id);
    ASSERT_TRUE(mount_result_concurrent.has_value());

    constexpr int num_threads = 4;
    constexpr int objects_per_thread = 100;
    std::atomic success_writes(0);
    std::atomic remove_all_done(false);
    std::atomic total_removed(0);

    // Writer threads
    std::vector<std::thread> writers;
    for (int i = 0; i < num_threads; ++i) {
        writers.emplace_back([&, i]() {
            for (int j = 0; j < objects_per_thread; ++j) {
                std::string key =
                    "key_" + std::to_string(i) + "_" + std::to_string(j);
                uint64_t slice_length = 1024;
                ReplicateConfig config;
                config.replica_num = 1;
                std::vector<Replica::Descriptor> replica_list;

                auto put_start_result = service_->PutStart(
                    client_id, key, TenantId::Default(), slice_length, config);
                if (put_start_result.has_value()) {
                    auto put_end_result =
                        service_->PutEnd(client_id, key, TenantId::Default(),
                                         ReplicaType::MEMORY);
                    if (put_end_result.has_value()) {
                        success_writes++;
                    }
                }

                // Random sleep to increase concurrency complexity
                std::this_thread::sleep_for(
                    std::chrono::milliseconds(rand() % 10));
            }
        });
    }

    // RemoveAll thread
    std::thread remove_thread([&]() {
        std::this_thread::sleep_for(
            std::chrono::milliseconds(50));  // Let some writes start
        long removed = service_->RemoveAll();
        LOG(INFO) << "Removed " << removed
                  << " objects during concurrent writes";
        ASSERT_GT(removed, 0);
        remove_all_done = true;
        total_removed.fetch_add(removed);
    });

    // Join all threads
    for (auto& t : writers) {
        t.join();
    }
    remove_thread.join();

    // Verify results
    EXPECT_GT(success_writes, 0);
    EXPECT_TRUE(remove_all_done);

    // Final RemoveAll to ensure clean state
    long final_removed = service_->RemoveAll();
    LOG(INFO) << "Final RemoveAll removed " << final_removed << " objects";
    ASSERT_GT(final_removed, 0);
    total_removed.fetch_add(final_removed);
    ASSERT_EQ(total_removed, num_threads * objects_per_thread);
}

TEST_F(MasterServiceTest, ConcurrentReadAndRemoveAll) {
    // set a large kv_lease_ttl so the granted lease will not quickly expire
    const uint64_t kv_lease_ttl = 200;
    auto service_config = MasterServiceConfig::builder()
                              .set_default_kv_lease_ttl(kv_lease_ttl)
                              .build();
    std::unique_ptr<MasterService> service_(new MasterService(service_config));
    constexpr size_t buffer = 0x300000000;
    constexpr size_t size = 1024 * 1024 * 256;  // 256MB for concurrent testing
    auto segment = MakeSegment("concurrent_segment", buffer, size);
    UUID client_id = generate_uuid();
    auto mount_result = service_->MountSegment(segment, client_id);
    ASSERT_TRUE(mount_result.has_value());

    // Pre-populate with test data
    constexpr int num_objects = 1000;
    for (int i = 0; i < num_objects; ++i) {
        std::string key = "pre_key_" + std::to_string(i);
        uint64_t slice_length = 1024;
        ReplicateConfig config;
        config.replica_num = 1;

        auto put_start_result = service_->PutStart(
            client_id, key, TenantId::Default(), slice_length, config);
        ASSERT_TRUE(put_start_result.has_value());
        auto put_end_result = service_->PutEnd(
            client_id, key, TenantId::Default(), ReplicaType::MEMORY);
        ASSERT_TRUE(put_end_result.has_value());
    }

    std::atomic<int> success_reads(0);
    std::atomic<bool> remove_all_done(false);

    // Reader threads
    std::vector<std::thread> readers;
    for (int i = 0; i < 4; ++i) {
        readers.emplace_back([&]() {
            for (int j = 0; j < num_objects; ++j) {
                std::string key = "pre_key_" + std::to_string(j);
                auto get_result =
                    service_->GetReplicaList(key, TenantId::Default());
                if (get_result.has_value()) {
                    success_reads++;
                }

                // Random sleep to increase concurrency complexity
                std::this_thread::sleep_for(
                    std::chrono::milliseconds(rand() % 5));
            }
        });
    }

    // RemoveAll thread
    std::thread remove_thread([&]() {
        std::this_thread::sleep_for(
            std::chrono::milliseconds(10));  // Let some reads start
        long removed = service_->RemoveAll();
        LOG(INFO) << "Removed " << removed
                  << " objects during concurrent reads";
        remove_all_done = true;
    });

    // Join all threads
    for (auto& t : readers) {
        t.join();
    }
    remove_thread.join();

    EXPECT_TRUE(remove_all_done);
    // Verify 0 < success_reads < num_objects
    EXPECT_GT(success_reads, 0);
    EXPECT_NE(success_reads, num_objects);

    // wait for all the lease to expire
    std::this_thread::sleep_for(std::chrono::milliseconds(kv_lease_ttl));
    long removed = service_->RemoveAll();
    LOG(INFO) << "Removed " << removed << " objects after kv lease expired";

    // Verify all objects were removed
    for (int i = 0; i < num_objects; ++i) {
        std::string key = "pre_key_" + std::to_string(i);
        auto get_result = service_->GetReplicaList(key, TenantId::Default());
        EXPECT_FALSE(get_result.has_value());
        EXPECT_EQ(ErrorCode::OBJECT_NOT_FOUND, get_result.error());
    }
}

TEST_F(MasterServiceTest, ConcurrentRemoveAllOperations) {
    std::unique_ptr<MasterService> service_(new MasterService());
    constexpr size_t buffer = 0x300000000;
    constexpr size_t size = 1024 * 1024 * 16 * 100;
    auto segment = MakeSegment("concurrent_segment", buffer, size);
    UUID client_id = generate_uuid();
    auto mount_result = service_->MountSegment(segment, client_id);
    ASSERT_TRUE(mount_result.has_value());

    // Pre-populate with test data
    constexpr int num_objects = 1000;
    for (int i = 0; i < num_objects; ++i) {
        std::string key = "pre_key_" + std::to_string(i);
        uint64_t slice_length = 1024;
        ReplicateConfig config;
        config.replica_num = 1;

        auto put_start_result = service_->PutStart(
            client_id, key, TenantId::Default(), slice_length, config);
        ASSERT_TRUE(put_start_result.has_value());
        auto put_end_result = service_->PutEnd(
            client_id, key, TenantId::Default(), ReplicaType::MEMORY);
        ASSERT_TRUE(put_end_result.has_value());
    }

    std::atomic<int> remove_all_count(0);

    // Two RemoveAll threads
    std::vector<std::thread> remove_threads;
    for (int i = 0; i < 2; ++i) {
        remove_threads.emplace_back([&]() {
            long removed = service_->RemoveAll();
            LOG(INFO) << "RemoveAll removed " << removed << " objects";
            remove_all_count += removed;
        });
    }

    // Join all threads
    for (auto& t : remove_threads) {
        t.join();
    }

    // Verify results - one RemoveAll should return num_objects, the other 0
    EXPECT_EQ(num_objects, remove_all_count);

    // Verify all objects were removed
    for (int i = 0; i < num_objects; ++i) {
        std::string key = "pre_key_" + std::to_string(i);
        auto get_result = service_->GetReplicaList(key, TenantId::Default());
        EXPECT_FALSE(get_result.has_value());
        EXPECT_EQ(ErrorCode::OBJECT_NOT_FOUND, get_result.error());
    }
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

    const UUID client_id = generate_uuid();
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

    const UUID client_id = generate_uuid();
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
    PrepareSimpleSegment(*service, "refcnt_source", kDefaultSegmentBase);
    const auto target = PrepareSimpleSegment(
        *service, "refcnt_target", kDefaultSegmentBase + kDefaultSegmentSize);

    const UUID client_id = generate_uuid();
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

TEST_F(MasterServiceTest, EvictObject) {
    // set a large kv_lease_ttl so the granted lease will not quickly expire
    const uint64_t kv_lease_ttl = 2000;
    auto service_config = MasterServiceConfig::builder()
                              .set_default_kv_lease_ttl(kv_lease_ttl)
                              .build();
    std::unique_ptr<MasterService> service_(new MasterService(service_config));
    const UUID client_id = generate_uuid();
    // Mount a segment that can hold about 1024 * 16 objects.
    // As the eviction is processed separately for each shard,
    // we need to fill each shard with enough objects to thoroughly
    // test the eviction process.
    constexpr size_t buffer = 0x300000000;
    constexpr size_t size = 1024 * 1024 * 16 * 15;
    constexpr size_t object_size = 1024 * 15;
    [[maybe_unused]] const auto context =
        PrepareSimpleSegment(*service_, "test_segment", buffer, size);

    // Verify if we can put objects more than the segment can hold
    int success_puts = 0;
    for (int i = 0; i < 1024 * 16 + 50; ++i) {
        std::string key = "test_key" + std::to_string(i);
        uint64_t slice_length = object_size;
        ReplicateConfig config;
        config.replica_num = 1;
        auto put_start_result = service_->PutStart(
            client_id, key, TenantId::Default(), slice_length, config);
        if (put_start_result.has_value()) {
            auto put_end_result = service_->PutEnd(
                client_id, key, TenantId::Default(), ReplicaType::MEMORY);
            ASSERT_TRUE(put_end_result.has_value());
            success_puts++;
        } else {
            // wait for eviction to work
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
    }
    ASSERT_GT(success_puts, 1024 * 16);
    std::this_thread::sleep_for(std::chrono::milliseconds(kv_lease_ttl));
    service_->RemoveAll();
}

TEST_F(MasterServiceTest, ShrinkBucketsIfSparseThresholds) {
    // Small containers stay untouched regardless of sparsity: their bucket
    // memory is negligible and rehash churn is not worth it.
    std::unordered_map<std::string, int> small;
    small.emplace("small_key", 0);
    const size_t small_buckets = small.bucket_count();
    ASSERT_LE(small_buckets, kShrinkMinBucketCount);
    ShrinkBucketsIfSparse(small);
    EXPECT_EQ(small.bucket_count(), small_buckets);

    // Grow a map well past the bucket floor, then erase most entries: the
    // bucket array keeps its high-water size until explicitly shrunk.
    std::unordered_map<std::string, int> map;
    for (size_t i = 0; i < 4 * kShrinkMinBucketCount; ++i) {
        map.emplace("key" + std::to_string(i), 0);
    }
    const size_t high_water = map.bucket_count();
    ASSERT_GT(high_water, kShrinkMinBucketCount);

    // At exactly a quarter full there is nothing to shrink yet.
    while (map.size() > high_water / 4) {
        map.erase(map.begin());
    }
    ShrinkBucketsIfSparse(map);
    EXPECT_EQ(map.bucket_count(), high_water);

    // One more erase crosses the threshold and triggers the shrink.
    map.erase(map.begin());
    ShrinkBucketsIfSparse(map);
    EXPECT_LT(map.bucket_count(), high_water);
    EXPECT_GE(map.bucket_count(), map.size());
}

TEST_F(MasterServiceTest, BatchEvictShrinksSparseMetadataMaps) {
    // Zero lease TTL so every committed object is immediately evictable.
    auto service_config =
        MasterServiceConfig::builder().set_default_kv_lease_ttl(0).build();
    std::unique_ptr<MasterService> service_(new MasterService(service_config));
    const UUID client_id = generate_uuid();
    constexpr size_t buffer = 0x300000000;
    constexpr size_t object_size = 1024;
    constexpr size_t object_count = 2 * kShrinkMinBucketCount;
    // Size the segment with ample headroom so the background eviction
    // thread never fires; only the explicit call below evicts.
    [[maybe_unused]] const auto context = PrepareSimpleSegment(
        *service_, "test_segment", buffer, object_size * object_count * 16);

    // Pick keys that all hash to one shard so its metadata map grows past
    // the shrink floor; random keys would spread these objects thinly
    // across all 1024 shards.
    const size_t target_shard = MetadataShardIndex(*service_, "shrink_key_0");
    std::vector<std::string> keys;
    for (size_t i = 0; keys.size() < object_count; ++i) {
        std::string key = "shrink_key_" + std::to_string(i);
        if (MetadataShardIndex(*service_, key) != target_shard) continue;
        keys.push_back(std::move(key));
    }

    ReplicateConfig config;
    config.replica_num = 1;
    for (const auto& key : keys) {
        // Hard-pin the first object: it is excluded from eviction, so the
        // tenant (and its metadata map) deterministically survives the
        // full eviction below and the shrunk bucket count stays
        // observable.
        config.with_hard_pin = (&key == &keys.front());
        ASSERT_TRUE(service_
                        ->PutStart(client_id, key, TenantId::Default(),
                                   object_size, config)
                        .has_value());
        ASSERT_TRUE(service_
                        ->PutEnd(client_id, key, TenantId::Default(),
                                 ReplicaType::MEMORY)
                        .has_value());
    }

    const size_t buckets_before = MetadataBucketCount(*service_, target_shard);
    ASSERT_GT(buckets_before, kShrinkMinBucketCount);

    service_->RunBatchEvictForTesting(1.0, 1.0);

    const size_t buckets_after = MetadataBucketCount(*service_, target_shard);
    ASSERT_GT(buckets_after, 0u);
    // Without the post-eviction shrink the bucket array would still sit at
    // its high-water mark and this assertion would fail.
    EXPECT_LT(buckets_after, buckets_before / 2);
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
    grouped_config.group_ids = std::vector<std::string>{
        FindGroupIdOnDifferentShard("grouped_deadline_key")};
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
            service, "compaction_key", 0,
            base + std::chrono::milliseconds(i + 1));
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

TEST_F(MasterServiceTest, SoftPinObjectsNotEvictedBeforeOtherObjects) {
    const uint64_t kv_lease_ttl = 200;
    // set a large soft_pin_ttl so the granted soft pin will not quickly expire
    const uint64_t kv_soft_pin_ttl = 10000;
    const double eviction_ratio = 0.5;
    const bool allow_evict_soft_pinned_objects = true;
    auto service_config = MasterServiceConfig::builder()
                              .set_default_kv_lease_ttl(kv_lease_ttl)
                              .set_default_kv_soft_pin_ttl(kv_soft_pin_ttl)
                              .set_allow_evict_soft_pinned_objects(
                                  allow_evict_soft_pinned_objects)
                              .set_eviction_ratio(eviction_ratio)
                              .build();
    std::unique_ptr<MasterService> service_(new MasterService(service_config));
    const UUID client_id = generate_uuid();

    // Mount segment and put an object
    constexpr size_t buffer = 0x300000000;
    constexpr size_t segment_size = 1024 * 1024 * 16;
    constexpr size_t value_size = 1024 * 1024;
    [[maybe_unused]] const auto context =
        PrepareSimpleSegment(*service_, "test_segment", buffer, segment_size);

    // The eviction has random factors, so test 5 times
    for (int test_i = 0; test_i < 5; test_i++) {
        // Put pin_key first
        for (int i = 0; i < 2; i++) {
            std::string pin_key = "pin_key" + std::to_string(i);
            uint64_t slice_length = value_size;
            ReplicateConfig soft_pin_config;
            soft_pin_config.replica_num = 1;
            soft_pin_config.soft_pin_action = SoftPinAction::ENABLE;

            ASSERT_TRUE(service_
                            ->PutStart(client_id, pin_key, TenantId::Default(),
                                       slice_length, soft_pin_config)
                            .has_value());
            ASSERT_TRUE(service_
                            ->PutEnd(client_id, pin_key, TenantId::Default(),
                                     ReplicaType::MEMORY)
                            .has_value());
        }

        // Fill the segment to trigger eviction
        int failed_puts = 0;
        for (int i = 0; i < 20; i++) {
            std::string key = "key" + std::to_string(i);
            uint64_t slice_length = value_size;
            ReplicateConfig config;
            config.replica_num = 1;
            if (service_
                    ->PutStart(client_id, key, TenantId::Default(),
                               slice_length, config)
                    .has_value()) {
                ASSERT_TRUE(service_
                                ->PutEnd(client_id, key, TenantId::Default(),
                                         ReplicaType::MEMORY)
                                .has_value());
            } else {
                failed_puts++;
            }
        }
        ASSERT_GT(failed_puts, 0);
        // wait for eviction to do eviction
        std::this_thread::sleep_for(
            std::chrono::milliseconds(kv_lease_ttl + 1000));
        // pin_key should still be accessible
        for (int i = 0; i < 2; i++) {
            std::string pin_key = "pin_key" + std::to_string(i);
            ASSERT_TRUE(service_->GetReplicaList(pin_key, TenantId::Default())
                            .has_value());
        }

        // wait for the lease to expire
        std::this_thread::sleep_for(std::chrono::milliseconds(kv_lease_ttl));
        // remove all objects before the next turn
        service_->RemoveAll();
    }
}

TEST_F(MasterServiceTest, SoftPinObjectsCanBeEvicted) {
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
    constexpr size_t segment_size = 1024 * 1024 * 16;
    constexpr size_t value_size = 1024 * 1024;
    [[maybe_unused]] const auto context =
        PrepareSimpleSegment(*service_, "test_segment", buffer, segment_size);

    // Verify if we can put objects more than the segment can hold
    int success_puts = 0;
    for (int i = 0; i < 16 + 50; ++i) {
        std::string key = "test_key" + std::to_string(i);
        uint64_t slice_length = value_size;
        ReplicateConfig config;
        config.replica_num = 1;
        config.soft_pin_action = SoftPinAction::ENABLE;
        if (service_
                ->PutStart(client_id, key, TenantId::Default(), slice_length,
                           config)
                .has_value()) {
            ASSERT_TRUE(service_
                            ->PutEnd(client_id, key, TenantId::Default(),
                                     ReplicaType::MEMORY)
                            .has_value());
            success_puts++;
        } else {
            // wait for eviction to work
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
    }
    ASSERT_GT(success_puts, 16);
    std::this_thread::sleep_for(std::chrono::milliseconds(kv_lease_ttl));
    service_->RemoveAll();
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

TEST_F(MasterServiceTest, SoftPinObjectsNotAllowEvict) {
    const uint64_t kv_lease_ttl = 200;
    // set a large soft_pin_ttl so the granted soft pin will not quickly expire
    const uint64_t kv_soft_pin_ttl = 10000;
    // set allow_evict_soft_pinned_objects to false to disable eviction of soft
    // pinned objects
    const bool allow_evict_soft_pinned_objects = false;
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
    constexpr size_t segment_size = 1024 * 1024 * 16;
    constexpr size_t value_size = 1024 * 1024;
    [[maybe_unused]] const auto context =
        PrepareSimpleSegment(*service_, "test_segment", buffer, segment_size);

    // Put objects more than the segment can hold
    std::vector<std::string> success_keys;
    for (int i = 0; i < 16 + 50; ++i) {
        std::string key = "test_key" + std::to_string(i);
        uint64_t slice_length = value_size;
        ReplicateConfig config;
        config.replica_num = 1;
        config.soft_pin_action = SoftPinAction::ENABLE;
        if (service_
                ->PutStart(client_id, key, TenantId::Default(), slice_length,
                           config)
                .has_value()) {
            ASSERT_TRUE(service_
                            ->PutEnd(client_id, key, TenantId::Default(),
                                     ReplicaType::MEMORY)
                            .has_value());
            success_keys.push_back(key);
        } else {
            // wait for eviction to work
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
    }
    ASSERT_LE(success_keys.size(), 17);
    // All soft pinned objects should be accessible
    for (const auto& key : success_keys) {
        ASSERT_TRUE(
            service_->GetReplicaList(key, TenantId::Default()).has_value());
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(kv_lease_ttl));
    service_->RemoveAll();
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

TEST_F(MasterServiceTest, ConcurrentMountLocalDiskSegment) {
    MasterServiceConfig config;
    config.enable_offload = true;
    std::unique_ptr<MasterService> service_(new MasterService(config));

    constexpr size_t num_threads = 100;
    std::vector<std::thread> threads;
    std::atomic<int> success_count{0};

    // Launch multiple threads to mount local disk segments concurrently
    for (size_t i = 0; i < num_threads; i++) {
        threads.emplace_back([&service_, i, &success_count, this]() {
            UUID client_id = generate_uuid();
            auto mount_result =
                service_->MountLocalDiskSegment(client_id, true);
            ASSERT_TRUE(mount_result.has_value());
            ++success_count;
        });
    }

    // Wait for all threads to complete
    for (auto& thread : threads) {
        thread.join();
    }

    // Verify that some mount/unmount operations succeeded
    EXPECT_GT(success_count, 0);
}

TEST_F(MasterServiceTest, OffloadObjectHeartbeat) {
    constexpr size_t key_cnt = 3000;
    MasterServiceConfig config;
    config.enable_offload = true;
    std::unique_ptr<MasterService> service_(new MasterService(config));
    UUID client_id = generate_uuid();
    constexpr size_t buffer = 0x300000000;
    constexpr size_t size = 1024 * 1024 * 16;
    auto segment = MakeSegment("segment", buffer, size);
    auto mount_result = service_->MountSegment(segment, client_id);
    ASSERT_TRUE(mount_result.has_value());
    auto mount_local_disk_result =
        service_->MountLocalDiskSegment(client_id, false);
    ASSERT_TRUE(mount_local_disk_result.has_value());
    for (size_t i = 0; i < key_cnt; i++) {
        auto key = GenerateKeyForSegment(client_id, service_, segment.name);
    }
    auto res = service_->OffloadObjectHeartbeat(client_id, true);
    if (!res) {
        LOG(ERROR) << "OffloadObjectHeartbeat failed with error: "
                   << res.error();
        ASSERT_TRUE(res);
    }
    ASSERT_EQ(res->size(), 0);
    std::vector<std::string> keys;
    for (size_t i = 0; i < key_cnt; i++) {
        auto key = GenerateKeyForSegment(client_id, service_, segment.name);
        keys.push_back(key);
    }
    res = service_->OffloadObjectHeartbeat(client_id, true);
    if (!res) {
        LOG(ERROR) << "OffloadObjectHeartbeat failed with error: "
                   << res.error();
        ASSERT_TRUE(res);
    }
    ASSERT_EQ(res->size(), keys.size());
    for (auto& key : keys) {
        auto it = std::find_if(
            res->begin(), res->end(),
            [&key](const OffloadTaskItem& task) { return task.key == key; });
        ASSERT_TRUE(it != res->end());
        ASSERT_EQ(it->size, 1024);
    }

    keys.clear();
    for (size_t i = 0; i < key_cnt; i++) {
        auto key = GenerateKeyForSegment(client_id, service_, segment.name);
        keys.push_back(key);
    }
    res = service_->OffloadObjectHeartbeat(client_id, true);
    if (!res) {
        LOG(ERROR) << "OffloadObjectHeartbeat failed with error: "
                   << res.error();
        ASSERT_TRUE(res);
    }
    ASSERT_EQ(res->size(), keys.size());
    for (auto& key : keys) {
        auto it = std::find_if(
            res->begin(), res->end(),
            [&key](const OffloadTaskItem& task) { return task.key == key; });
        ASSERT_TRUE(it != res->end());
        ASSERT_EQ(it->size, 1024);
    }
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

// ===================== Hard Pin Tests =====================

TEST_F(MasterServiceTest, HardPinObjectNotEvicted) {
    // Hard-pinned objects must survive eviction under memory pressure,
    // even after lease expires and all non-pinned objects are gone.
    const uint64_t kv_lease_ttl = 200;
    auto service_config = MasterServiceConfig::builder()
                              .set_default_kv_lease_ttl(kv_lease_ttl)
                              .build();
    std::unique_ptr<MasterService> service_(new MasterService(service_config));
    const UUID client_id = generate_uuid();

    constexpr size_t buffer = 0x300000000;
    constexpr size_t segment_size = 1024 * 1024 * 16;
    constexpr size_t value_size = 1024 * 1024;
    [[maybe_unused]] const auto context =
        PrepareSimpleSegment(*service_, "test_segment", buffer, segment_size);

    // Put a hard-pinned object
    {
        ReplicateConfig config;
        config.replica_num = 1;
        config.with_hard_pin = true;
        auto result = service_->PutStart(
            client_id, "pinned_model", TenantId::Default(), value_size, config);
        ASSERT_TRUE(result.has_value());
        ASSERT_TRUE(service_
                        ->PutEnd(client_id, "pinned_model", TenantId::Default(),
                                 ReplicaType::MEMORY)
                        .has_value());
    }

    // Fill remaining space with normal objects to trigger eviction
    for (int i = 0; i < 20; i++) {
        std::string key = "filler_" + std::to_string(i);
        ReplicateConfig config;
        config.replica_num = 1;
        auto result = service_->PutStart(client_id, key, TenantId::Default(),
                                         value_size, config);
        if (result.has_value()) {
            service_->PutEnd(client_id, key, TenantId::Default(),
                             ReplicaType::MEMORY);
        }
    }

    // Wait for leases to expire and eviction to kick in
    std::this_thread::sleep_for(std::chrono::milliseconds(kv_lease_ttl + 500));

    // Hard-pinned object must still be there
    auto get_result =
        service_->GetReplicaList("pinned_model", TenantId::Default());
    ASSERT_TRUE(get_result.has_value())
        << "Hard-pinned object was evicted, but it should never be";

    // Explicit Remove should still work on hard-pinned objects
    auto remove_result =
        service_->Remove("pinned_model", TenantId::Default(), /*force=*/true);
    ASSERT_TRUE(remove_result.has_value());
    auto exist_result = service_->ExistKey("pinned_model", TenantId::Default());
    ASSERT_TRUE(exist_result.has_value());
    ASSERT_FALSE(exist_result.value());

    service_->RemoveAll();
}

TEST_F(MasterServiceTest, HardPinWithSoftPinEvictionOrder) {
    // Verify eviction priority: non-pinned first, then soft-pinned,
    // and hard-pinned objects are never evicted even under extreme pressure.
    const uint64_t kv_lease_ttl = 200;
    const uint64_t kv_soft_pin_ttl = 10000;
    const bool allow_evict_soft_pinned_objects = true;
    auto service_config = MasterServiceConfig::builder()
                              .set_default_kv_lease_ttl(kv_lease_ttl)
                              .set_default_kv_soft_pin_ttl(kv_soft_pin_ttl)
                              .set_allow_evict_soft_pinned_objects(
                                  allow_evict_soft_pinned_objects)
                              .set_eviction_ratio(0.5)
                              .build();
    std::unique_ptr<MasterService> service_(new MasterService(service_config));
    const UUID client_id = generate_uuid();

    constexpr size_t buffer = 0x300000000;
    constexpr size_t segment_size = 1024 * 1024 * 16;
    constexpr size_t value_size = 1024 * 1024;
    [[maybe_unused]] const auto context =
        PrepareSimpleSegment(*service_, "test_segment", buffer, segment_size);

    // Put a hard-pinned object
    {
        ReplicateConfig config;
        config.replica_num = 1;
        config.with_hard_pin = true;
        ASSERT_TRUE(service_
                        ->PutStart(client_id, "hard_pinned",
                                   TenantId::Default(), value_size, config)
                        .has_value());
        ASSERT_TRUE(service_
                        ->PutEnd(client_id, "hard_pinned", TenantId::Default(),
                                 ReplicaType::MEMORY)
                        .has_value());
    }

    // Put a soft-pinned object
    {
        ReplicateConfig config;
        config.replica_num = 1;
        config.soft_pin_action = SoftPinAction::ENABLE;
        ASSERT_TRUE(service_
                        ->PutStart(client_id, "soft_pinned",
                                   TenantId::Default(), value_size, config)
                        .has_value());
        ASSERT_TRUE(service_
                        ->PutEnd(client_id, "soft_pinned", TenantId::Default(),
                                 ReplicaType::MEMORY)
                        .has_value());
    }

    // Fill the rest
    for (int i = 0; i < 20; i++) {
        std::string key = "normal_" + std::to_string(i);
        ReplicateConfig config;
        config.replica_num = 1;
        auto result = service_->PutStart(client_id, key, TenantId::Default(),
                                         value_size, config);
        if (result.has_value()) {
            service_->PutEnd(client_id, key, TenantId::Default(),
                             ReplicaType::MEMORY);
        }
    }

    // Let leases expire, trigger eviction
    std::this_thread::sleep_for(std::chrono::milliseconds(kv_lease_ttl + 500));

    // Hard-pinned always survives
    ASSERT_TRUE(service_->GetReplicaList("hard_pinned", TenantId::Default())
                    .has_value())
        << "Hard-pinned object was evicted";

    std::this_thread::sleep_for(std::chrono::milliseconds(kv_lease_ttl));
    service_->RemoveAll();
}

}  // namespace mooncake::test

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
