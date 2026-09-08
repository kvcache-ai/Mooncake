// master_service_test_fixture.h
//
// Shared MasterServiceTest fixture used by the master_service test binaries.
// Business-behavior coverage lives in the scenario DSL; this fixture is the
// implementation-coupled harness (segment setup, put helpers) shared by the
// focused behavior-area test files.

#pragma once

#include "master_service.h"
#include "types.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <optional>
#include <string>
#include <vector>

namespace mooncake::test {

std::vector<ObjectMeta> MakeObjectMetas(const std::vector<std::string>& keys) {
    std::vector<ObjectMeta> object_metas;
    object_metas.reserve(keys.size());
    for (const auto& key : keys) {
        object_metas.emplace_back(ObjectMeta{key, std::nullopt});
    }
    return object_metas;
}

class MasterServiceTest : public ::testing::Test {
   protected:
    void SetUp() override {
        google::InitGoogleLogging("MasterServiceTest");
        FLAGS_logtostderr = true;
    }

    struct MountedSegmentContext {
        UUID segment_id;
        UUID client_id;
    };

    static constexpr size_t kDefaultSegmentBase = 0x300000000;
    static constexpr size_t kDefaultSegmentSize = 1024 * 1024 * 16;
    static constexpr uint64_t kStrictTenantQuotaBytes = 4 * 1024 * 1024;

    void PauseReplicaCleanup(MasterService& service) {
        service.replica_cleanup_worker_.Stop();
    }

    void ResumeReplicaCleanup(MasterService& service) {
        service.replica_cleanup_worker_.Start();
        service.replica_cleanup_worker_.Schedule();
    }

    void ExpectKeyHiddenFromReadApis(MasterService& service,
                                     const std::string& key) {
        auto get = service.GetReplicaList(key, TenantId::Default());
        ASSERT_FALSE(get.has_value());
        EXPECT_EQ(ErrorCode::REPLICA_IS_NOT_READY, get.error());

        auto exists = service.ExistKey(key, TenantId::Default());
        ASSERT_TRUE(exists.has_value());
        EXPECT_FALSE(*exists);

        auto batch_exists = service.BatchExistKey({key}, TenantId::Default());
        ASSERT_EQ(1u, batch_exists.size());
        ASSERT_TRUE(batch_exists[0].has_value());
        EXPECT_FALSE(*batch_exists[0]);

        auto all_keys = service.GetAllKeys(TenantId::Default());
        ASSERT_TRUE(all_keys.has_value());
        EXPECT_EQ(all_keys->end(),
                  std::find(all_keys->begin(), all_keys->end(), key));
    }

    std::optional<std::chrono::system_clock::time_point> GetSoftPinDeadline(
        MasterService& service, const std::string& key,
        const std::string& tenant_id = "default") {
        const TenantId normalized_tenant =
            service.ResolveRequestTenantId(TenantId(tenant_id));
        const size_t shard_idx = service.getShardIndex(normalized_tenant, key);
        MasterService::MetadataShardAccessorRO shard(&service, shard_idx);
        const auto tenant_it = shard->tenants.find(normalized_tenant);
        if (tenant_it == shard->tenants.end()) {
            return std::nullopt;
        }
        const auto metadata_it = tenant_it->second.metadata.find(key);
        if (metadata_it == tenant_it->second.metadata.end()) {
            return std::nullopt;
        }
        return metadata_it->second.GetCommittedSoftPinTimeout();
    }

    void CleanupExpiredSoftPinsAt(
        MasterService& service,
        const std::chrono::system_clock::time_point& now) {
        service.CleanupExpiredSoftPins(now);
    }

    size_t MetadataShardIndex(MasterService& service, const std::string& key,
                              const TenantId& tenant_id = TenantId::Default()) {
        return service.getShardIndex(tenant_id, key);
    }

    size_t MetadataBucketCount(
        MasterService& service, size_t shard_idx,
        const TenantId& tenant_id = TenantId::Default()) {
        MasterService::MetadataShardAccessorRO shard(&service, shard_idx);
        const auto tenant_it = shard->tenants.find(tenant_id);
        return tenant_it == shard->tenants.end()
                   ? 0
                   : tenant_it->second.metadata.bucket_count();
    }

    void SetSoftPinDeadlineForTest(
        MasterService& service, const std::string& key,
        const std::chrono::system_clock::time_point& deadline,
        const std::string& tenant_id = "default") {
        const TenantId normalized_tenant =
            service.ResolveRequestTenantId(TenantId(tenant_id));
        const size_t shard_idx = service.getShardIndex(normalized_tenant, key);
        MasterService::MetadataShardAccessorRW shard(&service, shard_idx);
        auto& metadata = shard->tenants.at(normalized_tenant).metadata.at(key);
        {
            SpinLocker locker(&metadata.lock);
            metadata.soft_pin_timeout = deadline;
        }
        service.soft_pin_deadline_index_.Upsert(
            normalized_tenant.MakeScopedKey(key), shard_idx, deadline);
    }

    size_t SoftPinDeadlineHeapSize(MasterService& service) {
        return service.soft_pin_deadline_index_.HeapSizeForTest();
    }

    size_t SoftPinRegistrationCount(MasterService& service) {
        return service.soft_pin_deadline_index_.RegistrationCountForTest();
    }

    std::optional<uint32_t> GetReplicaRefcntBySegmentName(
        MasterService& service, const std::string& key,
        const std::string& segment_name) {
        MasterService::MetadataAccessorRO accessor(
            &service,
            service.MakeObjectIdentityForRequest(key, TenantId::Default()));
        if (!accessor.Exists()) {
            return std::nullopt;
        }

        for (const auto& replica : accessor.Get().GetAllReplicas()) {
            for (const auto& name : replica.get_segment_names()) {
                if (name.has_value() && *name == segment_name) {
                    return replica.get_refcnt();
                }
            }
        }
        return std::nullopt;
    }

    // Exposes the private medium-normalization helpers to test bodies: the
    // fixture is a friend of MasterService, but the classes gtest derives from
    // it are not, so TEST_F cannot reach them without this hop.
    std::optional<std::vector<std::string>> KvMediaForKey(
        MasterService& service, const std::string& key,
        const TenantId& tenant_id = TenantId::Default()) {
        MasterService::MetadataAccessorRO accessor(
            &service, service.MakeObjectIdentityForRequest(key, tenant_id));
        if (!accessor.Exists()) {
            return std::nullopt;
        }
        return MasterService::KvMediaForMetadata(accessor.Get());
    }

    std::optional<std::vector<std::string>> KvRemovalMediaForKey(
        MasterService& service, const std::string& key,
        const TenantId& tenant_id = TenantId::Default()) {
        MasterService::MetadataAccessorRO accessor(
            &service, service.MakeObjectIdentityForRequest(key, tenant_id));
        if (!accessor.Exists()) {
            return std::nullopt;
        }
        return MasterService::KvMediaForRemoval(accessor.Get());
    }

    // Lets a test line up a key with a shard the RemoveAll scan has already
    // passed, which is the only way to reproduce the commit/clear ordering race
    // deterministically.
    size_t ShardIndexForKey(MasterService& service, const std::string& key,
                            const TenantId& tenant_id = TenantId::Default()) {
        return service.getShardIndex(tenant_id, key);
    }

    void UpsertSoftPinDeadlineIndexForTest(
        MasterService& service, const std::string& key, size_t shard_idx,
        const std::chrono::system_clock::time_point& deadline,
        const std::string& tenant_id = "default") {
        service.soft_pin_deadline_index_.Upsert(
            TenantId(tenant_id).MakeScopedKey(key), shard_idx, deadline);
    }

    size_t PopExpiredSoftPinDeadlinesForTest(
        MasterService& service,
        const std::chrono::system_clock::time_point& now) {
        return service.soft_pin_deadline_index_.PopExpired(now).size();
    }

    std::chrono::system_clock::time_point ComputeSoftPinDeadlineForTest(
        const std::chrono::system_clock::time_point& now, uint64_t ttl_ms) {
        return MasterService::ObjectMetadata::ComputeSoftPinDeadline(now,
                                                                     ttl_ms);
    }

    std::string WriteTenantPolicyFile(
        const std::map<std::string, uint64_t>& tenant_quotas) {
        TenantQuotaPolicySnapshot snapshot;
        snapshot.tenant_quotas = tenant_quotas;
        auto path =
            std::filesystem::temp_directory_path() /
            ("mooncake_master_service_test_" + std::to_string(::getpid()) +
             "_" + std::to_string(next_policy_file_++) + ".yaml");
        std::ofstream out(path);
        out << FormatTenantQuotaPolicyYaml(snapshot);
        out.close();
        policy_files_.push_back(path.string());
        return path.string();
    }

    MasterServiceConfig MakeStrictTenantConfig(
        const std::vector<std::string>& tenants) {
        std::map<std::string, uint64_t> tenant_quotas;
        for (const auto& tenant : tenants) {
            tenant_quotas.emplace(tenant, kStrictTenantQuotaBytes);
        }
        return MasterServiceConfig::builder()
            .set_enable_multi_tenants(true)
            .set_tenant_quota_connector_type("file")
            .set_tenant_quota_connector_uri(
                WriteTenantPolicyFile(tenant_quotas))
            .build();
    }

    WrappedMasterServiceConfig MakeStrictWrappedConfig(
        const std::vector<std::string>& tenants) {
        WrappedMasterServiceConfig config;
        config.default_kv_lease_ttl = 100;
        config.enable_metric_reporting = false;
        config.enable_multi_tenants = true;
        config.tenant_quota_connector_type = "file";
        std::map<std::string, uint64_t> tenant_quotas;
        for (const auto& tenant : tenants) {
            tenant_quotas.emplace(tenant, kStrictTenantQuotaBytes);
        }
        config.tenant_quota_connector_uri =
            WriteTenantPolicyFile(tenant_quotas);
        return config;
    }

    Segment MakeSegment(std::string name = "test_segment",
                        size_t base = kDefaultSegmentBase,
                        size_t size = kDefaultSegmentSize,
                        std::string host_id = "") const {
        Segment segment;
        segment.id = generate_uuid();
        segment.name = std::move(name);
        segment.base = base;
        segment.size = size;
        segment.te_endpoint = segment.name;
        segment.host_id = std::move(host_id);
        return segment;
    }

#ifdef USE_NOF
    NoFSegment MakeNoFSegment(
        std::string name = "test_nof_segment",
        std::string endpoint = "test_nof_segment_endpoint",
        size_t base = kDefaultSegmentBase + kDefaultSegmentSize,
        size_t size = kDefaultSegmentSize) const {
        NoFSegment segment;
        segment.id = generate_uuid();
        segment.name = std::move(name);
        segment.base = base;
        segment.size = size;
        segment.te_endpoint = std::move(endpoint);
        return segment;
    }
#endif

    MountedSegmentContext PrepareSimpleSegment(
        MasterService& service, std::string name = "test_segment",
        size_t base = kDefaultSegmentBase, size_t size = kDefaultSegmentSize,
        std::string host_id = "") const {
        Segment segment =
            MakeSegment(std::move(name), base, size, std::move(host_id));
        UUID client_id = generate_uuid();
        auto mount_result = service.MountSegment(segment, client_id);
        EXPECT_TRUE(mount_result.has_value());
        return {.segment_id = segment.id, .client_id = client_id};
    }

    std::string PutObjectOnSegment(MasterService& service,
                                   const UUID& client_id,
                                   const std::string& segment_name,
                                   size_t slice_length = 1024) const {
        static std::atomic<uint64_t> counter{0};
        std::string key =
            "drain_job_key_" + std::to_string(counter.fetch_add(1));

        ReplicateConfig config;
        config.replica_num = 1;
        config.preferred_segment = segment_name;

        auto put_start = service.PutStart(client_id, key, TenantId::Default(),
                                          slice_length, config);
        EXPECT_TRUE(put_start.has_value());
        EXPECT_TRUE(service
                        .PutEnd(client_id, key, TenantId::Default(),
                                ReplicaType::MEMORY)
                        .has_value());
        return key;
    }

    std::string FindGroupIdOnDifferentShard(const std::string& key) const {
        static constexpr size_t kMetadataShardCountForTest = 1024;
        const size_t key_shard =
            std::hash<std::string>{}(key) % kMetadataShardCountForTest;
        for (int i = 0; i < 10000; ++i) {
            std::string group_id = key + "_group_" + std::to_string(i);
            if (std::hash<std::string>{}(group_id) %
                    kMetadataShardCountForTest !=
                key_shard) {
                return group_id;
            }
        }
        return key + "_fallback_group";
    }

    void PutCompletedObject(MasterService& service, const UUID& client_id,
                            const std::string& key,
                            const ReplicateConfig& config,
                            uint64_t slice_length = 1024) const {
        auto put_start = service.PutStart(client_id, key, TenantId::Default(),
                                          slice_length, config);
        ASSERT_TRUE(put_start.has_value())
            << "PutStart failed for key=" << key
            << ", error=" << toString(put_start.error());
        ASSERT_TRUE(service
                        .PutEnd(client_id, key, TenantId::Default(),
                                ReplicaType::MEMORY)
                        .has_value());
    }

    void PutCompletedObject(MasterService& service, const UUID& client_id,
                            const std::string& key, const TenantId& tenant_id,
                            const ReplicateConfig& config,
                            uint64_t slice_length = 1024) const {
        auto put_start =
            service.PutStart(client_id, key, tenant_id, slice_length, config);
        ASSERT_TRUE(put_start.has_value())
            << "PutStart failed for key=" << key << ", tenant_id=" << tenant_id
            << ", error=" << toString(put_start.error());
        ASSERT_TRUE(
            service.PutEnd(client_id, key, tenant_id, ReplicaType::MEMORY)
                .has_value());
    }

    bool ExecutePendingMoveTasks(MasterService& service,
                                 const UUID& client_id) const {
        auto fetched = service.FetchTasks(client_id, /*batch_size=*/16);
        EXPECT_TRUE(fetched.has_value());
        if (!fetched.has_value() || fetched->empty()) {
            return false;
        }

        bool processed = false;
        for (const auto& assignment : *fetched) {
            if (assignment.type != TaskType::REPLICA_MOVE) {
                continue;
            }

            ReplicaMovePayload payload;
            struct_json::from_json(payload, assignment.payload);

            const TenantId tenant_id(payload.tenant_id);
            EXPECT_TRUE(tenant_id.IsValid());
            if (!tenant_id.IsValid()) {
                return false;
            }
            auto move_start =
                service.MoveStart(client_id, payload.key, tenant_id,
                                  payload.source, payload.target);
            EXPECT_TRUE(move_start.has_value());
            EXPECT_TRUE(
                service.MoveEnd(client_id, payload.key, tenant_id).has_value());

            TaskCompleteRequest complete_request;
            complete_request.id = assignment.id;
            complete_request.status = TaskStatus::SUCCESS;
            complete_request.message = "move_done";
            EXPECT_TRUE(service.MarkTaskToComplete(client_id, complete_request)
                            .has_value());
            processed = true;
        }
        return processed;
    }

    bool FailPendingMoveTasks(MasterService& service,
                              const UUID& client_id) const {
        auto fetched = service.FetchTasks(client_id, /*batch_size=*/16);
        EXPECT_TRUE(fetched.has_value());
        if (!fetched.has_value() || fetched->empty()) {
            return false;
        }

        bool processed = false;
        for (const auto& assignment : *fetched) {
            if (assignment.type != TaskType::REPLICA_MOVE) {
                continue;
            }

            TaskCompleteRequest complete_request;
            complete_request.id = assignment.id;
            complete_request.status = TaskStatus::FAILED;
            complete_request.message = "move_failed";
            EXPECT_TRUE(service.MarkTaskToComplete(client_id, complete_request)
                            .has_value());
            processed = true;
        }
        return processed;
    }

    template <typename Predicate>
    void WaitUntil(
        Predicate&& predicate,
        std::chrono::milliseconds timeout = std::chrono::milliseconds(4000),
        std::chrono::milliseconds interval =
            std::chrono::milliseconds(50)) const {
        const auto deadline = std::chrono::steady_clock::now() + timeout;
        while (std::chrono::steady_clock::now() < deadline) {
            if (predicate()) {
                return;
            }
            std::this_thread::sleep_for(interval);
        }
        EXPECT_TRUE(predicate());
    }

    std::vector<Replica::Descriptor> replica_list;
    std::vector<std::string> policy_files_;
    size_t next_policy_file_ = 0;

    void TearDown() override {
        for (const auto& path : policy_files_) {
            std::error_code ec;
            std::filesystem::remove(path, ec);
        }
        google::ShutdownGoogleLogging();
    }

    std::vector<std::string> GetGroupMemberKeysForTest(
        MasterService& service, const std::string& group_id,
        const std::string& tenant_id = "default") {
        const TenantId normalized_tenant =
            service.ResolveRequestTenantId(TenantId(tenant_id));
        MasterService::GroupDomainAccessorRO gs(&service);
        auto it = gs->groups.find(normalized_tenant.MakeScopedKey(group_id));
        if (it == gs->groups.end()) {
            return {};
        }
        return {it->second.member_keys.begin(), it->second.member_keys.end()};
    }

    void ClearGroupStateForTest(MasterService& service) {
        MasterService::GroupDomainAccessorRW gs(&service);
        gs->groups.clear();
    }

    void RebuildGroupStateForTest(MasterService& service) {
        service.RebuildGroupState();
    }

    std::shared_ptr<Lease> GetGroupLeaseForTest(
        MasterService& service, const std::string& group_id,
        const std::string& tenant_id = "default") {
        const TenantId normalized_tenant =
            service.ResolveRequestTenantId(TenantId(tenant_id));
        MasterService::GroupDomainAccessorRO gs(&service);
        auto it = gs->groups.find(normalized_tenant.MakeScopedKey(group_id));
        return it == gs->groups.end() ? nullptr : it->second.lease;
    }

    void ReRouteRestoredObjectsMigrationForTest(MasterService& service) {
        const UUID client_id = generate_uuid();
        const std::string grouped_key = "reroute_grouped_key";
        const std::string ungrouped_key = "reroute_ungrouped_key";
        const std::string group_id = FindGroupIdOnDifferentShard(grouped_key);

        ReplicateConfig grouped_config;
        grouped_config.replica_num = 1;
        grouped_config.group_ids = std::vector<std::string>{group_id};
        ReplicateConfig ungrouped_config;
        ungrouped_config.replica_num = 1;

        PutCompletedObject(service, client_id, grouped_key, grouped_config);
        PutCompletedObject(service, client_id, ungrouped_key, ungrouped_config);

        const TenantId tenant = TenantId::Default();
        const size_t grouped_correct =
            service.getShardIndex(tenant, grouped_key);
        const size_t wrong = (grouped_correct + 1) %
                             static_cast<size_t>(MasterService::kNumShards);

        // Reachable initially (placed on the correct hash(tenant, key) shard).
        EXPECT_TRUE(service.ExistKey(grouped_key, tenant).value_or(false));

        // Simulate an old snapshot that placed grouped_key on a stale shard.
        {
            MasterService::MetadataShardAccessorRW src(&service,
                                                       grouped_correct);
            auto tenant_it = src->tenants.find(tenant);
            ASSERT_NE(tenant_it, src->tenants.end());
            auto obj_it = tenant_it->second.metadata.find(grouped_key);
            ASSERT_NE(obj_it, tenant_it->second.metadata.end());
            auto node = tenant_it->second.metadata.extract(obj_it);
            ASSERT_FALSE(node.empty());
            MasterService::MetadataShardAccessorRW dst(&service, wrong);
            auto& dst_tenant =
                service.GetOrCreateTenantState(dst.get(), tenant);
            dst_tenant.metadata.insert(std::move(node));
        }

        // Now unreachable via hash(tenant, key) lookup (the old-snapshot
        // problem).
        EXPECT_FALSE(service.ExistKey(grouped_key, tenant).value_or(true));

        // Run the migration.
        service.ReRouteRestoredObjectsByKey();

        // Reachable again, and back on the correct shard.
        EXPECT_TRUE(service.ExistKey(grouped_key, tenant).value_or(false));
        MasterService::MetadataShardAccessorRW shard(&service, grouped_correct);
        auto tenant_it = shard->tenants.find(tenant);
        ASSERT_NE(tenant_it, shard->tenants.end());
        EXPECT_NE(tenant_it->second.metadata.find(grouped_key),
                  tenant_it->second.metadata.end());
        // The stale shard no longer holds it.
        MasterService::MetadataShardAccessorRW stale(&service, wrong);
        auto stale_it = stale->tenants.find(tenant);
        if (stale_it != stale->tenants.end()) {
            EXPECT_EQ(stale_it->second.metadata.find(grouped_key),
                      stale_it->second.metadata.end());
        }
        // The correctly-placed ungrouped object is unaffected.
        EXPECT_TRUE(service.ExistKey(ungrouped_key, tenant).value_or(false));
    }
};

}  // namespace mooncake::test
