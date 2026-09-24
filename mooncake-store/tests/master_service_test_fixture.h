// master_service_test_fixture.h
//
// Shared MasterServiceTest fixture used by the master_service test binaries.
// Business-behavior coverage lives in the scenario DSL; this fixture is the
// implementation-coupled harness (segment setup, put helpers) shared by the
// focused behavior-area test files.

#pragma once

#include "master_service.h"
#include "master_service/master_service_test_peer.h"
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
        MasterServiceTestPeer::ReplicaCleanupWorker(service).Stop();
    }

    void ResumeReplicaCleanup(MasterService& service) {
        MasterServiceTestPeer::ReplicaCleanupWorker(service).Start();
        MasterServiceTestPeer::ReplicaCleanupWorker(service).Schedule();
    }

    std::shared_ptr<ClientLivenessRecord> FindClientLivenessForTest(
        MasterService& service, const UUID& client_id) {
        return MasterServiceTestPeer(service).FindClientRecord(client_id);
    }

    bool ProcessClientOffboardingForTest(MasterService& service,
                                         ClientOffboardingJob& job) {
        return MasterServiceTestPeer(service).ProcessClientOffboardingJob(job);
    }

    ErrorCode PrepareUnmountSegmentForTest(MasterService& service,
                                           const UUID& segment_id,
                                           size_t& metrics_dec_capacity) {
        auto access =
            MasterServiceTestPeer::SegmentManager(service).getSegmentAccess();
        return access.PrepareUnmountSegment(segment_id, metrics_dec_capacity);
    }

    ErrorCode CommitUnmountSegmentForTest(MasterService& service,
                                          const UUID& segment_id,
                                          const UUID& client_id,
                                          size_t metrics_dec_capacity) {
        auto access =
            MasterServiceTestPeer::SegmentManager(service).getSegmentAccess();
        return access.CommitUnmountSegment(segment_id, client_id,
                                           metrics_dec_capacity);
    }

    std::chrono::seconds ClientOffboardingRetryDelayForTest(
        uint64_t retry_count) {
        return ClientOffboardingWorker::RetryDelay(retry_count);
    }

    bool ClientOffboardingShouldAlertForTest(uint64_t retry_count) {
        return ClientOffboardingWorker::ShouldAlert(retry_count);
    }

    // Run the stale-handle sweep synchronously through the test peer.
    void ClearInvalidHandlesForTest(MasterService& service) {
        MasterServiceTestPeer(service).ClearInvalidHandles();
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
            MasterServiceTestPeer(service).ResolveRequestTenantId(
                TenantId(tenant_id));
        auto entry = MasterServiceTestPeer::FindObject(
            service,
            MasterServiceTestPeer::ObjectIdentity{normalized_tenant, key});
        if (entry == nullptr) {
            return std::nullopt;
        }
        return entry->WithSharedAccess(
            [](const ObjectMetadata& metadata, const ObjectEntry::State&) {
                return metadata.GetCommittedSoftPinTimeout();
            });
    }

    void CleanupExpiredSoftPinsAt(
        MasterService& service,
        const std::chrono::system_clock::time_point& now) {
        MasterServiceTestPeer(service).CleanupExpiredSoftPins(now);
    }

    void SetSoftPinDeadlineForTest(
        MasterService& service, const std::string& key,
        const std::chrono::system_clock::time_point& deadline,
        const std::string& tenant_id = "default") {
        const TenantId normalized_tenant =
            MasterServiceTestPeer(service).ResolveRequestTenantId(
                TenantId(tenant_id));
        auto entry = MasterServiceTestPeer::FindObject(
            service,
            MasterServiceTestPeer::ObjectIdentity{normalized_tenant, key});
        ASSERT_TRUE(entry != nullptr);
        entry->WithExclusiveAccess(
            [&](ObjectMetadata& metadata, ObjectEntry::State&) {
                SpinLocker locker(&metadata.lock);
                metadata.soft_pin_timeout = deadline;
            });
        MasterServiceTestPeer::SoftPinDeadlineIndex(service).Upsert(
            normalized_tenant.MakeScopedKey(key), deadline);
    }

    size_t SoftPinDeadlineHeapSize(MasterService& service) {
        return MasterServiceTestPeer(service).SoftPinHeapSize();
    }

    size_t SoftPinRegistrationCount(MasterService& service) {
        return MasterServiceTestPeer(service).SoftPinRegistrationCount();
    }

    // What a segment-name lookup reports about one object: whether it carries a
    // replica on that segment, and that replica's refcount. A plain result,
    // because the read helper's own optional already means "not read".
    struct ReplicaRefcntLookup {
        bool found;
        uint32_t refcnt;
    };

    // Reads one object's envelope and state the way the read paths do, with the
    // callback contract of MasterService::WithObjectMetadataForRead. Nothing
    // when the object is absent or no longer readable.
    template <typename Fn>
    [[nodiscard]] auto WithObjectForRead(MasterService& service,
                                         const std::string& key,
                                         const TenantId& tenant_id,
                                         Fn&& fn) const
        -> std::optional<std::invoke_result_t<
            Fn, const metadata::Tenant&, const std::shared_ptr<ObjectEntry>&,
            const ObjectMetadata&, const ObjectEntry::State&>> {
        return MasterServiceTestPeer(service).WithPublishedObjectForRead(
            tenant_id, key, std::forward<Fn>(fn));
    }

    std::optional<uint32_t> GetReplicaRefcntBySegmentName(
        MasterService& service, const std::string& key,
        const std::string& segment_name) {
        auto lookup = WithObjectForRead(
            service, key, TenantId::Default(),
            [&](const metadata::Tenant&, const std::shared_ptr<ObjectEntry>&,
                const ObjectMetadata& metadata,
                const ObjectEntry::State&) -> ReplicaRefcntLookup {
                for (const auto& replica : metadata.GetAllReplicas()) {
                    for (const auto& name : replica.get_segment_names()) {
                        if (name.has_value() && *name == segment_name) {
                            return ReplicaRefcntLookup{true,
                                                       replica.get_refcnt()};
                        }
                    }
                }
                return ReplicaRefcntLookup{false, 0};
            });
        if (!lookup.has_value() || !lookup->found) {
            return std::nullopt;
        }
        return lookup->refcnt;
    }

    // Inspect normalized media as the read paths see it.
    std::optional<std::vector<std::string>> KvMediaForKey(
        MasterService& service, const std::string& key,
        const TenantId& tenant_id = TenantId::Default()) {
        return WithObjectForRead(
            service, key, tenant_id,
            [](const metadata::Tenant&, const std::shared_ptr<ObjectEntry>&,
               const ObjectMetadata& metadata, const ObjectEntry::State&) {
                return MasterServiceTestPeer::KvMediaForMetadata(metadata);
            });
    }

    std::optional<std::vector<std::string>> KvRemovalMediaForKey(
        MasterService& service, const std::string& key,
        const TenantId& tenant_id = TenantId::Default()) {
        return WithObjectForRead(
            service, key, tenant_id,
            [](const metadata::Tenant&, const std::shared_ptr<ObjectEntry>&,
               const ObjectMetadata& metadata, const ObjectEntry::State&) {
                return MasterServiceTestPeer::KvMediaForRemoval(metadata);
            });
    }

    void UpsertSoftPinDeadlineIndexForTest(
        MasterService& service, const std::string& key,
        const std::chrono::system_clock::time_point& deadline,
        const std::string& tenant_id = "default") {
        MasterServiceTestPeer::SoftPinDeadlineIndex(service).Upsert(
            TenantId(tenant_id).MakeScopedKey(key), deadline);
    }

    size_t PopExpiredSoftPinDeadlinesForTest(
        MasterService& service,
        const std::chrono::system_clock::time_point& now) {
        return MasterServiceTestPeer::SoftPinDeadlineIndex(service)
            .PopExpired(now)
            .size();
    }

    std::chrono::system_clock::time_point ComputeSoftPinDeadlineForTest(
        const std::chrono::system_clock::time_point& now,
        std::chrono::milliseconds ttl) {
        return MasterServiceTestPeer::ObjectMetadata::ComputeSoftPinDeadline(
            now, ttl);
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

    // A group name unrelated to the object key. Group membership lives in the
    // object's own tenant, so the name only has to differ from the key.
    std::string UnrelatedGroupId(const std::string& key) const {
        return key + "_group";
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
            MasterServiceTestPeer(service).ResolveRequestTenantId(
                TenantId(tenant_id));
        // Membership is per tenant, so the group id alone is not enough.
        auto tenant_handle =
            MasterServiceTestPeer::Tenants(service).Lookup(normalized_tenant);
        if (tenant_handle == nullptr) {
            return {};
        }
        return tenant_handle->GroupMembers(group_id);
    }

    void ClearGroupStateForTest(MasterService& service) {
        // Drops every grouped entry's membership from its tenant's group
        // index, leaving the group table empty for a rebuild.
        MasterServiceTestPeer::Tenants(service).Visit(
            [&](const TenantId&,
                const std::shared_ptr<metadata::Tenant>& handle) {
                for (const auto& entry : handle->SnapshotObjects()) {
                    if (entry->group_id().empty()) {
                        continue;
                    }
                    handle->UnregisterGroupMember(entry);
                }
            });
    }

    void RebuildGroupStateForTest(MasterService& service) {
        MasterServiceTestPeer(service).RebuildGroupState();
    }

    // The shared group lease, read from a member's own lease: every grouped
    // object points at its group's single shared Lease.
    std::shared_ptr<Lease> GetGroupLeaseForTest(
        MasterService& service, const std::string& group_id,
        const std::string& tenant_id = "default") {
        const TenantId normalized_tenant =
            MasterServiceTestPeer(service).ResolveRequestTenantId(
                TenantId(tenant_id));
        auto tenant_handle =
            MasterServiceTestPeer::Tenants(service).Lookup(normalized_tenant);
        if (tenant_handle == nullptr) {
            return nullptr;
        }
        for (const auto& member_key : tenant_handle->GroupMembers(group_id)) {
            auto entry = tenant_handle->Get(member_key);
            if (entry == nullptr) {
                continue;
            }
            return entry->WithSharedAccess(
                [](const ObjectMetadata& metadata,
                   const ObjectEntry::State&) -> std::shared_ptr<Lease> {
                    return metadata.lease_;
                });
        }
        return nullptr;
    }
};

}  // namespace mooncake::test
