#include "master_service.h"

#include <limits>
#include <mutex>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "types.h"

namespace mooncake::test {

class MasterServiceTenantQuotaTest : public ::testing::Test {
   protected:
    static constexpr size_t kSegmentBase = 0x500000000;

    MasterServiceConfig MakeConfig(uint64_t default_quota,
                                   uint64_t pool_capacity,
                                   bool enable_quota = true,
                                   std::string root_fs_dir = "") {
        return MasterServiceConfig::builder()
            .set_root_fs_dir(root_fs_dir)
            .set_enable_tenant_quota(enable_quota)
            .set_default_tenant_quota_bytes(default_quota)
            .set_tenant_quota_pool_capacity_bytes(pool_capacity)
            .build();
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
        EXPECT_TRUE(result.has_value());
        return client_id;
    }

    ReplicateConfig MemoryConfig() {
        ReplicateConfig config;
        config.replica_num = 1;
        return config;
    }

    MasterServiceConfig MakeOffloadConfig(uint64_t default_quota,
                                          uint64_t pool_capacity) {
        auto config = MakeConfig(default_quota, pool_capacity);
        config.enable_offload = true;
        config.offload_on_evict = true;
        config.promotion_on_hit = true;
        config.promotion_admission_threshold = 1;
        config.default_kv_lease_ttl = 0;
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

    void MountLocalDiskSegment(MasterService& service, const UUID& client_id) {
        auto mount = service.MountLocalDiskSegment(client_id, true);
        ASSERT_TRUE(mount.has_value()) << toString(mount.error());
    }

    void InjectLocalDiskReplica(MasterService& service, const UUID& client_id,
                                const std::string& key,
                                const std::string& tenant_id, int64_t size,
                                const std::string& transport_endpoint) {
        std::vector<OffloadTaskItem> tasks{
            OffloadTaskItem{.tenant_id = tenant_id, .key = key, .size = size}};
        StorageObjectMetadata metadata;
        metadata.bucket_id = 0;
        metadata.offset = 0;
        metadata.key_size = static_cast<int64_t>(key.size());
        metadata.data_size = size;
        metadata.transport_endpoint = transport_endpoint;
        std::vector<StorageObjectMetadata> metadatas{metadata};
        auto result = service.NotifyOffloadSuccess(client_id, tasks, metadatas);
        ASSERT_TRUE(result.has_value()) << toString(result.error());
    }

    TenantQuotaSnapshot Snapshot(MasterService& service,
                                 const std::string& tenant_id) {
        auto snapshot = service.GetTenantQuotaSnapshotForTesting(tenant_id);
        EXPECT_TRUE(snapshot.has_value());
        return *snapshot;
    }

    tl::expected<void, ErrorCode> ReserveQuota(MasterService& service,
                                               const std::string& tenant_id,
                                               uint64_t bytes) {
        return service.ReserveTenantQuota(tenant_id, bytes);
    }

    void CommitQuota(MasterService& service, const std::string& tenant_id,
                     uint64_t bytes) {
        service.CommitTenantQuota(tenant_id, bytes);
    }

    void AbortQuota(MasterService& service, const std::string& tenant_id,
                    uint64_t bytes) {
        service.AbortTenantQuota(tenant_id, bytes);
    }

    void ReleaseQuota(MasterService& service, const std::string& tenant_id,
                      uint64_t bytes) {
        service.ReleaseTenantQuota(tenant_id, bytes);
    }

    void ReleaseQuotaPartial(MasterService& service,
                             const std::string& tenant_id, uint64_t bytes) {
        service.ReleaseTenantQuotaPartial(tenant_id, bytes);
    }

    void RecomputeTenantQuotas(MasterService& service) {
        service.RecomputeTenantEffectiveQuotas();
    }

    void BatchEvict(MasterService& service) {
        service.BatchEvict(/*evict_ratio_target=*/1.0,
                           /*evict_ratio_lowerbound=*/1.0);
    }

    void SetExplicitTenantPolicy(MasterService& service,
                                 const std::string& tenant_id,
                                 uint64_t requested_quota_bytes) {
        const auto normalized_tenant = NormalizeTenantId(tenant_id);
        const auto shard_idx =
            service.getTenantQuotaShardIndex(normalized_tenant);
        auto& shard = service.tenant_quota_shards_[shard_idx];
        std::lock_guard<std::mutex> lock(shard.mutex);
        auto& state = shard.tenants[normalized_tenant];
        state.requested_quota_bytes = requested_quota_bytes;
        state.effective_quota_bytes = requested_quota_bytes;
        state.has_explicit_policy = true;
        state.over_quota = false;
    }

    void SetEmptyInheritedTenantState(MasterService& service,
                                      const std::string& tenant_id) {
        const auto normalized_tenant = NormalizeTenantId(tenant_id);
        const auto shard_idx =
            service.getTenantQuotaShardIndex(normalized_tenant);
        auto& shard = service.tenant_quota_shards_[shard_idx];
        std::lock_guard<std::mutex> lock(shard.mutex);
        auto& state = shard.tenants[normalized_tenant];
        state.requested_quota_bytes = service.default_tenant_quota_bytes_;
        state.effective_quota_bytes = service.default_tenant_quota_bytes_;
        state.has_explicit_policy = false;
        state.over_quota = false;
    }

    void ExpectSameAccounting(const TenantQuotaSnapshot& before,
                              const TenantQuotaSnapshot& after) {
        EXPECT_EQ(after.requested_quota_bytes, before.requested_quota_bytes);
        EXPECT_EQ(after.effective_quota_bytes, before.effective_quota_bytes);
        EXPECT_EQ(after.used_bytes, before.used_bytes);
        EXPECT_EQ(after.reserved_bytes, before.reserved_bytes);
        EXPECT_EQ(after.committed_count, before.committed_count);
        EXPECT_EQ(after.has_explicit_policy, before.has_explicit_policy);
        EXPECT_EQ(after.over_quota, before.over_quota);
    }

    size_t next_segment_offset_ = 0;
};

TEST_F(MasterServiceTenantQuotaTest, DisabledPreservesLegacyPutRemove) {
    MasterService service(MakeConfig(/*default_quota=*/128,
                                     /*pool_capacity=*/128,
                                     /*enable_quota=*/false));
    UUID client_id = MountSegment(service);

    PutComplete(service, client_id, "large", "tenant-a", 512);
    EXPECT_TRUE(
        service.Remove("large", "tenant-a", /*force=*/true).has_value());
    EXPECT_FALSE(
        service.GetTenantQuotaSnapshotForTesting("tenant-a").has_value());
}

TEST_F(MasterServiceTenantQuotaTest, SameKeyDifferentTenantsIndependent) {
    MasterService service(MakeConfig(/*default_quota=*/1000,
                                     /*pool_capacity=*/2000));
    UUID client_id = MountSegment(service);

    PutComplete(service, client_id, "shared-key", "tenant-a", 800);
    PutComplete(service, client_id, "shared-key", "tenant-b", 800);

    EXPECT_EQ(Snapshot(service, "tenant-a").used_bytes, 800);
    EXPECT_EQ(Snapshot(service, "tenant-b").used_bytes, 800);
}

TEST_F(MasterServiceTenantQuotaTest, SameTenantSharesQuotaAcrossKeys) {
    MasterService service(MakeConfig(/*default_quota=*/1000,
                                     /*pool_capacity=*/1000));
    UUID client_id = MountSegment(service);

    PutComplete(service, client_id, "key-a", "tenant-a", 600);
    auto over_quota =
        service.PutStart(client_id, "key-b", "tenant-a", 500, MemoryConfig());

    ASSERT_FALSE(over_quota.has_value());
    EXPECT_EQ(over_quota.error(), ErrorCode::TENANT_QUOTA_EXCEEDED);
    EXPECT_EQ(Snapshot(service, "tenant-a").used_bytes, 600);
    EXPECT_EQ(Snapshot(service, "tenant-a").reserved_bytes, 0);
}

TEST_F(MasterServiceTenantQuotaTest, FirstTenantPutStartUsesPoolCapacity) {
    MasterService service(MakeConfig(/*default_quota=*/1000,
                                     /*pool_capacity=*/100));
    UUID client_id = MountSegment(service);

    auto over_quota =
        service.PutStart(client_id, "large", "tenant-a", 800, MemoryConfig());

    ASSERT_FALSE(over_quota.has_value());
    EXPECT_EQ(over_quota.error(), ErrorCode::TENANT_QUOTA_EXCEEDED);
    EXPECT_FALSE(
        service.GetTenantQuotaSnapshotForTesting("tenant-a").has_value());

    auto start =
        service.PutStart(client_id, "small", "tenant-a", 80, MemoryConfig());

    ASSERT_TRUE(start.has_value()) << toString(start.error());
    auto snapshot = Snapshot(service, "tenant-a");
    EXPECT_EQ(snapshot.requested_quota_bytes, 1000);
    EXPECT_EQ(snapshot.effective_quota_bytes, 100);
    EXPECT_EQ(snapshot.used_bytes, 0);
    EXPECT_EQ(snapshot.reserved_bytes, 80);
    AbortQuota(service, "tenant-a", 80);
}

TEST_F(MasterServiceTenantQuotaTest,
       ZeroDefaultQuotaInitializesNewTenantAsUnlimited) {
    MasterService service(MakeConfig(/*default_quota=*/0,
                                     /*pool_capacity=*/1));

    auto reserve = ReserveQuota(service, "tenant-a", 4096);

    ASSERT_TRUE(reserve.has_value()) << toString(reserve.error());
    auto snapshot = Snapshot(service, "tenant-a");
    EXPECT_EQ(snapshot.requested_quota_bytes, 0);
    EXPECT_EQ(snapshot.effective_quota_bytes,
              std::numeric_limits<uint64_t>::max());
    EXPECT_EQ(snapshot.reserved_bytes, 4096);
}

TEST_F(MasterServiceTenantQuotaTest,
       OverQuotaReserveDoesNotChangeReservedBytes) {
    MasterService service(MakeConfig(/*default_quota=*/100,
                                     /*pool_capacity=*/100));
    ASSERT_TRUE(ReserveQuota(service, "tenant-a", 80).has_value());
    auto before = Snapshot(service, "tenant-a");

    auto reserve = ReserveQuota(service, "tenant-a", 30);

    ASSERT_FALSE(reserve.has_value());
    EXPECT_EQ(reserve.error(), ErrorCode::TENANT_QUOTA_EXCEEDED);
    ExpectSameAccounting(before, Snapshot(service, "tenant-a"));
}

TEST_F(MasterServiceTenantQuotaTest,
       FirstOverQuotaReserveDoesNotCreateTenantState) {
    MasterService service(MakeConfig(/*default_quota=*/100,
                                     /*pool_capacity=*/100));

    auto reserve = ReserveQuota(service, "tenant-a", 101);

    ASSERT_FALSE(reserve.has_value());
    EXPECT_EQ(reserve.error(), ErrorCode::TENANT_QUOTA_EXCEEDED);
    EXPECT_FALSE(
        service.GetTenantQuotaSnapshotForTesting("tenant-a").has_value());
}

TEST_F(MasterServiceTenantQuotaTest, RecomputePrunesEmptyInheritedTenantState) {
    MasterService service(MakeConfig(/*default_quota=*/100,
                                     /*pool_capacity=*/1000));
    SetEmptyInheritedTenantState(service, "tenant-a");
    ASSERT_TRUE(
        service.GetTenantQuotaSnapshotForTesting("tenant-a").has_value());
    SetExplicitTenantPolicy(service, "tenant-explicit", 200);

    RecomputeTenantQuotas(service);

    EXPECT_FALSE(
        service.GetTenantQuotaSnapshotForTesting("tenant-a").has_value());
    auto explicit_snapshot = Snapshot(service, "tenant-explicit");
    EXPECT_TRUE(explicit_snapshot.has_explicit_policy);
    EXPECT_EQ(explicit_snapshot.requested_quota_bytes, 200);
}

TEST_F(MasterServiceTenantQuotaTest,
       AbortPrunesInactiveInheritedTenantAndRecomputesQuotas) {
    MasterService service(MakeConfig(/*default_quota=*/1000,
                                     /*pool_capacity=*/100));

    ASSERT_TRUE(ReserveQuota(service, "tenant-a", 50).has_value());
    ASSERT_TRUE(ReserveQuota(service, "tenant-b", 40).has_value());
    EXPECT_EQ(Snapshot(service, "tenant-b").effective_quota_bytes, 50);

    AbortQuota(service, "tenant-a", 50);

    EXPECT_FALSE(
        service.GetTenantQuotaSnapshotForTesting("tenant-a").has_value());
    EXPECT_EQ(Snapshot(service, "tenant-b").effective_quota_bytes, 100);
    ASSERT_TRUE(ReserveQuota(service, "tenant-b", 60).has_value());
    EXPECT_EQ(Snapshot(service, "tenant-b").reserved_bytes, 100);
    AbortQuota(service, "tenant-b", 100);
}

TEST_F(MasterServiceTenantQuotaTest,
       FullReleasePrunesInactiveInheritedTenantAndRecomputesQuotas) {
    MasterService service(MakeConfig(/*default_quota=*/1000,
                                     /*pool_capacity=*/100));
    UUID client_id = MountSegment(service);
    PutComplete(service, client_id, "key-a", "tenant-a", 50);
    PutComplete(service, client_id, "key-b", "tenant-b", 40);
    EXPECT_EQ(Snapshot(service, "tenant-b").effective_quota_bytes, 50);

    ASSERT_TRUE(
        service.Remove("key-a", "tenant-a", /*force=*/true).has_value());

    EXPECT_FALSE(
        service.GetTenantQuotaSnapshotForTesting("tenant-a").has_value());
    EXPECT_EQ(Snapshot(service, "tenant-b").effective_quota_bytes, 100);
    auto reserve = service.PutStart(client_id, "key-b-extra", "tenant-b", 60,
                                    MemoryConfig());
    ASSERT_TRUE(reserve.has_value()) << toString(reserve.error());
    EXPECT_EQ(Snapshot(service, "tenant-b").reserved_bytes, 60);
    AbortQuota(service, "tenant-b", 60);
}

TEST_F(MasterServiceTenantQuotaTest, CommitMismatchDoesNotMutateAccounting) {
    MasterService service(MakeConfig(/*default_quota=*/100,
                                     /*pool_capacity=*/100));
    ASSERT_TRUE(ReserveQuota(service, "tenant-a", 40).has_value());
    auto before = Snapshot(service, "tenant-a");

    CommitQuota(service, "tenant-a", 50);

    ExpectSameAccounting(before, Snapshot(service, "tenant-a"));
}

TEST_F(MasterServiceTenantQuotaTest, AbortMismatchDoesNotMutateAccounting) {
    MasterService service(MakeConfig(/*default_quota=*/100,
                                     /*pool_capacity=*/100));
    ASSERT_TRUE(ReserveQuota(service, "tenant-a", 40).has_value());
    auto before = Snapshot(service, "tenant-a");

    AbortQuota(service, "tenant-a", 50);

    ExpectSameAccounting(before, Snapshot(service, "tenant-a"));
}

TEST_F(MasterServiceTenantQuotaTest, ReleaseMismatchDoesNotMutateAccounting) {
    MasterService service(MakeConfig(/*default_quota=*/100,
                                     /*pool_capacity=*/100));
    ASSERT_TRUE(ReserveQuota(service, "tenant-a", 40).has_value());
    CommitQuota(service, "tenant-a", 40);
    auto before = Snapshot(service, "tenant-a");

    ReleaseQuota(service, "tenant-a", 50);

    ExpectSameAccounting(before, Snapshot(service, "tenant-a"));
}

TEST_F(MasterServiceTenantQuotaTest,
       ReleasePartialMismatchDoesNotMutateAccounting) {
    MasterService service(MakeConfig(/*default_quota=*/100,
                                     /*pool_capacity=*/100));
    ASSERT_TRUE(ReserveQuota(service, "tenant-a", 40).has_value());
    CommitQuota(service, "tenant-a", 40);
    auto before = Snapshot(service, "tenant-a");

    ReleaseQuotaPartial(service, "tenant-a", 50);

    ExpectSameAccounting(before, Snapshot(service, "tenant-a"));
}

TEST_F(MasterServiceTenantQuotaTest,
       PhysicalAllocationFailureKeepsAllocatorError) {
    MasterService service(MakeConfig(/*default_quota=*/4096,
                                     /*pool_capacity=*/4096));
    UUID client_id = MountSegment(service, /*size=*/512);

    auto result = service.PutStart(client_id, "too-large", "tenant-a", 1024,
                                   MemoryConfig());

    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::NO_AVAILABLE_HANDLE);
    EXPECT_FALSE(
        service.GetTenantQuotaSnapshotForTesting("tenant-a").has_value());
}

TEST_F(MasterServiceTenantQuotaTest, RemoveReleasesCommittedCharge) {
    MasterService service(MakeConfig(/*default_quota=*/1000,
                                     /*pool_capacity=*/1000));
    UUID client_id = MountSegment(service);

    PutComplete(service, client_id, "key", "tenant-a", 400);
    ASSERT_EQ(Snapshot(service, "tenant-a").used_bytes, 400);

    ASSERT_TRUE(service.Remove("key", "tenant-a", /*force=*/true).has_value());
    EXPECT_FALSE(
        service.GetTenantQuotaSnapshotForTesting("tenant-a").has_value());
}

TEST_F(MasterServiceTenantQuotaTest,
       BatchEvictReleasesEvictedMemoryReplicaCharge) {
    MasterService service(MakeOffloadConfig(/*default_quota=*/1000,
                                            /*pool_capacity=*/1000));
    UUID client_id =
        MountSegment(service, /*size=*/4096, "quota_evict_segment");
    MountLocalDiskSegment(service, client_id);

    PutComplete(service, client_id, "key", "tenant-a", 400);
    InjectLocalDiskReplica(service, client_id, "key", "tenant-a", 400,
                           "quota_evict_segment");
    ASSERT_EQ(Snapshot(service, "tenant-a").used_bytes, 400);

    BatchEvict(service);

    EXPECT_FALSE(
        service.GetTenantQuotaSnapshotForTesting("tenant-a").has_value());
    auto reserve = service.PutStart(client_id, "after-evict", "tenant-a", 1000,
                                    MemoryConfig());
    ASSERT_TRUE(reserve.has_value()) << toString(reserve.error());
    EXPECT_EQ(Snapshot(service, "tenant-a").reserved_bytes, 1000);
    auto revoke = service.PutRevoke(client_id, "after-evict", "tenant-a",
                                    ReplicaType::MEMORY);
    ASSERT_TRUE(revoke.has_value()) << toString(revoke.error());
}

TEST_F(MasterServiceTenantQuotaTest, PromotionSuccessCommitsTenantQuota) {
    MasterService service(MakeOffloadConfig(/*default_quota=*/1000,
                                            /*pool_capacity=*/1000));
    UUID client_id =
        MountSegment(service, /*size=*/4096, "quota_promotion_segment");
    MountLocalDiskSegment(service, client_id);
    InjectLocalDiskReplica(service, client_id, "cold", "tenant-a", 400,
                           "quota_promotion_segment");

    auto replicas = service.GetReplicaList("cold", "tenant-a");
    ASSERT_TRUE(replicas.has_value()) << toString(replicas.error());
    auto alloc = service.PromotionAllocStart(client_id, "cold", "tenant-a", 400,
                                             {"quota_promotion_segment"});
    ASSERT_TRUE(alloc.has_value()) << toString(alloc.error());
    EXPECT_EQ(Snapshot(service, "tenant-a").used_bytes, 0);
    EXPECT_EQ(Snapshot(service, "tenant-a").reserved_bytes, 400);

    auto notify = service.NotifyPromotionSuccess(client_id, "cold", "tenant-a");
    ASSERT_TRUE(notify.has_value()) << toString(notify.error());

    EXPECT_EQ(Snapshot(service, "tenant-a").used_bytes, 400);
    EXPECT_EQ(Snapshot(service, "tenant-a").reserved_bytes, 0);
    auto over_quota = service.PutStart(client_id, "too-much", "tenant-a", 700,
                                       MemoryConfig());
    ASSERT_FALSE(over_quota.has_value());
    EXPECT_EQ(over_quota.error(), ErrorCode::TENANT_QUOTA_EXCEEDED);
}

TEST_F(MasterServiceTenantQuotaTest, PromotionAllocStartRejectsOverQuota) {
    MasterService service(MakeOffloadConfig(/*default_quota=*/300,
                                            /*pool_capacity=*/300));
    UUID client_id =
        MountSegment(service, /*size=*/4096, "quota_promotion_reject_segment");
    MountLocalDiskSegment(service, client_id);
    InjectLocalDiskReplica(service, client_id, "cold", "tenant-a", 400,
                           "quota_promotion_reject_segment");

    auto replicas = service.GetReplicaList("cold", "tenant-a");
    ASSERT_TRUE(replicas.has_value()) << toString(replicas.error());
    auto alloc = service.PromotionAllocStart(
        client_id, "cold", "tenant-a", 400, {"quota_promotion_reject_segment"});

    ASSERT_FALSE(alloc.has_value());
    EXPECT_EQ(alloc.error(), ErrorCode::TENANT_QUOTA_EXCEEDED);
    EXPECT_FALSE(
        service.GetTenantQuotaSnapshotForTesting("tenant-a").has_value());
}

TEST_F(MasterServiceTenantQuotaTest,
       DiskPutEndBeforeMemoryKeepsQuotaReservation) {
    MasterService service(
        MakeConfig(/*default_quota=*/1000, /*pool_capacity=*/1000,
                   /*enable_quota=*/true, /*root_fs_dir=*/"/tmp/mooncake"));
    UUID client_id = MountSegment(service);

    auto start =
        service.PutStart(client_id, "key", "tenant-a", 400, MemoryConfig());
    ASSERT_TRUE(start.has_value()) << toString(start.error());
    EXPECT_EQ(Snapshot(service, "tenant-a").used_bytes, 0);
    EXPECT_EQ(Snapshot(service, "tenant-a").reserved_bytes, 400);

    auto disk_end =
        service.PutEnd(client_id, "key", "tenant-a", ReplicaType::DISK);
    ASSERT_TRUE(disk_end.has_value()) << toString(disk_end.error());
    EXPECT_EQ(Snapshot(service, "tenant-a").used_bytes, 0);
    EXPECT_EQ(Snapshot(service, "tenant-a").reserved_bytes, 400);

    auto memory_end =
        service.PutEnd(client_id, "key", "tenant-a", ReplicaType::MEMORY);
    ASSERT_TRUE(memory_end.has_value()) << toString(memory_end.error());
    EXPECT_EQ(Snapshot(service, "tenant-a").used_bytes, 400);
    EXPECT_EQ(Snapshot(service, "tenant-a").reserved_bytes, 0);
}

TEST_F(MasterServiceTenantQuotaTest, ChangedSizeUpsertSuccessSwapsCharge) {
    MasterService service(MakeConfig(/*default_quota=*/1000,
                                     /*pool_capacity=*/1000));
    UUID client_id = MountSegment(service);

    PutComplete(service, client_id, "key", "tenant-a", 400);
    auto start =
        service.UpsertStart(client_id, "key", "tenant-a", 600, MemoryConfig());
    ASSERT_TRUE(start.has_value()) << toString(start.error());
    EXPECT_EQ(Snapshot(service, "tenant-a").used_bytes, 400);
    EXPECT_EQ(Snapshot(service, "tenant-a").reserved_bytes, 600);

    auto end =
        service.UpsertEnd(client_id, "key", "tenant-a", ReplicaType::MEMORY);
    ASSERT_TRUE(end.has_value()) << toString(end.error());
    EXPECT_EQ(Snapshot(service, "tenant-a").used_bytes, 600);
    EXPECT_EQ(Snapshot(service, "tenant-a").reserved_bytes, 0);
}

TEST_F(MasterServiceTenantQuotaTest, ChangedSizeUpsertRevokeReleasesOldAndNew) {
    MasterService service(MakeConfig(/*default_quota=*/1000,
                                     /*pool_capacity=*/1000));
    UUID client_id = MountSegment(service);

    PutComplete(service, client_id, "key", "tenant-a", 400);
    auto start =
        service.UpsertStart(client_id, "key", "tenant-a", 600, MemoryConfig());
    ASSERT_TRUE(start.has_value()) << toString(start.error());

    auto revoke =
        service.UpsertRevoke(client_id, "key", "tenant-a", ReplicaType::MEMORY);
    ASSERT_TRUE(revoke.has_value()) << toString(revoke.error());
    EXPECT_FALSE(
        service.GetTenantQuotaSnapshotForTesting("tenant-a").has_value());
}

}  // namespace mooncake::test
