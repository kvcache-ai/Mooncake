#include "master_service.h"

#include <limits>

#include <gtest/gtest.h>

#include "types.h"

namespace mooncake::test {

class MasterServiceTenantQuotaTest : public ::testing::Test {
   protected:
    static constexpr size_t kSegmentBase = 0x500000000;

    MasterServiceConfig MakeConfig(uint64_t default_quota,
                                   uint64_t pool_capacity,
                                   bool enable_quota = true) {
        return MasterServiceConfig::builder()
            .set_root_fs_dir("")
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

TEST_F(MasterServiceTenantQuotaTest,
       FirstTenantPutStartUsesInlineDefaultWithoutRecompute) {
    MasterService service(MakeConfig(/*default_quota=*/1000,
                                     /*pool_capacity=*/100));
    UUID client_id = MountSegment(service);

    auto start =
        service.PutStart(client_id, "key", "tenant-a", 800, MemoryConfig());

    ASSERT_TRUE(start.has_value()) << toString(start.error());
    auto snapshot = Snapshot(service, "tenant-a");
    EXPECT_EQ(snapshot.requested_quota_bytes, 1000);
    EXPECT_EQ(snapshot.effective_quota_bytes, 1000);
    EXPECT_EQ(snapshot.used_bytes, 0);
    EXPECT_EQ(snapshot.reserved_bytes, 800);
    AbortQuota(service, "tenant-a", 800);
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
    EXPECT_EQ(Snapshot(service, "tenant-a").reserved_bytes, 0);
}

TEST_F(MasterServiceTenantQuotaTest, RemoveReleasesCommittedCharge) {
    MasterService service(MakeConfig(/*default_quota=*/1000,
                                     /*pool_capacity=*/1000));
    UUID client_id = MountSegment(service);

    PutComplete(service, client_id, "key", "tenant-a", 400);
    ASSERT_EQ(Snapshot(service, "tenant-a").used_bytes, 400);

    ASSERT_TRUE(service.Remove("key", "tenant-a", /*force=*/true).has_value());
    EXPECT_EQ(Snapshot(service, "tenant-a").used_bytes, 0);
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
    EXPECT_EQ(Snapshot(service, "tenant-a").used_bytes, 0);
    EXPECT_EQ(Snapshot(service, "tenant-a").reserved_bytes, 0);
}

}  // namespace mooncake::test
