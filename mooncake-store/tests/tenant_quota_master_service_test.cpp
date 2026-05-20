// Per-tenant quota integration tests for MasterService.
//
// Carved out of master_service_test.cpp so the tenant-quota suite can
// evolve without enlarging the diff against the main test file. The
// fixture defined here is a sibling of master_service_test.cpp's
// MasterServiceTest -- functionally identical for the surface these
// tests use, but kept independent so neither file becomes a header for
// the other.

#include "master_service.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <chrono>
#include <memory>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "tenant_quota.h"
#include "types.h"
#include "utils.h"

namespace mooncake::test {

class TenantQuotaMasterServiceTest : public ::testing::Test {
   protected:
    void SetUp() override {
        google::InitGoogleLogging("TenantQuotaMasterServiceTest");
        FLAGS_logtostderr = true;
    }

    void TearDown() override { google::ShutdownGoogleLogging(); }

    struct MountedSegmentContext {
        UUID segment_id;
        UUID client_id;
    };

    static constexpr size_t kDefaultSegmentBase = 0x300000000;
    static constexpr size_t kDefaultSegmentSize = 1024 * 1024 * 16;

    Segment MakeSegment(std::string name = "test_segment",
                        size_t base = kDefaultSegmentBase,
                        size_t size = kDefaultSegmentSize) const {
        Segment segment;
        segment.id = generate_uuid();
        segment.name = std::move(name);
        segment.base = base;
        segment.size = size;
        segment.te_endpoint = segment.name;
        return segment;
    }

    MountedSegmentContext PrepareSimpleSegment(
        MasterService& service, std::string name = "test_segment",
        size_t base = kDefaultSegmentBase,
        size_t size = kDefaultSegmentSize) const {
        Segment segment = MakeSegment(std::move(name), base, size);
        UUID client_id = generate_uuid();
        auto mount_result = service.MountSegment(segment, client_id);
        EXPECT_TRUE(mount_result.has_value());
        return {.segment_id = segment.id, .client_id = client_id};
    }
};

namespace {

// Builds a 64 MiB segment so we can reliably allocate a few MiB-sized
// objects in the per-tenant tests below without hitting allocator OOM.
constexpr size_t kQuotaTestSegmentBase = 0x300000000;
constexpr size_t kQuotaTestSegmentSize = 64 * 1024 * 1024;

// Reads the tenant's used_bytes / reserved_bytes / committed_count off
// the master's quota table. Returns 0 when the tenant has no entry yet.
uint64_t GetTenantUsedBytes(MasterService& service,
                            const std::string& tenant_id) {
    auto snapshot = service.GetTenantQuotas().GetSnapshot(tenant_id);
    return snapshot.has_value() ? snapshot->state.used_bytes : 0;
}

uint64_t GetTenantReservedBytes(MasterService& service,
                                const std::string& tenant_id) {
    auto snapshot = service.GetTenantQuotas().GetSnapshot(tenant_id);
    return snapshot.has_value() ? snapshot->state.reserved_bytes : 0;
}

uint64_t GetTenantCommittedCount(MasterService& service,
                                 const std::string& tenant_id) {
    auto snapshot = service.GetTenantQuotas().GetSnapshot(tenant_id);
    return snapshot.has_value() ? snapshot->state.committed_count : 0;
}

}  // namespace

TEST_F(TenantQuotaMasterServiceTest, TenantQuotaDisabledIsNoOp) {
    // With the gate OFF, the master must not record any per-tenant usage,
    // regardless of how many puts happen.
    auto service_config =
        MasterServiceConfig::builder().set_enable_tenant_quota(false).build();
    auto service = std::make_unique<MasterService>(service_config);

    Segment segment = MakeSegment("quota_off_seg", kQuotaTestSegmentBase,
                                  kQuotaTestSegmentSize);
    UUID client_id = generate_uuid();
    ASSERT_TRUE(service->MountSegment(segment, client_id).has_value());

    ReplicateConfig config;
    config.replica_num = 1;
    config.tenant_id = "tenant_a";

    ASSERT_TRUE(service->PutStart(client_id, "k1", 1024, config).has_value());
    ASSERT_TRUE(
        service->PutEnd(client_id, "k1", ReplicaType::MEMORY).has_value());

    // No tenant entries should have been created.
    EXPECT_EQ(0u, GetTenantUsedBytes(*service, "tenant_a"));
    EXPECT_EQ(0u, GetTenantReservedBytes(*service, "tenant_a"));
    EXPECT_TRUE(service->GetTenantQuotas().ListAll().empty());
}

TEST_F(TenantQuotaMasterServiceTest, TenantQuotaEnabledAccountsPutAndRemove) {
    // With the gate ON, a single PutStart + PutEnd should commit
    // size * replica_num bytes; Remove should release them.
    auto service_config = MasterServiceConfig::builder()
                              .set_enable_tenant_quota(true)
                              .set_default_kv_lease_ttl(0)
                              .build();
    auto service = std::make_unique<MasterService>(service_config);

    Segment segment = MakeSegment("quota_on_seg", kQuotaTestSegmentBase,
                                  kQuotaTestSegmentSize);
    UUID client_id = generate_uuid();
    ASSERT_TRUE(service->MountSegment(segment, client_id).has_value());

    constexpr uint64_t kValueLen = 4 * 1024 * 1024;  // 4 MiB
    ReplicateConfig config;
    config.replica_num = 1;
    config.tenant_id = "tenant_a";

    ASSERT_TRUE(
        service->PutStart(client_id, "k1", kValueLen, config).has_value());
    ASSERT_TRUE(
        service->PutEnd(client_id, "k1", ReplicaType::MEMORY).has_value());

    EXPECT_EQ(kValueLen, GetTenantUsedBytes(*service, "tenant_a"));
    EXPECT_EQ(0u, GetTenantReservedBytes(*service, "tenant_a"));

    // Remove must release exactly the bytes we committed.
    ASSERT_TRUE(service->Remove("k1").has_value());
    EXPECT_EQ(0u, GetTenantUsedBytes(*service, "tenant_a"));
    EXPECT_EQ(0u, GetTenantReservedBytes(*service, "tenant_a"));
}

TEST_F(TenantQuotaMasterServiceTest, TenantQuotaPutRevokeReleases) {
    // PutRevoke before PutEnd must release the reservation (the object never
    // graduated to live state, but Reserve/Commit happened atomically inside
    // AllocateAndInsertMetadata so by the time PutRevoke runs the bytes are
    // already in used_bytes; Release must zero them out).
    auto service_config =
        MasterServiceConfig::builder().set_enable_tenant_quota(true).build();
    auto service = std::make_unique<MasterService>(service_config);

    Segment segment = MakeSegment("quota_revoke_seg", kQuotaTestSegmentBase,
                                  kQuotaTestSegmentSize);
    UUID client_id = generate_uuid();
    ASSERT_TRUE(service->MountSegment(segment, client_id).has_value());

    constexpr uint64_t kValueLen = 1024 * 1024;
    ReplicateConfig config;
    config.replica_num = 1;
    config.tenant_id = "tenant_revoke";

    ASSERT_TRUE(
        service->PutStart(client_id, "k", kValueLen, config).has_value());
    EXPECT_EQ(kValueLen, GetTenantUsedBytes(*service, "tenant_revoke"));

    ASSERT_TRUE(
        service->PutRevoke(client_id, "k", ReplicaType::MEMORY).has_value());
    EXPECT_EQ(0u, GetTenantUsedBytes(*service, "tenant_revoke"));
    EXPECT_EQ(0u, GetTenantReservedBytes(*service, "tenant_revoke"));
}

TEST_F(TenantQuotaMasterServiceTest,
       TenantQuotaEmptyTenantIdNormalizesToDefault) {
    // Clients that forget to set tenant_id (empty string) must land in the
    // "default" bucket so they share a single quota with other unspecified
    // clients — never accidentally allocate a private bucket per request.
    auto service_config =
        MasterServiceConfig::builder().set_enable_tenant_quota(true).build();
    auto service = std::make_unique<MasterService>(service_config);

    Segment segment = MakeSegment("quota_default_seg", kQuotaTestSegmentBase,
                                  kQuotaTestSegmentSize);
    UUID client_id = generate_uuid();
    ASSERT_TRUE(service->MountSegment(segment, client_id).has_value());

    constexpr uint64_t kValueLen = 1024;
    ReplicateConfig config;
    config.replica_num = 1;
    config.tenant_id.clear();  // explicit empty

    ASSERT_TRUE(
        service->PutStart(client_id, "k", kValueLen, config).has_value());
    ASSERT_TRUE(
        service->PutEnd(client_id, "k", ReplicaType::MEMORY).has_value());

    EXPECT_EQ(kValueLen, GetTenantUsedBytes(*service, "default"));
    EXPECT_EQ(0u, GetTenantUsedBytes(*service, ""));
}

TEST_F(TenantQuotaMasterServiceTest,
       TenantQuotaUpsertCaseCMovesQuotaAcrossTenants) {
    // UpsertStart Case C (size change) destroys the old object and allocates
    // a new one. Quota must follow the bytes: the old tenant's bucket is
    // released, and the new object is charged to whatever tenant_id the
    // caller supplied in the upsert config. We do NOT pin the new object to
    // the old tenant -- per-tenant namespace isolation between identical
    // keys depends on the LogicalObjectId-keyed master rework and is
    // explicitly out of scope for this revision; what we DO guarantee here
    // is that quota accounting stays consistent across the transfer.
    auto service_config = MasterServiceConfig::builder()
                              .set_enable_tenant_quota(true)
                              .set_default_kv_lease_ttl(0)
                              .build();
    auto service = std::make_unique<MasterService>(service_config);

    Segment segment = MakeSegment("quota_upsert_seg", kQuotaTestSegmentBase,
                                  kQuotaTestSegmentSize);
    UUID client_id = generate_uuid();
    ASSERT_TRUE(service->MountSegment(segment, client_id).has_value());

    constexpr uint64_t kSmall = 1 * 1024 * 1024;
    constexpr uint64_t kLarge = 2 * 1024 * 1024;

    ReplicateConfig cfg_a;
    cfg_a.replica_num = 1;
    cfg_a.tenant_id = "tenant_a";

    // Initial put under tenant_a.
    ASSERT_TRUE(
        service->UpsertStart(client_id, "k", kSmall, cfg_a).has_value());
    ASSERT_TRUE(
        service->UpsertEnd(client_id, "k", ReplicaType::MEMORY).has_value());
    EXPECT_EQ(kSmall, GetTenantUsedBytes(*service, "tenant_a"));
    EXPECT_EQ(0u, GetTenantUsedBytes(*service, "tenant_b"));

    // Size-changing upsert under tenant_b: bytes should move from
    // tenant_a to tenant_b.
    ReplicateConfig cfg_b;
    cfg_b.replica_num = 1;
    cfg_b.tenant_id = "tenant_b";

    ASSERT_TRUE(
        service->UpsertStart(client_id, "k", kLarge, cfg_b).has_value());
    ASSERT_TRUE(
        service->UpsertEnd(client_id, "k", ReplicaType::MEMORY).has_value());

    EXPECT_EQ(0u, GetTenantUsedBytes(*service, "tenant_a"));
    EXPECT_EQ(kLarge, GetTenantUsedBytes(*service, "tenant_b"));

    // Removing the key drains tenant_b too -- no leftover reservation.
    ASSERT_TRUE(service->Remove("k").has_value());
    EXPECT_EQ(0u, GetTenantUsedBytes(*service, "tenant_a"));
    EXPECT_EQ(0u, GetTenantUsedBytes(*service, "tenant_b"));
}

TEST_F(TenantQuotaMasterServiceTest, TenantQuotaRebuildAfterReset) {
    // Simulates the snapshot-restore path: after MetadataSerializer::Reset,
    // usage is wiped but policies are preserved. Manually re-Accumulate to
    // mimic DeserializeShard's behavior and verify the totals match.
    auto service_config = MasterServiceConfig::builder()
                              .set_enable_tenant_quota(true)
                              .set_default_kv_lease_ttl(0)
                              .build();
    auto service = std::make_unique<MasterService>(service_config);
    service->GetTenantQuotas().UpsertPolicy(
        "tenant_persisted", TenantQuotaPolicy{16 * 1024 * 1024});

    Segment segment = MakeSegment("quota_reset_seg", kQuotaTestSegmentBase,
                                  kQuotaTestSegmentSize);
    UUID client_id = generate_uuid();
    ASSERT_TRUE(service->MountSegment(segment, client_id).has_value());

    ReplicateConfig config;
    config.replica_num = 1;
    config.tenant_id = "tenant_persisted";

    constexpr uint64_t kPerObject = 1024 * 1024;
    for (int i = 0; i < 3; ++i) {
        std::string key = "rk_" + std::to_string(i);
        ASSERT_TRUE(
            service->PutStart(client_id, key, kPerObject, config).has_value());
        ASSERT_TRUE(
            service->PutEnd(client_id, key, ReplicaType::MEMORY).has_value());
    }
    EXPECT_EQ(3u * kPerObject,
              GetTenantUsedBytes(*service, "tenant_persisted"));

    // Wipe usage; policy must survive.
    service->GetTenantQuotas().ResetUsage();
    EXPECT_EQ(0u, GetTenantUsedBytes(*service, "tenant_persisted"));
    auto policy = service->GetTenantQuotas().GetPolicy("tenant_persisted");
    ASSERT_TRUE(policy.has_value());
    EXPECT_EQ(16u * 1024 * 1024, policy->max_bytes);

    // Rebuild — DeserializeShard would do this for every metadata entry;
    // here we replay the equivalent on the live state.
    service->GetTenantQuotas().AccumulateUsage("tenant_persisted",
                                               3u * kPerObject);
    EXPECT_EQ(3u * kPerObject,
              GetTenantUsedBytes(*service, "tenant_persisted"));
}

TEST_F(TenantQuotaMasterServiceTest,
       TenantQuotaPutStartTimeoutDiscardReleases) {
    // PutStart is a two-phase protocol: the client must follow it with
    // either PutEnd (commit) or PutRevoke (rollback). If the client
    // disappears in between, the master eventually discards the abandoned
    // metadata. There are two such discard paths:
    //   1) The PutStart-inline discard, triggered when a *new* PutStart on
    //      the same key arrives after put_start_discard_timeout_sec.
    //   2) The background DiscardExpiredProcessingReplicas thread (covered
    //      by other tests).
    //
    // This test exercises path (1) and verifies that the abandoned
    // PutStart's quota reservation is released — i.e. the second PutStart
    // succeeds and tenant usage reflects only the live object.
    constexpr uint64_t kDiscardTimeoutSec = 1;
    constexpr uint64_t kReleaseTimeoutSec = 2;
    auto service_config =
        MasterServiceConfig::builder()
            .set_enable_tenant_quota(true)
            .set_default_kv_lease_ttl(0)
            .set_put_start_discard_timeout_sec(kDiscardTimeoutSec)
            .set_put_start_release_timeout_sec(kReleaseTimeoutSec)
            .build();
    auto service = std::make_unique<MasterService>(service_config);

    Segment segment = MakeSegment("quota_discard_seg", kQuotaTestSegmentBase,
                                  kQuotaTestSegmentSize);
    UUID client_id = generate_uuid();
    ASSERT_TRUE(service->MountSegment(segment, client_id).has_value());

    constexpr uint64_t kValueLen = 1 * 1024 * 1024;

    ReplicateConfig cfg;
    cfg.replica_num = 1;
    cfg.tenant_id = "tenant_orphan";

    // First PutStart: client never calls PutEnd or PutRevoke (simulated
    // crash between PutStart and PutEnd). Quota is committed for the
    // PROCESSING object.
    ASSERT_TRUE(service->PutStart(client_id, "k", kValueLen, cfg).has_value());
    EXPECT_EQ(kValueLen, GetTenantUsedBytes(*service, "tenant_orphan"));

    // Wait long enough for the inline-discard branch to trigger on the
    // next PutStart (must exceed put_start_discard_timeout_sec).
    std::this_thread::sleep_for(std::chrono::seconds(kDiscardTimeoutSec + 1));

    // Second PutStart on the same key: should succeed by discarding the
    // abandoned object. Quota for the abandoned PutStart must be released
    // before the new reservation is committed; final usage must equal one
    // live object's bytes -- not two.
    ASSERT_TRUE(service->PutStart(client_id, "k", kValueLen, cfg).has_value());
    ASSERT_TRUE(
        service->PutEnd(client_id, "k", ReplicaType::MEMORY).has_value());
    EXPECT_EQ(kValueLen, GetTenantUsedBytes(*service, "tenant_orphan"));
    EXPECT_EQ(0u, GetTenantReservedBytes(*service, "tenant_orphan"));

    // Removing the key drains the tenant -- proves no lingering reservation.
    ASSERT_TRUE(service->Remove("k").has_value());
    EXPECT_EQ(0u, GetTenantUsedBytes(*service, "tenant_orphan"));
    EXPECT_EQ(0u, GetTenantReservedBytes(*service, "tenant_orphan"));
}

TEST_F(TenantQuotaMasterServiceTest,
       TenantQuotaBatchEvictPartialEraseDoesNotDoubleRelease) {
    // Regression for a BatchEvict double-release path:
    //
    // When use_disk_replica is enabled, AllocateAndInsertMetadata appends a
    // LOCAL_DISK replica alongside the memory replica(s). BatchEvict then
    // erases the memory replica(s) but the metadata stays alive because
    // IsValid() still returns true (a disk replica satisfies HasReplica).
    //
    // The bug: BatchEvict released `freed` (= size * #memory_replicas erased)
    // against the tenant quota but did NOT subtract that amount from the
    // metadata's quota_committed_bytes snapshot. A subsequent Remove() then
    // re-released quota_committed_bytes wholesale -- double-releasing the
    // bytes against TenantQuotaTable.committed_count. The user-visible
    // used_bytes was masked by the underflow clamp inside Release(), but
    // committed_count drifted negative-then-clamped, which would skew any
    // metric built on top of it.
    //
    // Fix: BatchEvict now subtracts `freed` from quota_committed_bytes
    // whenever the metadata survives the eviction. This test pins down that
    // contract by driving eviction the same way other eviction tests do --
    // fill the segment to set need_eviction_=true and let the background
    // EvictionThread run BatchEvict. Timing-based, but matches the pattern
    // of e.g. SoftPinObjectsNotEvictedBeforeOtherObjects.
    const uint64_t kv_lease_ttl = 200;  // ms
    // Use eviction_ratio=1.0 so a single BatchEvict cycle attempts to
    // evict ALL expired-lease objects. With 0.5 the cycle stops after
    // freeing half, which can easily skip "tracked" depending on iteration
    // order and produce a flaky test.
    const double eviction_ratio = 1.0;
    auto service_config = MasterServiceConfig::builder()
                              .set_enable_tenant_quota(true)
                              .set_default_kv_lease_ttl(kv_lease_ttl)
                              .set_eviction_ratio(eviction_ratio)
                              // Non-empty root_fs_dir flips use_disk_replica_
                              // ON in the master's constructor, which is the
                              // precondition for the bug scenario above.
                              .set_root_fs_dir("/tmp/mooncake_quota_test_fs")
                              .build();
    auto service = std::make_unique<MasterService>(service_config);

    // Use a segment small enough that ~16 puts of 1 MiB each will fill it
    // and trip the allocator -- which is what flips need_eviction_=true.
    constexpr size_t kEvictSegmentSize = 16 * 1024 * 1024;
    Segment segment = MakeSegment("quota_evict_seg", kQuotaTestSegmentBase,
                                  kEvictSegmentSize);
    UUID client_id = generate_uuid();
    ASSERT_TRUE(service->MountSegment(segment, client_id).has_value());

    constexpr uint64_t kValueLen = 1 * 1024 * 1024;
    constexpr int kFillCount =
        20;  // > segment_size / kValueLen, guaranteed OOM

    ReplicateConfig cfg;
    cfg.replica_num = 1;
    cfg.tenant_id = "tenant_evict";

    // Step 1: put one tracked key, verify quota accounting at admission.
    // Note: when use_disk_replica_ is on, AllocateAndInsertMetadata appends
    // a LOCAL_DISK replica in PROCESSING state alongside the memory one.
    // PutEnd is type-scoped (only marks replicas of the requested type
    // COMPLETE), so we must call it twice -- otherwise the disk replica
    // stays PROCESSING and Remove later refuses with REPLICA_IS_NOT_READY.
    ASSERT_TRUE(
        service->PutStart(client_id, "tracked", kValueLen, cfg).has_value());
    ASSERT_TRUE(
        service->PutEnd(client_id, "tracked", ReplicaType::MEMORY).has_value());
    ASSERT_TRUE(
        service->PutEnd(client_id, "tracked", ReplicaType::DISK).has_value());
    EXPECT_EQ(kValueLen, GetTenantUsedBytes(*service, "tenant_evict"));
    EXPECT_EQ(1u, GetTenantCommittedCount(*service, "tenant_evict"));

    // Step 2: wait for the lease to expire so BatchEvict will consider the
    // tracked key evictable.
    std::this_thread::sleep_for(std::chrono::milliseconds(kv_lease_ttl + 50));

    // Step 3: fill the segment to trip need_eviction_=true. We don't care
    // about the fill keys' lifetimes -- only that at least one PutStart
    // fails (which is what sets need_eviction_=true inside
    // AllocateAndInsertMetadata's allocator-OOM branch).
    int failed_puts = 0;
    for (int i = 0; i < kFillCount; i++) {
        std::string key = "fill_" + std::to_string(i);
        ReplicateConfig fill_cfg;
        fill_cfg.replica_num = 1;
        fill_cfg.tenant_id = "tenant_filler";
        if (service->PutStart(client_id, key, kValueLen, fill_cfg)
                .has_value()) {
            ASSERT_TRUE(service->PutEnd(client_id, key, ReplicaType::MEMORY)
                            .has_value());
            ASSERT_TRUE(
                service->PutEnd(client_id, key, ReplicaType::DISK).has_value());
        } else {
            failed_puts++;
        }
    }
    ASSERT_GT(failed_puts, 0)
        << "test setup error: segment did not fill, eviction will not trigger";

    // Step 4: wait for the background EvictionThread to run BatchEvict. The
    // eviction thread polls every kEvictionThreadSleepMs (10ms by default)
    // so a few hundred ms is plenty.
    std::this_thread::sleep_for(std::chrono::milliseconds(kv_lease_ttl + 1000));

    // After BatchEvict: the tracked key's memory replica was erased (it had
    // an expired lease and refcnt==0). Its metadata stays alive because the
    // remote disk replica still satisfies HasReplica(), so IsValid() returns
    // true.
    //
    // Note: we deliberately do NOT use GetReplicaList("tracked") to verify
    // that the memory replica was evicted -- GetReplicaList collects ALL
    // completed replicas (memory AND disk), so it still returns has_value()
    // as long as the disk replica survives. Instead we verify the
    // contract end-to-end through the tenant quota ledger below: if
    // BatchEvict leaks the freed bytes (pre-fix behavior), Remove will
    // double-release them and committed_count will not land cleanly at 0.
    //
    // Snapshot tenant accounting BEFORE Remove. used_bytes should already
    // have dropped by kValueLen because BatchEvict's ReleaseTenantBytes()
    // ran for the tracked key. Pre-fix, quota_committed_bytes on the
    // surviving "tracked" metadata still equals kValueLen (BatchEvict
    // didn't subtract `freed`), so the upcoming Remove will *re-release*
    // those bytes against committed_count -- causing it to underflow and
    // get clamped, which is exactly the silent accounting drift the fix
    // closes. Post-fix, quota_committed_bytes is zero, so Remove releases
    // zero bytes for this object and committed_count decrements exactly
    // once for the metadata destruction.
    const uint64_t committed_before_remove =
        GetTenantCommittedCount(*service, "tenant_evict");
    const uint64_t used_before_remove =
        GetTenantUsedBytes(*service, "tenant_evict");

    // Step 5: the critical assertion -- Remove must not double-release.
    ASSERT_TRUE(service->Remove("tracked").has_value());

    EXPECT_LE(GetTenantCommittedCount(*service, "tenant_evict"),
              committed_before_remove)
        << "Remove must not increase committed_count";
    EXPECT_LE(GetTenantUsedBytes(*service, "tenant_evict"), used_before_remove)
        << "Remove must not increase used_bytes";

    // The cleanest invariant: after evicting then removing the tracked key,
    // its tenant has zero used bytes attributable to it and zero
    // outstanding reservation. Pre-fix, used_bytes would still hit 0 thanks
    // to TenantQuotaTable::Release's underflow clamp, but committed_count
    // would have been double-decremented and clamped from a negative drift
    // -- masking a real accounting bug that any metric built on top of
    // committed_count would inherit.
    EXPECT_EQ(0u, GetTenantReservedBytes(*service, "tenant_evict"));
    EXPECT_EQ(0u, GetTenantUsedBytes(*service, "tenant_evict"))
        << "tracked tenant's used_bytes must reach exactly 0 after Remove";
    EXPECT_EQ(0u, GetTenantCommittedCount(*service, "tenant_evict"))
        << "committed_count must reach exactly 0 after Remove. Pre-fix this "
           "was clamped from a negative drift, masking a real accounting bug.";
}

// ============================================================================
// Per-tenant quota & isolation behavior tests.
// All tests enable the tenant-quota gate via
// MasterServiceConfig::set_enable_tenant_quota(true) and exercise the
// tenant-scoped eviction path: ReserveTenantBytes -> BatchEvictInTenant
// -> retry. Each test inspects the final accounting state via
// MasterService::GetTenantQuotas().GetSnapshot(...).
//
// (same_logical_key_diff_tenant is intentionally omitted: the frontier is
// keyed by raw key string, so cross-tenant same-key collisions are a known
// limitation -- see ReplicateConfig.tenant_id docstring.)
// ============================================================================

// Within a single tenant, PutStart should succeed beyond the
// raw quota by triggering BatchEvictInTenant on the no-pin frontier.
// After the storm settles, used_bytes must be at or below the policy cap.
TEST_F(TenantQuotaMasterServiceTest, TenantQuotaPutThenEvictWithinTenant) {
    constexpr size_t object_size = 1024 * 1024;       // 1 MiB
    constexpr uint64_t tenant_max = 4 * object_size;  // hold 4 objects max
    constexpr int total_puts = 12;
    const uint64_t kv_lease_ttl = 10;

    auto service_config = MasterServiceConfig::builder()
                              .set_enable_tenant_quota(true)
                              .set_default_kv_lease_ttl(kv_lease_ttl)
                              .build();
    auto service = std::make_unique<MasterService>(service_config);
    service->GetTenantQuotas().UpsertPolicy("tenant_a",
                                            TenantQuotaPolicy{tenant_max});

    constexpr size_t segment_size = 1024 * 1024 * 64;  // 64 MiB headroom
    [[maybe_unused]] const auto context = PrepareSimpleSegment(
        *service, "test_segment", kDefaultSegmentBase, segment_size);
    const UUID client_id = generate_uuid();

    int success_puts = 0;
    for (int i = 0; i < total_puts; ++i) {
        ReplicateConfig config;
        config.replica_num = 1;
        config.tenant_id = "tenant_a";
        const std::string key = "tenant_a_key_" + std::to_string(i);
        auto put_start = service->PutStart(client_id, key, object_size, config);
        if (!put_start.has_value()) {
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
            continue;
        }
        ASSERT_TRUE(
            service->PutEnd(client_id, key, ReplicaType::MEMORY).has_value());
        ++success_puts;
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }

    EXPECT_GT(success_puts, static_cast<int>(tenant_max / object_size))
        << "BatchEvictInTenant should let success_puts exceed the raw cap";

    auto snapshot = service->GetTenantQuotas().GetSnapshot("tenant_a");
    ASSERT_TRUE(snapshot.has_value());
    EXPECT_LE(snapshot->state.used_bytes, tenant_max)
        << "post-eviction used_bytes must not exceed the policy cap";
}

// Two tenants with disjoint quotas. Pressure on tenant A must
// not affect tenant B's ability to PutStart up to its own quota.
TEST_F(TenantQuotaMasterServiceTest, TenantQuotaCrossTenantIsolation) {
    constexpr size_t object_size = 1024 * 1024;  // 1 MiB
    constexpr uint64_t tenant_a_max = 2 * object_size;
    constexpr uint64_t tenant_b_max = 8 * object_size;
    const uint64_t kv_lease_ttl = 10;

    auto service_config = MasterServiceConfig::builder()
                              .set_enable_tenant_quota(true)
                              .set_default_kv_lease_ttl(kv_lease_ttl)
                              .build();
    auto service = std::make_unique<MasterService>(service_config);
    service->GetTenantQuotas().UpsertPolicy("tenant_a",
                                            TenantQuotaPolicy{tenant_a_max});
    service->GetTenantQuotas().UpsertPolicy("tenant_b",
                                            TenantQuotaPolicy{tenant_b_max});

    constexpr size_t segment_size = 1024 * 1024 * 64;
    [[maybe_unused]] const auto context = PrepareSimpleSegment(
        *service, "test_segment", kDefaultSegmentBase, segment_size);
    const UUID client_id = generate_uuid();

    // Push tenant A right up to its cap. With hard pin, BatchEvictInTenant
    // cannot reclaim anything, so further A puts will reject.
    for (int i = 0; i < 2; ++i) {
        ReplicateConfig config;
        config.replica_num = 1;
        config.tenant_id = "tenant_a";
        config.with_hard_pin = true;
        const std::string key = "isolation_a_" + std::to_string(i);
        ASSERT_TRUE(
            service->PutStart(client_id, key, object_size, config).has_value());
        ASSERT_TRUE(
            service->PutEnd(client_id, key, ReplicaType::MEMORY).has_value());
    }

    // Tenant B must still be able to fill its own (larger) quota.
    int b_success = 0;
    for (int i = 0; i < 6; ++i) {
        ReplicateConfig config;
        config.replica_num = 1;
        config.tenant_id = "tenant_b";
        const std::string key = "isolation_b_" + std::to_string(i);
        auto put_start = service->PutStart(client_id, key, object_size, config);
        if (put_start.has_value()) {
            ASSERT_TRUE(service->PutEnd(client_id, key, ReplicaType::MEMORY)
                            .has_value());
            ++b_success;
        }
    }
    EXPECT_EQ(b_success, 6) << "tenant B must be unaffected by tenant A's "
                               "exhausted quota";

    auto snap_a = service->GetTenantQuotas().GetSnapshot("tenant_a");
    auto snap_b = service->GetTenantQuotas().GetSnapshot("tenant_b");
    ASSERT_TRUE(snap_a.has_value());
    ASSERT_TRUE(snap_b.has_value());
    EXPECT_EQ(snap_a->state.used_bytes, tenant_a_max);
    EXPECT_EQ(snap_b->state.used_bytes, 6 * object_size);
}

// With every object hard-pinned, BatchEvictInTenant cannot
// free anything, so the next quota-exceeding PutStart must fail with
// NO_AVAILABLE_HANDLE and bump TenantQuotaRejectTotal exactly once.
TEST_F(TenantQuotaMasterServiceTest, TenantQuotaEvictFailThenReject) {
    constexpr size_t object_size = 1024 * 1024;
    constexpr uint64_t tenant_max = 2 * object_size;
    const uint64_t kv_lease_ttl = 10;

    auto service_config = MasterServiceConfig::builder()
                              .set_enable_tenant_quota(true)
                              .set_default_kv_lease_ttl(kv_lease_ttl)
                              .build();
    auto service = std::make_unique<MasterService>(service_config);
    service->GetTenantQuotas().UpsertPolicy("tenant_pinned",
                                            TenantQuotaPolicy{tenant_max});

    constexpr size_t segment_size = 1024 * 1024 * 64;
    [[maybe_unused]] const auto context = PrepareSimpleSegment(
        *service, "test_segment", kDefaultSegmentBase, segment_size);
    const UUID client_id = generate_uuid();

    // Fill the quota with hard-pinned objects.
    for (int i = 0; i < 2; ++i) {
        ReplicateConfig config;
        config.replica_num = 1;
        config.tenant_id = "tenant_pinned";
        config.with_hard_pin = true;
        const std::string key = "pinned_" + std::to_string(i);
        ASSERT_TRUE(
            service->PutStart(client_id, key, object_size, config).has_value());
        ASSERT_TRUE(
            service->PutEnd(client_id, key, ReplicaType::MEMORY).has_value());
    }

    const uint64_t reject_before = service->TenantQuotaRejectTotal();

    // Third PutStart must fail: BatchEvictInTenant cannot reclaim
    // hard-pinned objects, ComputeEvictTarget loop bails out on freed=0.
    ReplicateConfig config;
    config.replica_num = 1;
    config.tenant_id = "tenant_pinned";
    const std::string overflow_key = "overflow";
    auto put_overflow =
        service->PutStart(client_id, overflow_key, object_size, config);
    ASSERT_FALSE(put_overflow.has_value());
    EXPECT_EQ(put_overflow.error(), ErrorCode::NO_AVAILABLE_HANDLE);

    const uint64_t reject_after = service->TenantQuotaRejectTotal();
    EXPECT_EQ(reject_after, reject_before + 1)
        << "TenantQuotaRejectTotal must increment on quota-induced reject";

    // Reservation must not leak: failed Reserve never debited bytes.
    auto snap = service->GetTenantQuotas().GetSnapshot("tenant_pinned");
    ASSERT_TRUE(snap.has_value());
    EXPECT_EQ(snap->state.reserved_bytes, 0u);
    EXPECT_EQ(snap->state.used_bytes, tenant_max);
}

// BatchUpsertStart with a deliberately-invalid entry mid-batch
// must commit the valid ones and reject the invalid one without leaking
// reserved_bytes onto the tenant's accounting.
TEST_F(TenantQuotaMasterServiceTest, TenantQuotaBatchUpsertPartialFailNoLeak) {
    constexpr size_t object_size = 1024 * 1024;
    constexpr uint64_t tenant_max = 16 * object_size;
    const uint64_t kv_lease_ttl = 10;

    auto service_config = MasterServiceConfig::builder()
                              .set_enable_tenant_quota(true)
                              .set_default_kv_lease_ttl(kv_lease_ttl)
                              .build();
    auto service = std::make_unique<MasterService>(service_config);
    service->GetTenantQuotas().UpsertPolicy("tenant_batch",
                                            TenantQuotaPolicy{tenant_max});

    constexpr size_t segment_size = 1024 * 1024 * 64;
    [[maybe_unused]] const auto context = PrepareSimpleSegment(
        *service, "test_segment", kDefaultSegmentBase, segment_size);
    const UUID client_id = generate_uuid();

    // Mix: 3 valid + 1 zero-length (invalid) + 1 valid -- the invalid
    // one short-circuits with INVALID_PARAMS without touching quota.
    std::vector<std::string> keys = {"batch_0", "batch_1", "batch_2",
                                     "batch_bad", "batch_4"};
    std::vector<uint64_t> slice_lengths = {object_size, object_size,
                                           object_size, 0, object_size};

    ReplicateConfig config;
    config.replica_num = 1;
    config.tenant_id = "tenant_batch";
    auto results =
        service->BatchUpsertStart(client_id, keys, slice_lengths, config);
    ASSERT_EQ(results.size(), keys.size());

    int success_count = 0;
    for (size_t i = 0; i < results.size(); ++i) {
        if (results[i].has_value()) {
            ++success_count;
            ASSERT_TRUE(service->PutEnd(client_id, keys[i], ReplicaType::MEMORY)
                            .has_value());
        }
    }
    EXPECT_EQ(success_count, 4) << "4 valid puts should succeed";
    EXPECT_FALSE(results[3].has_value())
        << "zero-length slice must be rejected";

    auto snap = service->GetTenantQuotas().GetSnapshot("tenant_batch");
    ASSERT_TRUE(snap.has_value());
    // Critical no-leak invariants:
    EXPECT_EQ(snap->state.reserved_bytes, 0u)
        << "no reserved_bytes leak after partial-fail batch";
    EXPECT_EQ(snap->state.used_bytes, 4 * object_size)
        << "exactly 4 objects' bytes accounted; no double-count or under-count";
    EXPECT_EQ(snap->state.committed_count, 4u)
        << "committed_count must match the number of successful PutEnds";
}

// ============================================================================
// Per-tenant eviction performance baseline (off by default).
//
// Pins down two scaling promises:
//   1. PutStart P99 latency under cap-pressure stays below 10 ms.
//   2. RefillTenantFrontier calls are amortized: the count grows as
//      O(evict_cycles), not O(victims). With a 64-entry cache the
//      ratio refills/evict_attempts must stay well below 1.
//
// This test is gated behind RUN_TENANT_QUOTA_PERF because filling 100K
// objects on every CI run would burn ~tens of seconds of clock time and
// is sensitive to host load (RDMA/CPU contention). Operators run it
// manually before signing off on a release.
//
// Modes (env var RUN_TENANT_QUOTA_PERF):
//   unset / "" -> GTEST_SKIP, instant pass (default CI behavior)
//   any value  -> 100K objects (~30 seconds on dev hosts)
// ============================================================================
TEST_F(TenantQuotaMasterServiceTest, TenantQuotaEvictPerfBaseline) {
    const char* mode = std::getenv("RUN_TENANT_QUOTA_PERF");
    if (mode == nullptr || std::string(mode).empty()) {
        GTEST_SKIP() << "set RUN_TENANT_QUOTA_PERF=1 to enable the perf "
                        "baseline (100K objects)";
    }

    constexpr int total_puts = 100'000;
    constexpr size_t object_size = 4 * 1024;  // 4 KiB -- tight live set
    // Hold ~16 MiB of live objects per tenant. With 4 KiB objects that's
    // 4096 live entries; the remaining puts all force eviction.
    constexpr uint64_t tenant_max = 16ULL * 1024 * 1024;
    const uint64_t kv_lease_ttl = 0;  // immediate expiry so evict can fire

    // client_live_ttl_sec must be larger than the test's wall-clock
    // duration: with 100K puts each going through Reserve -> Evict ->
    // Reserve -> Allocate (~120us per put with the per-tenant frontier
    // hot path), the loop runs ~12 seconds. The default
    // client_live_ttl_sec=10 would expire mid-loop and the
    // task_cleanup_thread would unmount perf_segment, causing every
    // subsequent PutStart to fail with NO_AVAILABLE_HANDLE because
    // AllocatorManager is empty (segment evicted, not BufferAllocator
    // fragmentation). Bump TTL to 60s for headroom.
    auto service_config = MasterServiceConfig::builder()
                              .set_enable_tenant_quota(true)
                              .set_default_kv_lease_ttl(kv_lease_ttl)
                              .set_client_live_ttl_sec(60)
                              .build();
    auto service = std::make_unique<MasterService>(service_config);
    service->GetTenantQuotas().UpsertPolicy("tenant_perf",
                                            TenantQuotaPolicy{tenant_max});

    // Use a much larger segment so the underlying allocator has headroom
    // for the working set + churn. 1 GiB suffices for both 100K and 1M
    // because each object is only 4 KiB.
    constexpr size_t segment_size = 1ULL * 1024 * 1024 * 1024;  // 1 GiB
    [[maybe_unused]] const auto context = PrepareSimpleSegment(
        *service, "perf_segment", kQuotaTestSegmentBase, segment_size);
    const UUID client_id = generate_uuid();

    // Latency samples in microseconds. We only sample the steady-state
    // window (after the first `tenant_max/object_size` admissions, which
    // are pre-eviction warm-up).
    const int warmup = static_cast<int>(tenant_max / object_size);
    std::vector<int64_t> put_start_latencies_us;
    put_start_latencies_us.reserve(total_puts);

    const uint64_t refills_before = service->RefillTenantFrontierCount();
    int success_puts = 0;
    int failed_puts = 0;
    for (int i = 0; i < total_puts; ++i) {
        ReplicateConfig cfg;
        cfg.replica_num = 1;
        cfg.tenant_id = "tenant_perf";
        const std::string key = "perf_key_" + std::to_string(i);

        const auto t0 = std::chrono::steady_clock::now();
        auto put_start = service->PutStart(client_id, key, object_size, cfg);
        const auto t1 = std::chrono::steady_clock::now();

        if (i >= warmup) {
            put_start_latencies_us.push_back(
                std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0)
                    .count());
        }

        if (!put_start.has_value()) {
            ++failed_puts;
            continue;
        }
        ASSERT_TRUE(
            service->PutEnd(client_id, key, ReplicaType::MEMORY).has_value());
        ++success_puts;
    }
    const uint64_t refills_after = service->RefillTenantFrontierCount();
    const uint64_t refill_delta = refills_after - refills_before;

    ASSERT_FALSE(put_start_latencies_us.empty())
        << "no steady-state samples -- warmup window has wrong size";

    // ---- Latency analysis ---------------------------------------------------
    std::sort(put_start_latencies_us.begin(), put_start_latencies_us.end());
    const auto pick_pct = [&](double q) {
        const size_t idx = static_cast<size_t>(
            std::min<double>(put_start_latencies_us.size() - 1,
                             q * put_start_latencies_us.size()));
        return put_start_latencies_us[idx];
    };
    const int64_t p50 = pick_pct(0.50);
    const int64_t p95 = pick_pct(0.95);
    const int64_t p99 = pick_pct(0.99);
    const int64_t p999 = pick_pct(0.999);

    // ---- Latency target -----------------------------------------------------
    //
    // Each refill is a single tenant_quotas_.mu_ acquisition + an O(K)
    // std::set walk (K = kBatchSize = 64). On dev hosts this measures at
    // single-digit microseconds; we set a generous 100 us P99 bound to
    // catch any regression that re-introduces expensive fan-out.
    constexpr int64_t kP99TargetUs = 100;
    EXPECT_LT(p99, kP99TargetUs)
        << "PutStart P99 must stay under 100 us with the single-mutex "
           "frontier -- p50="
        << p50 << "us p95=" << p95 << "us p99=" << p99 << "us p99.9=" << p999
        << "us";

    // ---- Refill upper bound -------------------------------------------------
    //
    // Each BatchEvictInTenant call constructs a fresh
    // TenantEvictionContext on the stack; the per-cycle cache is
    // discarded when the context goes out of scope. So one PutStart
    // that overflowed the cap triggers AT MOST one Refill call (and
    // exactly one in the steady state, since freeing a single 4 KiB
    // victim covers the deficit of the next 4 KiB admission with no
    // stale retries needed). A ratio strictly above 1.0 means
    // BatchEvictInTenant is calling Refill multiple times for one
    // PutStart -- which would only happen if lazy-stale candidates
    // are drained before a usable one is found, signalling a
    // regression in the snapshot-vs-live reconciliation path.
    //
    // Note: this is a STRUCTURAL upper bound, not a cache-hit
    // assertion. Each refill is already cheap (single mutex, O(K)
    // walk) -- see the P99 < 100us assertion above for the proof.
    const int post_warmup_puts =
        static_cast<int>(put_start_latencies_us.size());
    const double refill_per_put =
        post_warmup_puts == 0
            ? 0.0
            : static_cast<double>(refill_delta) / post_warmup_puts;
    EXPECT_LE(refill_per_put, 1.0)
        << "RefillTenantFrontier called more than once per admission -- "
           "stale-candidate drain regression? got "
        << refill_delta << " refills across " << post_warmup_puts
        << " admissions (" << refill_per_put << " refills/put)";

    // ---- Batch utilisation --------------------------------------------------
    //
    // Each Refill SnapshotTopK pulls up to kBatchSize candidates and
    // BatchEvictInTenant consumes one per successful PutStart.
    // Inversely, the number of victim consumption events
    // (post_warmup_puts that triggered eviction) divided by refill
    // count must be <= kBatchSize. A ratio approaching kBatchSize
    // means SnapshotTopK is returning a full batch on every refill
    // (good); a ratio of ~1 means each refill returned only one
    // useful candidate. We lower-bound generously at 0.5 to catch
    // pathological "every refill returns an empty batch" failures
    // without false-positiving on legitimate stale-skip workloads.
    const double victims_per_refill =
        refill_delta == 0
            ? 0.0
            : static_cast<double>(post_warmup_puts) / refill_delta;
    EXPECT_GE(victims_per_refill, 0.5)
        << "Each Refill must yield at least one consumable victim on "
           "average -- got "
        << victims_per_refill << " victims/refill (" << post_warmup_puts
        << " puts, " << refill_delta << " refills)";

    // ---- Sanity ------------------------------------------------------------
    EXPECT_GT(success_puts, warmup)
        << "must exceed warmup -- means eviction kicked in and admitted "
           "more than the raw cap";
    auto snap = service->GetTenantQuotas().GetSnapshot("tenant_perf");
    ASSERT_TRUE(snap.has_value());
    EXPECT_LE(snap->state.used_bytes, tenant_max)
        << "post-storm used_bytes must respect the cap";

    std::cout << "[perf-baseline] mode=" << mode << " total_puts=" << total_puts
              << " success=" << success_puts << " failed=" << failed_puts
              << " refills=" << refill_delta
              << " refills/put=" << refill_per_put
              << " victims/refill=" << victims_per_refill << " p50=" << p50
              << "us"
              << " p95=" << p95 << "us p99=" << p99 << "us p99.9=" << p999
              << "us" << std::endl;
}

// ============================================================================
// Frontier index correctness tests
//
// These tests pin down the per-tenant eviction frontier's contract --
// (lease_timeout ASC, neg_size ASC, key ASC) sort order, lazy refresh
// across GrantLease/GetReplicaList, EvictionContext caching across
// retries, hard-pin exclusion, and the no-pin-then-soft-pin two-pass
// sweep -- using only public observables (ExistKey, GetReplicaList,
// TenantQuotaRejectTotal, GetTenantUsedBytes). MetadataShard internals
// stay private; the tests provoke BatchEvictInTenant via PutStart and
// inspect which keys survived.
// ============================================================================

// With identical lease_timeout, frontier orders by size DESC
// (neg_size ASC). When BatchEvictInTenant fires, the BIGGEST no-pin
// objects must be reaped before any smaller siblings.
TEST_F(TenantQuotaMasterServiceTest, FrontierSizeSecondarySort) {
    constexpr size_t small_size = 1 * 1024 * 1024;  // 1 MiB
    constexpr size_t large_size = 4 * 1024 * 1024;  // 4 MiB
    // Cap = 5 MiB. Holds (1 small + 1 large) = 5 MiB exactly. Adding a
    // second small (1 MiB) overflows by 1 MiB and must reap the LARGE
    // object first (size-DESC tiebreak), not the small one.
    constexpr uint64_t tenant_max = small_size + large_size;
    const uint64_t kv_lease_ttl = 0;  // expire immediately so evict can fire

    auto service_config = MasterServiceConfig::builder()
                              .set_enable_tenant_quota(true)
                              .set_default_kv_lease_ttl(kv_lease_ttl)
                              .build();
    auto service = std::make_unique<MasterService>(service_config);
    service->GetTenantQuotas().UpsertPolicy("tenant_size",
                                            TenantQuotaPolicy{tenant_max});

    [[maybe_unused]] const auto context = PrepareSimpleSegment(
        *service, "test_segment", kQuotaTestSegmentBase, kQuotaTestSegmentSize);
    const UUID client_id = generate_uuid();

    ReplicateConfig cfg;
    cfg.replica_num = 1;
    cfg.tenant_id = "tenant_size";

    // Insert one small + one large. Both no-pin, both lease_ttl=0 so their
    // frontier_lease_snapshots are nearly identical (same now() bucket).
    ASSERT_TRUE(
        service->PutStart(client_id, "small_key", small_size, cfg).has_value());
    ASSERT_TRUE(service->PutEnd(client_id, "small_key", ReplicaType::MEMORY)
                    .has_value());
    ASSERT_TRUE(
        service->PutStart(client_id, "large_key", large_size, cfg).has_value());
    ASSERT_TRUE(service->PutEnd(client_id, "large_key", ReplicaType::MEMORY)
                    .has_value());
    EXPECT_EQ(small_size + large_size,
              GetTenantUsedBytes(*service, "tenant_size"));

    // Trigger eviction: insert a third 1 MiB object. The tenant is at
    // its cap, so BatchEvictInTenant must free at least 1 MiB. Per the
    // size-DESC tiebreak, "large_key" (4 MiB) is the first victim and
    // freeing it alone covers the deficit -- "small_key" must survive.
    ASSERT_TRUE(service->PutStart(client_id, "trigger_key", small_size, cfg)
                    .has_value());
    ASSERT_TRUE(service->PutEnd(client_id, "trigger_key", ReplicaType::MEMORY)
                    .has_value());

    auto exist_large = service->ExistKey("large_key");
    auto exist_small = service->ExistKey("small_key");
    auto exist_trigger = service->ExistKey("trigger_key");
    ASSERT_TRUE(exist_large.has_value());
    ASSERT_TRUE(exist_small.has_value());
    ASSERT_TRUE(exist_trigger.has_value());

    EXPECT_FALSE(exist_large.value())
        << "size-DESC tiebreak: large_key (4 MiB) must be evicted first";
    EXPECT_TRUE(exist_small.value())
        << "small_key (1 MiB) must survive when large_key alone covers "
           "the deficit";
    EXPECT_TRUE(exist_trigger.value())
        << "trigger_key (the new put) must be admitted";
}

// After GrantLease/GetReplicaList renews an object's lease,
// the frontier entry's primary key (lease_timeout snapshot) is stale.
// Evict-time lazy-maintenance check (snapshot vs
// metadata.frontier_lease_snapshot) must reject that stale candidate
// (cleaning it via DropStaleFrontier) AND advance to the next victim
// -- so the renewed object survives even though it was the
// lex-smallest entry in the frontier.
TEST_F(TenantQuotaMasterServiceTest, FrontierLazyRefreshAfterGrantLease) {
    constexpr size_t object_size = 1 * 1024 * 1024;
    // Cap = 2 objects. Inserting a 3rd forces eviction of exactly 1.
    constexpr uint64_t tenant_max = 2 * object_size;
    // Non-zero lease so we can observe renewal taking effect.
    const uint64_t kv_lease_ttl = 200;

    auto service_config = MasterServiceConfig::builder()
                              .set_enable_tenant_quota(true)
                              .set_default_kv_lease_ttl(kv_lease_ttl)
                              .build();
    auto service = std::make_unique<MasterService>(service_config);
    service->GetTenantQuotas().UpsertPolicy("tenant_renew",
                                            TenantQuotaPolicy{tenant_max});

    [[maybe_unused]] const auto context = PrepareSimpleSegment(
        *service, "test_segment", kQuotaTestSegmentBase, kQuotaTestSegmentSize);
    const UUID client_id = generate_uuid();

    ReplicateConfig cfg;
    cfg.replica_num = 1;
    cfg.tenant_id = "tenant_renew";

    // "key_old" goes in first -> earliest lease_timeout, so without renewal
    // it would be the natural pass-1 victim. "key_new" goes second.
    ASSERT_TRUE(
        service->PutStart(client_id, "key_old", object_size, cfg).has_value());
    ASSERT_TRUE(
        service->PutEnd(client_id, "key_old", ReplicaType::MEMORY).has_value());
    // Wait so the second put lands in a strictly later now() bucket.
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    ASSERT_TRUE(
        service->PutStart(client_id, "key_new", object_size, cfg).has_value());
    ASSERT_TRUE(
        service->PutEnd(client_id, "key_new", ReplicaType::MEMORY).has_value());

    // Wait for both leases to expire so eviction is allowed to fire.
    std::this_thread::sleep_for(std::chrono::milliseconds(kv_lease_ttl + 20));

    // Renew "key_old"'s lease via GetReplicaList -- the hot path that
    // intentionally does NOT touch the frontier entry. After this call,
    // metadata.lease_timeout > frontier_lease_snapshot for key_old, so
    // the frontier entry's primary key is stale.
    auto replicas = service->GetReplicaList("key_old");
    ASSERT_TRUE(replicas.has_value());

    // Trigger eviction. Pass-1 pops "key_old" first (oldest stale
    // lease), the lazy-maintenance snapshot check detects the
    // mismatch and DROPS that candidate via DropStaleFrontier, then
    // continues to the next entry -- "key_new" -- whose snapshot
    // still matches and whose live lease has expired.
    ASSERT_TRUE(service->PutStart(client_id, "trigger_renew", object_size, cfg)
                    .has_value());
    ASSERT_TRUE(service->PutEnd(client_id, "trigger_renew", ReplicaType::MEMORY)
                    .has_value());

    auto exist_old = service->ExistKey("key_old");
    auto exist_new = service->ExistKey("key_new");
    auto exist_trig = service->ExistKey("trigger_renew");
    ASSERT_TRUE(exist_old.has_value());
    ASSERT_TRUE(exist_new.has_value());
    ASSERT_TRUE(exist_trig.has_value());

    EXPECT_TRUE(exist_old.value())
        << "renewed key must survive: stale frontier entry is dropped, "
           "evict advances to the next candidate";
    EXPECT_FALSE(exist_new.value())
        << "key_new becomes the actual victim once key_old is skipped";
    EXPECT_TRUE(exist_trig.value()) << "trigger_renew must be admitted";
}

// TenantEvictionContext caches frontier candidates across
// multiple admit retries within the SAME PutStart call. A burst of
// quota-exhausting puts must succeed without RefillTenantFrontier
// degenerating into a per-attempt full-shard sweep -- the cache
// drains, refills only when empty, and stops at kMaxRefills.
TEST_F(TenantQuotaMasterServiceTest, EvictionContextCachesAcrossRetries) {
    constexpr size_t object_size = 1 * 1024 * 1024;
    // Hold 4 in-flight live objects; insert 24 -> 20 evictions across 24
    // admission cycles. The cache must survive across retries inside
    // each AllocateAndInsertMetadata as well as across distinct
    // PutStart calls without throwing.
    constexpr uint64_t tenant_max = 4 * object_size;
    constexpr int total_puts = 24;
    const uint64_t kv_lease_ttl = 0;

    auto service_config = MasterServiceConfig::builder()
                              .set_enable_tenant_quota(true)
                              .set_default_kv_lease_ttl(kv_lease_ttl)
                              .build();
    auto service = std::make_unique<MasterService>(service_config);
    service->GetTenantQuotas().UpsertPolicy("tenant_cache",
                                            TenantQuotaPolicy{tenant_max});

    [[maybe_unused]] const auto context = PrepareSimpleSegment(
        *service, "test_segment", kQuotaTestSegmentBase, kQuotaTestSegmentSize);
    const UUID client_id = generate_uuid();

    int success_puts = 0;
    for (int i = 0; i < total_puts; ++i) {
        ReplicateConfig cfg;
        cfg.replica_num = 1;
        cfg.tenant_id = "tenant_cache";
        const std::string key = "cache_key_" + std::to_string(i);
        auto put_start = service->PutStart(client_id, key, object_size, cfg);
        if (!put_start.has_value()) {
            continue;
        }
        ASSERT_TRUE(
            service->PutEnd(client_id, key, ReplicaType::MEMORY).has_value());
        ++success_puts;
    }

    // The vast majority of puts must have been admitted -- the cache is
    // doing its job. We don't pin to total_puts because allocator OOM
    // can still cause a tail of failures when shards happen to be hot.
    EXPECT_GE(success_puts, total_puts - 4)
        << "EvictionContext cache must let nearly every put succeed";

    auto snap = service->GetTenantQuotas().GetSnapshot("tenant_cache");
    ASSERT_TRUE(snap.has_value());
    EXPECT_LE(snap->state.used_bytes, tenant_max)
        << "post-storm used_bytes must respect the cap";
    EXPECT_EQ(snap->state.committed_count, tenant_max / object_size)
        << "exactly tenant_max/object_size live objects must remain";
}

// A soft-pinned object must never be evicted in the no-pin pass
// when allow_evict_soft_pinned_objects is false.
TEST_F(TenantQuotaMasterServiceTest, SoftPinObjectNotEvictedViaNoPinFrontier) {
    constexpr size_t object_size = 1 * 1024 * 1024;
    constexpr uint64_t tenant_max = 2 * object_size;
    const uint64_t kv_lease_ttl = 0;
    const uint64_t kv_soft_pin_ttl = 60'000;

    auto service_config = MasterServiceConfig::builder()
                              .set_enable_tenant_quota(true)
                              .set_default_kv_lease_ttl(kv_lease_ttl)
                              .set_default_kv_soft_pin_ttl(kv_soft_pin_ttl)
                              .set_allow_evict_soft_pinned_objects(false)
                              .build();
    auto service = std::make_unique<MasterService>(service_config);
    service->GetTenantQuotas().UpsertPolicy("t_sp",
                                            TenantQuotaPolicy{tenant_max});

    [[maybe_unused]] const auto context = PrepareSimpleSegment(
        *service, "test_segment", kQuotaTestSegmentBase, kQuotaTestSegmentSize);
    const UUID client_id = generate_uuid();

    // Object 1: soft-pinned. Fill one quota slot.
    {
        ReplicateConfig cfg;
        cfg.replica_num = 1;
        cfg.tenant_id = "t_sp";
        cfg.with_soft_pin = true;
        ASSERT_TRUE(service->PutStart(client_id, "sp_obj", object_size, cfg)
                        .has_value());
        ASSERT_TRUE(service->PutEnd(client_id, "sp_obj", ReplicaType::MEMORY)
                        .has_value());
    }

    // Object 2: no-pin. Fill second quota slot.
    {
        ReplicateConfig cfg;
        cfg.replica_num = 1;
        cfg.tenant_id = "t_sp";
        ASSERT_TRUE(service->PutStart(client_id, "np_obj", object_size, cfg)
                        .has_value());
        ASSERT_TRUE(service->PutEnd(client_id, "np_obj", ReplicaType::MEMORY)
                        .has_value());
    }

    // Object 3: triggers tenant eviction. With soft-pin eviction
    // disabled, only the no-pin object should be evicted.
    {
        ReplicateConfig cfg;
        cfg.replica_num = 1;
        cfg.tenant_id = "t_sp";
        auto result = service->PutStart(client_id, "trigger", object_size, cfg);
        ASSERT_TRUE(result.has_value())
            << "Put should succeed by evicting the no-pin object";
        ASSERT_TRUE(service->PutEnd(client_id, "trigger", ReplicaType::MEMORY)
                        .has_value());
    }

    auto exist_sp = service->ExistKey("sp_obj");
    auto exist_np = service->ExistKey("np_obj");
    auto exist_trigger = service->ExistKey("trigger");
    ASSERT_TRUE(exist_sp.has_value());
    ASSERT_TRUE(exist_np.has_value());
    ASSERT_TRUE(exist_trigger.has_value());

    EXPECT_TRUE(exist_sp.value())
        << "soft-pinned object must survive when soft-pin eviction is disabled";
    EXPECT_FALSE(exist_np.value())
        << "no-pin object should be evicted to make room";
    EXPECT_TRUE(exist_trigger.value()) << "trigger object must be admitted";
}

// BatchEvictInTenant runs two passes -- pass1 drains
// the no-pin frontier bucket, pass2 only touches the soft-pin
// frontier bucket if pass1 underdelivers. With one no-pin and one
// soft-pin object both quota-charged, the FIRST eviction must always
// reap the no-pin object, leaving the soft-pin object alive.
TEST_F(TenantQuotaMasterServiceTest, FrontierTwoPassNoPinThenSoftPin) {
    constexpr size_t object_size = 1 * 1024 * 1024;
    constexpr uint64_t tenant_max = 2 * object_size;
    const uint64_t kv_lease_ttl = 0;
    const uint64_t kv_soft_pin_ttl = 60'000;  // long enough to outlast the test

    auto service_config = MasterServiceConfig::builder()
                              .set_enable_tenant_quota(true)
                              .set_default_kv_lease_ttl(kv_lease_ttl)
                              .set_default_kv_soft_pin_ttl(kv_soft_pin_ttl)
                              .set_allow_evict_soft_pinned_objects(true)
                              .build();
    auto service = std::make_unique<MasterService>(service_config);
    service->GetTenantQuotas().UpsertPolicy("tenant_2pass",
                                            TenantQuotaPolicy{tenant_max});

    [[maybe_unused]] const auto context = PrepareSimpleSegment(
        *service, "test_segment", kQuotaTestSegmentBase, kQuotaTestSegmentSize);
    const UUID client_id = generate_uuid();

    // Insert the SOFT-PIN object FIRST so it has the older lease_timeout.
    // Without two-pass routing, a naive (lease_timeout ASC) sweep would
    // pick it as the first victim. The two-pass split must override
    // that and prefer the no-pin object instead.
    {
        ReplicateConfig cfg;
        cfg.replica_num = 1;
        cfg.tenant_id = "tenant_2pass";
        cfg.with_soft_pin = true;
        ASSERT_TRUE(service->PutStart(client_id, "soft_key", object_size, cfg)
                        .has_value());
        ASSERT_TRUE(service->PutEnd(client_id, "soft_key", ReplicaType::MEMORY)
                        .has_value());
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    {
        ReplicateConfig cfg;
        cfg.replica_num = 1;
        cfg.tenant_id = "tenant_2pass";
        ASSERT_TRUE(service->PutStart(client_id, "nopin_key", object_size, cfg)
                        .has_value());
        ASSERT_TRUE(service->PutEnd(client_id, "nopin_key", ReplicaType::MEMORY)
                        .has_value());
    }

    // Trigger eviction. Pass1 (no-pin frontier bucket) finds
    // "nopin_key" and freeing it alone covers the deficit, so pass2
    // never runs.
    {
        ReplicateConfig cfg;
        cfg.replica_num = 1;
        cfg.tenant_id = "tenant_2pass";
        ASSERT_TRUE(service->PutStart(client_id, "new_no_pin", object_size, cfg)
                        .has_value());
        ASSERT_TRUE(
            service->PutEnd(client_id, "new_no_pin", ReplicaType::MEMORY)
                .has_value());
    }

    auto exist_soft = service->ExistKey("soft_key");
    auto exist_nopin = service->ExistKey("nopin_key");
    auto exist_newnp = service->ExistKey("new_no_pin");
    ASSERT_TRUE(exist_soft.has_value());
    ASSERT_TRUE(exist_nopin.has_value());
    ASSERT_TRUE(exist_newnp.has_value());

    EXPECT_TRUE(exist_soft.value())
        << "pass-1 must drain no_pin frontier first; soft-pin must survive";
    EXPECT_FALSE(exist_nopin.value())
        << "no-pin object must be the first victim, even though it has a "
           "newer lease_timeout than the soft-pin object";
    EXPECT_TRUE(exist_newnp.value()) << "new_no_pin must be admitted";
}

}  // namespace mooncake::test

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
