// Snapshot persistence tests for per-tenant quota policy.
//
// Carved out of master_service_test_for_snapshot.cpp so the tenant-quota
// suite can evolve without enlarging the diff against the main snapshot
// test file. The fixture defined here is a sibling of that file's
// MasterServiceSnapshotTest -- functionally identical for the surface
// these tests use, but kept independent so neither file becomes a
// header for the other.

#include "master_service_test_for_snapshot_base.h"

#include <chrono>
#include <thread>
#include <utility>

namespace mooncake::test {

class TenantQuotaSnapshotTest : public MasterServiceSnapshotTestBase {
   protected:
    static bool glog_initialized_;

    void SetUp() override {
        // Call base class SetUp first to reset MasterMetricManager state.
        MasterServiceSnapshotTestBase::SetUp();

        if (!glog_initialized_) {
            google::InitGoogleLogging("TenantQuotaSnapshotTest");
            FLAGS_logtostderr = true;
            glog_initialized_ = true;
        }
    }
};

bool TenantQuotaSnapshotTest::glog_initialized_ = false;

// Verify per-tenant quota policies survive snapshot persist + restore.
// Three flavors are exercised in one test:
//   1. operator-supplied default_tenant_quota_bytes round-trips,
//   2. explicit per-tenant overrides round-trip with their original values,
//   3. tenants that were only seen via traffic (no explicit policy) do not
//      pollute the persisted policy set.
TEST_F(TenantQuotaSnapshotTest, TenantQuotaPolicyPersistAndRestore) {
    constexpr uint64_t kDefaultMaxBytes = 1ULL << 30;          // 1 GiB
    constexpr uint64_t kTenantAMaxBytes = 256ULL << 20;        // 256 MiB
    constexpr uint64_t kTenantBMaxBytes = 0;                   // unlimited
    constexpr uint64_t kTenantCMaxBytes = 4ULL * 1024 * 1024;  // 4 MiB

    auto service_config = MasterServiceConfig::builder()
                              .set_memory_allocator(BufferAllocatorType::OFFSET)
                              .set_enable_tenant_quota(true)
                              .set_default_tenant_quota_bytes(kDefaultMaxBytes)
                              .set_enable_snapshot(true)
                              .set_snapshot_interval_seconds(1)
                              .set_snapshot_object_store_type("local")
                              .build();
    service_.reset(new MasterService(service_config));

    auto& quotas = service_->GetTenantQuotas();
    quotas.UpsertPolicy("tenant_a", TenantQuotaPolicy{kTenantAMaxBytes});
    quotas.UpsertPolicy("tenant_b", TenantQuotaPolicy{kTenantBMaxBytes});
    quotas.UpsertPolicy("tenant_c", TenantQuotaPolicy{kTenantCMaxBytes});

    // Persist current state.
    std::string snapshot_id = GenerateSnapshotId();
    auto persist_result = CallPersistState(service_.get(), snapshot_id);
    ASSERT_TRUE(persist_result.has_value())
        << "Failed to persist state: " << persist_result.error().message;

    // Spin up a second service that restores from the same snapshot
    // location. The restore path is the same one the supervisor uses
    // when the master is brought back up after a crash.
    ::setenv("MOONCAKE_MASTER_SERVICE_SNAPSHOT_TEST_SKIP_CLEANUP", "1", 1);
    auto restore_config =
        MasterServiceConfig::builder()
            .set_memory_allocator(BufferAllocatorType::OFFSET)
            .set_enable_tenant_quota(true)
            // Intentionally pick a wrong default to prove the snapshot
            // overrides config -- if the restore silently no-oped, the
            // assertions below would resolve to this number instead.
            .set_default_tenant_quota_bytes(7)
            .set_enable_snapshot_restore(true)
            .set_snapshot_object_store_type("local")
            .build();
    std::unique_ptr<MasterService> restored_service(
        new MasterService(restore_config));
    ::unsetenv("MOONCAKE_MASTER_SERVICE_SNAPSHOT_TEST_SKIP_CLEANUP");

    const auto& restored_quotas = restored_service->GetTenantQuotas();

    EXPECT_EQ(restored_quotas.GetDefaultPolicy().max_bytes, kDefaultMaxBytes);

    auto policy_a = restored_quotas.GetPolicy("tenant_a");
    ASSERT_TRUE(policy_a.has_value());
    EXPECT_EQ(policy_a->max_bytes, kTenantAMaxBytes);

    auto policy_b = restored_quotas.GetPolicy("tenant_b");
    ASSERT_TRUE(policy_b.has_value());
    EXPECT_EQ(policy_b->max_bytes, kTenantBMaxBytes);

    auto policy_c = restored_quotas.GetPolicy("tenant_c");
    ASSERT_TRUE(policy_c.has_value());
    EXPECT_EQ(policy_c->max_bytes, kTenantCMaxBytes);

    // Tenants without an explicit policy must not appear in the restored
    // policy set (they fall through to the default at lookup time).
    EXPECT_FALSE(restored_quotas.GetPolicy("tenant_unknown").has_value());

    auto snapshots = restored_quotas.ListAll();
    int explicit_count = 0;
    for (const auto& snap : snapshots) {
        if (snap.has_explicit_policy) ++explicit_count;
    }
    EXPECT_EQ(explicit_count, 3);
}

// A tenant policy added on the master, then removed, and finally re-added
// with a different value must persist with its latest setting.
TEST_F(TenantQuotaSnapshotTest, TenantQuotaPolicyEraseThenReupsert) {
    auto service_config = MasterServiceConfig::builder()
                              .set_memory_allocator(BufferAllocatorType::OFFSET)
                              .set_enable_tenant_quota(true)
                              .set_enable_snapshot(true)
                              .set_snapshot_interval_seconds(1)
                              .set_snapshot_object_store_type("local")
                              .build();
    service_.reset(new MasterService(service_config));

    auto& quotas = service_->GetTenantQuotas();
    quotas.UpsertPolicy("tenant_x", TenantQuotaPolicy{1024});
    EXPECT_TRUE(quotas.ErasePolicy("tenant_x"));
    quotas.UpsertPolicy("tenant_x", TenantQuotaPolicy{4096});

    std::string snapshot_id = GenerateSnapshotId();
    auto persist_result = CallPersistState(service_.get(), snapshot_id);
    ASSERT_TRUE(persist_result.has_value());

    ::setenv("MOONCAKE_MASTER_SERVICE_SNAPSHOT_TEST_SKIP_CLEANUP", "1", 1);
    auto restore_config = MasterServiceConfig::builder()
                              .set_memory_allocator(BufferAllocatorType::OFFSET)
                              .set_enable_tenant_quota(true)
                              .set_enable_snapshot_restore(true)
                              .set_snapshot_object_store_type("local")
                              .build();
    std::unique_ptr<MasterService> restored_service(
        new MasterService(restore_config));
    ::unsetenv("MOONCAKE_MASTER_SERVICE_SNAPSHOT_TEST_SKIP_CLEANUP");

    auto restored_policy =
        restored_service->GetTenantQuotas().GetPolicy("tenant_x");
    ASSERT_TRUE(restored_policy.has_value());
    EXPECT_EQ(restored_policy->max_bytes, 4096u);
}

// Round-trip with no explicit policies still emits a serializable
// tenant_quotas section (default-only). This guards against regressions
// where the writer would skip the field on an empty policy set and the
// reader would then mistakenly treat the snapshot as legacy.
TEST_F(TenantQuotaSnapshotTest, TenantQuotaPolicyEmptyRoundTrip) {
    constexpr uint64_t kDefaultMaxBytes = 512ULL << 20;
    auto service_config = MasterServiceConfig::builder()
                              .set_memory_allocator(BufferAllocatorType::OFFSET)
                              .set_enable_tenant_quota(true)
                              .set_default_tenant_quota_bytes(kDefaultMaxBytes)
                              .set_enable_snapshot(true)
                              .set_snapshot_interval_seconds(1)
                              .set_snapshot_object_store_type("local")
                              .build();
    service_.reset(new MasterService(service_config));

    std::string snapshot_id = GenerateSnapshotId();
    auto persist_result = CallPersistState(service_.get(), snapshot_id);
    ASSERT_TRUE(persist_result.has_value());

    ::setenv("MOONCAKE_MASTER_SERVICE_SNAPSHOT_TEST_SKIP_CLEANUP", "1", 1);
    auto restore_config = MasterServiceConfig::builder()
                              .set_memory_allocator(BufferAllocatorType::OFFSET)
                              .set_enable_tenant_quota(true)
                              .set_default_tenant_quota_bytes(0)
                              .set_enable_snapshot_restore(true)
                              .set_snapshot_object_store_type("local")
                              .build();
    std::unique_ptr<MasterService> restored_service(
        new MasterService(restore_config));
    ::unsetenv("MOONCAKE_MASTER_SERVICE_SNAPSHOT_TEST_SKIP_CLEANUP");

    EXPECT_EQ(restored_service->GetTenantQuotas().GetDefaultPolicy().max_bytes,
              kDefaultMaxBytes);
    EXPECT_TRUE(restored_service->GetTenantQuotas().ListAll().empty());
}

// Restore-time cleanup erases objects whose lease has expired or whose
// replicas are not COMPLETE. Those objects had their bytes accumulated
// into the tenant ledger by DeserializeShard; the cleanup path must
// release them back. Without that release the tenant ledger overstates
// usage and any subsequent PutStart against the offending tenant fails
// with NO_AVAILABLE_HANDLE under a tight quota.
TEST_F(TenantQuotaSnapshotTest, TenantQuotaUsageReleasedByRestoreTimeCleanup) {
    // The whole point of this test is that restore-time cleanup deletes
    // objects whose lease has expired. The base fixture's TearDown does a
    // persist -> restore -> re-persist byte-equality roundtrip on `service_`,
    // which by the time TearDown runs still holds those expired-lease
    // objects -- so the original snapshot and the round-tripped snapshot
    // necessarily diverge. Opt out of that invariant; the assertions below
    // already pin down the contract this test cares about.
    skip_teardown_roundtrip_ = true;

    // Lease TTL is short enough that all written objects expire before
    // restore-time cleanup runs.
    constexpr uint64_t kShortLeaseMs = 10;
    constexpr size_t kSlice = 4 * 1024;
    constexpr int kNumObjects = 8;
    constexpr uint64_t kTenantQuotaBytes = kSlice * kNumObjects * 2;

    auto service_config = MasterServiceConfig::builder()
                              .set_memory_allocator(BufferAllocatorType::OFFSET)
                              .set_enable_tenant_quota(true)
                              .set_default_tenant_quota_bytes(kTenantQuotaBytes)
                              .set_default_kv_lease_ttl(kShortLeaseMs)
                              .set_snapshot_object_store_type("local")
                              .build();
    service_.reset(new MasterService(service_config));
    EnsureSnapshotStores(service_.get());

    constexpr size_t kSegBase = 0x100000000;
    constexpr size_t kSegSize = 64 * 1024 * 1024;
    PrepareSimpleSegment(*service_, "seg_a", kSegBase, kSegSize);

    UUID client_id = generate_uuid();
    ReplicateConfig cfg;
    cfg.replica_num = 1;
    cfg.tenant_id = "tenant_lease_expire";
    for (int i = 0; i < kNumObjects; ++i) {
        const std::string key = "tenant_lease_key_" + std::to_string(i);
        auto put_start = service_->PutStart(client_id, key, kSlice, cfg);
        ASSERT_TRUE(put_start.has_value());
        auto put_end = service_->PutEnd(client_id, key, ReplicaType::MEMORY);
        ASSERT_TRUE(put_end.has_value());
    }

    {
        auto snap =
            service_->GetTenantQuotas().GetSnapshot("tenant_lease_expire");
        ASSERT_TRUE(snap.has_value());
        ASSERT_EQ(snap->state.used_bytes,
                  static_cast<uint64_t>(kSlice) * kNumObjects);
    }

    std::string snapshot_id = GenerateSnapshotId();
    ASSERT_TRUE(CallPersistState(service_.get(), snapshot_id).has_value());

    // Wait until every restored object's lease will be expired by the
    // time TryRestoreStateFromSnapshot evaluates the cleanup predicate.
    std::this_thread::sleep_for(std::chrono::milliseconds(kShortLeaseMs * 4));

    auto restore_config = MasterServiceConfig::builder()
                              .set_memory_allocator(BufferAllocatorType::OFFSET)
                              .set_enable_tenant_quota(true)
                              .set_default_tenant_quota_bytes(kTenantQuotaBytes)
                              .set_default_kv_lease_ttl(kShortLeaseMs)
                              .set_enable_snapshot_restore(true)
                              .set_snapshot_object_store_type("local")
                              .build();
    std::unique_ptr<MasterService> restored_service(
        new MasterService(restore_config));

    auto restored_snap =
        restored_service->GetTenantQuotas().GetSnapshot("tenant_lease_expire");
    if (restored_snap.has_value()) {
        EXPECT_EQ(restored_snap->state.used_bytes, 0u)
            << "restore-time cleanup must release tenant bytes for objects "
               "it erases";
    }
    EXPECT_EQ(restored_service->GetKeyCount(), 0u)
        << "all expired-lease objects should have been cleaned up at restore";
}

// A snapshot whose tenant_quotas section is missing the required
// default_policy field is treated as corrupted, not as "default
// unlimited". Otherwise a partially-written snapshot would silently
// disable quota enforcement after restart.
TEST_F(TenantQuotaSnapshotTest, TenantQuotaMalformedSectionRejectsRestore) {
    auto service_config = MasterServiceConfig::builder()
                              .set_memory_allocator(BufferAllocatorType::OFFSET)
                              .set_enable_tenant_quota(true)
                              .set_default_tenant_quota_bytes(1024)
                              .set_snapshot_object_store_type("local")
                              .build();
    service_.reset(new MasterService(service_config));
    EnsureSnapshotStores(service_.get());

    std::string snapshot_id = GenerateSnapshotId();
    ASSERT_TRUE(CallPersistState(service_.get(), snapshot_id).has_value());

    // Replace the "default_policy" key inside tenant_quotas with a
    // same-length sentinel so the field lookup misses while leaving the
    // msgpack structure intact. The restore path must then refuse to
    // accept the snapshot rather than silently treat the absent default
    // as max_bytes=0 (= unlimited).
    namespace fs = std::filesystem;
    fs::path metadata_path = fs::path(GetSnapshotDir(snapshot_id)) / "metadata";
    std::vector<uint8_t> blob;
    {
        std::ifstream in(metadata_path, std::ios::binary);
        ASSERT_TRUE(in.good());
        blob.assign(std::istreambuf_iterator<char>(in),
                    std::istreambuf_iterator<char>());
    }
    // Replace the literal "default_policy" with a same-length sentinel so
    // the field lookup misses but msgpack structure stays intact.
    constexpr std::string_view needle = "default_policy";
    constexpr std::string_view replacement = "DEFAULT_POLI__";
    static_assert(needle.size() == replacement.size());
    auto it =
        std::search(blob.begin(), blob.end(), needle.begin(), needle.end());
    ASSERT_NE(it, blob.end()) << "default_policy field not found in blob";
    std::copy(replacement.begin(), replacement.end(), it);
    {
        std::ofstream out(metadata_path, std::ios::binary | std::ios::trunc);
        out.write(reinterpret_cast<const char*>(blob.data()), blob.size());
    }

    auto restore_config = MasterServiceConfig::builder()
                              .set_memory_allocator(BufferAllocatorType::OFFSET)
                              .set_enable_tenant_quota(true)
                              .set_default_tenant_quota_bytes(2048)
                              .set_enable_snapshot_restore(true)
                              .set_snapshot_object_store_type("local")
                              .build();
    std::unique_ptr<MasterService> restored_service(
        new MasterService(restore_config));

    // RestoreState swallows per-snapshot failures and falls back to a
    // fresh start, so observable proof of rejection is that the snapshot
    // policy did NOT take effect: the restored default is the one from
    // the restart config (2048), not from the corrupted snapshot (1024).
    EXPECT_EQ(restored_service->GetTenantQuotas().GetDefaultPolicy().max_bytes,
              2048u);
}

// Companion to the previous case: with `default_policy` intact but the
// `policies` array key truncated, the restore must still fail. Otherwise
// a partially-written snapshot would silently drop every explicit
// per-tenant override.
TEST_F(TenantQuotaSnapshotTest, TenantQuotaMissingPoliciesArrayRejectsRestore) {
    auto service_config = MasterServiceConfig::builder()
                              .set_memory_allocator(BufferAllocatorType::OFFSET)
                              .set_enable_tenant_quota(true)
                              .set_default_tenant_quota_bytes(1024)
                              .set_snapshot_object_store_type("local")
                              .build();
    service_.reset(new MasterService(service_config));
    EnsureSnapshotStores(service_.get());

    std::string snapshot_id = GenerateSnapshotId();
    ASSERT_TRUE(CallPersistState(service_.get(), snapshot_id).has_value());

    namespace fs = std::filesystem;
    fs::path metadata_path = fs::path(GetSnapshotDir(snapshot_id)) / "metadata";
    std::vector<uint8_t> blob;
    {
        std::ifstream in(metadata_path, std::ios::binary);
        ASSERT_TRUE(in.good());
        blob.assign(std::istreambuf_iterator<char>(in),
                    std::istreambuf_iterator<char>());
    }
    constexpr std::string_view needle = "policies";
    constexpr std::string_view replacement = "POLI____";
    static_assert(needle.size() == replacement.size());
    auto it =
        std::search(blob.begin(), blob.end(), needle.begin(), needle.end());
    ASSERT_NE(it, blob.end()) << "policies field not found in blob";
    std::copy(replacement.begin(), replacement.end(), it);
    {
        std::ofstream out(metadata_path, std::ios::binary | std::ios::trunc);
        out.write(reinterpret_cast<const char*>(blob.data()), blob.size());
    }

    auto restore_config = MasterServiceConfig::builder()
                              .set_memory_allocator(BufferAllocatorType::OFFSET)
                              .set_enable_tenant_quota(true)
                              .set_default_tenant_quota_bytes(2048)
                              .set_enable_snapshot_restore(true)
                              .set_snapshot_object_store_type("local")
                              .build();
    std::unique_ptr<MasterService> restored_service(
        new MasterService(restore_config));

    EXPECT_EQ(restored_service->GetTenantQuotas().GetDefaultPolicy().max_bytes,
              2048u);
}

// An individual policy entry that omits `max_bytes` must also be
// rejected. Otherwise the named tenant would silently restore as
// unlimited (max_bytes=0), which is the worst possible failure mode for
// a quota system.
TEST_F(TenantQuotaSnapshotTest,
       TenantQuotaPolicyMissingMaxBytesRejectsRestore) {
    constexpr uint64_t kDefaultMax = 1024;
    constexpr uint64_t kTenantAMax = 4096;
    auto service_config = MasterServiceConfig::builder()
                              .set_memory_allocator(BufferAllocatorType::OFFSET)
                              .set_enable_tenant_quota(true)
                              .set_default_tenant_quota_bytes(kDefaultMax)
                              .set_snapshot_object_store_type("local")
                              .build();
    service_.reset(new MasterService(service_config));
    EnsureSnapshotStores(service_.get());
    service_->GetTenantQuotas().UpsertPolicy("tenant_a",
                                             TenantQuotaPolicy{kTenantAMax});

    std::string snapshot_id = GenerateSnapshotId();
    ASSERT_TRUE(CallPersistState(service_.get(), snapshot_id).has_value());

    namespace fs = std::filesystem;
    fs::path metadata_path = fs::path(GetSnapshotDir(snapshot_id)) / "metadata";
    std::vector<uint8_t> blob;
    {
        std::ifstream in(metadata_path, std::ios::binary);
        ASSERT_TRUE(in.good());
        blob.assign(std::istreambuf_iterator<char>(in),
                    std::istreambuf_iterator<char>());
    }
    // The first "max_bytes" lives in default_policy; corrupt the SECOND
    // occurrence, which belongs to the explicit policy entry.
    constexpr std::string_view needle = "max_bytes";
    constexpr std::string_view replacement = "max_byte_";
    static_assert(needle.size() == replacement.size());
    auto first =
        std::search(blob.begin(), blob.end(), needle.begin(), needle.end());
    ASSERT_NE(first, blob.end()) << "first max_bytes not found";
    auto second = std::search(first + needle.size(), blob.end(), needle.begin(),
                              needle.end());
    ASSERT_NE(second, blob.end())
        << "second max_bytes (in policies[0]) not found";
    std::copy(replacement.begin(), replacement.end(), second);
    {
        std::ofstream out(metadata_path, std::ios::binary | std::ios::trunc);
        out.write(reinterpret_cast<const char*>(blob.data()), blob.size());
    }

    auto restore_config = MasterServiceConfig::builder()
                              .set_memory_allocator(BufferAllocatorType::OFFSET)
                              .set_enable_tenant_quota(true)
                              .set_default_tenant_quota_bytes(8192)
                              .set_enable_snapshot_restore(true)
                              .set_snapshot_object_store_type("local")
                              .build();
    std::unique_ptr<MasterService> restored_service(
        new MasterService(restore_config));

    // Snapshot rejected → both default and explicit policy fall back to
    // the restart config: default==8192 from the builder, no tenant_a
    // override loaded.
    EXPECT_EQ(restored_service->GetTenantQuotas().GetDefaultPolicy().max_bytes,
              8192u);
    EXPECT_FALSE(
        restored_service->GetTenantQuotas().GetPolicy("tenant_a").has_value());
}

}  // namespace mooncake::test

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
