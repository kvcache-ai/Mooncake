#include "tenant/quota_policy.h"

#include <array>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <future>
#include <initializer_list>
#include <memory>
#include <string>
#include <system_error>
#include <unordered_map>
#include <utility>
#include <vector>

#include <gtest/gtest.h>
#include <unistd.h>

#include "metadata/namespace.h"
#include "tenant/quota_policy_store.h"

namespace mooncake {
namespace {

// The quota table and its control plane are covered by tenant_quota_test.cpp
// and tenant_quota_manager_test.cpp. These cases cover what the policy adds:
// charging each key from its object as the namespace reports every write
// release, and asking the store to shed a tenant over its watermark.

const TenantId kTenantA("tenant-a");
const TenantId kTenantB("tenant-b");

// The store a policy is built over: namespaces it creates on demand with the
// policy attached, a settable capacity, and every eviction it is asked for.
class FakeStore final : public StoreControl {
   public:
    void VisitNamespaces(
        const std::function<void(const TenantId&, route::ObjectRoute&)>& fn)
        override {
        for (auto& [tenant_id, ns] : namespaces) {
            fn(tenant_id, ns->objects);
        }
    }

    size_t ObjectCount(const TenantId& tenant_id) const override {
        const auto it = namespaces.find(tenant_id);
        return it == namespaces.end() ? 0 : it->second->objects.ObjectCount();
    }

    uint64_t AllocatableMemoryBytes() const override { return capacity; }

    EvictionResult EvictNamespaceMemory(const TenantId& tenant_id,
                                        uint64_t target_bytes) override {
        evictions.emplace_back(tenant_id, target_bytes);
        return {.freed_bytes = target_bytes, .evicted_objects = 1};
    }

    uint64_t capacity = 1 << 20;
    std::unordered_map<TenantId, std::unique_ptr<metadata::Namespace>,
                       TenantIdHash>
        namespaces;
    std::vector<std::pair<TenantId, uint64_t>> evictions;
};

std::vector<Replica> MemoryReplicas(
    std::initializer_list<ReplicaStatus> statuses) {
    std::vector<Replica> replicas;
    for (const ReplicaStatus status : statuses) {
        replicas.emplace_back(std::unique_ptr<AllocatedBuffer>{}, status);
    }
    return replicas;
}

// Publishes an object of `length` bytes with `replicas` under the key `guard`
// holds, which holds no object.
void PublishUnder(const route::WriteGuard& guard, size_t length,
                  std::vector<Replica> replicas) {
    guard.Publish(
        UUID{1, 2},
        std::chrono::system_clock::time_point(std::chrono::seconds(1)), length,
        std::move(replicas), std::nullopt, false, ObjectDataType::UNKNOWN,
        std::string{}, TenantId(), guard.key());
}

void MarkMemoryReplicasComplete(const route::WriteGuard& guard) {
    guard.metadata().VisitReplicas(
        [](const Replica& replica) {
            return replica.is_memory_replica() && replica.is_processing();
        },
        [](Replica& replica) { replica.mark_complete(); });
}

class TenantQuotaPolicyTest : public ::testing::Test {
   protected:
    void SetUp() override {
        // Loading needs the file to exist; it starts with no policies.
        std::ofstream(policy_path_)
            << FormatTenantQuotaPolicyYaml(TenantQuotaPolicySnapshot{});
        MakePolicy({});
    }

    void TearDown() override {
        // The namespaces hold the accounts the policy hands out.
        store_.namespaces.clear();
        policy_.reset();
        std::error_code ec;
        std::filesystem::remove(policy_path_, ec);
    }

    // Builds the policy over the same policy file, so registered tenants carry
    // over; namespaces must be created again afterwards.
    void MakePolicy(TenantQuotaPolicy::Options options) {
        store_.namespaces.clear();
        policy_.reset();
        policy_ = std::make_unique<TenantQuotaPolicy>(store_, options);
        policy_->OpenPolicyStoreOrThrow("file", policy_path_, "test_cluster");
    }

    // The tenant's namespace, created on first use as the core would.
    metadata::Namespace& NamespaceOf(const TenantId& tenant_id) {
        auto& ns = store_.namespaces[tenant_id];
        if (ns == nullptr) {
            ns = std::make_unique<metadata::Namespace>(
                tenant_id, policy_.get(),
                policy_->OnNamespaceCreated(tenant_id));
        }
        return *ns;
    }

    void Register(const TenantId& tenant_id, uint64_t quota_bytes) {
        auto upserted = policy_->UpsertPolicy(tenant_id, quota_bytes);
        ASSERT_TRUE(upserted.has_value()) << toString(upserted.error());
    }

    uint64_t Charged(const TenantId& tenant_id) const {
        const auto snapshot = policy_->GetSnapshot(tenant_id);
        return snapshot.has_value() ? snapshot->charged_bytes : 0;
    }

    // Grows `key` by `grow_bytes`, then publishes an object under it, under
    // one write guard.
    void Put(const TenantId& tenant_id, const std::string& key,
             uint64_t grow_bytes, size_t length,
             std::initializer_list<ReplicaStatus> statuses) {
        auto& ns = NamespaceOf(tenant_id);
        auto guard = ns.objects.WriteKey(key);
        ASSERT_FALSE(guard.has_object());
        if (grow_bytes != 0) {
            ASSERT_TRUE(ns.Grow(guard, grow_bytes).has_value());
        }
        PublishUnder(guard, length, MemoryReplicas(statuses));
    }

    // The words the policy keeps on `key`, read under a write guard whose
    // release settles nothing new.
    std::array<uint64_t, 2> Words(const TenantId& tenant_id,
                                  const std::string& key) {
        auto guard = NamespaceOf(tenant_id).objects.Write(key);
        EXPECT_TRUE(guard.has_value());
        return guard.has_value() ? guard->owner_state().policy_words
                                 : std::array<uint64_t, 2>{};
    }

    const std::string policy_path_ = (std::filesystem::temp_directory_path() /
                                      ("mooncake_tenant_quota_policy_test_" +
                                       std::to_string(::getpid()) + ".yaml"))
                                         .string();
    FakeStore store_;
    std::unique_ptr<TenantQuotaPolicy> policy_;
};

TEST(TenantQuotaPolicyChargeTest, ChargesEveryMemoryReplicaThatHoldsMemory) {
    std::vector<Replica> replicas =
        MemoryReplicas({ReplicaStatus::INITIALIZED, ReplicaStatus::PROCESSING,
                        ReplicaStatus::COMPLETE, ReplicaStatus::REMOVED,
                        ReplicaStatus::FAILED});
    replicas.emplace_back("/tmp/disk-replica", 100, ReplicaStatus::COMPLETE);
    const ObjectMetadata metadata(UUID{1, 2}, std::chrono::system_clock::now(),
                                  100, std::move(replicas));
    EXPECT_EQ(TenantQuotaPolicy::MemoryCharge(metadata), 400u);
}

TEST(TenantQuotaPolicyChargeTest, SaturatesAtTheAccountingRange) {
    const ObjectMetadata metadata(
        UUID{1, 2}, std::chrono::system_clock::now(),
        TenantQuotaAccount::kMaxChargedBytes,
        MemoryReplicas({ReplicaStatus::COMPLETE, ReplicaStatus::COMPLETE}));
    EXPECT_EQ(TenantQuotaPolicy::MemoryCharge(metadata),
              TenantQuotaAccount::kMaxChargedBytes);
}

TEST_F(TenantQuotaPolicyTest, AdmitsWritesOnlyForRegisteredTenants) {
    auto refused = policy_->AdmitWrite(kTenantA);
    ASSERT_FALSE(refused.has_value());
    EXPECT_EQ(refused.error(), ErrorCode::TENANT_NOT_REGISTERED);

    Register(kTenantA, 1000);
    auto admitted = policy_->AdmitWrite(kTenantA);
    ASSERT_TRUE(admitted.has_value());
    EXPECT_NE(admitted->get(), nullptr);
}

TEST_F(TenantQuotaPolicyTest, AnAdmittedWriteHoldsOffAPolicyDelete) {
    Register(kTenantA, 1000);
    auto admitted = policy_->AdmitWrite(kTenantA);
    ASSERT_TRUE(admitted.has_value());

    auto deleted = std::async(std::launch::async,
                              [&] { return policy_->DeletePolicy(kTenantA); });
    EXPECT_EQ(deleted.wait_for(std::chrono::milliseconds(50)),
              std::future_status::timeout);
    admitted->reset();
    EXPECT_TRUE(deleted.get().has_value());
    EXPECT_FALSE(policy_->IsTenantRegistered(kTenantA));
}

TEST_F(TenantQuotaPolicyTest, GrowthIsChargedUpFrontAndRefusedPastTheQuota) {
    Register(kTenantA, 1000);
    auto& ns = NamespaceOf(kTenantA);
    {
        auto guard = ns.objects.WriteKey("k");
        ASSERT_TRUE(ns.Grow(guard, 600).has_value());
        EXPECT_EQ(Charged(kTenantA), 600u);
        auto refused = ns.Grow(guard, 500);
        ASSERT_FALSE(refused.has_value());
        EXPECT_EQ(refused.error(), ErrorCode::TENANT_QUOTA_EXCEEDED);
        EXPECT_EQ(Charged(kTenantA), 600u);
        PublishUnder(guard, 300,
                     MemoryReplicas({ReplicaStatus::PROCESSING,
                                     ReplicaStatus::PROCESSING}));
    }
    EXPECT_EQ(Charged(kTenantA), 600u);
    EXPECT_EQ(Words(kTenantA, "k"), (std::array<uint64_t, 2>{600, 0}));
}

TEST_F(TenantQuotaPolicyTest, AnUnregisteredTenantCannotGrow) {
    auto& ns = NamespaceOf(kTenantA);
    auto guard = ns.objects.WriteKey("k");
    auto refused = ns.Grow(guard, 1);
    ASSERT_FALSE(refused.has_value());
    EXPECT_EQ(refused.error(), ErrorCode::TENANT_NOT_REGISTERED);
}

TEST_F(TenantQuotaPolicyTest, GrowthThatPublishesNothingIsGivenBack) {
    Register(kTenantA, 1000);
    auto& ns = NamespaceOf(kTenantA);
    {
        auto guard = ns.objects.WriteKey("k");
        ASSERT_TRUE(ns.Grow(guard, 600).has_value());
    }
    EXPECT_EQ(Charged(kTenantA), 0u);
    EXPECT_TRUE(ns.Empty());
}

TEST_F(TenantQuotaPolicyTest, EveryReleaseSettlesToWhatTheObjectOwes) {
    Register(kTenantA, 1000);
    Put(kTenantA, "k", 600, 300,
        {ReplicaStatus::PROCESSING, ReplicaStatus::PROCESSING});
    auto& ns = NamespaceOf(kTenantA);

    // Finishing the write holds the same memory.
    {
        auto guard = ns.objects.Write("k");
        ASSERT_TRUE(guard.has_value());
        MarkMemoryReplicasComplete(*guard);
    }
    EXPECT_EQ(Charged(kTenantA), 600u);

    // Dropping a replica gives its share back.
    {
        auto guard = ns.objects.Write("k");
        ASSERT_TRUE(guard.has_value());
        bool first = true;
        ASSERT_EQ(guard->metadata().EraseReplicas([&](const Replica&) {
            return std::exchange(first, false);
        }),
                  1u);
    }
    EXPECT_EQ(Charged(kTenantA), 300u);
    EXPECT_EQ(Words(kTenantA, "k"), (std::array<uint64_t, 2>{300, 0}));

    // A removed replica keeps its memory until it is dropped.
    {
        auto guard = ns.objects.Write("k");
        ASSERT_TRUE(guard.has_value());
        guard->metadata().VisitReplicas(
            [](const Replica&) { return true; },
            [](Replica& replica) { replica.mark_removed(); });
    }
    EXPECT_EQ(Charged(kTenantA), 300u);

    // A torn-down key owes nothing and keeps no words.
    {
        auto guard = ns.objects.Write("k");
        ASSERT_TRUE(guard.has_value());
        ns.BeforeTearDown(*guard);
        guard->TearDown();
    }
    EXPECT_EQ(Charged(kTenantA), 0u);
    EXPECT_TRUE(ns.Empty());
}

TEST_F(TenantQuotaPolicyTest, GrowthNotAskedForIsChargedPastTheQuota) {
    Register(kTenantA, 1000);
    // Memory the core did not grow for, already in use, is charged anyway.
    Put(kTenantA, "k1", 0, 300,
        {ReplicaStatus::COMPLETE, ReplicaStatus::COMPLETE,
         ReplicaStatus::COMPLETE, ReplicaStatus::COMPLETE});
    EXPECT_EQ(Charged(kTenantA), 1200u);
    EXPECT_TRUE(policy_->GetSnapshot(kTenantA)->over_quota);

    auto& ns = NamespaceOf(kTenantA);
    auto guard = ns.objects.WriteKey("k2");
    auto refused = ns.Grow(guard, 1);
    ASSERT_FALSE(refused.has_value());
    EXPECT_EQ(refused.error(), ErrorCode::TENANT_QUOTA_EXCEEDED);
}

TEST_F(TenantQuotaPolicyTest, AReplacedObjectStaysChargedUntilTheNewWrite) {
    Register(kTenantA, 1000);
    Put(kTenantA, "k", 600, 300,
        {ReplicaStatus::COMPLETE, ReplicaStatus::COMPLETE});
    auto& ns = NamespaceOf(kTenantA);

    // An upsert that reallocates: the old object's 600 bytes stay charged
    // while the new one is written.
    {
        auto guard = ns.objects.Write("k");
        ASSERT_TRUE(guard.has_value());
        ns.BeforeReplace(*guard);
        ASSERT_TRUE(ns.Grow(*guard, 200).has_value());
        guard->TearDown();
        PublishUnder(*guard, 200, MemoryReplicas({ReplicaStatus::PROCESSING}));
    }
    EXPECT_EQ(Charged(kTenantA), 800u);
    EXPECT_EQ(Words(kTenantA, "k"), (std::array<uint64_t, 2>{800, 600}));

    // Once the new write settles, only the new object is charged.
    {
        auto guard = ns.objects.Write("k");
        ASSERT_TRUE(guard.has_value());
        MarkMemoryReplicasComplete(*guard);
    }
    EXPECT_EQ(Charged(kTenantA), 200u);
    EXPECT_EQ(Words(kTenantA, "k"), (std::array<uint64_t, 2>{200, 0}));
}

TEST_F(TenantQuotaPolicyTest, TearingDownAReplacementDropsWhatItCarried) {
    Register(kTenantA, 1000);
    Put(kTenantA, "k", 600, 300,
        {ReplicaStatus::COMPLETE, ReplicaStatus::COMPLETE});
    auto& ns = NamespaceOf(kTenantA);
    {
        auto guard = ns.objects.Write("k");
        ASSERT_TRUE(guard.has_value());
        ns.BeforeReplace(*guard);
        ASSERT_TRUE(ns.Grow(*guard, 200).has_value());
        guard->TearDown();
        PublishUnder(*guard, 200, MemoryReplicas({ReplicaStatus::PROCESSING}));
    }
    ASSERT_EQ(Charged(kTenantA), 800u);

    {
        auto guard = ns.objects.Write("k");
        ASSERT_TRUE(guard.has_value());
        ns.BeforeTearDown(*guard);
        guard->TearDown();
    }
    EXPECT_EQ(Charged(kTenantA), 0u);
    EXPECT_TRUE(ns.Empty());
}

TEST_F(TenantQuotaPolicyTest, RestoreRebuildsAccountsAndKeyWords) {
    Register(kTenantA, 1000);
    Register(kTenantB, 1000);
    Put(kTenantA, "a1", 200, 100,
        {ReplicaStatus::COMPLETE, ReplicaStatus::COMPLETE});
    Put(kTenantA, "a2", 100, 100, {ReplicaStatus::COMPLETE});
    Put(kTenantB, "b1", 50, 50, {ReplicaStatus::COMPLETE});

    // Restored metadata the policy never saw: a cursor changes objects and
    // words without reporting a write release.
    for (auto& [tenant_id, ns] : store_.namespaces) {
        for (auto object : ns->objects.WriteCursor()) {
            object.metadata().AddReplicas(
                MemoryReplicas({ReplicaStatus::COMPLETE}));
            object.owner_state().policy_words = {0, 0};
        }
    }
    policy_->OnRestored();

    EXPECT_EQ(Charged(kTenantA), 500u);
    EXPECT_EQ(Charged(kTenantB), 100u);
    EXPECT_EQ(Words(kTenantA, "a1"), (std::array<uint64_t, 2>{300, 0}));
    EXPECT_EQ(Words(kTenantA, "a2"), (std::array<uint64_t, 2>{200, 0}));
    EXPECT_EQ(Words(kTenantB, "b1"), (std::array<uint64_t, 2>{100, 0}));

    // Later releases settle against the rebuilt words without a mismatch.
    for (const std::string key : {"a1", "a2"}) {
        auto& ns = NamespaceOf(kTenantA);
        auto guard = ns.objects.Write(key);
        ASSERT_TRUE(guard.has_value());
        ns.BeforeTearDown(*guard);
        guard->TearDown();
    }
    EXPECT_EQ(Charged(kTenantA), 0u);
}

TEST_F(TenantQuotaPolicyTest, CapacityChangesReachTheEffectiveQuotas) {
    store_.capacity = 1000;
    Register(kTenantA, 1000);
    EXPECT_EQ(policy_->GetSnapshot(kTenantA)->effective_quota_bytes, 1000u);
    EXPECT_EQ(policy_->AllocatableCapacityBytes(), 1000u);

    store_.capacity = 400;
    policy_->OnCapacityChanged();
    EXPECT_EQ(policy_->GetSnapshot(kTenantA)->effective_quota_bytes, 400u);
}

TEST_F(TenantQuotaPolicyTest, ATenantWithObjectsCannotBeDeleted) {
    Register(kTenantA, 1000);
    // An object that holds no memory charges nothing but still counts.
    Put(kTenantA, "k", 0, 100, {ReplicaStatus::FAILED});
    ASSERT_EQ(Charged(kTenantA), 0u);

    auto refused = policy_->DeletePolicy(kTenantA);
    ASSERT_FALSE(refused.has_value());
    EXPECT_EQ(refused.error(), ErrorCode::TENANT_NOT_EMPTY);
    EXPECT_TRUE(policy_->IsTenantRegistered(kTenantA));

    {
        auto& ns = NamespaceOf(kTenantA);
        auto guard = ns.objects.Write("k");
        ASSERT_TRUE(guard.has_value());
        ns.BeforeTearDown(*guard);
        guard->TearDown();
    }
    EXPECT_TRUE(policy_->DeletePolicy(kTenantA).has_value());
    EXPECT_FALSE(policy_->IsTenantRegistered(kTenantA));
}

TEST_F(TenantQuotaPolicyTest, MaintenanceEvictsTenantsOverTheWatermark) {
    MakePolicy({.eviction_high_watermark_ratio = 0.75,
                .eviction_ratio = 0.25,
                .eviction_check_interval = std::chrono::milliseconds(0)});
    Register(kTenantA, 1024);
    Register(kTenantB, 1024);
    // A at 7/8 of its quota, B at 1/2.
    Put(kTenantA, "a", 0, 448,
        {ReplicaStatus::COMPLETE, ReplicaStatus::COMPLETE});
    Put(kTenantB, "b", 0, 512, {ReplicaStatus::COMPLETE});

    policy_->OnMaintenance();
    // A is evicted down to watermark - eviction_ratio = 1/2: 3/8 of 1024.
    ASSERT_EQ(store_.evictions.size(), 1u);
    EXPECT_EQ(store_.evictions[0].first, kTenantA);
    EXPECT_EQ(store_.evictions[0].second, 384u);
}

TEST_F(TenantQuotaPolicyTest, MaintenanceChecksAtMostOncePerInterval) {
    MakePolicy({.eviction_high_watermark_ratio = 0.75,
                .eviction_ratio = 0.25,
                .eviction_check_interval = std::chrono::hours(1)});
    Register(kTenantA, 1024);
    Put(kTenantA, "a", 0, 448,
        {ReplicaStatus::COMPLETE, ReplicaStatus::COMPLETE});

    policy_->OnMaintenance();
    policy_->OnMaintenance();
    EXPECT_EQ(store_.evictions.size(), 1u);
}

TEST_F(TenantQuotaPolicyTest, MaintenanceWithoutAWatermarkEvictsNothing) {
    MakePolicy({.eviction_check_interval = std::chrono::milliseconds(0)});
    Register(kTenantA, 1024);
    Put(kTenantA, "a", 0, 1024,
        {ReplicaStatus::COMPLETE, ReplicaStatus::COMPLETE});

    policy_->OnMaintenance();
    EXPECT_TRUE(store_.evictions.empty());
}

}  // namespace
}  // namespace mooncake
