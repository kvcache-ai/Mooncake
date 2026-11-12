#include "tenant/quota_manager.h"

#include <atomic>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <stdexcept>
#include <string>
#include <system_error>
#include <vector>

#include <gtest/gtest.h>
#include <unistd.h>

namespace mooncake {
namespace {

const TenantId kTenantA("tenant-a");
const TenantId kTenantB("tenant-b");

class TenantQuotaManagerTest : public ::testing::Test {
   protected:
    void TearDown() override {
        for (const auto& path : policy_files_) {
            std::error_code ec;
            std::filesystem::remove(path, ec);
        }
    }

    // A fresh policy file path; the file itself is created by the first save.
    std::string NewPolicyPath() {
        auto path = std::filesystem::temp_directory_path() /
                    ("mooncake_tenant_quota_manager_test_" +
                     std::to_string(::getpid()) + "_" +
                     std::to_string(policy_files_.size()) + ".yaml");
        std::error_code ec;
        std::filesystem::remove(path, ec);
        policy_files_.push_back(path.string());
        return path.string();
    }

    // A manager over a fresh file store whose capacity reads `capacity_`.
    std::unique_ptr<TenantQuotaManager> NewManager(const std::string& path) {
        auto manager = std::make_unique<TenantQuotaManager>(
            [this] { return capacity_.load(); });
        manager->OpenPolicyStore("file", path, "test_cluster");
        return manager;
    }

    TenantQuotaSnapshot Snapshot(const TenantQuotaManager& manager,
                                 const TenantId& tenant_id) {
        auto snapshot = manager.GetSnapshot(tenant_id);
        EXPECT_TRUE(snapshot.has_value());
        return snapshot.value_or(TenantQuotaSnapshot{});
    }

    std::atomic<uint64_t> capacity_{1000};
    std::vector<std::string> policy_files_;
};

const auto kNoObjects = [](const TenantId&) { return false; };

TEST_F(TenantQuotaManagerTest, UpsertGetListDelete) {
    auto manager = NewManager(NewPolicyPath());
    EXPECT_FALSE(manager->IsTenantRegistered(kTenantA));
    EXPECT_FALSE(manager->GetSnapshot(kTenantA).has_value());

    auto upsert = manager->UpsertPolicy(kTenantA, 100);
    ASSERT_TRUE(upsert.has_value()) << toString(upsert.error());
    EXPECT_EQ(upsert->requested_quota_bytes, 100);
    EXPECT_EQ(upsert->effective_quota_bytes, 100);
    EXPECT_TRUE(upsert->has_explicit_policy);
    EXPECT_FALSE(upsert->admission_closed);
    EXPECT_TRUE(manager->IsTenantRegistered(kTenantA));

    ASSERT_TRUE(manager->UpsertPolicy(kTenantA, 300).has_value());
    ASSERT_TRUE(manager->UpsertPolicy(kTenantB, 200).has_value());
    EXPECT_EQ(Snapshot(*manager, kTenantA).requested_quota_bytes, 300);
    EXPECT_EQ(manager->ListSnapshots().size(), 2);

    auto deleted = manager->DeletePolicy(kTenantA, kNoObjects);
    ASSERT_TRUE(deleted.has_value()) << toString(deleted.error());
    // Nothing charged and no policy left, so the tenant has no snapshot.
    EXPECT_FALSE(deleted->has_value());
    EXPECT_FALSE(manager->IsTenantRegistered(kTenantA));
    EXPECT_EQ(manager->ListSnapshots().size(), 1);

    auto missing = manager->DeletePolicy(kTenantA, kNoObjects);
    ASSERT_FALSE(missing.has_value());
    EXPECT_EQ(missing.error(), ErrorCode::OBJECT_NOT_FOUND);
}

TEST_F(TenantQuotaManagerTest, UpsertRejectsOutOfRangeQuota) {
    auto manager = NewManager(NewPolicyPath());
    auto zero = manager->UpsertPolicy(kTenantA, 0);
    ASSERT_FALSE(zero.has_value());
    EXPECT_EQ(zero.error(), ErrorCode::INVALID_PARAMS);

    auto too_large = manager->UpsertPolicy(
        kTenantA, TenantQuotaAccount::kMaxChargedBytes + 1);
    ASSERT_FALSE(too_large.has_value());
    EXPECT_EQ(too_large.error(), ErrorCode::INVALID_PARAMS);
    EXPECT_FALSE(manager->IsTenantRegistered(kTenantA));
}

TEST_F(TenantQuotaManagerTest, DeleteRefusesTenantThatIsNotEmpty) {
    auto manager = NewManager(NewPolicyPath());
    ASSERT_TRUE(manager->UpsertPolicy(kTenantA, 100).has_value());

    auto& account = manager->AccountFor(kTenantA);
    ASSERT_TRUE(account.TryCharge(10).has_value());
    auto charged = manager->DeletePolicy(kTenantA, kNoObjects);
    ASSERT_FALSE(charged.has_value());
    EXPECT_EQ(charged.error(), ErrorCode::TENANT_NOT_EMPTY);
    EXPECT_TRUE(manager->IsTenantRegistered(kTenantA));
    ASSERT_TRUE(account.Release(10).has_value());

    // Nothing charged but an object still there: the policy comes back.
    bool asked = false;
    auto has_object =
        manager->DeletePolicy(kTenantA, [&](const TenantId& tenant_id) {
            asked = true;
            EXPECT_EQ(tenant_id, kTenantA);
            return true;
        });
    ASSERT_FALSE(has_object.has_value());
    EXPECT_EQ(has_object.error(), ErrorCode::TENANT_NOT_EMPTY);
    EXPECT_TRUE(asked);
    EXPECT_TRUE(manager->IsTenantRegistered(kTenantA));
    EXPECT_EQ(Snapshot(*manager, kTenantA).requested_quota_bytes, 100);
    EXPECT_EQ(Snapshot(*manager, kTenantA).effective_quota_bytes, 100);

    EXPECT_TRUE(manager->DeletePolicy(kTenantA, kNoObjects).has_value());
    EXPECT_FALSE(manager->IsTenantRegistered(kTenantA));
}

TEST_F(TenantQuotaManagerTest, PoliciesReloadFromTheStore) {
    const std::string path = NewPolicyPath();
    {
        auto writer = NewManager(path);
        ASSERT_TRUE(writer->UpsertPolicy(kTenantA, 100).has_value());
        ASSERT_TRUE(writer->UpsertPolicy(kTenantB, 200).has_value());
    }

    auto reader = NewManager(path);
    EXPECT_FALSE(reader->IsTenantRegistered(kTenantA));
    reader->LoadPoliciesOrThrow();
    EXPECT_TRUE(reader->IsTenantRegistered(kTenantA));
    EXPECT_TRUE(reader->IsTenantRegistered(kTenantB));
    EXPECT_EQ(Snapshot(*reader, kTenantA).requested_quota_bytes, 100);
    EXPECT_EQ(Snapshot(*reader, kTenantB).effective_quota_bytes, 200);
}

TEST_F(TenantQuotaManagerTest, PolicyStoreErrorsThrow) {
    TenantQuotaManager manager([] { return uint64_t{0}; });
    EXPECT_THROW(manager.LoadPoliciesOrThrow(), std::runtime_error);
    EXPECT_THROW(manager.OpenPolicyStore("unknown", "uri", "test_cluster"),
                 std::invalid_argument);

    // The file is not there yet.
    manager.OpenPolicyStore("file", NewPolicyPath(), "test_cluster");
    EXPECT_THROW(manager.LoadPoliciesOrThrow(), std::runtime_error);
}

TEST_F(TenantQuotaManagerTest, RecomputeFollowsCapacity) {
    capacity_ = 300;
    auto manager = NewManager(NewPolicyPath());
    ASSERT_TRUE(manager->UpsertPolicy(kTenantA, 100).has_value());
    ASSERT_TRUE(manager->UpsertPolicy(kTenantB, 200).has_value());
    EXPECT_EQ(Snapshot(*manager, kTenantA).effective_quota_bytes, 100);
    EXPECT_EQ(Snapshot(*manager, kTenantB).effective_quota_bytes, 200);

    // Effective quotas only move when asked to recompute.
    capacity_ = 150;
    EXPECT_EQ(Snapshot(*manager, kTenantA).effective_quota_bytes, 100);
    manager->Recompute();
    EXPECT_EQ(Snapshot(*manager, kTenantA).effective_quota_bytes, 50);
    EXPECT_EQ(Snapshot(*manager, kTenantB).effective_quota_bytes, 100);

    capacity_ = 1000;
    manager->Recompute();
    EXPECT_EQ(Snapshot(*manager, kTenantA).effective_quota_bytes, 100);
    EXPECT_EQ(Snapshot(*manager, kTenantB).effective_quota_bytes, 200);
}

TEST_F(TenantQuotaManagerTest, RebuildUsageOverwritesCharges) {
    auto manager = NewManager(NewPolicyPath());
    ASSERT_TRUE(manager->UpsertPolicy(kTenantA, 100).has_value());
    ASSERT_TRUE(manager->UpsertPolicy(kTenantB, 100).has_value());
    auto& account_a = manager->AccountFor(kTenantA);
    EXPECT_EQ(&account_a, &manager->AccountFor(kTenantA));
    ASSERT_TRUE(account_a.TryCharge(10).has_value());
    ASSERT_TRUE(manager->AccountFor(kTenantB).TryCharge(5).has_value());

    const TenantId orphan("orphan");
    manager->RebuildUsageOrThrow({{kTenantA, 40}, {orphan, 7}});
    EXPECT_EQ(account_a.ChargedBytes(), 40);
    EXPECT_FALSE(account_a.AdmissionClosed());
    // A tenant missing from the usage map is charged nothing.
    EXPECT_EQ(Snapshot(*manager, kTenantB).charged_bytes, 0);
    // A tenant with objects but no policy gets a closed account.
    auto orphan_snapshot = Snapshot(*manager, orphan);
    EXPECT_EQ(orphan_snapshot.charged_bytes, 7);
    EXPECT_EQ(orphan_snapshot.effective_quota_bytes, 0);
    EXPECT_TRUE(orphan_snapshot.admission_closed);
    EXPECT_FALSE(manager->IsTenantRegistered(orphan));

    EXPECT_THROW(manager->RebuildUsageOrThrow(
                     {{kTenantA, TenantQuotaAccount::kMaxChargedBytes + 1}}),
                 std::runtime_error);
}

}  // namespace
}  // namespace mooncake
