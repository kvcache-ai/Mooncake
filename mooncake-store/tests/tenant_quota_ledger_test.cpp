#include "tenant_quota_ledger.h"

#include <gtest/gtest.h>

namespace mooncake {
namespace {

class TenantQuotaLedgerTest : public ::testing::Test {
   protected:
    void SetUp() override {
        ASSERT_TRUE(table_.UpsertTenantPolicy(
            tenant_id_, TenantQuotaAccount::kMaxChargedBytes));
        table_.RecomputeEffectiveQuotas(TenantQuotaAccount::kMaxChargedBytes);
        account_ = table_.GetOrCreateTenantHandle(tenant_id_);
    }

    void Charge(uint64_t bytes) { ASSERT_TRUE(account_->TryCharge(bytes)); }

    const TenantId tenant_id_{"ledger-test"};
    TenantQuotaTable table_;
    TenantQuotaHandle account_{nullptr};
};

TEST_F(TenantQuotaLedgerTest, PendingFullyCommits) {
    TenantQuotaLedger ledger;
    Charge(100);
    ASSERT_TRUE(ledger.AdoptPendingCharge(account_, 100));

    EXPECT_TRUE(ledger.SettlePrimaryWrite(account_, 100));
    EXPECT_EQ(ledger.PendingBytes(), 0);
    EXPECT_EQ(ledger.CommittedBytes(), 100);
    EXPECT_EQ(ledger.TotalChargedBytes(), 100);
    EXPECT_EQ(account_->ChargedBytes(), 100);
}

TEST_F(TenantQuotaLedgerTest, PendingPartiallyCommits) {
    TenantQuotaLedger ledger;
    Charge(100);
    ASSERT_TRUE(ledger.AdoptPendingCharge(account_, 100));

    EXPECT_TRUE(ledger.SettlePrimaryWrite(account_, 60));
    EXPECT_EQ(ledger.PendingBytes(), 0);
    EXPECT_EQ(ledger.CommittedBytes(), 60);
    EXPECT_EQ(account_->ChargedBytes(), 60);
}

TEST_F(TenantQuotaLedgerTest, PendingFullyRefunds) {
    TenantQuotaLedger ledger;
    Charge(100);
    ASSERT_TRUE(ledger.AdoptPendingCharge(account_, 100));

    EXPECT_TRUE(ledger.RefundPending(account_));
    EXPECT_EQ(ledger.TotalChargedBytes(), 0);
    EXPECT_EQ(account_->ChargedBytes(), 0);
}

TEST_F(TenantQuotaLedgerTest, SettlesAdditionalCharge) {
    TenantQuotaLedger ledger;
    Charge(40);
    ASSERT_TRUE(ledger.AdoptPendingCharge(account_, 40));
    ASSERT_TRUE(ledger.SettlePrimaryWrite(account_, 40));
    Charge(60);
    uint64_t task_pending_bytes = 60;

    EXPECT_TRUE(ledger.SettleAdditional(account_, task_pending_bytes, 35));
    EXPECT_EQ(task_pending_bytes, 60);
    EXPECT_EQ(ledger.CommittedBytes(), 75);
    EXPECT_EQ(account_->ChargedBytes(), 75);
}

TEST_F(TenantQuotaLedgerTest, ReleasesCommittedBytesPartially) {
    TenantQuotaLedger ledger;
    Charge(100);
    ASSERT_TRUE(ledger.AdoptPendingCharge(account_, 100));
    ASSERT_TRUE(ledger.SettlePrimaryWrite(account_, 100));

    EXPECT_TRUE(ledger.ReleaseCommitted(account_, 40));
    EXPECT_EQ(ledger.CommittedBytes(), 60);
    EXPECT_EQ(account_->ChargedBytes(), 60);
}

TEST_F(TenantQuotaLedgerTest, PrimaryWriteSettlementReleasesReplacementCharge) {
    TenantQuotaLedger old_ledger;
    TenantQuotaLedger replacement_owner;
    TenantQuotaLedger new_ledger;
    Charge(80);
    ASSERT_TRUE(old_ledger.AdoptPendingCharge(account_, 80));
    ASSERT_TRUE(old_ledger.SettlePrimaryWrite(account_, 80));
    Charge(120);
    ASSERT_TRUE(new_ledger.AdoptPendingCharge(account_, 120));

    ASSERT_TRUE(
        old_ledger.TransferReplacementCharge(account_, replacement_owner));
    ASSERT_TRUE(
        replacement_owner.TransferReplacementCharge(account_, new_ledger));
    ASSERT_TRUE(new_ledger.SettlePrimaryWrite(account_, 100));
    EXPECT_EQ(new_ledger.PendingBytes(), 0);
    EXPECT_EQ(new_ledger.CommittedBytes(), 100);
    EXPECT_EQ(new_ledger.ReplacedBytes(), 0);
    EXPECT_EQ(new_ledger.TotalChargedBytes(), 100);
    EXPECT_EQ(account_->ChargedBytes(), 100);
}

TEST_F(TenantQuotaLedgerTest,
       PrimaryWriteMismatchPreservesPendingAndReplacementCharge) {
    TenantQuotaLedger old_ledger;
    TenantQuotaLedger new_ledger;
    Charge(30);
    ASSERT_TRUE(old_ledger.AdoptPendingCharge(account_, 30));
    ASSERT_TRUE(old_ledger.SettlePrimaryWrite(account_, 30));
    ASSERT_TRUE(old_ledger.TransferReplacementCharge(account_, new_ledger));
    Charge(70);
    ASSERT_TRUE(new_ledger.AdoptPendingCharge(account_, 70));

    auto settle_result = new_ledger.SettlePrimaryWrite(account_, 80);

    ASSERT_FALSE(settle_result);
    EXPECT_EQ(settle_result.error(), TenantQuotaError::kAccountingMismatch);
    EXPECT_EQ(new_ledger.PendingBytes(), 70);
    EXPECT_EQ(new_ledger.CommittedBytes(), 0);
    EXPECT_EQ(new_ledger.ReplacedBytes(), 30);
    EXPECT_EQ(new_ledger.TotalChargedBytes(), 100);
    EXPECT_EQ(account_->ChargedBytes(), 100);

    ASSERT_TRUE(account_->Release(80));
    auto account_mismatch = new_ledger.SettlePrimaryWrite(account_, 50);
    ASSERT_FALSE(account_mismatch);
    EXPECT_EQ(account_mismatch.error(), TenantQuotaError::kAccountingMismatch);
    EXPECT_EQ(new_ledger.PendingBytes(), 70);
    EXPECT_EQ(new_ledger.CommittedBytes(), 0);
    EXPECT_EQ(new_ledger.ReplacedBytes(), 30);
    EXPECT_EQ(new_ledger.TotalChargedBytes(), 100);
    EXPECT_EQ(account_->ChargedBytes(), 20);
}

TEST_F(TenantQuotaLedgerTest, RollsBackReplacementChargeOnFailure) {
    TenantQuotaLedger old_ledger;
    TenantQuotaLedger replacement_owner;
    Charge(80);
    ASSERT_TRUE(old_ledger.AdoptPendingCharge(account_, 80));
    ASSERT_TRUE(old_ledger.SettlePrimaryWrite(account_, 80));
    ASSERT_TRUE(
        old_ledger.TransferReplacementCharge(account_, replacement_owner));

    EXPECT_TRUE(replacement_owner.ReleaseReplacement(account_));
    EXPECT_EQ(replacement_owner.TotalChargedBytes(), 0);
    EXPECT_EQ(account_->ChargedBytes(), 0);
}

TEST_F(TenantQuotaLedgerTest, AccountingMismatchDoesNotMutateState) {
    TenantQuotaLedger ledger;
    Charge(50);
    ASSERT_TRUE(ledger.AdoptPendingCharge(account_, 50));

    auto settle_result = ledger.SettlePrimaryWrite(account_, 51);
    ASSERT_FALSE(settle_result);
    EXPECT_EQ(settle_result.error(), TenantQuotaError::kAccountingMismatch);
    EXPECT_EQ(ledger.PendingBytes(), 50);
    EXPECT_EQ(account_->ChargedBytes(), 50);

    ASSERT_TRUE(ledger.SettlePrimaryWrite(account_, 50));
    auto release_result = ledger.ReleaseCommitted(account_, 51);
    ASSERT_FALSE(release_result);
    EXPECT_EQ(release_result.error(), TenantQuotaError::kAccountingMismatch);
    EXPECT_EQ(ledger.CommittedBytes(), 50);
    EXPECT_EQ(account_->ChargedBytes(), 50);

    ASSERT_TRUE(ledger.ReleaseAll(account_));
    auto duplicate_release_all = ledger.ReleaseAll(account_);
    ASSERT_FALSE(duplicate_release_all);
    EXPECT_EQ(duplicate_release_all.error(),
              TenantQuotaError::kAccountingMismatch);

    TenantQuotaLedger inconsistent_ledger;
    ASSERT_TRUE(inconsistent_ledger.Rebuild(account_, 10));
    auto account_mismatch = inconsistent_ledger.ReleaseCommitted(account_, 10);
    ASSERT_FALSE(account_mismatch);
    EXPECT_EQ(account_mismatch.error(), TenantQuotaError::kAccountingMismatch);
    EXPECT_EQ(inconsistent_ledger.CommittedBytes(), 10);
    EXPECT_EQ(account_->ChargedBytes(), 0);
}

TEST_F(TenantQuotaLedgerTest, DuplicateReplacementOperationsAreRejected) {
    TenantQuotaLedger source;
    TenantQuotaLedger destination;
    Charge(30);
    ASSERT_TRUE(source.AdoptPendingCharge(account_, 30));
    ASSERT_TRUE(source.SettlePrimaryWrite(account_, 30));
    ASSERT_TRUE(source.TransferReplacementCharge(account_, destination));

    auto duplicate_transfer =
        source.TransferReplacementCharge(account_, destination);
    ASSERT_FALSE(duplicate_transfer);
    EXPECT_EQ(duplicate_transfer.error(),
              TenantQuotaError::kAccountingMismatch);
    EXPECT_EQ(destination.ReplacedBytes(), 30);
    EXPECT_EQ(account_->ChargedBytes(), 30);

    ASSERT_TRUE(destination.ReleaseReplacement(account_));
    auto duplicate_release = destination.ReleaseReplacement(account_);
    ASSERT_FALSE(duplicate_release);
    EXPECT_EQ(duplicate_release.error(), TenantQuotaError::kAccountingMismatch);
    EXPECT_EQ(destination.TotalChargedBytes(), 0);
    EXPECT_EQ(account_->ChargedBytes(), 0);
}

TEST_F(TenantQuotaLedgerTest, RebuildMatchesGlobalChargedBytes) {
    TenantQuotaLedger ledger;
    ASSERT_TRUE(ledger.Rebuild(account_, 90));
    ASSERT_TRUE(table_.RebuildUsage({{tenant_id_, {.charged_bytes = 90}}}));

    EXPECT_EQ(ledger.PendingBytes(), 0);
    EXPECT_EQ(ledger.CommittedBytes(), 90);
    EXPECT_EQ(ledger.ReplacedBytes(), 0);
    EXPECT_EQ(ledger.TotalChargedBytes(), account_->ChargedBytes());
}

}  // namespace
}  // namespace mooncake
