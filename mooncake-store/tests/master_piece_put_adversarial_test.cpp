#include "master_service.h"
#include "master_service/master_service_test_peer.h"
#include "tenant/quota_policy_store.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <future>
#include <memory>
#include <thread>

namespace mooncake::test {
namespace {

class MasterPiecePutAdversarialTest : public ::testing::Test {
   protected:
    void SetUp() override {
        config_.enable_piece_put = true;
        config_.max_piece_put_rounds = 2;
        service_ = std::make_unique<MasterService>(config_);
        const auto master_instance = service_->GetPiecePutMasterInstance();
        ASSERT_TRUE(master_instance);
        segment_.piece_put_master_instance = *master_instance;
        segment_.id = generate_uuid();
        segment_.name = "piece-put-adversarial";
        segment_.base = 0x500000000;
        segment_.size = 16 * 1024 * 1024;
        segment_.protocol = "tcp";
        segment_.te_endpoint = "127.0.0.1:19991";
        segment_.tcp_instance = std::string(32, 'a');
        ASSERT_TRUE(service_->MountSegment(segment_, owner_));
    }

    PiecePutStartRequest Request(uint64_t offset, uint64_t length,
                                 uint64_t sequence) const {
        return {"assembled",
                1024,
                offset,
                length,
                "cohort",
                sequence,
                false,
                std::nullopt,
                service_ ? *service_->GetPiecePutAdmission(owner_, 0)
                         : PiecePutAdmission{}};
    }

    auto Start(const PiecePutStartRequest& request) {
        return service_->PiecePutStart(owner_, request, TenantId::Default());
    }

    auto Finish(const PiecePutStartRequest& request,
                const PiecePutStartResponse& grant,
                PiecePutFinishOutcome outcome) {
        return service_->PiecePutFinish(
            owner_, {request.key, *grant.token, request.operation_seq, outcome},
            TenantId::Default());
    }

    bool SegmentIsMounted() {
        const auto details = service_->GetSegmentsDetail();
        return details &&
               std::any_of(details->begin(), details->end(),
                           [this](const auto& detail) {
                               return detail.segment_id == segment_.id;
                           });
    }

    template <typename Predicate>
    bool WaitUntil(Predicate&& predicate,
                   std::chrono::seconds timeout = std::chrono::seconds(2)) {
        const auto deadline = std::chrono::steady_clock::now() + timeout;
        do {
            if (predicate()) return true;
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
        } while (std::chrono::steady_clock::now() < deadline);
        return false;
    }

    void StopEvictionWorker() {
        MasterServiceTestPeer::EvictionRunning(*service_).store(false);
        auto& worker = MasterServiceTestPeer::EvictionThread(*service_);
        if (worker.joinable()) worker.join();
    }

    void RecreateSingleRoundService() {
        service_.reset();
        config_.max_piece_put_rounds = 1;
        service_ = std::make_unique<MasterService>(config_);
        segment_.piece_put_master_instance =
            *service_->GetPiecePutMasterInstance();
        ASSERT_TRUE(service_->MountSegment(segment_, owner_));
        MasterServiceTestPeer(*service_)
            .StopPiecePutMaintenanceWorkersForTesting();
    }

    void ExpireRound(const PiecePutStartRequest& request,
                     const TenantId& tenant = TenantId::Default()) {
        MasterServiceTestPeer::MetadataAccessorRW access(
            service_.get(), {tenant, request.key},
            MasterServiceTestPeer::MetadataAccessorRW::AsIs{});
        ASSERT_TRUE(access.IsPublished());
        ASSERT_TRUE(access.GetState().piece_put);
        access.GetState().piece_put->state.Expire(
            PiecePutState::TimePoint::max());
    }

    void ExpectOriginalGrant(const PiecePutStartRequest& request,
                             const PiecePutStartResponse& grant) {
        auto lookup = request;
        lookup.lookup_only = true;
        lookup.token = grant.token;
        const auto result = Start(lookup);
        ASSERT_TRUE(result);
        EXPECT_EQ(result->token, grant.token);
        EXPECT_FALSE(result->replica);
        const auto exists =
            service_->ExistKey(request.key, TenantId::Default());
        ASSERT_TRUE(exists);
        EXPECT_FALSE(*exists);
    }

    MasterServiceConfig config_;
    std::unique_ptr<MasterService> service_;
    const UUID owner_ = generate_uuid();
    Segment segment_;
};

class MasterPiecePutNamespaceTest : public MasterPiecePutAdversarialTest {
   protected:
    void SetUp() override {
        const auto suffix = generate_uuid();
        policy_path_ = std::filesystem::temp_directory_path() /
                       ("piece-put-namespace-" + std::to_string(suffix.first) +
                        "-" + std::to_string(suffix.second) + ".yaml");
        WritePolicies(true);
        config_.enable_multi_tenants = true;
        config_.tenant_quota_connector_type = "file";
        config_.tenant_quota_connector_uri = policy_path_.string();
        MasterPiecePutAdversarialTest::SetUp();
    }

    void TearDown() override {
        service_.reset();
        std::error_code ignored;
        std::filesystem::remove(policy_path_, ignored);
    }

    void WritePolicies(bool include_a) {
        TenantQuotaPolicySnapshot policy;
        policy.tenant_quotas = {{"tenant-b", 8 * 1024 * 1024}};
        if (include_a) {
            policy.tenant_quotas.emplace("tenant-a", 8 * 1024 * 1024);
        }
        std::ofstream output(policy_path_);
        ASSERT_TRUE(output.is_open());
        output << FormatTenantQuotaPolicyYaml(policy);
        ASSERT_TRUE(output.good());
    }

    bool HasNamespace(const TenantId& tenant) const {
        return MasterServiceTestPeer::Namespaces(*service_).Lookup(tenant) !=
               nullptr;
    }

    std::filesystem::path policy_path_;
};

TEST_F(MasterPiecePutNamespaceTest,
       RejectedWritesAndClosesDoNotCreateNamespacesButKeepNoncesSpent) {
    for (uint64_t sequence = 1; sequence <= 32; ++sequence) {
        const TenantId tenant("unknown-" + std::to_string(sequence));
        const auto request = Request(0, 1024, sequence);
        const auto denied = service_->PiecePutStart(owner_, request, tenant);
        ASSERT_FALSE(denied);
        EXPECT_EQ(denied.error(), ErrorCode::TENANT_NOT_REGISTERED);
        EXPECT_FALSE(HasNamespace(tenant));
        auto close = request;
        close.close_no_post = true;
        const auto closed = service_->PiecePutStart(owner_, close, tenant);
        ASSERT_TRUE(closed);
        EXPECT_TRUE(closed->admission_closed);
        EXPECT_FALSE(closed->token);
        EXPECT_FALSE(HasNamespace(tenant));
    }
    // Changing to a registered tenant cannot revive an already denied nonce.
    const auto replay = service_->PiecePutStart(owner_, Request(0, 1024, 1),
                                                TenantId("tenant-a"));
    ASSERT_TRUE(replay);
    EXPECT_TRUE(replay->admission_closed);
    EXPECT_FALSE(replay->replica);
    EXPECT_EQ(service_->GetStorageUsageSnapshot().memory.used_bytes, 0u);
}

TEST_F(MasterPiecePutNamespaceTest,
       InlineReclaimReusesExactQuotaForGrowthAndShrink) {
    RecreateSingleRoundService();
    const TenantId tenant("tenant-a");
    const auto policy =
        service_->tenant_quota_policy()->UpsertPolicy(tenant, 2048);
    ASSERT_TRUE(policy);
    ASSERT_EQ(policy->effective_quota_bytes, 2048u);
    const auto start = [&](const PiecePutStartRequest& request) {
        return service_->PiecePutStart(owner_, request, tenant);
    };
    const auto finish = [&](const PiecePutStartRequest& request,
                            const PiecePutStartResponse& grant,
                            PiecePutFinishOutcome outcome) {
        return service_->PiecePutFinish(
            owner_, {request.key, *grant.token, request.operation_seq, outcome},
            tenant);
    };
    const auto charge = [&] {
        return MasterServiceTestPeer(*service_)
            .TenantQuotaAccount(tenant)
            .ChargedBytes();
    };
    const auto first = Request(0, 512, 1);
    const auto old = start(first);
    ASSERT_TRUE(old);
    ASSERT_TRUE(finish(first, *old, PiecePutFinishOutcome::APPLIED));
    ExpireRound(first, tenant);
    auto larger = Request(0, 1024, 2);
    larger.total_size = 2048;
    larger.write_id = "larger-round";
    const auto grown = start(larger);
    ASSERT_TRUE(grown);
    EXPECT_EQ(charge(), 2048u);
    EXPECT_EQ(service_->GetStorageUsageSnapshot().memory.used_bytes, 2048u);
    ASSERT_TRUE(finish(larger, *grown, PiecePutFinishOutcome::APPLIED));
    ExpireRound(larger, tenant);
    auto smaller = Request(0, 256, 3);
    smaller.total_size = 512;
    smaller.write_id = "smaller-round";
    const auto shrunk = start(smaller);
    ASSERT_TRUE(shrunk);
    EXPECT_EQ(charge(), 512u);
    EXPECT_EQ(service_->GetStorageUsageSnapshot().memory.used_bytes, 512u);
    EXPECT_EQ(MasterServiceTestPeer(*service_).PiecePutRoundCountForTesting(),
              1u);
    ASSERT_TRUE(finish(smaller, *shrunk, PiecePutFinishOutcome::NO_POST));
    EXPECT_EQ(charge(), 0u);
    EXPECT_EQ(service_->GetStorageUsageSnapshot().memory.used_bytes, 0u);
    EXPECT_EQ(MasterServiceTestPeer(*service_).PiecePutRoundCountForTesting(),
              0u);
}

TEST_F(MasterPiecePutNamespaceTest,
       InlineReclaimQuotaFailureReturnsOldChargeAndAllowsSmallerRetry) {
    RecreateSingleRoundService();
    const TenantId tenant("tenant-a");
    const auto policy =
        service_->tenant_quota_policy()->UpsertPolicy(tenant, 1024);
    ASSERT_TRUE(policy);
    ASSERT_EQ(policy->effective_quota_bytes, 1024u);
    const auto first = Request(0, 512, 1);
    const auto old = service_->PiecePutStart(owner_, first, tenant);
    ASSERT_TRUE(old);
    ASSERT_TRUE(
        service_->PiecePutFinish(owner_,
                                 {first.key, *old->token, first.operation_seq,
                                  PiecePutFinishOutcome::APPLIED},
                                 tenant));
    ExpireRound(first, tenant);
    auto larger = Request(0, 2048, 2);
    larger.total_size = 2048;
    larger.write_id = "over-quota";
    const auto refused = service_->PiecePutStart(owner_, larger, tenant);
    ASSERT_FALSE(refused);
    EXPECT_EQ(refused.error(), ErrorCode::TENANT_QUOTA_EXCEEDED);
    EXPECT_FALSE(service_->GetReplicaList(first.key, tenant));
    EXPECT_EQ(MasterServiceTestPeer(*service_)
                  .TenantQuotaAccount(tenant)
                  .ChargedBytes(),
              0u);
    EXPECT_EQ(service_->GetStorageUsageSnapshot().memory.used_bytes, 0u);
    EXPECT_EQ(MasterServiceTestPeer(*service_).PiecePutRoundCountForTesting(),
              0u);
    auto smaller = Request(0, 512, 3);
    smaller.total_size = 512;
    smaller.write_id = "smaller-retry";
    const auto retry = service_->PiecePutStart(owner_, smaller, tenant);
    ASSERT_TRUE(retry);
    EXPECT_EQ(MasterServiceTestPeer(*service_)
                  .TenantQuotaAccount(tenant)
                  .ChargedBytes(),
              512u);
    ASSERT_TRUE(service_->PiecePutFinish(
        owner_,
        {smaller.key, *retry->token, smaller.operation_seq,
         PiecePutFinishOutcome::NO_POST},
        tenant));
    EXPECT_EQ(MasterServiceTestPeer(*service_)
                  .TenantQuotaAccount(tenant)
                  .ChargedBytes(),
              0u);
}

TEST_F(MasterPiecePutNamespaceTest,
       InlineReclaimAllocatorFailureReturnsGrowthChargeAndBudget) {
    MasterServiceTestPeer(*service_).StopPiecePutMaintenanceWorkersForTesting();
    constexpr uint64_t mib = 1024 * 1024;
    const TenantId tenant_a("tenant-a"), tenant_b("tenant-b");
    const auto start = [&](const PiecePutStartRequest& request,
                           const TenantId& tenant) {
        return service_->PiecePutStart(owner_, request, tenant);
    };
    const auto finish = [&](const PiecePutStartRequest& request,
                            const PiecePutStartResponse& grant,
                            const TenantId& tenant,
                            PiecePutFinishOutcome outcome) {
        return service_->PiecePutFinish(
            owner_, {request.key, *grant.token, request.operation_seq, outcome},
            tenant);
    };
    auto first = Request(0, mib / 2, 1);
    first.total_size = mib;
    const auto old = start(first, tenant_a);
    ASSERT_TRUE(old);
    ASSERT_TRUE(finish(first, *old, tenant_a, PiecePutFinishOutcome::APPLIED));
    // Interleave 2 MiB B extents with temporary A extents. Removing the A
    // objects leaves enough aggregate space but no contiguous 4 MiB extent.
    std::vector<std::string> holes;
    for (uint64_t i = 0; i < 4; ++i) {
        auto filler = Request(0, 2 * mib, 10 + 2 * i);
        filler.total_size = 2 * mib;
        filler.key = "b-filler-" + std::to_string(i);
        const auto filled = start(filler, tenant_b);
        ASSERT_TRUE(filled);
        ASSERT_TRUE(
            finish(filler, *filled, tenant_b, PiecePutFinishOutcome::APPLIED));
        if (i == 3) break;
        auto hole = Request(0, 2 * mib, 11 + 2 * i);
        hole.total_size = 2 * mib;
        hole.key = "a-hole-" + std::to_string(i);
        const auto filled_hole = start(hole, tenant_a);
        ASSERT_TRUE(filled_hole);
        ASSERT_TRUE(finish(hole, *filled_hole, tenant_a,
                           PiecePutFinishOutcome::APPLIED));
        holes.push_back(hole.key);
    }
    for (const auto& key : holes) {
        ASSERT_TRUE(service_->Remove(key, tenant_a, true));
    }
    ExpireRound(first, tenant_a);
    auto larger = Request(0, 4 * mib, 50);
    larger.total_size = 4 * mib;
    larger.write_id = "fragmented-growth";
    const auto refused = start(larger, tenant_a);
    ASSERT_FALSE(refused);
    EXPECT_EQ(refused.error(), ErrorCode::NO_AVAILABLE_HANDLE);
    EXPECT_FALSE(service_->GetReplicaList(first.key, tenant_a));
    EXPECT_EQ(MasterServiceTestPeer(*service_)
                  .TenantQuotaAccount(tenant_a)
                  .ChargedBytes(),
              0u);
    EXPECT_EQ(MasterServiceTestPeer(*service_)
                  .TenantQuotaAccount(tenant_b)
                  .ChargedBytes(),
              8 * mib);
    EXPECT_EQ(service_->GetStorageUsageSnapshot().memory.used_bytes, 8 * mib);
    EXPECT_EQ(MasterServiceTestPeer(*service_).PiecePutRoundCountForTesting(),
              0u);
    auto smaller = Request(0, mib, 51);
    smaller.total_size = mib;
    smaller.write_id = "fits-after-failure";
    const auto retry = start(smaller, tenant_a);
    ASSERT_TRUE(retry);
    EXPECT_EQ(MasterServiceTestPeer(*service_)
                  .TenantQuotaAccount(tenant_a)
                  .ChargedBytes(),
              mib);
    ASSERT_TRUE(
        finish(smaller, *retry, tenant_a, PiecePutFinishOutcome::NO_POST));
    EXPECT_EQ(MasterServiceTestPeer(*service_)
                  .TenantQuotaAccount(tenant_a)
                  .ChargedBytes(),
              0u);
    EXPECT_EQ(service_->GetStorageUsageSnapshot().memory.used_bytes, 8 * mib);
}

TEST_F(MasterPiecePutNamespaceTest,
       MissingNamespaceCloseFencesDelayedStartWithoutCreatingNamespace) {
    const TenantId tenant("tenant-a");
    const auto request = Request(0, 1024, 1);
    auto lookup = request;
    lookup.lookup_only = true;
    ASSERT_TRUE(service_->PiecePutStart(owner_, lookup, tenant));
    EXPECT_FALSE(HasNamespace(tenant));
    auto close = request;
    close.close_no_post = true;
    const auto closed = service_->PiecePutStart(owner_, close, tenant);
    ASSERT_TRUE(closed);
    EXPECT_TRUE(closed->admission_closed);
    EXPECT_FALSE(HasNamespace(tenant));
    const auto delayed = service_->PiecePutStart(owner_, request, tenant);
    ASSERT_TRUE(delayed);
    EXPECT_TRUE(delayed->admission_closed);
    EXPECT_FALSE(delayed->replica);
    EXPECT_EQ(service_->GetStorageUsageSnapshot().memory.used_bytes, 0u);

    auto fresh = Request(0, 1024, 2);
    const auto grant = service_->PiecePutStart(owner_, fresh, tenant);
    ASSERT_TRUE(grant);
    ASSERT_TRUE(grant->replica);
    ASSERT_TRUE(
        service_->PiecePutFinish(owner_,
                                 {fresh.key, *grant->token, fresh.operation_seq,
                                  PiecePutFinishOutcome::NO_POST},
                                 tenant));
}

TEST_F(MasterPiecePutNamespaceTest,
       MissingNamespaceLookupDoesNotSpendTheReservationNonce) {
    const TenantId tenant("tenant-b");
    const auto request = Request(0, 1024, 1);
    auto lookup = request;
    lookup.lookup_only = true;
    const auto missing = service_->PiecePutStart(owner_, lookup, tenant);
    ASSERT_TRUE(missing);
    EXPECT_FALSE(missing->admission_closed);
    EXPECT_FALSE(HasNamespace(tenant));
    const auto grant = service_->PiecePutStart(owner_, request, tenant);
    ASSERT_TRUE(grant);
    ASSERT_TRUE(grant->replica);
    ASSERT_TRUE(service_->PiecePutFinish(
        owner_,
        {request.key, *grant->token, request.operation_seq,
         PiecePutFinishOutcome::NO_POST},
        tenant));
}

TEST_F(MasterPiecePutNamespaceTest,
       OriginalTokenAgainstMissingNamespaceDoesNotDereferenceAnEmptyAccessor) {
    const TenantId original_tenant("tenant-a"), missing_tenant("tenant-b");
    const auto request = Request(0, 512, 1);
    const auto grant =
        service_->PiecePutStart(owner_, request, original_tenant);
    ASSERT_TRUE(grant);
    ASSERT_TRUE(grant->token);
    ASSERT_FALSE(HasNamespace(missing_tenant));

    auto lookup = request;
    lookup.lookup_only = true;
    lookup.token = grant->token;
    const auto missing =
        service_->PiecePutStart(owner_, lookup, missing_tenant);
    ASSERT_TRUE(missing);
    EXPECT_FALSE(missing->token);
    EXPECT_FALSE(missing->replica);
    EXPECT_EQ(missing->round_state, PiecePutRoundState::UNKNOWN);

    auto close = lookup;
    close.close_no_post = true;
    const auto closed = service_->PiecePutStart(owner_, close, missing_tenant);
    ASSERT_TRUE(closed);
    EXPECT_TRUE(closed->admission_closed);
    EXPECT_FALSE(closed->token);
    const auto finished = service_->PiecePutFinish(
        owner_,
        {request.key, *grant->token, request.operation_seq,
         PiecePutFinishOutcome::NO_POST},
        missing_tenant);
    ASSERT_TRUE(finished);
    EXPECT_EQ(finished->round_state, PiecePutRoundState::UNKNOWN);
    EXPECT_FALSE(HasNamespace(missing_tenant));

    // The rejected namespace must not change or settle the original grant.
    const auto original =
        service_->PiecePutStart(owner_, lookup, original_tenant);
    ASSERT_TRUE(original);
    EXPECT_EQ(original->token, grant->token);
    EXPECT_EQ(original->piece_state, PiecePutPieceState::EXPOSED);
    ASSERT_TRUE(service_->PiecePutFinish(
        owner_,
        {request.key, *grant->token, request.operation_seq,
         PiecePutFinishOutcome::NO_POST},
        original_tenant));
}

TEST_F(MasterPiecePutNamespaceTest,
       CloseRechecksNamespaceCreatedAfterItsFirstMissAndSettlesGrant) {
    const TenantId tenant("tenant-a");
    const auto request = Request(0, 1024, 1);
    auto close = request;
    close.close_no_post = true;
    std::promise<void> missed_namespace;
    auto missed = missed_namespace.get_future();
    std::promise<void> continue_close;
    const auto resume = continue_close.get_future().share();
    MasterServiceTestPeer(*service_).SetPiecePutMissingNamespaceHookForTesting(
        [&] {
            missed_namespace.set_value();
            resume.wait();
        });
    auto closing = std::async(std::launch::async, [&] {
        return service_->PiecePutStart(owner_, close, tenant);
    });
    // Release before the async future is destroyed, including ASSERT exits.
    struct ResumeClose {
        std::promise<void>& promise;
        bool released{false};
        void Release() {
            if (!released) {
                promise.set_value();
                released = true;
            }
        }
        ~ResumeClose() { Release(); }
    } release{continue_close};
    ASSERT_EQ(missed.wait_for(std::chrono::seconds(2)),
              std::future_status::ready);
    EXPECT_FALSE(HasNamespace(tenant));
    // Start wins consumption while Close is paused after its first miss.
    const auto grant = service_->PiecePutStart(owner_, request, tenant);
    ASSERT_TRUE(grant);
    ASSERT_TRUE(grant->replica);
    EXPECT_TRUE(HasNamespace(tenant));
    release.Release();
    const auto closed = closing.get();
    MasterServiceTestPeer(*service_).SetPiecePutMissingNamespaceHookForTesting(
        {});
    ASSERT_TRUE(closed);
    EXPECT_TRUE(closed->admission_closed);
    EXPECT_EQ(closed->piece_state, PiecePutPieceState::WITHDRAWN);
    EXPECT_FALSE(closed->replica);
    EXPECT_EQ(service_->GetStorageUsageSnapshot().memory.used_bytes, 0u);
    const auto late = service_->PiecePutStart(owner_, request, tenant);
    ASSERT_TRUE(late);
    EXPECT_TRUE(late->admission_closed);
    EXPECT_FALSE(late->replica);
    EXPECT_FALSE(late->token);
}

TEST_F(MasterPiecePutNamespaceTest,
       RevokedTenantOldSessionCloseSettlesOnlyItsOriginalClaim) {
    const TenantId tenant("tenant-a");
    const auto first = Request(0, 512, 1);
    const auto old_grant = service_->PiecePutStart(owner_, first, tenant);
    ASSERT_TRUE(old_grant);
    ASSERT_TRUE(
        service_->GetPiecePutAdmission(owner_, first.admission.session_id));
    const auto second = Request(512, 512, 1);
    const auto new_grant = service_->PiecePutStart(owner_, second, tenant);
    ASSERT_TRUE(new_grant);
    ASSERT_NE(first.admission.session_id, second.admission.session_id);
    WritePolicies(false);
    MasterServiceTestPeer(*service_).LoadTenantQuotaPoliciesFromStoreOrThrow();
    service_->tenant_quota_policy()->OnRestored();
    ASSERT_FALSE(service_->tenant_quota_policy()->IsTenantRegistered(tenant));

    auto close = first;
    close.close_no_post = true;
    const auto closed = service_->PiecePutStart(owner_, close, tenant);
    ASSERT_TRUE(closed);
    EXPECT_TRUE(closed->admission_closed);
    EXPECT_EQ(closed->piece_state, PiecePutPieceState::WITHDRAWN);
    EXPECT_EQ(closed->token, old_grant->token);
    auto lookup = second;
    lookup.lookup_only = true;
    const auto retained = service_->PiecePutStart(owner_, lookup, tenant);
    ASSERT_TRUE(retained);
    EXPECT_EQ(retained->piece_state, PiecePutPieceState::EXPOSED);
    EXPECT_EQ(retained->token, new_grant->token);
    close = second;
    close.close_no_post = true;
    const auto settled = service_->PiecePutStart(owner_, close, tenant);
    ASSERT_TRUE(settled);
    EXPECT_EQ(settled->piece_state, PiecePutPieceState::WITHDRAWN);
    EXPECT_EQ(service_->GetStorageUsageSnapshot().memory.used_bytes, 0u);
}

TEST_F(MasterPiecePutAdversarialTest,
       BulkOrdinaryMutatorsCannotDiscardAnUnsettledRound) {
    const auto request = Request(0, 512, 1);
    const auto grant = Start(request);
    ASSERT_TRUE(grant);

    for (const auto retiring : {false, true}) {
        if (retiring) {
            ASSERT_TRUE(
                Finish(request, *grant, PiecePutFinishOutcome::UNCERTAIN));
        }
        const auto ends = service_->BatchPutEnd(
            owner_, {{request.key, std::nullopt}}, TenantId::Default());
        ASSERT_EQ(ends.size(), 1u);
        EXPECT_FALSE(ends[0]);
        const auto revokes = service_->BatchPutRevoke(owner_, {request.key},
                                                      TenantId::Default());
        ASSERT_EQ(revokes.size(), 1u);
        EXPECT_FALSE(revokes[0]);
        const auto upserts = service_->BatchUpsertStart(
            owner_, {request.key}, TenantId::Default(), {2048},
            ReplicateConfig{});
        ASSERT_EQ(upserts.size(), 1u);
        EXPECT_FALSE(upserts[0]);
        const auto removals =
            service_->BatchRemove({request.key}, TenantId::Default(), true);
        ASSERT_EQ(removals.size(), 1u);
        EXPECT_FALSE(removals[0]);
        EXPECT_EQ(service_->RemoveAll(TenantId::Default(), true), 0);
        const auto regex =
            service_->RemoveByRegex(".*", TenantId::Default(), true);
        ASSERT_TRUE(regex);
        EXPECT_EQ(*regex, 0);
        ExpectOriginalGrant(request, *grant);
    }
    ASSERT_TRUE(Finish(request, *grant, PiecePutFinishOutcome::NO_POST));
}

TEST_F(MasterPiecePutAdversarialTest,
       RoundBudgetIsHeldUntilEveryExposedGrantSettles) {
    service_.reset();
    config_.max_piece_put_rounds = 1;
    service_ = std::make_unique<MasterService>(config_);
    const auto master_instance = service_->GetPiecePutMasterInstance();
    ASSERT_TRUE(master_instance);
    segment_.piece_put_master_instance = *master_instance;
    ASSERT_TRUE(service_->MountSegment(segment_, owner_));
    const auto first = Request(0, 512, 1);
    const auto second = Request(512, 512, 2);
    const auto first_grant = Start(first);
    const auto second_grant = Start(second);
    ASSERT_TRUE(first_grant);
    ASSERT_TRUE(second_grant);
    ASSERT_TRUE(Finish(first, *first_grant, PiecePutFinishOutcome::NO_POST));

    auto other = Request(0, 1024, 3);
    other.key = "another-object";
    const auto exhausted = Start(other);
    ASSERT_FALSE(exhausted);
    EXPECT_EQ(exhausted.error(), ErrorCode::NO_AVAILABLE_HANDLE);
    ExpectOriginalGrant(second, *second_grant);

    ASSERT_TRUE(Finish(second, *second_grant, PiecePutFinishOutcome::NO_POST));
    ++other.operation_seq;
    const auto admitted = Start(other);
    ASSERT_TRUE(admitted);
    ASSERT_TRUE(admitted->token);
    const auto stale =
        Finish(first, *first_grant, PiecePutFinishOutcome::APPLIED);
    ASSERT_TRUE(stale);
    EXPECT_EQ(stale->round_state, PiecePutRoundState::UNKNOWN);
    ASSERT_TRUE(Finish(other, *admitted, PiecePutFinishOutcome::NO_POST));
}

TEST_F(MasterPiecePutAdversarialTest,
       SettledExpiredRoundReplacedInlineAtSingleRoundBudget) {
    RecreateSingleRoundService();
    const auto first = Request(0, 512, 1);
    const auto old = Start(first);
    ASSERT_TRUE(old);
    ASSERT_TRUE(Finish(first, *old, PiecePutFinishOutcome::APPLIED));
    ExpireRound(first);

    // The old operation still names its original result, never a new grant.
    const auto replay = Start(first);
    ASSERT_TRUE(replay);
    EXPECT_EQ(replay->token, old->token);
    EXPECT_EQ(replay->piece_state, PiecePutPieceState::APPLIED);
    EXPECT_EQ(replay->round_state, PiecePutRoundState::RETIRING);
    EXPECT_FALSE(replay->replica);

    auto next = Request(0, 1024, 2);
    next.write_id = "replacement";
    const auto replacement = Start(next);
    ASSERT_TRUE(replacement);
    ASSERT_TRUE(replacement->token);
    EXPECT_NE(replacement->token->generation, old->token->generation);
    EXPECT_EQ(service_->GetStorageUsageSnapshot().memory.used_bytes, 1024u);
    EXPECT_EQ(MasterServiceTestPeer(*service_).PiecePutRoundCountForTesting(),
              1u);
    const auto retry = Start(next);
    ASSERT_TRUE(retry);
    EXPECT_EQ(retry->token, replacement->token);

    const auto late = Finish(first, *old, PiecePutFinishOutcome::APPLIED);
    ASSERT_TRUE(late);
    EXPECT_EQ(late->round_state, PiecePutRoundState::UNKNOWN);
    ExpectOriginalGrant(next, *replacement);
    auto conflict = Request(0, 1024, 3);
    conflict.write_id = "conflicting-open-batch";
    const auto rejected = Start(conflict);
    ASSERT_FALSE(rejected);
    EXPECT_EQ(rejected.error(), ErrorCode::INVALID_PARAMS);

    ASSERT_TRUE(Finish(next, *replacement, PiecePutFinishOutcome::NO_POST));
    EXPECT_EQ(MasterServiceTestPeer(*service_).PiecePutRoundCountForTesting(),
              0u);
    EXPECT_EQ(service_->GetStorageUsageSnapshot().memory.used_bytes, 0u);
    ++next.operation_seq;
    const auto after_destruction = Start(next);
    ASSERT_TRUE(after_destruction);
    ASSERT_TRUE(
        Finish(next, *after_destruction, PiecePutFinishOutcome::NO_POST));
    EXPECT_EQ(MasterServiceTestPeer(*service_).PiecePutRoundCountForTesting(),
              0u);
}

TEST_F(MasterPiecePutAdversarialTest,
       ConcurrentFreshPiecesReplaceOnlyOneSafelyRetiredGeneration) {
    RecreateSingleRoundService();
    const auto first = Request(0, 512, 1);
    const auto old = Start(first);
    ASSERT_TRUE(old);
    ASSERT_TRUE(Finish(first, *old, PiecePutFinishOutcome::APPLIED));
    ExpireRound(first);
    // Independent producers use independent windows; only the key guard
    // coordinates replacement and the following two claims.
    const UUID left_client = generate_uuid();
    const UUID right_client = generate_uuid();
    const auto left_admission = service_->GetPiecePutAdmission(left_client, 0);
    const auto right_admission =
        service_->GetPiecePutAdmission(right_client, 0);
    ASSERT_TRUE(left_admission);
    ASSERT_TRUE(right_admission);
    EXPECT_NE(left_admission->session_id, right_admission->session_id);
    auto left = Request(0, 512, 1);
    auto right = Request(512, 512, 1);
    left.admission = *left_admission;
    right.admission = *right_admission;
    std::promise<void> go, left_waiting, right_waiting;
    const auto ready = go.get_future().share();
    auto a = std::async(std::launch::async, [&] {
        left_waiting.set_value();
        ready.wait();
        return service_->PiecePutStart(left_client, left, TenantId::Default());
    });
    auto b = std::async(std::launch::async, [&] {
        right_waiting.set_value();
        ready.wait();
        return service_->PiecePutStart(right_client, right,
                                       TenantId::Default());
    });
    left_waiting.get_future().wait();
    right_waiting.get_future().wait();
    go.set_value();
    const auto first_grant = a.get();
    const auto second_grant = b.get();
    ASSERT_TRUE(first_grant);
    ASSERT_TRUE(second_grant);
    ASSERT_TRUE(first_grant->token);
    ASSERT_TRUE(second_grant->token);
    EXPECT_EQ(first_grant->token->generation, second_grant->token->generation);
    EXPECT_NE(first_grant->token->generation, old->token->generation);
    EXPECT_NE(first_grant->token->slot, second_grant->token->slot);
    EXPECT_EQ(first_grant->token->admission_session_id,
              left.admission.session_id);
    EXPECT_EQ(second_grant->token->admission_session_id,
              right.admission.session_id);
    EXPECT_EQ(MasterServiceTestPeer(*service_).PiecePutRoundCountForTesting(),
              1u);
    EXPECT_EQ(service_->GetStorageUsageSnapshot().memory.used_bytes, 1024u);
    ASSERT_TRUE(service_->PiecePutFinish(
        left_client,
        {left.key, *first_grant->token, left.operation_seq,
         PiecePutFinishOutcome::APPLIED},
        TenantId::Default()));
    const auto completed = service_->PiecePutFinish(
        right_client,
        {right.key, *second_grant->token, right.operation_seq,
         PiecePutFinishOutcome::APPLIED},
        TenantId::Default());
    ASSERT_TRUE(completed);
    EXPECT_EQ(completed->round_state, PiecePutRoundState::ROUND_PUBLISHED);
    EXPECT_EQ(MasterServiceTestPeer(*service_).PiecePutRoundCountForTesting(),
              0u);
}

TEST_F(MasterPiecePutAdversarialTest,
       UnknownRetiringRoundStaysBusyWithoutReusingBytesOrBudget) {
    RecreateSingleRoundService();
    const auto first = Request(0, 512, 1);
    const auto old = Start(first);
    ASSERT_TRUE(old);
    const auto address =
        old->replica->get_memory_descriptor().buffer_descriptor.buffer_address_;
    ExpireRound(first);  // Time alone is not a drain proof.
    auto next = Request(0, 1024, 2);
    next.write_id = "replacement";
    const auto busy = Start(next);
    ASSERT_FALSE(busy);
    EXPECT_EQ(busy.error(), ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    auto same_batch = Request(512, 512, 3);
    const auto also_busy = Start(same_batch);
    ASSERT_FALSE(also_busy);
    EXPECT_EQ(also_busy.error(), ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    ExpectOriginalGrant(first, *old);
    auto other = Request(0, 1024, 4);
    other.key = "another-key";
    const auto no_budget = Start(other);
    ASSERT_FALSE(no_budget);
    EXPECT_EQ(no_budget.error(), ErrorCode::NO_AVAILABLE_HANDLE);
    EXPECT_EQ(service_->GetStorageUsageSnapshot().memory.used_bytes, 1024u);
    EXPECT_EQ(MasterServiceTestPeer(*service_).PiecePutRoundCountForTesting(),
              1u);
    EXPECT_FALSE(service_->PutStart(owner_, first.key, TenantId::Default(),
                                    1024, ReplicateConfig{}));
    EXPECT_FALSE(service_->Remove(first.key, TenantId::Default(), true));
    auto ordinary = service_->PutStart(owner_, "ordinary", TenantId::Default(),
                                       1024, ReplicateConfig{});
    ASSERT_TRUE(ordinary);
    EXPECT_NE(ordinary->front()
                  .get_memory_descriptor()
                  .buffer_descriptor.buffer_address_,
              address);
    ObjectMeta end{"ordinary", std::nullopt};
    end.strict_write_proof = *MakeStrictWriteProof(*ordinary);
    ASSERT_TRUE(service_->PutEnd(owner_, end, TenantId::Default(),
                                 ReplicaType::MEMORY));
    ASSERT_TRUE(service_->Remove("ordinary", TenantId::Default(), true));

    // A late definitive result can still settle the old grant and reclaim it.
    ASSERT_TRUE(Finish(first, *old, PiecePutFinishOutcome::APPLIED));
    EXPECT_EQ(service_->GetStorageUsageSnapshot().memory.used_bytes, 0u);
    EXPECT_EQ(MasterServiceTestPeer(*service_).PiecePutRoundCountForTesting(),
              0u);
    next.operation_seq = 5;
    const auto replacement = Start(next);
    ASSERT_TRUE(replacement);
    ASSERT_TRUE(Finish(next, *replacement, PiecePutFinishOutcome::NO_POST));
}

TEST_F(MasterPiecePutAdversarialTest,
       ExactPhysicalRetirementAllowsInlineReplacementOnNewTarget) {
    RecreateSingleRoundService();
    const auto first = Request(0, 512, 1);
    const auto old = Start(first);
    ASSERT_TRUE(old);
    ASSERT_TRUE(Finish(first, *old, PiecePutFinishOutcome::UNCERTAIN));
    // Unit-level lower-layer proof; integration tests use a real local drain.
    ASSERT_TRUE(
        service_->RetirePiecePutEndpoint(owner_, *segment_.tcp_instance));
    auto replacement_segment = segment_;
    replacement_segment.id = generate_uuid();
    replacement_segment.name += "-replacement";
    replacement_segment.base += segment_.size * 2;
    replacement_segment.te_endpoint = "127.0.0.1:19992";
    replacement_segment.tcp_instance = std::string(32, 'b');
    ASSERT_TRUE(service_->MountSegment(replacement_segment, owner_));
    auto next = Request(0, 1024, 2);
    next.write_id = "replacement";
    const auto replacement = Start(next);
    ASSERT_TRUE(replacement);
    ASSERT_TRUE(replacement->replica);
    EXPECT_EQ(replacement->replica->get_memory_descriptor()
                  .buffer_descriptor.buffer_address_,
              replacement_segment.base);
    EXPECT_EQ(service_->GetStorageUsageSnapshot().memory.used_bytes, 1024u);
    EXPECT_EQ(MasterServiceTestPeer(*service_).PiecePutRoundCountForTesting(),
              1u);
    const auto late = Finish(first, *old, PiecePutFinishOutcome::APPLIED);
    ASSERT_TRUE(late);
    EXPECT_EQ(late->round_state, PiecePutRoundState::UNKNOWN);
    ExpectOriginalGrant(next, *replacement);
    ASSERT_TRUE(Finish(next, *replacement, PiecePutFinishOutcome::NO_POST));
}

TEST_F(MasterPiecePutAdversarialTest,
       PeriodicMaintenanceReclaimsSettledIncompleteRoundButPinsExposed) {
    service_.reset();
    config_.piece_put_timeout_sec = 1;
    service_ = std::make_unique<MasterService>(config_);
    const auto master_instance = service_->GetPiecePutMasterInstance();
    ASSERT_TRUE(master_instance);
    segment_.piece_put_master_instance = *master_instance;
    ASSERT_TRUE(service_->MountSegment(segment_, owner_));
    const auto settled = Request(0, 512, 1);
    const auto settled_grant = Start(settled);
    ASSERT_TRUE(settled_grant);
    ASSERT_TRUE(
        Finish(settled, *settled_grant, PiecePutFinishOutcome::APPLIED));
    auto exposed = Request(0, 512, 2);
    exposed.key = "still-exposed";
    const auto exposed_grant = Start(exposed);
    ASSERT_TRUE(exposed_grant);

    // A lookup neither writes nor schedules cleanup. The periodic sweep must
    // reclaim the settled but incomplete object, below the memory watermark.
    auto lookup = settled;
    lookup.lookup_only = true;
    lookup.token = settled_grant->token;
    ASSERT_TRUE(WaitUntil(
        [&] {
            const auto result = Start(lookup);
            return result && result->round_state == PiecePutRoundState::UNKNOWN;
        },
        std::chrono::seconds(4)));
    auto exposed_lookup = exposed;
    exposed_lookup.lookup_only = true;
    exposed_lookup.token = exposed_grant->token;
    const auto retained = Start(exposed_lookup);
    ASSERT_TRUE(retained);
    EXPECT_EQ(retained->round_state, PiecePutRoundState::RETIRING);
    EXPECT_EQ(retained->piece_state, PiecePutPieceState::EXPOSED);
    EXPECT_EQ(retained->token, exposed_grant->token);

    // Reclamation returns the key and its round charge; expiry alone must
    // never do the same for an outstanding grant.
    auto replacement = Request(0, 1024, 3);
    replacement.write_id = "next-cohort";
    const auto replacement_grant = Start(replacement);
    ASSERT_TRUE(replacement_grant);
    ASSERT_TRUE(replacement_grant->token);
    EXPECT_NE(replacement_grant->token->generation,
              settled_grant->token->generation);
    ASSERT_TRUE(Finish(replacement, *replacement_grant,
                       PiecePutFinishOutcome::NO_POST));
    ASSERT_TRUE(
        Finish(exposed, *exposed_grant, PiecePutFinishOutcome::NO_POST));
}

TEST_F(MasterPiecePutAdversarialTest,
       EligibleAllocationFailureSignalsEvictionBelowWatermark) {
    StopEvictionWorker();
    auto cached = Request(0, 8 * 1024 * 1024, 1);
    cached.total_size = cached.length;
    const auto grant = Start(cached);
    ASSERT_TRUE(grant);
    ASSERT_TRUE(Finish(cached, *grant, PiecePutFinishOutcome::APPLIED));
    EXPECT_FALSE(MasterServiceTestPeer::NeedMemEviction(*service_).load());

    // The pool is only half full, but no 12 MiB allocation fits. A failed
    // PiecePut must signal the same pressure recovery as an ordinary Put.
    auto large = Request(0, 12 * 1024 * 1024, 2);
    large.key = "needs-capacity";
    large.total_size = large.length;
    const auto result = Start(large);
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error(), ErrorCode::NO_AVAILABLE_HANDLE);
    EXPECT_TRUE(MasterServiceTestPeer::NeedMemEviction(*service_).load());
}

TEST_F(MasterPiecePutAdversarialTest,
       MissingEligibleTargetDoesNotSignalAllocationPressure) {
    StopEvictionWorker();
    ASSERT_TRUE(service_->GracefulUnmountSegment(segment_.id, owner_, 10000));
    const auto result = Start(Request(0, 1024, 1));
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error(), ErrorCode::NO_AVAILABLE_HANDLE);
    EXPECT_FALSE(MasterServiceTestPeer::NeedMemEviction(*service_).load());
}

TEST_F(MasterPiecePutAdversarialTest,
       RoundBudgetFailureDoesNotSignalAllocationPressure) {
    StopEvictionWorker();
    const auto first = Request(0, 512, 1);
    auto second = Request(0, 512, 2);
    second.key = "second-round";
    const auto first_grant = Start(first);
    const auto second_grant = Start(second);
    ASSERT_TRUE(first_grant);
    ASSERT_TRUE(second_grant);
    auto third = Request(0, 1024, 3);
    third.key = "over-budget";
    const auto result = Start(third);
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error(), ErrorCode::NO_AVAILABLE_HANDLE);
    EXPECT_FALSE(MasterServiceTestPeer::NeedMemEviction(*service_).load());
    ASSERT_TRUE(Finish(first, *first_grant, PiecePutFinishOutcome::NO_POST));
    ASSERT_TRUE(Finish(second, *second_grant, PiecePutFinishOutcome::NO_POST));
}

TEST_F(MasterPiecePutAdversarialTest,
       ExistingRoundCanCompleteDuringGraceWithoutAdmittingANewObject) {
    const auto first = Request(0, 512, 1);
    const auto first_grant = Start(first);
    ASSERT_TRUE(first_grant);
    ASSERT_TRUE(service_->GracefulUnmountSegment(segment_.id, owner_, 10000));

    auto unrelated = Request(0, 1024, 2);
    unrelated.key = "new-object";
    EXPECT_FALSE(Start(unrelated));
    const auto second = Request(512, 512, 3);
    const auto second_grant = Start(second);
    ASSERT_TRUE(second_grant);
    ASSERT_TRUE(second_grant->replica);
    ASSERT_TRUE(Finish(first, *first_grant, PiecePutFinishOutcome::APPLIED));
    const auto final =
        Finish(second, *second_grant, PiecePutFinishOutcome::APPLIED);
    ASSERT_TRUE(final);
    EXPECT_EQ(final->round_state, PiecePutRoundState::ROUND_PUBLISHED);
    EXPECT_TRUE(service_->GetReplicaList(first.key, TenantId::Default()));
}

TEST_F(MasterPiecePutAdversarialTest,
       ExpiredGraceRetriesUnmountAfterLateSettlement) {
    const auto request = Request(0, 1024, 1);
    const auto grant = Start(request);
    ASSERT_TRUE(grant);
    ASSERT_TRUE(service_->GracefulUnmountSegment(segment_.id, owner_, 0));
    // The deadline callback must first encounter an outstanding hold. The
    // subsequent explicit call establishes hard close even on a slow runner.
    const auto blocked = service_->UnmountSegment(segment_.id, owner_);
    ASSERT_FALSE(blocked);
    EXPECT_EQ(blocked.error(), ErrorCode::OBJECT_REPLICA_BUSY);
    std::this_thread::sleep_for(std::chrono::milliseconds(150));
    ASSERT_TRUE(SegmentIsMounted());

    const auto final = Finish(request, *grant, PiecePutFinishOutcome::APPLIED);
    ASSERT_TRUE(final);
    EXPECT_NE(final->round_state, PiecePutRoundState::ROUND_PUBLISHED);
    EXPECT_TRUE(WaitUntil([this] { return !SegmentIsMounted(); }));
}

TEST_F(MasterPiecePutAdversarialTest,
       OldGraceDeadlineCannotUnmountAReplacementRegistration) {
    ASSERT_TRUE(service_->GracefulUnmountSegment(segment_.id, owner_, 100));
    ASSERT_TRUE(service_->UnmountSegment(segment_.id, owner_));
    // Replacing a listener instance also proves that the old listener drained.
    ASSERT_TRUE(
        service_->RetirePiecePutEndpoint(owner_, *segment_.tcp_instance));
    segment_.tcp_instance = std::string(32, 'b');
    ASSERT_TRUE(service_->MountSegment(segment_, owner_));
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    EXPECT_TRUE(SegmentIsMounted());

    const auto request = Request(0, 1024, 1);
    const auto grant = Start(request);
    ASSERT_TRUE(grant);
    EXPECT_EQ(grant->tcp_instance, std::string(32, 'b'));
    ASSERT_TRUE(Finish(request, *grant, PiecePutFinishOutcome::NO_POST));
}

TEST_F(MasterPiecePutAdversarialTest,
       TokenFromAnOldMasterCannotCompleteANewRound) {
    const auto request = Request(0, 1024, 1);
    const auto old_grant = Start(request);
    ASSERT_TRUE(old_grant);
    // This is a metadata-only identity test: no TCP write is posted. A real
    // target must drain its old endpoint before mounting with a new Master.
    service_.reset();
    service_ = std::make_unique<MasterService>(config_);
    const auto master_instance = service_->GetPiecePutMasterInstance();
    ASSERT_TRUE(master_instance);
    segment_.piece_put_master_instance = *master_instance;
    segment_.tcp_instance = std::string(32, 'b');
    ASSERT_TRUE(service_->MountSegment(segment_, owner_));
    auto current_request = request;
    current_request.admission = *service_->GetPiecePutAdmission(owner_, 0);
    const auto new_grant = Start(current_request);
    ASSERT_TRUE(new_grant);
    EXPECT_NE(old_grant->token->master_instance,
              new_grant->token->master_instance);
    const auto stale =
        Finish(request, *old_grant, PiecePutFinishOutcome::APPLIED);
    ASSERT_TRUE(stale);
    EXPECT_EQ(stale->round_state, PiecePutRoundState::UNKNOWN);
    ExpectOriginalGrant(current_request, *new_grant);
    const auto final =
        Finish(request, *new_grant, PiecePutFinishOutcome::APPLIED);
    ASSERT_TRUE(final);
    EXPECT_EQ(final->round_state, PiecePutRoundState::ROUND_PUBLISHED);
}

TEST_F(MasterPiecePutAdversarialTest,
       TokenBindsCanonicalDefaultWhenTenantIsolationIsOff) {
    const auto request = Request(0, 1024, 1);
    const auto grant =
        service_->PiecePutStart(owner_, request, TenantId("alias-a"));
    ASSERT_TRUE(grant);
    ASSERT_TRUE(grant->token);
    EXPECT_EQ(grant->token->resolved_tenant, TenantId::kDefaultValue);
    // Both raw RPC contexts resolve to the same namespace when isolation is
    // off.
    const auto final = service_->PiecePutFinish(
        owner_,
        {request.key, *grant->token, request.operation_seq,
         PiecePutFinishOutcome::APPLIED},
        TenantId("alias-b"));
    ASSERT_TRUE(final);
    EXPECT_EQ(final->round_state, PiecePutRoundState::ROUND_PUBLISHED);
    auto lookup = request;
    lookup.lookup_only = true;
    lookup.token = grant->token;
    const auto status =
        service_->PiecePutStart(owner_, lookup, TenantId("alias-c"));
    ASSERT_TRUE(status);
    EXPECT_EQ(status->round_state, PiecePutRoundState::ROUND_PUBLISHED);
}

TEST_F(MasterPiecePutAdversarialTest,
       TokenCannotCrossTenantForCompletedOrActiveGenerationCollisions) {
    service_.reset();
    const auto suffix = generate_uuid();
    const auto policy_path =
        std::filesystem::temp_directory_path() /
        ("piece-put-tenant-" + std::to_string(suffix.first) + "-" +
         std::to_string(suffix.second) + ".yaml");
    struct RemovePolicy {
        std::filesystem::path path;
        ~RemovePolicy() {
            std::error_code ignored;
            std::filesystem::remove(path, ignored);
        }
    } remove_policy{policy_path};
    TenantQuotaPolicySnapshot policy;
    policy.tenant_quotas = {{"tenant-a", 8 * 1024 * 1024},
                            {"tenant-b", 8 * 1024 * 1024}};
    {
        std::ofstream output(policy_path);
        ASSERT_TRUE(output.is_open());
        output << FormatTenantQuotaPolicyYaml(policy);
        ASSERT_TRUE(output.good());
    }
    config_.enable_multi_tenants = true;
    config_.tenant_quota_connector_type = "file";
    config_.tenant_quota_connector_uri = policy_path.string();
    // This local service is destroyed before its policy file.
    MasterService service(config_);
    const auto master_instance = service.GetPiecePutMasterInstance();
    ASSERT_TRUE(master_instance);
    segment_.piece_put_master_instance = *master_instance;
    ASSERT_TRUE(service.MountSegment(segment_, owner_));
    const TenantId tenant_a("tenant-a"), tenant_b("tenant-b");
    auto first = Request(0, 1024, 1);
    first.admission = *service.GetPiecePutAdmission(owner_, 0);
    const auto first_grant = service.PiecePutStart(owner_, first, tenant_a);
    ASSERT_TRUE(first_grant);
    ASSERT_TRUE(first_grant->token);
    ASSERT_EQ(first_grant->token->resolved_tenant, tenant_a.value());
    const auto ordinary =
        service.PutStart(owner_, first.key, tenant_b, 1024, ReplicateConfig{});
    ASSERT_TRUE(ordinary);
    ObjectMeta ordinary_end{first.key, std::nullopt};
    ordinary_end.strict_write_proof = *MakeStrictWriteProof(*ordinary);
    ASSERT_TRUE(
        service.PutEnd(owner_, ordinary_end, tenant_b, ReplicaType::MEMORY));
    // Each namespace starts its generation counter at one. The B record is an
    // ordinary Put, so an A grant must not infer that any B PiecePut published.
    const auto crossed_end = service.PiecePutFinish(
        owner_,
        {first.key, *first_grant->token, first.operation_seq,
         PiecePutFinishOutcome::APPLIED},
        tenant_b);
    ASSERT_TRUE(crossed_end);
    EXPECT_EQ(crossed_end->round_state, PiecePutRoundState::UNKNOWN);
    auto lookup = first;
    lookup.lookup_only = true;
    lookup.token = first_grant->token;
    const auto crossed_lookup = service.PiecePutStart(owner_, lookup, tenant_b);
    ASSERT_TRUE(crossed_lookup);
    EXPECT_EQ(crossed_lookup->round_state, PiecePutRoundState::UNKNOWN);
    EXPECT_TRUE(service.GetReplicaList(first.key, tenant_b));
    ASSERT_TRUE(service.PiecePutFinish(
        owner_,
        {first.key, *first_grant->token, first.operation_seq,
         PiecePutFinishOutcome::NO_POST},
        tenant_a));
    ASSERT_TRUE(service.Remove(first.key, tenant_b, true));

    // Recreate both namespaces at generation two with the same client, nonce
    // and slot. Tenant binding alone must prevent a crossed End settling B.
    auto second = Request(0, 1024, 2);
    second.admission = *service.GetPiecePutAdmission(owner_, 0);
    const auto a = service.PiecePutStart(owner_, second, tenant_a);
    second.admission =
        *service.GetPiecePutAdmission(owner_, second.admission.session_id);
    const auto b = service.PiecePutStart(owner_, second, tenant_b);
    ASSERT_TRUE(a);
    ASSERT_TRUE(b);
    ASSERT_TRUE(a->token);
    ASSERT_TRUE(b->token);
    ASSERT_EQ(a->token->generation, b->token->generation);
    const auto crossed_active =
        service.PiecePutFinish(owner_,
                               {second.key, *a->token, second.operation_seq,
                                PiecePutFinishOutcome::APPLIED},
                               tenant_b);
    ASSERT_TRUE(crossed_active);
    EXPECT_EQ(crossed_active->round_state, PiecePutRoundState::UNKNOWN);
    lookup = second;
    lookup.lookup_only = true;
    lookup.token = b->token;
    const auto retained = service.PiecePutStart(owner_, lookup, tenant_b);
    ASSERT_TRUE(retained);
    EXPECT_EQ(retained->piece_state, PiecePutPieceState::EXPOSED);
    EXPECT_EQ(retained->token, b->token);
    ASSERT_TRUE(
        service.PiecePutFinish(owner_,
                               {second.key, *a->token, second.operation_seq,
                                PiecePutFinishOutcome::NO_POST},
                               tenant_a));
    ASSERT_TRUE(
        service.PiecePutFinish(owner_,
                               {second.key, *b->token, second.operation_seq,
                                PiecePutFinishOutcome::NO_POST},
                               tenant_b));
}

TEST_F(MasterPiecePutAdversarialTest,
       OldMountReplayRejectedByNewMasterBeforeAllocatorRegistration) {
    const auto old_segment = segment_;
    // The old Master accepted this Mount. The target may not have received
    // its reply, while another producer already obtained a PiecePut grant.
    const auto request = Request(0, 1024, 1);
    const auto old_grant = Start(request);
    ASSERT_TRUE(old_grant);
    service_.reset();
    service_ = std::make_unique<MasterService>(config_);
    const auto current_instance = service_->GetPiecePutMasterInstance();
    ASSERT_TRUE(current_instance);
    ASSERT_NE(*old_segment.piece_put_master_instance, *current_instance);

    const auto replay = service_->MountSegment(old_segment, owner_);
    ASSERT_FALSE(replay);
    EXPECT_EQ(replay.error(), ErrorCode::INVALID_WRITE);
    const auto remount = service_->ReMountSegment({old_segment}, owner_);
    ASSERT_FALSE(remount);
    EXPECT_EQ(remount.error(), ErrorCode::INVALID_WRITE);
    const auto details = service_->GetSegmentsDetail();
    ASSERT_TRUE(details);
    EXPECT_TRUE(details->empty());

    auto missing_instance = old_segment;
    missing_instance.piece_put_master_instance = UUID{};
    EXPECT_FALSE(service_->MountSegment(missing_instance, owner_));
    // This is a metadata-only test: a real target must replace/drain the old
    // endpoint and backing before binding a new Master instance.
    segment_.tcp_instance = std::string(32, 'b');
    segment_.piece_put_master_instance = *current_instance;
    ASSERT_TRUE(service_->MountSegment(segment_, owner_));

    MasterServiceConfig disabled_config;
    MasterService disabled(disabled_config);
    EXPECT_FALSE(disabled.GetPiecePutMasterInstance());
    EXPECT_FALSE(disabled.MountSegment(old_segment, owner_));
    EXPECT_FALSE(disabled.ReMountSegment({old_segment}, owner_));
    auto ordinary = old_segment;
    ordinary.tcp_instance = std::string{};
    ASSERT_TRUE(disabled.MountSegment(ordinary, owner_));
}

}  // namespace
}  // namespace mooncake::test
