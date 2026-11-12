#include "master_service.h"
#include "master_service/master_service_test_peer.h"
#include "object_test_helpers.h"

#include <chrono>
#include <future>
#include <memory>
#include <string>
#include <vector>

#include <gtest/gtest.h>

namespace mooncake::test {
namespace {

class MasterPiecePutMaintenanceTest : public ::testing::Test {
   protected:
    void SetUp() override {
        MasterServiceConfig config;
        config.enable_piece_put = true;
        service_ = std::make_unique<MasterService>(config);
        peer_ = std::make_unique<MasterServiceTestPeer>(*service_);
        peer_->StopPiecePutMaintenanceWorkersForTesting();
        segment_.id = generate_uuid();
        segment_.name = "maintenance-target";
        segment_.base = 0x600000000;
        segment_.size = 16 * 1024 * 1024;
        segment_.protocol = "tcp";
        segment_.te_endpoint = "127.0.0.1:19994";
        segment_.tcp_instance = std::string(32, 'm');
        segment_.piece_put_master_instance =
            *service_->GetPiecePutMasterInstance();
        ASSERT_TRUE(service_->MountSegment(segment_, owner_));
    }

    PiecePutStartRequest Request(std::string key, uint64_t sequence) {
        return {std::move(key),
                1024,
                0,
                512,
                "maintenance-round",
                sequence,
                false,
                std::nullopt,
                *service_->GetPiecePutAdmission(owner_, 0)};
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

    void ExpireRound(const std::string& key) {
        auto guard = MasterServiceTestPeer::WriteObject(
            *service_, {TenantId::Default(), key});
        ASSERT_TRUE(guard);
        ASSERT_TRUE(guard->state().piece_put);
        // The irreversible deadline transition without a sleep-based test.
        guard->state().piece_put->state.Expire(PiecePutState::TimePoint::max());
    }

    std::unique_ptr<MasterService> service_;
    std::unique_ptr<MasterServiceTestPeer> peer_;
    const UUID owner_ = generate_uuid();
    Segment segment_;
};

TEST_F(MasterPiecePutMaintenanceTest,
       SettledPieceReclaimsInOneSweepWithoutLockingOrdinaryBacklog) {
    const auto request = Request("settled", 1);
    const auto grant = Start(request);
    ASSERT_TRUE(grant);
    ASSERT_TRUE(Finish(request, *grant, PiecePutFinishOutcome::APPLIED));
    ExpireRound(request.key);
    auto& ns = peer_->GetOrCreateNamespace(TenantId::Default());
    constexpr size_t kOrdinaryKeys = 8192;
    for (size_t i = 0; i < kOrdinaryKeys; ++i) {
        auto guard = ns.objects.WriteKey("ordinary-" + std::to_string(i));
        (void)PublishEnvelope(guard);
        guard.state().is_processing = true;
    }
    ASSERT_EQ(ns.PiecePutInFlightKeys(),
              (std::vector<std::string>{request.key}));

    std::promise<void> entered;
    auto started = entered.get_future();
    std::future<void> swept;
    bool completed_without_ordinary_lock = false;
    {
        auto held = ns.objects.Write("ordinary-0");
        ASSERT_TRUE(held);
        swept = std::async(std::launch::async, [&] {
            entered.set_value();
            peer_->SweepPiecePutRoundsForTesting();
        });
        started.wait();
        // This verifies lock independence, not a production reclamation SLA.
        // A generic in-flight scan blocks on held until this scope ends.
        completed_without_ordinary_lock =
            swept.wait_for(std::chrono::seconds(2)) ==
            std::future_status::ready;
    }
    swept.get();
    EXPECT_TRUE(completed_without_ordinary_lock);
    EXPECT_EQ(peer_->PiecePutRoundCountForTesting(), 0u);
    EXPECT_TRUE(ns.PiecePutInFlightKeys().empty());
    EXPECT_FALSE(ns.objects.Contains(request.key));
    EXPECT_EQ(ns.InFlightKeys().size(), kOrdinaryKeys);
}

TEST_F(MasterPiecePutMaintenanceTest,
       PublishedRoundLeavesThePieceIndexWithoutAStaleEntry) {
    auto request = Request("published", 1);
    request.length = request.total_size;
    const auto grant = Start(request);
    ASSERT_TRUE(grant);
    auto& ns = peer_->GetOrCreateNamespace(TenantId::Default());
    ASSERT_EQ(ns.PiecePutInFlightKeys(),
              (std::vector<std::string>{request.key}));
    const auto finished =
        Finish(request, *grant, PiecePutFinishOutcome::APPLIED);
    ASSERT_TRUE(finished);
    EXPECT_EQ(finished->round_state, PiecePutRoundState::ROUND_PUBLISHED);
    EXPECT_TRUE(ns.PiecePutInFlightKeys().empty());
    EXPECT_EQ(peer_->PiecePutRoundCountForTesting(), 0u);
    EXPECT_TRUE(service_->GetReplicaList(request.key, TenantId::Default()));
}

TEST_F(MasterPiecePutMaintenanceTest,
       ExpiredExposedRoundRemainsIsolatedUntilSettlementOrDrain) {
    const auto request = Request("exposed", 1);
    const auto grant = Start(request);
    ASSERT_TRUE(grant);
    ExpireRound(request.key);
    auto& ns = peer_->GetOrCreateNamespace(TenantId::Default());
    peer_->SweepPiecePutRoundsForTesting();
    EXPECT_EQ(peer_->PiecePutRoundCountForTesting(), 1u);
    EXPECT_EQ(ns.PiecePutInFlightKeys(),
              (std::vector<std::string>{request.key}));
    {
        auto retained = ns.objects.Read(request.key);
        ASSERT_TRUE(retained);
        ASSERT_TRUE(retained->state().piece_put);
        EXPECT_EQ(retained->state().piece_put->state.round_state(),
                  PiecePutRoundState::RETIRING);
        EXPECT_EQ(retained->state().piece_put->state.fragments().front().state,
                  PiecePutPieceState::EXPOSED);
    }

    // Metadata-only proof injection: the real Client API drains its listener
    // before this RPC. Transfer-level drain is covered by integration tests.
    ASSERT_TRUE(
        service_->RetirePiecePutEndpoint(owner_, *segment_.tcp_instance));
    peer_->SweepPiecePutRoundsForTesting();
    EXPECT_EQ(peer_->PiecePutRoundCountForTesting(), 0u);
    EXPECT_TRUE(ns.PiecePutInFlightKeys().empty());
    EXPECT_FALSE(ns.objects.Contains(request.key));
    const auto stale = Finish(request, *grant, PiecePutFinishOutcome::APPLIED);
    ASSERT_TRUE(stale);
    EXPECT_EQ(stale->round_state, PiecePutRoundState::UNKNOWN);
}

}  // namespace
}  // namespace mooncake::test
