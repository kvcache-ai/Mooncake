#include "master_service.h"
#include "piece_put_types.h"
#include <gtest/gtest.h>
#include <algorithm>
#include <future>
#include <random>
#include <thread>

namespace mooncake::test {
class MasterPiecePutTest : public ::testing::Test {
   protected:
    void SetUp() override {
        MasterServiceConfig config;
        config.enable_piece_put = true;
        service = std::make_unique<MasterService>(config);
        owner = generate_uuid();
        segment.id = generate_uuid();
        segment.name = "piece-target";
        segment.base = 0x200000000;
        segment.size = 16 * 1024 * 1024;
        segment.protocol = "tcp";
        segment.te_endpoint = "127.0.0.1:19990";
        segment.tcp_instance = std::string(32, 'a');
        segment.piece_put_master_instance =
            *service->GetPiecePutMasterInstance();
        ASSERT_TRUE(service->MountSegment(segment, owner));
    }
    PiecePutStartRequest Request(uint64_t offset, uint64_t length, uint64_t seq,
                                 std::string write = "round-1") {
        return {"assembled",
                1024,
                offset,
                length,
                std::move(write),
                seq,
                false,
                std::nullopt,
                *service->GetPiecePutAdmission(owner, 0)};
    }
    auto Start(const PiecePutStartRequest& r, UUID caller = {}) {
        return service->PiecePutStart(caller == UUID{} ? owner : caller, r,
                                      TenantId::Default());
    }
    auto Finish(const PiecePutStartRequest& r, const PiecePutStartResponse& g,
                PiecePutFinishOutcome outcome, UUID caller = {}) {
        return service->PiecePutFinish(
            caller == UUID{} ? owner : caller,
            PiecePutFinishRequest{r.key, *g.token, r.operation_seq, outcome},
            TenantId::Default());
    }
    void Hidden() {
        auto exists = service->ExistKey("assembled", TenantId::Default());
        ASSERT_TRUE(exists);
        EXPECT_FALSE(*exists);
        auto get = service->GetReplicaList("assembled", TenantId::Default());
        EXPECT_FALSE(get);
    }
    std::unique_ptr<MasterService> service;
    UUID owner;
    Segment segment;
};

TEST_F(MasterPiecePutTest,
       MountOutcomeDistinguishesRejectionFromBindingFailure) {
    MountSegmentOutcome unknown;
    EXPECT_FALSE(unknown.definitely_not_accepted);
    auto rejected = segment;
    rejected.id = generate_uuid();
    rejected.base = 0;
    auto outcome = service->MountSegmentWithOutcome(rejected, owner);
    EXPECT_EQ(outcome.error, ErrorCode::INVALID_PARAMS);
    EXPECT_TRUE(outcome.definitely_not_accepted);
    EXPECT_FALSE(service->QuerySegmentStatusById(rejected.id));
    rejected = segment;
    rejected.id = generate_uuid();
    rejected.piece_put_master_instance = UUID{};
    outcome = service->MountSegmentWithOutcome(rejected, owner);
    EXPECT_EQ(outcome.error, ErrorCode::INVALID_WRITE);
    EXPECT_TRUE(outcome.definitely_not_accepted);

    // Same-name segments are supported. A later owner's reverse-name entry
    // makes this replay fail in BindTcpInstance after Mount returned ALREADY.
    auto other = segment;
    other.id = generate_uuid();
    other.base += segment.size;
    other.te_endpoint = "127.0.0.1:19991";
    other.tcp_instance = "other-owner-tcp-instance";
    ASSERT_TRUE(service->MountSegment(other, generate_uuid()));
    outcome = service->MountSegmentWithOutcome(segment, owner);
    EXPECT_EQ(outcome.error, ErrorCode::ILLEGAL_CLIENT);
    EXPECT_FALSE(outcome.definitely_not_accepted);
    EXPECT_TRUE(service->QuerySegmentStatusById(segment.id));
}

TEST_F(MasterPiecePutTest, FirstAndRepeatedExactRemountPreserveOpenWrite) {
    const auto first = Request(0, 512, 1);
    const auto grant = Start(first);
    ASSERT_TRUE(grant);
    ASSERT_TRUE(grant->replica);
    ASSERT_TRUE(grant->token);
    ASSERT_TRUE(service->ReMountSegment({segment}, owner));
    ASSERT_TRUE(service->ReMountSegment({segment}, owner));
    auto retry = Start(first);
    ASSERT_TRUE(retry);
    EXPECT_EQ(retry->token, grant->token);
    ASSERT_TRUE(retry->replica);
    EXPECT_EQ(retry->replica->get_memory_descriptor()
                  .buffer_descriptor.buffer_address_,
              grant->replica->get_memory_descriptor()
                  .buffer_descriptor.buffer_address_);
    auto changed = segment;
    changed.host_id = "different-host";
    EXPECT_FALSE(service->ReMountSegment({changed}, owner));
    ASSERT_TRUE(Finish(first, *grant, PiecePutFinishOutcome::APPLIED));
    const auto last = Request(512, 512, 2);
    auto tail = Start(last);
    ASSERT_TRUE(tail);
    auto done = Finish(last, *tail, PiecePutFinishOutcome::APPLIED);
    ASSERT_TRUE(done);
    EXPECT_EQ(done->round_state, PiecePutRoundState::ROUND_PUBLISHED);
}

TEST_F(MasterPiecePutTest, RandomArrivalAndFinishPublishesOnlyFullCoverage) {
    std::vector<PiecePutStartRequest> requests;
    for (uint64_t i = 0; i < 16; ++i)
        requests.push_back(Request(i * 64, 64, i + 1));
    std::mt19937 random(20261107);
    std::shuffle(requests.begin(), requests.end(), random);
    std::vector<PiecePutStartResponse> grants;
    for (const auto& request : requests) {
        auto g = Start(request);
        ASSERT_TRUE(g);
        ASSERT_TRUE(g->replica);
        ASSERT_TRUE(g->token);
        grants.push_back(*g);
    }
    Hidden();
    for (size_t i = 0; i < requests.size(); ++i) {
        auto end =
            Finish(requests[i], grants[i], PiecePutFinishOutcome::APPLIED);
        ASSERT_TRUE(end);
        if (i + 1 < requests.size())
            Hidden();
        else
            EXPECT_EQ(end->round_state, PiecePutRoundState::ROUND_PUBLISHED);
    }
    auto read = service->GetReplicaList("assembled", TenantId::Default());
    ASSERT_TRUE(read);
    ASSERT_EQ(read->replicas.size(), 1);
    EXPECT_EQ(read->replicas[0].get_memory_descriptor().buffer_descriptor.size_,
              1024);
    auto duplicate =
        Finish(requests[0], grants[0], PiecePutFinishOutcome::APPLIED);
    ASSERT_TRUE(duplicate);
    EXPECT_EQ(duplicate->round_state, PiecePutRoundState::ROUND_PUBLISHED);
}

TEST_F(MasterPiecePutTest, RetryPreservesGrantAndRejectsWrongBatchAndWriter) {
    const auto r = Request(0, 512, 1);
    auto g = Start(r);
    ASSERT_TRUE(g);
    auto retry = Start(r);
    ASSERT_TRUE(retry);
    EXPECT_EQ(retry->token, g->token);
    auto wrong = Request(512, 512, 3, "different-computation");
    EXPECT_FALSE(Start(wrong));
    EXPECT_FALSE(
        Finish(r, *g, PiecePutFinishOutcome::APPLIED, generate_uuid()));
    auto applied = Finish(r, *g, PiecePutFinishOutcome::APPLIED);
    ASSERT_TRUE(applied);
    auto again = Start(r);
    ASSERT_TRUE(again);
    EXPECT_EQ(again->piece_state, PiecePutPieceState::APPLIED);
    EXPECT_FALSE(again->replica);
    // A delayed uncertain outcome does not undo a definitive ACK.
    ASSERT_TRUE(Finish(r, *g, PiecePutFinishOutcome::UNCERTAIN));
    Hidden();
    const auto last = Request(512, 512, 2);
    auto h = Start(last);
    ASSERT_TRUE(h);
    auto done = Finish(last, *h, PiecePutFinishOutcome::APPLIED);
    ASSERT_TRUE(done);
    EXPECT_EQ(done->round_state, PiecePutRoundState::ROUND_PUBLISHED);
}

TEST_F(MasterPiecePutTest, OrdinaryWritersCannotPublishOrReplaceAnOpenRound) {
    const auto r = Request(0, 512, 1);
    auto g = Start(r);
    ASSERT_TRUE(g);
    auto put = service->PutStart(owner, "assembled", TenantId::Default(), 1024,
                                 ReplicateConfig{});
    EXPECT_FALSE(put);
    EXPECT_FALSE(service->PutEnd(owner, ObjectMeta{"assembled", std::nullopt},
                                 TenantId::Default(), ReplicaType::MEMORY));
    EXPECT_FALSE(service->PutRevoke(owner, "assembled", TenantId::Default(),
                                    ReplicaType::MEMORY));
    EXPECT_FALSE(service->Remove("assembled", TenantId::Default(), true));
    Hidden();
    auto retry = Start(r);
    ASSERT_TRUE(retry);
    EXPECT_EQ(retry->token, g->token);
}

TEST_F(MasterPiecePutTest, UncertainWriteBlocksReuseUntilEndpointDrainProof) {
    const auto r = Request(0, 512, 1);
    auto g = Start(r);
    ASSERT_TRUE(g);
    ASSERT_TRUE(Finish(r, *g, PiecePutFinishOutcome::UNCERTAIN));
    Hidden();
    auto lookup = r;
    lookup.lookup_only = true;
    auto cleanup = Start(lookup);
    ASSERT_TRUE(cleanup);
    EXPECT_FALSE(cleanup->replica);
    EXPECT_EQ(cleanup->token, g->token);
    EXPECT_FALSE(Start(Request(0, 1024, 2, "next-round")));
    auto unmount = service->UnmountSegment(segment.id, owner);
    EXPECT_FALSE(unmount);
    // Retiring an unrelated endpoint fences its own delayed Mount only; it
    // cannot settle the current allocation. The exact endpoint owner is
    // checked.
    ASSERT_TRUE(service->RetirePiecePutEndpoint(owner, "wrong-instance"));
    EXPECT_FALSE(service->UnmountSegment(segment.id, owner));
    EXPECT_FALSE(
        service->RetirePiecePutEndpoint(generate_uuid(), std::string(32, 'a')));
    // This unit test supplies the lower-layer proof; end-to-end tests obtain
    // it from the actual TCP endpoint's synchronous drain.
    ASSERT_TRUE(service->RetirePiecePutEndpoint(owner, std::string(32, 'a')));
    ASSERT_TRUE(service->RetirePiecePutEndpoint(owner, std::string(32, 'a')));
    bool unmounted = false;
    for (int i = 0; i < 100; ++i) {
        if (service->UnmountSegment(segment.id, owner)) {
            unmounted = true;
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    EXPECT_TRUE(unmounted);
    Hidden();
}

TEST_F(MasterPiecePutTest, MissingStartLookupDoesNotCreateOrExposeAnAddress) {
    auto r = Request(0, 1024, 1);
    r.lookup_only = true;
    auto lookup = Start(r);
    ASSERT_TRUE(lookup);
    EXPECT_EQ(lookup->round_state, PiecePutRoundState::UNKNOWN);
    EXPECT_FALSE(lookup->replica);
    r.lookup_only = false;
    auto g = Start(r);
    ASSERT_TRUE(g);
    auto no_post = Finish(r, *g, PiecePutFinishOutcome::NO_POST);
    ASSERT_TRUE(no_post);
    // The failed physical round must be erased, never mistaken for publication.
    auto stale = Finish(r, *g, PiecePutFinishOutcome::APPLIED);
    ASSERT_TRUE(stale);
    EXPECT_EQ(stale->round_state, PiecePutRoundState::UNKNOWN);
    Hidden();
}

TEST_F(MasterPiecePutTest,
       GraceAllowsExistingRoundToFinishWhileBlockingNewObjects) {
    const auto first = Request(0, 512, 1);
    auto g = Start(first);
    ASSERT_TRUE(g);
    ASSERT_TRUE(Finish(first, *g, PiecePutFinishOutcome::APPLIED));
    ASSERT_TRUE(service->GracefulUnmountSegment(segment.id, owner, 5000));
    auto new_object = Request(0, 1024, 3);
    new_object.key = "new-object";
    EXPECT_FALSE(Start(new_object));
    const auto last = Request(512, 512, 2);
    auto h = Start(last);
    ASSERT_TRUE(h);
    ASSERT_TRUE(h->replica);
    auto result = Finish(last, *h, PiecePutFinishOutcome::APPLIED);
    ASSERT_TRUE(result);
    EXPECT_EQ(result->round_state, PiecePutRoundState::ROUND_PUBLISHED);
    ASSERT_TRUE(service->GetReplicaList("assembled", TenantId::Default()));
}

TEST_F(MasterPiecePutTest, AllocationFailureLeavesNoPartiallyInitializedRound) {
    auto too_large = Request(0, 32 * 1024 * 1024, 1);
    too_large.total_size = too_large.length;
    EXPECT_FALSE(Start(too_large));
    auto small = Request(0, 1024, 2);
    auto g = Start(small);
    ASSERT_TRUE(g);
    auto result = Finish(small, *g, PiecePutFinishOutcome::APPLIED);
    ASSERT_TRUE(result);
    EXPECT_EQ(result->round_state, PiecePutRoundState::ROUND_PUBLISHED);
}
TEST_F(MasterPiecePutTest, CloseBeforeStartFencesDelayedReservation) {
    const auto request = Request(0, 1024, 1);
    auto close = request;
    close.close_no_post = true;
    const auto closed = Start(close);
    ASSERT_TRUE(closed);
    EXPECT_TRUE(closed->admission_closed);
    EXPECT_FALSE(closed->token);
    EXPECT_FALSE(closed->replica);
    const auto delayed = Start(request);
    ASSERT_TRUE(delayed);
    EXPECT_TRUE(delayed->admission_closed);
    EXPECT_FALSE(delayed->token);
    EXPECT_FALSE(delayed->replica);
    Hidden();
    const auto fresh = Start(Request(0, 1024, 2));
    ASSERT_TRUE(fresh);
    EXPECT_TRUE(fresh->replica);
}

TEST_F(MasterPiecePutTest, CloseAfterLookupFencesMissingRequest) {
    const auto request = Request(0, 1024, 1);
    auto lookup = request;
    lookup.lookup_only = true;
    const auto missing = Start(lookup);
    ASSERT_TRUE(missing);
    EXPECT_FALSE(missing->admission_closed);
    lookup.close_no_post = true;
    const auto closed = Start(lookup);
    ASSERT_TRUE(closed);
    EXPECT_TRUE(closed->admission_closed);
    const auto delayed = Start(request);
    ASSERT_TRUE(delayed);
    EXPECT_TRUE(delayed->admission_closed);
    EXPECT_FALSE(delayed->replica);
    Hidden();
}

TEST_F(MasterPiecePutTest, RacingCloseAndStartCannotLeaveAFutureGrant) {
    // Either linearization is valid: close consumes the bit first, or it finds
    // and withdraws the grant issued by Start. After close returns no grant can
    // remain EXPOSED or appear later. No physical writes are posted in this
    // test.
    for (uint64_t sequence = 1; sequence <= 32; ++sequence) {
        const auto request = Request(0, 1024, sequence);
        auto close = request;
        close.close_no_post = true;
        std::promise<void> begin;
        auto ready = begin.get_future().share();
        auto starting = std::async(std::launch::async, [&] {
            ready.wait();
            return Start(request);
        });
        auto closing = std::async(std::launch::async, [&] {
            ready.wait();
            return Start(close);
        });
        begin.set_value();
        const auto closed = closing.get();
        ASSERT_TRUE(closed);
        EXPECT_TRUE(closed->admission_closed);
        const auto late = Start(request);
        ASSERT_TRUE(late);
        EXPECT_TRUE(late->admission_closed);
        EXPECT_FALSE(late->replica);
        const auto started = starting.get();
        ASSERT_TRUE(started);
        auto lookup = request;
        lookup.lookup_only = true;
        const auto missing = Start(lookup);
        ASSERT_TRUE(missing);
        EXPECT_FALSE(missing->replica);
        EXPECT_FALSE(missing->token);
    }
    Hidden();
}

TEST_F(MasterPiecePutTest, SuccessfulRemovedObjectCannotBeRecreatedByReplay) {
    const auto request = Request(0, 1024, 1);
    const auto grant = Start(request);
    ASSERT_TRUE(grant);
    ASSERT_TRUE(Finish(request, *grant, PiecePutFinishOutcome::APPLIED));
    ASSERT_TRUE(service->Remove(request.key, TenantId::Default(), true));
    const auto replay = Start(request);
    ASSERT_TRUE(replay);
    EXPECT_TRUE(replay->admission_closed);
    EXPECT_FALSE(replay->replica);
    EXPECT_FALSE(replay->token);
    Hidden();
}

TEST_F(MasterPiecePutTest, NoPostReclaimKeepsTheNonceSpent) {
    const auto request = Request(0, 1024, 1);
    const auto grant = Start(request);
    ASSERT_TRUE(grant);
    ASSERT_TRUE(Finish(request, *grant, PiecePutFinishOutcome::NO_POST));
    const auto replay = Start(request);
    ASSERT_TRUE(replay);
    EXPECT_TRUE(replay->admission_closed);
    EXPECT_FALSE(replay->replica);
    auto changed_key = request;
    changed_key.key = "other-object";
    const auto crossed = Start(changed_key);
    ASSERT_TRUE(crossed);
    EXPECT_TRUE(crossed->admission_closed);
    EXPECT_FALSE(crossed->replica);
    Hidden();
}

TEST_F(MasterPiecePutTest, SessionRotationFencesOldStartsButAllowsSettlement) {
    const auto first = Request(0, 512, 1);
    const auto grant = Start(first);
    ASSERT_TRUE(grant);
    const auto next =
        service->GetPiecePutAdmission(owner, first.admission.session_id);
    ASSERT_TRUE(next);
    EXPECT_NE(next->session_id, first.admission.session_id);
    const auto stale = Start(first);
    ASSERT_TRUE(stale);
    EXPECT_TRUE(stale->admission_closed);
    EXPECT_FALSE(stale->replica);
    // The same per-window nonce is a different operation in the new session.
    const auto second = Request(512, 512, 1);
    ASSERT_EQ(second.admission.session_id, next->session_id);
    const auto other = Start(second);
    ASSERT_TRUE(other);
    ASSERT_TRUE(other->replica);
    auto lookup = first;
    lookup.lookup_only = true;
    lookup.token = grant->token;
    const auto original = Start(lookup);
    ASSERT_TRUE(original);
    EXPECT_EQ(original->token, grant->token);
    ASSERT_TRUE(Finish(first, *grant, PiecePutFinishOutcome::APPLIED));
    const auto published =
        Finish(second, *other, PiecePutFinishOutcome::APPLIED);
    ASSERT_TRUE(published);
    EXPECT_EQ(published->round_state, PiecePutRoundState::ROUND_PUBLISHED);
}

TEST_F(MasterPiecePutTest, OldSessionCloseSettlesOnlyOriginalGrant) {
    const auto first = Request(0, 512, 1);
    const auto grant = Start(first);
    ASSERT_TRUE(grant);
    ASSERT_TRUE(
        service->GetPiecePutAdmission(owner, first.admission.session_id));
    const auto second = Request(512, 512, 1);
    const auto other = Start(second);
    ASSERT_TRUE(other);
    auto close = first;
    close.close_no_post = true;
    const auto closed = Start(close);
    ASSERT_TRUE(closed);
    EXPECT_TRUE(closed->admission_closed);
    EXPECT_EQ(closed->piece_state, PiecePutPieceState::WITHDRAWN);
    auto lookup = second;
    lookup.lookup_only = true;
    const auto pending = Start(lookup);
    ASSERT_TRUE(pending);
    EXPECT_EQ(pending->piece_state, PiecePutPieceState::EXPOSED);
    EXPECT_EQ(pending->token, other->token);
    ASSERT_TRUE(Finish(second, *other, PiecePutFinishOutcome::NO_POST));
}

TEST_F(MasterPiecePutTest,
       CloseCannotUndoAppliedAndRotationCannotDrainUncertain) {
    const auto first = Request(0, 512, 1);
    const auto grant = Start(first);
    ASSERT_TRUE(grant);
    ASSERT_TRUE(Finish(first, *grant, PiecePutFinishOutcome::APPLIED));
    auto close = first;
    close.close_no_post = true;
    const auto applied = Start(close);
    ASSERT_TRUE(applied);
    EXPECT_EQ(applied->piece_state, PiecePutPieceState::APPLIED);
    EXPECT_EQ(applied->round_state, PiecePutRoundState::OPEN);
    const auto second = Request(512, 512, 2);
    const auto other = Start(second);
    ASSERT_TRUE(other);
    ASSERT_TRUE(Finish(second, *other, PiecePutFinishOutcome::UNCERTAIN));
    ASSERT_TRUE(
        service->GetPiecePutAdmission(owner, first.admission.session_id));
    auto lookup = second;
    lookup.lookup_only = true;
    lookup.token = other->token;
    const auto retained = Start(lookup);
    ASSERT_TRUE(retained);
    EXPECT_EQ(retained->piece_state, PiecePutPieceState::EXPOSED);
    EXPECT_EQ(retained->round_state, PiecePutRoundState::RETIRING);
    EXPECT_FALSE(service->Remove(first.key, TenantId::Default(), true));
}

TEST_F(MasterPiecePutTest,
       WrongMasterOrSessionNeverGrantsOrSettlesActiveClaim) {
    const auto request = Request(0, 1024, 1);
    auto wrong = request;
    wrong.admission.master_instance = generate_uuid();
    EXPECT_FALSE(Start(wrong));
    wrong = request;
    wrong.admission.session_id = 0;
    EXPECT_FALSE(Start(wrong));
    wrong = request;
    wrong.admission.session_id += 1;
    EXPECT_FALSE(Start(wrong));
    const auto grant = Start(request);
    ASSERT_TRUE(grant);
    auto token = *grant->token;
    ++token.admission_session_id;
    const auto bad =
        service->PiecePutFinish(owner,
                                {request.key, token, request.operation_seq,
                                 PiecePutFinishOutcome::NO_POST},
                                TenantId::Default());
    EXPECT_FALSE(bad);
    auto lookup = request;
    lookup.lookup_only = true;
    lookup.token = grant->token;
    const auto retained = Start(lookup);
    ASSERT_TRUE(retained);
    EXPECT_EQ(retained->piece_state, PiecePutPieceState::EXPOSED);
    ASSERT_TRUE(Finish(request, *grant, PiecePutFinishOutcome::NO_POST));
}

TEST(PiecePutGateTest, DefaultMasterDoesNotEnablePiecePut) {
    MasterServiceConfig config;
    MasterService service(config);
    auto result = service.PiecePutStart(
        generate_uuid(), {"key", 16, 0, 16, "task", 1, false, std::nullopt},
        TenantId::Default());
    EXPECT_FALSE(result);
}
}  // namespace mooncake::test

namespace mooncake::test {
TEST_F(MasterPiecePutTest,
       OldMasterNoPostClosureIsDefinitiveWithoutAnAllocation) {
    auto old = Request(0, 1024, 1);
    old.admission.master_instance = generate_uuid();
    EXPECT_FALSE(Start(old));
    old.lookup_only = true;
    old.close_no_post = true;
    const auto closed = Start(old);
    ASSERT_TRUE(closed);
    EXPECT_TRUE(closed->admission_closed);
    EXPECT_FALSE(closed->token);
    EXPECT_FALSE(closed->replica);
    EXPECT_EQ(service->GetStorageUsageSnapshot().memory.used_bytes, 0u);
    auto current = Request(0, 1024, 1);
    const auto grant = Start(current);
    ASSERT_TRUE(grant);
    EXPECT_TRUE(grant->replica);
    EXPECT_TRUE(Finish(current, *grant, PiecePutFinishOutcome::NO_POST));
}
}  // namespace mooncake::test
