#include <gtest/gtest.h>
#include <algorithm>
#include <array>
#include <barrier>
#include <chrono>
#include <future>
#include <thread>
#include <vector>
#include "master_service_test_fixture.h"
#include "piece_put.h"

namespace mooncake::test {
namespace {
PiecePutStartRequest Piece(std::string key, uint64_t offset, uint64_t op,
                           uint64_t total = 12288, uint64_t length = 4096) {
    PiecePutStartRequest request;
    request.key = std::move(key);
    request.total_size = total;
    request.offset = offset;
    request.length = length;
    request.write_id = "request-42-attempt-1";
    request.op_id = op;
    return request;
}
}  // namespace
class PiecePutMasterTest : public MasterServiceTest {};

TEST_F(PiecePutMasterTest, OutOfOrderCoverageIsHiddenUntilEveryPieceFinishes) {
    MasterService service;
    const auto target = PrepareSimpleSegment(service);
    const UUID other = generate_uuid();
    auto middle = service.PiecePutStart(target.client_id, Piece("kv", 4096, 1),
                                        TenantId::Default());
    auto last =
        service.PiecePutStart(other, Piece("kv", 8192, 2), TenantId::Default());
    auto first = service.PiecePutStart(target.client_id, Piece("kv", 0, 3),
                                       TenantId::Default());
    ASSERT_TRUE(middle);
    ASSERT_TRUE(last);
    ASSERT_TRUE(first);
    ASSERT_EQ(middle->replicas.size(), first->replicas.size());
    for (size_t i = 0; i < middle->replicas.size(); ++i)
        EXPECT_EQ(middle->replicas[i].id, first->replicas[i].id);
    ExpectKeyHiddenFromReadApis(service, "kv");
    auto done = service.PiecePutFinish(other, {last->handle, true},
                                       TenantId::Default());
    ASSERT_TRUE(done);
    EXPECT_TRUE(done->piece_completed);
    EXPECT_FALSE(done->object_published);
    ASSERT_TRUE(service.PiecePutFinish(target.client_id, {middle->handle, true},
                                       TenantId::Default()));
    ExpectKeyHiddenFromReadApis(service, "kv");
    done = service.PiecePutFinish(target.client_id, {first->handle, true},
                                  TenantId::Default());
    ASSERT_TRUE(done);
    EXPECT_TRUE(done->object_published);
    ASSERT_TRUE(service.GetReplicaList("kv", TenantId::Default()));
    auto replay = service.PiecePutFinish(
        target.client_id, {first->handle, true}, TenantId::Default());
    ASSERT_TRUE(replay);
    EXPECT_TRUE(replay->object_published);
    EXPECT_TRUE(service.PiecePutFinish(target.client_id, {first->handle, false},
                                       TenantId::Default()));
    EXPECT_TRUE(service.ExistKey("kv", TenantId::Default()).value());
}

TEST_F(PiecePutMasterTest, StartReplayKeepsAddressAndRejectsChangedGeometry) {
    MasterService service;
    const auto target = PrepareSimpleSegment(service);
    auto request = Piece("replay", 0, 7);
    auto original =
        service.PiecePutStart(target.client_id, request, TenantId::Default());
    ASSERT_TRUE(original);
    auto replay =
        service.PiecePutStart(target.client_id, request, TenantId::Default());
    ASSERT_TRUE(replay);
    ASSERT_EQ(replay->replicas.size(), original->replicas.size());
    for (size_t i = 0; i < replay->replicas.size(); ++i)
        EXPECT_EQ(replay->replicas[i].id, original->replicas[i].id);
    EXPECT_EQ(replay->handle.generation, original->handle.generation);
    request.offset = 4096;
    EXPECT_FALSE(
        service.PiecePutStart(target.client_id, request, TenantId::Default()));
    request = Piece("replay", 0, 8);
    EXPECT_FALSE(
        service.PiecePutStart(target.client_id, request, TenantId::Default()));
    request = Piece("replay", 4096, 9);
    request.write_id = "another-attempt";
    EXPECT_FALSE(
        service.PiecePutStart(target.client_id, request, TenantId::Default()));
}

TEST_F(PiecePutMasterTest, GapAndCompletedDuplicateDoNotOvercount) {
    MasterService service;
    const auto target = PrepareSimpleSegment(service);
    auto first = service.PiecePutStart(target.client_id, Piece("gap", 0, 1),
                                       TenantId::Default());
    auto last = service.PiecePutStart(target.client_id, Piece("gap", 8192, 2),
                                      TenantId::Default());
    ASSERT_TRUE(first);
    ASSERT_TRUE(last);
    for (int i = 0; i < 3; ++i)
        ASSERT_TRUE(service.PiecePutFinish(
            target.client_id, {first->handle, true}, TenantId::Default()));
    ASSERT_TRUE(service.PiecePutFinish(target.client_id, {last->handle, true},
                                       TenantId::Default()));
    ExpectKeyHiddenFromReadApis(service, "gap");
    auto middle = service.PiecePutStart(target.client_id, Piece("gap", 4096, 3),
                                        TenantId::Default());
    ASSERT_TRUE(middle);
    auto done = service.PiecePutFinish(target.client_id, {middle->handle, true},
                                       TenantId::Default());
    ASSERT_TRUE(done);
    EXPECT_TRUE(done->object_published);
}

TEST_F(PiecePutMasterTest, WrongProducerAndOldMasterCannotFinish) {
    MasterService service;
    const auto target = PrepareSimpleSegment(service);
    auto start = service.PiecePutStart(
        target.client_id, Piece("identity", 0, 1, 4096), TenantId::Default());
    ASSERT_TRUE(start);
    EXPECT_FALSE(service.PiecePutFinish(generate_uuid(), {start->handle, true},
                                        TenantId::Default()));
    auto stale = start->handle;
    stale.master_epoch = generate_uuid();
    EXPECT_FALSE(service.PiecePutFinish(target.client_id, {stale, true},
                                        TenantId::Default()));
    ExpectKeyHiddenFromReadApis(service, "identity");
    EXPECT_TRUE(service.PiecePutFinish(target.client_id, {start->handle, true},
                                       TenantId::Default()));
}

TEST_F(PiecePutMasterTest,
       OrdinaryEndAndRevokeCannotPublishOrFreeAnActiveRound) {
    MasterService service;
    const auto target = PrepareSimpleSegment(service);
    auto start = service.PiecePutStart(
        target.client_id, Piece("protected", 0, 1, 4096), TenantId::Default());
    ASSERT_TRUE(start);
    EXPECT_FALSE(service.PutEnd(target.client_id, "protected",
                                TenantId::Default(), ReplicaType::ALL));
    EXPECT_FALSE(service.PutRevoke(target.client_id, "protected",
                                   TenantId::Default(), ReplicaType::ALL));
    ExpectKeyHiddenFromReadApis(service, "protected");
    auto done = service.PiecePutFinish(target.client_id, {start->handle, true},
                                       TenantId::Default());
    ASSERT_TRUE(done);
    EXPECT_TRUE(done->object_published);
}

TEST_F(PiecePutMasterTest, UpsertPreemptionRejectsTheOldFinish) {
    MasterService service;
    const auto target = PrepareSimpleSegment(service);
    auto start = service.PiecePutStart(target.client_id, Piece("replace", 0, 1),
                                       TenantId::Default());
    ASSERT_TRUE(start);
    ASSERT_TRUE(service.UpsertStart(target.client_id, "replace",
                                    TenantId::Default(), 4096, {}));
    EXPECT_FALSE(service.PiecePutFinish(target.client_id, {start->handle, true},
                                        TenantId::Default()));
    ASSERT_TRUE(service.UpsertEnd(target.client_id, "replace",
                                  TenantId::Default(), ReplicaType::ALL));
    EXPECT_TRUE(service.ExistKey("replace", TenantId::Default()).value());
}

TEST_F(PiecePutMasterTest, FailedPieceAbortsWholeRoundAndDoesNotHarmOtherKeys) {
    MasterService service;
    const auto target = PrepareSimpleSegment(service);
    auto first = service.PiecePutStart(target.client_id, Piece("failed", 0, 1),
                                       TenantId::Default());
    auto second = service.PiecePutStart(
        target.client_id, Piece("failed", 4096, 2), TenantId::Default());
    ASSERT_TRUE(first);
    ASSERT_TRUE(second);
    PutCompletedObject(service, target.client_id, "stable", {});
    ASSERT_TRUE(service.PiecePutFinish(target.client_id, {first->handle, true},
                                       TenantId::Default()));
    EXPECT_FALSE(service.PiecePutFinish(
        target.client_id, {second->handle, false}, TenantId::Default()));
    EXPECT_FALSE(service.PiecePutFinish(target.client_id, {first->handle, true},
                                        TenantId::Default()));
    EXPECT_FALSE(service.ExistKey("failed", TenantId::Default()).value());
    EXPECT_TRUE(service.ExistKey("stable", TenantId::Default()).value());
}

TEST_F(PiecePutMasterTest, LateFailureCannotUndoAnAlreadyConfirmedPiece) {
    MasterService service;
    const auto target = PrepareSimpleSegment(service);
    auto first = service.PiecePutStart(target.client_id,
                                       Piece("late-failure", 0, 1, 8192),
                                       TenantId::Default());
    auto second = service.PiecePutStart(target.client_id,
                                        Piece("late-failure", 4096, 2, 8192),
                                        TenantId::Default());
    ASSERT_TRUE(first);
    ASSERT_TRUE(second);
    ASSERT_TRUE(service.PiecePutFinish(target.client_id, {first->handle, true},
                                       TenantId::Default()));
    ASSERT_TRUE(service.PiecePutFinish(target.client_id, {first->handle, false},
                                       TenantId::Default()));
    auto done = service.PiecePutFinish(target.client_id, {second->handle, true},
                                       TenantId::Default());
    ASSERT_TRUE(done);
    EXPECT_TRUE(done->object_published);
}

TEST_F(PiecePutMasterTest, DeadlineIsCheckedWithoutDependingOnSweepOrder) {
    MasterServiceConfig config;
    config.put_start_discard_timeout_sec = 1;
    MasterService service(config);
    // Stop only the existing expiry driver so this tests the API deadline
    // check.
    MasterServiceTestPeer::EvictionRunning(service).store(false);
    if (MasterServiceTestPeer::EvictionThread(service).joinable())
        MasterServiceTestPeer::EvictionThread(service).join();
    const auto target = PrepareSimpleSegment(service);
    auto start = service.PiecePutStart(
        target.client_id, Piece("expired", 0, 1, 4096), TenantId::Default());
    ASSERT_TRUE(start);
    std::this_thread::sleep_for(std::chrono::milliseconds(1100));
    EXPECT_FALSE(service.PiecePutFinish(target.client_id, {start->handle, true},
                                        TenantId::Default()));
    EXPECT_FALSE(service.ExistKey("expired", TenantId::Default()).value());
    auto next = Piece("expired", 0, 2, 4096);
    next.write_id = "attempt-2";
    EXPECT_TRUE(
        service.PiecePutStart(target.client_id, next, TenantId::Default()));
}

TEST_F(PiecePutMasterTest, MultipleReplicasPublishTogether) {
    MasterService service;
    const auto a = PrepareSimpleSegment(service, "a", kDefaultSegmentBase,
                                        kDefaultSegmentSize, "host-a");
    PrepareSimpleSegment(service, "b",
                         kDefaultSegmentBase + kDefaultSegmentSize,
                         kDefaultSegmentSize, "host-b");
    auto request = Piece("replicated", 0, 1, 8192);
    request.config.replica_num = 2;
    auto first =
        service.PiecePutStart(a.client_id, request, TenantId::Default());
    request.offset = 4096;
    request.op_id = 2;
    auto second =
        service.PiecePutStart(a.client_id, request, TenantId::Default());
    ASSERT_TRUE(first);
    ASSERT_TRUE(second);
    ASSERT_EQ(first->replicas.size(), 2u);
    ASSERT_TRUE(service.PiecePutFinish(a.client_id, {first->handle, true},
                                       TenantId::Default()));
    ExpectKeyHiddenFromReadApis(service, "replicated");
    ASSERT_TRUE(service.PiecePutFinish(a.client_id, {second->handle, true},
                                       TenantId::Default()));
    auto read = service.GetReplicaList("replicated", TenantId::Default());
    ASSERT_TRUE(read);
    EXPECT_EQ(read->replicas.size(), 2u);
}

TEST_F(PiecePutMasterTest,
       ConcurrentProducersAndIndependentKeysSettleExactlyOnce) {
    MasterService service;
    PrepareSimpleSegment(service);
    constexpr size_t kProducers = 4, kKeys = 8;
    std::barrier gate(kProducers);
    std::vector<std::future<bool>> producers;
    for (size_t rank = 0; rank < kProducers; ++rank) {
        producers.push_back(std::async(std::launch::async, [&, rank] {
            const auto client = generate_uuid();
            gate.arrive_and_wait();
            for (size_t key = 0; key < kKeys; ++key) {
                auto request = Piece("parallel-" + std::to_string(key),
                                     rank * 4096, key + 1, kProducers * 4096);
                auto start =
                    service.PiecePutStart(client, request, TenantId::Default());
                if (!start ||
                    !service.PiecePutFinish(client, {start->handle, true},
                                            TenantId::Default()))
                    return false;
            }
            return true;
        }));
    }
    for (auto& future : producers) EXPECT_TRUE(future.get());
    for (size_t key = 0; key < kKeys; ++key)
        EXPECT_TRUE(service
                        .ExistKey("parallel-" + std::to_string(key),
                                  TenantId::Default())
                        .value());
}

TEST_F(PiecePutMasterTest,
       InvalidRangesAndDirectDiskAreRejectedBeforePublishing) {
    MasterService service;
    const auto target = PrepareSimpleSegment(service);
    auto request = Piece("invalid", UINT64_MAX, 1);
    EXPECT_FALSE(
        service.PiecePutStart(target.client_id, request, TenantId::Default()));
    request = Piece("invalid", 0, 2);
    request.config.dfs_replica_num = 1;
    EXPECT_FALSE(
        service.PiecePutStart(target.client_id, request, TenantId::Default()));
    request = Piece("invalid", 0, 3);
    request.config.prefer_alloc_in_same_node = true;
    EXPECT_FALSE(
        service.PiecePutStart(target.client_id, request, TenantId::Default()));
    EXPECT_FALSE(service.ExistKey("invalid", TenantId::Default()).value());
}

TEST_F(PiecePutMasterTest, OldCommitIsRejectedAfterSameSizeInPlaceUpsert) {
    MasterService service;
    const auto target = PrepareSimpleSegment(service);
    auto start = service.PiecePutStart(
        target.client_id, Piece("same-size", 0, 1, 4096), TenantId::Default());
    ASSERT_TRUE(start);
    ASSERT_TRUE(service.PiecePutFinish(target.client_id, {start->handle, true},
                                       TenantId::Default()));
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
    ASSERT_TRUE(service.UpsertStart(target.client_id, "same-size",
                                    TenantId::Default(), 4096, {}));
    ASSERT_TRUE(service.UpsertEnd(target.client_id, "same-size",
                                  TenantId::Default(), ReplicaType::ALL));
    EXPECT_FALSE(service.PiecePutFinish(target.client_id, {start->handle, true},
                                        TenantId::Default()));
}

TEST_F(PiecePutMasterTest, LosingOneFixedReplicaAbortsTheWholeAssembly) {
    MasterService service;
    const auto a = PrepareSimpleSegment(service, "left", kDefaultSegmentBase,
                                        kDefaultSegmentSize, "host-left");
    const auto b = PrepareSimpleSegment(
        service, "right", kDefaultSegmentBase + kDefaultSegmentSize,
        kDefaultSegmentSize, "host-right");
    auto request = Piece("target-exit", 0, 1, 8192);
    request.config.replica_num = 2;
    auto first =
        service.PiecePutStart(a.client_id, request, TenantId::Default());
    request.offset = 4096;
    request.op_id = 2;
    auto second =
        service.PiecePutStart(a.client_id, request, TenantId::Default());
    ASSERT_TRUE(first);
    ASSERT_TRUE(second);
    ASSERT_TRUE(service.PiecePutFinish(a.client_id, {first->handle, true},
                                       TenantId::Default()));
    ASSERT_TRUE(service.UnmountSegment(b.segment_id, b.client_id));
    EXPECT_FALSE(service.PiecePutFinish(a.client_id, {second->handle, true},
                                        TenantId::Default()));
    auto exists = service.ExistKey("target-exit", TenantId::Default());
    ASSERT_TRUE(exists);
    EXPECT_FALSE(*exists);
}

TEST_F(PiecePutMasterTest, DiskRegistrationCannotExposeAnUnfinishedObject) {
    MasterService service;
    const auto target = PrepareSimpleSegment(service);
    auto start = service.PiecePutStart(target.client_id,
                                       Piece("disk-notify", 0, 1, 4096),
                                       TenantId::Default());
    ASSERT_TRUE(start);
    Replica old_disk("/unused/test-only-path", 4096, ReplicaStatus::COMPLETE);
    EXPECT_FALSE(service.AddReplica(target.client_id, "disk-notify",
                                    TenantId::Default(), old_disk));
    ExpectKeyHiddenFromReadApis(service, "disk-notify");
    auto done = service.PiecePutFinish(target.client_id, {start->handle, true},
                                       TenantId::Default());
    ASSERT_TRUE(done);
    EXPECT_TRUE(done->object_published);
}

TEST_F(PiecePutMasterTest, PublishedFinishReplaySurvivesAnUnfinishedCopy) {
    MasterService service;
    const auto source =
        PrepareSimpleSegment(service, "copy-source", kDefaultSegmentBase,
                             kDefaultSegmentSize, "host-source");
    PrepareSimpleSegment(service, "copy-target",
                         kDefaultSegmentBase + kDefaultSegmentSize,
                         kDefaultSegmentSize, "host-target");
    auto request = Piece("copy-pending", 0, 1, 4096);
    request.config.preferred_segment = "copy-source";
    auto start =
        service.PiecePutStart(source.client_id, request, TenantId::Default());
    ASSERT_TRUE(start);
    auto published = service.PiecePutFinish(
        source.client_id, {start->handle, true}, TenantId::Default());
    ASSERT_TRUE(published);
    ASSERT_TRUE(published->object_published);

    // An ordinary background copy adds a PROCESSING replica without starting
    // a new primary write. The already-published source is still readable.
    auto copy =
        service.CopyStart(source.client_id, request.key, TenantId::Default(),
                          "copy-source", {"copy-target"});
    ASSERT_TRUE(copy);
    auto replay = service.PiecePutFinish(
        source.client_id, {start->handle, true}, TenantId::Default());
    ASSERT_TRUE(replay);
    EXPECT_TRUE(replay->piece_completed);
    EXPECT_TRUE(replay->object_published);
    EXPECT_TRUE(service.GetReplicaList(request.key, TenantId::Default()));
}

TEST_F(PiecePutMasterTest, MissingProducerExpiresWithoutAnotherWriteRequest) {
    MasterServiceConfig config;
    config.put_start_discard_timeout_sec = 1;
    MasterService service(config);
    const auto target = PrepareSimpleSegment(service);
    auto start = service.PiecePutStart(
        target.client_id, Piece("orphan", 0, 1, 8192), TenantId::Default());
    ASSERT_TRUE(start);
    ASSERT_EQ(start->replicas.size(), 1u);
    // This value descriptor does not retain the physical allocation itself.
    const auto original =
        start->replicas.front().get_memory_descriptor().buffer_descriptor;
    auto hidden = service.GetReplicaList("orphan", TenantId::Default());
    ASSERT_FALSE(hidden);
    ASSERT_EQ(hidden.error(), ErrorCode::REPLICA_IS_NOT_READY);

    // A producer exits without Finish. Read-only queries do not run PiecePut
    // expiry, so OBJECT_NOT_FOUND must come from the existing worker's sweep.
    const auto until =
        std::chrono::steady_clock::now() + std::chrono::seconds(4);
    bool removed = false;
    do {
        auto read = service.GetReplicaList("orphan", TenantId::Default());
        if (!read && read.error() == ErrorCode::OBJECT_NOT_FOUND) {
            removed = true;
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    } while (std::chrono::steady_clock::now() < until);
    ASSERT_TRUE(removed);
    auto retry = Piece("orphan", 0, 2, 8192);
    retry.write_id = "attempt-2";
    auto replacement =
        service.PiecePutStart(target.client_id, retry, TenantId::Default());
    ASSERT_TRUE(replacement);
    ASSERT_EQ(replacement->replicas.size(), 1u);
    const auto next =
        replacement->replicas.front().get_memory_descriptor().buffer_descriptor;
    // The sole segment stays mounted, and its release grace is much longer
    // than this expiry wait. The advertised old range must remain isolated.
    EXPECT_EQ(next.transport_endpoint_, original.transport_endpoint_);
    const bool overlaps =
        original.buffer_address_ <= next.buffer_address_
            ? next.buffer_address_ - original.buffer_address_ < original.size_
            : original.buffer_address_ - next.buffer_address_ < next.size_;
    EXPECT_FALSE(overlaps);
}

TEST_F(PiecePutMasterTest,
       PublishedFinishReplayDoesNotDependOnTemporaryTargetAvailability) {
    MasterService service;
    const auto target = PrepareSimpleSegment(service);
    auto start = service.PiecePutStart(target.client_id,
                                       Piece("suspected-target", 0, 1, 4096),
                                       TenantId::Default());
    ASSERT_TRUE(start);
    auto published = service.PiecePutFinish(
        target.client_id, {start->handle, true}, TenantId::Default());
    ASSERT_TRUE(published);
    ASSERT_TRUE(published->object_published);

    const auto liveness = FindClientLivenessForTest(service, target.client_id);
    ASSERT_TRUE(liveness);
    ASSERT_EQ(
        liveness->Evaluate(ClientLivenessRecord::Clock::now(),
                           std::chrono::seconds::zero(), std::chrono::hours(1)),
        ClientLivenessTransition::BECAME_SUSPECTED);
    MasterMetricManager::instance().client_liveness_became_suspected();
    EXPECT_FALSE(
        service.GetReplicaList("suspected-target", TenantId::Default()));

    // Retained storage can be temporarily unavailable while the completed
    // write remains published. Lost-Finish confirmation must not become stale.
    auto replay = service.PiecePutFinish(
        target.client_id, {start->handle, true}, TenantId::Default());
    ASSERT_TRUE(replay);
    EXPECT_TRUE(replay->piece_completed);
    EXPECT_TRUE(replay->object_published);
    EXPECT_EQ(liveness->state(), ClientLivenessState::SUSPECTED);
    ASSERT_TRUE(service.Ping(target.client_id));
    EXPECT_EQ(liveness->state(), ClientLivenessState::ACTIVE);
    EXPECT_TRUE(
        service.GetReplicaList("suspected-target", TenantId::Default()));
}

TEST_F(PiecePutMasterTest,
       InsufficientTargetsRollBackReservationAndTenantCharge) {
    const TenantId tenant("tenant-a");
    MasterService service(MakeStrictTenantConfig({tenant.value()}));
    const auto target = PrepareSimpleSegment(service, "first-target");
    auto* quota = service.tenant_quota_policy();
    ASSERT_NE(quota, nullptr);
    // The full two-replica reservation fits the quota, but a second target
    // is missing. A failed allocation must refund that reservation.
    constexpr uint64_t kObjectSize = kStrictTenantQuotaBytes / 2;
    auto request = Piece("allocation-retry", 0, 1, kObjectSize, kObjectSize);
    request.config.replica_num = 2;
    auto failed = service.PiecePutStart(target.client_id, request, tenant);
    ASSERT_FALSE(failed);
    EXPECT_EQ(failed.error(), ErrorCode::NO_AVAILABLE_HANDLE);
    auto missing = service.GetReplicaList(request.key, tenant);
    ASSERT_FALSE(missing);
    EXPECT_EQ(missing.error(), ErrorCode::OBJECT_NOT_FOUND);
    auto snapshot = quota->GetSnapshot(tenant);
    ASSERT_TRUE(snapshot);
    EXPECT_EQ(snapshot->charged_bytes, 0u);

    PrepareSimpleSegment(service, "second-target",
                         kDefaultSegmentBase + kDefaultSegmentSize);
    auto retry = service.PiecePutStart(target.client_id, request, tenant);
    ASSERT_TRUE(retry);
    EXPECT_EQ(retry->replicas.size(), 2u);
    snapshot = quota->GetSnapshot(tenant);
    ASSERT_TRUE(snapshot);
    EXPECT_EQ(snapshot->charged_bytes, kStrictTenantQuotaBytes);
    auto finished =
        service.PiecePutFinish(target.client_id, {retry->handle, true}, tenant);
    ASSERT_TRUE(finished);
    EXPECT_TRUE(finished->object_published);
    EXPECT_TRUE(service.GetReplicaList(request.key, tenant));
}

TEST_F(PiecePutMasterTest,
       QuotaRejectionCanRecoverWithoutAffectingTheSameKeyInAnotherTenant) {
    const TenantId tenant_a("tenant-a"), tenant_b("tenant-b");
    MasterService service(
        MakeStrictTenantConfig({tenant_a.value(), tenant_b.value()}));
    const auto target = PrepareSimpleSegment(service);
    auto* quota = service.tenant_quota_policy();
    ASSERT_NE(quota, nullptr);
    ASSERT_TRUE(service.PutStart(target.client_id, "occupying", tenant_a,
                                 kStrictTenantQuotaBytes, {}));
    auto request = Piece("shared-key", 0, 1, 4096);
    auto refused = service.PiecePutStart(target.client_id, request, tenant_a);
    ASSERT_FALSE(refused);
    EXPECT_EQ(refused.error(), ErrorCode::TENANT_QUOTA_EXCEEDED);
    auto missing = service.GetReplicaList(request.key, tenant_a);
    ASSERT_FALSE(missing);
    EXPECT_EQ(missing.error(), ErrorCode::OBJECT_NOT_FOUND);
    auto snapshot = quota->GetSnapshot(tenant_a);
    ASSERT_TRUE(snapshot);
    EXPECT_EQ(snapshot->charged_bytes, kStrictTenantQuotaBytes);

    auto other = service.PiecePutStart(target.client_id, request, tenant_b);
    ASSERT_TRUE(other);
    auto other_done = service.PiecePutFinish(target.client_id,
                                             {other->handle, true}, tenant_b);
    ASSERT_TRUE(other_done);
    EXPECT_TRUE(other_done->object_published);
    ASSERT_TRUE(service.PutRevoke(target.client_id, "occupying", tenant_a,
                                  ReplicaType::ALL));
    snapshot = quota->GetSnapshot(tenant_a);
    ASSERT_TRUE(snapshot);
    EXPECT_EQ(snapshot->charged_bytes, 0u);

    // Retry the original tenant/key/operation after capacity becomes available.
    auto retry = service.PiecePutStart(target.client_id, request, tenant_a);
    ASSERT_TRUE(retry);
    auto done = service.PiecePutFinish(target.client_id, {retry->handle, true},
                                       tenant_a);
    ASSERT_TRUE(done);
    EXPECT_TRUE(done->object_published);
    for (const auto& tenant : {tenant_a, tenant_b}) {
        EXPECT_TRUE(service.GetReplicaList(request.key, tenant));
        snapshot = quota->GetSnapshot(tenant);
        ASSERT_TRUE(snapshot);
        EXPECT_EQ(snapshot->charged_bytes, request.total_size);
    }
}

TEST_F(PiecePutMasterTest,
       FullManagerAdmitsExistingRoundsAndReusesPublishedRoundCapacity) {
    MasterServiceConfig config;
    config.put_start_discard_timeout_sec = 3600;
    config.put_start_release_timeout_sec = 7200;
    MasterService service(config);
    const auto target = PrepareSimpleSegment(service);
    constexpr size_t kRoundCapacity = 1024;
    constexpr uint64_t kObjectSize = 128, kPieceSize = 64;
    PiecePutCommitHandle first;
    for (size_t index = 0; index < kRoundCapacity; ++index) {
        SCOPED_TRACE(index);
        auto start =
            service.PiecePutStart(target.client_id,
                                  Piece("active-" + std::to_string(index), 0, 1,
                                        kObjectSize, kPieceSize),
                                  TenantId::Default());
        ASSERT_TRUE(start);
        if (index == 0) first = start->handle;
    }
    auto newcomer = Piece("newcomer", 0, 1, kObjectSize, kObjectSize);
    auto refused =
        service.PiecePutStart(target.client_id, newcomer, TenantId::Default());
    ASSERT_FALSE(refused);
    EXPECT_EQ(refused.error(), ErrorCode::NO_AVAILABLE_HANDLE);

    // Finishing one contribution keeps its round active. Both Finish and a
    // later non-overlapping Start must remain available at the global limit.
    auto partial = service.PiecePutFinish(target.client_id, {first, true},
                                          TenantId::Default());
    ASSERT_TRUE(partial);
    EXPECT_FALSE(partial->object_published);
    auto tail = service.PiecePutStart(
        target.client_id,
        Piece("active-0", kPieceSize, 2, kObjectSize, kPieceSize),
        TenantId::Default());
    ASSERT_TRUE(tail);
    auto done = service.PiecePutFinish(target.client_id, {tail->handle, true},
                                       TenantId::Default());
    ASSERT_TRUE(done);
    EXPECT_TRUE(done->object_published);
    auto admitted =
        service.PiecePutStart(target.client_id, newcomer, TenantId::Default());
    ASSERT_TRUE(admitted);
    auto newcomer_done = service.PiecePutFinish(
        target.client_id, {admitted->handle, true}, TenantId::Default());
    ASSERT_TRUE(newcomer_done);
    EXPECT_TRUE(newcomer_done->object_published);
}

}  // namespace mooncake::test
