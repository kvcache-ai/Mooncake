#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <barrier>
#include <chrono>
#include <future>
#include <thread>
#include <vector>

#include "master_service_test_fixture.h"
#include "../src/unified_io_validation.h"

namespace mooncake::test {
namespace {

PutStartItem MakeRangeItem(std::string key, uint64_t offset,
                           uint64_t length = 4096, uint64_t total_size = 12288,
                           std::string write_id = "run-42-attempt-1") {
    return PutStartItem{
        .key = std::move(key),
        .size = length,
        .target = RangeTarget{.offset = offset,
                              .total_size = total_size,
                              .write_id = std::move(write_id)},
    };
}

class UnifiedPiecePutMasterTest : public MasterServiceTest {
   protected:
    tl::expected<PutStartResponse, ErrorCode> StartOne(
        MasterService& service, const UUID& client_id, const PutStartItem& item,
        const ReplicateConfig& config = {},
        const TenantId& tenant_id = TenantId::Default()) {
        return service.BatchPutStart(client_id, tenant_id, {item}, config)
            .front();
    }

    tl::expected<void, ErrorCode> FinishOne(
        MasterService& service, const UUID& client_id,
        const PutFinishRequest& req,
        const TenantId& tenant_id = TenantId::Default()) {
        return service.BatchPutFinish(client_id, tenant_id, {req}).front();
    }
};

TEST_F(UnifiedPiecePutMasterTest,
       OutOfOrderRangesStayHiddenUntilFullCoverageAndReturnSlicedDescriptors) {
    MasterService service;
    const auto target = PrepareSimpleSegment(service);
    const UUID other = generate_uuid();

    auto middle =
        StartOne(service, target.client_id, MakeRangeItem("kv", 4096));
    auto last = StartOne(service, other, MakeRangeItem("kv", 8192));
    auto first = StartOne(service, target.client_id, MakeRangeItem("kv", 0));
    ASSERT_TRUE(middle);
    ASSERT_TRUE(last);
    ASSERT_TRUE(first);

    ASSERT_EQ(first->replicas.size(), 1u);
    ASSERT_EQ(middle->replicas.size(), 1u);
    ASSERT_EQ(last->replicas.size(), 1u);

    // All three producers share the same underlying ReplicaID, while Master
    // returns descriptors pre-sliced to each producer's `[base + offset,
    // size)`.
    EXPECT_EQ(first->replicas[0].id, middle->replicas[0].id);
    EXPECT_EQ(first->replicas[0].id, last->replicas[0].id);
    const auto& first_buf =
        first->replicas[0].get_memory_descriptor().buffer_descriptor;
    const auto& middle_buf =
        middle->replicas[0].get_memory_descriptor().buffer_descriptor;
    const auto& last_buf =
        last->replicas[0].get_memory_descriptor().buffer_descriptor;
    EXPECT_EQ(first_buf.size_, 4096u);
    EXPECT_EQ(middle_buf.size_, 4096u);
    EXPECT_EQ(last_buf.size_, 4096u);
    EXPECT_EQ(middle_buf.buffer_address_, first_buf.buffer_address_ + 4096u);
    EXPECT_EQ(last_buf.buffer_address_, first_buf.buffer_address_ + 8192u);

    // Object stays hidden from Exist, Probe, and Get until every byte lands.
    ExpectKeyHiddenFromReadApis(service, "kv");
    auto unified_exist = service.BatchProbeKey({ProbeLeaseMode::AllHits, 1},
                                               {"kv"}, TenantId::Default());
    ASSERT_EQ(unified_exist.size(), 1u);
    ASSERT_TRUE(unified_exist[0]);
    EXPECT_FALSE(*unified_exist[0]);

    // Runtime assembly state lives on ObjectState::range_write, not metadata.
    {
        auto hold = MasterServiceTestPeer::ReadObject(
            service,
            MasterServiceTestPeer::ObjectIdentity{TenantId::Default(), "kv"});
        ASSERT_TRUE(hold);
        EXPECT_TRUE(hold->state().range_write);
        EXPECT_TRUE(hold->state().is_processing);
    }

    ASSERT_TRUE(FinishOne(service, other, {last->token, true}));
    ExpectKeyHiddenFromReadApis(service, "kv");

    ASSERT_TRUE(FinishOne(service, target.client_id, {first->token, true}));
    ExpectKeyHiddenFromReadApis(service, "kv");

    ASSERT_TRUE(FinishOne(service, target.client_id, {middle->token, true}));

    // Once published, transient ObjectState::range_write is cleared and the
    // object is a normal single-part object readable via WholeObject.
    {
        auto hold = MasterServiceTestPeer::ReadObject(
            service,
            MasterServiceTestPeer::ObjectIdentity{TenantId::Default(), "kv"});
        ASSERT_TRUE(hold);
        EXPECT_FALSE(hold->state().range_write);
        EXPECT_FALSE(hold->state().HasWorkInFlight());
    }

    auto unified_get = service.BatchGetReplicaList(TenantId::Default(),
                                                   {{"kv", WholeObject{}}});
    ASSERT_EQ(unified_get.size(), 1u);
    ASSERT_TRUE(unified_get[0]);
    EXPECT_EQ(unified_get[0]->part_count, 1u);
    ASSERT_EQ(unified_get[0]->parts.size(), 1u);
    EXPECT_EQ(unified_get[0]->parts[0].length, 12288u);
    ASSERT_EQ(unified_get[0]->parts[0].replicas.size(), 1u);
    const auto& whole_buf = unified_get[0]
                                ->parts[0]
                                .replicas[0]
                                .get_memory_descriptor()
                                .buffer_descriptor;
    EXPECT_EQ(whole_buf.buffer_address_, first_buf.buffer_address_);
    EXPECT_EQ(whole_buf.size_, 12288u);

    auto exists_after = service.BatchProbeKey({ProbeLeaseMode::None, 1}, {"kv"},
                                              TenantId::Default());
    ASSERT_EQ(exists_after.size(), 1u);
    ASSERT_TRUE(exists_after[0]);
    EXPECT_TRUE(*exists_after[0]);
}

TEST_F(UnifiedPiecePutMasterTest,
       OverlappingRangeAndWriteIdMismatchAndWrongClientAreRejected) {
    MasterService service;
    const auto target = PrepareSimpleSegment(service);
    const UUID other = generate_uuid();

    auto first = StartOne(service, target.client_id, MakeRangeItem("kv", 0));
    ASSERT_TRUE(first);

    // Overlapping range is rejected.
    auto overlap = StartOne(service, other, MakeRangeItem("kv", 2048));
    ASSERT_FALSE(overlap);
    EXPECT_EQ(overlap.error(), ErrorCode::OBJECT_ALREADY_EXISTS);

    // Different write_id does not implicitly preempt the active round.
    auto other_round = StartOne(
        service, other, MakeRangeItem("kv", 4096, 4096, 12288, "run-99"));
    ASSERT_FALSE(other_round);
    EXPECT_EQ(other_round.error(), ErrorCode::OBJECT_ALREADY_EXISTS);

    // Config mismatch (e.g. replica_num or total_size) is rejected.
    ReplicateConfig two_rep;
    two_rep.replica_num = 2;
    auto bad_cfg = StartOne(
        service, other,
        MakeRangeItem("kv", 4096, 4096, 12288, "run-42-attempt-1"), two_rep);
    ASSERT_FALSE(bad_cfg);
    EXPECT_EQ(bad_cfg.error(), ErrorCode::INVALID_PARAMS);

    // Unsupported mode returns NOT_SUPPORTED cleanly.
    ReplicateConfig same_node_cfg;
    same_node_cfg.prefer_alloc_in_same_node = true;
    auto unsupported_mode = StartOne(
        service, other, MakeRangeItem("kv-unsupported", 0, 4096, 4096, "r1"),
        same_node_cfg);
    ASSERT_FALSE(unsupported_mode);
    EXPECT_EQ(unsupported_mode.error(), ErrorCode::NOT_SUPPORTED);

    auto bad_total =
        StartOne(service, other,
                 MakeRangeItem("kv", 4096, 4096, 16384, "run-42-attempt-1"));
    ASSERT_FALSE(bad_total);
    EXPECT_EQ(bad_total.error(), ErrorCode::INVALID_PARAMS);

    // Wrong client cannot finish another producer's span.
    auto wrong_client = FinishOne(service, other, {first->token, true});
    ASSERT_FALSE(wrong_client);
    EXPECT_EQ(wrong_client.error(), ErrorCode::ILLEGAL_CLIENT);

    // Ordinary PutEnd / PutRevoke cannot settle an active RangeTarget write.
    auto legacy_end = service.PutEnd(target.client_id, "kv",
                                     TenantId::Default(), ReplicaType::MEMORY);
    ASSERT_FALSE(legacy_end);
    EXPECT_EQ(legacy_end.error(), ErrorCode::INVALID_WRITE);

    auto legacy_revoke = service.PutRevoke(
        target.client_id, "kv", TenantId::Default(), ReplicaType::MEMORY);
    ASSERT_FALSE(legacy_revoke);
    EXPECT_EQ(legacy_revoke.error(), ErrorCode::INVALID_WRITE);

    // Checksum on RangeTarget finish is rejected.
    PutFinishRequest with_checksum{
        .token = first->token,
        .commit = true,
        .replica_type = ReplicaType::MEMORY,
        .checksum = 12345u,
    };
    auto bad_checksum = FinishOne(service, target.client_id, with_checksum);
    ASSERT_FALSE(bad_checksum);
    EXPECT_EQ(bad_checksum.error(), ErrorCode::INVALID_PARAMS);
}

TEST_F(UnifiedPiecePutMasterTest,
       AbortDiscardsWholeRoundAndLateFailureDoesNotUndoConfirmedSpan) {
    MasterService service;
    const auto target = PrepareSimpleSegment(service);
    const UUID other = generate_uuid();

    auto a = StartOne(service, target.client_id,
                      MakeRangeItem("kv1", 0, 4096, 8192));
    auto b = StartOne(service, other, MakeRangeItem("kv1", 4096, 4096, 8192));
    ASSERT_TRUE(a);
    ASSERT_TRUE(b);

    // Confirm span A, then send a late failure for span A: must not abort.
    ASSERT_TRUE(FinishOne(service, target.client_id, {a->token, true}));
    ASSERT_TRUE(FinishOne(service, target.client_id, {a->token, false}));
    // Span B can still commit and publish the object.
    ASSERT_TRUE(FinishOne(service, other, {b->token, true}));
    auto got = service.GetReplicaList("kv1", TenantId::Default());
    ASSERT_TRUE(got);

    // Now test aborting an unfinished span on kv2: aborts the whole round.
    auto c = StartOne(service, target.client_id,
                      MakeRangeItem("kv2", 0, 4096, 8192));
    auto d = StartOne(service, other, MakeRangeItem("kv2", 4096, 4096, 8192));
    ASSERT_TRUE(c);
    ASSERT_TRUE(d);
    ASSERT_TRUE(FinishOne(service, other, {d->token, false}));

    auto late_c = FinishOne(service, target.client_id, {c->token, true});
    ASSERT_FALSE(late_c);
    EXPECT_EQ(late_c.error(), ErrorCode::OBJECT_NOT_FOUND);

    // Fresh write_id can immediately reuse kv2.
    auto fresh =
        StartOne(service, target.client_id,
                 MakeRangeItem("kv2", 0, 8192, 8192, "run-42-attempt-2"));
    ASSERT_TRUE(fresh);
    ASSERT_TRUE(FinishOne(service, target.client_id, {fresh->token, true}));
    EXPECT_TRUE(service.GetReplicaList("kv2", TenantId::Default()).has_value());
}

TEST_F(UnifiedPiecePutMasterTest,
       UpsertPreemptionAndInPlaceUpsertRejectOldRangeTokens) {
    MasterService service;
    const auto target = PrepareSimpleSegment(service);

    // 1. Preempt active RangeTarget write with UpsertStart.
    auto range =
        StartOne(service, target.client_id, MakeRangeItem("kv", 0, 4096, 8192));
    ASSERT_TRUE(range);
    auto upserted = service.UpsertStart(target.client_id, "kv",
                                        TenantId::Default(), 8192, {});
    ASSERT_TRUE(upserted);
    ASSERT_TRUE(service.UpsertEnd(target.client_id, "kv", TenantId::Default(),
                                  ReplicaType::MEMORY));

    auto stale = FinishOne(service, target.client_id, {range->token, true});
    ASSERT_FALSE(stale);
    EXPECT_EQ(stale.error(), ErrorCode::INVALID_WRITE);

    // 2. Publish a RangeTarget object, then do a same-size in-place Upsert:
    //    old token must still be rejected.
    auto r2 = StartOne(service, target.client_id,
                       MakeRangeItem("kv2", 0, 8192, 8192));
    ASSERT_TRUE(r2);
    ASSERT_TRUE(FinishOne(service, target.client_id, {r2->token, true}));
    auto inplace = service.UpsertStart(target.client_id, "kv2",
                                       TenantId::Default(), 8192, {});
    ASSERT_TRUE(inplace);
    ASSERT_TRUE(service.UpsertEnd(target.client_id, "kv2", TenantId::Default(),
                                  ReplicaType::MEMORY));
    auto stale_after_inplace =
        FinishOne(service, target.client_id, {r2->token, true});
    ASSERT_FALSE(stale_after_inplace);
    EXPECT_EQ(stale_after_inplace.error(), ErrorCode::INVALID_WRITE);
}

TEST_F(UnifiedPiecePutMasterTest,
       LosingOneFixedTargetReplicaAbortsWholeAssembly) {
    MasterService service;
    const auto seg0 = PrepareSimpleSegment(service, "seg0", 0x300000000ULL);
    const auto seg1 = PrepareSimpleSegment(service, "seg1", 0x400000000ULL);
    (void)seg1;

    ReplicateConfig cfg;
    cfg.replica_num = 2;
    auto p0 = StartOne(service, seg0.client_id,
                       MakeRangeItem("kv", 0, 4096, 8192, "run-2rep"), cfg);
    ASSERT_TRUE(p0);
    ASSERT_EQ(p0->replicas.size(), 2u);

    ASSERT_TRUE(service.UnmountSegment(seg1.segment_id, seg1.client_id));

    // Joining producer or finishing producer observes the lost fixed replica
    // and aborts the round instead of publishing a degraded object.
    auto fin = FinishOne(service, seg0.client_id, {p0->token, true});
    ASSERT_FALSE(fin);
    EXPECT_TRUE(fin.error() == ErrorCode::OBJECT_NOT_FOUND ||
                fin.error() == ErrorCode::INVALID_VERSION);
    auto exists = service.ExistKey("kv", TenantId::Default());
    ASSERT_TRUE(exists);
    EXPECT_FALSE(*exists);
}

TEST_F(UnifiedPiecePutMasterTest, MissingProducerExpiresViaInFlightSweep) {
    MasterServiceConfig cfg;
    cfg.put_start_discard_timeout_sec = 1;
    cfg.put_start_release_timeout_sec = 2;
    MasterService service(cfg);
    const auto target = PrepareSimpleSegment(service);

    auto p0 = StartOne(service, target.client_id,
                       MakeRangeItem("kv-expire", 0, 4096, 8192));
    ASSERT_TRUE(p0);

    // Wait for the background eviction thread to sweep the abandoned round
    // automatically (past the 2s release timeout) without any incoming write.
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(6);
    bool swept = false;
    while (std::chrono::steady_clock::now() < deadline) {
        auto hold = MasterServiceTestPeer::ReadObject(
            service, MasterServiceTestPeer::ObjectIdentity{TenantId::Default(),
                                                           "kv-expire"});
        if (!hold) {
            swept = true;
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    ASSERT_TRUE(swept);

    auto retry =
        StartOne(service, target.client_id,
                 MakeRangeItem("kv-expire", 0, 8192, 8192, "run-fresh"));
    ASSERT_TRUE(retry);
    ASSERT_TRUE(FinishOne(service, target.client_id, {retry->token, true}));
}

TEST_F(UnifiedPiecePutMasterTest,
       ExpiredRangeRoundRejectsSameWriteIdAndAllowsFreshReclaim) {
    MasterServiceConfig cfg;
    cfg.put_start_discard_timeout_sec = 1;
    cfg.put_start_release_timeout_sec = 30;
    MasterService service(cfg);
    const auto target = PrepareSimpleSegment(service);

    ASSERT_TRUE(StartOne(service, target.client_id,
                         MakeRangeItem("kv-same", 0, 4096, 8192, "run-1")));
    ASSERT_TRUE(StartOne(service, target.client_id,
                         MakeRangeItem("kv-fresh", 0, 4096, 8192, "run-1")));

    std::this_thread::sleep_for(std::chrono::milliseconds(1100));

    // Same write_id after discard timeout must fail with LEASE_EXPIRED.
    auto late_join =
        StartOne(service, target.client_id,
                 MakeRangeItem("kv-same", 4096, 4096, 8192, "run-1"));
    ASSERT_FALSE(late_join);
    EXPECT_EQ(late_join.error(), ErrorCode::LEASE_EXPIRED);

    // Different write_id after discard timeout lazily reclaims immediately.
    auto fresh = StartOne(service, target.client_id,
                          MakeRangeItem("kv-fresh", 0, 8192, 8192, "run-2"));
    ASSERT_TRUE(fresh);
    ASSERT_TRUE(FinishOne(service, target.client_id, {fresh->token, true}));
}

TEST_F(UnifiedPiecePutMasterTest,
       UnifiedWholeObjectAndPartTargetAndReadTargetSemantics) {
    MasterService service;
    const auto target = PrepareSimpleSegment(service);

    // WholeObject write via unified BatchPutStart / BatchPutFinish.
    PutStartItem whole_item{
        .key = "whole-key",
        .size = 2048,
        .target = WholeObject{},
    };
    auto started = StartOne(service, target.client_id, whole_item);
    ASSERT_TRUE(started);
    ASSERT_EQ(started->replicas.size(), 1u);
    EXPECT_EQ(started->token.key, "whole-key");
    EXPECT_TRUE(std::holds_alternative<WholeObject>(started->token.target));

    // Commit with checksum via unified BatchPutFinish.
    ASSERT_TRUE(FinishOne(service, target.client_id,
                          {.token = started->token,
                           .commit = true,
                           .replica_type = ReplicaType::MEMORY,
                           .checksum = 0xCAFEBABEULL}));

    // Unified BatchGetReplicaList with explicit WholeObject and PartIndex.
    auto gets = service.BatchGetReplicaList(TenantId::Default(),
                                            {{"whole-key", WholeObject{}},
                                             {"whole-key", PartIndex{0}},
                                             {"whole-key", PartIndex{1}}});
    ASSERT_EQ(gets.size(), 3u);
    ASSERT_TRUE(gets[0]);
    EXPECT_EQ(gets[0]->part_count, 1u);
    ASSERT_EQ(gets[0]->parts.size(), 1u);
    EXPECT_EQ(gets[0]->parts[0].length, 2048u);
    EXPECT_EQ(gets[0]->parts[0].checksum, 0xCAFEBABEULL);
    ASSERT_TRUE(gets[1]);
    EXPECT_EQ(gets[1]->part_count, 1u);
    ASSERT_EQ(gets[1]->parts.size(), 1u);
    ASSERT_FALSE(gets[2]);
    EXPECT_EQ(gets[2].error(), ErrorCode::INVALID_PARAMS);

    // Multi-part PartTarget currently returns NOT_SUPPORTED cleanly.
    PutStartItem multi_part_item{
        .key = "mp-key",
        .size = 1024,
        .target = PartTarget{.part_index = 0, .part_count = 2},
    };
    auto mp = StartOne(service, target.client_id, multi_part_item);
    ASSERT_FALSE(mp);
    EXPECT_EQ(mp.error(), ErrorCode::NOT_SUPPORTED);
}

TEST_F(UnifiedPiecePutMasterTest, ConcurrentProducersAssembleOneObject) {
    MasterService service;
    const auto target = PrepareSimpleSegment(service);
    constexpr size_t kProducers = 8;
    constexpr uint64_t kPieceSize = 4096;
    constexpr uint64_t kTotalSize = kProducers * kPieceSize;

    std::barrier sync_point(kProducers);
    std::vector<std::future<ErrorCode>> futures;
    futures.reserve(kProducers);
    for (size_t rank = 0; rank < kProducers; ++rank) {
        futures.push_back(std::async(std::launch::async, [&, rank] {
            const UUID cid = rank == 0 ? target.client_id : generate_uuid();
            sync_point.arrive_and_wait();
            auto start = StartOne(
                service, cid,
                MakeRangeItem("concurrent-kv", rank * kPieceSize, kPieceSize,
                              kTotalSize, "concurrent-run"));
            if (!start) return start.error();
            auto fin = FinishOne(service, cid, {start->token, true});
            return fin ? ErrorCode::OK : fin.error();
        }));
    }
    for (auto& f : futures) {
        EXPECT_EQ(f.get(), ErrorCode::OK);
    }

    auto got = service.BatchGetReplicaList(TenantId::Default(),
                                           {{"concurrent-kv", WholeObject{}}});
    ASSERT_EQ(got.size(), 1u);
    ASSERT_TRUE(got[0]);
    ASSERT_EQ(got[0]->parts.size(), 1u);
    ASSERT_EQ(got[0]->parts[0].replicas.size(), 1u);
    EXPECT_EQ(got[0]
                  ->parts[0]
                  .replicas[0]
                  .get_memory_descriptor()
                  .buffer_descriptor.size_,
              kTotalSize);
}

TEST(UnifiedIoValidationTest, PutCapabilitiesAndTargetPrecedence) {
    char buf[16]{};
    ReplicateConfig nof_cfg;
    nof_cfg.nof_replica_num = 1;
    const auto nof_caps = detail::ResolveUnifiedPutCapabilities(
        /*has_transfer_engine=*/true, "tcp", /*is_using_tent=*/false,
        /*object_checksum_enabled=*/false, /*legacy_tcp_proto_forced=*/false,
        nof_cfg);
    EXPECT_EQ(nof_caps.whole, ErrorCode::NOT_SUPPORTED);
    EXPECT_EQ(nof_caps.range, ErrorCode::NOT_SUPPORTED);

    // Parameter errors take precedence over capability errors consistently
    // across PartTarget and RangeTarget.
    Client::PutItem bad_range{
        .key = "k",
        .slices = {{buf, 16}},
        .target = RangeTarget{.offset = 8, .total_size = 16, .write_id = "w1"},
    };
    EXPECT_EQ(detail::ValidatePutItem(bad_range, nof_caps).error(),
              ErrorCode::INVALID_PARAMS);

    Client::PutItem valid_range{
        .key = "k",
        .slices = {{buf, 16}},
        .target = RangeTarget{.offset = 0, .total_size = 16, .write_id = "w1"},
    };
    EXPECT_EQ(detail::ValidatePutItem(valid_range, nof_caps).error(),
              ErrorCode::NOT_SUPPORTED);

    // Checksum or TCP v1 disables RangeTarget while keeping WholeObject valid.
    const auto tcp_v1_caps = detail::ResolveUnifiedPutCapabilities(
        true, "tcp", false, false, /*legacy_tcp_proto_forced=*/true, {});
    EXPECT_EQ(tcp_v1_caps.whole, ErrorCode::OK);
    EXPECT_EQ(tcp_v1_caps.range, ErrorCode::NOT_SUPPORTED);

    Client::PutItem whole_item{.key = "k", .slices = {{buf, 8}, {buf + 8, 8}}};
    auto whole_len = detail::ValidatePutItem(whole_item, tcp_v1_caps);
    ASSERT_TRUE(whole_len);
    EXPECT_EQ(*whole_len, 16u);
    EXPECT_EQ(detail::ValidatePutItem(valid_range, tcp_v1_caps).error(),
              ErrorCode::NOT_SUPPORTED);

    // Overlong write_id is rejected identically on Client and Master.
    Client::PutItem long_id_range{
        .key = "k",
        .slices = {{buf, 16}},
        .target = RangeTarget{.offset = 0,
                              .total_size = 16,
                              .write_id = std::string(300, 'x')},
    };
    const auto ok_caps = detail::ResolveUnifiedPutCapabilities(
        true, "tcp", false, false, false, {});
    EXPECT_EQ(detail::ValidatePutItem(long_id_range, ok_caps).error(),
              ErrorCode::INVALID_PARAMS);
}

TEST(UnifiedIoValidationTest, GetItemAndAllocatedReplicaValidation) {
    char buf[16]{};
    EXPECT_EQ(detail::ValidateGetItem({.key = "", .slices = {{buf, 8}}}),
              ErrorCode::INVALID_PARAMS);
    EXPECT_EQ(detail::ValidateGetItem({.key = "k", .slices = {}}),
              ErrorCode::INVALID_PARAMS);
    EXPECT_EQ(detail::ValidateGetItem({.key = "k", .slices = {{nullptr, 8}}}),
              ErrorCode::INVALID_PARAMS);
    // PartIndex bounds are validated against Master's part_count rather than
    // hardcoded on the client, so PartIndex{1} passes local validation.
    EXPECT_EQ(detail::ValidateGetItem(
                  {.key = "k", .slices = {{buf, 8}}, .target = PartIndex{1}}),
              ErrorCode::OK);
    EXPECT_EQ(detail::ValidateGetItem(
                  {.key = "k", .slices = {{buf, 8}}, .target = PartIndex{0}}),
              ErrorCode::OK);

    // Multi-part slice planning across 3 parts (lengths 4, 6, 6 = 16).
    std::vector<PartReplicaInfo> parts{
        PartReplicaInfo{.replicas = {}, .length = 4},
        PartReplicaInfo{.replicas = {}, .length = 6},
        PartReplicaInfo{.replicas = {}, .length = 6},
    };
    std::vector<Slice> scatter{{buf, 5}, {buf + 5, 11}};
    auto whole_plan = detail::PlanMultiPartReadSlices(parts, scatter, 0);
    ASSERT_TRUE(whole_plan);
    ASSERT_EQ(whole_plan->size(), 3u);
    EXPECT_EQ((*whole_plan)[0].part_index, 0u);
    EXPECT_EQ((*whole_plan)[0].part_src_offset, 0u);
    EXPECT_TRUE((*whole_plan)[0].full_part);
    ASSERT_EQ((*whole_plan)[0].slices.size(), 1u);
    EXPECT_EQ((*whole_plan)[0].slices[0].ptr, buf);
    EXPECT_EQ((*whole_plan)[0].slices[0].size, 4u);
    // Part 1 spans the boundary between scatter[0] (1 byte) and scatter[1] (5).
    EXPECT_EQ((*whole_plan)[1].part_index, 1u);
    EXPECT_TRUE((*whole_plan)[1].full_part);
    ASSERT_EQ((*whole_plan)[1].slices.size(), 2u);
    EXPECT_EQ((*whole_plan)[1].slices[0].ptr, buf + 4);
    EXPECT_EQ((*whole_plan)[1].slices[0].size, 1u);
    EXPECT_EQ((*whole_plan)[1].slices[1].ptr, buf + 5);
    EXPECT_EQ((*whole_plan)[1].slices[1].size, 5u);
    EXPECT_EQ((*whole_plan)[2].part_index, 2u);
    EXPECT_TRUE((*whole_plan)[2].full_part);

    // Sub-range across parts 0..2: [2, 12) = 10 bytes.
    std::vector<Slice> sub{{buf, 10}};
    auto range_plan = detail::PlanMultiPartReadSlices(parts, sub, 2);
    ASSERT_TRUE(range_plan);
    ASSERT_EQ(range_plan->size(), 3u);
    EXPECT_EQ((*range_plan)[0].part_src_offset, 2u);
    EXPECT_FALSE((*range_plan)[0].full_part);
    EXPECT_EQ((*range_plan)[1].part_src_offset, 0u);
    EXPECT_TRUE((*range_plan)[1].full_part);
    EXPECT_EQ((*range_plan)[2].part_src_offset, 0u);
    EXPECT_FALSE((*range_plan)[2].full_part);

    // Whole-object length mismatch returns TRANSFER_FAIL.
    auto bad_len = detail::PlanMultiPartReadSlices(parts, sub, 0);
    ASSERT_FALSE(bad_len);
    EXPECT_EQ(bad_len.error(), ErrorCode::TRANSFER_FAIL);

    Replica::Descriptor tcp_rep;
    tcp_rep.id = 1;
    tcp_rep.status = ReplicaStatus::PROCESSING;
    MemoryDescriptor mem_desc;
    mem_desc.buffer_descriptor.size_ = 16;
    mem_desc.buffer_descriptor.protocol_ = "tcp";
    mem_desc.buffer_descriptor.transport_endpoint_ = "127.0.0.1:9000";
    tcp_rep.descriptor_variant = mem_desc;

    EXPECT_EQ(detail::ValidateAllocatedWriteReplicas(
                  {tcp_rep}, 16, /*is_range=*/true,
                  [](const std::string&) { return std::optional<int>(2); }),
              ErrorCode::OK);
    EXPECT_EQ(detail::ValidateAllocatedWriteReplicas(
                  {tcp_rep}, 16, /*is_range=*/true,
                  [](const std::string&) { return std::optional<int>(1); }),
              ErrorCode::NOT_SUPPORTED);
    EXPECT_EQ(detail::ValidateAllocatedWriteReplicas(
                  {tcp_rep}, 16, /*is_range=*/true,
                  [](const std::string&) { return std::optional<int>{}; }),
              ErrorCode::NOT_SUPPORTED);
    EXPECT_EQ(detail::ValidateAllocatedWriteReplicas(
                  {tcp_rep}, 32, /*is_range=*/true,
                  [](const std::string&) { return std::optional<int>(2); }),
              ErrorCode::INVALID_REPLICA);
}

}  // namespace
}  // namespace mooncake::test
