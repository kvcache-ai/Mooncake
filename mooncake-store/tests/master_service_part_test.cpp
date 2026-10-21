// master_service_part_test.cpp
//
// An object whose content is written as several parts. Every part is written
// on its own, by its own PutStart and PutEnd, and the object is readable only
// once every one of them has arrived.
//
// The cases here are the write side: what a part write accepts and refuses,
// when the object becomes readable, how a late End is kept away from a write
// that replaced it, what the timeouts reclaim, and what a batch carries.

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <optional>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "master_service_test_fixture.h"
#include "metadata_store.h"

namespace mooncake::test {
namespace {

constexpr uint32_t kParts = 8;

ReplicateConfig OneMemoryReplica() {
    ReplicateConfig config;
    config.replica_num = 1;
    return config;
}

// The replica ids a part write handed out, which is what its End names.
std::vector<ReplicaID> StartedIds(
    const tl::expected<std::vector<Replica::Descriptor>, ErrorCode>& started) {
    std::vector<ReplicaID> ids;
    if (!started.has_value()) {
        return ids;
    }
    ids.reserve(started->size());
    for (const auto& descriptor : *started) {
        ids.push_back(descriptor.id);
    }
    return ids;
}

class MasterServicePartTest : public MasterServiceTest {
   protected:
    void SetUp() override {
        MasterServiceTest::SetUp();
        client_ = PrepareSimpleSegment(service_).client_id;
    }

    tl::expected<std::vector<Replica::Descriptor>, ErrorCode> PutStartPart(
        const std::string& key, uint32_t part_index, uint32_t part_count,
        uint64_t length = 1024) {
        return service_.PutStart(client_, key, TenantId::Default(), length,
                                 OneMemoryReplica(), part_index, part_count);
    }

    tl::expected<void, ErrorCode> PutEndPart(
        const std::string& key, uint32_t part_index,
        const std::vector<ReplicaID>& replica_ids = {}) {
        std::optional<std::vector<ReplicaID>> ids;
        if (!replica_ids.empty()) {
            ids = replica_ids;
        }
        return service_.PutEnd(client_, ObjectMeta{key, std::nullopt},
                               TenantId::Default(), ReplicaType::MEMORY,
                               part_index, std::move(ids));
    }

    // Writes one part from start to end and returns whether both halves were
    // accepted.
    bool WriteWholePart(const std::string& key, uint32_t part_index,
                        uint32_t part_count, uint64_t length = 1024) {
        auto started = PutStartPart(key, part_index, part_count, length);
        if (!started.has_value()) {
            return false;
        }
        return PutEndPart(key, part_index, StartedIds(started)).has_value();
    }

    // Whether the object is one a reader can see. ExistKey is the read that
    // asks for no part, so it answers for the object as a whole.
    bool IsReadable(const std::string& key) {
        return service_.ExistKey(key, TenantId::Default()).value_or(false);
    }

    // Runs the pass that reclaims what the timeouts cover, judged as of `now`:
    // `time_point::max()` puts every deadline in the past, a real instant
    // judges each write against its own start.
    void DiscardExpiredProcessingForTest(
        MasterService& service, const TenantId& tenant_id,
        const std::string& key, std::chrono::system_clock::time_point now) {
        const size_t shard_idx =
            MasterServiceTestPeer(service).getShardIndex(tenant_id, key);
        MasterServiceTestPeer::MetadataShardAccessorRW shard(&service,
                                                             shard_idx);
        MasterServiceTestPeer(service).DiscardExpiredProcessingReplicas(shard,
                                                                        now);
    }

    MasterService service_;
    UUID client_{};
};

// The parts may arrive in any order; until the last one lands the object is one
// a reader cannot see.
TEST_F(MasterServicePartTest, BecomesReadableOnlyWhenEveryPartHasLanded) {
    const std::string key = "parted_key";
    const std::vector<uint32_t> order = {3, 0, 7, 1, 6, 2, 5, 4};
    for (size_t i = 0; i + 1 < order.size(); ++i) {
        ASSERT_TRUE(WriteWholePart(key, order[i], kParts))
            << "part " << order[i] << " was refused";
        EXPECT_FALSE(IsReadable(key))
            << "the object became readable with part " << order[i] << " only";
        auto exists = service_.ExistKey(key, TenantId::Default());
        ASSERT_TRUE(exists.has_value());
        EXPECT_FALSE(*exists);
        auto all_keys = service_.GetAllKeys(TenantId::Default());
        ASSERT_TRUE(all_keys.has_value());
        EXPECT_EQ(all_keys->end(),
                  std::find(all_keys->begin(), all_keys->end(), key));
        // A read that names no part asks for a whole object, and an object of
        // several parts has no whole to give.
        auto get = service_.GetReplicaList(key, TenantId::Default());
        ASSERT_FALSE(get.has_value());
        EXPECT_EQ(ErrorCode::INVALID_PARAMS, get.error());
    }
    ASSERT_TRUE(WriteWholePart(key, order.back(), kParts));
    EXPECT_TRUE(IsReadable(key));
}

// The parts of one object are pieces of one write, so they name one session.
TEST_F(MasterServicePartTest, RefusesAPartThatNamesAnotherSession) {
    const std::string key = "session_key";
    ReplicateConfig tagged = OneMemoryReplica();
    tagged.session_ids = std::vector<std::string>{"session-a"};
    auto first = service_.PutStart(client_, key, TenantId::Default(), 1024,
                                   tagged, 0, 2);
    ASSERT_TRUE(first.has_value()) << toString(first.error());
    ASSERT_TRUE(service_
                    .PutEnd(client_, ObjectMeta{key, std::nullopt},
                            TenantId::Default(), ReplicaType::MEMORY, 0,
                            StartedIds(first))
                    .has_value());

    // The object-level record reads the session from a part, so a second part
    // naming another one leaves it two answers to choose between.
    ReplicateConfig other = OneMemoryReplica();
    other.session_ids = std::vector<std::string>{"session-b"};
    auto refused =
        service_.PutStart(client_, key, TenantId::Default(), 1024, other, 1, 2);
    ASSERT_FALSE(refused.has_value());
    EXPECT_EQ(ErrorCode::INVALID_PARAMS, refused.error());

    // Naming the same session is what a part of this object does.
    ReplicateConfig same = OneMemoryReplica();
    same.session_ids = std::vector<std::string>{"session-a"};
    auto accepted =
        service_.PutStart(client_, key, TenantId::Default(), 1024, same, 1, 2);
    ASSERT_TRUE(accepted.has_value()) << toString(accepted.error());

    // A part of another object is free to name its own session.
    ASSERT_TRUE(service_
                    .PutStart(client_, "other_session_key", TenantId::Default(),
                              1024, other, 0, 1)
                    .has_value());
}

// A part that already holds content belongs to the write that filled it.
TEST_F(MasterServicePartTest, RefusesASecondWriteOfAPartThatHoldsContent) {
    const std::string key = "occupied_key";
    ASSERT_TRUE(WriteWholePart(key, 0, 2));
    auto refused = PutStartPart(key, 0, 2);
    ASSERT_FALSE(refused.has_value());
    EXPECT_EQ(ErrorCode::OBJECT_ALREADY_EXISTS, refused.error());
    // The other part is still free.
    ASSERT_TRUE(WriteWholePart(key, 1, 2));
    EXPECT_TRUE(IsReadable(key));
}

// One write of a part is in flight until it completes; a second writer of the
// same part is refused while the first one may still be streaming.
TEST_F(MasterServicePartTest, RefusesASecondWriterOfTheSamePart) {
    const std::string key = "contended_key";
    ASSERT_TRUE(PutStartPart(key, 0, 1).has_value());
    auto refused = PutStartPart(key, 0, 1);
    ASSERT_FALSE(refused.has_value());
    EXPECT_EQ(ErrorCode::OBJECT_ALREADY_EXISTS, refused.error());
}

// An End from a write that was taken over names replicas the current write
// never handed out, and is refused without touching the new ones.
TEST_F(MasterServicePartTest, LateEndOfASupersededWriteIsRefused) {
    MasterService service(MasterServiceConfig::builder()
                              .set_put_start_discard_timeout_sec(0)
                              .set_put_start_release_timeout_sec(2)
                              .build());
    const UUID client = PrepareSimpleSegment(service).client_id;

    const std::string key = "superseded_key";
    auto first = service.PutStart(client, key, TenantId::Default(), 1024,
                                  OneMemoryReplica(), 0, 1);
    ASSERT_TRUE(first.has_value()) << toString(first.error());
    const auto superseded_ids = StartedIds(first);
    ASSERT_FALSE(superseded_ids.empty());

    // The discard timeout is zero, so the next write takes the part over.
    auto second = service.PutStart(client, key, TenantId::Default(), 1024,
                                   OneMemoryReplica(), 0, 1);
    ASSERT_TRUE(second.has_value()) << toString(second.error());
    const auto current_ids = StartedIds(second);
    ASSERT_FALSE(current_ids.empty());
    EXPECT_NE(superseded_ids.front(), current_ids.front());

    auto stale = service.PutEnd(client, ObjectMeta{key, std::nullopt},
                                TenantId::Default(), ReplicaType::MEMORY, 0,
                                superseded_ids);
    ASSERT_FALSE(stale.has_value());
    EXPECT_EQ(ErrorCode::INVALID_WRITE, stale.error());
    // The refused End left the live write alone: it still completes.
    EXPECT_FALSE(service.GetReplicaList(key, TenantId::Default()).has_value());
    ASSERT_TRUE(service
                    .PutEnd(client, ObjectMeta{key, std::nullopt},
                            TenantId::Default(), ReplicaType::MEMORY, 0,
                            current_ids)
                    .has_value());
    EXPECT_TRUE(service.GetReplicaList(key, TenantId::Default()).has_value());
}

// Repeating an End with the same replicas is what a client does when it did not
// see the answer to its first one; it succeeds and changes nothing.
TEST_F(MasterServicePartTest, RepeatedEndOfTheSameReplicasSucceeds) {
    const std::string key = "repeated_end_key";
    auto started = PutStartPart(key, 0, 1);
    ASSERT_TRUE(started.has_value());
    const auto ids = StartedIds(started);
    ASSERT_TRUE(PutEndPart(key, 0, ids).has_value());
    auto lease_after_first = service_.GetReplicaList(key, TenantId::Default());
    ASSERT_TRUE(lease_after_first.has_value());

    ASSERT_TRUE(PutEndPart(key, 0, ids).has_value());
    auto after_second = service_.GetReplicaList(key, TenantId::Default());
    ASSERT_TRUE(after_second.has_value());
    EXPECT_EQ(lease_after_first->replicas.size(),
              after_second->replicas.size());
    EXPECT_EQ(lease_after_first->object_checksum,
              after_second->object_checksum);
}

// The part position is what the request says about itself, and a request that
// says something impossible is refused before it touches anything.
TEST_F(MasterServicePartTest, RefusesImpossiblePartPositions) {
    const std::string key = "position_key";
    EXPECT_EQ(ErrorCode::INVALID_PARAMS, PutStartPart(key, 0, 0).error());
    EXPECT_EQ(ErrorCode::INVALID_PARAMS, PutStartPart(key, 2, 2).error());
    EXPECT_EQ(ErrorCode::INVALID_PARAMS,
              PutStartPart(key, 0, 1, /*length=*/0).error());
    EXPECT_FALSE(service_.ExistKey(key, TenantId::Default()).value_or(true));

    // The count is fixed by the first write.
    ASSERT_TRUE(PutStartPart(key, 0, 2).has_value());
    EXPECT_EQ(ErrorCode::INVALID_PARAMS, PutStartPart(key, 1, 4).error());

    // An End and a Revoke name a part that the object has.
    EXPECT_EQ(ErrorCode::INVALID_PARAMS, PutEndPart(key, 3).error());
    EXPECT_EQ(ErrorCode::INVALID_PARAMS,
              service_
                  .PutRevoke(client_, key, TenantId::Default(),
                             ReplicaType::MEMORY, 3, std::nullopt)
                  .error());
}

// A Revoke that names replicas this part no longer holds has nothing to do, and
// saying so twice is not an error.
TEST_F(MasterServicePartTest, RevokeThatNamesNoReplicaOfThePartIsIdempotent) {
    const std::string key = "revoked_twice_key";
    // The other part stays complete, so giving up this one leaves the object
    // in place for a second Revoke to find.
    ASSERT_TRUE(WriteWholePart(key, 1, 2));
    auto started = PutStartPart(key, 0, 2);
    ASSERT_TRUE(started.has_value()) << toString(started.error());
    const auto ids = StartedIds(started);
    ASSERT_FALSE(ids.empty());
    ASSERT_TRUE(service_
                    .PutRevoke(client_, key, TenantId::Default(),
                               ReplicaType::MEMORY, 0, ids)
                    .has_value());
    ASSERT_TRUE(service_
                    .PutRevoke(client_, key, TenantId::Default(),
                               ReplicaType::MEMORY, 0, ids)
                    .has_value());
}

// A part whose write was given up holds no content, and the record of that
// write stays. Everything that walks the object afterwards has to skip the part
// that is no longer there, and the part can be written again.
TEST_F(MasterServicePartTest, APartGivenUpLeavesTheObjectUsable) {
    const std::string key = "given_up_part_key";
    ASSERT_TRUE(WriteWholePart(key, 1, 2));
    auto started = PutStartPart(key, 0, 2);
    ASSERT_TRUE(started.has_value()) << toString(started.error());
    ASSERT_TRUE(service_
                    .PutRevoke(client_, key, TenantId::Default(),
                               ReplicaType::MEMORY, 0, StartedIds(started))
                    .has_value());
    EXPECT_FALSE(IsReadable(key));

    // The reclaim pass walks the object, a removal asks whether every part is
    // whole, and the End that rewrites the part walks it again: none of them
    // reads through the part that gave its content up.
    DiscardExpiredProcessingForTest(service_, TenantId::Default(), key,
                                    std::chrono::system_clock::now());
    EXPECT_FALSE(IsReadable(key));
    EXPECT_FALSE(service_.Remove(key, TenantId::Default()).has_value());

    ASSERT_TRUE(WriteWholePart(key, 0, 2));
    EXPECT_TRUE(IsReadable(key));
}

// A batch End carries many parts of one key, answers in the order it was
// asked, and refuses a part list of another length rather than reading it as
// the default.
TEST_F(MasterServicePartTest, BatchEndCarriesEveryPartOfOneKey) {
    const std::string key = "batched_key";
    constexpr uint32_t kBatchParts = 32;

    std::vector<std::vector<ReplicaID>> per_part_ids;
    per_part_ids.reserve(kBatchParts);
    for (uint32_t part = 0; part < kBatchParts; ++part) {
        auto started = PutStartPart(key, part, kBatchParts);
        ASSERT_TRUE(started.has_value())
            << "part " << part << ": " << toString(started.error());
        per_part_ids.push_back(StartedIds(started));
        ASSERT_EQ(per_part_ids.back().size(), 1u);
    }

    std::vector<ObjectMeta> object_metas(kBatchParts,
                                         ObjectMeta{key, std::nullopt});
    std::vector<uint32_t> part_indices;
    for (uint32_t part = 0; part < kBatchParts; ++part) {
        part_indices.push_back(part);
    }
    auto wrong_length = service_.BatchPutEnd(
        client_, object_metas, TenantId::Default(), ReplicaType::MEMORY,
        std::vector<uint32_t>{0}, per_part_ids);
    ASSERT_EQ(wrong_length.size(), kBatchParts);
    for (const auto& result : wrong_length) {
        ASSERT_FALSE(result.has_value());
        EXPECT_EQ(ErrorCode::INVALID_PARAMS, result.error());
    }
    EXPECT_FALSE(IsReadable(key));

    auto ended =
        service_.BatchPutEnd(client_, object_metas, TenantId::Default(),
                             ReplicaType::MEMORY, part_indices, per_part_ids);
    ASSERT_EQ(ended.size(), kBatchParts);
    for (size_t i = 0; i < ended.size(); ++i) {
        ASSERT_TRUE(ended[i].has_value())
            << "part " << i << ": " << toString(ended[i].error());
    }
    EXPECT_TRUE(IsReadable(key));
}

// A batch names one part per item, so every per-item list it indexes is as long
// as the batch: a shorter one is answered for every item instead of read past
// its end.
TEST_F(MasterServicePartTest, RefusesAPerItemListShorterThanTheBatch) {
    const std::string key = "batched_short_list_key";
    const std::vector<std::string> keys(3, key);
    const std::vector<uint32_t> part_indices{0, 1, 2};
    const ReplicateConfig config = OneMemoryReplica();

    const auto short_lengths = service_.BatchPutStart(
        client_, keys, std::vector<uint64_t>{1024, 1024}, part_indices,
        /*part_count=*/3, config, TenantId::Default());
    ASSERT_EQ(short_lengths.size(), keys.size());
    for (const auto& item : short_lengths) {
        ASSERT_FALSE(item.has_value());
        EXPECT_EQ(ErrorCode::INVALID_PARAMS, item.error());
    }

    const auto short_parts = service_.BatchPutStart(
        client_, keys, std::vector<uint64_t>(keys.size(), 1024),
        std::vector<uint32_t>{0, 1}, /*part_count=*/3, config,
        TenantId::Default());
    ASSERT_EQ(short_parts.size(), keys.size());
    for (const auto& item : short_parts) {
        ASSERT_FALSE(item.has_value());
        EXPECT_EQ(ErrorCode::INVALID_PARAMS, item.error());
    }
    EXPECT_FALSE(IsReadable(key));
}

// The surfaces that have no per-part form refuse the object rather than act on
// one part alone: copy and move hand it one replica set for all of it, and an
// upsert has one length to write. Offload, promotion, dynamic replication and
// the add-replica registration ask the same question through the same refusal;
// these three are the ones a fixture drives without a background task.
TEST_F(MasterServicePartTest, RefusesSurfacesThatHaveNoPerPartForm) {
    const std::string key = "parted_transition_key";
    ASSERT_TRUE(WriteWholePart(key, 0, 2));
    ASSERT_TRUE(WriteWholePart(key, 1, 2));
    const std::string target = "transition_target";
    PrepareSimpleSegment(service_, target,
                         kDefaultSegmentBase + 3 * kDefaultSegmentSize);

    auto copied = service_.CopyStart(client_, key, TenantId::Default(),
                                     "test_segment", {target});
    ASSERT_FALSE(copied.has_value());
    EXPECT_EQ(ErrorCode::INVALID_PARAMS, copied.error());

    auto moved = service_.MoveStart(client_, key, TenantId::Default(),
                                    "test_segment", target);
    ASSERT_FALSE(moved.has_value());
    EXPECT_EQ(ErrorCode::INVALID_PARAMS, moved.error());

    auto upserted = service_.UpsertStart(client_, key, TenantId::Default(),
                                         1024, OneMemoryReplica());
    ASSERT_FALSE(upserted.has_value());
    EXPECT_EQ(ErrorCode::INVALID_PARAMS, upserted.error());
}

// Removal acts on the whole object, and one whose parts are not all written is
// the object a read is refused for: it stays until the incompleteness timeout
// reclaims it, so the two answers match.
TEST_F(MasterServicePartTest, RefusesToRemoveAnObjectWithAnUnwrittenPart) {
    const std::string key = "parted_remove_key";
    ASSERT_TRUE(WriteWholePart(key, 0, 2));
    auto refused = service_.Remove(key, TenantId::Default());
    ASSERT_FALSE(refused.has_value());
    EXPECT_EQ(ErrorCode::REPLICA_IS_NOT_READY, refused.error());

    ASSERT_TRUE(WriteWholePart(key, 1, 2));
    ASSERT_TRUE(service_.Remove(key, TenantId::Default()).has_value());
    EXPECT_FALSE(IsReadable(key));
}

// A read of one part answers with that part: its own replica, its own checksum,
// and the object's part count. Everything else it reports is what the read of a
// whole object reports.
TEST_F(MasterServicePartTest, PartReadAnswersWithThatPartOnly) {
    const std::string key = "part_read_key";
    constexpr uint32_t kReadParts = 3;
    for (uint32_t part = 0; part < kReadParts; ++part) {
        auto started = PutStartPart(key, part, kReadParts);
        ASSERT_TRUE(started.has_value()) << toString(started.error());
        auto ended =
            service_.PutEnd(client_, ObjectMeta{key, uint64_t{1000} + part},
                            TenantId::Default(), ReplicaType::MEMORY, part,
                            StartedIds(started));
        ASSERT_TRUE(ended.has_value()) << toString(ended.error());
    }

    const std::string solo = "part_read_solo_key";
    auto solo_started = PutStartPart(solo, 0, 1);
    ASSERT_TRUE(solo_started.has_value());
    ASSERT_TRUE(service_
                    .PutEnd(client_, ObjectMeta{solo, std::nullopt},
                            TenantId::Default(), ReplicaType::MEMORY, 0,
                            StartedIds(solo_started))
                    .has_value());
    auto solo_response = service_.GetReplicaList(solo, TenantId::Default());
    ASSERT_TRUE(solo_response.has_value()) << toString(solo_response.error());

    for (uint32_t part = 0; part < kReadParts; ++part) {
        auto response = service_.GetReplicaList(key, TenantId::Default(), part);
        ASSERT_TRUE(response.has_value()) << toString(response.error());
        ASSERT_EQ(response->replicas.size(), 1u);
        ASSERT_TRUE(response->part_count.has_value());
        EXPECT_EQ(*response->part_count, kReadParts);
        ASSERT_TRUE(response->object_checksum.has_value());
        EXPECT_EQ(*response->object_checksum, uint64_t{1000} + part);
        EXPECT_EQ(response->lease_ttl_ms, solo_response->lease_ttl_ms);
    }

    // A part the object does not have is refused.
    auto out_of_range =
        service_.GetReplicaList(key, TenantId::Default(), kReadParts);
    ASSERT_FALSE(out_of_range.has_value());
    EXPECT_EQ(ErrorCode::INVALID_PARAMS, out_of_range.error());
}

// A reader of a part waits for the whole object, and a reader that names no
// part of an object of several is refused.
TEST_F(MasterServicePartTest, PartReadWaitsForTheWholeObject) {
    const std::string key = "part_read_wait_key";
    ASSERT_TRUE(WriteWholePart(key, 0, 2));
    auto early = service_.GetReplicaList(key, TenantId::Default(), uint32_t{0});
    ASSERT_FALSE(early.has_value());
    EXPECT_EQ(ErrorCode::REPLICA_IS_NOT_READY, early.error());
    ASSERT_TRUE(WriteWholePart(key, 1, 2));
    auto complete =
        service_.GetReplicaList(key, TenantId::Default(), uint32_t{0});
    ASSERT_TRUE(complete.has_value()) << toString(complete.error());
}

// The admin and regex surfaces show whole objects, so an object of several
// parts is not something they can answer.
TEST_F(MasterServicePartTest, AdminAndRegexReadsRefuseAPartedObject) {
    const std::string key = "part_admin_key";
    ASSERT_TRUE(WriteWholePart(key, 0, 2));
    ASSERT_TRUE(WriteWholePart(key, 1, 2));

    auto admin = service_.GetReplicaListForAdmin(key, TenantId::Default());
    ASSERT_FALSE(admin.has_value());
    EXPECT_EQ(ErrorCode::INVALID_PARAMS, admin.error());

    auto admin_batch =
        service_.BatchGetReplicaListForAdmin({key}, TenantId::Default());
    ASSERT_EQ(admin_batch.size(), 1u);
    ASSERT_FALSE(admin_batch[0].has_value());
    EXPECT_EQ(ErrorCode::INVALID_PARAMS, admin_batch[0].error());

    auto by_regex = service_.GetReplicaListByRegex(key, TenantId::Default());
    ASSERT_TRUE(by_regex.has_value());
    EXPECT_TRUE(by_regex->empty());
}

// An object becomes readable when its last part lands, and stops being
// readable when a part loses its only replica: the content a reader would have
// to gather is no longer there. The parts that remain keep the object, so the
// part that went away can be written again.
TEST_F(MasterServicePartTest,
       LosesReadabilityWhenAPublishedPartLosesItsReplica) {
    constexpr uint32_t kLosingParts = 3;
    // Two segments, so the unmount below can take away one part's replica and
    // leave the other parts' replicas where they are.
    (void)PrepareSimpleSegment(service_, "keeper_segment");
    auto loser = PrepareSimpleSegment(
        service_, "loser_segment", kDefaultSegmentBase + kDefaultSegmentSize);
    const std::string key = "part_loss_key";

    for (uint32_t part = 0; part < kLosingParts; ++part) {
        ReplicateConfig config = OneMemoryReplica();
        // Every part but the last sits on the segment that stays, so the only
        // replica the unmount takes away is the lost part's.
        config.preferred_segment =
            part + 1 == kLosingParts ? "loser_segment" : "keeper_segment";
        auto started = service_.PutStart(client_, key, TenantId::Default(),
                                         1024, config, part, kLosingParts);
        ASSERT_TRUE(started.has_value()) << toString(started.error());
        ASSERT_TRUE(service_
                        .PutEnd(client_, ObjectMeta{key, std::nullopt},
                                TenantId::Default(), ReplicaType::MEMORY, part,
                                StartedIds(started))
                        .has_value());
    }
    EXPECT_TRUE(IsReadable(key));
    auto whole = service_.GetReplicaList(key, TenantId::Default(), uint32_t{0});
    ASSERT_TRUE(whole.has_value()) << toString(whole.error());

    ASSERT_TRUE(
        service_.UnmountSegment(loser.segment_id, loser.client_id).has_value());
    EXPECT_FALSE(IsReadable(key));
    // Every read of every part waits for the object to be whole again, not just
    // the read of the part that went away.
    for (uint32_t part = 0; part < kLosingParts; ++part) {
        auto response = service_.GetReplicaList(key, TenantId::Default(), part);
        ASSERT_FALSE(response.has_value()) << "part " << part << " still read";
        EXPECT_EQ(ErrorCode::REPLICA_IS_NOT_READY, response.error());
    }

    // The object survives on the parts that remain, so the lost part can be
    // written again and the object becomes readable once more.
    auto rewritten =
        service_.PutStart(client_, key, TenantId::Default(), 1024,
                          OneMemoryReplica(), kLosingParts - 1, kLosingParts);
    ASSERT_TRUE(rewritten.has_value()) << toString(rewritten.error());
    ASSERT_TRUE(service_
                    .PutEnd(client_, ObjectMeta{key, std::nullopt},
                            TenantId::Default(), ReplicaType::MEMORY,
                            kLosingParts - 1, StartedIds(rewritten))
                    .has_value());
    EXPECT_TRUE(IsReadable(key));
}

// A write that goes silent holds its part, not the object: the silent part is
// reclaimed when its own write expires, and a write that is still running keeps
// its own part and keeps the object alive.
TEST_F(MasterServicePartTest, ReclaimsThePartsWhoseWriteWentSilent) {
    constexpr uint32_t kSilentParts = 3;
    const std::string key = "part_silent_key";
    ASSERT_TRUE(WriteWholePart(key, 0, kSilentParts));
    auto silent = PutStartPart(key, 1, kSilentParts);
    ASSERT_TRUE(silent.has_value()) << toString(silent.error());

    // The pass judges each part by when its own write started, so the silent
    // write and the running one are told apart by an instant between their
    // starts moved forward by the release timeout: past the deadline of the
    // first, ahead of the deadline of the second.
    std::this_thread::sleep_for(std::chrono::milliseconds(2));
    const auto between = std::chrono::system_clock::now();
    auto running = PutStartPart(key, 2, kSilentParts);
    ASSERT_TRUE(running.has_value()) << toString(running.error());
    EXPECT_FALSE(IsReadable(key));

    DiscardExpiredProcessingForTest(
        service_, TenantId::Default(), key,
        between + std::chrono::seconds(DEFAULT_PUT_START_RELEASE_TIMEOUT));

    // The silent part gave its replicas up.
    auto reclaimed = service_.GetReplicaList(key, TenantId::Default(), 1);
    ASSERT_FALSE(reclaimed.has_value());
    EXPECT_EQ(ErrorCode::REPLICA_IS_NOT_READY, reclaimed.error());
    // The running write is untouched: it still completes, and the part that
    // went silent can be written again.
    ASSERT_TRUE(PutEndPart(key, 2, StartedIds(running)).has_value());
    ASSERT_TRUE(WriteWholePart(key, 1, kSilentParts));
    EXPECT_TRUE(IsReadable(key));
}

// Seven of eight parts arrive and the eighth never starts. No write is left to
// finish the object, so once the newest write of it has been silent for a
// release timeout the object is reclaimed whole instead of holding space no
// reader can use.
TEST_F(MasterServicePartTest, ReclaimsAnObjectWhoseLastPartNeverArrives) {
    const std::string key = "part_never_finished_key";
    for (uint32_t part = 0; part + 1 < kParts; ++part) {
        ASSERT_TRUE(WriteWholePart(key, part, kParts))
            << "part " << part << " was refused";
    }
    EXPECT_FALSE(IsReadable(key));
    ASSERT_EQ(kParts, MasterServiceTestPeer::GetPartCount(
                          service_, TenantId::Default(), key));

    DiscardExpiredProcessingForTest(
        service_, TenantId::Default(), key,
        std::chrono::system_clock::time_point::max());

    EXPECT_FALSE(IsReadable(key));
    EXPECT_EQ(0u, MasterServiceTestPeer::GetPartCount(
                      service_, TenantId::Default(), key));
    // The key carries nothing of the object that was reclaimed: the same layout
    // can be written again from the first part.
    ASSERT_TRUE(WriteWholePart(key, 0, kParts));
}

// The parts that arrived early belong to a write still to come, so an object
// that is not readable is not an eviction candidate: space pressure leaves it
// to the incompleteness timeout. Once every part has arrived it is a candidate
// like any other object.
TEST_F(MasterServicePartTest, DoesNotEvictAnIncompleteObject) {
    // A zero lease TTL leaves nothing but incompleteness to keep the object.
    MasterService service(
        MasterServiceConfig::builder().set_default_kv_lease_ttl(0).build());
    const UUID client = PrepareSimpleSegment(service).client_id;
    const std::string key = "part_not_evicted_key";
    auto started = service.PutStart(client, key, TenantId::Default(), 1024,
                                    OneMemoryReplica(), 0, 2);
    ASSERT_TRUE(started.has_value()) << toString(started.error());
    ASSERT_TRUE(service
                    .PutEnd(client, ObjectMeta{key, std::nullopt},
                            TenantId::Default(), ReplicaType::MEMORY, 0,
                            StartedIds(started))
                    .has_value());
    ASSERT_FALSE(service.ExistKey(key, TenantId::Default()).value());

    MasterServiceTestPeer(service).RunBatchEvictForTesting(1.0, 1.0);

    // The object is still waiting for its second part, and that part is all it
    // takes to make it readable: had the eviction taken the first part away,
    // this write would leave the object unreadable.
    EXPECT_EQ(2u, MasterServiceTestPeer::GetPartCount(
                      service, TenantId::Default(), key));
    auto last = service.PutStart(client, key, TenantId::Default(), 1024,
                                 OneMemoryReplica(), 1, 2);
    ASSERT_TRUE(last.has_value()) << toString(last.error());
    ASSERT_TRUE(service
                    .PutEnd(client, ObjectMeta{key, std::nullopt},
                            TenantId::Default(), ReplicaType::MEMORY, 1,
                            StartedIds(last))
                    .has_value());
    ASSERT_TRUE(service.ExistKey(key, TenantId::Default()).value());

    MasterServiceTestPeer(service).RunBatchEvictForTesting(1.0, 1.0);

    EXPECT_FALSE(service.ExistKey(key, TenantId::Default()).value());
}

// A master that replicates metadata refuses to create an object of several
// parts: a standby promoted from its records may predate parts and would read
// one as an object with no usable replica, then drop it.
TEST_F(MasterServicePartTest, AMasterThatReplicatesMetadataRefusesParts) {
    MasterService service(MasterServiceConfig::builder()
                              .set_enable_ha(true)
                              .set_enable_oplog(true)
                              .set_cluster_id("parts_cluster")
                              .build());
    const UUID client = PrepareSimpleSegment(service).client_id;

    const std::string key = "replicated_parts_key";
    auto refused = service.PutStart(client, key, TenantId::Default(), 1024,
                                    OneMemoryReplica(), 0, 2);
    ASSERT_FALSE(refused.has_value());
    EXPECT_EQ(ErrorCode::INVALID_PARAMS, refused.error());
    EXPECT_EQ(0u, MasterServiceTestPeer::GetPartCount(
                      service, TenantId::Default(), key));

    // The same master serves an object of one part as it always did.
    ASSERT_TRUE(service
                    .PutStart(client, key, TenantId::Default(), 1024,
                              OneMemoryReplica(), 0, 1)
                    .has_value());
}
}  // namespace
}  // namespace mooncake::test
