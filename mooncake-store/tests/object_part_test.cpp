// object_part_test.cpp
//
// The content the envelope now holds in a part: the operations on the part
// itself, the envelope's forwarding to its single part, and the OpLog payload
// that carries the fields.

#include "object_metadata.h"
#include "object_part.h"

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <memory>
#include <optional>
#include <ranges>
#include <string>
#include <utility>
#include <vector>

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "client_liveness.h"
#include "master_config.h"
#include "master_service.h"
#include "master_service/master_service_test_peer.h"
#include "metadata_store.h"
#include "replica.h"

namespace mooncake {
namespace {

constexpr size_t kObjectLength = 4096;
constexpr uint64_t kChecksum = 77;
constexpr const char* kGroupId = "group-1";
constexpr const char* kSegmentEndpoint = "part-test-segment";
constexpr auto kWriteTime =
    std::chrono::system_clock::time_point(std::chrono::seconds(1700000000));

// A disk replica names a file and needs no allocator, so a suite about the
// part's own bookkeeping builds one without a segment.
Replica MakeDiskReplica(ReplicaStatus status) {
    return Replica(std::string("file://part-test"), kObjectLength, status);
}

std::vector<Replica> MakeDiskReplicas(
    const std::vector<ReplicaStatus>& statuses) {
    std::vector<Replica> replicas;
    replicas.reserve(statuses.size());
    for (const auto status : statuses) {
        replicas.push_back(MakeDiskReplica(status));
    }
    return replicas;
}

// A completed replica on a segment whose owning client is serving: the service
// finds this one readable until the segment's endpoint is awaiting a remount.
Replica MakeServingDiskReplica() {
    Replica replica(UUID{7, 7}, kObjectLength, kSegmentEndpoint,
                    ReplicaStatus::COMPLETE);
    replica.bindClientLiveness(std::make_shared<ClientLivenessRecord>(
        ClientLivenessRecord::Clock::now()));
    return replica;
}

std::vector<Replica> MakeServingDiskReplicas() {
    std::vector<Replica> replicas;
    replicas.push_back(MakeServingDiskReplica());
    return replicas;
}

std::unique_ptr<ObjectMetadata> MakeEnvelope(size_t length,
                                             std::vector<Replica>&& replicas) {
    return std::make_unique<ObjectMetadata>(
        UUID{1, 2}, kWriteTime, length, std::move(replicas), std::nullopt,
        false, ObjectDataType::UNKNOWN, std::string(kGroupId), TenantId(),
        "part-test-key");
}

}  // namespace

// A case that builds a service needs the logging setup the service-backed
// suites use.
class ServiceBackedTest : public ::testing::Test {
   protected:
    void SetUp() override {
        google::InitGoogleLogging("ObjectPartTest");
        FLAGS_logtostderr = true;
    }

    // glog rejects a second InitGoogleLogging with the same process, so each
    // case releases the one it took.
    void TearDown() override { google::ShutdownGoogleLogging(); }
};

// Building the OpLog payload needs a service.
class OplogPayloadTest : public ServiceBackedTest {};

TEST(ObjectPartTest, HoldsItsLengthAndChecksum) {
    ObjectPart part(kObjectLength, std::vector<Replica>{});

    EXPECT_EQ(part.length(), kObjectLength);
    EXPECT_FALSE(part.checksum().has_value());
    EXPECT_TRUE(part.replicas().empty());

    part.SetChecksum(kChecksum);
    ASSERT_TRUE(part.checksum().has_value());
    EXPECT_EQ(*part.checksum(), kChecksum);
}

TEST(ObjectPartTest, AppendsReplicasAndReadsThemThroughTheSpan) {
    ObjectPart part(kObjectLength, std::vector<Replica>{});
    auto replicas =
        MakeDiskReplicas({ReplicaStatus::COMPLETE, ReplicaStatus::PROCESSING});
    const ReplicaID processing_id = replicas.back().id();
    part.AddReplicas(std::move(replicas));

    EXPECT_EQ(part.replicas().size(), 2u);
    EXPECT_TRUE(
        std::ranges::any_of(part.replicas(), Replica::fn_is_processing));
    EXPECT_FALSE(
        std::ranges::all_of(part.replicas(), Replica::fn_is_completed));
    EXPECT_TRUE(std::ranges::any_of(part.replicas(), [&](const Replica& r) {
        return r.id() == processing_id;
    }));

    // These are disk replicas, so the medium predicates answer false for them.
    EXPECT_FALSE(
        std::ranges::any_of(part.replicas(), Replica::fn_is_memory_replica));
    EXPECT_FALSE(
        std::ranges::any_of(part.replicas(), Replica::fn_is_nof_replica));
}

TEST(ObjectPartTest, TheSpanReachesTheStoredReplicas) {
    ObjectPart part(kObjectLength, std::vector<Replica>{});
    part.AddReplicas(MakeDiskReplicas({ReplicaStatus::PROCESSING}));

    const auto processing =
        std::ranges::find_if(part.replicas(), Replica::fn_is_processing);
    ASSERT_NE(processing, part.replicas().end());
    processing->mark_complete();

    EXPECT_TRUE(std::ranges::all_of(part.replicas(), Replica::fn_is_completed));
}

TEST(ObjectPartTest, PopsOnlyTheMatchingReplicas) {
    ObjectPart part(kObjectLength, std::vector<Replica>{});
    part.AddReplicas(
        MakeDiskReplicas({ReplicaStatus::COMPLETE, ReplicaStatus::PROCESSING,
                          ReplicaStatus::COMPLETE}));

    auto popped = part.PopReplicas(&Replica::fn_is_processing);
    ASSERT_EQ(popped.size(), 1u);
    EXPECT_EQ(popped.front().status(), ReplicaStatus::PROCESSING);
    EXPECT_EQ(part.replicas().size(), 2u);
    EXPECT_TRUE(std::ranges::all_of(part.replicas(), Replica::fn_is_completed));

    const ReplicaID kept_id = part.replicas().front().id();
    auto erased = part.PopReplicas(
        [&](const Replica& replica) { return replica.id() == kept_id; });
    EXPECT_EQ(erased.size(), 1u);
    EXPECT_EQ(part.replicas().size(), 1u);

    auto remaining = part.PopReplicas();
    EXPECT_EQ(remaining.size(), 1u);
    EXPECT_TRUE(part.replicas().empty());
}

TEST(ObjectPartTest, ReportsWhetherAReplicaIsReadable) {
    ObjectPart part(kObjectLength, std::vector<Replica>{});
    part.AddReplicas(
        MakeDiskReplicas({ReplicaStatus::COMPLETE, ReplicaStatus::PROCESSING}));

    // The part does not own the rule: the caller hands it in, and the part
    // reports whether any of its replicas passes it.
    EXPECT_TRUE(part.HasReadableReplica(Replica::fn_is_completed));
    EXPECT_TRUE(part.HasReadableReplica(Replica::fn_is_processing));
    EXPECT_FALSE(part.HasReadableReplica(
        [](const Replica& replica) { return replica.is_memory_replica(); }));

    ObjectPart without_replicas(kObjectLength, std::vector<Replica>{});
    EXPECT_FALSE(without_replicas.HasReadableReplica(Replica::fn_is_completed));
}

TEST(ObjectPartTest, ReadableDescriptorsKeepOnlyWhatTheCallerAccepts) {
    ObjectPart part(kObjectLength, std::vector<Replica>{});
    part.AddReplicas(
        MakeDiskReplicas({ReplicaStatus::COMPLETE, ReplicaStatus::PROCESSING}));

    // The caller answers readability and fills the descriptor in one step, so a
    // replica it refuses leaves no descriptor behind: this is the path a
    // replica whose storage went away takes.
    auto descriptors = part.ReadableDescriptors(
        [](const Replica& replica, Replica::Descriptor& descriptor) {
            if (!replica.is_completed()) {
                return false;
            }
            descriptor = replica.get_descriptor();
            return true;
        });
    ASSERT_EQ(descriptors.size(), 1u);
    EXPECT_TRUE(descriptors.front().is_disk_replica());

    // A caller that refuses every replica leaves the reader with nothing, which
    // is what the read path turns into "not ready".
    auto none = part.ReadableDescriptors(
        [](const Replica&, Replica::Descriptor&) { return false; });
    EXPECT_TRUE(none.empty());
}

TEST(EnvelopePartTest, TheEnvelopeReadsAndWritesItsSinglePart) {
    auto metadata = MakeEnvelope(kObjectLength,
                                 MakeDiskReplicas({ReplicaStatus::COMPLETE}));
    metadata->MutablePart(0).SetChecksum(kChecksum);

    // The read accessor and the write accessor name the same part, and the
    // envelope's own operations answer with what that part holds.
    const ObjectPart& part = metadata->GetPart(0);
    EXPECT_EQ(&metadata->MutablePart(0), &part);
    EXPECT_EQ(part.length(), kObjectLength);
    EXPECT_EQ(*part.checksum(), kChecksum);
    EXPECT_EQ(metadata->TotalLength(), part.length());
    EXPECT_EQ(metadata->CountReplicas(), part.replicas().size());
    EXPECT_EQ(metadata->MutablePart(0).replicas().data(),
              part.replicas().data());
    EXPECT_EQ(metadata->MutablePart(0).replicas().size(),
              part.replicas().size());
    EXPECT_TRUE(metadata->HasReplica(&Replica::fn_is_completed));
}

TEST(EnvelopePartTest, TheEnvelopeAnswersItsQueriesAndPopsFromItsPart) {
    auto metadata = MakeEnvelope(
        kObjectLength,
        MakeDiskReplicas({ReplicaStatus::COMPLETE, ReplicaStatus::PROCESSING,
                          ReplicaStatus::COMPLETE}));

    // The envelope's queries answer over the replicas its part holds.
    EXPECT_EQ(metadata->CountReplicas(), 3u);
    EXPECT_EQ(metadata->CountReplicas(&Replica::fn_is_completed), 2u);
    EXPECT_TRUE(metadata->HasReplica(&Replica::fn_is_processing));
    EXPECT_FALSE(metadata->AllReplicas(&Replica::fn_is_completed));

    // Taking replicas out through the envelope takes every part's replicas the
    // predicate matches, and only those.
    auto erased = metadata->PopAllReplicas(&Replica::fn_is_processing);
    ASSERT_EQ(erased.size(), 1u);
    EXPECT_EQ(erased.front().status(), ReplicaStatus::PROCESSING);

    EXPECT_EQ(metadata->CountReplicas(), 2u);
    EXPECT_TRUE(metadata->AllReplicas(&Replica::fn_is_completed));

    // The part the envelope names is what stayed behind.
    const ObjectPart& part = metadata->GetPart(0);
    EXPECT_EQ(part.replicas().size(), 2u);
    EXPECT_TRUE(std::ranges::all_of(part.replicas(), Replica::fn_is_completed));
}

TEST(EnvelopePartTest, ValidityFollowsThePartsLengthAndReplicas) {
    auto with_replica = MakeEnvelope(
        kObjectLength, MakeDiskReplicas({ReplicaStatus::COMPLETE}));
    EXPECT_TRUE(with_replica->IsValid());

    auto without_replica = MakeEnvelope(kObjectLength, {});
    EXPECT_FALSE(without_replica->IsValid());

    auto without_content =
        MakeEnvelope(0, MakeDiskReplicas({ReplicaStatus::COMPLETE}));
    EXPECT_FALSE(without_content->IsValid());
}

TEST(EnvelopePartTest, ReadabilityFollowsTheReplicasThePartHolds) {
    auto completed = MakeEnvelope(kObjectLength,
                                  MakeDiskReplicas({ReplicaStatus::COMPLETE}));
    EXPECT_TRUE(completed->IsReadable(Replica::fn_is_completed));
    EXPECT_FALSE(completed->IsReadable(Replica::fn_is_processing));

    auto still_writing = MakeEnvelope(
        kObjectLength, MakeDiskReplicas({ReplicaStatus::PROCESSING}));
    EXPECT_FALSE(still_writing->IsReadable(Replica::fn_is_completed));

    // Nothing to read: an object whose replicas are all gone is not readable,
    // whatever rule the caller brings.
    auto without_replicas = MakeEnvelope(kObjectLength, {});
    EXPECT_FALSE(
        without_replicas->IsReadable([](const Replica&) { return true; }));
}

// The readability rule lives with the service and is handed to the part and the
// envelope. A replica whose segment is not mounted yet is the case the part
// cannot see on its own, so this reaches both through the service's rule.
class ReadableReplicaTest : public ServiceBackedTest {};

TEST_F(ReadableReplicaTest, TheRemountShieldWithholdsTheDescriptor) {
    MasterService service{MasterServiceConfig{}};
    test::MasterServiceTestPeer peer(service);
    ObjectPart part(kObjectLength, MakeServingDiskReplicas());
    auto metadata = MakeEnvelope(kObjectLength, MakeServingDiskReplicas());
    const auto is_readable = [&peer](const Replica& replica) {
        return peer.IsReplicaReadable(replica);
    };

    ASSERT_TRUE(peer.IsReplicaReadable(part.replicas().front()));
    EXPECT_EQ(peer.GetReadableReplicaDescriptors(part).size(), 1u);
    EXPECT_TRUE(metadata->IsReadable(is_readable));

    peer.MarkEndpointAwaitingRemount(kSegmentEndpoint);

    EXPECT_FALSE(peer.IsReplicaReadable(part.replicas().front()));
    EXPECT_TRUE(peer.GetReadableReplicaDescriptors(part).empty());
    EXPECT_FALSE(metadata->IsReadable(is_readable));
    // The part is unchanged: it held a replica the whole time, and whether that
    // replica can be read was never its to decide.
    EXPECT_TRUE(part.HasReadableReplica([](const Replica&) { return true; }));
}

// The two pops the service offers are named for the range they cover, and the
// cache-total accounting they carry follows that range. An object holds one
// part, so the case builds a second one to see the difference.
class ObjectPopRangeTest : public ServiceBackedTest {};

TEST_F(ObjectPopRangeTest, ThePartPopAndTheAllPopCoverTheirOwnParts) {
    MasterService service{MasterServiceConfig{}};
    test::MasterServiceTestPeer peer(service);
    auto metadata = MakeEnvelope(kObjectLength,
                                 MakeDiskReplicas({ReplicaStatus::COMPLETE}));
    test::MasterServiceTestPeer::AppendPart(
        *metadata, kObjectLength,
        MakeDiskReplicas({ReplicaStatus::COMPLETE, ReplicaStatus::PROCESSING}));
    ASSERT_EQ(metadata->PartCount(), 2u);
    const int64_t file_cache_before =
        MasterMetricManager::instance().get_file_cache_nums();

    // The part pop asks the object's part. The processing replica the other
    // part holds matches the predicate and stays where it is.
    auto from_the_part = peer.PopPartReplicasWithCacheTotalAccounting(
        *metadata, &Replica::fn_is_processing);
    EXPECT_TRUE(from_the_part.empty());
    EXPECT_EQ(metadata->GetPart(0).replicas().size(), 1u);
    EXPECT_EQ(metadata->GetPart(1).replicas().size(), 2u);

    // The accounting follows the range: the object's part still holds a
    // completed cache replica, so the object is still counted as one.
    EXPECT_TRUE(metadata->disk_cache_total_accounted);
    EXPECT_EQ(MasterMetricManager::instance().get_file_cache_nums(),
              file_cache_before + 1);

    // The all pop asks every part, so the same predicate takes the processing
    // replica out of the other part too.
    auto from_every_part = peer.PopAllReplicasWithCacheTotalAccounting(
        *metadata, &Replica::fn_is_processing);
    ASSERT_EQ(from_every_part.size(), 1u);
    EXPECT_EQ(from_every_part.front().status(), ReplicaStatus::PROCESSING);
    EXPECT_EQ(metadata->GetPart(1).replicas().size(), 1u);

    // The pop without a predicate leaves the object with nothing, and the
    // accounting comes back down with it.
    auto every_replica = peer.PopAllReplicasWithCacheTotalAccounting(*metadata);
    EXPECT_EQ(every_replica.size(), 2u);
    EXPECT_TRUE(metadata->GetPart(0).replicas().empty());
    EXPECT_TRUE(metadata->GetPart(1).replicas().empty());
    EXPECT_FALSE(metadata->disk_cache_total_accounted);
    EXPECT_EQ(MasterMetricManager::instance().get_file_cache_nums(),
              file_cache_before);
}

// The OpLog payload carries the envelope's own fields and no checksum, so this
// case pins the length the part holds and the fields beside it.
TEST_F(OplogPayloadTest, TheOplogPayloadCarriesTheEnvelopeFields) {
    MasterService service{MasterServiceConfig{}};
    auto metadata = MakeEnvelope(kObjectLength, {});

    const std::string bytes =
        test::MasterServiceTestPeer::SerializeMetadataForOpLogForTesting(
            service, *metadata);

    MetadataPayload payload;
    ASSERT_EQ(struct_pack::deserialize_to(payload, bytes),
              struct_pack::errc::ok);
    EXPECT_EQ(payload.client_id, (UUID{1, 2}));
    EXPECT_EQ(payload.size, metadata->TotalLength());
    EXPECT_TRUE(payload.replicas.empty());
    EXPECT_EQ(payload.group_id.value_or(""), kGroupId);
    EXPECT_EQ(payload.data_type.value_or(ObjectDataType::UNKNOWN),
              ObjectDataType::UNKNOWN);
    EXPECT_FALSE(payload.hard_pinned.value_or(true));
}

}  // namespace mooncake
