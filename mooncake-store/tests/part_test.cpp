// part_test.cpp
//
// The content the envelope now holds in a part: the operations on the part
// itself, the envelope's forwarding to its single part, and the OpLog payload
// that carries the fields.

#include "object_metadata.h"
#include "part.h"

#include <chrono>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include <glog/logging.h>
#include <gtest/gtest.h>

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

std::unique_ptr<ObjectMetadata> MakeEnvelope(size_t length,
                                             std::vector<Replica>&& replicas) {
    return std::make_unique<ObjectMetadata>(
        UUID{1, 2}, kWriteTime, length, std::move(replicas), std::nullopt,
        false, ObjectDataType::UNKNOWN, std::string(kGroupId), TenantId(),
        "part-test-key");
}

}  // namespace

// Building the OpLog payload needs a service, so this case carries the logging
// setup the other service-backed suites use.
class PartPayloadTest : public ::testing::Test {
   protected:
    void SetUp() override {
        google::InitGoogleLogging("PartPayloadTest");
        FLAGS_logtostderr = true;
    }

    // glog rejects a second InitGoogleLogging with the same process, so each
    // case releases the one it took.
    void TearDown() override { google::ShutdownGoogleLogging(); }
};

TEST(PartTest, HoldsItsContentAndItsReplicas) {
    Part part(kObjectLength, std::vector<Replica>{});

    EXPECT_EQ(part.length(), kObjectLength);
    EXPECT_FALSE(part.checksum().has_value());
    EXPECT_EQ(part.CountReplicas(), 0u);

    part.SetChecksum(kChecksum);
    ASSERT_TRUE(part.checksum().has_value());
    EXPECT_EQ(*part.checksum(), kChecksum);
}

TEST(PartTest, AppendsAndFindsReplicas) {
    Part part(kObjectLength, std::vector<Replica>{});
    auto replicas =
        MakeDiskReplicas({ReplicaStatus::COMPLETE, ReplicaStatus::PROCESSING});
    const ReplicaID processing_id = replicas.back().id();
    part.AddReplicas(std::move(replicas));

    EXPECT_EQ(part.CountReplicas(), 2u);
    EXPECT_EQ(part.CountReplicas(&Replica::fn_is_completed), 1u);
    EXPECT_EQ(part.GetAllReplicas().size(), 2u);
    EXPECT_TRUE(part.HasReplica(&Replica::fn_is_completed));
    EXPECT_TRUE(part.HasReplica(&Replica::fn_is_processing));
    EXPECT_FALSE(part.AllReplicas(&Replica::fn_is_completed));

    ASSERT_NE(part.GetReplicaByID(processing_id), nullptr);
    EXPECT_EQ(part.GetReplicaByID(processing_id)->status(),
              ReplicaStatus::PROCESSING);
    EXPECT_EQ(part.GetReplicaByID(0), nullptr);

    // Nothing here is a memory or NoF replica, which the medium predicate the
    // store reaches for through the part reports from the replica itself.
    EXPECT_FALSE(part.HasReplica(&Replica::fn_is_memory_replica));
    EXPECT_FALSE(part.HasReplica(&Replica::fn_is_nof_replica));
}

TEST(PartTest, VisitsOnlyTheMatchingReplicas) {
    Part part(kObjectLength, std::vector<Replica>{});
    part.AddReplicas(
        MakeDiskReplicas({ReplicaStatus::COMPLETE, ReplicaStatus::PROCESSING,
                          ReplicaStatus::COMPLETE}));

    size_t visited = 0;
    const size_t matched =
        part.VisitReplicas(&Replica::fn_is_completed, [&](Replica& replica) {
            ++visited;
            EXPECT_EQ(replica.status(), ReplicaStatus::COMPLETE);
        });

    EXPECT_EQ(matched, 2u);
    EXPECT_EQ(visited, 2u);
}

TEST(PartTest, PopsAndErasesOnlyTheMatchingReplicas) {
    Part part(kObjectLength, std::vector<Replica>{});
    part.AddReplicas(
        MakeDiskReplicas({ReplicaStatus::COMPLETE, ReplicaStatus::PROCESSING,
                          ReplicaStatus::COMPLETE}));

    auto popped = part.PopReplicas(&Replica::fn_is_processing);
    ASSERT_EQ(popped.size(), 1u);
    EXPECT_EQ(popped.front().status(), ReplicaStatus::PROCESSING);
    EXPECT_EQ(part.CountReplicas(), 2u);
    EXPECT_TRUE(part.AllReplicas(&Replica::fn_is_completed));

    const ReplicaID kept_id = part.GetAllReplicas().front().id();
    EXPECT_EQ(part.EraseReplicas([&](const Replica& replica) {
        return replica.id() == kept_id;
    }),
              1u);
    EXPECT_EQ(part.CountReplicas(), 1u);

    auto remaining = part.PopReplicas();
    EXPECT_EQ(remaining.size(), 1u);
    EXPECT_EQ(part.CountReplicas(), 0u);
    EXPECT_TRUE(part.GetAllReplicas().empty());
}

TEST(PartTest, EraseReplicasReportsTheNumberOfMatches) {
    Part part(kObjectLength, std::vector<Replica>{});
    part.AddReplicas(
        MakeDiskReplicas({ReplicaStatus::COMPLETE, ReplicaStatus::COMPLETE}));

    EXPECT_EQ(part.EraseReplicas(&Replica::fn_is_completed), 2u);
    EXPECT_EQ(part.CountReplicas(), 0u);
}

TEST(EnvelopePartTest, TheEnvelopeForwardsToItsSinglePart) {
    auto metadata = MakeEnvelope(kObjectLength,
                                 MakeDiskReplicas({ReplicaStatus::COMPLETE}));
    metadata->MutablePart(0).SetChecksum(kChecksum);

    // The read accessor and the write accessor name the same part, and the
    // envelope's own operations answer with what that part holds.
    const Part& part = metadata->GetPart(0);
    EXPECT_EQ(&metadata->MutablePart(0), &part);
    EXPECT_EQ(part.length(), kObjectLength);
    EXPECT_EQ(*part.checksum(), kChecksum);
    EXPECT_EQ(metadata->TotalLength(), part.length());
    EXPECT_EQ(metadata->CountReplicas(), part.CountReplicas());
    EXPECT_EQ(&metadata->GetAllReplicas(), &part.GetAllReplicas());
    EXPECT_TRUE(metadata->HasReplica(&Replica::fn_is_completed));
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

// The OpLog payload carries the envelope's own fields and no checksum, so this
// case pins the length the part holds and the fields beside it.
TEST_F(PartPayloadTest, TheOplogPayloadCarriesTheEnvelopeFields) {
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
