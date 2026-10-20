// master_service_part_scenario_test.cpp
//
// The part layer, driven through MasterService directly with the feature flag
// on: writing, reading, the lifecycle and repair of an object written in
// several parts, and the drain and eviction paths that treat it as one object.

#include "master_service.h"
#include "master_service/master_service_test_peer.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <cctype>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "master_service_test_fixture.h"
#include "types.h"

namespace mooncake::test {
namespace {

constexpr uint64_t kPartLength = 512;
constexpr const char* kSecondSegment = "part_scenario_second_segment";

MasterServiceConfig PartScenarioConfig() {
    MasterServiceConfig config;
    config.enable_multi_part_objects = true;
    return config;
}

PartWriteRequest PartConfig(uint32_t part_index, uint32_t part_count,
                            const std::string& preferred_segment = {}) {
    PartWriteRequest request;
    request.part_index = part_index;
    request.part_count = part_count;
    request.config.preferred_segment = preferred_segment;
    return request;
}

PartEndRequest PartEndMeta(const std::string& key, uint32_t part_index,
                           std::vector<ReplicaID> replica_ids,
                           std::optional<uint64_t> checksum = std::nullopt) {
    PartEndRequest request;
    request.key = key;
    request.part_index = part_index;
    request.replica_ids = std::move(replica_ids);
    request.object_checksum = checksum;
    return request;
}

std::vector<ReplicaID> ReplicaIdsOf(
    const std::vector<Replica::Descriptor>& descriptors) {
    std::vector<ReplicaID> ids;
    ids.reserve(descriptors.size());
    for (const auto& descriptor : descriptors) {
        ids.push_back(descriptor.id);
    }
    return ids;
}

// A call that unexpectedly succeeded fails the case here instead of tripping
// the library's own assertion on error().
template <typename T>
ErrorCode ErrorOf(const tl::expected<T, ErrorCode>& result) {
    EXPECT_FALSE(result.has_value());
    return result.has_value() ? ErrorCode::OK : result.error();
}

void SweepMultiPartObjectsAt(MasterService& service,
                             std::chrono::system_clock::time_point now) {
    MasterServiceTestPeer peer(service);
    for (size_t shard_index = 0;
         shard_index < MasterServiceTestPeer::MetadataShards(service).size();
         ++shard_index) {
        MasterServiceTestPeer::MetadataShardAccessorRW shard(&service,
                                                             shard_index);
        peer.SweepMultiPartObjects(shard, now);
    }
}

}  // namespace

class MasterServicePartScenarioTest : public MasterServiceTest {
   protected:
    void SetUp() override {
        service_ = std::make_unique<MasterService>(PartScenarioConfig());
        first_ = PrepareSimpleSegment(*service_);
        // The second segment gets its own address range, so a part written on
        // it really is a replica of another segment.
        second_ =
            PrepareSimpleSegment(*service_, kSecondSegment,
                                 kDefaultSegmentBase + kDefaultSegmentSize);
    }

    void TearDown() override { service_.reset(); }

    // Writes an object whose parts are all ended, so the object is readable.
    void WriteObjectInParts(const std::string& key, uint64_t length_zero,
                            uint64_t length_one) {
        const TenantId tenant = TenantId::Default();
        auto part_zero = service_->PutStart(first_.client_id, key, tenant,
                                            length_zero, PartConfig(0, 2));
        ASSERT_TRUE(part_zero.has_value());
        auto part_one = service_->PutStart(second_.client_id, key, tenant,
                                           length_one, PartConfig(1, 2));
        ASSERT_TRUE(part_one.has_value());
        ASSERT_TRUE(
            service_
                ->PutEnd(first_.client_id,
                         PartEndMeta(key, 0, ReplicaIdsOf(part_zero.value())),
                         tenant, ReplicaType::ALL)
                .has_value());
        ASSERT_TRUE(
            service_
                ->PutEnd(second_.client_id,
                         PartEndMeta(key, 1, ReplicaIdsOf(part_one.value())),
                         tenant, ReplicaType::ALL)
                .has_value());
    }

    // The object lost a part: that part's replicas are gone, and both parts'
    // write records stand for attempts whose writers stopped long ago.
    void LosePart(const std::string& key, size_t part_index) {
        MasterServiceTestPeer::MetadataAccessorRW accessor(
            service_.get(),
            MasterServiceTestPeer::ObjectIdentity{TenantId::Default(), key});
        ASSERT_TRUE(accessor.Exists());
        ObjectPart* part = accessor.Get().MutablePart(part_index);
        ASSERT_NE(part, nullptr);
        part->PopReplicas();
        const auto stale =
            std::chrono::system_clock::now() -
            std::chrono::seconds(2 * DEFAULT_PUT_START_RELEASE_TIMEOUT);
        accessor.Get().BeginPartWrite(0, first_.client_id, stale);
        accessor.Get().BeginPartWrite(1, second_.client_id, stale);
    }

    // Whether the envelope is still there, readable or not: a part that was
    // lost leaves the object without content but keeps its metadata.
    bool HoldsMetadata(const std::string& key) {
        MasterServiceTestPeer::MetadataAccessorRO accessor(
            service_.get(),
            MasterServiceTestPeer::ObjectIdentity{TenantId::Default(), key});
        return accessor.Exists();
    }

    std::unique_ptr<MasterService> service_;
    MountedSegmentContext first_;
    MountedSegmentContext second_;
};

TEST_F(MasterServicePartScenarioTest,
       PartsArrivingOutOfOrderAreNotReadableUntilTheLastEnd) {
    const TenantId tenant = TenantId::Default();
    const std::string key = "out_of_order_key";
    const UUID third_writer = generate_uuid();

    // The writers arrive in the order 2, 0, 1.
    auto part_two = service_->PutStart(third_writer, key, tenant, kPartLength,
                                       PartConfig(2, 3, kSecondSegment));
    ASSERT_TRUE(part_two.has_value());
    auto part_zero = service_->PutStart(first_.client_id, key, tenant,
                                        kPartLength, PartConfig(0, 3));
    ASSERT_TRUE(part_zero.has_value());
    auto part_one = service_->PutStart(second_.client_id, key, tenant,
                                       kPartLength, PartConfig(1, 3));
    ASSERT_TRUE(part_one.has_value());

    ASSERT_TRUE(
        service_
            ->PutEnd(third_writer,
                     PartEndMeta(key, 2, ReplicaIdsOf(part_two.value())),
                     tenant, ReplicaType::ALL)
            .has_value());
    ASSERT_TRUE(
        service_
            ->PutEnd(first_.client_id,
                     PartEndMeta(key, 0, ReplicaIdsOf(part_zero.value())),
                     tenant, ReplicaType::ALL)
            .has_value());

    EXPECT_FALSE(service_->ExistKey(key, tenant).value_or(true));
    EXPECT_EQ(ErrorOf(service_->GetReplicaListForPart(key, tenant, 1)),
              ErrorCode::REPLICA_IS_NOT_READY);

    ASSERT_TRUE(
        service_
            ->PutEnd(second_.client_id,
                     PartEndMeta(key, 1, ReplicaIdsOf(part_one.value())),
                     tenant, ReplicaType::ALL)
            .has_value());
    EXPECT_TRUE(service_->ExistKey(key, tenant).value_or(false));
}

TEST_F(MasterServicePartScenarioTest, APartReadAnswersOnlyForThatPart) {
    const TenantId tenant = TenantId::Default();
    const std::string key = "part_read_key";
    const UUID first_writer = generate_uuid();
    const UUID second_writer = generate_uuid();

    auto part_zero = service_->PutStart(first_writer, key, tenant, kPartLength,
                                        PartConfig(0, 2));
    ASSERT_TRUE(part_zero.has_value());
    auto part_one = service_->PutStart(second_writer, key, tenant, kPartLength,
                                       PartConfig(1, 2, kSecondSegment));
    ASSERT_TRUE(part_one.has_value());

    ASSERT_TRUE(
        service_
            ->PutEnd(first_writer,
                     PartEndMeta(key, 0, ReplicaIdsOf(part_zero.value()), 40),
                     tenant, ReplicaType::ALL)
            .has_value());
    ASSERT_TRUE(
        service_
            ->PutEnd(second_writer,
                     PartEndMeta(key, 1, ReplicaIdsOf(part_one.value()), 41),
                     tenant, ReplicaType::ALL)
            .has_value());

    auto first_read = service_->GetReplicaListForPart(key, tenant, 0);
    ASSERT_TRUE(first_read.has_value());
    EXPECT_EQ(first_read->part_count.value_or(0), 2u);
    EXPECT_EQ(first_read->object_checksum.value_or(0), 40u);
    EXPECT_EQ(ReplicaIdsOf(first_read->replicas),
              ReplicaIdsOf(part_zero.value()));

    auto second_read = service_->GetReplicaListForPart(key, tenant, 1);
    ASSERT_TRUE(second_read.has_value());
    EXPECT_EQ(second_read->part_count.value_or(0), 2u);
    EXPECT_EQ(second_read->object_checksum.value_or(0), 41u);
    EXPECT_EQ(ReplicaIdsOf(second_read->replicas),
              ReplicaIdsOf(part_one.value()));

    // A part outside the object does not exist. A read that names no part is
    // refused as well, which the batch and admin reads cover below.
    EXPECT_EQ(ErrorOf(service_->GetReplicaListForPart(key, tenant, 2)),
              ErrorCode::INVALID_PARAMS);
}

TEST_F(MasterServicePartScenarioTest,
       ALateEndAfterThePartWasReallocatedIsRejected) {
    const TenantId tenant = TenantId::Default();
    const std::string key = "late_end_key";
    const UUID writer = generate_uuid();

    auto first_attempt =
        service_->PutStart(writer, key, tenant, kPartLength, PartConfig(0, 2));
    ASSERT_TRUE(first_attempt.has_value());
    const std::vector<ReplicaID> stale_ids =
        ReplicaIdsOf(first_attempt.value());

    // The writer goes quiet past its discard timeout: the part keeps its
    // PROCESSING replicas but the attempt is old enough to be replaced.
    const auto stale =
        std::chrono::system_clock::now() -
        std::chrono::seconds(DEFAULT_PUT_START_DISCARD_TIMEOUT + 60);
    {
        MasterServiceTestPeer::MetadataAccessorRW accessor(
            service_.get(), MasterServiceTestPeer::ObjectIdentity{tenant, key});
        ASSERT_TRUE(accessor.Exists());
        accessor.Get().BeginPartWrite(0, writer, stale);
    }

    auto second_attempt =
        service_->PutStart(writer, key, tenant, kPartLength, PartConfig(0, 2));
    ASSERT_TRUE(second_attempt.has_value());
    const std::vector<ReplicaID> current_ids =
        ReplicaIdsOf(second_attempt.value());
    EXPECT_NE(current_ids.front(), stale_ids.front());

    // The abandoned attempt's End names replicas it no longer owns, so it
    // completes nothing and the part's replicas stay as they are.
    EXPECT_EQ(ErrorOf(service_->PutEnd(writer, PartEndMeta(key, 0, stale_ids),
                                       tenant, ReplicaType::ALL)),
              ErrorCode::INVALID_WRITE);
    {
        MasterServiceTestPeer::MetadataAccessorRO accessor(
            service_.get(), MasterServiceTestPeer::ObjectIdentity{tenant, key});
        ASSERT_TRUE(accessor.Exists());
        ASSERT_NE(accessor.Get().GetPart(0), nullptr);
        EXPECT_TRUE(accessor.Get().GetPart(0)->HasOnlyProcessingReplicas());
    }

    EXPECT_TRUE(service_
                    ->PutEnd(writer, PartEndMeta(key, 0, current_ids), tenant,
                             ReplicaType::ALL)
                    .has_value());
    {
        MasterServiceTestPeer::MetadataAccessorRO accessor(
            service_.get(), MasterServiceTestPeer::ObjectIdentity{tenant, key});
        ASSERT_TRUE(accessor.Exists());
        ASSERT_NE(accessor.Get().GetPart(0), nullptr);
        EXPECT_TRUE(std::ranges::all_of(accessor.Get().GetPart(0)->replicas(),
                                        &Replica::fn_is_completed));
    }
    // The object still waits for its other part, so it is not readable yet.
    EXPECT_FALSE(service_->ExistKey(key, tenant).value_or(true));
}

TEST_F(MasterServicePartScenarioTest, RevokingOnePartLeavesTheOthers) {
    const TenantId tenant = TenantId::Default();
    const std::string key = "revoke_one_part_key";
    const UUID writer = generate_uuid();

    auto part_zero =
        service_->PutStart(writer, key, tenant, kPartLength, PartConfig(0, 2));
    ASSERT_TRUE(part_zero.has_value());
    ASSERT_TRUE(
        service_
            ->PutEnd(writer,
                     PartEndMeta(key, 0, ReplicaIdsOf(part_zero.value())),
                     tenant, ReplicaType::ALL)
            .has_value());
    auto part_one = service_->PutStart(writer, key, tenant, kPartLength,
                                       PartConfig(1, 2, kSecondSegment));
    ASSERT_TRUE(part_one.has_value());

    ASSERT_TRUE(service_
                    ->PutRevokeForPart(writer, key, tenant, 1,
                                       ReplicaIdsOf(part_one.value()),
                                       ReplicaType::ALL)
                    .has_value());

    {
        MasterServiceTestPeer::MetadataAccessorRO accessor(
            service_.get(), MasterServiceTestPeer::ObjectIdentity{tenant, key});
        ASSERT_TRUE(accessor.Exists());
        EXPECT_EQ(accessor.Get().GetPart(1), nullptr);
        ASSERT_NE(accessor.Get().GetPart(0), nullptr);
        EXPECT_TRUE(std::ranges::all_of(accessor.Get().GetPart(0)->replicas(),
                                        &Replica::fn_is_completed));
    }

    // The part can be written again, and that makes the object readable.
    auto retry = service_->PutStart(writer, key, tenant, kPartLength,
                                    PartConfig(1, 2, kSecondSegment));
    ASSERT_TRUE(retry.has_value());
    ASSERT_TRUE(service_
                    ->PutEnd(writer,
                             PartEndMeta(key, 1, ReplicaIdsOf(retry.value())),
                             tenant, ReplicaType::ALL)
                    .has_value());
    EXPECT_TRUE(service_->ExistKey(key, tenant).value_or(false));
}

TEST_F(MasterServicePartScenarioTest,
       LosingOnePartMakesTheObjectUnreadableWhileItsLeaseHolds) {
    const TenantId tenant = TenantId::Default();
    const std::string key = "lost_part_key";
    const UUID first_writer = generate_uuid();
    const UUID second_writer = generate_uuid();

    // Each part names the segment it lives on, so unmounting the second segment
    // takes part 1 alone and part 0 keeps the replica the object survives on.
    auto part_zero = service_->PutStart(first_writer, key, tenant, kPartLength,
                                        PartConfig(0, 2, "test_segment"));
    ASSERT_TRUE(part_zero.has_value());
    auto part_one = service_->PutStart(second_writer, key, tenant, kPartLength,
                                       PartConfig(1, 2, kSecondSegment));
    ASSERT_TRUE(part_one.has_value());
    ASSERT_TRUE(
        service_
            ->PutEnd(first_writer,
                     PartEndMeta(key, 0, ReplicaIdsOf(part_zero.value())),
                     tenant, ReplicaType::ALL)
            .has_value());
    ASSERT_TRUE(
        service_
            ->PutEnd(second_writer,
                     PartEndMeta(key, 1, ReplicaIdsOf(part_one.value())),
                     tenant, ReplicaType::ALL)
            .has_value());
    EXPECT_TRUE(service_->ExistKey(key, tenant).value_or(false));

    // A reader takes the object, which grants the object's lease.
    ASSERT_TRUE(service_->GetReplicaListForPart(key, tenant, 0).has_value());

    // Remove part 1 through the segment-offline path. Pause the asynchronous
    // cleanup so the test can observe both the immediate read shield and the
    // later metadata removal explicitly.
    PauseReplicaCleanup(*service_);
    ASSERT_TRUE(service_->UnmountSegment(second_.segment_id, second_.client_id)
                    .has_value());
    EXPECT_EQ(ErrorOf(service_->GetReplicaListForPart(key, tenant, 1)),
              ErrorCode::REPLICA_IS_NOT_READY);
    ClearInvalidHandlesForTest(*service_);
    ResumeReplicaCleanup(*service_);

    // No partial hit: the object is unreadable at once, and every part read
    // says so.
    EXPECT_FALSE(service_->ExistKey(key, tenant).value_or(true));
    EXPECT_EQ(ErrorOf(service_->GetReplicaListForPart(key, tenant, 1)),
              ErrorCode::REPLICA_IS_NOT_READY);
    EXPECT_EQ(ErrorOf(service_->GetReplicaListForPart(key, tenant, 0)),
              ErrorCode::REPLICA_IS_NOT_READY);

    {
        MasterServiceTestPeer::MetadataAccessorRO accessor(
            service_.get(), MasterServiceTestPeer::ObjectIdentity{tenant, key});
        ASSERT_TRUE(accessor.Exists());
        const ObjectMetadata& metadata = accessor.Get();
        ASSERT_NE(metadata.GetPart(1), nullptr);
        EXPECT_TRUE(metadata.GetPart(1)->replicas().empty());
        ASSERT_NE(metadata.GetPart(0), nullptr);
        EXPECT_GT(metadata.EvictionDeadline(),
                  std::chrono::system_clock::now());
    }

    // While the lease the read granted still holds, the incompleteness sweep
    // leaves the object and the replica it protects alone. Both parts' write
    // records are moved past the release timeout, so only the lease stands
    // between the object and the sweep: the check below pins the ordering the
    // sweep reasons about before it runs.
    const auto now = std::chrono::system_clock::now();
    const auto stale =
        now - std::chrono::seconds(DEFAULT_PUT_START_RELEASE_TIMEOUT + 60);
    {
        MasterServiceTestPeer::MetadataAccessorRW accessor(
            service_.get(), MasterServiceTestPeer::ObjectIdentity{tenant, key});
        ASSERT_TRUE(accessor.Exists());
        accessor.Get().BeginPartWrite(0, first_writer, stale);
        accessor.Get().BeginPartWrite(1, second_writer, stale);
    }
    {
        MasterServiceTestPeer::MetadataAccessorRO accessor(
            service_.get(), MasterServiceTestPeer::ObjectIdentity{tenant, key});
        ASSERT_TRUE(accessor.Exists());
        const ObjectMetadata& metadata = accessor.Get();
        ASSERT_LT(metadata.LastPutStartTime() +
                      std::chrono::seconds(DEFAULT_PUT_START_RELEASE_TIMEOUT),
                  now);
        ASSERT_GT(metadata.EvictionDeadline(), now);
    }
    SweepMultiPartObjectsAt(*service_, now);
    EXPECT_TRUE(HoldsMetadata(key));
}

TEST_F(MasterServicePartScenarioTest,
       IncompleteMultiPartObjectIsReclaimedAfterItsReleaseTimeout) {
    const TenantId tenant = TenantId::Default();
    const UUID writer = generate_uuid();
    const std::string key = "reclaimed_part_key";

    auto start =
        service_->PutStart(writer, key, tenant, kPartLength, PartConfig(0, 2));
    ASSERT_TRUE(start.has_value());
    ASSERT_TRUE(service_
                    ->PutEnd(writer,
                             PartEndMeta(key, 0, ReplicaIdsOf(start.value())),
                             tenant, ReplicaType::ALL)
                    .has_value());
    // Part 1 never arrives, and part 0's attempt is long past its release
    // timeout with nobody reading the object.
    const auto stale =
        std::chrono::system_clock::now() -
        std::chrono::seconds(DEFAULT_PUT_START_RELEASE_TIMEOUT + 60);
    {
        MasterServiceTestPeer::MetadataAccessorRW accessor(
            service_.get(), MasterServiceTestPeer::ObjectIdentity{tenant, key});
        ASSERT_TRUE(accessor.Exists());
        accessor.Get().BeginPartWrite(0, writer, stale);
    }

    // A second object is still waiting for parts that may yet arrive, and an
    // ordinary object has one part: the sweep reclaims the first object only.
    ASSERT_TRUE(service_
                    ->PutStart(writer, "still_arriving_key", tenant,
                               kPartLength, PartConfig(0, 2))
                    .has_value());
    ASSERT_TRUE(service_
                    ->PutStart(writer, "one_part_object", tenant, kPartLength,
                               ReplicateConfig{})
                    .has_value());
    SweepMultiPartObjectsAt(*service_, std::chrono::system_clock::now());

    EXPECT_FALSE(HoldsMetadata(key));
    // The two other objects are still there, each with the part its writer has
    // not finished, so the sweep reclaimed the first object alone.
    EXPECT_EQ(ErrorOf(service_->GetReplicaListForPart("still_arriving_key",
                                                      tenant, 0)),
              ErrorCode::REPLICA_IS_NOT_READY);
    EXPECT_EQ(
        ErrorOf(service_->GetReplicaListForPart("one_part_object", tenant, 0)),
        ErrorCode::REPLICA_IS_NOT_READY);
    EXPECT_FALSE(service_->ExistKey(key, tenant).value_or(true));
}

TEST_F(MasterServicePartScenarioTest,
       ExpiredPartAttemptLeavesCompletedPartsInPlace) {
    const TenantId tenant = TenantId::Default();
    const std::string key = "expired_part_attempt_key";
    const UUID writer = generate_uuid();

    auto part_zero =
        service_->PutStart(writer, key, tenant, kPartLength, PartConfig(0, 2));
    ASSERT_TRUE(part_zero.has_value());
    ASSERT_TRUE(
        service_
            ->PutEnd(writer,
                     PartEndMeta(key, 0, ReplicaIdsOf(part_zero.value())),
                     tenant, ReplicaType::ALL)
            .has_value());
    ASSERT_TRUE(service_
                    ->PutStart(writer, key, tenant, kPartLength,
                               PartConfig(1, 2, kSecondSegment))
                    .has_value());

    const auto stale =
        std::chrono::system_clock::now() -
        std::chrono::seconds(DEFAULT_PUT_START_RELEASE_TIMEOUT + 60);
    {
        MasterServiceTestPeer::MetadataAccessorRW accessor(
            service_.get(), MasterServiceTestPeer::ObjectIdentity{tenant, key});
        ASSERT_TRUE(accessor.Exists());
        accessor.Get().BeginPartWrite(1, writer, stale);
    }

    MasterServiceTestPeer peer(*service_);
    for (size_t shard_index = 0;
         shard_index < MasterServiceTestPeer::MetadataShards(*service_).size();
         ++shard_index) {
        MasterServiceTestPeer::MetadataShardAccessorRW shard(service_.get(),
                                                             shard_index);
        peer.DiscardExpiredProcessingReplicas(shard,
                                              std::chrono::system_clock::now());
    }

    MasterServiceTestPeer::MetadataAccessorRO accessor(
        service_.get(), MasterServiceTestPeer::ObjectIdentity{tenant, key});
    ASSERT_TRUE(accessor.Exists());
    EXPECT_EQ(accessor.Get().GetPart(1), nullptr);
    ASSERT_NE(accessor.Get().GetPart(0), nullptr);
    EXPECT_TRUE(std::ranges::all_of(accessor.Get().GetPart(0)->replicas(),
                                    &Replica::fn_is_completed));
}

TEST_F(MasterServicePartScenarioTest,
       AReadOfAPartNothingWasWrittenToIsNotReady) {
    const TenantId tenant = TenantId::Default();
    const std::string key = "unwritten_part_key";
    const UUID first_writer = generate_uuid();
    const UUID second_writer = generate_uuid();

    // Only part 0 is written, and that write is finished, so part 1 holds no
    // content at all.
    auto part_zero = service_->PutStart(first_writer, key, tenant, kPartLength,
                                        PartConfig(0, 2));
    ASSERT_TRUE(part_zero.has_value());
    ASSERT_TRUE(
        service_
            ->PutEnd(first_writer,
                     PartEndMeta(key, 0, ReplicaIdsOf(part_zero.value())),
                     tenant, ReplicaType::ALL)
            .has_value());
    EXPECT_EQ(ErrorOf(service_->GetReplicaListForPart(key, tenant, 1)),
              ErrorCode::REPLICA_IS_NOT_READY);

    // And a part whose writer revoked its attempt holds nothing either.
    auto part_one = service_->PutStart(second_writer, key, tenant, kPartLength,
                                       PartConfig(1, 2, kSecondSegment));
    ASSERT_TRUE(part_one.has_value());
    ASSERT_TRUE(service_
                    ->PutRevokeForPart(second_writer, key, tenant, 1,
                                       ReplicaIdsOf(part_one.value()),
                                       ReplicaType::ALL)
                    .has_value());
    EXPECT_EQ(ErrorOf(service_->GetReplicaListForPart(key, tenant, 1)),
              ErrorCode::REPLICA_IS_NOT_READY);

    // The revoked part can be written again from scratch.
    {
        MasterServiceTestPeer::MetadataAccessorRO accessor(
            service_.get(), MasterServiceTestPeer::ObjectIdentity{tenant, key});
        ASSERT_TRUE(accessor.Exists());
        EXPECT_EQ(accessor.Get().PartCount(), 2u);
    }
    EXPECT_TRUE(service_
                    ->PutStart(second_writer, key, tenant, kPartLength,
                               PartConfig(1, 2, kSecondSegment))
                    .has_value());
}

TEST_F(MasterServicePartScenarioTest,
       ALegacyWriteCannotTakeOverAnObjectWrittenInSeveralParts) {
    const TenantId tenant = TenantId::Default();
    const std::string key = "layout_conflict_key";
    const UUID writer = generate_uuid();

    // The object is created by a writer of part 1, so part 0 does not exist
    // yet.
    auto part_one = service_->PutStart(writer, key, tenant, kPartLength,
                                       PartConfig(1, 2, kSecondSegment));
    ASSERT_TRUE(part_one.has_value());

    // An ordinary write names no part and cannot join it.
    EXPECT_EQ(ErrorOf(service_->PutStart(writer, key, tenant, kPartLength,
                                         ReplicateConfig{})),
              ErrorCode::INVALID_PARAMS);
    // A scope of one part against an object of two is a different object too.
    EXPECT_EQ(ErrorOf(service_->PutStart(writer, key, tenant, kPartLength,
                                         PartConfig(0, 1))),
              ErrorCode::INVALID_PARAMS);
    EXPECT_EQ(ErrorOf(service_->UpsertStart(writer, key, tenant, kPartLength,
                                            ReplicateConfig{})),
              ErrorCode::INVALID_PARAMS);

    // The reverse direction is refused as well: an ordinary object keeps one
    // part.
    const std::string legacy_key = "layout_conflict_legacy_key";
    ASSERT_TRUE(service_
                    ->PutStart(writer, legacy_key, tenant, kPartLength,
                               ReplicateConfig{})
                    .has_value());
    EXPECT_EQ(ErrorOf(service_->PutStart(writer, legacy_key, tenant,
                                         kPartLength, PartConfig(0, 2))),
              ErrorCode::INVALID_PARAMS);
}

// The part-scoped revoke names its attempt for an object written in several
// parts; the ordinary entry point below reads the attempt from the object. Both
// cases are kept: a caller of one never reaches the other.
TEST_F(MasterServicePartScenarioTest,
       ARevokeFromAnAttemptThatLostThePartChangesNothing) {
    const TenantId tenant = TenantId::Default();
    const std::string key = "stale_revoke_key";
    const UUID writer = generate_uuid();

    auto first_attempt =
        service_->PutStart(writer, key, tenant, kPartLength, PartConfig(0, 2));
    ASSERT_TRUE(first_attempt.has_value());
    const std::vector<ReplicaID> stale_ids =
        ReplicaIdsOf(first_attempt.value());

    // The attempt goes quiet past its discard timeout, and the same writer
    // takes the part again with new replicas.
    const auto stale =
        std::chrono::system_clock::now() -
        std::chrono::seconds(DEFAULT_PUT_START_DISCARD_TIMEOUT + 60);
    {
        MasterServiceTestPeer::MetadataAccessorRW accessor(
            service_.get(), MasterServiceTestPeer::ObjectIdentity{tenant, key});
        ASSERT_TRUE(accessor.Exists());
        accessor.Get().BeginPartWrite(0, writer, stale);
    }
    auto second_attempt =
        service_->PutStart(writer, key, tenant, kPartLength, PartConfig(0, 2));
    ASSERT_TRUE(second_attempt.has_value());
    const std::vector<ReplicaID> current_ids =
        ReplicaIdsOf(second_attempt.value());
    EXPECT_NE(current_ids.front(), stale_ids.front());

    // The old attempt's revoke names replicas the part no longer holds, so it
    // changes nothing: the current attempt keeps its replicas and its writer.
    EXPECT_EQ(ErrorOf(service_->PutRevokeForPart(writer, key, tenant, 0,
                                                 stale_ids, ReplicaType::ALL)),
              ErrorCode::INVALID_WRITE);
    {
        MasterServiceTestPeer::MetadataAccessorRO accessor(
            service_.get(), MasterServiceTestPeer::ObjectIdentity{tenant, key});
        ASSERT_TRUE(accessor.Exists());
        const ObjectMetadata& metadata = accessor.Get();
        ASSERT_NE(metadata.GetPart(0), nullptr);
        std::vector<ReplicaID> held_ids;
        for (const auto& replica : metadata.GetPart(0)->replicas()) {
            held_ids.push_back(replica.id());
        }
        EXPECT_EQ(held_ids, current_ids);
        EXPECT_TRUE(metadata.GetPart(0)->HasOnlyProcessingReplicas());
    }
    EXPECT_TRUE(service_
                    ->PutEnd(writer, PartEndMeta(key, 0, current_ids), tenant,
                             ReplicaType::ALL)
                    .has_value());
}

TEST_F(MasterServicePartScenarioTest, APartEndNamesItsOwnAttempt) {
    const TenantId tenant = TenantId::Default();
    const std::string key = "end_scope_key";
    const UUID writer = generate_uuid();

    // The part carries two replicas, so an End can name a subset of the
    // attempt's replicas as well as a foreign one.
    PartWriteRequest part_config = PartConfig(0, 2);
    part_config.config.replica_num = 2;
    auto part =
        service_->PutStart(writer, key, tenant, kPartLength, part_config);
    ASSERT_TRUE(part.has_value());
    const std::vector<ReplicaID> ids = ReplicaIdsOf(part.value());
    ASSERT_EQ(ids.size(), 2u);

    // An End without a replica scope would complete whatever is PROCESSING in
    // the part, which after a timeout reallocation belongs to another writer.
    EXPECT_EQ(ErrorOf(service_->PutEnd(writer, ObjectMeta{key, std::nullopt},
                                       tenant, ReplicaType::ALL)),
              ErrorCode::INVALID_PARAMS);
    EXPECT_EQ(ErrorOf(service_->PutEnd(writer, PartEndMeta(key, 0, {}), tenant,
                                       ReplicaType::ALL)),
              ErrorCode::INVALID_PARAMS);

    // An End that names another attempt's replica changes nothing.
    EXPECT_EQ(ErrorOf(service_->PutEnd(
                  writer, PartEndMeta(key, 0, {ids.front() + 1000}), tenant,
                  ReplicaType::ALL)),
              ErrorCode::INVALID_WRITE);
    // One that names only part of the attempt would leave the rest of it
    // behind, so it is refused as well.
    EXPECT_EQ(
        ErrorOf(service_->PutEnd(writer, PartEndMeta(key, 0, {ids.front()}),
                                 tenant, ReplicaType::ALL)),
        ErrorCode::INVALID_WRITE);
    {
        MasterServiceTestPeer::MetadataAccessorRO accessor(
            service_.get(), MasterServiceTestPeer::ObjectIdentity{tenant, key});
        ASSERT_TRUE(accessor.Exists());
        ASSERT_NE(accessor.Get().GetPart(0), nullptr);
        EXPECT_EQ(accessor.Get().GetPart(0)->replicas().size(), 2u);
        EXPECT_TRUE(accessor.Get().GetPart(0)->HasOnlyProcessingReplicas());
    }

    // The attempt's own set completes the part, and its retry is a no-op.
    ASSERT_TRUE(
        service_
            ->PutEnd(writer, PartEndMeta(key, 0, ids), tenant, ReplicaType::ALL)
            .has_value());
    EXPECT_TRUE(
        service_
            ->PutEnd(writer, PartEndMeta(key, 0, ids), tenant, ReplicaType::ALL)
            .has_value());
    EXPECT_EQ(ErrorOf(service_->PutRevokeForPart(writer, key, tenant, 0, ids,
                                                 ReplicaType::ALL)),
              ErrorCode::INVALID_WRITE);
    {
        MasterServiceTestPeer::MetadataAccessorRO accessor(
            service_.get(), MasterServiceTestPeer::ObjectIdentity{tenant, key});
        ASSERT_TRUE(accessor.Exists());
        ASSERT_NE(accessor.Get().GetPart(0), nullptr);
        EXPECT_TRUE(std::ranges::all_of(accessor.Get().GetPart(0)->replicas(),
                                        &Replica::fn_is_completed));
    }
}

TEST_F(MasterServicePartScenarioTest,
       CopyAndMoveLeaveAnObjectWrittenInSeveralPartsAlone) {
    const TenantId tenant = TenantId::Default();
    const std::string key = "copy_multipart_key";
    const UUID first_writer = generate_uuid();
    const UUID second_writer = generate_uuid();

    auto part_zero = service_->PutStart(first_writer, key, tenant, kPartLength,
                                        PartConfig(0, 2));
    ASSERT_TRUE(part_zero.has_value());
    auto part_one = service_->PutStart(second_writer, key, tenant, kPartLength,
                                       PartConfig(1, 2, kSecondSegment));
    ASSERT_TRUE(part_one.has_value());
    ASSERT_TRUE(
        service_
            ->PutEnd(first_writer,
                     PartEndMeta(key, 0, ReplicaIdsOf(part_zero.value())),
                     tenant, ReplicaType::ALL)
            .has_value());
    ASSERT_TRUE(
        service_
            ->PutEnd(second_writer,
                     PartEndMeta(key, 1, ReplicaIdsOf(part_one.value())),
                     tenant, ReplicaType::ALL)
            .has_value());
    EXPECT_TRUE(service_->ExistKey(key, tenant).value_or(false));

    // One part of the object lives on each segment, so a copy or a move that
    // took a part for the whole object would drop the other one. The caller is
    // the client that mounted the source segment, so the request gets past the
    // client checks and meets the object.
    EXPECT_EQ(ErrorOf(service_->CopyStart(second_.client_id, key, tenant,
                                          kSecondSegment, {kSecondSegment})),
              ErrorCode::INVALID_PARAMS);
    EXPECT_EQ(ErrorOf(service_->MoveStart(second_.client_id, key, tenant,
                                          kSecondSegment, "test_segment")),
              ErrorCode::INVALID_PARAMS);
    EXPECT_TRUE(service_->ExistKey(key, tenant).value_or(false));
}

TEST_F(MasterServicePartScenarioTest,
       AnEvictionPassReleasesEveryPartOfAnObject) {
    MasterServiceConfig config = PartScenarioConfig();
    config.enable_offload = true;
    config.offload_on_evict = true;
    config.default_kv_lease_ttl = 0;
    MasterService service{config};
    PrepareSimpleSegment(service);
    const TenantId tenant = TenantId::Default();
    const std::string key = "evicted_multipart_key";
    const UUID first_writer = generate_uuid();
    const UUID second_writer = generate_uuid();

    auto part_zero = service.PutStart(first_writer, key, tenant, kPartLength,
                                      PartConfig(0, 2));
    ASSERT_TRUE(part_zero.has_value());
    auto part_one = service.PutStart(second_writer, key, tenant, kPartLength,
                                     PartConfig(1, 2));
    ASSERT_TRUE(part_one.has_value());
    ASSERT_TRUE(
        service
            .PutEnd(first_writer,
                    PartEndMeta(key, 0, ReplicaIdsOf(part_zero.value())),
                    tenant, ReplicaType::ALL)
            .has_value());
    ASSERT_TRUE(service
                    .PutEnd(second_writer,
                            PartEndMeta(key, 1, ReplicaIdsOf(part_one.value())),
                            tenant, ReplicaType::ALL)
                    .has_value());
    EXPECT_TRUE(service.ExistKey(key, tenant).value_or(false));

    // An offload would mirror one part and leave the others to be released, so
    // the object is released whole instead.
    MasterServiceTestPeer peer(service);
    peer.RunBatchEvictForTesting(1.0, 1.0);
    {
        MasterServiceTestPeer::MetadataAccessorRO accessor(
            &service, MasterServiceTestPeer::ObjectIdentity{tenant, key});
        EXPECT_FALSE(accessor.Exists());
    }
    EXPECT_EQ(ErrorOf(service.GetReplicaListForPart(key, tenant, 0)),
              ErrorCode::OBJECT_NOT_FOUND);
    EXPECT_EQ(ErrorOf(service.GetReplicaListForPart(key, tenant, 1)),
              ErrorCode::OBJECT_NOT_FOUND);
}

TEST_F(MasterServicePartScenarioTest,
       AGroupEvictionTakesTheCompleteMemberAndLeavesTheIncompleteOne) {
    constexpr size_t kSegmentSize = 4 * 1024 * 1024;
    constexpr uint64_t kObjectLength = 2 * 1024 * 1024;
    MasterServiceConfig config = PartScenarioConfig();
    // Both members have to be eviction candidates, so their leases have to be
    // over by the time the pass runs.
    config.default_kv_lease_ttl = 0;
    MasterService service{config};
    [[maybe_unused]] const auto context =
        PrepareSimpleSegment(service, "part_group_eviction_segment",
                             kDefaultSegmentBase, kSegmentSize);
    const TenantId tenant = TenantId::Default();
    const UUID writer = generate_uuid();

    const std::string complete_key = "group_evict_complete_key";
    const std::string incomplete_key = "group_evict_incomplete_key";
    const std::string group_id = FindGroupIdOnDifferentShard(complete_key);

    ReplicateConfig complete_config;
    complete_config.replica_num = 1;
    complete_config.group_ids = std::vector<std::string>{group_id};
    PutCompletedObject(service, writer, complete_key, complete_config,
                       kObjectLength);

    // The same group holds an object whose second part never arrives.
    PartWriteRequest part_config;
    part_config.part_index = 0;
    part_config.part_count = 2;
    part_config.config = complete_config;
    auto partial = service.PutStart(writer, incomplete_key, tenant,
                                    kObjectLength / 2, part_config);
    ASSERT_TRUE(partial.has_value());
    ASSERT_TRUE(service
                    .PutEnd(writer,
                            PartEndMeta(incomplete_key, 0,
                                        ReplicaIdsOf(partial.value())),
                            tenant, ReplicaType::ALL)
                    .has_value());

    // Fill the segment so the pool eviction pass has to free memory. The zero
    // lease TTL makes both members eligible without a timing dependency.
    ReplicateConfig trigger_config;
    trigger_config.replica_num = 1;
    EXPECT_FALSE(service
                     .PutStart(writer, "group_evict_trigger_key", tenant,
                               kObjectLength, trigger_config)
                     .has_value());
    WaitUntil(
        [&] { return !service.ExistKey(complete_key, tenant).value_or(true); });

    EXPECT_FALSE(service.ExistKey(complete_key, tenant).value_or(true));
    {
        MasterServiceTestPeer::MetadataAccessorRO accessor(
            &service,
            MasterServiceTestPeer::ObjectIdentity{tenant, incomplete_key});
        EXPECT_TRUE(accessor.Exists());
    }
}

// Batch and admin reads name no part, so an object written in several parts has
// no single slice for them to answer with.
TEST_F(MasterServicePartScenarioTest,
       BatchAndAdminReadsRefuseAnObjectWrittenInSeveralParts) {
    const TenantId tenant = TenantId::Default();
    const std::string key = "batch_read_part_key";
    WriteObjectInParts(key, kPartLength, kPartLength);

    EXPECT_EQ(service_->GetReplicaList(key, tenant).error(),
              ErrorCode::INVALID_PARAMS);
    auto batch = service_->BatchGetReplicaList({key}, tenant);
    ASSERT_EQ(batch.size(), 1u);
    EXPECT_EQ(batch[0].error(), ErrorCode::INVALID_PARAMS);
    EXPECT_EQ(service_->GetReplicaListForAdmin(key, tenant).error(),
              ErrorCode::INVALID_PARAMS);
    auto by_regex = service_->GetReplicaListByRegex(key, tenant);
    ASSERT_TRUE(by_regex.has_value());
    EXPECT_TRUE(by_regex->empty());

    // The part-scoped read is the one that answers, and it reports how many
    // parts the object has.
    auto part_read = service_->GetReplicaListForPart(key, tenant, 1);
    ASSERT_TRUE(part_read.has_value());
    EXPECT_EQ(part_read->part_count.value_or(0), 2u);
}

// RemoveAll reports the bytes it freed on one line of its own, and parts of one
// object have different lengths, so the report is the sum over its parts. The
// number has no other reader, so reading it out of the log is a stand-in until
// RemoveAll reports it as a value the caller gets back.
class RemoveAllFreedBytesSink : public google::LogSink {
   public:
    void send(google::LogSeverity severity, const char* full_filename,
              const char* base_filename, int line, const struct ::tm* tm_time,
              const char* message, size_t message_len) override {
        (void)severity;
        (void)full_filename;
        (void)base_filename;
        (void)line;
        (void)tm_time;

        const std::string text(message, message_len);
        if (text.find("action=remove_all_objects") == std::string::npos) {
            return;
        }
        constexpr char kField[] = "total_freed_size=";
        const size_t field_at = text.find(kField);
        if (field_at == std::string::npos) {
            return;
        }
        size_t digit_at = field_at + sizeof(kField) - 1;
        uint64_t bytes = 0;
        while (digit_at < text.size() &&
               std::isdigit(static_cast<unsigned char>(text[digit_at]))) {
            bytes = bytes * 10 + static_cast<uint64_t>(text[digit_at] - '0');
            ++digit_at;
        }
        freed_bytes_ = bytes;
    }

    std::optional<uint64_t> freed_bytes() const { return freed_bytes_; }

   private:
    std::optional<uint64_t> freed_bytes_;
};

// RemoveAll reports the line above only when verbose logging is on.
class VerboseLogForTest {
   public:
    explicit VerboseLogForTest(int level) : previous_(FLAGS_v) {
        FLAGS_v = level;
    }
    ~VerboseLogForTest() { FLAGS_v = previous_; }

    VerboseLogForTest(const VerboseLogForTest&) = delete;
    VerboseLogForTest& operator=(const VerboseLogForTest&) = delete;

   private:
    int previous_;
};

TEST_F(MasterServicePartScenarioTest, RemoveAllCountsEachPartsLength) {
    MasterService service{PartScenarioConfig()};
    const TenantId tenant = TenantId::Default();
    const std::string key = "two_lengths_key";
    PrepareSimpleSegment(service);
    const UUID writer = generate_uuid();
    auto part_zero =
        service.PutStart(writer, key, tenant, 512, PartConfig(0, 2));
    ASSERT_TRUE(part_zero.has_value());
    auto part_one =
        service.PutStart(writer, key, tenant, 2048, PartConfig(1, 2));
    ASSERT_TRUE(part_one.has_value());
    ASSERT_TRUE(
        service
            .PutEnd(writer,
                    PartEndMeta(key, 0, ReplicaIdsOf(part_zero.value())),
                    tenant, ReplicaType::ALL)
            .has_value());
    ASSERT_TRUE(service
                    .PutEnd(writer,
                            PartEndMeta(key, 1, ReplicaIdsOf(part_one.value())),
                            tenant, ReplicaType::ALL)
                    .has_value());

    RemoveAllFreedBytesSink sink;
    google::AddLogSink(&sink);
    long removed_count = 0;
    {
        VerboseLogForTest verbose(1);
        removed_count = service.RemoveAll(true);
    }
    google::RemoveLogSink(&sink);

    EXPECT_EQ(removed_count, 1);
    ASSERT_TRUE(sink.freed_bytes().has_value());
    EXPECT_EQ(*sink.freed_bytes(), 512 + 2048);
}

// Dynamic replication copies a whole object, so it leaves an object written in
// several parts alone: a read of one part must not ask for a copy of part 0,
// and a proposal that does name such an object is refused.
TEST_F(MasterServicePartScenarioTest,
       DynamicReplicationLeavesAnObjectWrittenInSeveralPartsAlone) {
    MasterServiceConfig config = PartScenarioConfig();
    // Only an enforcing controller accepts a proposal at all, and the heat
    // window it keeps shows whether a read asked for a copy.
    config.dynamic_replication_mode = "enforce";
    MasterService service{config};
    const auto first = PrepareSimpleSegment(service);
    const auto second = PrepareSimpleSegment(
        service, kSecondSegment, kDefaultSegmentBase + kDefaultSegmentSize);
    const TenantId tenant = TenantId::Default();
    const std::string key = "dynamic_replication_part_key";
    const UUID writer = generate_uuid();

    auto part_zero = service.PutStart(first.client_id, key, tenant, kPartLength,
                                      PartConfig(0, 2));
    ASSERT_TRUE(part_zero.has_value());
    auto part_one = service.PutStart(second.client_id, key, tenant, kPartLength,
                                     PartConfig(1, 2));
    ASSERT_TRUE(part_one.has_value());
    ASSERT_TRUE(
        service
            .PutEnd(first.client_id,
                    PartEndMeta(key, 0, ReplicaIdsOf(part_zero.value())),
                    tenant, ReplicaType::ALL)
            .has_value());
    ASSERT_TRUE(service
                    .PutEnd(second.client_id,
                            PartEndMeta(key, 1, ReplicaIdsOf(part_one.value())),
                            tenant, ReplicaType::ALL)
                    .has_value());

    ASSERT_TRUE(service.GetReplicaListForPart(key, tenant, 1).has_value());
    EXPECT_TRUE(
        MasterServiceTestPeer::DynamicReplicationWindows(service).empty());

    // The key is made hot the way a read would make it hot, so the refusal
    // below is the part scope rather than the heat admission.
    MasterServiceTestPeer peer(service);
    bool hot = false;
    for (uint32_t hit = 0; hit < peer.DynamicReplicationAdmissionMinHits();
         ++hit) {
        hot = peer.ObserveDynamicReplicationAccess(
            MasterServiceTestPeer::ObjectIdentity{tenant, key});
    }
    ASSERT_TRUE(hot);

    // A proposal is refused before a lease is taken or a pending task is
    // created, so nothing is left waiting for a timeout.
    ReplicaActionProposal proposal;
    proposal.action = ReplicaActionType::ADD;
    proposal.proposal_id = generate_uuid();
    proposal.tenant_id = tenant.value();
    proposal.key = key;
    proposal.expire_at_ms_epoch =
        MasterServiceTestPeer::DynamicReplicationNowMs() +
        std::chrono::seconds(30).count() * 1000;
    EXPECT_EQ(service.SubmitReplicaActionProposal(proposal).error(),
              ErrorCode::INVALID_PARAMS);
    EXPECT_FALSE(MasterServiceTestPeer::FindDynamicReplicationLease(
                     service, tenant, proposal.proposal_id)
                     .has_value());
}

TEST_F(MasterServicePartScenarioTest,
       APartScopeRefusesReplicaTypesOtherThanMemory) {
    const TenantId tenant = TenantId::Default();
    const UUID writer = generate_uuid();

    // A part holds memory replicas only: a nof, dfs or root-fs disk replica of
    // a part would describe it as one file with one length. So a part never
    // holds two replica types, and one End or one revoke cannot leave a replica
    // of another type behind.
    PartWriteRequest with_dfs = PartConfig(0, 2);
    with_dfs.config.dfs_replica_num = 1;
    EXPECT_EQ(ErrorOf(service_->PutStart(writer, "part_with_dfs_key", tenant,
                                         kPartLength, with_dfs)),
              ErrorCode::INVALID_PARAMS);

    PartWriteRequest with_nof = PartConfig(0, 2);
    with_nof.config.nof_replica_num = 1;
    EXPECT_EQ(ErrorOf(service_->PutStart(writer, "part_with_nof_key", tenant,
                                         kPartLength, with_nof)),
              ErrorCode::INVALID_PARAMS);

    // The memory replicas a part does hold are allocated as usual.
    EXPECT_TRUE(service_
                    ->PutStart(writer, "part_with_memory_key", tenant,
                               kPartLength, PartConfig(0, 2))
                    .has_value());
}

TEST_F(MasterServicePartScenarioTest,
       APartScopeRefusesTheDiskReplicaARootDirectoryAdds) {
    // A root directory makes every write of this service take a disk replica,
    // whatever the request asks for, so the refusal cannot rest on the
    // request's own replica counts.
    MasterServiceConfig config = PartScenarioConfig();
    config.root_fs_dir = (std::filesystem::temp_directory_path() /
                          ("master_part_root_fs_" + std::to_string(::getpid())))
                             .string();
    std::filesystem::create_directories(config.root_fs_dir);
    MasterService service{config};
    PrepareSimpleSegment(service);
    const TenantId tenant = TenantId::Default();
    const UUID writer = generate_uuid();

    PartWriteRequest part_two_of_two = PartConfig(0, 2);
    EXPECT_EQ(ErrorOf(service.PutStart(writer, "root_fs_part_key", tenant,
                                       kPartLength, part_two_of_two)),
              ErrorCode::INVALID_PARAMS);

    // The ordinary write of the same service does take both replicas, so the
    // refusal is about the part scope.
    auto whole = service.PutStart(writer, "root_fs_whole_key", tenant,
                                  kPartLength, ReplicateConfig{});
    ASSERT_TRUE(whole.has_value());
    ASSERT_EQ(whole->size(), 2u);
    EXPECT_TRUE(std::ranges::any_of(whole.value(), [](const auto& descriptor) {
        return descriptor.is_memory_replica();
    }));
    EXPECT_TRUE(std::ranges::any_of(whole.value(), [](const auto& descriptor) {
        return descriptor.is_disk_replica();
    }));
}

// The same stale revoke through the ordinary entry point, on an object with one
// part: the part-scoped case above does not reach this path.
TEST_F(MasterServicePartScenarioTest,
       ARevokeFromAnAttemptThatLostASinglePartAttemptChangesNothing) {
    const TenantId tenant = TenantId::Default();
    const std::string key = "stale_single_part_revoke_key";
    const UUID writer = generate_uuid();

    // The writer's first attempt names replicas that are gone by the time it
    // revokes them: the same writer took the object again with new ones.
    auto first_attempt =
        service_->PutStart(writer, key, tenant, kPartLength, ReplicateConfig{});
    ASSERT_TRUE(first_attempt.has_value());
    const std::vector<ReplicaID> stale_ids =
        ReplicaIdsOf(first_attempt.value());
    ASSERT_TRUE(
        service_->PutRevoke(writer, key, tenant, ReplicaType::ALL).has_value());
    auto second_attempt =
        service_->PutStart(writer, key, tenant, kPartLength, ReplicateConfig{});
    ASSERT_TRUE(second_attempt.has_value());
    const std::vector<ReplicaID> current_ids =
        ReplicaIdsOf(second_attempt.value());
    ASSERT_NE(current_ids.front(), stale_ids.front());

    // A part-scoped revoke has to name the attempt that holds the part, for an
    // ordinary object as well: clearing the part for the old attempt would take
    // the buffer away from a writer that is still writing into it.
    EXPECT_EQ(ErrorOf(service_->PutRevokeForPart(writer, key, tenant, 0,
                                                 stale_ids, ReplicaType::ALL)),
              ErrorCode::INVALID_WRITE);
    {
        MasterServiceTestPeer::MetadataAccessorRO accessor(
            service_.get(), MasterServiceTestPeer::ObjectIdentity{tenant, key});
        ASSERT_TRUE(accessor.Exists());
        ASSERT_NE(accessor.Get().GetPart(0), nullptr);
        std::vector<ReplicaID> held_ids;
        for (const auto& replica : accessor.Get().GetPart(0)->replicas()) {
            held_ids.push_back(replica.id());
        }
        EXPECT_EQ(held_ids, current_ids);
        EXPECT_TRUE(accessor.Get().GetPart(0)->HasProcessingReplica());
    }
    EXPECT_TRUE(service_
                    ->PutEnd(writer, PartEndMeta(key, 0, current_ids), tenant,
                             ReplicaType::ALL)
                    .has_value());
}

// The sweep is driven directly here, with the part dropped out of the metadata.
// The segment-offline path that drops parts in production has its own case,
// `LosingOnePartMakesTheObjectUnreadableWhileItsLeaseHolds`.
TEST_F(MasterServicePartScenarioTest,
       ASweepKeepsAPublishedObjectWhileItsRepairWindowIsOpen) {
    const TenantId tenant = TenantId::Default();
    const std::string key = "lost_part_of_published_key";
    WriteObjectInParts(key, kPartLength, kPartLength);
    ASSERT_TRUE(service_->ExistKey(key, tenant).value_or(false));

    // Part 1 is gone, so the object is no longer readable.
    const auto now = std::chrono::system_clock::now();
    const auto first_sweep =
        now + std::chrono::seconds(DEFAULT_PUT_START_RELEASE_TIMEOUT + 60);
    LosePart(key, 1);
    EXPECT_FALSE(service_->ExistKey(key, tenant).value_or(true));

    // The sweep runs once the lease the object was read under is over. It drops
    // objects that never became readable; this one was readable, so the parts
    // it still holds are left to a writer that repairs them.
    SweepMultiPartObjectsAt(*service_, first_sweep);
    EXPECT_TRUE(HoldsMetadata(key));

    // The repair write of the lost part makes the object readable again.
    auto repair =
        service_->PutStart(second_.client_id, key, tenant, kPartLength,
                           PartConfig(1, 2, kSecondSegment));
    ASSERT_TRUE(repair.has_value());
    ASSERT_TRUE(service_
                    ->PutEnd(second_.client_id,
                             PartEndMeta(key, 1, ReplicaIdsOf(repair.value())),
                             tenant, ReplicaType::ALL)
                    .has_value());
    EXPECT_TRUE(service_->ExistKey(key, tenant).value_or(false));
    EXPECT_TRUE(service_->GetReplicaListForPart(key, tenant, 0).has_value());
    EXPECT_TRUE(service_->GetReplicaListForPart(key, tenant, 1).has_value());

    // Another part is lost after the repair, and the sweep that sees it runs
    // past the window the first loss opened. That deadline is gone, so this
    // loss gets a window of its own rather than being reclaimed on sight.
    LosePart(key, 0);
    const auto second_sweep =
        first_sweep +
        std::chrono::seconds(DEFAULT_PUT_START_RELEASE_TIMEOUT + 100);
    SweepMultiPartObjectsAt(*service_, second_sweep);
    EXPECT_TRUE(HoldsMetadata(key));

    // And the second window closes on its own deadline.
    SweepMultiPartObjectsAt(
        *service_, second_sweep + std::chrono::seconds(
                                      DEFAULT_PUT_START_RELEASE_TIMEOUT + 1));
    EXPECT_FALSE(HoldsMetadata(key));
    EXPECT_FALSE(service_->ExistKey(key, tenant).value_or(true));
}

TEST_F(MasterServicePartScenarioTest,
       ASweepReclaimsAPublishedObjectOnceItsRepairWindowCloses) {
    const TenantId tenant = TenantId::Default();
    const std::string key = "unrepaired_part_key";
    WriteObjectInParts(key, kPartLength, kPartLength);

    const auto now = std::chrono::system_clock::now();
    LosePart(key, 1);

    // The first sweep that sees the object unreadable opens the window a repair
    // write has, and keeps the object it may be repaired from.
    const auto window_opens_at =
        now + std::chrono::seconds(DEFAULT_PUT_START_RELEASE_TIMEOUT + 60);
    SweepMultiPartObjectsAt(*service_, window_opens_at);
    EXPECT_TRUE(HoldsMetadata(key));

    // A repair that never comes leaves the object to the sweep once the window
    // closes, so the parts still on it do not hold memory for good.
    SweepMultiPartObjectsAt(
        *service_,
        window_opens_at +
            std::chrono::seconds(DEFAULT_PUT_START_RELEASE_TIMEOUT + 1));
    EXPECT_FALSE(HoldsMetadata(key));
    EXPECT_FALSE(service_->ExistKey(key, tenant).value_or(true));
}

TEST_F(MasterServicePartScenarioTest,
       ARepairPutStartAfterTheWindowClosedIsRefused) {
    const TenantId tenant = TenantId::Default();
    const std::string key = "repair_after_window_key";
    WriteObjectInParts(key, kPartLength, kPartLength);

    // The object lost part 1 a while ago, and the sweep that sees it opens the
    // window a repair has.
    const auto now = std::chrono::system_clock::now();
    LosePart(key, 1);
    // The sweep opens the window one release timeout before the clock reads
    // now, so the window is over by the time the repair below starts.
    SweepMultiPartObjectsAt(
        *service_,
        now - std::chrono::seconds(DEFAULT_PUT_START_RELEASE_TIMEOUT));
    EXPECT_TRUE(HoldsMetadata(key));

    // The window is over. A repair write that starts now would allocate buffers
    // the sweep is about to reclaim with the object, so it is refused before
    // anything is allocated, and the attempt that lost the part is left as it
    // was.
    std::chrono::system_clock::time_point attempt_start;
    {
        MasterServiceTestPeer::MetadataAccessorRO accessor(
            service_.get(), MasterServiceTestPeer::ObjectIdentity{tenant, key});
        ASSERT_TRUE(accessor.Exists());
        const ObjectMetadata::PartWriteRecord* write =
            accessor.Get().GetPartWriteRecord(1);
        ASSERT_NE(write, nullptr);
        attempt_start = write->put_start_time;
    }
    EXPECT_EQ(
        ErrorOf(service_->PutStart(second_.client_id, key, tenant, kPartLength,
                                   PartConfig(1, 2, kSecondSegment))),
        ErrorCode::INVALID_WRITE);
    {
        MasterServiceTestPeer::MetadataAccessorRO accessor(
            service_.get(), MasterServiceTestPeer::ObjectIdentity{tenant, key});
        ASSERT_TRUE(accessor.Exists());
        const ObjectMetadata::PartWriteRecord* write =
            accessor.Get().GetPartWriteRecord(1);
        ASSERT_NE(write, nullptr);
        EXPECT_EQ(write->client_id, second_.client_id);
        EXPECT_EQ(write->put_start_time, attempt_start);
        ASSERT_NE(accessor.Get().GetPart(1), nullptr);
        EXPECT_TRUE(accessor.Get().GetPart(1)->replicas().empty());
    }

    // The sweep that follows takes the object whole, as the window said it
    // would.
    SweepMultiPartObjectsAt(*service_, now);
    EXPECT_FALSE(HoldsMetadata(key));
}

TEST_F(MasterServicePartScenarioTest,
       ARepairPutStartInsideTheWindowKeepsItFixed) {
    const TenantId tenant = TenantId::Default();
    const std::string key = "repair_inside_window_key";
    WriteObjectInParts(key, kPartLength, kPartLength);

    // The sweep opens the window at a time already behind the current clock, so
    // the repair attempts below arrive inside it with their own, later start
    // times — the arrivals an arrival clock would take as a reason to wait
    // longer.
    const auto now = std::chrono::system_clock::now();
    const auto window_opens_at =
        now - std::chrono::seconds(DEFAULT_PUT_START_RELEASE_TIMEOUT / 2);
    LosePart(key, 1);
    SweepMultiPartObjectsAt(*service_, window_opens_at);
    const auto window_closes_at =
        window_opens_at +
        std::chrono::seconds(DEFAULT_PUT_START_RELEASE_TIMEOUT);

    // A writer starts the lost part again and again without finishing it. Each
    // attempt gives its buffers up before the next one, and each start is newer
    // than the one before, so the object's latest arrival keeps moving.
    for (int attempt = 0; attempt < 2; ++attempt) {
        auto repair =
            service_->PutStart(second_.client_id, key, tenant, kPartLength,
                               PartConfig(1, 2, kSecondSegment));
        ASSERT_TRUE(repair.has_value());
        {
            MasterServiceTestPeer::MetadataAccessorRO accessor(
                service_.get(),
                MasterServiceTestPeer::ObjectIdentity{tenant, key});
            ASSERT_TRUE(accessor.Exists());
            EXPECT_GT(accessor.Get().LastPutStartTime(), window_opens_at);
        }
        ASSERT_TRUE(service_
                        ->PutRevokeForPart(second_.client_id, key, tenant, 1,
                                           ReplicaIdsOf(repair.value()),
                                           ReplicaType::ALL)
                        .has_value());
    }
    // The attempt that is still open when the window closes is the newest
    // arrival the object has ever seen.
    ASSERT_TRUE(service_
                    ->PutStart(second_.client_id, key, tenant, kPartLength,
                               PartConfig(1, 2, kSecondSegment))
                    .has_value());

    // The window still closes when it said it would, and not one attempt later.
    SweepMultiPartObjectsAt(*service_,
                            window_closes_at + std::chrono::seconds(1));
    EXPECT_FALSE(HoldsMetadata(key));
    EXPECT_FALSE(service_->ExistKey(key, tenant).value_or(true));
}

// Every path that finds the object readable ends the window a lost part had,
// including a repeated End that has nothing left to write.
TEST_F(MasterServicePartScenarioTest,
       ARepeatedEndAfterALostPartClosesTheRepairWindowToo) {
    const TenantId tenant = TenantId::Default();
    const std::string key = "repeated_end_repair_key";

    // Each part on its own segment, so the second one can be taken out of reach
    // while its replicas stay where they are.
    auto part_zero =
        service_->PutStart(first_.client_id, key, tenant, kPartLength,
                           PartConfig(0, 2, "test_segment"));
    ASSERT_TRUE(part_zero.has_value());
    auto part_one =
        service_->PutStart(second_.client_id, key, tenant, kPartLength,
                           PartConfig(1, 2, kSecondSegment));
    ASSERT_TRUE(part_one.has_value());
    const std::vector<ReplicaID> part_zero_ids =
        ReplicaIdsOf(part_zero.value());
    const std::vector<ReplicaID> part_one_ids = ReplicaIdsOf(part_one.value());
    ASSERT_TRUE(service_
                    ->PutEnd(first_.client_id,
                             PartEndMeta(key, 0, part_zero_ids), tenant,
                             ReplicaType::ALL)
                    .has_value());
    ASSERT_TRUE(service_
                    ->PutEnd(second_.client_id,
                             PartEndMeta(key, 1, part_one_ids), tenant,
                             ReplicaType::ALL)
                    .has_value());
    ASSERT_TRUE(service_->ExistKey(key, tenant).value_or(false));

    // The second segment goes out of reach, so part 1 cannot be read although
    // it still holds its replicas, and the sweep opens the window.
    MasterServiceTestPeer peer(*service_);
    peer.MarkEndpointAwaitingRemount(kSecondSegment);
    EXPECT_FALSE(service_->ExistKey(key, tenant).value_or(true));
    const auto now = std::chrono::system_clock::now();
    SweepMultiPartObjectsAt(*service_, now);
    EXPECT_TRUE(HoldsMetadata(key));

    // The segment comes back and the object is readable again, with no write to
    // report it. A repeated End for the part that is already complete carries
    // nothing to write either, and it is what tells the sweep the window is
    // over.
    peer.ClearEndpointAwaitingRemount(kSecondSegment);
    ASSERT_TRUE(service_->ExistKey(key, tenant).value_or(false));
    EXPECT_TRUE(service_
                    ->PutEnd(second_.client_id,
                             PartEndMeta(key, 1, part_one_ids), tenant,
                             ReplicaType::ALL)
                    .has_value());

    // A part lost after that gets a window of its own, measured from this loss
    // rather than from the one the first window was opened for. Part 0 keeps
    // the replicas the object is repaired from.
    LosePart(key, 1);
    const auto after_first_window =
        now + std::chrono::seconds(DEFAULT_PUT_START_RELEASE_TIMEOUT + 100);
    SweepMultiPartObjectsAt(*service_, after_first_window);
    EXPECT_TRUE(HoldsMetadata(key));
    SweepMultiPartObjectsAt(
        *service_,
        after_first_window +
            std::chrono::seconds(DEFAULT_PUT_START_RELEASE_TIMEOUT + 1));
    EXPECT_FALSE(HoldsMetadata(key));
}

TEST_F(MasterServicePartScenarioTest,
       ADrainJobLeavesASegmentAloneWhileAPartOfAnObjectIsOnIt) {
    auto service_config = MasterServiceConfig::builder()
                              .set_enable_multi_part_objects(true)
                              .set_default_kv_lease_ttl(0)
                              .build();
    auto service = std::make_unique<MasterService>(service_config);
    [[maybe_unused]] const auto drain_ctx0 = PrepareSimpleSegment(
        *service, "segment_0", 0x300000000, kDefaultSegmentSize);
    [[maybe_unused]] const auto drain_ctx1 = PrepareSimpleSegment(
        *service, "segment_1", 0x400000000, kDefaultSegmentSize);
    const TenantId tenant = TenantId::Default();
    const UUID writer = generate_uuid();
    const std::string key = "drain_multipart_key";

    // A part other than part 0 sits on the segment being drained: a scan that
    // only looks at part 0 would read the object as absent from it.
    auto part_zero = service->PutStart(writer, key, tenant, kPartLength,
                                       PartConfig(0, 2, "segment_1"));
    ASSERT_TRUE(part_zero.has_value());
    auto part_one = service->PutStart(writer, key, tenant, kPartLength,
                                      PartConfig(1, 2, "segment_0"));
    ASSERT_TRUE(part_one.has_value());
    ASSERT_TRUE(
        service
            ->PutEnd(writer,
                     PartEndMeta(key, 0, ReplicaIdsOf(part_zero.value())),
                     tenant, ReplicaType::ALL)
            .has_value());
    ASSERT_TRUE(
        service
            ->PutEnd(writer,
                     PartEndMeta(key, 1, ReplicaIdsOf(part_one.value())),
                     tenant, ReplicaType::ALL)
            .has_value());
    ASSERT_TRUE(service->ExistKey(key, tenant).value_or(false));

    CreateDrainJobRequest request;
    request.segments = {"segment_0"};
    request.target_segments = {"segment_1"};
    request.max_concurrency = 1;
    auto job_id = service->CreateDrainJob(request);
    ASSERT_TRUE(job_id.has_value());
    WaitUntil([&] {
        auto query = service->QueryDrainJob(job_id.value());
        return query.has_value() && query->blocked_units >= 1;
    });

    // A move task carries one object's replica between segments, so such an
    // object is never drained part by part: its unit is blocked, the job does
    // not succeed, and the segment is not reported as drained while a part of
    // the object is still on it.
    auto query = service->QueryDrainJob(job_id.value());
    ASSERT_TRUE(query.has_value());
    EXPECT_NE(query->status, JobStatus::SUCCEEDED);
    EXPECT_GE(query->blocked_units, 1u);
    auto segment_status = service->QuerySegmentStatus("segment_0");
    ASSERT_TRUE(segment_status.has_value());
    EXPECT_NE(segment_status.value(), SegmentStatus::DRAINED);
    EXPECT_TRUE(service->ExistKey(key, tenant).value_or(false));
}

TEST_F(MasterServicePartScenarioTest,
       ASecondWriterOfAPartIsRefusedAndLeavesTheFirstAttempt) {
    const TenantId tenant = TenantId::Default();
    const std::string key = "second_writer_key";

    auto part = service_->PutStart(first_.client_id, key, tenant, kPartLength,
                                   PartConfig(0, 2));
    ASSERT_TRUE(part.has_value());
    const std::vector<ReplicaID> first_ids = ReplicaIdsOf(part.value());

    // The part belongs to the attempt that started it: another writer is
    // refused, and the replicas and the record of the first attempt stay.
    EXPECT_EQ(ErrorOf(service_->PutStart(second_.client_id, key, tenant,
                                         kPartLength, PartConfig(0, 2))),
              ErrorCode::OBJECT_ALREADY_EXISTS);
    {
        MasterServiceTestPeer::MetadataAccessorRO accessor(
            service_.get(), MasterServiceTestPeer::ObjectIdentity{tenant, key});
        ASSERT_TRUE(accessor.Exists());
        ASSERT_NE(accessor.Get().GetPart(0), nullptr);
        std::vector<ReplicaID> held_ids;
        for (const auto& replica : accessor.Get().GetPart(0)->replicas()) {
            held_ids.push_back(replica.id());
        }
        EXPECT_EQ(held_ids, first_ids);
        const ObjectMetadata::PartWriteRecord* write =
            accessor.Get().GetPartWriteRecord(0);
        ASSERT_NE(write, nullptr);
        EXPECT_EQ(write->client_id, first_.client_id);
    }

    // The first writer completes it, and the part is written from then on.
    ASSERT_TRUE(service_
                    ->PutEnd(first_.client_id, PartEndMeta(key, 0, first_ids),
                             tenant, ReplicaType::ALL)
                    .has_value());
    EXPECT_EQ(ErrorOf(service_->PutStart(second_.client_id, key, tenant,
                                         kPartLength, PartConfig(0, 2))),
              ErrorCode::OBJECT_ALREADY_EXISTS);
}

// The stored event marks the object becoming readable, which is the part that
// completes it, and a repeated End has nothing new to announce.
TEST_F(MasterServicePartScenarioTest, TheStoredEventWaitsForTheLastPart) {
    MasterServiceConfig config = PartScenarioConfig();
    config.enable_kv_events = true;
    config.kv_events_bind_endpoint =
        "ipc:///tmp/mooncake_part_events_" + std::to_string(::getpid()) + "_" +
        std::to_string(
            std::chrono::steady_clock::now().time_since_epoch().count());
    config.kv_events_backend_id = "mooncake-test";
    MasterService service{config};
    if (!service.KvEventsEnabled()) {
        GTEST_SKIP() << "the master is built without the KV event publisher "
                        "(ENABLE_KV_EVENTS=OFF), so it has nothing to publish";
    }
    const auto first = PrepareSimpleSegment(service);
    const auto second = PrepareSimpleSegment(
        service, kSecondSegment, kDefaultSegmentBase + kDefaultSegmentSize);
    const TenantId tenant = TenantId::Default();
    const std::string key = "stored_event_key";
    const auto published = [&service] {
        return service.GetKvEventStats().published_events;
    };

    auto part_zero = service.PutStart(first.client_id, key, tenant, kPartLength,
                                      PartConfig(0, 2, "test_segment"));
    ASSERT_TRUE(part_zero.has_value());
    const std::vector<ReplicaID> part_zero_ids =
        ReplicaIdsOf(part_zero.value());
    ASSERT_TRUE(service
                    .PutEnd(first.client_id, PartEndMeta(key, 0, part_zero_ids),
                            tenant, ReplicaType::ALL)
                    .has_value());

    // One part is done, so the object is not readable and nothing is announced.
    EXPECT_EQ(published(), 0u);

    auto part_one = service.PutStart(second.client_id, key, tenant, kPartLength,
                                     PartConfig(1, 2, kSecondSegment));
    ASSERT_TRUE(part_one.has_value());
    const std::vector<ReplicaID> part_one_ids = ReplicaIdsOf(part_one.value());
    ASSERT_TRUE(service
                    .PutEnd(second.client_id, PartEndMeta(key, 1, part_one_ids),
                            tenant, ReplicaType::ALL)
                    .has_value());
    for (int wait = 0; wait < 100 && published() == 0; ++wait) {
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    EXPECT_EQ(published(), 1u);

    // The part that completed the object is ended again, and the subscriber
    // hears nothing new about it.
    ASSERT_TRUE(service
                    .PutEnd(second.client_id, PartEndMeta(key, 1, part_one_ids),
                            tenant, ReplicaType::ALL)
                    .has_value());
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    EXPECT_EQ(published(), 1u);
}

}  // namespace mooncake::test
