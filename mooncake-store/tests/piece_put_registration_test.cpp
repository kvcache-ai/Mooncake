#include "segment.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <chrono>
#include <memory>
#include <type_traits>
#include <utility>

namespace mooncake {
namespace {

static_assert(!std::is_copy_constructible_v<SegmentWriteHold>);
static_assert(!std::is_copy_assignable_v<SegmentWriteHold>);
static_assert(std::is_nothrow_move_constructible_v<SegmentWriteHold>);
static_assert(std::is_nothrow_move_assignable_v<SegmentWriteHold>);

class PiecePutRegistrationTest : public ::testing::Test {
   protected:
    void SetUp() override {
        google::InitGoogleLogging("PiecePutRegistrationTest");
        FLAGS_logtostderr = true;
        segment_.id = generate_uuid();
        segment_.name = "piece-put-registration";
        segment_.base = 0x400000000;
        segment_.size = 16 * 1024 * 1024;
        segment_.protocol = "tcp";
        segment_.te_endpoint = "piece-put-target:23000";
        segment_.tcp_instance = "tcp-instance-one";
        auto access = manager_.getSegmentAccess();
        ASSERT_EQ(access.MountSegment(segment_, session_), ErrorCode::OK);
        ASSERT_EQ(access.BindTcpInstance(segment_.id, "tcp-instance-one"),
                  ErrorCode::OK);
        registration_ = access.GetRegistration(segment_.id);
    }

    void TearDown() override {
        manager_.releaseCapacityMetrics();
        google::ShutdownGoogleLogging();
    }

    std::unique_ptr<AllocatedBuffer> Allocate(size_t size = 1024) {
        auto access = manager_.getAllocatorAccess();
        const auto* registrations =
            access.getAllocatorManager().getAllocators(segment_.name);
        if (registrations == nullptr || registrations->empty()) return nullptr;
        return registrations->front()->Allocate(size);
    }

    tl::expected<SegmentWriteHold, ErrorCode> Hold(
        const AllocatedBuffer& buffer) {
        return manager_.getAllocatorAccess().AcquireWriteHold(buffer);
    }

    SegmentManager manager_{BufferAllocatorType::OFFSET};
    const UUID client_ = generate_uuid();
    ClientSessionSharedPtr session_ = std::make_shared<ClientLivenessRecord>(
        client_, ClientLivenessRecord::Clock::now());
    Segment segment_;
    std::shared_ptr<SegmentAllocatorRegistration> registration_;
};

TEST_F(PiecePutRegistrationTest, MoveTransfersOneObjectHold) {
    auto first_buffer = Allocate();
    auto second_buffer = Allocate();
    ASSERT_TRUE(first_buffer);
    ASSERT_TRUE(second_buffer);
    auto first = Hold(*first_buffer);
    auto second = Hold(*second_buffer);
    ASSERT_TRUE(first.has_value());
    ASSERT_TRUE(second.has_value());
    EXPECT_EQ(registration_->WriteHoldCount(), 2u);

    // Holding exposed memory does not freeze other offsets in the same pool.
    auto ordinary_buffer = Allocate();
    ASSERT_TRUE(ordinary_buffer);
    EXPECT_NE(ordinary_buffer->data(), first_buffer->data());

    SegmentWriteHold moved = std::move(*first);
    EXPECT_FALSE(static_cast<bool>(*first));
    EXPECT_EQ(registration_->WriteHoldCount(), 2u);
    moved = std::move(*second);
    EXPECT_FALSE(static_cast<bool>(*second));
    EXPECT_EQ(registration_->WriteHoldCount(), 1u);
    moved = SegmentWriteHold{};
    EXPECT_FALSE(registration_->HasWriteHolds());
}

TEST_F(PiecePutRegistrationTest, UnmountAndAliasWaitForTheOriginalHold) {
    auto buffer = Allocate();
    ASSERT_TRUE(buffer);
    auto hold = Hold(*buffer);
    ASSERT_TRUE(hold.has_value());
    size_t capacity = 0;

    {
        auto access = manager_.getSegmentAccess();
        EXPECT_EQ(access.PrepareUnmountSegment(segment_.id, capacity),
                  ErrorCode::OBJECT_REPLICA_BUSY);
        EXPECT_EQ(access.CommitUnmountSegment(segment_.id, client_, capacity),
                  ErrorCode::OBJECT_REPLICA_BUSY);
        Segment alias = segment_;
        alias.id = generate_uuid();
        alias.name = "piece-put-alias";
        EXPECT_EQ(access.MountSegment(alias, session_),
                  ErrorCode::OBJECT_REPLICA_BUSY);
        Segment rejected;
        EXPECT_FALSE(access.GetSegment(alias.id, rejected));
        EXPECT_FALSE(access.GetAllocator(alias.id));
        EXPECT_FALSE(access.GetRegistration(alias.id));
        EXPECT_EQ(
            access.SetSegmentStatusByName(segment_.name, SegmentStatus::OK),
            ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    }
    EXPECT_TRUE(buffer->isAllocatorValid());
    EXPECT_FALSE(hold->CanContinue(SegmentWriteHold::Clock::now()));
    EXPECT_FALSE(hold->IsPhysicallyRetired());

    buffer.reset();
    *hold = SegmentWriteHold{};
    auto access = manager_.getSegmentAccess();
    EXPECT_EQ(access.PrepareUnmountSegment(segment_.id, capacity),
              ErrorCode::OK);
    EXPECT_EQ(access.CommitUnmountSegment(segment_.id, client_, capacity),
              ErrorCode::OK);
}

TEST_F(PiecePutRegistrationTest, GracefulDeadlineOnlyStopsNewAllocations) {
    auto buffer = Allocate();
    ASSERT_TRUE(buffer);
    auto hold = Hold(*buffer);
    ASSERT_TRUE(hold.has_value());
    const auto deadline =
        SegmentWriteHold::Clock::now() + std::chrono::hours(1);
    {
        auto access = manager_.getSegmentAccess();
        EXPECT_EQ(access.PrepareGracefulUnmountSegment(segment_.id, deadline),
                  ErrorCode::OK);
        EXPECT_EQ(access.PrepareGracefulUnmountSegment(
                      segment_.id, deadline + std::chrono::hours(1)),
                  ErrorCode::OK);
    }
    EXPECT_EQ(registration_->GracefulDeadline(), deadline);
    EXPECT_FALSE(registration_->IsAllocatable());
    EXPECT_TRUE(buffer->isAvailable());
    EXPECT_TRUE(hold->CanContinue(deadline - std::chrono::nanoseconds(1)));
    EXPECT_FALSE(hold->CanContinue(deadline));
    EXPECT_EQ(registration_->Allocate(1024), nullptr);
    {
        auto access = manager_.getSegmentAccess();
        EXPECT_EQ(access.CloseForWrites(segment_.id, registration_),
                  ErrorCode::OK);
    }
    EXPECT_FALSE(hold->CanContinue(deadline - std::chrono::nanoseconds(1)));
}

TEST_F(PiecePutRegistrationTest, HoldBlocksAllocatorAndSessionReplacement) {
    auto buffer = Allocate();
    ASSERT_TRUE(buffer);
    auto hold = Hold(*buffer);
    ASSERT_TRUE(hold.has_value());
    auto replacement = std::make_shared<OffsetBufferAllocator>(
        segment_.name, segment_.base, segment_.size, segment_.te_endpoint,
        segment_.protocol, ReplicaType::MEMORY);
    auto fresh_session = std::make_shared<ClientLivenessRecord>(
        client_, ClientLivenessRecord::Clock::now());
    const auto allocator = registration_->GetAllocator();

    {
        auto access = manager_.getSegmentAccess();
        EXPECT_FALSE(
            access.ReplaceAllocators({{segment_.id, allocator, replacement}}));
        EXPECT_EQ(access.BindClientSession(fresh_session),
                  ErrorCode::OBJECT_REPLICA_BUSY);
        EXPECT_EQ(access.BindBufferToSegment(segment_.id, *buffer),
                  ErrorCode::OBJECT_REPLICA_BUSY);
    }
    auto snapshot = manager_.getAllocatorAccess().SnapshotAllocatorManager();
    AllocatorManager candidates;
    auto replacement_registration =
        candidates.addAllocator(segment_.name, replacement, session_);
    EXPECT_FALSE(snapshot.replaceRegistrations(
        {{segment_.name, registration_, replacement_registration}}));
    EXPECT_EQ(snapshot.getAllocators(segment_.name)->front(), registration_);
    EXPECT_EQ(registration_->GetAllocator(), allocator);
    EXPECT_EQ(registration_->GetClientSession(), session_);
}

TEST_F(PiecePutRegistrationTest, RetirementProofCannotApplyToANewRegistration) {
    auto buffer = Allocate();
    ASSERT_TRUE(buffer);
    auto hold = Hold(*buffer);
    ASSERT_TRUE(hold.has_value());
    const auto original = registration_;
    {
        auto access = manager_.getSegmentAccess();
        EXPECT_EQ(access.RetireTcpEndpoint(client_, "tcp-instance-one"),
                  ErrorCode::OK);
    }
    EXPECT_TRUE(hold->IsPhysicallyRetired());
    EXPECT_FALSE(hold->CanContinue(SegmentWriteHold::Clock::now()));
    EXPECT_EQ(original->WriteHoldCount(), 1u);

    buffer.reset();
    *hold = SegmentWriteHold{};
    {
        auto access = manager_.getSegmentAccess();
        size_t capacity = 0;
        ASSERT_EQ(access.PrepareUnmountSegment(segment_.id, capacity),
                  ErrorCode::OK);
        ASSERT_EQ(access.CommitUnmountSegment(segment_.id, client_, capacity),
                  ErrorCode::OK);
        auto remounted = segment_;
        remounted.tcp_instance = "tcp-instance-two";
        ASSERT_EQ(access.MountSegment(remounted, session_), ErrorCode::OK);
        ASSERT_EQ(access.BindTcpInstance(segment_.id, "tcp-instance-two"),
                  ErrorCode::OK);
        registration_ = access.GetRegistration(segment_.id);
        EXPECT_NE(registration_, original);
        EXPECT_EQ(access.MarkPhysicallyRetired(segment_.id, original),
                  ErrorCode::SEGMENT_NOT_FOUND);
    }
    EXPECT_FALSE(registration_->IsPhysicallyRetired());
    EXPECT_TRUE(registration_->IsAllocatable());
}

TEST_F(PiecePutRegistrationTest, TcpInstanceBindingIsImmutable) {
    auto access = manager_.getSegmentAccess();
    EXPECT_EQ(access.BindTcpInstance(segment_.id, "tcp-instance-one"),
              ErrorCode::OK);
    EXPECT_EQ(access.BindTcpInstance(segment_.id, "tcp-instance-two"),
              ErrorCode::INVALID_PARAMS);
    EXPECT_EQ(registration_->TcpInstance(), "tcp-instance-one");
}

TEST_F(PiecePutRegistrationTest,
       HoldRequiresOptInBeforeAliasProtectionIsNeeded) {
    SegmentManager manager(BufferAllocatorType::OFFSET);
    Segment segment = segment_;
    segment.id = generate_uuid();
    segment.name = "ordinary-target";
    segment.tcp_instance.reset();
    {
        auto access = manager.getSegmentAccess();
        ASSERT_EQ(access.MountSegment(segment, session_), ErrorCode::OK);
    }
    std::unique_ptr<AllocatedBuffer> buffer;
    {
        auto access = manager.getAllocatorAccess();
        const auto* registrations =
            access.getAllocatorManager().getAllocators(segment.name);
        ASSERT_NE(registrations, nullptr);
        ASSERT_EQ(registrations->size(), 1u);
        buffer = registrations->front()->Allocate(1024);
        ASSERT_TRUE(buffer);
        const auto no_opt_in = access.AcquireWriteHold(*buffer);
        ASSERT_FALSE(no_opt_in);
        EXPECT_EQ(no_opt_in.error(), ErrorCode::INVALID_PARAMS);
    }
    {
        auto access = manager.getSegmentAccess();
        ASSERT_EQ(access.BindTcpInstance(segment.id, "first-opt-in"),
                  ErrorCode::OK);
    }
    auto hold = manager.getAllocatorAccess().AcquireWriteHold(*buffer);
    ASSERT_TRUE(hold);
    {
        auto access = manager.getSegmentAccess();
        Segment alias = segment;
        alias.id = generate_uuid();
        alias.name = "ordinary-alias";
        EXPECT_EQ(access.MountSegment(alias, session_),
                  ErrorCode::OBJECT_REPLICA_BUSY);
    }
    manager.releaseCapacityMetrics();
}

TEST_F(PiecePutRegistrationTest,
       RejectedMountPublishesNoAllocatorOrTcpBinding) {
    Segment rejected = segment_;
    rejected.id = generate_uuid();
    rejected.name = "rejected-registration";
    rejected.base = 0;
    rejected.te_endpoint = "rejected-target:23001";
    rejected.tcp_instance = "rejected-tcp-instance";
    auto access = manager_.getSegmentAccess();
    const auto original_allocator = registration_->GetAllocator();
    EXPECT_EQ(access.MountSegment(rejected, session_),
              ErrorCode::INVALID_PARAMS);
    Segment found;
    EXPECT_FALSE(access.GetSegment(rejected.id, found));
    EXPECT_FALSE(access.GetAllocator(rejected.id));
    EXPECT_FALSE(access.GetRegistration(rejected.id));
    EXPECT_EQ(registration_->GetAllocator(), original_allocator);

    // Reusing the rejected instance at a different endpoint proves no
    // provisional endpoint binding escaped the failed mount.
    rejected.base = segment_.base + segment_.size;
    rejected.te_endpoint = "rejected-target:23002";
    EXPECT_EQ(access.MountSegment(rejected, session_), ErrorCode::OK);
    EXPECT_EQ(access.BindTcpInstance(rejected.id, *rejected.tcp_instance),
              ErrorCode::OK);
}

TEST_F(PiecePutRegistrationTest,
       ExactRemountWithWriteHoldPreservesEveryBinding) {
    auto buffer = Allocate();
    ASSERT_TRUE(buffer);
    auto hold = Hold(*buffer);
    ASSERT_TRUE(hold);
    const auto allocator = registration_->GetAllocator();
    {
        auto access = manager_.getSegmentAccess();
        EXPECT_EQ(access.ValidateRemountSegment(segment_, client_, session_),
                  ErrorCode::OK);
        EXPECT_EQ(access.ReMountSegment({segment_}, session_), ErrorCode::OK);
        EXPECT_EQ(
            access.ValidateRemountSegment(segment_, client_, session_, true),
            ErrorCode::OK);
        EXPECT_EQ(access.GetRegistration(segment_.id), registration_);
        EXPECT_EQ(access.GetAllocator(segment_.id), allocator);
        Segment found;
        ASSERT_TRUE(access.GetSegment(segment_.id, found));
        EXPECT_EQ(found.id, segment_.id);
        EXPECT_EQ(found.base, segment_.base);
        EXPECT_EQ(found.size, segment_.size);
        ASSERT_TRUE(found.tcp_instance);
        EXPECT_EQ(*found.tcp_instance, *segment_.tcp_instance);
    }
    EXPECT_EQ(registration_->GetClientSession(), session_);
    EXPECT_EQ(registration_->WriteHoldCount(), 1u);
    EXPECT_TRUE(hold->CanContinue(SegmentWriteHold::Clock::now()));
}

TEST_F(PiecePutRegistrationTest, RemountCannotChangeBindingsBehindWriteHold) {
    auto buffer = Allocate();
    ASSERT_TRUE(buffer);
    auto hold = Hold(*buffer);
    ASSERT_TRUE(hold);
    auto fresh_session = std::make_shared<ClientLivenessRecord>(
        client_, ClientLivenessRecord::Clock::now());
    auto access = manager_.getSegmentAccess();
    EXPECT_EQ(access.ValidateRemountSegment(segment_, client_, fresh_session),
              ErrorCode::OBJECT_REPLICA_BUSY);
    EXPECT_EQ(
        access.ValidateRemountSegment(segment_, generate_uuid(), session_),
        ErrorCode::ILLEGAL_CLIENT);
    auto changed = segment_;
    changed.locality = SegmentLocality{0, {}};
    EXPECT_EQ(access.ValidateRemountSegment(changed, client_, session_),
              ErrorCode::OBJECT_REPLICA_BUSY);
    changed = segment_;
    changed.host_id = "different-host";
    EXPECT_EQ(access.ValidateRemountSegment(changed, client_, session_),
              ErrorCode::INVALID_PARAMS);
    changed = segment_;
    changed.tcp_instance = "different-tcp-instance";
    EXPECT_EQ(access.ValidateRemountSegment(changed, client_, session_),
              ErrorCode::OBJECT_REPLICA_BUSY);
    changed = segment_;
    changed.id = generate_uuid();
    EXPECT_EQ(access.ValidateRemountSegment(changed, client_, session_, true),
              ErrorCode::SEGMENT_NOT_FOUND);
    EXPECT_EQ(access.CloseForWrites(segment_.id, registration_), ErrorCode::OK);
    EXPECT_NE(access.ValidateRemountSegment(segment_, client_, session_),
              ErrorCode::OK);
}

}  // namespace
}  // namespace mooncake
