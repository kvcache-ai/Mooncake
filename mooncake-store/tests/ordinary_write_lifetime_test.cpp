#include "master_service.h"
#include "master_service/master_service_test_peer.h"

#include <gtest/gtest.h>

#include <chrono>
#include <cstdlib>
#include <new>
#include <memory>
#include <string>
#include <vector>

namespace {
// Fail only a selected allocation shape on the calling test thread. Background
// maintenance remains unaffected, and the failure disarms itself before unwind.
thread_local size_t failure_size = 0;
thread_local size_t failure_skip = 0;
thread_local bool failure_consumed = false;
thread_local const mooncake::SegmentAllocatorRegistration* failure_target_a =
    nullptr;
thread_local const mooncake::SegmentAllocatorRegistration* failure_target_b =
    nullptr;

class FailAllocation {
   public:
    FailAllocation(
        size_t size, size_t skip,
        const mooncake::SegmentAllocatorRegistration* target_a = nullptr,
        const mooncake::SegmentAllocatorRegistration* target_b = nullptr) {
        failure_size = size;
        failure_skip = skip;
        failure_consumed = false;
        failure_target_a = target_a;
        failure_target_b = target_b;
    }
    ~FailAllocation() {
        failure_size = 0;
        failure_target_a = nullptr;
        failure_target_b = nullptr;
    }
};
}  // namespace

// Keep the replaceable new/delete families together, as in session_tag_test.
// Otherwise ASan may pair its allocator with this test's free-based delete.
void* operator new(size_t size) {
    const bool target_held = failure_target_a == nullptr ||
                             failure_target_a->WriteHoldCount() != 0 ||
                             (failure_target_b != nullptr &&
                              failure_target_b->WriteHoldCount() != 0);
    if (failure_size != 0 && size == failure_size && target_held) {
        if (failure_skip == 0) {
            failure_size = 0;
            failure_consumed = true;
            throw std::bad_alloc();
        }
        --failure_skip;
    }
    if (void* pointer = std::malloc(size ? size : 1)) return pointer;
    throw std::bad_alloc();
}
void* operator new(size_t size, const std::nothrow_t&) noexcept {
    try {
        return operator new(size);
    } catch (...) {
        return nullptr;
    }
}
void* operator new[](size_t size) { return operator new(size); }
void* operator new[](size_t size, const std::nothrow_t& tag) noexcept {
    return operator new(size, tag);
}
void operator delete(void* pointer) noexcept { std::free(pointer); }
void operator delete(void* pointer, size_t) noexcept { std::free(pointer); }
void operator delete(void* pointer, const std::nothrow_t&) noexcept {
    std::free(pointer);
}
void operator delete[](void* pointer) noexcept { std::free(pointer); }
void operator delete[](void* pointer, size_t) noexcept { std::free(pointer); }
void operator delete[](void* pointer, const std::nothrow_t&) noexcept {
    std::free(pointer);
}

namespace mooncake::test {
namespace {

class OrdinaryWriteLifetimeTest : public ::testing::Test {
   protected:
    void SetUp() override {
        MasterServiceConfig config;
        config.enable_piece_put = true;
        config.memory_allocator = BufferAllocatorType::OFFSET;
        config.default_kv_lease_ttl = 0;
        service_ = std::make_unique<MasterService>(config);
        MasterServiceTestPeer::EvictionRunning(*service_).store(false);
        auto& worker = MasterServiceTestPeer::EvictionThread(*service_);
        if (worker.joinable()) worker.join();
        segment_ = Mount("shared-target", 0x700000000, 'a');
    }

    Segment Mount(std::string name, uintptr_t base, char instance) {
        Segment segment;
        segment.id = generate_uuid();
        segment.name = std::move(name);
        segment.base = base;
        segment.size = 16 * 1024 * 1024;
        segment.protocol = "tcp";
        segment.te_endpoint =
            "127.0.0.1:" + std::to_string(23000 + instance - 'a');
        segment.tcp_instance = std::string(32, instance);
        segment.piece_put_master_instance =
            *service_->GetPiecePutMasterInstance();
        EXPECT_TRUE(service_->MountSegment(segment, owner_));
        return segment;
    }

    std::shared_ptr<SegmentAllocatorRegistration> Registration(
        const Segment& segment) {
        return MasterServiceTestPeer::SegmentManager(*service_)
            .getSegmentAccess()
            .GetRegistration(segment.id);
    }

    auto Put(std::string key, const Segment& segment) {
        ReplicateConfig config;
        config.preferred_segments = {segment.name};
        return service_->PutStart(owner_, key, TenantId::Default(), 1024,
                                  config);
    }

    void Reap() {
        MasterServiceTestPeer(*service_).RunBatchEvictForTesting(0, 0);
    }

    ObjectMeta EndMeta(const std::string& key,
                       const std::vector<Replica::Descriptor>& replicas) {
        ObjectMeta metadata{key, std::nullopt};
        if (auto proof = MakeStrictWriteProof(replicas))
            metadata.strict_write_proof = std::move(*proof);
        return metadata;
    }

    PiecePutStartRequest Piece(std::string key = "piece") {
        PiecePutStartRequest request{
            std::move(key),
            1024,
            0,
            1024,
            "round",
            1,
            false,
            std::nullopt,
            *service_->GetPiecePutAdmission(owner_, 0)};
        return request;
    }

    std::unique_ptr<MasterService> service_;
    const UUID owner_ = generate_uuid();
    Segment segment_;
};

TEST_F(OrdinaryWriteLifetimeTest, OrdinaryAndPieceUseOnePoolWithSeparateHolds) {
    auto put = Put("ordinary", segment_);
    ASSERT_TRUE(put);
    ASSERT_EQ(put->size(), 1u);
    const auto& descriptor = put->front().get_memory_descriptor();
    ASSERT_TRUE(descriptor.strict_tcp_instance);
    EXPECT_EQ(*descriptor.strict_tcp_instance, *segment_.tcp_instance);
    auto registration = Registration(segment_);
    ASSERT_TRUE(registration);
    EXPECT_EQ(registration->WriteHoldCount(), 1u);

    auto request = Piece();
    auto piece = service_->PiecePutStart(owner_, request, TenantId::Default());
    ASSERT_TRUE(piece);
    ASSERT_TRUE(piece->replica);
    EXPECT_NE(piece->replica->get_memory_descriptor()
                  .buffer_descriptor.buffer_address_,
              descriptor.buffer_descriptor.buffer_address_);
    EXPECT_EQ(registration->WriteHoldCount(), 2u);
    ASSERT_TRUE(service_->PiecePutFinish(
        owner_,
        {request.key, *piece->token, request.operation_seq,
         PiecePutFinishOutcome::NO_POST},
        TenantId::Default()));
    EXPECT_EQ(registration->WriteHoldCount(), 1u);

    ASSERT_TRUE(service_->PutEnd(owner_, EndMeta("ordinary", *put),
                                 TenantId::Default(), ReplicaType::MEMORY));
    EXPECT_EQ(registration->WriteHoldCount(), 0u);
    auto read = service_->GetReplicaList("ordinary", TenantId::Default());
    ASSERT_TRUE(read);
    EXPECT_FALSE(
        read->replicas.front().get_memory_descriptor().strict_tcp_instance);
}

TEST_F(OrdinaryWriteLifetimeTest, RevokeCannotReleaseAnUncertainTargetOnTtl) {
    auto put = Put("ordinary", segment_);
    ASSERT_TRUE(put);
    auto registration = Registration(segment_);
    const auto address =
        put->front().get_memory_descriptor().buffer_descriptor.buffer_address_;
    ASSERT_TRUE(service_->PutRevoke(owner_, "ordinary", TenantId::Default(),
                                    ReplicaType::MEMORY));
    Reap();
    EXPECT_EQ(registration->WriteHoldCount(), 1u);
    EXPECT_EQ(service_->GetStorageUsageSnapshot().memory.used_bytes, 1024u);

    auto request = Piece();
    auto piece = service_->PiecePutStart(owner_, request, TenantId::Default());
    ASSERT_TRUE(piece);
    ASSERT_TRUE(piece->replica);
    EXPECT_NE(piece->replica->get_memory_descriptor()
                  .buffer_descriptor.buffer_address_,
              address);
    ASSERT_TRUE(service_->PiecePutFinish(
        owner_,
        {request.key, *piece->token, request.operation_seq,
         PiecePutFinishOutcome::NO_POST},
        TenantId::Default()));
    ASSERT_TRUE(
        service_->RetirePiecePutEndpoint(owner_, *segment_.tcp_instance));
    Reap();
    EXPECT_EQ(registration->WriteHoldCount(), 0u);
    EXPECT_EQ(service_->GetStorageUsageSnapshot().memory.used_bytes, 0u);
}

TEST_F(OrdinaryWriteLifetimeTest,
       RemovalAllocationFailureLeavesUnsettledBufferOwned) {
    const auto put = Put("ordinary", segment_);
    ASSERT_TRUE(put);
    const auto address =
        put->front().get_memory_descriptor().buffer_descriptor.buffer_address_;
    auto registration = Registration(segment_);
    bool threw = false;
    {
        // The first Replica-sized allocation reserves quarantine capacity.
        // Failure preparing the detached vector must leave metadata untouched.
        FailAllocation fail(sizeof(Replica), 1);
        try {
            (void)service_->PutRevoke(owner_, "ordinary", TenantId::Default(),
                                      ReplicaType::MEMORY);
        } catch (const std::bad_alloc&) {
            threw = true;
        }
    }
    ASSERT_TRUE(failure_consumed);
    ASSERT_TRUE(threw);
    EXPECT_EQ(registration->WriteHoldCount(), 1u);
    EXPECT_EQ(service_->GetStorageUsageSnapshot().memory.used_bytes, 1024u);
    {
        MasterServiceTestPeer::MetadataAccessorRO access(
            service_.get(),
            MasterServiceTestPeer(*service_).MakeObjectIdentityForRequest(
                "ordinary", TenantId::Default()));
        ASSERT_TRUE(access.Exists());
        EXPECT_EQ(access.Get().CountReplicas(), 1u);
    }
    const auto other = Put("other", segment_);
    ASSERT_TRUE(other);
    EXPECT_NE(other->front()
                  .get_memory_descriptor()
                  .buffer_descriptor.buffer_address_,
              address);
    // A retry settles the original allocation, not a replacement extent.
    ASSERT_TRUE(service_->PutEnd(owner_, EndMeta("ordinary", *put),
                                 TenantId::Default(), ReplicaType::MEMORY));
}

TEST_F(OrdinaryWriteLifetimeTest,
       InPlaceUpsertWrapperFailurePreservesEveryCompletedReplica) {
    const auto second = Mount("second-target", 0x710000000, 'b');
    ReplicateConfig config;
    config.replica_num = 2;
    const auto put = service_->PutStart(owner_, "ordinary", TenantId::Default(),
                                        1024, config);
    ASSERT_TRUE(put);
    ASSERT_EQ(put->size(), 2u);
    ASSERT_TRUE(service_->PutEnd(owner_, EndMeta("ordinary", *put),
                                 TenantId::Default(), ReplicaType::MEMORY));
    auto first_registration = Registration(segment_);
    auto second_registration = Registration(second);
    bool threw = false;
    {
        // Exclude same-sized std::function preparation before any target hold.
        // Of the owning wrappers allocated after acquisition, fail the second.
        FailAllocation fail(sizeof(SegmentWriteHold), 1,
                            first_registration.get(),
                            second_registration.get());
        try {
            (void)service_->UpsertStart(owner_, "ordinary", TenantId::Default(),
                                        1024, config);
        } catch (const std::bad_alloc&) {
            threw = true;
        }
    }
    ASSERT_TRUE(failure_consumed);
    ASSERT_TRUE(threw);
    ASSERT_EQ(first_registration->WriteHoldCount(), 0u);
    ASSERT_EQ(second_registration->WriteHoldCount(), 0u);
    const auto readable =
        service_->GetReplicaList("ordinary", TenantId::Default());
    ASSERT_TRUE(readable);
    EXPECT_EQ(readable->replicas.size(), 2u);
    EXPECT_EQ(service_->GetStorageUsageSnapshot().memory.used_bytes, 2048u);
    const auto retry = service_->UpsertStart(owner_, "ordinary",
                                             TenantId::Default(), 1024, config);
    ASSERT_TRUE(retry);
    EXPECT_EQ(first_registration->WriteHoldCount(), 1u);
    EXPECT_EQ(second_registration->WriteHoldCount(), 1u);
    ASSERT_TRUE(service_->PutEnd(owner_, EndMeta("ordinary", *retry),
                                 TenantId::Default(), ReplicaType::MEMORY));
}

TEST_F(OrdinaryWriteLifetimeTest, InPlaceUpsertReacquiresHoldBeforeExposure) {
    auto put = Put("ordinary", segment_);
    ASSERT_TRUE(put);
    const auto address =
        put->front().get_memory_descriptor().buffer_descriptor.buffer_address_;
    ASSERT_TRUE(service_->PutEnd(owner_, EndMeta("ordinary", *put),
                                 TenantId::Default(), ReplicaType::MEMORY));
    auto registration = Registration(segment_);
    EXPECT_EQ(registration->WriteHoldCount(), 0u);
    auto upsert = service_->UpsertStart(owner_, "ordinary", TenantId::Default(),
                                        1024, ReplicateConfig{});
    ASSERT_TRUE(upsert);
    EXPECT_EQ(upsert->front()
                  .get_memory_descriptor()
                  .buffer_descriptor.buffer_address_,
              address);
    EXPECT_TRUE(upsert->front().get_memory_descriptor().strict_tcp_instance);
    EXPECT_EQ(registration->WriteHoldCount(), 1u);
    ASSERT_TRUE(service_->PutRevoke(owner_, "ordinary", TenantId::Default(),
                                    ReplicaType::MEMORY));
    Reap();
    EXPECT_EQ(registration->WriteHoldCount(), 1u);
}

TEST_F(OrdinaryWriteLifetimeTest, CopyAndMoveTargetsKeepHoldsAcrossRevoke) {
    const auto copy_target = Mount("copy-target", 0x710000000, 'b');
    const auto move_target = Mount("move-target", 0x720000000, 'c');
    auto put = Put("ordinary", segment_);
    ASSERT_TRUE(put);
    ASSERT_TRUE(service_->PutEnd(owner_, EndMeta("ordinary", *put),
                                 TenantId::Default(), ReplicaType::MEMORY));
    const auto copy =
        service_->CopyStart(owner_, "ordinary", TenantId::Default(),
                            segment_.name, {copy_target.name});
    ASSERT_TRUE(copy);
    ASSERT_EQ(copy->targets.size(), 1u);
    EXPECT_TRUE(
        copy->targets.front().get_memory_descriptor().strict_tcp_instance);
    auto copy_registration = Registration(copy_target);
    EXPECT_EQ(copy_registration->WriteHoldCount(), 1u);
    ASSERT_TRUE(service_->CopyRevoke(owner_, "ordinary", TenantId::Default()));
    Reap();
    EXPECT_EQ(copy_registration->WriteHoldCount(), 1u);

    const auto move =
        service_->MoveStart(owner_, "ordinary", TenantId::Default(),
                            segment_.name, move_target.name);
    ASSERT_TRUE(move);
    ASSERT_TRUE(move->target);
    EXPECT_TRUE(move->target->get_memory_descriptor().strict_tcp_instance);
    auto move_registration = Registration(move_target);
    EXPECT_EQ(move_registration->WriteHoldCount(), 1u);
    ASSERT_TRUE(service_->MoveRevoke(owner_, "ordinary", TenantId::Default()));
    Reap();
    EXPECT_EQ(move_registration->WriteHoldCount(), 1u);
}

TEST_F(OrdinaryWriteLifetimeTest, SameClientOldEndCannotSettleReplacementPut) {
    auto first = Put("ordinary", segment_);
    ASSERT_TRUE(first);
    const auto old_meta = EndMeta("ordinary", *first);
    auto replacement = service_->UpsertStart(
        owner_, "ordinary", TenantId::Default(), 1024, ReplicateConfig{});
    ASSERT_TRUE(replacement);
    auto registration = Registration(segment_);
    EXPECT_EQ(registration->WriteHoldCount(), 2u);
    auto stale = service_->PutEnd(owner_, old_meta, TenantId::Default(),
                                  ReplicaType::MEMORY);
    ASSERT_FALSE(stale);
    EXPECT_EQ(stale.error(), ErrorCode::INVALID_WRITE);
    EXPECT_FALSE(service_->PutEnd(owner_, "ordinary", TenantId::Default(),
                                  ReplicaType::MEMORY));
    EXPECT_EQ(registration->WriteHoldCount(), 2u);
    ASSERT_TRUE(service_->PutEnd(owner_, EndMeta("ordinary", *replacement),
                                 TenantId::Default(), ReplicaType::MEMORY));
    EXPECT_EQ(registration->WriteHoldCount(), 1u);
}

TEST_F(OrdinaryWriteLifetimeTest, InPlaceUpsertRenewsIdentityOnSameReplica) {
    auto put = Put("ordinary", segment_);
    ASSERT_TRUE(put);
    const auto old_meta = EndMeta("ordinary", *put);
    ASSERT_TRUE(service_->PutEnd(owner_, old_meta, TenantId::Default(),
                                 ReplicaType::MEMORY));
    auto upsert = service_->UpsertStart(owner_, "ordinary", TenantId::Default(),
                                        1024, ReplicateConfig{});
    ASSERT_TRUE(upsert);
    ASSERT_EQ(upsert->front().id, put->front().id);
    EXPECT_NE(*upsert->front().get_memory_descriptor().strict_write_id,
              *put->front().get_memory_descriptor().strict_write_id);
    auto stale = service_->UpsertEnd(owner_, old_meta, TenantId::Default(),
                                     ReplicaType::MEMORY);
    ASSERT_FALSE(stale);
    EXPECT_EQ(stale.error(), ErrorCode::INVALID_WRITE);
    EXPECT_EQ(Registration(segment_)->WriteHoldCount(), 1u);
    auto current_meta = EndMeta("ordinary", *upsert);
    ASSERT_TRUE(service_->UpsertEnd(owner_, current_meta, TenantId::Default(),
                                    ReplicaType::MEMORY));
    // The exact successful End remains idempotent after its hold is released.
    EXPECT_TRUE(service_->UpsertEnd(owner_, current_meta, TenantId::Default(),
                                    ReplicaType::MEMORY));
}

TEST_F(OrdinaryWriteLifetimeTest, SameClientOldCopyEndCannotSettleNewCopy) {
    const auto target = Mount("copy-target", 0x710000000, 'b');
    auto put = Put("ordinary", segment_);
    ASSERT_TRUE(put);
    ASSERT_TRUE(service_->PutEnd(owner_, EndMeta("ordinary", *put),
                                 TenantId::Default(), ReplicaType::MEMORY));
    auto first = service_->CopyStart(owner_, "ordinary", TenantId::Default(),
                                     segment_.name, {target.name});
    ASSERT_TRUE(first);
    const auto old_proof = MakeStrictWriteProof(first->targets);
    ASSERT_TRUE(service_->CopyRevoke(owner_, "ordinary", TenantId::Default()));
    auto current = service_->CopyStart(owner_, "ordinary", TenantId::Default(),
                                       segment_.name, {target.name});
    ASSERT_TRUE(current);
    EXPECT_FALSE(service_->CopyEnd(owner_, "ordinary", TenantId::Default()));
    auto stale = service_->CopyEnd(owner_, "ordinary", TenantId::Default(),
                                   UUID{}, 0, old_proof);
    ASSERT_FALSE(stale);
    EXPECT_EQ(stale.error(), ErrorCode::INVALID_WRITE);
    EXPECT_EQ(Registration(target)->WriteHoldCount(), 2u);
    ASSERT_TRUE(service_->CopyEnd(owner_, "ordinary", TenantId::Default(),
                                  UUID{}, 0,
                                  MakeStrictWriteProof(current->targets)));
    EXPECT_EQ(Registration(target)->WriteHoldCount(), 1u);
}

TEST_F(OrdinaryWriteLifetimeTest, SameClientOldMoveEndCannotSettleNewMove) {
    const auto target = Mount("move-target", 0x720000000, 'c');
    auto put = Put("ordinary", segment_);
    ASSERT_TRUE(put);
    ASSERT_TRUE(service_->PutEnd(owner_, EndMeta("ordinary", *put),
                                 TenantId::Default(), ReplicaType::MEMORY));
    auto first = service_->MoveStart(owner_, "ordinary", TenantId::Default(),
                                     segment_.name, target.name);
    ASSERT_TRUE(first);
    ASSERT_TRUE(first->target);
    const auto old_proof = MakeStrictWriteProof({*first->target});
    ASSERT_TRUE(service_->MoveRevoke(owner_, "ordinary", TenantId::Default()));
    auto current = service_->MoveStart(owner_, "ordinary", TenantId::Default(),
                                       segment_.name, target.name);
    ASSERT_TRUE(current);
    ASSERT_TRUE(current->target);
    EXPECT_FALSE(service_->MoveEnd(owner_, "ordinary", TenantId::Default()));
    auto stale =
        service_->MoveEnd(owner_, "ordinary", TenantId::Default(), old_proof);
    ASSERT_FALSE(stale);
    EXPECT_EQ(stale.error(), ErrorCode::INVALID_WRITE);
    EXPECT_EQ(Registration(target)->WriteHoldCount(), 2u);
    ASSERT_TRUE(service_->MoveEnd(owner_, "ordinary", TenantId::Default(),
                                  MakeStrictWriteProof({*current->target})));
    EXPECT_EQ(Registration(target)->WriteHoldCount(), 1u);
}

TEST_F(OrdinaryWriteLifetimeTest, SameClientOldPromotionCannotSettleNewTarget) {
    // Publish a cold source directly; this test exercises settlement
    // identities, independently of the SSD scheduler and promotion admission
    // thresholds.
    {
        using Accessor = MasterServiceTestPeer::MetadataAccessorRW;
        const MasterServiceTestPeer::ObjectIdentity object_id{
            TenantId::Default(), "cold"};
        Accessor accessor(service_.get(), object_id, Accessor::CreateKey{});
        std::vector<Replica> replicas;
        replicas.emplace_back(owner_, 1024, segment_.te_endpoint,
                              ReplicaStatus::COMPLETE);
        accessor.Publish(owner_, std::chrono::system_clock::now(), 1024,
                         std::move(replicas), std::nullopt, false,
                         ObjectDataType::UNKNOWN, std::string{},
                         TenantId::Default(), std::string{"cold"});
    }
    ASSERT_TRUE(
        service_->RegisterPrefetchTask(owner_, "cold", TenantId::Default()));
    auto first = service_->PromotionAllocStart(
        owner_, "cold", TenantId::Default(), 1024, {segment_.name});
    ASSERT_TRUE(first);
    const auto old_proof = MakeStrictWriteProof({first->memory_descriptor});
    ASSERT_TRUE(
        service_->NotifyPromotionFailure(owner_, "cold", TenantId::Default()));
    ASSERT_TRUE(
        service_->RegisterPrefetchTask(owner_, "cold", TenantId::Default()));
    auto current = service_->PromotionAllocStart(
        owner_, "cold", TenantId::Default(), 1024, {segment_.name});
    ASSERT_TRUE(current);
    EXPECT_FALSE(
        service_->NotifyPromotionSuccess(owner_, "cold", TenantId::Default()));
    auto stale = service_->NotifyPromotionSuccess(
        owner_, "cold", TenantId::Default(), old_proof);
    ASSERT_FALSE(stale);
    EXPECT_EQ(stale.error(), ErrorCode::INVALID_WRITE);
    EXPECT_EQ(Registration(segment_)->WriteHoldCount(), 2u);
    ASSERT_TRUE(service_->NotifyPromotionSuccess(
        owner_, "cold", TenantId::Default(),
        MakeStrictWriteProof({current->memory_descriptor})));
    EXPECT_EQ(Registration(segment_)->WriteHoldCount(), 1u);
}

TEST_F(OrdinaryWriteLifetimeTest, WrongMasterProofCannotSettleProtectedWrite) {
    auto put = Put("ordinary", segment_);
    ASSERT_TRUE(put);
    auto metadata = EndMeta("ordinary", *put);
    metadata.strict_write_proof->master_instance = generate_uuid();
    auto ended = service_->PutEnd(owner_, metadata, TenantId::Default(),
                                  ReplicaType::MEMORY);
    ASSERT_FALSE(ended);
    EXPECT_EQ(ended.error(), ErrorCode::INVALID_WRITE);
    EXPECT_EQ(Registration(segment_)->WriteHoldCount(), 1u);
}

TEST_F(OrdinaryWriteLifetimeTest, PublishedPieceRejectsOrdinaryEnd) {
    auto request = Piece();
    auto piece = service_->PiecePutStart(owner_, request, TenantId::Default());
    ASSERT_TRUE(piece);
    ASSERT_TRUE(piece->token);
    auto finish = service_->PiecePutFinish(
        owner_,
        {request.key, *piece->token, request.operation_seq,
         PiecePutFinishOutcome::APPLIED},
        TenantId::Default());
    ASSERT_TRUE(finish);
    ASSERT_EQ(finish->round_state, PiecePutRoundState::ROUND_PUBLISHED);
    auto ended = service_->PutEnd(owner_, request.key, TenantId::Default(),
                                  ReplicaType::MEMORY);
    ASSERT_FALSE(ended);
    EXPECT_EQ(ended.error(), ErrorCode::INVALID_WRITE);
}

TEST_F(OrdinaryWriteLifetimeTest, FailedUnexposedAllocationDoesNotLeakHolds) {
    ReplicateConfig config;
    EXPECT_FALSE(service_->PutStart(owner_, "too-large", TenantId::Default(),
                                    segment_.size + 1024, config));
    EXPECT_EQ(Registration(segment_)->WriteHoldCount(), 0u);
    EXPECT_EQ(service_->GetStorageUsageSnapshot().memory.used_bytes, 0u);
}

TEST_F(OrdinaryWriteLifetimeTest, EndpointRetirementPrecedesDelayedMount) {
    const auto instance = std::string(32, 'z');
    ASSERT_TRUE(service_->RetirePiecePutEndpoint(owner_, instance));
    ASSERT_TRUE(service_->RetirePiecePutEndpoint(owner_, instance));
    Segment delayed = segment_;
    delayed.id = generate_uuid();
    delayed.name = "delayed";
    delayed.base += segment_.size;
    delayed.tcp_instance = instance;
    auto mounted = service_->MountSegment(delayed, owner_);
    ASSERT_FALSE(mounted);
    EXPECT_EQ(mounted.error(), ErrorCode::OBJECT_REPLICA_BUSY);
    auto remounted = service_->ReMountSegment({delayed}, owner_);
    EXPECT_FALSE(remounted);
}

TEST_F(OrdinaryWriteLifetimeTest, EndpointRetirementClosesAllItsSegments) {
    Segment another = segment_;
    another.id = generate_uuid();
    another.name = "second-region";
    another.base += segment_.size;
    ASSERT_TRUE(service_->MountSegment(another, owner_));
    ASSERT_TRUE(Put("first", segment_));
    ASSERT_TRUE(Put("second", another));
    auto first_registration = Registration(segment_);
    auto second_registration = Registration(another);
    ASSERT_TRUE(
        service_->RetirePiecePutEndpoint(owner_, *segment_.tcp_instance));
    EXPECT_TRUE(first_registration->IsPhysicallyRetired());
    EXPECT_TRUE(second_registration->IsPhysicallyRetired());
    EXPECT_FALSE(Put("after", segment_));
    EXPECT_FALSE(service_->MountSegment(another, owner_));
}

TEST_F(OrdinaryWriteLifetimeTest, OtherOwnerCannotClaimOrRetireAnInstance) {
    const auto other = generate_uuid();
    auto retired =
        service_->RetirePiecePutEndpoint(other, *segment_.tcp_instance);
    ASSERT_FALSE(retired);
    EXPECT_EQ(retired.error(), ErrorCode::ILLEGAL_CLIENT);
    Segment alias = segment_;
    alias.id = generate_uuid();
    alias.name = "wrong-owner";
    alias.base += segment_.size;
    EXPECT_FALSE(service_->MountSegment(alias, other));
    EXPECT_TRUE(Put("still-serving", segment_));
}

TEST_F(OrdinaryWriteLifetimeTest, ProtectedEndpointCannotDowngradeToLegacy) {
    Segment legacy = segment_;
    legacy.id = generate_uuid();
    legacy.name = "legacy-alias";
    legacy.base += segment_.size;
    legacy.tcp_instance.reset();
    EXPECT_FALSE(service_->MountSegment(legacy, owner_));
    ASSERT_TRUE(
        service_->RetirePiecePutEndpoint(owner_, *segment_.tcp_instance));
    EXPECT_FALSE(service_->MountSegment(legacy, owner_));
    EXPECT_FALSE(service_->ReMountSegment({legacy}, owner_));
}

TEST_F(OrdinaryWriteLifetimeTest, LegacyEndpointCannotUpgradeAfterExposure) {
    Segment legacy = segment_;
    legacy.id = generate_uuid();
    legacy.name = "legacy-target";
    legacy.base += segment_.size;
    legacy.te_endpoint = "127.0.0.1:23900";
    legacy.tcp_instance.reset();
    ASSERT_TRUE(service_->MountSegment(legacy, owner_));
    ASSERT_TRUE(Put("legacy-write", legacy));
    Segment upgraded = legacy;
    upgraded.tcp_instance = std::string(32, 'q');
    EXPECT_FALSE(service_->MountSegment(upgraded, owner_));
    upgraded.id = generate_uuid();
    upgraded.name = "upgraded-alias";
    upgraded.base += segment_.size;
    EXPECT_FALSE(service_->MountSegment(upgraded, owner_));
}

TEST(OrdinaryWriteLegacyTest, LegacyTcpDescriptorsKeepTheOrdinaryPath) {
    MasterService service;
    const auto owner = generate_uuid();
    Segment segment;
    segment.id = generate_uuid();
    segment.name = "legacy";
    segment.base = 0x730000000;
    segment.size = 16 * 1024 * 1024;
    segment.protocol = "tcp";
    segment.te_endpoint = "127.0.0.1:23100";
    ASSERT_TRUE(service.MountSegment(segment, owner));
    auto put = service.PutStart(owner, "legacy", TenantId::Default(), 1024,
                                ReplicateConfig{});
    ASSERT_TRUE(put);
    EXPECT_FALSE(put->front().get_memory_descriptor().strict_tcp_instance);
    ASSERT_TRUE(service.PutRevoke(owner, "legacy", TenantId::Default(),
                                  ReplicaType::MEMORY));
    EXPECT_EQ(service.GetStorageUsageSnapshot().memory.used_bytes, 0u);
}

}  // namespace
}  // namespace mooncake::test
