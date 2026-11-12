#include "allocation_strategy.h"
#include "master_service.h"
#include "master_service/master_service_test_peer.h"

#include <gtest/gtest.h>

#include <chrono>
#include <functional>
#include <memory>
#include <set>
#include <string>
#include <vector>

namespace mooncake::test {
namespace {

// Observe the actual candidate snapshot and interleave a registration change
// after allocation, before Master reacquires its exact write hold.
class ObservedAllocationStrategy final : public RandomAllocationStrategy {
   public:
    std::function<void()> after_first_allocation;
    std::vector<const AllocatorManager*> snapshots;
    std::vector<size_t> candidate_counts;

    tl::expected<std::vector<Replica>, ErrorCode> Allocate(
        const AllocatorManager& manager, size_t length, size_t count,
        const std::vector<std::string>& preferred,
        const std::set<std::string>& excluded, ReplicaType type) override {
        snapshots.push_back(&manager);
        candidate_counts.push_back(manager.getNames().size());
        auto result = RandomAllocationStrategy::Allocate(
            manager, length, count, preferred, excluded, type);
        if (snapshots.size() == 1 && after_first_allocation) {
            after_first_allocation();
        }
        return result;
    }
};

class MasterPiecePutPolicyTest : public ::testing::Test {
   protected:
    void Create(
        AllocationStrategyType strategy = AllocationStrategyType::RANDOM,
        bool offload = false, size_t queue_limit = 64) {
        MasterServiceConfig config;
        config.enable_piece_put = true;
        config.memory_allocator = BufferAllocatorType::OFFSET;
        config.allocation_strategy_type = strategy;
        config.max_piece_put_rounds = 64;
        config.default_kv_lease_ttl = 0;
        config.enable_offload = offload;
        config.offloading_queue_limit = queue_limit;
        service_ = std::make_unique<MasterService>(config);
        MasterServiceTestPeer(*service_)
            .StopPiecePutMaintenanceWorkersForTesting();
    }

    Segment MakeSegment(std::string name, std::string host,
                        bool strict = true) {
        Segment segment;
        segment.id = generate_uuid();
        segment.name = std::move(name);
        segment.host_id = std::move(host);
        segment.base = 0xa00000000 + next_segment_ * 32 * 1024 * 1024;
        segment.size = 16 * 1024 * 1024;
        segment.protocol = "tcp";
        segment.te_endpoint =
            "127.0.0.1:" + std::to_string(24000 + next_segment_++);
        if (strict) {
            segment.tcp_instance = "instance-" + segment.name;
            segment.piece_put_master_instance =
                *service_->GetPiecePutMasterInstance();
        }
        return segment;
    }

    Segment Mount(std::string name, std::string host, bool strict = true) {
        auto segment = MakeSegment(std::move(name), std::move(host), strict);
        EXPECT_TRUE(service_->MountSegment(segment, owner_));
        return segment;
    }

    std::shared_ptr<SegmentAllocatorRegistration> Registration(
        const Segment& segment) {
        return MasterServiceTestPeer::SegmentManager(*service_)
            .getSegmentAccess()
            .GetRegistration(segment.id);
    }

    PiecePutStartRequest Request(std::string key, uint64_t sequence,
                                 const PiecePutAdmission& admission) {
        return {std::move(key), 1024,         0,        1024, "round", sequence,
                false,          std::nullopt, admission};
    }

    auto Start(const PiecePutStartRequest& request) {
        return service_->PiecePutStart(producer_, request, TenantId::Default());
    }

    auto Finish(const PiecePutStartRequest& request,
                const PiecePutStartResponse& grant,
                PiecePutFinishOutcome outcome) {
        return service_->PiecePutFinish(
            producer_,
            {request.key, *grant.token, request.operation_seq, outcome},
            TenantId::Default());
    }

    void ExpectOffloadState(const std::string& key, bool task, uint32_t refs) {
        auto object = MasterServiceTestPeer::ReadObject(
            *service_, {TenantId::Default(), key});
        ASSERT_TRUE(object);
        EXPECT_EQ(static_cast<bool>(object->state().offloading_task), task);
        const Replica* source = nullptr;
        for (const auto& replica : object->metadata().GetAllReplicas()) {
            if (replica.is_memory_replica()) {
                source = &replica;
                break;
            }
        }
        ASSERT_NE(source, nullptr);
        EXPECT_EQ(source->get_refcnt(), refs);
    }

    void ExpectPublished(const PiecePutStartRequest& request,
                         const PiecePutStartResponse& grant) {
        const auto finished =
            Finish(request, grant, PiecePutFinishOutcome::APPLIED);
        ASSERT_TRUE(finished);
        EXPECT_EQ(finished->round_state, PiecePutRoundState::ROUND_PUBLISHED);
        auto lookup = request;
        lookup.lookup_only = true;
        lookup.token = grant.token;
        const auto status = Start(lookup);
        ASSERT_TRUE(status);
        EXPECT_EQ(status->round_state, PiecePutRoundState::ROUND_PUBLISHED);
        EXPECT_FALSE(status->replica);
    }

    std::unique_ptr<MasterService> service_;
    const UUID owner_ = generate_uuid();
    const UUID producer_ = generate_uuid();
    size_t next_segment_{0};
};

TEST_F(MasterPiecePutPolicyTest, HostHintBelongsToTheBoundedAdmissionSession) {
    Create();
    MasterServiceTestPeer peer(*service_);
    const auto global_hosts = peer.ClientHostIdCountForTesting();
    const auto first = service_->GetPiecePutAdmission(producer_, 0, "host-a");
    ASSERT_TRUE(first);
    {
        auto guard = peer.PiecePutAdmissionsForTesting().Acquire(
            producer_, first->session_id, PiecePutState::Clock::now());
        ASSERT_TRUE(guard);
        ASSERT_TRUE(static_cast<bool>(*guard));
        EXPECT_EQ(guard->HostId(), "host-a");
    }
    const auto rotated =
        service_->GetPiecePutAdmission(producer_, first->session_id, "host-b");
    ASSERT_TRUE(rotated);
    EXPECT_NE(rotated->session_id, first->session_id);
    // A lost-reply retry returns the same immutable host binding.
    const auto retry = service_->GetPiecePutAdmission(
        producer_, first->session_id, "different-retry-host");
    ASSERT_TRUE(retry);
    EXPECT_EQ(retry->session_id, rotated->session_id);
    {
        auto old = peer.PiecePutAdmissionsForTesting().Acquire(
            producer_, first->session_id, PiecePutState::Clock::now());
        ASSERT_TRUE(old);
        EXPECT_FALSE(static_cast<bool>(*old));
        auto current = peer.PiecePutAdmissionsForTesting().Acquire(
            producer_, rotated->session_id, PiecePutState::Clock::now());
        ASSERT_TRUE(current);
        ASSERT_TRUE(static_cast<bool>(*current));
        EXPECT_EQ(current->HostId(), "host-b");
    }
    peer.PiecePutAdmissionsForTesting().Sweep(PiecePutState::Clock::now() +
                                              std::chrono::seconds(61));
    auto expired = peer.PiecePutAdmissionsForTesting().Acquire(
        producer_, rotated->session_id, PiecePutState::Clock::now());
    ASSERT_TRUE(expired);
    EXPECT_FALSE(static_cast<bool>(*expired));
    EXPECT_EQ(peer.ClientHostIdCountForTesting(), global_hosts);
    const auto invalid = service_->GetPiecePutAdmission(
        generate_uuid(), 0,
        std::string(PiecePutAdmissionRegistry::kMaxHostIdBytes + 1, 'h'));
    ASSERT_FALSE(invalid);
    EXPECT_EQ(invalid.error(), ErrorCode::INVALID_PARAMS);
    EXPECT_EQ(peer.ClientHostIdCountForTesting(), global_hosts);
}

TEST_F(MasterPiecePutPolicyTest,
       LocalFirstUsesAdmissionHostAndStrictCandidates) {
    Create(AllocationStrategyType::LOCAL_FIRST);
    const auto remote = Mount("remote-strict", "host-remote");
    const auto legacy = Mount("local-legacy", "host-local", false);
    const auto closing = Mount("local-closing", "host-local");
    const auto local = Mount("local-strict", "host-local");
    ASSERT_TRUE(service_->GracefulUnmountSegment(closing.id, owner_, 60000));
    const auto admission =
        service_->GetPiecePutAdmission(producer_, 0, "host-local");
    ASSERT_TRUE(admission);
    for (uint64_t sequence = 1; sequence <= 16; ++sequence) {
        const auto request =
            Request("local-" + std::to_string(sequence), sequence, *admission);
        const auto grant = Start(request);
        ASSERT_TRUE(grant);
        ASSERT_TRUE(grant->replica);
        EXPECT_EQ(grant->replica->get_memory_descriptor()
                      .buffer_descriptor.transport_endpoint_,
                  local.te_endpoint);
        EXPECT_EQ(grant->tcp_instance, *local.tcp_instance);
        ASSERT_TRUE(Finish(request, *grant, PiecePutFinishOutcome::NO_POST));
    }
    EXPECT_EQ(Registration(remote)->WriteHoldCount(), 0u);
    EXPECT_EQ(Registration(legacy)->WriteHoldCount(), 0u);
    EXPECT_EQ(Registration(closing)->WriteHoldCount(), 0u);
}

TEST_F(MasterPiecePutPolicyTest, FreeRatioFirstUsesTheConfiguredStrategy) {
    Create(AllocationStrategyType::FREE_RATIO_FIRST);
    const auto full = Mount("fuller", "host-a");
    const auto empty = Mount("emptier", "host-b");
    auto filler = Registration(full)->Allocate(12 * 1024 * 1024);
    ASSERT_TRUE(filler);
    const auto admission = service_->GetPiecePutAdmission(producer_, 0);
    ASSERT_TRUE(admission);
    for (uint64_t sequence = 1; sequence <= 8; ++sequence) {
        const auto request =
            Request("ratio-" + std::to_string(sequence), sequence, *admission);
        const auto grant = Start(request);
        ASSERT_TRUE(grant);
        ASSERT_TRUE(grant->replica);
        EXPECT_EQ(grant->replica->get_memory_descriptor()
                      .buffer_descriptor.transport_endpoint_,
                  empty.te_endpoint);
        ASSERT_TRUE(Finish(request, *grant, PiecePutFinishOutcome::NO_POST));
    }
}

TEST_F(MasterPiecePutPolicyTest, BatchReusesOneSnapshotUntilTheNextCall) {
    Create();
    const auto original = Mount("original", "host-a");
    auto added = MakeSegment("added-during-batch", "host-b");
    auto observed = std::make_shared<ObservedAllocationStrategy>();
    const auto session = Registration(original)->GetClientSession();
    ASSERT_TRUE(session);
    observed->after_first_allocation = [&] {
        auto access =
            MasterServiceTestPeer::SegmentManager(*service_).getSegmentAccess();
        ASSERT_EQ(access.MountSegment(added, session), ErrorCode::OK);
        ASSERT_EQ(access.BindTcpInstance(added.id, *added.tcp_instance),
                  ErrorCode::OK);
    };
    MasterServiceTestPeer::AllocationStrategy(*service_) = observed;
    const auto admission = service_->GetPiecePutAdmission(producer_, 0);
    ASSERT_TRUE(admission);
    std::vector<PiecePutStartRequest> requests;
    for (uint64_t sequence = 1; sequence <= 32; ++sequence) {
        requests.push_back(
            Request("batch-" + std::to_string(sequence), sequence, *admission));
    }
    const auto grants =
        service_->BatchPiecePutStart(producer_, requests, TenantId::Default());
    ASSERT_EQ(grants.size(), requests.size());
    ASSERT_EQ(observed->candidate_counts.size(), requests.size());
    for (size_t index = 0; index < requests.size(); ++index) {
        ASSERT_TRUE(grants[index]);
        ASSERT_TRUE(grants[index]->replica);
        EXPECT_EQ(observed->candidate_counts[index], 1u);
        EXPECT_EQ(observed->snapshots[index], observed->snapshots.front());
        ASSERT_TRUE(Finish(requests[index], *grants[index],
                           PiecePutFinishOutcome::NO_POST));
    }
    const auto next = Request("next-batch", 33, *admission);
    const auto next_grants =
        service_->BatchPiecePutStart(producer_, {next}, TenantId::Default());
    ASSERT_EQ(next_grants.size(), 1u);
    ASSERT_TRUE(next_grants.front());
    ASSERT_TRUE(next_grants.front()->token);
    EXPECT_EQ(observed->candidate_counts.back(), 2u);
    ASSERT_TRUE(
        Finish(next, *next_grants.front(), PiecePutFinishOutcome::NO_POST));
}

TEST_F(MasterPiecePutPolicyTest,
       RegistrationClosedAfterAllocationNeverExposesADescriptor) {
    Create();
    const auto target = Mount("closing-after-allocation", "host-a");
    const auto registration = Registration(target);
    auto observed = std::make_shared<ObservedAllocationStrategy>();
    observed->after_first_allocation = [&] {
        auto access =
            MasterServiceTestPeer::SegmentManager(*service_).getSegmentAccess();
        size_t capacity = 0;
        ASSERT_EQ(access.PrepareUnmountSegment(target.id, capacity),
                  ErrorCode::OK);
    };
    MasterServiceTestPeer::AllocationStrategy(*service_) = observed;
    const auto admission = service_->GetPiecePutAdmission(producer_, 0);
    ASSERT_TRUE(admission);
    const auto request = Request("never-exposed", 1, *admission);
    const auto denied = Start(request);
    EXPECT_FALSE(denied);
    EXPECT_EQ(registration->WriteHoldCount(), 0u);
    EXPECT_EQ(service_->GetStorageUsageSnapshot().memory.used_bytes, 0u);
    EXPECT_FALSE(MasterServiceTestPeer::NeedMemEviction(*service_).load());
    const auto replay = Start(request);
    ASSERT_TRUE(replay);
    EXPECT_TRUE(replay->admission_closed);
    EXPECT_FALSE(replay->replica);
    EXPECT_FALSE(replay->token);
}

TEST_F(MasterPiecePutPolicyTest,
       EagerQueueRejectionAndDuplicateFinishKeepOneTaskAndOneSourcePin) {
    Create(AllocationStrategyType::RANDOM, true, 1);
    Mount("offload-target", "host-a");
    ASSERT_TRUE(service_->MountLocalDiskSegment(owner_, true));
    const auto admission = service_->GetPiecePutAdmission(producer_, 0);
    ASSERT_TRUE(admission);
    const auto accepted = Request("queued", 1, *admission);
    const auto first = Start(accepted);
    ASSERT_TRUE(first);
    ASSERT_TRUE(first->token);
    ExpectPublished(accepted, *first);
    ExpectOffloadState(accepted.key, true, 1);
    const auto rejected = Request("queue-full", 2, *admission);
    const auto second = Start(rejected);
    ASSERT_TRUE(second);
    ASSERT_TRUE(second->token);
    ExpectPublished(rejected, *second);
    ExpectOffloadState(rejected.key, false, 0);
    const auto tasks = service_->OffloadObjectHeartbeat(owner_, true);
    ASSERT_TRUE(tasks);
    ASSERT_EQ(tasks->size(), 1u);
    EXPECT_EQ(tasks->front().key, accepted.key);
    ExpectPublished(accepted, *first);
    ExpectOffloadState(accepted.key, true, 1);
    const auto repeated = service_->OffloadObjectHeartbeat(owner_, true);
    ASSERT_TRUE(repeated);
    EXPECT_TRUE(repeated->empty());
    ASSERT_TRUE(service_->NotifyOffloadSuccess(
        owner_, *tasks, {StorageObjectMetadata{0, 0, 0, -1, ""}}));
    ExpectOffloadState(accepted.key, false, 0);
}

TEST_F(MasterPiecePutPolicyTest,
       PartialEagerFanoutKeepsTheEnqueuedMirrorAndExactlyOneSourcePin) {
    Create(AllocationStrategyType::RANDOM, true);
    const auto first = Mount("partial-first", "host-a");
    const auto second = MakeSegment("partial-no-disk", "host-b");
    const UUID second_owner = generate_uuid();
    ASSERT_TRUE(service_->MountSegment(second, second_owner));
    ASSERT_TRUE(service_->MountLocalDiskSegment(owner_, true));
    ReplicateConfig config;
    config.replica_num = 2;
    config.preferred_segments = {first.name, second.name};
    const std::string key = "partial-fanout";
    const auto replicas =
        service_->PutStart(producer_, key, TenantId::Default(), 1024, config);
    ASSERT_TRUE(replicas);
    ASSERT_EQ(replicas->size(), 2u);
    ObjectMeta end{key, std::nullopt};
    const auto proof = MakeStrictWriteProof(*replicas);
    ASSERT_TRUE(proof);
    end.strict_write_proof = *proof;
    ASSERT_TRUE(service_->PutEnd(producer_, end, TenantId::Default(),
                                 ReplicaType::MEMORY));
    {
        const auto object = MasterServiceTestPeer::ReadObject(
            *service_, {TenantId::Default(), key});
        ASSERT_TRUE(object);
        const auto& task = object->state().offloading_task;
        ASSERT_TRUE(task);
        ASSERT_EQ(task->mirror_clients.size(), 1u);
        EXPECT_EQ(task->mirror_clients.front(), owner_);
        uint32_t refs = 0;
        bool found_source = false;
        for (const auto& replica : object->metadata().GetAllReplicas()) {
            refs += replica.get_refcnt();
            if (replica.id() == task->source_id) {
                found_source = true;
                EXPECT_EQ(replica.get_refcnt(), 1u);
            }
        }
        EXPECT_EQ(refs, 1u);
        EXPECT_TRUE(found_source);
    }
    const auto tasks = service_->OffloadObjectHeartbeat(owner_, true);
    ASSERT_TRUE(tasks);
    ASSERT_EQ(tasks->size(), 1u);
    EXPECT_EQ(tasks->front().key, key);
    ASSERT_TRUE(service_->NotifyOffloadSuccess(
        owner_, *tasks, {StorageObjectMetadata{0, 0, 0, -1, ""}}));
    const auto object = MasterServiceTestPeer::ReadObject(
        *service_, {TenantId::Default(), key});
    ASSERT_TRUE(object);
    EXPECT_FALSE(object->state().offloading_task);
    for (const auto& replica : object->metadata().GetAllReplicas()) {
        EXPECT_EQ(replica.get_refcnt(), 0u);
    }
}

TEST_F(MasterPiecePutPolicyTest,
       HistoricalPublicationSurvivesOffloadAndMemoryReplicaRemoval) {
    Create(AllocationStrategyType::RANDOM, true);
    const auto target = Mount("offload-history", "host-a");
    ASSERT_TRUE(service_->MountLocalDiskSegment(owner_, true));
    const auto admission = service_->GetPiecePutAdmission(producer_, 0);
    ASSERT_TRUE(admission);
    const auto request = Request("history", 1, *admission);
    const auto grant = Start(request);
    ASSERT_TRUE(grant);
    ASSERT_TRUE(grant->token);
    ExpectPublished(request, *grant);
    const auto tasks = service_->OffloadObjectHeartbeat(owner_, true);
    ASSERT_TRUE(tasks);
    ASSERT_EQ(tasks->size(), 1u);
    ASSERT_TRUE(service_->NotifyOffloadSuccess(
        owner_, *tasks,
        {StorageObjectMetadata{0, 0, static_cast<int64_t>(request.key.size()),
                               1024, "127.0.0.1:24999"}}));
    ExpectOffloadState(request.key, false, 0);
    {
        const auto object = MasterServiceTestPeer::ReadObject(
            *service_, {TenantId::Default(), request.key});
        ASSERT_TRUE(object);
        EXPECT_EQ(object->metadata().GetAllReplicas().size(), 2u);
    }
    ExpectPublished(request, *grant);
    ASSERT_TRUE(service_->UnmountSegment(target.id, owner_));
    {
        MasterServiceTestPeer::MetadataAccessorRW object(
            service_.get(), {TenantId::Default(), request.key});
        ASSERT_TRUE(object.Exists());
        ASSERT_EQ(object.Get().GetAllReplicas().size(), 1u);
        EXPECT_TRUE(
            object.Get().GetAllReplicas().front().is_local_disk_replica());
    }
    ExpectPublished(request, *grant);
}

}  // namespace
}  // namespace mooncake::test
