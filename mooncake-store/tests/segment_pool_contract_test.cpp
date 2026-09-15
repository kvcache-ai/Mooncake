#include <gtest/gtest.h>

#include <array>
#include <chrono>
#include <stdexcept>

#include "master_metric_manager.h"
#include "nof_segment_manager.h"
#include "segment/pool_read_access.h"
#include "segment/pool_write_access.h"

namespace mooncake::test {
namespace {
constexpr uint64_t kSize = 16 * 1024 * 1024;

RegionDriverRegistry Drivers() {
    RegionDriverConfig config;
    config.memory_allocator = BufferAllocatorType::OFFSET;
    auto drivers = CreateRegionDrivers(config);
    EXPECT_TRUE(drivers);
    return std::move(*drivers);
}

Segment Region(size_t index, std::string name) {
    Segment segment;
    segment.id = UUID{1, index};
    segment.name = std::move(name);
    segment.base = 0x300000000ULL + index * kSize;
    segment.size = kSize;
    segment.te_endpoint = "endpoint-" + std::to_string(index);
    segment.host_id = "host";
    return segment;
}

Replica::Descriptor Descriptor(const Segment& region, uintptr_t address,
                               uint64_t size = 4096) {
    Replica::Descriptor desc;
    desc.id = 1;
    desc.status = ReplicaStatus::COMPLETE;
    desc.descriptor_variant =
        MemoryDescriptor{{size, address, "tcp", region.te_endpoint}};
    return desc;
}

std::shared_ptr<ClientLivenessRecord> Live() {
    return std::make_shared<ClientLivenessRecord>(
        ClientLivenessRecord::Clock::now());
}

TEST(SegmentPoolContractTest,
     QueriesAreOwnedAndSelectDrainTargetsWithoutCatalog) {
    SegmentPool pool(Drivers());
    const auto first = Region(0, "source");
    const auto second = Region(1, "target");
    const UUID owner{2, 0};
    ASSERT_EQ(pool.AcquireWriteAccess().MountSegment(first, owner),
              ErrorCode::OK);
    ASSERT_EQ(pool.AcquireWriteAccess().MountSegment(second, owner),
              ErrorCode::OK);
    const auto description = pool.AcquireReadAccess().FindSegment(first.id);
    ASSERT_TRUE(description);
    const std::vector<std::string> sources{first.name}, targets{second.name};
    {
        auto view = pool.AcquireReadAccess();
        UUID resolved_owner;
        EXPECT_EQ(view.GetSegmentOwner(first.name, resolved_owner),
                  ErrorCode::OK);
        EXPECT_EQ(resolved_owner, owner);
        EXPECT_EQ(view.SelectDrainTarget(first.name, {}, {}), second.name);
        EXPECT_FALSE(view.SelectDrainTarget(first.name, targets, {}));
        EXPECT_EQ(view.ValidateDrain(sources, sources),
                  ErrorCode::INVALID_PARAMS);
    }
    ASSERT_EQ(pool.AcquireWriteAccess().StartDrain(sources, targets),
              ErrorCode::OK);
    EXPECT_FALSE(pool.AcquireReadAccess().HasServingCandidate(first.name));
    pool.AcquireWriteAccess().Clear();
    EXPECT_EQ(description->segment, first);
    EXPECT_EQ(description->client_id, owner);
}

TEST(SegmentPoolContractTest, UnmountReleaseAndAckAreSeparateAndIdempotent) {
    SegmentPool pool(Drivers());
    const auto region = Region(0, "unmount");
    const UUID owner{2, 0};
    ASSERT_EQ(pool.AcquireWriteAccess().MountSegment(region, owner),
              ErrorCode::OK);
    auto replica = pool.AllocateInSegment(region.name, 4096);
    ASSERT_TRUE(replica);
    auto access = pool.AcquireWriteAccess();
    auto operation = access.BeginUnmount(region.id, owner);
    ASSERT_TRUE(operation);
    EXPECT_TRUE(replica->has_invalid_mem_handle());
    EXPECT_EQ(access.BeginUnmount(region.id, owner)->id, operation->id);
    EXPECT_EQ(access.AcknowledgeUnmount(operation->id),
              ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    EXPECT_EQ(access.ReleaseUnmountedResources(operation->id), ErrorCode::OK);
    EXPECT_EQ(access.ReleaseUnmountedResources(operation->id), ErrorCode::OK);
    EXPECT_FALSE(access.FindSegment(region.id));
    EXPECT_TRUE(access.IsNameReserved(region.name));
    EXPECT_EQ(access.MountSegment(region, owner), ErrorCode::INVALID_PARAMS);
    EXPECT_EQ(access.AcknowledgeUnmount(operation->id), ErrorCode::OK);
    EXPECT_EQ(access.AcknowledgeUnmount(operation->id), ErrorCode::OK);
    ASSERT_EQ(access.MountSegment(region, owner), ErrorCode::OK);
    EXPECT_EQ(access.ReleaseUnmountedResources(operation->id),
              ErrorCode::SEGMENT_NOT_FOUND);
    EXPECT_TRUE(access.FindSegment(region.id));
}

TEST(SegmentPoolContractTest, ClearingPoolInvalidatesOutstandingOperations) {
    SegmentPool pool(Drivers());
    const auto region = Region(0, "epoch");
    const UUID owner{2, 0};
    auto access = pool.AcquireWriteAccess();
    ASSERT_EQ(access.MountSegment(region, owner), ErrorCode::OK);
    auto old = access.BeginUnmount(region.id, owner);
    ASSERT_TRUE(old);
    access.Clear();
    ASSERT_EQ(access.MountSegment(region, owner), ErrorCode::OK);
    auto current = access.BeginUnmount(region.id, owner);
    ASSERT_TRUE(current);
    EXPECT_NE(current->id, old->id);
    EXPECT_EQ(access.ReleaseUnmountedResources(old->id),
              ErrorCode::SEGMENT_NOT_FOUND);
    EXPECT_EQ(access.AcknowledgeUnmount(old->id), ErrorCode::OK);
    EXPECT_TRUE(access.IsNameReserved(region.name));
}

TEST(SegmentPoolContractTest, SharedNameReservationsSurviveIndependentOwners) {
    SegmentPool pool(Drivers());
    const auto first = Region(0, "shared");
    const auto second = Region(1, "shared");
    auto access = pool.AcquireWriteAccess();
    ASSERT_EQ(access.MountSegment(first, UUID{2, 0}), ErrorCode::OK);
    ASSERT_EQ(access.MountSegment(second, UUID{2, 1}), ErrorCode::OK);
    auto a = access.BeginUnmount(first.id, UUID{2, 0});
    auto b = access.BeginUnmount(second.id, UUID{2, 1});
    ASSERT_TRUE(a);
    ASSERT_TRUE(b);
    ASSERT_EQ(access.ReleaseUnmountedResources(a->id), ErrorCode::OK);
    ASSERT_EQ(access.AcknowledgeUnmount(a->id), ErrorCode::OK);
    EXPECT_TRUE(access.IsNameReserved(first.name));
    ASSERT_EQ(access.ReleaseUnmountedResources(b->id), ErrorCode::OK);
    ASSERT_EQ(access.AcknowledgeUnmount(b->id), ErrorCode::OK);
    EXPECT_FALSE(access.IsNameReserved(first.name));
}

TEST(SegmentPoolContractTest,
     RecoveryValidatesAliasesRangesAndCapacityBeforePublication) {
    const auto region = Region(0, "recover");
    auto recovery =
        SegmentRecovery::Create({{region.name, region.te_endpoint, kSize}});
    ASSERT_TRUE(recovery);
    auto first = Descriptor(region, region.base);
    ASSERT_TRUE((*recovery)->Restore(first, 4096));
    EXPECT_FALSE((*recovery)->Restore(first, 4096));
    auto second = Descriptor(region, region.base + 4096);
    EXPECT_FALSE((*recovery)->Restore(second, 8192));
    EXPECT_TRUE((*recovery)->Restore(second, 4096));
    EXPECT_FALSE(
        SegmentRecovery::Create({{region.name, region.te_endpoint, kSize},
                                 {"other", region.name, kSize}}));
}

TEST(SegmentPoolContractTest, FailedRemountBatchPublishesNothing) {
    SegmentPool pool(Drivers());
    const auto first = Region(0, "first");
    auto bad = Region(1, "bad");
    bad.size = 0;
    const std::array segments{first, bad};
    auto request = pool.PlanRemount(segments, UUID{2, 0});
    ASSERT_TRUE(request);
    EXPECT_FALSE(pool.PrepareRemount(std::move(*request), Live()));
    EXPECT_TRUE(pool.AcquireReadAccess().Segments().empty());
    EXPECT_EQ(pool.GetMemoryUsage().capacity_bytes, 0U);
}

TEST(SegmentPoolContractTest,
     RemountReturnsBoundBuffersAndSettlesPlaceholderMetricsOnce) {
    auto& metrics = MasterMetricManager::instance();
    const auto before = metrics.get_allocated_mem_size();
    {
        SegmentPool pool(Drivers());
        const auto region = Region(0, "bindings");
        const auto desc = Descriptor(region, region.base);
        auto recovery =
            SegmentRecovery::Create({{region.name, region.te_endpoint, kSize}});
        ASSERT_TRUE(recovery);
        auto placeholder = (*recovery)->Restore(desc, 4096);
        ASSERT_TRUE(placeholder);
        EXPECT_TRUE(pool.InstallRecovery(std::move(*recovery))
                        .contains(region.te_endpoint));
        EXPECT_EQ(metrics.get_allocated_mem_size(), before + 4096);
        const std::array segments{region};
        auto request = pool.PlanRemount(segments, UUID{2, 0});
        ASSERT_TRUE(request);
        EXPECT_TRUE(request->NeedsBuffers());
        auto descriptor = desc.get_memory_descriptor().buffer_descriptor;
        descriptor.transport_endpoint_ = region.name;  // historical alias
        ASSERT_EQ(request->AddBuffer(42, descriptor), true);
        EXPECT_FALSE(request->AddBuffer(42, descriptor));
        std::vector<RestoredBuffer> buffers;
        {
            auto prepared = pool.PrepareRemount(std::move(*request), Live());
            ASSERT_TRUE(prepared);
            buffers = prepared->Commit();
        }
        ASSERT_EQ(buffers.size(), 1U);
        EXPECT_EQ(buffers.front().binding_id, 42U);
        EXPECT_TRUE(buffers.front().buffer->isAllocatorValid());
        EXPECT_EQ(metrics.get_allocated_mem_size(), before + 4096);
        auto allocator = pool.AcquireReadAccess().GetAllocator(region.id);
        auto repeated = pool.PlanRemount(segments, UUID{2, 0});
        ASSERT_TRUE(repeated);
        EXPECT_FALSE(repeated->NeedsBuffers());
        {
            auto prepared = pool.PrepareRemount(std::move(*repeated), Live());
            ASSERT_TRUE(prepared);
            EXPECT_TRUE(prepared->Commit().empty());
        }
        EXPECT_EQ(pool.AcquireReadAccess().GetAllocator(region.id), allocator);
        pool.AcquireWriteAccess().Clear();
        EXPECT_FALSE(buffers.front().buffer->isAllocatorValid());
    }
    EXPECT_EQ(metrics.get_allocated_mem_size(), before);
}

TEST(SegmentPoolContractTest, PendingExternalAckCannotBeLostInSnapshot) {
    SegmentPool pool(Drivers());
    const auto region = Region(0, "snapshot-operation");
    const UUID owner{2, 0};
    auto access = pool.AcquireWriteAccess();
    ASSERT_EQ(access.MountSegment(region, owner), ErrorCode::OK);
    auto operation = access.BeginUnmount(region.id, owner);
    ASSERT_TRUE(operation);
    EXPECT_FALSE(
        pool.CaptureSnapshot());  // externally quiesced; capture takes no lock
    ASSERT_EQ(access.ReleaseUnmountedResources(operation->id), ErrorCode::OK);
    EXPECT_FALSE(pool.CaptureSnapshot());
    ASSERT_EQ(access.AcknowledgeUnmount(operation->id), ErrorCode::OK);
    EXPECT_TRUE(pool.CaptureSnapshot());
}

TEST(SegmentPoolContractTest,
     PlacementResolvesHostHintsButPreservesExplicitAndExcludedTargets) {
    for (auto policy :
         {PlacementPolicyType::RANDOM, PlacementPolicyType::FREE_RATIO_FIRST,
          PlacementPolicyType::LOCAL_FIRST}) {
        SegmentPool pool(Drivers(), policy);
        auto local = Region(0, "local");
        local.host_id = "writer";
        auto remote = Region(1, "remote");
        remote.host_id = "other";
        ASSERT_EQ(pool.AcquireWriteAccess().MountSegment(local, UUID{2, 0}),
                  ErrorCode::OK);
        ASSERT_EQ(pool.AcquireWriteAccess().MountSegment(remote, UUID{2, 1}),
                  ErrorCode::OK);
        ReplicaAllocationRequest request;
        request.replicas.size = 4096;
        request.host_affinity = {"writer", "key"};
        request.placement.preferred_segment_name = remote.name;
        auto preferred = pool.AllocateReplicas(request);
        ASSERT_TRUE(preferred);
        EXPECT_EQ(preferred->front()
                      .get_descriptor()
                      .get_memory_descriptor()
                      .buffer_descriptor.transport_endpoint_,
                  remote.te_endpoint);
        const std::array excluded{remote.name};
        request.placement.excluded_segment_names = excluded;
        auto fallback = pool.AllocateReplicas(request);
        ASSERT_TRUE(fallback);
        EXPECT_EQ(fallback->front()
                      .get_descriptor()
                      .get_memory_descriptor()
                      .buffer_descriptor.transport_endpoint_,
                  local.te_endpoint);
        auto view = pool.AcquireReadAccess();
        const std::array existing{local.name};
        EXPECT_EQ(view.SelectReplicationTarget("key", 4096, existing,
                                               std::nullopt, 0.9),
                  remote.name);
        EXPECT_FALSE(view.SelectReplicationTarget("key", kSize, existing,
                                                  std::nullopt, 0.9));
    }
}

TEST(SegmentPoolContractTest, ClientUnmountBatchDoesNotAffectSiblingOwner) {
    SegmentPool pool(Drivers());
    const auto first = Region(0, "shared");
    const auto sibling = Region(1, "shared");
    auto access = pool.AcquireWriteAccess();
    ASSERT_EQ(access.MountSegment(first, UUID{2, 0}), ErrorCode::OK);
    ASSERT_EQ(access.MountSegment(sibling, UUID{2, 1}), ErrorCode::OK);
    auto batch = access.BeginClientUnmount(UUID{2, 0});
    ASSERT_EQ(batch.prepared.size(), 1U);
    EXPECT_TRUE(batch.pending.empty());
    EXPECT_EQ(batch.prepared.front().segment.id, first.id);
    EXPECT_TRUE(access.HasServingCandidate(sibling.name));
    EXPECT_EQ(access.FindSegment(sibling.id)->status, SegmentStatus::OK);
}

TEST(SegmentPoolContractTest, ConfiguredMemoryAndNoFShareAllocationProtocol) {
    for (auto policy :
         {PlacementPolicyType::RANDOM, PlacementPolicyType::FREE_RATIO_FIRST,
          PlacementPolicyType::LOCAL_FIRST}) {
        SegmentPool memory(Drivers(), policy);
        NoFSegmentManager nof(BufferAllocatorType::OFFSET, policy);
        const auto region = Region(0, "allocation");
        const UUID owner{2, 0};
        ASSERT_EQ(memory.AcquireWriteAccess().MountSegment(region, owner),
                  ErrorCode::OK);
        NoFSegment device;
        device.id = region.id;
        device.name = region.name;
        device.base = region.base;
        device.size = region.size;
        device.te_endpoint = region.te_endpoint;
        ASSERT_EQ(nof.AcquireWriteAccess().MountSegment(device, owner),
                  ErrorCode::OK);
        ReplicaAllocationRequest request;
        request.replicas.size = 4096;
        request.placement.preferred_segment_name = region.name;
        auto memory_result = memory.AllocateReplicas(request);
        ASSERT_TRUE(memory_result);
        EXPECT_TRUE(memory_result->front().is_memory_replica());
        request.replicas.type = ReplicaType::NOF_SSD;
        auto nof_result = nof.AllocateReplicas(request);
        ASSERT_TRUE(nof_result);
        EXPECT_TRUE(nof_result->front().is_nof_replica());
    }
    // SSD-owner ranking is meaningful only for memory placement. NoF keeps
    // its random fallback and does not require a LocalSSD metrics provider.
    EXPECT_NO_THROW(
        NoFSegmentManager(BufferAllocatorType::OFFSET,
                          PlacementPolicyType::SSD_FREE_RATIO_FIRST));
    EXPECT_THROW(
        SegmentPool(Drivers(), PlacementPolicyType::SSD_FREE_RATIO_FIRST),
        std::invalid_argument);
}

}  // namespace
}  // namespace mooncake::test
