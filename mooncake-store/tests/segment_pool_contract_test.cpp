#include "segment_pool_test_peer.h"
#include "client_registry.h"

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

TEST(SegmentPoolContractTest,
     QueriesAreOwnedAndSelectDrainTargetsWithoutCatalog) {
    ClientRegistry clients{false};
    SegmentPool pool(Drivers());
    const auto first = Region(0, "source");
    const auto second = Region(1, "target");
    const UUID owner{2, 0};
    ASSERT_EQ(pool.AcquireWriteAccess().MountSegment(
                  first, clients.GetOrCreate(owner)),
              ErrorCode::OK);
    ASSERT_EQ(pool.AcquireWriteAccess().MountSegment(
                  second, clients.GetOrCreate(owner)),
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
    ClientRegistry clients{false};
    SegmentPool pool(Drivers());
    const auto region = Region(0, "unmount");
    const UUID owner{2, 0};
    ASSERT_EQ(pool.AcquireWriteAccess().MountSegment(
                  region, clients.GetOrCreate(owner)),
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
    EXPECT_TRUE(SegmentPoolTestPeer::IsNameReserved(access, region.name));
    EXPECT_EQ(access.MountSegment(region, clients.GetOrCreate(owner)),
              ErrorCode::INVALID_PARAMS);
    EXPECT_EQ(access.AcknowledgeUnmount(operation->id), ErrorCode::OK);
    EXPECT_EQ(access.AcknowledgeUnmount(operation->id), ErrorCode::OK);
    ASSERT_EQ(access.MountSegment(region, clients.GetOrCreate(owner)),
              ErrorCode::OK);
    EXPECT_EQ(access.ReleaseUnmountedResources(operation->id),
              ErrorCode::SEGMENT_NOT_FOUND);
    EXPECT_TRUE(access.FindSegment(region.id));
}

TEST(SegmentPoolContractTest, ClearingPoolInvalidatesOutstandingOperations) {
    ClientRegistry clients{false};
    SegmentPool pool(Drivers());
    const auto region = Region(0, "epoch");
    const UUID owner{2, 0};
    auto access = pool.AcquireWriteAccess();
    ASSERT_EQ(access.MountSegment(region, clients.GetOrCreate(owner)),
              ErrorCode::OK);
    auto old = access.BeginUnmount(region.id, owner);
    ASSERT_TRUE(old);
    access.Clear();
    ASSERT_EQ(access.MountSegment(region, clients.GetOrCreate(owner)),
              ErrorCode::OK);
    auto current = access.BeginUnmount(region.id, owner);
    ASSERT_TRUE(current);
    EXPECT_NE(current->id, old->id);
    EXPECT_EQ(access.ReleaseUnmountedResources(old->id),
              ErrorCode::SEGMENT_NOT_FOUND);
    EXPECT_EQ(access.AcknowledgeUnmount(old->id), ErrorCode::OK);
    EXPECT_TRUE(SegmentPoolTestPeer::IsNameReserved(access, region.name));
}

TEST(SegmentPoolContractTest, SharedNameReservationsSurviveIndependentOwners) {
    ClientRegistry clients{false};
    SegmentPool pool(Drivers());
    const auto first = Region(0, "shared");
    const auto second = Region(1, "shared");
    auto access = pool.AcquireWriteAccess();
    ASSERT_EQ(access.MountSegment(first, clients.GetOrCreate(UUID{2, 0})),
              ErrorCode::OK);
    ASSERT_EQ(access.MountSegment(second, clients.GetOrCreate(UUID{2, 1})),
              ErrorCode::OK);
    auto a = access.BeginUnmount(first.id, UUID{2, 0});
    auto b = access.BeginUnmount(second.id, UUID{2, 1});
    ASSERT_TRUE(a);
    ASSERT_TRUE(b);
    ASSERT_EQ(access.ReleaseUnmountedResources(a->id), ErrorCode::OK);
    ASSERT_EQ(access.AcknowledgeUnmount(a->id), ErrorCode::OK);
    EXPECT_TRUE(SegmentPoolTestPeer::IsNameReserved(access, first.name));
    ASSERT_EQ(access.ReleaseUnmountedResources(b->id), ErrorCode::OK);
    ASSERT_EQ(access.AcknowledgeUnmount(b->id), ErrorCode::OK);
    EXPECT_FALSE(SegmentPoolTestPeer::IsNameReserved(access, first.name));
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

TEST(SegmentPoolContractTest, MountPublishesHostOnlyOnSuccess) {
    ClientRegistry clients{false};
    SegmentPool pool(Drivers());
    const UUID id{2, 0};
    clients.ResolveHostId(id, "writer");
    auto region = Region(0, "host-publication");
    {
        auto registration = clients.Register(id);
        ASSERT_TRUE(registration);
        const auto& session = registration->session();
        EXPECT_EQ(session->host_id(), "writer");
        auto invalid = region;
        invalid.size = 0;
        EXPECT_NE(pool.AcquireWriteAccess().MountSegment(invalid, session),
                  ErrorCode::OK);
        EXPECT_EQ(session->host_id(), "writer");
        ASSERT_EQ(pool.AcquireWriteAccess().MountSegment(region, session),
                  ErrorCode::OK);
        EXPECT_EQ(session->host_id(), region.host_id);
        registration->Commit();
    }
    EXPECT_EQ(clients.ResolveHostId(id), region.host_id);
    const auto session = clients.Find(id);
    clients.ResolveHostId(id, "updated-writer");
    EXPECT_EQ(pool.AcquireWriteAccess().MountSegment(region, session),
              ErrorCode::SEGMENT_ALREADY_EXISTS);
    EXPECT_EQ(session->host_id(), "updated-writer");
    auto conflicting = region;
    conflicting.host_id = "conflicting";
    EXPECT_EQ(pool.AcquireWriteAccess().MountSegment(conflicting, session),
              ErrorCode::INVALID_PARAMS);
    EXPECT_EQ(session->host_id(), "updated-writer");
    auto empty = Region(1, "empty-host");
    empty.host_id.clear();
    ASSERT_EQ(pool.AcquireWriteAccess().MountSegment(empty, session),
              ErrorCode::OK);
    EXPECT_EQ(session->host_id(), "updated-writer");
}

TEST(SegmentPoolContractTest, RemountPublishesHostOnBatchCommitNotPreparation) {
    ClientRegistry clients{false};
    SegmentPool pool(Drivers());
    const UUID id{2, 0};
    const auto session = clients.GetOrCreate(id);
    session->SetHostId("original");
    auto empty = Region(0, "empty");
    empty.host_id.clear();
    auto first = Region(1, "first");
    first.host_id = "first-host";
    auto last = Region(2, "last");
    last.host_id = "last-host";
    const std::array segments{empty, first, last};
    {
        auto request = pool.PlanRemount(segments, id);
        ASSERT_TRUE(request);
        auto prepared = pool.PrepareRemount(std::move(*request), session);
        ASSERT_TRUE(prepared);
        EXPECT_EQ(session->host_id(), "original");
    }
    EXPECT_TRUE(pool.AcquireReadAccess().Segments().empty());
    EXPECT_EQ(clients.ResolveHostId(id), "original");
    // Exercise both new mounts and remounts that reuse existing regions.
    for (const bool regions_already_mounted : {false, true}) {
        SCOPED_TRACE(regions_already_mounted ? "existing regions"
                                             : "new regions");
        session->SetHostId("before-commit");
        auto request = pool.PlanRemount(segments, id);
        ASSERT_TRUE(request);
        {
            auto prepared = pool.PrepareRemount(std::move(*request), session);
            ASSERT_TRUE(prepared);
            EXPECT_EQ(session->host_id(), "before-commit");
            EXPECT_TRUE(prepared->Commit().empty());
            EXPECT_EQ(session->host_id(), "first-host");
        }
        EXPECT_EQ(clients.ResolveHostId(id), "first-host");
    }
    session->SetHostId("keep");
    auto request = pool.PlanRemount(std::array{empty}, id);
    ASSERT_TRUE(request);
    {
        auto prepared = pool.PrepareRemount(std::move(*request), session);
        ASSERT_TRUE(prepared);
        prepared->Commit();
    }
    EXPECT_EQ(session->host_id(), "keep");
}

TEST(SegmentPoolContractTest, FailedRemountBatchPublishesNothing) {
    ClientRegistry clients{false};
    SegmentPool pool(Drivers());
    const auto first = Region(0, "first");
    auto bad = Region(1, "bad");
    bad.size = 0;
    const std::array segments{first, bad};
    auto request = pool.PlanRemount(segments, UUID{2, 0});
    ASSERT_TRUE(request);
    const auto session = clients.GetOrCreate(UUID{2, 0});
    session->SetHostId("unchanged");
    EXPECT_FALSE(pool.PrepareRemount(std::move(*request), session));
    EXPECT_EQ(session->host_id(), "unchanged");
    EXPECT_TRUE(pool.AcquireReadAccess().Segments().empty());
    EXPECT_EQ(pool.GetMemoryUsage().capacity_bytes, 0U);
}

TEST(SegmentPoolContractTest,
     RemountReturnsBoundBuffersAndSettlesPlaceholderMetricsOnce) {
    ClientRegistry clients{false};
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
            auto prepared = pool.PrepareRemount(
                std::move(*request), clients.GetOrCreate(UUID{2, 0}));
            ASSERT_TRUE(prepared);
            buffers = prepared->Commit();
        }
        ASSERT_EQ(buffers.size(), 1U);
        EXPECT_EQ(buffers.front().binding_id, 42U);
        EXPECT_TRUE(buffers.front().buffer->isAllocatorValid());
        EXPECT_EQ(metrics.get_allocated_mem_size(), before + 4096);
        auto allocator = SegmentPoolTestPeer::GetAllocator(
            pool.AcquireReadAccess(), region.id);
        auto repeated = pool.PlanRemount(segments, UUID{2, 0});
        ASSERT_TRUE(repeated);
        EXPECT_FALSE(repeated->NeedsBuffers());
        {
            auto prepared = pool.PrepareRemount(
                std::move(*repeated), clients.GetOrCreate(UUID{2, 0}));
            ASSERT_TRUE(prepared);
            EXPECT_TRUE(prepared->Commit().empty());
        }
        EXPECT_EQ(SegmentPoolTestPeer::GetAllocator(pool.AcquireReadAccess(),
                                                    region.id),
                  allocator);
        pool.AcquireWriteAccess().Clear();
        EXPECT_FALSE(buffers.front().buffer->isAllocatorValid());
    }
    EXPECT_EQ(metrics.get_allocated_mem_size(), before);
}

TEST(SegmentPoolContractTest, PendingExternalAckCannotBeLostInSnapshot) {
    ClientRegistry clients{false};
    SegmentPool pool(Drivers());
    const auto region = Region(0, "snapshot-operation");
    const UUID owner{2, 0};
    auto access = pool.AcquireWriteAccess();
    ASSERT_EQ(access.MountSegment(region, clients.GetOrCreate(owner)),
              ErrorCode::OK);
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
    ClientRegistry clients{false};
    for (auto policy :
         {PlacementPolicyType::RANDOM, PlacementPolicyType::FREE_RATIO_FIRST,
          PlacementPolicyType::LOCAL_FIRST}) {
        SegmentPool pool(Drivers(), policy);
        auto local = Region(0, "local");
        local.host_id = "writer";
        auto remote = Region(1, "remote");
        remote.host_id = "other";
        ASSERT_EQ(pool.AcquireWriteAccess().MountSegment(
                      local, clients.GetOrCreate(UUID{2, 0})),
                  ErrorCode::OK);
        ASSERT_EQ(pool.AcquireWriteAccess().MountSegment(
                      remote, clients.GetOrCreate(UUID{2, 1})),
                  ErrorCode::OK);
        ReplicaAllocationRequest request;
        request.replicas.size = 4096;
        request.host_affinity = {"writer", "key", true};
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
    ClientRegistry clients{false};
    SegmentPool pool(Drivers());
    const auto first = Region(0, "shared");
    const auto sibling = Region(1, "shared");
    auto access = pool.AcquireWriteAccess();
    ASSERT_EQ(access.MountSegment(first, clients.GetOrCreate(UUID{2, 0})),
              ErrorCode::OK);
    ASSERT_EQ(access.MountSegment(sibling, clients.GetOrCreate(UUID{2, 1})),
              ErrorCode::OK);
    auto batch = access.BeginClientUnmount(clients.GetOrCreate(UUID{2, 0}));
    ASSERT_EQ(batch.prepared.size(), 1U);
    EXPECT_TRUE(batch.pending.empty());
    EXPECT_EQ(batch.prepared.front().segment.id, first.id);
    EXPECT_TRUE(access.HasServingCandidate(sibling.name));
    EXPECT_EQ(access.FindSegment(sibling.id)->status, SegmentStatus::OK);
}

TEST(SegmentPoolContractTest, ConfiguredMemoryAndNoFShareAllocationProtocol) {
    ClientRegistry clients{false};
    for (auto policy :
         {PlacementPolicyType::RANDOM, PlacementPolicyType::FREE_RATIO_FIRST,
          PlacementPolicyType::LOCAL_FIRST}) {
        SegmentPool memory(Drivers(), policy);
        NoFSegmentManager nof(BufferAllocatorType::OFFSET, policy);
        const auto region = Region(0, "allocation");
        const UUID owner{2, 0};
        ASSERT_EQ(memory.AcquireWriteAccess().MountSegment(
                      region, clients.GetOrCreate(owner)),
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

// These compile-time checks protect the production boundary, independently of
// the white-box peer used to construct allocator fixtures below.
template <typename T>
concept ExposesResourceInternals =
    requires(T& access) { access.Catalog(); } || requires(T& access) {
        access.Placement();
    } || requires(T& access) { access.GetAllocator(UUID{}); };
template <typename T>
concept ExposesResourceTransactions =
    requires(T& access, const Segment& segment) {
        access.PrepareMount(segment, UUID{});
    } || requires(T& access) { access.PrepareUnmount(UUID{}, UUID{}); } ||
    requires(T& access) {
        access.SetSegmentStatusByName("segment", SegmentStatus::OK);
    };
static_assert(!ExposesResourceInternals<SegmentPool::ReadAccess>);
static_assert(!ExposesResourceInternals<SegmentPool::WriteAccess>);
static_assert(!ExposesResourceTransactions<SegmentPool::WriteAccess>);

TEST(SegmentPoolContractTest,
     BufferSnapshotValidationDoesNotFreeRejectedAllocation) {
    ClientRegistry clients{false};
    SegmentPool source(Drivers());
    const auto region = Region(0, "snapshot-buffer");
    ASSERT_EQ(source.AcquireWriteAccess().MountSegment(
                  region, clients.GetOrCreate(UUID{2, 0})),
              ErrorCode::OK);
    auto allocator = SegmentPoolTestPeer::GetAllocator(
        source.AcquireReadAccess(), region.id);
    auto buffer = allocator->allocate(4096);
    ASSERT_TRUE(buffer);
    auto record = source.CaptureBufferSnapshot(*buffer);
    ASSERT_TRUE(record);
    auto locked_record =
        source.AcquireReadAccess().CaptureBufferSnapshot(*buffer);
    ASSERT_TRUE(locked_record);
    EXPECT_EQ(locked_record->region_id, record->region_id);
    ASSERT_TRUE(record->allocation);
    EXPECT_EQ(locked_record->allocation->metadata,
              record->allocation->metadata);
    auto snapshot = source.CaptureSnapshot();
    ASSERT_TRUE(snapshot);
    SegmentPool restored(Drivers());
    ASSERT_TRUE(restored.RestoreSnapshot(std::move(*snapshot), false));
    const auto used = restored.GetMemoryUsage().used_bytes;
    for (int corruption = 0; corruption < 7; ++corruption) {
        SCOPED_TRACE(corruption);
        auto invalid = *record;
        switch (corruption) {
            case 0:
                invalid.region_id = UUID{9, 9};
                break;
            case 1:
                invalid.address = region.base + region.size;
                break;
            case 2:
                invalid.size = region.size + 1;
                break;
            case 3:
                invalid.allocation.reset();
                break;
            case 4:
                ++invalid.allocation->size;
                break;
            case 5:
                ++invalid.allocation->offset;
                break;
            case 6:
                invalid.allocation->metadata = UINT32_MAX;
                break;
        }
        auto rejected = restored.AcquireReadAccess().RestoreBuffer(invalid);
        ASSERT_FALSE(rejected);
        EXPECT_EQ(rejected.error(), ErrorCode::DESERIALIZE_FAIL);
        EXPECT_EQ(restored.GetMemoryUsage().used_bytes, used);
    }
    auto rebound = restored.AcquireReadAccess().RestoreBuffer(*record);
    ASSERT_TRUE(rebound);
    EXPECT_EQ((*rebound)->size(), 4096U);
    EXPECT_FALSE((*rebound)->isAvailable());
    ASSERT_TRUE(restored.RestoreBufferBindings(clients.Snapshot(), {}));
    EXPECT_TRUE((*rebound)->isAvailable());
    rebound->reset();
    EXPECT_EQ(restored.GetMemoryUsage().used_bytes, 0U);
}

TEST(SegmentPoolContractTest,
     AllocationReportsPressureWithoutExposingPlacementEntries) {
    ClientRegistry clients{false};
    SegmentPool pool(Drivers());
    ReplicaAllocationRequest request;
    request.replicas.size = 4096;
    AllocationDiagnostics diagnostics{true};
    EXPECT_FALSE(pool.AllocateReplicas(request, &diagnostics));
    EXPECT_FALSE(diagnostics.reclamation_may_help);
    const auto region = Region(0, "pressure");
    ASSERT_EQ(pool.AcquireWriteAccess().MountSegment(
                  region, clients.GetOrCreate(UUID{2, 0})),
              ErrorCode::OK);
    request.replicas.size = region.size;
    auto full = pool.AllocateReplicas(request, &diagnostics);
    ASSERT_TRUE(full);
    EXPECT_FALSE(diagnostics.reclamation_may_help);
    request.replicas.size = 4096;
    EXPECT_FALSE(pool.AllocateReplicas(request, &diagnostics));
    EXPECT_TRUE(diagnostics.reclamation_may_help);
    full->clear();
    auto success = pool.AllocateReplicas(request, &diagnostics);
    ASSERT_TRUE(success);
    EXPECT_FALSE(diagnostics.reclamation_may_help);
    request.replicas.count = 2;
    auto partial = pool.AllocateReplicas(request, &diagnostics);
    ASSERT_TRUE(partial);
    EXPECT_EQ(partial->size(), 1U);
    // Eviction cannot create a second independent target.
    EXPECT_FALSE(diagnostics.reclamation_may_help);
    request.replicas.size = 0;
    EXPECT_FALSE(pool.AllocateReplicas(request, &diagnostics));
    EXPECT_FALSE(diagnostics.reclamation_may_help);
}

TEST(SegmentPoolContractTest, MountRequiresOwnerAndPublishesBoundResources) {
    ClientRegistry clients(false);
    SegmentPool pool(Drivers());
    const auto region = Region(0, "session-bound");
    const auto now = ClientSession::TimePoint{};
    const auto session = clients.GetOrCreate(UUID{2, 0}, now);
    EXPECT_EQ(pool.AcquireWriteAccess().MountSegment(region, nullptr),
              ErrorCode::INVALID_PARAMS);
    ASSERT_EQ(pool.AcquireWriteAccess().MountSegment(region, session),
              ErrorCode::OK);
    EXPECT_EQ(pool.AcquireReadAccess().FindClientSession(region.id), session);
    EXPECT_EQ(pool.AcquireWriteAccess().MountSegment(region, session),
              ErrorCode::SEGMENT_ALREADY_EXISTS);
    auto replica = pool.AllocateInSegment(region.name, 4096);
    ASSERT_TRUE(replica);
    Replica::Descriptor descriptor;
    EXPECT_EQ(replica->getClientLiveness(), session);
    EXPECT_TRUE(replica->getDescriptorIfAvailable(descriptor));

    ASSERT_EQ(
        session->Evaluate(now + std::chrono::seconds(1),
                          std::chrono::seconds(1), std::chrono::seconds(1)),
        ClientLivenessTransition::BECAME_SUSPECTED);
    EXPECT_FALSE(pool.AllocateInSegment(region.name, 4096));
    EXPECT_FALSE(pool.AcquireReadAccess().HasServingCandidate(region.name));
    EXPECT_FALSE(replica->getDescriptorIfAvailable(descriptor));
    std::vector<std::string> serving;
    pool.AcquireReadAccess().GetActiveSegmentNames(serving);
    EXPECT_TRUE(serving.empty());
    EXPECT_GT(pool.GetMemoryUsage().capacity_bytes, 0U);
    ASSERT_EQ(clients.Observe(session, now + std::chrono::seconds(1)),
              ClientLivenessObservation::RECOVERED_ACTIVE);
    EXPECT_TRUE(pool.AllocateInSegment(region.name, 4096));
    EXPECT_TRUE(replica->getDescriptorIfAvailable(descriptor));
}

TEST(SegmentPoolContractTest, OldSessionCannotMountOrUnmountNewIncarnation) {
    ClientRegistry clients(false);
    SegmentPool pool(Drivers());
    const auto now = ClientSession::TimePoint{};
    const UUID id{2, 0};
    const auto old = clients.GetOrCreate(id, now);
    auto region = Region(0, "incarnation");
    ASSERT_EQ(pool.AcquireWriteAccess().MountSegment(region, old),
              ErrorCode::OK);
    auto replica = pool.AllocateInSegment(region.name, 4096);
    ASSERT_TRUE(replica);
    Replica::Descriptor descriptor;
    ASSERT_EQ(old->Evaluate(now + std::chrono::seconds(1),
                            std::chrono::seconds(1), std::chrono::seconds(1)),
              ClientLivenessTransition::BECAME_SUSPECTED);
    ASSERT_EQ(old->Evaluate(now + std::chrono::seconds(2),
                            std::chrono::seconds(1), std::chrono::seconds(1)),
              ClientLivenessTransition::BECAME_OFFLINE);
    ASSERT_TRUE(clients.Remove(old));
    const auto current = clients.GetOrCreate(id);
    EXPECT_EQ(pool.AcquireWriteAccess().MountSegment(region, current),
              ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    const std::array segments{region};
    auto request = pool.PlanRemount(segments, id);
    ASSERT_TRUE(request);
    EXPECT_FALSE(pool.PrepareRemount(std::move(*request), current));
    {
        auto access = pool.AcquireWriteAccess();
        auto batch = access.BeginClientUnmount(old);
        ASSERT_EQ(batch.prepared.size(), 1U);
        ASSERT_EQ(access.ReleaseUnmountedResources(batch.prepared[0].id),
                  ErrorCode::OK);
        ASSERT_EQ(access.AcknowledgeUnmount(batch.prepared[0].id),
                  ErrorCode::OK);
        ASSERT_EQ(access.MountSegment(region, current), ErrorCode::OK);
        EXPECT_TRUE(access.BeginClientUnmount(old).prepared.empty());
        EXPECT_EQ(access.BeginUnmount(region.id, old).error(),
                  ErrorCode::SEGMENT_NOT_FOUND);
        EXPECT_EQ(access.MountSegment(Region(1, "late-old-mount"), old),
                  ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    }
    EXPECT_EQ(pool.AcquireReadAccess().FindClientSession(region.id), current);
    EXPECT_TRUE(pool.AllocateInSegment(region.name, 4096));
    EXPECT_FALSE(replica->getDescriptorIfAvailable(descriptor));
    EXPECT_EQ(replica->getClientLiveness(), old);
}

TEST(SegmentPoolContractTest,
     SnapshotRegionsRemainUnavailableUntilOwnersRestored) {
    ClientRegistry clients(false);
    const UUID id{2, 0};
    const auto session = clients.GetOrCreate(id);
    SegmentPool source(Drivers());
    const auto region = Region(0, "restore-owner");
    ASSERT_EQ(source.AcquireWriteAccess().MountSegment(region, session),
              ErrorCode::OK);
    auto allocator = SegmentPoolTestPeer::GetAllocator(
        source.AcquireReadAccess(), region.id);
    auto buffer = allocator->allocate(4096);
    ASSERT_TRUE(buffer);
    auto buffer_snapshot = source.CaptureBufferSnapshot(*buffer);
    ASSERT_TRUE(buffer_snapshot);
    auto snapshot = source.CaptureSnapshot();
    ASSERT_TRUE(snapshot);
    SegmentPool restored(Drivers());
    ASSERT_TRUE(restored.RestoreSnapshot(std::move(*snapshot), false));
    auto restored_buffer =
        restored.AcquireReadAccess().RestoreBuffer(*buffer_snapshot);
    ASSERT_TRUE(restored_buffer);
    EXPECT_TRUE((*restored_buffer)->isAllocatorValid());
    EXPECT_FALSE((*restored_buffer)->isAvailable());
    EXPECT_FALSE((*restored_buffer)->getClientLiveness());
    EXPECT_FALSE(restored.AcquireReadAccess().FindClientSession(region.id));
    EXPECT_FALSE(restored.AcquireReadAccess().HasServingCandidate(region.name));
    EXPECT_FALSE(restored.AllocateInSegment(region.name, 4096));
    EXPECT_FALSE(restored.RestoreBufferBindings({}, {}));
    EXPECT_EQ(restored.AcquireWriteAccess().MountSegment(region, session),
              ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    ASSERT_TRUE(restored.RestoreBufferBindings(clients.Snapshot(), {}));
    EXPECT_EQ(restored.AcquireReadAccess().FindClientSession(region.id),
              session);
    EXPECT_TRUE(restored.AllocateInSegment(region.name, 4096));
    // No buffer-by-buffer rebinding: owner attachment updates the shared region
    // lifetime, including buffers restored before the registry was rebuilt.
    EXPECT_EQ((*restored_buffer)->getClientLiveness(), session);
    EXPECT_TRUE((*restored_buffer)->isAvailable());
    ASSERT_EQ(session->Evaluate(ClientSession::Clock::now(),
                                std::chrono::seconds(0), std::chrono::hours(1)),
              ClientLivenessTransition::BECAME_SUSPECTED);
    EXPECT_FALSE((*restored_buffer)->isAvailable());
}

}  // namespace
}  // namespace mooncake::test
