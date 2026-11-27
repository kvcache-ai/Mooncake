#include <gtest/gtest.h>

#include <array>
#include <barrier>
#include <numeric>
#include <cstdlib>
#include <new>
#include <thread>

#include "segment.h"
#include "master_service/master_service_test_peer.h"

#include "allocation_probe.h"

namespace mooncake {
namespace {
using test::AllocationProbe;
constexpr size_t kCapacity = 32UL << 20;
constexpr size_t kRequest = 257;

TEST(AllocatorOriginTest, CachelibRollsBackRawBlockAndUsage) {
    auto create = [] {
        return CachelibBufferAllocator::Create("test-cachelib",
                                               DEFAULT_CXL_BASE, kCapacity,
                                               "endpoint", "rdma")
            .value();
    };
    auto reference = create();
    auto first = reference->allocate(kRequest);
    ASSERT_TRUE(first);
    auto dummy = std::make_shared<DummyBufferAllocator>("dummy", "endpoint");
    size_t origin_bytes;
    {
        // Short strings keep the origin and its control block in one
        // allocation.
        AllocationProbe probe;
        BufferAllocatorBase::GetBufferOrigin(dummy);
        origin_bytes = probe.bytes;
    }
    ASSERT_GT(origin_bytes, sizeof(AllocatedBuffer));
    for (const size_t target : {sizeof(AllocatedBuffer), origin_bytes}) {
        auto allocator = create();
        auto tracker = std::make_shared<StorageUsageTracker>();
        allocator->AttachUsageTracker(tracker);
        auto allocate = [&] {
            AllocationProbe fail(0, target);
            return allocator->allocate(kRequest);
        };
        EXPECT_THROW(allocate(), std::bad_alloc);
        EXPECT_EQ(allocator->size(), 0u);
        EXPECT_EQ(tracker->GetUsage().used_bytes, 0u);
        auto retry = allocator->allocate(kRequest);
        ASSERT_TRUE(retry);
        EXPECT_EQ(retry->data(), first->data());
        EXPECT_EQ(tracker->GetUsage().used_bytes, kRequest);
        retry.reset();
        EXPECT_EQ(allocator->size(), 0u);
        EXPECT_EQ(tracker->GetUsage().used_bytes, 0u);
    }
}

void CheckMasterRemount(bool with_standby_replica) {
    const UUID client{9, 8};
    std::vector<Segment> segments(2);
    for (size_t i = 0; i < segments.size(); ++i) {
        auto& segment = segments[i];
        segment.id = {7, static_cast<int64_t>(i + 1)};
        segment.name = segment.te_endpoint =
            std::string("remount") + std::to_string(i);
        segment.host_id = "host";
        segment.size = kCapacity;
        segment.base = DEFAULT_CXL_BASE + i * kCapacity;
        segment.protocol = "rdma";
    }
    auto dummy = std::make_shared<DummyBufferAllocator>("dummy", "endpoint");
    size_t origin_bytes;
    {
        AllocationProbe probe;
        BufferAllocatorBase::GetBufferOrigin(dummy);
        origin_bytes = probe.bytes;
    }
    // Other allocations may share the origin size on a given STL/ABI.
    // Sweep all matching allocations until the whole batch succeeds.
    for (size_t nth = 0;; ++nth) {
        SCOPED_TRACE(nth);
        MasterService service(
            MasterServiceConfig::builder()
                .set_enable_ha(false)
                .set_memory_allocator(BufferAllocatorType::OFFSET)
                .build());
        test::MasterServiceTestPeer peer(service);
        auto& manager = peer.SegmentManager(service);
        const auto& first = segments.front();
        const std::string key = "restored-key";
        if (with_standby_replica) {
            StandbySegmentInfo info;
            info.segment_name = first.name;
            info.transport_endpoint = first.te_endpoint;
            info.capacity = first.size;
            info.is_memory_segment = true;
            StandbyObjectMetadata meta;
            meta.client_id = client;
            meta.size = kRequest;
            Replica::Descriptor replica;
            replica.id = 1;
            replica.status = ReplicaStatus::COMPLETE;
            MemoryDescriptor mem;
            mem.buffer_descriptor = {kRequest, first.base, first.protocol,
                                     first.te_endpoint};
            replica.descriptor_variant = std::move(mem);
            meta.replicas.push_back(std::move(replica));
            ASSERT_TRUE(service.RestoreFromStandbySnapshot(
                {{"default", key, std::move(meta)}}, 7, {info}));
        }
        const auto initial = manager.GetMemoryUsage();
        const auto capacity =
            MasterMetricManager::instance().get_total_mem_capacity();
        const auto used =
            MasterMetricManager::instance().get_allocated_mem_size();
        auto attempt = [&] {
            AllocationProbe fail(nth, origin_bytes);
            return service.ReMountSegment(segments, client);
        };
        try {
            auto result = attempt();
            if (result.has_value()) {
                EXPECT_GE(nth, 4u);
                break;
            }
            // Offset import already reports an unavailable imported handle as
            // INVALID_PARAMS. Both errors must leave the batch retryable.
            EXPECT_TRUE(result.error() == ErrorCode::INTERNAL_ERROR ||
                        (with_standby_replica &&
                         result.error() == ErrorCode::INVALID_PARAMS));
        } catch (const std::bad_alloc&) {
            ADD_FAILURE() << "remount propagated an allocation failure";
        }
        EXPECT_FALSE(peer.FindClientRecord(client));
        EXPECT_EQ(manager.GetMemoryUsage().used_bytes, initial.used_bytes);
        EXPECT_EQ(MasterMetricManager::instance().get_allocated_mem_size(),
                  used);
        EXPECT_EQ(manager.GetMemoryUsage().capacity_bytes,
                  initial.capacity_bytes);
        EXPECT_EQ(MasterMetricManager::instance().get_total_mem_capacity(),
                  capacity);
        for (const auto& segment : segments) {
            EXPECT_FALSE(manager.getSegmentAccess().GetAllocator(segment.id));
            auto snapshot =
                manager.getAllocatorAccess().SnapshotAllocatorManager();
            EXPECT_EQ(snapshot.getAllocators(segment.name), nullptr);
        }
        if (with_standby_replica) {
            auto replica = service.GetReplicaList(key, TenantId::Default());
            ASSERT_FALSE(replica);
            EXPECT_EQ(replica.error(), ErrorCode::REPLICA_IS_NOT_READY);
        }
        ASSERT_TRUE(service.ReMountSegment(segments, client).has_value());
        if (with_standby_replica) {
            EXPECT_TRUE(service.GetReplicaList(key, TenantId::Default()));
        }
        auto owner = peer.FindClientRecord(client);
        ASSERT_TRUE(owner);
        EXPECT_EQ(manager.GetMemoryUsage().capacity_bytes,
                  initial.capacity_bytes + 2 * kCapacity);
        for (const auto& segment : segments) {
            auto snapshot =
                manager.getAllocatorAccess().SnapshotAllocatorManager();
            auto slots = snapshot.getAllocators(segment.name);
            ASSERT_NE(slots, nullptr);
            ASSERT_EQ(slots->size(), 1u);
            auto buffer = slots->front()->Allocate(kRequest);
            ASSERT_TRUE(buffer);
            EXPECT_EQ(buffer->getClientSession(), owner);
        }
    }
}

TEST(AllocatorOriginTest, FailedMountIndexInsertionsPreserveExistingTopology) {
    SegmentManager manager(BufferAllocatorType::OFFSET);
    auto session = std::make_shared<ClientLivenessRecord>(
        UUID{11, 12}, ClientLivenessRecord::Clock::now());
    Segment original;
    original.id = {13, 14};
    original.name = original.te_endpoint = "shared-name";
    original.host_id = "original-host";
    original.protocol = "rdma";
    original.base = DEFAULT_CXL_BASE;
    original.size = kCapacity;
    ASSERT_EQ(manager.getSegmentAccess().MountSegment(original, session),
              ErrorCode::OK);
    auto first = manager.getSegmentAccess().GetAllocator(original.id);
    Segment added = original;
    added.id = {13, 15};
    added.host_id = "new-host";
    added.base += kCapacity;
    auto enriched = original;
    enriched.locality = SegmentLocality{1, {"mlx5_0"}};
    const std::vector<Segment> batch{enriched, added};
    const std::vector<ScopedSegmentAccess::AllocatorReplacement> swaps;
    const auto capacity =
        MasterMetricManager::instance().get_total_mem_capacity();
    for (size_t nth = 0;; ++nth) {
        SCOPED_TRACE(nth);
        ErrorCode result;
        {
            auto access = manager.getSegmentAccess();
            AllocationProbe fail(nth);
            result = access.ReMountSegment(batch, session, swaps);
        }
        if (result == ErrorCode::OK) break;
        EXPECT_FALSE(first->GetLocalityHandle());
        EXPECT_FALSE(manager.getSegmentAccess().GetAllocator(added.id));
        EXPECT_EQ(manager.GetMemoryUsage().capacity_bytes, kCapacity);
        EXPECT_EQ(MasterMetricManager::instance().get_total_mem_capacity(),
                  capacity);
        auto snapshot = manager.getAllocatorAccess().SnapshotAllocatorManager();
        auto slots = snapshot.getAllocators(original.name);
        ASSERT_NE(slots, nullptr);
        ASSERT_EQ(slots->size(), 1u);
        EXPECT_EQ(slots->front()->GetAllocator(), first);
        EXPECT_TRUE(slots->front()->Allocate(kRequest));
        auto metadata = snapshot.GetSegmentMetadata(original.name);
        ASSERT_NE(metadata, nullptr);
        EXPECT_EQ(metadata->id, original.id);
        EXPECT_FALSE(metadata->locality);
    }
    EXPECT_EQ(first->GetLocality(), enriched.locality);
    EXPECT_TRUE(manager.getSegmentAccess().GetAllocator(added.id));
    EXPECT_EQ(manager.GetMemoryUsage().capacity_bytes, 2 * kCapacity);
    manager.releaseCapacityMetrics();
}

TEST(AllocatorOriginTest, SecondMountOriginFailureRollsBackMasterRemount) {
    CheckMasterRemount(false);
}

TEST(AllocatorOriginTest, ImportedReplicaSurvivesFailedMasterRemountAndRetry) {
    CheckMasterRemount(true);
}

TEST(AllocatorOriginTest, BatchRebindSurvivesEveryPreparationFailure) {
    SegmentManager manager(BufferAllocatorType::OFFSET);
    const UUID client{1, 2};
    auto session = [&] {
        return std::make_shared<ClientLivenessRecord>(
            client, ClientLivenessRecord::Clock::now());
    };
    auto old_session = session();
    auto next_session = session();
    std::array<Segment, 3> segments;
    std::array<std::shared_ptr<SegmentAllocatorRegistration>, 3> old;
    std::array<std::unique_ptr<AllocatedBuffer>, 3> buffers;
    auto registration = [&](const Segment& segment) {
        auto access = manager.getAllocatorAccess();
        return access.SnapshotAllocatorManager()
            .getAllocators(segment.name)
            ->front();
    };
    for (size_t i = 0; i < segments.size(); ++i) {
        auto& segment = segments[i];
        segment.id = {3, static_cast<int64_t>(i + 1)};
        segment.name = segment.te_endpoint = std::to_string(i);
        segment.protocol = "rdma";
        segment.base = DEFAULT_CXL_BASE + i * kCapacity;
        segment.size = kCapacity;
        segment.locality = SegmentLocality{0, {"mlx5_0"}};
        ASSERT_EQ(manager.getSegmentAccess().MountSegment(segment, old_session),
                  ErrorCode::OK);
        old[i] = registration(segment);
        buffers[i] = old[i]->Allocate(kRequest);
        ASSERT_TRUE(buffers[i]);
        buffers[i]->change_to_cxl("binding");
    }
    const auto& first = segments.front();
    auto replacement = std::make_shared<OffsetBufferAllocator>(
        first.name, first.base, first.size, first.te_endpoint, first.protocol);
    BufferAllocatorBase::GetBufferOrigin(replacement);
    std::vector<ScopedSegmentAccess::AllocatorReplacement> swaps{
        {first.id, old.front()->GetAllocator(), replacement}};
    size_t failures = 0;
    for (;; ++failures) {
        bool committed;
        try {
            auto access = manager.getSegmentAccess();
            AllocationProbe fail(failures);
            committed = access.ReplaceAllocators(swaps, next_session);
        } catch (const std::bad_alloc&) {
            ASSERT_FALSE(replacement->GetLocalityHandle());
            for (size_t i = 0; i < segments.size(); ++i) {
                ASSERT_EQ(registration(segments[i]), old[i]);
                ASSERT_TRUE(buffers[i]->isAvailable());
                auto fresh = old[i]->Allocate(kRequest);
                ASSERT_TRUE(fresh);
                ASSERT_EQ(fresh->getClientSession(), old_session);
            }
            continue;
        }
        ASSERT_TRUE(committed);
        break;
    }
    EXPECT_GT(failures, 0u);
    EXPECT_EQ(replacement->GetLocality(), first.locality);
    for (size_t i = 0; i < segments.size(); ++i) {
        EXPECT_FALSE(old[i]->Allocate(kRequest));
        EXPECT_FALSE(buffers[i]->isAllocatorValid());
        EXPECT_EQ(buffers[i]->getClientSession(), old_session);
        EXPECT_EQ(buffers[i]->getTransportEndpoint(), "binding");
        auto fresh = registration(segments[i])->Allocate(kRequest);
        ASSERT_TRUE(fresh);
        fresh->change_to_cxl("binding");
        EXPECT_TRUE(fresh->isAvailable());
        EXPECT_EQ(fresh->getClientSession(), next_session);
        EXPECT_EQ(fresh->getTransportEndpoint(), "binding");
    }
}

TEST(AllocatorOriginTest, FailedCxlPublicationReleasesRawBlockAndCanRetry) {
    auto created = CachelibBufferAllocator::Create(
        "cxl", DEFAULT_CXL_BASE, kCapacity, "endpoint", "cxl");
    ASSERT_TRUE(created);
    auto allocator = *created;
    auto buffer = allocator->allocate(kRequest);
    ASSERT_TRUE(buffer);
    void* raw = buffer->data();
    auto convert = [&] {
        AllocationProbe fail(0);
        buffer->change_to_cxl("binding");
    };
    EXPECT_THROW(convert(), std::bad_alloc);
    EXPECT_EQ(buffer->data(), raw);
    buffer.reset();
    ASSERT_EQ(allocator->size(), 0u);
    buffer = allocator->allocate(kRequest);
    ASSERT_TRUE(buffer);
    ASSERT_EQ(buffer->data(), raw);
    buffer->change_to_cxl("binding");
    EXPECT_EQ(buffer->get_descriptor().buffer_address_,
              reinterpret_cast<uintptr_t>(raw) - DEFAULT_CXL_BASE);
    buffer.reset();
    EXPECT_EQ(allocator->size(), 0u);
}

TEST(AllocatorOriginTest, TransportVariantsAreSharedAcrossThreads) {
    auto dummy = std::make_shared<DummyBufferAllocator>("dummy", "endpoint");
    const AllocatedBuffer::Descriptor rdma{kRequest, DEFAULT_CXL_BASE + 4096,
                                           "rdma", "endpoint"};
    const AllocatedBuffer::Descriptor cxl{kRequest, 4096, "cxl", "binding-A"};
    const AllocatedBuffer::Descriptor other{kRequest, 8192, "cxl", "binding-B"};
    BufferAllocatorBase::GetBufferOrigin(dummy);
    std::array<std::thread, 4> workers;
    std::array<size_t, 4> cold_allocations{};
    std::barrier start(workers.size());
    for (size_t i = 0; i < workers.size(); ++i) {
        workers[i] = std::thread([&, i] {
            start.arrive_and_wait();
            AllocationProbe probe;
            AllocatedBuffer restored(dummy, rdma);
            cold_allocations[i] = probe.calls;
        });
    }
    for (auto& worker : workers) worker.join();
    EXPECT_EQ(std::accumulate(cold_allocations.begin(), cold_allocations.end(),
                              size_t{0}),
              1u);
    AllocatedBuffer saved_rdma(dummy, rdma), saved_cxl(dummy, cxl),
        saved_other(dummy, other);
    size_t allocations;
    std::thread reader([&] {
        AllocationProbe probe;
        for (size_t i = 0; i < 100; ++i) {
            AllocatedBuffer restored(dummy, rdma);
            AllocatedBuffer converted(
                dummy, reinterpret_cast<void*>(DEFAULT_CXL_BASE + 4096),
                kRequest);
            converted.change_to_cxl("binding-A");
            AllocatedBuffer other_binding(dummy, other);
        }
        allocations = probe.calls;
    });
    reader.join();
    EXPECT_EQ(allocations, 0u);
    EXPECT_EQ(saved_rdma.get_descriptor().protocol_, "rdma");
    EXPECT_EQ(saved_rdma.getTransportEndpoint(), "endpoint");
    EXPECT_EQ(saved_cxl.getTransportEndpoint(), "binding-A");
    EXPECT_EQ(saved_other.getTransportEndpoint(), "binding-B");
    EXPECT_EQ(saved_cxl.data(), reinterpret_cast<void*>(4096));
    EXPECT_EQ(saved_other.data(), reinterpret_cast<void*>(8192));
}
}  // namespace
}  // namespace mooncake
