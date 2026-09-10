#include "segment/pool.h"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <future>
#include <memory>
#include <map>
#include <set>
#include <string>
#include <thread>

#include "master_metric_manager.h"
#include "segment/pool_read_access.h"
#include "segment/pool_write_access.h"
#include "test_buffer_allocator.h"

namespace mooncake::test {
namespace {

auto ActiveRegionsByHost(const RegionCatalog& catalog) {
    std::map<std::string, std::map<std::string, std::set<UUID>>> result;
    for (const auto& r : catalog.Regions()) {
        if (r.status != SegmentStatus::OK || r.segment.host_id.empty())
            continue;
        result[r.segment.host_id][r.segment.name].insert(r.segment.id);
    }
    return result;
}

constexpr size_t kRegionSize = 16U * 1024 * 1024;

RegionDriverRegistry Drivers(bool enable_cxl = false) {
    RegionDriverConfig config;
    config.memory_allocator = BufferAllocatorType::OFFSET;
    if (enable_cxl) {
        config.cxl = CxlRegionDriverConfig{"cxl-test-path", kRegionSize};
    }
    auto drivers = CreateRegionDrivers(config);
    EXPECT_TRUE(drivers.has_value());
    return std::move(drivers.value());
}

Segment MakeSegment(size_t index, std::string name,
                    std::string protocol = "tcp", std::string host = {}) {
    Segment segment;
    segment.id = generate_uuid();
    segment.name = std::move(name);
    segment.base = 0x100000000ULL + index * 0x2000000ULL;
    segment.size = kRegionSize;
    segment.te_endpoint = segment.name + "-endpoint";
    segment.protocol = std::move(protocol);
    segment.host_id = std::move(host);
    return segment;
}

void CommitUnmount(SegmentPool& pool, const Segment& segment,
                   const UUID& client_id) {
    auto transaction = [&] {
        auto access = pool.AcquireWriteAccess();
        return access.PrepareUnmount(segment.id, client_id);
    }();
    ASSERT_TRUE(transaction.has_value());
    auto access = pool.AcquireWriteAccess();
    ASSERT_EQ(std::move(*transaction).Commit(access), ErrorCode::OK);
    EXPECT_EQ(std::move(*transaction).Commit(access),
              ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    EXPECT_EQ(std::move(*transaction).Rollback(access),
              ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
}

}  // namespace

TEST(SegmentPoolTest, ClassifiesOnlyCxlProtocolAsCxl) {
    SegmentPool pool(Drivers(true));
    const UUID client = generate_uuid();
    auto host = MakeSegment(0, "host", "rdma");
    auto cxl = MakeSegment(1, "cxl", "cxl");
    {
        auto access = pool.AcquireWriteAccess();
        ASSERT_EQ(access.MountSegment(host, client), ErrorCode::OK);
        ASSERT_EQ(access.MountSegment(cxl, client), ErrorCode::OK);
    }

    {
        auto view = pool.AcquireReadAccess();
        ASSERT_NE(view.Catalog().Find(host.id), nullptr);
        EXPECT_EQ(view.Catalog().Find(host.id)->kind, RegionKind::HOST_MEMORY);
        ASSERT_NE(view.Catalog().Find(cxl.id), nullptr);
        EXPECT_EQ(view.Catalog().Find(cxl.id)->kind, RegionKind::CXL);
        EXPECT_NE(
            view.Placement().Find(host.name, AllocationCandidateKind::NATIVE),
            nullptr);
        EXPECT_NE(view.Placement().Find(cxl.name, AllocationCandidateKind::CXL),
                  nullptr);
    }

    CommitUnmount(pool, host, client);
    CommitUnmount(pool, cxl, client);
}

TEST(SegmentPoolTest, PreparedMountPublishesOnlyOnCommit) {
    SegmentPool pool(Drivers());
    const UUID client = generate_uuid();
    auto segment = MakeSegment(0, "prepared");
    {
        auto access = pool.AcquireWriteAccess();
        auto transaction = access.PrepareMount(segment, client);
        ASSERT_TRUE(transaction.has_value());
    }
    EXPECT_EQ(pool.AcquireReadAccess().Catalog().Find(segment.id), nullptr);

    {
        auto access = pool.AcquireWriteAccess();
        auto transaction = access.PrepareMount(segment, client);
        ASSERT_TRUE(transaction.has_value());
        ASSERT_EQ(transaction->Commit(access), ErrorCode::OK);
    }
    EXPECT_NE(pool.AcquireReadAccess().Catalog().Find(segment.id), nullptr);
    CommitUnmount(pool, segment, client);
}

TEST(SegmentPoolTest, MountCommitRejectsPreparedIdAndOwnerConflicts) {
    SegmentPool pool(Drivers());
    const UUID client = generate_uuid();
    auto segment = MakeSegment(0, "prepared-conflict");
    auto other = MakeSegment(1, segment.name);
    {
        auto access = pool.AcquireWriteAccess();
        auto first = access.PrepareMount(segment, client);
        auto duplicate = access.PrepareMount(segment, client);
        auto conflicting_owner = access.PrepareMount(other, generate_uuid());
        ASSERT_TRUE(first.has_value());
        ASSERT_TRUE(duplicate.has_value());
        ASSERT_TRUE(conflicting_owner.has_value());
        ASSERT_EQ(first->Commit(access), ErrorCode::OK);
        EXPECT_EQ(duplicate->Commit(access), ErrorCode::SEGMENT_ALREADY_EXISTS);
        EXPECT_EQ(conflicting_owner->Commit(access), ErrorCode::INVALID_PARAMS);
        EXPECT_EQ(first->Commit(access),
                  ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    }
    {
        auto read = pool.AcquireReadAccess();
        EXPECT_EQ(read.Catalog().Regions().size(), 1U);
        EXPECT_EQ(read.GetAllocator(other.id), nullptr);
        auto* entry = read.Placement().Find(segment.name,
                                            AllocationCandidateKind::NATIVE);
        ASSERT_NE(entry, nullptr);
        ASSERT_EQ(entry->candidates.size(), 1U);
        EXPECT_EQ((*entry->candidates.begin())->allocator_handle(),
                  read.GetAllocator(segment.id));
    }
    EXPECT_EQ(pool.GetMemoryUsage().capacity_bytes, kRegionSize);
    EXPECT_TRUE(pool.AllocateInSegment(segment.name,
                                       AllocationCandidateKind::NATIVE, 4096)
                    .has_value());
    CommitUnmount(pool, segment, client);
}

TEST(SegmentPoolTest, RemountCommitRejectsInterveningStatusChanges) {
    for (auto initial : {SegmentStatus::OK, SegmentStatus::DRAINING}) {
        SegmentPool pool(Drivers());
        const UUID client = generate_uuid();
        auto segment = MakeSegment(0, "status-conflict");
        {
            auto access = pool.AcquireWriteAccess();
            ASSERT_EQ(access.MountSegment(segment, client), ErrorCode::OK);
            ASSERT_EQ(access.SetSegmentStatusByName(segment.name, initial),
                      ErrorCode::OK);
        }
        auto original = pool.AcquireReadAccess().GetAllocator(segment.id);
        const auto next = initial == SegmentStatus::OK ? SegmentStatus::DRAINING
                                                       : SegmentStatus::OK;
        {
            auto access = pool.AcquireWriteAccess();
            auto prepared = access.PrepareMount(segment, client);
            ASSERT_TRUE(prepared.has_value());
            ASSERT_EQ(access.SetSegmentStatusByName(segment.name, next),
                      ErrorCode::OK);
            EXPECT_EQ(prepared->Commit(access),
                      ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
        }
        {
            auto read = pool.AcquireReadAccess();
            EXPECT_EQ(read.GetAllocator(segment.id), original);
            EXPECT_EQ(read.Catalog().Find(segment.id)->status, next);
        }
        EXPECT_EQ(pool.AllocateInSegment(segment.name,
                                         AllocationCandidateKind::NATIVE, 4096)
                      .has_value(),
                  next == SegmentStatus::OK);
        CommitUnmount(pool, segment, client);
    }
}

TEST(SegmentPoolTest, RemountCommitRejectsReplacedOrRemovedResource) {
    for (bool clear : {false, true}) {
        SegmentPool pool(Drivers());
        const UUID client = generate_uuid();
        auto segment = MakeSegment(0, "resource-conflict");
        {
            auto access = pool.AcquireWriteAccess();
            ASSERT_EQ(access.MountSegment(segment, client), ErrorCode::OK);
            auto stale = access.PrepareMount(segment, client);
            ASSERT_TRUE(stale.has_value());
            if (clear) {
                access.Clear();
            } else {
                auto replacement = access.PrepareMount(segment, client);
                ASSERT_TRUE(replacement.has_value());
                ASSERT_EQ(replacement->Commit(access), ErrorCode::OK);
            }
            EXPECT_EQ(stale->Commit(access),
                      ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
        }
        EXPECT_EQ(pool.GetMemoryUsage().capacity_bytes,
                  clear ? 0U : kRegionSize);
        EXPECT_EQ(pool.AllocateInSegment(segment.name,
                                         AllocationCandidateKind::NATIVE, 4096)
                      .has_value(),
                  !clear);
        if (!clear) {
            CommitUnmount(pool, segment, client);
        }
    }
}

TEST(SegmentPoolTest, RestoreReturnsImportedBuffersInInputOrder) {
    SegmentPool pool(Drivers());
    const UUID client = generate_uuid();
    auto segment = MakeSegment(0, "restore");
    std::vector<AllocatedBuffer::Descriptor> descriptors{
        {4096, segment.base + 8192, "tcp", segment.te_endpoint},
        {4096, segment.base, "tcp", segment.te_endpoint}};
    {
        auto access = pool.AcquireWriteAccess();
        auto transaction = access.PrepareRestore(segment, client, descriptors);
        ASSERT_TRUE(transaction.has_value());
        ASSERT_EQ(transaction->imported_buffers().size(), 2U);
        EXPECT_EQ(transaction->imported_requested_bytes(), 8192U);
        EXPECT_EQ(reinterpret_cast<uintptr_t>(
                      transaction->imported_buffers()[0]->data()),
                  segment.base + 8192);
        EXPECT_EQ(reinterpret_cast<uintptr_t>(
                      transaction->imported_buffers()[1]->data()),
                  segment.base);
        ASSERT_EQ(transaction->Commit(access), ErrorCode::OK);
        auto buffers = transaction->TakeImportedBuffers();
        buffers.clear();
    }
    CommitUnmount(pool, segment, client);
}

TEST(SegmentPoolTest, RollbackRestoresOriginalLifecycleState) {
    SegmentPool pool(Drivers());
    const UUID client = generate_uuid();
    auto segment = MakeSegment(0, "draining", "tcp", "host");
    {
        auto access = pool.AcquireWriteAccess();
        ASSERT_EQ(access.MountSegment(segment, client), ErrorCode::OK);
        ASSERT_EQ(access.SetSegmentStatusByName(segment.name,
                                                SegmentStatus::DRAINING),
                  ErrorCode::OK);
        auto transaction = access.PrepareUnmount(segment.id, client);
        ASSERT_TRUE(transaction.has_value());
        ASSERT_EQ(std::move(*transaction).Rollback(access), ErrorCode::OK);
        ASSERT_NE(access.Catalog().Find(segment.id), nullptr);
        EXPECT_EQ(access.Catalog().Find(segment.id)->status,
                  SegmentStatus::DRAINING);
    }

    {
        auto view = pool.AcquireReadAccess();
        ASSERT_NE(view.Catalog().Find(segment.id), nullptr);
        EXPECT_EQ(view.Catalog().Find(segment.id)->status,
                  SegmentStatus::DRAINING);
        EXPECT_TRUE(ActiveRegionsByHost(view.Catalog()).empty());
        EXPECT_EQ(view.Placement().Find(segment.name,
                                        AllocationCandidateKind::NATIVE),
                  nullptr);
    }

    {
        auto access = pool.AcquireWriteAccess();
        ASSERT_EQ(
            access.SetSegmentStatusByName(segment.name, SegmentStatus::OK),
            ErrorCode::OK);
    }
    CommitUnmount(pool, segment, client);
}

TEST(SegmentPoolTest, ImmediateUnmountRollbackRestoresPlacement) {
    SegmentPool pool(Drivers());
    const UUID client = generate_uuid();
    auto segment = MakeSegment(0, "rollback", "tcp", "host");
    {
        auto access = pool.AcquireWriteAccess();
        ASSERT_EQ(access.MountSegment(segment, client), ErrorCode::OK);
    }
    auto transaction = [&] {
        auto access = pool.AcquireWriteAccess();
        return access.PrepareUnmount(segment.id, client);
    }();
    ASSERT_TRUE(transaction.has_value());
    {
        auto access = pool.AcquireWriteAccess();
        EXPECT_TRUE(ActiveRegionsByHost(access.Catalog()).empty());
        ASSERT_EQ(std::move(*transaction).Rollback(access), ErrorCode::OK);
        EXPECT_EQ(std::move(*transaction).Rollback(access),
                  ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
        EXPECT_EQ(std::move(*transaction).Commit(access),
                  ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
        EXPECT_TRUE(ActiveRegionsByHost(access.Catalog())
                        .at("host")
                        .at(segment.name)
                        .contains(segment.id));
    }
    {
        auto view = pool.AcquireReadAccess();
        EXPECT_NE(view.Placement().Find(segment.name,
                                        AllocationCandidateKind::NATIVE),
                  nullptr);
    }
    CommitUnmount(pool, segment, client);
}

TEST(SegmentPoolTest,
     UnmountMoveTransfersOwnershipAndFailureConsumesTransaction) {
    SegmentPool pool(Drivers());
    const UUID client = generate_uuid();
    auto segment = MakeSegment(0, "consume-unmount");
    auto access = pool.AcquireWriteAccess();
    ASSERT_EQ(access.MountSegment(segment, client), ErrorCode::OK);
    auto prepared = access.PrepareUnmount(segment.id, client);
    ASSERT_TRUE(prepared.has_value());
    auto transaction = std::move(*prepared);
    EXPECT_EQ(std::move(*prepared).Commit(access),
              ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    EXPECT_EQ(access.Catalog().Find(segment.id)->status,
              SegmentStatus::UNMOUNTING);
    access.Clear();
    EXPECT_EQ(std::move(transaction).Rollback(access),
              ErrorCode::SEGMENT_NOT_FOUND);
    EXPECT_EQ(std::move(transaction).Commit(access),
              ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
}

TEST(SegmentPoolTest, AllocationKeepsReadLockAcrossAllocatorCall) {
    for (bool named : {false, true}) {
        SCOPED_TRACE(named);
        SegmentPool pool(Drivers());
        const UUID client = generate_uuid();
        auto segment = MakeSegment(0, "blocking");
        auto allocator = std::make_shared<TestBufferAllocator>(
            segment.name, segment.te_endpoint, kRegionSize, segment.base);
        {
            MountedRegion mounted{segment, client, SegmentStatus::OK,
                                  RegionKind::HOST_MEMORY};
            auto access = pool.AcquireWriteAccess();
            auto transaction = access.PrepareAdopt(mounted, allocator, true);
            ASSERT_TRUE(transaction.has_value());
            ASSERT_EQ(transaction->Commit(access), ErrorCode::OK);
        }

        allocator->BlockNext();
        auto started = allocator->AllocationStarted();
        auto allocation = std::async(std::launch::async, [&] {
            if (named) {
                return pool
                    .AllocateInSegment(segment.name,
                                       AllocationCandidateKind::NATIVE, 4096)
                    .has_value();
            }
            ReplicaAllocationRequest request;
            request.replicas.size = 4096;
            return pool.AllocateReplicas(request).has_value();
        });
        ASSERT_EQ(started.wait_for(std::chrono::seconds(5)),
                  std::future_status::ready);

        std::atomic<bool> unmount_finished{false};
        std::thread unmount([&] {
            CommitUnmount(pool, segment, client);
            unmount_finished.store(true, std::memory_order_release);
        });
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
        EXPECT_FALSE(unmount_finished.load(std::memory_order_acquire));
        allocator->AllowAllocation();
        EXPECT_TRUE(allocation.get());
        unmount.join();
        EXPECT_TRUE(unmount_finished.load(std::memory_order_acquire));
    }
}

TEST(SegmentPoolTest, LocalPlacementUsesMountedHostIdentity) {
    SegmentPool pool(Drivers());
    const UUID client = generate_uuid();
    auto local = MakeSegment(0, "local", "tcp", "writer");
    auto remote = MakeSegment(1, "remote", "tcp", "other-host");
    {
        auto access = pool.AcquireWriteAccess();
        ASSERT_EQ(access.MountSegment(local, client), ErrorCode::OK);
        ASSERT_EQ(access.MountSegment(remote, client), ErrorCode::OK);
    }
    ReplicaAllocationRequest request;
    request.replicas.size = 4096;
    request.host_affinity = {"writer", "key"};
    auto result = pool.AllocateReplicas(request, LocalFirstPlacementPolicy{});
    ASSERT_TRUE(result.has_value());
    ASSERT_EQ(result->size(), 1U);
    EXPECT_EQ(ReplicaEndpoint(result->front()), local.te_endpoint);
}

TEST(SegmentPoolTest, SameNameKindsRemainIndependentAcrossUnmountRollback) {
    SegmentPool pool(Drivers(true));
    const UUID client = generate_uuid();
    auto native = MakeSegment(0, "shared", "tcp", "host");
    auto cxl = MakeSegment(1, "shared", "cxl", "host");
    {
        auto access = pool.AcquireWriteAccess();
        ASSERT_EQ(access.MountSegment(native, client), ErrorCode::OK);
        ASSERT_EQ(access.MountSegment(cxl, client), ErrorCode::OK);
    }
    {
        auto native_replica = pool.AllocateInSegment(
            "shared", AllocationCandidateKind::NATIVE, 4096);
        auto cxl_replica = pool.AllocateInSegment(
            "shared", AllocationCandidateKind::CXL, 4096);
        ASSERT_TRUE(native_replica.has_value());
        ASSERT_TRUE(cxl_replica.has_value());
        EXPECT_EQ(ReplicaEndpoint(*native_replica), native.te_endpoint);
        EXPECT_EQ(ReplicaEndpoint(*cxl_replica), cxl.name);
    }
    auto transaction = [&] {
        auto access = pool.AcquireWriteAccess();
        return access.PrepareUnmount(cxl.id, client);
    }();
    ASSERT_TRUE(transaction.has_value());
    EXPECT_TRUE(
        pool.AllocateInSegment("shared", AllocationCandidateKind::NATIVE, 4096)
            .has_value());
    EXPECT_FALSE(
        pool.AllocateInSegment("shared", AllocationCandidateKind::CXL, 4096)
            .has_value());
    {
        auto access = pool.AcquireReadAccess();
        size_t used = 0;
        size_t capacity = 0;
        EXPECT_EQ(
            access.QueryAllocationCandidates(
                "shared", AllocationCandidateKind::NATIVE, used, capacity),
            ErrorCode::OK);
        EXPECT_EQ(capacity, kRegionSize);
        EXPECT_EQ(access.QueryAllocationCandidates(
                      "shared", AllocationCandidateKind::CXL, used, capacity),
                  ErrorCode::SEGMENT_NOT_FOUND);
    }
    {
        auto access = pool.AcquireWriteAccess();
        ASSERT_EQ(std::move(*transaction).Rollback(access), ErrorCode::OK);
    }
    EXPECT_TRUE(
        pool.AllocateInSegment("shared", AllocationCandidateKind::CXL, 4096)
            .has_value());
    CommitUnmount(pool, cxl, client);
    EXPECT_TRUE(
        pool.AllocateInSegment("shared", AllocationCandidateKind::NATIVE, 4096)
            .has_value());
    CommitUnmount(pool, native, client);
}

TEST(SegmentPoolTest,
     FailedRestoreKeepsExistingRegionAndRequiresCanonicalEndpoint) {
    SegmentPool pool(Drivers());
    const UUID client = generate_uuid();
    auto segment = MakeSegment(0, "restore-validation");
    {
        auto access = pool.AcquireWriteAccess();
        ASSERT_EQ(access.MountSegment(segment, client), ErrorCode::OK);
    }
    auto original = pool.AcquireReadAccess().GetAllocator(segment.id);
    std::vector<AllocatedBuffer::Descriptor> descriptors{
        {4096, segment.base, "tcp", segment.name}};
    {
        auto access = pool.AcquireWriteAccess();
        auto alias = access.PrepareRestore(segment, client, descriptors);
        ASSERT_FALSE(alias.has_value());
        EXPECT_EQ(alias.error(), ErrorCode::INVALID_PARAMS);
        descriptors[0].transport_endpoint_ = segment.te_endpoint;
        descriptors.push_back(descriptors[0]);
        auto overlap = access.PrepareRestore(segment, client, descriptors);
        ASSERT_FALSE(overlap.has_value());
        EXPECT_EQ(overlap.error(), ErrorCode::INVALID_PARAMS);
    }
    EXPECT_EQ(pool.AcquireReadAccess().GetAllocator(segment.id), original);
    EXPECT_TRUE(pool.AllocateInSegment(segment.name,
                                       AllocationCandidateKind::NATIVE, 4096)
                    .has_value());
    CommitUnmount(pool, segment, client);
}

TEST(SegmentPoolTest,
     RemountReplacesPublishedCandidateWithoutDuplicatingCapacity) {
    auto& metrics = MasterMetricManager::instance();
    const auto capacity_before = metrics.get_total_mem_capacity();
    SegmentPool pool(Drivers());
    const UUID client = generate_uuid();
    auto segment = MakeSegment(0, "remount");
    {
        auto access = pool.AcquireWriteAccess();
        ASSERT_EQ(access.MountSegment(segment, client), ErrorCode::OK);
    }
    EXPECT_EQ(metrics.get_total_mem_capacity(), capacity_before + kRegionSize);
    auto old_allocator = pool.AcquireReadAccess().GetAllocator(segment.id);
    {
        auto access = pool.AcquireWriteAccess();
        auto transaction = access.PrepareMount(segment, client);
        ASSERT_TRUE(transaction.has_value());
        ASSERT_EQ(transaction->Commit(access), ErrorCode::OK);
    }
    {
        auto access = pool.AcquireReadAccess();
        EXPECT_NE(access.GetAllocator(segment.id), old_allocator);
        EXPECT_TRUE(access.IsInactive(old_allocator, segment.name));
        EXPECT_FALSE(
            access.IsInactive(access.GetAllocator(segment.id), segment.name));
        const auto* entry = access.Placement().Find(
            segment.name, AllocationCandidateKind::NATIVE);
        ASSERT_NE(entry, nullptr);
        EXPECT_EQ(entry->candidates.size(), 1U);
    }
    EXPECT_EQ(metrics.get_total_mem_capacity(), capacity_before + kRegionSize);
    EXPECT_EQ(pool.GetMemoryUsageSnapshot().capacity_bytes, kRegionSize);
    old_allocator.reset();
    EXPECT_EQ(pool.GetMemoryUsage().capacity_bytes, kRegionSize);
    EXPECT_TRUE(pool.AllocateInSegment(segment.name,
                                       AllocationCandidateKind::NATIVE, 4096)
                    .has_value());
    CommitUnmount(pool, segment, client);
    EXPECT_EQ(metrics.get_total_mem_capacity(), capacity_before);
}

TEST(SegmentPoolTest, GracefulUnmountRetainsResourceUntilFinalization) {
    SegmentPool pool(Drivers());
    const UUID client = generate_uuid();
    auto segment = MakeSegment(0, "graceful");
    {
        auto access = pool.AcquireWriteAccess();
        ASSERT_EQ(access.MountSegment(segment, client), ErrorCode::OK);
    }
    auto allocator = pool.AcquireReadAccess().GetAllocator(segment.id);
    auto buffer = allocator->allocate(4096);
    ASSERT_NE(buffer, nullptr);
    auto transaction = [&] {
        auto access = pool.AcquireWriteAccess();
        return access.PrepareGracefulUnmount(segment.id, client);
    }();
    ASSERT_TRUE(transaction.has_value());
    {
        auto access = pool.AcquireReadAccess();
        EXPECT_FALSE(access.IsInactive(allocator, segment.name));
        EXPECT_EQ(access.GetAllocator(segment.id), allocator);
        EXPECT_EQ(access.Placement().Find(segment.name,
                                          AllocationCandidateKind::NATIVE),
                  nullptr);
    }
    allocator.reset();
    EXPECT_TRUE(buffer->isAllocatorValid());
    {
        auto access = pool.AcquireWriteAccess();
        ASSERT_EQ(std::move(*transaction).Finalize(access), ErrorCode::OK);
    }
    EXPECT_FALSE(buffer->isAllocatorValid());
    buffer.reset();
    EXPECT_EQ(pool.GetMemoryUsage().capacity_bytes, 0U);
}

TEST(SegmentPoolTest, GracefulUnmountCanResumeAndConsumesMovedTransaction) {
    SegmentPool pool(Drivers());
    const UUID client = generate_uuid();
    auto segment = MakeSegment(0, "graceful-resume");
    {
        auto access = pool.AcquireWriteAccess();
        ASSERT_EQ(access.MountSegment(segment, client), ErrorCode::OK);
        auto wrong_client =
            access.PrepareGracefulUnmount(segment.id, generate_uuid());
        ASSERT_FALSE(wrong_client.has_value());
        EXPECT_EQ(wrong_client.error(), ErrorCode::INVALID_PARAMS);
        auto abandoned = access.PrepareGracefulUnmount(segment.id, client);
        ASSERT_TRUE(abandoned.has_value());
    }
    {
        auto access = pool.AcquireReadAccess();
        ASSERT_NE(access.Catalog().Find(segment.id), nullptr);
        EXPECT_EQ(access.Catalog().Find(segment.id)->status,
                  SegmentStatus::GRACEFULLY_UNMOUNTING);
        EXPECT_EQ(access.Placement().Find(segment.name,
                                          AllocationCandidateKind::NATIVE),
                  nullptr);
    }
    auto transaction = [&] {
        auto access = pool.AcquireWriteAccess();
        return access.PrepareGracefulUnmount(segment.id, client);
    }();
    ASSERT_TRUE(transaction.has_value());
    auto moved = std::move(*transaction);
    {
        auto access = pool.AcquireWriteAccess();
        EXPECT_EQ(std::move(*transaction).Finalize(access),
                  ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
        EXPECT_NE(access.Catalog().Find(segment.id), nullptr);
        EXPECT_EQ(std::move(moved).Finalize(access), ErrorCode::OK);
        EXPECT_EQ(std::move(moved).Finalize(access),
                  ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
        EXPECT_EQ(access.Catalog().Find(segment.id), nullptr);
    }
}

TEST(SegmentPoolTest, StaleGracefulUnmountCannotFinalizeNewMount) {
    SegmentPool pool(Drivers());
    const UUID client = generate_uuid();
    auto segment = MakeSegment(0, "graceful-remount");
    auto access = pool.AcquireWriteAccess();
    ASSERT_EQ(access.MountSegment(segment, client), ErrorCode::OK);
    auto stale = access.PrepareGracefulUnmount(segment.id, client);
    auto resumed = access.PrepareGracefulUnmount(segment.id, client);
    ASSERT_TRUE(stale.has_value());
    ASSERT_TRUE(resumed.has_value());
    ASSERT_EQ(std::move(*resumed).Finalize(access), ErrorCode::OK);
    ASSERT_EQ(access.MountSegment(segment, client), ErrorCode::OK);
    auto current = access.PrepareGracefulUnmount(segment.id, client);
    ASSERT_TRUE(current.has_value());
    EXPECT_EQ(std::move(*stale).Finalize(access),
              ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    ASSERT_NE(access.Catalog().Find(segment.id), nullptr);
    EXPECT_EQ(std::move(*current).Finalize(access), ErrorCode::OK);
}

TEST(SegmentPoolTest, UnmountCannotEraseRegionOwnedByAnotherClient) {
    for (bool graceful : {false, true}) {
        SegmentPool pool(Drivers());
        const UUID client = generate_uuid();
        const UUID new_client = generate_uuid();
        auto segment = MakeSegment(0, "unmount-new-owner");
        auto access = pool.AcquireWriteAccess();
        ASSERT_EQ(access.MountSegment(segment, client), ErrorCode::OK);
        auto remount = [&] {
            access.Clear();
            ASSERT_EQ(access.MountSegment(segment, new_client), ErrorCode::OK);
        };
        if (graceful) {
            auto stale = access.PrepareGracefulUnmount(segment.id, client);
            ASSERT_TRUE(stale.has_value());
            remount();
            EXPECT_EQ(std::move(*stale).Finalize(access),
                      ErrorCode::INVALID_PARAMS);
        } else {
            auto stale = access.PrepareUnmount(segment.id, client);
            ASSERT_TRUE(stale.has_value());
            remount();
            EXPECT_EQ(std::move(*stale).Commit(access),
                      ErrorCode::INVALID_PARAMS);
        }
        const auto* mounted = access.Catalog().Find(segment.id);
        ASSERT_NE(mounted, nullptr);
        EXPECT_EQ(mounted->client_id, new_client);
        EXPECT_EQ(mounted->status, SegmentStatus::OK);
        auto current = access.PrepareUnmount(segment.id, new_client);
        ASSERT_TRUE(current.has_value());
        EXPECT_EQ(std::move(*current).Commit(access), ErrorCode::OK);
    }
}

TEST(SegmentPoolTest, GracefulUnmountCannotFinalizeUnexpectedStatus) {
    for (auto status : {SegmentStatus::OK, SegmentStatus::DRAINING}) {
        SegmentPool pool(Drivers());
        const UUID client = generate_uuid();
        auto segment = MakeSegment(0, "graceful-status-changed");
        auto access = pool.AcquireWriteAccess();
        ASSERT_EQ(access.MountSegment(segment, client), ErrorCode::OK);
        auto transaction = access.PrepareGracefulUnmount(segment.id, client);
        ASSERT_TRUE(transaction.has_value());
        ASSERT_EQ(access.SetSegmentStatusByName(segment.name, status),
                  ErrorCode::OK);
        EXPECT_EQ(std::move(*transaction).Finalize(access),
                  ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
        ASSERT_NE(access.Catalog().Find(segment.id), nullptr);
        EXPECT_EQ(access.Catalog().Find(segment.id)->status, status);
        auto current = access.PrepareUnmount(segment.id, client);
        ASSERT_TRUE(current.has_value());
        EXPECT_EQ(std::move(*current).Commit(access), ErrorCode::OK);
    }
}

TEST(SegmentPoolTest, StaleGracefulUnmountCannotFinalizeNewDrain) {
    SegmentPool pool(Drivers());
    const UUID client = generate_uuid();
    auto segment = MakeSegment(0, "graceful-new-drain");
    auto access = pool.AcquireWriteAccess();
    ASSERT_EQ(access.MountSegment(segment, client), ErrorCode::OK);
    auto stale = access.PrepareGracefulUnmount(segment.id, client);
    ASSERT_TRUE(stale.has_value());
    ASSERT_EQ(access.SetSegmentStatusByName(segment.name, SegmentStatus::OK),
              ErrorCode::OK);
    auto current = access.PrepareGracefulUnmount(segment.id, client);
    ASSERT_TRUE(current.has_value());
    EXPECT_EQ(std::move(*stale).Finalize(access),
              ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    ASSERT_NE(access.Catalog().Find(segment.id), nullptr);
    EXPECT_EQ(std::move(*current).Finalize(access), ErrorCode::OK);
}

TEST(SegmentPoolTest, StaleUnmountCannotModifyRegionAfterClear) {
    for (bool rollback : {false, true}) {
        SegmentPool pool(Drivers());
        const UUID client = generate_uuid();
        auto segment = MakeSegment(0, "unmount-clear");
        auto access = pool.AcquireWriteAccess();
        ASSERT_EQ(access.MountSegment(segment, client), ErrorCode::OK);
        auto stale = access.PrepareUnmount(segment.id, client);
        ASSERT_TRUE(stale.has_value());
        access.Clear();
        ASSERT_EQ(access.MountSegment(segment, client), ErrorCode::OK);
        auto current = access.PrepareUnmount(segment.id, client);
        ASSERT_TRUE(current.has_value());
        EXPECT_EQ(rollback ? std::move(*stale).Rollback(access)
                           : std::move(*stale).Commit(access),
                  ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
        ASSERT_NE(access.Catalog().Find(segment.id), nullptr);
        EXPECT_EQ(access.Catalog().Find(segment.id)->status,
                  SegmentStatus::UNMOUNTING);
        EXPECT_EQ(std::move(*current).Rollback(access), ErrorCode::OK);
    }
}

TEST(SegmentPoolTest, GracefulUnmountFailureConsumesTransaction) {
    SegmentPool pool(Drivers());
    const UUID client = generate_uuid();
    auto segment = MakeSegment(0, "graceful-removed");
    auto access = pool.AcquireWriteAccess();
    ASSERT_EQ(access.MountSegment(segment, client), ErrorCode::OK);
    auto transaction = access.PrepareGracefulUnmount(segment.id, client);
    ASSERT_TRUE(transaction.has_value());
    access.Clear();
    EXPECT_EQ(std::move(*transaction).Finalize(access),
              ErrorCode::SEGMENT_NOT_FOUND);
    EXPECT_EQ(std::move(*transaction).Finalize(access),
              ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
}

TEST(SegmentPoolTest, RemovedCxlBindingIsInactiveWhileSiblingRemainsReadable) {
    SegmentPool pool(Drivers(true));
    const UUID client = generate_uuid();
    auto first = MakeSegment(0, "first-binding", "cxl");
    auto second = MakeSegment(1, "second-binding", "cxl");
    {
        auto access = pool.AcquireWriteAccess();
        ASSERT_EQ(access.MountSegment(first, client), ErrorCode::OK);
        ASSERT_EQ(access.MountSegment(second, client), ErrorCode::OK);
    }
    auto allocator = pool.AcquireReadAccess().GetAllocator(first.id);
    auto transaction = [&] {
        auto access = pool.AcquireWriteAccess();
        return access.PrepareUnmount(first.id, client);
    }();
    ASSERT_TRUE(transaction.has_value());
    EXPECT_TRUE(pool.AcquireReadAccess().IsInactive(allocator, first.name));
    {
        auto access = pool.AcquireWriteAccess();
        ASSERT_EQ(std::move(*transaction).Commit(access), ErrorCode::OK);
    }
    {
        auto access = pool.AcquireReadAccess();
        EXPECT_TRUE(access.IsInactive(allocator, first.name));
        EXPECT_FALSE(access.IsInactive(allocator, second.name));
    }
    auto graceful = [&] {
        auto access = pool.AcquireWriteAccess();
        return access.PrepareGracefulUnmount(second.id, client);
    }();
    ASSERT_TRUE(graceful.has_value());
    EXPECT_FALSE(pool.AcquireReadAccess().IsInactive(allocator, second.name));
    {
        auto access = pool.AcquireWriteAccess();
        ASSERT_EQ(std::move(*graceful).Finalize(access), ErrorCode::OK);
    }
    EXPECT_TRUE(pool.AcquireReadAccess().IsInactive(allocator, second.name));
}

TEST(SegmentPoolTest, CxlBindingsShareUsageAndDriverCapacityLifetime) {
    auto& metrics = MasterMetricManager::instance();
    const auto before = metrics.get_total_mem_capacity();
    {
        SegmentPool pool(Drivers(true));
        const UUID client = generate_uuid();
        auto first = MakeSegment(0, "cxl-first", "cxl");
        auto second = MakeSegment(1, "cxl-second", "cxl");
        {
            auto access = pool.AcquireWriteAccess();
            ASSERT_EQ(access.MountSegment(first, client), ErrorCode::OK);
            ASSERT_EQ(access.MountSegment(second, client), ErrorCode::OK);
        }
        EXPECT_EQ(pool.GetMemoryUsageSnapshot().capacity_bytes, kRegionSize);
        EXPECT_EQ(pool.GetMemoryUsage().capacity_bytes, kRegionSize);
        EXPECT_EQ(metrics.get_total_mem_capacity(), before + kRegionSize);
        CommitUnmount(pool, first, client);
        CommitUnmount(pool, second, client);
        EXPECT_EQ(metrics.get_total_mem_capacity(), before + kRegionSize);
    }
    EXPECT_EQ(metrics.get_total_mem_capacity(), before);
}

TEST(RegionCatalogTest, RegistrationAndRemovalKeepRelatedIndexesConsistent) {
    RegionCatalog catalog;
    const UUID client = generate_uuid();
    const UUID other_client = generate_uuid();
    auto first = MakeSegment(0, "shared", "tcp", "host");
    auto second = MakeSegment(1, "shared", "tcp", "host");
    ASSERT_EQ(catalog.Register({first, client, SegmentStatus::OK}),
              ErrorCode::OK);
    ASSERT_EQ(catalog.Register({second, client, SegmentStatus::OK}),
              ErrorCode::OK);
    EXPECT_TRUE(ActiveRegionsByHost(catalog).at("host").at("shared").contains(
        first.id));

    auto duplicate = first;
    duplicate.name = "duplicate";
    EXPECT_EQ(catalog.Register({duplicate, other_client, SegmentStatus::OK}),
              ErrorCode::SEGMENT_ALREADY_EXISTS);
    auto conflict = MakeSegment(2, "shared", "tcp", "other-host");
    EXPECT_EQ(catalog.Register({conflict, other_client, SegmentStatus::OK}),
              ErrorCode::INVALID_PARAMS);
    EXPECT_EQ(catalog.Regions().size(), 2U);
    EXPECT_TRUE(catalog.RegionIds("duplicate").empty());
    EXPECT_FALSE(ActiveRegionsByHost(catalog).contains("other-host"));
    EXPECT_EQ(catalog.FindOwnerClientId("shared"), client);

    ASSERT_TRUE(catalog.Erase(first.id));
    EXPECT_FALSE(ActiveRegionsByHost(catalog).at("host").at("shared").contains(
        first.id));
    EXPECT_TRUE(ActiveRegionsByHost(catalog).at("host").at("shared").contains(
        second.id));
    EXPECT_EQ(catalog.RegionIds("shared").size(), 1U);
    ASSERT_TRUE(catalog.Erase(second.id));
    EXPECT_TRUE(catalog.RegionIds("shared").empty());
    EXPECT_FALSE(catalog.FindOwnerClientId("shared").has_value());
    EXPECT_TRUE(ActiveRegionsByHost(catalog).empty());
    EXPECT_TRUE(catalog.Regions().empty());
}

TEST(RegionCatalogTest, StatusChangesPreserveIdentityAndOwnership) {
    RegionCatalog catalog;
    const UUID client = generate_uuid();
    auto first = MakeSegment(0, "shared", "tcp", "host");
    auto second = MakeSegment(1, "shared", "tcp", "host");
    ASSERT_EQ(catalog.Register({first, client, SegmentStatus::DRAINING}),
              ErrorCode::OK);
    EXPECT_TRUE(ActiveRegionsByHost(catalog).empty());
    ASSERT_EQ(catalog.Register({second, client, SegmentStatus::OK}),
              ErrorCode::OK);
    ASSERT_TRUE(catalog.SetStatus(first.id, SegmentStatus::OK));
    EXPECT_EQ(ActiveRegionsByHost(catalog).at("host").at("shared").size(), 2U);
    ASSERT_TRUE(catalog.SetStatus(second.id, SegmentStatus::UNMOUNTING));
    EXPECT_TRUE(ActiveRegionsByHost(catalog).at("host").at("shared").contains(
        first.id));
    EXPECT_FALSE(ActiveRegionsByHost(catalog).at("host").at("shared").contains(
        second.id));
    ASSERT_TRUE(catalog.SetStatus(first.id, SegmentStatus::DRAINING));
    EXPECT_TRUE(ActiveRegionsByHost(catalog).empty());
    EXPECT_EQ(catalog.RegionIds("shared").size(), 2U);
    EXPECT_EQ(catalog.FindOwnerClientId("shared"), client);
    EXPECT_FALSE(catalog.SetStatus(generate_uuid(), SegmentStatus::OK));
    EXPECT_TRUE(ActiveRegionsByHost(catalog).empty());

    EXPECT_EQ(catalog.Find(first.id)->status, SegmentStatus::DRAINING);
    EXPECT_EQ(catalog.Find(second.id)->status, SegmentStatus::UNMOUNTING);
}

TEST(SegmentPoolTest, HostPlacementTracksLastMemberAndLifecycle) {
    SegmentPool pool(Drivers());
    const UUID client = generate_uuid();
    auto a1 = MakeSegment(0, "a", "tcp", "host-b");
    auto a2 = MakeSegment(1, "a", "tcp", "host-b");
    auto a3 = MakeSegment(2, "a", "tcp", "host-d");
    auto b = MakeSegment(3, "b", "tcp", "host-b");
    auto c = MakeSegment(4, "c", "tcp", "host-d");
    {
        auto access = pool.AcquireWriteAccess();
        for (const auto& segment : {a1, a2, a3, b, c}) {
            ASSERT_EQ(access.MountSegment(segment, client), ErrorCode::OK);
        }
    }
    std::string key;
    while (std::hash<std::string_view>{}(key) % 2 != 0) key += 'x';
    auto names = [&] {
        auto access = pool.AcquireReadAccess();
        std::vector<std::string> result;
        access.Placement().VisitHostOrderedSegmentNames(
            "host-b", key, [&](auto name) {
                result.emplace_back(name);
                return false;
            });
        return result;
    };
    EXPECT_EQ(names(), (std::vector<std::string>{"a", "b", "c"}));
    for (const auto& segment : {a1, a2, a3}) {
        auto access = pool.AcquireWriteAccess();
        auto transaction = access.PrepareUnmount(segment.id, client);
        ASSERT_TRUE(transaction.has_value());
        ASSERT_EQ(std::move(*transaction).Commit(access), ErrorCode::OK);
    }
    EXPECT_EQ(names(), (std::vector<std::string>{"b", "c"}));
    auto graceful = [&] {
        auto access = pool.AcquireWriteAccess();
        return access.PrepareGracefulUnmount(b.id, client);
    }();
    ASSERT_TRUE(graceful.has_value());
    EXPECT_EQ(names(), (std::vector<std::string>{"c"}));
    auto unmount = [&] {
        auto access = pool.AcquireWriteAccess();
        return access.PrepareUnmount(c.id, client);
    }();
    ASSERT_TRUE(unmount.has_value());
    EXPECT_TRUE(names().empty());
    {
        auto access = pool.AcquireWriteAccess();
        ASSERT_EQ(std::move(*unmount).Rollback(access), ErrorCode::OK);
        ASSERT_EQ(std::move(*graceful).Finalize(access), ErrorCode::OK);
        ASSERT_EQ(access.MountSegment(b, client), ErrorCode::OK);
        auto replacement = access.PrepareMount(c, client);
        ASSERT_TRUE(replacement.has_value());
        ASSERT_EQ(replacement->Commit(access), ErrorCode::OK);
    }
    EXPECT_EQ(names(), (std::vector<std::string>{"b", "c"}));
    {
        auto access = pool.AcquireWriteAccess();
        ASSERT_EQ(access.SetSegmentStatusByName("b", SegmentStatus::DRAINING),
                  ErrorCode::OK);
    }
    EXPECT_EQ(names(), (std::vector<std::string>{"c"}));
    CommitUnmount(pool, c, client);
    EXPECT_TRUE(names().empty());
    {
        auto access = pool.AcquireWriteAccess();
        access.Clear();
        ASSERT_EQ(access.MountSegment(b, client), ErrorCode::OK);
    }
    EXPECT_EQ(names(), (std::vector<std::string>{"b"}));
}

TEST(SegmentPoolTest, AbandonedAdoptionDoesNotRegisterUsage) {
    SegmentPool pool(Drivers());
    const UUID client = generate_uuid();
    auto segment = MakeSegment(0, "adopt-abandoned");
    auto allocator = std::make_shared<TestBufferAllocator>(
        segment.name, segment.te_endpoint, kRegionSize, segment.base);
    {
        auto access = pool.AcquireWriteAccess();
        auto transaction = access.PrepareAdopt(
            {segment, client, SegmentStatus::OK}, allocator, true);
        ASSERT_TRUE(transaction.has_value());
    }
    EXPECT_TRUE(pool.AcquireReadAccess().Catalog().Regions().empty());
    EXPECT_EQ(pool.GetMemoryUsage().capacity_bytes, 0U);

    SegmentPool destination(Drivers());
    auto access = destination.AcquireWriteAccess();
    auto transaction = access.PrepareAdopt({segment, client, SegmentStatus::OK},
                                           allocator, true);
    ASSERT_TRUE(transaction.has_value());
    ASSERT_EQ(transaction->Commit(access), ErrorCode::OK);
    EXPECT_EQ(destination.GetMemoryUsage().capacity_bytes, kRegionSize);
    EXPECT_EQ(pool.GetMemoryUsage().capacity_bytes, 0U);
}

TEST(SegmentPoolTest, RejectedAdoptionCommitDoesNotRegisterUsage) {
    SegmentPool pool(Drivers());
    const UUID client = generate_uuid();
    auto segment = MakeSegment(0, "adopt-rejected");
    auto allocator = std::make_shared<TestBufferAllocator>(
        segment.name, segment.te_endpoint, kRegionSize, segment.base);
    auto access = pool.AcquireWriteAccess();
    auto transaction = access.PrepareAdopt({segment, client, SegmentStatus::OK},
                                           allocator, true);
    ASSERT_TRUE(transaction.has_value());
    ASSERT_EQ(access.MountSegment(segment, client), ErrorCode::OK);
    EXPECT_EQ(transaction->Commit(access), ErrorCode::SEGMENT_ALREADY_EXISTS);
    access.Clear();
    EXPECT_EQ(pool.GetMemoryUsage().capacity_bytes, 0U);
}

TEST(SegmentPoolTest, AdoptRejectsCatalogConflictWithoutReplacingResource) {
    SegmentPool pool(Drivers());
    const UUID client = generate_uuid();
    auto segment = MakeSegment(0, "adopt-conflict");
    auto access = pool.AcquireWriteAccess();
    ASSERT_EQ(access.MountSegment(segment, client), ErrorCode::OK);
    auto allocator = std::make_shared<TestBufferAllocator>(
        segment.name, segment.te_endpoint, kRegionSize, segment.base);
    auto duplicate = access.PrepareAdopt({segment, client, SegmentStatus::OK},
                                         allocator, true);
    ASSERT_FALSE(duplicate.has_value());
    EXPECT_EQ(duplicate.error(), ErrorCode::SEGMENT_ALREADY_EXISTS);
    segment.id = generate_uuid();
    auto conflict = access.PrepareAdopt(
        {segment, generate_uuid(), SegmentStatus::OK}, allocator, true);
    ASSERT_FALSE(conflict.has_value());
    EXPECT_EQ(conflict.error(), ErrorCode::INVALID_PARAMS);
    EXPECT_EQ(access.Catalog().Regions().size(), 1U);
}

}  // namespace mooncake::test
