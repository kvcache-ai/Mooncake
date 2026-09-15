#include "segment_pool_test_peer.h"
#include "client_registry.h"

#include "segment/pool.h"

#include <gtest/gtest.h>
#include <msgpack.hpp>

#include <sys/wait.h>
#include <unistd.h>

#include <atomic>
#include <cerrno>
#include <chrono>
#include <future>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "ha/snapshot/store_resource_snapshot_codec.h"
#include "local_ssd/manager.h"
#include "master_metric_manager.h"
#include "serialize/serializer.h"
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
        return SegmentPoolTestPeer::PrepareUnmount(access, segment.id,
                                                   client_id);
    }();
    ASSERT_TRUE(transaction.has_value());
    auto access = pool.AcquireWriteAccess();
    ASSERT_EQ(std::move(*transaction).Commit(access), ErrorCode::OK);
    EXPECT_EQ(std::move(*transaction).Commit(access),
              ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    EXPECT_EQ(std::move(*transaction).Rollback(access),
              ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
}

tl::expected<std::vector<uint8_t>, SerializationError> CaptureAndEncode(
    const SegmentPool& pool, const LocalSsdPersistedState& local_ssd) {
    auto snapshot = pool.CaptureSnapshot();
    if (!snapshot) {
        return tl::make_unexpected(
            SerializationError(snapshot.error(), "capture SegmentPool failed"));
    }
    return ha::StoreResourceSnapshotCodec::Encode(*snapshot, local_ssd);
}

tl::expected<LocalSsdPersistedState, SerializationError> DecodeAndRestore(
    SegmentPool& pool, const std::vector<uint8_t>& bytes,
    bool account_capacity) {
    auto decoded = ha::StoreResourceSnapshotCodec::Decode(bytes);
    if (!decoded) return tl::make_unexpected(decoded.error());
    auto restored = pool.RestoreSnapshot(std::move(decoded->segment_pool),
                                         account_capacity);
    if (!restored)
        return tl::make_unexpected(
            SerializationError(restored.error(), "restore SegmentPool failed"));
    return std::move(decoded->local_ssd);
}

}  // namespace

TEST(SegmentPoolTest, ClassifiesOnlyCxlProtocolAsCxl) {
    ClientRegistry clients{false};
    SegmentPool pool(Drivers(true));
    const UUID client = generate_uuid();
    auto host = MakeSegment(0, "host", "rdma");
    auto cxl = MakeSegment(1, "cxl", "cxl");
    {
        auto access = pool.AcquireWriteAccess();
        ASSERT_EQ(access.MountSegment(host, clients.GetOrCreate(client)),
                  ErrorCode::OK);
        ASSERT_EQ(access.MountSegment(cxl, clients.GetOrCreate(client)),
                  ErrorCode::OK);
    }

    {
        auto view = pool.AcquireReadAccess();
        ASSERT_TRUE(view.FindSegment(host.id).has_value());
        EXPECT_EQ(SegmentPoolTestPeer::Catalog(view).Find(host.id)->kind,
                  RegionKind::HOST_MEMORY);
        ASSERT_TRUE(view.FindSegment(cxl.id).has_value());
        EXPECT_EQ(SegmentPoolTestPeer::Catalog(view).Find(cxl.id)->kind,
                  RegionKind::CXL);
        EXPECT_NE(SegmentPoolTestPeer::Placement(view).Find(
                      host.name, AllocationCandidateKind::NATIVE),
                  nullptr);
        EXPECT_NE(SegmentPoolTestPeer::Placement(view).Find(
                      cxl.name, AllocationCandidateKind::CXL),
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
        auto transaction =
            SegmentPoolTestPeer::PrepareMount(access, segment, client);
        ASSERT_TRUE(transaction.has_value());
    }
    EXPECT_FALSE(pool.AcquireReadAccess().FindSegment(segment.id).has_value());

    {
        auto access = pool.AcquireWriteAccess();
        auto transaction =
            SegmentPoolTestPeer::PrepareMount(access, segment, client);
        ASSERT_TRUE(transaction.has_value());
        ASSERT_EQ(transaction->Commit(access), ErrorCode::OK);
    }
    EXPECT_TRUE(pool.AcquireReadAccess().FindSegment(segment.id).has_value());
    CommitUnmount(pool, segment, client);
}

TEST(SegmentPoolTest, MountCommitRejectsPreparedIdConflicts) {
    SegmentPool pool(Drivers());
    const UUID client = generate_uuid();
    auto segment = MakeSegment(0, "prepared-conflict");
    {
        auto access = pool.AcquireWriteAccess();
        auto first = SegmentPoolTestPeer::PrepareMount(access, segment, client);
        auto duplicate =
            SegmentPoolTestPeer::PrepareMount(access, segment, client);
        auto conflicting_owner =
            SegmentPoolTestPeer::PrepareMount(access, segment, generate_uuid());
        ASSERT_TRUE(first.has_value());
        ASSERT_TRUE(duplicate.has_value());
        ASSERT_TRUE(conflicting_owner.has_value());
        ASSERT_EQ(first->Commit(access), ErrorCode::OK);
        EXPECT_EQ(duplicate->Commit(access), ErrorCode::SEGMENT_ALREADY_EXISTS);
        EXPECT_EQ(conflicting_owner->Commit(access),
                  ErrorCode::SEGMENT_ALREADY_EXISTS);
        EXPECT_EQ(first->Commit(access),
                  ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    }
    {
        auto read = pool.AcquireReadAccess();
        EXPECT_EQ(read.Segments().size(), 1U);
        auto* entry = SegmentPoolTestPeer::Placement(read).Find(
            segment.name, AllocationCandidateKind::NATIVE);
        ASSERT_NE(entry, nullptr);
        ASSERT_EQ(entry->candidates.size(), 1U);
        EXPECT_EQ((*entry->candidates.begin())->allocator_handle(),
                  SegmentPoolTestPeer::GetAllocator(read, segment.id));
    }
    EXPECT_EQ(pool.GetMemoryUsage().capacity_bytes, kRegionSize);
    EXPECT_TRUE(SegmentPoolTestPeer::AllocateInSegment(
                    pool, segment.name, AllocationCandidateKind::NATIVE, 4096)
                    .has_value());
    CommitUnmount(pool, segment, client);
}

TEST(SegmentPoolTest, RemountCommitRejectsInterveningStatusChanges) {
    ClientRegistry clients{false};
    for (auto initial : {SegmentStatus::OK, SegmentStatus::DRAINING}) {
        SegmentPool pool(Drivers());
        const UUID client = generate_uuid();
        auto segment = MakeSegment(0, "status-conflict");
        {
            auto access = pool.AcquireWriteAccess();
            ASSERT_EQ(access.MountSegment(segment, clients.GetOrCreate(client)),
                      ErrorCode::OK);
            ASSERT_EQ(SegmentPoolTestPeer::SetSegmentStatusByName(
                          access, segment.name, initial),
                      ErrorCode::OK);
        }
        auto original = SegmentPoolTestPeer::GetAllocator(
            pool.AcquireReadAccess(), segment.id);
        const auto next = initial == SegmentStatus::OK ? SegmentStatus::DRAINING
                                                       : SegmentStatus::OK;
        {
            auto access = pool.AcquireWriteAccess();
            auto prepared =
                SegmentPoolTestPeer::PrepareMount(access, segment, client);
            ASSERT_TRUE(prepared.has_value());
            ASSERT_EQ(SegmentPoolTestPeer::SetSegmentStatusByName(
                          access, segment.name, next),
                      ErrorCode::OK);
            EXPECT_EQ(prepared->Commit(access),
                      ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
        }
        {
            auto read = pool.AcquireReadAccess();
            EXPECT_EQ(SegmentPoolTestPeer::GetAllocator(read, segment.id),
                      original);
            EXPECT_EQ(
                SegmentPoolTestPeer::Catalog(read).Find(segment.id)->status,
                next);
        }
        EXPECT_EQ(SegmentPoolTestPeer::AllocateInSegment(
                      pool, segment.name, AllocationCandidateKind::NATIVE, 4096)
                      .has_value(),
                  next == SegmentStatus::OK);
        CommitUnmount(pool, segment, client);
    }
}

TEST(SegmentPoolTest, RemountCommitRejectsReplacedOrRemovedResource) {
    ClientRegistry clients{false};
    for (bool clear : {false, true}) {
        SegmentPool pool(Drivers());
        const UUID client = generate_uuid();
        auto segment = MakeSegment(0, "resource-conflict");
        {
            auto access = pool.AcquireWriteAccess();
            ASSERT_EQ(access.MountSegment(segment, clients.GetOrCreate(client)),
                      ErrorCode::OK);
            auto stale =
                SegmentPoolTestPeer::PrepareMount(access, segment, client);
            ASSERT_TRUE(stale.has_value());
            if (clear) {
                access.Clear();
            } else {
                auto replacement =
                    SegmentPoolTestPeer::PrepareMount(access, segment, client);
                ASSERT_TRUE(replacement.has_value());
                ASSERT_EQ(replacement->Commit(access), ErrorCode::OK);
            }
            EXPECT_EQ(stale->Commit(access),
                      ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
        }
        EXPECT_EQ(pool.GetMemoryUsage().capacity_bytes,
                  clear ? 0U : kRegionSize);
        EXPECT_EQ(SegmentPoolTestPeer::AllocateInSegment(
                      pool, segment.name, AllocationCandidateKind::NATIVE, 4096)
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
        auto transaction = SegmentPoolTestPeer::PrepareRestore(
            access, segment, client, descriptors);
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
    ClientRegistry clients{false};
    SegmentPool pool(Drivers());
    const UUID client = generate_uuid();
    auto segment = MakeSegment(0, "draining", "tcp", "host");
    {
        auto access = pool.AcquireWriteAccess();
        ASSERT_EQ(access.MountSegment(segment, clients.GetOrCreate(client)),
                  ErrorCode::OK);
        ASSERT_EQ(SegmentPoolTestPeer::SetSegmentStatusByName(
                      access, segment.name, SegmentStatus::DRAINING),
                  ErrorCode::OK);
        auto transaction =
            SegmentPoolTestPeer::PrepareUnmount(access, segment.id, client);
        ASSERT_TRUE(transaction.has_value());
        ASSERT_EQ(std::move(*transaction).Rollback(access), ErrorCode::OK);
        ASSERT_TRUE(access.FindSegment(segment.id).has_value());
        EXPECT_EQ(SegmentPoolTestPeer::Catalog(access).Find(segment.id)->status,
                  SegmentStatus::DRAINING);
    }

    {
        auto view = pool.AcquireReadAccess();
        ASSERT_TRUE(view.FindSegment(segment.id).has_value());
        EXPECT_EQ(SegmentPoolTestPeer::Catalog(view).Find(segment.id)->status,
                  SegmentStatus::DRAINING);
        EXPECT_TRUE(
            ActiveRegionsByHost(SegmentPoolTestPeer::Catalog(view)).empty());
        EXPECT_EQ(SegmentPoolTestPeer::Placement(view).Find(
                      segment.name, AllocationCandidateKind::NATIVE),
                  nullptr);
    }

    {
        auto access = pool.AcquireWriteAccess();
        ASSERT_EQ(SegmentPoolTestPeer::SetSegmentStatusByName(
                      access, segment.name, SegmentStatus::OK),
                  ErrorCode::OK);
    }
    CommitUnmount(pool, segment, client);
}

TEST(SegmentPoolTest, ImmediateUnmountRollbackRestoresPlacement) {
    ClientRegistry clients{false};
    SegmentPool pool(Drivers());
    const UUID client = generate_uuid();
    auto segment = MakeSegment(0, "rollback", "tcp", "host");
    {
        auto access = pool.AcquireWriteAccess();
        ASSERT_EQ(access.MountSegment(segment, clients.GetOrCreate(client)),
                  ErrorCode::OK);
    }
    auto transaction = [&] {
        auto access = pool.AcquireWriteAccess();
        return SegmentPoolTestPeer::PrepareUnmount(access, segment.id, client);
    }();
    ASSERT_TRUE(transaction.has_value());
    {
        auto access = pool.AcquireWriteAccess();
        EXPECT_TRUE(
            ActiveRegionsByHost(SegmentPoolTestPeer::Catalog(access)).empty());
        ASSERT_EQ(std::move(*transaction).Rollback(access), ErrorCode::OK);
        EXPECT_EQ(std::move(*transaction).Rollback(access),
                  ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
        EXPECT_EQ(std::move(*transaction).Commit(access),
                  ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
        EXPECT_TRUE(ActiveRegionsByHost(SegmentPoolTestPeer::Catalog(access))
                        .at("host")
                        .at(segment.name)
                        .contains(segment.id));
    }
    {
        auto view = pool.AcquireReadAccess();
        EXPECT_NE(SegmentPoolTestPeer::Placement(view).Find(
                      segment.name, AllocationCandidateKind::NATIVE),
                  nullptr);
    }
    CommitUnmount(pool, segment, client);
}

TEST(SegmentPoolTest,
     UnmountMoveTransfersOwnershipAndFailureConsumesTransaction) {
    ClientRegistry clients{false};
    SegmentPool pool(Drivers());
    const UUID client = generate_uuid();
    auto segment = MakeSegment(0, "consume-unmount");
    auto access = pool.AcquireWriteAccess();
    ASSERT_EQ(access.MountSegment(segment, clients.GetOrCreate(client)),
              ErrorCode::OK);
    auto prepared =
        SegmentPoolTestPeer::PrepareUnmount(access, segment.id, client);
    ASSERT_TRUE(prepared.has_value());
    auto transaction = std::move(*prepared);
    EXPECT_EQ(std::move(*prepared).Commit(access),
              ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    EXPECT_EQ(SegmentPoolTestPeer::Catalog(access).Find(segment.id)->status,
              SegmentStatus::UNMOUNTING);
    access.Clear();
    EXPECT_EQ(std::move(transaction).Rollback(access),
              ErrorCode::SEGMENT_NOT_FOUND);
    EXPECT_EQ(std::move(transaction).Commit(access),
              ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
}

TEST(SegmentPoolTest, UnmountTryCommitPreservesTokenAfterFailure) {
    ClientRegistry clients{false};
    SegmentPool pool(Drivers());
    SegmentPool empty_pool(Drivers());
    const UUID client = generate_uuid();
    const auto segment = MakeSegment(0, "retry-unmount");
    auto access = pool.AcquireWriteAccess();
    ASSERT_EQ(access.MountSegment(segment, clients.GetOrCreate(client)),
              ErrorCode::OK);
    auto transaction =
        SegmentPoolTestPeer::PrepareUnmount(access, segment.id, client);
    ASSERT_TRUE(transaction);
    {
        auto missing_access = empty_pool.AcquireWriteAccess();
        EXPECT_EQ(transaction->TryCommit(missing_access),
                  ErrorCode::SEGMENT_NOT_FOUND);
    }
    EXPECT_EQ(transaction->TryCommit(access), ErrorCode::OK);
    EXPECT_FALSE(access.FindSegment(segment.id).has_value());
    EXPECT_EQ(transaction->TryCommit(access),
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
            auto transaction = SegmentPoolTestPeer::PrepareAdopt(
                access, mounted, allocator, true);
            ASSERT_TRUE(transaction.has_value());
            ASSERT_EQ(transaction->Commit(access), ErrorCode::OK);
        }

        allocator->BlockNext();
        auto started = allocator->AllocationStarted();
        auto allocation = std::async(std::launch::async, [&] {
            if (named) {
                return SegmentPoolTestPeer::AllocateInSegment(
                           pool, segment.name, AllocationCandidateKind::NATIVE,
                           4096)
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
    ClientRegistry clients{false};
    SegmentPool pool(Drivers(), PlacementPolicyType::LOCAL_FIRST);
    const UUID client = generate_uuid();
    auto local = MakeSegment(0, "local", "tcp", "writer");
    auto remote = MakeSegment(1, "remote", "tcp", "other-host");
    {
        auto access = pool.AcquireWriteAccess();
        ASSERT_EQ(access.MountSegment(local, clients.GetOrCreate(client)),
                  ErrorCode::OK);
        ASSERT_EQ(access.MountSegment(remote, clients.GetOrCreate(client)),
                  ErrorCode::OK);
    }
    ReplicaAllocationRequest request;
    request.replicas.size = 4096;
    request.host_affinity = {"writer", "key"};
    auto result = pool.AllocateReplicas(request);
    ASSERT_TRUE(result.has_value());
    ASSERT_EQ(result->size(), 1U);
    EXPECT_EQ(ReplicaEndpoint(result->front()), local.te_endpoint);
}

TEST(SegmentPoolTest, HostPreferenceIsResolvedInsidePool) {
    ClientRegistry clients{false};
    for (const bool prefer_same_node : {false, true}) {
        for (const size_t replica_count : {1U, 2U}) {
            SCOPED_TRACE(prefer_same_node);
            SCOPED_TRACE(replica_count);
            SegmentPool pool(Drivers(), PlacementPolicyType::FREE_RATIO_FIRST);
            const UUID client = generate_uuid();
            auto local = MakeSegment(0, "local", "tcp", "writer");
            auto remote = MakeSegment(1, "remote", "tcp", "other-host");
            auto remote2 = MakeSegment(2, "remote2", "tcp", "other-host2");
            {
                auto access = pool.AcquireWriteAccess();
                ASSERT_EQ(
                    access.MountSegment(local, clients.GetOrCreate(client)),
                    ErrorCode::OK);
                ASSERT_EQ(
                    access.MountSegment(remote, clients.GetOrCreate(client)),
                    ErrorCode::OK);
                ASSERT_EQ(
                    access.MountSegment(remote2, clients.GetOrCreate(client)),
                    ErrorCode::OK);
            }
            // Make local less attractive to FREE_RATIO_FIRST without filling
            // it.
            auto occupied = pool.AllocateInSegment(local.name, kRegionSize / 2);
            ASSERT_TRUE(occupied);
            ReplicaAllocationRequest request;
            request.replicas = {4096, replica_count, ReplicaType::MEMORY};
            request.host_affinity = {"writer", "key", prefer_same_node};
            auto result = pool.AllocateReplicas(request);
            ASSERT_TRUE(result);
            ASSERT_EQ(result->size(), replica_count);
            for (const auto& replica : *result) {
                if (prefer_same_node && replica_count == 1) {
                    EXPECT_EQ(ReplicaEndpoint(replica), local.te_endpoint);
                } else {
                    EXPECT_NE(ReplicaEndpoint(replica), local.te_endpoint);
                }
            }
        }
    }
}

TEST(SegmentPoolTest, SameNameKindsRemainIndependentAcrossUnmountRollback) {
    ClientRegistry clients{false};
    SegmentPool pool(Drivers(true));
    const UUID client = generate_uuid();
    auto native = MakeSegment(0, "shared", "tcp", "host");
    auto cxl = MakeSegment(1, "shared", "cxl", "host");
    {
        auto access = pool.AcquireWriteAccess();
        ASSERT_EQ(access.MountSegment(native, clients.GetOrCreate(client)),
                  ErrorCode::OK);
        ASSERT_EQ(access.MountSegment(cxl, clients.GetOrCreate(client)),
                  ErrorCode::OK);
    }
    {
        auto native_replica = SegmentPoolTestPeer::AllocateInSegment(
            pool, "shared", AllocationCandidateKind::NATIVE, 4096);
        auto cxl_replica = SegmentPoolTestPeer::AllocateInSegment(
            pool, "shared", AllocationCandidateKind::CXL, 4096);
        ASSERT_TRUE(native_replica.has_value());
        ASSERT_TRUE(cxl_replica.has_value());
        EXPECT_EQ(ReplicaEndpoint(*native_replica), native.te_endpoint);
        EXPECT_EQ(ReplicaEndpoint(*cxl_replica), cxl.name);
    }
    auto transaction = [&] {
        auto access = pool.AcquireWriteAccess();
        return SegmentPoolTestPeer::PrepareUnmount(access, cxl.id, client);
    }();
    ASSERT_TRUE(transaction.has_value());
    EXPECT_TRUE(SegmentPoolTestPeer::AllocateInSegment(
                    pool, "shared", AllocationCandidateKind::NATIVE, 4096)
                    .has_value());
    EXPECT_FALSE(SegmentPoolTestPeer::AllocateInSegment(
                     pool, "shared", AllocationCandidateKind::CXL, 4096)
                     .has_value());
    {
        auto access = pool.AcquireReadAccess();
        size_t used = 0;
        size_t capacity = 0;
        EXPECT_EQ(access.QueryCapacity("shared", used, capacity),
                  ErrorCode::OK);
        EXPECT_EQ(capacity, kRegionSize);
    }
    {
        auto access = pool.AcquireWriteAccess();
        ASSERT_EQ(std::move(*transaction).Rollback(access), ErrorCode::OK);
    }
    EXPECT_TRUE(SegmentPoolTestPeer::AllocateInSegment(
                    pool, "shared", AllocationCandidateKind::CXL, 4096)
                    .has_value());
    CommitUnmount(pool, cxl, client);
    EXPECT_TRUE(SegmentPoolTestPeer::AllocateInSegment(
                    pool, "shared", AllocationCandidateKind::NATIVE, 4096)
                    .has_value());
    CommitUnmount(pool, native, client);
}

TEST(SegmentPoolTest,
     FailedRestoreKeepsExistingRegionAndRequiresCanonicalEndpoint) {
    ClientRegistry clients{false};
    SegmentPool pool(Drivers());
    const UUID client = generate_uuid();
    auto segment = MakeSegment(0, "restore-validation");
    {
        auto access = pool.AcquireWriteAccess();
        ASSERT_EQ(access.MountSegment(segment, clients.GetOrCreate(client)),
                  ErrorCode::OK);
    }
    auto original =
        SegmentPoolTestPeer::GetAllocator(pool.AcquireReadAccess(), segment.id);
    std::vector<AllocatedBuffer::Descriptor> descriptors{
        {4096, segment.base, "tcp", segment.name}};
    {
        auto access = pool.AcquireWriteAccess();
        auto alias = SegmentPoolTestPeer::PrepareRestore(access, segment,
                                                         client, descriptors);
        ASSERT_FALSE(alias.has_value());
        EXPECT_EQ(alias.error(), ErrorCode::INVALID_PARAMS);
        descriptors[0].transport_endpoint_ = segment.te_endpoint;
        descriptors.push_back(descriptors[0]);
        auto overlap = SegmentPoolTestPeer::PrepareRestore(access, segment,
                                                           client, descriptors);
        ASSERT_FALSE(overlap.has_value());
        EXPECT_EQ(overlap.error(), ErrorCode::INVALID_PARAMS);
    }
    EXPECT_EQ(
        SegmentPoolTestPeer::GetAllocator(pool.AcquireReadAccess(), segment.id),
        original);
    EXPECT_TRUE(SegmentPoolTestPeer::AllocateInSegment(
                    pool, segment.name, AllocationCandidateKind::NATIVE, 4096)
                    .has_value());
    CommitUnmount(pool, segment, client);
}

TEST(SegmentPoolTest,
     RemountReplacesPublishedCandidateWithoutDuplicatingCapacity) {
    ClientRegistry clients{false};
    auto& metrics = MasterMetricManager::instance();
    const auto capacity_before = metrics.get_total_mem_capacity();
    SegmentPool pool(Drivers());
    const UUID client = generate_uuid();
    auto segment = MakeSegment(0, "remount");
    {
        auto access = pool.AcquireWriteAccess();
        ASSERT_EQ(access.MountSegment(segment, clients.GetOrCreate(client)),
                  ErrorCode::OK);
    }
    EXPECT_EQ(metrics.get_total_mem_capacity(), capacity_before + kRegionSize);
    auto old_allocator =
        SegmentPoolTestPeer::GetAllocator(pool.AcquireReadAccess(), segment.id);
    {
        auto access = pool.AcquireWriteAccess();
        auto transaction =
            SegmentPoolTestPeer::PrepareMount(access, segment, client);
        ASSERT_TRUE(transaction.has_value());
        ASSERT_EQ(transaction->Commit(access), ErrorCode::OK);
    }
    {
        auto access = pool.AcquireReadAccess();
        EXPECT_NE(SegmentPoolTestPeer::GetAllocator(access, segment.id),
                  old_allocator);
        const auto* entry = SegmentPoolTestPeer::Placement(access).Find(
            segment.name, AllocationCandidateKind::NATIVE);
        ASSERT_NE(entry, nullptr);
        EXPECT_EQ(entry->candidates.size(), 1U);
    }
    EXPECT_EQ(metrics.get_total_mem_capacity(), capacity_before + kRegionSize);
    EXPECT_EQ(pool.GetMemoryUsageSnapshot().capacity_bytes, kRegionSize);
    old_allocator.reset();
    EXPECT_EQ(pool.GetMemoryUsage().capacity_bytes, kRegionSize);
    EXPECT_TRUE(SegmentPoolTestPeer::AllocateInSegment(
                    pool, segment.name, AllocationCandidateKind::NATIVE, 4096)
                    .has_value());
    CommitUnmount(pool, segment, client);
    EXPECT_EQ(metrics.get_total_mem_capacity(), capacity_before);
}

TEST(SegmentPoolTest, GracefulUnmountRetainsResourceUntilFinalization) {
    ClientRegistry clients{false};
    SegmentPool pool(Drivers());
    const UUID client = generate_uuid();
    auto segment = MakeSegment(0, "graceful");
    {
        auto access = pool.AcquireWriteAccess();
        ASSERT_EQ(access.MountSegment(segment, clients.GetOrCreate(client)),
                  ErrorCode::OK);
    }
    auto allocator =
        SegmentPoolTestPeer::GetAllocator(pool.AcquireReadAccess(), segment.id);
    auto buffer = allocator->allocate(4096);
    ASSERT_NE(buffer, nullptr);
    auto transaction = [&] {
        auto access = pool.AcquireWriteAccess();
        return SegmentPoolTestPeer::PrepareGracefulUnmount(access, segment.id,
                                                           client);
    }();
    ASSERT_TRUE(transaction.has_value());
    {
        auto access = pool.AcquireReadAccess();
        EXPECT_EQ(SegmentPoolTestPeer::GetAllocator(access, segment.id),
                  allocator);
        EXPECT_EQ(SegmentPoolTestPeer::Placement(access).Find(
                      segment.name, AllocationCandidateKind::NATIVE),
                  nullptr);
    }
    auto named = SegmentPoolTestPeer::AllocateInSegment(
        pool, segment.name, AllocationCandidateKind::NATIVE, 4096);
    ASSERT_FALSE(named.has_value());
    EXPECT_EQ(named.error(), ErrorCode::SEGMENT_NOT_FOUND);
    ReplicaAllocationRequest request;
    request.replicas.size = 4096;
    auto allocation = pool.AllocateReplicas(request);
    ASSERT_FALSE(allocation.has_value());
    EXPECT_EQ(allocation.error(), ErrorCode::NO_AVAILABLE_HANDLE);

    allocator.reset();
    EXPECT_TRUE(buffer->isAvailable());
    {
        auto access = pool.AcquireWriteAccess();
        ASSERT_EQ(std::move(*transaction).Finalize(access), ErrorCode::OK);
    }
    EXPECT_FALSE(buffer->isAvailable());
    buffer.reset();
    EXPECT_EQ(pool.GetMemoryUsage().capacity_bytes, 0U);
}

TEST(SegmentPoolTest, GracefulUnmountCanResumeAndConsumesMovedTransaction) {
    ClientRegistry clients{false};
    SegmentPool pool(Drivers());
    const UUID client = generate_uuid();
    auto segment = MakeSegment(0, "graceful-resume");
    {
        auto access = pool.AcquireWriteAccess();
        ASSERT_EQ(access.MountSegment(segment, clients.GetOrCreate(client)),
                  ErrorCode::OK);
        auto wrong_client = SegmentPoolTestPeer::PrepareGracefulUnmount(
            access, segment.id, generate_uuid());
        ASSERT_FALSE(wrong_client.has_value());
        EXPECT_EQ(wrong_client.error(), ErrorCode::INVALID_PARAMS);
        auto abandoned = SegmentPoolTestPeer::PrepareGracefulUnmount(
            access, segment.id, client);
        ASSERT_TRUE(abandoned.has_value());
    }
    {
        auto access = pool.AcquireReadAccess();
        ASSERT_TRUE(access.FindSegment(segment.id).has_value());
        EXPECT_EQ(SegmentPoolTestPeer::Catalog(access).Find(segment.id)->status,
                  SegmentStatus::GRACEFULLY_UNMOUNTING);
        EXPECT_EQ(SegmentPoolTestPeer::Placement(access).Find(
                      segment.name, AllocationCandidateKind::NATIVE),
                  nullptr);
    }
    auto transaction = [&] {
        auto access = pool.AcquireWriteAccess();
        return SegmentPoolTestPeer::PrepareGracefulUnmount(access, segment.id,
                                                           client);
    }();
    ASSERT_TRUE(transaction.has_value());
    auto moved = std::move(*transaction);
    {
        auto access = pool.AcquireWriteAccess();
        EXPECT_EQ(std::move(*transaction).Finalize(access),
                  ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
        EXPECT_TRUE(access.FindSegment(segment.id).has_value());
        EXPECT_EQ(std::move(moved).Finalize(access), ErrorCode::OK);
        EXPECT_EQ(std::move(moved).Finalize(access),
                  ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
        EXPECT_FALSE(access.FindSegment(segment.id).has_value());
    }
}

TEST(SegmentPoolTest, StaleGracefulUnmountCannotFinalizeNewMount) {
    ClientRegistry clients{false};
    SegmentPool pool(Drivers());
    const UUID client = generate_uuid();
    auto segment = MakeSegment(0, "graceful-remount");
    auto access = pool.AcquireWriteAccess();
    ASSERT_EQ(access.MountSegment(segment, clients.GetOrCreate(client)),
              ErrorCode::OK);
    auto stale =
        SegmentPoolTestPeer::PrepareGracefulUnmount(access, segment.id, client);
    auto resumed =
        SegmentPoolTestPeer::PrepareGracefulUnmount(access, segment.id, client);
    ASSERT_TRUE(stale.has_value());
    ASSERT_TRUE(resumed.has_value());
    ASSERT_EQ(std::move(*resumed).Finalize(access), ErrorCode::OK);
    ASSERT_EQ(access.MountSegment(segment, clients.GetOrCreate(client)),
              ErrorCode::OK);
    auto current =
        SegmentPoolTestPeer::PrepareGracefulUnmount(access, segment.id, client);
    ASSERT_TRUE(current.has_value());
    EXPECT_EQ(std::move(*stale).Finalize(access),
              ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    ASSERT_TRUE(access.FindSegment(segment.id).has_value());
    EXPECT_EQ(std::move(*current).Finalize(access), ErrorCode::OK);
}

TEST(SegmentPoolTest, UnmountCannotEraseRegionOwnedByAnotherClient) {
    ClientRegistry clients{false};
    for (bool graceful : {false, true}) {
        SegmentPool pool(Drivers());
        const UUID client = generate_uuid();
        const UUID new_client = generate_uuid();
        auto segment = MakeSegment(0, "unmount-new-owner");
        auto access = pool.AcquireWriteAccess();
        ASSERT_EQ(access.MountSegment(segment, clients.GetOrCreate(client)),
                  ErrorCode::OK);
        auto remount = [&] {
            access.Clear();
            ASSERT_EQ(
                access.MountSegment(segment, clients.GetOrCreate(new_client)),
                ErrorCode::OK);
        };
        if (graceful) {
            auto stale = SegmentPoolTestPeer::PrepareGracefulUnmount(
                access, segment.id, client);
            ASSERT_TRUE(stale.has_value());
            remount();
            EXPECT_EQ(std::move(*stale).Finalize(access),
                      ErrorCode::INVALID_PARAMS);
        } else {
            auto stale =
                SegmentPoolTestPeer::PrepareUnmount(access, segment.id, client);
            ASSERT_TRUE(stale.has_value());
            remount();
            EXPECT_EQ(std::move(*stale).Commit(access),
                      ErrorCode::INVALID_PARAMS);
        }
        const auto mounted = access.FindSegment(segment.id);
        ASSERT_TRUE(mounted.has_value());
        EXPECT_EQ(mounted->client_id, new_client);
        EXPECT_EQ(mounted->status, SegmentStatus::OK);
        auto current =
            SegmentPoolTestPeer::PrepareUnmount(access, segment.id, new_client);
        ASSERT_TRUE(current.has_value());
        EXPECT_EQ(std::move(*current).Commit(access), ErrorCode::OK);
    }
}

TEST(SegmentPoolTest, GracefulUnmountCannotFinalizeUnexpectedStatus) {
    ClientRegistry clients{false};
    for (auto status : {SegmentStatus::OK, SegmentStatus::DRAINING}) {
        SegmentPool pool(Drivers());
        const UUID client = generate_uuid();
        auto segment = MakeSegment(0, "graceful-status-changed");
        auto access = pool.AcquireWriteAccess();
        ASSERT_EQ(access.MountSegment(segment, clients.GetOrCreate(client)),
                  ErrorCode::OK);
        auto transaction = SegmentPoolTestPeer::PrepareGracefulUnmount(
            access, segment.id, client);
        ASSERT_TRUE(transaction.has_value());
        ASSERT_EQ(SegmentPoolTestPeer::SetSegmentStatusByName(
                      access, segment.name, status),
                  ErrorCode::OK);
        EXPECT_EQ(std::move(*transaction).Finalize(access),
                  ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
        ASSERT_TRUE(access.FindSegment(segment.id).has_value());
        EXPECT_EQ(SegmentPoolTestPeer::Catalog(access).Find(segment.id)->status,
                  status);
        auto current =
            SegmentPoolTestPeer::PrepareUnmount(access, segment.id, client);
        ASSERT_TRUE(current.has_value());
        EXPECT_EQ(std::move(*current).Commit(access), ErrorCode::OK);
    }
}

TEST(SegmentPoolTest, StaleGracefulUnmountCannotFinalizeNewDrain) {
    ClientRegistry clients{false};
    SegmentPool pool(Drivers());
    const UUID client = generate_uuid();
    auto segment = MakeSegment(0, "graceful-new-drain");
    auto access = pool.AcquireWriteAccess();
    ASSERT_EQ(access.MountSegment(segment, clients.GetOrCreate(client)),
              ErrorCode::OK);
    auto stale =
        SegmentPoolTestPeer::PrepareGracefulUnmount(access, segment.id, client);
    ASSERT_TRUE(stale.has_value());
    ASSERT_EQ(SegmentPoolTestPeer::SetSegmentStatusByName(access, segment.name,
                                                          SegmentStatus::OK),
              ErrorCode::OK);
    auto current =
        SegmentPoolTestPeer::PrepareGracefulUnmount(access, segment.id, client);
    ASSERT_TRUE(current.has_value());
    EXPECT_EQ(std::move(*stale).Finalize(access),
              ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    ASSERT_TRUE(access.FindSegment(segment.id).has_value());
    EXPECT_EQ(std::move(*current).Finalize(access), ErrorCode::OK);
}

TEST(SegmentPoolTest, StaleUnmountCannotModifyRegionAfterClear) {
    ClientRegistry clients{false};
    for (bool rollback : {false, true}) {
        SegmentPool pool(Drivers());
        const UUID client = generate_uuid();
        auto segment = MakeSegment(0, "unmount-clear");
        auto access = pool.AcquireWriteAccess();
        ASSERT_EQ(access.MountSegment(segment, clients.GetOrCreate(client)),
                  ErrorCode::OK);
        auto stale =
            SegmentPoolTestPeer::PrepareUnmount(access, segment.id, client);
        ASSERT_TRUE(stale.has_value());
        access.Clear();
        ASSERT_EQ(access.MountSegment(segment, clients.GetOrCreate(client)),
                  ErrorCode::OK);
        auto current =
            SegmentPoolTestPeer::PrepareUnmount(access, segment.id, client);
        ASSERT_TRUE(current.has_value());
        EXPECT_EQ(rollback ? std::move(*stale).Rollback(access)
                           : std::move(*stale).Commit(access),
                  ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
        ASSERT_TRUE(access.FindSegment(segment.id).has_value());
        EXPECT_EQ(SegmentPoolTestPeer::Catalog(access).Find(segment.id)->status,
                  SegmentStatus::UNMOUNTING);
        EXPECT_EQ(std::move(*current).Rollback(access), ErrorCode::OK);
    }
}

TEST(SegmentPoolTest, GracefulUnmountFailureConsumesTransaction) {
    ClientRegistry clients{false};
    SegmentPool pool(Drivers());
    const UUID client = generate_uuid();
    auto segment = MakeSegment(0, "graceful-removed");
    auto access = pool.AcquireWriteAccess();
    ASSERT_EQ(access.MountSegment(segment, clients.GetOrCreate(client)),
              ErrorCode::OK);
    auto transaction =
        SegmentPoolTestPeer::PrepareGracefulUnmount(access, segment.id, client);
    ASSERT_TRUE(transaction.has_value());
    access.Clear();
    EXPECT_EQ(std::move(*transaction).Finalize(access),
              ErrorCode::SEGMENT_NOT_FOUND);
    EXPECT_EQ(std::move(*transaction).Finalize(access),
              ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
}

TEST(SegmentPoolTest, RemovedCxlBindingIsInactiveWhileSiblingRemainsReadable) {
    ClientRegistry clients{false};
    SegmentPool pool(Drivers(true), PlacementPolicyType::CXL);
    const UUID client = generate_uuid();
    auto first = MakeSegment(0, "first-binding", "cxl");
    auto second = MakeSegment(1, "second-binding", "cxl");
    {
        auto access = pool.AcquireWriteAccess();
        ASSERT_EQ(access.MountSegment(first, clients.GetOrCreate(client)),
                  ErrorCode::OK);
        ASSERT_EQ(access.MountSegment(second, clients.GetOrCreate(client)),
                  ErrorCode::OK);
    }
    auto first_replica = pool.AllocateInSegment(first.name, 4096);
    auto second_replica = pool.AllocateInSegment(second.name, 4096);
    ASSERT_TRUE(first_replica);
    ASSERT_TRUE(second_replica);
    auto operation = pool.AcquireWriteAccess().BeginUnmount(first.id, client);
    ASSERT_TRUE(operation);
    EXPECT_TRUE(first_replica->has_invalid_mem_handle());
    EXPECT_FALSE(second_replica->has_invalid_mem_handle());
    {
        auto access = pool.AcquireWriteAccess();
        ASSERT_EQ(access.ReleaseUnmountedResources(operation->id),
                  ErrorCode::OK);
        ASSERT_EQ(access.AcknowledgeUnmount(operation->id), ErrorCode::OK);
        ASSERT_EQ(access.StartGracefulUnmount(second.id, client),
                  ErrorCode::OK);
    }
    EXPECT_TRUE(first_replica->has_invalid_mem_handle());
    EXPECT_FALSE(second_replica->has_invalid_mem_handle());
    EXPECT_FALSE(pool.AllocateInSegment(second.name, 4096));
    operation = pool.AcquireWriteAccess().BeginUnmount(second.id, client);
    ASSERT_TRUE(operation);
    EXPECT_TRUE(second_replica->has_invalid_mem_handle());
    auto access = pool.AcquireWriteAccess();
    ASSERT_EQ(access.ReleaseUnmountedResources(operation->id), ErrorCode::OK);
    ASSERT_EQ(access.AcknowledgeUnmount(operation->id), ErrorCode::OK);
}

TEST(SegmentPoolTest, CxlBindingsShareUsageAndDriverCapacityLifetime) {
    ClientRegistry clients{false};
    auto& metrics = MasterMetricManager::instance();
    const auto before = metrics.get_total_mem_capacity();
    {
        SegmentPool pool(Drivers(true));
        const UUID client = generate_uuid();
        auto first = MakeSegment(0, "cxl-first", "cxl");
        auto second = MakeSegment(1, "cxl-second", "cxl");
        EXPECT_EQ(pool.GetMemoryUsageSnapshot().capacity_bytes, kRegionSize);
        EXPECT_EQ(pool.GetMemoryUsage().capacity_bytes, kRegionSize);
        {
            auto access = pool.AcquireWriteAccess();
            ASSERT_EQ(access.MountSegment(first, clients.GetOrCreate(client)),
                      ErrorCode::OK);
            ASSERT_EQ(access.MountSegment(second, clients.GetOrCreate(client)),
                      ErrorCode::OK);
        }
        EXPECT_EQ(pool.GetMemoryUsageSnapshot().capacity_bytes, kRegionSize);
        EXPECT_EQ(pool.GetMemoryUsage().capacity_bytes, kRegionSize);
        EXPECT_EQ(metrics.get_total_mem_capacity(), before + kRegionSize);
        CommitUnmount(pool, first, client);
        CommitUnmount(pool, second, client);
        EXPECT_EQ(pool.GetMemoryUsageSnapshot().capacity_bytes, kRegionSize);
        EXPECT_EQ(pool.GetMemoryUsage().capacity_bytes, kRegionSize);
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
    auto shared = MakeSegment(2, "shared", "tcp", "other-host");
    ASSERT_EQ(catalog.Register({shared, other_client, SegmentStatus::OK}),
              ErrorCode::OK);
    EXPECT_EQ(catalog.Regions().size(), 3U);
    EXPECT_EQ(catalog.Find(shared.id)->client_id, other_client);
    EXPECT_TRUE(ActiveRegionsByHost(catalog).contains("other-host"));
    ASSERT_TRUE(catalog.Erase(shared.id));
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
    ClientRegistry clients{false};
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
            ASSERT_EQ(access.MountSegment(segment, clients.GetOrCreate(client)),
                      ErrorCode::OK);
        }
    }
    std::string key;
    while (std::hash<std::string_view>{}(key) % 2 != 0) key += 'x';
    auto names = [&] {
        auto access = pool.AcquireReadAccess();
        std::vector<std::string> result;
        SegmentPoolTestPeer::Placement(access).VisitHostOrderedSegmentNames(
            "host-b", key, [&](auto name) {
                result.emplace_back(name);
                return false;
            });
        return result;
    };
    EXPECT_EQ(names(), (std::vector<std::string>{"a", "b", "c"}));
    for (const auto& segment : {a1, a2, a3}) {
        auto access = pool.AcquireWriteAccess();
        auto transaction =
            SegmentPoolTestPeer::PrepareUnmount(access, segment.id, client);
        ASSERT_TRUE(transaction.has_value());
        ASSERT_EQ(std::move(*transaction).Commit(access), ErrorCode::OK);
    }
    EXPECT_EQ(names(), (std::vector<std::string>{"b", "c"}));
    auto graceful = [&] {
        auto access = pool.AcquireWriteAccess();
        return SegmentPoolTestPeer::PrepareGracefulUnmount(access, b.id,
                                                           client);
    }();
    ASSERT_TRUE(graceful.has_value());
    EXPECT_EQ(names(), (std::vector<std::string>{"c"}));
    auto unmount = [&] {
        auto access = pool.AcquireWriteAccess();
        return SegmentPoolTestPeer::PrepareUnmount(access, c.id, client);
    }();
    ASSERT_TRUE(unmount.has_value());
    EXPECT_TRUE(names().empty());
    {
        auto access = pool.AcquireWriteAccess();
        ASSERT_EQ(std::move(*unmount).Rollback(access), ErrorCode::OK);
        ASSERT_EQ(std::move(*graceful).Finalize(access), ErrorCode::OK);
        ASSERT_EQ(access.MountSegment(b, clients.GetOrCreate(client)),
                  ErrorCode::OK);
        auto replacement = SegmentPoolTestPeer::PrepareMount(access, c, client);
        ASSERT_TRUE(replacement.has_value());
        ASSERT_EQ(replacement->Commit(access), ErrorCode::OK);
    }
    EXPECT_EQ(names(), (std::vector<std::string>{"b", "c"}));
    {
        auto access = pool.AcquireWriteAccess();
        ASSERT_EQ(SegmentPoolTestPeer::SetSegmentStatusByName(
                      access, "b", SegmentStatus::DRAINING),
                  ErrorCode::OK);
    }
    EXPECT_EQ(names(), (std::vector<std::string>{"c"}));
    CommitUnmount(pool, c, client);
    EXPECT_TRUE(names().empty());
    {
        auto access = pool.AcquireWriteAccess();
        access.Clear();
        ASSERT_EQ(access.MountSegment(b, clients.GetOrCreate(client)),
                  ErrorCode::OK);
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
        auto transaction = SegmentPoolTestPeer::PrepareAdopt(
            access, {segment, client, SegmentStatus::OK}, allocator, true);
        ASSERT_TRUE(transaction.has_value());
    }
    EXPECT_TRUE(pool.AcquireReadAccess().Segments().empty());
    EXPECT_EQ(pool.GetMemoryUsage().capacity_bytes, 0U);

    SegmentPool destination(Drivers());
    auto access = destination.AcquireWriteAccess();
    auto transaction = SegmentPoolTestPeer::PrepareAdopt(
        access, {segment, client, SegmentStatus::OK}, allocator, true);
    ASSERT_TRUE(transaction.has_value());
    ASSERT_EQ(transaction->Commit(access), ErrorCode::OK);
    EXPECT_EQ(destination.GetMemoryUsage().capacity_bytes, kRegionSize);
    EXPECT_EQ(pool.GetMemoryUsage().capacity_bytes, 0U);
}

TEST(SegmentPoolTest, RejectedAdoptionCommitDoesNotRegisterUsage) {
    ClientRegistry clients{false};
    SegmentPool pool(Drivers());
    const UUID client = generate_uuid();
    auto segment = MakeSegment(0, "adopt-rejected");
    auto allocator = std::make_shared<TestBufferAllocator>(
        segment.name, segment.te_endpoint, kRegionSize, segment.base);
    auto access = pool.AcquireWriteAccess();
    auto transaction = SegmentPoolTestPeer::PrepareAdopt(
        access, {segment, client, SegmentStatus::OK}, allocator, true);
    ASSERT_TRUE(transaction.has_value());
    ASSERT_EQ(access.MountSegment(segment, clients.GetOrCreate(client)),
              ErrorCode::OK);
    EXPECT_EQ(transaction->Commit(access), ErrorCode::SEGMENT_ALREADY_EXISTS);
    access.Clear();
    EXPECT_EQ(pool.GetMemoryUsage().capacity_bytes, 0U);
}

TEST(SegmentPoolTest, AdoptRejectsCatalogConflictWithoutReplacingResource) {
    ClientRegistry clients{false};
    SegmentPool pool(Drivers());
    const UUID client = generate_uuid();
    auto segment = MakeSegment(0, "adopt-conflict");
    auto access = pool.AcquireWriteAccess();
    ASSERT_EQ(access.MountSegment(segment, clients.GetOrCreate(client)),
              ErrorCode::OK);
    auto allocator = std::make_shared<TestBufferAllocator>(
        segment.name, segment.te_endpoint, kRegionSize, segment.base);
    auto duplicate = SegmentPoolTestPeer::PrepareAdopt(
        access, {segment, client, SegmentStatus::OK}, allocator, true);
    ASSERT_FALSE(duplicate.has_value());
    EXPECT_EQ(duplicate.error(), ErrorCode::SEGMENT_ALREADY_EXISTS);
    auto conflict = SegmentPoolTestPeer::PrepareAdopt(
        access, {segment, generate_uuid(), SegmentStatus::OK}, allocator, true);
    ASSERT_FALSE(conflict.has_value());
    EXPECT_EQ(conflict.error(), ErrorCode::SEGMENT_ALREADY_EXISTS);
    EXPECT_EQ(access.Segments().size(), 1U);
}

TEST(SegmentPoolTest, SnapshotCaptureDoesNotAcquireInheritedPoolMutex) {
    ClientRegistry clients{false};
    SegmentPool pool(Drivers());
    const UUID client = generate_uuid();
    auto segment = MakeSegment(0, "fork-safe");
    {
        auto access = pool.AcquireWriteAccess();
        ASSERT_EQ(access.MountSegment(segment, clients.GetOrCreate(client)),
                  ErrorCode::OK);

        // A fork child can inherit this mutex as locked by a vanished thread.
        // Capture must not acquire the inherited runtime lock.
        auto encoded = CaptureAndEncode(pool, LocalSsdPersistedState{});
        ASSERT_TRUE(encoded.has_value()) << encoded.error().message;
    }
    CommitUnmount(pool, segment, client);
}

TEST(SegmentPoolTest, SnapshotRoundTripPreservesCatalogAndHost) {
    ClientRegistry clients{false};
    SegmentPool source(Drivers());
    const UUID client = generate_uuid();
    auto segment = MakeSegment(0, "snapshot", "tcp", "host-a");
    ASSERT_EQ(source.AcquireWriteAccess().MountSegment(
                  segment, clients.GetOrCreate(client)),
              ErrorCode::OK);
    auto encoded = CaptureAndEncode(source, LocalSsdPersistedState{});
    ASSERT_TRUE(encoded.has_value()) << encoded.error().message;

    SegmentPool restored(Drivers());
    auto stale = MakeSegment(1, "stale");
    ASSERT_EQ(restored.AcquireWriteAccess().MountSegment(
                  stale, clients.GetOrCreate(client)),
              ErrorCode::OK);
    // Replacement must also allow an existing UUID and a different old owner.
    ASSERT_EQ(restored.AcquireWriteAccess().MountSegment(
                  segment, clients.GetOrCreate(generate_uuid())),
              ErrorCode::OK);
    auto decoded = DecodeAndRestore(restored, *encoded, false);
    ASSERT_TRUE(decoded.has_value()) << decoded.error().message;

    {
        auto view = restored.AcquireReadAccess();
        EXPECT_FALSE(view.FindSegment(stale.id).has_value());
        const auto mounted = view.FindSegment(segment.id);
        ASSERT_TRUE(mounted.has_value());
        EXPECT_EQ(mounted->segment.host_id, segment.host_id);
        EXPECT_EQ(mounted->client_id, client);
        EXPECT_EQ(mounted->status, SegmentStatus::OK);
    }

    restored.AcquireWriteAccess().Clear();
    CommitUnmount(source, segment, client);
}

TEST(SegmentPoolTest, SharedNameIndexesSurviveSnapshotAndReverseUnmount) {
    ClientRegistry clients{false};
    SegmentPool source(Drivers());
    const UUID client = generate_uuid();
    auto first = MakeSegment(0, "shared-hostname", "tcp", "shared-host");
    auto second = MakeSegment(1, first.name, "tcp", first.host_id);
    second.te_endpoint = "second-endpoint";
    {
        auto access = source.AcquireWriteAccess();
        // Split registrations of one client retain the same logical name.
        ASSERT_EQ(access.MountSegment(first, clients.GetOrCreate(client)),
                  ErrorCode::OK);
        ASSERT_EQ(access.MountSegment(second, clients.GetOrCreate(client)),
                  ErrorCode::OK);
    }
    auto encoded = CaptureAndEncode(source, {});
    ASSERT_TRUE(encoded.has_value());
    SegmentPool restored(Drivers());
    ASSERT_TRUE(DecodeAndRestore(restored, *encoded, false).has_value());
    ASSERT_TRUE(restored.RestoreBufferBindings(clients.Snapshot(), {}));
    {
        auto access = restored.AcquireReadAccess();
        ASSERT_EQ(access.Segments().size(), 2U);
        for (const auto& segment : {first, second}) {
            const auto region = access.FindSegment(segment.id);
            ASSERT_TRUE(region.has_value());
            EXPECT_EQ(region->client_id, client);
            EXPECT_EQ(region->segment.te_endpoint, segment.te_endpoint);
        }
        const auto* entry = SegmentPoolTestPeer::Placement(access).Find(
            first.name, AllocationCandidateKind::NATIVE);
        ASSERT_NE(entry, nullptr);
        EXPECT_EQ(entry->candidates.size(), 2U);
    }

    CommitUnmount(restored, second, client);
    {
        auto access = restored.AcquireReadAccess();
        EXPECT_FALSE(access.FindSegment(second.id).has_value());
        EXPECT_FALSE(
            SegmentPoolTestPeer::Catalog(access).RegionIds(first.name).empty());
        EXPECT_EQ(access.FindOwnerClientId(first.name), client);
        const auto* entry = SegmentPoolTestPeer::Placement(access).Find(
            first.name, AllocationCandidateKind::NATIVE);
        ASSERT_NE(entry, nullptr);
        EXPECT_EQ(entry->candidates.size(), 1U);
        std::vector<std::string> names;
        SegmentPoolTestPeer::Placement(access).VisitHostOrderedSegmentNames(
            first.host_id, "key", [&](auto name) {
                names.emplace_back(name);
                return false;
            });
        EXPECT_EQ(names, (std::vector<std::string>{first.name}));
    }
    auto allocated = SegmentPoolTestPeer::AllocateInSegment(
        restored, first.name, AllocationCandidateKind::NATIVE, 4096);
    ASSERT_TRUE(allocated.has_value());
    EXPECT_EQ(ReplicaEndpoint(*allocated), first.te_endpoint);

    CommitUnmount(restored, first, client);
    auto access = restored.AcquireReadAccess();
    EXPECT_TRUE(access.Segments().empty());
    EXPECT_FALSE(access.FindOwnerClientId(first.name).has_value());
    EXPECT_FALSE(SegmentPoolTestPeer::Placement(access).Contains(
        first.name, AllocationCandidateKind::NATIVE));
    std::vector<std::string> names;
    SegmentPoolTestPeer::Placement(access).VisitHostOrderedSegmentNames(
        first.host_id, "key", [&](auto name) {
            names.emplace_back(name);
            return false;
        });
    EXPECT_TRUE(names.empty());
}

TEST(SegmentPoolTest, SnapshotPreservesSsdPlacementOwnerRanking) {
    ClientRegistry clients{false};
    SegmentPool source(Drivers());
    LocalSsdManager local_ssd;
    auto busy = MakeSegment(0, "busy");
    auto free = MakeSegment(1, "free");
    const UUID busy_owner = generate_uuid();
    const UUID free_owner = generate_uuid();
    ASSERT_EQ(source.AcquireWriteAccess().MountSegment(
                  busy, clients.GetOrCreate(busy_owner)),
              ErrorCode::OK);
    ASSERT_EQ(source.AcquireWriteAccess().MountSegment(
                  free, clients.GetOrCreate(free_owner)),
              ErrorCode::OK);
    for (const auto& owner : {busy_owner, free_owner}) {
        ASSERT_EQ(local_ssd.RegisterClient(owner, true), ErrorCode::OK);
        ASSERT_TRUE(local_ssd.ReportCapacity(owner, 1000).has_value());
    }
    ASSERT_TRUE(local_ssd.AdjustUsedBytes(busy_owner, 900));
    ASSERT_TRUE(local_ssd.AdjustUsedBytes(free_owner, 100));

    auto encoded = CaptureAndEncode(source, local_ssd.ExportPersistedState());
    ASSERT_TRUE(encoded.has_value());
    SegmentPool restored(Drivers(), PlacementPolicyType::SSD_FREE_RATIO_FIRST,
                         &local_ssd);
    ASSERT_TRUE(DecodeAndRestore(restored, *encoded, false).has_value());
    ASSERT_TRUE(restored.RestoreBufferBindings(clients.Snapshot(), {}));
    // SSD usage is a live metrics view. Restored placement must look it up by
    // the preserved owner rather than by segment name or insertion order.
    ReplicaAllocationRequest request;
    request.replicas.size = 4096;
    auto allocated = restored.AllocateReplicas(request);
    ASSERT_TRUE(allocated.has_value());
    ASSERT_EQ(allocated->size(), 1U);
    EXPECT_EQ(ReplicaEndpoint(allocated->front()), free.te_endpoint);
    {
        auto access = restored.AcquireReadAccess();
        EXPECT_EQ(access.FindOwnerClientId(busy.name), busy_owner);
        EXPECT_EQ(access.FindOwnerClientId(free.name), free_owner);
    }
}

TEST(SegmentPoolTest, RestoreSnapshotRejectsConflictsBeforeReplacingPool) {
    ClientRegistry clients{false};
    SegmentPool pool(Drivers());
    const auto segment = MakeSegment(0, "restore-conflict");
    ASSERT_EQ(pool.AcquireWriteAccess().MountSegment(
                  segment, clients.GetOrCreate(generate_uuid())),
              ErrorCode::OK);
    auto allocator =
        SegmentPoolTestPeer::GetAllocator(pool.AcquireReadAccess(), segment.id);
    auto snapshot = pool.CaptureSnapshot();
    auto duplicate = pool.CaptureSnapshot();
    ASSERT_TRUE(snapshot);
    ASSERT_TRUE(duplicate);
    snapshot->regions.push_back(std::move(duplicate->regions.front()));
    EXPECT_FALSE(pool.RestoreSnapshot(std::move(*snapshot), true));
    EXPECT_EQ(
        SegmentPoolTestPeer::GetAllocator(pool.AcquireReadAccess(), segment.id),
        allocator);

    auto invalid = pool.CaptureSnapshot();
    ASSERT_TRUE(invalid);
    invalid->regions.front().allocator.allocation_state.layout.reset();
    EXPECT_FALSE(pool.RestoreSnapshot(std::move(*invalid), true));
    EXPECT_EQ(
        SegmentPoolTestPeer::GetAllocator(pool.AcquireReadAccess(), segment.id),
        allocator);
}

TEST(SegmentPoolTest, SnapshotReplacementPreservesAllocatedMetrics) {
    ClientRegistry clients{false};
    auto& metrics = MasterMetricManager::instance();
    const auto baseline = metrics.get_allocated_mem_size();

    for (bool account_capacity : {false, true}) {
        SegmentPool pool(Drivers());
        const UUID client = generate_uuid();
        auto segment = MakeSegment(0, "snapshot-allocated-metrics");
        ASSERT_EQ(pool.AcquireWriteAccess().MountSegment(
                      segment, clients.GetOrCreate(client)),
                  ErrorCode::OK);
        auto buffer = SegmentPoolTestPeer::GetAllocator(
                          pool.AcquireReadAccess(), segment.id)
                          ->allocate(4096);
        ASSERT_NE(buffer, nullptr);
        auto encoded = CaptureAndEncode(pool, LocalSsdPersistedState{});
        ASSERT_TRUE(encoded.has_value()) << encoded.error().message;

        auto decoded = DecodeAndRestore(pool, *encoded, account_capacity);
        ASSERT_TRUE(decoded.has_value()) << decoded.error().message;
        buffer.reset();  // The replaced allocator is no longer live.
        EXPECT_EQ(metrics.get_allocated_mem_size(), baseline + 4096);
        EXPECT_EQ(metrics.get_segment_allocated_mem_size(segment.name), 4096);
        EXPECT_EQ(metrics.get_segment_total_mem_capacity(segment.name),
                  account_capacity ? kRegionSize : 0);
        {
            auto next = SegmentPoolTestPeer::GetAllocator(
                            pool.AcquireReadAccess(), segment.id)
                            ->allocate(4096);
            ASSERT_NE(next, nullptr);
            EXPECT_EQ(metrics.get_segment_allocated_mem_size(segment.name),
                      8192);
        }
        EXPECT_EQ(metrics.get_segment_allocated_mem_size(segment.name), 4096);
        pool.AcquireWriteAccess().Clear();
        EXPECT_EQ(metrics.get_allocated_mem_size(), baseline);
        EXPECT_EQ(metrics.get_segment_allocated_mem_size(segment.name), 0);
    }
}

TEST(SegmentPoolTest, SnapshotRestoresPlacementAndEveryLifecycleStatus) {
    ClientRegistry clients{false};
    SegmentPool source(Drivers());
    const UUID client = generate_uuid();
    std::vector<Segment> segments;
    const std::vector<SegmentStatus> statuses{
        SegmentStatus::OK,        SegmentStatus::OK,
        SegmentStatus::UNDEFINED, SegmentStatus::DRAINING,
        SegmentStatus::DRAINED,   SegmentStatus::GRACEFULLY_UNMOUNTING,
        SegmentStatus::UNMOUNTING};
    for (size_t i = 0; i < statuses.size(); ++i) {
        segments.push_back(MakeSegment(i, "lifecycle-" + std::to_string(i),
                                       "tcp", "host-" + std::to_string(i)));
        auto access = source.AcquireWriteAccess();
        ASSERT_EQ(access.MountSegment(
                      segments.back(),
                      clients.GetOrCreate(i == 1 ? UUID{8, 9} : client)),
                  ErrorCode::OK);
        ASSERT_EQ(SegmentPoolTestPeer::SetSegmentStatusByName(
                      access, segments.back().name, statuses[i]),
                  ErrorCode::OK);
    }

    // Same-name regions must all survive; only their logical name is
    // deduplicated.
    auto sibling = MakeSegment(8, segments[0].name, "tcp", segments[0].host_id);
    ASSERT_EQ(source.AcquireWriteAccess().MountSegment(
                  sibling, clients.GetOrCreate(client)),
              ErrorCode::OK);
    auto encoded = CaptureAndEncode(source, {});
    ASSERT_TRUE(encoded.has_value());
    SegmentPool restored(Drivers());
    ASSERT_TRUE(DecodeAndRestore(restored, *encoded, false).has_value());
    ASSERT_TRUE(restored.RestoreBufferBindings(clients.Snapshot(), {}));
    {
        auto access = restored.AcquireReadAccess();
        EXPECT_EQ(access.Segments().size(), segments.size() + 1);
        for (size_t i = 0; i < segments.size(); ++i) {
            const auto region = access.FindSegment(segments[i].id);
            ASSERT_TRUE(region.has_value());
            EXPECT_EQ(region->status, statuses[i]);
            EXPECT_EQ(region->client_id, i == 1 ? (UUID{8, 9}) : client);
            EXPECT_EQ(SegmentPoolTestPeer::Placement(access).Contains(
                          segments[i].name, AllocationCandidateKind::NATIVE),
                      statuses[i] == SegmentStatus::OK);
            EXPECT_NE(SegmentPoolTestPeer::GetAllocator(access, segments[i].id),
                      nullptr);
        }
        std::vector<std::string> names;
        access.GetActiveSegmentNames(names);
        EXPECT_EQ(names, (std::vector<std::string>{segments[0].name,
                                                   segments[1].name}));
        std::vector<std::string> by_host;
        SegmentPoolTestPeer::Placement(access).VisitHostOrderedSegmentNames(
            segments[1].host_id, "key", [&](auto name) {
                by_host.emplace_back(name);
                return false;
            });
        ASSERT_EQ(by_host.size(), 2U);
        EXPECT_EQ(by_host.front(), segments[1].name);
        const auto* entry = SegmentPoolTestPeer::Placement(access).Find(
            segments[0].name, AllocationCandidateKind::NATIVE);
        ASSERT_NE(entry, nullptr);
        EXPECT_EQ(entry->candidates.size(), 2U);
    }

    CommitUnmount(restored, segments[0], client);
    EXPECT_TRUE(SegmentPoolTestPeer::Placement(restored.AcquireReadAccess())
                    .Contains(sibling.name, AllocationCandidateKind::NATIVE));
    CommitUnmount(restored, sibling, client);
    EXPECT_FALSE(SegmentPoolTestPeer::Placement(restored.AcquireReadAccess())
                     .Contains(sibling.name, AllocationCandidateKind::NATIVE));
    auto buffer = SegmentPoolTestPeer::GetAllocator(
                      restored.AcquireReadAccess(), segments[1].id)
                      ->allocate(4096);
    ASSERT_NE(buffer, nullptr);
}

TEST(SegmentPoolTest,
     SnapshotPreservesFragmentationAndLegacyAllocationHandles) {
    ClientRegistry clients{false};
    SegmentPool source(Drivers());
    const UUID client = generate_uuid();
    const auto segment = MakeSegment(0, "snapshot-handles");
    ASSERT_EQ(source.AcquireWriteAccess().MountSegment(
                  segment, clients.GetOrCreate(client)),
              ErrorCode::OK);

    auto allocator = std::dynamic_pointer_cast<OffsetBufferAllocator>(
        SegmentPoolTestPeer::GetAllocator(source.AcquireReadAccess(),
                                          segment.id));
    ASSERT_NE(allocator, nullptr);
    auto first = allocator->allocate(4096);
    auto hole = allocator->allocate(4096);
    auto last = allocator->allocate(4096);
    ASSERT_NE(first, nullptr);
    ASSERT_NE(hole, nullptr);
    ASSERT_NE(last, nullptr);
    hole.reset();

    // Preserve the historical AllocatedBuffer/OffsetAllocationHandle wire
    // shapes independently of the deleted SegmentSerializer. Saved handles
    // refer to the original node IDs, not a freshly reconstructed layout.
    std::map<uint64_t, uint32_t> nodes;
    allocator->getOffsetAllocator()->visit_used_nodes(
        [&](uint64_t address, uint64_t, uint32_t index) {
            nodes.emplace(address, index);
        });
    ASSERT_EQ(nodes.size(), 2U);
    const auto layout = allocator->getOffsetAllocator()->CaptureSnapshot();
    msgpack::sbuffer handles;
    MsgpackPacker packer(&handles);
    packer.pack_array(2);
    for (const auto* buffer : {first.get(), last.get()}) {
        const auto address = reinterpret_cast<uint64_t>(buffer->data());
        packer.pack_array(5);
        packer.pack(static_cast<uint64_t>(buffer->size()));
        packer.pack(address);
        packer.pack(UuidToString(segment.id));
        packer.pack(true);
        packer.pack_array(3);
        packer.pack(address);
        packer.pack(static_cast<uint64_t>(buffer->size()));
        packer.pack_array(2);
        packer.pack(static_cast<uint32_t>((address - segment.base) >>
                                          layout.multiplier_bits));
        packer.pack(nodes.at(address));
    }

    auto encoded = CaptureAndEncode(source, {});
    ASSERT_TRUE(encoded.has_value());
    SegmentPool restored(Drivers());
    ASSERT_TRUE(DecodeAndRestore(restored, *encoded, false).has_value());
    auto target = std::dynamic_pointer_cast<OffsetBufferAllocator>(
        SegmentPoolTestPeer::GetAllocator(restored.AcquireReadAccess(),
                                          segment.id));
    ASSERT_NE(target, nullptr);
    EXPECT_EQ(restored.GetMemoryUsage().used_bytes, 8192U);

    auto next = target->allocate(4096);
    ASSERT_NE(next, nullptr);
    EXPECT_NE(next->data(), first->data());
    EXPECT_NE(next->data(), last->data());
    next.reset();

    auto objects = msgpack::unpack(handles.data(), handles.size());
    for (uint32_t i = 0; i < 2; ++i) {
        auto buffer = Serializer<AllocatedBuffer>::deserialize(
            objects.get().via.array.ptr[i], restored.AcquireReadAccess());
        ASSERT_TRUE(buffer.has_value());
        buffer->reset();
        EXPECT_EQ(restored.GetMemoryUsage().used_bytes, (1U - i) * 4096U);
    }

    // Restored handles released and coalesced every block.
    auto all = target->allocate(kRegionSize);
    ASSERT_NE(all, nullptr);
}

TEST(SegmentPoolTest, SnapshotReadersAndEmptyReplacementBalanceMetrics) {
    ClientRegistry clients{false};
    auto& metrics = MasterMetricManager::instance();
    SegmentPool source(Drivers());
    const auto segment = MakeSegment(0, "snapshot-reader");
    ASSERT_EQ(source.AcquireWriteAccess().MountSegment(
                  segment, clients.GetOrCreate(generate_uuid())),
              ErrorCode::OK);
    auto live = SegmentPoolTestPeer::GetAllocator(source.AcquireReadAccess(),
                                                  segment.id)
                    ->allocate(4096);
    ASSERT_NE(live, nullptr);
    auto encoded = CaptureAndEncode(source, {});
    ASSERT_TRUE(encoded.has_value());

    SegmentPool empty(Drivers());
    auto empty_encoded = CaptureAndEncode(empty, {});
    ASSERT_TRUE(empty_encoded.has_value());
    const auto baseline_used = metrics.get_allocated_mem_size();
    const auto baseline_capacity = metrics.get_total_mem_capacity();

    for (bool account_capacity : {false, true}) {
        for (bool clear_with_empty : {false, true}) {
            {
                SegmentPool reader(Drivers());
                for (int i = 0; i < 3; ++i) {
                    ASSERT_TRUE(
                        DecodeAndRestore(reader, *encoded, account_capacity)
                            .has_value());
                    EXPECT_EQ(reader.GetMemoryUsage().used_bytes, 4096U);
                    EXPECT_EQ(reader.GetMemoryUsage().capacity_bytes,
                              kRegionSize);
                    EXPECT_EQ(metrics.get_allocated_mem_size(),
                              baseline_used + 4096);
                    EXPECT_EQ(metrics.get_total_mem_capacity(),
                              baseline_capacity +
                                  (account_capacity ? kRegionSize : 0));
                }

                if (clear_with_empty) {
                    ASSERT_TRUE(DecodeAndRestore(reader, *empty_encoded,
                                                 account_capacity)
                                    .has_value());
                    EXPECT_TRUE(reader.AcquireReadAccess().Segments().empty());
                    EXPECT_EQ(reader.GetMemoryUsage().used_bytes, 0U);
                    EXPECT_EQ(reader.GetMemoryUsage().capacity_bytes, 0U);
                    EXPECT_EQ(metrics.get_allocated_mem_size(), baseline_used);
                }
            }
            EXPECT_EQ(metrics.get_allocated_mem_size(), baseline_used);
            EXPECT_EQ(metrics.get_total_mem_capacity(), baseline_capacity);
            EXPECT_EQ(metrics.get_segment_allocated_mem_size(segment.name),
                      4096);
            EXPECT_EQ(metrics.get_segment_total_mem_capacity(segment.name),
                      kRegionSize);
        }
    }
}

TEST(SegmentPoolTest, SnapshotReplacementBalancesExternallyRetainedAllocator) {
    ClientRegistry clients{false};
    auto& metrics = MasterMetricManager::instance();
    const auto baseline = metrics.get_allocated_mem_size();
    SegmentPool pool(Drivers());
    const auto segment = MakeSegment(0, "snapshot-retained");
    ASSERT_EQ(pool.AcquireWriteAccess().MountSegment(
                  segment, clients.GetOrCreate(generate_uuid())),
              ErrorCode::OK);
    auto retained =
        SegmentPoolTestPeer::GetAllocator(pool.AcquireReadAccess(), segment.id);
    auto old_buffer = retained->allocate(4096);
    ASSERT_NE(old_buffer, nullptr);
    auto encoded = CaptureAndEncode(pool, {});
    ASSERT_TRUE(encoded.has_value());
    ASSERT_TRUE(DecodeAndRestore(pool, *encoded, true).has_value());
    EXPECT_NE(
        SegmentPoolTestPeer::GetAllocator(pool.AcquireReadAccess(), segment.id),
        retained);
    EXPECT_EQ(metrics.get_allocated_mem_size(), baseline + 8192);
    EXPECT_EQ(metrics.get_segment_allocated_mem_size(segment.name), 8192);

    // Clearing the replacement must not erase the old allocator's usage label.
    pool.AcquireWriteAccess().Clear();
    EXPECT_EQ(metrics.get_segment_total_mem_capacity(segment.name), 0);
    EXPECT_EQ(metrics.get_segment_allocated_mem_size(segment.name), 4096);

    old_buffer.reset();
    retained.reset();
    EXPECT_EQ(metrics.get_segment_allocated_mem_size(segment.name), 0);
    EXPECT_EQ(metrics.get_allocated_mem_size(), baseline);
    EXPECT_EQ(pool.GetMemoryUsage().used_bytes, 0U);
}

TEST(SegmentPoolTest, DestroyedPoolAndRetainedAllocatorRemoveMetricLabels) {
    ClientRegistry clients{false};
    auto& metrics = MasterMetricManager::instance();
    for (bool retain_allocator : {false, true}) {
        SCOPED_TRACE(retain_allocator);
        const std::string name = "snapshot-label-cleanup";
        const std::string label = "segment=\"" + name + "\"";
        std::shared_ptr<BufferAllocatorBase> retained;
        {
            SegmentPool source(Drivers());
            auto segment = MakeSegment(0, name);
            ASSERT_EQ(source.AcquireWriteAccess().MountSegment(
                          segment, clients.GetOrCreate(generate_uuid())),
                      ErrorCode::OK);
            auto live = SegmentPoolTestPeer::GetAllocator(
                            source.AcquireReadAccess(), segment.id)
                            ->allocate(4096);
            ASSERT_NE(live, nullptr);
            auto encoded = CaptureAndEncode(source, {});
            ASSERT_TRUE(encoded.has_value());
            SegmentPool restored(Drivers());
            ASSERT_TRUE(DecodeAndRestore(restored, *encoded, true).has_value());
            if (retain_allocator) {
                retained = SegmentPoolTestPeer::GetAllocator(
                    restored.AcquireReadAccess(), segment.id);
            }
            EXPECT_NE(metrics.serialize_metrics().find(label),
                      std::string::npos);
        }
        if (retain_allocator) {
            EXPECT_EQ(metrics.get_segment_allocated_mem_size(name), 4096);
            EXPECT_NE(metrics.serialize_metrics().find(label),
                      std::string::npos);
        }
        retained.reset();
        EXPECT_EQ(metrics.get_segment_allocated_mem_size(name), 0);
        EXPECT_EQ(metrics.get_segment_total_mem_capacity(name), 0);
        EXPECT_EQ(metrics.serialize_metrics().find(label), std::string::npos);
    }
}

TEST(SegmentPoolTest, EmptySnapshotAllocatorKeepsOtherPoolsCapacityLabels) {
    ClientRegistry clients{false};
    SegmentPool source(Drivers());
    const auto segment = MakeSegment(0, "snapshot-shared-capacity");
    ASSERT_EQ(source.AcquireWriteAccess().MountSegment(
                  segment, clients.GetOrCreate(generate_uuid())),
              ErrorCode::OK);
    auto encoded = CaptureAndEncode(source, {});
    ASSERT_TRUE(encoded.has_value());
    auto& metrics = MasterMetricManager::instance();
    const auto baseline = metrics.get_total_mem_capacity();
    {
        SegmentPool reader(Drivers());
        ASSERT_TRUE(DecodeAndRestore(reader, *encoded, true).has_value());
        EXPECT_EQ(metrics.get_segment_total_mem_capacity(segment.name),
                  2 * kRegionSize);
        reader.AcquireWriteAccess().Clear();
    }
    EXPECT_EQ(metrics.get_total_mem_capacity(), baseline);
    EXPECT_EQ(metrics.get_segment_total_mem_capacity(segment.name),
              kRegionSize);
    source.AcquireWriteAccess().Clear();
    EXPECT_EQ(metrics.get_segment_total_mem_capacity(segment.name), 0);
}

TEST(SegmentPoolTest, ForkedSnapshotEncodesWhileAnotherThreadOwnsPoolLock) {
    ClientRegistry clients{false};
    SegmentPool pool(Drivers());
    const UUID client = generate_uuid();
    const auto segment = MakeSegment(0, "real-fork");
    ASSERT_EQ(pool.AcquireWriteAccess().MountSegment(
                  segment, clients.GetOrCreate(client)),
              ErrorCode::OK);
    auto live =
        SegmentPoolTestPeer::GetAllocator(pool.AcquireReadAccess(), segment.id)
            ->allocate(4096);
    ASSERT_NE(live, nullptr);

    int fds[2];
    ASSERT_EQ(pipe(fds), 0);
    std::promise<void> locked, release;
    auto released = release.get_future();
    std::thread holder([&] {
        auto access = pool.AcquireWriteAccess();
        locked.set_value();
        released.wait();
    });
    locked.get_future().wait();

    const pid_t child = fork();
    if (child == 0) {
        close(fds[0]);
        alarm(10);  // A lock regression must fail, not hang the test suite.
        auto encoded = CaptureAndEncode(pool, {});
        if (!encoded) {
            _exit(1);
        }

        size_t offset = 0;
        while (offset < encoded->size()) {
            const auto n = write(fds[1], encoded->data() + offset,
                                 encoded->size() - offset);
            if (n < 0 && errno == EINTR) {
                continue;
            }
            if (n <= 0) {
                _exit(2);
            }
            offset += static_cast<size_t>(n);
        }
        _exit(0);
    }

    close(fds[1]);
    release.set_value();
    holder.join();

    std::vector<uint8_t> bytes;
    uint8_t chunk[4096];
    ssize_t n;
    while ((n = read(fds[0], chunk, sizeof(chunk))) != 0) {
        if (n < 0 && errno == EINTR) {
            continue;
        }
        if (n < 0) {
            break;
        }
        bytes.insert(bytes.end(), chunk, chunk + n);
    }
    close(fds[0]);
    ASSERT_GT(child, 0);

    int status = 0;
    pid_t waited;
    do {
        waited = waitpid(child, &status, 0);
    } while (waited < 0 && errno == EINTR);
    ASSERT_EQ(waited, child);
    ASSERT_TRUE(WIFEXITED(status)) << status;
    ASSERT_EQ(WEXITSTATUS(status), 0);

    SegmentPool restored(Drivers());
    ASSERT_TRUE(DecodeAndRestore(restored, bytes, false).has_value());
    EXPECT_EQ(restored.GetMemoryUsage().used_bytes, 4096U);
    EXPECT_TRUE(
        restored.AcquireReadAccess().FindSegment(segment.id).has_value());
}

}  // namespace mooncake::test
