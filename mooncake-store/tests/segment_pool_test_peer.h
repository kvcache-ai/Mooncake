#pragma once

#include "../src/segment/pool_transaction_internal.h"

#include "segment/pool_read_access.h"
#include "client_registry.h"
#include "segment/pool_write_access.h"

namespace mooncake {

// White-box operations borrow the caller's guard for the duration of the call.
// References returned by Catalog/Placement require that guard to remain alive.
class SegmentPoolTestPeer {
   public:
    static ClientSessionPtr TestSession(SegmentPool::WriteAccess& access,
                                        const UUID& id, const UUID& client) {
        if (const auto* mounted = access.catalog_.Find(id)) {
            if (const auto* resource =
                    access.segment_pool_.GetResource(*mounted)) {
                auto session = resource->candidate->client_session();
                if (session && session->client_id() == client) return session;
            }
        }
        ClientRegistry clients(false);
        return clients.GetOrCreate(client);
    }
    static bool IsNameReserved(const SegmentPool::WriteAccess& access,
                               std::string_view name) {
        return access.IsNameReserved(name);
    }
    static auto AllocateInSegment(const SegmentPool& pool,
                                  std::string_view name,
                                  AllocationCandidateKind kind, size_t size,
                                  ReplicaType type = ReplicaType::MEMORY) {
        return pool.AllocateInSegment(name, kind, size, type);
    }
    static const RegionCatalog& Catalog(const SegmentPool::ReadAccess& access) {
        return access.catalog_;
    }
    static const RegionCatalog& Catalog(
        const SegmentPool::WriteAccess& access) {
        return access.catalog_;
    }
    static const PlacementIndex& Placement(
        const SegmentPool::ReadAccess& access) {
        return access.pool_.placement_index_;
    }
    static std::shared_ptr<BufferAllocatorBase> GetAllocator(
        const SegmentPool::ReadAccess& access, const UUID& id) {
        return access.GetAllocator(id);
    }
    static auto PrepareMount(SegmentPool::WriteAccess& access,
                             const Segment& segment, const UUID& client) {
        auto txn = access.PrepareMount(segment, client);
        if (txn)
            txn->BindClientSession(TestSession(access, segment.id, client));
        return txn;
    }
    static auto PrepareRestore(
        SegmentPool::WriteAccess& access, const Segment& segment,
        const UUID& client,
        std::span<const AllocatedBuffer::Descriptor> descriptors) {
        auto txn = access.PrepareRestore(segment, client, descriptors);
        if (txn)
            txn->BindClientSession(TestSession(access, segment.id, client));
        return txn;
    }
    static auto PrepareAdopt(SegmentPool::WriteAccess& access,
                             MountedRegion mounted,
                             std::shared_ptr<BufferAllocatorBase> allocator,
                             bool account_capacity_metrics,
                             SegmentPool::WriteAccess::AdoptMode mode =
                                 SegmentPool::WriteAccess::AdoptMode::Insert) {
        auto session =
            TestSession(access, mounted.segment.id, mounted.client_id);
        auto txn = access.PrepareAdopt(std::move(mounted), std::move(allocator),
                                       account_capacity_metrics, mode);
        if (txn) txn->BindClientSession(std::move(session));
        return txn;
    }
    static auto PrepareUnmount(SegmentPool::WriteAccess& access, const UUID& id,
                               const UUID& client) {
        return access.PrepareUnmount(id, client);
    }
    static auto PrepareGracefulUnmount(SegmentPool::WriteAccess& access,
                                       const UUID& id, const UUID& client) {
        return access.PrepareGracefulUnmount(id, client);
    }
    static ErrorCode SetSegmentStatusByName(SegmentPool::WriteAccess& access,
                                            std::string_view name,
                                            SegmentStatus status) {
        return access.SetSegmentStatusByName(name, status);
    }
    static bool BindBufferToSegment(SegmentPool::WriteAccess& access,
                                    const UUID& id, AllocatedBuffer& buffer) {
        return access.BindBufferToSegment(id, buffer);
    }
    static bool RebindBufferToOwningSegment(SegmentPool::WriteAccess& access,
                                            AllocatedBuffer& buffer) {
        return access.RebindBufferToOwningSegment(buffer);
    }
};

}  // namespace mooncake
