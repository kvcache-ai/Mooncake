#include "pool_transaction_internal.h"

#include "segment/snapshot.h"

#include <algorithm>
#include <set>

#include "segment/pool.h"
#include "segment/pool_read_access.h"
#include "segment/pool_write_access.h"

namespace mooncake {

tl::expected<BufferSnapshot, ErrorCode> SegmentPool::CaptureBufferSnapshot(
    const AllocatedBuffer& buffer) const {
    // Deliberately mutex-free: called from the forked snapshot child.
    const auto allocator = buffer.getAllocator();
    for (const auto& mounted : catalog_.Regions()) {
        const auto* resource = GetResource(mounted);
        if (mounted.kind == RegionKind::HOST_MEMORY && allocator && resource &&
            resource->allocator() == allocator) {
            return BufferSnapshot{
                mounted.segment.id, buffer.size(),
                reinterpret_cast<uintptr_t>(buffer.data()),
                buffer.offset_handle_
                    ? std::optional(buffer.offset_handle_->CaptureSnapshot())
                    : std::nullopt};
        }
    }
    return tl::unexpected(ErrorCode::SERIALIZE_FAIL);
}

tl::expected<BufferSnapshot, ErrorCode>
SegmentPool::ReadAccess::CaptureBufferSnapshot(
    const AllocatedBuffer& buffer) const {
    // This guard already holds the runtime read lock. Reuse the mutex-free
    // resource projection used by the forked snapshot child.
    return pool_.CaptureBufferSnapshot(buffer);
}

tl::expected<std::unique_ptr<AllocatedBuffer>, ErrorCode>
SegmentPool::ReadAccess::RestoreBuffer(const BufferSnapshot& snapshot) const {
    const auto* mounted = catalog_.Find(snapshot.region_id);
    const auto* resource = mounted ? pool_.GetResource(*mounted) : nullptr;
    if (!resource || mounted->kind != RegionKind::HOST_MEMORY)
        return tl::unexpected(ErrorCode::DESERIALIZE_FAIL);
    const auto& segment = mounted->segment;
    const auto& allocation = snapshot.allocation;
    if (!allocation || snapshot.size == 0 || snapshot.address < segment.base ||
        snapshot.address - segment.base >= segment.size ||
        snapshot.size > segment.size - (snapshot.address - segment.base) ||
        allocation->address != snapshot.address ||
        allocation->size != snapshot.size)
        return tl::unexpected(ErrorCode::DESERIALIZE_FAIL);
    auto allocator =
        std::dynamic_pointer_cast<OffsetBufferAllocator>(resource->allocator());
    if (!allocator) return tl::unexpected(ErrorCode::DESERIALIZE_FAIL);
    auto handle = allocator->getOffsetAllocator()->RestoreHandle(*allocation);
    if (!handle) return tl::unexpected(ErrorCode::DESERIALIZE_FAIL);
    auto buffer = std::make_unique<AllocatedBuffer>(
        allocator, reinterpret_cast<void*>(snapshot.address), snapshot.size,
        std::move(handle));
    resource->candidate->BindBuffer(*buffer);
    return buffer;
}

tl::expected<SegmentPoolSnapshot, ErrorCode> SegmentPool::CaptureSnapshot()
    const {
    // Pending external acknowledgements and name reservations are deliberately
    // process-local. Never produce a snapshot that silently drops them.
    if (!unmounts_.empty()) {
        return tl::unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    }
    const auto* driver = GetDriver(RegionKind::HOST_MEMORY);
    if (!driver || driver->allocator_type() != BufferAllocatorType::OFFSET) {
        return tl::make_unexpected(ErrorCode::SERIALIZE_UNSUPPORTED);
    }
    // Validate supported resources before copying any allocator layouts.
    for (const auto& mounted : catalog_.Regions()) {
        if (mounted.kind != RegionKind::HOST_MEMORY) {
            return tl::make_unexpected(ErrorCode::SERIALIZE_UNSUPPORTED);
        }
    }

    SegmentPoolSnapshot snapshot;
    placement_index_.GetActiveSegmentNames(AllocationCandidateKind::NATIVE,
                                           snapshot.active_names);
    snapshot.regions.reserve(catalog_.Regions().size());
    for (const auto& mounted : catalog_.Regions()) {
        const auto* resource = GetResource(mounted);
        if (!resource || !resource->allocator()) {
            return tl::make_unexpected(ErrorCode::SERIALIZE_FAIL);
        }
        const auto* allocator = dynamic_cast<const OffsetBufferAllocator*>(
            resource->allocator().get());
        if (!allocator) {
            return tl::make_unexpected(ErrorCode::SERIALIZE_UNSUPPORTED);
        }
        snapshot.regions.push_back({mounted, allocator->CaptureSnapshot()});
    }
    std::sort(snapshot.regions.begin(), snapshot.regions.end(),
              [](const auto& lhs, const auto& rhs) {
                  return lhs.mounted.segment.id < rhs.mounted.segment.id;
              });
    return snapshot;
}

tl::expected<void, ErrorCode> SegmentPool::RestoreSnapshot(
    SegmentPoolSnapshot snapshot, bool account_capacity_metrics) {
    auto access = AcquireWriteAccess();
    const auto* driver = GetDriver(RegionKind::HOST_MEMORY);
    if (!driver ||
        snapshot.memory_allocator_type != BufferAllocatorType::OFFSET ||
        driver->allocator_type() != snapshot.memory_allocator_type) {
        return tl::make_unexpected(ErrorCode::DESERIALIZE_FAIL);
    }
    std::set<UUID> ids;
    std::set<std::string> active_names;
    for (const auto& region : snapshot.regions) {
        const auto& mounted = region.mounted;
        if (!IsKnownSegmentStatus(mounted.status)) {
            return tl::make_unexpected(ErrorCode::DESERIALIZE_FAIL);
        }
        if (!ids.insert(mounted.segment.id).second ||
            mounted.kind != RegionKind::HOST_MEMORY ||
            region.allocator.segment_name != mounted.segment.name ||
            region.allocator.base != mounted.segment.base ||
            region.allocator.capacity != mounted.segment.size ||
            region.allocator.transport_endpoint !=
                mounted.segment.te_endpoint) {
            return tl::make_unexpected(ErrorCode::DESERIALIZE_FAIL);
        }
        if (mounted.status == SegmentStatus::OK)
            active_names.insert(mounted.segment.name);
    }
    if (active_names != std::set<std::string>(snapshot.active_names.begin(),
                                              snapshot.active_names.end()) ||
        active_names.size() != snapshot.active_names.size()) {
        return tl::make_unexpected(ErrorCode::DESERIALIZE_FAIL);
    }
    std::vector<RegionMountTxn> prepared;
    prepared.reserve(snapshot.regions.size());
    // Preserve the active-name order even for snapshots captured in ID order.
    auto prepare =
        [&](RegionSnapshot& region) -> tl::expected<void, ErrorCode> {
        auto allocator =
            OffsetBufferAllocator::Restore(std::move(region.allocator));
        if (!allocator) return tl::make_unexpected(allocator.error());
        auto txn = access.PrepareAdopt(region.mounted, std::move(*allocator),
                                       account_capacity_metrics,
                                       WriteAccess::AdoptMode::ReplacePool);
        if (!txn) return tl::make_unexpected(txn.error());
        prepared.push_back(std::move(*txn));
        return {};
    };
    for (const auto& name : snapshot.active_names) {
        for (auto& region : snapshot.regions) {
            if (region.mounted.status == SegmentStatus::OK &&
                region.mounted.segment.name == name) {
                auto result = prepare(region);
                if (!result) return result;
            }
        }
    }
    for (auto& region : snapshot.regions) {
        if (region.mounted.status != SegmentStatus::OK) {
            auto result = prepare(region);
            if (!result) return result;
        }
    }
    access.Clear();
    for (auto& txn : prepared) {
        const auto result = txn.Commit(access);
        if (result != ErrorCode::OK) return tl::make_unexpected(result);
    }
    return {};
}

}  // namespace mooncake
