#include "segment/snapshot.h"

#include <algorithm>
#include <map>
#include <set>

#include "segment/pool.h"
#include "segment/pool_write_access.h"

namespace mooncake {

tl::expected<SegmentPoolSnapshot, ErrorCode> SegmentPool::CaptureSnapshot()
    const {
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
    std::map<std::string, UUID> owners;
    std::set<std::string> active_names;
    for (const auto& region : snapshot.regions) {
        const auto& mounted = region.mounted;
        if (!IsKnownSegmentStatus(mounted.status)) {
            return tl::make_unexpected(ErrorCode::DESERIALIZE_FAIL);
        }
        auto [owner, inserted] =
            owners.emplace(mounted.segment.name, mounted.client_id);
        if (!ids.insert(mounted.segment.id).second ||
            (!inserted && owner->second != mounted.client_id) ||
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
