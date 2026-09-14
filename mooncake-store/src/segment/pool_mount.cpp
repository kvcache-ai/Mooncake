#include "segment/pool_write_access.h"

#include <algorithm>

#include "master_metric_manager.h"
#include "segment/pool.h"

namespace mooncake {
namespace {

RegionKind ClassifyRegion(const Segment& segment) {
    return segment.protocol == "cxl" ? RegionKind::CXL
                                     : RegionKind::HOST_MEMORY;
}

RegionResourceSpec MakeResourceSpec(const Segment& segment) {
    return RegionResourceSpec{segment.id, segment.name, segment.base,
                              segment.size, segment.te_endpoint};
}

}  // namespace

tl::expected<RegionMountTxn, ErrorCode> SegmentPool::WriteAccess::PrepareMount(
    const Segment& segment, const UUID& client_id) {
    return PrepareWithLiveAllocations(segment, client_id, {}, 0);
}

tl::expected<RegionMountTxn, ErrorCode>
SegmentPool::WriteAccess::PrepareRestore(
    const Segment& segment, const UUID& client_id,
    std::span<const AllocatedBuffer::Descriptor> descriptors) {
    if (std::any_of(descriptors.begin(), descriptors.end(),
                    [](const auto& descriptor) {
                        return descriptor.protocol_ == "cxl";
                    })) {
        return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_MODE);
    }
    auto live_allocations =
        BuildRegionLiveAllocations(MakeResourceSpec(segment), descriptors);
    if (!live_allocations) {
        return tl::make_unexpected(live_allocations.error());
    }
    uint64_t requested_bytes = 0;
    for (const auto& allocation : *live_allocations) {
        requested_bytes += allocation.requested_size;
    }
    return PrepareWithLiveAllocations(segment, client_id, *live_allocations,
                                      requested_bytes);
}

tl::expected<RegionMountTxn, ErrorCode>
SegmentPool::WriteAccess::PrepareWithLiveAllocations(
    const Segment& segment, const UUID& client_id,
    const std::vector<LiveAllocation>& live_allocations,
    uint64_t imported_requested_bytes) {
    const RegionKind kind = ClassifyRegion(segment);
    RegionDriver* driver = segment_pool_.GetDriver(kind);
    if (!driver) {
        return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_MODE);
    }

    auto existing = catalog_.Find(segment.id);
    const bool existed = existing != nullptr;
    std::weak_ptr<BufferAllocatorBase> previous_allocator;
    if (existed) {
        if (existing->status == SegmentStatus::UNMOUNTING) {
            return tl::make_unexpected(
                ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
        }
        if (existing->client_id != client_id || existing->kind != kind ||
            existing->segment != segment) {
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
        const auto* existing_resource = segment_pool_.GetResource(*existing);
        if (!existing_resource) {
            return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
        }
        previous_allocator = existing_resource->allocator();
    } else {
        auto owner = catalog_.FindOwnerClientId(segment.name);
        if (owner && *owner != client_id) {
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
    }

    auto resource =
        driver->PrepareOpen(MakeResourceSpec(segment), live_allocations);
    if (!resource) {
        return tl::make_unexpected(resource.error());
    }
    resource->resource().allocator()->AttachUsageTracker(
        segment_pool_.usage_tracker_);
    MountedRegion mounted{segment, client_id, SegmentStatus::OK, kind};
    if (existed) {
        mounted.status = existing->status;
    }
    return RegionMountTxn(TransactionKey{}, std::move(mounted), existed, true,
                          std::move(*resource), imported_requested_bytes,
                          std::move(previous_allocator));
}

tl::expected<RegionMountTxn, ErrorCode> SegmentPool::WriteAccess::PrepareAdopt(
    MountedRegion mounted, std::shared_ptr<BufferAllocatorBase> allocator,
    bool account_capacity_metrics) {
    mounted.kind = ClassifyRegion(mounted.segment);
    RegionDriver* driver = segment_pool_.GetDriver(mounted.kind);
    if (!driver) {
        return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_MODE);
    }
    if (catalog_.Find(mounted.segment.id)) {
        return tl::make_unexpected(ErrorCode::SEGMENT_ALREADY_EXISTS);
    }
    auto owner = catalog_.FindOwnerClientId(mounted.segment.name);
    if (owner && *owner != mounted.client_id) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    auto resource = driver->PrepareAdopt(MakeResourceSpec(mounted.segment),
                                         std::move(allocator));
    if (!resource) {
        return tl::make_unexpected(resource.error());
    }
    return RegionMountTxn(TransactionKey{}, std::move(mounted), false,
                          account_capacity_metrics, std::move(*resource));
}

ErrorCode RegionMountTxn::Commit(SegmentPool::WriteAccess& access) noexcept {
    if (committed_) {
        return ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS;
    }
    const auto result =
        access.PublishMount(mounted_, existed_, account_capacity_metrics_,
                            resource_, previous_allocator_);
    if (result == ErrorCode::OK) {
        committed_ = true;
    }
    return result;
}

ErrorCode SegmentPool::WriteAccess::PublishMount(
    const MountedRegion& mounted, bool existed, bool account_capacity_metrics,
    PreparedRegionResource& prepared,
    const std::weak_ptr<BufferAllocatorBase>& previous_allocator) noexcept {
    const auto* current = catalog_.Find(mounted.segment.id);
    if (existed) {
        if (!current || current->segment != mounted.segment ||
            current->client_id != mounted.client_id ||
            current->kind != mounted.kind ||
            current->status != mounted.status) {
            return ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS;
        }
        const auto* current_resource = segment_pool_.GetResource(*current);
        if (!current_resource ||
            current_resource->allocator() != previous_allocator.lock()) {
            return ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS;
        }
    } else {
        if (current) {
            return ErrorCode::SEGMENT_ALREADY_EXISTS;
        }
        const auto owner = catalog_.FindOwnerClientId(mounted.segment.name);
        if (owner && *owner != mounted.client_id) {
            return ErrorCode::INVALID_PARAMS;
        }
    }
    auto& new_resource = prepared.resource();
    if (existed && mounted.status == SegmentStatus::OK) {
        auto& old_resource = *segment_pool_.GetResource(mounted);
        prepared.Commit();
        const bool replaced = segment_pool_.placement_index_.ReplaceCandidate(
            mounted.segment.name, *old_resource.candidate,
            *new_resource.candidate, mounted.segment.host_id);
        DCHECK(replaced);
    } else {
        prepared.Commit();
        if (!existed) {
            const auto registered = catalog_.Register(mounted);
            DCHECK(registered == ErrorCode::OK);
        }
    }
    const auto transitioned =
        TransitionRegion(mounted, new_resource, mounted.status);
    DCHECK(transitioned == ErrorCode::OK);

    new_resource.allocator()->AttachUsageTracker(segment_pool_.usage_tracker_);

    if (account_capacity_metrics && mounted.kind == RegionKind::HOST_MEMORY &&
        segment_pool_.capacity_accounted_region_ids_.insert(mounted.segment.id)
            .second) {
        MasterMetricManager::instance().inc_total_mem_capacity(
            mounted.segment.name, mounted.segment.size);
    }
    return ErrorCode::OK;
}

ErrorCode SegmentPool::WriteAccess::MountSegment(const Segment& segment,
                                                 const UUID& client_id) {
    if (const auto* mounted = catalog_.Find(segment.id)) {
        return mounted->status == SegmentStatus::OK
                   ? ErrorCode::SEGMENT_ALREADY_EXISTS
                   : ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS;
    }
    auto prepared = PrepareMount(segment, client_id);
    if (!prepared) {
        return prepared.error();
    }
    return prepared->Commit(*this);
}

}  // namespace mooncake
