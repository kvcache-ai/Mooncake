#include "segment/pool_write_access.h"

#include <algorithm>
#include <utility>

#include "master_metric_manager.h"
#include "segment/pool.h"

namespace mooncake {
namespace {

RegionKind ClassifyRegion(const Segment& segment) {
    return segment.protocol == "cxl" ? RegionKind::CXL
                                     : RegionKind::HOST_MEMORY;
}

RegionResourceSpec MakeResourceSpec(
    const Segment& segment, ClientSessionSharedPtr owner_session = nullptr) {
    return RegionResourceSpec{segment.id,
                              segment.name,
                              segment.base,
                              segment.size,
                              segment.te_endpoint,
                              segment.protocol,
                              std::move(owner_session)};
}

bool HasUnmountingRegionWithName(const RegionCatalog& catalog,
                                 std::string_view name) {
    for (const auto& id : catalog.RegionIds(name)) {
        const auto* mounted = catalog.Find(id);
        if (mounted && mounted->status == SegmentStatus::UNMOUNTING) {
            return true;
        }
    }
    return false;
}

bool HasValidLocality(const Segment& segment) noexcept {
    return !segment.locality || segment.locality->numa_node >= 0;
}

bool DescribesSameSegment(const Segment& authoritative,
                          const Segment& candidate) {
    return authoritative.id == candidate.id &&
           authoritative.name == candidate.name &&
           authoritative.base == candidate.base &&
           authoritative.size == candidate.size &&
           authoritative.te_endpoint == candidate.te_endpoint &&
           authoritative.protocol == candidate.protocol &&
           authoritative.host_id == candidate.host_id &&
           (!authoritative.locality || !candidate.locality ||
            *authoritative.locality == *candidate.locality);
}

bool MatchesMountedRegion(const MountedRegion& mounted, const Segment& segment,
                          const UUID& client_id, RegionKind kind) {
    return mounted.client_id == client_id && mounted.kind == kind &&
           DescribesSameSegment(mounted.segment, segment);
}

ErrorCode CheckNewRegionName(const RegionCatalog& catalog,
                             std::string_view name, const UUID& client_id) {
    if (auto owner = catalog.FindOwnerClientId(name);
        owner && *owner != client_id) {
        return ErrorCode::INVALID_PARAMS;
    }
    if (HasUnmountingRegionWithName(catalog, name)) {
        return ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS;
    }
    return ErrorCode::OK;
}

}  // namespace

tl::expected<RegionMountTxn, ErrorCode> SegmentPool::WriteAccess::PrepareMount(
    const Segment& segment, ClientSessionSharedPtr owner_session) {
    return PrepareWithLiveAllocations(segment, std::move(owner_session), {}, 0);
}

tl::expected<RegionMountTxn, ErrorCode>
SegmentPool::WriteAccess::PrepareRestore(
    const Segment& segment, ClientSessionSharedPtr owner_session,
    std::span<const AllocatedBuffer::Descriptor> descriptors) {
    if (!owner_session) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
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
    return PrepareWithLiveAllocations(segment, std::move(owner_session),
                                      *live_allocations, requested_bytes);
}

tl::expected<RegionMountTxn, ErrorCode>
SegmentPool::WriteAccess::PrepareWithLiveAllocations(
    const Segment& segment, ClientSessionSharedPtr owner_session,
    const std::vector<LiveAllocation>& live_allocations,
    uint64_t imported_requested_bytes) {
    if (!owner_session || !HasValidLocality(segment)) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    const UUID client_id = owner_session->ClientId();
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
        if (!MatchesMountedRegion(*existing, segment, client_id, kind)) {
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
        const auto* existing_resource = segment_pool_.GetResource(*existing);
        if (!existing_resource) {
            return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
        }
        previous_allocator = existing_resource->allocator();
    } else {
        if (auto err = CheckNewRegionName(catalog_, segment.name, client_id);
            err != ErrorCode::OK) {
            return tl::make_unexpected(err);
        }
    }

    auto resource = driver->PrepareOpen(
        MakeResourceSpec(segment, owner_session), live_allocations);
    if (!resource) {
        return tl::make_unexpected(resource.error());
    }
    MountedRegion mounted{segment, client_id, SegmentStatus::OK,
                          kind,    0,         std::move(owner_session)};
    if (existed) {
        mounted.status = existing->status;
        if (!mounted.segment.locality && existing->segment.locality) {
            mounted.segment.locality = existing->segment.locality;
        }
    }
    resource->resource().allocator()->SetLocality(mounted.segment.locality);
    resource->resource().allocator()->AttachUsageTracker(
        segment_pool_.usage_tracker_);
    return RegionMountTxn(TransactionKey{}, std::move(mounted), existed, true,
                          std::move(*resource), imported_requested_bytes,
                          std::move(previous_allocator));
}

tl::expected<RegionMountTxn, ErrorCode> SegmentPool::WriteAccess::PrepareAdopt(
    MountedRegion mounted, std::shared_ptr<BufferAllocatorBase> allocator,
    bool account_capacity_metrics) {
    if (!HasValidLocality(mounted.segment)) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    mounted.kind = ClassifyRegion(mounted.segment);
    RegionDriver* driver = segment_pool_.GetDriver(mounted.kind);
    if (!driver) {
        return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_MODE);
    }
    if (catalog_.Find(mounted.segment.id)) {
        return tl::make_unexpected(ErrorCode::SEGMENT_ALREADY_EXISTS);
    }
    if (auto err = CheckNewRegionName(catalog_, mounted.segment.name,
                                      mounted.client_id);
        err != ErrorCode::OK) {
        return tl::make_unexpected(err);
    }
    auto resource = driver->PrepareAdopt(
        MakeResourceSpec(mounted.segment, mounted.owner_session),
        std::move(allocator));
    if (!resource) {
        return tl::make_unexpected(resource.error());
    }
    if (mounted.segment.locality) {
        resource->resource().allocator()->SetLocality(mounted.segment.locality);
    }
    return RegionMountTxn(TransactionKey{}, std::move(mounted), false,
                          account_capacity_metrics, std::move(*resource));
}

ErrorCode SegmentPool::WriteAccess::ValidateRemountSegment(
    const Segment& segment, const UUID& client_id) const {
    if (!HasValidLocality(segment)) {
        return ErrorCode::INVALID_PARAMS;
    }
    if (const auto* mounted = catalog_.Find(segment.id)) {
        return MatchesMountedRegion(*mounted, segment, client_id,
                                    ClassifyRegion(segment))
                   ? ErrorCode::OK
                   : ErrorCode::INVALID_PARAMS;
    }
    auto owner = catalog_.FindOwnerClientId(segment.name);
    if (owner && *owner != client_id) {
        return ErrorCode::INVALID_PARAMS;
    }
    return ErrorCode::OK;
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
        if (!current ||
            !MatchesMountedRegion(*current, mounted.segment, mounted.client_id,
                                  mounted.kind) ||
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
        if (auto err = CheckNewRegionName(catalog_, mounted.segment.name,
                                          mounted.client_id);
            err != ErrorCode::OK) {
            return err;
        }
    }
    auto& new_resource = prepared.resource();
    if (existed) {
        auto& old_resource = *segment_pool_.GetResource(mounted);
        old_resource.candidate->InvalidateLifetime();
        prepared.Commit();
        if (mounted.status == SegmentStatus::OK) {
            const bool replaced =
                segment_pool_.placement_index_.ReplaceCandidate(
                    mounted.segment.name, *old_resource.candidate,
                    *new_resource.candidate, mounted.segment.host_id);
            DCHECK(replaced);
        }
        if (current->segment.locality && !mounted.segment.locality) {
            new_resource.allocator()->SetLocality(current->segment.locality);
        } else if (!current->segment.locality && mounted.segment.locality) {
            const bool updated = catalog_.SetLocality(mounted.segment.id,
                                                      mounted.segment.locality);
            DCHECK(updated);
        }
        if (current->owner_session != mounted.owner_session) {
            const bool updated = catalog_.SetOwnerSession(
                mounted.segment.id, mounted.owner_session);
            DCHECK(updated);
        }
    } else {
        prepared.Commit();
        const auto registered = catalog_.Register(mounted);
        DCHECK(registered == ErrorCode::OK);
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

ErrorCode SegmentPool::WriteAccess::MountSegment(
    const Segment& segment, ClientSessionSharedPtr owner_session) {
    if (!owner_session || !HasValidLocality(segment)) {
        return ErrorCode::INVALID_PARAMS;
    }
    const UUID client_id = owner_session->ClientId();
    if (const auto* mounted = catalog_.Find(segment.id)) {
        if (!MatchesMountedRegion(*mounted, segment, client_id,
                                  ClassifyRegion(segment))) {
            return ErrorCode::INVALID_PARAMS;
        }
        if (mounted->status != SegmentStatus::OK) {
            return ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS;
        }
        if (!mounted->segment.locality && segment.locality) {
            const bool updated =
                catalog_.SetLocality(segment.id, segment.locality);
            DCHECK(updated);
            if (auto* resource = segment_pool_.GetResource(*mounted)) {
                resource->allocator()->SetLocality(segment.locality);
            }
        }
        return ErrorCode::SEGMENT_ALREADY_EXISTS;
    }
    auto prepared = PrepareMount(segment, std::move(owner_session));
    if (!prepared) {
        return prepared.error();
    }
    return prepared->Commit(*this);
}

}  // namespace mooncake
