#include "segment/pool_write_access.h"

#include <algorithm>
#include "master_metric_manager.h"
#include "segment/pool.h"

namespace mooncake {
SegmentPool::WriteAccess::WriteAccess(AccessKey, SegmentPool& segment_pool)
    : segment_pool_(segment_pool),
      lock_(segment_pool.pool_mutex_),
      catalog_(segment_pool.catalog_) {}

tl::expected<RegionUnmountTxn, ErrorCode>
SegmentPool::WriteAccess::PrepareUnmount(const UUID& segment_id,
                                         const UUID& client_id) {
    auto mounted = catalog_.Find(segment_id);
    if (mounted == nullptr) {
        return tl::make_unexpected(ErrorCode::SEGMENT_NOT_FOUND);
    }
    if (mounted->client_id != client_id) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    if (mounted->status == SegmentStatus::UNMOUNTING) {
        return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    }
    RegionResource* resource = segment_pool_.GetResource(*mounted);
    if (!resource) {
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }
    return RegionUnmountTxn(TransactionKey{}, *this, *mounted, *resource);
}

RegionUnmountTxn::RegionUnmountTxn(TransactionKey,
                                   SegmentPool::WriteAccess& access,
                                   const MountedRegion& mounted,
                                   RegionResource& resource)
    : segment_(mounted.segment),
      client_id_(mounted.client_id),
      previous_status_(mounted.status) {
    const auto result =
        access.TransitionRegion(mounted, resource, SegmentStatus::UNMOUNTING);
    DCHECK(result == ErrorCode::OK);
    generation_ = mounted.generation;
}

RegionUnmountTxn::RegionUnmountTxn(RegionUnmountTxn&& other) noexcept
    : segment_(std::move(other.segment_)),
      client_id_(other.client_id_),
      previous_status_(other.previous_status_),
      generation_(other.generation_),
      finished_(std::exchange(other.finished_, true)) {}

ErrorCode RegionUnmountTxn::Commit(SegmentPool::WriteAccess& access) && {
    auto transaction = std::move(*this);
    if (transaction.finished_) {
        return ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS;
    }
    auto result = access.EraseUnmountedRegion(
        transaction.segment_.id, transaction.client_id_,
        SegmentStatus::UNMOUNTING, transaction.generation_);
    if (result == ErrorCode::OK) {
        transaction.finished_ = true;
    }
    return result;
}

ErrorCode RegionUnmountTxn::Rollback(SegmentPool::WriteAccess& access) && {
    auto transaction = std::move(*this);
    if (transaction.finished_) {
        return ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS;
    }
    return access.RestoreUnmountedRegion(
        transaction.segment_.id, transaction.client_id_,
        transaction.previous_status_, transaction.generation_);
}

ErrorCode SegmentPool::WriteAccess::RestoreUnmountedRegion(
    const UUID& segment_id, const UUID& client_id,
    SegmentStatus previous_status, uint64_t generation) {
    auto mounted = catalog_.Find(segment_id);
    if (mounted == nullptr) {
        return ErrorCode::SEGMENT_NOT_FOUND;
    }
    if (mounted->client_id != client_id ||
        mounted->status != SegmentStatus::UNMOUNTING ||
        mounted->generation != generation) {
        return ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS;
    }
    RegionResource* resource = segment_pool_.GetResource(*mounted);
    RegionDriver* driver = segment_pool_.GetDriver(mounted->kind);
    if (!resource || !driver) {
        LOG(ERROR) << "Unmount rollback missing resource or driver: segment="
                   << mounted->segment.name << ", id=" << segment_id
                   << ", kind=" << static_cast<int>(mounted->kind)
                   << ", resource_present=" << (resource != nullptr)
                   << ", driver_present=" << (driver != nullptr);
        return ErrorCode::INTERNAL_ERROR;
    }
    return TransitionRegion(*mounted, *resource, previous_status);
}

tl::expected<RegionGracefulUnmountTxn, ErrorCode>
SegmentPool::WriteAccess::PrepareGracefulUnmount(const UUID& segment_id,
                                                 const UUID& client_id) {
    auto mounted = catalog_.Find(segment_id);
    if (mounted == nullptr) {
        return tl::make_unexpected(ErrorCode::SEGMENT_NOT_FOUND);
    }
    if (mounted->client_id != client_id) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    if (mounted->status != SegmentStatus::OK &&
        mounted->status != SegmentStatus::DRAINING &&
        mounted->status != SegmentStatus::GRACEFULLY_UNMOUNTING) {
        return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    }
    RegionResource* resource = segment_pool_.GetResource(*mounted);
    if (!resource) {
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }
    return RegionGracefulUnmountTxn(TransactionKey{}, *this, *mounted,
                                    *resource);
}

RegionGracefulUnmountTxn::RegionGracefulUnmountTxn(
    TransactionKey, SegmentPool::WriteAccess& access,
    const MountedRegion& mounted, RegionResource& resource)
    : segment_id_(mounted.segment.id), client_id_(mounted.client_id) {
    const auto result = access.TransitionRegion(
        mounted, resource, SegmentStatus::GRACEFULLY_UNMOUNTING);
    DCHECK(result == ErrorCode::OK);
    generation_ = mounted.generation;
}

RegionGracefulUnmountTxn::RegionGracefulUnmountTxn(
    RegionGracefulUnmountTxn&& other) noexcept
    : segment_id_(other.segment_id_),
      client_id_(other.client_id_),
      generation_(other.generation_),
      finished_(std::exchange(other.finished_, true)) {}

ErrorCode RegionGracefulUnmountTxn::Finalize(
    SegmentPool::WriteAccess& access) && {
    auto transaction = std::move(*this);
    if (transaction.finished_) {
        return ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS;
    }
    return access.EraseUnmountedRegion(
        transaction.segment_id_, transaction.client_id_,
        SegmentStatus::GRACEFULLY_UNMOUNTING, transaction.generation_);
}

ErrorCode SegmentPool::WriteAccess::EraseUnmountedRegion(
    const UUID& segment_id, const UUID& client_id,
    SegmentStatus expected_status, uint64_t generation) {
    auto mounted = catalog_.Find(segment_id);
    if (mounted == nullptr) {
        return ErrorCode::SEGMENT_NOT_FOUND;
    }
    if (mounted->client_id != client_id) {
        return ErrorCode::INVALID_PARAMS;
    }
    if (mounted->status != expected_status ||
        mounted->generation != generation) {
        return ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS;
    }
    const MountedRegion record = *mounted;
    RegionDriver* driver = segment_pool_.GetDriver(record.kind);
    if (!driver || !driver->GetResource(segment_id)) {
        return ErrorCode::INTERNAL_ERROR;
    }
    // Resource existence was validated above. Erase is the commit point; all
    // remaining catalog/index operations are non-failing.
    const bool erased = driver->Erase(segment_id);
    DCHECK(erased);

    const bool capacity_accounted =
        segment_pool_.capacity_accounted_region_ids_.erase(segment_id) != 0;
    const bool removed = catalog_.Erase(segment_id);
    DCHECK(removed);
    if (capacity_accounted) {
        MasterMetricManager::instance().dec_total_mem_capacity(
            record.segment.name, record.segment.size);
        if (catalog_.RegionIds(record.segment.name).empty()) {
            MasterMetricManager::instance().remove_segment_metrics(
                record.segment.name);
        }
    }
    return ErrorCode::OK;
}

const RegionCatalog& SegmentPool::WriteAccess::Catalog() const {
    return catalog_;
}

ErrorCode SegmentPool::WriteAccess::SetSegmentStatusByName(
    std::string_view segment_name, SegmentStatus status) {
    const auto regions = catalog_.RegionIds(segment_name);
    if (regions.empty()) {
        return ErrorCode::SEGMENT_NOT_FOUND;
    }
    for (const auto& id : regions) {
        auto mounted = catalog_.Find(id);
        if (mounted == nullptr || !segment_pool_.GetDriver(mounted->kind) ||
            !segment_pool_.GetResource(*mounted)) {
            return ErrorCode::INTERNAL_ERROR;
        }
        if (mounted->status == SegmentStatus::UNMOUNTING) {
            return ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS;
        }
    }

    for (const auto& id : regions) {
        const auto& mounted = *catalog_.Find(id);
        auto& resource = *segment_pool_.GetResource(mounted);
        const auto result = TransitionRegion(mounted, resource, status);
        if (result != ErrorCode::OK) return result;
    }
    return ErrorCode::OK;
}

ErrorCode SegmentPool::WriteAccess::TransitionRegion(
    const MountedRegion& mounted, RegionResource& resource,
    SegmentStatus status) {
    auto& driver = *segment_pool_.GetDriver(mounted.kind);
    if (status == SegmentStatus::OK) {
        if (!resource.active && !driver.Reactivate(mounted.segment.id)) {
            LOG(ERROR) << "Failed to reactivate region: segment="
                       << mounted.segment.name << ", id=" << mounted.segment.id;
            return ErrorCode::INTERNAL_ERROR;
        }
        (void)segment_pool_.placement_index_.AddCandidate(
            mounted.segment.name, *resource.candidate, mounted.segment.host_id);
    } else {
        (void)segment_pool_.placement_index_.RemoveCandidate(
            mounted.segment.name, *resource.candidate, mounted.segment.host_id);
        if (resource.active) {
            const bool deactivated = driver.Deactivate(mounted.segment.id);
            DCHECK(deactivated);
        }
    }
    const bool updated = catalog_.SetStatus(mounted.segment.id, status);
    DCHECK(updated);
    return ErrorCode::OK;
}

void SegmentPool::WriteAccess::Clear() noexcept {
    for (const auto& mounted : catalog_.Regions()) {
        if (auto* driver = segment_pool_.GetDriver(mounted.kind)) {
            (void)driver->Erase(mounted.segment.id);
        }
    }
    segment_pool_.ReleaseCapacityMetrics();
    segment_pool_.placement_index_.Clear();
    catalog_.Clear();
}

}  // namespace mooncake
