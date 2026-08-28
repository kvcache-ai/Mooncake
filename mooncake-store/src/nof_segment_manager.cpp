#include "nof_segment_manager.h"

#include <algorithm>
#include <unordered_set>

#include "master_metric_manager.h"

namespace mooncake {

ScopedNoFSegmentWriteAccess::ScopedNoFSegmentWriteAccess(
    NoFSegmentManager* manager)
    : nof_segment_manager_(manager), lock_(manager->manager_mutex_) {}

ScopedNoFSegmentWriteAccess NoFSegmentManager::AcquireWriteAccess() {
    return ScopedNoFSegmentWriteAccess(this);
}

ScopedPlacementReadAccess NoFSegmentManager::AcquirePlacementAccess() const {
    return ScopedPlacementReadAccess(placement_index_, manager_mutex_);
}

ErrorCode ScopedNoFSegmentWriteAccess::MountSegment(const NoFSegment& segment,
                                                    const UUID& client_id) {
    if (segment.size == 0) {
        return ErrorCode::INVALID_PARAMS;
    }
    const auto existing =
        nof_segment_manager_->mounted_segments_.find(segment.id);
    if (existing != nof_segment_manager_->mounted_segments_.end()) {
        return existing->second.status == SegmentStatus::OK
                   ? ErrorCode::SEGMENT_ALREADY_EXISTS
                   : ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS;
    }
    for (const auto& [_, mounted] : nof_segment_manager_->mounted_segments_) {
        if (mounted.status == SegmentStatus::OK &&
            mounted.segment.te_endpoint == segment.te_endpoint) {
            return ErrorCode::SEGMENT_ALREADY_EXISTS;
        }
    }

    std::shared_ptr<BufferAllocatorBase> allocator;
    try {
        if (nof_segment_manager_->memory_allocator_ ==
            BufferAllocatorType::CACHELIB) {
            allocator = std::make_shared<CachelibBufferAllocator>(
                segment.name, segment.base, segment.size, segment.te_endpoint,
                ReplicaType::NOF_SSD);
        } else if (nof_segment_manager_->memory_allocator_ ==
                   BufferAllocatorType::OFFSET) {
            allocator = std::make_shared<OffsetBufferAllocator>(
                segment.name, segment.base, segment.size, segment.te_endpoint,
                ReplicaType::NOF_SSD);
        } else {
            return ErrorCode::INVALID_PARAMS;
        }
    } catch (...) {
        return ErrorCode::INVALID_PARAMS;
    }
    allocator->AttachUsageTracker(nof_segment_manager_->usage_tracker_);
    auto target = std::make_unique<AllocationTarget>(
        allocator.get(), AllocationTargetKind::NATIVE);
    nof_segment_manager_->placement_index_.AddTarget(segment.name,
                                                     target.get());
    nof_segment_manager_->mounted_segments_.emplace(
        segment.id, MountedNoFSegment{segment, client_id, SegmentStatus::OK,
                                      std::move(allocator), std::move(target)});
    MasterMetricManager::instance().inc_total_nof_capacity(segment.name,
                                                           segment.size);
    return ErrorCode::OK;
}

ErrorCode ScopedNoFSegmentWriteAccess::ReMountSegment(
    const std::vector<NoFSegment>& segments, const UUID& client_id) {
    for (const auto& segment : segments) {
        auto result = MountSegment(segment, client_id);
        if (result == ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS ||
            result == ErrorCode::INTERNAL_ERROR) {
            return result;
        }
    }
    return ErrorCode::OK;
}

ErrorCode ScopedNoFSegmentWriteAccess::PrepareUnmountSegment(
    const UUID& segment_id, const UUID& client_id) {
    auto mounted = nof_segment_manager_->mounted_segments_.find(segment_id);
    if (mounted == nof_segment_manager_->mounted_segments_.end()) {
        return ErrorCode::SEGMENT_NOT_FOUND;
    }
    if (mounted->second.client_id != client_id) {
        return ErrorCode::INVALID_PARAMS;
    }
    if (mounted->second.status == SegmentStatus::UNMOUNTING) {
        return ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS;
    }
    if (mounted->second.target) {
        nof_segment_manager_->placement_index_.RemoveTarget(
            mounted->second.segment.name, mounted->second.target.get());
    }
    mounted->second.target.reset();
    mounted->second.allocator.reset();
    mounted->second.status = SegmentStatus::UNMOUNTING;
    return ErrorCode::OK;
}

ErrorCode ScopedNoFSegmentWriteAccess::CommitUnmountSegment(
    const UUID& segment_id, const UUID& client_id) {
    auto mounted = nof_segment_manager_->mounted_segments_.find(segment_id);
    if (mounted == nof_segment_manager_->mounted_segments_.end()) {
        return ErrorCode::SEGMENT_NOT_FOUND;
    }
    if (mounted->second.client_id != client_id ||
        mounted->second.status != SegmentStatus::UNMOUNTING) {
        return ErrorCode::INVALID_PARAMS;
    }
    const std::string name = mounted->second.segment.name;
    const size_t capacity = mounted->second.segment.size;
    nof_segment_manager_->mounted_segments_.erase(mounted);
    MasterMetricManager::instance().dec_total_nof_capacity(name, capacity);
    const bool name_still_mounted =
        std::any_of(nof_segment_manager_->mounted_segments_.begin(),
                    nof_segment_manager_->mounted_segments_.end(),
                    [&name](const auto& entry) {
                        return entry.second.segment.name == name;
                    });
    if (!name_still_mounted) {
        MasterMetricManager::instance().remove_nof_segment_metrics(name);
    }
    return ErrorCode::OK;
}

void NoFSegmentManager::GetMountedSegmentsSnapshot(
    std::vector<MountedNoFSegmentSnapshot>& segments) const {
    std::shared_lock lock(manager_mutex_);
    segments.clear();
    segments.reserve(mounted_segments_.size());
    for (const auto& [_, mounted] : mounted_segments_) {
        segments.push_back(
            {mounted.client_id, mounted.segment, mounted.status});
    }
}

StorageUsageSnapshot NoFSegmentManager::GetUsageSnapshot() const {
    std::shared_lock lock(manager_mutex_);
    StorageUsageSnapshot snapshot;
    std::unordered_set<const BufferAllocatorBase*> counted_allocators;
    for (const auto& [_, mounted] : mounted_segments_) {
        if (!mounted.allocator ||
            !counted_allocators.insert(mounted.allocator.get()).second) {
            continue;
        }
        const size_t used = mounted.allocator->size();
        const size_t capacity = mounted.allocator->capacity();
        snapshot.used_bytes += used;
        snapshot.capacity_bytes += capacity;
        auto& segment = snapshot.segments[mounted.allocator->getSegmentName()];
        segment.used_bytes += used;
        segment.capacity_bytes += capacity;
    }
    return snapshot;
}

tl::expected<std::vector<NoFSegmentOwnerInfo>, ErrorCode>
NoFSegmentManager::GetSegmentsByName(std::string_view segment_name) const {
    std::shared_lock lock(manager_mutex_);
    std::vector<NoFSegmentOwnerInfo> result;
    for (const auto& [segment_id, mounted] : mounted_segments_) {
        if (mounted.segment.name == segment_name) {
            result.emplace_back(segment_id, mounted.client_id);
        }
    }
    if (result.empty()) {
        return tl::make_unexpected(ErrorCode::SEGMENT_NOT_FOUND);
    }
    return result;
}

}  // namespace mooncake
