#include "nof_segment_manager.h"

#include <algorithm>
#include <numeric>

#include "master_metric_manager.h"

namespace mooncake {

tl::expected<std::vector<Replica>, ErrorCode> NoFSegmentManager::Allocate(
    PlacementPolicyType policy_type, const ReplicaPlacementRequest& request) {
    ScopedPlacementReadAccess placement(placement_index_, manager_mutex_);
    const ReplicaAllocationRequest resolved{
        .size = request.size,
        .replica_count = request.replica_count,
        .preferred_group = request.preferred_group,
        .preferred_groups = request.preferred_groups,
        .resolved_preferred_groups = {},
        .excluded_groups = request.excluded_groups,
        .replica_type = request.replica_type,
    };
    return replica_allocator_.Allocate(placement, policy_type, resolved);
}

ErrorCode ScopedNoFSegmentWriteAccess::MountSegment(const NoFSegment& segment,
                                                    const UUID& client_id) {
    if (segment.size == 0) {
        return ErrorCode::INVALID_PARAMS;
    }
    if (nof_segment_manager_->mounted_segments_.contains(segment.id)) {
        return ErrorCode::SEGMENT_ALREADY_EXISTS;
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
    auto target = std::make_unique<AllocationTarget>(
        allocator.get(), AllocationTargetKind::NATIVE);
    nof_segment_manager_->placement_index_.AddTarget(segment.name,
                                                     target.get());
    nof_segment_manager_->segment_ids_by_client_[client_id].push_back(
        segment.id);
    nof_segment_manager_->owner_client_by_group_name_[segment.name] = client_id;
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
    const UUID& segment_id, size_t& metrics_dec_capacity) {
    auto mounted = nof_segment_manager_->mounted_segments_.find(segment_id);
    if (mounted == nof_segment_manager_->mounted_segments_.end()) {
        return ErrorCode::SEGMENT_NOT_FOUND;
    }
    if (mounted->second.status == SegmentStatus::UNMOUNTING) {
        return ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS;
    }
    metrics_dec_capacity = mounted->second.segment.size;
    if (mounted->second.allocation_target) {
        nof_segment_manager_->placement_index_.RemoveTarget(
            mounted->second.segment.name,
            mounted->second.allocation_target.get());
    }
    mounted->second.allocation_target.reset();
    mounted->second.buf_allocator.reset();
    mounted->second.status = SegmentStatus::UNMOUNTING;
    return ErrorCode::OK;
}

ErrorCode ScopedNoFSegmentWriteAccess::CommitUnmountSegment(
    const UUID& segment_id, const UUID& client_id,
    const size_t& metrics_dec_capacity) {
    auto mounted = nof_segment_manager_->mounted_segments_.find(segment_id);
    if (mounted == nof_segment_manager_->mounted_segments_.end()) {
        return ErrorCode::SEGMENT_NOT_FOUND;
    }
    const std::string name = mounted->second.segment.name;
    auto client = nof_segment_manager_->segment_ids_by_client_.find(client_id);
    if (client != nof_segment_manager_->segment_ids_by_client_.end()) {
        auto id =
            std::find(client->second.begin(), client->second.end(), segment_id);
        if (id != client->second.end()) {
            client->second.erase(id);
        }
        if (client->second.empty()) {
            nof_segment_manager_->segment_ids_by_client_.erase(client);
        }
    }
    nof_segment_manager_->mounted_segments_.erase(mounted);
    bool name_exists = false;
    for (const auto& [_, other] : nof_segment_manager_->mounted_segments_) {
        if (other.segment.name == name) {
            nof_segment_manager_->owner_client_by_group_name_[name] =
                other.client_id;
            name_exists = true;
            break;
        }
    }
    if (!name_exists) {
        nof_segment_manager_->owner_client_by_group_name_.erase(name);
    }
    MasterMetricManager::instance().dec_total_nof_capacity(
        name, metrics_dec_capacity);
    MasterMetricManager::instance().remove_nof_segment_metrics(name);
    return ErrorCode::OK;
}

ErrorCode ScopedNoFSegmentWriteAccess::GetClientSegments(
    const UUID& client_id, std::vector<NoFSegment>& segments) const {
    auto client = nof_segment_manager_->segment_ids_by_client_.find(client_id);
    if (client == nof_segment_manager_->segment_ids_by_client_.end()) {
        return ErrorCode::SEGMENT_NOT_FOUND;
    }
    segments.clear();
    for (const auto& id : client->second) {
        auto mounted = nof_segment_manager_->mounted_segments_.find(id);
        if (mounted != nof_segment_manager_->mounted_segments_.end()) {
            segments.push_back(mounted->second.segment);
        }
    }
    return ErrorCode::OK;
}

ErrorCode ScopedNoFSegmentWriteAccess::GetMountedSegments(
    std::vector<MountedNoFSegmentSnapshot>& segments) const {
    segments.clear();
    for (const auto& [id, mounted] : nof_segment_manager_->mounted_segments_) {
        segments.push_back(
            {id, mounted.client_id, mounted.segment, mounted.status});
    }
    return ErrorCode::OK;
}

ErrorCode ScopedNoFSegmentWriteAccess::GetAllSegments(
    std::vector<std::string>& all_segments) {
    all_segments.clear();
    for (const auto& [_, mounted] : nof_segment_manager_->mounted_segments_) {
        if (mounted.status == SegmentStatus::OK) {
            all_segments.push_back(mounted.segment.name);
        }
    }
    return ErrorCode::OK;
}

ErrorCode ScopedNoFSegmentWriteAccess::QuerySegments(const std::string& name,
                                                     size_t& used,
                                                     size_t& capacity) {
    auto* group = nof_segment_manager_->placement_index_.GetView().Find(name);
    if (!group) {
        return ErrorCode::SEGMENT_NOT_FOUND;
    }
    used = 0;
    capacity = 0;
    for (const auto* target : group->targets) {
        used += target->Used();
        capacity += target->Capacity();
    }
    return capacity == 0 ? ErrorCode::SEGMENT_NOT_FOUND : ErrorCode::OK;
}

void NoFSegmentManager::GetMountedSegmentsSnapshot(
    std::vector<MountedNoFSegmentSnapshot>& segments) const {
    std::shared_lock lock(manager_mutex_);
    segments.clear();
    segments.reserve(mounted_segments_.size());
    for (const auto& [id, mounted] : mounted_segments_) {
        segments.push_back(
            {id, mounted.client_id, mounted.segment, mounted.status});
    }
}

}  // namespace mooncake
