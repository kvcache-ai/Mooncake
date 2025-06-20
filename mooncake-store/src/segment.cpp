#include "segment.h"

#include "master_metric_manager.h"

namespace mooncake {

ErrorCode ScopedSegmentAccess::MountSegment(const Segment& segment,
                                            const UUID& client_id) {
    const uintptr_t buffer = segment.base;
    const size_t size = segment.size;

    // Check if parameters are valid before allocating memory.
    if (buffer == 0 || size == 0 || buffer % facebook::cachelib::Slab::kSize ||
        size % facebook::cachelib::Slab::kSize) {
        LOG(ERROR) << "buffer=" << buffer << " or size=" << size
                   << " is not aligned to " << facebook::cachelib::Slab::kSize;
        return ErrorCode::INVALID_PARAMS;
    }

    // Check if segment already exists
    auto exist_segment_it =
        segment_manager_->mounted_segments_.find(segment.id);
    if (exist_segment_it != segment_manager_->mounted_segments_.end()) {
        auto& exist_segment = exist_segment_it->second;
        if (exist_segment.status == SegmentStatus::OK) {
            LOG(WARNING) << "segment_name=" << segment.name
                         << ", warn=segment_already_exists";
            return ErrorCode::SEGMENT_ALREADY_EXISTS;
        } else {
            LOG(ERROR) << "segment_name=" << segment.name
                       << ", error=segment_already_exists_but_not_ok"
                       << ", status=" << exist_segment.status;
            return ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS;
        }
    }

    std::shared_ptr<BufferAllocator> allocator;
    try {
        // SlabAllocator may throw an exception if the size or base is invalid
        // for the slab allocator.
        allocator =
            std::make_shared<BufferAllocator>(segment.name, buffer, size);
        if (!allocator) {
            LOG(ERROR) << "segment_name=" << segment.name
                       << ", error=failed_to_create_allocator";
            return ErrorCode::INVALID_PARAMS;
        }
    } catch (...) {
        LOG(ERROR) << "segment_name=" << segment.name
                   << ", error=unknown_exception_during_allocator_creation";
        return ErrorCode::INVALID_PARAMS;
    }

    segment_manager_->allocators_.push_back(allocator);
    segment_manager_->allocators_by_name_[segment.name].push_back(allocator);
    segment_manager_->client_segments_[client_id].push_back(segment.id);
    segment_manager_->mounted_segments_[segment.id] = {
        segment, SegmentStatus::OK, std::move(allocator)};

    MasterMetricManager::instance().inc_total_capacity(size);

    return ErrorCode::OK;
}

ErrorCode ScopedSegmentAccess::ReMountSegment(
    const std::vector<Segment>& segments, const UUID& client_id) {
    for (const auto& segment : segments) {
        ErrorCode err = MountSegment(segment, client_id);
        if (err == ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS ||
            err == ErrorCode::INTERNAL_ERROR) {
            LOG(ERROR) << "segment_name=" << segment.name
                       << ", error=fail_to_remount_segment";
            return err;
        } else if (err == ErrorCode::INVALID_PARAMS) {
            // Ignore INVALID_PARAMS. This error cannot be solved by a new
            // remount request.
            LOG(WARNING) << "segment_name=" << segment.name
                         << ", warn=invalid_params";
        } else if (err == ErrorCode::SEGMENT_ALREADY_EXISTS) {
            // Segment already exists, no need to remount.
            LOG(WARNING) << "segment_name=" << segment.name
                         << ", warn=segment_already_exists";
        } else if (err != ErrorCode::OK) {
            // Ignore other errors. The error may not be solvable by a new
            // remount request.
            LOG(ERROR) << "segment_name=" << segment.name
                       << ", error=unexpected_error (" << err << ")";
        }
    }

    return ErrorCode::OK;
}

ErrorCode ScopedSegmentAccess::PrepareUnmountSegment(
    const UUID& segment_id, size_t& metrics_dec_capacity) {
    auto it = segment_manager_->mounted_segments_.find(segment_id);
    if (it == segment_manager_->mounted_segments_.end()) {
        LOG(WARNING) << "segment_id=" << segment_id
                     << ", warn=segment_not_found";
        return ErrorCode::SEGMENT_NOT_FOUND;
    }
    if (it->second.status == SegmentStatus::UNMOUNTING) {
        LOG(ERROR) << "segment_id=" << segment_id
                   << ", error=segment_is_unmounting";
        return ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS;
    }

    auto& mounted_segment = it->second;
    auto& segment = mounted_segment.segment;
    metrics_dec_capacity = segment.size;

    // Remove the allocator from the segment manager
    std::shared_ptr<BufferAllocator> allocator = mounted_segment.buf_allocator;

    // 1. Remove from allocators
    auto alloc_it = std::find(segment_manager_->allocators_.begin(),
                              segment_manager_->allocators_.end(), allocator);
    if (alloc_it != segment_manager_->allocators_.end()) {
        segment_manager_->allocators_.erase(alloc_it);
    } else {
        LOG(ERROR) << "segment_name=" << segment.name
                   << ", error=allocator_not_found_in_allocators";
    }

    // 2. Remove from allocators_by_name
    bool found_in_allocators_by_name = false;
    auto name_it = segment_manager_->allocators_by_name_.find(segment.name);
    if (name_it != segment_manager_->allocators_by_name_.end()) {
        auto& allocators = name_it->second;
        auto alloc_it =
            std::find(allocators.begin(), allocators.end(), allocator);
        if (alloc_it != allocators.end()) {
            allocators.erase(alloc_it);
            found_in_allocators_by_name = true;
        }
        if (allocators.empty()) {
            segment_manager_->allocators_by_name_.erase(name_it);
        }
    }
    if (!found_in_allocators_by_name) {
        LOG(ERROR) << "segment_name=" << segment.name
                   << ", error=allocator_not_found_in_allocators_by_name";
    }

    // 3. Remove from mounted_segment
    mounted_segment.buf_allocator.reset();

    // Set the segment status to UNMOUNTING
    mounted_segment.status = SegmentStatus::UNMOUNTING;

    return ErrorCode::OK;
}

ErrorCode ScopedSegmentAccess::CommitUnmountSegment(
    const UUID& segment_id, const UUID& client_id,
    const size_t& metrics_dec_capacity) {
    // Remove from client_segments_
    bool found_in_client_segments = false;
    auto client_it = segment_manager_->client_segments_.find(client_id);
    if (client_it != segment_manager_->client_segments_.end()) {
        auto& segments = client_it->second;
        auto segment_it =
            std::find(segments.begin(), segments.end(), segment_id);
        if (segment_it != segments.end()) {
            segments.erase(segment_it);
            found_in_client_segments = true;
        }
        if (segments.empty()) {
            segment_manager_->client_segments_.erase(client_it);
        }
    }
    if (!found_in_client_segments) {
        LOG(ERROR) << "segment_id=" << segment_id
                   << ", error=segment_not_found_in_client_segments";
    }

    // Remove from mounted_segments_
    segment_manager_->mounted_segments_.erase(segment_id);

    // Decrease the total capacity
    MasterMetricManager::instance().dec_total_capacity(metrics_dec_capacity);

    return ErrorCode::OK;
}

ErrorCode ScopedSegmentAccess::GetClientSegments(
    const UUID& client_id, std::vector<Segment>& segments) const {
    auto it = segment_manager_->client_segments_.find(client_id);
    if (it == segment_manager_->client_segments_.end()) {
        return ErrorCode::SEGMENT_NOT_FOUND;
    }
    segments.clear();
    for (auto& segment_id : it->second) {
        auto segment_it = segment_manager_->mounted_segments_.find(segment_id);
        if (segment_it != segment_manager_->mounted_segments_.end()) {
            segments.emplace_back(segment_it->second.segment);
        }
    }
    return ErrorCode::OK;
}

ErrorCode ScopedSegmentAccess::GetAllSegments(
    std::vector<std::string>& all_segments) {
    all_segments.clear();
    for (auto& segment : segment_manager_->mounted_segments_) {
        if (segment.second.status == SegmentStatus::OK) {
            all_segments.push_back(segment.second.segment.name);
        }
    }
    return ErrorCode::OK;
}

ErrorCode ScopedSegmentAccess::QuerySegments(const std::string& segment,
                                             size_t& used, size_t& capacity) {
    const auto& allocators =
        segment_manager_->allocators_by_name_.find(segment);
    if (allocators != segment_manager_->allocators_by_name_.end()) {
        // Allocators Only contains the segments with OK status, so just return
        // the first one.
        capacity = allocators->second[0]->capacity();
        used = allocators->second[0]->size();
    } else {
        VLOG(1) << "### DEBUG ### MasterService::QuerySegments(" << segment
                << ") not found!";
        return ErrorCode::SEGMENT_NOT_FOUND;
        }
    return ErrorCode::OK;
}
}  // namespace mooncake