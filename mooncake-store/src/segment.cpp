#include "segment.h"

#include "master_metric_manager.h"

namespace mooncake {

ErrorCode ScopedSegmentAccess::MountSegment(const Segment& segment,
                                            const UUID& client_id) {
    const uintptr_t buffer = segment.base;
    const size_t size = segment.size;

    // Check if cxl storage is enable
    if (segment_manager_->enable_cxl_ && segment.protocol == "cxl") {
        LOG(INFO) << "Start Mounting CXL Segment.";
        if (segment_manager_->memory_allocator_ ==
            BufferAllocatorType::CACHELIB) {
            auto allocator = segment_manager_->cxl_global_allocator_;
            if (segment_manager_->cxl_global_allocator_ == nullptr) {
                LOG(ERROR) << "Cxl global allocator has not been initialized.";
                return ErrorCode::INTERNAL_ERROR;
            }
            segment_manager_->allocator_manager_.addAllocator(segment.name,
                                                              allocator);
            segment_manager_->client_segments_[client_id].push_back(segment.id);
            segment_manager_->mounted_segments_[segment.id] = {
                segment, SegmentStatus::OK, allocator};
            segment_manager_->client_by_name_[segment.name] = client_id;
            MasterMetricManager::instance().inc_total_mem_capacity(segment.name,
                                                                   size);

            LOG(INFO) << "[CXL Segment Mounted Successfully] Segment name: "
                      << segment.name
                      << ", Mount size: " << (size / 1024 / 1024 / 1024)
                      << " GB";
            return ErrorCode::OK;
        }
        return ErrorCode::INTERNAL_ERROR;
    }
    // Check if parameters are valid before allocating memory.
    if (buffer == 0 || size == 0) {
        LOG(ERROR) << "buffer=" << buffer << " or size=" << size
                   << " is invalid";
        return ErrorCode::INVALID_PARAMS;
    }

    if (segment_manager_->memory_allocator_ == BufferAllocatorType::CACHELIB &&
        (buffer % facebook::cachelib::Slab::kSize ||
         size % facebook::cachelib::Slab::kSize)) {
        LOG(ERROR) << "buffer=" << buffer << " or size=" << size
                   << " is not aligned to " << facebook::cachelib::Slab::kSize
                   << " as required by Cachelib";
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

    std::shared_ptr<BufferAllocatorBase> allocator;
    // CachelibBufferAllocator may throw an exception if the size or base is
    // invalid for the slab allocator.
    try {
        // Create allocator based on the configured type
        switch (segment_manager_->memory_allocator_) {
            case BufferAllocatorType::CACHELIB:
                allocator = std::make_shared<CachelibBufferAllocator>(
                    segment.name, buffer, size, segment.te_endpoint);
                break;
            case BufferAllocatorType::OFFSET:
                allocator = std::make_shared<OffsetBufferAllocator>(
                    segment.name, buffer, size, segment.te_endpoint);
                break;
            default:
                LOG(ERROR) << "segment_name=" << segment.name
                           << ", error=unknown_memory_allocator="
                           << static_cast<int>(
                                  segment_manager_->memory_allocator_);
                return ErrorCode::INVALID_PARAMS;
        }

        if (!allocator) {
            LOG(ERROR) << "segment_name=" << segment.name
                       << ", error=failed_to_create_allocator";
            return ErrorCode::INVALID_PARAMS;
        }
    } catch (...) {
        LOG(ERROR) << "segment_name=" << segment.name
                   << ", error=exception_during_allocator_creation";
        return ErrorCode::INVALID_PARAMS;
    }

    segment_manager_->allocator_manager_.addAllocator(segment.name, allocator);
    segment_manager_->client_segments_[client_id].push_back(segment.id);
    segment_manager_->mounted_segments_[segment.id] = {
        segment, SegmentStatus::OK, std::move(allocator)};
    segment_manager_->client_by_name_[segment.name] = client_id;
    MasterMetricManager::instance().inc_total_mem_capacity(segment.name, size);

    return ErrorCode::OK;
}

ErrorCode ScopedSegmentAccess::MountLocalDiskSegment(const UUID& client_id,
                                                     bool enable_offloading) {
    auto exist_segment_it =
        segment_manager_->client_local_disk_segment_.find(client_id);
    if (exist_segment_it !=
        segment_manager_->client_local_disk_segment_.end()) {
        LOG(WARNING) << "client_id=" << client_id
                     << ", warn=local_disk_segment_already_exists";
        return ErrorCode::SEGMENT_ALREADY_EXISTS;
    }
    segment_manager_->client_local_disk_segment_.emplace(
        client_id, std::make_shared<LocalDiskSegment>(enable_offloading));
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
    std::shared_ptr<BufferAllocatorBase> allocator =
        mounted_segment.buf_allocator;

    // 1. Remove from allocators
    if (!segment_manager_->allocator_manager_.removeAllocator(segment.name,
                                                              allocator)) {
        LOG(ERROR) << "Allocator " << segment.id << " of segment "
                   << segment.name << " not found in allocator manager";
    }

    // 2. Remove from mounted_segment
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

    // segment_id -> segment_name
    std::string segment_name;
    bool is_cxl = false;
    auto&& segment = segment_manager_->mounted_segments_.find(segment_id);
    if (segment != segment_manager_->mounted_segments_.end()) {
        segment_name = segment->second.segment.name;
        // Also remove from segment_name_client_id_map_
        segment_manager_->client_by_name_.erase(segment_name);
        is_cxl = (segment->second.segment.protocol == "cxl");
    }
    // Remove from mounted_segments_
    segment_manager_->mounted_segments_.erase(segment_id);

    // Decrease the total capacity
    if (!is_cxl) {
        MasterMetricManager::instance().dec_total_mem_capacity(
            segment_name, metrics_dec_capacity);
    }

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
    size_t total_used = 0, total_capacity = 0;
    const auto& allocator_manager = segment_manager_->allocator_manager_;
    const auto& allocators = allocator_manager.getAllocators(segment);
    if (allocators != nullptr) {
        for (const auto& allocator : *allocators) {
            total_used += allocator->size();
            total_capacity += allocator->capacity();
        }
    }

    if (total_capacity == 0) {
        VLOG(1) << "### DEBUG ### MasterService::QuerySegments(" << segment
                << ") not found!";
        return ErrorCode::SEGMENT_NOT_FOUND;
    }

    used = total_used;
    capacity = total_capacity;

    return ErrorCode::OK;
}

ErrorCode ScopedSegmentAccess::GetClientIdBySegmentName(
    const std::string& segment_name, UUID& client_id) const {
    auto it = segment_manager_->client_by_name_.find(segment_name);
    if (it == segment_manager_->client_by_name_.end()) {
        LOG(ERROR) << "segment_name=" << segment_name
                   << ", error=segment_not_found";
        return ErrorCode::SEGMENT_NOT_FOUND;
    }
    client_id = it->second;
    return ErrorCode::OK;
}

bool ScopedSegmentAccess::ExistsSegmentName(
    const std::string& segment_name) const {
    auto it = segment_manager_->client_by_name_.find(segment_name);
    return it != segment_manager_->client_by_name_.end();
}

void SegmentManager::initializeCxlAllocator(const std::string& cxl_path,
                                            const size_t cxl_size) {
    LOG(INFO) << "Init CXL global allocator.";
    LOG(INFO) << "[CXL] create allocator with "
            << "path=" << cxl_path << " base=0x" << std::hex << DEFAULT_CXL_BASE
            << std::dec << " size=" << cxl_size << " (" << std::fixed
            << std::setprecision(2) << cxl_size / (1024.0 * 1024 * 1024)
            << " GB)";

    cxl_global_allocator_ = std::make_shared<CachelibBufferAllocator>(
        cxl_path, DEFAULT_CXL_BASE, cxl_size, cxl_path);
}
}  // namespace mooncake