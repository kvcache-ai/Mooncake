#include "centralized_segment_manager.h"

namespace mooncake {

ErrorCode CentralizedSegmentManager::InnerCheckMountSegment(const Segment& segment,
                                                            const UUID& client_id) {
    const uintptr_t buffer = segment.base;
    const size_t size = segment.size;

    // Check if parameters are valid before allocating memory.
    if (buffer == 0 || size == 0) {
        LOG(ERROR) << "buffer=" << buffer << " or size=" << size
                   << " is invalid";
        return ErrorCode::INVALID_PARAMS;
    }

    if (memory_allocator_ == BufferAllocatorType::CACHELIB &&
        (buffer % facebook::cachelib::Slab::kSize ||
         size % facebook::cachelib::Slab::kSize)) {
        LOG(ERROR) << "buffer=" << buffer << " or size=" << size
                   << " is not aligned to " << facebook::cachelib::Slab::kSize
                   << " as required by Cachelib";
        return ErrorCode::INVALID_PARAMS;
    }

    // Check if segment already exists
    auto exist_segment_it = mounted_segments_.find(segment.id);
    if (exist_segment_it != mounted_segments_.end()) {
        auto exist_segment = std::static_pointer_cast<CentralizedSegment>(exist_segment_it->second);
        if (exist_segment->status == SegmentStatus::OK) {
            LOG(WARNING) << "segment_name=" << segment.name
                         << ", warn=segment_already_exists";
            return ErrorCode::SEGMENT_ALREADY_EXISTS;
        } else {
            LOG(ERROR) << "segment_name=" << segment.name
                       << ", error=segment_already_exists_but_not_ok"
                       << ", status=" << exist_segment->status;
            return ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS;
        }
    }

    return ErrorCode::OK;
}

ErrorCode CentralizedSegmentManager::InnerMountSegment(const Segment& segment,
                                                       const UUID& client_id) {
    ErrorCode ret = InnerCheckMountSegment(segment, client_id);
    if (ret != ErrorCode::OK) {
        LOG(ERROR) << "fail to inner check mount segment"
                   << ", segment_name=" << segment.name
                   << ", ret=" << ret;
        return ret;
    }
    const uintptr_t buffer = segment.base;
    const size_t size = segment.size;
    std::shared_ptr<BufferAllocatorBase> allocator;
    // CachelibBufferAllocator may throw an exception if the size or base is
    // invalid for the slab allocator.
    try {
        // Create allocator based on the configured type
        switch (memory_allocator_) {
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
                           << static_cast<int>(memory_allocator_);
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

    allocator_manager_.addAllocator(segment.name, allocator);
    client_segments_[client_id].push_back(segment.id);

    auto mounted_segment = std::make_shared<CentralizedSegment>();
    static_cast<Segment&>(*mounted_segment) = segment;
    mounted_segment->status = SegmentStatus::OK;
    mounted_segment->buf_allocator = allocator;
    mounted_segments_[segment.id] = mounted_segment;
    client_by_name_[segment.name] = client_id;

    return ErrorCode::OK;
}

ErrorCode CentralizedSegmentManager::MountLocalDiskSegment(const UUID& client_id,
                                                           bool enable_offloading) {
    std::unique_lock<std::shared_mutex> lock_(segment_mutex_);
    auto exist_segment_it = client_local_disk_segment_.find(client_id);
    if (exist_segment_it != client_local_disk_segment_.end()) {
        LOG(WARNING) << "client_id=" << client_id
                     << ", warn=local_disk_segment_already_exists";
        return ErrorCode::SEGMENT_ALREADY_EXISTS;
    }
    client_local_disk_segment_.emplace(
        client_id, std::make_shared<LocalDiskSegment>(enable_offloading));
    return ErrorCode::OK;
}

auto CentralizedSegmentManager::OffloadObjectHeartbeat(const UUID& client_id,
                                                       bool enable_offloading)
    -> tl::expected<std::unordered_map<std::string, int64_t>, ErrorCode> {
    std::shared_lock<std::shared_mutex> lock_(segment_mutex_);
    auto local_disk_segment_it = client_local_disk_segment_.find(client_id);
    if (local_disk_segment_it == client_local_disk_segment_.end()) {
        LOG(ERROR) << "Local disk segment not fount with client id = "
                   << client_id;
        return tl::make_unexpected(ErrorCode::SEGMENT_NOT_FOUND);
    }
    MutexLocker locker(&local_disk_segment_it->second->offloading_mutex_);
    local_disk_segment_it->second->enable_offloading = enable_offloading;
    if (enable_offloading) {
        return std::move(local_disk_segment_it->second->offloading_objects);
    }
    return {};
}

ErrorCode CentralizedSegmentManager::PushOffloadingQueue(
    const std::string& key, const int64_t size, const std::string& segment_name) {
    std::shared_lock<std::shared_mutex> lock_(segment_mutex_);
    auto client_id_it = client_by_name_.find(segment_name);
    if (client_id_it == client_by_name_.end()) {
        LOG(ERROR) << "Segment " << segment_name << " not found";
        return ErrorCode::SEGMENT_NOT_FOUND;
    }
    auto local_disk_segment_it =
        client_local_disk_segment_.find(client_id_it->second);
    if (local_disk_segment_it == client_local_disk_segment_.end()) {
        LOG(ERROR) << "Local disk segment not fount with client id = "
                   << client_id_it->second;
        return ErrorCode::UNABLE_OFFLOADING;
    }
    MutexLocker locker(&local_disk_segment_it->second->offloading_mutex_);
    if (!local_disk_segment_it->second->enable_offloading) {
        LOG(ERROR) << "Offloading is not enabled for client id = "
                   << client_id_it->second;
        return ErrorCode::UNABLE_OFFLOADING;
    }
    if (local_disk_segment_it->second->offloading_objects.size() >= OFFLOADING_QUEUE_LIMIT) {
        LOG(ERROR) << "Offloading queue is full";
        return ErrorCode::KEYS_ULTRA_LIMIT;
    }
    local_disk_segment_it->second->offloading_objects.emplace(key, size);
    return ErrorCode::OK;
}

ErrorCode CentralizedSegmentManager::InnerReMountSegment(
    const std::vector<Segment>& segments, const UUID& client_id) {
    for (const auto& segment : segments) {
        ErrorCode err = InnerMountSegment(segment, client_id);

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

ErrorCode CentralizedSegmentManager::BatchPrepareUnmountClientSegments(
    const std::vector<UUID>& clients, std::vector<UUID> &unmount_segments,
    std::vector<size_t> &dec_capacities, std::vector<UUID> &client_ids,
    std::vector<std::string> &segment_names) {
    std::unique_lock<std::shared_mutex> lock_(segment_mutex_);
    ErrorCode ret = ErrorCode::OK;
    for (auto& client_id : clients) {
        std::vector<std::shared_ptr<Segment>> segments;
        ret = InnerGetClientSegments(client_id, segments);
        if (ret != ErrorCode::OK) {
            LOG(ERROR) << "fail to inner get client segments"
                       << ", client_id=" << client_id
                       << ", error=" << ret;
            continue;
        }
        for (auto& seg : segments) {
            auto centralized_seg = std::static_pointer_cast<CentralizedSegment>(seg);
            ret = InnerPrepareUnmountSegment(*centralized_seg);
            if (ret != ErrorCode::OK) {
                LOG(ERROR) << "fail to inner prepare unmount segment"
                           << ", client_id=" << client_id
                           << ", segment_name=" << centralized_seg->name
                           << ", error=" << ret;
                continue;
            }
            unmount_segments.push_back(centralized_seg->id);
            dec_capacities.push_back(centralized_seg->size);
            client_ids.push_back(client_id);
            segment_names.push_back(centralized_seg->name);
        }
    }

    return ErrorCode::OK;
}

ErrorCode CentralizedSegmentManager::PrepareUnmountSegment(
    const UUID& segment_id, size_t& metrics_dec_capacity,
    std::string& segment_name) {
    std::unique_lock<std::shared_mutex> lock_(segment_mutex_);
    auto it = mounted_segments_.find(segment_id);
    if (it == mounted_segments_.end()) {
        LOG(WARNING) << "segment_id=" << segment_id
                     << ", warn=segment_not_found";
        return ErrorCode::SEGMENT_NOT_FOUND;
    }
    auto mounted_segment = std::static_pointer_cast<CentralizedSegment>(it->second);
    if (mounted_segment->status == SegmentStatus::UNMOUNTING) {
        LOG(ERROR) << "segment_id=" << segment_id
                   << ", error=segment_is_unmounting";
        return ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS;
    }
    metrics_dec_capacity = mounted_segment->size;
    segment_name = mounted_segment->name;
    ErrorCode res = InnerPrepareUnmountSegment(*mounted_segment);
    if (res != ErrorCode::OK) {
        LOG(ERROR) << "fail to inner prepare unmount segment"
                   << ", segment_id=" << segment_id
                   << ", error=" << res;
        return res;
    }

    return ErrorCode::OK;
}

ErrorCode CentralizedSegmentManager::InnerPrepareUnmountSegment(CentralizedSegment& mounted_segment) {
    // Remove the allocator from the segment manager
    std::shared_ptr<BufferAllocatorBase> allocator = mounted_segment.buf_allocator;

    // 1. Remove from allocators
    if (allocator_manager_.removeAllocator(mounted_segment.name, allocator)) {
        LOG(ERROR) << "Allocator " << mounted_segment.id << " of segment "
                   << mounted_segment.name << " not found in allocator manager";
    }

    // 2. Remove from mounted_segment
    mounted_segment.buf_allocator.reset();

    // Set the segment status to UNMOUNTING
    mounted_segment.status = SegmentStatus::UNMOUNTING;

    return ErrorCode::OK;
}

ErrorCode CentralizedSegmentManager::GetAllSegments(
    std::vector<std::string>& all_segments) {
    std::shared_lock<std::shared_mutex> lock_(segment_mutex_);
    all_segments.clear();
    for (auto& segment_it : mounted_segments_) {
        auto mounted_segment = std::static_pointer_cast<CentralizedSegment>(segment_it.second);
        if (mounted_segment->status == SegmentStatus::OK) {
            all_segments.push_back(mounted_segment->name);
        }
    }
    return ErrorCode::OK;
}

ErrorCode CentralizedSegmentManager::QuerySegments(const std::string& segment,
                                                   size_t& used, size_t& capacity) {
    std::shared_lock<std::shared_mutex> lock_(segment_mutex_);
    const auto& allocators = allocator_manager_.getAllocators(segment);
    if (allocators != nullptr) {
        for (const auto& allocator : *allocators) {
            used += allocator->size();
            capacity += allocator->capacity();
        }
    }

    if (capacity == 0) {
        VLOG(1) << "### DEBUG ### MasterService::QuerySegments(" << segment
                << ") not found!";
        return ErrorCode::SEGMENT_NOT_FOUND;
    }
    return ErrorCode::OK;
}

}  // namespace mooncake
