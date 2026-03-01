#include "centralized_segment_manager.h"
#include <unordered_set>
#include <glog/logging.h>

namespace mooncake {
void CentralizedSegmentManager::SetAllocatorChangeCallback(
    AllocatorChangeCallback cb) {
    allocator_change_cb_ = std::move(cb);
}
tl::expected<void, ErrorCode> CentralizedSegmentManager::SetGlobalVisibility(
    bool visible) {
    SharedMutexLocker lock_(&segment_mutex_, shared_lock);
    if (!allocator_change_cb_) {
        LOG(ERROR) << "allocator_change_cb_ is null";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    ErrorCode first_error = ErrorCode::OK;
    for (const auto& entry : mounted_segments_) {
        auto mounted_seg =
            std::static_pointer_cast<MountedCentralizedSegment>(entry.second);
        if (mounted_seg->buf_allocator) {
            auto ret = allocator_change_cb_(
                mounted_seg->name, mounted_seg->buf_allocator, visible);
            if (!ret.has_value()) {
                LOG(ERROR) << "SetGlobalVisibility failed for segment="
                           << mounted_seg->name << " visible=" << visible
                           << " error=" << ret.error();
                if (first_error == ErrorCode::OK) {
                    first_error = ret.error();
                }
            }
        }
    }

    if (first_error != ErrorCode::OK) {
        return tl::make_unexpected(first_error);
    }
    return {};
}

ErrorCode CentralizedSegmentManager::InnerCheckMountSegment(
    const Segment& segment) {
    if (!segment.IsCentralizedSegment()) {
        LOG(ERROR) << "segment is not centralized";
        return ErrorCode::INVALID_PARAMS;
    }
    const uintptr_t buffer = segment.GetCentralizedExtra().base;
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
        LOG(WARNING) << "segment_name=" << segment.name
                     << ", warn=segment_already_exists";
        return ErrorCode::SEGMENT_ALREADY_EXISTS;
    }

    return ErrorCode::OK;
}

tl::expected<void, ErrorCode> CentralizedSegmentManager::InnerMountSegment(
    const Segment& segment) {
    ErrorCode ret = InnerCheckMountSegment(segment);
    if (ret != ErrorCode::OK) {
        LOG(ERROR) << "fail to inner check mount segment"
                   << ", segment_name=" << segment.name << ", ret=" << ret;
        return tl::make_unexpected(ret);
    }
    const uintptr_t buffer = segment.GetCentralizedExtra().base;
    const size_t size = segment.size;
    std::shared_ptr<BufferAllocatorBase> allocator;
    // CachelibBufferAllocator may throw an exception if the size or base is
    // invalid for the slab allocator.
    try {
        // Create allocator based on the configured type
        switch (memory_allocator_) {
            case BufferAllocatorType::CACHELIB:
                allocator = std::make_shared<CachelibBufferAllocator>(
                    segment.name, buffer, size,
                    segment.GetCentralizedExtra().te_endpoint, segment.id);
                break;
            case BufferAllocatorType::OFFSET:
                allocator = std::make_shared<OffsetBufferAllocator>(
                    segment.name, buffer, size,
                    segment.GetCentralizedExtra().te_endpoint, segment.id);
                break;
            default:
                LOG(ERROR) << "segment_name=" << segment.name
                           << ", error=unknown_memory_allocator="
                           << static_cast<int>(memory_allocator_);
                return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }

        if (!allocator) {
            LOG(ERROR) << "segment_name=" << segment.name
                       << ", error=failed_to_create_allocator";
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
    } catch (...) {
        LOG(ERROR) << "segment_name=" << segment.name
                   << ", error=exception_during_allocator_creation";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    auto mounted_segment = std::make_shared<MountedCentralizedSegment>();
    static_cast<Segment&>(*mounted_segment) = segment;
    mounted_segment->buf_allocator = allocator;

    if (allocator_change_cb_) {
        // Callback to add the allocator to the global manager
        auto ret = allocator_change_cb_(mounted_segment->name,
                                        mounted_segment->buf_allocator, true);
        if (!ret.has_value()) {
            LOG(ERROR) << "Failed to add allocator to global manager. "
                          "Rolling back mount operation. "
                       << "segment=" << mounted_segment->name
                       << " error=" << ret.error();
            return tl::make_unexpected(ret.error());
        }
    }
    mounted_segments_[mounted_segment->id] = mounted_segment;

    return {};
}

tl::expected<void, ErrorCode> CentralizedSegmentManager::MountLocalDiskSegment(
    bool enable_offloading) {
    SharedMutexLocker lock_(&segment_mutex_);
    if (local_disk_segment_) {
        LOG(WARNING) << "warn=local_disk_segment_already_exists";
        return tl::make_unexpected(ErrorCode::SEGMENT_ALREADY_EXISTS);
    }
    local_disk_segment_ = std::make_shared<LocalDiskSegment>(enable_offloading);
    return {};
}

auto CentralizedSegmentManager::OffloadObjectHeartbeat(bool enable_offloading)
    -> tl::expected<std::unordered_map<std::string, int64_t>, ErrorCode> {
    SharedMutexLocker lock_(&segment_mutex_, shared_lock);
    if (!local_disk_segment_) {
        LOG(ERROR) << "Local disk segment not found";
        return tl::make_unexpected(ErrorCode::SEGMENT_NOT_FOUND);
    }
    MutexLocker locker(&local_disk_segment_->offloading_mutex_);
    local_disk_segment_->enable_offloading = enable_offloading;
    if (enable_offloading) {
        return std::move(local_disk_segment_->offloading_objects);
    }
    return {};
}

tl::expected<void, ErrorCode> CentralizedSegmentManager::PushOffloadingQueue(
    const std::string& key, const int64_t size,
    const std::string& segment_name) {
    SharedMutexLocker lock_(&segment_mutex_, shared_lock);
    if (!local_disk_segment_) {
        LOG(ERROR) << "Local disk segment not found";
        return tl::make_unexpected(ErrorCode::UNABLE_OFFLOADING);
    }

    // Check whether segment belongs to this client
    bool found = false;
    for (const auto& entry : mounted_segments_) {
        if (entry.second->name == segment_name) {
            found = true;
            break;
        }
    }
    if (!found) {
        LOG(ERROR) << "Segment " << segment_name << " not found";
        return tl::make_unexpected(ErrorCode::SEGMENT_NOT_FOUND);
    }

    MutexLocker locker(&local_disk_segment_->offloading_mutex_);
    if (!local_disk_segment_->enable_offloading) {
        LOG(ERROR) << "Offloading is not enabled";
        return tl::make_unexpected(ErrorCode::UNABLE_OFFLOADING);
    }
    if (local_disk_segment_->offloading_objects.size() >=
        OFFLOADING_QUEUE_LIMIT) {
        LOG(ERROR) << "Offloading queue is full";
        return tl::make_unexpected(ErrorCode::KEYS_ULTRA_LIMIT);
    }
    local_disk_segment_->offloading_objects.emplace(key, size);
    return {};
}

tl::expected<std::vector<std::string>, ErrorCode>
CentralizedSegmentManager::QueryIp() {
    SharedMutexLocker lock_(&segment_mutex_, shared_lock);

    std::unordered_set<std::string> unique_ips;
    // Iterate mounted segments for this client
    for (const auto& entry : mounted_segments_) {
        const auto& segment = entry.second;
        if (!segment) {
            LOG(ERROR) << "unexpected null segment";
            return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
        } else if (!segment->IsCentralizedSegment()) {
            LOG(ERROR) << "unexpected segment type"
                       << ", segment_id=" << segment->id
                       << ", segment_name=" << segment->name;
            return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
        }

        const auto& extra = segment->GetCentralizedExtra();
        if (!extra.te_endpoint.empty()) {
            size_t colon_pos = extra.te_endpoint.find(':');
            if (colon_pos != std::string::npos) {
                std::string ip = extra.te_endpoint.substr(0, colon_pos);
                unique_ips.emplace(ip);
            } else {
                unique_ips.emplace(extra.te_endpoint);
            }
        }
    }

    if (unique_ips.empty()) {
        LOG(WARNING) << "QueryIp: has no valid IP addresses";
        return std::vector<std::string>{};
    }
    return std::vector<std::string>(unique_ips.begin(), unique_ips.end());
}

tl::expected<std::pair<size_t, size_t>, ErrorCode>
CentralizedSegmentManager::QuerySegments(const std::string& segment) {
    SharedMutexLocker lock_(&segment_mutex_, shared_lock);
    for (const auto& entry : mounted_segments_) {
        if (entry.second->name == segment) {
            auto mounted_seg =
                std::static_pointer_cast<MountedCentralizedSegment>(
                    entry.second);
            if (mounted_seg->buf_allocator) {
                return std::make_pair(mounted_seg->buf_allocator->size(),
                                      mounted_seg->buf_allocator->capacity());
            } else {
                LOG(ERROR) << "unexpected null allocator";
                return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
            }
        }
    }
    LOG(WARNING) << "the segment doesn't exist" << ", segment=" << segment;
    return tl::make_unexpected(ErrorCode::SEGMENT_NOT_FOUND);
}

auto CentralizedSegmentManager::OnUnmountSegment(
    const std::shared_ptr<Segment>& segment) -> tl::expected<void, ErrorCode> {
    auto mounted_seg =
        std::static_pointer_cast<MountedCentralizedSegment>(segment);

    // Remove allocator from upper-level global_allocator_manager_ via callback
    if (mounted_seg->buf_allocator) {
        if (allocator_change_cb_) {
            allocator_change_cb_(segment->name, mounted_seg->buf_allocator,
                                 /*is_add=*/false);
        }
    }

    return {};
}

}  // namespace mooncake
