#include "segment_manager.h"
#include "master_metric_manager.h"
#include <glog/logging.h>

namespace mooncake {

tl::expected<void, ErrorCode> SegmentManager::MountSegment(
    const Segment& segment) {
    SharedMutexLocker lock(&segment_mutex_);
    auto it = mounted_segments_.find(segment.id);
    if (it != mounted_segments_.end()) {
        LOG(WARNING) << "segment_name=" << segment.name
                     << ", warn=segment_already_exists";
        return tl::make_unexpected(ErrorCode::SEGMENT_ALREADY_EXISTS);
    }
    auto ret = InnerMountSegment(segment);
    if (!ret.has_value()) {
        LOG(ERROR) << "fail to mount segment"
                   << ", segment_id=" << segment.id
                   << ", segment_name=" << segment.name
                   << ", segment_size=" << segment.size
                   << ", ret=" << ret.error();
        return ret;
    }
    MasterMetricManager::instance().inc_total_mem_capacity(segment.name,
                                                           segment.size);
    return {};
}

tl::expected<void, ErrorCode> SegmentManager::UnmountSegment(
    const UUID& segment_id) {
    std::string segment_name;
    size_t segment_size = 0;
    {
        // Phase 1: Remove segment under segment_mutex_ (fast).
        // For centralized structure, after this its allocator is removed from
        // global_allocator_manager_ (via allocator_change_cb_).
        SharedMutexLocker lock(&segment_mutex_);
        auto it = mounted_segments_.find(segment_id);
        if (it == mounted_segments_.end()) {
            LOG(WARNING) << "attempt to unmount segment but it does not exist";
            return tl::make_unexpected(ErrorCode::SEGMENT_NOT_FOUND);
        }
        auto ret = OnUnmountSegment(it->second);
        if (!ret.has_value()) {
            LOG(ERROR) << "fail to unmount segment"
                       << ", segment_id=" << segment_id
                       << ", ret=" << ret.error();
            return ret;
        }
        segment_name = it->second->name;
        segment_size = it->second->size;
        mounted_segments_.erase(it);
    }
    // Phase 2: Clean up metadata referencing this segment WITHOUT holding
    // segment_mutex_. This avoids blocking MountSegment/QuerySegments etc.
    if (segment_removal_cb_) {
        segment_removal_cb_(segment_id);
    }
    MasterMetricManager::instance().dec_total_mem_capacity(segment_name,
                                                           segment_size);
    return {};
}

tl::expected<std::vector<Segment>, ErrorCode> SegmentManager::GetSegments() {
    SharedMutexLocker lock(&segment_mutex_, shared_lock);
    std::vector<Segment> segments;
    for (const auto& entry : mounted_segments_) {
        segments.push_back(*entry.second);
    }
    return segments;
}

tl::expected<std::shared_ptr<Segment>, ErrorCode> SegmentManager::QuerySegment(
    const UUID& segment_id) {
    SharedMutexLocker lock(&segment_mutex_, shared_lock);
    auto it = mounted_segments_.find(segment_id);
    if (it == mounted_segments_.end()) {
        LOG(WARNING) << "QuerySegment: segment not found"
                     << ", segment_id=" << segment_id;
        return tl::make_unexpected(ErrorCode::SEGMENT_NOT_FOUND);
    }
    return it->second;
}

void SegmentManager::SetSegmentRemovalCallback(SegmentRemovalCallback cb) {
    segment_removal_cb_ = std::move(cb);
}

}  // namespace mooncake
