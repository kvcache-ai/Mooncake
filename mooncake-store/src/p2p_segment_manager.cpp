#include "p2p_segment_manager.h"
#include <glog/logging.h>

namespace mooncake {

tl::expected<std::pair<size_t, size_t>, ErrorCode>
P2PSegmentManager::QuerySegments(const std::string& segment) {
    SharedMutexLocker lock_(&segment_mutex_, shared_lock);
    bool found = false;
    size_t capacity = 0;
    size_t used = 0;
    for (const auto& entry : mounted_segments_) {
        if (entry.second->name == segment) {
            capacity += entry.second->size;
            if (entry.second->IsP2PSegment()) {
                used += entry.second->GetP2PExtra().usage;
            }
            found = true;
            break;
        }
    }

    if (!found) {
        return tl::make_unexpected(ErrorCode::SEGMENT_NOT_FOUND);
    }

    return std::make_pair(used, capacity);
}

tl::expected<void, ErrorCode> P2PSegmentManager::InnerMountSegment(
    const Segment& segment) {
    if (!segment.IsP2PSegment()) {
        LOG(ERROR) << "P2PSegmentManager only supports P2PSegmentExtraData";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    auto new_segment = std::make_shared<Segment>(segment);
    mounted_segments_[new_segment->id] = new_segment;

    if (on_segment_added_) {
        on_segment_added_(*new_segment);
    }

    return {};
}

tl::expected<void, ErrorCode> P2PSegmentManager::OnUnmountSegment(
    const std::shared_ptr<Segment>& segment) {
    if (on_segment_removed_) {
        on_segment_removed_(*segment);
    }
    return {};
}

tl::expected<size_t, ErrorCode> P2PSegmentManager::UpdateSegmentUsage(
    const UUID& segment_id, size_t usage) {
    SharedMutexLocker lock(&segment_mutex_);
    auto it = mounted_segments_.find(segment_id);
    if (it == mounted_segments_.end()) {
        LOG(WARNING) << "fail to update segment usage, segment doesn't exist"
                     << ", segment_id: " << segment_id;
        return tl::make_unexpected(ErrorCode::SEGMENT_NOT_FOUND);
    } else if (!it->second->IsP2PSegment()) {
        LOG(ERROR) << "unexpected segment type"
                   << ", segment_id=" << segment_id;
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    } else if (usage > it->second->size) {
        LOG(ERROR) << "usage is larger than segment size"
                   << ", segment_id=" << segment_id << ", usage=" << usage
                   << ", segment_size=" << it->second->size;
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    size_t old_usage = it->second->GetP2PExtra().usage;
    it->second->GetP2PExtra().usage = usage;
    return old_usage;
}

size_t P2PSegmentManager::GetSegmentUsage(const UUID& segment_id) const {
    SharedMutexLocker lock(&segment_mutex_, shared_lock);
    auto it = mounted_segments_.find(segment_id);
    if (it == mounted_segments_.end()) {
        LOG(WARNING) << "segment does not exist"
                     << ", segment_id=" << segment_id;
    } else if (!it->second->IsP2PSegment()) {
        LOG(ERROR) << "unexpected segment type"
                   << ", segment_id=" << segment_id;
    } else {
        return it->second->GetP2PExtra().usage;
    }
    return 0;
}

void P2PSegmentManager::ForEachSegment(const SegmentVisitor& visitor) const {
    SharedMutexLocker lock(&segment_mutex_, shared_lock);
    for (const auto& [id, segment] : mounted_segments_) {
        if (!segment->IsP2PSegment()) {
            LOG(ERROR) << "P2PSegmentManager only supports P2PSegmentExtraData"
                       << ", segment_id: " << segment->id;
            continue;
        }
        if (visitor(*segment)) {
            break;
        }
    }
}

}  // namespace mooncake
