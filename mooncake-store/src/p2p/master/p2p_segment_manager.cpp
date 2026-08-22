#include "p2p/master/p2p_segment_manager.h"
#include "p2p/master/p2p_master_metric_manager.h"
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

    const MemoryType type = new_segment->GetP2PExtra().memory_type;
    if (type == MemoryType::NVME) {
        P2PMasterMetricManager::instance().inc_total_file_capacity(
            segment.size);
        P2PMasterMetricManager::instance().inc_allocated_file_size(
            segment.GetP2PExtra().usage);
    } else {
        if (type != MemoryType::DRAM) {
            LOG(WARNING) << "mounting segment with unsupported memory type, "
                            "counting toward mem capacity"
                         << ", segment_id=" << segment.id
                         << ", name=" << segment.name
                         << ", memory_type=" << MemoryTypeToString(type);
        }
        P2PMasterMetricManager::instance().inc_total_mem_capacity(segment.name,
                                                                  segment.size);
        P2PMasterMetricManager::instance().inc_allocated_mem_size(
            segment.name, segment.GetP2PExtra().usage);
    }
    return {};
}

tl::expected<void, ErrorCode> P2PSegmentManager::OnUnmountSegment(
    const std::shared_ptr<Segment>& segment) {
    if (on_segment_removed_) {
        on_segment_removed_(*segment);
    }

    // drop capacity and the last reported usage
    const MemoryType type = segment->GetP2PExtra().memory_type;
    const size_t usage = segment->GetP2PExtra().usage;
    if (type == MemoryType::NVME) {
        P2PMasterMetricManager::instance().dec_total_file_capacity(
            segment->size);
        P2PMasterMetricManager::instance().dec_allocated_file_size(usage);
    } else {
        if (type != MemoryType::DRAM) {
            LOG(WARNING) << "unmounting segment with unsupported memory type, "
                            "counting toward mem capacity"
                         << ", segment_id=" << segment->id
                         << ", name=" << segment->name
                         << ", memory_type=" << MemoryTypeToString(type);
        }
        P2PMasterMetricManager::instance().dec_total_mem_capacity(
            segment->name, segment->size);
        P2PMasterMetricManager::instance().dec_allocated_mem_size(segment->name,
                                                                  usage);
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
    const size_t old_usage = it->second->GetP2PExtra().usage;
    it->second->GetP2PExtra().usage = usage;

    const MemoryType memory_type = it->second->GetP2PExtra().memory_type;
    const int64_t delta = usage >= old_usage
                              ? static_cast<int64_t>(usage - old_usage)
                              : -static_cast<int64_t>(old_usage - usage);
    auto& metrics = P2PMasterMetricManager::instance();
    if (memory_type == MemoryType::NVME) {
        if (delta >= 0) {
            metrics.inc_allocated_file_size(delta);
        } else {
            metrics.dec_allocated_file_size(-delta);
        }
    } else {
        // Unsupported types count toward the mem gauge.
        if (delta >= 0) {
            metrics.inc_allocated_mem_size(it->second->name, delta);
        } else {
            metrics.dec_allocated_mem_size(it->second->name, -delta);
        }
    }
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
