#include "p2p/master/p2p_segment_manager.h"

#include <glog/logging.h>

#include "p2p/master/p2p_master_metric_manager.h"

namespace mooncake {

tl::expected<void, ErrorCode> P2PSegmentManager::MountSegment(
    const P2PSegment& segment) {
    SharedMutexLocker lock(&segment_mutex_);
    auto it = mounted_segments_.find(segment.id);
    if (it != mounted_segments_.end()) {
        LOG(WARNING) << "segment_name=" << segment.name
                     << ", warn=segment_already_exists";
        return tl::make_unexpected(ErrorCode::SEGMENT_ALREADY_EXISTS);
    }
    mounted_segments_[segment.id] = segment;
    total_capacity_ += segment.size;
    total_usage_ += segment.usage;

    const MemoryType type = segment.memory_type;
    if (type == MemoryType::NVME) {
        P2PMasterMetricManager::instance().inc_total_file_capacity(
            segment.size);
        P2PMasterMetricManager::instance().inc_allocated_file_size(
            segment.usage);
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
            segment.name, segment.usage);
    }
    return {};
}

tl::expected<void, ErrorCode> P2PSegmentManager::UnmountSegment(
    const UUID& segment_id) {
    SharedMutexLocker lock(&segment_mutex_);
    auto it = mounted_segments_.find(segment_id);
    if (it == mounted_segments_.end()) {
        LOG(WARNING) << "attempt to unmount segment but it does not exist"
                     << ", segment_id=" << segment_id;
        return tl::make_unexpected(ErrorCode::SEGMENT_NOT_FOUND);
    }

    const P2PSegment& segment = it->second;
    if (segment.size > total_capacity_ || segment.usage > total_usage_) {
        LOG(ERROR) << "segment aggregate underflow during unmount"
                   << ", segment_id=" << segment_id
                   << ", total_capacity=" << total_capacity_
                   << ", segment_size=" << segment.size
                   << ", total_usage=" << total_usage_
                   << ", segment_usage=" << segment.usage;
        total_capacity_ = 0;
        total_usage_ = 0;
    } else {
        total_capacity_ -= segment.size;
        total_usage_ -= segment.usage;
    }

    // Drop capacity and the last reported usage.
    const MemoryType type = segment.memory_type;
    const size_t usage = segment.usage;
    if (type == MemoryType::NVME) {
        P2PMasterMetricManager::instance().dec_total_file_capacity(
            segment.size);
        P2PMasterMetricManager::instance().dec_allocated_file_size(usage);
    } else {
        if (type != MemoryType::DRAM) {
            LOG(WARNING) << "unmounting segment with unsupported memory type, "
                            "counting toward mem capacity"
                         << ", segment_id=" << segment.id
                         << ", name=" << segment.name
                         << ", memory_type=" << MemoryTypeToString(type);
        }
        P2PMasterMetricManager::instance().dec_total_mem_capacity(segment.name,
                                                                  segment.size);
        P2PMasterMetricManager::instance().dec_allocated_mem_size(segment.name,
                                                                  usage);
    }
    mounted_segments_.erase(it);
    return {};
}

tl::expected<std::pair<size_t, size_t>, ErrorCode>
P2PSegmentManager::QuerySegments(const std::string& segment) {
    SharedMutexLocker lock(&segment_mutex_, shared_lock);
    for (const auto& entry : mounted_segments_) {
        if (entry.second.name == segment) {
            return std::make_pair(entry.second.usage, entry.second.size);
        }
    }
    return tl::make_unexpected(ErrorCode::SEGMENT_NOT_FOUND);
}

tl::expected<P2PSegment, ErrorCode> P2PSegmentManager::QuerySegment(
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

tl::expected<void, ErrorCode> P2PSegmentManager::CheckSegmentExists(
    const UUID& segment_id) const {
    SharedMutexLocker lock(&segment_mutex_, shared_lock);
    if (!mounted_segments_.contains(segment_id)) {
        LOG(WARNING) << "CheckSegmentExists: segment not found"
                     << ", segment_id=" << segment_id;
        return tl::make_unexpected(ErrorCode::SEGMENT_NOT_FOUND);
    }
    return {};
}

tl::expected<std::vector<P2PSegment>, ErrorCode>
P2PSegmentManager::GetSegments() {
    SharedMutexLocker lock(&segment_mutex_, shared_lock);
    std::vector<P2PSegment> segments;
    segments.reserve(mounted_segments_.size());
    for (const auto& entry : mounted_segments_) {
        segments.push_back(entry.second);
    }
    return segments;
}

tl::expected<size_t, ErrorCode> P2PSegmentManager::UpdateSegmentUsage(
    const UUID& segment_id, size_t usage) {
    SharedMutexLocker lock(&segment_mutex_);
    auto it = mounted_segments_.find(segment_id);
    if (it == mounted_segments_.end()) {
        LOG(WARNING) << "fail to update segment usage, segment doesn't exist"
                     << ", segment_id=" << segment_id;
        return tl::make_unexpected(ErrorCode::SEGMENT_NOT_FOUND);
    }
    if (usage > it->second.size) {
        LOG(ERROR) << "usage is larger than segment size"
                   << ", segment_id=" << segment_id << ", usage=" << usage
                   << ", segment_size=" << it->second.size;
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    const size_t old_usage = it->second.usage;
    it->second.usage = usage;
    total_usage_ = total_usage_ - old_usage + usage;

    const int64_t delta = usage >= old_usage
                              ? static_cast<int64_t>(usage - old_usage)
                              : -static_cast<int64_t>(old_usage - usage);
    auto& metrics = P2PMasterMetricManager::instance();
    if (it->second.memory_type == MemoryType::NVME) {
        if (delta >= 0) {
            metrics.inc_allocated_file_size(delta);
        } else {
            metrics.dec_allocated_file_size(-delta);
        }
    } else {
        // Unsupported types count toward the mem gauge.
        if (delta >= 0) {
            metrics.inc_allocated_mem_size(it->second.name, delta);
        } else {
            metrics.dec_allocated_mem_size(it->second.name, -delta);
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
        return 0;
    }
    return it->second.usage;
}

void P2PSegmentManager::ForEachSegment(const SegmentVisitor& visitor) const {
    SharedMutexLocker lock(&segment_mutex_, shared_lock);
    for (const auto& [id, segment] : mounted_segments_) {
        if (visitor(segment)) {
            break;
        }
    }
}

std::pair<size_t, size_t> P2PSegmentManager::GetCapacityUsage() const {
    SharedMutexLocker lock(&segment_mutex_, shared_lock);
    return {total_capacity_, total_usage_};
}

}  // namespace mooncake
