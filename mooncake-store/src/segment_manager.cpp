#include "segment_manager.h"

#include <glog/logging.h>
#include <unordered_set>

namespace mooncake {

ErrorCode SegmentManager::MountSegment(const Segment& segment,
                                       const UUID& client_id) {
    ErrorCode ret = ErrorCode::OK;
    std::unique_lock<std::shared_mutex> lock_(segment_mutex_);

    ret = InnerMountSegment(segment, client_id);
    if (ret != ErrorCode::OK) {
        LOG(ERROR) << "fail to inner mount segment"
                   << ", segment_name=" << segment.name
                   << ", ret=" << ret;
        return ret;
    }

    return ret;
}

// TODO:
// the "pre_func" is used to be compatible with the old code (heartbeat mechanism).
// however, it breaks the cohesion of the segment manager abstraction.
// thus, we will refactor it in the future
ErrorCode SegmentManager::MountSegment(const Segment& segment,
                                       const UUID& client_id,
                                       std::function<ErrorCode()>& pre_func) {
    ErrorCode ret = ErrorCode::OK;
    std::unique_lock<std::shared_mutex> lock_(segment_mutex_);

    ret = pre_func();
    if (ret != ErrorCode::OK) {
        LOG(ERROR) << "fail to do pre_func"
                   << ", segment_name=" << segment.name
                   << ", ret=" << ret;
        return ret;
    }

    ret = InnerMountSegment(segment, client_id);
    if (ret != ErrorCode::OK) {
        LOG(ERROR) << "fail to inner mount segment"
                   << ", segment_name=" << segment.name
                   << ", ret=" << ret;
        return ret;
    }

    return ret;
}

// TODO:
// the "pre_func" is used to be compatible with the old code (heartbeat mechanism).
// however, it breaks the cohesion of the segment manager abstraction.
// thus, we will refactor it in the future
ErrorCode SegmentManager::ReMountSegment(const std::vector<Segment>& segments,
                                         const UUID& client_id,
                                         std::function<ErrorCode()>& pre_func) {
    ErrorCode ret = ErrorCode::OK;
    std::unique_lock<std::shared_mutex> lock_(segment_mutex_);
    ret = pre_func();
    if (ret != ErrorCode::OK) {
        LOG(ERROR) << "fail to do pre_func"
                   << ", client_id=" << client_id
                   << ", ret=" << ret;
        return ret;
    }
    ret = InnerReMountSegment(segments, client_id);
    if (ret != ErrorCode::OK) {
        LOG(ERROR) << "fail to inner remount segment"
                   << ", ret=" << ret;
        return ret;
    }

    return ret;
}

ErrorCode SegmentManager::UnmountSegment(const UUID& segment_id, const UUID& client_id) {
    std::unique_lock<std::shared_mutex> lock_(segment_mutex_);
    ErrorCode ret = InnerUnmountSegment(segment_id, client_id);
    if (ret != ErrorCode::OK) {
        LOG(ERROR) << "fail to inner unmount segment"
                   << ", segment_id=" << segment_id
                   << ", ret=" << ret;
        return ret;
    }

    return ErrorCode::OK;
}

ErrorCode SegmentManager::BatchUnmountSegments(const std::vector<UUID>& unmount_segments,
                                               const std::vector<UUID>& client_ids,
                                               const std::vector<std::string>& segment_names) {
    if (unmount_segments.size() != client_ids.size() ||
        unmount_segments.size() != segment_names.size()) {
        LOG(ERROR) << "invalid length"
                   << ", unmount_segments.size()=" << unmount_segments.size()
                   << ", client_ids.size()=" << client_ids.size()
                   << ", segment_names.size()=" << segment_names.size();
        return ErrorCode::INVALID_PARAMS;
    }
    ErrorCode ret = ErrorCode::OK;
    std::unique_lock<std::shared_mutex> lock_(segment_mutex_);
    for (size_t i = 0; i < unmount_segments.size(); i++) {
        ret = InnerUnmountSegment(unmount_segments[i], client_ids[i]);
        if (ret != ErrorCode::OK) {
            LOG(ERROR) << "fail to inner unmount segment"
                       << ", segment_id=" << unmount_segments[i]
                       << ", ret=" << ret;
        } else {
            LOG(INFO) << "client_id=" << client_ids[i]
                      << ", segment_name=" << segment_names[i]
                      << ", action=unmount_expired_segment";
        }
    }

    return ErrorCode::OK;
}

ErrorCode SegmentManager::InnerUnmountSegment(const UUID& segment_id, const UUID& client_id) {
    // Remove from client_segments_
    bool found_in_client_segments = false;
    auto client_it = client_segments_.find(client_id);
    if (client_it != client_segments_.end()) {
        auto& segments = client_it->second;
        auto segment_it =
            std::find(segments.begin(), segments.end(), segment_id);
        if (segment_it != segments.end()) {
            segments.erase(segment_it);
            found_in_client_segments = true;
        }
        if (segments.empty()) {
            client_segments_.erase(client_it);
        }
    }
    if (!found_in_client_segments) {
        LOG(ERROR) << "segment not found in client_segments"
                   << ", segment_id=" << segment_id;
    }

// Remove from mounted_segments_
    mounted_segments_.erase(segment_id);

    return ErrorCode::OK;
}

ErrorCode SegmentManager::GetClientSegments(const UUID& client_id,
                                            std::vector<std::shared_ptr<Segment>>& segments) const {
    std::shared_lock<std::shared_mutex> lock_(segment_mutex_);
    ErrorCode ret = InnerGetClientSegments(client_id, segments);
    if (ret != ErrorCode::OK) {
        LOG(ERROR) << "fail to inner get client segments"
                   << ", client_id=" << client_id
                   << ", ret=" << ret;
        return ret;
    }
    return ErrorCode::OK;
}

ErrorCode SegmentManager::InnerGetClientSegments(
    const UUID& client_id, std::vector<std::shared_ptr<Segment>>& segments) const {
    auto it = client_segments_.find(client_id);
    if (it == client_segments_.end()) {
        LOG(ERROR) << "client not found" << ", client_id=" << client_id;
        return ErrorCode::SEGMENT_NOT_FOUND;
    }
    segments.clear();
    for (auto& segment_id : it->second) {
        auto segment_it = mounted_segments_.find(segment_id);
        if (segment_it != mounted_segments_.end()) {
            segments.emplace_back(segment_it->second);
        }
    }
    return ErrorCode::OK;
}

ErrorCode SegmentManager::QueryIp(const UUID& client_id, std::vector<std::string>& result) {
    std::shared_lock<std::shared_mutex> lock_(segment_mutex_);
    std::vector<std::shared_ptr<Segment>> segments;
    ErrorCode err = InnerGetClientSegments(client_id, segments);
    if (err != ErrorCode::OK) {
        if (err == ErrorCode::SEGMENT_NOT_FOUND) {
            VLOG(1) << "QueryIp: client_id=" << client_id
                    << " not found or has no segments";
            return ErrorCode::CLIENT_NOT_FOUND;
        }

        LOG(ERROR) << "QueryIp: failed to get segments for client_id="
                   << client_id << ", error=" << toString(err);

        return err;
    }

    std::unordered_set<std::string> unique_ips;
    unique_ips.reserve(segments.size());
    for (const auto& segment : segments) {
        if (!segment) {
            err = ErrorCode::INTERNAL_ERROR;
            LOG(ERROR) << "unexpected null segment"
                       << ", client_id=" << client_id
                       << ", ret=" << err;
            return err;
        } else if (!segment->te_endpoint.empty()) {
            size_t colon_pos = segment->te_endpoint.find(':');
            if (colon_pos != std::string::npos) {
                std::string ip = segment->te_endpoint.substr(0, colon_pos);
                unique_ips.emplace(ip);
            } else {
                unique_ips.emplace(segment->te_endpoint);
            }
        }
    }

    if (unique_ips.empty()) {
        LOG(WARNING) << "QueryIp: client_id=" << client_id
                     << " has no valid IP addresses";
        return ErrorCode::OK;
    }
    result.assign(unique_ips.begin(), unique_ips.end());
    return ErrorCode::OK;
}
}  // namespace mooncake
