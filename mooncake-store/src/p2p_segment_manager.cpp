#include "p2p_segment_manager.h"

namespace mooncake {

ErrorCode P2PSegmentManager::InnerMountSegment(const Segment& segment,
                                               const UUID& client_id) {
    // TODO

    return ErrorCode::OK;
}

ErrorCode P2PSegmentManager::InnerReMountSegment(
    const std::vector<Segment>& segments, const UUID& client_id) {
    // TODO

    return ErrorCode::OK;
}

ErrorCode P2PSegmentManager::GetAllSegments(
    std::vector<std::string>& all_segments) {
    std::unique_lock<std::shared_mutex> lock_(segment_mutex_);
    all_segments.clear();
    for (auto& segment_it : mounted_segments_) {
        all_segments.push_back(segment_it.second->name);
    }
    return ErrorCode::OK;
}

ErrorCode P2PSegmentManager::QuerySegments(const std::string& segment,
                                           size_t& used, size_t& capacity) {
    // TODO
    return ErrorCode::OK;
}

}  // namespace mooncake
