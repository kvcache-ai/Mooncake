#include "p2p_segment_manager.h"

namespace mooncake {
ErrorCode P2PSegmentManager::MountSegment(
    const Segment& segment, const UUID& client_id,
    std::function<ErrorCode()>& pre_func) {
    // TODO
    return ErrorCode::OK;
}

ErrorCode P2PSegmentManager::ReMountSegment(
    const std::vector<Segment>& segments, const UUID& client_id,
    std::function<ErrorCode()>& pre_func) {
    // TODO
    return ErrorCode::OK;
}
ErrorCode P2PSegmentManager::UnmountSegment(const UUID& segment_id,
                                            const UUID& client_id) {
    // TODO
    return ErrorCode::OK;
}

ErrorCode P2PSegmentManager::GetClientSegments(
    const UUID& client_id,
    std::vector<std::shared_ptr<Segment>>& segments) const {
    // TODO
    return ErrorCode::OK;
}

ErrorCode P2PSegmentManager::QuerySegments(const std::string& segment,
                                           size_t& used, size_t& capacity) {
    // TODO
    return ErrorCode::OK;
}

ErrorCode P2PSegmentManager::GetAllSegments(
    std::vector<std::string>& all_segments) {
    // TODO
    return ErrorCode::OK;
}
}  // namespace mooncake
