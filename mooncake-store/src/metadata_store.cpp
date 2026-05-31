#include "metadata_store.h"

namespace mooncake {

void StandbySegmentRegistry::OnSegmentMount(const StandbySegmentInfo& info) {
    std::lock_guard<std::shared_mutex> lock(mutex_);
    segments_by_endpoint_[info.transport_endpoint] = info;
}

void StandbySegmentRegistry::OnSegmentUnmount(const std::string& transport_endpoint) {
    std::lock_guard<std::shared_mutex> lock(mutex_);
    segments_by_endpoint_.erase(transport_endpoint);
}

void StandbySegmentRegistry::OnSegmentUpdate(const StandbySegmentInfo& info) {
    std::lock_guard<std::shared_mutex> lock(mutex_);
    segments_by_endpoint_[info.transport_endpoint] = info;
}

bool StandbySegmentRegistry::HasSegment(const std::string& transport_endpoint) const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    return segments_by_endpoint_.find(transport_endpoint) != segments_by_endpoint_.end();
}

std::optional<StandbySegmentInfo> StandbySegmentRegistry::GetSegment(
    const std::string& transport_endpoint) const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    auto it = segments_by_endpoint_.find(transport_endpoint);
    if (it == segments_by_endpoint_.end()) {
        return std::nullopt;
    }
    return it->second;
}

std::vector<StandbySegmentInfo> StandbySegmentRegistry::GetAllSegments() const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    std::vector<StandbySegmentInfo> result;
    result.reserve(segments_by_endpoint_.size());
    for (const auto& [endpoint, info] : segments_by_endpoint_) {
        result.push_back(info);
    }
    return result;
}

void StandbySegmentRegistry::Clear() {
    std::lock_guard<std::shared_mutex> lock(mutex_);
    segments_by_endpoint_.clear();
}

}  // namespace mooncake