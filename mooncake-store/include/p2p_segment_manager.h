#pragma once

#include <boost/functional/hash.hpp>
#include "mutex.h"
#include "types.h"

namespace mooncake {
// TODO: this class is a tmp placeholder. it will be implemented later
class P2PSegmentManager {
   public:
    ErrorCode MountSegment(const Segment& segment, const UUID& client_id,
                           std::function<ErrorCode()>& pre_func);

    ErrorCode ReMountSegment(const std::vector<Segment>& segments,
                             const UUID& client_id,
                             std::function<ErrorCode()>& pre_func);

    ErrorCode UnmountSegment(const UUID& segment_id, const UUID& client_id);

    ErrorCode GetClientSegments(
        const UUID& client_id,
        std::vector<std::shared_ptr<Segment>>& segments) const;

    ErrorCode QuerySegments(const std::string& segment, size_t& used,
                            size_t& capacity);
    ErrorCode GetAllSegments(std::vector<std::string>& all_segments);

   private:
    mutable SharedMutex segment_mutex_;
    std::unordered_map<UUID, std::shared_ptr<Segment>, boost::hash<UUID>>
        mounted_segments_
            GUARDED_BY(segment_mutex_);  // segment_id -> mounted segment
    std::unordered_map<UUID, std::vector<UUID>, boost::hash<UUID>>
        client_segments_
            GUARDED_BY(segment_mutex_);  // client_id -> segment_ids
};

}  // namespace mooncake