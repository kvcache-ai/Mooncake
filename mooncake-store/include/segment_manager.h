#pragma once

#include <boost/functional/hash.hpp>
#include <functional>
#include <ostream>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>
#include <mutex>

#include "types.h"

namespace mooncake {

class SegmentManager {
   public:
    virtual ~SegmentManager() = default;

    ErrorCode MountSegment(const Segment& segment, const UUID& client_id);
    ErrorCode MountSegment(const Segment& segment, const UUID& client_id,
                           std::function<ErrorCode()>& pre_func);

    ErrorCode ReMountSegment(const std::vector<Segment>& segments,
                             const UUID& client_id,
                             std::function<ErrorCode()>& pre_func);

    ErrorCode UnmountSegment(const UUID& segment_id,
                             const UUID& client_id);
    ErrorCode BatchUnmountSegments(const std::vector<UUID>& unmount_segments,
                                   const std::vector<UUID>& client_ids,
                                   const std::vector<std::string>& segment_names);

    /**
     * @brief Get all the segments of a client
     */
    ErrorCode GetClientSegments(const UUID& client_id,
                                std::vector<std::shared_ptr<Segment>>& segments) const;

    /**
     * @brief Get the names of all the segments
     */
    virtual ErrorCode GetAllSegments(std::vector<std::string>& all_segments) = 0;

    /**
     * @brief Get the segment by name. If there are multiple segments with the
     * same name, return the first one.
     */
    virtual ErrorCode QuerySegments(const std::string& segment, size_t& used,
                                    size_t& capacity) = 0;

    ErrorCode QueryIp(const UUID& client_id, std::vector<std::string>& result);
   protected:
     virtual ErrorCode InnerMountSegment(const Segment& segment, const UUID& client_id) = 0;
     virtual ErrorCode InnerReMountSegment(const std::vector<Segment>& segments,
                                           const UUID& client_id) = 0;
     ErrorCode InnerUnmountSegment(const UUID& segment_id, const UUID& client_id);
     ErrorCode InnerGetClientSegments(const UUID& client_id,
                                      std::vector<std::shared_ptr<Segment>>& segments) const;

   protected:
    mutable std::shared_mutex segment_mutex_;
    std::unordered_map<UUID, std::shared_ptr<Segment>, boost::hash<UUID>>
        mounted_segments_;  // segment_id -> mounted segment
    std::unordered_map<UUID, std::vector<UUID>, boost::hash<UUID>>
        client_segments_;  // client_id -> segment_ids

    friend class SegmentTest;  // for unit tests
};

}  // namespace mooncake
