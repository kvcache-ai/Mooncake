#pragma once

#include <boost/functional/hash.hpp>
#include <cstddef>
#include <functional>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>
#include <ylt/util/tl/expected.hpp>

#include "mutex.h"
#include "p2p/common/p2p_types.h"

namespace mooncake {

/**
 * @brief Manages the segments mounted by a single P2P client.
 */
class P2PSegmentManager {
   public:
    auto MountSegment(const P2PSegment& segment)
        -> tl::expected<void, ErrorCode>;
    auto UnmountSegment(const UUID& segment_id)
        -> tl::expected<void, ErrorCode>;
    // Segment names are not guaranteed to be unique within the cluster.
    // Returns the first matching segment.
    auto QuerySegments(const std::string& segment)
        -> tl::expected<std::pair<size_t, size_t>, ErrorCode>;
    auto QuerySegment(const UUID& segment_id)
        -> tl::expected<P2PSegment, ErrorCode>;
    auto CheckSegmentExists(const UUID& segment_id) const
        -> tl::expected<void, ErrorCode>;
    auto GetSegments() -> tl::expected<std::vector<P2PSegment>, ErrorCode>;

    /**
     * @brief update segment usage and return old usage
     */
    auto UpdateSegmentUsage(const UUID& segment_id, size_t usage)
        -> tl::expected<size_t, ErrorCode>;

    /**
     * @brief get segment usage
     */
    size_t GetSegmentUsage(const UUID& segment_id) const;

    /**
     * @brief Iterate over all mounted P2P segments under a single read lock.
     *        Visitor returns true to stop early.
     */
    using SegmentVisitor = std::function<bool(const P2PSegment& segment)>;
    void ForEachSegment(const SegmentVisitor& visitor) const;

    std::pair<size_t, size_t> GetCapacityUsage() const;

   private:
    mutable SharedMutex segment_mutex_;
    std::unordered_map<UUID, P2PSegment, boost::hash<UUID>> mounted_segments_
        GUARDED_BY(segment_mutex_);  // segment_id -> mounted segment
    size_t total_capacity_ GUARDED_BY(segment_mutex_){0};
    size_t total_usage_ GUARDED_BY(segment_mutex_){0};
};

}  // namespace mooncake
