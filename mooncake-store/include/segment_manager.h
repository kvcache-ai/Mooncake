#pragma once

#include <boost/functional/hash.hpp>
#include <memory>
#include <string>
#include <vector>

#include "mutex.h"
#include "types.h"
#include "ylt/util/tl/expected.hpp"

namespace mooncake {

class SegmentManager {
   public:
    virtual ~SegmentManager() = default;

    auto MountSegment(const Segment& segment) -> tl::expected<void, ErrorCode>;
    auto UnmountSegment(const UUID& segment_id)
        -> tl::expected<void, ErrorCode>;
    virtual auto QuerySegments(const std::string& segment)
        -> tl::expected<std::pair<size_t, size_t>, ErrorCode> = 0;
    auto QuerySegment(const UUID& segment_id)
        -> tl::expected<std::shared_ptr<Segment>, ErrorCode>;
    auto GetSegments() -> tl::expected<std::vector<Segment>, ErrorCode>;

    using SegmentRemovalCallback = std::function<void(const UUID& segment_id)>;
    void SetSegmentRemovalCallback(SegmentRemovalCallback cb);

   protected:
    // Pure virtual inner methods (Implementation specific, No locking required)
    virtual auto InnerMountSegment(const Segment& segment)
        -> tl::expected<void, ErrorCode> = 0;

    virtual auto OnUnmountSegment(const std::shared_ptr<Segment>& segment)
        -> tl::expected<void, ErrorCode> = 0;

   protected:
    mutable SharedMutex segment_mutex_;
    SegmentRemovalCallback segment_removal_cb_;

    std::unordered_map<UUID, std::shared_ptr<Segment>, boost::hash<UUID>>
        mounted_segments_
            GUARDED_BY(segment_mutex_);  // segment_id -> mounted segment
};

}  // namespace mooncake
