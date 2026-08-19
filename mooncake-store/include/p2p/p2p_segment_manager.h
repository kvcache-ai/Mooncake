#pragma once

#include <boost/functional/hash.hpp>
#include <memory>
#include <string>
#include <vector>

#include "mutex.h"
#include "p2p/p2p_types.h"
#include "types.h"
#include "ylt/util/tl/expected.hpp"

namespace mooncake {

class P2PSegmentManager {
   public:
    using SegmentRemovalCallback = std::function<void(const UUID& segment_id)>;
    using OnSegmentAddedCallback = std::function<void(const Segment& segment)>;
    using OnSegmentRemovedCallback =
        std::function<void(const Segment& segment)>;
    using SegmentVisitor = std::function<bool(const Segment& segment)>;

    auto MountSegment(const Segment& segment) -> tl::expected<void, ErrorCode>;
    auto UnmountSegment(const UUID& segment_id)
        -> tl::expected<void, ErrorCode>;
    auto QuerySegments(const std::string& segment)
        -> tl::expected<std::pair<size_t, size_t>, ErrorCode>;
    auto QuerySegment(const UUID& segment_id)
        -> tl::expected<std::shared_ptr<Segment>, ErrorCode>;
    auto GetSegments() -> tl::expected<std::vector<Segment>, ErrorCode>;

    void SetSegmentRemovalCallback(SegmentRemovalCallback cb);

    void SetSegmentChangeCallbacks(OnSegmentAddedCallback on_add,
                                   OnSegmentRemovedCallback on_remove) {
        on_segment_added_ = std::move(on_add);
        on_segment_removed_ = std::move(on_remove);
    }

    tl::expected<size_t, ErrorCode> UpdateSegmentUsage(const UUID& segment_id,
                                                       size_t usage);

    size_t GetSegmentUsage(const UUID& segment_id) const;

    void ForEachSegment(const SegmentVisitor& visitor) const;

   private:
    tl::expected<void, ErrorCode> InnerMountSegment(const Segment& segment);
    tl::expected<void, ErrorCode> OnUnmountSegment(
        const std::shared_ptr<Segment>& segment);

    mutable SharedMutex segment_mutex_;
    SegmentRemovalCallback segment_removal_cb_;
    OnSegmentAddedCallback on_segment_added_;
    OnSegmentRemovedCallback on_segment_removed_;

    std::unordered_map<UUID, std::shared_ptr<Segment>, boost::hash<UUID>>
        mounted_segments_
            GUARDED_BY(segment_mutex_);
};

}  // namespace mooncake