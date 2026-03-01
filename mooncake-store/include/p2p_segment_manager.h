#pragma once

#include <boost/functional/hash.hpp>
#include "segment_manager.h"
#include "types.h"

namespace mooncake {
class P2PSegmentManager : public SegmentManager {
   public:
    auto QuerySegments(const std::string& segment)
        -> tl::expected<std::pair<size_t, size_t>, ErrorCode> override;

    using OnSegmentAddedCallback = std::function<void(const Segment& segment)>;
    using OnSegmentRemovedCallback =
        std::function<void(const Segment& segment)>;

    void SetSegmentChangeCallbacks(OnSegmentAddedCallback on_add,
                                   OnSegmentRemovedCallback on_remove) {
        on_segment_added_ = std::move(on_add);
        on_segment_removed_ = std::move(on_remove);
    }
    /**
     * @brief update segment usage and return old usage
     */
    tl::expected<size_t, ErrorCode> UpdateSegmentUsage(const UUID& segment_id,
                                                       size_t usage);

    /**
     * @brief get segment usage
     */
    size_t GetSegmentUsage(const UUID& segment_id) const;

    /**
     * @brief Iterate over all mounted P2P segments under a single read lock.
     *        Visitor returns true to stop early.
     */
    using SegmentVisitor = std::function<bool(const Segment& segment)>;
    void ForEachSegment(const SegmentVisitor& visitor) const;

   protected:
    tl::expected<void, ErrorCode> InnerMountSegment(
        const Segment& segment) override;

    tl::expected<void, ErrorCode> OnUnmountSegment(
        const std::shared_ptr<Segment>& segment) override;

   private:
    OnSegmentAddedCallback on_segment_added_;
    OnSegmentRemovedCallback on_segment_removed_;
};

}  // namespace mooncake