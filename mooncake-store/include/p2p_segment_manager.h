#pragma once

#include "segment_manager.h"

namespace mooncake {
// TODO: this class is a tmp placeholder. it will be implemented later
class P2PSegmentManager : public SegmentManager {
   public:
    friend class SegmentTest;  // for unit tests

    virtual ErrorCode GetAllSegments(std::vector<std::string>& all_segments) override;
    virtual ErrorCode QuerySegments(const std::string& segment, size_t& used,
                                    size_t& capacity) override;
    protected:
     virtual ErrorCode InnerMountSegment(const Segment& segment, const UUID& client_id) override;
     virtual ErrorCode InnerReMountSegment(const std::vector<Segment>& segments,
                                           const UUID& client_id) override;

};

}  // namespace mooncake