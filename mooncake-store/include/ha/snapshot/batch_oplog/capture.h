#pragma once

#include <cstdint>
#include <utility>
#include <vector>

#include "ha/standby_metadata_store.h"

namespace mooncake {

class HotStandbyService;

class BatchOpLogSnapshotCapture {
   public:
    BatchOpLogSnapshotCapture(BatchOpLogSnapshotCapture&&) = default;
    BatchOpLogSnapshotCapture& operator=(BatchOpLogSnapshotCapture&&) = default;
    BatchOpLogSnapshotCapture(const BatchOpLogSnapshotCapture&) = delete;
    BatchOpLogSnapshotCapture& operator=(const BatchOpLogSnapshotCapture&) =
        delete;

    bool done() const { return cursor_.done(); }

    uint64_t last_included_seq;
    uint64_t last_included_batch_id;
    ViewVersionId producer_view_version;
    std::vector<StandbySegmentInfo> segments;

   private:
    friend class HotStandbyService;

    BatchOpLogSnapshotCapture(uint64_t last_seq, uint64_t last_batch_id,
                              ViewVersionId producer_view,
                              std::vector<StandbySegmentInfo> captured_segments,
                              StandbyMetadataStore::SnapshotCursor cursor,
                              uint64_t generation)
        : last_included_seq(last_seq),
          last_included_batch_id(last_batch_id),
          producer_view_version(producer_view),
          segments(std::move(captured_segments)),
          cursor_(std::move(cursor)),
          generation_(generation) {}

    StandbyMetadataStore::SnapshotCursor cursor_;
    uint64_t generation_;
};

}  // namespace mooncake
