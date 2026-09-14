#pragma once

#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <utility>
#include <vector>

#include "ha/standby_metadata_store.h"

namespace mooncake {

class HotStandbyService;

class BatchOpLogSnapshotCapture {
   public:
    BatchOpLogSnapshotCapture(BatchOpLogSnapshotCapture&& other) noexcept
        : last_included_seq(other.last_included_seq),
          last_included_batch_id(other.last_included_batch_id),
          producer_view_version(other.producer_view_version),
          segments(std::move(other.segments)),
          cursor_(std::move(other.cursor_)),
          generation_(std::exchange(other.generation_, 0)),
          lease_state_(std::move(other.lease_state_)) {}
    BatchOpLogSnapshotCapture& operator=(
        BatchOpLogSnapshotCapture&& other) noexcept {
        if (this != &other) {
            Release();
            last_included_seq = other.last_included_seq;
            last_included_batch_id = other.last_included_batch_id;
            producer_view_version = other.producer_view_version;
            segments = std::move(other.segments);
            cursor_ = std::move(other.cursor_);
            generation_ = std::exchange(other.generation_, 0);
            lease_state_ = std::move(other.lease_state_);
        }
        return *this;
    }
    BatchOpLogSnapshotCapture(const BatchOpLogSnapshotCapture&) = delete;
    BatchOpLogSnapshotCapture& operator=(const BatchOpLogSnapshotCapture&) =
        delete;
    ~BatchOpLogSnapshotCapture() { Release(); }

    bool done() const { return !lease_state_ || cursor_.done(); }

    uint64_t last_included_seq;
    uint64_t last_included_batch_id;
    ViewVersionId producer_view_version;
    std::vector<StandbySegmentInfo> segments;

   private:
    friend class HotStandbyService;

    struct LeaseState {
        std::condition_variable cv;
        std::mutex mutex;
        bool requested{false};
        bool active{false};
        uint64_t generation{0};
    };

    BatchOpLogSnapshotCapture(uint64_t last_seq, uint64_t last_batch_id,
                              ViewVersionId producer_view,
                              std::vector<StandbySegmentInfo> captured_segments,
                              StandbyMetadataStore::SnapshotCursor cursor,
                              uint64_t generation,
                              std::shared_ptr<LeaseState> lease_state)
        : last_included_seq(last_seq),
          last_included_batch_id(last_batch_id),
          producer_view_version(producer_view),
          segments(std::move(captured_segments)),
          cursor_(std::move(cursor)),
          generation_(generation),
          lease_state_(std::move(lease_state)) {}

    void Release() noexcept {
        auto lease_state = std::move(lease_state_);
        if (!lease_state) {
            return;
        }
        std::lock_guard<std::mutex> lock(lease_state->mutex);
        if (lease_state->active && generation_ == lease_state->generation) {
            lease_state->active = false;
            lease_state->cv.notify_all();
        }
        generation_ = 0;
    }

    StandbyMetadataStore::SnapshotCursor cursor_;
    uint64_t generation_;
    std::shared_ptr<LeaseState> lease_state_;
};

}  // namespace mooncake
