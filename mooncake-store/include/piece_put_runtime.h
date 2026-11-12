#pragma once

#include <atomic>
#include <cassert>
#include <cstddef>
#include <utility>

#include "piece_put_state.h"
#include "segment_allocator_registration.h"

namespace mooncake {

// The key owns this alongside its PROCESSING metadata. The fixed round charge
// remains held throughout RETIRING, including unresolved TCP writes.
struct PiecePutRuntime {
    PiecePutRuntime(PiecePutState&& initial_state,
                    SegmentWriteHold&& write_hold, std::atomic<size_t>& rounds)
        : state(std::move(initial_state)),
          hold(std::move(write_hold)),
          charged_rounds(&rounds) {}
    ~PiecePutRuntime() { ReleaseCharge(); }

    // A safely erased round can return its budget before this hold is
    // destroyed outside the key lock. Destruction must not return it twice.
    void ReleaseCharge() noexcept {
        if (auto* rounds = std::exchange(charged_rounds, nullptr)) {
            const auto previous =
                rounds->fetch_sub(1, std::memory_order_relaxed);
            assert(previous != 0);
            (void)previous;
        }
    }
    PiecePutRuntime(const PiecePutRuntime&) = delete;
    PiecePutRuntime& operator=(const PiecePutRuntime&) = delete;

    PiecePutState state;
    SegmentWriteHold hold;
    std::atomic<size_t>* charged_rounds;
};

}  // namespace mooncake
