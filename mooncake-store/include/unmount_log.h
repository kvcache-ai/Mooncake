#pragma once

#include <string>

namespace mooncake {

// Submission state of one segment's SEGMENT_UNMOUNT entry, shared by every
// party that may append it: the unmount RPC, the offboarding job that keeps
// retrying its segment, and the background flush. They hold the same object,
// so a retry that arrives after the master dropped its index entry (the entry
// is logged) still observes kLogged and does not append a second unmount --
// which the standby would apply after a later mount of the same endpoint.
//
// The state is reclaimed with the last holder: the index drops its reference
// once the entry is logged, and a job holding it keeps it only until the job
// finishes. Memory therefore tracks live responsibilities, with no history to
// prune and no cleanup thread.
struct SegmentUnmountLogState {
    enum class Phase {
        kPending,   // remembered, waiting for a submitter
        kInFlight,  // one submitter owns it right now
        kLogged,    // the entry reached the writer; never submit again
    };

    Phase phase{Phase::kPending};
    std::string endpoint;
    std::string payload;
};

}  // namespace mooncake
