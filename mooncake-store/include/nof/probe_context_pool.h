#pragma once

#include <atomic>
#include <memory>
#include <mutex>
#include <stack>
#include <string>
#include <vector>

#include "nof/nvmeof_initiator.h"

namespace mooncake {

// Context for one in-flight probe read, recycled through
// NofProbeContextPool.
struct NofProbeContext {
    std::atomic<bool> done{false};
    std::atomic<bool> success{false};
    std::mutex error_mutex;
    std::string error_reason;
    NofIOAdaptor adaptor{};
    // Segment the probe was submitted on. Used by the initiator teardown
    // drain to find the qpair a timed-out probe is still outstanding on.
    // Null when the context is idle.
    NofSegmentHandle* seg{nullptr};

    void Reset();
};

// Pool of probe contexts with a quarantine lane for timed-out probes.
//
// A probe that times out must NOT go back to the free pool: its NVMe command
// is still in flight and still holds &ctx->adaptor. Recycling it would let
// the next probe Reset() and reuse the context, and when the stale
// completion is processed by a later PollCompletion it would write
// done/success into the NEW probe — the heartbeat would consume the previous
// request's result. Timed-out contexts are quarantined until their original
// callback runs (done == true), then reaped back into the pool by the next
// Acquire(). A command that never completes (dead target) leaves its context
// quarantined forever — one context per dead-target probe, a bounded leak
// that is the price of never recycling an in-flight callback context.
class NofProbeContextPool {
   public:
    NofProbeContext* Acquire();
    // Normal path: the completion callback has already run for this context.
    void Recycle(NofProbeContext* ctx);
    // Timeout path: the command is still in flight. Keeps the context out of
    // the pool until its callback runs (see class comment).
    void Quarantine(NofProbeContext* ctx);

    size_t QuarantinedCount() const;
    // Snapshot of the quarantine lane, for the initiator teardown drain.
    std::vector<NofProbeContext*> QuarantinedSnapshot() const;

   private:
    void ReplenishLocked(size_t count);
    void ReapQuarantineLocked();

    std::vector<std::unique_ptr<NofProbeContext>> contexts_;  // owns all
    std::stack<NofProbeContext*> free_;
    std::vector<NofProbeContext*> quarantine_;
    mutable std::mutex mutex_;
};

}  // namespace mooncake
