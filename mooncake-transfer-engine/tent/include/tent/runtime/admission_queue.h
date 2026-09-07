// Copyright 2026 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef ADMISSION_QUEUE_H_
#define ADMISSION_QUEUE_H_

#include <cstddef>
#include <cstdint>
#include <deque>
#include <functional>
#include <map>
#include <utility>
#include <vector>

#include "tent/common/status.h"
#include "tent/common/types.h"

namespace mooncake {
namespace tent {

using QueueOwnerId = uint64_t;

// Owner kind is used only for admission accounting. It is not a dispatch
// priority.
enum class QueueOwnerKind {
    User,
    StagingInternal,
};

struct QueueLimits {
    size_t max_outstanding_owners{0};
    size_t max_outstanding_bytes{0};
    size_t staging_owner_reserve{0};
    size_t staging_byte_reserve{0};
    // Opt-in deadline-aware dispatch (RFC #2519 step 2). When false (default),
    // pickForDispatch keeps strict FIFO order — unchanged behavior. When true,
    // owners carrying a deadline (request.deadline_ns != 0) are dispatched
    // earliest-deadline-first; owners without a deadline keep FIFO order behind
    // them. This only reorders selection within the existing capacity limits;
    // it does not admit/reject or otherwise change what gets dispatched.
    bool deadline_aware{false};
    // Opt-in deadline-infeasible drop (RFC #2519 step 3). Local-decode MLU
    // threshold θ_local. <= 0 (default 0) disables drop entirely — behavior is
    // the step-2 EDF ordering (or FIFO). When > 0 (e.g. 1.5) and a bandwidth
    // provider is set, an owner whose predicted MLU (see DeadlineMlu:
    // (eligible bytes already dispatched + length) / bandwidth, over the
    // remaining window) reaches this threshold is dropped instead of
    // dispatched, and on_local_decode_suggested is raised so the caller can
    // recompute locally. Requires deadline_aware = true.
    double mlu_local_threshold{0.0};
    // Opt-in deadline proximity promotion. When > 0, pickForDispatch promotes
    // queued owners whose remaining slack (deadline_ns - now) is below this
    // threshold to the front of the dispatch queue, ahead of owners with more
    // slack or no deadline. This dynamically boosts urgency as a deadline
    // approaches, regardless of original admission order. Requires a
    // NowProvider (via setDegradationPolicy) or defaults to steady_clock.
    // 0 (default) disables promotion entirely.
    uint64_t promotion_slack_ns{0};
    // Opt-in exploration for the drop above. The drop and the bandwidth
    // estimate it divides by form a loop: dropping decides what is sent,
    // and what is sent is all the estimate can learn from. On a link whose
    // throughput needs enough work in flight, an estimate depressed by a
    // spell of contention admits too little to ever measure more, and the
    // queue settles below saturation for good. Per pickForDispatch call, up
    // to this many owners the drop predicate would reject are dispatched
    // anyway -- in EDF order, so the most urgent infeasible owner is probed
    // first -- so the meter can see whether the NIC sustains more than the
    // estimate says. A probe is dispatched like any other owner: it takes
    // dispatch budget, counts toward the queue ahead, may still miss its
    // deadline, and is not signalled through the degradation hook. It
    // recurs on every pick for as long as the drop predicate fires, so on a
    // link that genuinely is that slow it costs one late transfer per pick
    // indefinitely, and its bytes raise the queue ahead of the owners behind
    // it. 0 (default) = never probe. 1 is the value to use: the exploration the
    // climb costs is set by how far the estimate has to move, not by the
    // budget, so a larger budget only shortens the climb -- while, when the
    // estimate is right, it dispatches that many more late transfers per
    // pick and pushes that much more queue ahead onto the feasible owners
    // behind them.
    size_t mlu_probe_owners{0};
    // Which rejected owners are worth probing. A depressed estimate
    // misjudges owners just past the threshold: where it admits n owners a
    // pick, the (n+1)-th -- the one a probe should test -- scores (n+1)/n
    // times what the last owner scores at saturation, so at most twice
    // that, and that saturated score is itself below theta or the drop would
    // fire at line rate too. An owner far past the threshold -- a large
    // transfer with a tight deadline -- is infeasible whatever the estimate
    // says, and probing it buys a late transfer and no information. Only
    // owners with MLU below mlu_local_threshold x this factor are probe
    // candidates; the rest are dropped even with budget to spare. 2.0 covers
    // every plateau a depressed estimate can produce. <= 1.0 removes the
    // ceiling.
    double mlu_probe_ceiling_factor{2.0};
};

struct QueueOwnerInput {
    // Absolute task id within the caller's Batch, not relative to this submit.
    size_t owner_task_id{0};
    std::vector<size_t> derived_task_ids;
    Request request{};
    QueueOwnerKind kind{QueueOwnerKind::User};
    // True only when the caller has established that this owner's transfer
    // time is governed by the installed bandwidth provider -- for the RDMA
    // wiring, the aggregate transmit estimate over the local NICs. Default
    // false keeps degradation explicitly opt-in so a new enqueue path cannot
    // accidentally predict an MNNVL/TCP/staging transfer, whose completion
    // time that estimate says nothing about, from an RDMA wire rate.
    bool degradation_eligible{false};
};

struct QueueSubmit {
    uint64_t batch_token{0};
    // Caller-computed remaining public task slots for this submit.
    size_t batch_slots_left{0};
    std::vector<QueueOwnerInput> owners;
};

// RFC #2519 step 3: degradation signal raised when a transfer is predicted to
// miss its deadline and is dropped from dispatch. The bodies (compression /
// local recompute) live in the upper layer (vLLM/SGLang); TENT only raises the
// signal. No hook registered ⇒ the drop still happens but nothing is notified.
struct DegradationHooks {
    std::function<void(const Request&)> on_local_decode_suggested;
};

// Returns the rate at which the bytes of a degradation-eligible owner move on
// the wire, in bytes/second, or <= 0 if unknown (in which case the drop
// decision is skipped). This is a transmit rate, not a rate that folds in the
// wait behind other work: queueing enters the prediction separately, as
// DeadlineMlu's `bytes_ahead`. The RDMA wiring supplies the sum of the local
// NICs' transmit estimates. Injected by the owner so the admission queue does
// not depend on the device-selection layer directly.
using BandwidthProvider = std::function<double()>;

// Returns "now" as a steady-clock timestamp in nanoseconds, matching the units
// of Request.deadline_ns. Injectable so tests are deterministic.
using NowProvider = std::function<uint64_t()>;

// Runtime-private admission model. It is intentionally single-threaded; the
// eventual TransferEngineImpl integration owns synchronization.
class LocalTransferAdmissionQueue {
   public:
    explicit LocalTransferAdmissionQueue(QueueLimits limits);

    LocalTransferAdmissionQueue(const LocalTransferAdmissionQueue&) = delete;
    LocalTransferAdmissionQueue& operator=(const LocalTransferAdmissionQueue&) =
        delete;
    LocalTransferAdmissionQueue(LocalTransferAdmissionQueue&&) = delete;
    LocalTransferAdmissionQueue& operator=(LocalTransferAdmissionQueue&&) =
        delete;

    Status tryAdmit(const QueueSubmit& submit,
                    std::vector<QueueOwnerId>& admitted_owner_ids);

    // Returns the owners to dispatch. When step-3 drop is enabled
    // (mlu_local_threshold > 0, deadline_aware, and a bandwidth provider set),
    // owners predicted to miss their deadline are dropped: charged out of the
    // outstanding accounting, marked terminal (CANCELED), appended to
    // `dropped_owner_ids` (if non-null), and on_local_decode_suggested is
    // raised. `dropped_owner_ids` is cleared on entry.
    std::vector<QueueOwnerId> pickForDispatch(
        size_t max_owners, size_t max_bytes,
        std::vector<QueueOwnerId>* dropped_owner_ids = nullptr);

    // Install the step-3 degradation policy inputs. Optional; without it the
    // queue never drops (default behavior). now defaults to steady_clock.
    void setDegradationPolicy(BandwidthProvider bandwidth_provider,
                              DegradationHooks hooks,
                              NowProvider now_provider = nullptr);

    // How an owner that was dispatched as a probe (QueueLimits::
    // mlu_probe_owners) ended: met its deadline, or did not (missed it, or
    // ended in any terminal status other than COMPLETED). None for every
    // other owner.
    enum class ProbeOutcome { None, MetDeadline, MissedDeadline };

    // `probe_outcome`, when given, receives the probe verdict for this owner
    // so the caller can meter it; the queue keeps the same tally in
    // probeStats().
    Status complete(QueueOwnerId owner_id, TransferStatusEnum terminal_status,
                    ProbeOutcome* probe_outcome = nullptr);

    // Cancel an owner that has not entered the dispatch window. Idempotent for
    // an owner already canceled; dispatching owners must be canceled through
    // their selected transport instead.
    Status cancel(QueueOwnerId owner_id);

    Status retireBatch(uint64_t batch_token);

    Status resolveOwner(uint64_t batch_token, size_t public_task_id,
                        QueueOwnerId& owner_id) const;

    Status getPublicStatus(uint64_t batch_token, size_t public_task_id,
                           TransferStatusEnum& status) const;

    size_t outstandingOwners() const;

    size_t outstandingBytes() const;

    // Bytes of degradation-eligible owners currently Dispatching.
    size_t dispatchingBytes() const;

    // Owners dispatched as probes (see QueueLimits::mlu_probe_owners) since
    // construction, and how the completed ones fared against their deadline.
    // met + missed <= dispatched: the rest are still in flight.
    struct ProbeStats {
        size_t dispatched{0};
        size_t met_deadline{0};
        size_t missed_deadline{0};
    };
    ProbeStats probeStats() const;

   private:
    enum class QueueState {
        Queued,
        Dispatching,
        Terminal,
    };

    struct QueueOwner {
        uint64_t batch_token{0};
        Request request{};
        QueueOwnerKind kind{QueueOwnerKind::User};
        bool degradation_eligible{false};
        QueueState state{QueueState::Queued};
        TransferStatusEnum terminal_status{TransferStatusEnum::PENDING};
        bool probe{false};  // dispatched past the drop predicate
    };

    // now_provider_ when installed, else steady_clock; the clock the deadline
    // predictor and the probe verdict share.
    uint64_t clockNs() const;

    QueueLimits limits_;
    Status limits_status_;
    QueueOwnerId next_owner_id_{1};
    std::map<QueueOwnerId, QueueOwner> owners_;
    std::map<std::pair<uint64_t, size_t>, QueueOwnerId> public_to_owner_;
    std::deque<QueueOwnerId> fifo_;
    size_t outstanding_owners_{0};
    size_t outstanding_bytes_{0};
    size_t outstanding_user_owners_{0};
    size_t outstanding_user_bytes_{0};
    // Bytes of degradation-eligible owners dispatched and not yet completed:
    // the queue-ahead term of the step-3 drop prediction. Owners on other
    // transports share the queue but not the provider's bandwidth.
    size_t dispatching_bytes_{0};
    ProbeStats probe_stats_;

    // RFC #2519 step 3 degradation policy (all optional / opt-in).
    BandwidthProvider bandwidth_provider_;
    DegradationHooks degradation_hooks_;
    NowProvider now_provider_;
};

}  // namespace tent
}  // namespace mooncake

#endif  // ADMISSION_QUEUE_H_
