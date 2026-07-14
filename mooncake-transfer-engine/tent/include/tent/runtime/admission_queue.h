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
    // threshold θ_local. 0 (default) disables drop entirely — behavior is the
    // step-2 EDF ordering (or FIFO). When > 0 (e.g. 1.5) and a bandwidth
    // provider is set, an owner whose predicted MLU
    // (= predicted_transfer_time / remaining_window) reaches this threshold is
    // dropped instead of dispatched, and on_local_decode_suggested is raised so
    // the caller can recompute locally. Requires deadline_aware = true.
    double mlu_local_threshold{0.0};
    // Opt-in deadline proximity promotion. When > 0, pickForDispatch promotes
    // queued owners whose remaining slack (deadline_ns - now) is below this
    // threshold to the front of the dispatch queue, ahead of owners with more
    // slack or no deadline. This dynamically boosts urgency as a deadline
    // approaches, regardless of original admission order. Requires a
    // NowProvider (via setDegradationPolicy) or defaults to steady_clock.
    // 0 (default) disables promotion entirely.
    uint64_t promotion_slack_ns{0};
};

struct QueueOwnerInput {
    // Absolute task id within the caller's Batch, not relative to this submit.
    size_t owner_task_id{0};
    std::vector<size_t> derived_task_ids;
    Request request{};
    QueueOwnerKind kind{QueueOwnerKind::User};
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

// Returns the predicted transfer bandwidth in bytes/second, or <= 0 if unknown
// (in which case the drop decision is skipped). Injected by the owner so the
// admission queue does not depend on the device-selection layer directly.
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

    Status complete(QueueOwnerId owner_id, TransferStatusEnum terminal_status);

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
        QueueState state{QueueState::Queued};
        TransferStatusEnum terminal_status{TransferStatusEnum::PENDING};
    };

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

    // RFC #2519 step 3 degradation policy (all optional / opt-in).
    BandwidthProvider bandwidth_provider_;
    DegradationHooks degradation_hooks_;
    NowProvider now_provider_;
};

}  // namespace tent
}  // namespace mooncake

#endif  // ADMISSION_QUEUE_H_
