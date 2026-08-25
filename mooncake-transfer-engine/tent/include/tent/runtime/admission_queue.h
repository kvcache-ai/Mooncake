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

#include <array>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <functional>
#include <unordered_map>
#include <utility>
#include <vector>

#include "tent/common/status.h"
#include "tent/common/types.h"

namespace mooncake {
namespace tent {

using QueueOwnerId = uint64_t;

// Owner kind separates user work from staging-internal work. Request priority
// remains the primary dispatch ordering key.
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
    // pickForDispatch keeps strict FIFO order within each priority/kind lane.
    // When true, owners carrying a deadline (request.deadline_ns != 0) are
    // dispatched earliest-deadline-first within that lane; owners without a
    // deadline keep FIFO order behind them. Request priority still determines
    // dispatch order across lanes.
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
    // True only when the caller has established that this owner's transfer
    // time is governed by the installed bandwidth provider. Default false
    // keeps degradation explicitly opt-in so a new enqueue path cannot
    // accidentally apply an RDMA EWMA to MNNVL/TCP/staging paths.
    bool degradation_eligible{false};
};

struct QueueSubmit {
    uint64_t batch_token{0};
    // Caller-computed remaining public task slots for this submit.
    size_t batch_slots_left{0};
    // Public task ids in one submit form a contiguous range in
    // Batch::task_list, although owner/derived ids need not be presented in
    // that order.
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
        bool degradation_eligible{false};
        QueueState state{QueueState::Queued};
        TransferStatusEnum terminal_status{TransferStatusEnum::PENDING};
    };

    using OwnerMap = std::unordered_map<QueueOwnerId, QueueOwner>;

    struct BatchIndex {
        std::vector<QueueOwnerId> owner_ids;
        // Indexed by the absolute task id in Batch::task_list. Zero means the
        // task is not managed by this runtime queue (for example, a direct
        // submission made earlier in the same batch).
        std::vector<QueueOwnerId> public_task_owners;
    };

    class DispatchScheduler {
       public:
        struct Candidate {
            QueueOwnerId owner_id{0};
            size_t priority{0};
            size_t lane{0};
            bool found{false};
        };

        void enqueue(QueueOwnerId owner_id, int priority, QueueOwnerKind kind,
                     bool deadline_aware, const OwnerMap& owners);

        void promoteDeadlineUrgentOwners(uint64_t now_ns,
                                         uint64_t promotion_slack_ns,
                                         const OwnerMap& owners);

        Candidate next(const OwnerMap& owners);

        void consume(const Candidate& candidate);

       private:
        enum class KindLane : size_t {
            StagingInternal = 0,
            User = 1,
            Count = 2,
        };

        static constexpr size_t kPriorityCount =
            static_cast<size_t>(PRIO_LOW) + 1;

        struct LaneState {
            std::deque<QueueOwnerId> queue;
        };

        using KindLanes =
            std::array<LaneState, static_cast<size_t>(KindLane::Count)>;

        struct PriorityClass {
            KindLanes lanes;
            size_t next_kind_lane{0};
        };

        static size_t laneForKind(QueueOwnerKind kind);

        std::array<PriorityClass, kPriorityCount> classes_;
    };
    QueueLimits limits_;
    Status limits_status_;
    DispatchScheduler scheduler_;
    QueueOwnerId next_owner_id_{1};
    OwnerMap owners_;
    std::unordered_map<uint64_t, BatchIndex> batch_index_;
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
