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
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <deque>
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
};

struct QueueAgingConfig {
    // Zero disables aging for the corresponding priority class.
    std::chrono::microseconds medium_to_high{0};
    std::chrono::microseconds low_to_high{0};
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

// Runtime-private admission model. It is intentionally single-threaded; the
// eventual TransferEngineImpl integration owns synchronization.
class LocalTransferAdmissionQueue {
   public:
    using Clock = std::chrono::steady_clock;
    using TimePoint = Clock::time_point;

    explicit LocalTransferAdmissionQueue(QueueLimits limits);
    LocalTransferAdmissionQueue(QueueLimits limits, QueueAgingConfig aging);

    LocalTransferAdmissionQueue(const LocalTransferAdmissionQueue&) = delete;
    LocalTransferAdmissionQueue& operator=(const LocalTransferAdmissionQueue&) =
        delete;
    LocalTransferAdmissionQueue(LocalTransferAdmissionQueue&&) = delete;
    LocalTransferAdmissionQueue& operator=(LocalTransferAdmissionQueue&&) =
        delete;

    Status tryAdmit(const QueueSubmit& submit,
                    std::vector<QueueOwnerId>& admitted_owner_ids);
    Status tryAdmit(const QueueSubmit& submit,
                    std::vector<QueueOwnerId>& admitted_owner_ids,
                    TimePoint now);

    std::vector<QueueOwnerId> pickForDispatch(size_t max_owners,
                                              size_t max_bytes);
    std::vector<QueueOwnerId> pickForDispatch(size_t max_owners,
                                              size_t max_bytes, TimePoint now);

    Status complete(QueueOwnerId owner_id, TransferStatusEnum terminal_status);

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
        TimePoint queue_enter_time{};
        QueueState state{QueueState::Queued};
        TransferStatusEnum terminal_status{TransferStatusEnum::PENDING};
    };

    using OwnerMap = std::unordered_map<QueueOwnerId, QueueOwner>;

    struct PublicTaskOwner {
        size_t task_id{0};
        QueueOwnerId owner_id{0};
    };

    struct BatchIndex {
        std::vector<QueueOwnerId> owner_ids;
        std::vector<PublicTaskOwner> public_tasks;
    };

    static bool hasPublicTask(const BatchIndex& batch_index, size_t task_id);

    // Weighted byte-deficit scheduler over request priorities and owner-kind
    // lanes. It can skip a blocked head in another lane to use the dispatch
    // byte window, while preserving FIFO order inside one priority/kind lane.
    class DispatchScheduler {
       public:
        explicit DispatchScheduler(QueueAgingConfig aging = {});

        std::vector<QueueOwnerId> pick(size_t max_owners, size_t max_bytes,
                                       const OwnerMap& owners, TimePoint now);

        void enqueue(QueueOwnerId owner_id, int priority, QueueOwnerKind kind);

       private:
        enum class KindLane : size_t {
            StagingInternal = 0,
            User = 1,
            Count = 2,
        };

        static constexpr size_t kPriorityCount =
            static_cast<size_t>(PRIO_LOW) + 1;

        struct PickResult {
            QueueOwnerId owner_id{0};
            size_t byte_charge{0};
            bool found{false};
            bool blocked_by_credit{false};
            bool blocked_by_window{false};
        };

        struct LaneState {
            std::deque<QueueOwnerId> queue;
            size_t deficit_bytes{0};
        };

        using KindLanes =
            std::array<LaneState, static_cast<size_t>(KindLane::Count)>;

        struct PriorityClass {
            KindLanes lanes;
            size_t next_kind_lane{0};
        };

        static size_t laneForKind(QueueOwnerKind kind);

        static size_t priorityWeight(size_t priority);

        size_t quantumForPriority(size_t priority, size_t max_bytes) const;

        void addDeficit(size_t priority, size_t lane, size_t max_bytes);

        bool hasQueuedOwner(size_t priority, const OwnerMap& owners);

        PickResult pickFromPriority(size_t priority, size_t remaining_bytes,
                                    const OwnerMap& owners);

        void promoteAgedOwners(TimePoint now, const OwnerMap& owners);

        void promoteAgedPriority(size_t from_priority, size_t to_priority,
                                 std::chrono::microseconds threshold,
                                 TimePoint now, const OwnerMap& owners);

        std::array<PriorityClass, kPriorityCount> classes_;
        size_t next_priority_{0};
        QueueAgingConfig aging_;
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
};

}  // namespace tent
}  // namespace mooncake

#endif  // ADMISSION_QUEUE_H_
