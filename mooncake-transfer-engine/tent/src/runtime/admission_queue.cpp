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

#include "tent/runtime/admission_queue.h"

#include <algorithm>
#include <limits>

namespace mooncake {
namespace tent {
namespace {

struct PendingPublicTask {
    size_t task_id{0};
    size_t owner_index{0};

    bool operator<(const PendingPublicTask& other) const {
        return task_id < other.task_id;
    }
};

bool isSupportedTerminalStatus(TransferStatusEnum status) {
    return status == TransferStatusEnum::COMPLETED ||
           status == TransferStatusEnum::INVALID ||
           status == TransferStatusEnum::CANCELED ||
           status == TransferStatusEnum::TIMEOUT ||
           status == TransferStatusEnum::FAILED;
}

bool isSupportedOwnerKind(QueueOwnerKind kind) {
    switch (kind) {
        case QueueOwnerKind::User:
        case QueueOwnerKind::StagingInternal:
            return true;
    }
    return false;
}

bool isSupportedRequestPriority(int priority) {
    switch (priority) {
        case PRIO_HIGH:
        case PRIO_MEDIUM:
        case PRIO_LOW:
            return true;
    }
    return false;
}

Status checkedAdd(size_t lhs, size_t rhs, size_t& out) {
    if (rhs > std::numeric_limits<size_t>::max() - lhs) {
        return Status::InvalidArgument(
            "admission queue charge overflow" LOC_MARK);
    }
    out = lhs + rhs;
    return Status::OK();
}

Status validateLimits(const QueueLimits& limits) {
    if (limits.staging_owner_reserve > limits.max_outstanding_owners) {
        return Status::InvalidArgument(
            "staging owner reserve exceeds owner limit" LOC_MARK);
    }
    if (limits.staging_byte_reserve > limits.max_outstanding_bytes) {
        return Status::InvalidArgument(
            "staging byte reserve exceeds byte limit" LOC_MARK);
    }
    return Status::OK();
}

}  // namespace

bool LocalTransferAdmissionQueue::hasPublicTask(const BatchIndex& batch_index,
                                                size_t task_id) {
    auto it = std::lower_bound(batch_index.public_tasks.begin(),
                               batch_index.public_tasks.end(), task_id,
                               [](const auto& public_task, size_t key) {
                                   return public_task.task_id < key;
                               });
    return it != batch_index.public_tasks.end() && it->task_id == task_id;
}

LocalTransferAdmissionQueue::DispatchScheduler::DispatchScheduler(
    QueueAgingConfig aging)
    : aging_(aging) {}

size_t LocalTransferAdmissionQueue::DispatchScheduler::kindLane(
    QueueOwnerKind kind) {
    switch (kind) {
        case QueueOwnerKind::StagingInternal:
            return static_cast<size_t>(KindLane::StagingInternal);
        case QueueOwnerKind::User:
            return static_cast<size_t>(KindLane::User);
    }
    return static_cast<size_t>(KindLane::User);
}

void LocalTransferAdmissionQueue::DispatchScheduler::enqueue(
    QueueOwnerId owner_id, int priority, QueueOwnerKind kind) {
    classes_[static_cast<size_t>(priority)]
        .lanes[kindLane(kind)]
        .queue.push_back(owner_id);
}

size_t LocalTransferAdmissionQueue::DispatchScheduler::priorityWeight(
    size_t priority) {
    switch (priority) {
        case PRIO_HIGH:
            return 4;
        case PRIO_MEDIUM:
            return 2;
        case PRIO_LOW:
            return 1;
        default:
            return 1;
    }
}

size_t LocalTransferAdmissionQueue::DispatchScheduler::quantumForPriority(
    size_t priority, size_t max_bytes) const {
    const size_t base_quantum =
        std::max<size_t>(max_bytes / 8 + (max_bytes % 8 != 0), 1);
    const size_t weight = priorityWeight(priority);
    if (base_quantum > std::numeric_limits<size_t>::max() / weight) {
        return std::numeric_limits<size_t>::max();
    }
    return base_quantum * weight;
}

void LocalTransferAdmissionQueue::DispatchScheduler::addDeficit(
    size_t priority, size_t lane, size_t max_bytes) {
    auto& lane_state = classes_[priority].lanes[lane];
    const size_t quantum = quantumForPriority(priority, max_bytes);
    if (lane_state.deficit_bytes >= max_bytes ||
        quantum > max_bytes - lane_state.deficit_bytes) {
        lane_state.deficit_bytes = max_bytes;
        return;
    }
    lane_state.deficit_bytes += quantum;
}

bool LocalTransferAdmissionQueue::DispatchScheduler::hasQueuedOwner(
    size_t priority, const OwnerMap& owners) {
    auto& priority_class = classes_[priority];
    bool has_owner = false;
    for (auto& lane : priority_class.lanes) {
        while (!lane.queue.empty()) {
            const auto owner_id = lane.queue.front();
            auto owner_it = owners.find(owner_id);
            if (owner_it != owners.end() &&
                owner_it->second.state == QueueState::Queued) {
                has_owner = true;
                break;
            }
            lane.queue.pop_front();
        }
        if (lane.queue.empty()) {
            lane.deficit_bytes = 0;
        }
    }
    return has_owner;
}

LocalTransferAdmissionQueue::DispatchScheduler::PickResult
LocalTransferAdmissionQueue::DispatchScheduler::pickFromPriority(
    size_t priority, size_t remaining_bytes, const OwnerMap& owners) {
    auto& priority_class = classes_[priority];
    PickResult blocked;
    for (size_t offset = 0; offset < priority_class.lanes.size(); ++offset) {
        const size_t lane = (priority_class.next_kind_lane + offset) %
                            priority_class.lanes.size();
        auto& lane_state = priority_class.lanes[lane];

        while (!lane_state.queue.empty()) {
            const auto owner_id = lane_state.queue.front();
            auto owner_it = owners.find(owner_id);
            if (owner_it == owners.end() ||
                owner_it->second.state != QueueState::Queued) {
                lane_state.queue.pop_front();
                continue;
            }

            const size_t byte_charge = owner_it->second.request.length;
            blocked.has_owner = true;
            if (byte_charge > remaining_bytes) {
                blocked.blocked_by_window = true;
                break;
            }
            if (byte_charge > lane_state.deficit_bytes) {
                blocked.blocked_by_credit = true;
                break;
            }

            lane_state.queue.pop_front();
            priority_class.next_kind_lane =
                (lane + 1) % priority_class.lanes.size();
            lane_state.deficit_bytes -= byte_charge;
            if (lane_state.queue.empty()) {
                lane_state.deficit_bytes = 0;
            }
            return PickResult{owner_id, byte_charge, true, false, false};
        }
    }

    return blocked;
}

void LocalTransferAdmissionQueue::DispatchScheduler::promoteAgedOwners(
    TimePoint now, const OwnerMap& owners) {
    promoteAgedPriority(PRIO_MEDIUM, PRIO_HIGH, aging_.medium_to_high, now,
                        owners);
    promoteAgedPriority(PRIO_LOW, PRIO_HIGH, aging_.low_to_high, now, owners);
}

void LocalTransferAdmissionQueue::DispatchScheduler::promoteAgedPriority(
    size_t from_priority, size_t to_priority,
    std::chrono::microseconds threshold, TimePoint now,
    const OwnerMap& owners) {
    if (threshold <= std::chrono::microseconds::zero()) return;
    if (from_priority == PRIO_HIGH || from_priority >= classes_.size() ||
        to_priority >= classes_.size()) {
        return;
    }

    for (size_t lane = 0; lane < classes_[from_priority].lanes.size(); ++lane) {
        auto& source_lane = classes_[from_priority].lanes[lane];
        auto& target_lane = classes_[to_priority].lanes[lane];
        auto& queue = source_lane.queue;
        auto& promoted_queue = target_lane.queue;
        while (!queue.empty()) {
            const auto owner_id = queue.front();
            auto owner_it = owners.find(owner_id);
            if (owner_it == owners.end() ||
                owner_it->second.state != QueueState::Queued) {
                queue.pop_front();
                continue;
            }

            if (now - owner_it->second.enqueue_time < threshold) break;

            auto insert_it = promoted_queue.end();
            for (auto it = promoted_queue.begin(); it != promoted_queue.end();
                 ++it) {
                auto promoted_it = owners.find(*it);
                if (promoted_it == owners.end() ||
                    promoted_it->second.state != QueueState::Queued) {
                    continue;
                }
                if (owner_it->second.enqueue_time <
                    promoted_it->second.enqueue_time) {
                    insert_it = it;
                    break;
                }
            }
            queue.pop_front();
            if (promoted_queue.empty()) {
                target_lane.deficit_bytes = 0;
            }
            promoted_queue.insert(insert_it, owner_id);
        }
        if (queue.empty()) {
            source_lane.deficit_bytes = 0;
        }
    }
}

std::vector<QueueOwnerId> LocalTransferAdmissionQueue::DispatchScheduler::pick(
    size_t max_owners, size_t max_bytes, const OwnerMap& owners,
    TimePoint now) {
    std::vector<QueueOwnerId> picked;
    if (max_owners == 0 || max_bytes == 0) return picked;

    promoteAgedOwners(now, owners);

    size_t used_owners = 0;
    size_t used_bytes = 0;
    while (used_owners < max_owners && used_bytes < max_bytes) {
        bool made_progress = false;
        bool blocked_by_credit = false;
        bool blocked_by_window = false;
        const size_t start_priority = next_priority_;
        for (size_t offset = 0; offset < classes_.size(); ++offset) {
            const size_t priority = (start_priority + offset) % classes_.size();
            if (!hasQueuedOwner(priority, owners)) continue;

            auto& priority_class = classes_[priority];
            for (size_t lane = 0; lane < priority_class.lanes.size(); ++lane) {
                if (!priority_class.lanes[lane].queue.empty()) {
                    addDeficit(priority, lane, max_bytes);
                }
            }
            while (used_owners < max_owners && used_bytes < max_bytes) {
                auto result =
                    pickFromPriority(priority, max_bytes - used_bytes, owners);
                if (!result.has_owner) break;
                blocked_by_credit =
                    blocked_by_credit || result.blocked_by_credit;
                blocked_by_window =
                    blocked_by_window || result.blocked_by_window;
                if (result.owner_id == 0) break;

                picked.push_back(result.owner_id);
                ++used_owners;
                used_bytes += result.byte_charge;
                next_priority_ = (priority + 1) % classes_.size();
                made_progress = true;
            }
            if (used_owners >= max_owners || used_bytes >= max_bytes) break;
        }
        if (!made_progress && !blocked_by_credit) break;
        if (!made_progress && blocked_by_window && !blocked_by_credit) break;
    }
    return picked;
}

LocalTransferAdmissionQueue::LocalTransferAdmissionQueue(QueueLimits limits)
    : limits_(limits), limits_status_(validateLimits(limits)) {}

LocalTransferAdmissionQueue::LocalTransferAdmissionQueue(QueueLimits limits,
                                                         QueueAgingConfig aging)
    : limits_(limits),
      limits_status_(validateLimits(limits)),
      scheduler_(aging) {}

Status LocalTransferAdmissionQueue::tryAdmit(
    const QueueSubmit& submit, std::vector<QueueOwnerId>& admitted_owner_ids) {
    return tryAdmit(submit, admitted_owner_ids, Clock::now());
}

Status LocalTransferAdmissionQueue::tryAdmit(
    const QueueSubmit& submit, std::vector<QueueOwnerId>& admitted_owner_ids,
    TimePoint now) {
    admitted_owner_ids.clear();
    CHECK_STATUS(limits_status_);
    if (submit.batch_token == 0) {
        return Status::InvalidArgument("invalid batch token" LOC_MARK);
    }
    if (submit.owners.empty()) return Status::OK();

    std::vector<PendingPublicTask> public_tasks;
    size_t public_task_count = 0;
    for (const auto& owner : submit.owners) {
        CHECK_STATUS(checkedAdd(public_task_count, 1, public_task_count));
        CHECK_STATUS(checkedAdd(public_task_count,
                                owner.derived_task_ids.size(),
                                public_task_count));
    }
    public_tasks.reserve(public_task_count);
    size_t byte_charge = 0;
    size_t user_owner_charge = 0;
    size_t user_byte_charge = 0;

    for (size_t owner_index = 0; owner_index < submit.owners.size();
         ++owner_index) {
        const auto& owner = submit.owners[owner_index];
        if (!isSupportedOwnerKind(owner.kind)) {
            return Status::InvalidArgument(
                "unsupported queue owner kind" LOC_MARK);
        }
        if (!isSupportedRequestPriority(owner.request.priority)) {
            return Status::InvalidArgument(
                "unsupported queue request priority" LOC_MARK);
        }
        if (owner.request.length == 0) {
            return Status::InvalidArgument("empty transfer request" LOC_MARK);
        }

        public_tasks.push_back({owner.owner_task_id, owner_index});
        for (const auto derived_task_id : owner.derived_task_ids) {
            if (derived_task_id == owner.owner_task_id) {
                return Status::InvalidArgument(
                    "owner task id appears in derived task ids" LOC_MARK);
            }
            public_tasks.push_back({derived_task_id, owner_index});
        }

        CHECK_STATUS(
            checkedAdd(byte_charge, owner.request.length, byte_charge));
        if (owner.kind == QueueOwnerKind::User) {
            CHECK_STATUS(checkedAdd(user_owner_charge, 1, user_owner_charge));
            CHECK_STATUS(checkedAdd(user_byte_charge, owner.request.length,
                                    user_byte_charge));
        }
    }

    std::sort(public_tasks.begin(), public_tasks.end());
    auto duplicate_public_task =
        std::adjacent_find(public_tasks.begin(), public_tasks.end(),
                           [](const auto& lhs, const auto& rhs) {
                               return lhs.task_id == rhs.task_id;
                           });
    if (duplicate_public_task != public_tasks.end()) {
        return Status::InvalidArgument("duplicate public task id" LOC_MARK);
    }

    if (public_tasks.size() > submit.batch_slots_left) {
        return Status::TooManyRequests(
            "batch public task capacity exceeded" LOC_MARK);
    }

    auto batch_it = batch_index_.find(submit.batch_token);
    if (batch_it != batch_index_.end()) {
        for (const auto& public_task : public_tasks) {
            if (hasPublicTask(batch_it->second, public_task.task_id)) {
                return Status::InvalidEntry(
                    "public task id already admitted" LOC_MARK);
            }
        }
    }

    const size_t owner_charge = submit.owners.size();
    size_t next_outstanding_owners = 0;
    size_t next_outstanding_bytes = 0;
    size_t next_user_owners = 0;
    size_t next_user_bytes = 0;
    CHECK_STATUS(
        checkedAdd(outstanding_owners_, owner_charge, next_outstanding_owners));
    CHECK_STATUS(
        checkedAdd(outstanding_bytes_, byte_charge, next_outstanding_bytes));
    CHECK_STATUS(checkedAdd(outstanding_user_owners_, user_owner_charge,
                            next_user_owners));
    CHECK_STATUS(
        checkedAdd(outstanding_user_bytes_, user_byte_charge, next_user_bytes));

    const size_t user_owner_limit =
        limits_.max_outstanding_owners - limits_.staging_owner_reserve;
    const size_t user_byte_limit =
        limits_.max_outstanding_bytes - limits_.staging_byte_reserve;

    if (next_outstanding_owners > limits_.max_outstanding_owners) {
        return Status::TooManyRequests(
            "queue owner capacity exceeded" LOC_MARK);
    }
    if (next_outstanding_bytes > limits_.max_outstanding_bytes) {
        return Status::TooManyRequests("queue byte capacity exceeded" LOC_MARK);
    }
    if (next_user_owners > user_owner_limit) {
        return Status::TooManyRequests("user owner capacity exceeded" LOC_MARK);
    }
    if (next_user_bytes > user_byte_limit) {
        return Status::TooManyRequests("user byte capacity exceeded" LOC_MARK);
    }

    admitted_owner_ids.reserve(submit.owners.size());
    owners_.reserve(owners_.size() + submit.owners.size());
    auto& batch_index =
        batch_index_.try_emplace(submit.batch_token).first->second;
    batch_index.owner_ids.reserve(batch_index.owner_ids.size() +
                                  submit.owners.size());
    batch_index.public_tasks.reserve(batch_index.public_tasks.size() +
                                     public_tasks.size());

    std::vector<QueueOwnerId> owner_ids;
    owner_ids.reserve(submit.owners.size());
    for (const auto& owner_input : submit.owners) {
        const QueueOwnerId owner_id = next_owner_id_++;
        QueueOwner owner;
        owner.batch_token = submit.batch_token;
        owner.request = owner_input.request;
        owner.kind = owner_input.kind;
        owner.enqueue_time = now;
        owners_.emplace(owner_id, owner);
        batch_index.owner_ids.push_back(owner_id);
        owner_ids.push_back(owner_id);
        scheduler_.enqueue(owner_id, owner_input.request.priority,
                           owner_input.kind);
        admitted_owner_ids.push_back(owner_id);
    }
    const auto public_task_begin = batch_index.public_tasks.size();
    for (const auto& public_task : public_tasks) {
        batch_index.public_tasks.push_back(
            {public_task.task_id, owner_ids[public_task.owner_index]});
    }
    std::inplace_merge(batch_index.public_tasks.begin(),
                       batch_index.public_tasks.begin() + public_task_begin,
                       batch_index.public_tasks.end(),
                       [](const auto& lhs, const auto& rhs) {
                           return lhs.task_id < rhs.task_id;
                       });

    outstanding_owners_ = next_outstanding_owners;
    outstanding_bytes_ = next_outstanding_bytes;
    outstanding_user_owners_ = next_user_owners;
    outstanding_user_bytes_ = next_user_bytes;
    return Status::OK();
}

std::vector<QueueOwnerId> LocalTransferAdmissionQueue::pickForDispatch(
    size_t max_owners, size_t max_bytes) {
    return pickForDispatch(max_owners, max_bytes, Clock::now());
}

std::vector<QueueOwnerId> LocalTransferAdmissionQueue::pickForDispatch(
    size_t max_owners, size_t max_bytes, TimePoint now) {
    auto picked = scheduler_.pick(max_owners, max_bytes, owners_, now);
    for (const auto owner_id : picked) {
        owners_.find(owner_id)->second.state = QueueState::Dispatching;
    }
    return picked;
}

Status LocalTransferAdmissionQueue::requeueForDispatch(QueueOwnerId owner_id) {
    if (owner_id == 0) {
        return Status::InvalidArgument("invalid queue owner id" LOC_MARK);
    }
    auto owner_it = owners_.find(owner_id);
    if (owner_it == owners_.end()) {
        return Status::InvalidEntry("queue owner not found" LOC_MARK);
    }
    auto& owner = owner_it->second;
    if (owner.state != QueueState::Dispatching) {
        return Status::InvalidEntry("queue owner is not dispatching" LOC_MARK);
    }

    owner.state = QueueState::Queued;
    scheduler_.enqueue(owner_id, owner.request.priority, owner.kind);
    return Status::OK();
}

Status LocalTransferAdmissionQueue::complete(
    QueueOwnerId owner_id, TransferStatusEnum terminal_status) {
    if (owner_id == 0) {
        return Status::InvalidArgument("invalid queue owner id" LOC_MARK);
    }
    if (!isSupportedTerminalStatus(terminal_status)) {
        return Status::InvalidArgument("unsupported terminal status" LOC_MARK);
    }

    auto owner_it = owners_.find(owner_id);
    if (owner_it == owners_.end()) {
        return Status::InvalidEntry("queue owner not found" LOC_MARK);
    }
    auto& owner = owner_it->second;
    if (owner.state != QueueState::Dispatching) {
        return Status::InvalidEntry("queue owner is not dispatching" LOC_MARK);
    }

    owner.state = QueueState::Terminal;
    owner.terminal_status = terminal_status;
    --outstanding_owners_;
    outstanding_bytes_ -= owner.request.length;
    if (owner.kind == QueueOwnerKind::User) {
        --outstanding_user_owners_;
        outstanding_user_bytes_ -= owner.request.length;
    }
    return Status::OK();
}

Status LocalTransferAdmissionQueue::retireBatch(uint64_t batch_token) {
    if (batch_token == 0) {
        return Status::InvalidArgument("invalid batch token" LOC_MARK);
    }

    auto batch_it = batch_index_.find(batch_token);
    if (batch_it == batch_index_.end()) return Status::OK();

    for (const auto owner_id : batch_it->second.owner_ids) {
        auto owner_it = owners_.find(owner_id);
        if (owner_it == owners_.end()) {
            return Status::InternalError(
                "queue owner mapping is stale" LOC_MARK);
        }

        const auto& owner = owner_it->second;
        if (owner.batch_token != batch_token) {
            return Status::InternalError(
                "queue owner batch token mismatch" LOC_MARK);
        }
        if (owner.state != QueueState::Terminal) {
            return Status::InvalidEntry(
                "batch has non-terminal queue owners" LOC_MARK);
        }
    }

    for (const auto owner_id : batch_it->second.owner_ids) {
        owners_.erase(owner_id);
    }
    batch_index_.erase(batch_it);
    return Status::OK();
}

Status LocalTransferAdmissionQueue::resolveOwner(uint64_t batch_token,
                                                 size_t public_task_id,
                                                 QueueOwnerId& owner_id) const {
    if (batch_token == 0) {
        return Status::InvalidArgument("invalid batch token" LOC_MARK);
    }
    auto batch_it = batch_index_.find(batch_token);
    if (batch_it == batch_index_.end()) {
        return Status::InvalidEntry("public task id not found" LOC_MARK);
    }
    auto public_it =
        std::lower_bound(batch_it->second.public_tasks.begin(),
                         batch_it->second.public_tasks.end(), public_task_id,
                         [](const auto& public_task, size_t task_id) {
                             return public_task.task_id < task_id;
                         });
    if (public_it == batch_it->second.public_tasks.end() ||
        public_it->task_id != public_task_id) {
        return Status::InvalidEntry("public task id not found" LOC_MARK);
    }
    owner_id = public_it->owner_id;
    return Status::OK();
}

Status LocalTransferAdmissionQueue::getPublicStatus(
    uint64_t batch_token, size_t public_task_id,
    TransferStatusEnum& status) const {
    QueueOwnerId owner_id = 0;
    CHECK_STATUS(resolveOwner(batch_token, public_task_id, owner_id));
    auto owner_it = owners_.find(owner_id);
    if (owner_it == owners_.end()) {
        return Status::InternalError("queue owner mapping is stale" LOC_MARK);
    }
    switch (owner_it->second.state) {
        case QueueState::Queued:
        case QueueState::Dispatching:
            status = TransferStatusEnum::PENDING;
            break;
        case QueueState::Terminal:
            status = owner_it->second.terminal_status;
            break;
    }
    return Status::OK();
}

size_t LocalTransferAdmissionQueue::outstandingOwners() const {
    return outstanding_owners_;
}

size_t LocalTransferAdmissionQueue::outstandingBytes() const {
    return outstanding_bytes_;
}

}  // namespace tent
}  // namespace mooncake
