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
#include <chrono>
#include <limits>
#include <utility>

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

// Sort key for EDF: owners without a deadline (0) sort after all deadlined
// owners, so they never jump ahead of a real deadline.
inline uint64_t deadlineKey(uint64_t deadline_ns) {
    return deadline_ns == 0 ? std::numeric_limits<uint64_t>::max()
                            : deadline_ns;
}

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

size_t LocalTransferAdmissionQueue::DispatchScheduler::laneForKind(
    QueueOwnerKind kind) {
    return kind == QueueOwnerKind::StagingInternal
               ? static_cast<size_t>(KindLane::StagingInternal)
               : static_cast<size_t>(KindLane::User);
}

void LocalTransferAdmissionQueue::DispatchScheduler::enqueue(
    QueueOwnerId owner_id, int priority, QueueOwnerKind kind,
    bool deadline_aware, const OwnerMap& owners) {
    auto& queue = classes_[priority].lanes[laneForKind(kind)].queue;
    if (!deadline_aware) {
        queue.push_back(owner_id);
        return;
    }

    const auto owner_it = owners.find(owner_id);
    const uint64_t key =
        owner_it == owners.end()
            ? std::numeric_limits<uint64_t>::max()
            : deadlineKey(owner_it->second.request.deadline_ns);
    const auto pos = std::upper_bound(
        queue.begin(), queue.end(), key, [&owners](uint64_t deadline,
                                                   QueueOwnerId queued_id) {
            const auto queued_it = owners.find(queued_id);
            const uint64_t queued_deadline =
                queued_it == owners.end()
                    ? std::numeric_limits<uint64_t>::max()
                    : deadlineKey(queued_it->second.request.deadline_ns);
            return deadline < queued_deadline;
        });
    queue.insert(pos, owner_id);
}

void LocalTransferAdmissionQueue::DispatchScheduler::promoteDeadlineUrgentOwners(
    uint64_t now_ns, uint64_t promotion_slack_ns, const OwnerMap& owners) {
    if (promotion_slack_ns == 0) return;

    for (auto& priority_class : classes_) {
        for (auto& lane : priority_class.lanes) {
            std::stable_partition(
                lane.queue.begin(), lane.queue.end(), [&](QueueOwnerId id) {
                    const auto owner_it = owners.find(id);
                    if (owner_it == owners.end() ||
                        owner_it->second.state != QueueState::Queued) {
                        return false;
                    }
                    const uint64_t deadline =
                        owner_it->second.request.deadline_ns;
                    return deadline > now_ns &&
                           deadline - now_ns < promotion_slack_ns;
                });
        }
    }
}

LocalTransferAdmissionQueue::DispatchScheduler::Candidate
LocalTransferAdmissionQueue::DispatchScheduler::next(const OwnerMap& owners) {
    for (size_t priority = PRIO_HIGH; priority < classes_.size(); ++priority) {
        auto& priority_class = classes_[priority];
        for (size_t offset = 0; offset < priority_class.lanes.size(); ++offset) {
            const size_t lane =
                (priority_class.next_kind_lane + offset) %
                priority_class.lanes.size();
            auto& queue = priority_class.lanes[lane].queue;
            while (!queue.empty()) {
                const auto owner_it = owners.find(queue.front());
                if (owner_it != owners.end() &&
                    owner_it->second.state == QueueState::Queued) {
                    return Candidate{queue.front(), priority, lane, true};
                }
                queue.pop_front();
            }
        }
    }
    return {};
}

void LocalTransferAdmissionQueue::DispatchScheduler::consume(
    const Candidate& candidate) {
    if (!candidate.found || candidate.priority >= classes_.size() ||
        candidate.lane >= classes_[candidate.priority].lanes.size()) {
        return;
    }

    auto& priority_class = classes_[candidate.priority];
    auto& queue = priority_class.lanes[candidate.lane].queue;
    if (!queue.empty() && queue.front() == candidate.owner_id) {
        queue.pop_front();
        priority_class.next_kind_lane =
            (candidate.lane + 1) % priority_class.lanes.size();
    }
}

LocalTransferAdmissionQueue::LocalTransferAdmissionQueue(QueueLimits limits)
    : limits_(limits), limits_status_(validateLimits(limits)) {}

Status LocalTransferAdmissionQueue::tryAdmit(
    const QueueSubmit& submit, std::vector<QueueOwnerId>& admitted_owner_ids) {
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
    const auto duplicate_public_task =
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
        owner.degradation_eligible = owner_input.degradation_eligible;
        owners_.emplace(owner_id, owner);
        batch_index.owner_ids.push_back(owner_id);
        owner_ids.push_back(owner_id);
        scheduler_.enqueue(owner_id, owner_input.request.priority,
                           owner_input.kind, limits_.deadline_aware, owners_);
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

void LocalTransferAdmissionQueue::setDegradationPolicy(
    BandwidthProvider bandwidth_provider, DegradationHooks hooks,
    NowProvider now_provider) {
    bandwidth_provider_ = std::move(bandwidth_provider);
    degradation_hooks_ = std::move(hooks);
    now_provider_ = std::move(now_provider);
}

std::vector<QueueOwnerId> LocalTransferAdmissionQueue::pickForDispatch(
    size_t max_owners, size_t max_bytes,
    std::vector<QueueOwnerId>* dropped_owner_ids) {
    if (dropped_owner_ids) dropped_owner_ids->clear();
    std::vector<QueueOwnerId> picked;
    if (max_owners == 0 || max_bytes == 0) return picked;

    // RFC #2519 step 2 (opt-in): each priority/kind lane is kept EDF-ordered
    // at admission time, so the scheduler only consumes lane fronts here.
    // Request priority remains the outer ordering rule.
    //
    // RFC #2519 step 3 (opt-in): drop is active only when a positive threshold,
    // deadline awareness, and a bandwidth provider are all present.
    const bool drop_enabled = limits_.deadline_aware &&
                              limits_.mlu_local_threshold > 0.0 &&
                              static_cast<bool>(bandwidth_provider_);
    const bool promotion_enabled =
        limits_.deadline_aware && limits_.promotion_slack_ns > 0;
    const bool need_now = drop_enabled || promotion_enabled;
    const double bw_bps = drop_enabled ? bandwidth_provider_() : 0.0;
    const uint64_t now_ns =
        need_now
            ? (now_provider_
                   ? now_provider_()
                   : static_cast<uint64_t>(
                         std::chrono::duration_cast<std::chrono::nanoseconds>(
                             std::chrono::steady_clock::now()
                                 .time_since_epoch())
                             .count()))
            : 0;

    // Deadline proximity promotion runs within each priority/kind lane.
    if (promotion_enabled) {
        scheduler_.promoteDeadlineUrgentOwners(now_ns,
                                               limits_.promotion_slack_ns,
                                               owners_);
    }

    // Predicted MLU = predicted_transfer_time / remaining_window. Returns true
    // if the owner is predicted to miss its deadline hard enough to drop.
    auto shouldDrop = [&](const QueueOwner& owner) -> bool {
        if (!drop_enabled || !owner.degradation_eligible || bw_bps <= 0.0)
            return false;
        const uint64_t deadline_ns = owner.request.deadline_ns;
        if (deadline_ns == 0) return false;      // no deadline
        if (deadline_ns <= now_ns) return true;  // already past
        const double window_s = (deadline_ns - now_ns) / 1e9;
        const double predicted_time_s = owner.request.length / bw_bps;
        const double mlu = predicted_time_s / window_s;
        return mlu >= limits_.mlu_local_threshold;
    };

    auto dropOwner = [&](QueueOwnerId owner_id, QueueOwner& owner) {
        owner.state = QueueState::Terminal;
        owner.terminal_status = TransferStatusEnum::CANCELED;
        --outstanding_owners_;
        outstanding_bytes_ -= owner.request.length;
        if (owner.kind == QueueOwnerKind::User) {
            --outstanding_user_owners_;
            outstanding_user_bytes_ -= owner.request.length;
        }
        if (dropped_owner_ids) dropped_owner_ids->push_back(owner_id);
        if (degradation_hooks_.on_local_decode_suggested) {
            degradation_hooks_.on_local_decode_suggested(owner.request);
        }
    };

    size_t used_owners = 0;
    size_t used_bytes = 0;
    while (used_owners < max_owners && used_bytes < max_bytes) {
        const auto candidate = scheduler_.next(owners_);
        if (!candidate.found) break;

        auto owner_it = owners_.find(candidate.owner_id);
        if (owner_it == owners_.end()) continue;

        // Step 3: a dropped owner does not consume dispatch budget. Continue
        // at the same priority so an infeasible head cannot block its lane.
        if (shouldDrop(owner_it->second)) {
            scheduler_.consume(candidate);
            dropOwner(candidate.owner_id, owner_it->second);
            continue;
        }

        const auto& owner = owner_it->second;
        const size_t remaining_bytes = max_bytes - used_bytes;
        if (owner.request.length > remaining_bytes) break;

        scheduler_.consume(candidate);
        owner_it->second.state = QueueState::Dispatching;
        picked.push_back(candidate.owner_id);
        ++used_owners;
        used_bytes += owner.request.length;
    }
    return picked;
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

Status LocalTransferAdmissionQueue::cancel(QueueOwnerId owner_id) {
    if (owner_id == 0) {
        return Status::InvalidArgument("invalid queue owner id" LOC_MARK);
    }
    auto owner_it = owners_.find(owner_id);
    if (owner_it == owners_.end()) {
        return Status::InvalidEntry("queue owner not found" LOC_MARK);
    }
    auto& owner = owner_it->second;
    if (owner.state == QueueState::Terminal) {
        return owner.terminal_status == TransferStatusEnum::CANCELED
                   ? Status::OK()
                   : Status::InvalidEntry(
                         "queue owner is already terminal" LOC_MARK);
    }
    if (owner.state != QueueState::Queued) {
        return Status::InvalidEntry(
            "queue owner is already dispatching" LOC_MARK);
    }

    owner.state = QueueState::Terminal;
    owner.terminal_status = TransferStatusEnum::CANCELED;
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
